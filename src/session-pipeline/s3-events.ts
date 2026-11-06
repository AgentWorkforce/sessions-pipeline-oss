/**
 * The S3 half of the pipeline's StorageEventSource (adapter.ts plugs in R2;
 * this is "S3 via SQS"): one SQS message body from an S3 event notification
 * becomes zero or more normalized `ObjectLanded`s.
 *
 * S3 sends `{"Records":[...]}` (usually one record) and, once when a
 * notification is configured, `{"Event":"s3:TestEvent",...}`, which is
 * ignored. Keys arrive URL-encoded with `+` for a space. Puts are any
 * `ObjectCreated:*`; a delete is `ObjectRemoved:Delete` (a version deleted by
 * id, which is what the erasure job does) or `ObjectRemoved:DeleteMarkerCreated`
 * (a plain delete on a versioned bucket). Lifecycle expiry of noncurrent
 * versions is not notified and needs nothing: the current version is unchanged.
 *
 * The scheduled reconcile puts `{"reconcile":{"key","etag","version"}}` bodies
 * on the same queue, so its work takes the same path, retries and DLQ.
 */
import type { ObjectLanded } from "@rhc/lib/pipeline/adapter.ts";

export const RECONCILE_BODY_KEY = "reconcile";

interface S3Record {
  eventSource?: unknown;
  eventName?: unknown;
  eventTime?: unknown;
  s3?: {
    bucket?: { name?: unknown };
    object?: { key?: unknown; size?: unknown; eTag?: unknown; versionId?: unknown; sequencer?: unknown };
  };
}

export interface LandedEvent extends ObjectLanded {
  /** S3's per-key ordering token (hex, compare by length then value). */
  sequencer?: string;
}

export type ParsedBody =
  | { kind: "events"; events: LandedEvent[] }
  | { kind: "test" }
  | { kind: "invalid" };

/** S3 encodes keys like form values: `+` is a space, everything else percent-encoded. */
export function decodeS3Key(raw: string): string | null {
  try {
    return decodeURIComponent(raw.replace(/\+/g, " "));
  } catch {
    return null;
  }
}

function fromRecord(record: S3Record): LandedEvent | null {
  if (record.eventSource !== "aws:s3" || typeof record.eventName !== "string") return null;
  const name = record.eventName;
  const created = name.startsWith("ObjectCreated:");
  const deleted = name === "ObjectRemoved:Delete" || name === "ObjectRemoved:DeleteMarkerCreated";
  if (!created && !deleted) return null;
  const object = record.s3?.object;
  if (typeof object?.key !== "string" || !object.key) return null;
  const key = decodeS3Key(object.key);
  if (!key) return null;
  const bucket = record.s3?.bucket?.name;
  return {
    provider: "s3",
    key,
    ...(typeof bucket === "string" ? { bucket } : {}),
    ...(created && typeof object.eTag === "string" && object.eTag ? { etag: object.eTag.replace(/^"|"$/g, "") } : {}),
    ...(created && typeof object.versionId === "string" && object.versionId ? { version: object.versionId } : {}),
    ...(created && typeof object.size === "number" ? { size: object.size } : {}),
    ...(typeof record.eventTime === "string" ? { eventTime: record.eventTime } : {}),
    ...(typeof object.sequencer === "string" && object.sequencer ? { sequencer: object.sequencer } : {}),
    ...(deleted ? { deleted: true } : {}),
  };
}

/** Parse one SQS message body. Never throws. */
export function parseQueueBody(body: string): ParsedBody {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return { kind: "invalid" };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return { kind: "invalid" };
  const v = value as Record<string, unknown>;
  if (v.Event === "s3:TestEvent") return { kind: "test" };
  const reconcile = v[RECONCILE_BODY_KEY] as { key?: unknown; etag?: unknown; version?: unknown } | undefined;
  if (reconcile && typeof reconcile === "object") {
    if (typeof reconcile.key !== "string" || !reconcile.key) return { kind: "invalid" };
    return {
      kind: "events",
      events: [
        {
          provider: "s3",
          key: reconcile.key,
          ...(typeof reconcile.etag === "string" && reconcile.etag ? { etag: reconcile.etag } : {}),
          ...(typeof reconcile.version === "string" && reconcile.version ? { version: reconcile.version } : {}),
        },
      ],
    };
  }
  if (!Array.isArray(v.Records)) return { kind: "invalid" };
  const events: LandedEvent[] = [];
  for (const record of v.Records as S3Record[]) {
    const event = record && typeof record === "object" ? fromRecord(record) : null;
    if (event) events.push(event);
  }
  return { kind: "events", events };
}

/** S3 sequencers order events for one key: longer is later, then lexical. */
export function sequencerAfter(a: string | undefined, b: string | undefined): boolean {
  if (a === undefined) return false;
  if (b === undefined) return true;
  const len = Math.max(a.length, b.length);
  return a.padEnd(len, "0") > b.padEnd(len, "0");
}
