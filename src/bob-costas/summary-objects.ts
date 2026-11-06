// Ported from AgentWorkforce/relayhistory-cloud
// packages/relayhistory/src/lib/pipeline/summary-objects.ts @ 73434047b6d7
// (spec: docs/pipeline-summary-short.md). Body verbatim; only the imports differ
// (this repo's vendored pipeline and object store), plus `summaryObjectKey` and
// `SummaryKind` copied from upstream object-layout.ts, a `BufferSource` alias
// (this repo's tsconfig has no DOM lib), and one marked deviation in
// writeSummaryObject (reuse only under the current key, for rotation).
// Keep in sync on upstream changes.
/**
 * Summary objects in the bucket: the encrypted envelope at
 * `summary/{short,long}.json.enc` (`summaryObjectKey`), its compare-and-swap
 * writer and its reader. Shared by `summary-short` and, later,
 * `summary-long`.
 *
 * Supabase holds only the pointer (key, ciphertext SHA-256, key id, status,
 * revision); the text exists only in this object, encrypted under the
 * workspace's data key (AES-256-GCM). The key itself comes from an injected
 * `WorkspaceKeyring`; nothing here holds or derives one.
 *
 * Writes are fenced by revision without decrypting: the stored revision is in
 * the object's metadata, and each write is `ifMatch` on the version it read
 * (or create-only), so a late writer for an older revision never replaces a
 * newer summary. Nothing read or written here is logged.
 */
import {
  PipelineError,
  ObjectStoreError,
  isObjectStoreError,
  joinKey,
  objectKey,
  readAll,
  sessionPrefix,
  type ObjectKey,
  type ObjectStore,
  type SessionLocation,
} from "../shared/vendor.ts";

/**
 * From upstream `object-layout.ts` (same commit): the two summary kinds and
 * their key next to the session's raw.
 */
export const SUMMARY_KINDS = ["short", "long"] as const;
type BufferSource = ArrayBufferView | ArrayBuffer;
export type SummaryKind = (typeof SUMMARY_KINDS)[number];

export function summaryObjectKey(loc: SessionLocation, kind: SummaryKind): ObjectKey {
  return joinKey(sessionPrefix(loc), `summary/${kind}.json.enc`);
}

/** The envelope format (`v` in the stored JSON). */
export const SUMMARY_ENVELOPE_VERSION = 1;
/** Object metadata names; values are ids and numbers, never content. */
export const META_SUMMARY_REVISION = "summary-revision";
export const META_SUMMARY_KIND = "summary-kind";
export const META_SUMMARY_KEY_ID = "summary-key-id";
export const META_SUMMARY_STATUS = "summary-status";
/**
 * The manifest the summary describes: the SHA-256 of the indexed manifest
 * body. One per revision of one upload, so a re-upload that restarts its
 * revisions never matches an old object.
 */
export const META_SUMMARY_MANIFEST = "summary-manifest";

export type SummaryStatus =
  | "ready"
  | "disabled"
  | "quarantined"
  | "empty"
  | "unsupported";

/** What a summary object says, once decrypted. */
export interface SummaryRecord {
  v: 1;
  kind: SummaryKind;
  revision: number;
  status: SummaryStatus;
  /** Present only when `status` is `ready`. */
  summary?: string;
  model?: string;
  built_at: string;
  /** `raw.content_sha256` of the raw it was built from (ready only). */
  source_content_sha256?: string;
  trace_id: string;
}

export interface WorkspaceIds {
  orgId: string;
  workspaceId: string;
}

/**
 * The per-workspace data keys. Envelope encryption: a real keyring unwraps
 * each workspace's AES-256 key with a KMS or master key. Throw for a lookup
 * that failed; the stage retries.
 */
export interface WorkspaceKeyring {
  /** The key new summaries are written under. */
  currentKey(ids: WorkspaceIds): Promise<{ keyId: string; key: CryptoKey }>;
  /** A key by id, to read what an older key wrote. */
  keyById(ids: WorkspaceIds, keyId: string): Promise<CryptoKey>;
}

/**
 * The keyring until key custody is decided: it refuses, so no summary is ever
 * written unencrypted or under a made-up key. The stage stays off
 * (`PIPELINE_SUMMARY_SHORT_ENABLED`) until a real keyring is supplied.
 */
export const unconfiguredKeyring: WorkspaceKeyring = {
  currentKey: async () => {
    throw new PipelineError(
      "keyring_unconfigured",
      "no workspace keyring is configured",
      true,
    );
  },
  keyById: async () => {
    throw new PipelineError(
      "keyring_unconfigured",
      "no workspace keyring is configured",
      true,
    );
  },
};

const MESSAGES = {
  keyring_unavailable: "the workspace key lookup failed",
  summary_store_unavailable: "the summary object write failed",
  summary_store_rejected: "the object store refused the summary object",
  summary_contended: "the summary object kept changing under the writer",
  summary_store_timeout: "the object store did not answer in time",
} as const;

export type SummaryObjectErrorCode = keyof typeof MESSAGES;

function fail(code: SummaryObjectErrorCode, retryable: boolean): PipelineError {
  return new PipelineError(code, MESSAGES[code], retryable);
}

interface Envelope {
  v: 1;
  alg: "A256GCM";
  key_id: string;
  iv: string;
  ct: string;
}

export interface WrittenSummary {
  outcome: "written";
  key: string;
  /** Lowercase hex SHA-256 of the stored envelope. */
  sha256: string;
  keyId: string;
}

const MAX_WRITE_ATTEMPTS = 4;
/** Deadline for each object store call, so a stalled store never holds the summary lock. */
const DEFAULT_STORE_TIMEOUT_MS = 5_000;
/** The largest summary envelope read back; a long summary is a few KiB. */
const MAX_SUMMARY_OBJECT_BYTES = 1024 * 1024;

/**
 * Write `record` to `key` for the session's current manifest.
 *
 * The fence is the index row, not the object: `current` is the row's
 * revision and manifest SHA-256, read under the session's summary lock by the
 * caller, which holds it until its pointer is written. A record for any other
 * revision is refused (`stale`) without writing. Any stored object for another
 * manifest is older or from an earlier upload (revisions restart on a
 * re-upload, so they cannot be compared across uploads), and is overwritten.
 *
 * A stored object for the same manifest and status is reused, not rewritten:
 * every attempt at one outcome publishes the same bytes and hash, so a retry
 * can never leave the row pointing at a hash the object no longer has. The
 * same manifest with another status (an opt-out after a summary) is
 * overwritten.
 */
export async function writeSummaryObject(args: {
  store: ObjectStore;
  keyring: WorkspaceKeyring;
  ids: WorkspaceIds;
  key: string;
  record: SummaryRecord;
  /** The index row under the caller's lock: its revision and manifest SHA-256. */
  current: { revision: number; manifestSha256: string };
  storeTimeoutMs?: number;
}): Promise<WrittenSummary | { outcome: "stale"; storedRevision: number }> {
  const { store, record } = args;
  if (record.revision !== args.current.revision)
    return { outcome: "stale", storedRevision: args.current.revision };
  const k = objectKey(args.key);
  const call = <T>(fn: () => Promise<T>) =>
    withDeadline(fn, args.storeTimeoutMs);
  let current: { keyId: string; key: CryptoKey } | undefined;
  let sealed: { body: Uint8Array; sha256: string } | undefined;
  for (let attempt = 0; attempt < MAX_WRITE_ATTEMPTS; attempt++) {
    let head;
    try {
      head = await call(() => store.head(k));
    } catch (err) {
      throw storeError(err);
    }
    const keyId = head?.metadata[META_SUMMARY_KEY_ID];
    // 96bulls deviation: the current key is resolved before the reuse check, and
    // an object is reused only if it is already under that key, so a key
    // rotation re-encrypts summaries instead of stranding them on a retiring key.
    if (!current) {
      try {
        current = await args.keyring.currentKey(args.ids);
      } catch (err) {
        throw err instanceof PipelineError
          ? err
          : fail("keyring_unavailable", true);
      }
      const body = await seal(current, args.key, record);
      sealed = { body, sha256: await sha256Hex(body) };
    }
    if (
      head &&
      keyId &&
      keyId === current.keyId &&
      head.metadata[META_SUMMARY_MANIFEST] === args.current.manifestSha256 &&
      head.metadata[META_SUMMARY_REVISION] === String(record.revision) &&
      head.metadata[META_SUMMARY_STATUS] === record.status
    ) {
      const reused = await storedDigest(call, store, k);
      if (reused)
        return { outcome: "written", key: args.key, sha256: reused, keyId };
    }
    try {
      await call(() =>
        store.put(k, sealed!.body, {
          ...(head ? { ifMatch: head.version } : { ifNoneMatch: true }),
          contentSha256: sealed!.sha256,
          contentType: "application/json",
          metadata: {
            [META_SUMMARY_REVISION]: String(record.revision),
            [META_SUMMARY_KIND]: record.kind,
            [META_SUMMARY_STATUS]: record.status,
            [META_SUMMARY_MANIFEST]: args.current.manifestSha256,
            [META_SUMMARY_KEY_ID]: current!.keyId,
          },
        }),
      );
      return {
        outcome: "written",
        key: args.key,
        sha256: sealed!.sha256,
        keyId: current.keyId,
      };
    } catch (err) {
      // Another writer got there between the head and the put: look again.
      if (
        isObjectStoreError(err, "precondition_failed") ||
        isObjectStoreError(err, "already_exists")
      )
        continue;
      throw storeError(err);
    }
  }
  throw fail("summary_contended", true);
}

/** The SHA-256 of the stored object, or undefined once it is gone. */
async function storedDigest(
  call: <T>(fn: () => Promise<T>) => Promise<T>,
  store: ObjectStore,
  k: ReturnType<typeof objectKey>,
): Promise<string | undefined> {
  try {
    const object = await call(() => store.getObject(k));
    if (object.meta.size > MAX_SUMMARY_OBJECT_BYTES) {
      await object.body.cancel().catch(() => undefined);
      return undefined;
    }
    return await sha256Hex(await call(() => readAll(object.body)));
  } catch (err) {
    if (isObjectStoreError(err, "not_found")) return undefined;
    throw storeError(err);
  }
}

async function withDeadline<T>(
  fn: () => Promise<T>,
  timeoutMs: number | undefined,
): Promise<T> {
  const ms =
    Number.isSafeInteger(timeoutMs) && timeoutMs! > 0
      ? timeoutMs!
      : DEFAULT_STORE_TIMEOUT_MS;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      fn(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(fail("summary_store_timeout", true)),
          ms,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Overwrite a stored summary with a record that holds no text (an opt-out,
 * quarantine or empty session), if one exists. Creates nothing for a session
 * that never had a summary.
 */
export async function clearSummaryObject(args: {
  store: ObjectStore;
  keyring: WorkspaceKeyring;
  ids: WorkspaceIds;
  key: string;
  record: SummaryRecord;
  current: { revision: number; manifestSha256: string };
  storeTimeoutMs?: number;
}): Promise<"cleared" | "absent" | "stale"> {
  if (args.record.revision !== args.current.revision) return "stale";
  let head;
  try {
    head = await withDeadline(
      () => args.store.head(objectKey(args.key)),
      args.storeTimeoutMs,
    );
  } catch (err) {
    throw storeError(err);
  }
  if (!head) return "absent";
  const written = await writeSummaryObject(args);
  return written.outcome === "written" ? "cleared" : "stale";
}

/**
 * The summary an index row points at, or null while the object does not match
 * the pointer (a write between the object and the row, or a newer revision in
 * flight). Throws for a store or key failure.
 */
export async function readSummaryObject(args: {
  store: ObjectStore;
  keyring: WorkspaceKeyring;
  ids: WorkspaceIds;
  key: string;
  sha256: string;
  kind: SummaryKind;
  revision: number;
}): Promise<SummaryRecord | null> {
  let body: Uint8Array;
  try {
    const object = await args.store.getObject(objectKey(args.key));
    if (object.meta.size > MAX_SUMMARY_OBJECT_BYTES) {
      await object.body.cancel().catch(() => undefined);
      return null;
    }
    body = await readAll(object.body);
  } catch (err) {
    if (isObjectStoreError(err, "not_found")) return null;
    throw storeError(err);
  }
  if ((await sha256Hex(body)) !== args.sha256) return null;
  const envelope = parseEnvelope(body);
  if (!envelope) return null;
  let key: CryptoKey;
  try {
    key = await args.keyring.keyById(args.ids, envelope.key_id);
  } catch (err) {
    throw err instanceof PipelineError
      ? err
      : fail("keyring_unavailable", true);
  }
  const record = await open(key, envelope, args.key, args.kind, args.revision);
  return record &&
    record.kind === args.kind &&
    record.revision === args.revision
    ? record
    : null;
}

/** Associated data: the envelope only opens at its own key, kind and revision. */
function aad(key: string, kind: SummaryKind, revision: number): Uint8Array {
  return new TextEncoder().encode(`${key}\n${kind}\n${revision}`);
}

async function seal(
  current: { keyId: string; key: CryptoKey },
  key: string,
  record: SummaryRecord,
): Promise<Uint8Array> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = await crypto.subtle.encrypt(
    {
      name: "AES-GCM",
      iv,
      additionalData: aad(key, record.kind, record.revision) as BufferSource,
    },
    current.key,
    new TextEncoder().encode(JSON.stringify(record)) as BufferSource,
  );
  const envelope: Envelope = {
    v: SUMMARY_ENVELOPE_VERSION,
    alg: "A256GCM",
    key_id: current.keyId,
    iv: base64(iv),
    ct: base64(new Uint8Array(ct)),
  };
  return new TextEncoder().encode(JSON.stringify(envelope));
}

async function open(
  key: CryptoKey,
  envelope: Envelope,
  objectKeyName: string,
  kind: SummaryKind,
  revision: number,
): Promise<SummaryRecord | null> {
  try {
    const plain = await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv: unbase64(envelope.iv) as BufferSource,
        additionalData: aad(objectKeyName, kind, revision) as BufferSource,
      },
      key,
      unbase64(envelope.ct) as BufferSource,
    );
    return JSON.parse(new TextDecoder().decode(plain)) as SummaryRecord;
  } catch {
    // Wrong key, tampered bytes, or a different key/kind/revision.
    return null;
  }
}

function parseEnvelope(body: Uint8Array): Envelope | null {
  try {
    const e = JSON.parse(new TextDecoder().decode(body)) as Partial<Envelope>;
    return e.v === SUMMARY_ENVELOPE_VERSION &&
      e.alg === "A256GCM" &&
      typeof e.key_id === "string" &&
      typeof e.iv === "string" &&
      typeof e.ct === "string"
      ? (e as Envelope)
      : null;
  } catch {
    return null;
  }
}

function storeError(err: unknown): PipelineError {
  if (err instanceof PipelineError) return err;
  return err instanceof ObjectStoreError && !err.retryable
    ? fail("summary_store_rejected", false)
    : fail("summary_store_unavailable", true);
}

function base64(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

function unbase64(text: string): Uint8Array {
  return Uint8Array.from(atob(text), (c) => c.charCodeAt(0));
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    data as Uint8Array<ArrayBuffer>,
  );
  return Array.from(new Uint8Array(digest), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}
