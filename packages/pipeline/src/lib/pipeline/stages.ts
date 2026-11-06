/**
 * Stage handlers for the server half of the pipeline:
 *
 *   object landed → object-landed ─┬→ session-indexed (raw pointer, required)
 *                                  └→ file-normalize  (optional transform)
 *
 * `object-landed` validates the landed object and fans out two independent
 * messages. The pointer is enqueued first and the transform second, each on its
 * own queue and DLQ, so a transform that throws, times out or is dead-lettered
 * can never delay or undo raw pointer indexing. Raw is canonical; the transform
 * output is derived and can be rebuilt by replay.
 *
 * The concrete pointer index (Supabase `sessions`) and transform live with their
 * owning workstreams; here they are interfaces.
 */
import { PipelineError } from "./errors.js";
import type { DeadLetter, StageHandler, StageMessage } from "./dlq.js";
import {
  objectKeyIdentity,
  type PipelineStage,
  type TraceContext,
} from "./trace.js";

/** What an object store notification is normalized to before it is queued. */
export interface ObjectLandedPayload {
  /** Which object store reported it; the adapter sets it. */
  provider?: ObjectStoreProvider;
  bucket?: string;
  key: string;
  kind: "manifest" | "chunk";
  size?: number;
  etag?: string;
  /** Manifest object version/etag the event refers to, when the store reports it. */
  version?: string;
  /** The manifest's own `revision` (monotonic per session), read by the adapter. */
  revision?: number;
  /** The manifest's `raw.scrub` verdict, when the adapter read it. */
  scrubbed?: boolean;
  /**
   * The object was deleted. A deleted manifest removes the session's pointer;
   * the adapter gives each delete its own `version` (`deleted:<id>`).
   */
  deleted?: boolean;
  /**
   * Set only by the ingest for a RelayAuth migration commit (relayauth #111;
   * `migration-assertions.ts`): the signed manifest key (without any
   * deployment prefix) and the signed plan entry digest. Its work key is then
   * `${key}@${entry_sha256}` instead of `manifestWorkKey`. Only the ingest
   * writes queue messages, so this cannot come from a client.
   */
  migration?: { key: string; entry_sha256: string; base_revision?: number };
}

export type ObjectStoreProvider = "r2" | "s3" | "gcs" | "azure" | "fs";

export interface RawPointer {
  traceId: string;
  workspaceId: string;
  machineId?: string;
  harness?: string;
  sessionId?: string;
  manifestKey: string;
  manifestVersion?: string;
  provider?: ObjectStoreProvider;
  bucket?: string;
  scrubbed?: boolean;
  /** The manifest was deleted: remove the pointer rather than upsert it. */
  deleted?: boolean;
  /** Migration work: see `ObjectLandedPayload.migration`. */
  migration?: ObjectLandedPayload["migration"];
}

export interface SessionPointerIndex {
  /**
   * Idempotent: the same pointer twice leaves one row. `skipped` records work
   * that was handled without a write (a stale revision, a deleted session).
   */
  upsertRawPointer(
    pointer: RawPointer,
    ctx: TraceContext,
  ): Promise<void | { status: "skipped"; reason: string }>;
}

/** The stored normalized object, described from its own bytes. */
export interface NormalizedOutput {
  outputKey: string;
  /** The manifest this run read: `contentSha256` is its `raw.content_sha256`. */
  revision: number;
  normalizedVersion: number;
  contentSha256: string;
  /** Lowercase hex SHA-256 of the stored object. */
  outputSha256: string;
  outputBytes: number;
  /** The object's `convergence_events` lines. */
  convergenceEventCount: number;
  /** Records with no projection, which leave no line in the object. */
  unprojectedRecords: number;
  /** The object was already stored, so this run published nothing. */
  outputReused: boolean;
  /** Reused, and this run's bytes differ: same input and version, different output. */
  outputDivergent: boolean;
}

/** The manifest version a message was enqueued for. */
export interface NormalizeTarget {
  revision?: number;
  workKey?: string;
  orgId?: string;
}

/** A delayed message: a newer manifest stands, and its own event normalizes it. */
export interface NormalizeSkipped {
  skipped: "stale_manifest";
}

export interface SessionTransform {
  /** Idempotent by input key and transform version. Empty when nothing was normalized. */
  normalize(
    pointer: RawPointer,
    target?: NormalizeTarget,
  ): Promise<NormalizedOutput | NormalizeSkipped | Record<string, never>>;
}

export interface StageSenders {
  send(stage: PipelineStage, message: StageMessage): Promise<void>;
}

/**
 * The work key for a manifest version: `<object key>@<version or etag>`. A
 * delete with neither is `<object key>@deleted`, so it never shares a dead
 * letter with a put. The storage-notification adapter must use it as the
 * storage event's `workKey`; the index and transform work for that version
 * share it.
 */
export function manifestWorkKey(payload: ObjectLandedPayload) {
  const version =
    payload.version ?? payload.etag ?? (payload.deleted ? "deleted" : "");
  return `${payload.key}@${version}`;
}

const UNPREFIXED_MANIFEST_KEY =
  /(?:^|\/)(v1\/ws=[^/]+\/m=[^/]+\/h=[^/]+\/d=\d{4}\/\d{2}\/\d{2}\/s=[^/]+\/manifest\.json)$/;

/**
 * The work key an `object-landed` message must carry. Live work (no
 * `migration` field): `manifestWorkKey`, the object key and its version.
 * Migration work: the signed `${migration.key}@${migration.entry_sha256}`,
 * only when `migration.key` is exactly this object's key without its
 * deployment prefix and the digest is 64 lowercase hex; anything else is
 * `undefined` (refused). A message cannot be both: the field decides, and a
 * migration message with a malformed field never falls back to the live rule.
 */
export function objectLandedWorkKey(
  payload: ObjectLandedPayload,
): string | undefined {
  const migration = payload.migration;
  if (migration === undefined) return manifestWorkKey(payload);
  const unprefixed = UNPREFIXED_MANIFEST_KEY.exec(payload.key)?.[1];
  if (
    !migration ||
    typeof migration !== "object" ||
    payload.kind !== "manifest" ||
    payload.deleted === true ||
    typeof migration.key !== "string" ||
    typeof migration.entry_sha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(migration.entry_sha256) ||
    (migration.base_revision !== undefined &&
      !(
        Number.isSafeInteger(migration.base_revision) &&
        migration.base_revision >= 1
      )) ||
    unprefixed === undefined ||
    migration.key !== unprefixed
  )
    return undefined;
  // A v2 backfill names the base revision it appended to (migrationWorkKey).
  return migration.base_revision === undefined
    ? `${migration.key}@${migration.entry_sha256}`
    : `${migration.key}@${migration.entry_sha256}@r${migration.base_revision + 1}`;
}

export function objectLandedHandler(deps: {
  senders: StageSenders;
  /** Where a transform that cannot be enqueued waits for replay. */
  quarantine: (message: StageMessage, at: Date) => Promise<DeadLetter>;
  /** False until the normalize transform is enabled for this deployment. */
  transformEnabled: boolean;
}): StageHandler {
  return async (message, recorder) => {
    const payload = message.payload as ObjectLandedPayload;
    if (!payload || typeof payload.key !== "string")
      throw new PipelineError(
        "invalid_storage_event",
        "missing object key",
        false,
      );
    const identity = objectKeyIdentity(payload.key);
    if (!identity || identity.workspaceId !== message.ctx.workspaceId)
      throw new PipelineError(
        "invalid_object_key",
        "object key does not match the trace workspace",
        false,
      );
    // Chunks land before their manifest; only the manifest makes a session readable.
    if (payload.kind !== "manifest")
      return { status: "skipped", reason: "chunk" };
    const deleted = payload.deleted === true;
    const pointer: RawPointer = {
      traceId: message.ctx.traceId,
      ...identity,
      manifestKey: payload.key,
      ...(payload.version ? { manifestVersion: payload.version } : {}),
      ...(payload.provider ? { provider: payload.provider } : {}),
      ...(payload.bucket ? { bucket: payload.bucket } : {}),
      ...(typeof payload.scrubbed === "boolean"
        ? { scrubbed: payload.scrubbed }
        : {}),
      ...(deleted ? { deleted: true } : {}),
      ...(payload.migration ? { migration: payload.migration } : {}),
    };
    const workKey = objectLandedWorkKey(payload);
    // The trace verdict matches index success to manifest work by this key, so
    // the storage event must carry it (see `objectLandedWorkKey`: live work
    // by object version, migration work by its signed key and entry digest).
    if (workKey === undefined || message.workKey !== workKey)
      throw new PipelineError(
        "invalid_work_key",
        "storage event workKey must be objectLandedWorkKey(payload)",
        false,
      );
    const revision = Number.isSafeInteger(payload.revision)
      ? payload.revision
      : message.revision;
    // Every event this handler records about the manifest's work carries its
    // revision, so a late notification for an older version sorts as older.
    const revisionAttr = revision !== undefined ? { revision } : {};
    await deps.senders.send("session-indexed", {
      v: 1,
      stage: "session-indexed",
      ctx: message.ctx,
      workKey,
      ...(revision !== undefined ? { revision } : {}),
      payload: pointer,
    });
    // Nothing to normalize once the manifest is gone.
    if (deleted) return;
    if (!deps.transformEnabled) {
      await recorder.record("file-normalize", "skipped", {
        attrs: { reason: "not_enabled", work_key: workKey, ...revisionAttr },
        key: workKey,
      });
      return;
    }
    const transform: StageMessage = {
      v: 1,
      stage: "file-normalize",
      ctx: message.ctx,
      workKey,
      ...(revision !== undefined ? { revision } : {}),
      payload: pointer,
    };
    try {
      await deps.senders.send("file-normalize", transform);
    } catch (error) {
      // The pointer is already queued, so the transform is quarantined rather
      // than dropped: replay re-sends it once the queue is back. Only if that
      // also fails does the storage event retry (re-sending the pointer is a
      // harmless upsert).
      const letter = await deps.quarantine(transform, new Date());
      await recorder.record("file-normalize", "dead_lettered", {
        error,
        errorCode: "transform_enqueue_failed",
        attrs: {
          dead_letter_id: letter.id,
          work_key: workKey,
          ...revisionAttr,
        },
        key: `${workKey}:${letter.deadCount}`,
      });
    }
  };
}

export function sessionIndexedHandler(
  index: SessionPointerIndex,
): StageHandler {
  return async (message) =>
    index.upsertRawPointer(message.payload as RawPointer, message.ctx);
}

export function fileNormalizeHandler(
  transform: SessionTransform,
): StageHandler {
  return async (message, recorder) => {
    const output = await transform.normalize(message.payload as RawPointer, {
      ...(message.revision !== undefined ? { revision: message.revision } : {}),
      workKey: message.workKey,
      orgId: message.ctx.orgId,
    });
    if ("skipped" in output)
      return { status: "skipped", reason: output.skipped };
    if (output.outputKey)
      recorder.log.info("transform output", {
        output_key: output.outputKey,
        output_bytes: output.outputBytes,
        convergence_event_count: output.convergenceEventCount,
        unprojected_records: output.unprojectedRecords,
        output_reused: output.outputReused,
        output_divergent: output.outputDivergent,
      });
  };
}
