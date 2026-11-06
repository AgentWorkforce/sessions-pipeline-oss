/**
 * Stage 1 for an S3 event: the pipeline's `object-landed` handler, then
 * `session-indexed` through the HoopDb pointer index, run in process instead
 * of across two Postgres queue hops. The handler code is the same the
 * hoopdb-indexer runs (packages/pipeline stages.ts + src/hoopdb/index.ts):
 * the Supabase row is written only by `sessions.upsert_session_index`
 * (revision-fenced: a late, duplicated or replayed event never rolls a row
 * back), then `session_index_ext` and the logical `session_directory` row.
 *
 * Serialization. The read and the upsert run under the session's pointer lock
 * (a transaction-scoped advisory lock in Supabase), so a put
 * whose read raced an erasure cannot upsert after the delete's tombstone: the
 * second holder re-reads S3 and sees what the first left. Without it, a
 * revision-2 upsert that read before the delete would land after the
 * tombstone and bring the row back for good (`upsert_session_index` lets a
 * newer revision undelete).
 *
 * The event is a hint. The manifest is always re-read, so:
 *   - a put whose manifest is gone again is handled as a delete;
 *   - a delete whose key still has a current manifest (an old version deleted
 *     by id, or a re-upload) indexes the current manifest instead.
 */
import {
  buildObjectDeletedMessage,
  buildObjectLandedMessage,
  objectLandedHandler,
  PipelineError,
  PipelineRecorder,
  logOnlyPipelineEventSink,
  manifestKey,
  parseSessionKey,
  type ManifestReader,
  type OrgResolver,
  type PipelineQueue,
  type StageMessage,
} from "../shared/vendor.ts";
import type { ObjectLanded } from "@rhc/lib/pipeline/adapter.ts";
import type { HoopDb,Transact } from "../shared/db.ts";
import { enrichMessage } from "../shared/enrich.ts";
import { hoopDbSessionIndex } from "../hoopdb/index.ts";

export interface SessionScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}

export type PointerOutcome =
  | { status: "ignored"; reason: string }
  | { status: "indexed" | "unchanged" | "deleted" | "moved"; scope: SessionScope; manifestKey: string; revision?: number };

/**
 * The per-session lock keys. The pointer stage and the search stage each
 * serialize among themselves. They need not exclude each other, because a
 * search pass takes its lock after its own event's pointer committed. Keeping
 * them apart means a long search pass never holds up a pointer write.
 */
export function scopeLockKey(scope: SessionScope, stage: "pointer" | "search" = "pointer"): string {
  return [`96bulls:${stage}`, scope.orgId, scope.workspaceId, scope.harness, scope.sessionId].join("\u001f");
}

/** Run `work` while holding the session's lock (lambda.ts: pg_advisory_xact_lock in Supabase). */
export type ScopeLock = <T>(scope: SessionScope, work: () => Promise<T>) => Promise<T>;

export interface PointerDeps {
  db: HoopDb;
  lock: ScopeLock;
  reader: ManifestReader;
  /** HEAD: whether a manifest key has a current object (the move probe). */
  exists(key: string): Promise<boolean>;
  orgs: OrgResolver;
  allowPrefix(prefix: string | undefined, orgId: string): boolean;
  /** Where the index's fan-out (summary-short, summary-long, status-classify) goes. */
  fanout: PipelineQueue;
  publication?:{schemaGeneration:string;transact:Transact};
  now?: () => Date;
}

/** A fan-out queue that drops messages: no summary or state consumer runs in production yet. */
export const droppedFanout: PipelineQueue = {
  backend: "memory",
  async send() {},
  async sendBatch() {},
};

export async function indexPointer(landed: ObjectLanded, deps: PointerDeps): Promise<PointerOutcome> {
  const location = parseSessionKey(landed.key);
  if (!location) return { status: "ignored", reason: "not_a_session_key" };
  if(location.prefix?.includes('/upload-generations/'))return{status:"ignored",reason:"managed_publication_hint"};
  if (location.rest !== "manifest.json") return { status: "ignored", reason: "not_a_manifest" };
  if (landed.bucket && deps.reader.bucket && landed.bucket !== deps.reader.bucket)
    return { status: "ignored", reason: "other_bucket" };
  const orgId = await deps.orgs.orgFor(location);
  // A key under the deployment prefix always names its org; no org means a key
  // this deployment does not own.
  if (!orgId) return { status: "ignored", reason: "org_unresolved" };
  if (!deps.allowPrefix(location.prefix, orgId)) return { status: "ignored", reason: "outside_prefix" };
  // A legacy key of an adopted managed carrier must not update ext/fanout either.
  if(location.machineId.startsWith('srv_')){
    const managed=await deps.db.query<{managed:boolean}>("SELECT sessions.session_publication_is_managed($1,$2,$3,$4,$5) AS managed",
      [orgId,location.workspaceId,location.harness,location.sessionId,location.machineId]);
    if(managed.rows[0]?.managed===true)return{status:"ignored",reason:"managed_publication_hint"};
  }
  const scope: SessionScope = {
    orgId,
    workspaceId: location.workspaceId,
    harness: location.harness,
    sessionId: location.sessionId,
  };

  return deps.lock(scope, () => indexLocked(landed, orgId, scope, deps));
}

/** How far either side of a deleted key's date the same session is looked for (an exporter date move). */
export const MOVE_PROBE_DAYS = 45;

/**
 * The same session (same machine) live under another start date, if any. The
 * exporter re-keys a session whose first record moved earlier, writes the new
 * key, then erases the old prefix; the pointer row (whose identity has no date)
 * must follow the new key, not be tombstoned with the old one. Returns the
 * nearest live key, or undefined.
 */
export async function findMovedSibling(key: string, exists: (key: string) => Promise<boolean>): Promise<string | undefined> {
  const loc = parseSessionKey(key);
  if (!loc) return undefined;
  const base = Date.parse(`${loc.startDate}T00:00:00Z`);
  const offsets: number[] = [];
  for (let d = 1; d <= MOVE_PROBE_DAYS; d++) offsets.push(-d, d);
  for (let i = 0; i < offsets.length; i += 10) {
    const batch = offsets.slice(i, i + 10).map((d) => manifestKey({ ...loc, startDate: new Date(base + d * 86_400_000).toISOString().slice(0, 10) }));
    const found = await Promise.all(batch.map(async (k) => ((await exists(k)) ? k : undefined)));
    const hit = found.find((k) => k !== undefined);
    if (hit) return hit;
  }
  return undefined;
}

async function indexLocked(landed: ObjectLanded, orgId: string, scope: SessionScope, deps: PointerDeps, followed = false): Promise<PointerOutcome> {
  // Canonical state, not the event: re-read the manifest, under the lock.
  const read = await deps.reader.read(landed.key);
  if (!read && !followed) {
    // Gone. Before tombstoning, look for the same session under another date.
    const sibling = await findMovedSibling(landed.key, deps.exists);
    if (sibling) {
      const { etag: _e, version: _v, size: _s, deleted: _d, ...rest } = landed;
      return indexLocked({ ...rest, key: sibling }, orgId, scope, deps, true);
    }
  }
  const built = read
    ? buildObjectLandedMessage({
        landed: { ...landed, deleted: false },
        orgId,
        manifest: read.manifest,
        read: { ...(read.etag ? { etag: read.etag } : {}), ...(read.version ? { version: read.version } : {}) },
      })
    : buildObjectDeletedMessage({
        landed: { ...landed, deleted: true },
        orgId,
        ...(deps.now ? { now: deps.now() } : {}),
      });
  if (built.status === "ignored") return { status: "ignored", reason: built.reason };
  const landedMessage = await enrichMessage(built.message, deps.reader);

  let indexed: StageMessage | undefined;
  const recorder = new PipelineRecorder(landedMessage.ctx, { sink: logOnlyPipelineEventSink });
  await objectLandedHandler({
    senders: {
      async send(stage, message) {
        if (stage === "session-indexed") indexed = message;
      },
    },
    quarantine: async () => {
      throw new PipelineError("transform_disabled", "file-normalize is not enabled here", false);
    },
    transformEnabled: false,
  })(landedMessage, recorder, 1);
  if (!indexed) return { status: "ignored", reason: "no_pointer" };
  const message = await enrichMessage(indexed, deps.reader);

  const index = hoopDbSessionIndex({ db: deps.db, reader: deps.reader, queue: deps.fanout, current: () => message,publication:deps.publication });
  const upsert = () => index.upsertRawPointer(message.payload as Parameters<typeof index.upsertRawPointer>[0], message.ctx);
  const result = await upsert();
  if(result?.status==="skipped"&&result.reason==="publication_retired")return{status:"ignored",reason:"managed_publication_hint"};
  const revision = message.revision;
  if (!read) return { status: "deleted", scope, manifestKey: landed.key };

  // A moved session: the row names another key of the same identity (the start
  // date, and so `d=`, changed), and the revision fence kept this key out,
  // because a new key restarts its revision count. The key that is live in S3
  // wins; while both are live the move is in progress and the event retries.
  const pk = [orgId, scope.workspaceId, scope.harness, scope.sessionId, read.manifest.session.machine_id];
  const stored = (
    await deps.db.query<{ manifest_key: string; deleted: boolean }>(
      `SELECT manifest_key, deleted_at IS NOT NULL AS deleted FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5`,
      pk,
    )
  ).rows[0];
  if (stored && stored.manifest_key !== landed.key) {
    const otherLive = !stored.deleted && (await deps.reader.read(stored.manifest_key)) !== null;
    if (otherLive)
      throw new PipelineError("pointer_move_pending", "the session's other key is still live; retry after its erasure", true);
    // The stored key is gone (or tombstoned): re-key the row to this one. The
    // row is derived, and the upsert rebuilds it from this manifest.
    await deps.db.query(
      `DELETE FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5
          AND manifest_key = $6`,
      [...pk, stored.manifest_key],
    );
    await upsert();
    return { status: "moved", scope, manifestKey: landed.key, ...(revision !== undefined ? { revision } : {}) };
  }
  return {
    status: result && result.status === "skipped" ? "unchanged" : "indexed",
    scope,
    manifestKey: landed.key,
    ...(revision !== undefined ? { revision } : {}),
  };
}
