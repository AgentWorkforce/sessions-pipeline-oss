/**
 * Stage 2 for an S3 event: make ClickHouse's `transcript_events` for one
 * session scope equal what the session's manifests in S3 hold. This replaces
 * the relayhistory-cloud minute tail, which reads the same rows from Neon.
 *
 * Scope. A session (org, workspace, harness, session id) can have several
 * manifests: one per machine (`session_index` keys rows by machine), and the
 * Neon exporter (`m=delivery_…`) and the relayhistory-api live writer
 * (`m=srv_…`) each write their own copy of the same delivery records. The
 * scope's truth is every live pointer row's manifest, read from S3 (canonical)
 * and folded together by the pipeline's own transform:
 * `normalizeRelayhistoryRecords` keeps the newest revision of each
 * (origin, record) across all copies, and `projectRelayhistorySession` turns
 * the survivors into the `convergence_events` rows the Neon triggers would
 * write (identity included: a delivery event's machine is derived from its
 * record's origin, never from the manifest that carried it), so the same
 * event from two manifests is one ClickHouse row.
 *
 * Reconcile. As in the Neon tail (vendor/search-index/reconcile.ts): compare
 * the scope's identities and fingerprints, insert what is new or changed,
 * then remove ClickHouse rows S3 no longer has (erasure, retraction, a moved
 * ts). An incomplete read (a manifest missing, a chunk unreadable, a
 * truncated ClickHouse read) never deletes. With no live pointer left the
 * session is erased and every row of the scope goes, inside the window or not.
 *
 * Serialization. Two events for one scope run one at a time: the pointer
 * listing and the ClickHouse write happen under a transaction-scoped advisory
 * lock in Supabase, taken after the event's own pointer upsert committed, so
 * a reconcile never acts on a pointer set older than one already applied.
 *
 * Fingerprints. Rows from S3 carry `s3`-prefixed fingerprints, which never
 * equal a Neon-tail fingerprint, so the first S3 pass over a session rewrites
 * its rows once (ReplacingMergeTree keeps the newer `version`). Do not run the
 * Neon tail and this indexer at the same time: each would keep rewriting the
 * other's rows (correct, but wasted work).
 *
 * Nothing here logs content; errors carry codes only.
 */
import { ARTIFACT_MAX_SOURCE_BYTES, ARTIFACT_MAX_SOURCE_RECORDS } from "../session-read-api/canonical-artifact.ts";
import { readCanonicalScope } from "../session-read-api/canonical-projection.ts";
export { manifestUserId, readManifestRecords, projectedToNeonRow } from "../session-read-api/canonical-projection.ts";
import * as zlib from "node:zlib";
import { layoutComponent } from "../shared/layout-component.ts";
import { PipelineError, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { tsMillis } from "./vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "./vendor/search-index/schema.ts";
import { chNow, deleteRows, windowStart, type PendingDelete } from "./vendor/search-index/reconcile.ts";
import { scopeLockKey, type SessionScope } from "./pointer.ts";
import { directoryCatalogWriter, type writeDirectoryCatalog } from "../hoopdb/session-directory-catalog.ts";
import { writeDirectoryEvents, invalidateDirectoryEvents, completeErasedDirectoryEvents } from "../hoopdb/session-directory-events.ts";

export type Transact = <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;

export interface SearchIndexDeps {
  /** One Supabase transaction on one connection (for the advisory lock). */
  transact: Transact;
  reader: ManifestReader;
  store: ObjectStore;
  ch: ClickHouseClient;
  /** The deployment prefix of an org's keys, e.g. `96bulls/<org>`. */
  prefixFor(orgId: string): string;
  decompress?: (stored: Uint8Array, maxOutputLength: number) => Uint8Array;
  now?: () => Date;
  limits?: Partial<SearchIndexLimits>;
  /**
   * SESSION_DESKTOP_CATALOG_ENABLED=1 only: write the complete desktop catalog (migration
   * 20261105022000 columns). Absent or false: the pre-#182 catalog writer, exactly as before;
   * SEARCH_INDEX_ENABLED alone never selects the desktop writer
   * (docs/session-desktop-catalog.md, Activation blockers).
   */
  desktopCatalog?: boolean;
  /** Test seam (replaces the writer `desktopCatalog` selects); production writes the catalog in the same Supabase transaction. */
  writeCatalog?: typeof writeDirectoryCatalog;
  writeEvents?: typeof writeDirectoryEvents;
}

export interface SearchIndexLimits {
  /** Largest scope read into memory, summed over its manifests' raw bytes. */
  maxScopeBytes: number;
  /** Largest scope read, summed over its manifests' chunk record counts. */
  maxScopeEvents: number;
  /** Largest number of ClickHouse rows compared for one scope. */
  maxRows: number;
  /** Rows per insert. */
  insertBatch: number;
}

/** The search stage's own limits: the default path, live with SEARCH_INDEX_ENABLED=1. */
export const SEARCH_INDEX_DEFAULT_LIMITS: Readonly<SearchIndexLimits> = Object.freeze({
  // Raw text is held several times over (strings, records, projected and index
  // rows), and an out-of-memory kill fails the whole batch: stay well inside 3 GB.
  maxScopeBytes: 64 * 1024 * 1024,
  maxScopeEvents: 100_000,
  maxRows: 500_000,
  insertBatch: 5_000,
});

/**
 * Only with the replay stage (SESSION_REPLAY_PAGES_ENABLED=1): the index pass
 * must cover the same source the replay producer reads, so it shares the
 * producer's 256 MiB / 1M-record bound. This does not lift Worker limits or
 * cover production cohorts above the 256 MiB ceiling, and it is not yet proven
 * to fit the 3008 MiB Lambda (see docs/canonical-replay-pages.md).
 */
export const REPLAY_SEARCH_INDEX_LIMITS: Readonly<SearchIndexLimits> = Object.freeze({
  maxScopeBytes: ARTIFACT_MAX_SOURCE_BYTES,
  maxScopeEvents: ARTIFACT_MAX_SOURCE_RECORDS,
  maxRows: 1_000_000,
  insertBatch: 5_000,
});

export function searchIndexLimits(replayPagesEnabled: boolean): SearchIndexLimits {
  return { ...(replayPagesEnabled ? REPLAY_SEARCH_INDEX_LIMITS : SEARCH_INDEX_DEFAULT_LIMITS) };
}

export interface SearchIndexResult {
  manifests: number;
  rows: number;
  inserted: number;
  deleted: number;
  /** False when something could not be read: nothing was deleted. */
  complete: boolean;
  /** Why the read was incomplete (codes only). */
  reasons: string[];
  /** Another pass holds this session: nothing was done, re-enqueue the event. */
  busy?: boolean;
  /** Too large to index whole: no rows written, its index state marked partial. */
  partial?: boolean;
}

interface ScopeIdentity {
  machineId: string;
  sessionId: string;
  source: string;
  kind: string;
  eventId: string;
  workspaceId: string;
  tsMs: number;
  fingerprint: string;
}

/** Kinds projectRelayhistorySession always derives from exported delivery records (its ACTIVITY_KINDS). */
export const S3_ACTIVITY_KINDS = ["history", "session_event", "tool_call", "file_edit", "trajectory"] as const;

const SEP = "\u001f";
const keyOf = (i: Pick<ScopeIdentity, "machineId" | "source" | "kind" | "eventId">) =>
  [i.machineId, i.source, i.kind, i.eventId].join(SEP);

function defaultDecompress(stored: Uint8Array, maxOutputLength: number): Uint8Array {
  if (typeof zlib.zstdDecompressSync !== "function")
    throw new PipelineError("zstd_unavailable", "this runtime cannot zstd-decompress raw chunks", false);
  return new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength }));
}


async function clickhouseScope(
  ch: ClickHouseClient,
  scope: SessionScope,
  fromMs: number,
  limit: number,
  canonicalSessionId = scope.sessionId,
): Promise<ScopeIdentity[]> {
  const rows = await ch.select<{
    workspace_id: string;
    session_id: string;
    machine_id: string;
    source: string;
    kind: string;
    event_id: string;
    ts_ms: number;
    fingerprint: string;
  }>(
    `SELECT workspace_id, session_id, machine_id, source, kind, event_id,
        toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint
     FROM ${EVENTS_TABLE} FINAL
     WHERE org_id = {org:String} AND (session_id = {session:String} OR session_id = {canonical:String})
       AND workspace_id = {workspace:String} AND source = {source:String}
       AND toUnixTimestamp64Milli(ts) >= {from:Int64}
     LIMIT {limit:UInt32}`,
    {
      params: { org: scope.orgId, session: scope.sessionId, canonical: canonicalSessionId, workspace: scope.workspaceId, source: scope.harness, from: fromMs, limit },
      timeoutMs: 20_000,
    },
  );
  return rows.map((r) => ({
    workspaceId: r.workspace_id,
    sessionId: r.session_id,
    machineId: r.machine_id,
    source: r.source,
    kind: r.kind,
    eventId: r.event_id,
    tsMs: Number(r.ts_ms),
    fingerprint: r.fingerprint,
  }));
}

/**
 * The session's index state in `search_ingest_session_state` (vendored
 * schema.ts): `needs_full = 1` marks a session whose S3 pass was partial or
 * incomplete, so readers never take its rows as current; a complete pass
 * writes 0. `revision` is the highest manifest revision of the scope.
 */
export async function recordState(ch: ClickHouseClient, scope: SessionScope, revision: number, rows: number, complete: boolean, at: Date, canonicalSessionId = scope.sessionId) {
  const ts = chNow(at);
  await ch.insert(SESSION_STATE_TABLE, [...new Set([scope.sessionId, canonicalSessionId])].map(sessionId => ({
      org_id: scope.orgId,
      session_id: sessionId,
      revision,
      synced_at: ts,
      full_synced_at: complete ? ts : "1970-01-01 00:00:00.000",
      scanned_rows: Math.min(rows, 4_294_967_295),
      needs_full: complete ? 0 : 1,
    })));
}

/** Live manifests of a scope, from the pointer index. */
export async function livePointerKeys(tx: SqlExecutor, scope: SessionScope): Promise<string[]> {
  return (await livePointerSnapshot(tx, scope)).keys;
}

/** The source set and directory epoch share one MVCC snapshot. Pointer changes
 * increment the epoch; the final UPDATE compares it after locking the current row. */
async function livePointerSnapshot(tx: SqlExecutor, scope: SessionScope): Promise<{ keys: string[]; epoch: string; canonicalId:string }> {
  const { rows } = await tx.query<{ manifest_key: string; event_epoch: string; canonical_id:string }>(
    `SELECT p.manifest_key, coalesce(p.canonical_session_id,p.session_id) AS canonical_id, coalesce(d.event_projection_epoch, 0)::text AS event_epoch
       FROM sessions.session_index p LEFT JOIN LATERAL (
         SELECT d.event_projection_epoch FROM sessions.session_directory d
          WHERE d.org_id=p.org_id AND d.workspace_id=p.workspace_id AND d.harness=p.harness
            AND d.session_id IN(coalesce(p.canonical_session_id,p.session_id),p.session_id)
          ORDER BY (d.session_id=coalesce(p.canonical_session_id,p.session_id)) DESC LIMIT 1
       ) d ON true
      WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND p.session_id=$4 AND p.deleted_at IS NULL
      ORDER BY p.machine_id`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
  let canonicalId=rows[0]?.canonical_id;
  if(!rows.length){const previous=await tx.query<{canonical_id:string}>(`SELECT coalesce(canonical_session_id,session_id) AS canonical_id FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 ORDER BY revision DESC LIMIT 1`,[scope.orgId,scope.workspaceId,scope.harness,scope.sessionId]);canonicalId=previous.rows[0]?.canonical_id;}
  return { keys: rows.map((r) => r.manifest_key), epoch: rows[0]?.event_epoch ?? "0", canonicalId:canonicalId??scope.sessionId };
}

export async function indexSearchScope(scope: SessionScope, deps: SearchIndexDeps): Promise<SearchIndexResult> {
  const limits = { ...SEARCH_INDEX_DEFAULT_LIMITS, ...deps.limits };
  const now = deps.now ?? (() => new Date());
  return deps.transact(async (tx) => {
    // Non-blocking: a pass already running for this session is busy for as long
    // as its ClickHouse work takes. The caller re-enqueues this event with a
    // delay instead of waiting (and timing out into the DLQ behind a hot session).
    const { rows: got } = await tx.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok", [
      scopeLockKey(scope, "search"),
    ]);
    if (!got[0]?.ok) return { manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, busy: true, reasons: ["scope_busy"] };
    const pointerSnapshot = await livePointerSnapshot(tx, scope);
    const keys = pointerSnapshot.keys;
    const { rows: canonicalLock } = await tx.query<{ok:boolean}>("SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS ok", [["96bulls:directory-events",scope.orgId,scope.workspaceId,scope.harness,pointerSnapshot.canonicalId].join("\u001f")]);
    if (!canonicalLock[0]?.ok) return {manifests:0,rows:0,inserted:0,deleted:0,complete:false,busy:true,reasons:["canonical_scope_busy"]};
    const at = now();
    const version = at.getTime();
    // A stale DB alias is not authority to delete another canonical session.
    if(layoutComponent(pointerSnapshot.canonicalId)!==scope.sessionId){
      await invalidateDirectoryEvents(tx,scope);
      await recordState(deps.ch,scope,0,0,false,at,pointerSnapshot.canonicalId);
      return {manifests:0,rows:0,inserted:0,deleted:0,complete:false,reasons:["canonical_session_mismatch"]};
    }
    const {rows:siblings}=await tx.query<{manifest_key:string}>(`SELECT manifest_key FROM sessions.session_index
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND coalesce(canonical_session_id,session_id)=$4 AND deleted_at IS NULL ORDER BY session_id,machine_id`,[scope.orgId,scope.workspaceId,scope.harness,pointerSnapshot.canonicalId]);
    const siblingKeys=siblings.map(r=>r.manifest_key);
    if(!keys.length&&siblingKeys.length){
      // Erasing one alias must never erase a live canonical sibling.
      await invalidateDirectoryEvents(tx,scope);
      await recordState(deps.ch,scope,0,0,false,at,pointerSnapshot.canonicalId);
      return {manifests:0,rows:0,inserted:0,deleted:0,complete:false,reasons:["canonical_scope_still_live"]};
    }
    if (keys.length === 0) {
      // Erased: no live manifest is left, so every row of the scope goes, in or
      // out of the window. One predicate delete, no listing, so no size can
      // truncate it. It throws on failure and the message retries.
      await deps.ch.command(
        `DELETE FROM ${EVENTS_TABLE}
         WHERE org_id = {org:String} AND workspace_id = {workspace:String}
           AND source = {source:String} AND (session_id = {session:String} OR session_id = {canonical:String})`,
        {
          params: { org: scope.orgId, workspace: scope.workspaceId, source: scope.harness, session: scope.sessionId, canonical: pointerSnapshot.canonicalId },
          settings: { lightweight_deletes_sync: 2 },
          timeoutMs: 120_000,
        },
      );
      await recordState(deps.ch,scope,0,0,true,at,pointerSnapshot.canonicalId);
      await completeErasedDirectoryEvents(tx,scope);
      return { manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: true, reasons: ["erased"] };
    }
    const fromMs = Date.parse(await windowStart(deps.ch, at));
    const s3 = await readCanonicalScope(scope, siblingKeys, { ...deps, decompress: deps.decompress ?? defaultDecompress }, limits, version, pointerSnapshot.canonicalId);
    if(!siblingKeys.length||!s3.complete||s3.directorySessionId!==pointerSnapshot.canonicalId||s3.events===null){
      await invalidateDirectoryEvents(tx,scope);
      await recordState(deps.ch,scope,s3.revision,0,false,at,pointerSnapshot.canonicalId);
      return {manifests:s3.manifests,rows:0,inserted:0,deleted:0,complete:false,...(s3.partial?{partial:true}:{}),reasons:s3.reasons.length?s3.reasons:["canonical_metadata_incomplete"]};
    }
    const inWindow = s3.rows.filter((r) => tsMillis(`${r.ts.replace(" ", "T")}Z`) >= fromMs);
    const chRows = await clickhouseScope(deps.ch, scope, fromMs, limits.maxRows + 1, pointerSnapshot.canonicalId);
    const truncated = chRows.length > limits.maxRows;
    const existing = new Map<string, ScopeIdentity[]>();
    for (const r of chRows) existing.set(keyOf(r), [...(existing.get(keyOf(r)) ?? []), r]);

    const toInsert = inWindow.filter((r) => {
      const tsMs = tsMillis(`${r.ts.replace(" ", "T")}Z`);
      const same = existing.get(keyOf({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id })) ?? [];
      return !same.some((c) => c.sessionId === r.session_id && c.workspaceId === r.workspace_id && c.tsMs === tsMs && c.fingerprint === r.fingerprint);
    });
    for (let i = 0; i < toInsert.length; i += limits.insertBatch)
      await deps.ch.insert(EVENTS_TABLE, toInsert.slice(i, i + limits.insertBatch), { timeoutMs: 60_000 });

    const deletes: PendingDelete[] = [];
    let complete = s3.complete && !truncated;
    if (!complete) await invalidateDirectoryEvents(tx, scope);
    if (complete) {
      const wanted = new Map(inWindow.map((r) => [keyOf({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id }), r]));
      // Only kinds this pass can regenerate from S3 are diffed. ClickHouse also holds rows of
      // kinds the exported delivery stream never carries (e.g. tool_result/user/assistant from
      // other ingest paths); absent from S3 is not "retracted" for those, so they are kept.
      // Erasure (no live pointer, above) still removes every kind of the scope.
      const regenerable = new Set<string>([...S3_ACTIVITY_KINDS, ...inWindow.map((r) => r.kind)]);
      for (const [key, list] of existing) {
        const current = wanted.get(key);
        for (const c of list) {
          if (!regenerable.has(c.kind)) continue;
          if (!current || current.session_id !== c.sessionId || current.workspace_id !== c.workspaceId || tsMillis(`${current.ts.replace(" ", "T")}Z`) !== c.tsMs)
            deletes.push({ orgId: scope.orgId, sessionId: c.sessionId, identity: c } as PendingDelete);
        }
      }
      await deleteRows(deps.ch, deletes);
    }
    // Metadata and CH describe the same validated canonical source union.
    if(complete){
      const directoryKey={orgId:scope.orgId,workspaceId:scope.workspaceId,harness:scope.harness,sessionId:pointerSnapshot.canonicalId};
      const written=await (deps.writeEvents??writeDirectoryEvents)(tx,directoryKey,{sourceSessionId:scope.sessionId,expectedEpoch:pointerSnapshot.epoch,sources:s3.sources,events:s3.events});
      if(written.written)await (deps.writeCatalog??directoryCatalogWriter(deps.desktopCatalog===true))(tx,directoryKey,s3.catalog);
      else {complete=false;s3.reasons.push("directory_event_sources_changed");await invalidateDirectoryEvents(tx,scope);}
    }
    // Only a complete pass makes the session current; any other leaves it owing one.
    await recordState(deps.ch, scope, s3.revision, inWindow.length, complete, at, pointerSnapshot.canonicalId);
    return {
      manifests: s3.manifests,
      rows: inWindow.length,
      inserted: toInsert.length,
      deleted: deletes.length,
      complete,
      reasons: truncated ? [...s3.reasons, "clickhouse_truncated"] : s3.reasons,
    };
  });
}
