/** Content-free, complete canonical-event metadata for the logical directory.
 * Call only after all live manifests were verified/folded. Never aggregate the
 * ClickHouse window or pointer counters: both can be a strict subset of truth.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { canonicalTimestamp } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";
export const EVENT_PROJECTION_VERSION = 1;
const MAX_COUNT = 9223372036854775807n;
type Event = Record<string, unknown>;
export interface EventLocator {
  session_id: string;
  event_id: string;
  machine_id: string;
  kind: string;
  source: string;
  ts: string;
  fingerprint: string;
}
export interface DirectoryEvents {
  eventCount: string;
  eventTimes: string[];
  userIds: string[];
  machineIds: string[];
  models: string[];
  projectId: string | null;
  openingEvent: EventLocator | null;
  projectEvent: EventLocator | null;
  firstTs: string | null;
  lastTs: string | null;
  kinds: string[];
  taskRefs: unknown[];
  taskTitle: string | null;
  inputTokens: string;
  outputTokens: string;
  reasoningTokens: string;
  cacheReadTokens: string;
  cacheCreateTokens: string;
  cacheCreate5mTokens: string | null;
  cacheCreate1hTokens: string | null;
  costUsdMicros: string | null;
  workStatus: "active" | "idle" | "finished" | null;
  workStatusAt: string | null;
}
export interface DirectoryEventInput {
  sourceSessionId: string;
  expectedEpoch: string;
  sources: Array<{ manifestKey: string; revision: string }>;
  events: DirectoryEvents;
}
function invalid(): never {
  throw new Error("directory_event_metadata_invalid");
}
function count(value: unknown): bigint {
  if (value == null) return 0n;
  if (typeof value === "number" && (!Number.isSafeInteger(value) || value < 0))
    invalid();
  if (
    !["number", "string", "bigint"].includes(typeof value) ||
    !/^\d+$/u.test(String(value))
  )
    invalid();
  const n = BigInt(String(value));
  if (n > MAX_COUNT) invalid();
  return n;
}
function sum(values: bigint[]): string {
  const n = values.reduce((a, b) => a + b, 0n);
  if (n > MAX_COUNT) invalid();
  return String(n);
}
function canonical(value: unknown, depth = 0): unknown {
  if (depth > 12) invalid();
  if (Array.isArray(value)) return value.map((v) => canonical(v, depth + 1));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonical(v, depth + 1)]),
    );
  if (typeof value === "number" && !Number.isFinite(value)) invalid();
  return value;
}
const workKinds = new Set([
  "prompt",
  "user",
  "assistant",
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "task",
]);
function status(row: Event): DirectoryEvents["workStatus"] {
  if (!workKinds.has(String(row.kind))) return null;
  const s = String(row.task_status ?? "").toLowerCase();
  if (["completed", "complete", "finished", "done", "succeeded"].includes(s))
    return "finished";
  if (
    [
      "idle",
      "paused",
      "waiting",
      "blocked",
      "cancelled",
      "canceled",
      "failed",
    ].includes(s)
  )
    return "idle";
  if (["active", "working", "running", "in_progress"].includes(s))
    return "active";
  return row.kind === "task" ? null : "active";
}
/** Takes the complete surviving convergence rows, never catalog/content text. */
export function directoryEventsFromProjection(
  input: readonly Event[],
): DirectoryEvents {
  const unique = new Map<string, Event>();
  for (const row of input) {
    const key = JSON.stringify([
      row.machine_id,
      row.source,
      row.session_id,
      row.kind,
      row.event_id,
      row.ts,
    ]);
    const prior = unique.get(key);
    if (
      prior &&
      JSON.stringify(canonical(prior)) !== JSON.stringify(canonical(row))
    )
      invalid();
    unique.set(key, row);
  }
  const rows = [...unique.values()]
    .map<
      Event & { ts: string }
    >((row) => ({ ...row, ts: canonicalTimestamp(String(row.ts)) }))
    .sort(
      (a, b) =>
        a.ts.length - b.ts.length ||
        (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0) ||
        Buffer.compare(
          Buffer.from(String(a.event_id)),
          Buffer.from(String(b.event_id)),
        ),
    );
  const kinds = [...new Set(rows.map((r) => String(r.kind)))].sort();
  if (
    kinds.length > 64 ||
    kinds.some((k) => !k || k.length > 128 || /[\u0000-\u001f\u007f]/u.test(k))
  )
    invalid();
  const refs = new Map<string, unknown>();
  for (const row of rows) {
    const ref = row.task_ref;
    if (ref == null || JSON.stringify(ref) === "{}") continue;
    if (typeof ref !== "object") invalid();
    const clean = canonical(ref);
    const key = JSON.stringify(clean);
    if (key.length > 16384) invalid();
    refs.set(key, clean);
  }
  const taskRefs = [...refs.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, v]) => v);
  if (taskRefs.length > 1000 || JSON.stringify(taskRefs).length > 131072)
    invalid();
  const textSet = (field: string) => {
    const values = [
      ...new Set(
        rows.flatMap((row) =>
          typeof row[field] === "string" && row[field] !== ""
            ? [row[field] as string]
            : [],
        ),
      ),
    ].sort();
    if (
      values.length > 1000 ||
      values.some(
        (value) => value.length > 1024 || /[\u0000-\u001f\u007f]/u.test(value),
      )
    )
      invalid();
    return values;
  };
  const compareText = (left: string, right: string) =>
    Buffer.compare(Buffer.from(left), Buffer.from(right));
  const projectRows = rows
    .filter((row) => typeof row.project_id === "string")
    .sort((a, b) => compareText(String(a.project_id), String(b.project_id)));
  const project = projectRows.at(-1);
  const projectId = project ? String(project.project_id) : null;
  const isPath =
    projectId !== null &&
    (/^(?:[\\/]|~|\.{1,2}[\\/]|file:)/iu.test(projectId) ||
      /^[A-Za-z]:[\\/]/u.test(projectId));
  function locator(row: Event & { ts: string }): EventLocator {
    if (
      typeof row._index_fingerprint !== "string" ||
      !/^s3[0-9a-f]{14}$/u.test(row._index_fingerprint)
    )
      invalid();
    for (const field of [
      "session_id",
      "event_id",
      "machine_id",
      "kind",
      "source",
    ])
      if (typeof row[field] !== "string" || !row[field]) invalid();
    return {
      session_id: String(row.session_id),
      event_id: String(row.event_id),
      machine_id: String(row.machine_id),
      kind: String(row.kind),
      source: String(row.source),
      ts: row.ts,
      fingerprint: row._index_fingerprint,
    };
  }
  if (
    projectId !== null &&
    !isPath &&
    (projectId.length > 1024 || /[\u0000-\u001f\u007f]/u.test(projectId))
  )
    invalid();
  const projectEvent = isPath && project ? locator(project) : null;
  const opening = rows.find(
    (row) =>
      (["history", "prompt", "user"].includes(String(row.kind)) ||
        (row.kind === "session_event" && row.actor_role === "user")) &&
      typeof row.content === "string" &&
      row.content.replace(/^ +| +$/gu, "").length > 0,
  );
  const openingEvent = opening ? locator(opening) : null;
  const totals = (key: string) => sum(rows.map((row) => count(row[key])));
  const completeSplit = rows.every(
    (row) =>
      count(row.cache_create_tokens) === 0n ||
      ((row.cache_create_5m_tokens != null ||
        row.cache_create_1h_tokens != null) &&
        count(row.cache_create_5m_tokens) +
          count(row.cache_create_1h_tokens) ===
          count(row.cache_create_tokens)),
  );
  const work = [...rows].reverse().find((row) => status(row) !== null);
  const taskTitle =
    [...rows]
      .reverse()
      .map((r) => r.task_title)
      .find((t) => t != null) ?? null;
  if (
    taskTitle !== null &&
    (typeof taskTitle !== "string" || [...taskTitle].length > 200)
  )
    invalid();
  return {
    eventCount: String(rows.length),
    eventTimes: [...new Set(rows.map((row) => row.ts))],
    openingEvent,
    userIds: textSet("user_id"),
    machineIds: textSet("machine_id"),
    models: textSet("model"),
    projectId: isPath ? null : projectId,
    projectEvent,
    firstTs: rows[0]?.ts ?? null,
    lastTs: rows.at(-1)?.ts ?? null,
    kinds,
    taskRefs,
    taskTitle: taskTitle as string | null,
    inputTokens: totals("input_tokens"),
    outputTokens: totals("output_tokens"),
    reasoningTokens: totals("reasoning_tokens"),
    cacheReadTokens: totals("cache_read_tokens"),
    cacheCreateTokens: totals("cache_create_tokens"),
    cacheCreate5mTokens: completeSplit
      ? totals("cache_create_5m_tokens")
      : null,
    cacheCreate1hTokens: completeSplit
      ? totals("cache_create_1h_tokens")
      : null,
    costUsdMicros: rows.some((r) => r.cost_usd_micros != null)
      ? totals("cost_usd_micros")
      : null,
    workStatus: work ? status(work) : null,
    workStatusAt: work?.ts ?? null,
  };
}
/** Lock/fence against tombstones and compare the entire live source set. A
 * concurrent pointer change invalidates readiness in its own transaction. */
export async function writeDirectoryEvents(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  input: DirectoryEventInput,
): Promise<{ written: boolean }> {
  const expected = Object.fromEntries(
    input.sources.map((s) => [s.manifestKey, String(count(s.revision))]),
  );
  if (
    !input.sources.length ||
    Object.keys(expected).length !== input.sources.length
  )
    invalid();
  const e = input.events;
  const { rows } = await db.query<{ written: boolean }>(
    `WITH captured AS MATERIALIZED (
    SELECT m.source_session_id,m.epoch FROM sessions.session_directory_event_pending m
    WHERE m.org_id=$1 AND m.workspace_id=$2 AND m.harness=$3 AND m.source_session_id IN (
      SELECT p.session_id FROM sessions.session_index p WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND coalesce(p.canonical_session_id,p.session_id)=$4 AND p.deleted_at IS NULL
    )
  ), marker AS MATERIALIZED (
    SELECT m.* FROM sessions.session_directory_event_pending m
    WHERE m.org_id=$1 AND m.workspace_id=$2 AND m.harness=$3
      AND (m.source_session_id,m.epoch) IN(SELECT source_session_id,epoch FROM captured)
      ORDER BY source_session_id FOR UPDATE
  ), live AS MATERIALIZED (
    SELECT d.org_id,d.workspace_id,d.harness,d.session_id FROM sessions.session_directory d,marker m
    WHERE d.org_id=$1 AND d.workspace_id=$2 AND d.harness=$3 AND d.session_id IN($4,$5) AND d.deleted_at IS NULL AND (SELECT count(*) FROM marker)=(SELECT count(*) FROM captured)
    ORDER BY (d.session_id=$4) DESC LIMIT 1 FOR UPDATE OF d
  ), sources AS (
    SELECT jsonb_object_agg(p.manifest_key,p.revision::text) AS revisions,count(*) AS n,
      count(DISTINCT coalesce(p.canonical_session_id,p.session_id)) AS identities,
      min(coalesce(p.canonical_session_id,p.session_id)) AS canonical_id
    FROM sessions.session_index p WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3
      AND coalesce(p.canonical_session_id,p.session_id)=$4 AND p.deleted_at IS NULL
  ), updated AS (
    UPDATE sessions.session_directory d SET
      event_count=$7::bigint,event_first_ts=$8::timestamptz,event_last_ts=$9::timestamptz,
      kinds=$10::text[],task_refs=$11::jsonb,task_title=$12,
      event_user_ids=$26::text[],event_machine_ids=$27::text[],event_models=$28::text[],event_project_id=$29,opening_event=$30::jsonb,event_project_event=$31::jsonb,project_requires_hydration=$32::boolean,
      input_tokens=$13::bigint,output_tokens=$14::bigint,reasoning_tokens=$15::bigint,
      cache_read_tokens=$16::bigint,cache_create_tokens=$17::bigint,cache_create_5m_tokens=$18::bigint,cache_create_1h_tokens=$19::bigint,cost_usd_micros=$20::bigint,
      work_status=$21,work_status_at=$22::timestamptz,event_projection_version=1,event_projection_ready=true,event_projected_at=now(),event_projection_sources=$6::jsonb,
      search_text=sessions.session_directory_search_text(d.title,$12,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,d.short_summary)
    FROM live l,sources s WHERE d.org_id=l.org_id AND d.workspace_id=l.workspace_id AND d.harness=l.harness AND d.session_id=l.session_id
      AND s.revisions=$6::jsonb AND s.n=$23::integer AND s.identities=1 AND s.canonical_id=$4
      AND d.event_projection_epoch=$24::bigint
    RETURNING d.org_id,d.workspace_id,d.harness,d.session_id
  ), clean_marker AS (
    UPDATE sessions.session_directory_event_pending m SET dirty=false FROM updated u
    WHERE m.org_id=$1 AND m.workspace_id=$2 AND m.harness=$3 AND (m.source_session_id,m.epoch) IN(SELECT source_session_id,epoch FROM captured)
  ), drop_times AS (
    DELETE FROM sessions.session_directory_event_times t USING updated u
    WHERE t.org_id=u.org_id AND t.workspace_id=u.workspace_id AND t.harness=u.harness AND t.session_id=u.session_id
      AND NOT(t.event_ts=ANY($25::timestamptz[]))
  ), put_times AS (
    INSERT INTO sessions.session_directory_event_times(org_id,workspace_id,harness,session_id,event_ts)
    SELECT u.org_id,u.workspace_id,u.harness,u.session_id,x.ts FROM updated u,unnest($25::timestamptz[]) x(ts)
    ON CONFLICT DO NOTHING
  ) SELECT EXISTS(SELECT 1 FROM updated) AS written`,
    [
      key.orgId,
      key.workspaceId,
      key.harness,
      key.sessionId,
      input.sourceSessionId,
      JSON.stringify(expected),
      e.eventCount,
      e.firstTs,
      e.lastTs,
      e.kinds,
      JSON.stringify(e.taskRefs),
      e.taskTitle,
      e.inputTokens,
      e.outputTokens,
      e.reasoningTokens,
      e.cacheReadTokens,
      e.cacheCreateTokens,
      e.cacheCreate5mTokens,
      e.cacheCreate1hTokens,
      e.costUsdMicros,
      e.workStatus,
      e.workStatusAt,
      input.sources.length,
      String(count(input.expectedEpoch)),
      e.eventTimes,
      e.userIds,
      e.machineIds,
      e.models,
      e.projectId,
      e.openingEvent ? JSON.stringify(e.openingEvent) : null,
      e.projectEvent ? JSON.stringify(e.projectEvent) : null,
      e.projectEvent !== null,
    ],
  );
  return { written: rows[0]?.written === true };
}

/** Incomplete passes invalidate readiness without erasing the last good metadata. */
export async function invalidateDirectoryEvents(
  db: SqlExecutor,
  scope: SessionDirectoryKey,
): Promise<void> {
  await db.query(
    `UPDATE sessions.session_directory d SET event_projection_ready=false
    WHERE d.org_id=$1 AND d.workspace_id=$2 AND d.harness=$3
      AND (d.session_id=$4 OR d.session_id IN (
        SELECT coalesce(p.canonical_session_id,p.session_id) FROM sessions.session_index p
        WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND p.session_id=$4))`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
}

/** Empty scopes clear only the exact marker version locked by this statement.
 * A concurrent pointer mutation waits and dirties it again before committing. */
export async function completeErasedDirectoryEvents(
  db: SqlExecutor,
  scope: SessionDirectoryKey,
): Promise<void> {
  await db.query(
    `WITH captured AS MATERIALIZED (
   SELECT epoch FROM sessions.session_directory_event_pending WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND source_session_id=$4
 ), locked AS MATERIALIZED (
   SELECT m.* FROM sessions.session_directory_event_pending m WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND source_session_id=$4 AND epoch=(SELECT epoch FROM captured) FOR UPDATE
 ) UPDATE sessions.session_directory_event_pending m SET dirty=false FROM locked l
   WHERE m.org_id=l.org_id AND m.workspace_id=l.workspace_id AND m.harness=l.harness AND m.source_session_id=l.source_session_id
    AND NOT EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND p.session_id=$4 AND p.deleted_at IS NULL)`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
}
