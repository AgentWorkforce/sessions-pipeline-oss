/**
 * ClickHouse queries for the session read API over `transcript_events`
 * (vendor/search-index/schema.ts: ReplacingMergeTree(version), ORDER BY
 * (org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id)).
 *
 * Two round trips, because a ReplacingMergeTree can hold several versions of
 * one event until a merge folds them:
 *
 *   1. candidates: the next `limit` distinct event keys in page order, under
 *      sort-key predicates (tenant, ts window, cursor, session, source, kind)
 *      plus value predicates (q, project) evaluated against ANY version. "Some
 *      version matches" is a superset of "the newest version matches", so no
 *      result can be missed here.
 *   2. newest: for exactly those keys, the newest version of each
 *      (ORDER BY key, version DESC LIMIT 1 BY key), and only THEN the value
 *      predicates again, on that newest row. A newer version that was redacted
 *      or rewritten so that it no longer matches is dropped; its stale
 *      predecessor is never served (relayhistory-cloud #242 review H1).
 *
 * Every predicate is a bound parameter or an escaped literal; tenant ids come
 * only from verified auth. Ordering follows the table's sort key so reads stay
 * in order (#242 H2), and a cursor adds a plain `ts` bound next to the tuple
 * comparison so the primary index can prune (#242 H3).
 */
import { arrayLiteral, tuple, type QueryParams } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import type { EventPosition } from "./cursor.ts";

export const EVENTS_TABLE = "transcript_events";
/** Per (org, session) index state written by the pipeline (vendor/search-index/schema.ts). */
export const SESSION_STATE_TABLE = "search_ingest_session_state";
/**
 * At most this many partial sessions are listed per check. A session check
 * asks about one id. A search check lists the org's partial sessions to
 * attribute them to workspaces, so a list longer than this is not decided
 * (queries return cap + 1 rows to tell) and the search falls back whole.
 */
export const PARTIAL_SESSIONS_CAP = 1000;
/** Legacy search semantics: q matches the first 4096 characters of content, or the task title. */
export const CONTENT_PREFIX_CHARS = 4096;
/** The ngrams(3) text index answers queries this long or longer. */
export const INDEXED_QUERY_CHARS = 3;
/** relayhistory's page byte budget (lib/recall.ts): stop a page at about 2 MiB, keeping the first row. */
export const PAGE_BYTE_BUDGET = 2 * 1024 * 1024;
/** Per-row overhead counted against the budget besides content. */
export const ROW_OVERHEAD_BYTES = 1024;

export interface EventQuery {
  orgId: string;
  workspaceId: string;
  sessionId?: string;
  source?: string;
  kind?: string;
  projectId?: string;
  q?: string;
  /** Inclusive lower bound, epoch ms. */
  sinceMs?: number;
  /** Inclusive upper bound, epoch ms. */
  untilMs?: number;
  order: "asc" | "desc";
  cursor?: EventPosition;
  limit: number;
  maxContent?: number;
}

export interface Built {
  query: string;
  params: QueryParams;
}

const KEY_COLUMNS = "toUnixTimestamp64Milli(ts), session_id, source, machine_id, kind, event_id";
const KEY_BY = "(ts, session_id, source, machine_id, kind, event_id)";

function orderBy(order: "asc" | "desc"): string {
  const d = order === "desc" ? "DESC" : "ASC";
  return ["ts", "session_id", "source", "machine_id", "kind", "event_id"].map((c) => `${c} ${d}`).join(", ");
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/u;

export function qualifiedTable(database: string, table: string = EVENTS_TABLE): string {
  if (!IDENTIFIER.test(database)) throw new Error("ClickHouse database must be an identifier");
  return `\`${database}\`.\`${table}\``;
}

/**
 * Sessions whose latest index state is `needs_full = 1`: the pipeline's last S3
 * pass for them was partial (it wrote only the state row, search-index.ts
 * `indexSearchScope`) or a reconcile pass was recent-only or row-capped. Their
 * `transcript_events` rows are not current, so this backend must not present
 * them as a complete transcript or a complete search.
 *
 * The state table is keyed (org_id, session_id) and has no workspace or source,
 * so a session read checks its session id across sources. A search uses
 * buildWorkspacePartialSessionsQuery instead.
 *
 * With `sessionId`: that one session. Without: any session of the org, capped.
 */
export function buildPartialSessionsQuery(database: string, orgId: string, sessionId?: string): Built {
  const params: QueryParams = { org: orgId, cap: PARTIAL_SESSIONS_CAP };
  const where = ["org_id = {org:String}"];
  if (sessionId !== undefined) {
    where.push("session_id = {session:String}");
    params.session = sessionId;
  }
  const query = `SELECT session_id FROM ${qualifiedTable(database, SESSION_STATE_TABLE)}
WHERE ${where.join(" AND ")}
GROUP BY session_id
HAVING argMax(needs_full, synced_at) = 1
LIMIT {cap:UInt32}`;
  return { query, params };
}

/**
 * A workspace search's partial-session check. The state table has no
 * workspace, so the org's partial sessions are listed (at most cap + 1, so an
 * overflow is visible) with `in_workspace = 1` for each one that has any row,
 * any version, in this workspace's `transcript_events`. The ones without such
 * a row may still belong to this workspace (a session whose first pass was
 * partial has no row at all), so the caller checks those against this
 * workspace's live `session_index` pointers (page.ts
 * partialSessionsInWorkspace). In-workspace sessions sort first.
 */
export function buildWorkspacePartialSessionsQuery(database: string, orgId: string, workspaceId: string): Built {
  const partial = `SELECT session_id FROM ${qualifiedTable(database, SESSION_STATE_TABLE)}
WHERE org_id = {org:String}
GROUP BY session_id
HAVING argMax(needs_full, synced_at) = 1`;
  const query = `SELECT session_id, toUInt8(session_id IN (
  SELECT DISTINCT session_id FROM ${qualifiedTable(database)}
  WHERE org_id = {org:String} AND workspace_id = {ws:String} AND session_id IN (${partial})
)) AS in_workspace
FROM (${partial})
ORDER BY in_workspace DESC, session_id
LIMIT {cap:UInt32}`;
  return { query, params: { org: orgId, ws: workspaceId, cap: PARTIAL_SESSIONS_CAP + 1 } };
}

/** Sort-key predicates shared by both round trips. */
function keyPredicates(input: EventQuery, params: QueryParams): string[] {
  const where = ["org_id = {org:String}", "workspace_id = {ws:String}"];
  params.org = input.orgId;
  params.ws = input.workspaceId;
  if (input.sessionId !== undefined) {
    where.push("session_id = {session:String}");
    params.session = input.sessionId;
  }
  if (input.source !== undefined) {
    where.push("source = {source:String}");
    params.source = input.source;
  }
  if (input.kind !== undefined) {
    where.push("kind = {kind:String}");
    params.kind = input.kind;
  }
  if (input.sinceMs !== undefined) {
    where.push("ts >= fromUnixTimestamp64Milli({since:Int64}, 'UTC')");
    params.since = input.sinceMs;
  }
  if (input.untilMs !== undefined) {
    where.push("ts <= fromUnixTimestamp64Milli({until:Int64}, 'UTC')");
    params.until = input.untilMs;
  }
  const c = input.cursor;
  if (c) {
    const op = input.order === "desc" ? "<" : ">";
    const bound = input.order === "desc" ? "<=" : ">=";
    where.push(`ts ${bound} fromUnixTimestamp64Milli({c_ts:Int64}, 'UTC')`);
    where.push(`(${KEY_COLUMNS}) ${op} ({c_ts:Int64}, {c_session:String}, {c_source:String}, {c_machine:String}, {c_kind:String}, {c_event:String})`);
    Object.assign(params, { c_ts: c.tsMs, c_session: c.sessionId, c_source: c.source, c_machine: c.machineId, c_kind: c.kind, c_event: c.eventId });
  }
  return where;
}

function contentMatch(): string {
  return `positionCaseInsensitiveUTF8(substringUTF8(content, 1, ${CONTENT_PREFIX_CHARS}), {q:String}) > 0`;
}
function titleMatch(): string {
  return "positionCaseInsensitiveUTF8(task_title, {q:String}) > 0";
}

/**
 * Round trip 1: candidate keys. A workspace search (no session) runs one branch
 * per searched column so each column's text index is the primary filter of its
 * own scan (a skip index cannot prune under an OR spanning another column);
 * each branch dedupes keys BEFORE its LIMIT, so repeated versions of one event
 * can never fill a branch and fake an end of results.
 */
export function buildCandidateQuery(database: string, input: EventQuery): Built {
  const params: QueryParams = { n: input.limit };
  const where = keyPredicates(input, params);
  if (input.projectId !== undefined) {
    where.push("project_id = {project:String}");
    params.project = input.projectId;
  }
  const table = qualifiedTable(database);
  const ord = orderBy(input.order);
  const select = `SELECT ts, session_id, source, machine_id, kind, event_id FROM ${table}`;
  const limited = `ORDER BY ${ord} LIMIT 1 BY ${KEY_BY} LIMIT {n:UInt32}`;
  let inner: string;
  if (input.q === undefined) {
    inner = `${select} WHERE ${where.join(" AND ")} ${limited}`;
  } else {
    params.q = input.q;
    const indexed = input.sessionId === undefined && [...input.q].length >= INDEXED_QUERY_CHARS;
    if (input.sessionId !== undefined) {
      // In-session: the session bloom filter is the primary filter; one scan.
      inner = `${select} WHERE ${[...where, `(${contentMatch()} OR ${titleMatch()})`].join(" AND ")} ${limited}`;
    } else {
      // Every substring of q contains all of q's 3-grams, so the token check has no
      // false negatives; the exact check keeps candidates precise.
      const contentPred = indexed ? `hasAllTokens(content, lowerUTF8({q:String})) AND ${contentMatch()}` : contentMatch();
      const titlePred = indexed ? `hasAllTokens(task_title, lowerUTF8({q:String})) AND ${titleMatch()}` : titleMatch();
      const branch = (p: string) => `${select} WHERE ${[...where, p].join(" AND ")} ${limited}`;
      inner = `SELECT * FROM (${branch(contentPred)} UNION ALL ${branch(titlePred)}) ${limited}`;
    }
  }
  const query = `SELECT toUnixTimestamp64Milli(ts) AS ts_ms, session_id, source, machine_id, kind, event_id FROM (${inner}) ORDER BY ${ord}`;
  return { query, params };
}

export interface CandidateRow {
  ts_ms: number | string;
  session_id: string;
  source: string;
  machine_id: string;
  kind: string;
  event_id: string;
}

export function candidatePosition(r: CandidateRow): EventPosition {
  return { tsMs: Number(r.ts_ms), sessionId: r.session_id, source: r.source, machineId: r.machine_id, kind: r.kind, eventId: r.event_id };
}

/**
 * Round trip 2: the newest version of each candidate, value predicates applied
 * to that newest row only, then the page byte budget as a running sum in page
 * order. Content past the budget is not returned (the caller cuts the page
 * there and issues a cursor); with maxContent only maxContent+1 code points are
 * read, which is exactly enough to decide and apply relayhistory's truncation.
 */
export function buildNewestQuery(database: string, input: EventQuery, candidates: EventPosition[]): Built {
  if (candidates.length === 0) throw new Error("buildNewestQuery needs candidates");
  const params: QueryParams = { org: input.orgId, ws: input.workspaceId, lo: Math.min(...candidates.map((c) => c.tsMs)), hi: Math.max(...candidates.map((c) => c.tsMs)), budget: PAGE_BYTE_BUDGET };
  const keys = arrayLiteral(candidates.map((c) => tuple(c.tsMs, c.sessionId, c.source, c.machineId, c.kind, c.eventId)));
  const ord = orderBy(input.order);
  const value: string[] = [];
  if (input.q !== undefined) {
    params.q = input.q;
    value.push(`(${contentMatch()} OR ${titleMatch()})`);
  }
  if (input.projectId !== undefined) {
    params.project = input.projectId;
    value.push("project_id = {project:String}");
  }
  let contentExpr = "content";
  let sizeExpr = "content_bytes";
  if (input.maxContent !== undefined) {
    params.mc = input.maxContent + 1;
    contentExpr = "substringUTF8(content, 1, {mc:UInt32})";
    // A code point is at most 4 bytes.
    sizeExpr = "least(content_bytes, 4 * {mc:UInt32})";
  }
  const cols = "org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id, type, user_id, actor_name, actor_role, subagent_id, project_id, task_title, task_ref, tags, files_touched, tool_name, tool_status";
  const newest = `SELECT ${cols}, content, content_bytes, version FROM ${qualifiedTable(database)}
WHERE org_id = {org:String} AND workspace_id = {ws:String}
  AND ts >= fromUnixTimestamp64Milli({lo:Int64}, 'UTC') AND ts <= fromUnixTimestamp64Milli({hi:Int64}, 'UTC')
  AND (${KEY_COLUMNS}) IN ${keys}
ORDER BY ${ord}, version DESC LIMIT 1 BY ${KEY_BY}`;
  const filtered = `SELECT ${cols}, ${contentExpr} AS content_eff, ${sizeExpr} + ${ROW_OVERHEAD_BYTES} AS eff_bytes FROM (${newest})${value.length ? ` WHERE ${value.join(" AND ")}` : ""}`;
  const windowed = `SELECT *, row_number() OVER w AS rn, sum(eff_bytes) OVER w - eff_bytes AS bytes_before FROM (${filtered})
WINDOW w AS (ORDER BY ${ord} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`;
  const query = `SELECT org_id, workspace_id, toUnixTimestamp64Milli(ts) AS ts_ms, session_id, source, machine_id, kind, event_id, type, user_id, actor_name, actor_role, subagent_id, project_id, task_title, task_ref, tags, files_touched, tool_name, tool_status,
  if(bytes_before <= {budget:UInt64} OR rn = 1, content_eff, '') AS content, toUInt8(bytes_before <= {budget:UInt64} OR rn = 1) AS in_budget
FROM (${windowed}) ORDER BY ${ord}`;
  return { query, params };
}

export interface EventRow {
  org_id: string;
  workspace_id: string;
  ts_ms: number | string;
  session_id: string;
  source: string;
  machine_id: string;
  kind: string;
  event_id: string;
  type: string;
  user_id: string;
  actor_name: string;
  actor_role: string;
  subagent_id: string;
  project_id: string;
  task_title: string;
  task_ref: string;
  tags: string[];
  files_touched: string[];
  tool_name: string;
  tool_status: string;
  content: string;
  in_budget: number;
}

/**
 * Settings for every read: bounded time and result size. Read-only is the
 * ClickHouse user's own grant (SELECT on transcript_events), not a setting a
 * request could change.
 */
export const READ_SETTINGS = {
  max_execution_time: 2,
  max_result_bytes: 16 * 1024 * 1024,
  result_overflow_mode: "throw",
} as const;
