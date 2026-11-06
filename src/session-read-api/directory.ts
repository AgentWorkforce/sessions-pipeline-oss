/**
 * The session LIST and activity from the Supabase logical-session directory, for
 * `GET /v1/sessions` (no `q`) and `GET /v1/sessions/activity` (no `q`).
 *
 * Calls sessions.list_session_directory_for / session_directory_activity_for
 * (supabase/migrations/20261105029000_session_directory_read_api.sql) as the
 * `bulls96_read_api` role, inside the read API's own READ ONLY transaction with the
 * tenant bound from the verified RelayAuth assertion (liveness.ts TENANT_SETTINGS_SQL).
 * The functions pin the tenant to that binding; every returned row is rechecked here
 * too. No Supabase JWT secret is involved, and there is no Neon fallback anywhere:
 * when the migration is not applied (the functions do not exist) the route answers
 * 501 `directory_not_migrated` and Cloud keeps its current path.
 *
 * The row -> SessionSummary mapping is relayhistory-cloud's reviewed one
 * (packages/relayhistory/src/lib/session-directory-list.ts sessionFromDirectoryRow,
 * the LIST_BACKEND=supabase path), minus `shortSummary`, so the response parses under
 * the strict @relayhistory/cloud-client 0.2.0 `sessionsResponse` Cloud validates with.
 * Activity uses the directory's SPAN semantics (a session counts on every UTC day from
 * its first to its last activity), as relayhistory's LIST_BACKEND=supabase does.
 */
import type { TenantRunner } from "./liveness.ts";

/** One list_session_directory_for row (its RETURNS TABLE). pg returns timestamptz as Date. */
export interface DirectoryListRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  created_at: Date | string;
  last_activity_at: Date | string;
  title: string | null;
  task_title: string | null;
  agent_names: string[] | null;
  project_id: string | null;
  project_key: string | null;
  project_key_method: string | null;
  git_repo: string | null;
  git_branch: string | null;
  initial_commit: string | null;
  originator: string | null;
  agent_version: string | null;
  models: string[] | null;
  state: string | null;
  event_count: number | string;
  input_tokens: number | string;
  output_tokens: number | string;
  cost_usd_micros: number | string | null;
  machine_ids: string[] | null;
  user_ids: string[] | null;
  parent_session_ids: string[] | null;
  child_session_count: number | string;
  short_summary: string | null;
  short_summary_generated_at: Date | string | null;
  catalog_projected_at: Date | string | null;
  projected_at: Date | string | null;
}

export interface DirectoryActivityRow {
  day: Date | string;
  harness: string;
  sessions: number | string;
}

export interface DirectoryListArgs {
  sources?: string[];
  notSources?: string[];
  projectContains?: string;
  userIds?: string[];
  agent?: string;
  lastTsAfter?: Date;
  lastTsBefore?: Date;
  firstTsAfter?: Date;
  firstTsBefore?: Date;
  cursor?: DirectoryCursor;
  /** Page size, 1..200; the function returns up to limit + 1 rows. */
  limit: number;
}

export interface DirectoryCursor {
  /** ISO timestamp with up to six fractional digits (microseconds preserved). */
  lastActivityAt: string;
  sessionId: string;
  harness: string;
}

/** Null: the functions are not installed (the migration has not been applied). */
export interface Directory {
  list(orgId: string, workspaceId: string, args: DirectoryListArgs): Promise<DirectoryListRow[] | null>;
  activity(orgId: string, workspaceId: string, since: Date, until: Date): Promise<DirectoryActivityRow[] | null>;
}

/** Postgres refused for a reason the caller can name (0A000, 22023), never quoting values. */
export class DirectoryError extends Error {
  constructor(readonly reason: "agent_filter_not_ready" | "invalid_argument" | "tenant_mismatch" | "parse" | "refused") {
    super(`session directory: ${reason}`);
    this.name = "DirectoryError";
  }
}

export const LIST_FOR_SIGNATURE =
  "sessions.list_session_directory_for(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)";
export const ACTIVITY_FOR_SIGNATURE = "sessions.session_directory_activity_for(text, timestamptz, timestamptz)";

/** The directory list page bound (the function's p_limit 1..200). */
export const DIRECTORY_LIST_MAX_LIMIT = 200;

// Timestamps travel as text so microseconds survive the cursor round trip.
export const DIRECTORY_LIST_SQL = `SELECT org_id, workspace_id, harness, session_id,
       created_at, last_activity_at,
       to_char(last_activity_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_ts,
       title, task_title, agent_names, project_id, project_key, project_key_method, git_repo, git_branch,
       initial_commit, originator, agent_version, models, state, event_count, input_tokens, output_tokens,
       cost_usd_micros, machine_ids, user_ids, parent_session_ids, child_session_count, short_summary,
       short_summary_generated_at, catalog_projected_at, projected_at
  FROM sessions.list_session_directory_for(
    p_workspace_id => $1, p_sources => $2, p_not_sources => $3, p_project_contains => $4,
    p_user_ids => $5, p_agent => $6, p_last_ts_after => $7, p_last_ts_before => $8,
    p_first_ts_after => $9, p_first_ts_before => $10,
    p_cursor_last_activity_at => $11, p_cursor_session_id => $12, p_cursor_harness => $13,
    p_limit => $14)`;

export const DIRECTORY_ACTIVITY_SQL = `SELECT to_char(day, 'YYYY-MM-DD') AS day, harness, sessions
  FROM sessions.session_directory_activity_for($1, $2, $3)`;

export const DIRECTORY_INSTALLED_SQL = `SELECT to_regprocedure($1) IS NOT NULL AS installed`;

function mapPgError(error: unknown): never {
  const code = (error as { code?: string })?.code;
  const message = (error as { message?: string })?.message ?? "";
  if (code === "0A000" && message.includes("agent_filter_not_ready")) throw new DirectoryError("agent_filter_not_ready");
  if (code === "22023") throw new DirectoryError("invalid_argument");
  if (code === "42501") throw new DirectoryError("refused");
  throw error;
}

/** The directory over the read API's tenant runner: one READ ONLY transaction per call. */
export function sqlDirectory(run: TenantRunner): Directory {
  return {
    async list(orgId, workspaceId, a) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [LIST_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const iso = (d: Date | undefined) => (d ? d.toISOString() : null);
        const r = await q
          .query<DirectoryListRow>(DIRECTORY_LIST_SQL, [
            workspaceId,
            a.sources?.length ? a.sources : null,
            a.notSources?.length ? a.notSources : null,
            a.projectContains ?? null,
            a.userIds?.length ? a.userIds : null,
            a.agent ?? null,
            iso(a.lastTsAfter),
            iso(a.lastTsBefore),
            iso(a.firstTsAfter),
            iso(a.firstTsBefore),
            a.cursor?.lastActivityAt ?? null,
            a.cursor?.sessionId ?? null,
            a.cursor?.harness ?? null,
            a.limit,
          ])
          .catch(mapPgError);
        return r.rows;
      });
    },
    async activity(orgId, workspaceId, since, until) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [ACTIVITY_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const r = await q
          .query<DirectoryActivityRow>(DIRECTORY_ACTIVITY_SQL, [workspaceId, since.toISOString(), until.toISOString()])
          .catch(mapPgError);
        return r.rows;
      });
    },
  };
}

/**
 * List cursors, namespaced like the event cursors (cursor.ts `b96e1:`): a cursor from
 * this backend is never valid at relayhistory and a relayhistory `sessions-v2:` cursor
 * is refused here (`foreign_cursor`), so Cloud keeps a pagination on one backend.
 */
export const LIST_CURSOR_PREFIX = "b96s1:";
const CURSOR_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/u;
const HARNESS_NAME = /^[a-z0-9][a-z0-9._-]{0,63}$/u;

export function encodeListCursor(c: DirectoryCursor): string {
  return LIST_CURSOR_PREFIX + btoa(JSON.stringify([c.lastActivityAt, c.sessionId, c.harness])).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "");
}

/** undefined: no cursor; "foreign": not ours; "invalid": ours but malformed. */
export function decodeListCursor(raw: string | undefined): DirectoryCursor | undefined | "foreign" | "invalid" {
  if (raw === undefined || raw === "") return undefined;
  if (!raw.startsWith(LIST_CURSOR_PREFIX)) return "foreign";
  try {
    const b64 = raw.slice(LIST_CURSOR_PREFIX.length).replace(/-/gu, "+").replace(/_/gu, "/");
    const v = JSON.parse(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)));
    if (
      Array.isArray(v) && v.length === 3 &&
      typeof v[0] === "string" && CURSOR_TS.test(v[0]) && Number.isFinite(Date.parse(v[0])) &&
      typeof v[1] === "string" && v[1].length >= 1 && v[1].length <= 255 &&
      typeof v[2] === "string" && HARNESS_NAME.test(v[2])
    )
      return { lastActivityAt: v[0], sessionId: v[1], harness: v[2] };
  } catch {
    // fall through
  }
  return "invalid";
}

/** relayhistory lib/recall.ts MAX_SESSION_SUMMARY_CHARS and condenseTurn. */
export const MAX_SESSION_SUMMARY_CHARS = 240;
export function condenseTurn(content: string): string | null {
  const flat = content.replace(/\s+/gu, " ").trim();
  if (!flat) return null;
  if (flat.length <= MAX_SESSION_SUMMARY_CHARS) return flat;
  const cut = flat.slice(0, MAX_SESSION_SUMMARY_CHARS);
  const lastSpace = cut.lastIndexOf(" ");
  const body = lastSpace > MAX_SESSION_SUMMARY_CHARS * 0.6 ? cut.slice(0, lastSpace) : cut;
  return `${body.trimEnd()}…`;
}
/** relayhistory summaryFor(task_title, undefined): the directory has no opening turn. */
function summaryFor(taskTitle: string | null): { summary: string | null; summarySource: "task_title" | null } {
  if (typeof taskTitle === "string" && taskTitle.trim() !== "") return { summary: condenseTurn(taskTitle), summarySource: "task_title" };
  return { summary: null, summarySource: null };
}

const iso = (value: Date | string) => {
  const d = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(d.getTime())) throw new DirectoryError("parse");
  return d.toISOString();
};
const num = (value: unknown) => {
  if (typeof value !== "number" && !(typeof value === "string" && /^\d+$/u.test(value))) throw new DirectoryError("parse");
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0) throw new DirectoryError("parse");
  return n;
};
const strings = (value: unknown) =>
  Array.isArray(value) ? value.filter((item): item is string => typeof item === "string" && item !== "") : [];

export type WorkState = { status: "active" | "idle" | "finished"; updatedAt: string } | null;
function workState(row: DirectoryListRow): WorkState {
  const status = row.state === "active" ? "active" : row.state === "done" ? "finished" : row.state === "abandoned" ? "idle" : null;
  return status ? { status, updatedAt: iso(row.last_activity_at) } : null;
}

/** One directory row in the cloud-client `sessionSummary` shape (relayhistory's mapping). */
export function sessionFromDirectoryRow(row: DirectoryListRow) {
  const models = strings(row.models);
  const catalog = row.catalog_projected_at
    ? {
        workspaceId: row.workspace_id,
        title: row.title,
        gitBranch: row.git_branch,
        repoUrl: row.git_repo,
        initialCommit: row.initial_commit,
        projectKey: row.project_key,
        projectKeyMethod: row.project_key_method,
        models,
        originator: row.originator,
        agentVersion: row.agent_version,
        firstActivityAt: iso(row.created_at),
        lastActivityAt: iso(row.last_activity_at),
        parentSessionIds: strings(row.parent_session_ids),
        childSessionCount: num(row.child_session_count),
      }
    : undefined;
  // sessionSummary bound (cloud-client 0.2.0 and later): title at most 90 characters.
  if (row.title !== null && row.title !== undefined && row.title.length > 90) throw new DirectoryError("parse");
  return {
    title: row.title ?? null,
    // shortSummary is deliberately not sent: Cloud validates with the strict
    // @relayhistory/cloud-client 0.2.0 sessionSummary, which has no such key. Add it
    // when Cloud moves to a contract version that has it.
    sessionId: row.session_id,
    userIds: strings(row.user_ids).sort(),
    source: row.harness,
    projectId: row.project_id ?? null,
    machineIds: strings(row.machine_ids),
    firstTs: iso(row.created_at),
    lastTs: iso(row.last_activity_at),
    workState: workState(row),
    eventCount: num(row.event_count),
    kinds: [] as string[],
    models,
    taskTitle: row.task_title ?? null,
    ...summaryFor(row.task_title ?? null),
    taskRefs: [] as unknown[],
    totalCostUsdMicros: row.cost_usd_micros == null ? null : num(row.cost_usd_micros),
    totalInputTokens: num(row.input_tokens),
    totalOutputTokens: num(row.output_tokens),
    totalReasoningTokens: 0,
    totalCacheReadTokens: 0,
    totalCacheCreateTokens: 0,
    totalCacheCreate5mTokens: null,
    totalCacheCreate1hTokens: null,
    ...(catalog ? { catalog } : {}),
  };
}

export type DirectorySession = ReturnType<typeof sessionFromDirectoryRow>;

/** One page: rows rechecked against the tenant, limit + 1 trimmed into a cursor. */
export function directoryPage(rows: Array<DirectoryListRow & { cursor_ts?: string }>, tenant: { orgId: string; workspaceId: string }, limit: number) {
  if (rows.length > limit + 1) throw new DirectoryError("parse");
  for (const row of rows) if (row.org_id !== tenant.orgId || row.workspace_id !== tenant.workspaceId) throw new DirectoryError("tenant_mismatch");
  const hasMore = rows.length > limit;
  const visible = hasMore ? rows.slice(0, limit) : rows;
  const last = visible[visible.length - 1];
  let nextCursor: string | null = null;
  if (hasMore && last) {
    const ts = last.cursor_ts;
    if (typeof ts !== "string" || !CURSOR_TS.test(ts)) throw new DirectoryError("parse");
    nextCursor = encodeListCursor({ lastActivityAt: ts, sessionId: last.session_id, harness: last.harness });
  }
  return { sessions: visible.map(sessionFromDirectoryRow), nextCursor };
}

/** UTC dates (YYYY-MM-DD) from since through until, inclusive (relayhistory activityDates). */
export function activityDates(since: Date, until: Date): string[] {
  const out: string[] = [];
  const day = Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate());
  const last = Date.UTC(until.getUTCFullYear(), until.getUTCMonth(), until.getUTCDate());
  for (let t = day; t <= last; t += 86_400_000) out.push(new Date(t).toISOString().slice(0, 10));
  return out;
}

export interface ActivityDay {
  date: string;
  total: number;
  bySource: Record<string, number>;
}

/** Every UTC day in the window, ascending and zero-filled (cloud-client sessionActivityResponse). */
export function activityDays(rows: DirectoryActivityRow[], since: Date, until: Date): ActivityDay[] {
  const byDate = new Map<string, ActivityDay>(activityDates(since, until).map((date) => [date, { date, total: 0, bySource: {} }]));
  for (const row of rows) {
    const key = row.day instanceof Date ? row.day.toISOString().slice(0, 10) : String(row.day).slice(0, 10);
    const entry = byDate.get(key);
    if (!entry || typeof row.harness !== "string") continue;
    const sessions = num(row.sessions);
    if (sessions === 0) continue;
    entry.bySource[row.harness] = sessions;
    entry.total += sessions;
  }
  return [...byDate.values()];
}
