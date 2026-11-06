/**
 * Task-title backfill for the logical-session directory (P3b): Neon
 * `sessions.session_rollups.task_title` (READ ONLY) -> Supabase
 * `sessions.session_directory.task_title` (owner path), so the directory list can
 * answer `taskTitle` for history that predates the event projection.
 *
 * What it writes, and why only that (src/hoopdb/session-directory-projector.ts):
 *   - `title` is NOT written. The pointer projector owns it: every projection sets
 *     `title = coalesce(brief title, latest pointer title)`, so a backfilled value
 *     would be erased by the next projection (and could mask a brief title).
 *   - `task_title` is not in session_index; the pointer projector keeps it "as
 *     already projected". Its only other writer is the event projector
 *     (src/hoopdb/session-directory-events.ts), which replaces it with the value
 *     derived from canonical events and sets event_projection_version = 1. So the
 *     backfill fills task_title only where it is NULL, the row is live
 *     (deleted_at IS NULL) and the event projection has not run
 *     (event_projection_version = 0). Once the event projector runs, its value wins.
 *   - `search_text` is rebuilt for the touched rows with the one definition
 *     (sessions.session_directory_search_text), the same arguments the projectors pass,
 *     so the search document includes the task title as it would after a projection.
 *   - created_at / last_activity_at / counts are NOT written: they are NOT NULL and
 *     owned by the pointer projector's precedence (stable historical created_at, the
 *     winning pointer's last_activity_at and counters), so there is no null to fill.
 *
 * Collapse. session_rollups has one row per (org, session, workspace, source,
 * project_id); a session's rows are collapsed to the newest non-null task title by
 * (task_title_ts, task_title), the order the rollup itself uses to pick its title.
 *
 * Keys. The directory's workspace and harness are layout components
 * (src/shared/layout-component.ts, the one map the exporter and the courier use);
 * its session_id is the source id when the pointer recorded a canonical id, else the
 * layout component. Both candidate ids are tried (the projector's own canonical vs
 * legacy preference); exact source-id keys are offered before layout ids. A layout id
 * equal to another session's source id is ambiguous in the directory itself (legacy
 * rows were keyed by the layout id); fill-only means the first title offered wins.
 *
 * Titles are trimmed; an empty title is skipped; one over the directory CHECK
 * (200 characters) is skipped, as the event projector refuses it (never truncated).
 */
import { layoutComponent } from "../shared/layout-component.ts";

export interface Queryable {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export const TASK_TITLE_MAX_CHARS = 200;

export interface RollupKey {
  org_id: string;
  session_id: string;
  workspace_id: string;
  source: string;
}

export interface CollapsedTitle extends RollupKey {
  task_title: string;
}

/**
 * One keyset page of collapsed titles, in the session_rollups_key unique index order
 * (org_id, session_id, workspace_id, source), so the scan is an index walk.
 */
export const NEON_TITLE_PAGE_SQL = `SELECT r.org_id, r.session_id, r.workspace_id, r.source,
       (array_agg(r.task_title ORDER BY r.task_title_ts DESC NULLS LAST, r.task_title DESC))[1] AS task_title
  FROM sessions.session_rollups AS r
 WHERE r.task_title IS NOT NULL
   AND (r.org_id, r.session_id, r.workspace_id, r.source) > ($1, $2, $3, $4)
   AND r.org_id > $6
 GROUP BY r.org_id, r.session_id, r.workspace_id, r.source
 ORDER BY r.org_id, r.session_id, r.workspace_id, r.source
 LIMIT $5`;

/** Fill-only update. Every guard is in SQL, so a concurrent writer can never be overwritten. */
export const FILL_SQL = `UPDATE sessions.session_directory AS d
   SET task_title = s.task_title,
       search_text = sessions.session_directory_search_text(
         d.title, s.task_title, d.agent_name, d.project_id, d.git_repo, d.git_branch, d.model, d.short_summary)
  FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
       AS s(org_id, workspace_id, harness, session_id, task_title)
 WHERE d.org_id = s.org_id AND d.workspace_id = s.workspace_id
   AND d.harness = s.harness AND d.session_id = s.session_id
   AND d.task_title IS NULL AND d.deleted_at IS NULL AND d.event_projection_version = 0
RETURNING d.org_id`;

/** The same predicate, read-only: what FILL_SQL would change. */
export const PLAN_SQL = `SELECT count(*)::int AS fillable
  FROM sessions.session_directory AS d
  JOIN unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
       AS s(org_id, workspace_id, harness, session_id, task_title)
    ON d.org_id = s.org_id AND d.workspace_id = s.workspace_id
   AND d.harness = s.harness AND d.session_id = s.session_id
 WHERE d.task_title IS NULL AND d.deleted_at IS NULL AND d.event_projection_version = 0`;

export type Skip = "empty" | "too_long";

/** The directory value for a rollup title, or why it is skipped. */
export function directoryTitle(raw: string | null | undefined): { title: string } | { skip: Skip } {
  const title = (raw ?? "").trim();
  if (title === "") return { skip: "empty" };
  if ([...title].length > TASK_TITLE_MAX_CHARS) return { skip: "too_long" };
  return { title };
}

/** Directory key candidates for one Neon session: (workspace, harness) encoded; session raw and encoded. */
export function directoryCandidates(row: RollupKey): Array<[string, string, string, string]> {
  const ws = layoutComponent(row.workspace_id);
  const harness = layoutComponent(row.source);
  const ids = [...new Set([row.session_id, layoutComponent(row.session_id)])];
  return ids.map((id) => [row.org_id, ws, harness, id]);
}

export interface BackfillOptions {
  /** false (default): read both sides, write nothing. */
  apply: boolean;
  pageSize?: number;
  /** Pause between pages, ms. */
  pauseMs?: number;
  /** A page slower than this backs off by its own duration (capped at 15 s). */
  maxPageMs?: number;
  /** Start strictly after this org (resume by org; reruns are idempotent anyway). */
  afterOrg?: string;
  /** Stop after this many Neon pages (tests, canaries). */
  maxPages?: number;
  log?: (fields: Record<string, unknown>) => void;
  sleep?: (ms: number) => Promise<void>;
}

export interface BackfillResult {
  mode: "plan" | "apply";
  pages: number;
  sessions: number;
  skipped: Record<Skip, number>;
  /** plan: rows FILL_SQL would change now; apply: rows it changed. */
  filled: number;
  lastOrg: string | null;
}

/**
 * Pages Neon (the caller passes a client already in a read-only session; every page
 * also runs in its own BEGIN READ ONLY transaction), and per page fills the
 * directory in ONE owner transaction (apply) or counts what it would fill (plan,
 * inside BEGIN READ ONLY). Idempotent: a rerun fills only what is still NULL.
 */
export async function backfillDirectoryTitles(
  neon: Queryable,
  supabase: Queryable,
  options: BackfillOptions,
): Promise<BackfillResult> {
  const page = Math.max(1, Math.min(options.pageSize ?? 1000, 5000));
  const pause = Math.max(0, options.pauseMs ?? 250);
  const maxPageMs = options.maxPageMs ?? 4000;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const log = options.log ?? (() => {});
  const result: BackfillResult = {
    mode: options.apply ? "apply" : "plan",
    pages: 0,
    sessions: 0,
    skipped: { empty: 0, too_long: 0 },
    filled: 0,
    lastOrg: null,
  };
  // Keyset cursor (org, session, workspace, source); afterOrg is a separate bound
  // (org ids are never empty, so '' admits every org under any collation).
  let cursor: [string, string, string, string] = ["", "", "", ""];
  const afterOrg = options.afterOrg ?? "";
  for (;;) {
    if (options.maxPages !== undefined && result.pages >= options.maxPages) break;
    const started = Date.now();
    await neon.query("BEGIN READ ONLY");
    let rows: CollapsedTitle[];
    try {
      rows = (await neon.query<CollapsedTitle>(NEON_TITLE_PAGE_SQL, [...cursor, page, afterOrg])).rows;
      await neon.query("COMMIT");
    } catch (error) {
      await neon.query("ROLLBACK").catch(() => {});
      throw error;
    }
    if (rows.length === 0) break;
    result.pages += 1;
    result.sessions += rows.length;
    const cols: string[][] = [[], [], [], [], []];
    const seen = new Set<string>();
    // Exact (source id) keys first, then legacy layout ids, so when a layout id equals
    // another session's source id in the same page the exact match wins the key.
    const titled: Array<[CollapsedTitle, string]> = [];
    for (const row of rows) {
      const t = directoryTitle(row.task_title);
      if ("skip" in t) result.skipped[t.skip] += 1;
      else titled.push([row, t.title]);
    }
    for (const pass of [0, 1])
      for (const [row, title] of titled) {
        const key = directoryCandidates(row)[pass];
        if (!key) continue;
        const k = key.join("\u001f");
        if (seen.has(k)) continue;
        seen.add(k);
        key.forEach((v, i) => cols[i]!.push(v));
        cols[4]!.push(title);
      }
    if (cols[0]!.length > 0) {
      await supabase.query(options.apply ? "BEGIN" : "BEGIN READ ONLY");
      try {
        if (options.apply) {
          result.filled += (await supabase.query(FILL_SQL, cols)).rows.length;
        } else {
          result.filled += (await supabase.query<{ fillable: number }>(PLAN_SQL, cols)).rows[0]?.fillable ?? 0;
        }
        await supabase.query("COMMIT");
      } catch (error) {
        await supabase.query("ROLLBACK").catch(() => {});
        throw error;
      }
    }
    const tail = rows[rows.length - 1]!;
    if (result.lastOrg !== null && result.lastOrg !== tail.org_id) log({ m: "org_done", org: result.lastOrg });
    result.lastOrg = tail.org_id;
    cursor = [tail.org_id, tail.session_id, tail.workspace_id, tail.source];
    const ms = Date.now() - started;
    log({ m: "page", mode: result.mode, page: result.pages, sessions: rows.length, filled: result.filled, ms });
    if (rows.length < page) break;
    await sleep(ms > maxPageMs ? Math.min(ms, 15_000) : pause);
  }
  return result;
}
