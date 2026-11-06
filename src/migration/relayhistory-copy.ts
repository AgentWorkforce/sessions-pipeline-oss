/**
 * Row copy from RelayHistory's Neon `sessions` schema into Supabase, for the T0
 * cutover (docs/relayhistory-supabase-migration.md, docs/relayhistory-t0-write-hold.md).
 *
 *   mirror   T0 copy set (tokens, deletion tombstones, cursors). Source wins:
 *            changed rows are updated, and target rows whose key is gone from
 *            the source are deleted. Only before or during the hold, while
 *            Supabase has taken no writes of its own.
 *   backfill History after T0. Missing rows are inserted, newest key first;
 *            rows that already exist (written after T0) are never touched.
 *   check    Read-only: count source rows whose key Supabase lacks. Zero for
 *            every table is the criterion for removing the Neon read fallback
 *            (exit status 2 otherwise).
 *
 * Rows move as JSON through jsonb_populate_recordset, so every column type
 * (jsonb, arrays, timestamps, bytea, vector) is converted by Postgres itself.
 * Generated columns are skipped. The backfill's target session runs with
 * triggers off (prepareTargetSession), so rows land exactly as Neon has them.
 * The mirror needs no `session_replication_role`: it lands only on tables whose
 * triggers are reviewed guards (prepareGuardedTargetSession). Keyset pages on the primary key make each run
 * resumable from a checkpoint. Nothing here prints row contents or URLs.
 *
 * Usage: RELAYHISTORY_SOURCE_URL=... BULLS_TARGET_URL=... \
 *   bun run migration:copy --mode=mirror|backfill|check --tables=a,b [--chunk-size=500]
 *   [--checkpoint=/secure/path.json] [--keep-target-only=machines]
 *   [--execute --expect-supabase-ref=<ref>]   (without --execute: a read-only dry run)
 */
import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import pg from "pg";
import { guardFromArgv } from "./target-guard.js";

export type CopyMode = "mirror" | "backfill" | "check";

/** The query surface shared by node-postgres and PGlite. */
export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

export interface TablePlan {
  table: string;
  /** Conflict target: the primary key, or a unique key that treats NULLs as equal. */
  key: string[];
  /**
   * Keyset order as SQL expressions. A nullable key column orders as a text
   * NULL flag then `coalesce(col::text, '')`, because a plain tuple comparison
   * would silently skip rows whose key holds a NULL.
   */
  order: string[];
  nullableKey: boolean;
  columns: string[];
  /** Backfill and check page newest first by this NOT NULL, indexed time column, then key. */
  timeColumn?: string;
}

export interface TableResult {
  table: string;
  read: number;
  written: number;
  deleted: number;
  /** check mode: source rows whose key the target lacks. */
  missing: number;
  pages: number;
  after: unknown[] | null;
  complete: boolean;
}

const SCHEMA = "sessions";
const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;
const qualified = (table: string) => `${quoteIdent(SCHEMA)}.${quoteIdent(table)}`;

interface KeyInfo {
  columns: string[];
  nullable: boolean[];
}

/**
 * The primary key, else one unique, non-partial, column-only index that
 * either has all key columns NOT NULL or treats NULLs as equal (NULLS NOT
 * DISTINCT). Derived tables such as session_rollups have only the latter.
 */
async function keyInfo(db: Queryable, table: string): Promise<KeyInfo> {
  const { rows } = await db.query<{ index: string; name: string; nullable: boolean }>(
    `WITH candidate AS (
       SELECT i.indexrelid, i.indisprimary, i.indkey, i.indnkeyatts, c.oid AS relid
         FROM pg_index i
         JOIN pg_class c ON c.oid = i.indrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique AND i.indisvalid
          AND i.indpred IS NULL AND i.indexprs IS NULL
          AND (i.indisprimary OR i.indnullsnotdistinct OR NOT EXISTS (
                SELECT 1 FROM unnest(i.indkey) AS k(attnum)
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                 WHERE NOT a.attnotnull))
        ORDER BY i.indisprimary DESC, i.indnkeyatts, i.indexrelid
        LIMIT 1)
     SELECT candidate.indexrelid::text AS index, a.attname AS name, NOT a.attnotnull AS nullable
       FROM candidate
       JOIN unnest(candidate.indkey) WITH ORDINALITY AS k(attnum, position) ON true
       JOIN pg_attribute a ON a.attrelid = candidate.relid AND a.attnum = k.attnum
      WHERE k.position <= candidate.indnkeyatts
      ORDER BY k.position`,
    [SCHEMA, table],
  );
  return { columns: rows.map((row) => row.name), nullable: rows.map((row) => row.nullable) };
}

/** Writable (non-generated, non-dropped) columns, in table order. */
async function writableColumns(db: Queryable, table: string): Promise<string[]> {
  const { rows } = await db.query<{ name: string }>(
    `SELECT a.attname AS name
       FROM pg_attribute a
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0
        AND NOT a.attisdropped AND a.attgenerated = ''
      ORDER BY a.attnum`,
    [SCHEMA, table],
  );
  return rows.map((row) => row.name);
}

/**
 * Copy the source's columns that the target also has. The same primary key is
 * required on both sides; anything else fails closed before a row moves.
 */
const TIME_COLUMNS = ["ts", "received_at", "created_at", "updated_at"];

/**
 * A NOT NULL time column that leads a valid btree index on the source, so
 * ordering by it is an index walk, not a sort of the whole table.
 */
async function indexedTimeColumn(source: Queryable, table: string): Promise<string | null> {
  const { rows } = await source.query<{ name: string }>(
    `SELECT a.attname AS name
       FROM pg_index i
       JOIN pg_class c ON c.oid = i.indrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_class ic ON ic.oid = i.indexrelid
       JOIN pg_am am ON am.oid = ic.relam AND am.amname = 'btree'
       JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
      WHERE n.nspname = $1 AND c.relname = $2 AND i.indisvalid AND i.indpred IS NULL
        AND a.attnotnull AND a.attname = ANY ($3::text[])
      ORDER BY array_position($3::text[], a.attname::text)
      LIMIT 1`,
    [SCHEMA, table, TIME_COLUMNS],
  );
  return rows[0]?.name ?? null;
}

export async function planTable(
  source: Queryable,
  target: Queryable,
  table: string,
  options: { newestByTime?: boolean } = {},
): Promise<TablePlan> {
  const [sourceKey, targetKey, sourceColumns, targetColumns] = await Promise.all([
    keyInfo(source, table),
    keyInfo(target, table),
    writableColumns(source, table),
    writableColumns(target, table),
  ]);
  if (sourceKey.columns.length === 0)
    throw new Error(`${table}: no primary or usable unique key on the source`);
  if (sourceKey.columns.join("\0") !== targetKey.columns.join("\0"))
    throw new Error(`${table}: keys differ between source and target`);
  const available = new Set(targetColumns);
  const missing = sourceColumns.filter((column) => !available.has(column));
  if (missing.length)
    throw new Error(`${table}: target lacks ${missing.length} source column(s)`);
  const order = sourceKey.columns.flatMap((column, index) =>
    sourceKey.nullable[index]
      ? [
          `(CASE WHEN ${quoteIdent(column)} IS NULL THEN '1' ELSE '0' END)`,
          `coalesce(${quoteIdent(column)}::text, '')`,
        ]
      : [quoteIdent(column)],
  );
  const timeColumn = options.newestByTime ? await indexedTimeColumn(source, table) : null;
  return {
    table,
    key: sourceKey.columns,
    order: timeColumn ? [quoteIdent(timeColumn), ...order] : order,
    nullableKey: sourceKey.nullable.some(Boolean),
    columns: sourceColumns,
    ...(timeColumn ? { timeColumn } : {}),
  };
}

function keyTuple(columns: string[]): string {
  const list = columns.map(quoteIdent).join(", ");
  return columns.length === 1 ? list : `(${list})`;
}

function orderTuple(order: string[]): string {
  return order.length === 1 ? order[0]! : `(${order.join(", ")})`;
}

function keyParams(columns: string[], first: number): string {
  const list = columns.map((_, index) => `$${first + index}`).join(", ");
  return columns.length === 1 ? list : `(${list})`;
}

/**
 * One keyset page, serialized by Postgres: `rows` is JSON text that goes
 * straight into the target, and `last` is the page's final key as text values.
 * Node never parses row values, so bytea, timestamp without time zone, bigint
 * and vector survive unchanged.
 */
export function pageSql(plan: TablePlan, mode: CopyMode, hasAfter: boolean): string {
  const projection = plan.columns.map(quoteIdent).join(", ");
  const forward = mode === "mirror";
  const where = hasAfter
    ? ` WHERE ${orderTuple(plan.order)} ${forward ? ">" : "<"} ${keyParams(plan.order, 2)}`
    : "";
  const order = (ascending: boolean) =>
    plan.order.map((expression) => `${expression} ${ascending ? "ASC" : "DESC"}`).join(", ");
  const lastKey = `json_build_array(${plan.order.map((expression) => `${expression}::text`).join(", ")})::text`;
  return `WITH page AS (
    SELECT ${projection} FROM ${qualified(plan.table)}${where} ORDER BY ${order(forward)} LIMIT $1
  )
  SELECT coalesce((SELECT json_agg(page)::text FROM page), '[]') AS rows,
         (SELECT count(*)::int FROM page) AS n,
         (SELECT ${lastKey} FROM page ORDER BY ${order(!forward)} LIMIT 1) AS last`;
}

export function writeSql(plan: TablePlan, mode: CopyMode, fenced = false): string {
  const columns = plan.columns.map(quoteIdent).join(", ");
  const insert = `INSERT INTO ${qualified(plan.table)} (${columns})
    SELECT ${columns} FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb) s${
      mode === "backfill" && fenced ? tombstoneFilter(plan) : ""
    }`;
  const conflict = `ON CONFLICT (${plan.key.map(quoteIdent).join(", ")})`;
  if (mode === "backfill") return `${insert} ${conflict} DO NOTHING`;
  const updates = plan.columns.filter((column) => !plan.key.includes(column));
  if (updates.length === 0) return `${insert} ${conflict} DO NOTHING`;
  const set = updates
    .map((column) => `${quoteIdent(column)} = EXCLUDED.${quoteIdent(column)}`)
    .join(", ");
  // Skip identical rows so a catch-up pass rewrites only what changed.
  const changed = `(${updates.map((column) => `${qualified(plan.table)}.${quoteIdent(column)}`).join(", ")}) IS DISTINCT FROM (${updates.map((column) => `EXCLUDED.${quoteIdent(column)}`).join(", ")})`;
  return `${insert} ${conflict} DO UPDATE SET ${set} WHERE ${changed}`;
}

/** Mirror only: target keys within a source page's key range that the source no longer has. */
export function staleKeysSql(plan: TablePlan, hasAfter: boolean): string {
  const keys = plan.key.map(quoteIdent).join(", ");
  const lower = hasAfter ? `${keyTuple(plan.key)} > ${keyParams(plan.key, 1)} AND ` : "";
  const upperStart = hasAfter ? plan.key.length + 1 : 1;
  return `SELECT ${keys} FROM ${qualified(plan.table)}
    WHERE ${lower}${keyTuple(plan.key)} <= ${keyParams(plan.key, upperStart)}
      AND NOT (${keyTuple(plan.key)} IN (
        SELECT ${keys} FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $${upperStart + plan.key.length}::jsonb)))`;
}

const TOMBSTONE_COLUMNS = ["org_id", "workspace_id", "user_id", "origin_id", "source", "session_id"];

const has = (plan: TablePlan, column: string) => plan.columns.includes(column);
// Fenced statements take $1 = page rows, $2 = the page's session owners on Neon,
// $3 = Neon's resets (fixed after T0).
// Declares the fence parameters' types even where a table's filter does not use all of them.
const FENCE_PARAMS = "fence_params AS (SELECT $2::jsonb AS owners, $3::jsonb AS neon_resets)";
const NEON_RESETS =
  "jsonb_to_recordset($3::jsonb) AS n(org_id text, workspace_id text, user_id text, request_id text)";
const hasUser = (plan: TablePlan) =>
  ["org_id", "workspace_id", "user_id"].every((column) => has(plan, column));
const hasSession = (plan: TablePlan) => has(plan, "org_id") && has(plan, "session_id");

/**
 * A reset made on Supabase after T0, for the user in `alias`: a row in the
 * live sessions.session_resets that Neon (frozen at T0) does not have. Read
 * inside the page's insert, after its user locks, so a reset committed at any
 * point during the backfill is honoured.
 */
const postT0Reset = (alias: string) =>
  `EXISTS (SELECT 1 FROM sessions.session_resets r
     WHERE r.org_id = ${alias}.org_id AND r.workspace_id = ${alias}.workspace_id AND r.user_id = ${alias}.user_id
       AND NOT EXISTS (SELECT 1 FROM ${NEON_RESETS}
             WHERE n.org_id = r.org_id AND n.workspace_id = r.workspace_id
               AND n.user_id = r.user_id AND n.request_id = r.request_id))`;

const OWNERS = "jsonb_to_recordset($2::jsonb) AS o(org_id text, session_id text, workspace_id text, user_id text)";

/**
 * Backfill must not resurrect what users deleted on Supabase after T0. A hard
 * delete leaves no key to conflict with, so each row is checked against:
 *   - sessions.deleted_upload_sessions, matched on every tombstone column the
 *     table has (at least org_id and session_id);
 *   - resets made after T0 (postT0Reset): a reset deletes everything the user
 *     had, so none of their history returns. Tables without user columns
 *     match through the page's session owners on Neon ($2).
 */
export function tombstoneFilter(plan: TablePlan): string {
  const parts: string[] = [];
  if (hasSession(plan)) {
    const match = TOMBSTONE_COLUMNS.filter((column) => has(plan, column))
      .map((column) => `d.${quoteIdent(column)} = s.${quoteIdent(column)}`)
      .join(" AND ");
    parts.push(`NOT EXISTS (SELECT 1 FROM sessions.deleted_upload_sessions d WHERE ${match})`);
  }
  if (hasUser(plan)) parts.push(`NOT ${postT0Reset("s")}`);
  else if (hasSession(plan))
    parts.push(
      `NOT EXISTS (SELECT 1 FROM ${OWNERS} WHERE o.org_id = s.org_id AND o.session_id = s.session_id AND ${postT0Reset("o")})`,
    );
  return parts.length ? ` WHERE ${parts.join(" AND ")}` : "";
}

/** Whether the page needs its session owners from Neon ($2): session tables without user columns. */
export const needsOwners = (plan: TablePlan) => !hasUser(plan) && hasSession(plan);

/**
 * The per-user lock the deletion functions take (set_session_uploads,
 * reset_uploaded_sessions), for every user in the page, so a deletion either
 * commits before this page's insert reads the fences, or waits and deletes
 * what it inserted.
 */
export function deletionLockSql(plan: TablePlan): string | null {
  const users = hasUser(plan)
    ? `SELECT DISTINCT org_id, workspace_id, user_id FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb)`
    : needsOwners(plan)
      ? `SELECT DISTINCT org_id, workspace_id, user_id FROM ${OWNERS.replace("$2", "$1")}`
      : null;
  if (!users) return null;
  return `SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(org_id, workspace_id, user_id)::text, 0))
    FROM (${users} ORDER BY 1, 2, 3) AS users`;
}

/** Owners (workspace, user) on Neon of the sessions in a source page. */
export async function pageOwners(source: Queryable, plan: TablePlan, rows: string): Promise<string> {
  const owners = await source.query<{ owners: string }>(
    `SELECT coalesce(json_agg(DISTINCT jsonb_build_object(
              'org_id', e.org_id, 'session_id', e.session_id,
              'workspace_id', e.workspace_id, 'user_id', e.user_id))::text, '[]') AS owners
       FROM sessions.convergence_events e
       JOIN (SELECT DISTINCT org_id, session_id
               FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb)) AS k
         ON e.org_id = k.org_id AND e.session_id = k.session_id`,
    [rows],
  );
  return owners.rows[0]?.owners ?? "[]";
}

/** The deletion fence's fixed part: Neon's resets, which cannot change after T0. */
export interface Fence {
  neonResets: string;
}

/**
 * Read Neon's resets once. Resets on Supabase are always read live, inside
 * each statement, so a reset committed during the run is honoured.
 */
export async function loadFence(source: Queryable): Promise<Fence> {
  const neon = await source.query<{ keys: string }>(
    `SELECT coalesce(json_agg(json_build_object('org_id', org_id, 'workspace_id', workspace_id,
              'user_id', user_id, 'request_id', request_id))::text, '[]') AS keys
       FROM sessions.session_resets`,
  );
  return { neonResets: neon.rows[0]?.keys ?? "[]" };
}

/**
 * Tables derived from delivery records by the catalog projection. They are
 * never copied: a copy would resurrect rows that a post-T0 tombstone or a
 * post-T0 winner removed. migration:reproject rebuilds them from all records.
 */
export const REPROJECTED_TABLES = new Set([
  "session_catalog",
  "session_relationships",
  "session_markers",
  "session_commit_links",
  "session_catalog_agents",
]);

/**
 * check mode: rows of a source page whose key is absent on the target
 * (NULL-safe). With a fence, rows a post-T0 deletion removed on purpose are
 * not counted: only rows the backfill should have brought are.
 */
export function missingSql(plan: TablePlan, fenced = false): string {
  const match = plan.key
    .map((column) => `t.${quoteIdent(column)} IS NOT DISTINCT FROM s.${quoteIdent(column)}`)
    .join(" AND ");
  const fence = fenced ? tombstoneFilter(plan).replace(/^ WHERE /, " AND ") : "";
  return `${fenced ? `WITH ${FENCE_PARAMS} ` : ""}SELECT count(*)::int AS n
    FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb) s
   WHERE NOT EXISTS (SELECT 1 FROM ${qualified(plan.table)} t WHERE ${match})${fence}`;
}

export interface CopyOptions {
  mode: CopyMode;
  chunkSize: number;
  dryRun?: boolean;
  /**
   * Mirror only: keep target rows the source lacks. For tables another writer
   * shares on Supabase (sessions.machines: pipeline-only machines).
   */
  keepTargetOnly?: boolean;
  /** backfill and check: honour deletion tombstones and resets (from loadFence). */
  fence?: Fence;
  /** Resume point from a checkpoint; null starts from the first key. */
  after?: unknown[] | null;
  onPage?: (result: TableResult) => Promise<void>;
}

export async function copyTable(
  source: Queryable,
  target: Queryable,
  plan: TablePlan,
  options: CopyOptions,
): Promise<TableResult> {
  if (options.mode !== "check" && REPROJECTED_TABLES.has(plan.table))
    throw new Error(`${plan.table}: reprojected by migration:reproject, never copied`);
  if (options.mode === "mirror" && plan.timeColumn)
    throw new Error(`${plan.table}: mirror pages by key; plan it without newestByTime`);
  if (options.mode === "mirror" && plan.nullableKey)
    throw new Error(`${plan.table}: mirror needs a key without NULLs`);
  const result: TableResult = {
    table: plan.table,
    read: 0,
    written: 0,
    deleted: 0,
    missing: 0,
    pages: 0,
    after: options.after ?? null,
    complete: false,
  };
  for (;;) {
    const hasAfter = result.after !== null;
    const page = (
      await source.query<{ rows: string; n: number; last: string | null }>(
        pageSql(plan, options.mode, hasAfter),
        hasAfter ? [options.chunkSize, ...result.after!] : [options.chunkSize],
      )
    ).rows[0]!;
    const upper = page.last === null ? null : (JSON.parse(page.last) as string[]);
    if (options.mode === "mirror" && !options.keepTargetOnly) {
      // Delete target rows in this key range the source no longer has. With no
      // source page left, the open-ended tail of target-only keys goes too.
      const stale = upper
        ? await target.query(staleKeysSql(plan, hasAfter), [
            ...(hasAfter ? result.after! : []),
            ...upper,
            page.rows,
          ])
        : await target.query(
            `SELECT ${plan.key.map(quoteIdent).join(", ")} FROM ${qualified(plan.table)}${
              hasAfter ? ` WHERE ${keyTuple(plan.key)} > ${keyParams(plan.key, 1)}` : ""
            }`,
            hasAfter ? result.after! : [],
          );
      if (stale.rows.length && !options.dryRun) {
        const keys = plan.key.map(quoteIdent).join(", ");
        await target.query(
          `DELETE FROM ${qualified(plan.table)} WHERE ${keyTuple(plan.key)} IN (
             SELECT ${keys} FROM ${qualified(plan.table)}
              WHERE ${keyTuple(plan.key)} IN (${staleSubquery(plan, hasAfter, upper !== null)}))`,
          [...(hasAfter ? result.after! : []), ...(upper ?? []), ...(upper ? [page.rows] : [])],
        );
      }
      result.deleted += stale.rows.length;
    }
    if (upper === null) {
      result.complete = true;
      await options.onPage?.(result);
      return result;
    }
    const fence = options.fence;
    const owners = fence && needsOwners(plan) ? await pageOwners(source, plan, page.rows) : "[]";
    const fenceParams = fence ? [page.rows, owners, fence.neonResets] : [page.rows];
    if (options.mode === "check") {
      const absent = await target.query<{ n: number }>(missingSql(plan, fence !== undefined), fenceParams);
      result.missing += absent.rows[0]?.n ?? 0;
    } else if (!options.dryRun) {
      const fenced = options.mode === "backfill" && fence !== undefined;
      const lock = fenced ? deletionLockSql(plan) : null;
      if (fenced) await target.query("BEGIN");
      try {
        if (lock) await target.query(lock, [needsOwners(plan) ? owners : page.rows]);
        const written = await target.query<{ n: number }>(
          `WITH ${fenced ? `${FENCE_PARAMS}, ` : ""}written AS (${writeSql(plan, options.mode, fenced)} RETURNING 1) SELECT count(*)::int AS n FROM written`,
          fenced ? fenceParams : [page.rows],
        );
        if (fenced) await target.query("COMMIT");
        result.written += written.rows[0]?.n ?? 0;
      } catch (error) {
        if (fenced) await target.query("ROLLBACK").catch(() => {});
        throw error;
      }
    }
    result.read += page.n;
    result.pages += 1;
    result.after = upper;
    await options.onPage?.(result);
    if (page.n < options.chunkSize) {
      result.complete = true;
      // Backfill is done; mirror makes one more pass to clear trailing target-only keys.
      if (options.mode !== "mirror" || options.keepTargetOnly) return result;
    }
  }
}

/** The key subquery behind staleKeysSql, or the open tail when the source has no page left. */
function staleSubquery(plan: TablePlan, hasAfter: boolean, bounded: boolean): string {
  if (bounded) {
    const select = staleKeysSql(plan, hasAfter);
    return select;
  }
  const keys = plan.key.map(quoteIdent).join(", ");
  return `SELECT ${keys} FROM ${qualified(plan.table)}${
    hasAfter ? ` WHERE ${keyTuple(plan.key)} > ${keyParams(plan.key, 1)}` : ""
  }`;
}

/**
 * Copies replicate rows as they are: user triggers (projections, rollups,
 * revision feed, analysis wakeups) must not fire, or a backfill would replay
 * every historical session into the outbox, the brief queue and Cloud's feed.
 * `replica` disables them for this session only; live traffic keeps them.
 * Fails closed if the role may not set it.
 */
export async function prepareTargetSession(target: Queryable): Promise<void> {
  await target.query("SET session_replication_role = replica");
  const { rows } = await target.query<{ session_replication_role: string }>(
    "SHOW session_replication_role",
  );
  if (rows[0]?.session_replication_role !== "replica")
    throw new Error("target session could not disable triggers");
}

/**
 * The only user triggers allowed to fire while the T0 copy set lands with
 * triggers on. Each is a guard that raises or passes and writes nothing,
 * pinned to its table and function. A new trigger on a copy-set table is
 * refused until it is reviewed and added here. Each guard is also required:
 * a listed table whose guard is missing or disabled (drift) is refused, since
 * rows would otherwise land without the protection the guard exists for.
 */
export const GUARD_TRIGGERS: ReadonlyArray<{ table: string; trigger: string; fn: string }> = [
  // 0038/0044: raises upload_cleanup_not_ready until the cleanup rollout is ready.
  { table: "delivery_origins", trigger: "guard_delivery_origin_cleanup", fn: "sessions.guard_delivery_cleanup" },
];

/**
 * The mirror's target session, without `session_replication_role`: the
 * Supabase admin may not set it. Rows land through the live triggers, so this
 * is safe only for tables whose enabled user triggers are all guards in
 * GUARD_TRIGGERS and that take part in no foreign key (a cascade or an
 * ordering failure would not happen under `replica`). Never disables a
 * trigger. Fails closed on anything else, before a row is written.
 */
export async function prepareGuardedTargetSession(
  target: Queryable,
  tables: string[],
): Promise<void> {
  const { rows: role } = await target.query<{ session_replication_role: string }>(
    "SHOW session_replication_role",
  );
  if (role[0]?.session_replication_role !== "origin")
    throw new Error("guarded copy needs session_replication_role = origin");
  const { rows: triggers } = await target.query<{ table: string; trigger: string; fn: string }>(
    `SELECT c.relname AS table, t.tgname AS trigger, pn.nspname || '.' || p.proname AS fn
       FROM pg_trigger t
       JOIN pg_class c ON c.oid = t.tgrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_proc p ON p.oid = t.tgfoid
       JOIN pg_namespace pn ON pn.oid = p.pronamespace
      WHERE n.nspname = $1 AND c.relname = ANY($2::text[])
        AND NOT t.tgisinternal AND t.tgenabled IN ('O', 'A')
      ORDER BY 1, 2`,
    [SCHEMA, tables],
  );
  const unsafe = triggers.filter(
    (row) =>
      !GUARD_TRIGGERS.some(
        (guard) => guard.table === row.table && guard.trigger === row.trigger && guard.fn === row.fn,
      ),
  );
  if (unsafe.length > 0)
    throw new Error(
      `guarded copy refuses triggers that are not reviewed guards: ${unsafe.map((row) => `${row.table}.${row.trigger}`).join(", ")}`,
    );
  const { rows: keys } = await target.query<{ table: string }>(
    `SELECT DISTINCT c.relname AS table
       FROM pg_constraint k
       JOIN pg_class c ON c.oid IN (k.conrelid, k.confrelid)
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE k.contype = 'f' AND n.nspname = $1 AND c.relname = ANY($2::text[])
      ORDER BY 1`,
    [SCHEMA, tables],
  );
  if (keys.length > 0)
    throw new Error(
      `guarded copy refuses tables in a foreign key: ${keys.map((row) => row.table).join(", ")}`,
    );
  const missing = GUARD_TRIGGERS.filter(
    (guard) =>
      tables.includes(guard.table) &&
      !triggers.some(
        (row) => row.table === guard.table && row.trigger === guard.trigger && row.fn === guard.fn,
      ),
  );
  if (missing.length > 0)
    throw new Error(
      `guarded copy needs these guards enabled: ${missing.map((guard) => `${guard.table}.${guard.trigger}`).join(", ")}`,
    );
  // Required unconditionally for delivery_origins, not only when the guard was found.
  if (tables.includes("delivery_origins")) {
    const { rows: ready } = await target.query<{ ready: boolean }>(
      "SELECT sessions.upload_cleanup_ready() AS ready",
    );
    if (ready[0]?.ready !== true)
      throw new Error("guarded copy needs the upload cleanup rollout ready on the target");
  }
}

interface Checkpoint {
  version: 2;
  mode: CopyMode;
  /** sha256 of the source and target URLs: a checkpoint never resumes elsewhere. */
  endpointHash: string;
  /** Per table: progress plus the hash of the exact plan it was made with. */
  tables: Record<string, TableResult & { planHash: string }>;
}

const sha256 = (...parts: string[]) =>
  createHash("sha256").update(parts.join("\0")).digest("hex");

/** Columns, key, keyset order and page size: any change makes a stored position meaningless. */
export function planHash(plan: TablePlan, chunkSize: number): string {
  return sha256(JSON.stringify({ table: plan.table, key: plan.key, order: plan.order, columns: plan.columns, chunkSize }));
}

export async function loadCheckpoint(
  path: string | undefined,
  mode: CopyMode,
  endpointHash: string,
): Promise<Checkpoint> {
  const empty: Checkpoint = { version: 2, mode, endpointHash, tables: {} };
  if (!path) return empty;
  let parsed: Checkpoint;
  try {
    parsed = JSON.parse(await readFile(path, "utf8")) as Checkpoint;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty;
    throw error;
  }
  if (parsed.version !== 2 || parsed.mode !== mode)
    throw new Error("checkpoint is for a different version or mode");
  if (parsed.endpointHash !== endpointHash)
    throw new Error("checkpoint is for different databases");
  return parsed;
}

async function saveCheckpoint(path: string | undefined, checkpoint: Checkpoint) {
  if (!path) return;
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}`;
  await writeFile(temporary, `${JSON.stringify(checkpoint, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporary, path);
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

export async function main() {
  const sourceUrl = process.env.RELAYHISTORY_SOURCE_URL;
  const targetUrl = process.env.BULLS_TARGET_URL;
  if (!sourceUrl || !targetUrl)
    throw new Error("RELAYHISTORY_SOURCE_URL and BULLS_TARGET_URL are required");
  const mode = argument("mode");
  if (mode !== "mirror" && mode !== "backfill" && mode !== "check")
    throw new Error("--mode must be mirror, backfill or check");
  const tables = (argument("tables") ?? "").split(",").filter(Boolean);
  if (tables.length === 0 || tables.some((table) => !/^[a-z_][a-z0-9_]*$/.test(table)))
    throw new Error("--tables must list sessions table names");
  const chunkSize = Number(argument("chunk-size") ?? 500);
  if (!Number.isInteger(chunkSize) || chunkSize < 1)
    throw new Error("--chunk-size must be a positive integer");
  // Writes need --execute and a positively identified target; check never writes.
  const dryRun =
    mode === "check" || guardFromArgv(process.argv, targetUrl, sourceUrl) === "dry-run";
  const keepTargetOnly = new Set(
    (argument("keep-target-only") ?? "").split(",").filter(Boolean),
  );
  const checkpointPath = argument("checkpoint");
  const checkpoint = await loadCheckpoint(checkpointPath, mode, sha256(sourceUrl, targetUrl));
  // One connection per side for the whole run, so session settings hold.
  const source = new pg.Client({ connectionString: sourceUrl, application_name: "96bulls-copy-source" });
  const target = new pg.Client({ connectionString: targetUrl, application_name: "96bulls-copy-target" });
  try {
    await Promise.all([source.connect(), target.connect()]);
    // The source is only ever read.
    await source.query("SET default_transaction_read_only = on");
    // Dry runs and checks only read. The mirror (T0 copy set) lands through
    // reviewed guard triggers only, and a dry run checks them too; the backfill
    // lands with triggers off.
    if (dryRun) await target.query("SET default_transaction_read_only = on");
    if (mode === "mirror") await prepareGuardedTargetSession(target, tables);
    else if (!dryRun) await prepareTargetSession(target);
    const fence = mode === "backfill" || mode === "check" ? await loadFence(source) : undefined;
    const results: TableResult[] = [];
    for (const table of tables) {
      const plan = await planTable(source, target, table, { newestByTime: mode !== "mirror" });
      const hash = planHash(plan, chunkSize);
      const previous = checkpoint.tables[table];
      if (previous && previous.planHash !== hash)
        throw new Error(`${table}: checkpoint was made with a different plan; start a new checkpoint`);
      if (previous?.complete && mode === "backfill") {
        results.push(previous);
        continue;
      }
      const result = await copyTable(source, target, plan, {
        mode,
        chunkSize,
        dryRun,
        keepTargetOnly: keepTargetOnly.has(table),
        ...(fence ? { fence } : {}),
        after: mode === "backfill" ? (previous?.after ?? null) : null,
        onPage: async (progress) => {
          checkpoint.tables[table] = { ...progress, planHash: hash };
          if (!dryRun) await saveCheckpoint(checkpointPath, checkpoint);
        },
      });
      results.push(result);
    }
    process.stdout.write(
      `${JSON.stringify({ generatedAt: new Date().toISOString(), mode, dryRun, tables: results.map(({ after: _after, ...rest }) => rest) }, null, 2)}\n`,
    );
    if (mode === "check" && results.some((result) => result.missing > 0)) process.exitCode = 2;
  } finally {
    await Promise.all([source.end(), target.end()]);
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(
      `96bulls copy failed: ${error instanceof Error ? error.message : "unknown error"}`,
    );
    process.exit(1);
  });
