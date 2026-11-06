/**
 * Rollback after the flip (docs/relayhistory-t0-runbook.md, Rollback): bring
 * what Supabase accepted since T0 back to Neon, under a new hold, before the
 * gate could ever point the Worker at Neon again. Neon is never simply
 * repointed.
 *
 *   1. Deletions first. Supabase's deleted_upload_sessions rows and resets that
 *      Neon lacks were made after T0: the matching rows are deleted on Neon
 *      (every table with session columns; resets remove all of the user's
 *      rows), and the tombstone and reset rows themselves are copied over.
 *   2. Rows written since T0: for each table, rows whose indexed time column
 *      is at or after --since (all rows when a table has none) are upserted
 *      into Neon, Supabase winning conflicts.
 *
 * Writes run with triggers off (session_replication_role = replica) so rows
 * land as Supabase has them. Without --execute it is a read-only dry run.
 * Writing needs the source to be the named Supabase project and the target
 * the named Neon host:
 *
 *   SUPABASE_SOURCE_URL=... NEON_TARGET_URL=... bun run migration:reverse \
 *     --since=<T0 ISO> --tables=a,b [--chunk-size=500] \
 *     [--execute --expect-supabase-ref=<ref> --expect-neon-host=<host>]
 */
import pg from "pg";
import {
  planTable,
  prepareTargetSession,
  writeSql,
  type Queryable,
  type TablePlan,
} from "./relayhistory-copy.js";
import { supabaseProjectRef } from "./target-guard.js";

const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;
const qualified = (table: string) => `"sessions".${quoteIdent(table)}`;
const TOMBSTONE_COLUMNS = ["org_id", "workspace_id", "user_id", "origin_id", "source", "session_id"];

export interface ReverseResult {
  table: string;
  deleted: number;
  read: number;
  written: number;
}

/** Guard for the reverse direction: Supabase source, Neon target, both named. */
export function reverseWriteMode(input: {
  sourceUrl: string;
  targetUrl: string;
  execute: boolean;
  expectSupabaseRef?: string;
  expectNeonHost?: string;
}): "dry-run" | "execute" {
  if (!input.execute) return "dry-run";
  if (!input.expectSupabaseRef || supabaseProjectRef(input.sourceUrl) !== input.expectSupabaseRef)
    throw new Error("the source is not the expected Supabase project: refusing to write");
  let host: string;
  try {
    host = new URL(input.targetUrl).hostname;
  } catch {
    throw new Error("NEON_TARGET_URL is not a URL");
  }
  if (!input.expectNeonHost || host !== input.expectNeonHost || !/(^|\.)neon\.tech$/.test(host))
    throw new Error("the target is not the expected Neon host: refusing to write");
  return "execute";
}

/**
 * Deletions Supabase recorded after T0: its tombstones and resets that Neon
 * does not have, as JSON arrays.
 */
export async function postT0Deletions(supabase: Queryable, neon: Queryable) {
  const all = async (db: Queryable, table: string, columns: string[]) =>
    (
      await db.query<{ rows: string }>(
        `SELECT coalesce(json_agg(json_build_object(${columns.map((c) => `'${c}', ${quoteIdent(c)}`).join(", ")}))::text, '[]') AS rows
           FROM ${qualified(table)}`,
      )
    ).rows[0]!.rows;
  const resetColumns = ["org_id", "workspace_id", "user_id", "request_id", "deleted"];
  const [supabaseTombstones, neonTombstones, supabaseResets, neonResets] = await Promise.all([
    all(supabase, "deleted_upload_sessions", TOMBSTONE_COLUMNS),
    all(neon, "deleted_upload_sessions", TOMBSTONE_COLUMNS),
    all(supabase, "session_resets", resetColumns),
    all(neon, "session_resets", resetColumns),
  ]);
  const key = (row: Record<string, unknown>, columns: string[]) =>
    JSON.stringify(columns.map((column) => row[column]));
  const missing = (from: string, have: string, columns: string[]) => {
    const present = new Set((JSON.parse(have) as Record<string, unknown>[]).map((row) => key(row, columns)));
    return (JSON.parse(from) as Record<string, unknown>[]).filter((row) => !present.has(key(row, columns)));
  };
  return {
    tombstones: missing(supabaseTombstones, neonTombstones, TOMBSTONE_COLUMNS),
    resets: missing(supabaseResets, neonResets, ["org_id", "workspace_id", "user_id", "request_id"]),
  };
}

/** Delete on Neon what a post-T0 tombstone or reset removed on Supabase. */
export async function replayDeletions(
  neon: Queryable,
  plan: TablePlan,
  deletions: { tombstones: unknown[]; resets: unknown[] },
): Promise<number> {
  const has = (column: string) => plan.columns.includes(column);
  const conditions: string[] = [];
  if (has("org_id") && has("session_id"))
    conditions.push(
      `EXISTS (SELECT 1 FROM jsonb_to_recordset($1::jsonb) AS d(${TOMBSTONE_COLUMNS.map((c) => `${c} text`).join(", ")})
        WHERE ${TOMBSTONE_COLUMNS.filter(has).map((c) => `d.${c} = t.${quoteIdent(c)}`).join(" AND ")})`,
    );
  if (has("org_id") && has("workspace_id") && has("user_id"))
    conditions.push(
      `EXISTS (SELECT 1 FROM jsonb_to_recordset($2::jsonb) AS r(org_id text, workspace_id text, user_id text)
        WHERE r.org_id = t.org_id AND r.workspace_id = t.workspace_id AND r.user_id = t.user_id)`,
    );
  if (conditions.length === 0 || ["deleted_upload_sessions", "session_resets"].includes(plan.table)) return 0;
  const { rows } = await neon.query<{ n: number }>(
    `WITH fence AS (SELECT $1::jsonb AS t, $2::jsonb AS r),
          gone AS (DELETE FROM ${qualified(plan.table)} t WHERE ${conditions.join(" OR ")} RETURNING 1)
     SELECT count(*)::int AS n FROM gone`,
    [JSON.stringify(deletions.tombstones), JSON.stringify(deletions.resets)],
  );
  return rows[0]?.n ?? 0;
}

/** Upsert Supabase rows at or after `since` (all rows without a time column) into Neon. */
export async function reverseTable(
  supabase: Queryable,
  neon: Queryable,
  plan: TablePlan,
  options: { since: Date; chunkSize: number; dryRun?: boolean },
): Promise<{ read: number; written: number }> {
  const keys = plan.key.map(quoteIdent);
  const keyTuple = keys.length === 1 ? keys[0]! : `(${keys.join(", ")})`;
  const since = plan.timeColumn
    ? `${quoteIdent(plan.timeColumn)} >= '${options.since.toISOString()}'::timestamptz`
    : "true";
  let after: string[] | null = null;
  let read = 0;
  let written = 0;
  for (;;) {
    const params: unknown[] = [options.chunkSize, ...(after ?? [])];
    const lower = after
      ? ` AND ${keyTuple} > (${after.map((_, index) => `$${index + 2}`).join(", ")})`
      : "";
    const page = (
      await supabase.query<{ rows: string; n: number; last: string | null }>(
        `WITH page AS (
           SELECT ${plan.columns.map(quoteIdent).join(", ")} FROM ${qualified(plan.table)}
            WHERE ${since}${lower} ORDER BY ${keys.join(", ")} LIMIT $1)
         SELECT coalesce((SELECT json_agg(page)::text FROM page), '[]') AS rows,
                (SELECT count(*)::int FROM page) AS n,
                (SELECT json_build_array(${keys.map((k) => `${k}::text`).join(", ")})::text
                   FROM page ORDER BY ${keys.map((k) => `${k} DESC`).join(", ")} LIMIT 1) AS last`,
        params,
      )
    ).rows[0]!;
    if (page.last === null) return { read, written };
    if (!options.dryRun) {
      const result = await neon.query<{ n: number }>(
        `WITH written AS (${writeSql(plan, "mirror")} RETURNING 1) SELECT count(*)::int AS n FROM written`,
        [page.rows],
      );
      written += result.rows[0]?.n ?? 0;
    }
    read += page.n;
    after = JSON.parse(page.last) as string[];
    if (page.n < options.chunkSize) return { read, written };
  }
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

export async function main() {
  const sourceUrl = process.env.SUPABASE_SOURCE_URL;
  const targetUrl = process.env.NEON_TARGET_URL;
  if (!sourceUrl || !targetUrl) throw new Error("SUPABASE_SOURCE_URL and NEON_TARGET_URL are required");
  const since = new Date(argument("since") ?? "");
  if (Number.isNaN(since.getTime())) throw new Error("--since must be the T0 timestamp (ISO 8601)");
  const tables = (argument("tables") ?? "").split(",").filter(Boolean);
  if (tables.length === 0 || tables.some((table) => !/^[a-z_][a-z0-9_]*$/.test(table)))
    throw new Error("--tables must list sessions table names");
  const chunkSize = Number(argument("chunk-size") ?? 500);
  if (!Number.isInteger(chunkSize) || chunkSize < 1) throw new Error("--chunk-size must be a positive integer");
  const dryRun =
    reverseWriteMode({
      sourceUrl,
      targetUrl,
      execute: process.argv.includes("--execute"),
      ...(argument("expect-supabase-ref") ? { expectSupabaseRef: argument("expect-supabase-ref")! } : {}),
      ...(argument("expect-neon-host") ? { expectNeonHost: argument("expect-neon-host")! } : {}),
    }) === "dry-run";
  const supabase = new pg.Client({ connectionString: sourceUrl, application_name: "96bulls-reverse-source" });
  const neon = new pg.Client({ connectionString: targetUrl, application_name: "96bulls-reverse-target" });
  await Promise.all([supabase.connect(), neon.connect()]);
  try {
    await supabase.query("SET default_transaction_read_only = on");
    if (dryRun) await neon.query("SET default_transaction_read_only = on");
    else await prepareTargetSession(neon);
    const deletions = await postT0Deletions(supabase, neon);
    const results: ReverseResult[] = [];
    for (const table of tables) {
      const plan = await planTable(supabase, neon, table, { newestByTime: true });
      const keyPlan = { ...plan, order: plan.order.slice(plan.timeColumn ? 1 : 0) };
      const deleted = dryRun ? 0 : await replayDeletions(neon, keyPlan, deletions);
      const copied = await reverseTable(supabase, neon, keyPlan, { since, chunkSize, dryRun });
      results.push({ table, deleted, ...copied });
    }
    process.stdout.write(
      `${JSON.stringify({ since: since.toISOString(), dryRun, postT0: { tombstones: deletions.tombstones.length, resets: deletions.resets.length }, tables: results }, null, 2)}\n`,
    );
  } finally {
    await Promise.all([supabase.end(), neon.end()]);
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls reverse copy failed: ${error instanceof Error ? error.message : "unknown error"}`);
    process.exit(1);
  });
