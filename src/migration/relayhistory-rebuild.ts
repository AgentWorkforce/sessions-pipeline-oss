/**
 * After the backfill: rebuild derived state for sessions that straddle T0
 * (docs/relayhistory-t0-runbook.md step 13). Their rollups were built on
 * Supabase from post-T0 rows only, and the backfill (triggers off, existing
 * rows kept) does not touch them.
 *
 * Every session with an event at or after T0 is marked in
 * sessions.session_rollup_dirty and sessions.onboarding_rollup_dirty. Their
 * deferred triggers rebuild the session from all of its rows at commit and
 * clear the mark. Timing-cache rows are dropped; the next read refills them.
 * This runs as a normal session, with triggers on, in small transactions.
 *
 * Usage: BULLS_TARGET_URL=... bun run migration:rebuild --since=<T0 ISO> [--batch=200]
 *   [--execute --expect-supabase-ref=<ref>]   (without --execute: a read-only dry run)
 */
import pg from "pg";
import { guardFromArgv } from "./target-guard.js";

export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

export interface RebuildResult {
  sessions: number;
  batches: number;
  timingRowsDropped: number;
}

export async function rebuildSince(
  db: Queryable,
  since: Date,
  options: { batch: number; dryRun?: boolean },
): Promise<RebuildResult> {
  const result: RebuildResult = { sessions: 0, batches: 0, timingRowsDropped: 0 };
  let after: [string, string] | null = null;
  for (;;) {
    const page: { rows: { org_id: string; session_id: string }[] } = await db.query(
      `SELECT DISTINCT org_id, session_id FROM sessions.convergence_events
        WHERE ts >= $1 ${after ? "AND (org_id, session_id) > ($3, $4)" : ""}
        ORDER BY org_id, session_id LIMIT $2`,
      after ? [since.toISOString(), options.batch, ...after] : [since.toISOString(), options.batch],
    );
    if (page.rows.length === 0) return result;
    const keys = JSON.stringify(page.rows);
    if (!options.dryRun) {
      await db.query("BEGIN");
      try {
        for (const table of ["session_rollup_dirty", "onboarding_rollup_dirty"])
          await db.query(
            `INSERT INTO sessions.${table} (org_id, session_id)
             SELECT org_id, session_id FROM jsonb_to_recordset($1::jsonb) AS k(org_id text, session_id text)
             ON CONFLICT DO NOTHING`,
            [keys],
          );
        const dropped = await db.query<{ n: number }>(
          `WITH gone AS (
             DELETE FROM sessions.session_timing_cache c
              USING jsonb_to_recordset($1::jsonb) AS k(org_id text, session_id text)
              WHERE c.org_id = k.org_id AND c.session_id = k.session_id
              RETURNING 1)
           SELECT count(*)::int AS n FROM gone`,
          [keys],
        );
        result.timingRowsDropped += dropped.rows[0]?.n ?? 0;
        await db.query("COMMIT"); // the deferred rebuild triggers run here
      } catch (error) {
        await db.query("ROLLBACK").catch(() => {});
        throw error;
      }
    }
    result.sessions += page.rows.length;
    result.batches += 1;
    const last = page.rows.at(-1)!;
    after = [last.org_id, last.session_id];
  }
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

export async function main() {
  const url = process.env.BULLS_TARGET_URL;
  if (!url) throw new Error("BULLS_TARGET_URL is required");
  const since = new Date(argument("since") ?? "");
  if (Number.isNaN(since.getTime())) throw new Error("--since must be the T0 timestamp (ISO 8601)");
  const batch = Number(argument("batch") ?? 200);
  if (!Number.isInteger(batch) || batch < 1) throw new Error("--batch must be a positive integer");
  const dryRun = guardFromArgv(process.argv, url) === "dry-run";
  const client = new pg.Client({ connectionString: url, application_name: "96bulls-rebuild-straddling" });
  await client.connect();
  try {
    if (dryRun) await client.query("SET default_transaction_read_only = on");
    const result = await rebuildSince(client, since, { batch, dryRun });
    process.stdout.write(`${JSON.stringify({ since: since.toISOString(), dryRun, ...result }, null, 2)}\n`);
  } finally {
    await client.end();
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls rebuild failed: ${error instanceof Error ? error.message : "unknown error"}`);
    process.exit(1);
  });
