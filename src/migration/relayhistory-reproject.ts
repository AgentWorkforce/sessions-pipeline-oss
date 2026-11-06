/**
 * After the backfill: reproject every session-catalog key from all of its
 * delivery records (docs/relayhistory-t0-runbook.md, after step 13).
 *
 * The catalog tables (session_catalog, session_relationships, session_markers,
 * session_commit_links, session_catalog_agents) are derived from
 * sessions.delivery_records and are not copied: a trigger-off, existing-rows-kept
 * copy would bring back a pre-T0 row that a post-T0 tombstone, or a post-T0
 * winner without an agent name, had removed. delivery_records and
 * delivery_catalog_acceptance are copied (a post-T0 delete revision keeps its
 * key), and this tool then recomputes every key through the database's own
 * sessions.project_delivery_catalog_key(): the winner by acceptance order is
 * projected, and a key with no live contender loses its derived row.
 *
 * Keys are every catalog key of a live upsert record plus every key that has a
 * derived row (so orphans are removed), paged in (org_id, workspace_id,
 * key::text) order. Each page is one transaction in a normal session, triggers
 * on: per workspace, in page order, the delivery projection lock that live
 * deliveries take, then its keys. Agent-name rows left without a catalog row are
 * removed with them. Running it again changes nothing.
 *
 * Usage: BULLS_TARGET_URL=... bun run migration:reproject [--batch=500]
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

export const CATALOG_TABLES = [
  "session_catalog",
  "session_relationships",
  "session_markers",
  "session_commit_links",
  "session_catalog_agents",
] as const;
export type CatalogTable = (typeof CATALOG_TABLES)[number];
export type CatalogCounts = Record<CatalogTable, number>;

export interface ReprojectResult {
  /** Distinct (org, workspace, key) reprojected (or, on a dry run, that would be). */
  keys: number;
  /** Keys with no live upsert contender: their derived rows are removed. */
  keysWithoutContender: number;
  batches: number;
  /** Workspace projection locks taken, summed over batches. */
  workspaceLocks: number;
  /** session_catalog_agents rows removed because their catalog key has no row. */
  orphanAgentRowsRemoved: number;
  /** Rows whose normalized name is not the target's `lower(agent_name)`. Must be 0. */
  agentNameMismatches: number;
  before: CatalogCounts;
  /** Null on a dry run. */
  after: CatalogCounts | null;
}

const CONTENDER_KINDS =
  "('session', 'relationship', 'session_marker', 'commit_link')";

// Every key, tagged with whether a live upsert contends for it. Built from the
// same key function the projection uses; derived rows map back to the key shape
// project_delivery_catalog_key reads: [kind, source, session, ...]. After the
// first page every branch skips the workspaces already done ($2, $3), which the
// key index and the tables' primary keys serve; within the current workspace
// the page is still sorted from all of its keys.
const keySet = (resume: boolean) => {
  const from = resume ? "AND (org_id, workspace_id) >= ($2, $3)" : "";
  return `
  SELECT org_id, workspace_id, key, key::text AS key_text, bool_or(live) AS live
    FROM (
      SELECT d.org_id, d.workspace_id,
             sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) AS key,
             true AS live
        FROM sessions.delivery_records AS d
       WHERE d.kind IN ${CONTENDER_KINDS}
         AND sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) IS NOT NULL
         ${from}
      UNION ALL
      SELECT org_id, workspace_id, jsonb_build_array('session', source, session_id), false
        FROM sessions.session_catalog WHERE true ${from}
      UNION ALL
      SELECT org_id, workspace_id, jsonb_build_array('session', source, session_id), false
        FROM sessions.session_catalog_agents WHERE true ${from}
      UNION ALL
      SELECT org_id, workspace_id,
             jsonb_build_array('relationship', source, parent_session_id, relationship_uid), false
        FROM sessions.session_relationships WHERE true ${from}
      UNION ALL
      SELECT org_id, workspace_id, jsonb_build_array('session_marker', source, session_id, marker_uid), false
        FROM sessions.session_markers WHERE true ${from}
      UNION ALL
      SELECT org_id, workspace_id,
             jsonb_build_array('commit_link', source, session_id, commit_sha, match_method), false
        FROM sessions.session_commit_links WHERE true ${from}
    ) AS k
   GROUP BY org_id, workspace_id, key`;
};

interface KeyRow {
  org_id: string;
  workspace_id: string;
  key: unknown;
  key_text: string;
  live: boolean;
}

export async function catalogCounts(db: Queryable): Promise<CatalogCounts> {
  const { rows } = await db.query<Record<CatalogTable, number>>(
    `SELECT ${CATALOG_TABLES.map((table) => `(SELECT count(*)::int FROM sessions.${table}) AS ${table}`).join(", ")}`,
  );
  const row = rows[0]!;
  return Object.fromEntries(
    CATALOG_TABLES.map((table) => [table, Number(row[table])]),
  ) as CatalogCounts;
}

/**
 * The lookup trigger owns agent_name_normalized, so any row that differs got
 * past it (or predates the 0067 fill). NULL counts as a mismatch.
 */
export async function countAgentNameMismatches(db: Queryable): Promise<number> {
  const { rows } = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM sessions.session_catalog_agents
      WHERE agent_name_normalized IS DISTINCT FROM lower(agent_name)`,
  );
  return Number(rows[0]?.n ?? 0);
}

export async function reprojectCatalog(
  db: Queryable,
  options: { batch: number; dryRun?: boolean },
): Promise<ReprojectResult> {
  if (!Number.isInteger(options.batch) || options.batch < 1)
    throw new Error("batch must be a positive integer");
  const result: ReprojectResult = {
    keys: 0,
    keysWithoutContender: 0,
    batches: 0,
    workspaceLocks: 0,
    orphanAgentRowsRemoved: 0,
    agentNameMismatches: 0,
    before: await catalogCounts(db),
    after: null,
  };
  let after: [string, string, string] | null = null;
  for (;;) {
    const page: { rows: KeyRow[] } = await db.query<KeyRow>(
      `SELECT * FROM (${keySet(after !== null)}) AS keys
        ${after ? "WHERE (org_id, workspace_id, key_text) > ($2, $3, $4)" : ""}
        ORDER BY org_id, workspace_id, key_text LIMIT $1`,
      after ? [options.batch, ...after] : [options.batch],
    );
    if (page.rows.length === 0) break;
    // Workspaces in page order, which is sorted: concurrent runs take them in the
    // same order, and a live delivery only ever holds one.
    const workspaces = new Map<
      string,
      { org: string; workspace: string; keys: unknown[] }
    >();
    for (const row of page.rows) {
      const id = JSON.stringify([row.org_id, row.workspace_id]);
      const group = workspaces.get(id) ?? {
        org: row.org_id,
        workspace: row.workspace_id,
        keys: [],
      };
      group.keys.push(
        typeof row.key === "string" ? JSON.parse(row.key) : row.key,
      );
      workspaces.set(id, group);
    }
    if (!options.dryRun) {
      await db.query("BEGIN");
      try {
        for (const { org, workspace, keys } of workspaces.values()) {
          await db.query(
            "SELECT sessions.delivery_projection_lock_timed($1, $2)",
            [org, workspace],
          );
          await db.query(
            `SELECT sessions.project_delivery_catalog_key($1, $2, k.key)
               FROM jsonb_array_elements($3::jsonb) WITH ORDINALITY AS k(key, n) ORDER BY k.n`,
            [org, workspace, JSON.stringify(keys)],
          );
          // A name row whose catalog key has no row is an orphan: the catalog's
          // delete trigger only clears names for rows it deletes.
          const removed = await db.query<{ n: number }>(
            `WITH gone AS (
               DELETE FROM sessions.session_catalog_agents AS a
                USING jsonb_array_elements($3::jsonb) AS k(key)
                WHERE k.key->>0 = 'session' AND a.org_id = $1 AND a.workspace_id = $2
                  AND a.source = k.key->>1 AND a.session_id = k.key->>2
                  AND NOT EXISTS (SELECT 1 FROM sessions.session_catalog AS c
                                   WHERE c.org_id = a.org_id AND c.workspace_id = a.workspace_id
                                     AND c.source = a.source AND c.session_id = a.session_id)
                RETURNING 1)
             SELECT count(*)::int AS n FROM gone`,
            [org, workspace, JSON.stringify(keys)],
          );
          result.orphanAgentRowsRemoved += Number(removed.rows[0]?.n ?? 0);
        }
        await db.query("COMMIT");
      } catch (error) {
        await db.query("ROLLBACK").catch(() => {});
        throw error;
      }
    }
    result.keys += page.rows.length;
    result.keysWithoutContender += page.rows.filter((row) => !row.live).length;
    result.workspaceLocks += workspaces.size;
    result.batches += 1;
    const last = page.rows.at(-1)!;
    after = [last.org_id, last.workspace_id, last.key_text];
  }
  if (!options.dryRun) result.after = await catalogCounts(db);
  result.agentNameMismatches = await countAgentNameMismatches(db);
  return result;
}

function argument(name: string): string | undefined {
  return process.argv
    .find((value) => value.startsWith(`--${name}=`))
    ?.slice(name.length + 3);
}

export async function main() {
  const url = process.env.BULLS_TARGET_URL;
  if (!url) throw new Error("BULLS_TARGET_URL is required");
  const batch = Number(argument("batch") ?? 500);
  if (!Number.isInteger(batch) || batch < 1)
    throw new Error("--batch must be a positive integer");
  const dryRun = guardFromArgv(process.argv, url) === "dry-run";
  const client = new pg.Client({
    connectionString: url,
    application_name: "96bulls-reproject-catalog",
  });
  await client.connect();
  try {
    if (dryRun) await client.query("SET default_transaction_read_only = on");
    const result = await reprojectCatalog(client, { batch, dryRun });
    process.stdout.write(`${JSON.stringify({ dryRun, ...result }, null, 2)}\n`);
    if (result.agentNameMismatches > 0)
      throw new Error(
        `${result.agentNameMismatches} agent names differ from lower(agent_name)`,
      );
  } finally {
    await client.end();
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(
      `96bulls reproject failed: ${error instanceof Error ? error.message : "unknown error"}`,
    );
    process.exit(1);
  });
