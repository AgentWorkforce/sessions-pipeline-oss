/** Read-only CLI for the production search-index contract. */
import pg from "pg";
import { required } from "../shared/env.ts";
import {
  SEARCH_INDEX_CATALOG_SQL,
  auditSearchIndexes,
  type SearchIndexCatalogRow,
} from "./search-index-contract.ts";

const DIRECTORY_ROLLOUT_RELATION = "sessions.session_directory_rollout";

async function directoryProjectionComplete(pool: pg.Pool): Promise<boolean> {
  const exists = await pool.query<{ exists: boolean }>(
    "SELECT to_regclass($1) IS NOT NULL AS exists",
    [DIRECTORY_ROLLOUT_RELATION],
  );
  if (!exists.rows[0]?.exists) return false;
  const result = await pool.query<{ complete: boolean }>(`
    SELECT COALESCE(bool_and(completed_at IS NOT NULL), false) AS complete
      FROM sessions.session_directory_rollout`);
  return result.rows[0]?.complete === true;
}

export async function auditProductionSearchIndexes(
  url = required("HOOPDB_URL"),
) {
  const pool = new pg.Pool({
    connectionString: url,
    max: 1,
    application_name: "96bulls-search-index-audit",
    connectionTimeoutMillis: 15_000,
    statement_timeout: 30_000,
  });
  try {
    const result = await pool.query<SearchIndexCatalogRow>(
      SEARCH_INDEX_CATALOG_SQL,
    );
    return auditSearchIndexes(result.rows, {
      directoryProjectionComplete: await directoryProjectionComplete(pool),
    });
  } finally {
    await pool.end();
  }
}

if (import.meta.main) {
  auditProductionSearchIndexes()
    .then((report) => {
      process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
      if (!report.passed) process.exitCode = 1;
    })
    .catch((error) => {
      console.error(
        `96bulls: search index audit failed: ${error instanceof Error ? error.message : "unknown error"}`,
      );
      process.exitCode = 1;
    });
}
