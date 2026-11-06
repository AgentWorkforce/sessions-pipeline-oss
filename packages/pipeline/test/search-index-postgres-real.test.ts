import { describe, it } from "vitest";
import { searchIndexConformance } from "../src/search-index/conformance.js";
import { createPostgresSearchIndex, type SqlClient } from "../src/search-index/index.js";

// Real PostgreSQL run of the shared suite. Skipped unless
// SEARCH_CONFORMANCE_PG_URL names a disposable server whose role can create
// a database (the test creates and drops its own, with pg_trgm).
const url = process.env.SEARCH_CONFORMANCE_PG_URL;

if (!url) {
  describe.skip("SearchIndexAdapter conformance: postgres (real)", () => {
    it("needs SEARCH_CONFORMANCE_PG_URL", () => {});
  });
} else {
  searchIndexConformance("postgres (real server)", async () => {
    const { default: pg } = await import("pg");
    // A disposable database of its own: other proofs sharing the server may
    // have locked down the default database (pg-proof runs role proofs first).
    const name = `search_conformance_${process.pid}_${Date.now().toString(36)}`;
    const admin = new pg.Client({ connectionString: url });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${name}"`);
    const target = new URL(url);
    target.pathname = `/${name}`;
    const pool = new pg.Pool({ connectionString: target.toString(), max: 2 });
    await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    return {
      adapter: createPostgresSearchIndex({ client: pool as unknown as SqlClient }),
      dispose: async () => {
        await pool.end();
        await admin.query(`DROP DATABASE IF EXISTS "${name}"`);
        await admin.end();
      },
    };
  });
}
