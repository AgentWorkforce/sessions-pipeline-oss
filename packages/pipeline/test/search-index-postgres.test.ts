import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { describe, expect, it } from "vitest";
import { searchIndexConformance, syntheticDoc } from "../src/search-index/conformance.js";
import { createPostgresSearchIndex, likeLiteral, planScopeReconcile, positionOf, type SqlClient } from "../src/search-index/index.js";

// In-process PGlite with pg_trgm (no database service). CI's pg-proof job
// also runs this file against real PostgreSQL 15/16/17 via
// SEARCH_CONFORMANCE_PG_URL (search-index-postgres-real.test.ts).
searchIndexConformance("postgres (pglite + pg_trgm)", async () => {
  const db = new PGlite({ extensions: { pg_trgm } });
  await db.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
  return { adapter: createPostgresSearchIndex({ client: db as unknown as SqlClient }), dispose: () => db.close() };
});

describe("postgres search index specifics", () => {
  it("keeps distinct reconcile identities separate when fields contain separators", () => {
    const desired = syntheticDoc({ orgId: "o", kind: "a\u001fb", eventId: "c" });
    const stored = syntheticDoc({ orgId: "o", kind: "a", eventId: "b\u001fc" });
    expect(planScopeReconcile([desired], [stored], { complete: true })).toEqual({
      upserts: [desired],
      deletes: [{ ...positionOf(stored), workspaceId: stored.workspaceId }],
    });
    expect(planScopeReconcile([desired], [stored], { complete: false })).toEqual({
      upserts: [desired], deletes: [],
    });
  });

  it("escapes LIKE metacharacters", () => {
    expect(likeLiteral("100%_\\x")).toBe("%100\\%\\_\\\\x%");
  });

  it("refuses unsafe identifiers and a missing pg_trgm", async () => {
    const db = new PGlite();
    expect(() => createPostgresSearchIndex({ client: db as unknown as SqlClient, schema: 'x"; drop' })).toThrow();
    const adapter = createPostgresSearchIndex({ client: db as unknown as SqlClient });
    await expect(adapter.ensureSchema()).rejects.toMatchObject({ code: "schema_missing" });
    expect(await adapter.health()).toEqual({ ok: false, code: "schema_missing" });
    await db.close();
  });

  it("the query's q predicates match the trigram index expressions", async () => {
    // Guards the normalized search columns: if the
    // query and index drifted, no plan could use the index. Which index the
    // planner picks for a tenant-filtered query is a cost decision on real stats.
    const db = new PGlite({ extensions: { pg_trgm } });
    await db.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    const adapter = createPostgresSearchIndex({ client: db as unknown as SqlClient });
    await adapter.ensureSchema();
    const docs = Array.from({ length: 2000 }, (_, i) =>
      syntheticDoc({ orgId: "o", workspaceId: "w", eventId: `p${i}`, tsMs: i, content: `row ${i} filler text ${i * 7919}` }),
    );
    for (let i = 0; i < docs.length; i += 1000) await adapter.upsert(docs.slice(i, i + 1000));
    await db.exec("ANALYZE search_index.transcript_events");
    await db.exec("SET enable_seqscan = off");
    const plan = await db.query<{ "QUERY PLAN": string }>(
      `EXPLAIN SELECT 1 FROM search_index.transcript_events
        WHERE content_prefix_folded LIKE '%needle%' ESCAPE '\\' OR task_title_folded LIKE '%needle%' ESCAPE '\\'`,
    );
    const text = plan.rows.map((r) => r["QUERY PLAN"]).join("\n");
    expect(text).toContain("transcript_events_content_trgm");
    expect(text).toContain("transcript_events_title_trgm");
    await db.close();
  });
});
