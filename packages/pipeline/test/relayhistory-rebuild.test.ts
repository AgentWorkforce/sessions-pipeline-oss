/**
 * Rebuild of straddling sessions after the backfill
 * (`src/migration/relayhistory-rebuild.ts`), on the pinned legacy schema with
 * its real rollup triggers.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, expect, it } from "vitest";
import { rebuildSince } from "../../../src/migration/relayhistory-rebuild.js";

const MIGRATIONS = fileURLToPath(
  new URL("../../../vendor/relayhistory-cloud/migrations/", import.meta.url).href,
);
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");

const T0 = new Date("2026-10-31T00:00:00Z");
let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { pg_trgm } });
  await db.exec(`SET search_path = pg_catalog, public;
    CREATE SCHEMA IF NOT EXISTS sessions;
    CREATE TABLE IF NOT EXISTS sessions.__migrations (
      name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
  for (const file of readdirSync(MIGRATIONS).filter((name) => name.endsWith(".sql")).sort())
    await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
  await db.exec("SET search_path = public");
}, 120_000);
afterAll(async () => {
  await db?.close();
});

const event = (id: string, ts: string, tokens: number) =>
  `INSERT INTO sessions.convergence_events (org_id, workspace_id, machine_id, user_id, source,
     session_id, event_id, kind, type, ts, content, input_tokens, record)
   VALUES ('org-1', 'ws-1', 'm-1', 'user-1', 'claude', 's-1', '${id}', 'history', 'message',
           '${ts}', 'text', ${tokens}, '{}')`;
const rollup = async () =>
  (
    await db.query<{ event_count: string; input_tokens: string; first_ts: Date; last_ts: Date }>(
      "SELECT event_count::text, input_tokens::text, first_ts, last_ts FROM sessions.session_rollups WHERE session_id = 's-1'",
    )
  ).rows;

it("rebuilds a straddling session's rollup from all of its rows, and drops its timing cache", async () => {
  // After T0, live: triggers build a rollup from this one event.
  await db.exec(event("e-post", "2026-10-31T01:00:00Z", 5));
  expect(await rollup()).toMatchObject([{ event_count: "1", input_tokens: "5" }]);

  // The backfill lands pre-T0 history with triggers off: the rollup is stale.
  await db.exec("SET session_replication_role = replica");
  await db.exec(event("e-pre-1", "2026-10-30T10:00:00Z", 100));
  await db.exec(event("e-pre-2", "2026-10-30T11:00:00Z", 20));
  await db.exec(`INSERT INTO sessions.session_timing_cache (org_id, session_id, source, workspace_id)
                 SELECT 'org-1', 's-1', 'claude', 'ws-1' WHERE false`);
  await db.exec("SET session_replication_role = origin");
  expect(await rollup()).toMatchObject([{ event_count: "1" }]);

  const result = await rebuildSince(db, T0, { batch: 50 });
  expect(result).toMatchObject({ sessions: 1, batches: 1 });
  const [rebuilt] = await rollup();
  expect(rebuilt).toMatchObject({ event_count: "3", input_tokens: "125" });
  expect(new Date(rebuilt!.first_ts).toISOString()).toBe("2026-10-30T10:00:00.000Z");
  expect(new Date(rebuilt!.last_ts).toISOString()).toBe("2026-10-31T01:00:00.000Z");
  // The marks are consumed by the rebuild triggers.
  expect((await db.query("SELECT * FROM sessions.session_rollup_dirty")).rows).toEqual([]);
  expect((await db.query("SELECT * FROM sessions.onboarding_rollup_dirty")).rows).toEqual([]);
}, 60_000);

it("touches nothing on a dry run, and nothing for sessions without post-T0 events", async () => {
  const dry = await rebuildSince(db, T0, { batch: 50, dryRun: true });
  expect(dry).toMatchObject({ sessions: 1, timingRowsDropped: 0 });
  const none = await rebuildSince(db, new Date("2027-01-01T00:00:00Z"), { batch: 50 });
  expect(none).toEqual({ sessions: 0, batches: 0, timingRowsDropped: 0 });
});
