/**
 * Catalog reprojection after the backfill
 * (`src/migration/relayhistory-reproject.ts`), on the pinned legacy schema with
 * its real catalog triggers and projection functions.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, expect, it } from "vitest";
import {
  countAgentNameMismatches,
  reprojectCatalog,
} from "../../../src/migration/relayhistory-reproject.js";

const MIGRATIONS = fileURLToPath(
  new URL("../../../vendor/relayhistory-cloud/migrations/", import.meta.url)
    .href,
);
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");

let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { pg_trgm } });
  await db.exec(`SET search_path = pg_catalog, public;
    CREATE SCHEMA IF NOT EXISTS sessions;
    CREATE TABLE IF NOT EXISTS sessions.__migrations (
      name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
  for (const file of readdirSync(MIGRATIONS)
    .filter((name) => name.endsWith(".sql"))
    .sort())
    await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
  // Production's catalog stage (0029): its key index, then the live triggers.
  const { rows } = await db.query<{ index_name: string; definition: string }>(
    "SELECT index_name, definition FROM sessions.delivery_rollout_indexes() WHERE stage = 'catalog'",
  );
  for (const index of rows)
    await db.exec(`CREATE INDEX ${index.index_name} ${index.definition}`);
  await db.exec("SELECT sessions.activate_delivery_catalog()");
  // Production finished the 0038 upload-cleanup rollout, which lifts the
  // delivery write hold (its checkpoint guard only allows the rollout step).
  await db.exec(`SET session_replication_role = replica;
    UPDATE sessions.upload_cleanup_rollout SET phase = 5 WHERE singleton;
    SET session_replication_role = origin;`);
  await db.exec("SET search_path = public");
}, 120_000);
afterAll(async () => {
  await db?.close();
});

const ORG = "org-1";
const WS = "ws-1";
let revision = 0;

/** A delivery record row. `payload` null means a delete revision. */
const record = (
  origin: string,
  recordId: string,
  kind: string,
  sessionId: string,
  payload: Record<string, unknown> | null,
  workspace = WS,
) => {
  revision += 1;
  return `INSERT INTO sessions.delivery_records (org_id, workspace_id, origin_id, record_id, revision_id,
      revision, digest, kind, source, session_id, operation, payload, user_id)
    VALUES ('${ORG}', '${workspace}', '${origin}', '${recordId}', 'rev-${revision}', ${revision}, 'd-${revision}',
      '${kind}', 'claude', '${sessionId}', '${payload ? "upsert" : "delete"}',
      ${payload ? `'${JSON.stringify(payload)}'::jsonb` : "NULL"}, 'user-1')
    ON CONFLICT DO NOTHING`;
};
/** The backfilled acceptance order of a pre-T0 record: below every live (post-T0) seq. */
const acceptance = (
  origin: string,
  recordId: string,
  seq: number,
  workspace = WS,
) =>
  `INSERT INTO sessions.delivery_catalog_acceptance (org_id, workspace_id, origin_id, record_id, accepted_seq)
   VALUES ('${ORG}', '${workspace}', '${origin}', '${recordId}', ${seq}) ON CONFLICT DO NOTHING`;
const triggersOff = async (sql: string) => {
  await db.exec("SET session_replication_role = replica");
  try {
    await db.exec(sql);
  } finally {
    await db.exec("SET session_replication_role = origin");
  }
};

const catalog = async (session: string) =>
  (
    await db.query<{
      record_id: string;
      origin_id: string;
      title: string | null;
    }>(
      "SELECT record_id, origin_id, title FROM sessions.session_catalog WHERE session_id = $1",
      [session],
    )
  ).rows;
const agents = async (session: string) =>
  (
    await db.query<{ agent_name: string; record_id: string }>(
      "SELECT agent_name, record_id FROM sessions.session_catalog_agents WHERE session_id = $1",
      [session],
    )
  ).rows;
const snapshot = async () => {
  const out: Record<string, unknown[]> = {};
  for (const table of [
    "session_catalog",
    "session_relationships",
    "session_markers",
    "session_commit_links",
    "session_catalog_agents",
    "delivery_records",
    "delivery_catalog_acceptance",
  ])
    out[table] = (
      await db.query<{ row: unknown }>(
        `SELECT to_jsonb(t) AS row FROM sessions.${table} AS t ORDER BY to_jsonb(t)::text`,
      )
    ).rows.map((r) => r.row);
  return out;
};

it("sets up the T0 + backfill state the reprojection repairs", async () => {
  // Before T0, on Neon (not here): every record below marked "pre" was accepted.
  // After T0, live on Supabase, triggers on:
  //  (a) record a-r is deleted: its delete revision lands with no prior upsert.
  await db.exec(record("o-1", "a-r", "session", "s-a", null));
  //  (b) a newer record for s-b without an agent name wins the key.
  await db.exec(record("o-2", "b-post", "session", "s-b", { title: "post" }));
  expect(await catalog("s-b")).toMatchObject([{ record_id: "b-post" }]);
  expect(await agents("s-b")).toEqual([]);
  //  a live key in a second workspace, untouched by the backfill.
  await db.exec(
    record(
      "o-1",
      "w2-live",
      "session",
      "s-w2",
      { title: "w2", agent_name: "beta" },
      "ws-2",
    ),
  );

  // The backfill: base records and their acceptance, triggers off, existing rows kept.
  await triggersOff(`
    ${record("o-1", "a-r", "session", "s-a", { title: "pre", agent_name: "old" })};
    ${acceptance("o-1", "a-r", 10)};
    ${record("o-1", "b-pre", "session", "s-b", { title: "pre", agent_name: "alpha" })};
    ${acceptance("o-1", "b-pre", 11)};
    ${record("o-1", "c-pre", "session", "s-c", { title: "only pre", agent_name: "gamma", models_json: '["m"]' })};
    ${acceptance("o-1", "c-pre", 12)};
    ${record("o-1", "c-rel", "relationship", "s-c", { relationship_uid: "u-1", child_session_id: "s-c-child", relationship: "subagent", spawn_depth: 1 })};
    ${acceptance("o-1", "c-rel", 13)};
    ${record("o-1", "c-mark", "session_marker", "s-c", { marker_uid: "m-1", kind: "compaction", ts_ms: 1790000000000 })};
    ${acceptance("o-1", "c-mark", 14)};
    ${record("o-1", "c-commit", "commit_link", "s-c", { commit_sha: "abc123", match_method: "trailer", confidence: 0.5 })};
    ${acceptance("o-1", "c-commit", 15)};
  `);
  // The delete revision kept its key: the pre-T0 upsert did not replace it.
  expect(
    (
      await db.query(
        "SELECT operation FROM sessions.delivery_records WHERE record_id = 'a-r'",
      )
    ).rows,
  ).toEqual([{ operation: "delete" }]);

  // What a trigger-off copy of the derived tables would have resurrected.
  await triggersOff(`
    INSERT INTO sessions.session_catalog (org_id, workspace_id, source, session_id, record_id, origin_id,
        user_id, title, received_at)
      VALUES ('${ORG}', '${WS}', 'claude', 's-a', 'a-r', 'o-1', 'user-1', 'pre', now());
    INSERT INTO sessions.session_catalog_agents (org_id, workspace_id, source, session_id, agent_name, record_id)
      VALUES ('${ORG}', '${WS}', 'claude', 's-a', 'old', 'a-r'),
             ('${ORG}', '${WS}', 'claude', 's-b', 'alpha', 'b-pre'),
             ('${ORG}', '${WS}', 'claude', 's-orphan', 'ghost', 'gone');
    INSERT INTO sessions.session_relationships (org_id, workspace_id, source, parent_session_id,
        relationship_uid, record_id, origin_id, user_id, received_at)
      VALUES ('${ORG}', '${WS}', 'claude', 's-orphan', 'u-x', 'gone', 'o-1', 'user-1', now());
  `);
  expect(await catalog("s-a")).toHaveLength(1);
  expect(await agents("s-b")).toMatchObject([{ agent_name: "alpha" }]);
  expect(await catalog("s-c")).toEqual([]);
}, 60_000);

it("(e) a dry run reports the keys and writes nothing, even read-only", async () => {
  const before = await snapshot();
  await db.exec("SET default_transaction_read_only = on");
  try {
    const dry = await reprojectCatalog(db, { batch: 2, dryRun: true });
    // s-a, s-b, s-c, rel u-1, marker m-1, commit abc123, s-orphan, rel u-x (ws-1); s-w2 (ws-2).
    expect(dry).toMatchObject({
      keys: 9,
      keysWithoutContender: 3,
      batches: 5,
      after: null,
    });
    expect(dry.before.session_catalog_agents).toBe(4);
  } finally {
    await db.exec("SET default_transaction_read_only = off");
  }
  expect(await snapshot()).toEqual(before);
});

it("(a)(b)(c) reprojects every key: tombstones and lost agent names stay removed, pre-T0-only keys appear", async () => {
  const result = await reprojectCatalog(db, { batch: 2 });
  expect(result).toMatchObject({
    keys: 9,
    keysWithoutContender: 3,
    batches: 5,
    orphanAgentRowsRemoved: 1,
  });
  // Pages of 2 over 8 ws-1 keys and 1 ws-2 key: the 5th page holds ws-2 only.
  expect(result.workspaceLocks).toBe(5);

  // (a) the post-T0 tombstone wins: no catalog row, no agent name.
  expect(await catalog("s-a")).toEqual([]);
  expect(await agents("s-a")).toEqual([]);
  // (b) the post-T0 winner holds the key and its missing name removes the old one.
  expect(await catalog("s-b")).toMatchObject([
    { record_id: "b-post", origin_id: "o-2", title: "post" },
  ]);
  expect(await agents("s-b")).toEqual([]);
  // (c) the pre-T0-only key is projected, with its name and its other kinds.
  expect(await catalog("s-c")).toMatchObject([
    { record_id: "c-pre", title: "only pre" },
  ]);
  expect(await agents("s-c")).toMatchObject([
    { agent_name: "gamma", record_id: "c-pre" },
  ]);
  expect(
    (
      await db.query(
        "SELECT relationship_uid, child_session_id, spawn_depth FROM sessions.session_relationships WHERE parent_session_id = 's-c'",
      )
    ).rows,
  ).toEqual([
    { relationship_uid: "u-1", child_session_id: "s-c-child", spawn_depth: 1 },
  ]);
  expect(
    (
      await db.query(
        "SELECT marker_uid, marker_kind FROM sessions.session_markers WHERE session_id = 's-c'",
      )
    ).rows,
  ).toEqual([{ marker_uid: "m-1", marker_kind: "compaction" }]);
  expect(
    (
      await db.query(
        "SELECT commit_sha, match_method, confidence_basis_points FROM sessions.session_commit_links WHERE session_id = 's-c'",
      )
    ).rows,
  ).toEqual([
    {
      commit_sha: "abc123",
      match_method: "trailer",
      confidence_basis_points: 5000,
    },
  ]);
  // Orphaned derived rows with no contender are gone.
  expect(await agents("s-orphan")).toEqual([]);
  expect(
    (
      await db.query(
        "SELECT 1 FROM sessions.session_relationships WHERE parent_session_id = 's-orphan'",
      )
    ).rows,
  ).toEqual([]);
  // The live key in the other workspace is unchanged.
  expect(await catalog("s-w2")).toMatchObject([
    { record_id: "w2-live", title: "w2" },
  ]);
  expect(await agents("s-w2")).toMatchObject([{ agent_name: "beta" }]);
  expect(result.after).toEqual({
    session_catalog: 3,
    session_relationships: 1,
    session_markers: 1,
    session_commit_links: 1,
    session_catalog_agents: 2,
  });
}, 60_000);

it("(d) a second run changes nothing", async () => {
  const before = await snapshot();
  const again = await reprojectCatalog(db, { batch: 3 });
  expect(again).toMatchObject({
    keys: 6,
    keysWithoutContender: 0,
    orphanAgentRowsRemoved: 0,
    agentNameMismatches: 0,
  });
  expect(again.after).toEqual(again.before);
  expect(await snapshot()).toEqual(before);
}, 60_000);

it("counts agent names that got past the lookup trigger, NULL included", async () => {
  const { rows } = await db.query<{ n: number }>(
    "SELECT count(*)::int AS n FROM sessions.session_catalog_agents",
  );
  expect(rows[0]!.n).toBeGreaterThan(1);
  expect(await countAgentNameMismatches(db)).toBe(0);
  await db.exec(`SET session_replication_role = replica;
    UPDATE sessions.session_catalog_agents SET agent_name_normalized = 'bypassed'
     WHERE session_id = (SELECT min(session_id) FROM sessions.session_catalog_agents);
    UPDATE sessions.session_catalog_agents SET agent_name_normalized = NULL
     WHERE session_id = (SELECT max(session_id) FROM sessions.session_catalog_agents);
    SET session_replication_role = origin;`);
  expect(await countAgentNameMismatches(db)).toBe(2);
  await db.exec(
    "UPDATE sessions.session_catalog_agents SET agent_name = agent_name",
  );
  expect(await countAgentNameMismatches(db)).toBe(0);
});

it("leaves the workspace locks released and live delivery working afterwards", async () => {
  // A live upsert after reprojection still projects (the lock is free, triggers on).
  await db.exec(record("o-3", "c-newer", "session", "s-c", { title: "newer" }));
  expect(await catalog("s-c")).toMatchObject([
    { record_id: "c-newer", title: "newer" },
  ]);
  expect(await agents("s-c")).toEqual([]);
});
