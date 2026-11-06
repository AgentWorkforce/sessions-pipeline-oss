import { afterAll, beforeAll, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  objectKey,
  parseManifest,
  readAll,
} from "../../../packages/object-store/src/index.ts";
import {
  withLegacySource,
  legacyActivityPage,
  includeLegacyUnits,
} from "../neon-s3-sync/legacy-source.ts";
import { Syncer, DEFAULT_SYNC_CONFIG } from "../neon-s3-sync/sync.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { Throttle } from "../neon-s3-sync/throttle.ts";
import { canonicalLegacy } from "../../session-read-api/canonical-legacy.ts";
import { groupExistsInNeon } from "../neon-s3-sync/neon-reader.ts";
import { exportUnit } from "../neon-s3-sync/export-session.ts";
import {
  canonicalSnapshot,
  type CanonicalPointer,
} from "../../session-read-api/canonical.ts";
import type { Queryable } from "../relayhistory-export.ts";
const pg = new PGlite(),
  db = pg as unknown as Queryable;
const turnId = "11111111-1111-4111-8111-111111111111";
let root: string;
beforeAll(async () => {
  root = await mkdtemp(join(tmpdir(), "canonical-legacy-"));
  await pg.exec(`CREATE SCHEMA sessions;
 CREATE TABLE sessions.session_rollups(org_id text,session_id text);CREATE TABLE sessions.session_catalog(org_id text,session_id text);
 CREATE TABLE sessions.convergence_events(org_id text,workspace_id text,source text,session_id text);
 CREATE TABLE sessions.conversation_turns(id uuid PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,session_owner text,turn_index int,role text,content text,actor_name text,actor_role text,metadata jsonb,ts timestamptz);
 CREATE TABLE sessions.session_links(id bigint PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,link_kind text,link_ref text,link_url text,metadata jsonb,confidence_basis_points int,link_ts timestamptz,updated_at timestamptz,created_at timestamptz);
 CREATE TABLE sessions.session_outcomes(org_id text,workspace_id text,source text,session_id text,commit_sha text,user_id text,reverted boolean,shipped_at timestamptz,ingested_at timestamptz,created_at timestamptz);`);
  await pg.query(
    `INSERT INTO sessions.conversation_turns VALUES($1,'org_1','ws_1','owner','sess:1','owner',3,'assistant','preserved legacy transcript','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00.123456Z')`,
    [turnId],
  );
});
afterAll(async () => {
  await pg.close();
  await rm(root, { recursive: true, force: true });
});
test("turn-only discovery and unit scope do not depend on catalog or rollups", async () => {
  const rows = await legacyActivityPage(db, null, 1);
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({
    org_id: "org_1",
    workspace_id: "ws_1",
    source: "claude",
    session_id: "sess:1",
  });
  expect(await legacyActivityPage(db, rows[0]!, 1)).toEqual([]);
  const units = await includeLegacyUnits(db, "org_1", "sess:1", []);
  expect(units).toHaveLength(1);
  expect(units[0]).toMatchObject({
    workspaceId: "ws_1",
    source: "claude",
    sessionId: "sess:1",
  });
  expect(await includeLegacyUnits(db, "other-org", "sess:1", [])).toEqual([]);
});
test("exports and reads actual legacy row plus explicit empty-table coverage through canonical objects", async () => {
  const unit = (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!;
  const store = createFsStore({ root });
  const source = withLegacySource(
    db,
    { fenced: async () => false, async *lines() {} },
    async () => {},
    1,
  );
  const result = await exportUnit(
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    unit,
    source,
  );
  expect(result.lines).toBe(2);
  expect(result.manifests).toHaveLength(1);
  const key = result.manifests[0]!.manifestKey;
  const manifest = await parseManifest(
    new TextDecoder().decode(await readAll(await store.get(objectKey(key)))),
    key,
  );
  const pointer: CanonicalPointer = {
    org_id: "org_1",
    workspace_id: "ws_1",
    harness: "claude",
    session_id: "sess:1",
    machine_id: "legacy:records",
    user_id: "unknown",
    storage_provider: "s3",
    bucket: "bucket",
    manifest_key: key,
    content_sha256: manifest.raw.content_sha256,
    raw_format: "ndjson",
    revision: manifest.revision,
    deleted_at: null,
  };
  const snapshot = await canonicalSnapshot(
    {
      store,
      provider: "s3",
      bucket: "bucket",
      prefix: {
        PIPELINE_KEY_PREFIX: "96bulls",
        PIPELINE_ORG_FROM_PREFIX: "true",
      },
      pointers: async () => [pointer],
      decompress: (b) => new Uint8Array(zstdDecompressSync(b)),
    },
    {
      orgId: "org_1",
      workspaceId: "ws_1",
      source: "claude",
      sessionId: "sess:1",
    },
  );
  expect(snapshot.legacyCoverage).toEqual({
    version: 1,
    tables: ["conversation_turns", "session_links", "session_outcomes"],
  });
  expect(snapshot.rows).toHaveLength(1);
  expect(snapshot.rows[0]).toMatchObject({
    table: "conversation_turns",
    row: {
      id: turnId,
      legacy_stored_source: "owner",
      content: "preserved legacy transcript",
      turn_index: 3,
      metadata: { nativeCli: "claude" },
      ts: "2026-11-01T10:00:00.123456+00:00",
    },
  });
});

test("erasure guard ignores a legacy-only group unless legacy recognition is switched on", async () => {
  expect(await groupExistsInNeon(db, "org_1", "sess:1")).toBe(false);
  expect(await groupExistsInNeon(db, "org_1", "sess:1", false)).toBe(false);
});

test("erasure guard retains a legacy-only group with legacy recognition on, even with no catalog or rollup", async () => {
  expect(await groupExistsInNeon(db, "org_1", "sess:1", true)).toBe(true);
  expect(await groupExistsInNeon(db, "other-org", "sess:1", true)).toBe(false);
});

test("nonempty links and outcomes preserve exact source IDs, timestamp and metadata", async () => {
  await pg.exec(`INSERT INTO sessions.session_links VALUES(9007199254740993,'org_1','ws_1','claude','sess:1','commit','ref','url','{"kept":true}',5000,'2026-11-01T10:00:00.123456Z','2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');
 INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','abcdef','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`);
  const unit = (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!;
  const source = withLegacySource(
    db,
    { fenced: async () => false, async *lines() {} },
    async () => {},
    1,
  );
  const records = [];
  for await (const line of source.lines(unit))
    records.push(JSON.parse(new TextDecoder().decode(line.bytes)));
  const result = canonicalLegacy(records, {
    orgId: "org_1",
    workspaceId: "ws_1",
    source: "claude",
    sessionId: "sess:1",
  });
  expect(result.rows.find((r) => r.table === "session_links")).toMatchObject({
    row: {
      id: "9007199254740993",
      metadata: { kept: true },
      link_ts: "2026-11-01T10:00:00.123456+00:00",
    },
  });
  expect(result.rows.find((r) => r.table === "session_outcomes")).toMatchObject(
    {
      row: {
        commit_sha: "abcdef",
        user_id: "user_1",
        reverted: false,
        shipped_at: null,
      },
    },
  );
  expect(result.coverage).not.toBeNull();
});
test("opt-in sync discovers and exports a legacy-only group; default source stays inert", async () => {
  const fake: Queryable = {
    async query<T>(sql: string, params?: unknown[]) {
      if (sql.includes("pg_locks"))
        return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      if (
        sql.includes("FROM sessions.session_rollups") ||
        sql.includes("FROM sessions.session_catalog")
      )
        return { rows: [] as T[] };
      return db.query<T>(sql, params);
    },
  };
  const state = new SyncState(join(root, "sync.sqlite"));
  const store = createFsStore({ root: join(root, "sync-objects") });
  const throttle = new Throttle({
    rowsPerSec: 1e9,
    bytesPerSec: 1e12,
    slowWindows: [],
    slowFactor: 1,
  });
  const base = () => ({ fenced: async () => false, async *lines() {} });
  const config = { ...DEFAULT_SYNC_CONFIG, includeLegacySources: true };
  const syncer = new Syncer(
    fake,
    state,
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    throttle,
    () => {},
    config,
    () => false,
    base,
  );
  await syncer.sweepActivity();
  const work = state.nextWork(Date.now(), config.recentMs, 10)[0]!;
  expect(work).toBeTruthy();
  expect(await syncer.processGroup(work)).toBe(true);
  expect(state.exported("org_1", "sess:1")?.keys.length).toBe(1);
  const dormant = new Syncer(
    fake,
    state,
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    throttle,
    () => {},
    DEFAULT_SYNC_CONFIG,
    () => false,
    base,
  );
  const lines = [];
  for await (const line of dormant.source.lines(
    (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!,
  ))
    lines.push(line);
  expect(lines).toEqual([]);
  state.close();
});

test("legal source IDs cannot collide with the completeness marker across origins", async () => {
  await pg.exec(
    `INSERT INTO sessions.session_outcomes VALUES('org_1','ws_1','claude','sess:1','coverage','user_1',false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z');`,
  );
  const unit = (await includeLegacyUnits(db, "org_1", "sess:1", []))[0]!;
  const store = createFsStore({ root: join(root, "collision-objects") });
  const source = withLegacySource(
    db,
    {
      fenced: async () => false,
      async *lines() {
        // A delivery ID can legally equal even the new reserved-looking marker ID.
        yield {
          machine: "legacy:records",
          userId: "unknown",
          bytes: new TextEncoder().encode(
            JSON.stringify({
              kind: "session_event",
              origin_id: "legacy:scope",
              record_id: "legacy:scope:coverage",
              revision: "1",
              operation: "upsert",
              received_at: "2026-11-01T10:00:00Z",
              payload: {
                ts_ms: 1793527200000,
                text: "independent delivery",
                role: "assistant",
              },
            }) + "\n",
          ),
        };
      },
    },
    async () => {},
    1,
  );
  const result = await exportUnit(
    { store, keyRoot: "96bulls", grantId: "grant_1" },
    unit,
    source,
  );
  const key = result.manifests[0]!.manifestKey;
  const manifest = await parseManifest(
    new TextDecoder().decode(await readAll(await store.get(objectKey(key)))),
    key,
  );
  const pointer: CanonicalPointer = {
    org_id: "org_1",
    workspace_id: "ws_1",
    harness: "claude",
    session_id: "sess:1",
    machine_id: "legacy:records",
    user_id: "unknown",
    storage_provider: "s3",
    bucket: "bucket",
    manifest_key: key,
    content_sha256: manifest.raw.content_sha256,
    raw_format: "ndjson",
    revision: manifest.revision,
    deleted_at: null,
  };
  const snapshot = await canonicalSnapshot(
    {
      store,
      provider: "s3",
      bucket: "bucket",
      prefix: {
        PIPELINE_KEY_PREFIX: "96bulls",
        PIPELINE_ORG_FROM_PREFIX: "true",
      },
      pointers: async () => [pointer],
      decompress: (b) => new Uint8Array(zstdDecompressSync(b)),
    },
    {
      orgId: "org_1",
      workspaceId: "ws_1",
      source: "claude",
      sessionId: "sess:1",
    },
  );
  expect(snapshot.legacyCoverage).not.toBeNull();
  expect(
    snapshot.rows.find((r) => r.table === "convergence_events")?.row.content,
  ).toBe("independent delivery");
  expect(
    snapshot.rows.find(
      (r) => r.table === "session_outcomes" && r.row.commit_sha === "coverage",
    ),
  ).toBeTruthy();
  expect(
    snapshot.rows.find((r) => r.table === "conversation_turns")?.row.id,
  ).toBe(turnId);
});

test("metadata overrides a different stored provider while preserving raw source and all public turn fields", async () => {
  const id = "22222222-2222-4222-8222-222222222222";
  try {
    await pg.query(
      `INSERT INTO sessions.conversation_turns VALUES($1,'org_attr','ws_attr','codex','same-id','alice',5,'assistant','exact content','Agent','owner','{"nativeCli":"claude","kept":{"tool":true}}','2026-11-01T10:00:00.123456Z')`,
      [id],
    );
    const units = await includeLegacyUnits(db, "org_attr", "same-id", []);
    expect(units.map((u) => u.source)).toEqual(["claude"]);
    const source = withLegacySource(
        db,
        { fenced: async () => false, async *lines() {} },
        async () => {},
      ),
      records = [];
    for await (const line of source.lines(units[0]!))
      records.push(JSON.parse(new TextDecoder().decode(line.bytes)));
    const result = canonicalLegacy(records, {
      orgId: "org_attr",
      workspaceId: "ws_attr",
      source: "claude",
      sessionId: "same-id",
    });
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]!.row).toMatchObject({
      id,
      source: "claude",
      legacy_stored_source: "codex",
      session_owner: "alice",
      turn_index: 5,
      content: "exact content",
      actor_name: "Agent",
      actor_role: "owner",
      metadata: { nativeCli: "claude", kept: { tool: true } },
      ts: "2026-11-01T10:00:00.123456+00:00",
    });
  } finally {
    await pg.query(
      "DELETE FROM sessions.conversation_turns WHERE org_id='org_attr'",
    );
  }
});
test("raw source equal to owner uses only a unique event provider in that workspace", async () => {
  try {
    await pg.exec(`INSERT INTO sessions.conversation_turns VALUES('33333333-3333-4333-8333-333333333333','org_attr','ws_attr','alice','same-id','alice',0,'user','hello','User','owner','{}','2026-11-01T10:00:00Z');
 INSERT INTO sessions.convergence_events VALUES('org_attr','ws_attr','claude','same-id'),('org_attr','other-ws','codex','same-id'),('foreign','ws_attr','codex','same-id');`);
    const units = await includeLegacyUnits(db, "org_attr", "same-id", []);
    expect(units.map((u) => [u.workspaceId, u.source])).toEqual([
      ["ws_attr", "claude"],
    ]);
    const records = [],
      source = withLegacySource(
        db,
        { fenced: async () => false, async *lines() {} },
        async () => {},
      );
    for await (const line of source.lines(units[0]!))
      records.push(JSON.parse(new TextDecoder().decode(line.bytes)));
    expect(
      canonicalLegacy(records, {
        orgId: "org_attr",
        workspaceId: "ws_attr",
        source: "claude",
        sessionId: "same-id",
      }).rows[0]!.row,
    ).toMatchObject({ source: "claude", legacy_stored_source: "alice" });
  } finally {
    await pg.exec(
      "DELETE FROM sessions.conversation_turns WHERE org_id='org_attr';DELETE FROM sessions.convergence_events WHERE org_id IN('org_attr','foreign')",
    );
  }
});
test("truly unbound turn-only coverage refuses discovery and export admission without inventing a provider", async () => {
  try {
    await pg.exec(
      `INSERT INTO sessions.conversation_turns VALUES('44444444-4444-4444-8444-444444444444','org_attr','ws_attr','alice','same-id','alice',0,'user','hello','User','owner','{}','2026-11-01T10:00:00Z');`,
    );
    await expect(
      includeLegacyUnits(db, "org_attr", "same-id", []),
    ).rejects.toThrow("legacy_source_unbound");
    await expect(legacyActivityPage(db, null, 5000)).rejects.toThrow(
      "legacy_source_unbound",
    );
  } finally {
    await pg.query(
      "DELETE FROM sessions.conversation_turns WHERE org_id='org_attr'",
    );
  }
});
