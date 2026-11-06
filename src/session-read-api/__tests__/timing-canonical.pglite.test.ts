/**
 * Session timing end to end without Neon: pointer rows read AS bulls96_read_api
 * (every Supabase migration + the role bootstrap, tenant-bound), the canonical
 * S3 manifest and chunks they point at, the shared delivery projection, then
 * the timing port. The same projected rows are written into relayhistory's
 * table shapes and timed by relayhistory's own SQL (the vendored oracle): the
 * payloads must be identical.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { computeContentSha256, manifestKey, objectKey, rawChunkKey, type ChunkEntry, type ManifestV1 } from "../../../packages/object-store/src/index.ts";
import { canonicalSnapshot, type CanonicalDeps, type CanonicalIdentity } from "../canonical.ts";
import { poolTenantRunner } from "../liveness.ts";
import { sqlCanonicalPointers, timingFromCanonical } from "../session-timing-source.ts";
import { TimingInputError } from "../timing.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const psql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const AT = Date.parse("2026-11-01T10:00:00.000Z");
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const identity: CanonicalIdentity = { orgId: "org_1", workspaceId: "ws_1", source: "codex", sessionId: "sess_1" };
const location = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "codex", sessionId: "sess_1", startDate: "2026-11-01" };

/** A delivery stream: prompt, reply, two tools (one a wait), a lifecycle, a marker without a turn. */
function deliveryLines() {
  const line = (record_id: string, kind: string, payload: Record<string, unknown>, at = AT) => ({
    kind, origin_id: "origin-1", record_id, revision: "1", operation: "upsert", received_at: new Date(at).toISOString(), payload,
  });
  return [
    line("u1", "session_event", { ts_ms: AT + 1_000, role: "user", text: "please fix it", message_id: "m-u1" }),
    line("p1", "session_event", { ts_ms: AT + 2_000, role: "assistant", text: "", tool_use_id: "t1", message_id: "m-a1" }),
    line("t1", "tool_call", { ts_ms: AT + 2_100, name: "exec_command", tool_use_id: "t1", args_json: "{\"cmd\":\"ls\"}" }),
    line("t2", "tool_call", { ts_ms: AT + 5_000, name: "request_user_input", tool_use_id: "t2" }),
    line("a2", "session_event", { ts_ms: AT + 9_000, role: "assistant", text: "done", message_id: "m-a2" }),
    line("k1", "session_marker", { marker_uid: "mk1", kind: "task_started", turn_id: "turn-1", ts_ms: AT + 900 }),
    line("k2", "session_marker", { marker_uid: "mk2", kind: "task_complete", turn_id: "turn-1", ts_ms: AT + 9_500 }),
    line("k3", "session_marker", { marker_uid: "mk3", kind: "task_started", ts_ms: AT + 3_000 }),
  ];
}

let dir: string;
let deps: CanonicalDeps;
let manifest: ManifestV1;
async function writeSession(lines: unknown[]) {
  dir = await mkdtemp(join(tmpdir(), "timing-canonical-"));
  const store = createFsStore({ root: dir });
  const raw = new TextEncoder().encode(lines.map((r) => JSON.stringify(r)).join("\n") + "\n");
  const stored = new Uint8Array(zstdCompressSync(raw));
  const sha = await sha256Hex(raw);
  const chunk: ChunkEntry = {
    seq: 0, epoch: 0, start_offset: 0, bytes: raw.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored), stored_raw_bytes: raw.byteLength, raw_sha256: sha, compression: "zstd", records: lines.length,
  };
  const iso = new Date(AT).toISOString();
  manifest = {
    schema_v: 1, layout: "v1", trace_id: TRACE, revision: 1, created_at: iso, updated_at: iso,
    session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "codex", session_id: "sess_1", started_at: iso },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
    raw: {
      owner: "srv_bb", state: "open", updated_at: iso, format: "ndjson", compression: "zstd", chunk_count: 1,
      raw_bytes: raw.byteLength, stored_bytes: stored.byteLength, content_sha256: await computeContentSha256([chunk]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: iso, reason: "initial", first_seq: 0, opened_by: "srv_bb" }],
      next_offset: raw.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1", canonical_session_id: "sess_1" } },
  } as ManifestV1;
  await store.put(rawChunkKey(location, 0, 0, sha, "ndjson"), stored, {});
  await store.put(objectKey(manifestKey(location)), new TextEncoder().encode(JSON.stringify(manifest)), {});
  deps = {
    store, provider: "s3", bucket: "bucket",
    prefix: { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" },
    pointers: async () => [],
    decompress: (b) => new Uint8Array(zstdDecompressSync(b)),
  };
}
afterEach(async () => {
  if (dir) await rm(dir, { recursive: true, force: true });
});

// The Supabase side: every migration, the read role, a pointer row for the manifest.
let supa: PGlite;
beforeAll(async () => {
  supa = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await supa.exec(read("supabase/tests/supabase_shim.sql"));
  await supa.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await supa.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await supa.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await supa.exec(read(`supabase/migrations/${file}`));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role_verify.sql"));
}, 120_000);
afterAll(async () => {
  await supa?.close();
});
const asRole = () => ({
  async connect() {
    await supa.exec("SET ROLE bulls96_read_api");
    return {
      query: (text: string, params?: unknown[]) => supa.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>,
      release: () => void supa.exec("RESET ROLE"),
    };
  },
});
async function pointerRow(over: { org?: string; ws?: string; session?: string; machine?: string; deleted?: boolean; canonical?: string } = {}) {
  await supa.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, canonical_session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
     VALUES ($1, $2, 'codex', $3, $4, $5, 'user_1', $6::timestamptz, $6::timestamptz, 's3', 'bucket', $7, decode($8, 'hex'), 'ndjson', $9, 1,
             CASE WHEN $10 THEN now() END)`,
    [over.org ?? "org_1", over.ws ?? "ws_1", over.session ?? "sess_1", over.canonical ?? null, over.machine ?? "srv_bb", new Date(AT).toISOString(),
      manifestKey(location), manifest.raw.content_sha256, TRACE, over.deleted ?? false],
  );
}

// relayhistory's side: its table shapes and its own timing SQL.
let neon: PGlite;
beforeAll(async () => {
  neon = new PGlite();
  await neon.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.convergence_events (org_id text, workspace_id text, user_id text, machine_id text, source text, session_id text,
      event_id text, kind text, type text, ts timestamptz, actor_role text, tool_name text, duration_ms integer, record jsonb, tool_calls jsonb);
    CREATE TABLE sessions.conversation_turns (org_id text, workspace_id text, source text, session_id text, ts timestamptz, role text);
    CREATE TABLE sessions.session_markers (org_id text, workspace_id text, source text, session_id text, origin_id text, user_id text,
      turn_id text, marker_kind text, ts timestamptz);`);
  await neon.exec(readFileSync(join(import.meta.dir, "fixtures/relayhistory-session-timing-compute.sql"), "utf8"));
}, 60_000);
afterAll(async () => {
  await neon?.close();
});

/** What Neon would hold for these projected rows (markers stamped with their delivery's user), timed by relayhistory. */
async function relayhistoryTiming(rows: Array<{ table: string; row: Record<string, unknown> }>, deliveryUser: string) {
  await neon.exec("TRUNCATE sessions.convergence_events, sessions.conversation_turns, sessions.session_markers");
  for (const { table, row: r } of rows) {
    if (table === "convergence_events")
      await neon.query(
        `INSERT INTO sessions.convergence_events VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::timestamptz,$11,$12,$13,$14::jsonb,$15::jsonb)`,
        [identity.orgId, identity.workspaceId, r.user_id, r.machine_id, r.source, r.session_id, r.event_id, r.kind, r.type, r.ts, r.actor_role, r.tool_name, r.duration_ms, JSON.stringify(r.record), JSON.stringify(r.tool_calls)],
      );
    else if (table === "session_markers")
      await neon.query(`INSERT INTO sessions.session_markers VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::timestamptz)`, [identity.orgId, identity.workspaceId, r.source, r.session_id, r.origin_id, deliveryUser, r.turn_id, r.marker_kind, r.ts]);
  }
  const res = await neon.query<{ t: unknown }>("SELECT sessions.session_timing_compute($1,$2,$3,$4) AS t", [identity.orgId, identity.sessionId, identity.source, identity.workspaceId]);
  return res.rows[0]!.t;
}

describe("session timing from canonical S3, matching relayhistory", () => {
  test("pointers through the read role, then the projection, equal relayhistory's SQL over the same rows", async () => {
    await writeSession(deliveryLines());
    await supa.exec("TRUNCATE sessions.session_index CASCADE");
    await pointerRow();
    await pointerRow({ org: "org_2", machine: "foreign" });
    await pointerRow({ ws: "ws_2", machine: "other-ws" });
    const source = sqlCanonicalPointers(poolTenantRunner(asRole()));
    const pointers = await source.pointers(identity);
    expect(pointers).toHaveLength(1);
    expect(pointers[0]).toMatchObject({ org_id: "org_1", workspace_id: "ws_1", harness: "codex", session_id: "sess_1", manifest_key: manifestKey(location), deleted_at: null });
    // pg (the Worker) returns bigint as text, PGlite as a number; the canonical reader takes either.
    expect(String(pointers[0]!.revision)).toBe("1");
    expect(pointers[0]!.content_sha256).toBe(`\\x${manifest.raw.content_sha256}`);
    expect(await source.state(identity)).toEqual({ pointers: 1, live: 1 });

    const snapshot = await canonicalSnapshot({ ...deps, pointers: source.pointers }, identity);
    const timing = timingFromCanonical(identity, snapshot, { assumeNoTurns: true });
    expect(timing.coverage.eventCount).toBeGreaterThan(3);
    expect(timing.coverage.markerCount).toBe(3);
    expect(timing.coverage.lifecycleIntervalCount).toBe(1);
    expect(timing.workMs).not.toBeNull();
    expect(JSON.parse(JSON.stringify(timing))).toEqual((await relayhistoryTiming(snapshot.rows, "user_1")) as never);
  }, 60_000);

  test("without legacy turn evidence the session is refused unless the operator assumes no turns", async () => {
    await writeSession(deliveryLines());
    const snapshot = await canonicalSnapshot({ ...deps, pointers: async () => [{
      org_id: "org_1", workspace_id: "ws_1", harness: "codex", session_id: "sess_1", machine_id: "srv_bb", user_id: "user_1",
      storage_provider: "s3", bucket: "bucket", manifest_key: manifestKey(location), content_sha256: `\\x${manifest.raw.content_sha256}`,
      raw_format: "ndjson", revision: 1, deleted_at: null,
    }] }, identity);
    expect(snapshot.legacyCoverage).toBeNull();
    expect(() => timingFromCanonical(identity, snapshot, { assumeNoTurns: false })).toThrow(new TimingInputError("turns_unknown"));
  });

  test("tombstoned pointers are erased; another tenant's pointer rows are never read", async () => {
    await writeSession(deliveryLines());
    await supa.exec("TRUNCATE sessions.session_index CASCADE");
    await pointerRow({ deleted: true });
    await pointerRow({ org: "org_2" });
    const source = sqlCanonicalPointers(poolTenantRunner(asRole()));
    expect(await source.pointers(identity)).toEqual([]);
    expect(await source.state(identity)).toEqual({ pointers: 1, live: 0 });
    expect(await source.state({ ...identity, sessionId: "absent" })).toEqual({ pointers: 0, live: 0 });
  }, 60_000);

  test("a canonical session id is found by either id, as session_directory_pointers finds it", async () => {
    await writeSession(deliveryLines());
    await supa.exec("TRUNCATE sessions.session_index CASCADE");
    await pointerRow({ session: "raw-id", canonical: "sess_1" });
    const source = sqlCanonicalPointers(poolTenantRunner(asRole()));
    expect((await source.pointers(identity)).map((p) => p.session_id)).toEqual(["sess_1"]);
    expect((await source.pointers({ ...identity, sessionId: "raw-id" })).map((p) => p.session_id)).toEqual(["sess_1"]);
  }, 60_000);

  test("the widened grant is still pointer-only: no title, no content columns", async () => {
    await supa.exec("SET ROLE bulls96_read_api");
    try {
      await supa.exec("BEGIN READ ONLY");
      await expect(supa.query("SELECT title FROM sessions.session_index")).rejects.toThrow(/permission denied/);
      await supa.exec("ROLLBACK");
      await supa.exec("BEGIN READ ONLY");
      await expect(supa.query("SELECT model FROM sessions.session_index")).rejects.toThrow(/permission denied/);
      await supa.exec("ROLLBACK");
    } finally {
      await supa.exec("RESET ROLE");
    }
  });
});
