import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { computeContentSha256, manifestKey, objectKey, objectStoreManifestReader, prefixAllowed, prefixOrgResolver, rawChunkKey, type ChunkEntry, type ManifestV1, type ObjectStore, type ManifestReader } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout } from "../pointer.ts";
import { memoryLock } from "./support.ts";

// Every Supabase and HoopDb migration in PGlite (the reviewer's repro on #121), so
// the revision fence and the directory projection are the real SQL.
const repo = join(import.meta.dir, "../../..");
const readF = (p: string) => readFileSync(join(repo, p), "utf8");
const AT = "2026-11-01T10:00:00.000Z";
let root: string; let store: ObjectStore; let reader: ManifestReader; let pg: PGlite;
beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp"); await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-pointer-race-")); store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readF("supabase/tests/supabase_shim.sql"));
  await pg.exec(readF("supabase/bootstrap/session_directory_search_worker.sql"));
  await pg.exec(readF("supabase/bootstrap/session_token_verifier.sql"));
  await pg.exec(readF("supabase/bootstrap/session_ingest_roles.sql"));
  for (const f of readdirSync(join(repo, "supabase/migrations")).sort()) await pg.exec(readF(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repo, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort()) await pg.exec(readF(`src/hoopdb/migrations/${f}`));
}, 120_000);
afterEach(async () => { await pg.close(); await rm(root, { recursive: true, force: true }); });

const loc = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
/** The same session after the exporter moved its start date (review #2 N1). */
const locB = { ...loc, startDate: "2026-10-31" };
/** A move further than the probe reaches (MOVE_PROBE_DAYS). */
const locFar = { ...loc, startDate: "2026-08-01" };
async function writeManifest(revision: number, at: typeof loc = loc) {
  const loc = at;
  const source = new TextEncoder().encode(JSON.stringify({ kind: "session_event", origin_id: "o", record_id: `r${revision}`, revision: "1", operation: "upsert", received_at: AT, payload: { ts_ms: 1, text: "x" } }) + "\n");
  const stored = new Uint8Array(zstdCompressSync(source)); const sha = await sha256Hex(source);
  const chunk: ChunkEntry = { seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: 1 };
  await store.put(rawChunkKey(loc, chunk.seq, 0, sha, "ndjson"), stored, {});
  const manifest = { schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision, created_at: AT, updated_at: AT,
    session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "claude", session_id: "sess_1", started_at: `${loc.startDate}T10:00:00.000Z` },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "t" },
    raw: { owner: "srv_bb", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1, raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0, epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: "srv_bb" }], next_offset: source.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 } },
    extensions: { "96bulls": { user_id: "user_1" } } } as unknown as ManifestV1;
  const key = manifestKey(loc);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}
const body = (key: string, name: string, seq: string) => JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: name, eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: seq } } }] });

const lock = memoryLock();
function deps(gate?: () => Promise<void>, afterRead?: () => Promise<void>): ConsumerDeps {
  const db = { pool: undefined, end: async () => {}, async query(text: string, params?: unknown[]) {
    if (gate && text.includes("upsert_session_index")) await gate();
    const r = await pg.query(text, params as unknown[]); return { rows: r.rows };
  } } as unknown as HoopDb;
  const b = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
  // `afterRead` pauses once, right after this Lambda's first manifest read.
  let paused = false;
  const gatedReader: ManifestReader = afterRead
    ? { ...reader, read: async (k) => { const r = await reader.read(k); if (!paused) { paused = true; await afterRead(); } return r; } }
    : reader;
  return { pointer: { db, lock, reader: gatedReader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (p, o) => prefixAllowed(b, p, o), fanout: droppedFanout } };
}
const row = async () => (await pg.query<{ revision: string; deleted_at: string | null; manifest_key: string }>("SELECT revision::text, deleted_at, manifest_key FROM sessions.session_index WHERE session_id='sess_1'")).rows[0];
const failures = async (id: string, b: string, d = deps()) => (await processBatch([{ messageId: id, body: b }], d)).batchItemFailures.map((f) => f.itemIdentifier);

test("sequential: put rev1, put rev2, erase -> tombstoned", async () => {
  const key = await writeManifest(1);
  expect((await processBatch([{ messageId: "a", body: body(key, "ObjectCreated:Put", "01") }], deps())).batchItemFailures).toEqual([]);
  expect((await row())!.deleted_at).toBeNull();
  await store.delete(objectKey(key));
  await processBatch([{ messageId: "d", body: body(key, "ObjectRemoved:DeleteMarkerCreated", "03") }], deps());
  expect((await row())!.deleted_at).not.toBeNull();
}, 60_000);

test("a put in flight when the manifest is erased cannot bring the pointer back (review H1, real SQL)", async () => {
  let key = await writeManifest(1);
  await processBatch([{ messageId: "a", body: body(key, "ObjectCreated:Put", "01") }], deps());
  key = await writeManifest(2); // the live writer's revision 2
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  let entered!: () => void;
  const atUpsert = new Promise<void>((r) => (entered = r));
  // Lambda A: the rev-2 put, paused just before its upsert (after its S3 read).
  const a = processBatch([{ messageId: "p2", body: body(key, "ObjectCreated:Put", "02") }], deps(async () => { entered(); await held; }));
  await atUpsert;
  // The erasure job deletes the manifest; Lambda B handles the delete. It waits for A's session lock.
  await store.delete(objectKey(key));
  const b = processBatch([{ messageId: "d", body: body(key, "ObjectRemoved:DeleteMarkerCreated", "03") }], deps());
  // Let B run as far as it can while A is paused: to its tombstone without the
  // session lock (the bug), or until it blocks on A's lock (the fix).
  await Promise.race([b, new Promise((r) => setTimeout(r, 2_000))]);
  release();
  expect((await a).batchItemFailures).toEqual([]);
  expect((await b).batchItemFailures).toEqual([]);
  const final = await row();
  expect(final!.deleted_at).not.toBeNull();
  const directory = (await pg.query<{ deleted_at: string | null }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess_1'")).rows;
  expect(directory.every((d) => d.deleted_at !== null)).toBe(true);
}, 60_000);

test("the S3 read itself is under the lock: a delete cannot slip between a put's read and its upsert (review #2 L-b)", async () => {
  let key = await writeManifest(1);
  await processBatch([{ messageId: "a", body: body(key, "ObjectCreated:Put", "01") }], deps());
  key = await writeManifest(2);
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  let entered!: () => void;
  const afterRead = new Promise<void>((r) => (entered = r));
  // Lambda A pauses right after reading the rev-2 manifest, before anything else.
  const a = processBatch([{ messageId: "p2", body: body(key, "ObjectCreated:Put", "02") }], deps(undefined, async () => { entered(); await held; }));
  await afterRead;
  await store.delete(objectKey(key));
  const b = processBatch([{ messageId: "d", body: body(key, "ObjectRemoved:DeleteMarkerCreated", "03") }], deps());
  await Promise.race([b, new Promise((r) => setTimeout(r, 2_000))]);
  release();
  await a;
  await b;
  expect((await row())!.deleted_at).not.toBeNull();
}, 60_000);

describe("an exporter date move (review #2 N1, real SQL)", () => {
  async function atRevision3() {
    const a = await writeManifest(1);
    await processBatch([{ messageId: "1", body: body(a, "ObjectCreated:Put", "01") }], deps());
    await writeManifest(2);
    await processBatch([{ messageId: "2", body: body(a, "ObjectCreated:Put", "02") }], deps());
    await writeManifest(3);
    await processBatch([{ messageId: "3", body: body(a, "ObjectCreated:Put", "03") }], deps());
    return a;
  }

  const directoryLive = async () =>
    (await pg.query<{ deleted_at: string | null }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess_1'")).rows.some((d) => d.deleted_at === null);

  test("new key at rev 1 while the old one is live, then the old prefix erased: the session ends live at the new key", async () => {
    const a = await atRevision3();
    const b = await writeManifest(1, locB); // exporter: (prior at the new key ?? 0) + 1
    // Both keys live: the move is in progress, so the put retries (and never lands on the old row).
    expect(await failures("4", body(b, "ObjectCreated:Put", "04"))).toEqual(["4"]);
    expect((await row())!.manifest_key).toBe(a);
    await store.delete(objectKey(a));
    // The old key's delete finds the session under its new date and follows it.
    expect(await failures("5", body(a, "ObjectRemoved:DeleteMarkerCreated", "05"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b, revision: "1" });
    expect(await directoryLive()).toBe(true);
    // The retried put, and any reconcile re-enqueue, change nothing.
    expect(await failures("6", body(b, "ObjectCreated:Put", "06"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b });
    expect(await directoryLive()).toBe(true);
  }, 60_000);

  test("the old key's delete handled before the new key's put: the session ends live at the new key", async () => {
    const a = await atRevision3();
    const b = await writeManifest(1, locB);
    await store.delete(objectKey(a));
    expect(await failures("4", body(a, "ObjectRemoved:DeleteMarkerCreated", "04"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b });
    expect(await failures("5", body(b, "ObjectCreated:Put", "05"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: b });
    expect(await directoryLive()).toBe(true);
  }, 60_000);

  test("deleting the old key never tombstones a row that already names the new key", async () => {
    const a = await atRevision3();
    const far = await writeManifest(4, locFar); // carried revision, beyond the probe's reach
    expect(await failures("4", body(far, "ObjectCreated:Put", "04"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: far, revision: "4" });
    await store.delete(objectKey(a));
    // The old key's delete arrives late: the tombstone is fenced on the key the row names.
    expect(await failures("5", body(a, "ObjectRemoved:DeleteMarkerCreated", "05"))).toEqual([]);
    expect(await row()).toMatchObject({ deleted_at: null, manifest_key: far });
    expect(await directoryLive()).toBe(true);
  }, 60_000);
});
