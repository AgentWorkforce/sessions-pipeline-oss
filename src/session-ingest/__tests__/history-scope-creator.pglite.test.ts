import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

// Minimal SQL-only mask builder (history-mask-v1): leaves sorted by sha256(recordId); a 16-way
// root node above 500 records. Mirrors ingest_history_mask_register's own validation.
import { createHash } from "node:crypto";
const shaHex = (s: string) => createHash("sha256").update(s).digest("hex");
async function registerMask(reg: (doc: unknown) => Promise<any>, orgId: string, workspaceId: string, recordIds: string[]) {
  const sorted = [...recordIds].sort((a, b) => (shaHex(a) < shaHex(b) ? -1 : 1));
  const leaf = (prefix: string, ids: string[]) => reg({ protocol: "history-mask-v1", orgId, workspaceId, prefix, kind: "leaf", count: String(ids.length), items: ids.map((recordId) => ({ recordId, winner: null })) });
  if (sorted.length <= 500) { const r = await leaf("", sorted); return { sha256: r.sha256 as string, count: String(sorted.length) }; }
  const items: { digit: string; sha256: string }[] = [];
  for (const d of "0123456789abcdef") { const ids = sorted.filter((x) => shaHex(x)[0] === d); if (ids.length) items.push({ digit: d, sha256: (await leaf(d, ids)).sha256 }); }
  const r = await reg({ protocol: "history-mask-v1", orgId, workspaceId, prefix: "", kind: "node", count: String(sorted.length), items });
  return { sha256: r.sha256 as string, count: String(sorted.length) };
}
setDefaultTimeout(60000);
// Production scope-version creator + final install CAS (sessions.ingest_history_scope_*):
// the fixture INSERTs other suites use are replaced here by the real protocol.
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64);
let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + gen + "','pass')");
  // Operator seed above the drained legacy feature clock (H=1000): the journal refuses until it exists.
  await db.query("SELECT sessions.canonical_feature_journal_seed(1000,$1)", [gen]);
});
afterAll(async () => { await db?.close(); });
const run: any = { query: async (sql: string, args: unknown[]) => {
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await db.query(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
} };
const rpc = async (fn: string, p: unknown) => (await run.query("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0].r;
let n = 0;
async function scope(records: string[]) {
  const i = ++n, org = "creator-org-" + i, ws = "creator-ws", identity = { orgId: org, workspaceId: ws, source: "claude", sessionId: "creator-session-" + i };
  const row = { org_id: org, workspace_id: ws, user_id: "creator-user", harness: "claude", session_id: identity.sessionId, machine_id: "srv_origin", created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "creator-fixture", manifest_key: "96bulls/" + org + "/v1/ws=" + ws + "/m=srv_origin/h=claude/d=2026/11/01/s=" + identity.sessionId + "/manifest.json", content_sha256: "\\x" + "a".repeat(64), raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,$3,$4,now(),now())", [org, ws, "claude", identity.sessionId]);
  const mask = await registerMask((doc) => rpc("ingest_history_mask_register", { generation: gen, document: doc }), org, ws, records);
  const anchor = (g: string) => "96bulls/" + org + "/upload-generations/g=" + g + "/v1/ws=" + ws + "/m=history_scope/h=claude/d=2026/11/01/s=" + identity.sessionId + "/publications/" + g + "/manifest.json";
  return { identity, mask, anchor, row };
}
const install = (b: any, s: Awaited<ReturnType<typeof scope>>, g = "12345678-1234-1234-1234-123456789abc") => rpc("ingest_history_scope_install", {
  generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count,
  derivedAnchorKey: s.anchor(g), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" });
async function build(s: Awaited<ReturnType<typeof scope>>, keys: string[]) {
  const b = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  for (let i = 0; i < keys.length; i += 500) await rpc("ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: keys.slice(i, i + 500), owners: [] });
  const c = await rpc("ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken });
  return { b, c };
}

test("the production creator installs a ready scope that the existing capture/permit path accepts", async () => {
  const keys = Array.from({ length: 1200 }, (_, i) => "native-" + i);
  const s = await scope(keys), { b, c } = await build(s, keys);
  expect(c.epoch).toMatch(/^[1-9][0-9]*$/);
  const r = await install(b, s);
  expect(r).toMatchObject({ installed: true, retiredAuthorityId: null, maskSha256: s.mask.sha256, recordCount: "1200" });
  // The existing authority path (ingest_history_source_current via session_history_source_lock and
  // session_history_mask_page) accepts exactly what the creator installed.
  const evidence = { generation: gen, identity: s.identity, source: r.source, authorityId: r.authorityId, maskSha256: r.maskSha256 };
  expect(await rpc("session_history_source_lock", evidence)).toEqual({ current: true });
  let after = "0", seen = 0;
  for (;;) { const page = await rpc("session_history_mask_page", { ...evidence, after }); seen += page.records.length; if (page.next === null) break; after = page.next; }
  expect(seen).toBe(1200);
  expect((await rpc("session_history_capture", { generation: gen, identity: s.identity })).blocked).toBe(false);
  expect((await db.query("SELECT state FROM sessions.ingest_history_scope_builds WHERE build_id=$1", [b.buildId])).rows[0]).toEqual({ state: "installed" });
});

test("a mask record that was never registered as a scope key cannot be installed (coverage)", async () => {
  const s = await scope(["covered", "uncovered"]), { b } = await build(s, ["covered"]);
  await expect(install(b, s)).rejects.toThrow("mask record not registered as a scope key");
  expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0]!.n).toBe(0);
});

const invalidate = (s: { identity: { orgId: string; workspaceId: string; sessionId: string } }) => db.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
test("invalidation of an INSTALLED scope between capture and install refuses the stale replacement build", async () => {
  const s = await scope(["k1"]);
  await install((await build(s, ["k1"])).b, s);
  const { b } = await build(s, ["k1"]);
  await invalidate(s);
  await expect(install(b, s, "22222222-2222-2222-2222-222222222222")).rejects.toThrow("scope source changed since capture");
});

test("invalidating a scope with NO installed authority is a no-op: no required scope, epoch advance or directory change", async () => {
  const s = await scope(["k1"]), { b } = await build(s, ["k1"]);
  const epoch = async () => (await db.query<any>("SELECT epoch::text e FROM sessions.session_replay_publications WHERE org_id=$1", [s.identity.orgId])).rows[0].e;
  const before = await epoch();
  await invalidate(s);
  expect(await epoch()).toBe(before);
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
  expect((await install(b, s)).installed).toBe(true);
});

test("a pointer-set change between capture and install refuses", async () => {
  const s = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 2, content_sha256: "\\x" + "b".repeat(64) }])]);
  await expect(install(b, s)).rejects.toThrow("scope source changed since capture");
});

test("a newer build supersedes an older one: the older lease can never capture or install", async () => {
  const s = await scope(["k1"]);
  const old = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  const { b } = await build(s, ["k1"]);
  await expect(rpc("ingest_history_scope_capture", { generation: gen, buildId: old.buildId, leaseToken: old.leaseToken })).rejects.toThrow("scope build lease lost");
  await install(b, s);
  await expect(install(b, s)).rejects.toThrow("scope build lease lost");
});

test("an expired lease refuses at install", async () => {
  const s = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await db.query("UPDATE sessions.ingest_history_scope_builds SET lease_until=clock_timestamp()-interval'1 second' WHERE build_id=$1", [b.buildId]);
  await expect(install(b, s)).rejects.toThrow("scope build lease lost");
});

// A second machine uploads: the scope's frozen pointer inventory (and so its content) changes.
const addMachine = async (s: Awaited<ReturnType<typeof scope>>, machine: string) => {
  const row = { ...s.row, machine_id: machine, manifest_key: s.row.manifest_key.replace("m=srv_origin", "m=" + machine) };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  return row;
};
const derivedKey = (s: Awaited<ReturnType<typeof scope>>) => s.anchor("12345678-1234-1234-1234-123456789abc").replace("manifest.json", "derived/summary-short/11111111-1111-1111-1111-111111111111.bin");
// Everything an install could move for one scope: current row, every version, derived objects, journal.
const scopeState = async (org: string) => ({
  current: (await db.query<any>("SELECT authority_id::text a,ready FROM sessions.ingest_history_scope_current WHERE org_id=$1", [org])).rows,
  versions: (await db.query<any>("SELECT authority_id::text a,state,source_fingerprint f,content_fingerprint c,feature_revision::text r FROM sessions.ingest_history_scope_versions WHERE org_id=$1 ORDER BY created_at,authority_id", [org])).rows,
  derived: (await db.query<any>("SELECT o.object_key k,o.cleanup_state s,o.row_version::text v FROM sessions.ingest_history_derived_objects o JOIN sessions.ingest_history_scope_versions v USING(authority_id) WHERE v.org_id=$1 ORDER BY o.object_key", [org])).rows,
  journal: (await db.query<any>("SELECT revision::text r,kind FROM sessions.canonical_feature_journal WHERE org_id=$1 ORDER BY revision", [org])).rows,
});

test("reinstall over CHANGED content retires the prior version and its live derived objects atomically", async () => {
  const s = await scope(["k1"]);
  const first = await install((await build(s, ["k1"])).b, s);
  await db.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES($1,$2,repeat('c',64),10)", [derivedKey(s), first.authorityId]);
  await addMachine(s, "srv_second");
  const second = await install((await build(s, ["k1"])).b, s, "22222222-2222-2222-2222-222222222222");
  expect(second.retiredAuthorityId).toBe(first.authorityId);
  expect((await db.query("SELECT state FROM sessions.ingest_history_scope_versions WHERE authority_id=$1", [first.authorityId])).rows[0]).toEqual({ state: "retired" });
  expect((await db.query("SELECT cleanup_state FROM sessions.ingest_history_derived_objects WHERE authority_id=$1", [first.authorityId])).rows[0]).toEqual({ cleanup_state: "retired" });
  expect((await db.query("SELECT authority_id::text FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0]).toEqual({ authority_id: second.authorityId });
});

test("identical-content replacement: install refuses and leaves the current row, versions, derived objects and journal unchanged", async () => {
  const s = await scope(["k1"]);
  const first = await install((await build(s, ["k1"])).b, s);
  await db.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES($1,$2,repeat('c',64),10)", [derivedKey(s), first.authorityId]);
  const before = await scopeState(s.identity.orgId);
  expect(before.current).toEqual([{ a: first.authorityId, ready: true }]);
  expect(before.derived).toEqual([{ k: derivedKey(s), s: "live", v: "1" }]);
  expect(before.journal.map((x: any) => x.kind)).toEqual(["publish"]);
  // Same session_index rows, same epoch, same mask: only the (new) authority would differ.
  const { b } = await build(s, ["k1"]);
  await expect(install(b, s, "22222222-2222-2222-2222-222222222222")).rejects.toThrow("history: replacement does not change the source content");
  expect(await scopeState(s.identity.orgId)).toEqual(before);
  expect((await db.query<any>("SELECT state FROM sessions.ingest_history_scope_builds WHERE build_id=$1", [b.buildId])).rows[0].state).toBe("captured");
  // The authority-bound source fingerprints of distinct authorities always differ, so the content
  // identity is what the guard compares: the refused content equals the installed one.
  expect(before.versions[0].c).toMatch(/^[a-f0-9]{64}$/);
  expect(before.versions[0].c).not.toBe(before.versions[0].f);
});

test("a replacement over the same pointers after a managed invalidation (new epoch) installs", async () => {
  const s = await scope(["k1"]);
  const first = await install((await build(s, ["k1"])).b, s);
  await db.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
  const second = await install((await build(s, ["k1"])).b, s, "22222222-2222-2222-2222-222222222222");
  expect(second.retiredAuthorityId).toBe(first.authorityId);
  expect(BigInt(second.source.epoch)).toBeGreaterThan(BigInt(first.source.epoch));
});

test("a foreign-tenant mask root or an anchor outside the tenant prefix is refused", async () => {
  const s = await scope(["k1"]), other = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await expect(rpc("ingest_history_scope_install", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: other.mask.sha256, recordCount: other.mask.count, derivedAnchorKey: s.anchor("12345678-1234-1234-1234-123456789abc"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })).rejects.toThrow("mask root does not match scope");
  await expect(rpc("ingest_history_scope_install", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count, derivedAnchorKey: other.anchor("12345678-1234-1234-1234-123456789abc"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })).rejects.toThrow("derived anchor outside tenant");
});

test("only the ingest role may run the creator", async () => {
  const s = await scope(["k1"]);
  await db.exec("BEGIN; SET LOCAL ROLE session_directory_search_worker");
  try { await expect(db.query("SELECT sessions.ingest_history_scope_begin($1::jsonb)", [JSON.stringify({ generation: gen, identity: s.identity })])).rejects.toThrow(); }
  finally { await db.exec("ROLLBACK"); }
});

const journal = async (org: string) => (await db.query<any>("SELECT kind,source_authority,prior_source_authority,pointer_set,schema_generation FROM sessions.canonical_feature_journal WHERE org_id=$1 ORDER BY revision", [org])).rows;

const IMAGE = /^hs1:[a-f0-9]{64}:[a-f0-9]{64}:[0-9a-f-]{36}$/;
const pointerPage = async (authorityId: string) => (await db.query<any>("SELECT pointer FROM sessions.ingest_history_scope_pointer_page($1::uuid,0,1000)", [authorityId])).rows.map((x) => x.pointer.manifest_key);
const journalRows = async (org: string) => (await db.query<any>("SELECT revision::int r,kind,image_key,source_hash,source_authority,prior_source_authority,pointer_set,prior_keyset_hash,schema_generation FROM sessions.canonical_feature_journal WHERE org_id=$1 ORDER BY revision", [org])).rows;

test("610 compose: a new scope appends exactly one publish(new image, new authority, bounded descriptor) recorded as the current revision", async () => {
  const s = await scope(["k1"]), r = await install((await build(s, ["k1"])).b, s);
  const rows = await journalRows(s.identity.orgId);
  expect(rows.length).toBe(1);
  expect(rows[0]).toMatchObject({ kind: "publish", source_authority: { ...r.source, authorityId: r.authorityId }, prior_source_authority: null, prior_keyset_hash: null, schema_generation: gen });
  expect(rows[0].r).toBeGreaterThan(1000);
  expect(rows[0].image_key).toMatch(IMAGE);
  expect(rows[0].image_key.endsWith(":" + gen + ":" + r.authorityId)).toBe(true);
  expect(Object.keys(rows[0].pointer_set).sort()).toEqual(["authorityId", "count", "hash"]);
  expect(rows[0].pointer_set).toMatchObject({ authorityId: r.authorityId, count: "1" });
  expect(r.featureRevision).toBe(String(rows[0].r));
  const v = (await db.query<any>("SELECT feature_revision::int f,pointer_count,pointer_snapshot_sha256 h FROM sessions.ingest_history_scope_versions WHERE authority_id=$1", [r.authorityId])).rows[0];
  expect(v).toEqual({ f: rows[0].r, pointer_count: 1, h: rows[0].pointer_set.hash });
  expect(await pointerPage(r.authorityId)).toEqual([s.row.manifest_key]);
});

test("610 compose: a replacement appends retire(PRIOR image/authority/descriptor) THEN publish(NEW image/authority/descriptor)", async () => {
  const s = await scope(["k1"]);
  const first = await install((await build(s, ["k1"])).b, s);
  // A second machine uploads: the replacement is installed over a DIFFERENT pointer inventory.
  const second_row = { ...s.row, machine_id: "srv_second", manifest_key: s.row.manifest_key.replace("m=srv_origin", "m=srv_second") };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([second_row])]);
  const second = await install((await build(s, ["k1"])).b, s, "22222222-2222-2222-2222-222222222222");
  const rows = await journalRows(s.identity.orgId);
  expect(rows.map((x) => x.kind)).toEqual(["publish", "retire", "publish"]);
  const [pub1, retire, pub2] = rows;
  expect(retire.r).toBe(pub1.r + 1); expect(pub2.r).toBe(retire.r + 1);
  expect(retire.image_key).toBe(pub1.image_key);
  expect(pub2.image_key).not.toBe(pub1.image_key);
  expect(pub2.image_key).toMatch(IMAGE);
  expect(retire.source_authority).toEqual({ ...first.source, authorityId: first.authorityId });
  expect(retire.source_hash).toBe(first.source.fingerprint);
  expect(retire.pointer_set).toEqual(pub1.pointer_set);
  expect(retire.prior_keyset_hash).toBe(pub1.pointer_set.hash);
  expect(pub2.source_authority).toEqual({ ...second.source, authorityId: second.authorityId });
  expect(pub2.prior_source_authority).toEqual(retire.source_authority);
  expect(pub2.pointer_set).toMatchObject({ authorityId: second.authorityId, count: "2" });
  expect(pub2.pointer_set.hash).not.toBe(retire.pointer_set.hash);
  // Exact PRIOR keys page from the frozen prior authority; the new authority has the new inventory.
  expect(await pointerPage(retire.pointer_set.authorityId)).toEqual([s.row.manifest_key]);
  expect((await pointerPage(second.authorityId)).sort()).toEqual([s.row.manifest_key, second_row.manifest_key].sort());
  expect(second.featureRevision).toBe(String(pub2.r));
  // The prior authority is frozen: its inventory and revision can no longer move.
  await expect(db.query("UPDATE sessions.ingest_history_scope_versions SET pointer_count=pointer_count WHERE authority_id=$1", [first.authorityId])).resolves.toBeDefined();
  await expect(db.query("UPDATE sessions.ingest_history_scope_versions SET feature_revision=feature_revision+100 WHERE authority_id=$1", [first.authorityId])).rejects.toThrow("frozen");
  await expect(db.query("UPDATE sessions.ingest_history_scope_versions SET state='ready' WHERE authority_id=$1", [first.authorityId])).rejects.toThrow("frozen");
});

test("610 compose: a rolled-back replacement emits neither the retire nor the publish", async () => {
  const s = await scope(["k1"]);
  await install((await build(s, ["k1"])).b, s);
  await addMachine(s, "srv_second");                       // real content change: the replacement is legitimate
  const built = (await build(s, ["k1"])).b;
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  await db.query("SELECT sessions.ingest_history_scope_install($1::jsonb)", [JSON.stringify({ generation: gen, buildId: built.buildId, leaseToken: built.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count, derivedAnchorKey: s.anchor("33333333-3333-3333-3333-333333333333"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })]);
  await db.exec("ROLLBACK");
  expect((await journalRows(s.identity.orgId)).map((x) => x.kind)).toEqual(["publish"]);
});

test("610 compose: an unseeded journal refuses the install (no MAX+1 from 1)", async () => {
  const s = await scope(["k1"]), built = (await build(s, ["k1"])).b;
  await db.exec("BEGIN");
  try {
    await db.query("DELETE FROM sessions.canonical_feature_journal WHERE kind='seed'");
    await db.exec("SET LOCAL ROLE bulls96_session_ingest");
    await expect(db.query("SELECT sessions.ingest_history_scope_install($1::jsonb)", [JSON.stringify({ generation: gen, buildId: built.buildId, leaseToken: built.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count, derivedAnchorKey: s.anchor("44444444-4444-4444-4444-444444444444"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })])).rejects.toThrow("not seeded");
  } finally { await db.exec("ROLLBACK"); }
  await expect(db.query("SELECT sessions.canonical_feature_journal_seed(5,$1)", [gen])).rejects.toThrow("already seeded");
});

test("610 compose: the invoker refuses raw pointers, descriptors that miss the frozen authority, half priors and self-retire", async () => {
  const s = await scope(["k1"]), r = await install((await build(s, ["k1"])).b, s);
  const v = (await db.query<any>("SELECT pointer_snapshot_sha256 h,pointer_count::text c FROM sessions.ingest_history_scope_versions WHERE authority_id=$1", [r.authorityId])).rows[0];
  const sc = JSON.stringify({ orgId: s.identity.orgId, workspaceId: s.identity.workspaceId, harness: "claude", sessionId: s.identity.sessionId, generation: gen });
  const src = JSON.stringify({ ...r.source, authorityId: r.authorityId });
  const ds = (o: object = {}) => JSON.stringify({ authorityId: r.authorityId, hash: v.h, count: v.c, ...o });
  const call = (args: unknown[]) => db.query("SELECT sessions.canonical_feature_changed($1::jsonb,$2::jsonb,$3::jsonb,$4::jsonb,$5::jsonb)", args);
  await db.exec("BEGIN");
  try {
    // Even the exact frozen descriptor cannot publish the same immutable image twice.
    await db.exec("SAVEPOINT x"); await expect(call([sc, src, null, ds(), null])).rejects.toThrow("canonical_feature_journal_image_kind"); await db.exec("ROLLBACK TO SAVEPOINT x");
    for (const bad of [
      [sc, src, null, ds({ pointers: [] }), null],               // raw pointers are not part of the bounded shape
      [sc, src, null, ds({ count: "2" }), null],                 // count misses the frozen inventory
      [sc, src, null, ds({ hash: "c".repeat(64) }), null],       // hash misses the frozen inventory
      [sc, src, null, ds({ count: "01" }), null],                // non-canonical count
      [sc, src, src, ds(), null],                                // prior source without prior descriptor
      [sc, src, src, ds(), ds()],                                // self-retire
    ]) { await db.exec("SAVEPOINT x"); await expect(call(bad)).rejects.toThrow("canonical_feature_changed"); await db.exec("ROLLBACK TO SAVEPOINT x"); }
    // Identical content under a NEW authority is refused by the ABI itself ("invalid input"), before any
    // frozen-authority lookup: no retire+publish of the same fingerprint.
    const other = JSON.stringify({ ...r.source, authorityId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" });
    await db.exec("SAVEPOINT x");
    await expect(call([sc, src, other, ds(), ds({ authorityId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" })])).rejects.toThrow("canonical_feature_changed: invalid input");
    await db.exec("ROLLBACK TO SAVEPOINT x");
  } finally { await db.exec("ROLLBACK"); }
});

test("610 compose: a refused install and a rolled-back install emit no journal entry", async () => {
  const s = await scope(["k1"]);
  await install((await build(s, ["k1"])).b, s);
  const { b } = await build(s, ["k1"]);
  await db.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
  await expect(install(b, s, "22222222-2222-2222-2222-222222222222")).rejects.toThrow("scope source changed since capture");
  expect((await journal(s.identity.orgId)).map((x: any) => x.kind)).toEqual(["publish"]);
  const t = await scope(["k1"]), built = (await build(t, ["k1"])).b;
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  await db.query("SELECT sessions.ingest_history_scope_install($1::jsonb)", [JSON.stringify({ generation: gen, buildId: built.buildId, leaseToken: built.leaseToken, maskSha256: t.mask.sha256, recordCount: t.mask.count, derivedAnchorKey: t.anchor("12345678-1234-1234-1234-123456789abc"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })]);
  await db.exec("ROLLBACK");
  expect((await journal(t.identity.orgId)).length).toBe(0);
  expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_history_scope_current WHERE org_id=$1", [t.identity.orgId])).rows[0]!.n).toBe(0);
});

test("610 compose: no role may append to the journal directly", async () => {
  for (const role of ["bulls96_session_ingest", "session_directory_search_worker", "service_role"]) {
    await db.exec("BEGIN; SET LOCAL ROLE " + role);
    try { await expect(db.query("SELECT sessions.canonical_feature_changed('{}','{}',NULL,'{}',NULL)")).rejects.toThrow(); } finally { await db.exec("ROLLBACK"); }
  }
});

test("exact key set: the install refuses when the build registered keys the mask does not carry", async () => {
  const s = await scope(["k1"]);
  const { b } = await build(s, ["k1", "k2"]);             // k2 registered by this build, absent from the mask
  await expect(install(b, s)).rejects.toThrow("mask record set does not equal the build key set");
  expect((await db.query("SELECT count(*)::int n FROM sessions.canonical_feature_journal WHERE org_id=$1", [s.identity.orgId])).rows[0]!.n).toBe(0);
});

test("exact key set: keys a PREVIOUS build registered do not count for this build", async () => {
  const s = await scope(["k1", "k2"]);
  await build(s, ["k1", "k2"]);                            // abandoned by the next begin
  const { b } = await build(s, ["k1"]);                    // this build only registers k1
  await expect(install(b, s)).rejects.toThrow("mask record set does not equal the build key set");
});

test("producer role: read-only journal + frozen pointer pages; no writer, seed or creator access", async () => {
  const s = await scope(["k1"]), r = await install((await build(s, ["k1"])).b, s);
  const as = async <T>(fn: () => Promise<T>) => { await db.exec("BEGIN; SET LOCAL ROLE bulls96_canonical_feature_producer"); try { return await fn(); } finally { await db.exec("ROLLBACK"); } };
  expect(await as(async () => (await db.query<any>("SELECT count(*)::int n FROM sessions.canonical_feature_journal WHERE org_id=$1", [s.identity.orgId])).rows[0].n)).toBe(1);
  expect(await as(async () => (await db.query<any>("SELECT pointer->>'manifest_key' k FROM sessions.ingest_history_scope_pointer_page($1::uuid,0,10)", [r.authorityId])).rows.map((x) => x.k))).toEqual([s.row.manifest_key]);
  for (const sql of [
    "INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES(999999,'seed','','','{}','" + gen + "','seed','" + "0".repeat(64) + "','{}','{}')",
    "DELETE FROM sessions.canonical_feature_journal",
    "SELECT sessions.canonical_feature_changed('{}','{}',NULL,'{}',NULL)",
    "SELECT sessions.canonical_feature_journal_seed(1,'" + gen + "')",
    "SELECT sessions.ingest_history_scope_begin('{}')",
    "SELECT * FROM sessions.ingest_history_scope_versions",
  ]) await as(async () => { await expect(db.query(sql)).rejects.toThrow(/permission denied|must be/); });
  const attrs = (await db.query<any>("SELECT rolcanlogin,rolinherit,rolbypassrls,rolsuper FROM pg_roles WHERE rolname='bulls96_canonical_feature_producer'")).rows[0];
  expect(attrs).toEqual({ rolcanlogin: false, rolinherit: false, rolbypassrls: false, rolsuper: false });
});

test("no role outside the ingest Worker holds EXECUTE on the creator (default PUBLIC grant revoked)", async () => {
  const fns = ["ingest_history_scope_build_locked(jsonb,text)", "ingest_history_scope_begin(jsonb)", "ingest_history_scope_build_keys(jsonb)", "ingest_history_scope_capture(jsonb)", "ingest_history_scope_install(jsonb)"];
  for (const role of ["anon", "authenticated", "service_role", "session_directory_search_worker", "bulls96_session_ingest_seeder", "bulls96_canonical_feature_producer"])
    for (const fn of fns)
      expect((await db.query<any>("SELECT has_function_privilege($1,$2,'EXECUTE') ok", [role, "sessions." + fn])).rows[0].ok).toBe(false);
  for (const fn of fns.slice(1)) expect((await db.query<any>("SELECT has_function_privilege('bulls96_session_ingest',$1,'EXECUTE') ok", ["sessions." + fn])).rows[0].ok).toBe(true);
});
