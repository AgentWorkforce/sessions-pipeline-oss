import { afterAll, beforeAll, expect, test, setDefaultTimeout } from "bun:test";
import pg from "pg";
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
setDefaultTimeout(120000);
// Real PostgreSQL races for the production scope creator (PGlite has one connection).
const url = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64);
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
let admin: pg.Client, pool: pg.Pool, name = "";
beforeAll(async () => {
  if (!url) return;
  admin = new pg.Client({ connectionString: url }); await admin.connect(); name = "scope_creator_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name); const u = new URL(url); u.pathname = "/" + name;
  pool = new pg.Pool({ connectionString: u.href, max: 16 });
  await pool.query(read("supabase/tests/supabase_shim.sql")); await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
  for (const f of ["session_directory_search_worker.sql", "session_token_verifier.sql", "session_ingest_roles.sql"]) await pool.query(read("supabase/bootstrap/" + f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pool.query(read("supabase/migrations/" + f));
  await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
  await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')", [gen]);
  await pool.query("SELECT sessions.canonical_feature_journal_seed(1000,$1)", [gen]);
});
afterAll(async () => { if (!url) return; await pool?.end(); await admin.query("DROP DATABASE IF EXISTS " + name); await admin.end(); });
async function asIngest<T>(c: pg.PoolClient, work: () => Promise<T>, commit = true): Promise<T> {
  await c.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await work(); if (commit) await c.query("COMMIT"); return r; } catch (e) { await c.query("ROLLBACK"); throw e; }
}
const call = async (c: pg.PoolClient | pg.Pool, fn: string, p: unknown) => (await c.query("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0].r;
const runner = () => ({ query: async (sql: string, args: unknown[]) => { const c = await pool.connect(); try { return await asIngest(c, () => c.query(sql, args as never)); } finally { c.release(); } } });
let n = 0;
async function scope() {
  const i = ++n, org = "race-org-" + i, ws = "race-ws", identity = { orgId: org, workspaceId: ws, source: "claude", sessionId: "race-session-" + i };
  const row = { org_id: org, workspace_id: ws, user_id: "race-user", harness: "claude", session_id: identity.sessionId, machine_id: "srv_origin", created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "race-fixture", manifest_key: "96bulls/" + org + "/v1/ws=" + ws + "/m=srv_origin/h=claude/d=2026/11/01/s=" + identity.sessionId + "/manifest.json", content_sha256: "\\x" + "a".repeat(64), raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await pool.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await pool.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,$3,$4,now(),now())", [org, ws, "claude", identity.sessionId]);
  const mask = await registerMask((doc) => runner().query("SELECT sessions.ingest_history_mask_register($1::jsonb) r", [JSON.stringify({ generation: gen, document: doc })]).then((x: any) => x.rows[0].r), org, ws, ["k1"]);
  const anchor = (g: string) => "96bulls/" + org + "/upload-generations/g=" + g + "/v1/ws=" + ws + "/m=history_scope/h=claude/d=2026/11/01/s=" + identity.sessionId + "/publications/" + g + "/manifest.json";
  return { identity, mask, anchor, row };
}
async function captured(s: Awaited<ReturnType<typeof scope>>) {
  const c = await pool.connect();
  try {
    const b = await asIngest(c, () => call(c, "ingest_history_scope_begin", { generation: gen, identity: s.identity }));
    await asIngest(c, () => call(c, "ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [] }));
    await asIngest(c, () => call(c, "ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken }));
    return b;
  } finally { c.release(); }
}
const installArgs = (b: any, s: Awaited<ReturnType<typeof scope>>, g = "12345678-1234-1234-1234-123456789abc") => ({ generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count, derivedAnchorKey: s.anchor(g), bucket: "race-fixture", region: "us-east-1", keyRoot: "96bulls" });

// A second machine uploads: the frozen pointer inventory changes, so a replacement carries new content.
const addMachine = async (s: Awaited<ReturnType<typeof scope>>, machine: string) => pool.query("SELECT sessions.upsert_session_index($1::jsonb)",
  [JSON.stringify([{ ...s.row, machine_id: machine, manifest_key: s.row.manifest_key.replace("m=srv_origin", "m=" + machine) }])]);
const installNow = async (s: Awaited<ReturnType<typeof scope>>, g?: string) => { const b = await captured(s); const c = await pool.connect(); try { return await asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(b, s, g))); } finally { c.release(); } };

test.skipIf(!url)("install never blocks behind an in-flight invalidation and refuses once it commits", async () => {
  const s = await scope();
  await installNow(s);                                                    // only an INSTALLED scope is ever invalidated
  await addMachine(s, "srv_second");
  const b = await captured(s);
  const holder = await pool.connect(), installer = await pool.connect();
  try {
    await holder.query("BEGIN");
    await holder.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
    const t0 = Date.now();
    await expect(asIngest(installer, () => call(installer, "ingest_history_scope_install", installArgs(b, s, "22222222-2222-2222-2222-222222222222")))).rejects.toThrow("logical scope busy");
    expect(Date.now() - t0).toBeLessThan(5000);
    await holder.query("COMMIT");
    await expect(asIngest(installer, () => call(installer, "ingest_history_scope_install", installArgs(b, s, "22222222-2222-2222-2222-222222222222")))).rejects.toThrow("scope source changed since capture");
  } finally { holder.release(); installer.release(); }
});

test.skipIf(!url)("a build begun during an uncommitted install binds the pre-install authority and can never overwrite it", async () => {
  const s = await scope(), a = await captured(s);
  const ca = await pool.connect(), cb = await pool.connect();
  try {
    await ca.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    const installed = await call(ca, "ingest_history_scope_install", installArgs(a, s)); // holds A's build row + current row
    const late = asIngest(cb, () => call(cb, "ingest_history_scope_begin", { generation: gen, identity: s.identity })); // blocks on A's build row
    await new Promise((r) => setTimeout(r, 300));
    await ca.query("COMMIT");
    const b = await late;
    expect(b.expectedAuthorityId).toBeNull(); // read before A committed
    await asIngest(cb, () => call(cb, "ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [] }));
    await asIngest(cb, () => call(cb, "ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken }));
    await expect(asIngest(cb, () => call(cb, "ingest_history_scope_install", installArgs(b, s, "22222222-2222-2222-2222-222222222222")))).rejects.toThrow("scope source changed since capture");
    const cur = (await pool.query("SELECT authority_id::text a FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].a;
    expect(cur).toBe(installed.authorityId);
  } finally { ca.release(); cb.release(); }
});

test.skipIf(!url)("parallel installs of the same captured build: exactly one wins", async () => {
  const s = await scope(), b = await captured(s);
  const cs = await Promise.all([0, 1, 2, 3].map(() => pool.connect()));
  try {
    const results = await Promise.allSettled(cs.map((c) => asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(b, s)))));
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
    for (const r of results) if (r.status === "rejected") expect(String((r.reason as Error).message)).toMatch(/scope build lease lost|logical scope busy/);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.ingest_history_scope_versions WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(1);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.canonical_feature_journal WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(1);
  } finally { for (const c of cs) c.release(); }
});

test.skipIf(!url)("parallel installs of different scopes get distinct, gap-free journal revisions", async () => {
  const scopes = await Promise.all([0, 1, 2, 3, 4, 5].map(() => scope()));
  const builds = await Promise.all(scopes.map((s) => captured(s)));
  const before = (await pool.query("SELECT coalesce(max(revision),0)::int m FROM sessions.canonical_feature_journal")).rows[0].m;
  const cs = await Promise.all(scopes.map(() => pool.connect()));
  try {
    const results = await Promise.allSettled(cs.map((c, i) => asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(builds[i], scopes[i])))));
    expect(results.every((r) => r.status === "fulfilled")).toBe(true);
    const revs = (await pool.query("SELECT revision::int r FROM sessions.canonical_feature_journal WHERE revision>$1 ORDER BY revision", [before])).rows.map((x) => x.r);
    expect(revs).toEqual(Array.from({ length: 6 }, (_, i) => before + 1 + i));
  } finally { for (const c of cs) c.release(); }
});

const kinds = async (org: string) => (await pool.query("SELECT kind FROM sessions.canonical_feature_journal WHERE org_id=$1 ORDER BY revision", [org])).rows.map((x) => x.kind);

test.skipIf(!url)("an uncommitted replacement exposes neither half of the pair; commit exposes both, in order", async () => {
  const s = await scope();
  await installNow(s);
  await addMachine(s, "srv_second");
  const b = await captured(s), c = await pool.connect();
  try {
    await c.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    await call(c, "ingest_history_scope_install", installArgs(b, s, "22222222-2222-2222-2222-222222222222"));
    expect(await kinds(s.identity.orgId)).toEqual(["publish"]);       // another session sees no partial pair
    await c.query("COMMIT");
  } finally { c.release(); }
  expect(await kinds(s.identity.orgId)).toEqual(["publish", "retire", "publish"]);
});

test.skipIf(!url)("parallel installs of one replacement build: exactly one wins and exactly one retire/publish pair is written", async () => {
  const s = await scope();
  await installNow(s);
  await addMachine(s, "srv_second");
  const b = await captured(s), cs = await Promise.all([0, 1, 2, 3].map(() => pool.connect()));
  try {
    const results = await Promise.allSettled(cs.map((c, i) => asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(b, s, "3333333" + i + "-3333-3333-3333-333333333333")))));
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
  } finally { for (const c of cs) c.release(); }
  expect(await kinds(s.identity.orgId)).toEqual(["publish", "retire", "publish"]);
});

test.skipIf(!url)("erase then re-enable: the stale build refuses with no rows; a fresh build replaces with one ordered pair", async () => {
  const s = await scope();
  await installNow(s);
  const stale = await captured(s);
  await pool.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
  const c = await pool.connect();
  try { await expect(asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(stale, s)))).rejects.toThrow(); } finally { c.release(); }
  expect(await kinds(s.identity.orgId)).toEqual(["publish"]);
  await installNow(s, "44444444-4444-4444-4444-444444444444");
  expect(await kinds(s.identity.orgId)).toEqual(["publish", "retire", "publish"]);
});

// Exactly the ingest_lazy_native_slots writes the production ACK makes (slot init, then winner change).
const sha = (x: string) => createHash("sha256").update(x).digest("hex");
async function ackOn(c: pg.PoolClient | pg.Pool, s: Awaited<ReturnType<typeof scope>>, rec: string, revision = 1) {
  const key = sha("slot:" + s.identity.orgId + ":" + rec);
  await c.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES($1,$2::jsonb,$3,$4,'srv_origin',$5,$6,$7,$8) ON CONFLICT DO NOTHING",
    [key, JSON.stringify(["global-native-custody-v1", s.identity.orgId, s.identity.workspaceId, "srv_origin", rec]), s.identity.orgId, s.identity.workspaceId, rec, sha("src"), sha("ctl"), sha("wit")]);
  await c.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1,current_owner='u1',current_source='claude',current_session=$2,current_revision=$3,current_digest=$4,current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=$1",
    [key, s.identity.sessionId, revision, sha("digest:" + revision)]);
}

test.skipIf(!url)("concurrent ACKs on one managed scope both commit: the second waits for the scope lock, never 40001", async () => {
  const s = await scope();
  await installNow(s);
  const version = async () => (await pool.query("SELECT coalesce(max(request_version),0)::int v FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId])).rows[0].v;
  const before = await version();
  const c1 = await pool.connect(), c2 = await pool.connect();
  try {
    await c1.query("BEGIN"); await ackOn(c1, s, "k1");                      // holds the scope lock, uncommitted
    let done = false;
    const second = (async () => { await c2.query("BEGIN"); await ackOn(c2, s, "k2"); await c2.query("COMMIT"); done = true; })();
    await new Promise((r) => setTimeout(r, 300));
    expect(done).toBe(false);                                                 // waiting, not failed
    await c1.query("COMMIT");
    await second;
    expect(done).toBe(true);
  } finally { c1.release(); c2.release(); }
  expect(await version()).toBeGreaterThan(before + 1);                      // both ACKs invalidated the scope
});

test.skipIf(!url)("an install racing an uncommitted ACK never blocks the ACK; it waits on the fenced build and refuses", async () => {
  const s = await scope();
  await installNow(s);
  await addMachine(s, "srv_second");
  const b = await captured(s);
  const c1 = await pool.connect(), c2 = await pool.connect();
  try {
    await c1.query("BEGIN"); await ackOn(c1, s, "k1");                      // fenced the replacement build, invalidated the managed scope
    const install = asIngest(c2, () => call(c2, "ingest_history_scope_install", installArgs(b, s, "55555555-5555-5555-5555-555555555555"))).then(() => "installed", (e: Error) => e.message);
    await new Promise((r) => setTimeout(r, 300));
    await c1.query("COMMIT");                                               // the ACK itself was never blocked
    expect(await install).toContain("scope build lease lost");
  } finally { c1.release(); c2.release(); }
});

const legacy = async (s: Awaited<ReturnType<typeof scope>>) => (await pool.query("SELECT state,short_summary,short_summary_scrub_ruleset r,short_summary_source src FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [s.identity.orgId, s.identity.sessionId])).rows[0];
const summarize = (s: Awaited<ReturnType<typeof scope>>, text: string) => pool.query("UPDATE sessions.session_directory SET short_summary=$3,short_summary_scrub_ruleset='relayhistory-scrub',short_summary_scrub_version=1,short_summary_source='bob-costas/extractive/v1',short_summary_generated_at=now() WHERE org_id=$1 AND session_id=$2", [s.identity.orgId, s.identity.sessionId, text]);
const required = async (org: string) => (await pool.query("SELECT count(*)::int n FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [org])).rows[0].n;

test.skipIf(!url)("ACK first on an in-flight-only scope: the concurrent install waits, then refuses; legacy state never moves", async () => {
  const s = await scope();
  await summarize(s, "legacy");
  const before = await legacy(s), b = await captured(s);
  const c1 = await pool.connect(), c2 = await pool.connect();
  try {
    await c1.query("BEGIN"); await ackOn(c1, s, "k1");                      // holds the fenced build row
    const install = asIngest(c2, () => call(c2, "ingest_history_scope_install", installArgs(b, s))).then(() => "installed", (e: Error) => e.message);
    await new Promise((r) => setTimeout(r, 300));
    await c1.query("COMMIT");
    expect(await install).toContain("scope build lease lost");
  } finally { c1.release(); c2.release(); }
  expect(await legacy(s)).toEqual(before);
  expect(await required(s.identity.orgId)).toBe(0);
  await summarize(s, "legacy write after the ACK");
  expect((await legacy(s)).short_summary).toBe("legacy write after the ACK");
});

test.skipIf(!url)("install first on an in-flight scope: the concurrent ACK waits for it, then sees the INSTALLED scope and invalidates it", async () => {
  const s = await scope(), b = await captured(s);
  const c1 = await pool.connect(), c2 = await pool.connect();
  try {
    await c2.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    await call(c2, "ingest_history_scope_install", installArgs(b, s));     // holds the build row, uncommitted
    let acked = false;
    const ack = (async () => { await c1.query("BEGIN"); await ackOn(c1, s, "k1"); await c1.query("COMMIT"); acked = true; })();
    await new Promise((r) => setTimeout(r, 300));
    expect(acked).toBe(false);
    await c2.query("COMMIT");
    await ack;
  } finally { c1.release(); c2.release(); }
  expect(await required(s.identity.orgId)).toBe(1);                        // managed only once installed
  expect((await pool.query("SELECT ready FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].ready).toBe(false);
});

// ACK (user V) vs control (user U, an owner of the managed scope V's ACK touches). The production ACK takes
// the receipt seat clock, then its slot UPDATE trigger takes the scope lock; the control INSERTs its
// receipt (trigger: scope lock) and then ingest_lazy_clear takes the seat clock. Without one lock order
// this is a 40P01 deadlock; the control trigger now takes the seat clock FIRST.
test.skipIf(!url)("ACK vs control on a managed scope's owner: both commit (one waits), never 40P01", async () => {
  const s = await scope();
  const ctlEpoch = sha("ctl-epoch-race");
  const c0 = await pool.connect();
  try {
    const b = await asIngest(c0, () => call(c0, "ingest_history_scope_begin", { generation: gen, identity: s.identity }));
    await asIngest(c0, () => call(c0, "ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [{ userId: "U", controlEpoch: ctlEpoch, controlVersion: "1" }] }));
    await asIngest(c0, () => call(c0, "ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken }));
    await asIngest(c0, () => call(c0, "ingest_history_scope_install", installArgs(b, s)));
  } finally { c0.release(); }
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  const ack = await pool.connect(), ctl = await pool.connect();
  try {
    await ack.query("BEGIN");
    await ack.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");          // ingest_lazy_ack_batch:627
    const control = (async () => {
      await ctl.query("BEGIN");
      await ctl.query("INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation) VALUES($1,$2,'U',$3,2,$4,'reset','{}'::jsonb,$5)",
        [s.identity.orgId, s.identity.workspaceId, ctlEpoch, sha("ctl-digest"), gen]);                        // receipt trigger
      await ctl.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");         // ingest_lazy_clear:738
      await ctl.query("COMMIT");
      return "ok";
    })().catch(async (e: any) => { await ctl.query("ROLLBACK").catch(() => {}); return e.code ?? e.message; });
    await new Promise((r) => setTimeout(r, 300));
    const acked = (async () => { await ackOn(ack, s, "k1"); await ack.query("COMMIT"); return "ok"; })()
      .catch(async (e: any) => { await ack.query("ROLLBACK").catch(() => {}); return e.code ?? e.message; });
    expect(await Promise.all([acked, control])).toEqual(["ok", "ok"]);
  } finally { ack.release(); ctl.release(); }
  expect((await pool.query("SELECT ready FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].ready).toBe(false);
});

test.skipIf(!url)("resume (set_live true) refuses (55P03, nothing changed) while a session_index writer from the OFF period is in flight; a retry after it commits succeeds and the backfill sees its row", async () => {
  const s = await scope();
  const backfill = async () => { let c: unknown = null, n = 0; do { c = (await pool.query("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [c === null ? null : JSON.stringify(c)])).rows[0].c; n++; } while (c !== null && n < 100); };
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill();
  await pool.query("SELECT sessions.ingest_history_set_live(false)");
  const writer = await pool.connect(), resumer = await pool.connect();
  try {
    await writer.query("BEGIN");
    await writer.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 5, content_sha256: "\\x" + "d".repeat(64) }])]);   // untracked, uncommitted
    const t0 = Date.now();
    const first = await resumer.query("SELECT sessions.ingest_history_set_live(true)").then(() => "ok", (e: any) => e.code);
    expect(first).toBe("55P03");                                            // never waits on the OFF-era writer
    expect(Date.now() - t0).toBeLessThan(3000);
    expect((await pool.query("SELECT live FROM sessions.ingest_history_switch")).rows[0].live).toBe(false);   // nothing changed
    await writer.query("COMMIT");
    await resumer.query("SELECT sessions.ingest_history_set_live(true)");   // the operator's retry
  } finally { writer.release(); resumer.release(); }
  const sw = (await pool.query("SELECT live,refs_complete FROM sessions.ingest_history_switch")).rows[0];
  expect(sw).toEqual({ live: true, refs_complete: false });
  await backfill();
  expect((await pool.query("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [s.identity.orgId])).rows.map((x) => x.r)).toEqual(["5"]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test.skipIf(!url)("resume -> reinsert over a stale ref concurrent with a running backfill: both commit, no 23505, one current ref", async () => {
  const s = await scope();
  const backfill = async (c: pg.Pool | pg.PoolClient) => { let x: unknown = null, n = 0; do { x = (await c.query("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [x === null ? null : JSON.stringify(x)])).rows[0].c; n++; } while (x !== null && n < 100); };
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill(pool);
  await pool.query("SELECT sessions.ingest_history_set_live(false)");
  await pool.query("DELETE FROM sessions.session_index WHERE org_id=$1", [s.identity.orgId]);
  await pool.query("SELECT sessions.ingest_history_set_live(true)");
  const writer = await pool.connect(), filler = await pool.connect();
  try {
    await writer.query("BEGIN");
    await writer.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 7 }])]);       // replaces the stale ref, uncommitted
    const fill = (async () => { await filler.query("BEGIN"); await backfill(filler); await filler.query("COMMIT"); return "ok"; })()
      .catch(async (e: any) => { await filler.query("ROLLBACK").catch(() => {}); return e.code ?? e.message; });
    await new Promise((r) => setTimeout(r, 300));
    await writer.query("COMMIT");
    expect(await fill).toBe("ok");
  } finally { writer.release(); filler.release(); }
  expect((await pool.query("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [s.identity.orgId])).rows.map((x) => x.r)).toEqual(["7"]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test.skipIf(!url)("prune vs reinsert, prune first: the backfill holds the stale ref, the reinsert waits, the fresh ref survives", async () => {
  const s = await scope();
  const backfill = async (c: pg.Pool | pg.PoolClient) => { let x: unknown = null, n = 0; do { x = (await c.query("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [x === null ? null : JSON.stringify(x)])).rows[0].c; n++; } while (x !== null && n < 100); };
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill(pool);
  await pool.query("SELECT sessions.ingest_history_set_live(false)");
  await pool.query("DELETE FROM sessions.session_index WHERE org_id=$1", [s.identity.orgId]);
  await pool.query("SELECT sessions.ingest_history_set_live(true)");
  const filler = await pool.connect(), writer = await pool.connect();
  try {
    await filler.query("BEGIN");
    await backfill(filler);                                                  // pruned the stale ref, uncommitted (holds it)
    let written = false;
    const write = (async () => { await writer.query("BEGIN"); await writer.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 8 }])]); await writer.query("COMMIT"); written = true; return "ok"; })()
      .catch(async (e: any) => { await writer.query("ROLLBACK").catch(() => {}); return e.code ?? e.message; });
    await new Promise((r) => setTimeout(r, 300));
    expect(written).toBe(false);                                             // waits on the pruned ref's row lock
    await filler.query("COMMIT");
    expect(await write).toBe("ok");
  } finally { filler.release(); writer.release(); }
  expect((await pool.query("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [s.identity.orgId])).rows.map((x) => x.r)).toEqual(["8"]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test.skipIf(!url)("resume vs a control holding session_index (ROW EXCLUSIVE) then wanting the seat clock: resume fails fast with 55P03, the control commits, a retry succeeds; never 40P01", async () => {
  const s = await scope();
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  await pool.query("SELECT sessions.ingest_history_set_live(false)");
  const ctl = await pool.connect(), op = await pool.connect();
  try {
    await ctl.query("BEGIN");
    await ctl.query("UPDATE sessions.session_index SET indexed_at=now() WHERE org_id=$1", [s.identity.orgId]);   // 060000 retire-fence write
    const resume = await op.query("SELECT sessions.ingest_history_set_live(true)").then(() => "ok", (e: any) => e.code ?? e.message);
    expect(resume).toBe("55P03");
    await ctl.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");   // free: resume released it
    await ctl.query("COMMIT");
    expect(await op.query("SELECT sessions.ingest_history_set_live(true)").then(() => "ok", (e: any) => e.code)).toBe("ok");
  } finally { ctl.release(); op.release(); }
  expect((await pool.query("SELECT live FROM sessions.ingest_history_switch")).rows[0].live).toBe(true);
});

test.skipIf(!url)("resume vs the lazy batch-claim order (seat clock, THEN a session_index UPDATE): resume waits for the seat clock, both commit, never 40P01", async () => {
  const s = await scope();
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  await pool.query("SELECT sessions.ingest_history_set_live(false)");
  const claim = await pool.connect(), op = await pool.connect();
  try {
    await claim.query("BEGIN");
    await claim.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");          // ingest_lazy_records 055000:333
    const resume = op.query("SELECT sessions.ingest_history_set_live(true)").then(() => "ok", (e: any) => e.code ?? e.message);
    await new Promise((r) => setTimeout(r, 300));                                                             // resume now queued on the seat clock
    const claimed = (async () => {
      await claim.query("UPDATE sessions.session_index SET indexed_at=now() WHERE org_id=$1", [s.identity.orgId]);   // retire-fence write (060000:466)
      await claim.query("COMMIT"); return "ok";
    })().catch(async (e: any) => { await claim.query("ROLLBACK").catch(() => {}); return e.code ?? e.message; });
    expect(await Promise.all([claimed, resume])).toEqual(["ok", "ok"]);
  } finally { claim.release(); op.release(); }
  expect((await pool.query("SELECT live FROM sessions.ingest_history_switch")).rows[0].live).toBe(true);
});

test.skipIf(!url)("an already-live set_live(true) takes no lock (no-op returns while the seat clock is held)", async () => {
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  const holder = await pool.connect();
  try {
    await holder.query("BEGIN");
    await holder.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
    const t0 = Date.now();
    expect((await pool.query("SELECT sessions.ingest_history_set_live(true) r")).rows[0].r).toEqual({ live: true, changed: false });
    expect(Date.now() - t0).toBeLessThan(1000);
    await holder.query("COMMIT");
  } finally { holder.release(); }
});

test.skipIf(!url)("prune re-verifies under the row lock: a reinsert that commits after the candidate scan keeps its fresh ref", async () => {
  const [k1, k2] = [await scope(), await scope()].sort((a, b) => (a.identity.orgId < b.identity.orgId ? -1 : 1));
  const backfill = async (c: pg.Pool | pg.PoolClient) => { let x: unknown = null, n = 0; do { x = (await c.query("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [x === null ? null : JSON.stringify(x)])).rows[0].c; n++; } while (x !== null && n < 100); };
  const refOf = async (org: string) => (await pool.query("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [org])).rows.map((x) => x.r);
  await pool.query("INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,'{}'::jsonb,$3) ON CONFLICT DO NOTHING", [sha("seat-epoch"), sha("seat-src"), gen]);
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill(pool);
  await pool.query("SELECT sessions.ingest_history_set_live(false)");
  await pool.query("DELETE FROM sessions.session_index WHERE org_id=ANY($1)", [[k1.identity.orgId, k2.identity.orgId]]);   // both refs now stale
  await pool.query("SELECT sessions.ingest_history_set_live(true)");
  const a = await pool.connect(), filler = await pool.connect();
  try {
    await a.query("BEGIN");
    await a.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...k1.row, revision: 4 }])]);    // holds K1's ref
    // The prune scans BOTH stale refs as candidates, then blocks locking K1's.
    const fill = (async () => { await filler.query("BEGIN"); await backfill(filler); await filler.query("COMMIT"); return "ok"; })()
      .catch(async (e: any) => { await filler.query("ROLLBACK").catch(() => {}); return e.code ?? e.message; });
    await new Promise((r) => setTimeout(r, 300));
    // K2 is reinserted and COMMITTED after the candidate scan, before the prune reaches it.
    await pool.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...k2.row, revision: 9 }])]);
    await a.query("COMMIT");
    expect(await fill).toBe("ok");
  } finally { a.release(); filler.release(); }
  expect(await refOf(k1.identity.orgId)).toEqual(["4"]);
  expect(await refOf(k2.identity.orgId)).toEqual(["9"]);                   // not pruned from the old snapshot
  await pool.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});
