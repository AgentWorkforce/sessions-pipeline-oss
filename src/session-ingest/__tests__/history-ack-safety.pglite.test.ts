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
// 620 must be invisible to the live upload ACK and control paths while dormant, and only ever touch
// MANAGED scopes (an INSTALLED current authority) once active. A build in flight is not managed: a
// live ACK only fences (abandons) it. The writes below are exactly the
// statement-level writes ingest_lazy_ack_batch / slot initialization make on ingest_lazy_native_slots.
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


const hex = (s: string) => shaHex(s);
const SUMMARY = "UPDATE sessions.session_directory SET short_summary=$5,short_summary_scrub_ruleset='relayhistory-scrub',short_summary_scrub_version=1,short_summary_source='bob-costas/extractive/v1',short_summary_generated_at=now() WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4";
const summarize = (s: any, text: string) => db.query(SUMMARY, [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId, text]);
const dir = async (s: any) => (await db.query<any>("SELECT state,short_summary,short_summary_scrub_ruleset r,short_summary_scrub_version v,short_summary_source src,short_summary_generated_at g FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [s.identity.orgId, s.identity.sessionId])).rows[0];
// Initialize a native slot for record `rec`, then ACK it to the scope's session (owner u1), in one statement each.
async function ack(s: any, rec: string, revision = 1) {
  const key = hex("slot:" + s.identity.orgId + ":" + rec);
  await db.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES($1,$2::jsonb,$3,$4,'srv_origin',$5,$6,$7,$8) ON CONFLICT DO NOTHING",
    [key, JSON.stringify(["global-native-custody-v1", s.identity.orgId, s.identity.workspaceId, "srv_origin", rec]), s.identity.orgId, s.identity.workspaceId, rec, hex("src"), hex("ctl"), hex("wit")]);
  await db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1,current_owner='u1',current_source='claude',current_session=$2,current_revision=$3,current_digest=$4,current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=$1",
    [key, s.identity.sessionId, revision, hex("digest:" + revision)]);
}
const counts = async () => (await db.query<any>("SELECT (SELECT count(*)::int FROM sessions.ingest_history_reconcile_outbox) o,(SELECT count(*)::int FROM sessions.ingest_history_required_scopes) q,(SELECT count(*)::int FROM sessions.ingest_history_privacy_outbox) p")).rows[0];

test("dormant: an ACK on a session WITH a summary succeeds and changes nothing 620-owned", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "a legacy summary");
  await ack(s, "k1");
  await ack(s, "k1", 2);
  expect(await dir(s)).toMatchObject({ short_summary: "a legacy summary", r: "relayhistory-scrub", v: 1, src: "bob-costas/extractive/v1" });
  expect(await counts()).toEqual({ o: 0, q: 0, p: 0 });
  await summarize(s, "the legacy summarizer still writes");              // the BEFORE trigger is inert
  expect((await dir(s)).short_summary).toBe("the legacy summarizer still writes");
});

test("dormant: the named operational rollback runs after live ACK activity", async () => {
  const s = await scope(["k1"]);
  await ack(s, "k1");
  const down = readFileSync(join(root, "supabase/rollbacks/" + readdirSync(join(root, "supabase/rollbacks")).find((f) => f.includes("canonical_history_lineage"))!), "utf8");
  await db.exec("BEGIN");
  try { await db.exec(down); } finally { await db.exec("ROLLBACK"); }
});

const LEDGER = "upstream:20261105026000_canonical_history_lineage.sql";
const downSql = () => readFileSync(join(root, "supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql"), "utf8");
const upSql = () => readFileSync(join(root, "supabase/migrations/20261105026000_canonical_history_lineage.sql"), "utf8");
// The migrator's ledger (src/hoopdb/migrate.ts MIGRATION_LEDGER), as it stands with 026000 applied last.
const seedLedger = () => db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now());" +
  "INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES('upstream:20261105025000_canonical_feature_journal.sql','x'),('" + LEDGER + "','x') ON CONFLICT DO NOTHING");
const ledgerNames = async () => (await db.query<any>("SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:2026110502%' ORDER BY name")).rows.map((x) => x.name);

test("down then reapply (dormant): the down drops its ledger row, the migration reapplies, and a live ACK still succeeds", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "survives down/reapply");
  await db.exec("BEGIN");
  try {
    await seedLedger();
    await db.exec(downSql());
    expect(await ledgerNames()).toEqual(["upstream:20261105025000_canonical_feature_journal.sql"]);   // next migrate reapplies 026000
    expect((await db.query<any>("SELECT to_regclass('sessions.ingest_history_scope_builds') r")).rows[0].r).toBeNull();
    await db.exec(upSql());                                                                          // what migrate.ts then runs
    await db.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,'x')", [LEDGER]);
    await ack(s, "k1");
    expect(await dir(s)).toMatchObject({ short_summary: "survives down/reapply", r: "relayhistory-scrub" });
  } finally { await db.exec("ROLLBACK"); }
});

test("rollback after queued work once active (build in flight + fenced ACK, nothing installed): the down runs and reapplies", async () => {
  const s = await scope(["k1"]);
  const { b } = await build(s, ["k1"]);                                   // committed: 620 is now ACTIVE (live lease)
  await ack(s, "k1");                                                      // fenced, queued nothing legacy
  expect((await db.query<any>("SELECT state FROM sessions.ingest_history_scope_builds WHERE build_id=$1", [b.buildId])).rows[0].state).toBe("abandoned");
  await db.exec("BEGIN");
  try {
    await seedLedger();
    await db.exec(downSql());
    await db.exec(upSql());
    await ack(s, "k1", 3);
  } finally { await db.exec("ROLLBACK"); }
});

test("the down refuses (nothing dropped) when a later upstream migration is already applied", async () => {
  await db.exec("BEGIN");
  try {
    await seedLedger();
    await db.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES('upstream:20261105027000_later.sql','x')");
    await db.exec("SAVEPOINT d");
    await expect(db.exec(downSql())).rejects.toThrow("a later migration is applied");
    await db.exec("ROLLBACK TO SAVEPOINT d");
    expect((await db.query<any>("SELECT to_regclass('sessions.ingest_history_scope_builds') r")).rows[0].r).not.toBeNull();
  } finally { await db.exec("ROLLBACK"); }
});

test("active: an unmanaged session keeps its summary through ACKs and summarizer writes", async () => {
  const managed = await scope(["m1"]);
  await install((await build(managed, ["m1"])).b, managed);              // 620 now active somewhere
  const s = await scope(["k1"]);
  await summarize(s, "untouched");
  await ack(s, "k1");
  expect(await dir(s)).toMatchObject({ short_summary: "untouched", r: "relayhistory-scrub" });
  await summarize(s, "rewritten");
  expect((await dir(s)).short_summary).toBe("rewritten");
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
});

test("managed: an ACK invalidates the installed scope and clears the summary WITH all four provenance columns", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "summary before install");
  await install((await build(s, ["k1"])).b, s);
  await ack(s, "k1");
  expect(await dir(s)).toEqual({ state: "unknown", short_summary: null, r: null, v: null, src: null, g: null });
  expect((await db.query<any>("SELECT ready FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].ready).toBe(false);
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_reconcile_outbox o JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=o.native_key_sha256 WHERE n.org_id=$1", [s.identity.orgId])).rows[0].n).toBe(1);
  // A later summarizer write on a managed scope is neutralized consistently (no CHECK violation).
  await summarize(s, "legacy write after management");
  expect(await dir(s)).toMatchObject({ short_summary: null, r: null, v: null, src: null, g: null });
});

const epochOf = async (s: any) => (await db.query<any>("SELECT epoch::text e FROM sessions.session_replay_publications WHERE org_id=$1 AND session_id=$2", [s.identity.orgId, s.identity.sessionId])).rows[0]?.e ?? null;
const classified = async (s: any) => (await db.query<any>("SELECT sessions.ingest_history_scope_managed($1,$2,'claude',$3) m,sessions.ingest_history_scope_required($1,$2,'claude',$3) r,sessions.session_publication_is_managed($1,$2,'claude',$3,'srv_origin') p",
  [s.identity.orgId, s.identity.workspaceId, s.identity.sessionId])).rows[0];
// The statement-level receipt insert ingest_apply_ordered_control makes for one ordered control.
const insertControl = (org: string, ws: string, user: string, epoch: string, version: number) => db.query(
  "INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation) VALUES($1,$2,$3,$4,$5,$6,'selection','{}'::jsonb,$7)",
  [org, ws, user, epoch, version, hex("ctl-digest:" + version), gen]);
const buildState = async (b: any) => (await db.query<any>("SELECT state FROM sessions.ingest_history_scope_builds WHERE build_id=$1", [b.buildId])).rows[0].state;

for (const [label, rec] of [["a registered key", "k1"], ["a new native seated in the scope", "k-new"]] as const)
test("in flight (captured): an ACK on " + label + " only fences the build; legacy summary/provenance, epoch and classification are untouched", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "legacy summary before the ACK");
  const before = await dir(s);
  const { b } = await build(s, ["k1"]);
  const epoch = await epochOf(s);
  expect(epoch).not.toBeNull();
  // A captured build alone is not management: only an installed current authority is.
  expect(await classified(s)).toEqual({ m: false, r: false, p: false });
  await ack(s, rec);
  // No managed conversion: directory row (state, summary and all four provenance columns) unchanged.
  expect(await dir(s)).toEqual(before);
  expect(await epochOf(s)).toBe(epoch);
  expect(await classified(s)).toEqual({ m: false, r: false, p: false });
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_reconcile_outbox o JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=o.native_key_sha256 WHERE n.org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
  // The legacy summarizer still writes (the directory BEFORE trigger stays inert for this session).
  await summarize(s, "legacy summarizer after the ACK");
  expect(await dir(s)).toMatchObject({ short_summary: "legacy summarizer after the ACK", r: "relayhistory-scrub", v: 1, src: "bob-costas/extractive/v1" });
  expect((await dir(s)).g).not.toBeNull();
  // The build is fenced: abandoned, and its install refuses without installing anything.
  expect(await buildState(b)).toBe("abandoned");
  await expect(install(b, s)).rejects.toThrow("scope build lease lost");
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.canonical_feature_journal WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
  expect(await dir(s)).toMatchObject({ short_summary: "legacy summarizer after the ACK" });
});

test("in flight (keys): an ACK on a registered key fences the build before capture; nothing legacy moves", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "keys-phase summary");
  const before = await dir(s);
  const epoch = await epochOf(s);
  const b = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  await rpc("ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [] });
  await ack(s, "k1");
  expect(await dir(s)).toEqual(before);
  expect(await epochOf(s)).toBe(epoch);                                   // the replay epoch never advanced
  expect(await classified(s)).toEqual({ m: false, r: false, p: false });
  expect(await buildState(b)).toBe("abandoned");
  await expect(rpc("ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken })).rejects.toThrow("scope build lease lost");
  // A fresh build after the ACK installs normally.
  const { b: again } = await build(s, ["k1"]);
  expect((await install(again, s)).installed).toBe(true);
});

test("in flight: an ACK on an UNRELATED session does not fence the build", async () => {
  const s = await scope(["k1"]), other = await scope(["z1"]);
  const { b } = await build(s, ["k1"]);
  await ack(other, "z1");
  expect(await buildState(b)).toBe("captured");
  expect((await install(b, s)).installed).toBe(true);
});

test("in flight: a control change for a captured owner fences the build without invalidating", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "control-phase summary");
  const before = await dir(s);
  const b = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  await rpc("ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [{ userId: "u-ctl", controlEpoch: hex("ctl-epoch"), controlVersion: "1" }] });
  await rpc("ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken });
  const epoch = await epochOf(s);
  await insertControl(s.identity.orgId, s.identity.workspaceId, "u-ctl", hex("ctl-epoch"), 2);
  expect(await buildState(b)).toBe("abandoned");
  expect(await dir(s)).toEqual(before);
  expect(await epochOf(s)).toBe(epoch);
  expect(await classified(s)).toEqual({ m: false, r: false, p: false });
  await expect(install(b, s)).rejects.toThrow("scope build lease lost");
});

test("leftover state from an UNINSTALLED build (expired lease, stray required-scope row) never gates legacy writes or reads", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "leftover summary");
  const b = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  await rpc("ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [] });
  await db.query("UPDATE sessions.ingest_history_scope_builds SET lease_until=clock_timestamp()-interval'1 second' WHERE build_id=$1", [b.buildId]);   // crashed builder
  // The leftover row an older revision of 620 could leave behind: a required scope with no installed authority.
  await db.query("INSERT INTO sessions.ingest_history_required_scopes(org_id,workspace_id,harness,session_id) VALUES($1,$2,'claude',$3)", [s.identity.orgId, s.identity.workspaceId, s.identity.sessionId]);
  const epoch = await epochOf(s), before = await dir(s);
  await ack(s, "k1");
  expect(await dir(s)).toEqual(before);
  expect(await epochOf(s)).toBe(epoch);
  expect(await classified(s)).toEqual({ m: false, r: false, p: false });
  expect(await buildState(b)).toBe("keys");                               // an expired lease is not fenced (it can never install)
  await summarize(s, "legacy summarizer still writes");
  expect(await dir(s)).toMatchObject({ short_summary: "legacy summarizer still writes", r: "relayhistory-scrub", v: 1 });
  // The source snapshot names no History authority for it, so replay/brief/large-read/summary gates stay legacy.
  const snap = (await db.query<any>("SELECT sessions.session_replay_source_snapshot($1,$2,'claude',$3) s", [s.identity.orgId, s.identity.workspaceId, s.identity.sessionId])).rows[0].s;
  expect(snap.history).toBeUndefined();
  await db.query("DELETE FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId]);
});

test("the down refuses once an authority is installed and points at the kill switch", async () => {
  await db.exec("BEGIN");
  try {
    await db.exec("SAVEPOINT d");
    await expect(db.exec(downSql())).rejects.toThrow("use sessions.ingest_history_set_live(false)");
    await db.exec("ROLLBACK TO SAVEPOINT d");
  } finally { await db.exec("ROLLBACK"); }
});

test("kill switch after install: OFF makes every hook and gate inert with ledgers kept; ON invalidates installed scopes", async () => {
  const s = await scope(["k1"]);
  await install((await build(s, ["k1"])).b, s);
  const pending = await scope(["p1"]), { b: inflight } = await build(pending, ["p1"]);
  const ledgers = async () => (await db.query<any>("SELECT (SELECT count(*)::int FROM sessions.ingest_history_scope_versions) v,(SELECT count(*)::int FROM sessions.ingest_history_scope_current) c,(SELECT count(*)::int FROM sessions.canonical_feature_journal) j")).rows[0];
  const kept = await ledgers();
  expect((await db.query<any>("SELECT sessions.ingest_history_set_live(false) r")).rows[0].r).toMatchObject({ live: false, changed: true });
  expect(await buildState(inflight)).toBe("abandoned");
  expect((await db.query<any>("SELECT sessions.ingest_history_active() a")).rows[0].a).toBe(false);
  // The installed scope now behaves exactly as main: summarizer writes stick, ACKs move nothing.
  await summarize(s, "written while 620 is off");
  const epoch = await epochOf(s);
  await ack(s, "k1", 7);
  expect(await dir(s)).toMatchObject({ short_summary: "written while 620 is off", r: "relayhistory-scrub" });
  expect(await epochOf(s)).toBe(epoch);
  expect(await classified(s)).toEqual({ m: false, r: false, p: false });
  expect(await ledgers()).toEqual(kept);
  await expect(rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity })).rejects.toThrow("620 is switched off");
  // ON again: the untracked period is invalidated for every installed scope.
  expect((await db.query<any>("SELECT sessions.ingest_history_set_live(true) r")).rows[0].r).toMatchObject({ live: true, changed: true });
  expect(await dir(s)).toMatchObject({ state: "unknown", short_summary: null, r: null, v: null, src: null, g: null });
  expect((await db.query<any>("SELECT ready FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].ready).toBe(false);
  expect(BigInt(await epochOf(s))).toBeGreaterThan(BigInt(epoch));
  expect((await classified(s)).m).toBe(true);
  for (const role of ["bulls96_session_ingest", "service_role", "session_directory_search_worker"]) {
    await db.exec("BEGIN; SET LOCAL ROLE " + role);
    try { await expect(db.query("SELECT sessions.ingest_history_set_live(false)")).rejects.toThrow(/permission denied/); } finally { await db.exec("ROLLBACK"); }
  }
});

test("pointer tracking is dormant until armed; the backfill refuses unarmed, rewrites stale refs and completes the inventory gate", async () => {
  const refs = async (org: string) => (await db.query<any>("SELECT pointer_sha256 FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [org])).rows.length;
  const s = await scope(["k1"]);
  expect(await refs(s.identity.orgId)).toBe(0);                           // the indexer write paid nothing
  await expect(db.query("SELECT sessions.ingest_history_pointer_ref_backfill(NULL,10)")).rejects.toThrow("arm pointer tracking");
  await expect(rpc("ingest_history_inventory_claim", { generation: gen, bucket: "creator-fixture" })).rejects.toThrow("not tracked and backfilled");
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  let cursor: unknown = null, pages = 0;
  do { cursor = (await db.query<any>("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [cursor === null ? null : JSON.stringify(cursor)])).rows[0].c; pages++; } while (cursor !== null && pages < 100);
  expect(cursor).toBeNull();
  expect(await refs(s.identity.orgId)).toBe(1);
  expect((await db.query<any>("SELECT refs_complete FROM sessions.ingest_history_switch")).rows[0].refs_complete).toBe(true);
  // Armed: the trigger now maintains a changed pointer itself.
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 2, content_sha256: "\\x" + "b".repeat(64) }])]);
  const after = (await db.query<any>("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [s.identity.orgId])).rows[0].r;
  expect(after).toBe("2");
  // Disarm: completeness drops; a re-arm needs a fresh pass, which rewrites the stale ref.
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 3, content_sha256: "\\x" + "c".repeat(64) }])]);
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  expect((await db.query<any>("SELECT refs_complete FROM sessions.ingest_history_switch")).rows[0].refs_complete).toBe(false);
  cursor = null; pages = 0;
  do { cursor = (await db.query<any>("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [cursor === null ? null : JSON.stringify(cursor)])).rows[0].c; pages++; } while (cursor !== null && pages < 100);
  expect((await db.query<any>("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [s.identity.orgId])).rows[0].r).toBe("3");
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test("kill -> write -> resume: the pointer attestation is dropped; inventory refuses until a fresh backfill repairs update, delete and insert", async () => {
  const backfill = async () => { let c: unknown = null, n = 0; do { c = (await db.query<any>("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [c === null ? null : JSON.stringify(c)])).rows[0].c; n++; } while (c !== null && n < 100); };
  const sw = async () => (await db.query<any>("SELECT live,pointer_tracking p,refs_complete c FROM sessions.ingest_history_switch")).rows[0];
  const refOf = async (org: string) => (await db.query<any>("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [org])).rows.map((x) => x.r);
  const claim = () => rpc("ingest_history_inventory_claim", { generation: gen, bucket: "kill-resume-none" });
  const upd = await scope(["k1"]), del = await scope(["k1"]);
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill();
  expect(await sw()).toEqual({ live: true, p: true, c: true });
  expect(await refOf(upd.identity.orgId)).toEqual(["1"]);
  expect(await refOf(del.identity.orgId)).toEqual(["1"]);
  await db.query("SELECT sessions.ingest_history_set_live(false)");
  expect(await sw()).toEqual({ live: false, p: true, c: false });             // OFF drops the attestation
  // Untracked writes while OFF: an update, a delete and a brand-new pointer.
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...upd.row, revision: 2, content_sha256: "\\x" + "b".repeat(64) }])]);
  await db.query("DELETE FROM sessions.session_index WHERE org_id=$1", [del.identity.orgId]);
  const ins = await scope(["k1"]);
  expect(await refOf(upd.identity.orgId)).toEqual(["1"]);                    // stale while OFF, as designed
  expect(await refOf(del.identity.orgId)).toEqual(["1"]);
  expect(await refOf(ins.identity.orgId)).toEqual([]);
  await db.query("SELECT sessions.ingest_history_set_live(true)");
  expect(await sw()).toEqual({ live: true, p: true, c: false });              // resume never restores it
  await expect(claim()).rejects.toThrow("not tracked and backfilled");
  await backfill();
  expect(await sw()).toEqual({ live: true, p: true, c: true });
  expect(await refOf(upd.identity.orgId)).toEqual(["2"]);
  expect(await refOf(del.identity.orgId)).toEqual([]);                        // orphan removed
  expect(await refOf(ins.identity.orgId)).toEqual(["1"]);
  expect(await claim()).toEqual({ ticket: null });                            // admitted only now
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test("disarm -> DELETE -> rearm: the full backfill prunes the dangling ref before attesting; a claim refuses a dangling ref", async () => {
  const backfill = async () => { let c: unknown = null, n = 0; do { c = (await db.query<any>("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [c === null ? null : JSON.stringify(c)])).rows[0].c; n++; } while (c !== null && n < 100); };
  const dangling = async () => (await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_pointer_refs f WHERE NOT EXISTS(SELECT 1 FROM sessions.session_index i WHERE i.org_id=f.org_id AND i.workspace_id=f.workspace_id AND i.harness=f.harness AND i.session_id=f.session_id AND i.machine_id=f.machine_id)")).rows[0].n;
  const s = await scope(["k1"]), keep = await scope(["k1"]);
  for (const x of [s, keep]) await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...x.row, bucket: "dangle-bucket", revision: 2 }])]);
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill();
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
  await db.query("DELETE FROM sessions.session_index WHERE org_id=$1", [s.identity.orgId]);   // untracked delete
  expect(await dangling()).toBe(1);
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill();
  expect(await dangling()).toBe(0);
  expect((await db.query<any>("SELECT refs_complete FROM sessions.ingest_history_switch")).rows[0].refs_complete).toBe(true);
  // A dangling ref that still exists (planted here) is never admitted to a scan.
  const carrier = (await db.query<any>("SELECT carrier_sha256 c FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [keep.identity.orgId])).rows[0].c;
  await db.query("INSERT INTO sessions.ingest_history_pointer_refs VALUES($1,$2,'claude','ghost','srv_origin',$3,'{}'::jsonb,$4)", [keep.identity.orgId, keep.identity.workspaceId, carrier, "e".repeat(64)]);
  await expect(rpc("ingest_history_inventory_claim", { generation: gen, bucket: "dangle-bucket" })).rejects.toThrow("dangling pointer references");
  await backfill();                                                         // prunes it
  expect(await dangling()).toBe(0);
  expect((await rpc("ingest_history_inventory_claim", { generation: gen, bucket: "dangle-bucket" })).ticket).not.toBeNull();
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test("resume -> reinsert / update / delete over STALE refs left by OFF: every write repairs the ref (no 23505) before any rebackfill", async () => {
  const backfill = async () => { let c: unknown = null, n = 0; do { c = (await db.query<any>("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [c === null ? null : JSON.stringify(c)])).rows[0].c; n++; } while (c !== null && n < 100); };
  const refOf = async (org: string) => (await db.query<any>("SELECT pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [org])).rows.map((x) => x.r);
  const epochOf = async (org: string) => (await db.query<any>("SELECT c.epoch::text e FROM sessions.ingest_history_carriers c JOIN sessions.ingest_history_pointer_refs f USING(carrier_sha256) WHERE f.org_id=$1", [org])).rows[0]?.e;
  const re = await scope(["k1"]), up = await scope(["k1"]), same = await scope(["k1"]), gone = await scope(["k1"]);
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill();
  await db.query("SELECT sessions.ingest_history_set_live(false)");
  await db.query("DELETE FROM sessions.session_index WHERE org_id=$1", [re.identity.orgId]);                                  // untracked delete
  for (const [x, rev] of [[up, 2], [same, 2], [gone, 2]] as const)
    await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...x.row, revision: rev, content_sha256: "\\x" + "b".repeat(64) }])]);   // untracked updates
  await db.query("SELECT sessions.ingest_history_set_live(true)");
  const before = await epochOf(re.identity.orgId);
  // Reinsert of the deleted pointer: the stale ref at that key is replaced, not a PK violation.
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...re.row, revision: 2 }])]);
  expect(await refOf(re.identity.orgId)).toEqual(["2"]);
  expect(BigInt((await epochOf(re.identity.orgId))!)).toBeGreaterThan(BigInt(before!));      // carrier queued for rescan
  // A tracked change over a stale ref repairs it.
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...up.row, revision: 3, content_sha256: "\\x" + "c".repeat(64) }])]);
  expect(await refOf(up.identity.orgId)).toEqual(["3"]);
  // A pointer-unchanged write over a stale ref still repairs it (no early return on a stale ref).
  await db.query("UPDATE sessions.session_index SET indexed_at=now() WHERE org_id=$1", [same.identity.orgId]);
  expect(await refOf(same.identity.orgId)).toEqual(["2"]);
  // A tracked delete over a stale ref removes it.
  await db.query("DELETE FROM sessions.session_index WHERE org_id=$1", [gone.identity.orgId]);
  expect(await refOf(gone.identity.orgId)).toEqual([]);
  await backfill();
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});

test("reinsert onto a different carrier over a stale ref: the OLD carrier is rescanned too (NEW-key stale-ref repair)", async () => {
  const backfill = async () => { let c: unknown = null, n = 0; do { c = (await db.query<any>("SELECT sessions.ingest_history_pointer_ref_backfill($1::jsonb,5000) c", [c === null ? null : JSON.stringify(c)])).rows[0].c; n++; } while (c !== null && n < 100); };
  const s = await scope(["k1"]);
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(true)");
  await backfill();
  const old = (await db.query<any>("SELECT f.carrier_sha256 k,c.epoch::text e FROM sessions.ingest_history_pointer_refs f JOIN sessions.ingest_history_carriers c USING(carrier_sha256) WHERE f.org_id=$1", [s.identity.orgId])).rows[0];
  await db.query("UPDATE sessions.ingest_history_carriers SET scan_complete=true WHERE carrier_sha256=$1", [old.k]);
  await db.query("SELECT sessions.ingest_history_set_live(false)");
  await db.query("DELETE FROM sessions.session_index WHERE org_id=$1", [s.identity.orgId]);              // untracked: stale ref -> old carrier
  await db.query("SELECT sessions.ingest_history_set_live(true)");
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, bucket: "moved-bucket", revision: 2 }])]);   // reinsert on a NEW carrier
  const ref = (await db.query<any>("SELECT carrier_sha256 k,pointer->>'revision' r FROM sessions.ingest_history_pointer_refs WHERE org_id=$1", [s.identity.orgId])).rows;
  expect(ref.length).toBe(1);
  expect(ref[0].k).not.toBe(old.k);
  expect(ref[0].r).toBe("2");
  const after = (await db.query<any>("SELECT epoch::text e,scan_complete FROM sessions.ingest_history_carriers WHERE carrier_sha256=$1", [old.k])).rows[0];
  expect(BigInt(after.e)).toBeGreaterThan(BigInt(old.e));                                                  // the old carrier must be rescanned
  expect(after.scan_complete).toBe(false);
  await backfill();
  await db.query("SELECT sessions.ingest_history_set_pointer_tracking(false)");
});
