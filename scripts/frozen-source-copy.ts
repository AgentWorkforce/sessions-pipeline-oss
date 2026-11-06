/**
 * P1 frozen-source copy: Neon (frozen by relayhistory 0078 phase=active) -> Supabase
 * sessions.ingest_frozen_source_* (owner-only), then verify per org and seal.
 *
 * Usage (stdin: ONE JSON line {"neonOwnerUrl": "..."}; Supabase owner = the CLI temp login, SET ROLE postgres):
 *   bun scripts/frozen-source-copy.ts --plan                   read-only: per-org Neon row counts, no writes
 *   bun scripts/frozen-source-copy.ts --copy [--org X] [--concurrency 2] [--page 5000] [--max-page-ms 4000]
 *   bun scripts/frozen-source-copy.ts --seal                   verify every org (Neon vs Supabase digest) and write the seal
 *   bun scripts/frozen-source-copy.ts --repair-head --org O --workspace W --user U [--execute]
 *                                                              re-seed ONE stale mirror control head from Neon (dry run by default;
 *                                                              src/hoopdb/frozen-control-head-repair.ts has the refusal rules)
 *
 * Idempotent and resumable: inserts are ON CONFLICT DO NOTHING keyed by the native key, progress is
 * per org (a sealed org is skipped), and the seal is written only when EVERY Neon org verifies.
 * Throttled: at most --concurrency orgs at once; a page slower than --max-page-ms backs off.
 * Never writes Neon. Never prints credentials.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import pg from "pg";
import { SUPA_PAGE, SUPA_RECEIPTS, SUPA_CONTROL_HEADS } from "../src/migration/frozen-source-copy-queries.ts";
import { parseCliLogin } from "./schema-release-owner-operator-plan.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { repairControlHead } from "../src/hoopdb/frozen-control-head-repair.ts";

const REF = "abcdefghijklmnopqrst", HOST = "aws-0-us-east-1.pooler.supabase.com", CLI = process.env.SUPABASE_CLI ?? "supabase";
const args = process.argv.slice(2), flag = (k: string) => args.includes(k), val = (k: string, d: string) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1]! : d; };
const mode = flag("--plan") ? "plan" : flag("--copy") ? "copy" : flag("--seal") ? "seal" : flag("--repair-head") ? "repair-head" : "";
if (!mode) { console.error("one of --plan | --copy | --seal | --repair-head"); process.exit(64); }
const PAGE = Number(val("--page", "5000")), CONC = Number(val("--concurrency", "2")), MAX_PAGE_MS = Number(val("--max-page-ms", "4000")), ONLY = val("--org", "");
// A subset must never become the global completeness seal.
if (flag("--seal") && flag("--org")) {
  console.error("--seal requires the complete source; --org is not allowed");
  process.exit(64);
}
const { neonOwnerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0] || "{}");
if (!/^postgresql:\/\/neondb_owner:.+ep-example-branch-00000000/.test(neonOwnerUrl ?? "")) { console.error("neonOwnerUrl (production owner) required on stdin"); process.exit(64); }
const log = (m: string, f: Record<string, unknown> = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), m, ...f }));

const neon = () => { const c = new pg.Client({ connectionString: neonOwnerUrl, ssl: { rejectUnauthorized: true }, application_name: "p1-frozen-copy-readonly", statement_timeout: 30000 }); return c; };
const supa = async () => {
  const login = parseCliLogin(execFileSync(CLI, ["db", "dump", "--linked", "--project-ref", REF, "--dry-run"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000 }));
  const c = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: `cli_login_postgres.${REF}`, password: login.password,
    ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST }, options: "-c statement_timeout=60000 -c lock_timeout=5000", application_name: "p1-frozen-copy" });
  login.password = "";
  try { await c.connect(); await c.query("SET SESSION ROLE postgres"); return c; }
  catch (error) { await c.end().catch(() => {}); throw error; }
};

// One row's canonical text: identical on both sides (built in JS from fetched values).
const COLS = ["org_id", "workspace_id", "origin_id", "record_id", "revision_id", "revision", "digest", "kind", "source", "session_id", "operation", "user_id", "projection_accepted_seq", "catalog_accepted_seq"] as const;
const rowText = (r: Record<string, unknown>) => JSON.stringify(COLS.map((k) => r[k] === null || r[k] === undefined ? null : String(r[k])));
const MOD = 1n << 64n;
const addDigest = (acc: { n: bigint; sum: bigint }, text: string) => { acc.n += 1n; acc.sum = (acc.sum + BigInt("0x" + createHash("sha256").update(text).digest("hex").slice(0, 16))) % MOD; };

const NEON_PAGE = `SELECT r.org_id,r.workspace_id,r.origin_id,r.record_id,r.revision_id,r.revision,r.digest,r.kind,r.source,r.session_id,r.operation,r.user_id,
  a.accepted_seq AS projection_accepted_seq,k.accepted_seq AS catalog_accepted_seq
 FROM sessions.delivery_records r
 LEFT JOIN LATERAL(SELECT x.accepted_seq FROM sessions.delivery_projection_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) a ON true
 LEFT JOIN LATERAL(SELECT x.accepted_seq FROM sessions.delivery_catalog_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) k ON true
 WHERE r.org_id=$1 AND (r.workspace_id,r.origin_id,r.record_id) > ($2,$3,$4)
 ORDER BY r.workspace_id,r.origin_id,r.record_id LIMIT $5`;

const NEON_RECEIPTS = `SELECT org_id,workspace_id,origin_id,batch_id,digest,receipt::text AS receipt FROM sessions.delivery_receipts
 WHERE org_id=$1 AND (workspace_id,origin_id,batch_id) > ($2,$3,$4) ORDER BY workspace_id,origin_id,batch_id LIMIT $5`;

const receiptText = (r: Record<string, unknown>) => JSON.stringify([r.org_id, r.workspace_id, r.origin_id, r.batch_id, r.digest, r.receipt]);
async function scanReceipts(c: pg.Client, sql: string, org: string, onPage: (rows: Record<string, unknown>[]) => Promise<void>) {
  let last = ["", "", ""];
  for (;;) {
    const t = Date.now();
    const rows = (await c.query(sql, [org, ...last, Math.min(PAGE, 1000)])).rows;
    if (!rows.length) return;
    await onPage(rows);
    const tail = rows[rows.length - 1]!; last = [tail.workspace_id, tail.origin_id, tail.batch_id];
    const ms = Date.now() - t; if (ms > MAX_PAGE_MS) await new Promise((r) => setTimeout(r, Math.min(ms, 15000)));
    if (rows.length < Math.min(PAGE, 1000)) return;
  }
}
async function frozenPhase(n: pg.Client) {
  const s = (await n.query("SELECT sessions.delivery_cutover_status() AS s")).rows[0].s;
  if (s?.phase !== "active") throw new Error("neon cutover phase is not active; legacy tables are not frozen");
  return s;
}
async function orgs(n: pg.Client): Promise<string[]> {
  // delivery_origins (661 rows) names every origin a record can belong to; never scan the 66M-row table for this.
  return (await n.query("SELECT org_id FROM (SELECT DISTINCT org_id FROM sessions.delivery_origins) q ORDER BY org_id COLLATE \"C\"")).rows.map((r) => r.org_id as string);
}

async function scan(c: pg.Client, sql: string, org: string, onPage: (rows: Record<string, unknown>[]) => Promise<void>) {
  let last = ["", "", ""];
  for (;;) {
    const t = Date.now();
    const rows = (await c.query(sql, [org, ...last, PAGE])).rows;
    if (!rows.length) return;
    await onPage(rows);
    const tail = rows[rows.length - 1]!; last = [tail.workspace_id, tail.origin_id, tail.record_id];
    const ms = Date.now() - t; if (ms > MAX_PAGE_MS) await new Promise((r) => setTimeout(r, Math.min(ms, 15000)));
    if (rows.length < PAGE) return;
  }
}

async function copyOrg(org: string, s: pg.Client, check: () => void) {
  check();
  const n = neon();
  try {
    await n.connect();
    await n.query("BEGIN READ ONLY");
    const sealed = (await s.query("SELECT 1 FROM sessions.ingest_frozen_source_org_seals WHERE org_id=$1", [org])).rows.length;
    if (sealed) { log("org already sealed; skip", { org }); return; }
    let copied = 0;
    await scan(n, NEON_PAGE, org, async (rows) => {
      check();
      const col = (k: string) => rows.map((r) => r[k] === null || r[k] === undefined ? null : String(r[k]));
      await s.query(`INSERT INTO sessions.ingest_frozen_source_records(${COLS.join(",")})
        SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::bigint[],$7::text[],$8::text[],$9::text[],$10::text[],$11::text[],$12::text[],$13::bigint[],$14::bigint[])
        ON CONFLICT DO NOTHING`, COLS.map(col));
      copied += rows.length;
    });
    let receipts = 0;
    await scanReceipts(n, NEON_RECEIPTS, org, async (rows) => {
      check();
      const col = (k: string) => rows.map((r) => String(r[k]));
      await s.query(`INSERT INTO sessions.ingest_frozen_source_receipts(org_id,workspace_id,origin_id,batch_id,digest,receipt)
        SELECT a,b,c,d,e,f::jsonb FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::text[]) AS u(a,b,c,d,e,f) ON CONFLICT DO NOTHING`,
        ["org_id", "workspace_id", "origin_id", "batch_id", "digest", "receipt"].map(col));
      receipts += rows.length;
    });
    await n.query("ROLLBACK");
    log("org copied", { org, rows: copied, receipts });
  } finally { await n.end().catch(() => {}); }
}

async function repairHead() {
  const key = { org: val("--org", ""), workspace: val("--workspace", ""), user: val("--user", "") };
  if (!key.org || !key.user || !args.includes("--workspace") || [key.org, key.workspace, key.user].some((v) => v.startsWith("--"))) { console.error("--repair-head needs --org, --workspace (may be empty string) and --user"); process.exit(64); }
  const n = neon();
  try {
    await n.connect();
    const s = await supa();
    try {
      const r = await repairControlHead(n, s, key, { dryRun: !flag("--execute") });
      log("control head repair", { ...r });
    } finally { await s.end().catch(() => {}); }
  } finally { await n.end().catch(() => {}); }
}

async function main() {
  if (mode === "repair-head") return repairHead();
  const n = neon();
  try {
    await n.connect();
    await n.query("BEGIN READ ONLY");
    const status = await frozenPhase(n);
    const all = ONLY ? [ONLY] : await orgs(n);
    if (mode === "plan") {
      const est = (await n.query("SELECT reltuples::bigint AS n FROM pg_class WHERE oid='sessions.delivery_records'::regclass")).rows[0].n;
      log("plan", { phase: status.phase, epoch: status.epoch, orgs: all.length, estimatedRecords: String(est) });
      return;
    }
    await n.query("ROLLBACK");
    if (mode === "copy") {
      const s = await supa();
      try {
        // Privacy state is tiny; copy it whole, idempotently.
        const del = (await n.query("SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions")).rows;
        for (const d of del) await s.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", [d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id]);
        const rs = (await n.query("SELECT org_id,workspace_id,user_id,request_id,deleted FROM sessions.session_resets")).rows;
        for (const r of rs) await s.query("INSERT INTO sessions.ingest_frozen_source_resets VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", [r.org_id, r.workspace_id, r.user_id, r.request_id, r.deleted]);
        // Neon control heads written before relayhistory began mirroring them: seed the mirror so it is in lockstep.
        const heads = (await n.query("SELECT org_id,workspace_id,user_id,epoch,version::text AS version,digest,kind,payload,pending,before_privacy,privacy_version::text AS privacy_version FROM sessions.delivery_cutover_control_heads")).rows;
        for (const h of heads) {
          const got = (await s.query("SELECT version::text AS version,digest,pending FROM sessions.ingest_source_control_heads WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [h.org_id, h.workspace_id, h.user_id])).rows[0];
          if (got && (got.version !== h.version || got.digest !== h.digest)) throw new Error("control head mirror disagrees with Neon for a user; refusing");
          if (!got) await s.query("INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version) VALUES($1,$2,$3,$4,$5::bigint,$6,$7,$8,$9,$10,$11::bigint)",
            [h.org_id, h.workspace_id, h.user_id, h.epoch, h.version, h.digest, h.kind, JSON.stringify(h.payload), h.pending, h.before_privacy === null ? null : JSON.stringify(h.before_privacy), h.privacy_version]);
        }
        log("privacy copied", { deletions: del.length, resets: rs.length, controlHeads: heads.length });
        // CLI temporary-login refreshes invalidate prior passwords. Keep one
        // authenticated owner connection for the entire job; pg.Client queues
        // target statements while at most CONC source readers run concurrently.
        const queue = [...all]; let done = 0, failure: unknown;
        const check = () => { if (failure !== undefined) throw failure; };
        await Promise.all(Array.from({ length: Math.max(1, CONC) }, async () => {
          try {
            for (let org = queue.shift(); org; org = queue.shift()) {
              check(); await copyOrg(org, s, check);
              done++; log("progress", { done, of: all.length });
            }
          } catch (error) { if (failure === undefined) failure = error ?? new Error("copy_worker_failed"); }
        }));
        if (failure !== undefined) throw failure;
      } finally { await s.end().catch(() => {}); }
      return;
    }
    // --seal: verify every Neon org, then write org seals + the completeness seal in one transaction.
    const s = await supa();
    try {
      const results: { org: string; n: bigint; sum: bigint }[] = [];
      for (const org of all) {
        const a = { n: 0n, sum: 0n }, b = { n: 0n, sum: 0n };
        await n.query("BEGIN READ ONLY");
        await scan(n, NEON_PAGE, org, async (rows) => { for (const r of rows) addDigest(a, rowText(r)); });
        await n.query("ROLLBACK");
        await scan(s, SUPA_PAGE, org, async (rows) => { for (const r of rows) addDigest(b, rowText(r)); });
        if (a.n !== b.n || a.sum !== b.sum) throw new Error(`org ${org} mismatch: neon ${a.n}/${a.sum} supabase ${b.n}/${b.sum}`);
        const ra = { n: 0n, sum: 0n }, rb = { n: 0n, sum: 0n };
        await n.query("BEGIN READ ONLY");
        await scanReceipts(n, NEON_RECEIPTS, org, async (rows) => { for (const r of rows) addDigest(ra, receiptText(r)); });
        await n.query("ROLLBACK");
        await scanReceipts(s, SUPA_RECEIPTS, org, async (rows) => { for (const r of rows) addDigest(rb, receiptText(r)); });
        if (ra.n !== rb.n || ra.sum !== rb.sum) throw new Error(`org ${org} receipt mismatch: neon ${ra.n} supabase ${rb.n}`);
        results.push({ org, n: a.n, sum: a.sum });
        log("org verified", { org, rows: String(a.n) });
      }
      const extra = (await s.query("SELECT count(DISTINCT org_id)::int AS n FROM sessions.ingest_frozen_source_records WHERE NOT (org_id = ANY($1))", [all])).rows[0].n;
      if (extra) throw new Error(`${extra} orgs in Supabase copy are not in Neon`);
      const priv = { n: 0n, sum: 0n };
      for (const d of (await n.query("SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions")).rows) addDigest(priv, JSON.stringify(Object.values(d)));
      const privS = { n: 0n, sum: 0n };
      for (const d of (await s.query("SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.ingest_frozen_source_deletions")).rows) addDigest(privS, JSON.stringify(Object.values(d)));
      if (priv.n !== privS.n || priv.sum !== privS.sum) throw new Error("privacy copy mismatch");
      const nh = (await n.query("SELECT org_id,workspace_id,user_id,version::text v,digest,pending FROM sessions.delivery_cutover_control_heads ORDER BY 1,2,3")).rows;
      const sh = (await s.query(SUPA_CONTROL_HEADS)).rows;
      const key = (r: any) => [r.org_id, r.workspace_id, r.user_id, r.v, r.digest, r.pending].join("|");
      if (JSON.stringify(nh.map(key).sort()) !== JSON.stringify(sh.map(key).sort())) throw new Error("control head mirror differs from Neon");
      const pol = (await s.query("SELECT source_identity_sha256,control_epoch,privacy_baseline_sha256 FROM sessions.ingest_lazy_policy WHERE singleton")).rows[0];
      if (!pol || pol.control_epoch !== status.epoch || pol.privacy_baseline_sha256 !== status.privacyBaselineSha256) throw new Error("live policy does not match the Neon cutover descriptor");
      const manifest = results.map((r) => `${r.org}\t${r.n}\t${r.sum}`).join("\n") + `\nprivacy\t${priv.n}\t${priv.sum}\n`;
      const manifestSha = createHash("sha256").update(manifest).digest("hex");
      await s.query("BEGIN");
      for (const r of results) await s.query("INSERT INTO sessions.ingest_frozen_source_org_seals(org_id,record_rows,record_sum,privacy_rows,privacy_sum) VALUES($1,$2,$3,0,0) ON CONFLICT (org_id) DO UPDATE SET record_rows=EXCLUDED.record_rows,record_sum=EXCLUDED.record_sum", [r.org, String(r.n), String(r.sum)]);
      await s.query(`INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (singleton) DO NOTHING`,
        [pol.source_identity_sha256, pol.control_epoch, pol.privacy_baseline_sha256, results.length, String(results.reduce((a, r) => a + r.n, 0n)), manifestSha, JSON.stringify({ cutover: status, sealedFrom: "neon-frozen", privacyRows: String(priv.n) })]);
      await s.query("COMMIT");
      log("sealed", { orgs: results.length, manifestSha });
    } finally { await s.end().catch(() => {}); }
  } finally { await n.end().catch(() => {}); }
}
main().catch((e) => { log("STOPPED", { why: String(e?.message ?? e).slice(0, 300) }); process.exitCode = 1; });
