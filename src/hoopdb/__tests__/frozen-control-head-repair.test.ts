import { test, expect, afterEach } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { repairControlHead } from "../frozen-control-head-repair.ts";

const root = join(import.meta.dir, "../../..");
const open: PGlite[] = [];
afterEach(async () => { for (const d of open.splice(0)) await d.close(); });
const epoch = "3".repeat(64), d1 = "1".repeat(64), d2 = "2".repeat(64), key = { org: "org", workspace: "ws", user: "u" };

async function fixture() {
  const db = new PGlite(); open.push(db);
  // The mirror table exactly as the reviewed migration defines it; Neon's table has the same columns.
  const mig = readFileSync(join(root, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8");
  const ddl = mig.match(/CREATE TABLE sessions\.ingest_source_control_heads\([\s\S]*?\n\);/)![0];
  await db.exec("CREATE SCHEMA sessions;" + ddl + ddl.replace("ingest_source_control_heads", "delivery_cutover_control_heads"));
  const put = (t: string, v: string, d: string, pending = false) => db.query(`INSERT INTO sessions.${t}(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version)
    VALUES('org','ws','u',$1,$2::bigint,$3,'selection','{"origin_id":"o","sessions":[],"enabled":false}',$4,NULL,$5::bigint)
    ON CONFLICT (org_id,workspace_id,user_id) DO UPDATE SET version=EXCLUDED.version,digest=EXCLUDED.digest,pending=EXCLUDED.pending,privacy_version=EXCLUDED.privacy_version`, [epoch, v, d, pending, String(Number(v) - 1)]);
  const mirror = async () => (await db.query<any>("SELECT version::text v,digest,pending,privacy_version::text pv FROM sessions.ingest_source_control_heads")).rows[0];
  return { db, put, mirror };
}

test("replaces a stale mirror head with Neon's newer completed head, dry run first", async () => {
  const { db, put, mirror } = await fixture();
  await put("ingest_source_control_heads", "1", d1); await put("delivery_cutover_control_heads", "2", d2);
  const dry = await repairControlHead(db, db, key, { dryRun: true });
  expect(dry).toMatchObject({ outcome: "repaired", dryRun: true, before: { version: "1", digest: d1 }, after: { version: "2", digest: d2 } });
  expect((await mirror()).v).toBe("1");
  const r = await repairControlHead(db, db, key);
  expect(r).toMatchObject({ outcome: "repaired", dryRun: false });
  expect(await mirror()).toEqual({ v: "2", digest: d2, pending: false, pv: "1" });
  expect((await repairControlHead(db, db, key)).outcome).toBe("already");
});

test("refuses when either head is pending, Neon is not newer, or a head is missing", async () => {
  const { db, put, mirror } = await fixture();
  await expect(repairControlHead(db, db, key)).rejects.toThrow("neon_head_missing");
  await put("delivery_cutover_control_heads", "2", d2);
  await expect(repairControlHead(db, db, key)).rejects.toThrow("mirror_head_missing_use_copy");
  await put("ingest_source_control_heads", "1", d1, true);
  await expect(repairControlHead(db, db, key)).rejects.toThrow("mirror_head_pending");
  await put("ingest_source_control_heads", "1", d1); await put("delivery_cutover_control_heads", "2", d2, true);
  await expect(repairControlHead(db, db, key)).rejects.toThrow("neon_head_pending");
  await put("delivery_cutover_control_heads", "1", d2);
  await expect(repairControlHead(db, db, key)).rejects.toThrow("neon_not_newer");
  await put("ingest_source_control_heads", "3", d1); await put("delivery_cutover_control_heads", "2", d2);
  await expect(repairControlHead(db, db, key)).rejects.toThrow("neon_not_newer");
  expect((await mirror()).v).toBe("3");
});
