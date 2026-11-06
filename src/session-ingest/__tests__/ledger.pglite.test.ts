/**
 * The session ingest ledger (20261104022000_session_ingest_ledger.sql) in PGlite
 * with every Supabase migration and the role bootstrap, called through
 * `sqlLedger` AS bulls96_session_ingest (SET ROLE), as production logs in.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { dirname,join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { LedgerUnavailableError, type ClaimInput, type CommitInput, type LedgerRecord } from "../ledger.ts";
import { sqlLedger } from "../ledger-sql.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
function sql(path:string):string{return read(path).split("\n").map(line=>line.startsWith("\\ir ")?sql(join(dirname(path),line.slice(4).trim())):line.startsWith("\\")?"":line).join("\n");}
const ROLE = sql("supabase/bootstrap/session_ingest_role.sql");
const VERIFY = sql("supabase/bootstrap/session_ingest_role_verify.sql");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
    await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(ROLE);
  return db;
}

const attempts=new Map<string,string>();
const attemptKey=(c:ClaimInput)=>JSON.stringify([c.tenant,c.originId,c.batchId,c.batchDigest]);
const attempt=(c:ClaimInput)=>({batchId:c.batchId,batchDigest:c.batchDigest,claimToken:attempts.get(attemptKey(c))??"00000000-0000-0000-0000-000000000000"});
/** Each call runs as `role`, the way one pooled login would. */
function runAs(db: PGlite, role = "bulls96_session_ingest") {
  return async (text: string, params: unknown[]) => {
    await db.exec(`SET ROLE ${role}`);
    try {
      const out=(await db.query(text, params)) as { rows: Record<string, unknown>[] };
      if(text.includes("ingest_claim_batch")&&params[0]) {
        const result=out.rows[0]?.result as {claimToken?:string};
        if(result?.claimToken)attempts.set(attemptKey(JSON.parse(String(params[0]))),result.claimToken);
      }
      return out;
    } finally {
      await db.exec("RESET ROLE");
    }
  };
}

const hex = (c: string) => c.repeat(64);
const tenant = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const MACHINE = `srv_${"ab".repeat(16)}`;
const rec = (
  recordId: string,
  revision: number,
  digestChar: string,
  revisionId = `${recordId}:r${revision}`,
  session: [string, string | null] = ["claude", "s-1"],
): LedgerRecord => ({
  source: session[0],
  sessionId: session[1],
  recordId,
  revisionId,
  revision,
  digest: hex(digestChar),
});
function claimInput(over: Partial<ClaimInput> = {}): ClaimInput {
  return {
    tenant,
    originId: "origin-1",
    batchId: "batch-1",
    batchDigest: hex("a"),
    receiptId: `rhr_${hex("e")}`,
    sessions: [["claude", "s-1"]],
    records: [rec("r1", 1, "1")],
    ...over,
  };
}
const commitInput = (c: ClaimInput, sessions = c.sessions, records = c.records): CommitInput => ({
  tenant: c.tenant,
  originId: c.originId,
  batchId: c.batchId,
  batchDigest: c.batchDigest,
  claimToken:attempt(c).claimToken,
  sessions,
  records,
  machineId: MACHINE,
});
const MIGRATION = read("supabase/migrations/20261104022000_session_ingest_ledger.sql");
const revisions = async (db: PGlite) =>
  (await db.query("SELECT record_id, revision, revision_id, digest FROM sessions.ingest_record_revisions ORDER BY record_id")).rows;
const state = async (db: PGlite, batchId = "batch-1") =>
  (await db.query<{ state: string }>("SELECT state FROM sessions.ingest_batches WHERE batch_id = $1", [batchId])).rows[0]?.state;
/** claim then commit with every record written: the records become committed revisions. */
async function land(ledger: ReturnType<typeof sqlLedger>, c: ClaimInput) {
  const r = await ledger.claim(c);
  if (r.kind !== "claimed" && r.kind !== "resume") throw new Error(`expected a claim, got ${r.kind}`);
  await ledger.commit(commitInput(c));
}

describe("session ingest roles: provisioned by the bootstrap, verified by the migration (review High 1)", () => {
  const MIGRATION = "supabase/migrations/20261104022000_session_ingest_ledger.sql";
  async function upTo(bootstrap: (db: PGlite) => Promise<void>) {
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    open.push(db);
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
    await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
    await bootstrap(db);
    for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
      if (`supabase/migrations/${file}` === MIGRATION) return { db, error: await db.exec(read(MIGRATION)).then(() => null, (e: { code?: string; message?: string }) => e) };
      await db.exec(read(`supabase/migrations/${file}`));
    }
    throw new Error("migration not found");
  }

  test("the migration creates and alters no role (a CNPG / Cloud SQL owner has no CREATEROLE)", () => {
    const sqlText = read(MIGRATION).replace(/--[^\n]*/g, "");
    expect(sqlText).not.toMatch(/\b(CREATE|ALTER|DROP)\s+ROLE\b/i);
  });

  test("without the bootstrap the migration fails closed, 42704, naming the bootstrap", async () => {
    const { error } = await upTo(async () => {});
    expect(error?.code).toBe("42704");
    expect(error?.message).toContain("bulls96_session_ingest");
  });

  test("an elevated role fails the migration closed, 55000", async () => {
    const elevated = await upTo(async (db) => {
      await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
      await db.exec("ALTER ROLE bulls96_session_ingest_seeder INHERIT");
    });
    expect(elevated.error?.code).toBe("55000");
  });

  test("a member role fails the migration closed, 55000", async () => {
    const member = await upTo(async (db) => {
      await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
      await db.exec("GRANT service_role TO bulls96_session_ingest");
    });
    expect(member.error?.code).toBe("55000");
  });

  test("the bootstrap is idempotent and refuses to alter an elevated existing role", async () => {
    const ok = await upTo(async (db) => {
      await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
      await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
    });
    expect(ok.error).toBeNull();
    await expect(ok.db.exec("ALTER ROLE bulls96_session_ingest_seeder INHERIT").then(() => ok.db.exec(read("supabase/bootstrap/session_ingest_roles.sql")))).rejects.toMatchObject({ code: "55000" });
  });
});

describe("session ingest ledger", () => {
  test("no advisory lock of any kind in the migration", () => {
    expect(MIGRATION).not.toMatch(/pg_advisory|pg_try_advisory/i);
  });

  test("the bootstrap gives the role exactly the approved shape", async () => {
    const db = await database();
    await db.exec(VERIFY);
    await db.exec("GRANT SELECT ON sessions.ingest_batches TO bulls96_session_ingest");
    await expect(db.exec(VERIFY)).rejects.toThrow(/privilege on sessions.ingest_batches/);
  });

  test("the verify script catches EXECUTE on another definer function (the seed included)", async () => {
    const db = await database();
    await db.exec("GRANT EXECUTE ON FUNCTION sessions.ingest_seed_records(jsonb) TO bulls96_session_ingest");
    await expect(db.exec(VERIFY)).rejects.toThrow(/EXECUTE on sessions.ingest_seed_records\(jsonb\)/);
  });

  test("the role reads no ledger table directly", async () => {
    const db = await database();
    await expect(runAs(db)("SELECT * FROM sessions.ingest_batches", [])).rejects.toThrow(/permission denied/);
  });

  test("claim, commit, then replay", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    expect(await ledger.claim(c)).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [], stale: [], resets: 0 });
    expect(await state(db)).toBe("pending");
    expect(await revisions(db)).toEqual([]);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 0 });
    expect(await state(db)).toBe("committed");
    expect(await revisions(db)).toEqual([{ record_id: "r1", revision: 1, revision_id: "r1:r1", digest: hex("1") }]);
    expect(await ledger.claim(c)).toEqual({ kind: "replay", receiptId: c.receiptId });
  });

  test("a resume after a failed commit still reports its records as not stale", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({ records: [rec("r1", 1, "1"), rec("r2", 1, "1")] });
    await ledger.claim(c);
    // The commit never ran (the writer stopped): nothing is stored, so a retry writes again.
    expect(await ledger.claim(c)).toEqual({claimToken:expect.any(String), kind: "resume", deleted: [], stale: [], resets: 0 });
  });

  test("a record of a still-pending batch is not stale for another batch; after commit it is", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await ledger.claim(c);
    const other = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r1", 1, "1"), rec("r5", 1, "5")] });
    expect(await ledger.claim(other)).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [], stale: [], resets: 0 });
    await ledger.commit(commitInput(c));
    const third = claimInput({ batchId: "batch-3", batchDigest: hex("c"), records: [rec("r5", 1, "5"), rec("r1", 1, "1")] });
    expect(await ledger.claim(third)).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [], stale: [1], resets: 0 });
  });

  test("another digest under a batch id is a batch conflict, pending or committed", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await ledger.claim(c);
    const other = claimInput({ batchDigest: hex("b") });
    expect(await ledger.claim(other)).toEqual({ kind: "batch_conflict", currentDigest: c.batchDigest });
    await ledger.commit(commitInput(c));
    expect(await ledger.claim(other)).toEqual({ kind: "batch_conflict", currentDigest: c.batchDigest });
    expect((await db.query("SELECT * FROM sessions.ingest_batches")).rows.length).toBe(1);
  });

  test("#250: an equal stored revision with another digest is a replay and keeps the stored row", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 1, "1"), rec("r2", 1, "1")] }));
    const c = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r1", 1, "9", "r1:other"), rec("r3", 1, "3")] });
    expect(await ledger.claim(c)).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [], stale: [0], resets: 0 });
    // Even if a caller committed it as written, the equal revision keeps the stored row.
    await ledger.commit(commitInput(c));
    expect(await revisions(db)).toEqual([
      { record_id: "r1", revision: 1, revision_id: "r1:r1", digest: hex("1") },
      { record_id: "r2", revision: 1, revision_id: "r2:r1", digest: hex("1") },
      { record_id: "r3", revision: 1, revision_id: "r3:r1", digest: hex("3") },
    ]);
  });

  test("a lower revision is stale and never moves the record back", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 5, "5"), rec("r2", 1, "1")] }));
    // Stale even when reused with other content in the batch: no twin 409 below the stored revision.
    const c = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r2", 2, "2"), rec("r1", 3, "3", "x"), rec("r1", 3, "4", "y")] });
    expect(await ledger.claim(c)).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [], stale: [1, 2], resets: 0 });
    await ledger.commit(commitInput(c));
    expect((await revisions(db)).map((r) => [r.record_id, r.revision])).toEqual([["r1", 5], ["r2", 2]]);
  });

  test("a higher revision moves the record forward at commit, the highest of the batch", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 1, "1")] }));
    await land(ledger, claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r1", 2, "2"), rec("r1", 4, "4")] }));
    expect(await revisions(db)).toEqual([{ record_id: "r1", revision: 4, revision_id: "r1:r4", digest: hex("4") }]);
  });

  test("a new revision reused in the batch with other content is a 409 against its first peer", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const r = await ledger.claim(claimInput({ records: [rec("r1", 2, "2", "r1:b"), rec("r1", 2, "3", "r1:a")] }));
    expect(r).toEqual({
      kind: "revision_conflict",
      count: 1,
      conflicts: [
        { recordId: "r1", submittedRevisionId: "r1:b", submittedRevision: 2, submittedDigest: hex("2"), currentRevisionId: "r1:a", currentRevision: 2, currentDigest: hex("3") },
      ],
    });
    expect(await state(db)).toBeUndefined();
    // Above a stored lower revision it is still new, so still a 409.
    await land(ledger, claimInput({ batchId: "batch-0", batchDigest: hex("0"), records: [rec("r1", 1, "1")] }));
    expect((await ledger.claim(claimInput({ records: [rec("r1", 2, "2", "r1:b"), rec("r1", 2, "3", "r1:a")] }))).kind).toBe("revision_conflict");
  });

  test("twin conflicts are capped at 100, in identity order, with the total count", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const ids = Array.from({ length: 150 }, (_, i) => `r${String(i).padStart(3, "0")}`);
    const records = [...ids].reverse().flatMap((id) => [rec(id, 1, "1", `${id}:b`), rec(id, 1, "2", `${id}:a`)]);
    const r = await ledger.claim(claimInput({ records: records.slice(0, 300) }));
    if (r.kind !== "revision_conflict") throw new Error(`expected a conflict, got ${r.kind}`);
    expect(r.count).toBe(150);
    expect(r.conflicts.length).toBe(100);
    expect(r.conflicts.map((c) => c.recordId)).toEqual(ids.slice(0, 100));
  });

  test("replay_fast: every record stale or deleted commits the batch in one call, and a re-send replays", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 3, "3"), rec("r2", 1, "1")] }));
    await db.query(`INSERT INTO sessions.upload_deletions VALUES ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-9', now())`);
    const old = claimInput({
      batchId: "batch-old",
      batchDigest: hex("d"),
      sessions: [["claude", "s-1"], ["claude", "s-9"]],
      records: [rec("r1", 2, "7"), rec("r2", 1, "8"), rec("r7", 1, "7", "r7:r1", ["claude", "s-9"])],
    });
    let calls = 0;
    const counting = sqlLedger(async (text, params) => {
      calls++;
      return runAs(db)(text, params);
    });
    expect(await counting.claim(old)).toEqual({ kind: "replay_fast", deleted: [["claude", "s-9"]], stale: [0, 1], resets: 0 });
    expect(calls).toBe(1);
    expect(await state(db, "batch-old")).toBe("committed");
    expect(await ledger.claim(old)).toEqual({ kind: "replay", receiptId: old.receiptId });
    // Nothing moved.
    expect((await revisions(db)).map((r) => [r.record_id, r.revision])).toEqual([["r1", 3], ["r2", 1]]);
  });

  test("a resumed pending batch whose records were committed meanwhile turns into replay_fast", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await ledger.claim(c);
    await land(ledger, claimInput({ batchId: "batch-2", batchDigest: hex("b") }));
    expect(await ledger.claim(c)).toEqual({ kind: "replay_fast", deleted: [], stale: [0], resets: 0 });
    expect(await state(db)).toBe("committed");
  });

  test("an empty batch (every record retired) is replay_fast", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    expect(await ledger.claim(claimInput({ records: [], sessions: [] }))).toEqual({ kind: "replay_fast", deleted: [], stale: [], resets: 0 });
  });

  test("the batch's deleted sessions come back; other users' and origins' tombstones do not", async () => {
    const db = await database();
    await db.query(
      `INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES
         ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-2'),
         ('org-a', 'ws-a', 'user-2', 'origin-1', 'claude', 's-1'),
         ('org-a', 'ws-a', 'user-1', 'origin-2', 'claude', 's-1')`,
    );
    const ledger = sqlLedger(runAs(db));
    const r = await ledger.claim(claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"], ["codex", "s-2"]] }));
    expect(r).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [["claude", "s-2"]], stale: [], resets: 0 });
  });

  test("records of a deleted session are not judged: a twin there is no 409", async () => {
    const db = await database();
    await db.query(`INSERT INTO sessions.upload_deletions VALUES ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-2', now())`);
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({
      sessions: [["claude", "s-1"], ["claude", "s-2"]],
      records: [rec("r1", 1, "1"), rec("r9", 1, "1", "r9:a", ["claude", "s-2"]), rec("r9", 1, "2", "r9:b", ["claude", "s-2"])],
    });
    expect(await ledger.claim(c)).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [["claude", "s-2"]], stale: [], resets: 0 });
    // A record with no session is judged as before.
    const none = claimInput({ batchId: "batch-3", batchDigest: hex("c"), sessions: [], records: [rec("r8", 1, "1", "a", ["claude", null]), rec("r8", 1, "2", "b", ["claude", null])] });
    expect((await ledger.claim(none)).kind).toBe("revision_conflict");
  });

  test("claim reports the user's reset count", async () => {
    const db = await database();
    await db.query(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id) VALUES
      ('org-a', 'ws-a', 'user-1', 'q1'), ('org-a', 'ws-a', 'user-1', 'q2'), ('org-a', 'ws-a', 'user-2', 'q3')`);
    const ledger = sqlLedger(runAs(db));
    expect(await ledger.claim(claimInput())).toEqual({ claimToken:expect.any(String),kind: "claimed", deleted: [], stale: [], resets: 2 });
  });

  test("a tombstone after the claim re-arms erasure at commit, once", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"]] });
    await ledger.claim(c);
    await db.query(`INSERT INTO sessions.upload_deletions VALUES ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-2', now())`);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 1 });
    expect((await db.query("SELECT machine_id, source, session_id FROM sessions.upload_erasure_rearm")).rows).toEqual([
      { machine_id: MACHINE, source: "claude", session_id: "s-2" },
    ]);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 1 });
    expect((await db.query("SELECT * FROM sessions.upload_erasure_rearm")).rows.length).toBe(1);
    expect((await revisions(db)).length).toBe(1);
  });

  test("a reset after the claim re-arms every written session; one before it does not", async () => {
    const db = await database();
    await db.query(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at) VALUES ('org-a', 'ws-a', 'user-1', 'old', now() - interval '1 hour')`);
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"]] });
    await ledger.claim(c);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 0 });
    const c2 = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r2", 1, "1")], sessions: [["claude", "s-1"], ["claude", "s-2"]] });
    await ledger.claim(c2);
    await ledger.resetUploadedSessions(tenant,"new");
    expect(await ledger.commit(commitInput(c2))).toEqual({ rearmed: 2 });
  });

  test("commit without a claim for this digest is refused", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await expect(ledger.commit(commitInput(c))).rejects.toMatchObject({ code: "ledger_no_claim" });
    await ledger.claim(c);
    await expect(ledger.commit({ ...commitInput(c), batchDigest: hex("b") })).rejects.toMatchObject({ code: "ledger_no_claim" });
    expect(await revisions(db)).toEqual([]);
  });

  test("slots: limited per user and per org, refreshed by their holder, freed by release and by TTL", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const req = (holder: string, userId = "user-1", over: Partial<{ maxPerUser: number; maxPerOrg: number; ttlSeconds: number }> = {}) => ({
      tenant: { ...tenant, userId },
      holder,
      maxPerUser: 2,
      maxPerOrg: 3,
      ttlSeconds: 90,
      ...over,
    });
    expect(await ledger.acquireSlot(req("h1"))).toEqual({ ok: true });
    expect(await ledger.acquireSlot(req("h2"))).toEqual({ ok: true });
    const busy = await ledger.acquireSlot(req("h3"));
    expect(busy.ok).toBe(false);
    if (!busy.ok) expect(busy.retryAfterSeconds).toBeGreaterThanOrEqual(2);
    if (!busy.ok) expect(busy.retryAfterSeconds).toBeLessThanOrEqual(10);
    // The holder itself re-acquires (a retry) at the limit.
    expect(await ledger.acquireSlot(req("h2"))).toEqual({ ok: true });
    // Per org: a second user takes the third org slot, a fourth is refused.
    expect(await ledger.acquireSlot(req("h4", "user-2"))).toEqual({ ok: true });
    expect((await ledger.acquireSlot(req("h5", "user-3"))).ok).toBe(false);
    // Another org is not affected.
    expect(await ledger.acquireSlot({ ...req("h6"), tenant: { ...tenant, orgId: "org-b" } })).toEqual({ ok: true });
    await ledger.releaseSlot(tenant, "h1");
    expect(await ledger.acquireSlot(req("h3"))).toEqual({ ok: true });
    // A crashed holder's lease expires.
    await db.query("UPDATE sessions.ingest_tenant_slots SET expires_at = now() - interval '1 second' WHERE holder = 'h2'");
    await ledger.releaseSlot(tenant, "h3");
    expect(await ledger.acquireSlot(req("h7"))).toEqual({ ok: true });
    expect((await db.query("SELECT holder FROM sessions.ingest_tenant_slots WHERE holder = 'h2'")).rows).toEqual([]);
    await expect(ledger.acquireSlot(req("h8", "user-1", { ttlSeconds: 0 }))).rejects.toMatchObject({ code: "ledger_input_refused" });
  });

  test("retired unproven record seed is denied even to the seeder and cannot alter committed revisions", async () => {
    const db=await database(),ledger=sqlLedger(runAs(db));
    await land(ledger,claimInput({records:[rec("r1",3,"3")]}));
    const before=await revisions(db);
    for(const role of ["bulls96_session_ingest","bulls96_session_ingest_seeder","service_role"])
      await expect(runAs(db,role)("SELECT sessions.ingest_seed_records($1::jsonb)",[JSON.stringify([])])).rejects.toMatchObject({code:"42501"});
    expect(await revisions(db)).toEqual(before);
  });

  test("another role is refused even with EXECUTE", async () => {
    const db = await database();
    await db.exec("GRANT USAGE ON SCHEMA sessions TO service_role; GRANT EXECUTE ON FUNCTION sessions.ingest_claim_batch(jsonb) TO service_role");
    await expect(sqlLedger(runAs(db, "service_role")).claim(claimInput())).rejects.toMatchObject({ code: "ledger_caller_refused" });
    await db.exec("GRANT EXECUTE ON FUNCTION sessions.ingest_claim_batch(jsonb) TO bulls96_session_ingest_seeder");
    await expect(sqlLedger(runAs(db, "bulls96_session_ingest_seeder")).claim(claimInput())).rejects.toMatchObject({ code: "ledger_caller_refused" });
  });

  test("malformed input is refused with 22023 and writes nothing", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const bad: unknown[] = [
      { ...claimInput(), extra: 1 },
      { ...claimInput(), batchDigest: "nope" },
      { ...claimInput(), receiptId: "rhr_x" },
      { ...claimInput(), tenant: { ...tenant, orgId: "" } },
      { ...claimInput(), originId: "a\u0001b" },
      { ...claimInput(), originId: "x".repeat(513) },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), revision: 0 }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), revision: 1.5 }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), revision: "1" }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), digest: "z" }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), source: "" }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), sessionId: 7 }] },
      { ...claimInput(), records: [{ recordId: "r1", revisionId: "x", revision: 1, digest: hex("1") }] },
      { ...claimInput(), sessions: [["claude"]] },
      { ...claimInput(), sessions: [["claude", "s-1"], ["claude", "s-1"]] },
    ];
    for (const input of bad) await expect(ledger.claim(input as ClaimInput)).rejects.toMatchObject({ code: "ledger_input_refused" });
    await ledger.claim(claimInput());
    await expect(ledger.commit({ ...commitInput(claimInput()), records: [{ ...rec("r1", 1, "1"), revision: -1 }] })).rejects.toMatchObject({ code: "ledger_input_refused" });
    expect((await db.query("SELECT * FROM sessions.ingest_batches")).rows.length).toBe(1);
    expect(await revisions(db)).toEqual([]);
  });

  test("a workspace may be empty", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    expect((await ledger.claim(claimInput({ tenant: { ...tenant, workspaceId: "" } }))).kind).toBe("claimed");
  });

  test("a driver failure is a code, never its message", async () => {
    const ledger = sqlLedger(async () => {
      throw Object.assign(new Error("connection to secret-host failed with token abc"), { code: "ECONNRESET" });
    });
    const error = await ledger.claim(claimInput()).catch((e) => e);
    expect(error).toBeInstanceOf(LedgerUnavailableError);
    expect(error.code).toBe("ledger_connection");
    expect(String(error.message)).not.toContain("secret-host");
  });
});

describe("deletion, re-enable and reset forget revisions (review High 2)", () => {
  const staleOf = async (ledger: ReturnType<typeof sqlLedger>, c: ClaimInput) => {
    const r = await ledger.claim(c);
    return r.kind === "replay_fast" ? "replay_fast" : r.kind === "claimed" || r.kind === "resume" ? r.stale : r.kind;
  };

  test("disable then re-enable: the same revisions under a fresh batch id are written again, not acknowledged unwritten", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 3, "1"), rec("r2", 3, "1")] }));
    // Without a fence the regenerated history is a replay.
    expect(await staleOf(ledger, claimInput({ batchId: "regen-1", batchDigest: hex("b"), records: [rec("r1", 3, "2"), rec("r2", 3, "2")] }))).toBe("replay_fast");
    await ledger.setSessionUploads({ tenant, originId: "origin-1", sessions: [["claude", "s-1"]], enabled: false });
    expect(await revisions(db)).toEqual([]);
    const fenced = await ledger.claim(claimInput({ batchId: "while-off", batchDigest: hex("c"), records: [rec("r1", 3, "1")] }));
    expect(fenced.kind === "replay_fast" && fenced.deleted).toEqual([["claude", "s-1"]]);
    await ledger.setSessionUploads({ tenant, originId: "origin-1", sessions: [["claude", "s-1"]], enabled: true });
    const back = await ledger.claim(claimInput({ batchId: "re-enabled", batchDigest: hex("d"), records: [rec("r1", 3, "1"), rec("r2", 3, "1")] }));
    expect(back).toMatchObject({ kind: "claimed", deleted: [], stale: [] });
  });

  test("disable forgets only this user's revisions of that session on that origin", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"]], records: [rec("a", 1, "1"), rec("b", 1, "1", "b:r1", ["claude", "s-2"])] }));
    const other = { ...tenant, userId: "user-2" };
    await land(ledger, claimInput({ tenant: other, originId: "origin-1", batchId: "u2", batchDigest: hex("f"), records: [rec("c", 1, "1")] }));
    await ledger.setSessionUploads({ tenant, originId: "origin-1", sessions: [["claude", "s-1"]], enabled: false });
    expect((await revisions(db)).map((r) => r.record_id)).toEqual(["b", "c"]);
  });

  test("reset tombstones and forgets every session of the user, idempotent by request id", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ sessions: [["claude", "s-1"], ["codex", "s-9"]], records: [rec("a", 1, "1"), rec("b", 1, "1", "b:r1", ["codex", "s-9"]), rec("p", 1, "1", "p:r1", ["claude", null])] }));
    expect(await ledger.resetUploadedSessions(tenant, "req-1")).toBe(2);
    expect(await ledger.resetUploadedSessions(tenant, "req-1")).toBe(2);
    expect((await revisions(db)).map((r) => r.record_id)).toEqual(["p"]);
    const deletions = await db.query("SELECT source, session_id FROM sessions.upload_deletions ORDER BY source");
    expect(deletions.rows).toEqual([{ source: "claude", session_id: "s-1" }, { source: "codex", session_id: "s-9" }]);
  });

  test("retired bulk fence seed is denied and cannot inject tombstones or reset history",async()=>{
    const db=await database();
    for(const role of ["bulls96_session_ingest","bulls96_session_ingest_seeder"])
      await expect(runAs(db,role)("SELECT sessions.ingest_seed_fences($1::jsonb)",[JSON.stringify({deletions:[],resets:[]})])).rejects.toMatchObject({code:"42501"});
    expect((await db.query("SELECT count(*)::int AS n FROM sessions.upload_deletions")).rows[0]).toEqual({n:0});
    expect((await db.query("SELECT count(*)::int AS n FROM sessions.upload_resets")).rows[0]).toEqual({n:0});
  });

});

describe("staged fan-out SQL (review Medium)", () => {
  test("fanoutFence names the slice's fenced sessions; commitRevisions stores the rest and re-arms the fenced", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await ledger.setSessionUploads({ tenant, originId: "origin-1", sessions: [["claude", "gone"]], enabled: false });
    const c=claimInput({sessions:[["claude","s-1"],["claude","gone"]],records:[rec("a",2,"1"),rec("g",1,"1","g:r1",["claude","gone"])]});
    await ledger.claim(c);
    expect(await ledger.fanoutFence(tenant, "origin-1", [["claude", "s-1"], ["claude", "gone"]],attempt(c))).toEqual([["claude", "gone"]]);
    const out = await ledger.commitRevisions(tenant, "origin-1", MACHINE, [rec("a", 2, "1"), rec("g", 1, "1", "g:r1", ["claude", "gone"])],attempt(c));
    expect(out).toEqual({ committed: 1, rearmed: 1 });
    expect((await revisions(db)).map((r) => r.record_id)).toEqual(["a"]);
    const rearm = await db.query("SELECT session_id FROM sessions.upload_erasure_rearm");
    expect(rearm.rows).toEqual([{ session_id: "gone" }]);
    // Idempotent, and never lowers.
    expect(await ledger.commitRevisions(tenant, "origin-1", MACHINE, [rec("a", 1, "1")],attempt(c))).toEqual({ committed: 0, rearmed: 0 });
  });
});

describe("pending session ownership fences", () => {
  test("a disable between claim and direct commit never restores a stale revision", async () => {
    const db=await database(),ledger=sqlLedger(runAs(db)),c=claimInput();
    await ledger.claim(c);
    await ledger.setSessionUploads({tenant,originId:c.originId,sessions:c.sessions,enabled:false});
    expect(await ledger.commit(commitInput(c))).toEqual({rearmed:1});
    expect(await revisions(db)).toEqual([]);
    await ledger.setSessionUploads({tenant,originId:c.originId,sessions:c.sessions,enabled:true});
    expect(await ledger.claim({...c,batchId:"after-erase"})).toMatchObject({kind:"claimed",stale:[]});
  });
  test("reset includes pending and staged ownership before any revision is committed", async () => {
    const db=await database(),ledger=sqlLedger(runAs(db)),c=claimInput();
    await ledger.claim(c);await ledger.commit(commitInput(c,c.sessions,[]));
    expect(await revisions(db)).toEqual([]);
    expect(await ledger.resetUploadedSessions(tenant,"staged-reset")).toBe(1);
    expect(await ledger.fanoutFence(tenant,c.originId,c.sessions,attempt(c))).toEqual(c.sessions);
    expect(await ledger.commitRevisions(tenant,c.originId,MACHINE,c.records,attempt(c))).toEqual({committed:0,rearmed:1});
    expect(await revisions(db)).toEqual([]);
  });
  test("ownership reset remains scoped by tenant, workspace and user", async () => {
    const db=await database(),ledger=sqlLedger(runAs(db)),c=claimInput();
    await ledger.claim(c);
    for(const t of [{...tenant,orgId:"other-org"},{...tenant,workspaceId:"other-ws"},{...tenant,userId:"other-user"}]) {
      await ledger.claim({...c,tenant:t,batchId:"other-"+t.orgId+t.workspaceId+t.userId});
      expect(await ledger.resetUploadedSessions(t,"isolated")).toBe(1);
    }
    expect(await ledger.fanoutFence(tenant,c.originId,c.sessions,attempt(c))).toEqual([]);
    expect(await ledger.resetUploadedSessions(tenant,"own-reset")).toBe(1);
  });
});
