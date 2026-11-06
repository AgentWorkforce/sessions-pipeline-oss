/**
 * Actual PostgreSQL + filesystem-object race controls. Opt in with an owned,
 * synthetic SESSION_INGEST_TEST_PG_URL; never a deployed database. Each run creates
 * its own database. No locks are held across object IO by the implementation.
 */
import { afterAll, afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  plan,
  runMigrations,
  type MigrationProfile,
} from "../../hoopdb/migrate.ts";
import { readFileSync, mkdtempSync, rmSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync, zstdCompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { type ObjectStore, objectKey, readAll } from "../../shared/vendor.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliverBatch } from "../deliver.ts";
import { fanout, type FanoutMessage } from "../staging.ts";
import { batch, record, TENANT } from "./support.ts";
import type { Ledger } from "../ledger.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
const migration =
  "supabase/migrations/20261104030000_ingest_fence_ownership.sql";
const root = join(import.meta.dir, "../../..");
const sql = (path: string) => readFileSync(join(root, path), "utf8");
const cleanup: (() => Promise<void>)[] = [];
const base = mkdtempSync(join(tmpdir(), "ingest-pg-races-"));
// DROP DATABASE can wait for physical PG cleanup; bound disposal separately.
afterEach(async () => {
  for (const f of cleanup.splice(0).reverse()) await f();
}, 30000);
afterAll(() => {
  rmSync(base, { recursive: true, force: true });
});

function barrier() {
  let reach!: () => void, release!: () => void;
  return {
    reached: new Promise<void>((r) => {
      reach = r;
    }),
    resumed: new Promise<void>((r) => {
      release = r;
    }),
    reach: () => reach(),
    release: () => release(),
  };
}
async function database(repair = true, migrateSchema = true) {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "ingest_race_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url!);
  target.pathname = "/" + name;
  const pool = new pg.Pool({ connectionString: target.href });
  const checked = new Set<pg.PoolClient>();
  cleanup.push(async () => {
    for (const c of checked) {
      await c.query("ROLLBACK");
      c.release();
    }
    await pool.end();
    await admin.query("DROP DATABASE " + name);
    await admin.end();
  });
  await pool.query("CREATE SCHEMA sessions");
  await pool.query(sql("supabase/tests/supabase_shim.sql"));
  await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  for (const file of [
    "session_directory_search_worker.sql",
    "session_token_verifier.sql",
    "session_ingest_roles.sql",
  ])
    await pool.query(sql("supabase/bootstrap/" + file));
  if (migrateSchema)
    for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
      if ("supabase/migrations/" + file !== migration)
        await pool.query(sql("supabase/migrations/" + file));
    }
  if (repair && migrateSchema) await apply(pool);
  const client = async () => {
    const c = await pool.connect();
    checked.add(c);
    const release = c.release.bind(c);
    c.release = (destroy?: boolean) => {
      checked.delete(c);
      release(destroy);
    };
    await c.query("SET ROLE bulls96_session_ingest");
    const ledger = sqlLedger(async (t, p) => {
      const out = await c.query(t, p);
      return { rows: out.rows };
    });
    return { c, ledger, pid: c.processID! };
  };
  return { pool, client };
}
async function apply(pool: pg.Pool) {
  let text = sql(migration);
  // Each mutation must actually remove its critical fence from this exact SQL.
  const original = text;
  const mutation = process.env.INGEST_RACE_MUTATION;
  if (mutation === "no_serialization")
    text = text.replace(
      /^\s*PERFORM sessions\.ingest_lock_fences\([^\n]+\);\n/gm,
      "",
    );
  if (mutation === "no_pending_ownership")
    text = text.replace(
      /    UNION\n    SELECT o\.origin_id,o\.source,o\.session_id FROM sessions\.ingest_session_ownership AS o\n     WHERE o\.org_id=p_org AND o\.workspace_id=p_ws AND o\.user_id=p_user/,
      "",
    );
  if (mutation === "no_commit_filter")
    text = text.replace(
      "   WHERE i.session_id IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) AS f(pair)\n     WHERE f.pair->>0=i.source AND f.pair->>1=i.session_id)\n",
      "",
    );
  if (mutation === "no_epoch_check")
    text = text.replace("coalesce(f.epoch,0)<>e.epoch OR ", "");
  if (mutation === "no_nonce_binding")
    text = text.replace("AND a.claim_token=p_token::uuid ", "");
  if (mutation && text === original)
    throw Error("mutation did not alter its fence");
  await pool.query(text);
}
function storage() {
  const dir = mkdtempSync(join(base, "objects-"));
  const store = createFsStore({ root: dir }) as unknown as ObjectStore;
  const target = { store, keyRoot: "96bulls", sleep: async () => {} };
  return { store, target };
}
async function keys(store: ObjectStore) {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey("96bulls"), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.cursor;
  } while (cursor);
  return out.sort();
}
async function count(pool: pg.Pool, table: string) {
  return (await pool.query("SELECT count(*)::int n FROM sessions." + table))
    .rows[0].n;
}
async function waitForLock(pool: pg.Pool, pid: number, settled: () => boolean) {
  for (let i = 0; i < 100; i++) {
    if (settled()) return false;
    const r = await pool.query(
      "SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1",
      [pid],
    );
    if (r.rows[0]?.wait_event_type === "Lock") return true;
    await new Promise((r) => setTimeout(r, 5));
  }
  return false;
}
const selection = {
  tenant: TENANT,
  originId: "origin-1",
  sessions: [["claude", "sess-1"]] as [string, string][],
  enabled: false,
};
describe.skipIf(!url)("actual PG upload fence interleavings", () => {
  test("disable open during direct commit: revision stays absent and re-enable actually restores object bytes", async () => {
    const d = await database();
    const a = await d.client(),
      b = await d.client();
    const s = storage(),
      gate = barrier();
    const wrapped: Ledger = {
      ...b.ledger,
      async commit(p) {
        gate.reach();
        await gate.resumed;
        return b.ledger.commit(p);
      },
    };
    let settled = false;
    const upload = deliverBatch(
      { ledger: wrapped, target: s.target },
      TENANT,
      await batch([record({ record_id: "late-direct" })]),
    ).finally(() => {
      settled = true;
    });
    await gate.reached;
    expect((await keys(s.store)).some((k) => k.endsWith("manifest.json"))).toBe(
      true,
    );
    await a.c.query("BEGIN");
    await a.ledger.setSessionUploads(selection);
    gate.release();
    const blocked = await waitForLock(d.pool, b.pid, () => settled);
    await a.c.query("COMMIT");
    const first = await upload;
    expect(blocked).toBe(true);
    expect(first.stats.rearmed).toBe(1);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    // Represent the completed erasure, including its old manifest and pointer.
    for (const key of await keys(s.store)) await s.store.delete(objectKey(key));
    await a.ledger.setSessionUploads({ ...selection, enabled: true });
    const fresh = await deliverBatch(
      { ledger: b.ledger, target: s.target },
      TENANT,
      await batch([record({ record_id: "late-direct" })], {
        batch_id: "fresh-after-erasure",
      }),
    );
    const restored = await keys(s.store);
    expect(fresh.outcome).toBe("accepted");
    expect(fresh.stats.manifests_written).toBe(1);
    const chunks = restored.filter((k) => k.endsWith(".ndjson.zst"));
    expect(chunks).toHaveLength(1);
    const bytes = await readAll(
      (await s.store.getObject(objectKey(chunks[0]!))).body,
    );
    expect(new TextDecoder().decode(zstdDecompressSync(bytes))).toContain(
      "late-direct",
    );
    a.c.release();
    b.c.release();
  }, 15000);
  for (const inflight of [false, true])
    test(
      "reset reaches acknowledged staged sessions " +
        (inflight
          ? "after fanout fence / before object write"
          : "before fanout"),
      async () => {
        const d = await database();
        const a = await d.client(),
          b = await d.client();
        const s = storage(),
          gate = barrier();
        const messages: FanoutMessage[] = [];
        const records = [
          record({ record_id: "stage-one" }),
          record({ record_id: "stage-two", session_id: "sess-2" }),
        ];
        const receipt = await deliverBatch(
          {
            ledger: b.ledger,
            target: s.target,
            inlineMaxSessions: 0,
            fanout: async (m) => {
              messages.push(...m);
            },
          },
          TENANT,
          await batch(records),
        );
        expect(receipt.outcome).toBe("accepted_staged");
        expect(messages).toHaveLength(1);
        expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
        expect(
          (await keys(s.store)).some((k) => k.endsWith(".staged.ndjson.zst")),
        ).toBe(true);
        let copy: ReturnType<typeof fanout> | undefined;
        if (inflight) {
          const wrapped = {
            ...b.ledger,
            async fanoutFence(...p: Parameters<Ledger["fanoutFence"]>) {
              const out = await b.ledger.fanoutFence(...p);
              gate.reach();
              await gate.resumed;
              return out;
            },
          };
          copy = fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            wrapped,
          );
          await gate.reached;
        }
        // Reset completes while object IO is paused; no long SQL lock crosses the pause.
        const deleted = await a.ledger.resetUploadedSessions(
          TENANT,
          "reset-stage",
        );
        if (inflight) gate.release();
        const result = await (copy ??
          fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            b.ledger,
          ));
        expect(deleted).toBe(2);
        expect(
          await a.ledger.resetUploadedSessions(TENANT, "reset-stage"),
        ).toBe(2);
        expect(await count(d.pool, "upload_deletions")).toBe(2);
        expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
        expect(result.skipped).toBe(inflight ? 0 : 2);
        expect(result.revisions.rearmed).toBe(inflight ? 2 : 0);
        expect(
          (await keys(s.store)).filter((k) => k.endsWith("manifest.json")),
        ).toHaveLength(inflight ? 2 : 0);
        expect(await count(d.pool, "upload_erasure_rearm")).toBe(
          inflight ? 2 : 0,
        );
        a.c.release();
        b.c.release();
      },
      15000,
    );
  test("reset held open serializes fanout commit; unrelated user and workspace remain independent", async () => {
    const d = await database();
    const a = await d.client(),
      b = await d.client(),
      other = await d.client(),
      workspace = await d.client();
    const r = {
      source: "claude",
      sessionId: "sess-1",
      recordId: "r",
      revisionId: "r:1",
      revision: 1,
      digest: "a".repeat(64),
    };
    const claimed = await b.ledger.claim({
      tenant: TENANT,
      originId: "origin-1",
      batchId: "b",
      batchDigest: "b".repeat(64),
      receiptId: "rhr_" + "c".repeat(64),
      sessions: selection.sessions,
      records: [r],
    });
    if (claimed.kind !== "claimed") throw Error("expected claim");
    const attempt = {
      batchId: "b",
      batchDigest: "b".repeat(64),
      claimToken: claimed.claimToken,
    };
    await a.c.query("BEGIN");
    await a.ledger.resetUploadedSessions(TENANT, "held-reset");
    let settled = false;
    const commit = b.ledger
      .commitRevisions(
        TENANT,
        "origin-1",
        "srv_" + "a".repeat(32),
        [r],
        attempt,
      )
      .finally(() => {
        settled = true;
      });
    const blocked = await waitForLock(d.pool, b.pid, () => settled);
    const parallel = await Promise.all([
      other.ledger.resetUploadedSessions(
        { ...TENANT, userId: "different-user" },
        "other-user",
      ),
      // Different connection because concurrent SQL on one connection queues.
      workspace.ledger.resetUploadedSessions(
        { ...TENANT, workspaceId: "different-ws" },
        "other-ws",
      ),
    ]);
    await a.c.query("COMMIT");
    const result = await commit;
    expect(blocked).toBe(true);
    expect(parallel).toEqual([0, 0]);
    expect(result).toEqual({ committed: 0, rearmed: 1 });
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    a.c.release();
    b.c.release();
    other.c.release();
    workspace.c.release();
  }, 15000);
  test("append upgrade preserves existing revisions and makes them reset-visible; helper/table access stays closed", async () => {
    const d = await database(false),
      a = await d.client();
    const r = {
      source: "claude",
      sessionId: "old-session",
      recordId: "old-r",
      revisionId: "old-r:1",
      revision: 1,
      digest: "a".repeat(64),
    };
    const claim = {
      tenant: TENANT,
      originId: "origin-1",
      batchId: "old-b",
      batchDigest: "b".repeat(64),
      receiptId: "rhr_" + "c".repeat(64),
      sessions: [["claude", "old-session"]] as [string, string][],
      records: [r],
    };
    await a.c.query("SELECT sessions.ingest_claim_batch($1::jsonb)", [
      JSON.stringify(claim),
    ]);
    const { receiptId, ...commit } = claim;
    await a.c.query("SELECT sessions.ingest_commit_batch($1::jsonb)", [
      JSON.stringify({ ...commit, machineId: "srv_" + "a".repeat(32) }),
    ]);
    // Reproduce an already-corrupted old ledger, rather than only a clean install.
    const bad = {
      ...claim,
      batchId: "old-late",
      sessions: [["claude", "already-erased"]] as [string, string][],
      records: [{ ...r, recordId: "erased-r", sessionId: "already-erased" }],
    };
    await a.c.query("SELECT sessions.ingest_claim_batch($1::jsonb)", [
      JSON.stringify(bad),
    ]);
    await a.ledger.setSessionUploads({
      tenant: TENANT,
      originId: "origin-1",
      sessions: bad.sessions,
      enabled: false,
    });
    const { receiptId: badReceipt, ...badCommit } = bad;
    await a.c.query("SELECT sessions.ingest_commit_batch($1::jsonb)", [
      JSON.stringify({ ...badCommit, machineId: "srv_" + "a".repeat(32) }),
    ]);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(2);
    await apply(d.pool);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(1);
    expect(await count(d.pool, "ingest_session_ownership")).toBe(1);
    await expect(
      a.c.query("SELECT sessions.ingest_lock_fences($1,$2,$3)", [
        TENANT.orgId,
        TENANT.workspaceId,
        TENANT.userId,
      ]),
    ).rejects.toMatchObject({ code: "42501" });
    await expect(
      a.c.query("SELECT * FROM sessions.ingest_session_ownership"),
    ).rejects.toMatchObject({ code: "42501" });
    expect(await a.ledger.resetUploadedSessions(TENANT, "upgrade-reset")).toBe(
      1,
    );
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    a.c.release();
  }, 15000);
});

describe.skipIf(!url)("additional generation interleaving", () => {
  test("reenable before a paused pre-disable commit cannot acknowledge erased bytes", async () => {
    const d = await database(),
      a = await d.client(),
      b = await d.client(),
      s = storage(),
      gate = barrier();
    const wrapped: Ledger = {
      ...b.ledger,
      async commit(p) {
        gate.reach();
        await gate.resumed;
        return b.ledger.commit(p);
      },
    };
    const upload = deliverBatch(
      { ledger: wrapped, target: s.target },
      TENANT,
      await batch([record({ record_id: "generation-race" })]),
    );
    await gate.reached;
    await a.ledger.setSessionUploads(selection);
    for (const key of await keys(s.store)) await s.store.delete(objectKey(key));
    await a.ledger.setSessionUploads({ ...selection, enabled: true });
    gate.release();
    await expect(upload).rejects.toMatchObject({
      cause_code: "ledger_attempt_superseded",
    });
    const fresh = await deliverBatch(
      { ledger: b.ledger, target: s.target },
      TENANT,
      await batch([record({ record_id: "generation-race" })], {
        batch_id: "fresh-generation",
      }),
    );
    const proof = {
      fresh: fresh.outcome,
      objects: (await keys(s.store)).length,
      revisions: await count(d.pool, "ingest_record_revisions"),
    };
    console.log(JSON.stringify(proof));
    a.c.release();
    b.c.release();
    expect(fresh.outcome).toBe("accepted");
    expect(proof.objects).toBeGreaterThan(0);
  }, 15000);
});

describe.skipIf(!url)("immutable attempt generation", () => {
  test("old direct attempt never borrows a resumed attempt snapshot", async () => {
    const d = await database(),
      a = await d.client(),
      b = await d.client(),
      s = storage(),
      gate = barrier();
    let oldToken = "",
      newToken = "";
    const original = await batch([
      record({ record_id: "same-batch-generation" }),
    ]);
    const wrapped: Ledger = {
      ...b.ledger,
      async commit(p) {
        oldToken = p.claimToken;
        gate.reach();
        await gate.resumed;
        return b.ledger.commit(p);
      },
    };
    const old = deliverBatch(
      { ledger: wrapped, target: s.target },
      TENANT,
      original,
    );
    await gate.reached;
    await a.ledger.setSessionUploads(selection);
    for (const key of await keys(s.store)) await s.store.delete(objectKey(key));
    await a.ledger.setSessionUploads({ ...selection, enabled: true });
    const resumed: Ledger = {
      ...b.ledger,
      async commit(p) {
        newToken = p.claimToken;
        return b.ledger.commit(p);
      },
    };
    const fresh = await deliverBatch(
      { ledger: resumed, target: s.target },
      TENANT,
      original,
    );
    gate.release();
    await expect(old).rejects.toMatchObject({
      cause_code: "ledger_attempt_superseded",
    });
    expect(oldToken).not.toBe(newToken);
    expect(fresh.outcome).toBe("accepted");
    expect(await count(d.pool, "ingest_record_revisions")).toBe(1);
    const epochs = (
      await d.pool.query(
        "SELECT epochs->0->>'epoch' AS epoch FROM sessions.ingest_claim_attempts ORDER BY epochs->0->>'epoch'",
      )
    ).rows;
    expect(epochs.map((x) => x.epoch)).toEqual(["0", "1"]);
    a.c.release();
    b.c.release();
  }, 15000);
  for (const inflight of [false, true])
    test(
      "staged disable/erase/enable cycle " +
        (inflight ? "after" : "before") +
        " fanout fence",
      async () => {
        const d = await database(),
          a = await d.client(),
          b = await d.client(),
          s = storage(),
          gate = barrier();
        const messages: FanoutMessage[] = [];
        const original = await batch([record({ record_id: "stage-cycle" })]);
        const receipt = await deliverBatch(
          {
            ledger: b.ledger,
            target: s.target,
            inlineMaxSessions: 0,
            fanout: async (m) => {
              messages.push(...m);
            },
          },
          TENANT,
          original,
        );
        expect(receipt.outcome).toBe("accepted_staged");
        let copy: ReturnType<typeof fanout> | undefined;
        if (inflight) {
          const wrapped = {
            ...b.ledger,
            async fanoutFence(...p: Parameters<Ledger["fanoutFence"]>) {
              const out = await b.ledger.fanoutFence(...p);
              gate.reach();
              await gate.resumed;
              return out;
            },
          };
          copy = fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            wrapped,
          );
          await gate.reached;
        }
        await a.ledger.setSessionUploads(selection);
        // Erase session manifests/chunks/pointers but leave the durable stage for redrive.
        for (const key of await keys(s.store))
          if (!key.endsWith(".staged.ndjson.zst"))
            await s.store.delete(objectKey(key));
        await a.ledger.setSessionUploads({ ...selection, enabled: true });
        if (inflight) gate.release();
        const result = await (copy ??
          fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            b.ledger,
          ));
        expect(result.skipped).toBe(inflight ? 0 : 1);
        expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
        expect(result.revisions.rearmed).toBe(inflight ? 1 : 0);
        const fresh = await deliverBatch(
          { ledger: b.ledger, target: s.target },
          TENANT,
          await batch([record({ record_id: "stage-cycle" })], {
            batch_id: "fresh-stage-cycle",
          }),
        );
        expect(fresh.outcome).toBe("accepted");
        expect(await count(d.pool, "ingest_record_revisions")).toBe(1);
        a.c.release();
        b.c.release();
      },
      15000,
    );
});

describe.skipIf(!url)("attempt isolation and mixed batches", () => {
  test("invalidating one session preserves unrelated writes and same-batch retry restores only erased session", async () => {
    const d = await database(),
      a = await d.client(),
      b = await d.client(),
      s = storage(),
      gate = barrier();
    const original = await batch([
      record({ record_id: "cycle-target" }),
      record({ record_id: "untouched", session_id: "sess-2" }),
    ]);
    const wrapped: Ledger = {
      ...b.ledger,
      async commit(p) {
        gate.reach();
        await gate.resumed;
        return b.ledger.commit(p);
      },
    };
    const old = deliverBatch(
      { ledger: wrapped, target: s.target },
      TENANT,
      original,
    );
    await gate.reached;
    await a.ledger.setSessionUploads(selection);
    for (const key of await keys(s.store))
      if (key.includes("/s=sess-1/") || key.endsWith("/s=sess-1.json"))
        await s.store.delete(objectKey(key));
    await a.ledger.setSessionUploads({ ...selection, enabled: true });
    gate.release();
    await expect(old).rejects.toMatchObject({
      cause_code: "ledger_attempt_superseded",
    });
    expect(
      (
        await d.pool.query(
          "SELECT record_id FROM sessions.ingest_record_revisions",
        )
      ).rows,
    ).toEqual([{ record_id: "untouched" }]);
    const retry = await deliverBatch(
      { ledger: b.ledger, target: s.target },
      TENANT,
      original,
    );
    expect(retry.outcome).toBe("accepted");
    expect(retry.stats.stale).toBe(1);
    expect(retry.stats.manifests_written).toBe(1);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(2);
    a.c.release();
    b.c.release();
  }, 15000);
  test("foreign or nonexistent attempt nonce refuses before fanout writes, even for no-session groups", async () => {
    const d = await database(),
      a = await d.client();
    const c = {
      tenant: TENANT,
      originId: "origin-1",
      batchId: "nonce-b",
      batchDigest: "a".repeat(64),
      receiptId: "rhr_" + "b".repeat(64),
      sessions: selection.sessions,
      records: [
        {
          source: "claude",
          sessionId: "sess-1",
          recordId: "nonce-r",
          revisionId: "nonce-r:1",
          revision: 1,
          digest: "c".repeat(64),
        },
      ],
    };
    const claimed = await a.ledger.claim(c);
    if (claimed.kind !== "claimed") throw Error("expected claim");
    const real = {
      batchId: c.batchId,
      batchDigest: c.batchDigest,
      claimToken: claimed.claimToken,
    };
    const wrong = {
      ...real,
      claimToken: "00000000-0000-0000-0000-000000000001",
    };
    await expect(
      a.ledger.fanoutFence(TENANT, c.originId, [], wrong),
    ).rejects.toMatchObject({ code: "ledger_sqlstate_40001" });
    await expect(
      a.ledger.fanoutFence(
        { ...TENANT, userId: "foreign-user" },
        c.originId,
        c.sessions,
        real,
      ),
    ).rejects.toMatchObject({ code: "ledger_sqlstate_40001" });
    expect(
      await a.ledger.fanoutFence(TENANT, c.originId, c.sessions, real),
    ).toEqual([]);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    a.c.release();
  }, 15000);
});

describe.skipIf(!url || !process.env.REVIEW_RHC_REPLICA_MODULE)(
  "frozen transitional replica staged contract",
  () => {
    test("new additive v1 stage rebuilds through actual RHC consumer; old missing-token stage refuses upgraded fanout", async () => {
      const d = await database(),
        a = await d.client(),
        s = storage(),
        messages: FanoutMessage[] = [];
      let replica: any;
      const records = [
        record({ record_id: "replica-nonce-1" }),
        record({ record_id: "replica-nonce-2", session_id: "sess-2" }),
      ];
      await deliverBatch(
        {
          ledger: a.ledger,
          target: s.target,
          inlineMaxSessions: 0,
          fanout: async (m) => {
            messages.push(...m);
          },
          replica: {
            mode: "required",
            send: async (m) => {
              replica = m;
            },
            putDescriptor: async (key, body) => {
              await s.store.put(objectKey(key), body);
            },
          },
        },
        TENANT,
        await batch(records),
      );
      const key = messages[0]!.key;
      const original = new Uint8Array(
        await readAll((await s.store.getObject(objectKey(key))).body),
      );
      const decoded = new TextDecoder().decode(zstdDecompressSync(original)),
        nl = decoded.indexOf("\n");
      const header = JSON.parse(decoded.slice(0, nl));
      expect(header.v).toBe(1);
      expect(header.claim_token).toMatch(/^[0-9a-f-]{36}$/);
      expect(header.batch_id).toBeUndefined();
      const { rebuildPrepared } = await import(
        process.env.REVIEW_RHC_REPLICA_MODULE!
      );
      const rebuilt = await rebuildPrepared(s.store, replica);
      expect(
        rebuilt.records.map((r: any) => [
          r.record_id,
          r.revision,
          r.payload.text,
        ]),
      ).toEqual(records.map((r) => [r.record_id, r.revision, r.payload!.text]));
      // v2 is rejected by the real frozen consumer: additive v1 is deliberate.
      const write = async (h: any) => {
        await s.store.put(
          objectKey(key),
          new Uint8Array(
            zstdCompressSync(
              new TextEncoder().encode(JSON.stringify(h) + decoded.slice(nl)),
            ),
          ),
        );
      };
      await write({ ...header, v: 2 });
      await expect(rebuildPrepared(s.store, replica)).rejects.toMatchObject({
        code: "chunk_mismatch",
      });
      const { claim_token, ...old } = header;
      await write(old);
      await expect(
        fanout(
          s.target,
          messages[0]!,
          (v) => new Uint8Array(zstdDecompressSync(v)),
          a.ledger,
        ),
      ).rejects.toThrow("unknown version");
      expect(
        (await keys(s.store)).filter((k) => k.endsWith("manifest.json")),
      ).toHaveLength(0);
      await s.store.put(objectKey(key), original);
      expect(
        (
          await fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            a.ledger,
          )
        ).skipped,
      ).toBe(0);
      a.c.release();
    }, 15000);
  },
);

describe.skipIf(!url)("released main ledger upgrade", () => {
  test("populated exact main migration ledger accepts exactly the three append-only release migrations", async () => {
    const base = "4ddee50b78abcc74ba258201216769b7673e9369";
    const mainFiles = new Set(
      execFileSync(
        "git",
        ["ls-tree", "-r", "--name-only", base, "supabase/migrations"],
        { cwd: root, encoding: "utf8" },
      )
        .trim()
        .split("\n"),
    );
    const profile: MigrationProfile = {
      mode: "local",
      shim: true,
      bootstrap: true,
      verify: true,
      runtimeRole: "postgres",
    };
    const steps = await plan(profile),
      schema = steps.filter((x) => x.name.startsWith("upstream:"));
    const baseSteps = steps.filter(
      (x) =>
        !x.name.startsWith("verify:") &&
        (!x.name.startsWith("upstream:") ||
          mainFiles.has("supabase/migrations/" + x.name.slice(9))),
    );
    for (const step of baseSteps.filter((x) =>
      x.name.startsWith("upstream:"),
    )) {
      const path = "supabase/migrations/" + step.name.slice(9);
      const expected = execFileSync("git", ["show", base + ":" + path], {
        cwd: root,
      });
      expect(
        createHash("sha256").update(readFileSync(step.path)).digest("hex"),
      ).toBe(createHash("sha256").update(expected).digest("hex"));
    }
    const d = await database(false, false),
      connection = await d.pool.connect();
    try {
      const client = {
        async query<R>(text: string, params?: unknown[]) {
          const out: any = await connection.query(text, params);
          return {
            rows: (Array.isArray(out)
              ? (out.at(-1)?.rows ?? [])
              : (out.rows ?? [])) as R[],
          };
        },
      };
      await runMigrations(client, {
        profile: { ...profile, verify: false },
        steps: baseSteps,
      });
      const before = (
        await connection.query(
          "SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name",
        )
      ).rows;
      expect(before.filter(x=>x.name.startsWith("upstream:")||x.name.startsWith("96bulls:"))).toHaveLength(23);
      expect(before.filter(x=>x.name==="shim:supabase_roles")).toHaveLength(1);
      console.log(JSON.stringify({baselineSchemaRows:23,syntheticShimRows:1,releaseAppendSteps:3}));
      expect(
        before.some(
          (x) => x.name === "upstream:20261104020100_auth_session_verify.sql",
        ),
      ).toBe(true);
      expect(before.some((x) => x.name.includes("ingest_ledger"))).toBe(false);
      const oldNames = new Map([
        [
          "upstream:20261104021000_session_directory_event_metadata.sql",
          "upstream:20261104021000_session_directory_event_metadata.sql",
        ],
        [
          "upstream:20261104022000_session_ingest_ledger.sql",
          "upstream:20261102010000_session_ingest_ledger.sql",
        ],
      ]);
      const oldUpstream = steps
        .filter((x) => x.name.startsWith("upstream:"))
        .map((x) => ({ ...x, name: oldNames.get(x.name) ?? x.name }))
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
      const oldPlan = [
        ...steps.filter(
          (x) =>
            !x.name.startsWith("upstream:") &&
            !x.name.startsWith("96bulls:") &&
            !x.name.startsWith("verify:"),
        ),
        ...oldUpstream,
        ...steps.filter(
          (x) => x.name.startsWith("96bulls:") || x.name.startsWith("verify:"),
        ),
      ];
      await expect(
        runMigrations(client, { profile, steps: oldPlan }),
      ).rejects.toMatchObject({ code: "ledger_gap" });
      expect(
        (
          await connection.query(
            "SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name",
          )
        ).rows,
      ).toEqual(before);
      const upgraded = await runMigrations(client, { profile });
      expect(upgraded.applied).toBe(3);
      const after = (
        await connection.query(
          "SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name",
        )
      ).rows;
      for (const old of before)
        expect(after.find((x) => x.name === old.name)).toEqual(old);
      expect(after.length - before.length).toBe(3);
      expect(
        schema
          .filter(
            (x) => !mainFiles.has("supabase/migrations/" + x.name.slice(9)),
          )
          .map((x) => x.name),
      ).toEqual([
        "upstream:20261104021000_session_directory_event_metadata.sql",
        "upstream:20261104022000_session_ingest_ledger.sql",
        "upstream:20261104030000_ingest_fence_ownership.sql",
      ]);
    } finally {
      connection.release();
    }
  }, 30000);
});
