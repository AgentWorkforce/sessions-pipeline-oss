/**
 * The single-use ledger for RelayAuth migration assertions
 * (`supabase/migrations/20261031000000_migration_assertion_ledger.sql`),
 * applied with every other `supabase/migrations` file to local PGlite.
 *
 * The consume statement is the one relayhistory-cloud's
 * `sqlMigrationAssertionLedger` runs (relayhistory-cloud
 * feat/migration-assertion-ingest): insert-if-absent on jti and nonce, in the
 * same transaction as the manifest commit's `pipeline_jobs` row. Synthetic ids
 * only; no hosted Supabase is involved.
 */
import { PGlite, type Transaction } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { randomBytes } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { sqlMigrationAssertionLedger } from "../src/lib/pipeline/migration-assertions.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const ORG = "org_synthetic";
const WORKSPACE = "ws_synthetic";
const USER = "user_synthetic";
const PLAN = "a".repeat(64);
const GRANT = `mgr_${"0".repeat(32)}`;
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const WORK_KEY = `v1/ws=${WORKSPACE}/m=machine-synthetic/h=claude/d=2026/10/25/s=sess-synthetic-1/manifest.json@e1`;

let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_directory_search_worker.sql"),
      "utf8",
    ),
  );
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_token_verifier.sql"),
      "utf8",
    ),
  );
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_ingest_roles.sql"),
      "utf8",
    ),
  );
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
  await db.exec("SET ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec(
    "TRUNCATE sessions.migration_assertion_uses; TRUNCATE sessions.migration_base_read_uses; TRUNCATE sessions.pipeline_jobs",
  );
});

const jti = () => `tok_${randomBytes(16).toString("hex")}`;
const nonce = () => randomBytes(24).toString("base64url");

type Use = {
  jti: string;
  nonce: string;
  planDigest?: string;
  grantId?: string;
  expiresAt?: string;
};

async function insertUse(tx: Transaction | PGlite, use: Use) {
  const { rows } = await tx.query(
    `INSERT INTO sessions.migration_assertion_uses (jti, nonce, grant_id, org_id,
                                                    workspace_id, user_id, plan_digest, plan_entry_digest, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $7, COALESCE($8::timestamptz, now() + interval '65 seconds'))
     ON CONFLICT DO NOTHING
     RETURNING jti`,
    [
      use.jti,
      use.nonce,
      use.grantId ?? GRANT,
      ORG,
      WORKSPACE,
      USER,
      use.planDigest ?? PLAN,
      use.expiresAt ?? null,
    ],
  );
  return rows.length === 1;
}

async function insertJob(tx: Transaction) {
  await tx.query(
    `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
     VALUES ('object-landed', $1, $2::uuid, $3, $4, $5::jsonb)`,
    [WORK_KEY, TRACE, ORG, WORKSPACE, JSON.stringify({ workKey: WORK_KEY })],
  );
}

/** The ledger's consume: the use and the commit's queue row, or neither. */
function consume(use: Use, crash = false) {
  return db.transaction(async (tx) => {
    if (!(await insertUse(tx, use))) return "replayed" as const;
    await insertJob(tx);
    if (crash) throw new Error("crash between consume and commit");
    return "consumed" as const;
  });
}

const count = async (table: string) =>
  (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`))
    .rows[0]!.n;

describe("sessions.migration_assertion_uses", () => {
  it("consumes an assertion once", async () => {
    const use = { jti: jti(), nonce: nonce() };
    expect(await consume(use)).toBe("consumed");
    expect(await consume(use)).toBe("replayed");
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
    const { rows } = await db.query<Record<string, unknown>>(
      "SELECT grant_id, org_id, workspace_id, user_id, plan_digest, consumed_at IS NOT NULL AS stamped FROM sessions.migration_assertion_uses",
    );
    expect(rows).toEqual([
      {
        grant_id: GRANT,
        org_id: ORG,
        workspace_id: WORKSPACE,
        user_id: USER,
        plan_digest: PLAN,
        stamped: true,
      },
    ]);
  });

  it("refuses a reused nonce under a new jti", async () => {
    const first = { jti: jti(), nonce: nonce() };
    expect(await consume(first)).toBe("consumed");
    expect(await consume({ jti: jti(), nonce: first.nonce })).toBe("replayed");
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });

  it("rolls the consumption back with the commit, so a fresh assertion can retry", async () => {
    const crashed = { jti: jti(), nonce: nonce() };
    await expect(consume(crashed, true)).rejects.toThrow("crash");
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await consume({ jti: jti(), nonce: nonce() })).toBe("consumed");
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });

  it("enforces the key and value shapes", async () => {
    await expect(
      insertUse(db, { jti: jti(), nonce: nonce(), planDigest: "A".repeat(64) }),
    ).rejects.toThrow(/plan_digest/);
    await expect(
      insertUse(db, { jti: jti(), nonce: nonce(), planDigest: "a".repeat(63) }),
    ).rejects.toThrow(/plan_digest/);
    await expect(insertUse(db, { jti: jti(), nonce: "short" })).rejects.toThrow(
      /nonce/,
    );
    await expect(insertUse(db, { jti: "", nonce: nonce() })).rejects.toThrow(
      /jti/,
    );
  });

  it("retention: the ledger deletes at most 100 rows expired past the 5 minute margin per insert", async () => {
    for (let i = 0; i < 120; i++)
      await insertUse(db, {
        jti: jti(),
        nonce: nonce(),
        expiresAt: new Date(Date.now() - 3_600_000).toISOString(),
      });
    // Expired, but inside the margin: kept.
    await insertUse(db, {
      jti: jti(),
      nonce: nonce(),
      expiresAt: new Date(Date.now() - 60_000).toISOString(),
    });
    const ledger = sqlMigrationAssertionLedger({
      query: async (text, params) => ({
        rows: (await db.query(text, params)).rows as never[],
      }),
      transaction: (work) =>
        db.transaction((tx) =>
          work({
            query: async (text, params) => ({
              rows: (await tx.query(text, params)).rows as never[],
            }),
          }),
        ),
    });
    const fresh = {
      jti: jti(),
      nonce: nonce(),
      grantId: GRANT,
      planDigest: PLAN,
      planEntryDigest: PLAN,
      orgId: ORG,
      workspaceId: WORKSPACE,
      userId: USER,
      expiresAt: Math.floor(Date.now() / 1000) + 65,
    };
    expect(
      await ledger.consume(fresh, "object-landed", {
        v: 1,
        stage: "object-landed",
        ctx: { traceId: TRACE, orgId: ORG, workspaceId: WORKSPACE },
        workKey: WORK_KEY,
        payload: {},
      } as never),
    ).toBe("consumed");
    expect(await count("sessions.migration_assertion_uses")).toBe(20 + 1 + 1);
    // The still-live assertion is a replay after the purge.
    expect(await ledger.consume(fresh, "object-landed", {} as never)).toBe(
      "replayed",
    );
  });

  it("requires expires_at and plan_entry_digest; the chunk table is service-role only too", async () => {
    await expect(
      db.query(
        `INSERT INTO sessions.migration_assertion_uses (jti, nonce, grant_id, org_id, workspace_id, user_id, plan_digest)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [jti(), nonce(), GRANT, ORG, WORKSPACE, USER, PLAN],
      ),
    ).rejects.toThrow(/plan_entry_digest|expires_at/);
    for (const role of ["authenticated", "anon"]) {
      await db.exec(`RESET ROLE; SET ROLE ${role}`);
      try {
        await expect(
          db.query("SELECT 1 FROM sessions.migration_assertion_chunks"),
        ).rejects.toThrow(/permission denied/);
      } finally {
        await db.exec("RESET ROLE; SET ROLE service_role");
      }
    }
  });

  it("cross-ledger grant binding (blocker 3): a base read binds grant, org and plan for later commits, and a commit binds them for later base reads", async () => {
    const ledger = sqlMigrationAssertionLedger({
      query: async (text, params) => ({
        rows: (await db.query(text, params)).rows as never[],
      }),
      transaction: (work) =>
        db.transaction((tx) =>
          work({
            query: async (text, params) => ({
              rows: (await tx.query(text, params)).rows as never[],
            }),
          }),
        ),
    });
    const message = {
      v: 1,
      stage: "object-landed",
      ctx: { traceId: TRACE, orgId: ORG, workspaceId: WORKSPACE },
      workKey: WORK_KEY,
      payload: {},
    } as never;
    const useOf = (o: Record<string, unknown> = {}) => ({
      jti: jti(),
      nonce: nonce(),
      grantId: GRANT,
      planDigest: PLAN,
      planEntryDigest: PLAN,
      orgId: ORG,
      workspaceId: WORKSPACE,
      userId: USER,
      expiresAt: Math.floor(Date.now() / 1000) + 65,
      ...o,
    });
    const baseReadOf = (o: Record<string, unknown> = {}) =>
      useOf({ jti: `mbr_${randomBytes(16).toString("hex")}`, ...o });
    // A base read first binds the grant to (PLAN, ORG) ...
    expect(await ledger.consumeBaseRead(baseReadOf(), "k@base_read@r1")).toBe(
      "consumed",
    );
    // ... so a commit under the same grant with another plan, or another org, is refused,
    expect(
      await ledger.consume(
        useOf({ planDigest: "b".repeat(64) }),
        "object-landed",
        message,
      ),
    ).toBe("grant_mismatch");
    expect(
      await ledger.consume(
        useOf({ orgId: "org_other" }),
        "object-landed",
        message,
      ),
    ).toBe("grant_mismatch");
    // ... and nothing was written by the refused commits.
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    // A matching commit is accepted.
    expect(await ledger.consume(useOf(), "object-landed", message)).toBe(
      "consumed",
    );
    // The other direction: commits bind a second grant, and a base read with another plan or org is refused.
    const grant2 = `mgr_${"2".repeat(32)}`;
    expect(
      await ledger.consume(
        useOf({ grantId: grant2 }),
        "object-landed",
        message,
      ),
    ).toBe("consumed");
    expect(
      await ledger.consumeBaseRead(
        baseReadOf({ grantId: grant2, planDigest: "c".repeat(64) }),
        "k@base_read@r1",
      ),
    ).toBe("grant_mismatch");
    expect(
      await ledger.consumeBaseRead(
        baseReadOf({ grantId: grant2, orgId: "org_other" }),
        "k@base_read@r1",
      ),
    ).toBe("grant_mismatch");
    expect(await count("sessions.migration_base_read_uses")).toBe(1);
    // Separate replay namespaces: the same nonce in both ledgers is not a replay across them.
    const shared = nonce();
    expect(
      await ledger.consumeBaseRead(
        baseReadOf({ nonce: shared }),
        "k@base_read@r1",
      ),
    ).toBe("consumed");
    expect(
      await ledger.consume(useOf({ nonce: shared }), "object-landed", message),
    ).toBe("consumed");
    // But within one ledger it is.
    expect(
      await ledger.consumeBaseRead(
        baseReadOf({ nonce: shared }),
        "k@base_read@r1",
      ),
    ).toBe("replayed");
  });

  it("is service-role only: RLS on, no policy, no tenant grant", async () => {
    const { rows } = await db.query<{ rls: boolean; policies: number }>(
      `SELECT c.relrowsecurity AS rls,
              (SELECT count(*)::int FROM pg_policies
                WHERE schemaname = 'sessions'
                  AND tablename = 'migration_assertion_uses') AS policies
         FROM pg_class c
        WHERE c.oid = 'sessions.migration_assertion_uses'::regclass`,
    );
    expect(rows).toEqual([{ rls: true, policies: 0 }]);
    await insertUse(db, { jti: jti(), nonce: nonce() });
    for (const role of ["authenticated", "anon"]) {
      await db.exec(`RESET ROLE; SET ROLE ${role}`);
      try {
        await expect(
          db.query("SELECT * FROM sessions.migration_assertion_uses"),
        ).rejects.toThrow(/permission denied/);
        await expect(
          insertUse(db, { jti: jti(), nonce: nonce() }),
        ).rejects.toThrow(/permission denied/);
        await expect(
          db.query("DELETE FROM sessions.migration_assertion_uses"),
        ).rejects.toThrow(/permission denied/);
      } finally {
        await db.exec("RESET ROLE; SET ROLE service_role");
      }
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
  });
});
