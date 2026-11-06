/**
 * The readiness marker: HoopDb is ready for an image only when phase 2 (the Worker
 * role's exact-grant validator) passed for that image's migration-set generation.
 * Runs the real migrator (runMigrations) on in-process PGlite.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import type { AddressInfo } from "node:net";
import { SESSIONS_FUNCTION_GATE_SQL } from "../../shared/db.ts";
import {
  ROLE_VALIDATION_TABLE,
  ROLE_VERIFY_SQL_PATH,
  schemaVerdict,
  waitForSchema,
  type HoopDb,
  type Transact,
} from "../../shared/db.ts";
import {
  gateOpen,
  serveHealth,
  type WorkerHealth,
} from "../../shared/worker.ts";
import {
  expectedSchema,
  generation,
  plan,
  runMigrations,
  type MigrationClient,
  type MigrationProfile,
} from "../migrate.ts";

/** The local Docker profile, as PGlite (its superuser runs everything). */
const LOCAL: MigrationProfile = {
  mode: "local",
  shim: true,
  bootstrap: true,
  verify: true,
  runtimeRole: "postgres",
};

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const WORKER = "session_directory_search_worker";
const VERIFY = readFileSync(
  join(root, "supabase/bootstrap/session_directory_search_worker_verify.sql"),
  "utf8",
);
const TOKEN_VERIFY = readFileSync(
  join(root, "supabase/bootstrap/session_token_verifier_verify.sql"),
  "utf8",
);

const SEEDER_VERIFY = readFileSync(join(root,"supabase/bootstrap/session_ingest_seeder_verify.sql"),"utf8");
const INGEST_VERIFY = readFileSync(join(root,"supabase/bootstrap/session_ingest_verify.sql"),"utf8");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

/** The migrator's client over PGlite: parameterless text may hold several statements. */
function client(db: PGlite): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length)
        return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}

async function fresh(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  return db;
}

/** One transaction on the PGlite connection, as inTransaction does on a pooled one. */
const transact =
  (db: PGlite): Transact =>
  (work) =>
    db.transaction((tx) => work(tx as never)) as never;

/** A HoopDb over PGlite (pool.connect yields the same single connection). */
function hoopLike(db: PGlite): HoopDb {
  const c = client(db);
  return {
    ...c,
    pool: { connect: async () => ({ ...c, release() {} }) } as never,
    end: async () => {},
  } as HoopDb;
}

const verdict = async (db: PGlite) =>
  schemaVerdict(await expectedSchema(), transact(db));
const marker = async (db: PGlite) =>
  (
    await db.query<{ generation: string; status: string }>(
      `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE}`,
    )
  ).rows;
const failure = async (run: Promise<unknown>) =>
  (await run.then(
    () => null,
    (caught: unknown) => caught,
  )) as { code?: string; message?: string } | null;

describe("Worker role validation marker", () => {
  test("a clean run marks PASS for this generation and readiness goes true; a no-op rerun keeps it", async () => {
    const db = await fresh();
    expect(await verdict(db)).toMatchObject({
      ok: false,
      reason: "schema_pending",
    }); // no ledger yet
    const first = await runMigrations(client(db), { profile: LOCAL });
    expect(first.applied).toBeGreaterThan(0);
    expect(await marker(db)).toEqual([
      { generation: await generation(), status: "pass" },
    ]);
    expect(await verdict(db)).toEqual({ ok: true });
    const again = await runMigrations(client(db), { profile: LOCAL });
    expect(again.applied).toBe(0);
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("after phase 2 fails, readiness stays false across retries until the drift is fixed and phase 2 passes", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(`GRANT SELECT ON sessions.session_directory TO ${WORKER}`);
    for (let attempt = 0; attempt < 2; attempt++) {
      const error = await failure(
        runMigrations(client(db), { profile: LOCAL }),
      );
      expect(error?.code).toBe("55000");
      expect(error?.message).toContain(
        "has privileges on table sessions.session_directory",
      );
      expect(await verdict(db)).toEqual({
        ok: false,
        reason: "validation_pending",
      });
      expect((await marker(db))[0]?.status).toBe("pending");
    }
    await db.exec(`REVOKE SELECT ON sessions.session_directory FROM ${WORKER}`);
    await runMigrations(client(db), { profile: LOCAL });
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("an earlier PASS can't mask a later failing validator: the next migrator invalidates it first", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL }); // process A: PASS
    expect(await verdict(db)).toEqual({ ok: true });
    await db.exec(
      `GRANT SELECT (org_id) ON sessions.session_directory TO ${WORKER}`,
    );
    // Process B, observed by a reader racing it mid-run (just before phase 2).
    let seenMidRun: unknown;
    const error = await failure(
      runMigrations(client(db), {
        profile: LOCAL,
        beforeVerify: async () => {
          seenMidRun = await verdict(db);
        },
      }),
    );
    expect(error?.code).toBe("55000");
    expect(seenMidRun).toEqual({ ok: false, reason: "validation_pending" });
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
  });

  test("a stale PASS from another generation is not ready, for either side", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(
      `UPDATE ${ROLE_VALIDATION_TABLE} SET generation = '${"0".repeat(64)}'`,
    );
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    // And a reader built from a different migration set never accepts this PASS.
    await runMigrations(client(db), { profile: LOCAL });
    const other = { ...(await expectedSchema()), generation: "f".repeat(64) };
    expect(await schemaVerdict(other, transact(db))).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("a failure between the final migration commit and phase 2 leaves the ledger complete and readiness false", async () => {
    const db = await fresh();
    const error = await failure(
      runMigrations(client(db), {
        profile: LOCAL,
        beforeVerify: async () => {
          throw Object.assign(new Error("injected crash"), { code: "XXINJ" });
        },
      }),
    );
    expect(error?.code).toBe("XXINJ");
    expect((await marker(db))[0]?.status).toBe("pending");
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
  });

  test("a migration failure mid-run leaves readiness false, even after an earlier PASS", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const steps = await plan(LOCAL);
    // A real file whose SQL fails when applied, after the recorded prefix.
    const dir = mkdtempSync(join(tmpdir(), "g6-broken-"));
    writeFileSync(join(dir, "9999_broken.sql"), "SELECT 1 / 0;");
    const broken = [
      ...steps.slice(0, -1),
      {
        name: "96bulls:9999_broken.sql",
        path: join(dir, "9999_broken.sql"),
        sentinel: { evidence: "test step", sql: "SELECT false AS applied" },
      },
      ...steps.slice(-1),
    ];
    expect(
      await failure(
        runMigrations(client(db), { profile: LOCAL, steps: broken }),
      ),
    ).not.toBeNull();
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
  });

  test("with the in-process verifier off the migrator never marks itself ready; only an external PASS for this generation does", async () => {
    const db = await fresh();
    const steps = (await plan(LOCAL)).filter(
      (s) => !s.name.startsWith("verify:"),
    );
    await runMigrations(client(db), { profile: LOCAL, steps });
    expect((await marker(db))[0]).toEqual({
      generation: await generation(),
      status: "pending",
    });
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    // The external verifier's transaction (the mark_pass.sql contract, minus psql).
    await db.exec(`BEGIN; SELECT pg_advisory_xact_lock(961996, 1);
      ${VERIFY}
      ${TOKEN_VERIFY}
      UPDATE ${ROLE_VALIDATION_TABLE} SET status = 'pass', updated_at = now()
       WHERE name = '${WORKER}' AND generation = '${await generation()}' AND status = 'pending';
      COMMIT;`);
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("waitForSchema refuses to report ready while the marker is pending", async () => {
    const db = await fresh();
    const steps = (await plan(LOCAL)).filter(
      (s) => !s.name.startsWith("verify:"),
    );
    await runMigrations(client(db), { profile: LOCAL, steps });
    const error = await failure(
      waitForSchema(hoopLike(db), await expectedSchema(), 0),
    );
    expect(error?.message).toContain("validation_pending");
  });
});

describe("the live gate: the marker is never the sole authority", () => {
  const DRIFTED = {
    ok: false,
    reason: "role_validation_failed",
    error_code: "55000",
  };

  test("post-PASS drift with no migrator: schemaVerdict, waitForSchema and worker /readyz all fail, and recover when fixed", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    expect(await verdict(db)).toEqual({ ok: true });
    expect((await marker(db))[0]?.status).toBe("pass"); // the marker still says PASS
    await db.exec(`GRANT SELECT ON sessions.session_directory TO ${WORKER}`);
    expect(await verdict(db)).toEqual(DRIFTED);
    const waited = await failure(
      waitForSchema(hoopLike(db), await expectedSchema(), 0),
    );
    expect(waited?.message).toContain("role_validation_failed");
    // Worker /readyz, the real health server.
    const expected = await expectedSchema();
    const health: WorkerHealth = {
      ready: true,
      lastTickAt: Date.now(),
      consecutiveDbFailures: 0,
      schemaReady: async () => (await schemaVerdict(expected, transact(db))).ok,
    };
    const server = serveHealth(health, 0);
    await new Promise((r) => server.once("listening", r));
    const port = (server.address() as AddressInfo).port;
    try {
      const probe = async () =>
        (await fetch(`http://127.0.0.1:${port}/readyz`)).status;
      expect(await probe()).toBe(503);
      await db.exec(
        `REVOKE SELECT ON sessions.session_directory FROM ${WORKER}`,
      );
      expect(await probe()).toBe(200);
      expect(await verdict(db)).toEqual({ ok: true });
    } finally {
      server.close();
    }
  });

  test("the worker stops claiming when drift appears mid-run, and resumes once it is fixed", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const expected = await expectedSchema();
    const health = {
      schemaReady: async () => (await schemaVerdict(expected, transact(db))).ok,
    };
    let claims = 0;
    const tick = async () => {
      if (await gateOpen(health)) claims += 1; // runWorker claims only past this gate
    };
    await tick();
    await tick();
    expect(claims).toBe(2);
    await db.exec(
      `GRANT EXECUTE ON FUNCTION sessions.session_directory_worker_scope(text) TO ${WORKER}`,
    );
    await tick();
    await tick();
    expect(claims).toBe(2); // paused: nothing new claimed
    await db.exec(
      `REVOKE EXECUTE ON FUNCTION sessions.session_directory_worker_scope(text) FROM ${WORKER}`,
    );
    await tick();
    expect(claims).toBe(3);
    // An unset or throwing gate is closed.
    expect(await gateOpen({})).toBe(false);
    expect(
      await gateOpen({
        schemaReady: async () => {
          throw new Error("db down");
        },
      }),
    ).toBe(false);
  });

  test("an unreadable catalog fails closed (42501), never open", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(`CREATE ROLE g6_runtime NOLOGIN;
      GRANT USAGE ON SCHEMA sessions TO g6_runtime;
      GRANT SELECT ON sessions.__96bulls_migrations, ${ROLE_VALIDATION_TABLE} TO g6_runtime;`);
    await db.exec("SET ROLE g6_runtime");
    expect(await verdict(db)).toEqual({ ok: true }); // the runtime role reads every catalog it needs
    await db.exec(
      "RESET ROLE; REVOKE SELECT ON pg_catalog.pg_db_role_setting FROM PUBLIC; SET ROLE g6_runtime",
    );
    try {
      expect(await verdict(db)).toEqual({
        ok: false,
        reason: "role_validation_failed",
        error_code: "42501",
      });
    } finally {
      await db.exec(
        "RESET ROLE; GRANT SELECT ON pg_catalog.pg_db_role_setting TO PUBLIC",
      );
    }
  });

  test("the check is one READ COMMITTED, READ ONLY transaction with its mode bound, its own statement_timeout, ledger, marker, shared phase 2 file, then the marker as the final read", async () => {
    const expected = await expectedSchema();
    const seen: string[] = [];
    const recorder: Transact = async (work) =>
      work({
        async query(text: string) {
          seen.push(text);
          if (text.startsWith("SELECT name FROM sessions.__96bulls_migrations"))
            return { rows: expected.names.map((name) => ({ name })) } as never;
          if (text.startsWith("SELECT generation, status FROM"))
            return {
              rows: [{ generation: expected.generation, status: "pass" }],
            } as never;
          return { rows: [] } as never;
        },
      } as never);
    expect(await schemaVerdict(expected, recorder)).toEqual({ ok: true });
    expect(seen[0]).toBe(
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
    );
    expect(seen[1]).toMatch(/^SET LOCAL statement_timeout = \d+$/);
    expect(seen[2]).toBe("SELECT set_config('hoopdb.db_mode', $1, true)");
    expect(seen[3]).toStartWith(
      "SELECT name FROM sessions.__96bulls_migrations",
    );
    expect(seen[4]).toStartWith("SELECT generation, status FROM");
    expect(seen[5]).toBe(VERIFY);
    expect(seen[6]).toBe(TOKEN_VERIFY);
    expect(seen[7]).toBe(SEEDER_VERIFY);
    expect(seen[8]).toBe(INGEST_VERIFY);
    expect(seen[9]).toBe(SESSIONS_FUNCTION_GATE_SQL); // the live sessions-function boundary
    expect(seen[10]).toStartWith("SELECT generation, status FROM"); // the final read
    expect(seen).toHaveLength(11);
    // No advisory lock anywhere on the probe path.
    expect(seen.some((q) => /advisory/i.test(q))).toBe(false);
    // One source: the migrator's verify step and mark_pass.sql use the same file.
    expect(
      (await plan(LOCAL)).find((st) => st.name.startsWith("verify:"))?.path,
    ).toBe(ROLE_VERIFY_SQL_PATH);
    expect(MARK_PASS).toContain(
      "\\ir session_directory_search_worker_verify.sql",
    );
  });

  /** A real PGlite transaction whose marker reads see `flip` once it is set (a commit by another connection, as READ COMMITTED would see it). */
  const racing =
    (
      db: PGlite,
      state: { flipTo?: string; markerReads: number; verified: boolean },
    ): Transact =>
    (work) =>
      db.transaction((tx) =>
        work({
          async query(text: string, params?: unknown[]) {
            const r = (await tx.query(text, params)) as { rows: unknown[] };
            if (text === VERIFY) state.verified = true;
            if (text.startsWith("SELECT generation, status FROM")) {
              state.markerReads += 1;
              if (state.flipTo)
                return {
                  rows: (r.rows as { generation: string }[]).map((m) => ({
                    ...m,
                    status: state.flipTo,
                  })),
                };
            }
            return r;
          },
        } as never),
      ) as never;

  test("race: the marker flips to pending after phase 2 and before the final read: not ready", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const state = {
      flipTo: undefined as string | undefined,
      markerReads: 0,
      verified: false,
    };
    const v = await schemaVerdict(await expectedSchema(), racing(db, state), {
      afterVerify: async () => {
        state.flipTo = "pending"; // a migrator's pending upsert committed now
      },
    });
    expect(v).toEqual({ ok: false, reason: "validation_pending" });
    expect(state).toMatchObject({ verified: true, markerReads: 2 });
  });

  test("race, reverse: pending at the first read and pass afterwards never reports ready without the live check", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const state = {
      flipTo: "pending" as string | undefined,
      markerReads: 0,
      verified: false,
    };
    expect(
      await schemaVerdict(await expectedSchema(), racing(db, state)),
    ).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    expect(state).toMatchObject({ verified: false, markerReads: 1 }); // early exit, nothing reported ready
    // Now it reads 'pass', but the role has drifted: the live check still runs and closes it.
    await db.exec(`GRANT SELECT ON sessions.session_index TO ${WORKER}`);
    const later = {
      flipTo: undefined as string | undefined,
      markerReads: 0,
      verified: false,
    };
    expect(
      await schemaVerdict(await expectedSchema(), racing(db, later)),
    ).toEqual({
      ok: false,
      reason: "role_validation_failed",
      error_code: "55000",
    });
  });

  test("HOOPDB_ROLE_GATE_TIMEOUT_MS above the cap (or invalid) closes the gate instead of running unbounded", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const saved = process.env.HOOPDB_ROLE_GATE_TIMEOUT_MS;
    try {
      for (const bad of ["10001", "0", "abc", "1.5"]) {
        process.env.HOOPDB_ROLE_GATE_TIMEOUT_MS = bad;
        expect(await verdict(db)).toEqual({
          ok: false,
          reason: "role_validation_failed",
          error_code: "gate_timeout_invalid",
        });
      }
      process.env.HOOPDB_ROLE_GATE_TIMEOUT_MS = "10000";
      expect(await verdict(db)).toEqual({ ok: true });
    } finally {
      if (saved === undefined) delete process.env.HOOPDB_ROLE_GATE_TIMEOUT_MS;
      else process.env.HOOPDB_ROLE_GATE_TIMEOUT_MS = saved;
    }
  });

  // mark_pass.sql as psql runs it: \ir inlines the verifier and psql quotes
  // both the migration generation and the explicit database mode.
  const MARK_PASS = readFileSync(
    join(
      root,
      "supabase/bootstrap/session_directory_search_worker_mark_pass.sql",
    ),
    "utf8",
  );
  const markPass = (gen: string) =>
    MARK_PASS.split("\n")
      .filter((l) => !l.startsWith("\\set "))
      .map((l) =>
        l === "\\ir session_directory_search_worker_verify.sql"
          ? VERIFY
          : l === "\\ir session_token_verifier_verify.sql"
            ? TOKEN_VERIFY
            : l === "\\ir session_ingest_seeder_verify.sql"
              ? SEEDER_VERIFY
              : l === "\\ir session_ingest_verify.sql"
                ? INGEST_VERIFY
                : l
                .replace(":'generation'", `'${gen.replace(/'/g, "''")}'`)
                .replace(":'db_mode'", "'local'"),
      )
      .join("\n");
  const runMarkPass = async (db: PGlite, gen: string) => {
    const error = await failure(db.exec(markPass(gen)));
    if (error) await db.exec("ROLLBACK").catch(() => undefined);
    return error;
  };

  test("mark_pass.sql, run as written: wrong generation, drift and already-pass are refused; only this generation's pending row passes", async () => {
    const db = await fresh();
    const steps = (await plan(LOCAL)).filter(
      (st) => !st.name.startsWith("verify:"),
    );
    await runMigrations(client(db), { profile: LOCAL, steps });
    const gen = await generation();
    expect((await runMarkPass(db, "f".repeat(64)))?.message).toContain(
      "no pending role validation for generation",
    );
    expect((await runMarkPass(db, "not-a-digest"))?.message).toContain(
      "generation must be the 64-hex migration-set digest",
    );
    await db.exec(
      `GRANT SELECT (org_id) ON sessions.session_directory TO ${WORKER}`,
    );
    const drift = await runMarkPass(db, gen);
    expect(drift?.code).toBe("55000");
    expect(drift?.message).toContain(
      "has privileges on column org_id of table sessions.session_directory",
    );
    expect((await marker(db))[0]?.status).toBe("pending");
    await db.exec(
      `REVOKE SELECT (org_id) ON sessions.session_directory FROM ${WORKER}`,
    );
    expect(await runMarkPass(db, gen)).toBeNull();
    expect(await marker(db)).toEqual([{ generation: gen, status: "pass" }]);
    expect(await verdict(db)).toEqual({ ok: true });
    expect((await runMarkPass(db, gen))?.message).toContain(
      "no pending role validation for generation",
    );
  });
});

describe("role check deadlock retry (40P01)", () => {
  /** Fails the first `failures` submissions of the first verify SQL with a deadlock. */
  function deadlocking(db: PGlite, failures: number) {
    const base = client(db);
    let seen = 0;
    return {
      get attempts() { return seen; },
      client: {
        async query<R>(text: string, params?: unknown[]) {
          if (text === VERIFY) {
            seen += 1;
            if (seen <= failures)
              throw Object.assign(new Error("deadlock detected"), { code: "40P01" });
          }
          return base.query<R>(text, params);
        },
      } as MigrationClient,
    };
  }

  test("a deadlocked verify transaction rolls back and the retry marks PASS", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const dl = deadlocking(db, 1);
    const result = await runMigrations(dl.client, { profile: LOCAL });
    expect(result.applied).toBe(0);
    expect(dl.attempts).toBe(2);
    expect(await marker(db)).toEqual([{ generation: await generation(), status: "pass" }]);
  });

  test("a persistent deadlock gives up after the bounded attempts and leaves the marker pending", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const dl = deadlocking(db, 99);
    const caught = await failure(runMigrations(dl.client, { profile: LOCAL }));
    expect(caught?.code).toBe("40P01");
    expect(dl.attempts).toBe(3);
    expect(await marker(db)).toEqual([{ generation: await generation(), status: "pending" }]);
  });

  test("a non-deadlock verify failure is not retried", async () => {
    const db = await fresh();
    await runMigrations(client(db), { profile: LOCAL });
    const base = client(db);
    let seen = 0;
    const c: MigrationClient = {
      async query<R>(text: string, params?: unknown[]) {
        if (text === VERIFY && ++seen) throw Object.assign(new Error("lock timeout"), { code: "55P03" });
        return base.query<R>(text, params);
      },
    };
    expect((await failure(runMigrations(c, { profile: LOCAL })))?.code).toBe("55P03");
    expect(seen).toBe(1);
    expect(await marker(db)).toEqual([{ generation: await generation(), status: "pending" }]);
  });
});
