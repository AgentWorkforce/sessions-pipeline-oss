/** HoopDb access: a pg Pool that satisfies the vendored SqlExecutor, plus readiness gates. */
import { readFileSync } from "node:fs";
import { RUNTIME_APPLICATION_FUNCTIONS } from "../hoopdb/migrate-guards.ts";
import { fileURLToPath } from "node:url";
import pg from "pg";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { log } from "./log.ts";
import { required } from "./env.ts";

export interface HoopDb extends SqlExecutor {
  pool: pg.Pool;
  end(): Promise<void>;
}
/**
 * Runs `work` in one transaction on one pooled connection: COMMIT when it
 * resolves, ROLLBACK when it throws (then rethrows). Used by the migration
 * assertion ledger so its row and the pipeline_jobs row commit together.
 */
export async function inTransaction<T>(db: Pick<HoopDb, "pool">, work: (tx: SqlExecutor) => Promise<T>): Promise<T> {
  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const tx: SqlExecutor = {
      async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
        const r = await client.query(text, params as unknown[]);
        return { rows: r.rows as R[] };
      },
    };
    const result = await work(tx);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export function openHoopDb(url = required("HOOPDB_URL")): HoopDb {
  const pool = new pg.Pool({ connectionString: url, max: 8, application_name: process.env.OTEL_SERVICE_NAME ?? "96bulls" });
  pool.on("error", (err) => log.error("hoopdb pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  return {
    pool,
    async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const r = await pool.query(text, params as unknown[]);
      return { rows: r.rows as R[] };
    },
    end: () => pool.end(),
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Blocks until HoopDb answers. Services start before the database in a plain container stack. */
export async function waitForHoopDb(db: HoopDb, timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  for (;;) {
    try {
      await db.query("SELECT 1");
      return;
    } catch (err) {
      attempt += 1;
      if (Date.now() > deadline) throw err;
      if (attempt % 5 === 1) log.info("waiting for hoopdb", { attempt });
      await sleep(2_000);
    }
  }
}

/** Plan steps that provision or check roles (`shim:`, `bootstrap:`, `verify:`): never schema, never awaited. */
export function isPrerequisiteStep(name: string): boolean {
  return name.startsWith("shim:") || name.startsWith("bootstrap:") || name.startsWith("verify:");
}

/**
 * Readiness marker written by the migrator (src/hoopdb/migrate.ts) or an external
 * verifier: one row per validated role. `status` is 'pending' from the moment a migrator
 * starts (before any migration step) until phase 2
 * (supabase/bootstrap/session_directory_search_worker_verify.sql) passes for that
 * `generation` (the migration-set digest), in the same transaction.
 */
export const ROLE_VALIDATION_TABLE = "sessions.__96bulls_role_validation";
export const ROLE_VALIDATION_NAME = "session_directory_search_worker";

/** What a reader requires: every schema step ledgered, and PASS for its own generation. */
export interface ExpectedSchema {
  names: string[];
  generation: string;
  /** Exact database mode bound into the role verifier; absent/invalid never enables compatibility exceptions. */
  dbMode?: "local" | "selfhosted" | "cloudsql" | "supabase";
}

export type SchemaVerdict =
  | { ok: true }
  | { ok: false; reason: "schema_pending"; count: number }
  | { ok: false; reason: "validation_pending" }
  | { ok: false; reason: "role_validation_failed"; error_code: string };

/**
 * The one phase 2 source. The migrator (src/hoopdb/migrate.ts), the external
 * verifier (supabase/bootstrap/session_directory_search_worker_mark_pass.sql, via
 * \ir) and the live readiness gate below all execute this same file.
 */
export const ROLE_VERIFY_SQL_PATH = fileURLToPath(
  new URL("../../supabase/bootstrap/session_directory_search_worker_verify.sql", import.meta.url),
);
const ROLE_VERIFY_SQL = readFileSync(ROLE_VERIFY_SQL_PATH, "utf8");
// The auth token verifier's phase 2 (supabase/bootstrap/session_token_verifier_verify.sql):
// the same contract for session_token_verifier — run by the migrator's verify step, the
// external mark_pass wrapper, and this live gate. Read-only.
export const INGEST_SEED_VERIFY_SQL_PATH=fileURLToPath(new URL("../../supabase/bootstrap/session_ingest_seeder_verify.sql",import.meta.url));
export const TOKEN_VERIFIER_VERIFY_SQL_PATH=fileURLToPath(new URL("../../supabase/bootstrap/session_token_verifier_verify.sql",import.meta.url));
export const INGEST_ROLE_VERIFY_SQL_PATH=fileURLToPath(new URL("../../supabase/bootstrap/session_ingest_verify.sql",import.meta.url));
const INGEST_SEED_VERIFY_SQL=readFileSync(INGEST_SEED_VERIFY_SQL_PATH,"utf8");
const TOKEN_VERIFIER_VERIFY_SQL=readFileSync(TOKEN_VERIFIER_VERIFY_SQL_PATH,"utf8");
const INGEST_ROLE_VERIFY_SQL=readFileSync(INGEST_ROLE_VERIFY_SQL_PATH,"utf8");
export interface SchemaVerifierSql {searchWorker:string;tokenVerifier:string;ingestSeeder:string;ingest:string}

/**
 * Live sessions-function boundary, read-only, part of every readiness check: no sessions
 * function may be EXECUTE-able through PUBLIC, nor by the connected (runtime) role unless
 * it owns the function (owner-run local mode) or is a superuser. The migrator revokes
 * PUBLIC EXECUTE on every migrate, but a function created in sessions after a migrate,
 * outside the migrator, is PUBLIC-executable by PostgreSQL's built-in default until the
 * next migrate; this check turns readiness (and worker claiming) off at the next probe
 * instead (55000).
 */
const RUNTIME_FUNCTION_REGPROCEDURES = RUNTIME_APPLICATION_FUNCTIONS.map(
  (fn) => `'${fn.replaceAll("'", "''")}'::regprocedure`,
).join(", ");

export const SESSIONS_FUNCTION_GATE_SQL = `DO $sessions_fn_gate$
DECLARE v text;
BEGIN
  SELECT string_agg(p.oid::regprocedure::text, ', ' ORDER BY 1) INTO v
    FROM pg_proc p
   WHERE p.pronamespace = coalesce(to_regnamespace('sessions')::oid, 0)
     AND (p.proacl IS NULL
          OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')
          OR (has_function_privilege(current_user, p.oid, 'EXECUTE')
              AND p.proowner <> (SELECT oid FROM pg_roles WHERE rolname = current_user)
              AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
              AND p.oid <> ALL (ARRAY[${RUNTIME_FUNCTION_REGPROCEDURES}]::oid[])));
  IF v IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'live gate: sessions functions executable by PUBLIC or the runtime role: ' || v;
  END IF;
END
$sessions_fn_gate$`;

/** Runs `work` in one transaction on one connection (inTransaction, or a test double). */
export type Transact = <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;

/** Upper bound for HOOPDB_ROLE_GATE_TIMEOUT_MS: a gate slower than this is misconfigured. */
export const ROLE_GATE_TIMEOUT_MAX_MS = 10_000;

function gateTimeoutMs(): number {
  const raw = process.env.HOOPDB_ROLE_GATE_TIMEOUT_MS;
  const ms = raw === undefined || raw === "" ? 2_000 : Number(raw);
  if (!Number.isInteger(ms) || ms < 1 || ms > ROLE_GATE_TIMEOUT_MAX_MS)
    throw Object.assign(new Error(`HOOPDB_ROLE_GATE_TIMEOUT_MS must be an integer 1..${ROLE_GATE_TIMEOUT_MAX_MS}`), {
      code: "gate_timeout_invalid",
    });
  return ms;
}

const MARKER_SQL = `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1`;

/**
 * Is HoopDb ready for this image, now? One transaction, READ ONLY and pinned to
 * READ COMMITTED, with its own statement_timeout (HOOPDB_ROLE_GATE_TIMEOUT_MS, default
 * 2000, at most ROLE_GATE_TIMEOUT_MAX_MS), never the migrator's advisory lock and never
 * cached. In order:
 *   1. the ledger is complete (else schema_pending);
 *   2. the marker is 'pass' for expected.generation (else validation_pending; cheap
 *      early exit, phase 2 is not run);
 *   3. phase 2 itself, the shared supabase/bootstrap/session_directory_search_worker_verify.sql
 *      (any error: role_validation_failed with its SQLSTATE, logged);
 *   4. the marker again, as the FINAL read (else validation_pending).
 * READ COMMITTED is deliberate: every statement takes a fresh snapshot, so the final read
 * sees a migrator that flipped the marker to 'pending' while phase 2 ran. Under
 * REPEATABLE READ it would reuse step 1's snapshot and miss that flip.
 *
 * The verdict holds as of the final read only (current-tick semantics): the caller acts
 * on it for one probe response or one claim. Any other error fails closed: a missing
 * ledger is schema_pending, a missing marker table validation_pending, anything else
 * role_validation_failed. The marker is generation, coordination and audit evidence; it
 * is never the sole authority.
 */
export async function schemaVerdict(
  expected: ExpectedSchema,
  transact: Transact,
  opts: { afterVerify?: () => Promise<void> } = {},
): Promise<SchemaVerdict> {
  type Stage = "config" | "ledger" | "marker" | "verify" | "final";
  let stage = "config" as Stage; // widened: it is advanced inside the transaction callback
  try {
    const timeoutMs = gateTimeoutMs();
    return await transact(async (tx): Promise<SchemaVerdict> => {
      await tx.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY");
      await tx.query(`SET LOCAL statement_timeout = ${timeoutMs}`);
      await tx.query("SELECT set_config('hoopdb.db_mode', $1, true)", [expected.dbMode ?? "unspecified"]);
      stage = "ledger";
      const have = new Set(
        (await tx.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations")).rows.map((r) => r.name),
      );
      // The role shim, bootstrap and verify steps are not schema; the marker covers verify.
      const missing = expected.names.filter((n) => !isPrerequisiteStep(n) && !have.has(n));
      if (missing.length) return { ok: false, reason: "schema_pending", count: missing.length };
      const passed = async () => {
        const m = (await tx.query<{ generation: string; status: string }>(MARKER_SQL, [ROLE_VALIDATION_NAME])).rows[0];
        return m?.status === "pass" && m.generation === expected.generation;
      };
      stage = "marker";
      if (!(await passed())) return { ok: false, reason: "validation_pending" };
      stage = "verify";
      await tx.query(ROLE_VERIFY_SQL);
      await tx.query(TOKEN_VERIFIER_VERIFY_SQL);
      await tx.query(INGEST_SEED_VERIFY_SQL);
      await tx.query(INGEST_ROLE_VERIFY_SQL);
      await tx.query(SESSIONS_FUNCTION_GATE_SQL);
      if (opts.afterVerify) await opts.afterVerify(); // test seam: a concurrent flip lands here
      stage = "final";
      if (!(await passed())) return { ok: false, reason: "validation_pending" };
      return { ok: true };
    });
  } catch (err) {
    const error_code = (err as { code?: string }).code ?? "role_gate_error";
    if (error_code === "42P01" && stage === "ledger")
      return { ok: false, reason: "schema_pending", count: expected.names.filter((n) => !isPrerequisiteStep(n)).length };
    if (error_code === "42P01" && (stage === "marker" || stage === "final")) return { ok: false, reason: "validation_pending" };
    log.warn("live role validation gate closed", { error_code, reason: stage });
    return { ok: false, reason: "role_validation_failed", error_code };
  }
}

/**
 * Blocks until HoopDb is ready for this build (schemaVerdict): every migration
 * recorded in `sessions.__96bulls_migrations` and the Worker role validation marked
 * PASS for this build's generation.
 */
export async function waitForSchema(db: HoopDb, expected: ExpectedSchema, timeoutMs = 300_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  let reason = "unreachable";
  for (;;) {
    try {
      const v = await schemaVerdict(expected, (work) => inTransaction(db, work));
      if (v.ok) return;
      reason = v.reason;
    } catch {
      // ledger not there yet
      reason = "schema_pending";
    }
    attempt += 1;
    if (Date.now() > deadline) throw new Error(`96bulls: hoopdb schema did not become ready (${reason})`);
    if (attempt % 5 === 1) log.info("waiting for hoopdb schema", { attempt, reason });
    await sleep(2_000);
  }
}
