/**
 * HoopDb migrations, in order: the Supabase role shim and the search Worker role
 * bootstrap (plain Postgres only; the migrator is that database's superuser), the vendored relayhistory-cloud `sessions` migrations exactly as pinned, then
 * the 96bulls migrations. Each file is recorded with its sha256 in a ledger and
 * never re-applied; a changed applied file fails the run.
 *
 * Runs as a one-shot container (`96b-hoopdb-migrate`) or a Kubernetes Job. No
 * Helm hooks. Idempotent and safe to run on every `up`.
 */
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  functionBoundaryEvidence,
  migrationChecksumAlias,
  migrationChecksumMatches,
  RUNTIME_APPLICATION_FUNCTIONS,
  RUNTIME_APPLICATION_POLICY,
  RUNTIME_APPLICATION_RLS_TABLES,
  RUNTIME_APPLICATION_SEQUENCES,
  RUNTIME_APPLICATION_TABLE_PRIVILEGES,
  runtimeRoleViolations,
  sequencePreflight,
  sessionsFunctionAclDrift,
  sessionsFunctionAclPreview,
  type FunctionBoundaryMode,
} from "./migrate-guards.ts";
import {
  openHoopDb,
  waitForHoopDb,
  ROLE_VALIDATION_NAME,
  ROLE_VALIDATION_TABLE,
  ROLE_VERIFY_SQL_PATH,
  type ExpectedSchema,
} from "../shared/db.ts";
import { log } from "../shared/log.ts";
import { optional } from "../shared/env.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
// The relayhistory-cloud sessions migrations, moved into this repo by #1/#6 (provenance: supabase/README.md).
const UPSTREAM_MIGRATIONS = join(ROOT, "supabase/migrations");
const SHIM = join(ROOT, "supabase/tests/supabase_shim.sql");
// Privileged and idempotent. On Supabase and CNPG a deploy step runs it as a superuser
// before migrate (runbooks/session-directory-search-worker-bootstrap.md); here it then
// only verifies the bare role spec, on every run (never ledgered), and fails closed if
// the role is absent or differs.
const WORKER_BOOTSTRAP = join(
  ROOT,
  "supabase/bootstrap/session_directory_search_worker.sql",
);
// The auth token verifier role (session_token_verifier): the same contract, its own
// file. 20261101000100 verifies it and fails closed if absent (42704); it never creates it.
const TOKEN_VERIFIER_BOOTSTRAP = join(
  ROOT,
  "supabase/bootstrap/session_token_verifier.sql",
);
// Phase 2 for the verifier role: its exact catalog shape (role spec, memberships,
// schema USAGE, no table privilege, EXECUTE on exactly auth_session_verify).
// Runs after every migration like WORKER_VERIFY; read-only and never ledgered.
const TOKEN_VERIFIER_VERIFY = join(
  ROOT,
  "supabase/bootstrap/session_token_verifier_verify.sql",
);
// The session ingest roles (bulls96_session_ingest, _seeder): the same contract, its own
// file. 20261102010000 verifies them and fails closed if absent (42704); it never creates them.
const INGEST_BOOTSTRAP = join(ROOT, "supabase/bootstrap/session_ingest_roles.sql");
const INGEST_ROLE_VERIFY = join(ROOT,"supabase/bootstrap/session_ingest_verify.sql");
// Phase 2, after every migration: the role's exact direct-grant allowlist. Read-only, any
// role that reads the catalogs; runs everywhere, on every migrate, never ledgered.
// The one phase 2 source, shared with the live readiness gate (src/shared/db.ts).
const WORKER_VERIFY = ROLE_VERIFY_SQL_PATH;
const INGEST_SEED_VERIFY = join(ROOT,"supabase/bootstrap/session_ingest_seeder_verify.sql");
const OURS = join(ROOT, "src/hoopdb/migrations");

export const MIGRATION_LEDGER = "sessions.__96bulls_migrations";

interface Step {
  name: string;
  path: string;
  sentinel?: { evidence: string; sql: string };
}
/**
 * The database-mode contract. The migrator never infers the target from a hostname:
 * HOOPDB_DB_MODE is required and has no default.
 *   local       the Docker stack: plain Postgres, the migrator is its superuser.
 *   selfhosted  CNPG or another operator-controlled plain Postgres deployment.
 *   cloudsql    Google Cloud SQL: plain Postgres with Google's fixed platform superuser.
 *   supabase    a Supabase project. The plain-Postgres role shim and the privileged
 *               role bootstrap are refused outright (supabase_shim.sql: "Never apply
 *               this to a Supabase project"); the roles are platform or operator steps.
 * HOOPDB_SUPABASE_SHIM must be set explicitly ("true" or "false") in local and
 * selfhosted and cloudsql mode, and may only be unset or "false" in supabase mode.
 * HOOPDB_ROLE_BOOTSTRAP defaults to the shim choice and is likewise refused in supabase
 * mode. HOOPDB_RUNTIME_ROLE (required) names the role the services connect as: it gets
 * the exact application ACL in migrate-guards.ts plus read-only ledger/marker access.
 * Resolved before any connection; an invalid profile writes nothing anywhere.
 */
export type DbMode = "local" | "selfhosted" | "cloudsql" | "supabase";
export interface MigrationProfile {
  mode: DbMode;
  shim: boolean;
  bootstrap: boolean;
  verify: boolean;
  runtimeRole: string;
  /** HOOPDB_RUNTIME_ROLE_LOGIN; required outside local mode (the role is dedicated there). */
  runtimeLogin?: boolean;
  /** HOOPDB_RUNTIME_WORKLOAD_ROLE: the one role allowed to be a member of the runtime role. */
  workloadRole?: string;
  /**
   * HOOPDB_FUNCTION_BOUNDARY_MODE: which durable function boundary the reviewed deployment
   * selects. Configuration, not a human approval: the gate passes only when the catalog
   * itself proves the selected control. Required outside local mode.
   */
  functionBoundary?: FunctionBoundaryMode;
}

function profileError(code: string, message: string): Error {
  return Object.assign(new Error(`96bulls: ${message}`), { code });
}

function strictFlag(
  env: Record<string, string | undefined>,
  name: string,
): boolean | undefined {
  const v = env[name];
  if (v === undefined || v === "") return undefined;
  if (v === "1" || v.toLowerCase() === "true") return true;
  if (v === "0" || v.toLowerCase() === "false") return false;
  throw profileError(
    "db_profile_invalid",
    `${name} must be "true" or "false" (got "${v}")`,
  );
}

export function migrationProfile(
  env: Record<string, string | undefined> = process.env,
): MigrationProfile {
  const mode = env.HOOPDB_DB_MODE;
  if (
    mode !== "local" &&
    mode !== "selfhosted" &&
    mode !== "cloudsql" &&
    mode !== "supabase"
  )
    throw profileError(
      "db_mode_invalid",
      "HOOPDB_DB_MODE must be local, selfhosted, cloudsql or supabase; there is no default",
    );
  const shim = strictFlag(env, "HOOPDB_SUPABASE_SHIM");
  const bootstrap = strictFlag(env, "HOOPDB_ROLE_BOOTSTRAP");
  const verify = strictFlag(env, "HOOPDB_ROLE_VERIFY") ?? true;
  if (mode === "supabase" && shim)
    throw profileError(
      "shim_forbidden",
      "the plain-Postgres role shim never runs in supabase mode",
    );
  if (mode === "supabase" && bootstrap)
    throw profileError(
      "bootstrap_forbidden",
      "the privileged role bootstrap never runs from the migrator in supabase mode",
    );
  if (mode !== "supabase" && shim === undefined)
    throw profileError(
      "shim_unset",
      `HOOPDB_SUPABASE_SHIM must be set explicitly in ${mode} mode`,
    );
  const ident = /^[A-Za-z_][A-Za-z0-9_$-]{0,62}$/;
  const runtimeRole = env.HOOPDB_RUNTIME_ROLE ?? "";
  if (!ident.test(runtimeRole))
    throw profileError(
      "runtime_role_unset",
      "HOOPDB_RUNTIME_ROLE must name the role the services connect as",
    );
  const runtimeLogin = strictFlag(env, "HOOPDB_RUNTIME_ROLE_LOGIN");
  if (mode !== "local" && runtimeLogin === undefined)
    throw profileError(
      "runtime_role_login_unset",
      `HOOPDB_RUNTIME_ROLE_LOGIN must be set explicitly in ${mode} mode`,
    );
  const workloadRole = env.HOOPDB_RUNTIME_WORKLOAD_ROLE || undefined;
  if (workloadRole !== undefined && !ident.test(workloadRole))
    throw profileError(
      "db_profile_invalid",
      "HOOPDB_RUNTIME_WORKLOAD_ROLE must be a role name",
    );
  if (env.HOOPDB_FUNCTION_BOUNDARY_APPROVED !== undefined)
    throw profileError(
      "db_profile_invalid",
      "HOOPDB_FUNCTION_BOUNDARY_APPROVED was renamed HOOPDB_FUNCTION_BOUNDARY_MODE (it selects a catalog-proven control, it is not an approval); remove it",
    );
  const fb = env.HOOPDB_FUNCTION_BOUNDARY_MODE || undefined;
  if (fb === "event-trigger" && mode !== "supabase")
    throw profileError(
      "function_boundary_mode_unsupported",
      "HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger is supported only with HOOPDB_DB_MODE=supabase (selfhosted and cloudsql use global-default-revoke)",
    );
  if (
    fb !== undefined &&
    fb !== "global-default-revoke" &&
    fb !== "event-trigger"
  )
    throw profileError(
      "db_profile_invalid",
      "HOOPDB_FUNCTION_BOUNDARY_MODE must be global-default-revoke or (supabase only) event-trigger",
    );
  const runtime = {
    runtimeRole,
    ...(runtimeLogin === undefined ? {} : { runtimeLogin }),
    ...(workloadRole ? { workloadRole } : {}),
    ...(fb ? { functionBoundary: fb as FunctionBoundaryMode } : {}),
  };
  if (mode === "supabase") {
    if (shim)
      throw profileError(
        "shim_forbidden",
        "the plain-Postgres role shim never runs in supabase mode",
      );
    if (bootstrap)
      throw profileError(
        "bootstrap_forbidden",
        "the privileged role bootstrap never runs from the migrator in supabase mode",
      );
    return { mode, shim: false, bootstrap: false, verify, ...runtime };
  }
  if (shim === undefined)
    throw profileError(
      "shim_unset",
      `HOOPDB_SUPABASE_SHIM must be set explicitly in ${mode} mode`,
    );
  return { mode, shim, bootstrap: bootstrap ?? shim, verify, ...runtime };
}

async function schemaSteps(): Promise<Step[]> {
  const upstream = (await readdir(UPSTREAM_MIGRATIONS))
    .filter((f) => f.endsWith(".sql"))
    .sort();
  const ours = (await readdir(OURS)).filter((f) => f.endsWith(".sql")).sort();
  return [
    ...upstream.map((f) => ({
      name: `upstream:${f}`,
      path: join(UPSTREAM_MIGRATIONS, f),
    })),
    ...ours.map((f) => ({ name: `96bulls:${f}`, path: join(OURS, f) })),
  ];
}

export async function plan(profile: MigrationProfile): Promise<Step[]> {
  return [
    ...(profile.shim ? [{ name: "shim:supabase_roles", path: SHIM }] : []),
    ...(profile.bootstrap
      ? [
          {
            name: "bootstrap:session_directory_search_worker",
            path: WORKER_BOOTSTRAP,
          },
          {
            name: "bootstrap:session_token_verifier",
            path: TOKEN_VERIFIER_BOOTSTRAP,
          },
          { name: "bootstrap:session_ingest_roles", path: INGEST_BOOTSTRAP },
        ]
      : []),
    ...(await schemaSteps()),
    ...(profile.verify
      ? [
          {
            name: "verify:session_directory_search_worker",
            path: WORKER_VERIFY,
          },
          {
            name: "verify:session_token_verifier",
            path: TOKEN_VERIFIER_VERIFY,
          },
          {name:"verify:session_ingest_seeder",path:INGEST_SEED_VERIFY},
          {name:"verify:session_ingest",path:INGEST_ROLE_VERIFY},
        ]
      : []),
  ];
}

/** Names the services wait for: the schema steps, independent of the migrator's profile. */
export async function expectedMigrationNames(): Promise<string[]> {
  return (await schemaSteps()).map((s) => s.name);
}

/**
 * The migration-set generation: sha256 over every schema step (name and file sha256)
 * plus the phase 2 verifier, which defines the approved grants. Independent of the
 * shim/bootstrap/verify flags, so the migrator, an external verifier and every reader
 * compute the same value for the same image.
 */
export async function generation(): Promise<string> {
  const upstream = (await readdir(UPSTREAM_MIGRATIONS))
    .filter((f) => f.endsWith(".sql"))
    .sort();
  const ours = (await readdir(OURS)).filter((f) => f.endsWith(".sql")).sort();
  const steps: Step[] = [
    ...upstream.map((f) => ({
      name: `upstream:${f}`,
      path: join(UPSTREAM_MIGRATIONS, f),
    })),
    ...ours.map((f) => ({ name: `96bulls:${f}`, path: join(OURS, f) })),
    { name: "verify:session_directory_search_worker", path: WORKER_VERIFY },
    { name: "verify:session_token_verifier", path: TOKEN_VERIFIER_VERIFY },
    {name:"verify:session_ingest_seeder",path:INGEST_SEED_VERIFY},
    {name:"verify:session_ingest",path:INGEST_ROLE_VERIFY},
  ];
  const parts: string[][] = [];
  for (const s of steps)
    parts.push([
      s.name,
      createHash("sha256")
        .update(await readFile(s.path, "utf8"))
        .digest("hex"),
    ]);
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

/** What readiness requires of HoopDb for this image (see schemaVerdict). */
export async function expectedSchema(): Promise<ExpectedSchema> {
  const mode = process.env.HOOPDB_DB_MODE;
  return {
    names: await expectedMigrationNames(),
    generation: await generation(),
    ...(["local", "selfhosted", "cloudsql", "supabase"].includes(mode ?? "")
      ? { dbMode: mode as MigrationProfile["mode"] }
      : {}),
  };
}

/** The migrator's "pending first" write, under its advisory lock, before any step. */
/** Attempts for one role-check transaction when it loses a deadlock (40P01). */
export const ROLE_CHECK_DEADLOCK_ATTEMPTS = 3;

export const MARKER_PENDING_SQL = `INSERT INTO ${ROLE_VALIDATION_TABLE} (name, generation, status) VALUES ($1, $2, 'pending')
       ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, status = 'pending', updated_at = now()`;

export interface MigrationClient {
  query<R = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: R[] }>;
}

export interface RunOptions {
  /** Defaults to migrationProfile(process.env), which throws before any write. */
  profile?: MigrationProfile;
  steps?: Step[];
  /** Test seam: runs after the last migration commit, before phase 2. */
  beforeVerify?: () => Promise<void>;
  /** Release plan binding, checked under the migrator lock before writes. */
  expectedLedgerSha256?: string;
  /** External release gates rechecked under the migrator lock, before any write. */
  beforeWrite?: () => Promise<void>;
}

/** Shared read-only preflight: release planning and apply enforce the same guards. */
async function migrationPreflight(client: MigrationClient, profile: MigrationProfile, schema: Step[]) {
  const ledger = await readLedger(client);
  // A HoopDb built from the old vendored pin cannot be moved forward in place:
  // upstream changed 20261025000600 after it was applied there. Say so plainly.
  if ([...ledger.keys()].some((n) => n.startsWith("vendor:"))) {
    throw Object.assign(
      new Error(
        "96bulls: this HoopDb was built from the old vendored relayhistory-cloud pin; recreate it (local: `96bulls down <stage> --purge`, then `up`)",
      ),
      {
        code: "legacy_schema",
      },
    );
  }
  const runtime = await runtimeRolePreflight(client, profile);
  // Durable function boundary gate, every non-local mode: the selected control must be
  // proven by the catalog. Only explicit local (Docker dev) mode is ungated.
  if (profile.mode !== "local") {
    const evidence = await functionBoundaryEvidence(
      client,
      profile.functionBoundary,
      profile.mode,
      profile.runtimeRole,
    );
    if (!evidence.satisfied)
      throw profileError(
        "function_boundary_unproven",
        `no catalog-proven function boundary (HOOPDB_FUNCTION_BOUNDARY_MODE): ${JSON.stringify(evidence)}; see the runbook "Function boundary"`,
      );
  }
  await cliHistoryPreflight(client, schema);
  await sequencePreflight(client, schema, ledger);
  return { ledger, runtime };
}
export function migrationLedgerSha256(ledger: Map<string, string>): string {
  return createHash("sha256").update(JSON.stringify([...ledger].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0))).digest("hex");
}
/** Caller owns the READ ONLY transaction; never invokes bootstrap or schema SQL. */
export async function inspectMigrations(client: MigrationClient, profile: MigrationProfile) {
  if (profile.mode !== "supabase" || profile.shim || profile.bootstrap || !profile.verify)
    throw profileError("release_profile", "release inspection requires Supabase, verifier enabled, shim/bootstrap disabled");
  const steps = await schemaSteps();
  const { ledger } = await migrationPreflight(client, profile, steps);
  const acl = await sessionsFunctionAclPreview(client, profile.runtimeRole, steps.some(s => !ledger.has(s.name)));
  if (acl.function_acl_drift_extra.length || acl.function_acl_missing_refused.length)
    throw profileError("function_acl_drift", "release inspection refused ACL drift");
  // Prerequisites only, no provisioning. Authoritative migration/phase-2 SQL still
  // verifies the complete role contract, including memberships and effective grants.
  const roles = (await client.query<{rolname:string; safe:boolean}>(
    "SELECT rolname, NOT (rolcanlogin OR rolinherit OR rolsuper OR rolbypassrls OR rolcreatedb OR rolcreaterole OR rolreplication) AS safe FROM pg_roles WHERE rolname = ANY($1::text[])",
    [["session_directory_search_worker", "session_token_verifier"]])).rows;
  if (roles.length !== 2 || roles.some(r => !r.safe))
    throw profileError("bootstrap_prerequisite", "operator-provisioned search/token bare roles absent or unsafe");
  const inventory = [];
  for (const step of steps) inventory.push({
    name: step.name,
    sha256: createHash("sha256").update(await readFile(step.path, "utf8")).digest("hex"),
    pending: !ledger.has(step.name),
  });
  return { generation: await generation(), ledgerSha256: migrationLedgerSha256(ledger), inventory, acl };
}

/**
 * Applies the plan on one connection. Readiness marker (ROLE_VALIDATION_TABLE):
 *   1. under the advisory lock, before any step runs, the row is set to
 *      ('pending', <generation>): from here every reader is non-ready;
 *   2. only a successful phase 2, in the same transaction, sets it to 'pass' for the
 *      generation it validated;
 *   3. any failure, crash or a disabled verifier (HOOPDB_ROLE_VERIFY=false) leaves
 *      it 'pending'. With the verifier disabled only an external verifier
 *      (supabase/bootstrap/session_directory_search_worker_mark_pass.sql) can pass it.
 */
export async function runMigrations(
  client: MigrationClient,
  opts: RunOptions = {},
): Promise<{ applied: number; skipped: number }> {
  let applied = 0;
  let skipped = 0;
  const profile = opts.profile ?? migrationProfile();
  // One migrator at a time; every marker transition happens while this lock is held.
  await client.query("SELECT pg_advisory_lock(961996, 1)");
  try {
    const steps = opts.steps ?? (await plan(profile));
    const schema = steps.filter(
      (st) => st.name.startsWith("upstream:") || st.name.startsWith("96bulls:"),
    );
    // Read-only preflight, all under the lock: nothing below writes until every check passes.
    const { ledger, runtime } = await migrationPreflight(client, profile, schema);
    if (opts.expectedLedgerSha256 !== undefined &&
        migrationLedgerSha256(ledger) !== opts.expectedLedgerSha256)
      throw profileError("release_ledger_changed", "database ledger changed since the approved read-only plan");
    if (opts.beforeWrite) await opts.beforeWrite();
    await client.query("CREATE SCHEMA IF NOT EXISTS sessions");
    await client.query(`CREATE TABLE IF NOT EXISTS ${MIGRATION_LEDGER} (
      name text PRIMARY KEY,
      sha256 text NOT NULL,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${ROLE_VALIDATION_TABLE} (
      name text PRIMARY KEY,
      generation text NOT NULL,
      status text NOT NULL CHECK (status IN ('pending', 'pass')),
      updated_at timestamptz NOT NULL DEFAULT now()
    )`);
    await client.query(`REVOKE ALL ON ${ROLE_VALIDATION_TABLE} FROM PUBLIC`);
    // Bootstrap the readiness subset first. After every schema step, hardenAndCheck adds
    // the immutable application ACL and validates the complete catalog before PASS.
    if (runtime === "dedicated") {
      const grant = await client.query<{ sql: string }>(
        "SELECT format('GRANT USAGE ON SCHEMA sessions TO %I; GRANT SELECT ON %s, %s TO %I', $3::text, $1::text, $2::text, $3::text) AS sql",
        [MIGRATION_LEDGER, ROLE_VALIDATION_TABLE, profile.runtimeRole],
      );
      await client.query(grant.rows[0]!.sql);
    }
    const gen = await generation();
    // Pending first: committed before any migration can become visible.
    await client.query(MARKER_PENDING_SQL, [ROLE_VALIDATION_NAME, gen]);
    log.info("role validation pending", { reason: gen });
    // After the last migration, before phase 2 can mark PASS: the runtime role holds
    // exactly the approved grants against the full schema (e.g. no EXECUTE gained on a
    // function a migration just created).
    // Inside the validation transaction (with phase 2 and the PASS write): revoke PUBLIC
    // EXECUTE on every sessions function (idempotent; triggers still fire, PostgreSQL
    // does not check EXECUTE on a trigger function at fire time), then check the runtime
    // role against the full schema. Any failure rolls all of it back.
    const hardenAndCheck = async () => {
      await client.query(
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC",
      );
      if (runtime !== "dedicated") return;
      await installRuntimeApplicationAcl(client, profile.runtimeRole);
      const violations = await runtimeRoleViolations(
        client,
        runtimeCfg(profile),
        RUNTIME_TABLES,
        "post",
      );
      if (violations.length)
        throw profileError(
          "runtime_role_unsafe",
          `HOOPDB_RUNTIME_ROLE ${profile.runtimeRole} is not least-privilege: ${violations.join(", ")}`,
        );
      // Every non-local boundary mode (global-default-revoke in selfhosted and cloudsql,
      // event-trigger in supabase): every sessions function ACL must equal the committed
      // manifest plus the runtime application ACL exactly, or the whole validation
      // transaction (with phase 2 and PASS) rolls back and the marker stays pending.
      // Local (Docker dev) mode is unchanged: no check.
      const drift =
        profile.mode !== "local"
          ? await sessionsFunctionAclDrift(client, profile.runtimeRole)
          : [];
      if (drift.length)
        throw profileError(
          "function_acl_drift",
          `sessions function ACLs differ from src/hoopdb/sessions-function-acl.json: ${drift.slice(0, 5).join("; ")}${drift.length > 5 ? `; and ${drift.length - 5} more` : ""}`,
        );
    };
    const bindDbMode = () =>
      client.query("SELECT set_config('hoopdb.db_mode', $1, true)", [
        profile.mode,
      ]);
    let postChecked = false;
    // The validation marker flips to 'pass' once, after every phase-2 verifier
    // has passed: only the last verify step does the UPDATE, so an earlier
    // failure can never leave a half-checked generation marked pass.
    const lastVerifyIndex = steps.reduce(
      (acc, s, i) => (s.name.startsWith("verify:") ? i : acc),
      -1,
    );
    for (const [stepIndex, step] of steps.entries()) {
      const sql = await readFile(step.path, "utf8");
      // The role checks (phase 1 bootstrap, phase 2 verify) run on every migrate, each in
      // its own transaction, and are never recorded in the ledger.
      if (
        step.name.startsWith("bootstrap:") ||
        step.name.startsWith("verify:")
      ) {
        const verify = step.name.startsWith("verify:");
        if (verify) postChecked = true;
        if (verify && opts.beforeVerify) await opts.beforeVerify();
        // The hardening takes AccessExclusiveLock (CREATE POLICY) on live tables, so
        // it can lose a deadlock to autovacuum (40P01, observed on the 462 release).
        // The whole transaction rolls back, so a bounded retry is safe.
        for (let attempt = 1; ; attempt++) {
          await client.query("BEGIN");
          try {
            await bindDbMode();
            if (verify) await hardenAndCheck();
            await client.query(sql);
            if (verify && stepIndex === lastVerifyIndex) {
              const passed = await client.query(
                `UPDATE ${ROLE_VALIDATION_TABLE} SET status = 'pass', updated_at = now()
                  WHERE name = $1 AND generation = $2 AND status = 'pending' RETURNING name`,
                [ROLE_VALIDATION_NAME, gen],
              );
              if (passed.rows.length !== 1)
                throw Object.assign(
                  new Error(
                    "96bulls: role validation marker was not pending for this generation",
                  ),
                  { code: "marker_lost" },
                );
            }
            await client.query("COMMIT");
            break;
          } catch (err) {
            await client.query("ROLLBACK");
            const code = (err as { code?: string }).code;
            if (code === "40P01" && attempt < ROLE_CHECK_DEADLOCK_ATTEMPTS) {
              log.warn("role check deadlocked; retrying", { reason: step.name, attempt });
              await new Promise((r) => setTimeout(r, attempt * 1000));
              continue;
            }
            log.error("role check failed", {
              reason: step.name,
              error_code: code ?? "role_check_failed",
            });
            throw err;
          }
        }
        log.info(verify ? "role validation passed" : "role check passed", {
          reason: verify ? gen : step.name,
        });
        continue;
      }
      const sha = createHash("sha256").update(sql).digest("hex");
      const seen = ledger.get(step.name);
      if (seen && migrationChecksumMatches(step.name, seen, sha)) {
        const alias = migrationChecksumAlias(step.name, seen, sha);
        if (alias) log.info("accepted historical migration checksum", alias);
        skipped += 1;
        continue;
      }
      if (seen && seen !== sha) {
        // Unreachable after sequencePreflight; kept as a second line of defense.
        throw new Error(
          `96bulls: migration ${step.name} changed after it was applied; add a new migration instead`,
        );
      }
      await client.query("BEGIN");
      try {
        await bindDbMode();
        await client.query(sql);
        await client.query(
          `INSERT INTO ${MIGRATION_LEDGER} (name, sha256) VALUES ($1, $2)`,
          [step.name, sha],
        );
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        log.error("migration failed", {
          reason: step.name,
          error_code: (err as { code?: string }).code ?? "migration_failed",
        });
        throw err;
      }
      applied += 1;
      log.info("migration applied", { reason: step.name });
    }
    if (!postChecked) {
      // In-process verifier off: the same hardening and runtime check, in one transaction.
      await client.query("BEGIN");
      try {
        await bindDbMode();
        await hardenAndCheck();
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    }
    if (!steps.some((s) => s.name.startsWith("verify:")))
      log.info(
        "role validation left pending: in-process verifier disabled; an external verifier must pass it",
        { reason: gen },
      );
  } finally {
    await client
      .query("SELECT pg_advisory_unlock(961996, 1)")
      .catch(() => undefined);
  }
  log.info("hoopdb migrations complete", { count: applied, skipped });
  return { applied, skipped };
}

const RUNTIME_TABLES = {
  ledger: MIGRATION_LEDGER,
  marker: ROLE_VALIDATION_TABLE,
};
const runtimeCfg = (p: MigrationProfile) => ({
  mode: p.mode,
  role: p.runtimeRole,
  ...(p.runtimeLogin === undefined ? {} : { login: p.runtimeLogin }),
  ...(p.workloadRole ? { workload: p.workloadRole } : {}),
});

/**
 * Install only the immutable trusted-backend ACL. Preflight has already rejected drift.
 * The policy is all-row by design because workers span tenants; the runtime role is a
 * service credential, never an end-user identity, and still has no ownership/BYPASSRLS.
 */
async function installRuntimeApplicationAcl(
  client: MigrationClient,
  role: string,
): Promise<void> {
  for (const [relation, privileges] of Object.entries(
    RUNTIME_APPLICATION_TABLE_PRIVILEGES,
  )) {
    const grant = await client.query<{ sql: string }>(
      "SELECT format('GRANT %s ON TABLE %s TO %I', $1::text, $2::regclass, $3::text) AS sql",
      [privileges.join(", "), relation, role],
    );
    await client.query(grant.rows[0]!.sql);
  }
  for (const sequence of RUNTIME_APPLICATION_SEQUENCES) {
    const grant = await client.query<{ sql: string }>(
      "SELECT format('GRANT USAGE ON SEQUENCE %s TO %I', $1::regclass, $2::text) AS sql",
      [sequence, role],
    );
    await client.query(grant.rows[0]!.sql);
  }
  for (const fn of RUNTIME_APPLICATION_FUNCTIONS) {
    const grant = await client.query<{ sql: string }>(
      "SELECT format('GRANT EXECUTE ON FUNCTION %s TO %I', $1::regprocedure, $2::text) AS sql",
      [fn, role],
    );
    await client.query(grant.rows[0]!.sql);
  }
  for (const relation of RUNTIME_APPLICATION_RLS_TABLES) {
    const exists = await client.query<{ present: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM pg_policy
        WHERE polrelid = $1::regclass AND polname = $2) AS present`,
      [relation, RUNTIME_APPLICATION_POLICY],
    );
    if (exists.rows[0]!.present) continue;
    const create = await client.query<{ sql: string }>(
      "SELECT format('CREATE POLICY %I ON %s AS PERMISSIVE FOR ALL TO %I USING (true) WITH CHECK (true)', $1::text, $2::regclass, $3::text) AS sql",
      [RUNTIME_APPLICATION_POLICY, relation, role],
    );
    await client.query(create.rows[0]!.sql);
  }
}

async function readLedger(
  client: MigrationClient,
): Promise<Map<string, string>> {
  const there = (
    await client.query<{ r: string | null }>(
      "SELECT to_regclass($1)::text AS r",
      [MIGRATION_LEDGER],
    )
  ).rows[0]?.r;
  if (!there) return new Map();
  return new Map(
    (
      await client.query<{ name: string; sha256: string }>(
        `SELECT name, sha256 FROM ${MIGRATION_LEDGER}`,
      )
    ).rows.map((r) => [r.name, r.sha256]),
  );
}

/**
 * The runtime role must exist and be dedicated: the migrator itself (owner-run) is
 * accepted only in local mode. A dedicated role must pass the exact least-privilege
 * allowlist (runtimeRoleViolations). Otherwise it fails closed before any write.
 */
async function runtimeRolePreflight(
  client: MigrationClient,
  profile: MigrationProfile,
): Promise<"owner" | "dedicated"> {
  const r = (
    await client.query<{ rolname: string; me: string }>(
      "SELECT rolname, current_user AS me FROM pg_roles WHERE rolname = $1",
      [profile.runtimeRole],
    )
  ).rows[0];
  if (!r)
    throw profileError(
      "runtime_role_missing",
      `HOOPDB_RUNTIME_ROLE ${profile.runtimeRole} does not exist; nothing was written`,
    );
  if (r.rolname === r.me) {
    if (profile.mode !== "local")
      throw profileError(
        "runtime_role_unsafe",
        `in ${profile.mode} mode the services need a dedicated runtime role, not the migration owner`,
      );
    return "owner";
  }
  if (r.rolname === ROLE_VALIDATION_NAME)
    throw profileError(
      "runtime_role_unsafe",
      "HOOPDB_RUNTIME_ROLE must not be the search Worker",
    );
  const violations = await runtimeRoleViolations(
    client,
    runtimeCfg(profile),
    RUNTIME_TABLES,
    "pre",
  );
  if (violations.length)
    throw profileError(
      "runtime_role_unsafe",
      `HOOPDB_RUNTIME_ROLE ${profile.runtimeRole} is not least-privilege: ${violations.join(", ")}`,
    );
  return "dedicated";
}

/**
 * Refuses, before any write, when Supabase's CLI history
 * (supabase_migrations.schema_migrations) records any of these migrations, or exists
 * but can't be read: `supabase db push` and this migrator must never both apply them
 * (mixed_migration_authority). See the runbook decision point.
 */
async function cliHistoryPreflight(
  client: MigrationClient,
  schema: Step[],
): Promise<void> {
  const reg = async (name: string) =>
    (
      await client.query<{ r: string | null }>(
        "SELECT to_regclass($1)::text AS r",
        [name],
      )
    ).rows[0]?.r ?? null;
  let cliHistory: string | null;
  try {
    cliHistory = await reg("supabase_migrations.schema_migrations");
  } catch (err) {
    // The schema exists but the migrator may not even look inside: it can't prove there
    // is no second authority.
    if ((err as { code?: string }).code === "42501")
      throw profileError(
        "mixed_migration_authority",
        "supabase_migrations exists but the migrator cannot read it; one authority only, see the runbook decision point",
      );
    throw err;
  }
  if (cliHistory) {
    const versions = schema
      .filter((st) => st.name.startsWith("upstream:"))
      .map((st) => st.name.slice("upstream:".length).split("_")[0]!);
    let hit: { version: string }[];
    try {
      hit = (
        await client.query<{ version: string }>(
          "SELECT version FROM supabase_migrations.schema_migrations WHERE version = ANY($1::text[]) ORDER BY version",
          [versions],
        )
      ).rows;
    } catch (err) {
      // Present but unreadable by the migrator: it can't prove there is no second authority.
      if ((err as { code?: string }).code === "42501")
        throw profileError(
          "mixed_migration_authority",
          "supabase_migrations.schema_migrations exists but the migrator cannot read it; one authority only, see the runbook decision point",
        );
      throw err;
    }
    if (hit.length)
      throw profileError(
        "mixed_migration_authority",
        `supabase db push recorded ${hit.map((h) => h.version).join(", ")}; one authority only, see the runbook decision point`,
      );
  }
}

/** Read-only: does the ledger lack any migration this image would apply? */
async function pendingMigrationsExist(
  client: MigrationClient,
): Promise<boolean> {
  const has = await client.query<{ t: string | null }>(
    `SELECT to_regclass('${MIGRATION_LEDGER}')::text AS t`,
  );
  if (!has.rows[0]?.t) return true;
  const done = new Set(
    (
      await client.query<{ name: string }>(
        `SELECT name FROM ${MIGRATION_LEDGER}`,
      )
    ).rows.map((r) => r.name),
  );
  return (await expectedMigrationNames()).some((n) => !done.has(n));
}

function migrationProfileMode(): FunctionBoundaryMode | undefined {
  const v = process.env.HOOPDB_FUNCTION_BOUNDARY_MODE;
  return v === "global-default-revoke" || v === "event-trigger" ? v : undefined;
}

export async function migrate(): Promise<{ applied: number; skipped: number }> {
  // Before any connection: an invalid profile fails here, having written nothing.
  const profile = migrationProfile();
  const db = openHoopDb();
  await waitForHoopDb(db);
  const client = await db.pool.connect();
  try {
    return await runMigrations(client, { profile });
  } finally {
    client.release();
    await db.end();
  }
}

async function checksumAliasEvidence(client: MigrationClient) {
  const there = (
    await client.query<{ r: string | null }>(
      "SELECT to_regclass($1)::text AS r",
      [MIGRATION_LEDGER],
    )
  ).rows[0]?.r;
  if (!there) return [];
  const rows = (
    await client.query<{ name: string; sha256: string }>(
      `SELECT name, sha256 FROM ${MIGRATION_LEDGER}`,
    )
  ).rows;
  const steps = await schemaSteps();
  const actual = new Map<string, string>();
  for (const step of steps)
    actual.set(
      step.name,
      createHash("sha256")
        .update(await readFile(step.path, "utf8"))
        .digest("hex"),
    );
  return rows.flatMap((row) => {
    const alias = migrationChecksumAlias(
      row.name,
      row.sha256,
      actual.get(row.name) ?? "",
    );
    return alias ? [alias] : [];
  });
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "hoopdb-migrate";
  if (process.argv.includes("--activation-check")) {
    // For an operator or the activation dossier: the function-boundary evidence (names and
    // booleans only) for HOOPDB_URL, HOOPDB_FUNCTION_BOUNDARY_MODE and HOOPDB_DB_MODE.
    // Without HOOPDB_DB_MODE every creator is covered (no platform exclusion). Outside
    // local mode it also previews the exact-ACL drift check, read-only: grants the
    // migrator would refuse (function_acl_drift_extra) and grants it would add
    // (function_acl_missing). Exit 1 if unproven or if any extra exists.
    const dbMode = process.env.HOOPDB_DB_MODE;
    if (
      dbMode !== undefined &&
      !["local", "selfhosted", "cloudsql", "supabase"].includes(dbMode)
    ) {
      console.error("db_mode_invalid");
      process.exit(1);
    }
    const db = openHoopDb();
    const mode = migrationProfileMode();
    Promise.all([
      functionBoundaryEvidence(
        db,
        mode,
        dbMode as DbMode | undefined,
        process.env.HOOPDB_RUNTIME_ROLE || undefined,
      ),
      checksumAliasEvidence(db),
      // The preview needs the runtime role whose #116 grants the migrator installs; local
      // mode has no drift check, so neither runs there.
      dbMode === "local" || !process.env.HOOPDB_RUNTIME_ROLE
        ? Promise.resolve(null)
        : pendingMigrationsExist(db).then((pending) =>
            sessionsFunctionAclPreview(
              db,
              process.env.HOOPDB_RUNTIME_ROLE!,
              pending,
            ),
          ),
    ])
      .then(async ([e, migration_checksum_aliases, acl]) => {
        const satisfied =
          e.satisfied &&
          (acl?.function_acl_drift_extra.length ?? 0) === 0 &&
          (acl?.function_acl_missing_refused.length ?? 0) === 0;
        console.log(
          JSON.stringify({
            ...e,
            migration_checksum_aliases,
            function_acl_preview:
              dbMode === "local"
                ? "not_applicable_local"
                : acl
                  ? "checked"
                  : "skipped_runtime_role_unset",
            ...(acl ?? {}),
            function_boundary_satisfied: e.satisfied,
            satisfied,
          }),
        );
        await db.end();
        process.exit(satisfied ? 0 : 1);
      })
      .catch(async (err) => {
        console.error(
          (err as { code?: string }).code ?? "activation_check_failed",
        );
        await db.end();
        process.exit(1);
      });
  } else if (process.argv.includes("--generation")) {
    // For an external verifier: the generation its PASS record must name.
    generation().then((g) => {
      console.log(g);
      process.exit(0);
    });
  } else
    migrate()
      .then(() => process.exit(0))
      .catch((err) => {
        log.error("hoopdb migrate exited", {
          error_code: (err as { code?: string }).code ?? "migrate_failed",
        });
        if (optional("DEBUG", "") === "1") console.error(err);
        process.exit(1);
      });
}
