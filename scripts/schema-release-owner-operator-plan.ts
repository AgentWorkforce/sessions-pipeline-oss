/** PLAN ONLY through the operator's existing Supabase CLI passwordless session. */
import { execFileSync } from "node:child_process";
import pg from "pg";
import { generation, inspectMigrations, migrationProfile } from "../src/hoopdb/migrate.ts";
import { functionBoundaryEvidence } from "../src/hoopdb/migrate-guards.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
const PROJECT = "abcdefghijklmnopqrst";
const CLI = "supabase";
function refuse(code: string): never { throw Object.assign(new Error(code), { code }); }
/** Data parser only. Never execute or source the shell script printed by the CLI. */
export function parseCliLogin(script: string): { password: string } {
  if (Buffer.byteLength(script) > 65536) refuse("cli_reply_limit");
  const fields = new Map<string, string>();
  for (const raw of script.split("\n")) {
    const match = /^(?:export )?(PGHOST|PGPORT|PGUSER|PGPASSWORD|PGDATABASE)=(.*)$/u.exec(raw.trim());
    if (!match) continue;
    const key = match[1]!, encoded = match[2]!.trim();
    if (fields.has(key)) refuse("cli_duplicate_field");
    let value: string;
    if (encoded.startsWith('"')) {
      try { value = JSON.parse(encoded); } catch { return refuse("cli_field_parse"); }
    } else if (encoded.startsWith("'")) {
      if (!/^'[^']*'$/u.test(encoded)) refuse("cli_field_parse");
      value = encoded.slice(1, -1);
    } else {
      if (!/^[A-Za-z0-9_./:+-]+$/u.test(encoded)) refuse("cli_field_parse");
      value = encoded;
    }
    if (typeof value !== "string" || /[\x00-\x1f\x7f]/u.test(value)) refuse("cli_field_parse");
    fields.set(key, value);
  }
  if (fields.size !== 5 || fields.get("PGHOST") !== `db.${PROJECT}.supabase.co` || fields.get("PGPORT") !== "5432" || fields.get("PGUSER") !== "cli_login_postgres" || fields.get("PGDATABASE") !== "postgres") refuse("cli_target_pin");
  const password = fields.get("PGPASSWORD")!;
  if (password.length < 16 || /redact|your-password|\*\*\*/iu.test(password)) refuse("cli_credential_unavailable");
  return { password };
}
export function ownerPlanInputs(env: NodeJS.ProcessEnv, argv: string[]) {
  if (env.OPERATOR_MODE !== "plan" || argv.length !== 2) refuse("operator_plan_only");
  const sha = env.OPERATOR_SOURCE_SHA, digest = env.OPERATOR_GENERATION;
  if (!sha || !/^[0-9a-f]{40}$/u.test(sha) || !digest || !/^[0-9a-f]{64}$/u.test(digest)) refuse("operator_source_pin");
  return { sha, digest };
}
interface OwnerPlanClient {
  connect(): Promise<void>;
  query(sql: string): Promise<unknown>;
  end(): Promise<void>;
}
/** Never queue ROLLBACK on a failed connection; pg cannot service that queue. */
export async function withOwnerPlanClient<T>(client: OwnerPlanClient, work: () => Promise<T>): Promise<T> {
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    began = true;
    return await work();
  } finally {
    try { if (began) await client.query("ROLLBACK").catch(() => {}); }
    finally { await client.end().catch(() => {}); }
  }
}
export async function operatorOwnerPlan() {
  const input = ownerPlanInputs(process.env, process.argv);
  const childEnv = Object.fromEntries(["PATH", "HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "DBUS_SESSION_BUS_ADDRESS"].flatMap(k => process.env[k] ? [[k, process.env[k]!]] : []));
  const run = (exe: string, args: string[]) => execFileSync(exe, args, { env: childEnv, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000, maxBuffer: 65536 });
  if (run("git", ["rev-parse", "HEAD"]).trim() !== input.sha || run("git", ["status", "--porcelain"]).trim() || await generation() !== input.digest) refuse("operator_checkout_generation");
  if (run(CLI, ["--version"]).trim() !== "2.122.0") refuse("operator_cli_version");
  let raw = run(CLI, ["db", "dump", "--linked", "--project-ref", PROJECT, "--dry-run"]);
  const login = parseCliLogin(raw); raw = "";
  // Session pooler keeps this one connection/transaction; no Management API query adapter.
  const client = new pg.Client({ host: "aws-0-us-east-1.pooler.supabase.com", port: 5432, database: "postgres", user: `cli_login_postgres.${PROJECT}`, password: login.password, ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: "aws-0-us-east-1.pooler.supabase.com" }, connectionTimeoutMillis: 10000, options: "-c statement_timeout=15000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=20000", application_name: "96bulls-owner-operator-plan-readonly" });
  login.password = "";
  client.on("error", () => {});
  return withOwnerPlanClient(client, async () => {
    await client.query("SET LOCAL ROLE postgres");
    const me = (await client.query("SELECT current_user AS owner, session_user AS login, current_database() AS db, current_setting('transaction_read_only') AS ro, pg_get_userbyid(nspowner) AS schema_owner FROM pg_namespace WHERE nspname='sessions'")).rows[0];
    if (!me || me.owner !== "postgres" || me.login !== "cli_login_postgres" || me.schema_owner !== "postgres" || me.db !== "postgres" || me.ro !== "on") refuse("operator_owner_identity");
    await client.query("SELECT pg_advisory_xact_lock(961996,1)");
    const profile = migrationProfile({ HOOPDB_DB_MODE: "supabase", HOOPDB_SUPABASE_SHIM: "false", HOOPDB_ROLE_BOOTSTRAP: "false", HOOPDB_ROLE_VERIFY: "true", HOOPDB_RUNTIME_ROLE: "bulls96_runtime", HOOPDB_RUNTIME_ROLE_LOGIN: "true", HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke" });
    const report = await inspectMigrations(client, profile);
    const trigger = await functionBoundaryEvidence(client, "event-trigger", "supabase", "bulls96_runtime");
    console.log(JSON.stringify({ version: 1, status: "planned_readonly_not_apply_approval", at: new Date().toISOString(), project: PROJECT, sourceSha: input.sha, boundary: "global-default-revoke", existingEventTriggerMatchesSource: trigger.satisfied, ...report }));
  });
}
if (import.meta.main) operatorOwnerPlan().catch(error => { const code = typeof error?.code === "string" && /^[A-Za-z0-9_]{1,64}$/u.test(error.code) ? error.code : "operator_plan_failed"; console.log(JSON.stringify({ status: "refused", code })); process.exitCode = 1; });
