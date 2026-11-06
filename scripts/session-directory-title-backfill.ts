/**
 * P3b directory task-title backfill: Neon sessions.session_rollups (READ ONLY) ->
 * Supabase sessions.session_directory.task_title (owner), fill-only.
 * Rules and precedence: src/hoopdb/session-directory-title-backfill.ts.
 *
 * Usage (stdin: ONE JSON line {"neonUrl": "postgresql://..."}, any Neon role that can
 * SELECT sessions.session_rollups; Supabase owner = the CLI temp login, SET ROLE postgres,
 * as scripts/frozen-source-copy.ts):
 *   bun scripts/session-directory-title-backfill.ts            --plan (default): reads both sides, writes nothing
 *   bun scripts/session-directory-title-backfill.ts --apply [--page 1000] [--pause-ms 250]
 *        [--max-page-ms 4000] [--after-org ORG] [--max-pages N]
 *
 * Neon: the session is forced read-only (default_transaction_read_only = on, verified;
 * statement_timeout 20 s) and every page is its own BEGIN READ ONLY transaction.
 * Idempotent and resumable: a rerun fills only what is still NULL; to skip finished orgs pass
 * --after-org with the org of the last `org_done` log line (orgs are paged in order).
 * Canary first: --apply --max-pages 1 --page 100.
 * Never writes Neon. Never prints credentials or titles.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import pg from "pg";
import { parseCliLogin } from "./schema-release-owner-operator-plan.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { sessionReadOnly } from "../src/migration/neon-s3-sync/neon-reader.ts";
import { backfillDirectoryTitles } from "../src/hoopdb/session-directory-title-backfill.ts";

const REF = "abcdefghijklmnopqrst", HOST = "aws-0-us-east-1.pooler.supabase.com", CLI = process.env.SUPABASE_CLI ?? "supabase";
const args = process.argv.slice(2);
const flag = (k: string) => args.includes(k);
const val = (k: string) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : undefined; };
const num = (k: string, d: number) => { const v = val(k); if (v === undefined) return d; const n = Number(v); if (!Number.isInteger(n) || n < 0) { console.error(`${k} must be a non-negative integer`); process.exit(64); } return n; };
if (flag("--plan") && flag("--apply")) { console.error("--plan and --apply are exclusive"); process.exit(64); }
const apply = flag("--apply");
const log = (f: Record<string, unknown>) => console.log(JSON.stringify({ at: new Date().toISOString(), service: "p3b-title-backfill", ...f }));

const { neonUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0] || "{}") as { neonUrl?: string };
if (!/^postgres(ql)?:\/\/[^@]+@[^/]+\.neon\.tech\//.test(neonUrl ?? "")) { console.error("neonUrl (a Neon connection string) required on stdin"); process.exit(64); }

const neon = new pg.Client({ connectionString: neonUrl, ssl: { rejectUnauthorized: true }, application_name: "p3b-title-backfill-readonly" });
const login = parseCliLogin(execFileSync(CLI, ["db", "dump", "--linked", "--project-ref", REF, "--dry-run"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000 }));
const supa = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: `cli_login_postgres.${REF}`, password: login.password,
  ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST }, options: "-c statement_timeout=60000 -c lock_timeout=5000", application_name: "p3b-title-backfill" });
login.password = "";
try {
  await neon.connect();
  await sessionReadOnly(neon);
  await supa.connect();
  await supa.query("SET SESSION ROLE postgres");
  const result = await backfillDirectoryTitles(neon, supa, {
    apply,
    pageSize: num("--page", 1000),
    pauseMs: num("--pause-ms", 250),
    maxPageMs: num("--max-page-ms", 4000),
    afterOrg: val("--after-org"),
    maxPages: val("--max-pages") === undefined ? undefined : num("--max-pages", 0),
    log,
  });
  log({ m: "done", ...result });
} catch (error) {
  log({ m: "failed", code: (error as { code?: string }).code ?? (error as Error).name });
  process.exitCode = 1;
} finally {
  await neon.end().catch(() => {});
  await supa.end().catch(() => {});
}
