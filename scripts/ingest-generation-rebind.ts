/**
 * Operator CLI for the P1 generation window (owner, production Supabase via the CLI temp login + SET ROLE postgres):
 *   bun scripts/ingest-generation-rebind.ts --from <old64hex> --to <new64hex>            dry run (rolls back)
 *   bun scripts/ingest-generation-rebind.ts --from <old64hex> --to <new64hex> --execute  commit
 * Run AFTER owner-apply has marked readiness PASS for --to, in the same window as the ingest Worker and
 * buffered GC redeploys. Idempotent. Prints only the counts.
 */
import { execFileSync } from "node:child_process";
import pg from "pg";
import { parseCliLogin } from "./schema-release-owner-operator-plan.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { rebindIngestGeneration } from "../src/hoopdb/ingest-generation-rebind.ts";
const REF = "abcdefghijklmnopqrst", HOST = "aws-0-us-east-1.pooler.supabase.com";
const a = process.argv.slice(2), get = (k: string) => { const i = a.indexOf(k); return i >= 0 ? a[i + 1] : undefined; };
const from = get("--from") ?? "", to = get("--to") ?? "", execute = a.includes("--execute");
const login = parseCliLogin(execFileSync(process.env.SUPABASE_CLI ?? "supabase", ["db", "dump", "--linked", "--project-ref", REF, "--dry-run"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000 }));
const c = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: "cli_login_postgres." + REF, password: login.password,
  ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST }, options: "-c statement_timeout=30000 -c lock_timeout=5000", application_name: "p1-generation-rebind" });
login.password = "";
await c.connect();
try {
  await c.query("SET SESSION ROLE postgres");
  console.log(JSON.stringify(await rebindIngestGeneration(c, { from, to, dryRun: !execute })));
} catch (e) { console.log(JSON.stringify({ status: "refused", code: (e as { code?: string }).code ?? "error" })); process.exitCode = 1; }
finally { await c.end(); }
