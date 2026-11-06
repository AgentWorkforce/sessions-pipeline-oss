/** Shared strict connection and READ ONLY inspection for CI and operator planning. */
import pg from "pg";
import { inspectMigrations, migrationProfile } from "./migrate.ts";
import {
  SCHEMA_RELEASE as P,
  refuse,
  releaseConnection,
} from "./schema-release-contract.ts";
import { SUPABASE_PROD_CA_2021 } from "../session-pipeline/supabase-ca.ts";
export function releaseClient(url: string | undefined) {
  // Individual fields prevent URL sslmode/options from replacing TLS configuration.
  const client = new pg.Client({
    ...releaseConnection(url),
    ssl: {
      ca: SUPABASE_PROD_CA_2021,
      rejectUnauthorized: true,
      servername: P.host,
    },
    application_name: "96bulls-guarded-schema-release",
    connectionTimeoutMillis: 10000,
    options:
      "-c statement_timeout=300000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=60000",
  });
  client.on("error", () => {}); // Never dump driver/connection errors to logs.
  return client;
}
export function releaseProfile(
  boundary: "event-trigger" | "global-default-revoke",
) {
  return migrationProfile({
    HOOPDB_DB_MODE: "supabase",
    HOOPDB_SUPABASE_SHIM: "false",
    HOOPDB_ROLE_BOOTSTRAP: "false",
    HOOPDB_ROLE_VERIFY: "true",
    HOOPDB_RUNTIME_ROLE: P.runtime,
    HOOPDB_RUNTIME_ROLE_LOGIN: "true",
    HOOPDB_FUNCTION_BOUNDARY_MODE: boundary,
  });
}
export async function inspectRelease(
  client: pg.Client,
  boundary: "event-trigger" | "global-default-revoke",
) {
  await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const me = (
      await client.query(
        "SELECT current_user AS me, session_user AS session, current_database() AS db, r.rolcanlogin, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication FROM pg_roles r WHERE r.rolname = current_user",
      )
    ).rows[0];
    if (
      !me ||
      me.me !== P.owner ||
      me.session !== P.owner ||
      me.db !== P.database ||
      !me.rolcanlogin ||
      me.rolsuper ||
      me.rolcreaterole ||
      me.rolcreatedb ||
      me.rolbypassrls ||
      me.rolreplication
    )
      refuse("release_owner");
    await client.query("SELECT pg_advisory_xact_lock(961996, 1)");
    return await inspectMigrations(client, releaseProfile(boundary));
  } finally {
    await client.query("ROLLBACK");
  }
}
export function releaseErrorCode(error: unknown) {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^[a-zA-Z0-9_]{1,64}$/.test(code)
    ? code
    : "release_failed";
}
