/**
 * Neon write fence for the T0 hold (docs/relayhistory-t0-runbook.md step 9).
 *
 * After the drain, nothing may write to Neon's `sessions` schema. The Worker
 * reuses Cloud's runtime role, and Cloud writes its own `public` schema with
 * it, so a role-wide read-only setting is not an option. The fence is scoped:
 *
 *   - INSERT/UPDATE/DELETE/TRUNCATE on every `sessions` table, revoked from the
 *     given roles and PUBLIC;
 *   - EXECUTE on every VOLATILE `sessions` function revoked from the same. The
 *     SECURITY DEFINER writers (accept_delivery_batch, the acks...) run as their
 *     owner, so table privileges alone would not stop them;
 *   - STABLE and IMMUTABLE functions stay executable, so pre-T0 reads work.
 *
 * `plan` reads the catalog only and prints the fence SQL plus a restore script
 * that re-grants exactly what the fence removes. The operator reviews and applies them
 * under approval A7; this tool never writes.
 *
 * Usage: RELAYHISTORY_SOURCE_URL=... bun run migration:neon-fence --roles=role_a,role_b
 */
import pg from "pg";

export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;

export interface FencePlan {
  fence: string[];
  restore: string[];
  tables: number;
  volatileFunctions: number;
  readableFunctions: number;
}

export async function planFence(db: Queryable, roles: string[]): Promise<FencePlan> {
  if (roles.length === 0) throw new Error("at least one role is required");
  for (const role of roles)
    if (!/^[a-z_][a-z0-9_$-]*$/i.test(role)) throw new Error(`bad role name ${role}`);
  const grantees = [...roles.map(quoteIdent), "PUBLIC"].join(", ");

  const tables = (
    await db.query<{ name: string }>(
      `SELECT c.relname AS name
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
        ORDER BY c.relname`,
    )
  ).rows;
  const functions = (
    await db.query<{ signature: string; volatility: string }>(
      `SELECT p.oid::regprocedure::text AS signature, p.provolatile AS volatility
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sessions' AND p.prokind = 'f'
        ORDER BY 1`,
    )
  ).rows;
  const volatile = functions.filter((fn) => fn.volatility === "v");

  const fence = [
    "BEGIN;",
    ...tables.map(
      (table) =>
        `REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE sessions.${quoteIdent(table.name)} FROM ${grantees};`,
    ),
    ...volatile.map((fn) => `REVOKE EXECUTE ON FUNCTION ${fn.signature} FROM ${grantees};`),
    "COMMIT;",
  ];
  // Restore re-grants exactly the privileges the fence removes, read from the
  // current ACLs (a NULL ACL means the type's default: EXECUTE to PUBLIC for a
  // function, nothing to others for a table). Plain GRANTs, so the owner role
  // can run it without superuser.
  const granted = (
    await db.query<{ object: string; kind: string; grantee: string; privilege: string; grantable: boolean }>(
      `WITH objects AS (
         SELECT 'table' AS kind, format('sessions.%I', c.relname) AS object,
                coalesce(c.relacl, acldefault('r', c.relowner)) AS acl
           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
         UNION ALL
         SELECT 'function', p.oid::regprocedure::text,
                coalesce(p.proacl, acldefault('f', p.proowner))
           FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
          WHERE n.nspname = 'sessions' AND p.prokind = 'f' AND p.provolatile = 'v')
       SELECT o.kind, o.object,
              CASE WHEN e.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS grantee,
              e.privilege_type AS privilege, e.is_grantable AS grantable
         FROM objects o
         CROSS JOIN LATERAL aclexplode(o.acl) e
         LEFT JOIN pg_roles r ON r.oid = e.grantee
        WHERE (e.grantee = 0 OR r.rolname = ANY ($1::text[]))
          AND e.privilege_type = ANY (ARRAY['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'EXECUTE'])
        ORDER BY 1, 2, 3, 4`,
      [roles],
    )
  ).rows;
  const restore = [
    "BEGIN;",
    ...granted.map(
      (row) =>
        `GRANT ${row.privilege} ON ${row.kind === "table" ? "TABLE" : "FUNCTION"} ${row.object} TO ${row.grantee}${row.grantable ? " WITH GRANT OPTION" : ""};`,
    ),
    "COMMIT;",
  ];
  return {
    fence,
    restore,
    tables: tables.length,
    volatileFunctions: volatile.length,
    readableFunctions: functions.length - volatile.length,
  };
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

export async function main() {
  const url = process.env.RELAYHISTORY_SOURCE_URL;
  if (!url) throw new Error("RELAYHISTORY_SOURCE_URL is required");
  const roles = (argument("roles") ?? "").split(",").filter(Boolean);
  const client = new pg.Client({ connectionString: url, application_name: "96bulls-neon-fence-plan" });
  await client.connect();
  try {
    await client.query("SET default_transaction_read_only = on");
    const plan = await planFence(client, roles);
    process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
  } finally {
    await client.end();
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls neon fence plan failed: ${error instanceof Error ? error.message : "unknown error"}`);
    process.exit(1);
  });
