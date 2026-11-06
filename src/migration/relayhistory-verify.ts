/**
 * Fail-closed, read-only source/target verifier for the RelayHistory migration.
 * Schema mode checks that the target is a compatible superset. Full mode uses
 * deterministic primary-key ranges so large tables can be verified and resumed
 * without restarting a monolithic scan.
 *
 * Under the T0 fresh-write cutover (docs/relayhistory-supabase-migration.md),
 * schema mode and --emit-sequence-fence gate T0. Full mode assumes the target
 * holds exactly the source rows, so it does not apply once Supabase accepts
 * writes; the backfill verifier replaces it.
 *
 * Required env: RELAYHISTORY_SOURCE_URL, BULLS_TARGET_URL.
 * Usage: bun run migration:verify [--mode=schema|full] [--chunk-size=10000]
 *   [--checkpoint=/safe/path/checkpoint.json] [--emit-sequence-fence]
 *   [--source-role=<Neon runtime role> --target-role=<Supabase runtime role>]
 * Schema mode does not pass without both roles: the target role must hold every
 * privilege the source role holds in `sessions`.
 */
import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import pg from "pg";

type Mode = "schema" | "full";

interface Column {
  name: string;
  udt: string;
  nullable: boolean;
  ordinal: number;
}

export interface Fingerprint {
  count: string;
  hash1: string;
  hash2: string;
}

interface TableCheckpoint {
  schemaSignature: string;
  keyColumns: string[];
  after: unknown[] | null;
  source: Fingerprint;
  target: Fingerprint;
  chunks: number;
  complete: boolean;
  equal: boolean;
  targetTailDetected?: boolean;
}

interface VerifyCheckpoint {
  version: 1;
  endpointHash: string;
  chunkSize: number;
  tables: Record<string, TableCheckpoint>;
}

const EMPTY_FINGERPRINT: Fingerprint = { count: "0", hash1: "0", hash2: "0" };

export function quoteIdent(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function keyComparison(columns: string[], operator: ">" | "<=", firstParameter: number): string {
  const identifiers = columns.map(quoteIdent);
  const parameters = columns.map((_, index) => `$${firstParameter + index}`);
  if (columns.length === 1) return `${identifiers[0]} ${operator} ${parameters[0]}`;
  return `(${identifiers.join(", ")}) ${operator} (${parameters.join(", ")})`;
}

/** Retained for callers that need the legacy one-shot SQL; full mode never uses it. */
export function fingerprintSql(schema: string, table: string, columns: string[]): string {
  const projection = columns.map(quoteIdent).join(", ");
  return `SELECT count(*)::text AS count,
    coalesce(sum(hashtextextended(row_to_json(row_value)::text, 0)::numeric), 0)::text AS hash1,
    coalesce(sum(hashtextextended(row_to_json(row_value)::text, 961996)::numeric), 0)::text AS hash2
  FROM (SELECT ${projection} FROM ${quoteIdent(schema)}.${quoteIdent(table)}) AS row_value`;
}

export function chunkFingerprintSql(
  schema: string,
  table: string,
  columns: string[],
  keyColumns: string[],
  hasAfter: boolean,
): string {
  const projection = columns.map(quoteIdent).join(", ");
  const predicates: string[] = [];
  if (hasAfter) predicates.push(keyComparison(keyColumns, ">", 1));
  predicates.push(keyComparison(keyColumns, "<=", hasAfter ? keyColumns.length + 1 : 1));
  return `SELECT count(*)::text AS count,
    coalesce(sum(hashtextextended(row_to_json(row_value)::text, 0)::numeric), 0)::text AS hash1,
    coalesce(sum(hashtextextended(row_to_json(row_value)::text, 961996)::numeric), 0)::text AS hash2
  FROM (SELECT ${projection} FROM ${quoteIdent(schema)}.${quoteIdent(table)}
    WHERE ${predicates.join(" AND ")}) AS row_value`;
}

export function chunkUpperBoundSql(
  schema: string,
  table: string,
  keyColumns: string[],
  hasAfter: boolean,
): string {
  const keys = keyColumns.map(quoteIdent).join(", ");
  const limitParameter = hasAfter ? keyColumns.length + 1 : 1;
  const where = hasAfter ? ` WHERE ${keyComparison(keyColumns, ">", 1)}` : "";
  return `SELECT ${keys} FROM (
    SELECT ${keys} FROM ${quoteIdent(schema)}.${quoteIdent(table)}${where}
    ORDER BY ${keys} LIMIT $${limitParameter}
  ) AS verify_chunk ORDER BY ${keyColumns.map((column) => `${quoteIdent(column)} DESC`).join(", ")} LIMIT 1`;
}

/** One database session: a pg.PoolClient or a PGlite. Never a pool (see readCatalog). */
export interface Queryable {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

/** A column as pg_attribute/pg_attrdef define it; "" means none for generated and identity. */
export interface ColumnDefinition {
  type: string;
  notNull: boolean;
  default: string | null;
  generated: string;
  identity: string;
  collation: string | null;
}

export interface TableSecurity {
  rowSecurity: boolean;
  forceRowSecurity: boolean;
}

export interface CatalogObjects {
  indexes: Map<string, string>;
  constraints: Map<string, string>;
  functions: Set<string>;
  triggers: Map<string, string>;
  /** table → column → definition. */
  columns: Map<string, Map<string, ColumnDefinition>>;
  /** Signature → pg_get_functiondef, for plain functions and procedures. */
  functionDefinitions: Map<string, string>;
  /** table → row-level security flags. */
  tables: Map<string, TableSecurity>;
  /** table.policy → canonical definition. */
  policies: Map<string, string>;
  /** User-defined enum, composite, domain and range types → canonical definition. */
  types: Map<string, string>;
}

/** What a runtime role may do in `sessions`, by has_*_privilege (so grants to PUBLIC and inherited roles count). */
export interface RoleAccess {
  name: string;
  exists: boolean;
  /** "<privilege> on <kind> <name>", e.g. "SELECT on table machines". */
  privileges: Set<string>;
  /** Tables whose row security this role bypasses (superuser, BYPASSRLS, or owner without FORCE). */
  rowSecurityExempt: Set<string>;
}

const TABLE_PRIVILEGES = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"];
const SEQUENCE_PRIVILEGES = ["USAGE", "SELECT", "UPDATE"];

/**
 * The live sessions schema beyond the table list: column definitions, indexes
 * (including those the runner builds CONCURRENTLY), constraints, functions and
 * their full definitions, triggers, row security and policies, and types. A
 * missing rollout trigger or search index would otherwise pass the T0 gate.
 *
 * `db` must be a single session: everything is read in one read-only snapshot
 * with search_path pinned to pg_catalog, so deparsed definitions qualify every
 * other schema identically on both sides whatever each server's default path is.
 * With `role`, also reads that role's privileges (see RoleAccess).
 */
export async function readCatalog(
  db: Queryable,
  role?: string,
): Promise<{ objects: CatalogObjects; role: RoleAccess | null }> {
  await db.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
  try {
    await db.query("SET LOCAL search_path = pg_catalog");
    const objects = await catalogObjects(db);
    return { objects, role: role ? await roleAccess(db, role) : null };
  } finally {
    await db.query("ROLLBACK");
  }
}

async function catalogObjects(db: Queryable): Promise<CatalogObjects> {
  // Sequential: one session runs one query at a time anyway.
  const indexes = await db.query<{ name: string; def: string; valid: boolean }>(
    `SELECT i.indexname AS name, i.indexdef AS def, x.indisvalid AS valid
       FROM pg_indexes i
       JOIN pg_class c ON c.relname = i.indexname
       JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
       JOIN pg_index x ON x.indexrelid = c.oid
      WHERE i.schemaname = 'sessions'`,
  );
  const constraints = await db.query<{ key: string; def: string }>(
    `SELECT c.relname || '.' || k.conname AS key, pg_get_constraintdef(k.oid) AS def
       FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND NOT c.relispartition`,
  );
  // pg_get_functiondef rejects aggregates and window functions; their signatures still count.
  const functions = await db.query<{ sig: string; def: string | null }>(
    `SELECT p.oid::regprocedure::text AS sig,
            CASE WHEN p.prokind IN ('f', 'p') THEN pg_get_functiondef(p.oid) END AS def
       FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'sessions'`,
  );
  const triggers = await db.query<{ key: string; def: string }>(
    `SELECT c.relname || '.' || t.tgname AS key, pg_get_triggerdef(t.oid) AS def
       FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND NOT t.tgisinternal AND NOT c.relispartition`,
  );
  const tables = await db.query<{ name: string; rls: boolean; force: boolean }>(
    `SELECT c.relname::text AS name, c.relrowsecurity AS rls, c.relforcerowsecurity AS force
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p') AND NOT c.relispartition`,
  );
  const columns = await db.query<{
    table: string;
    column: string;
    type: string;
    not_null: boolean;
    default: string | null;
    generated: string;
    identity: string;
    collation: string | null;
  }>(
    `SELECT c.relname::text AS table, a.attname::text AS column,
            format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS not_null,
            pg_get_expr(d.adbin, d.adrelid) AS default,
            a.attgenerated::text AS generated, a.attidentity::text AS identity,
            co.collname::text AS collation
       FROM pg_attribute a
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       LEFT JOIN pg_collation co ON co.oid = a.attcollation
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p') AND NOT c.relispartition
        AND a.attnum > 0 AND NOT a.attisdropped`,
  );
  const policies = await db.query<{
    key: string;
    permissive: string;
    roles: string[];
    cmd: string;
    qual: string | null;
    with_check: string | null;
  }>(
    `SELECT tablename || '.' || policyname AS key, permissive, roles::text[] AS roles,
            cmd, qual, with_check
       FROM pg_policies WHERE schemaname = 'sessions'`,
  );
  const types = await db.query<{
    name: string;
    kind: string;
    base: string | null;
    not_null: boolean;
    default: string | null;
    collation: string | null;
    labels: string[];
    attributes: string[];
    constraints: string[];
  }>(
    `SELECT t.typname::text AS name, t.typtype::text AS kind,
            CASE t.typtype
              WHEN 'd' THEN format_type(t.typbasetype, t.typtypmod)
              WHEN 'r' THEN (SELECT format_type(r.rngsubtype, NULL) FROM pg_range r WHERE r.rngtypid = t.oid)
            END AS base,
            t.typnotnull AS not_null, pg_get_expr(t.typdefaultbin, 0) AS default,
            (SELECT co.collname::text FROM pg_collation co WHERE co.oid = t.typcollation) AS collation,
            ARRAY(SELECT e.enumlabel::text FROM pg_enum e WHERE e.enumtypid = t.oid
                   ORDER BY e.enumsortorder) AS labels,
            ARRAY(SELECT a.attname::text || ' ' || format_type(a.atttypid, a.atttypmod)
                         || coalesce(' COLLATE ' || co.collname::text, '')
                    FROM pg_attribute a LEFT JOIN pg_collation co ON co.oid = a.attcollation
                   WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
                   ORDER BY a.attnum) AS attributes,
            ARRAY(SELECT k.conname::text || ' ' || pg_get_constraintdef(k.oid)
                    FROM pg_constraint k WHERE k.contypid = t.oid ORDER BY k.conname) AS constraints
       FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
      WHERE n.nspname = 'sessions'
        AND (t.typtype IN ('e', 'd', 'r')
             OR (t.typtype = 'c' AND (SELECT c.relkind FROM pg_class c WHERE c.oid = t.typrelid) = 'c'))`,
  );
  const columnsByTable = new Map<string, Map<string, ColumnDefinition>>();
  for (const row of columns.rows) {
    const table = columnsByTable.get(row.table) ?? new Map<string, ColumnDefinition>();
    table.set(row.column, {
      type: row.type,
      notNull: row.not_null,
      default: row.default,
      generated: row.generated,
      identity: row.identity,
      collation: row.collation,
    });
    columnsByTable.set(row.table, table);
  }
  return {
    // An INVALID index (interrupted CONCURRENTLY build) does not count as present.
    indexes: new Map(indexes.rows.filter((row) => row.valid).map((row) => [row.name, row.def])),
    constraints: new Map(constraints.rows.map((row) => [row.key, row.def])),
    functions: new Set(functions.rows.map((row) => row.sig)),
    triggers: new Map(triggers.rows.map((row) => [row.key, row.def])),
    columns: columnsByTable,
    functionDefinitions: new Map(
      functions.rows.flatMap((row) => (row.def === null ? [] : [[row.sig, row.def] as [string, string]])),
    ),
    tables: new Map(tables.rows.map((row) => [row.name, { rowSecurity: row.rls, forceRowSecurity: row.force }])),
    policies: new Map(policies.rows.map((row) => [row.key, JSON.stringify({
      cmd: row.cmd,
      permissive: row.permissive,
      roles: [...row.roles].sort(),
      qual: row.qual,
      withCheck: row.with_check,
    })])),
    types: new Map(types.rows.map((row) => [row.name, JSON.stringify({
      kind: row.kind,
      base: row.base,
      notNull: row.not_null,
      default: row.default,
      collation: row.collation,
      labels: row.labels,
      attributes: row.attributes,
      constraints: row.constraints,
    })])),
  };
}

async function roleAccess(db: Queryable, name: string): Promise<RoleAccess> {
  const found = await db.query(`SELECT 1 FROM pg_roles WHERE rolname = $1`, [name]);
  if (found.rows.length === 0) return { name, exists: false, privileges: new Set(), rowSecurityExempt: new Set() };
  const privileges = await db.query<{ key: string }>(
    `WITH role AS (SELECT oid FROM pg_roles WHERE rolname = $1),
          sessions AS (SELECT oid FROM pg_namespace WHERE nspname = 'sessions')
     SELECT 'USAGE on schema sessions' AS key FROM role CROSS JOIN sessions
      WHERE has_schema_privilege(role.oid, sessions.oid, 'USAGE')
     UNION ALL
     SELECT p.privilege || ' on table ' || c.relname
       FROM role CROSS JOIN sessions JOIN pg_class c ON c.relnamespace = sessions.oid
       CROSS JOIN unnest($2::text[]) AS p(privilege)
      WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND NOT c.relispartition
        AND has_table_privilege(role.oid, c.oid, p.privilege)
     UNION ALL
     SELECT p.privilege || ' on sequence ' || c.relname
       FROM role CROSS JOIN sessions JOIN pg_class c ON c.relnamespace = sessions.oid
       CROSS JOIN unnest($3::text[]) AS p(privilege)
      WHERE c.relkind = 'S' AND has_sequence_privilege(role.oid, c.oid, p.privilege)
     UNION ALL
     SELECT 'EXECUTE on function ' || f.oid::regprocedure::text
       FROM role CROSS JOIN sessions JOIN pg_proc f ON f.pronamespace = sessions.oid
      WHERE has_function_privilege(role.oid, f.oid, 'EXECUTE')`,
    [name, TABLE_PRIVILEGES, SEQUENCE_PRIVILEGES],
  );
  const exempt = await db.query<{ name: string }>(
    `SELECT c.relname::text AS name
       FROM pg_roles r CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE r.rolname = $1 AND n.nspname = 'sessions' AND c.relkind IN ('r', 'p') AND NOT c.relispartition
        AND (r.rolsuper OR r.rolbypassrls
             OR (pg_has_role(r.oid, c.relowner, 'USAGE') AND NOT c.relforcerowsecurity))`,
    [name],
  );
  return {
    name,
    exists: true,
    privileges: new Set(privileges.rows.map((row) => row.key)),
    rowSecurityExempt: new Set(exempt.rows.map((row) => row.name)),
  };
}

/**
 * Intentional, reviewed differences between Neon and the Supabase target, as
 * the exact gap text schemaDifferences reports. Keep this minimal: nothing else
 * may differ. Objects only the target has (the 96Bulls columns on machines, the
 * machines_member_read policy, pipeline tables) need no entry: a superset passes.
 */
export const ALLOWED_SCHEMA_DIFFERENCES: ReadonlySet<string> = new Set([
  // 20261025000000_sessions_index.sql: tenants read their org's machines through
  // the machines_member_read policy. The runtime-role check still requires the
  // RelayHistory role to bypass it (missingPrivileges).
  "different row security machines: source disabled, target enabled",
  // The Supabase auth path intentionally enables RLS on the shared legacy
  // auth_sessions table; Neon leaves it off. Reviewed divergence.
  "different row security auth_sessions: source disabled, target enabled",
  // 20261030000000_relayhistory_machines_compat.sql: sessions.upsert_machine
  // inserts machines rows without a workspace.
  "different column default machines.workspace_id: source none, target 'default'::text",
]);

const orNone = (value: string | null) => (value === null || value === "" ? "none" : value);
const COLUMN_ATTRIBUTES: Array<[string, (column: ColumnDefinition) => string]> = [
  ["type", (column) => column.type],
  ["nullability", (column) => (column.notNull ? "NOT NULL" : "nullable")],
  ["default", (column) => orNone(column.default)],
  ["generated", (column) => orNone(column.generated)],
  ["identity", (column) => orNone(column.identity)],
  ["collation", (column) => orNone(column.collation)],
];

/** Every way the target is not a superset of the source with identical definitions, allowlisted ones included. */
export function schemaDifferences(source: CatalogObjects, target: CatalogObjects): string[] {
  const out: string[] = [];
  for (const [kind, from, to] of [
    ["index", source.indexes, target.indexes],
    ["constraint", source.constraints, target.constraints],
    ["trigger", source.triggers, target.triggers],
    ["policy", source.policies, target.policies],
    ["type", source.types, target.types],
  ] as const) {
    for (const [name, def] of from) {
      const found = to.get(name);
      if (found === undefined) out.push(`missing ${kind} ${name}`);
      else if (found !== def) out.push(`different ${kind} ${name}`);
    }
  }
  for (const signature of source.functions)
    if (!target.functions.has(signature)) out.push(`missing function ${signature}`);
  for (const [signature, def] of source.functionDefinitions) {
    const found = target.functionDefinitions.get(signature);
    if (found !== undefined && found !== def) out.push(`different function definition ${signature}`);
  }
  const flag = (on: boolean) => (on ? "enabled" : "disabled");
  for (const [table, security] of source.tables) {
    const found = target.tables.get(table);
    if (!found) {
      out.push(`missing table ${table}`);
      continue;
    }
    if (found.rowSecurity !== security.rowSecurity)
      out.push(`different row security ${table}: source ${flag(security.rowSecurity)}, target ${flag(found.rowSecurity)}`);
    if (found.forceRowSecurity !== security.forceRowSecurity)
      out.push(`different forced row security ${table}: source ${flag(security.forceRowSecurity)}, target ${flag(found.forceRowSecurity)}`);
  }
  for (const [table, columns] of source.columns) {
    const targetColumns = target.columns.get(table);
    if (!targetColumns) continue; // reported as a missing table
    for (const [name, column] of columns) {
      const found = targetColumns.get(name);
      if (!found) {
        out.push(`missing column ${table}.${name}`);
        continue;
      }
      for (const [attribute, read] of COLUMN_ATTRIBUTES) {
        if (read(found) !== read(column))
          out.push(`different column ${attribute} ${table}.${name}: source ${read(column)}, target ${read(found)}`);
      }
    }
  }
  return out.sort();
}

/** Every source object the target lacks or defines differently, less the reviewed allowlist. */
export function missingObjects(source: CatalogObjects, target: CatalogObjects): string[] {
  return schemaDifferences(source, target).filter((gap) => !ALLOWED_SCHEMA_DIFFERENCES.has(gap));
}

/** The allowlisted differences actually present, reported so a reviewer sees them. */
export function allowedDifferences(source: CatalogObjects, target: CatalogObjects): string[] {
  return schemaDifferences(source, target).filter((gap) => ALLOWED_SCHEMA_DIFFERENCES.has(gap));
}

/**
 * Tables where a target-side RLS enablement is the reviewed hardening, not a
 * missing privilege for the runtime role: under the Supabase auth backend the
 * shared auth_sessions table is reached only through service_role and the
 * session_token_verifier edge — the app runtime role must not read it, so the
 * new restriction is intended.
 */
export const ALLOWED_NEW_ROW_SECURITY: ReadonlySet<string> = new Set(["auth_sessions"]);

/**
 * Every privilege the source runtime role holds that the target runtime role
 * lacks, and every table where the target role would be filtered by row
 * security that never filtered the source role (an RLS-enabled table with no
 * policy for the role reads as empty and rejects writes).
 */
export function missingPrivileges(
  sourceRole: RoleAccess,
  targetRole: RoleAccess,
  source: CatalogObjects,
  target: CatalogObjects,
): string[] {
  const out: string[] = [];
  if (!sourceRole.exists) out.push(`source role ${sourceRole.name} does not exist`);
  if (!targetRole.exists) out.push(`target role ${targetRole.name} does not exist`);
  if (out.length) return out;
  for (const privilege of sourceRole.privileges)
    if (!targetRole.privileges.has(privilege)) out.push(`target role ${targetRole.name} lacks ${privilege}`);
  const filtered = (objects: CatalogObjects, role: RoleAccess, table: string) =>
    objects.tables.get(table)?.rowSecurity === true && !role.rowSecurityExempt.has(table);
  for (const table of source.tables.keys()) {
    if (ALLOWED_NEW_ROW_SECURITY.has(table)) continue;
    if (target.tables.has(table) && !filtered(source, sourceRole, table) && filtered(target, targetRole, table))
      out.push(`target role ${targetRole.name} is subject to row security on ${table}; source role ${sourceRole.name} is not`);
  }
  return out.sort();
}

/** Each database's own ledger and rollout progress: never copied, never compared. */
export function isPerDatabaseState(table: string): boolean {
  return table === "__migrations" || table.endsWith("_rollout");
}

export function selectUsableKey(sourceKey: string[], targetKey: string[]): string[] | null {
  if (sourceKey.length === 0 || sourceKey.length !== targetKey.length) return null;
  return sourceKey.every((column, index) => column === targetKey[index]) ? sourceKey : null;
}

export function addFingerprint(left: Fingerprint, right: Fingerprint): Fingerprint {
  return {
    count: (BigInt(left.count) + BigInt(right.count)).toString(),
    hash1: (BigInt(left.hash1) + BigInt(right.hash1)).toString(),
    hash2: (BigInt(left.hash2) + BigInt(right.hash2)).toString(),
  };
}

function equalFingerprint(left: Fingerprint, right: Fingerprint): boolean {
  return left.count === right.count && left.hash1 === right.hash1 && left.hash2 === right.hash2;
}

async function tables(pool: pg.Pool): Promise<string[]> {
  const result = await pool.query<{ table_name: string }>(`
    SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'sessions' AND table_type = 'BASE TABLE'
       AND table_name NOT LIKE '__96bulls_%'
     ORDER BY table_name`);
  return result.rows.map((row) => row.table_name);
}

async function columns(pool: pg.Pool, table: string): Promise<Column[]> {
  const result = await pool.query<{
    column_name: string;
    udt_name: string;
    is_nullable: "YES" | "NO";
    ordinal_position: number;
  }>(`
    SELECT column_name, udt_name, is_nullable, ordinal_position
      FROM information_schema.columns
     WHERE table_schema = 'sessions' AND table_name = $1
     ORDER BY ordinal_position`, [table]);
  return result.rows.map((row) => ({
    name: row.column_name,
    udt: row.udt_name,
    nullable: row.is_nullable === "YES",
    ordinal: row.ordinal_position,
  }));
}

async function primaryKey(pool: pg.Pool, table: string): Promise<string[]> {
  const result = await pool.query<{ column_name: string }>(`
    SELECT attribute.attname AS column_name
      FROM pg_index index_definition
      JOIN pg_class table_definition ON table_definition.oid = index_definition.indrelid
      JOIN pg_namespace namespace ON namespace.oid = table_definition.relnamespace
      JOIN unnest(index_definition.indkey) WITH ORDINALITY AS key_column(attnum, position) ON true
      JOIN pg_attribute attribute
        ON attribute.attrelid = table_definition.oid AND attribute.attnum = key_column.attnum
     WHERE namespace.nspname = 'sessions' AND table_definition.relname = $1
       AND index_definition.indisprimary
       AND key_column.position <= index_definition.indnkeyatts
     ORDER BY key_column.position`, [table]);
  return result.rows.map((row) => row.column_name);
}

async function upperBound(
  pool: pg.Pool,
  table: string,
  keyColumns: string[],
  after: unknown[] | null,
  size: number,
): Promise<unknown[] | null> {
  const values = after ? [...after, size] : [size];
  const result = await pool.query<Record<string, unknown>>(
    chunkUpperBoundSql("sessions", table, keyColumns, after !== null),
    values,
  );
  const row = result.rows[0];
  return row ? keyColumns.map((column) => row[column]) : null;
}

async function chunkFingerprint(
  pool: pg.Pool,
  table: string,
  columns: string[],
  keyColumns: string[],
  after: unknown[] | null,
  upper: unknown[],
): Promise<Fingerprint> {
  const values = after ? [...after, ...upper] : upper;
  const result = await pool.query<Fingerprint>(
    chunkFingerprintSql("sessions", table, columns, keyColumns, after !== null),
    values,
  );
  return result.rows[0]!;
}

async function hasTail(pool: pg.Pool, table: string, keyColumns: string[], after: unknown[] | null): Promise<boolean> {
  const keys = keyColumns.map(quoteIdent).join(", ");
  const where = after ? ` WHERE ${keyComparison(keyColumns, ">", 1)}` : "";
  const result = await pool.query(
    `SELECT ${keys} FROM ${quoteIdent("sessions")}.${quoteIdent(table)}${where} ORDER BY ${keys} LIMIT 1`,
    after ?? [],
  );
  return result.rowCount !== 0;
}

interface SequenceValue {
  name: string;
  lastValue: string | null;
}

async function sequences(pool: pg.Pool): Promise<SequenceValue[]> {
  const result = await pool.query<{ sequencename: string; last_value: string | null }>(`
    SELECT sequencename, last_value::text
      FROM pg_sequences WHERE schemaname = 'sessions' ORDER BY sequencename`);
  return result.rows.map((row) => ({ name: row.sequencename, lastValue: row.last_value }));
}

/** readCatalog on one pooled connection; a broken session is discarded, not reused. */
async function catalogFrom(pool: pg.Pool, role: string | undefined) {
  const client = await pool.connect();
  try {
    const catalog = await readCatalog(client, role);
    client.release();
    return catalog;
  } catch (error) {
    client.release(true);
    throw error;
  }
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

function parseMode(): Mode {
  const raw = argument("mode") ?? "schema";
  if (raw !== "schema" && raw !== "full") throw new Error("--mode must be schema or full");
  return raw;
}

function parseChunkSize(): number {
  const value = Number(argument("chunk-size") ?? process.env.VERIFY_CHUNK_SIZE ?? 10_000);
  if (!Number.isInteger(value) || value < 1) throw new Error("--chunk-size must be a positive integer");
  return value;
}

function sequenceFenceSql(source: SequenceValue[], gap = 1_000_000): string[] {
  return source.flatMap((item) => item.lastValue === null ? [] : [
    `SELECT setval('${quoteIdent("sessions")}.${quoteIdent(item.name)}'::regclass, ${BigInt(item.lastValue) + BigInt(gap)}, true);`,
  ]);
}

function schemaSignature(columns: Column[], keyColumns: string[]): string {
  return createHash("sha256").update(JSON.stringify({ columns, keyColumns })).digest("hex");
}

function endpointHash(sourceUrl: string, targetUrl: string): string {
  return createHash("sha256").update(sourceUrl).update("\0").update(targetUrl).digest("hex");
}

async function loadCheckpoint(path: string | undefined, hash: string, size: number): Promise<VerifyCheckpoint> {
  const empty: VerifyCheckpoint = { version: 1, endpointHash: hash, chunkSize: size, tables: {} };
  if (!path) return empty;
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty;
    throw error;
  }
  const parsed = JSON.parse(raw) as Partial<VerifyCheckpoint>;
  if (parsed.version !== 1 || parsed.endpointHash !== hash || parsed.chunkSize !== size || !parsed.tables) {
    throw new Error("checkpoint does not match these endpoints or chunk size");
  }
  const integerText = /^-?\d+$/;
  for (const [table, state] of Object.entries(parsed.tables)) {
    const validFingerprint = (value: Fingerprint | undefined) =>
      value !== undefined && integerText.test(value.count) && integerText.test(value.hash1) && integerText.test(value.hash2);
    if (
      !table || !state || typeof state.schemaSignature !== "string" ||
      !Array.isArray(state.keyColumns) || state.keyColumns.length === 0 ||
      state.keyColumns.some((column) => typeof column !== "string" || !column) ||
      (state.after !== null && (!Array.isArray(state.after) || state.after.length !== state.keyColumns.length)) ||
      !validFingerprint(state.source) || !validFingerprint(state.target) ||
      !Number.isInteger(state.chunks) || state.chunks < 0 ||
      typeof state.complete !== "boolean" || typeof state.equal !== "boolean"
    ) {
      throw new Error("checkpoint is corrupt or incomplete");
    }
  }
  return parsed as VerifyCheckpoint;
}

async function saveCheckpoint(path: string | undefined, checkpoint: VerifyCheckpoint): Promise<void> {
  if (!path) return;
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}`;
  await writeFile(temporary, `${JSON.stringify(checkpoint, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
  await rename(temporary, path);
}

async function verifyTableData(
  source: pg.Pool,
  target: pg.Pool,
  table: string,
  names: string[],
  keyColumns: string[],
  signature: string,
  size: number,
  checkpoint: VerifyCheckpoint,
  checkpointPath: string | undefined,
): Promise<TableCheckpoint> {
  let state = checkpoint.tables[table];
  if (!state || state.schemaSignature !== signature || state.keyColumns.join("\0") !== keyColumns.join("\0")) {
    state = {
      schemaSignature: signature,
      keyColumns,
      after: null,
      source: { ...EMPTY_FINGERPRINT },
      target: { ...EMPTY_FINGERPRINT },
      chunks: 0,
      complete: false,
      equal: true,
    };
    checkpoint.tables[table] = state;
  }
  if (state.complete) return state;

  for (;;) {
    const upper = await upperBound(source, table, keyColumns, state.after, size);
    if (!upper) {
      const targetHasTail = await hasTail(target, table, keyColumns, state.after);
      state.targetTailDetected = targetHasTail;
      state.equal = state.equal && !targetHasTail && equalFingerprint(state.source, state.target);
      state.complete = true;
      await saveCheckpoint(checkpointPath, checkpoint);
      return state;
    }
    const [sourceChunk, targetChunk] = await Promise.all([
      chunkFingerprint(source, table, names, keyColumns, state.after, upper),
      chunkFingerprint(target, table, names, keyColumns, state.after, upper),
    ]);
    state.source = addFingerprint(state.source, sourceChunk);
    state.target = addFingerprint(state.target, targetChunk);
    state.equal = state.equal && equalFingerprint(sourceChunk, targetChunk);
    state.after = upper;
    state.chunks += 1;
    await saveCheckpoint(checkpointPath, checkpoint);
  }
}

export async function main() {
  const sourceUrl = process.env.RELAYHISTORY_SOURCE_URL;
  const targetUrl = process.env.BULLS_TARGET_URL;
  if (!sourceUrl || !targetUrl) throw new Error("RELAYHISTORY_SOURCE_URL and BULLS_TARGET_URL are required");
  const mode = parseMode();
  const size = parseChunkSize();
  const checkpointPath = argument("checkpoint") ?? process.env.VERIFY_CHECKPOINT_PATH;
  const checkpoint = mode === "full"
    ? await loadCheckpoint(checkpointPath, endpointHash(sourceUrl, targetUrl), size)
    : { version: 1 as const, endpointHash: endpointHash(sourceUrl, targetUrl), chunkSize: size, tables: {} };
  const resumedTables = Object.values(checkpoint.tables).filter((table) => table.chunks > 0).length;
  const source = new pg.Pool({ connectionString: sourceUrl, max: 2, application_name: "96bulls-migration-verify-source", statement_timeout: 0 });
  const target = new pg.Pool({ connectionString: targetUrl, max: 2, application_name: "96bulls-migration-verify-target", statement_timeout: 0 });
  let passed = true;
  try {
    const sourceTables = await tables(source);
    const targetTables = new Set(await tables(target));
    const results: Array<Record<string, unknown>> = [];
    for (const table of sourceTables) {
      if (!targetTables.has(table)) {
        passed = false;
        results.push({ table, schema: "missing" });
        continue;
      }
      const [sourceColumns, targetColumnList] = await Promise.all([columns(source, table), columns(target, table)]);
      const targetColumns = new Map(targetColumnList.map((column) => [column.name, column]));
      const incompatible = sourceColumns.flatMap((column) => {
        const candidate = targetColumns.get(column.name);
        return !candidate || candidate.udt !== column.udt
          ? [{ column: column.name, sourceType: column.udt, targetType: candidate?.udt ?? null }]
          : [];
      });
      if (incompatible.length) passed = false;
      const result: Record<string, unknown> = {
        table,
        schema: incompatible.length ? "incompatible" : "compatible",
        ...(incompatible.length ? { incompatible } : {}),
      };
      if (mode === "full" && incompatible.length === 0 && isPerDatabaseState(table)) {
        Object.assign(result, { data: "skipped", reason: "per-database runner or rollout state" });
      } else if (mode === "full" && incompatible.length === 0) {
        const [sourceKey, targetKey] = await Promise.all([primaryKey(source, table), primaryKey(target, table)]);
        const keyColumns = selectUsableKey(sourceKey, targetKey);
        if (!keyColumns && sourceKey.length === 0 && targetKey.length === 0) {
          // No primary key (derived tables keyed by a nullable unique index):
          // compare whole-table fingerprints, which are order-independent sums.
          const names = sourceColumns.map((column) => column.name);
          const [sourcePrint, targetPrint] = await Promise.all([
            source.query<Fingerprint>(fingerprintSql("sessions", table, names)),
            target.query<Fingerprint>(fingerprintSql("sessions", table, names)),
          ]);
          const equal = equalFingerprint(sourcePrint.rows[0]!, targetPrint.rows[0]!);
          if (!equal) passed = false;
          Object.assign(result, {
            data: equal ? "equal" : "different",
            method: "whole-table fingerprint",
            source: sourcePrint.rows[0],
            target: targetPrint.rows[0],
          });
        } else if (!keyColumns) {
          passed = false;
          Object.assign(result, {
            data: "unverifiable",
            reason: "matching primary key required for bounded deterministic verification",
            sourceKey,
            targetKey,
          });
        } else {
          const signature = schemaSignature(sourceColumns, keyColumns);
          const state = await verifyTableData(
            source,
            target,
            table,
            sourceColumns.map((column) => column.name),
            keyColumns,
            signature,
            size,
            checkpoint,
            checkpointPath,
          );
          if (!state.equal) passed = false;
          Object.assign(result, {
            data: state.equal ? "equal" : "different",
            keyColumns,
            chunks: state.chunks,
            targetTailDetected: state.targetTailDetected ?? false,
            source: state.source,
            target: state.target,
          });
        }
      }
      results.push(result);
    }
    const sourceRoleName = argument("source-role");
    const targetRoleName = argument("target-role");
    const [sourceCatalog, targetCatalog] = await Promise.all([
      catalogFrom(source, sourceRoleName),
      catalogFrom(target, targetRoleName),
    ]);
    const sourceObjects = sourceCatalog.objects;
    const targetObjects = targetCatalog.objects;
    const objectGaps = missingObjects(sourceObjects, targetObjects);
    if (objectGaps.length) passed = false;
    let roles: Record<string, unknown>;
    if (sourceCatalog.role && targetCatalog.role) {
      const roleGaps = missingPrivileges(sourceCatalog.role, targetCatalog.role, sourceObjects, targetObjects);
      if (roleGaps.length) passed = false;
      roles = {
        checked: true,
        source: sourceCatalog.role.name,
        target: targetCatalog.role.name,
        sourcePrivileges: sourceCatalog.role.privileges.size,
        missing: roleGaps,
      };
    } else {
      // The target must grant the RelayHistory runtime role what Neon grants it,
      // so schema mode (the T0 gate) cannot pass without the mapping.
      if (mode === "schema") passed = false;
      roles = {
        checked: false,
        reason: mode === "schema"
          ? "--source-role and --target-role are required in schema mode"
          : "pass --source-role and --target-role to check runtime-role grants",
      };
    }
    const [sourceSequences, targetSequences] = await Promise.all([sequences(source), sequences(target)]);
    const targetByName = new Map(targetSequences.map((item) => [item.name, item.lastValue]));
    const sequenceResults = sourceSequences.map((item) => {
      const targetValue = targetByName.get(item.name) ?? null;
      const ready = item.lastValue === null || (targetValue !== null && BigInt(targetValue) >= BigInt(item.lastValue));
      if (mode === "full" && !ready) passed = false;
      return { name: item.name, source: item.lastValue, target: targetValue, ready };
    });
    const output: Record<string, unknown> = {
      generatedAt: new Date().toISOString(),
      mode,
      passed,
      checkpoint: {
        enabled: mode === "full" && Boolean(checkpointPath),
        chunkSize: size,
        resumedTables,
        completedTables: Object.values(checkpoint.tables).filter((table) => table.complete).length,
      },
      tables: results,
      objects: {
        checked: sourceObjects.indexes.size + sourceObjects.constraints.size + sourceObjects.functions.size +
          sourceObjects.triggers.size + sourceObjects.tables.size + sourceObjects.policies.size + sourceObjects.types.size +
          [...sourceObjects.columns.values()].reduce((sum, table) => sum + table.size, 0),
        missing: objectGaps,
        allowed: allowedDifferences(sourceObjects, targetObjects),
      },
      roles,
      sequences: sequenceResults,
    };
    if (process.argv.includes("--emit-sequence-fence")) output.sequenceFenceSql = sequenceFenceSql(sourceSequences);
    process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
    if (!passed) process.exitCode = 2;
  } finally {
    await Promise.all([source.end(), target.end()]);
  }
}

if (import.meta.main) main().catch((error) => {
  console.error(`96bulls migration verification failed: ${error instanceof Error ? error.message : "unknown error"}`);
  process.exit(1);
});
