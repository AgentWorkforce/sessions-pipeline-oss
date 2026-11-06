/**
 * Schema parity for the RelayHistory (Neon) to Supabase cutover
 * (`docs/relayhistory-supabase-migration.md`, lane 2).
 *
 * After T0 the RelayHistory Worker writes to the Supabase `sessions` project,
 * which also carries the 96Bulls pipeline schema (`supabase/migrations`). The
 * combined database must therefore be a compatible superset of the legacy
 * schema: every legacy table, column, index, constraint, function and trigger,
 * with the same types, and no new column a legacy INSERT cannot satisfy. Both
 * migration sets keep shipping after T0, so both apply orders must hold.
 *
 * The legacy migrations and the runner's concurrent indexes are pinned in
 * `vendor/relayhistory-cloud` (`scripts/sync-relayhistory-schema.sh`). Live
 * catalogs (out-of-band indexes, the 0029 rollout) are checked separately by
 * the read-only verifier.
 */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const ROOT = new URL("../../../", import.meta.url).href;
const read = (path: string) =>
  readFileSync(fileURLToPath(new URL(path, ROOT).href), "utf8");
const sqlFiles = (dir: string) =>
  readdirSync(fileURLToPath(new URL(dir, ROOT).href))
    .filter((file) => file.endsWith(".sql"))
    .sort()
    .map((file) => ({ name: file, sql: read(`${dir}${file}`) }));

// PGlite has no pgvector here, so (like relayhistory-cloud's own in-process test)
// vector storage becomes text and HNSW indexes are skipped; both catalogs go
// through the same substitution. CONCURRENTLY cannot run inside PGlite's
// implicit multi-statement transaction; the resulting index is identical.
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");
// relayhistory-cloud's runner (`scripts/apply-neon-migration.mjs`) creates the
// schema and its ledger, and runs every file with this search_path.
const LEGACY_RUNNER = `SET search_path = pg_catalog, public;
CREATE SCHEMA IF NOT EXISTS sessions;
CREATE TABLE IF NOT EXISTS sessions.__migrations (
  name text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);`;
// After the migrations commit, the runner builds hot-table indexes CONCURRENTLY
// (CONCURRENT_INDEXES, extracted by the sync script). They are part of the
// production schema even though no migration file creates them.
const runnerIndexes = (
  JSON.parse(read("vendor/relayhistory-cloud/concurrent-indexes.json")) as {
    name: string;
    definition: string;
  }[]
)
  .map(
    (index) => `CREATE INDEX IF NOT EXISTS ${index.name} ${index.definition};`,
  )
  .join("\n");
const legacy = [
  { name: "legacy runner prelude", sql: LEGACY_RUNNER },
  ...sqlFiles("vendor/relayhistory-cloud/migrations/").map((m) => ({
    ...m,
    sql: forPglite(m.sql),
  })),
  { name: "legacy runner concurrent indexes", sql: forPglite(runnerIndexes) },
];
const bulls = sqlFiles("supabase/migrations/").map((m) => ({
  ...m,
  sql: forPglite(m.sql),
}));

type Migration = { name: string; sql: string };

const open: PGlite[] = [];
async function database(...sets: Migration[][]) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const migration of sets.flat()) {
    try {
      await db.exec(migration.sql);
    } catch (error) {
      throw new Error(`${migration.name}: ${(error as Error).message}`);
    }
  }
  return db;
}
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

interface Column {
  type: string;
  notNull: boolean;
  hasDefault: boolean;
}
interface Catalog {
  tables: Set<string>;
  columns: Map<string, Column>;
  indexes: Map<string, string>;
  constraints: Map<string, string>;
  functions: Set<string>;
  triggers: Set<string>;
}

async function rows<T>(db: PGlite, sql: string) {
  return (await db.query<T>(sql)).rows;
}

async function catalog(db: PGlite): Promise<Catalog> {
  const tables = await rows<{ name: string }>(
    db,
    `SELECT c.relname AS name FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p') AND NOT c.relispartition`,
  );
  const columns = await rows<{
    key: string;
    type: string;
    not_null: boolean;
    has_default: boolean;
  }>(
    db,
    `SELECT c.relname || '.' || a.attname AS key,
            format_type(a.atttypid, a.atttypmod) AS type,
            a.attnotnull AS not_null,
            (a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '') AS has_default
       FROM pg_attribute a
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
        AND NOT c.relispartition AND a.attnum > 0 AND NOT a.attisdropped`,
  );
  const indexes = await rows<{ name: string; def: string }>(
    db,
    `SELECT indexname AS name, indexdef AS def FROM pg_indexes WHERE schemaname = 'sessions'`,
  );
  const constraints = await rows<{ key: string; def: string }>(
    db,
    `SELECT c.relname || '.' || k.conname AS key, pg_get_constraintdef(k.oid) AS def
       FROM pg_constraint k
       JOIN pg_class c ON c.oid = k.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND NOT c.relispartition`,
  );
  const functions = await rows<{ sig: string }>(
    db,
    `SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS sig
       FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'sessions'`,
  );
  const triggers = await rows<{ key: string }>(
    db,
    `SELECT c.relname || '.' || t.tgname AS key
       FROM pg_trigger t
       JOIN pg_class c ON c.oid = t.tgrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND NOT t.tgisinternal AND NOT c.relispartition`,
  );
  return {
    tables: new Set(tables.map((t) => t.name)),
    columns: new Map(
      columns.map((c) => [
        c.key,
        { type: c.type, notNull: c.not_null, hasDefault: c.has_default },
      ]),
    ),
    indexes: new Map(indexes.map((i) => [i.name, i.def])),
    constraints: new Map(constraints.map((k) => [k.key, k.def])),
    functions: new Set(functions.map((f) => f.sig)),
    triggers: new Set(triggers.map((t) => t.key)),
  };
}

/** Every way `target` fails to be a compatible superset of `source`. */
function gaps(source: Catalog, target: Catalog): string[] {
  const out: string[] = [];
  for (const table of source.tables)
    if (!target.tables.has(table)) out.push(`missing table ${table}`);
  for (const [key, column] of source.columns) {
    const found = target.columns.get(key);
    if (!found) out.push(`missing column ${key}`);
    else if (found.type !== column.type)
      out.push(`column ${key}: ${found.type}, legacy ${column.type}`);
    else if (found.notNull && !column.notNull)
      out.push(`column ${key}: NOT NULL, legacy nullable`);
  }
  // A column only the target has must not break a legacy INSERT.
  for (const [key, column] of target.columns) {
    const table = key.slice(0, key.indexOf("."));
    if (
      source.tables.has(table) &&
      !source.columns.has(key) &&
      column.notNull &&
      !column.hasDefault
    )
      out.push(`extra column ${key} is NOT NULL without a default`);
  }
  for (const [name, def] of source.indexes) {
    const found = target.indexes.get(name);
    if (!found) out.push(`missing index ${name}`);
    else if (found !== def) out.push(`index ${name}: ${found}, legacy ${def}`);
  }
  for (const [key, def] of source.constraints) {
    const found = target.constraints.get(key);
    if (!found) out.push(`missing constraint ${key}`);
    else if (found !== def)
      out.push(`constraint ${key}: ${found}, legacy ${def}`);
  }
  for (const fn of source.functions)
    if (!target.functions.has(fn)) out.push(`missing function ${fn}`);
  for (const trigger of source.triggers)
    if (!target.triggers.has(trigger)) out.push(`missing trigger ${trigger}`);
  return out.sort();
}

describe("RelayHistory schema on the Supabase sessions project", () => {
  it("pins a non-empty legacy migration set and the runner's indexes", () => {
    expect(read("vendor/relayhistory-cloud/SOURCE")).toMatch(
      /^AgentWorkforce\/relayhistory-cloud [0-9a-f]{40}:/,
    );
    expect(legacy.length).toBeGreaterThan(2);
    expect(runnerIndexes).toMatch(/convergence_events_content_prefix_trgm_idx/);
  });

  it.each([
    ["legacy, then 96Bulls", [legacy, bulls]],
    ["96Bulls, then legacy", [bulls, legacy]],
  ] as const)(
    "%s: is a compatible superset of the legacy schema",
    async (_order, sets) => {
      const source = await catalog(await database(legacy));
      const target = await catalog(await database(...sets));
      expect(gaps(source, target)).toEqual([]);
    },
    120_000,
  );

  it.each([
    ["legacy, then 96Bulls", [legacy, bulls]],
    ["96Bulls, then legacy", [bulls, legacy]],
  ] as const)(
    "%s: both products write the shared machines row; tenants read only pipeline columns",
    async (_order, sets) => {
      const db = await database(...sets);
      await db.query(
        `INSERT INTO sessions.machines
           (org_id, workspace_id, machine_id, hostname, label, os, relayhistory_version, cursors_json)
         VALUES ('org-a', 'ws-a', 'm-1', 'host-1', 'laptop', 'darwin', '1.2.3', '{"claude":"c1"}')`,
      );
      await db.query(
        `SELECT sessions.upsert_machine('org-a', 'm-1', 'laptop', 'host-2', 'darwin', '0.4.0', now())`,
      );
      await db.query(
        `SELECT sessions.upsert_machine('org-a', 'm-2', null, 'host-3', 'linux', '0.4.0', now())`,
      );
      const machines = await rows<Record<string, unknown>>(
        db,
        `SELECT machine_id, workspace_id, hostname, cursors_json, hostname_aliases, client_version
           FROM sessions.machines ORDER BY machine_id`,
      );
      expect(machines).toEqual([
        {
          machine_id: "m-1",
          workspace_id: "ws-a",
          hostname: "host-1",
          cursors_json: { claude: "c1" },
          hostname_aliases: ["host-2"],
          client_version: "0.4.0",
        },
        {
          machine_id: "m-2",
          workspace_id: "default",
          hostname: null,
          cursors_json: {},
          hostname_aliases: ["host-3"],
          client_version: "0.4.0",
        },
      ]);
      // A signed-in tenant of org-a, through the 96Bulls RLS policy.
      await db.exec(`SET ROLE authenticated;
        SELECT set_config('request.jwt.claims', '{"app_metadata":{"org_id":"org-a"}}', false);`);
      expect(
        await rows(
          db,
          `SELECT org_id, machine_id, machine_name, hostname_aliases, os, client_version
             FROM sessions.machines ORDER BY machine_id`,
        ),
      ).toHaveLength(2);
      for (const select of [
        "*",
        "cursors_json",
        "hostname",
        "label",
        "workspace_id",
        "relayhistory_version",
      ])
        await expect(
          db.query(`SELECT ${select} FROM sessions.machines`),
        ).rejects.toThrow(/permission denied/);
      await expect(
        db.query(`UPDATE sessions.machines SET machine_name = 'x'`),
      ).rejects.toThrow(/permission denied/);
      await db.exec("RESET ROLE");
    },
    120_000,
  );
});
