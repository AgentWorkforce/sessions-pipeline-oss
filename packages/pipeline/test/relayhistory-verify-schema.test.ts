/**
 * The live-schema T0 gate (`src/migration/relayhistory-verify.ts --mode=schema`)
 * against real catalogs: two PGlite databases standing in for Neon (legacy
 * migrations only) and the Supabase sessions project (legacy plus 96Bulls, in
 * either order). Pure comparisons are unit-tested in
 * `src/migration/__tests__/relayhistory-verify.test.ts`.
 */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ALLOWED_SCHEMA_DIFFERENCES,
  allowedDifferences,
  missingObjects,
  missingPrivileges,
  readCatalog,
} from "../../../src/migration/relayhistory-verify.js";

// Each test boots and migrates two or three databases.
vi.setConfig({ testTimeout: 120_000 });

// Built exactly as relayhistory-schema-parity.test.ts builds them.
const ROOT = new URL("../../../", import.meta.url).href;
const read = (path: string) =>
  readFileSync(fileURLToPath(new URL(path, ROOT).href), "utf8");
const sqlFiles = (dir: string) =>
  readdirSync(fileURLToPath(new URL(dir, ROOT).href))
    .filter((file) => file.endsWith(".sql"))
    .sort()
    .map((file) => ({ name: file, sql: read(`${dir}${file}`) }));
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");
const LEGACY_RUNNER = `SET search_path = pg_catalog, public;
CREATE SCHEMA IF NOT EXISTS sessions;
CREATE TABLE IF NOT EXISTS sessions.__migrations (
  name text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);`;
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
type Migration = { name: string; sql: string };
const legacy: Migration[] = [
  { name: "legacy runner prelude", sql: LEGACY_RUNNER },
  ...sqlFiles("vendor/relayhistory-cloud/migrations/").map((m) => ({
    ...m,
    sql: forPglite(m.sql),
  })),
  { name: "legacy runner concurrent indexes", sql: forPglite(runnerIndexes) },
];
const bulls: Migration[] = sqlFiles("supabase/migrations/").map((m) => ({
  ...m,
  sql: forPglite(m.sql),
}));

// The RelayHistory Worker's runtime role, as each runner would grant it.
const RUNTIME_ROLE = `CREATE ROLE relayhistory_app;
GRANT USAGE ON SCHEMA sessions TO relayhistory_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sessions TO relayhistory_app;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA sessions TO relayhistory_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO relayhistory_app;`;

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
  // Reset what the legacy runner's SET left behind, as a fresh session would.
  await db.exec("RESET search_path");
  return db;
}
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

describe("relayhistory-verify schema gate on real catalogs", () => {
  it.each([
    ["legacy, then 96Bulls", [legacy, bulls]],
    ["96Bulls, then legacy", [bulls, legacy]],
  ] as const)(
    "%s: passes, using exactly the reviewed allowlist",
    async (_order, sets) => {
      const sourceDb = await database(legacy);
      const targetDb = await database(...sets);
      await sourceDb.exec(RUNTIME_ROLE);
      await targetDb.exec(`${RUNTIME_ROLE}
        ALTER ROLE relayhistory_app BYPASSRLS;`);
      const source = await readCatalog(sourceDb, "relayhistory_app");
      const target = await readCatalog(targetDb, "relayhistory_app");
      expect(source.objects.functionDefinitions.size).toBeGreaterThan(10);
      expect(
        source.objects.columns.get("machines")?.get("workspace_id"),
      ).toMatchObject({
        notNull: true,
        default: null,
        collation: "default",
      });
      expect(missingObjects(source.objects, target.objects)).toEqual([]);
      expect(allowedDifferences(source.objects, target.objects)).toEqual(
        [...ALLOWED_SCHEMA_DIFFERENCES].sort(),
      );
      expect(source.role?.privileges).toContain("USAGE on schema sessions");
      expect(source.role?.privileges).toContain("SELECT on table machines");
      expect(
        missingPrivileges(
          source.role!,
          target.role!,
          source.objects,
          target.objects,
        ),
      ).toEqual([]);
    },
  );

  it("reports every kind of drift a hand-built target could have", async () => {
    const ddl = `CREATE SCHEMA sessions;
      CREATE TYPE sessions.state AS ENUM ('queued', 'running', 'done');
      CREATE TYPE sessions.pair AS (left_id bigint, right_id text);
      CREATE DOMAIN sessions.label AS text NOT NULL DEFAULT 'x' CHECK (VALUE <> '');
      CREATE TABLE sessions.jobs (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        state sessions.state NOT NULL DEFAULT 'queued',
        name text COLLATE "C",
        upper_name text GENERATED ALWAYS AS (upper(name)) STORED,
        note text
      );
      CREATE TABLE sessions.notes (org_id text NOT NULL, body text);
      ALTER TABLE sessions.notes ENABLE ROW LEVEL SECURITY;
      CREATE POLICY notes_org ON sessions.notes FOR SELECT TO PUBLIC USING (org_id = 'a');
      CREATE FUNCTION sessions.touch(p_id bigint) RETURNS bigint LANGUAGE sql AS 'SELECT p_id';
      CREATE ROLE app;
      GRANT USAGE ON SCHEMA sessions TO app;
      GRANT SELECT, INSERT ON sessions.jobs, sessions.notes TO app;
      GRANT USAGE ON SEQUENCE sessions.jobs_id_seq TO app;`;
    const sourceDb = new PGlite();
    const targetDb = new PGlite();
    open.push(sourceDb, targetDb);
    await sourceDb.exec(ddl);
    await targetDb.exec(`${ddl}
      ALTER TYPE sessions.state ADD VALUE 'failed';
      DROP TYPE sessions.pair;
      CREATE TYPE sessions.pair AS (left_id bigint, right_id varchar(10));
      ALTER DOMAIN sessions.label DROP NOT NULL;
      ALTER TABLE sessions.jobs DROP COLUMN upper_name;
      ALTER TABLE sessions.jobs ALTER COLUMN id SET GENERATED BY DEFAULT,
        ALTER COLUMN state DROP DEFAULT,
        ALTER COLUMN name TYPE text COLLATE "POSIX",
        ALTER COLUMN note SET NOT NULL,
        ADD COLUMN upper_name text;
      ALTER TABLE sessions.notes FORCE ROW LEVEL SECURITY;
      ALTER POLICY notes_org ON sessions.notes USING (org_id = 'b');
      CREATE OR REPLACE FUNCTION sessions.touch(p_id bigint) RETURNS bigint LANGUAGE sql AS 'SELECT p_id + 1';
      REVOKE INSERT ON sessions.jobs FROM app;
      REVOKE USAGE ON SEQUENCE sessions.jobs_id_seq FROM app;
      ALTER TABLE sessions.notes OWNER TO app;`);
    const source = await readCatalog(sourceDb, "app");
    const target = await readCatalog(targetDb, "app");
    expect(missingObjects(source.objects, target.objects)).toEqual([
      "different column collation jobs.name: source C, target POSIX",
      "different column default jobs.state: source 'queued'::sessions.state, target none",
      // A generation expression lives in pg_attrdef like a default.
      "different column default jobs.upper_name: source upper(name), target none",
      "different column generated jobs.upper_name: source s, target none",
      "different column identity jobs.id: source a, target d",
      "different column nullability jobs.note: source nullable, target NOT NULL",
      "different forced row security notes: source disabled, target enabled",
      "different function definition sessions.touch(bigint)",
      "different policy notes.notes_org",
      "different type label",
      "different type pair",
      "different type state",
    ]);
    // The source role is subject to the notes policy too, so owning (but
    // FORCEd) notes on the target is no regression; the lost grants are.
    expect(
      missingPrivileges(
        source.role!,
        target.role!,
        source.objects,
        target.objects,
      ),
    ).toEqual([
      "target role app lacks INSERT on table jobs",
      "target role app lacks USAGE on sequence jobs_id_seq",
    ]);
    const nobody = await readCatalog(targetDb, "nobody");
    expect(
      missingPrivileges(
        source.role!,
        nobody.role!,
        source.objects,
        target.objects,
      ),
    ).toEqual(["target role nobody does not exist"]);
  });

  it("requires the target runtime role to bypass row security the source never applied", async () => {
    const sourceDb = await database(legacy);
    const targetDb = await database(legacy, bulls);
    await sourceDb.exec(RUNTIME_ROLE);
    await targetDb.exec(RUNTIME_ROLE);
    const source = await readCatalog(sourceDb, "relayhistory_app");
    const target = await readCatalog(targetDb, "relayhistory_app");
    expect(
      missingPrivileges(
        source.role!,
        target.role!,
        source.objects,
        target.objects,
      ),
    ).toEqual([
      "target role relayhistory_app is subject to row security on machines; source role relayhistory_app is not",
    ]);
  });
});
