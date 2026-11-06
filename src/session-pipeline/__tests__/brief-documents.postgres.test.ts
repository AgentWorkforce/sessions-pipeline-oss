/**
 * Real-PostgreSQL lock proof for the default-off brief-document trigger. Opt in
 * with the owned synthetic SESSION_INGEST_TEST_PG_URL; this creates and drops an
 * isolated database and never connects to a deployed database.
 */
import { afterAll, beforeAll, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

setDefaultTimeout(120_000);
const url = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../..");
const sql = (path: string) =>
  readFileSync(join(root, path), "utf8")
    .split("\n")
    .filter((line) => !line.startsWith("\\"))
    .join("\n");
let admin: pg.Client;
let pool: pg.Pool;
let name = "";

beforeAll(async () => {
  if (!url) return;
  admin = new pg.Client({ connectionString: url });
  await admin.connect();
  name = "brief_documents_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url);
  target.pathname = "/" + name;
  pool = new pg.Pool({ connectionString: target.href, max: 4 });
  await pool.query(sql("supabase/tests/supabase_shim.sql"));
  await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  for (const file of [
    "session_directory_search_worker.sql",
    "session_token_verifier.sql",
    "session_ingest_roles.sql",
  ])
    await pool.query(sql("supabase/bootstrap/" + file));
  for (const file of readdirSync(join(root, "supabase/migrations"))
    .filter((file) => file.endsWith(".sql"))
    .sort())
    await pool.query(sql("supabase/migrations/" + file));
});

afterAll(async () => {
  if (!url) return;
  await pool.end();
  await admin.query("DROP DATABASE IF EXISTS " + name);
  await admin.end();
});

test.skipIf(!url)("default-off writes in one scope do not serialize on publication state", async () => {
  await pool.query(
    `INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)
     VALUES('brief-lock-org','brief-lock-ws','claude','one',now(),now()),
           ('brief-lock-org','brief-lock-ws','claude','two',now(),now())`,
  );
  const a = await pool.connect();
  const b = await pool.connect();
  try {
    await a.query("BEGIN");
    await a.query(
      "UPDATE sessions.session_directory SET title='held one' WHERE org_id='brief-lock-org' AND session_id='one'",
    );
    await b.query("BEGIN; SET LOCAL lock_timeout='1500ms'");
    const started = Date.now();
    await b.query(
      "UPDATE sessions.session_directory SET title='free two' WHERE org_id='brief-lock-org' AND session_id='two'",
    );
    expect(Date.now() - started).toBeLessThan(1_000);
    await b.query("COMMIT");
    await a.query("COMMIT");
  } finally {
    await a.query("ROLLBACK").catch(() => {});
    await b.query("ROLLBACK").catch(() => {});
    a.release();
    b.release();
  }
  expect(
    (
      await pool.query(
        `SELECT
          (SELECT count(*)::int FROM sessions.session_brief_document_scopes) scopes,
          (SELECT count(*)::int FROM sessions.session_brief_document_outbox) outbox`,
      )
    ).rows[0],
  ).toEqual({ scopes: 0, outbox: 0 });
});

test.skipIf(!url)("activation setter is owner-only", async () => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN; SET LOCAL ROLE service_role");
    await expect(
      client.query("SELECT sessions.set_session_brief_documents_live(true)"),
    ).rejects.toMatchObject({ code: "42501" });
  } finally {
    await client.query("ROLLBACK");
    client.release();
  }
});
