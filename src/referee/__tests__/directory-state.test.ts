/**
 * The Referee's verdict reaches the logical-session directory: set_session_state
 * updates only the pointer, so the handler re-projects the session afterwards.
 * In-process PGlite with every supabase migration; synthetic data.
 */
import { afterEach, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { refereeHandler } from "../handler.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const KEY = { orgId: "o", workspaceId: "w", harness: "codex", sessionId: "s" };

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

test("a classified state is projected into the directory", async () => {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const dir of ["supabase/migrations", "src/hoopdb/migrations"])
    for (const file of readdirSync(join(root, dir))
      .filter((f) => f.endsWith(".sql"))
      .sort())
      await db.exec(read(`${dir}/${file}`));
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, trace_id, revision)
     VALUES ('o', 'w', 'codex', 's', 'm', 'u', now(), now(), 's3', 'bucket', 'key/m',
             '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1)`,
  );
  await projectSessionDirectory(db as never, KEY);
  const state = async () =>
    (
      await db.query<{ state: string }>(
        "SELECT state FROM sessions.session_directory WHERE session_id = 's'",
      )
    ).rows[0]?.state;
  expect(await state()).toBe("unknown");

  const manifest = {
    trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
    revision: 1,
    session: {
      workspace_id: "w",
      harness: "codex",
      session_id: "s",
      machine_id: "m",
    },
    raw: { state: "closed", updated_at: new Date().toISOString() },
  };
  const handler = refereeHandler({
    db: db as never,
    reader: { read: async () => ({ manifest }) } as never,
  });
  const info = () => {};
  await handler(
    { payload: { manifestKey: "key/m" }, ctx: { orgId: "o" } } as never,
    { log: { info, warn: info, error: info } } as never,
  );
  expect(await state()).toBe("done");
}, 30_000);
