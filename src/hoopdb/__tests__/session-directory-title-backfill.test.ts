/**
 * The P3b task-title backfill (src/hoopdb/session-directory-title-backfill.ts): Neon
 * rollups collapse to the newest title per session, the directory is filled only where
 * task_title is NULL on a live, not-yet-event-projected row, `title` (projector-owned)
 * is never touched, plan writes nothing, Neon is read only, and a rerun is a no-op.
 * Supabase side: in-process PGlite with every supabase migration. Neon side: PGlite
 * with relayhistory's session_rollups shape (0030).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { sessionReadOnly } from "../../migration/neon-s3-sync/neon-reader.ts";
import { layoutComponent } from "../../shared/layout-component.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import { PGLITE_TEST_TIMEOUT } from "../../session-pipeline/__tests__/replay-fixture.ts";
import {
  backfillDirectoryTitles,
  directoryTitle,
  type Queryable,
} from "../session-directory-title-backfill.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
}, PGLITE_TEST_TIMEOUT);

async function supabase(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"])
    await db.exec(read(`supabase/bootstrap/${f}.sql`));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort())
    await db.exec(read(`supabase/migrations/${file}`));
  return db;
}

async function neon(): Promise<PGlite> {
  const db = new PGlite();
  open.push(db);
  await db.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.session_rollups (
      org_id text NOT NULL, session_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL,
      project_id text, first_ts timestamptz NOT NULL, last_ts timestamptz NOT NULL,
      task_title text, task_title_ts timestamptz,
      CONSTRAINT session_rollups_key UNIQUE NULLS NOT DISTINCT (org_id, session_id, workspace_id, source, project_id))`);
  return db;
}

type Rollup = { org?: string; ws?: string; source?: string; id: string; project?: string | null; title?: string | null; titleTs?: string | null };
async function rollups(db: PGlite, rows: Rollup[]) {
  for (const r of rows)
    await db.query(
      `INSERT INTO sessions.session_rollups (org_id, session_id, workspace_id, source, project_id, first_ts, last_ts, task_title, task_title_ts)
       VALUES ($1, $2, $3, $4, $5, '2026-10-01T00:00:00Z', '2026-10-02T00:00:00Z', $6, $7)`,
      [r.org ?? "org-a", r.id, r.ws ?? "ws-a", r.source ?? "claude", r.project ?? null, r.title ?? null, r.titleTs ?? null],
    );
}

type Dir = { org?: string; ws?: string; harness?: string; id: string; title?: string | null; taskTitle?: string | null; deleted?: boolean; eventVersion?: number };
async function directory(db: PGlite, rows: Dir[]) {
  for (const d of rows)
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, task_title, deleted_at)
       VALUES ($1, $2, $3, $4, '2026-10-01T00:00:00Z', '2026-10-02T00:00:00Z', $5, $6, CASE WHEN $7 THEN now() END)`,
      [d.org ?? "org-a", d.ws ?? "ws-a", d.harness ?? "claude", d.id, d.title ?? null, d.taskTitle ?? null, d.deleted ?? false],
    );
}

async function row(db: PGlite, id: string, harness = "claude") {
  return (
    await db.query<{ title: string | null; task_title: string | null; search_text: string; deleted_at: string | null }>(
      "SELECT title, task_title, search_text, deleted_at FROM sessions.session_directory WHERE session_id = $1 AND harness = $2",
      [id, harness],
    )
  ).rows[0];
}

/** A Neon client that records every statement, after the CLI's read-only session setup. */
async function readOnlyNeon(db: PGlite): Promise<{ client: Queryable; statements: string[] }> {
  const statements: string[] = [];
  const client: Queryable = {
    async query<T>(sql: string, params?: unknown[]) {
      statements.push(sql);
      // pg's simple protocol takes several statements when there are no params; PGlite's exec does.
      if (!params?.length && sql.includes(";")) {
        const results = await db.exec(sql);
        return { rows: (results.at(-1)?.rows ?? []) as T[] };
      }
      return (await db.query<T>(sql, params)) as { rows: T[] };
    },
  };
  await sessionReadOnly(client);
  return { client, statements };
}

const quiet = { sleep: async () => {}, pauseMs: 0 };

describe("session directory task-title backfill", () => {
  test("collapses a session's rollup rows to the newest title, fills NULL task_title, never touches title", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, [
      { id: "s1", project: "p1", title: "old", titleTs: "2026-10-01T00:00:00Z" },
      { id: "s1", project: "p2", title: "newest", titleTs: "2026-10-03T00:00:00Z" },
      { id: "s1", project: "p3", title: null, titleTs: null },
      { id: "s2", title: "   ", titleTs: "2026-10-01T00:00:00Z" },
      { id: "s3", title: "x".repeat(201), titleTs: "2026-10-01T00:00:00Z" },
    ]);
    await directory(s, [{ id: "s1", title: "brief title" }, { id: "s2" }, { id: "s3" }]);
    const { client } = await readOnlyNeon(n);
    const result = await backfillDirectoryTitles(client, s, { apply: true, ...quiet });
    expect(result).toMatchObject({ mode: "apply", sessions: 3, filled: 1, skipped: { empty: 1, too_long: 1 } });
    const s1 = await row(s, "s1");
    expect(s1?.task_title).toBe("newest");
    expect(s1?.title).toBe("brief title");
    expect(s1?.search_text).toContain("newest");
    expect((await row(s, "s2"))?.task_title).toBeNull();
    expect((await row(s, "s3"))?.task_title).toBeNull();
  }, PGLITE_TEST_TIMEOUT);

  test("equal timestamps pick the greater title, as the rollup does", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, [
      { id: "s1", project: "p1", title: "alpha", titleTs: "2026-10-03T00:00:00Z" },
      { id: "s1", project: "p2", title: "beta", titleTs: "2026-10-03T00:00:00Z" },
    ]);
    await directory(s, [{ id: "s1" }]);
    await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, ...quiet });
    expect((await row(s, "s1"))?.task_title).toBe("beta");
  }, PGLITE_TEST_TIMEOUT);

  test("never overwrites, skips tombstones and event-projected rows, and keeps tenants apart", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, [
      { id: "kept", title: "neon", titleTs: "2026-10-03T00:00:00Z" },
      { id: "gone", title: "neon", titleTs: "2026-10-03T00:00:00Z" },
      { id: "evented", title: "neon", titleTs: "2026-10-03T00:00:00Z" },
      { id: "other", org: "org-b", title: "neon", titleTs: "2026-10-03T00:00:00Z" },
    ]);
    await directory(s, [
      { id: "kept", taskTitle: "already" },
      { id: "gone", deleted: true },
      { id: "evented" },
      { id: "other", org: "org-a" },
    ]);
    await s.exec("UPDATE sessions.session_directory SET event_projection_version = 1 WHERE session_id = 'evented'");
    const result = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, ...quiet });
    expect(result.filled).toBe(0);
    expect((await row(s, "kept"))?.task_title).toBe("already");
    expect((await row(s, "gone"))?.task_title).toBeNull();
    expect((await row(s, "evented"))?.task_title).toBeNull();
    expect((await row(s, "other"))?.task_title).toBeNull();
  }, PGLITE_TEST_TIMEOUT);

  test("matches the directory's layout-encoded keys and the legacy physical session id", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    const rawId = "pi:abc/def";
    await rollups(n, [
      { id: rawId, ws: "ws a", source: "claude", title: "canonical", titleTs: "2026-10-03T00:00:00Z" },
      { id: "legacy/id", title: "legacy", titleTs: "2026-10-03T00:00:00Z" },
    ]);
    await directory(s, [
      { id: rawId, ws: layoutComponent("ws a") },
      { id: layoutComponent("legacy/id") },
    ]);
    const result = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, ...quiet });
    expect(result.filled).toBe(2);
    expect((await row(s, rawId))?.task_title).toBe("canonical");
    expect((await row(s, layoutComponent("legacy/id")))?.task_title).toBe("legacy");
  }, PGLITE_TEST_TIMEOUT);

  test("plan writes nothing and reports what apply then fills; a rerun is a no-op", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, Array.from({ length: 7 }, (_, i) => ({ id: `s${i}`, title: `t${i}`, titleTs: "2026-10-03T00:00:00Z" })));
    await directory(s, Array.from({ length: 7 }, (_, i) => ({ id: `s${i}` })));
    const before = (await s.query("SELECT task_title, search_text FROM sessions.session_directory ORDER BY session_id")).rows;
    const plan = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: false, pageSize: 3, ...quiet });
    expect(plan).toMatchObject({ mode: "plan", pages: 3, sessions: 7, filled: 7 });
    expect((await s.query("SELECT task_title, search_text FROM sessions.session_directory ORDER BY session_id")).rows).toEqual(before);
    const apply = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, pageSize: 3, ...quiet });
    expect(apply.filled).toBe(7);
    const rerun = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, pageSize: 3, ...quiet });
    expect(rerun.filled).toBe(0);
    const replan = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: false, ...quiet });
    expect(replan.filled).toBe(0);
  }, PGLITE_TEST_TIMEOUT);

  test("plan runs the Supabase side in a READ ONLY transaction", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, [{ id: "s1", title: "t", titleTs: "2026-10-03T00:00:00Z" }]);
    await directory(s, [{ id: "s1" }]);
    const seen: string[] = [];
    const spy: Queryable = { async query<T>(sql: string, params?: unknown[]) { seen.push(sql.trim().split(/\s+/).slice(0, 3).join(" ")); return (await s.query<T>(sql, params)) as { rows: T[] }; } };
    await backfillDirectoryTitles((await readOnlyNeon(n)).client, spy, { apply: false, ...quiet });
    expect(seen[0]).toBe("BEGIN READ ONLY");
    expect(seen.some((q) => q.startsWith("UPDATE"))).toBe(false);
  }, PGLITE_TEST_TIMEOUT);

  test("Neon is read only: every page is a READ ONLY transaction in a read-only session, and nothing writes", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, [{ id: "s1", title: "t", titleTs: "2026-10-03T00:00:00Z" }]);
    await directory(s, [{ id: "s1" }]);
    const { client, statements } = await readOnlyNeon(n);
    await backfillDirectoryTitles(client, s, { apply: true, ...quiet });
    const work = statements.slice(2);
    expect(work.filter((q) => q.startsWith("BEGIN"))).toEqual(work.filter((q) => q === "BEGIN READ ONLY"));
    expect(work.some((q) => /^\s*(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)/i.test(q))).toBe(false);
    // The session the CLI sets up refuses writes outright.
    await expect(client.query("INSERT INTO sessions.session_rollups (org_id, session_id, workspace_id, source, first_ts, last_ts) VALUES ('o','s','w','x',now(),now())"))
      .rejects.toMatchObject({ code: "25006" });
  }, PGLITE_TEST_TIMEOUT);

  test("a later pointer projection keeps the backfilled task_title and owns title", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, [{ id: "s1", title: "from neon", titleTs: "2026-10-03T00:00:00Z" }]);
    const pointer = (title: string, revision: number) =>
      s.query(
        `INSERT INTO sessions.session_index
           (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
            title, state, event_count, input_tokens, storage_provider, bucket, manifest_key, trace_id,
            revision, indexed_at)
         VALUES ('org-a', 'ws-a', 'claude', 's1', 'm1', 'u1', '2026-10-01T00:00:00Z', '2026-10-02T00:00:00Z',
                 $1, 'unknown', 1, 1, 's3', 'bucket', 'key/m1', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', $2, now())
         ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
           title = excluded.title, revision = excluded.revision`,
        [title, revision],
      );
    const key = { orgId: "org-a", workspaceId: "ws-a", harness: "claude", sessionId: "s1" };
    await pointer("pointer one", 1);
    await projectSessionDirectory(s, key);
    await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, ...quiet });
    expect(await row(s, "s1")).toMatchObject({ title: "pointer one", task_title: "from neon" });
    await pointer("pointer two", 2);
    await projectSessionDirectory(s, key);
    const after = await row(s, "s1");
    expect(after).toMatchObject({ title: "pointer two", task_title: "from neon" });
    expect(after?.search_text).toContain("from neon");
  }, PGLITE_TEST_TIMEOUT);

  test("--after-org resumes strictly after an org; maxPages bounds a canary run", async () => {
    const [n, s] = await Promise.all([neon(), supabase()]);
    await rollups(n, ["org-a", "org-b", "org-c"].map((org) => ({ org, id: "s1", title: org, titleTs: "2026-10-03T00:00:00Z" })));
    await directory(s, ["org-a", "org-b", "org-c"].map((org) => ({ org, id: "s1" })));
    const canary = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, pageSize: 1, maxPages: 1, ...quiet });
    expect(canary).toMatchObject({ pages: 1, filled: 1, lastOrg: "org-a" });
    const resumed = await backfillDirectoryTitles((await readOnlyNeon(n)).client, s, { apply: true, afterOrg: "org-b", ...quiet });
    expect(resumed).toMatchObject({ sessions: 1, filled: 1, lastOrg: "org-c" });
    const titles = (await s.query<{ task_title: string | null }>("SELECT task_title FROM sessions.session_directory ORDER BY org_id")).rows.map((r) => r.task_title);
    expect(titles).toEqual(["org-a", null, "org-c"]);
  }, PGLITE_TEST_TIMEOUT);

  test("directoryTitle trims, skips empty, and refuses (never truncates) over 200 characters", () => {
    expect(directoryTitle("  a  ")).toEqual({ title: "a" });
    expect(directoryTitle(null)).toEqual({ skip: "empty" });
    expect(directoryTitle("é".repeat(200))).toEqual({ title: "é".repeat(200) });
    expect(directoryTitle("é".repeat(201))).toEqual({ skip: "too_long" });
  }, PGLITE_TEST_TIMEOUT);
});
