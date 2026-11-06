/**
 * 20261102000000_session_briefs: tables, deletion triggers, RLS, and the Worker brief RPC
 * (role guard, signed-claims scope, cross-tenant isolation, status derivation). PGlite
 * with every supabase migration and the 96bulls ones, as src/hoopdb/migrate.ts orders them.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { SESSION_BRIEF_RPC } from "../../hoopdb/search-index-contract.ts";
import { eraseSessionDirectory } from "../../hoopdb/session-directory-summary.ts";
import { KEY, WORKER, asRole, briefRpc, claims, code, database, hoopDb, pointer, read } from "./harness.ts";


// Each test builds a PGlite database with the full migration plan (a few seconds).
setDefaultTimeout(60_000);

const UP = read("supabase/migrations/20261102000000_session_briefs.sql");
const DOWN = read("supabase/rollbacks/20261102000000_session_briefs.down.sql");
const STATE_UP = read("supabase/migrations/20261102020000_session_brief_state.sql");
const STATE_DOWN = read("supabase/rollbacks/20261102020000_session_brief_state.down.sql");
const VERIFY = read("supabase/bootstrap/session_directory_search_worker_verify.sql");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
async function db() {
  const pg = await database();
  open.push(pg);
  return pg;
}

/** A ready brief for KEY covering machine m-1 at `revision`. */
async function readyBrief(pg: PGlite, revision = 1, key = KEY) {
  await pg.query(
    `INSERT INTO sessions.session_briefs
       (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
        source_fingerprint, title, description, brief_revision, brief_source_revisions,
        brief_fingerprint, scrub_ruleset, scrub_version, prompt_version, engine_version, model,
        message_count, shortened_messages, input_tokens, output_tokens, generated_at)
     VALUES ($1, $2, $3, $4, 'ready', $5, jsonb_build_object('m-1', $5::bigint), repeat('a', 64),
             'Brief title', 'Brief description.', $5, jsonb_build_object('m-1', $5::bigint),
             repeat('a', 64), 'relayhistory-scrub', 1, 'session-summary-v2',
             'session-summary-v2:96bulls-v1', 'gpt-5.6-luna', 3, 0, 10, 5, now())`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId, revision],
  );
}

describe("session_briefs schema", () => {
  test("applies after the full chain, passes the Worker verify allowlist, and survives down then up", async () => {
    const pg = await db();
    await pg.exec(VERIFY);
    await pg.exec(STATE_DOWN);
    await pg.exec(VERIFY);
    await pg.exec(DOWN);
    expect((await pg.query("SELECT to_regclass('sessions.session_briefs') AS t")).rows[0]).toEqual({ t: null });
    await pg.exec(UP);
    await pg.exec(STATE_UP);
    await pg.exec(VERIFY);
  });

  test("the state down path keeps v2 briefs and the Worker RPC, and the up restores state", async () => {
    const pg = await db();
    await readyBrief(pg);
    await pg.exec(STATE_DOWN);
    expect((await pg.query("SELECT title FROM sessions.session_briefs")).rows).toEqual([{ title: "Brief title" }]);
    const without = (await pg.query("SELECT proretset FROM pg_proc WHERE oid = 'sessions.session_brief(text, text, text)'::regprocedure")).rows;
    expect(without).toHaveLength(1);
    await pg.exec(STATE_UP);
    await pg.query("UPDATE sessions.session_briefs SET state = 'answered', prompt_version = 'session-summary-v3'");
    expect((await pg.query("SELECT state FROM sessions.session_briefs")).rows).toEqual([{ state: "answered" }]);
  });

  test("the state down path removes already-generated v3 briefs so the up can reapply", async () => {
    const pg = await db();
    await pg.query(
      `INSERT INTO sessions.session_briefs
         (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
          source_fingerprint, title, description, state, brief_revision, brief_source_revisions,
          brief_fingerprint, scrub_ruleset, scrub_version, prompt_version, engine_version, model,
          message_count, shortened_messages, input_tokens, output_tokens, generated_at)
       VALUES ($1, $2, $3, $4, 'ready', 1, jsonb_build_object('m-1', 1), repeat('a', 64),
               'V3 brief title', 'Two sentences. Second one.', 'completed', 1, jsonb_build_object('m-1', 1),
               repeat('a', 64), 'relayhistory-scrub', 1, 'session-summary-v3',
               'session-summary-v3:96bulls-v1', 'gpt-5.6-luna', 3, 0, 10, 5, now())`,
      [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId],
    );
    await pg.exec(STATE_DOWN);
    expect((await pg.query("SELECT count(*)::int AS n FROM sessions.session_briefs")).rows).toEqual([{ n: 0 }]);
    await pg.exec(STATE_UP);
  });

  test("a state is one of the five, describes a brief, and is required after the v2 prompt", async () => {
    const pg = await db();
    await readyBrief(pg);
    const set = (sql: string) => code(pg.query(`UPDATE sessions.session_briefs SET ${sql}`));
    expect(await set(`state = 'done'`)).toBe("23514");
    expect(await set(`prompt_version = 'session-summary-v3'`)).toBe("23514");
    expect(await set(`state = 'blocked', prompt_version = 'session-summary-v3'`)).toBeUndefined();
    // Clearing the text must clear the state too: the same clear passes once state is NULL.
    const clear = `title = NULL, description = NULL, brief_revision = NULL, brief_source_revisions = NULL,
                   brief_fingerprint = NULL, scrub_ruleset = NULL, scrub_version = NULL,
                   prompt_version = NULL, engine_version = NULL, model = NULL, generated_at = NULL,
                   status = 'quarantined', error = 'session_quarantined'`;
    expect(await set(clear)).toBe("23514");
    expect(await set(`${clear}, state = NULL`)).toBeUndefined();
  });

  test("the bounds of the brief contract are enforced by the database too", async () => {
    const pg = await db();
    await readyBrief(pg);
    const set = (sql: string) =>
      code(pg.query(`UPDATE sessions.session_briefs SET ${sql}`));
    expect(await set(`title = repeat('t', 91)`)).toBe("23514");
    expect(await set(`description = repeat('d', 321)`)).toBe("23514");
    expect(await set(`title = E'two\\nlines'`)).toBe("23514");
    expect(await set(`error = 'Free text from a provider'`)).toBe("23514");
    // All-or-nothing provenance: text without its metadata, or ready without text.
    expect(await set(`model = NULL`)).toBe("23514");
    expect(
      await set(`title = NULL, description = NULL, brief_revision = NULL, brief_source_revisions = NULL,
                 brief_fingerprint = NULL, scrub_ruleset = NULL, scrub_version = NULL,
                 prompt_version = NULL, engine_version = NULL, model = NULL, generated_at = NULL`),
    ).toBe("23514");
  });

  test("no direct table access for anon or authenticated, or for the Worker role", async () => {
    const pg = await db();
    await readyBrief(pg);
    for (const role of ["anon", "authenticated", WORKER]) {
      for (const table of ["session_briefs", "session_brief_wakeups", "session_brief_attempts"]) {
        expect(
          await code(asRole(pg, role, claims("org-a", ["ws-a"]), () => pg.query(`SELECT * FROM sessions.${table}`))),
          `${role} ${table}`,
        ).toBe("42501");
      }
    }
  });
});

describe("deletion removes the brief and its wakeup", () => {
  async function seeded() {
    const pg = await db();
    await pointer(pg, { machine: "m-1" });
    await pointer(pg, { machine: "m-2" });
    await readyBrief(pg);
    await pg.query(
      `INSERT INTO sessions.session_brief_wakeups (org_id, workspace_id, harness, session_id, admission, due_at)
       VALUES ($1, $2, $3, $4, 'background', now())`,
      [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId],
    );
    return pg;
  }
  const count = async (pg: PGlite) =>
    (
      await pg.query<{ b: number; w: number }>(
        `SELECT (SELECT count(*) FROM sessions.session_briefs)::int AS b,
                (SELECT count(*) FROM sessions.session_brief_wakeups)::int AS w`,
      )
    ).rows[0];

  test("one machine's pointer tombstoned (the brief may describe its content)", async () => {
    const pg = await seeded();
    await pg.query("UPDATE sessions.session_index SET indexed_at = now() WHERE machine_id = 'm-1'");
    expect(await count(pg)).toEqual({ b: 1, w: 1 });
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE machine_id = 'm-1'");
    expect(await count(pg)).toEqual({ b: 0, w: 0 });
  });

  test("a pointer row deleted", async () => {
    const pg = await seeded();
    await pg.query("DELETE FROM sessions.session_index WHERE machine_id = 'm-2'");
    expect(await count(pg)).toEqual({ b: 0, w: 0 });
  });

  test("the directory row erased", async () => {
    const pg = await seeded();
    expect(await eraseSessionDirectory(hoopDb(pg), KEY)).toBe(1);
    expect(await count(pg)).toEqual({ b: 0, w: 0 });
  });

  test("another session's brief is untouched", async () => {
    const pg = await seeded();
    const other = { ...KEY, sessionId: "s-2" };
    await pointer(pg, { key: other });
    await readyBrief(pg, 1, other);
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 's-1'");
    expect(
      (await pg.query("SELECT session_id FROM sessions.session_briefs")).rows,
    ).toEqual([{ session_id: "s-2" }]);
  });
});

describe("sessions.session_brief (Worker RPC)", () => {
  test("returns exactly the contract columns; ready when the brief covers every live pointer", async () => {
    const pg = await db();
    await pointer(pg, { revision: 1 });
    await readyBrief(pg, 1);
    const rows = await briefRpc(pg);
    expect(rows).toHaveLength(1);
    expect(Object.keys(rows[0]!)).toEqual([...SESSION_BRIEF_RPC.returns]);
    expect(rows[0]).toMatchObject({
      status: "ready",
      stale: false,
      title: "Brief title",
      description: "Brief description.",
      model: "gpt-5.6-luna",
      error: null,
    });
    expect(Number(rows[0]!.revision)).toBe(1);
    expect(Number(rows[0]!.current_revision)).toBe(1);
  });

  test("status follows the brief, the wakeup and the live pointers", async () => {
    const pg = await db();
    await pointer(pg, { revision: 1 });
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "missing", stale: false, title: null });
    const wake = () =>
      pg.query(
        `INSERT INTO sessions.session_brief_wakeups (org_id, workspace_id, harness, session_id, admission, due_at)
         VALUES ('org-a', 'ws-a', 'claude', 's-1', 'background', now()) ON CONFLICT DO NOTHING`,
      );
    await wake();
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "pending", stale: false });
    await readyBrief(pg, 1);
    await pointer(pg, { revision: 2 });
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "refreshing", stale: true, title: "Brief title" });
    await pg.exec("DELETE FROM sessions.session_brief_wakeups");
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "stale", stale: true });
    await pg.exec("UPDATE sessions.session_briefs SET status = 'rate_limited', error = 'session_brief_rate_limited'");
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "rate_limited", error: "session_brief_rate_limited" });
    await pg.exec("UPDATE sessions.session_briefs SET status = 'failed', error = 'model_output_invalid'");
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "failed", stale: true, error: "model_output_invalid" });
    // A second machine joining makes the brief stale even at the same revision.
    await pg.exec("UPDATE sessions.session_briefs SET status = 'ready', error = NULL, brief_source_revisions = '{\"m-1\": 2}'");
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "ready" });
    await pointer(pg, { machine: "m-2", revision: 2 });
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "stale" });
  });

  test("no live pointer: no row", async () => {
    const pg = await db();
    await pointer(pg);
    await readyBrief(pg);
    await pg.exec("UPDATE sessions.session_index SET deleted_at = now()");
    expect(await briefRpc(pg)).toEqual([]);
  });

  test("cross-tenant: another org or an unclaimed workspace sees nothing or is refused", async () => {
    const pg = await db();
    await pointer(pg);
    await readyBrief(pg);
    // Same ids, another org's claims: the org comes only from the claims.
    expect(await briefRpc(pg, KEY, claims("org-b", ["ws-a"]))).toEqual([]);
    // A workspace the claims do not cover.
    expect(await code(briefRpc(pg, KEY, claims("org-a", ["ws-other"])))).toBe("42501");
  });

  test("role guard and token pins: refused for every other caller and every bad token", async () => {
    const pg = await db();
    await pointer(pg);
    await readyBrief(pg);
    for (const role of ["anon", "authenticated", "service_role"])
      expect(await code(briefRpc(pg, KEY, claims("org-a", ["ws-a"]), role)), role).toBe("42501");
    const iat = Math.floor(Date.now() / 1000);
    for (const bad of [
      claims("org-a", ["ws-a"], { role: "authenticated" }),
      claims("org-a", ["ws-a"], { iss: "https://other.test" }),
      claims("org-a", ["ws-a"], { aud: "relayhistory" }),
      claims("org-a", ["ws-a"], { iat, exp: iat + 3600 }),
      null,
    ])
      expect(await code(briefRpc(pg, KEY, bad as string)), String(bad)).toBe("42501");
    await pg.exec("DELETE FROM sessions.session_directory_search_token_policy");
    expect(await code(briefRpc(pg))).toBe("42501");
  });

  test("invalid arguments are 22023", async () => {
    const pg = await db();
    await pointer(pg);
    const call = (ws: string | null, harness: string | null, session: string | null) =>
      asRole(pg, WORKER, claims("org-a", ["ws-a"]), () =>
        pg.query("SELECT * FROM sessions.session_brief($1, $2, $3)", [ws, harness, session]),
      );
    expect(await code(call(null, "claude", "s-1"))).toBe("22023");
    expect(await code(call("ws-a", "Bad Harness", "s-1"))).toBe("22023");
    expect(await code(call("ws-a", "claude", ""))).toBe("22023");
  });
});
