import { beforeAll, afterAll, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  directoryEventsFromProjection,
  writeDirectoryEvents,
} from "../session-directory-events.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";
const root = join(import.meta.dir, "../../..");
const key = {
  orgId: "org-a",
  workspaceId: "ws-a",
  harness: "codex",
  sessionId: "s1",
};
const row = (overrides: Record<string, unknown> = {}) => ({
  machine_id: "m1",
  event_id: "e1",
  source: "codex",
  session_id: "s1",
  kind: "assistant",
  ts: "2026-10-01T00:00:00.000001Z",
  input_tokens: 1,
  output_tokens: 2,
  reasoning_tokens: 3,
  cache_read_tokens: 4,
  cache_create_tokens: 5,
  cache_create_5m_tokens: 5,
  cache_create_1h_tokens: 0,
  cost_usd_micros: 6,
  task_ref: { id: "pr:1", url: "https://github.test/pull/1" },
  task_title: null,
  task_status: null,
  ...overrides,
});
describe("canonical directory event aggregates", () => {
  test("deduplicates full event identities and preserves exact counters/nullability", () => {
    const a = row({ reasoning_tokens: "9007199254740993" });
    const result = directoryEventsFromProjection([
      a,
      a,
      row({
        event_id: "e2",
        cache_create_tokens: 9,
        cache_create_5m_tokens: null,
        cache_create_1h_tokens: null,
        cost_usd_micros: null,
      }),
    ]);
    expect(result).toMatchObject({
      eventCount: "2",
      inputTokens: "2",
      outputTokens: "4",
      reasoningTokens: "9007199254740996",
      cacheReadTokens: "8",
      cacheCreateTokens: "14",
      cacheCreate5mTokens: null,
      cacheCreate1hTokens: null,
      costUsdMicros: "6",
      kinds: ["assistant"],
      taskRefs: [a.task_ref],
    });
  });
  test("uses newest work signal by exact timestamp/event id and ignores generic presence status", () => {
    const result = directoryEventsFromProjection([
      row(),
      row({
        event_id: "e2",
        ts: "2026-10-01T00:00:00.000002Z",
        kind: "task",
        task_status: "completed",
      }),
      row({
        event_id: "e3",
        ts: "2026-10-02T00:00:00.000000Z",
        kind: "presence",
        task_status: "running",
      }),
    ]);
    expect(result.workStatus).toBe("finished");
    expect(result.workStatusAt).toBe("2026-10-01T00:00:00.000002Z");
  });
  test("opening locator follows earliest qualifying event without storing its text", () => {
    const fp = "s3" + "a".repeat(14);
    const result = directoryEventsFromProjection([
      row({
        event_id: "later",
        kind: "history",
        content: "LATER PROMPT",
        ts: "2026-10-02T00:00:00.000001Z",
        _index_fingerprint: fp,
      }),
      row({
        session_id: "s1",
        event_id: "first",
        kind: "session_event",
        actor_role: "user",
        content: "FIRST PROMPT",
        _index_fingerprint: fp,
      }),
      row({
        event_id: "blank",
        kind: "prompt",
        content: "   ",
        ts: "2026-09-30T00:00:00.000001Z",
        _index_fingerprint: fp,
      }),
    ]);
    expect(result.openingEvent).toEqual({
      session_id: "s1",
      event_id: "first",
      machine_id: "m1",
      kind: "session_event",
      source: "codex",
      ts: "2026-10-01T00:00:00.000001Z",
      fingerprint: fp,
    });
    expect(JSON.stringify(result)).not.toContain("FIRST PROMPT");
    expect(JSON.stringify(result)).not.toContain("LATER PROMPT");
    expect(directoryEventsFromProjection([row()]).openingEvent).toBeNull();
  });
  test("same-timestamp work state uses the source PostgreSQL C event-id order", () => {
    const result = directoryEventsFromProjection([
      row({ event_id: "_", kind: "task", task_status: "completed" }),
      row({ event_id: "Z", kind: "task", task_status: "paused" }),
    ]);
    expect(result.workStatus).toBe("finished");
  });
  test("zero-cache events preserve complete zero splits and null cost", () => {
    expect(
      directoryEventsFromProjection([
        row({
          cache_create_tokens: 0,
          cache_create_5m_tokens: null,
          cache_create_1h_tokens: null,
          cost_usd_micros: null,
        }),
      ]),
    ).toMatchObject({
      cacheCreate5mTokens: "0",
      cacheCreate1hTokens: "0",
      costUsdMicros: null,
    });
  });
  test("refuses conflicting duplicates, unsafe numbers and bounded-metadata overflow", () => {
    expect(() =>
      directoryEventsFromProjection([row(), row({ input_tokens: 99 })]),
    ).toThrow();
    for (const value of [
      -1,
      Number.MAX_SAFE_INTEGER + 1,
      "1.5",
      "9223372036854775808",
    ])
      expect(() =>
        directoryEventsFromProjection([row({ reasoning_tokens: value })]),
      ).toThrow();
    expect(() =>
      directoryEventsFromProjection([
        row({ task_ref: { id: "x".repeat(100000) } }),
      ]),
    ).toThrow();
  });
});
describe("directory event writer fencing", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    const read = (p: string) => readFileSync(join(root, p), "utf8");
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    await db.exec(
      read("supabase/bootstrap/session_directory_search_worker.sql"),
    );
    await db.exec(
      read("supabase/bootstrap/session_token_verifier.sql"),
    );
    await db.exec(
      read("supabase/bootstrap/session_ingest_roles.sql"),
    );
    for (const f of readdirSync(join(root, "supabase/migrations")).sort())
      await db.exec(read(`supabase/migrations/${f}`));
  }, 120000);
  afterAll(async () => {
    await db?.close();
  });
  async function seed() {
    await db.exec(
      "DELETE FROM sessions.session_directory; DELETE FROM sessions.session_index; DELETE FROM sessions.session_directory_event_pending;",
    );
    await db.query(
      `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,raw_format,trace_id,revision,event_count) VALUES('org-a','ws-a','codex','s1','m1','u1','2026-10-01','2026-10-02','s3','b','key1','ndjson','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',1,99)`,
    );
    await projectSessionDirectory(db as never, key);
  }
  const sources = [{ manifestKey: "key1", revision: "1" }];
  test("writes complete source aggregates; pointer refresh preserves them and invalidates readiness", async () => {
    await seed();
    const events = directoryEventsFromProjection([row()]);
    expect(
      await writeDirectoryEvents(db as never, key, {
        sourceSessionId: "s1",
        expectedEpoch: "0",
        sources,
        events,
      }),
    ).toEqual({ written: true });
    await projectSessionDirectory(db as never, key);
    expect(
      (
        await db.query(
          "SELECT event_count,event_projection_ready,event_projection_version FROM sessions.session_directory",
        )
      ).rows[0],
    ).toMatchObject({
      event_count: 1,
      event_projection_ready: true,
      event_projection_version: 1,
    });
    await db.exec(
      "UPDATE sessions.session_index SET revision=2,event_count=400 WHERE session_id='s1'",
    );
    await projectSessionDirectory(db as never, key);
    expect(
      (
        await db.query(
          "SELECT event_count,event_projection_ready FROM sessions.session_directory",
        )
      ).rows[0],
    ).toMatchObject({ event_count: 1, event_projection_ready: false });
    expect(
      await writeDirectoryEvents(db as never, key, {
        sourceSessionId: "s1",
        expectedEpoch: "0",
        sources,
        events,
      }),
    ).toEqual({ written: false });
  });
  test("mutation epoch rejects a writer even if a stale statement sees the old source set", async () => {
    await seed();
    await db.exec(
      "UPDATE sessions.session_directory SET event_projection_epoch=1,event_projection_ready=false",
    );
    expect(
      await writeDirectoryEvents(db as never, key, {
        sourceSessionId: "s1",
        expectedEpoch: "0",
        sources,
        events: directoryEventsFromProjection([row()]),
      }),
    ).toEqual({ written: false });
    expect(
      await writeDirectoryEvents(db as never, key, {
        sourceSessionId: "s1",
        expectedEpoch: "1",
        sources,
        events: directoryEventsFromProjection([row()]),
      }),
    ).toEqual({ written: true });
  });
  test("state-only pointer classification preserves readiness and event-derived work state", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([
        row({ task_status: "completed" }),
      ]),
    });
    await db.exec(
      "UPDATE sessions.session_index SET state='active',state_classified_at=now() WHERE session_id='s1'",
    );
    await projectSessionDirectory(db as never, key);
    expect(
      (
        await db.query(
          "SELECT event_projection_ready,work_status FROM sessions.session_directory",
        )
      ).rows[0],
    ).toMatchObject({ event_projection_ready: true, work_status: "finished" });
  });
  test("the tenant-bound list RPC returns new event metadata and readiness", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([row()]),
    });
    await db.query(
      "INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES ('https://issuer.test','session-directory-search') ON CONFLICT DO NOTHING",
    );
    const iat = Math.floor(Date.now() / 1000);
    await db.query("SELECT set_config('request.jwt.claims',$1,false)", [
      JSON.stringify({
        role: "session_directory_search_worker",
        iss: "https://issuer.test",
        aud: "session-directory-search",
        iat,
        exp: iat + 55,
        app_metadata: { org_id: "org-a", workspace_ids: ["ws-a"] },
      }),
    ]);
    await db.exec("SET ROLE session_directory_search_worker");
    try {
      const result = await db.query(
        "SELECT * FROM sessions.list_session_directory_events('ws-a')",
      );
      expect(result.rows[0]).toMatchObject({
        kinds: ["assistant"],
        reasoning_tokens: 3,
        cache_read_tokens: 4,
        event_projection_version: 1,
        event_projection_ready: true,
        work_status: "active",
      });
    } finally {
      await db.exec("RESET ROLE");
    }
  });
  async function asWorker<T>(work: () => Promise<T>): Promise<T> {
    await db.query(
      "INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES ('https://issuer.test','session-directory-search') ON CONFLICT DO NOTHING",
    );
    const iat = Math.floor(Date.now() / 1000);
    await db.query("SELECT set_config('request.jwt.claims',$1,false)", [
      JSON.stringify({
        role: "session_directory_search_worker",
        iss: "https://issuer.test",
        aud: "session-directory-search",
        iat,
        exp: iat + 55,
        app_metadata: { org_id: "org-a", workspace_ids: ["ws-a"] },
      }),
    ]);
    await db.exec("SET ROLE session_directory_search_worker");
    try {
      return await work();
    } finally {
      await db.exec("RESET ROLE");
    }
  }
  test("strict list attests the entire workspace before a selective page while legacy RPC remains available", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([row()]),
    });
    await db.exec(
      "INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES('org-a','ws-a','claude','unready','2026-10-01','2026-10-01')",
    );
    await asWorker(async () => {
      await expect(
        db.query(
          "SELECT * FROM sessions.list_session_directory_events('ws-a',p_sources=>ARRAY['codex'],p_limit=>1)",
        ),
      ).rejects.toMatchObject({ code: "0A000" });
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.list_session_directory('ws-a',p_sources=>ARRAY['codex'],p_limit=>1)",
          )
        ).rows,
      ).toHaveLength(1);
    });
  });
  test("strict list independently rechecks current source set even if readiness was wrongly left true", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([row()]),
    });
    await db.exec(
      "UPDATE sessions.session_index SET revision=2; UPDATE sessions.session_directory SET event_projection_ready=true",
    );
    await asWorker(async () => {
      await expect(
        db.query(
          "SELECT * FROM sessions.list_session_directory_events('ws-a')",
        ),
      ).rejects.toMatchObject({ code: "0A000" });
    });
  });
  test("event activity counts sparse UTC days and inclusive microsecond boundary windows", async () => {
    await seed();
    const events = directoryEventsFromProjection([
      row({ ts: "2026-10-01T12:00:00.000001Z" }),
      row({ event_id: "e2", ts: "2026-10-03T00:00:00.000002Z" }),
      row({ event_id: "e3", ts: "2026-10-03T00:00:00.000002Z" }),
    ]);
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events,
    });
    expect(
      (
        await db.query(
          "SELECT count(*)::int AS n FROM sessions.session_directory_event_times",
        )
      ).rows[0],
    ).toEqual({ n: 2 });
    await asWorker(async () => {
      const rows = (
        await db.query(
          "SELECT day::text,harness,sessions FROM sessions.session_directory_event_activity('ws-a','2026-10-01','2026-10-04')",
        )
      ).rows;
      expect(rows).toEqual([
        { day: "2026-10-01", harness: "codex", sessions: 1 },
        { day: "2026-10-03", harness: "codex", sessions: 1 },
      ]);
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.session_directory_event_activity('ws-a','2026-10-03T00:00:00.000002Z','2026-10-03T00:00:00.000002Z')",
          )
        ).rows,
      ).toHaveLength(1);
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.session_directory_event_activity('ws-a','2026-10-03T00:00:00.000001Z','2026-10-03T00:00:00.000001Z')",
          )
        ).rows,
      ).toHaveLength(0);
    });
    await db.exec("UPDATE sessions.session_index SET revision=2");
    await asWorker(async () => {
      await expect(
        db.query(
          "SELECT * FROM sessions.session_directory_event_activity('ws-a','2026-10-01','2026-10-04')",
        ),
      ).rejects.toMatchObject({ code: "0A000" });
    });
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "1",
      sources: [{ manifestKey: "key1", revision: "2" }],
      events: directoryEventsFromProjection([
        row({ ts: "2026-10-02T12:00:00.000001Z" }),
      ]),
    });
    expect(
      (
        await db.query(
          "SELECT event_ts::text FROM sessions.session_directory_event_times",
        )
      ).rows,
    ).toEqual([{ event_ts: "2026-10-02 12:00:00.000001+00" }]);
  });
  test("strict public identities and user filter use canonical event origins, not pointer writers", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([
        row({
          machine_id: "delivery:origin",
          user_id: "event-user",
          model: "event-model",
          project_id: "recorded/project",
        }),
      ]),
    });
    await asWorker(async () => {
      const result = (
        await db.query(
          "SELECT * FROM sessions.list_session_directory_events('ws-a',p_user_ids=>ARRAY['event-user'])",
        )
      ).rows;
      expect(result[0]).toMatchObject({
        user_ids: ["event-user"],
        machine_ids: ["delivery:origin"],
        models: ["event-model"],
        project_id: "recorded/project",
      });
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.list_session_directory_events('ws-a',p_user_ids=>ARRAY['u1'])",
          )
        ).rows,
      ).toHaveLength(0);
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.list_session_directory('ws-a',p_user_ids=>ARRAY['u1'])",
          )
        ).rows,
      ).toHaveLength(1);
    });
  });
  test("path-backed project IDs stay out of PG and filtered reads refuse instead of dropping them", async () => {
    await seed();
    const fp = "s3" + "a".repeat(14);
    const events = directoryEventsFromProjection([
      row({ project_id: "/Users/private/repo", _index_fingerprint: fp }),
    ]);
    expect(events.projectId).toBeNull();
    expect(events.projectEvent).toMatchObject({
      session_id: "s1",
      event_id: "e1",
      fingerprint: fp,
    });
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events,
    });
    const stored = (
      await db.query(
        "SELECT event_project_id,event_project_event,project_requires_hydration FROM sessions.session_directory",
      )
    ).rows[0];
    expect(stored).toMatchObject({
      event_project_id: null,
      project_requires_hydration: true,
    });
    expect(JSON.stringify(stored)).not.toContain("/Users/private");
    await asWorker(async () => {
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.list_session_directory_events('ws-a')",
          )
        ).rows,
      ).toHaveLength(1);
      await expect(
        db.query(
          "SELECT * FROM sessions.list_session_directory_events('ws-a',p_project_contains=>'repo')",
        ),
      ).rejects.toMatchObject({ code: "0A000" });
    });
  });
  test("legacy directory target retains its key while locators retain the actual event session ID", async () => {
    await seed();
    await db.exec(
      "UPDATE sessions.session_index SET canonical_session_id='canonical'",
    );
    await projectSessionDirectory(db as never, key);
    const events = directoryEventsFromProjection([
      row({
        kind: "history",
        content: "PRIVATE PROMPT",
        _index_fingerprint: "s3" + "a".repeat(14),
      }),
    ]);
    expect(
      await writeDirectoryEvents(
        db as never,
        { ...key, sessionId: "canonical" },
        { sourceSessionId: "s1", expectedEpoch: "1", sources, events },
      ),
    ).toEqual({ written: true });
    await asWorker(async () => {
      const result = (
        await db.query(
          "SELECT session_id,opening_event FROM sessions.list_session_directory_events('ws-a')",
        )
      ).rows[0];
      expect(result).toMatchObject({
        session_id: "s1",
        opening_event: { session_id: "s1", event_id: "e1" },
      });
      expect(JSON.stringify(result)).not.toContain("PRIVATE PROMPT");
    });
  });
  test("a phantom pointer without a directory row blocks the strict workspace", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([row()]),
    });
    await db.exec(
      "INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,raw_format,trace_id,revision) SELECT org_id,workspace_id,harness,'phantom',machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,'phantom-key',raw_format,trace_id,revision FROM sessions.session_index WHERE session_id='s1'",
    );
    await asWorker(async () => {
      await expect(
        db.query(
          "SELECT * FROM sessions.list_session_directory_events('ws-a',p_limit=>1)",
        ),
      ).rejects.toMatchObject({ code: "0A000" });
    });
  });
  test("a complete canonical union fences every physical sibling rather than last-writer totals", async () => {
    await seed();
    await db.exec(
      "UPDATE sessions.session_index SET canonical_session_id='canonical'",
    );
    await db.exec(
      "INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,raw_format,trace_id,revision,canonical_session_id) SELECT org_id,workspace_id,harness,'alias-two','m2',user_id,created_at,last_activity_at,storage_provider,bucket,'key2',raw_format,trace_id,revision,canonical_session_id FROM sessions.session_index WHERE session_id='s1'",
    );
    const events = directoryEventsFromProjection([
      row(),
      row(),
      row({ event_id: "e2", machine_id: "m2" }),
    ]);
    expect(
      await writeDirectoryEvents(
        db as never,
        { ...key, sessionId: "canonical" },
        {
          sourceSessionId: "s1",
          expectedEpoch: "1",
          sources: [...sources, { manifestKey: "key2", revision: "1" }],
          events,
        },
      ),
    ).toEqual({ written: true });
    expect(
      (
        await db.query(
          "SELECT event_count,event_projection_ready FROM sessions.session_directory",
        )
      ).rows[0],
    ).toMatchObject({ event_count: 2, event_projection_ready: true });
    expect(
      (
        await db.query(
          "SELECT count(*)::int AS n FROM sessions.session_directory_event_pending WHERE dirty",
        )
      ).rows[0],
    ).toEqual({ n: 0 });
    expect(
      await writeDirectoryEvents(
        db as never,
        { ...key, sessionId: "canonical" },
        {
          sourceSessionId: "s1",
          expectedEpoch: "1",
          sources,
          events: directoryEventsFromProjection([row()]),
        },
      ),
    ).toEqual({ written: false });
  });
  test("tombstones clear aggregates and a late writer cannot restore them", async () => {
    await seed();
    const events = directoryEventsFromProjection([row()]);
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events,
    });
    await db.exec(
      "UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='s1'",
    );
    await projectSessionDirectory(db as never, key);
    expect(
      (
        await db.query(
          "SELECT kinds,task_refs,reasoning_tokens,event_projection_ready,event_projection_version FROM sessions.session_directory",
        )
      ).rows[0],
    ).toMatchObject({
      kinds: [],
      task_refs: [],
      reasoning_tokens: 0,
      event_projection_ready: false,
      event_projection_version: 0,
    });
    expect(
      (
        await db.query(
          "SELECT count(*)::int AS n FROM sessions.session_directory_event_times",
        )
      ).rows[0],
    ).toEqual({ n: 0 });
    expect(
      await writeDirectoryEvents(db as never, key, {
        sourceSessionId: "s1",
        expectedEpoch: "0",
        sources,
        events,
      }),
    ).toEqual({ written: false });
  });
  test("new machine pointers invalidate readiness even before their aggregate pass", async () => {
    await seed();
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([row()]),
    });
    await db.exec(
      "INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,raw_format,trace_id,revision) SELECT org_id,workspace_id,harness,session_id,'m2',user_id,created_at,last_activity_at,storage_provider,bucket,'key2',raw_format,trace_id,revision FROM sessions.session_index WHERE machine_id='m1'",
    );
    expect(
      (
        await db.query(
          "SELECT event_projection_ready FROM sessions.session_directory",
        )
      ).rows[0],
    ).toMatchObject({ event_projection_ready: false });
  });
  test("project name is exact recorded-repo metadata across harnesses and uses its recency index", async () => {
    await seed();
    await db.exec(
      "UPDATE sessions.session_directory SET git_repo='https://github.com/AgentWorkforce/relaycast.git'",
    );
    await writeDirectoryEvents(db as never, key, {
      sourceSessionId: "s1",
      expectedEpoch: "0",
      sources,
      events: directoryEventsFromProjection([row()]),
    });
    await asWorker(async () => {
      expect(
        (
          await db.query(
            "SELECT project_name FROM sessions.list_session_directory_events('ws-a',p_project_name=>' RELAYCAST ')",
          )
        ).rows,
      ).toEqual([{ project_name: "relaycast" }]);
      expect(
        (
          await db.query(
            "SELECT * FROM sessions.list_session_directory_events('ws-a',p_project_name=>'relay')",
          )
        ).rows,
      ).toHaveLength(0);
    });
    await db.exec(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,git_repo,event_count,event_first_ts,event_last_ts)
      SELECT CASE WHEN n<=20000 THEN 'plan-org' ELSE 'other-org' END,'plan-ws',(ARRAY['codex','claude','opencode'])[1+n%3],'plan-'||n,'2026-01-01'::timestamptz+n*interval '1 minute','2026-01-01'::timestamptz+n*interval '1 minute',
        CASE WHEN n%500=0 THEN 'https://github.com/a/relaycast.git' ELSE 'https://github.com/a/project-'||(n%50)||'.git' END,1,'2026-01-01'::timestamptz+n*interval '1 minute','2026-01-01'::timestamptz+n*interval '1 minute' FROM generate_series(1,80000) n;
      ANALYZE sessions.session_directory;`);
    const built = (
      await db.query<{ sql: string }>(
        "SELECT sessions.session_directory_events_list_sql(0,false,0,false,false,false,false,false,false,false,false,true) AS sql",
      )
    ).rows[0]!.sql;
    const literals = [
      "'plan-org'",
      "'plan-ws'",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "21",
      "NULL",
      "NULL",
      "NULL",
      "'relaycast'",
    ];
    const query = built.replace(/\$(\d+)/g, (_, n) => literals[Number(n) - 1]!);
    const plan = JSON.stringify(
      (await db.query("EXPLAIN (FORMAT JSON) " + query)).rows,
    );
    expect(plan).toContain("session_directory_project_name_recent_idx");
    const visible = (
      await db.query<{ harness: string; session_id: string }>(query)
    ).rows;
    expect(new Set(visible.map((r) => r.harness)).size).toBe(3);
    expect(
      new Set(visible.map((r) => r.harness + ":" + r.session_id)).size,
    ).toBe(21);
    expect(
      (
        await db.query(
          "SELECT count(*)::int AS n FROM sessions.session_directory WHERE org_id='plan-org' AND project_name='relaycast'",
        )
      ).rows[0],
    ).toEqual({ n: 40 });
  }, 120000);
});
