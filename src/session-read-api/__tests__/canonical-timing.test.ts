import { afterAll, beforeAll, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import {
  canonicalTiming,
  canonicalTimingProjection,
} from "../canonical-timing.ts";
import { deliveryMachine } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
type Row = Record<string, unknown>;
const id = {
  orgId: "org-a",
  workspaceId: "ws-a",
  source: "codex",
  sessionId: "session",
};
const scope = {
  org_id: id.orgId,
  workspace_id: id.workspaceId,
  source: id.source,
  session_id: id.sessionId,
};
const time = (s: number) =>
  new Date(Date.parse("2026-10-01T00:00:00Z") + s * 1000).toISOString();
function fixture() {
  const events: Row[] = [],
    markers: Row[] = [],
    turns: Row[] = [];
  const event = (s: number, change: Row = {}) =>
    events.push({
      ...scope,
      user_id: "u",
      machine_id: deliveryMachine("ws-a", "origin"),
      event_id: "e" + events.length,
      kind: "session_event",
      type: "text",
      ts: time(s),
      actor_role: "assistant",
      tool_name: null,
      duration_ms: null,
      record: {},
      tool_calls: [],
      ...change,
    });
  const pair = (
    key: string,
    start: number,
    end: number,
    name = "exec_command",
    change: Row = {},
  ) => {
    event(start, {
      type: "tool_use",
      tool_name: name,
      record: {
        payload: { tool_use_id: key, kind: "tool_use", role: "assistant" },
      },
      ...change,
    });
    event(end, {
      type: "tool_result",
      actor_role: "tool_result",
      record: {
        payload: { tool_use_id: key, kind: "tool_result", role: "tool_result" },
      },
      ...change,
    });
  };
  const marker = (kind: string, s: number, change: Row = {}) =>
    markers.push({
      ...scope,
      user_id: "u",
      origin_id: "origin",
      turn_id: "turn",
      marker_kind: kind,
      ts: time(s),
      ...change,
    });
  const turn = (s: number, role = "assistant", change: Row = {}) =>
    turns.push({ ...scope, ts: time(s), role, ...change });
  return { events, markers, turns, event, pair, marker, turn };
}
type Fixture = ReturnType<typeof fixture>;
const cases: Array<[string, (f: Fixture) => void]> = [
  ["empty remains unknown", () => {}],
  [
    "system turns and completion-only lifecycle",
    (f) => {
      f.turn(-100, "system");
      f.turn(5, "user");
      f.turn(10);
      f.turn(200, "system");
      f.marker("task_complete", 20);
    },
  ],
  [
    "explicit output role beats generic call kind",
    (f) =>
      f.event(1, {
        kind: "tool_call",
        type: "tool_call",
        actor_role: "tool_result",
        tool_name: "exec_command",
        record: { call_id: "orphan" },
      }),
  ],
  [
    "concurrent work survives lifecycle wait",
    (f) => {
      f.marker("task_started", 0);
      f.marker("task_complete", 10);
      f.pair("wait", 2, 8, "request_user_input");
      f.pair("parallel", 4, 6);
    },
  ],
  [
    "standalone duration does not invent unanswered waits",
    (f) => {
      f.event(1, {
        kind: "tool_call",
        type: "",
        tool_name: "exec_command",
        duration_ms: 2000,
      });
      f.event(10, {
        kind: "tool_call",
        tool_name: "request_user_input",
        duration_ms: 5000,
      });
      f.event(100, { type: "tool_result", duration_ms: 90000 });
    },
  ],
  [
    "assistant kind and prompt-only action",
    (f) => {
      f.event(1, { kind: "prompt", actor_role: null });
      f.event(2, { kind: "assistant", actor_role: null });
    },
  ],
  [
    "overlaps explicit waits async and idle tails",
    (f) => {
      f.event(0, { kind: "prompt", actor_role: "user" });
      f.marker("task_started", 1);
      f.marker("task_complete", 21);
      f.pair("a", 2, 8);
      f.pair("b", 5, 10);
      f.pair("input", 10, 15, "functions.request_user_input");
      f.pair("async", 15, 10000, "functions.request_user_input_async");
      f.event(20000, { kind: "discovery", actor_role: "system" });
    },
  ],
  [
    "no inferred gap duration",
    (f) => {
      f.event(0, { kind: "prompt", actor_role: "user" });
      f.event(-100, {
        actor_role: "system",
        record: { payload: { role: "system" } },
      });
      f.event(10000, {
        actor_role: "system",
        record: { payload: { role: "system" } },
      });
      f.event(100);
      f.event(110, {
        type: "tool_use",
        tool_name: "request_user_input",
        record: { call_id: "wait" },
      });
      f.marker("task_started", 200);
    },
  ],
  [
    "known zero and reversed intervals",
    (f) => {
      f.pair("instant", 1, 1);
      f.pair("reverse", 10, 5);
    },
  ],
  [
    "explicit result duration has result anchor",
    (f) => {
      f.pair("a", 1, 11);
      f.events[1]!.duration_ms = 2000;
      f.pair("b", 10, 12);
    },
  ],
  [
    "tenant source user machine boundaries",
    (f) => {
      f.event(0, {
        type: "tool_use",
        tool_name: "exec_command",
        record: { call_id: "shared" },
      });
      for (const change of [
        { user_id: "other" },
        { machine_id: "other" },
        { workspace_id: "other" },
        { source: "claude" },
        { org_id: "other" },
      ])
        f.event(10, {
          type: "tool_result",
          record: { call_id: "shared" },
          ...change,
        });
    },
  ],
  [
    "team work remains visible",
    (f) => {
      f.pair("mine", 1, 3);
      f.pair("theirs", 4, 7, "exec_command", { user_id: "other" });
    },
  ],
  [
    "workspace excludes foreign turns and markers",
    (f) => {
      f.pair("a", 1, 3);
      f.pair("b", 10, 20, "exec_command", { workspace_id: "other" });
      f.turn(30, "assistant", { workspace_id: "other" });
      f.marker("task_started", 40, { workspace_id: "other" });
    },
  ],
  [
    "turn source identity is exact",
    (f) => {
      f.pair("a", 1, 3);
      f.turn(0, "user");
      f.turn(10);
      f.turn(999, "assistant", { source: "claude" });
      f.turn(1000, "assistant", { source: "" });
    },
  ],
  [
    "all records beyond a replay page",
    (f) => {
      for (let i = 0; i < 205; i++)
        f.event(i, { kind: "prompt", type: "prompt", actor_role: "user" });
      f.pair("late", 300, 310);
    },
  ],
  [
    "mirrors and reused ambiguous call IDs",
    (f) => {
      f.pair("a", 1, 3);
      f.event(1, {
        kind: "tool_call",
        type: "tool_call",
        tool_name: "exec_command",
        tool_calls: [{ toolUseId: "a", name: "exec_command" }],
      });
      f.pair("reused", 5, 6);
      f.event(10, { type: "tool_use", record: { call_id: "reused" } });
    },
  ],
  [
    "another uploader wait cannot subtract work",
    (f) => {
      f.marker("task_started", 1);
      f.marker("task_complete", 11);
      f.pair("work", 1, 11);
      f.pair("wait", 1, 11, "AskUserQuestion", { user_id: "other" });
    },
  ],
  [
    "duplicate lifecycle starts and null marker times",
    (f) => {
      f.marker("task_started", 1);
      f.marker("task_started", 2);
      f.marker("task_complete", 5);
      f.marker("other", 6, { ts: null });
    },
  ],
  [
    "microsecond work remains fractional milliseconds",
    (f) => {
      f.pair("tiny", 0, 0);
      f.events[0]!.ts = "2026-10-01T00:00:00.000001Z";
      f.events[1]!.ts = "2026-10-01T00:00:00.001002Z";
    },
  ],
  [
    "single tool descriptor IDs and multiple descriptor ambiguity",
    (f) => {
      f.event(1, {
        kind: "tool_call",
        type: "tool_call",
        tool_calls: [{ id: "id", name: "exec_command" }],
      });
      f.event(4, {
        type: "tool_result",
        actor_role: "tool_result",
        record: { payload: { callId: "id" } },
      });
      f.event(10, {
        kind: "tool_call",
        type: "tool_call",
        duration_ms: 500,
        tool_calls: [
          { id: "not-paired", name: "exec_command" },
          { id: "other" },
        ],
      });
    },
  ],
  [
    "null payload does not fall back to outer record",
    (f) => {
      f.event(1, {
        type: "tool_use",
        tool_name: "exec_command",
        record: { payload: null, call_id: "ignored" },
        duration_ms: 300,
      });
    },
  ],
  [
    "Postgres simple lowercase ASCII-fold classification",
    (f) => {
      f.event(1, { actor_role: "ASSİSTANT", type: "text" });
    },
  ],
  [
    "pre-epoch microsecond bounds",
    (f) => {
      f.pair("early", 0, 0);
      f.events[0]!.ts = "1969-12-31T23:59:59.123456Z";
      f.events[1]!.ts = "1969-12-31T23:59:59.124567Z";
    },
  ],
  [
    "largest call name matches database text ordering",
    (f) => {
      f.pair("named", 1, 4, "functions.request_user_input");
      f.event(1, {
        type: "tool_use",
        tool_name: "z.exec_command",
        record: { payload: { tool_use_id: "named", kind: "tool_use" } },
      });
    },
  ],
];
const pg = new PGlite();
beforeAll(async () => {
  await pg.exec(`CREATE SCHEMA sessions;
 CREATE TABLE sessions.convergence_events(org_id text,workspace_id text,user_id text,machine_id text,source text,session_id text,event_id text,kind text,type text,ts timestamptz,actor_role text,tool_name text,duration_ms bigint,record jsonb,tool_calls jsonb);
 CREATE TABLE sessions.session_markers(org_id text,workspace_id text,user_id text,source text,session_id text,origin_id text,turn_id text,marker_kind text,ts timestamptz);
 CREATE TABLE sessions.conversation_turns(org_id text,workspace_id text,source text,session_id text,ts timestamptz,role text);`);
  const migration = await readFile(
    new URL(
      "../../../vendor/relayhistory-cloud/migrations/0033_session_timing_cache.sql",
      import.meta.url,
    ),
    "utf8",
  );
  const compute =
    /CREATE FUNCTION sessions\.session_timing_compute\([\s\S]*?\$compute\$;/.exec(
      migration,
    )?.[0];
  if (!compute) throw new Error("timing SQL oracle missing");
  await pg.exec(compute);
});
afterAll(() => pg.close());
test.each(cases)(
  "canonical timing matches actual0033 SQL: %s",
  async (_name, setup) => {
    const f = fixture();
    setup(f);
    await pg.exec(
      "TRUNCATE sessions.convergence_events,sessions.session_markers,sessions.conversation_turns",
    );
    for (const [table, rows] of [
      ["convergence_events", f.events],
      ["session_markers", f.markers],
      ["conversation_turns", f.turns],
    ] as const)
      if (rows.length)
        await pg.query(
          `INSERT INTO sessions.${table} SELECT * FROM jsonb_populate_recordset(NULL::sessions.${table},$1::jsonb)`,
          [JSON.stringify(rows)],
        );
    const expected = (
      await pg.query<{ timing: unknown }>(
        "SELECT sessions.session_timing_compute($1,$2,$3,$4) AS timing",
        [id.orgId, id.sessionId, id.source, id.workspaceId],
      )
    ).rows[0]!.timing;
    expect(canonicalTiming(id, f)).toEqual(expected);
  },
);

test("timing projection refuses unknown legacy coverage rather than inventing missing turns", () => {
  expect(canonicalTimingProjection(id, [], false)).toBeNull();
  const result = canonicalTimingProjection(id, [], true);
  expect(result).toMatchObject({
    table: "session_timing",
    row: {
      source: "codex",
      session_id: "session",
      timing: {
        startedAt: null,
        workMs: null,
        coverage: { eventCount: 0, turnCount: 0 },
      },
    },
  });
});
