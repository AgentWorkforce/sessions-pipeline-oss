/**
 * Parity: the read API's timing port (timing.ts) against relayhistory's own
 * sessions.session_timing_compute v1, vendored verbatim
 * (fixtures/relayhistory-session-timing-compute.sql), executed in PGlite over
 * the same rows. Every case must produce an identical payload, and every input
 * the SQL refuses must be refused by the port too.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { deliveryMachine } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {
  computeSessionTiming,
  TimingInputError,
  type TimingEventRow,
  type TimingMarkerRow,
  type TimingTurnRow,
} from "../timing.ts";

const ORACLE = readFileSync(join(import.meta.dir, "fixtures/relayhistory-session-timing-compute.sql"), "utf8");
const ORG = "org-p";
const WS = "ws-p";

/** The columns of relayhistory's tables that the timing SQL reads. */
const SCHEMA = `
CREATE SCHEMA sessions;
CREATE TABLE sessions.convergence_events (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL, machine_id text NOT NULL,
  source text NOT NULL, session_id text NOT NULL, event_id text NOT NULL, kind text NOT NULL, type text,
  ts timestamptz NOT NULL, actor_role text, tool_name text, duration_ms integer,
  record jsonb NOT NULL, tool_calls jsonb NOT NULL DEFAULT '[]');
CREATE TABLE sessions.conversation_turns (
  org_id text NOT NULL, workspace_id text, source text NOT NULL, session_id text NOT NULL,
  ts timestamptz NOT NULL, role text NOT NULL);
CREATE TABLE sessions.session_markers (
  org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
  origin_id text NOT NULL, user_id text NOT NULL, turn_id text, marker_kind text, ts timestamptz);
`;

let db: PGlite;
beforeAll(async () => {
  db = new PGlite();
  await db.exec(SCHEMA);
  await db.exec(ORACLE);
}, 60_000);
afterAll(async () => {
  await db?.close();
});

interface Case {
  events: TimingEventRow[];
  markers?: TimingMarkerRow[];
  turns?: TimingTurnRow[];
}

let n = 0;
async function oracle(c: Case, sessionId: string, source: string): Promise<unknown> {
  for (const [i, e] of c.events.entries())
    await db.query(
      `INSERT INTO sessions.convergence_events (org_id, workspace_id, user_id, machine_id, source, session_id, event_id, kind, type, ts,
         actor_role, tool_name, duration_ms, record, tool_calls) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::timestamptz,$11,$12,$13,$14::jsonb,$15::jsonb)`,
      [ORG, e.workspace_id ?? WS, e.user_id ?? "u", e.machine_id ?? "m", source, sessionId, `e${i}`, e.kind, e.type ?? null, e.ts,
        e.actor_role ?? null, e.tool_name ?? null, e.duration_ms ?? null, JSON.stringify(e.record ?? {}), JSON.stringify(e.tool_calls ?? [])],
    );
  for (const m of c.markers ?? [])
    await db.query(
      `INSERT INTO sessions.session_markers (org_id, workspace_id, source, session_id, origin_id, user_id, turn_id, marker_kind, ts)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::timestamptz)`,
      [ORG, m.workspace_id ?? WS, source, sessionId, m.origin_id, m.user_id ?? "u", m.turn_id ?? null, m.marker_kind ?? null, m.ts ?? null],
    );
  for (const t of c.turns ?? [])
    await db.query(`INSERT INTO sessions.conversation_turns (org_id, workspace_id, source, session_id, ts, role) VALUES ($1,$2,$3,$4,$5::timestamptz,$6)`, [ORG, WS, source, sessionId, t.ts, t.role]);
  const r = await db.query<{ t: unknown }>("SELECT sessions.session_timing_compute($1, $2, $3, $4) AS t", [ORG, sessionId, source, WS]);
  return r.rows[0]!.t;
}

/** The port over the same rows, with the defaults oracle() inserts. */
function port(c: Case, sessionId: string, source: string) {
  return computeSessionTiming(
    {
      sessionId,
      source,
      events: c.events.map((e) => ({ ...e, workspace_id: e.workspace_id ?? WS, user_id: e.user_id ?? "u", machine_id: e.machine_id ?? "m", record: e.record ?? {}, tool_calls: e.tool_calls ?? [] })),
      markers: (c.markers ?? []).map((m) => ({ ...m, workspace_id: m.workspace_id ?? WS, user_id: m.user_id ?? "u" })),
      turns: c.turns ?? [],
    },
    (ws, origin) => deliveryMachine(ws ?? WS, origin),
  );
}

async function expectParity(c: Case): Promise<void> {
  const sessionId = `s-${++n}`;
  const source = "codex";
  let sqlError: unknown = null;
  let expected: unknown;
  try {
    expected = await oracle(c, sessionId, source);
  } catch (error) {
    sqlError = error;
  }
  if (sqlError) {
    expect(() => port(c, sessionId, source)).toThrow(TimingInputError);
    return;
  }
  expect(JSON.parse(JSON.stringify(port(c, sessionId, source)))).toEqual(expected as never);
}

const T = (s: number, micros = 0) => new Date(Date.UTC(2026, 9, 1, 12, 0, 0) + s * 1000).toISOString().replace(/\.000Z$/, `.${String(micros).padStart(6, "0")}+00`).replace("T", " ");
const call = (s: number, id: string | null, name = "exec_command", extra: Partial<TimingEventRow> = {}): TimingEventRow => ({
  kind: "tool_call", type: "tool_call", ts: T(s), tool_name: name, record: { payload: { kind: "function_call", ...(id ? { call_id: id } : {}) } }, ...extra,
});
const result = (s: number, id: string | null, extra: Partial<TimingEventRow> = {}): TimingEventRow => ({
  kind: "session_event", type: "tool_result", ts: T(s), actor_role: "tool_result", record: { payload: { kind: "function_call_output", ...(id ? { call_id: id } : {}) } }, ...extra,
});
const msg = (s: number, role: string, kind = "session_event"): TimingEventRow => ({ kind, type: role, ts: T(s), actor_role: role, record: { payload: { role } } });
const marker = (s: number | null, kind: string, turn: string | null, origin = "o-1"): TimingMarkerRow => ({ origin_id: origin, turn_id: turn, marker_kind: kind, ts: s === null ? null : T(s) });

describe("timing parity with relayhistory session_timing_compute v1", () => {
  test("empty session", async () => {
    await expectParity({ events: [] });
  });

  test.each<[string, Case]>([
    ["matched call/result, no durations", { events: [msg(0, "user"), call(1, "c1"), result(4, "c1"), msg(5, "assistant")] }],
    ["result duration wins over call duration", { events: [call(1, "c1", "exec", { duration_ms: 900 }), result(10, "c1", { duration_ms: 2500 })] }],
    ["call duration with no result", { events: [call(1, "c1", "exec", { duration_ms: 1234 })] }],
    ["duplicated call start is unmatched", { events: [call(1, "c1"), call(2, "c1"), result(3, "c1")] }],
    ["result before call is unmatched", { events: [call(5, "c1"), result(3, "c1")] }],
    ["calls without ids count as unmatched", { events: [call(1, null), call(2, null, "Bash", { duration_ms: 40 })] }],
    ["orphan result with a duration is work", { events: [result(9, "zz", { tool_name: "exec", duration_ms: 700 })] }],
    ["a wait (request_user_input) is waiting, not work", { events: [call(1, "w1", "functions.request_user_input"), result(30, "w1"), call(31, "c2"), result(32, "c2")] }],
    ["AskUserQuestion is a wait; async is neither", { events: [call(1, "w1", "mcp__x__AskUserQuestion"), result(5, "w1"), call(6, "a1", "request_user_input_async"), result(9, "a1")] }],
    ["waits with durations are not counted as work by duration", { events: [call(1, "w1", "request_user_input", { duration_ms: 99 }), result(8, "w1", { duration_ms: 50 })] }],
    ["overlapping work merges", { events: [call(1, "a"), call(2, "b"), result(5, "a"), result(6, "b")] }],
    ["adjacent work merges", { events: [call(1, "a"), result(2, "a"), call(2, "b"), result(3, "b")] }],
    ["per-machine call keys", { events: [call(1, "c", "exec", { machine_id: "m1" }), result(2, "c", { machine_id: "m2" }), result(3, "c", { machine_id: "m1" })] }],
    ["tool_calls array of one supplies the id and name", { events: [{ kind: "tool_call", ts: T(1), record: { payload: {} }, tool_calls: [{ toolUseId: "t1", name: "Read" }] }, result(2, "t1")] }],
    ["tool_calls of two supplies neither id", { events: [{ kind: "tool_call", ts: T(1), record: { payload: {} }, tool_calls: [{ id: "x" }, { id: "y" }] }] }],
    ["record without payload is the payload", { events: [{ kind: "tool_call", ts: T(1), record: { kind: "tool_use", tool_use_id: "q", name: "Grep" } }, { kind: "tool_call", ts: T(3), record: { kind: "tool_result", tool_use_id: "q" } }] }],
    ["payload null reads as nothing", { events: [{ kind: "session_event", type: "assistant", ts: T(1), actor_role: "assistant", record: { payload: null } }] }],
    ["system and metadata rows are not activity", { events: [msg(0, "system"), { kind: "session_event", type: "session_meta", ts: T(1), record: { payload: { kind: "session_meta" } } }, msg(2, "user")] }],
    ["file_edit and assistant kinds are actions", { events: [msg(0, "user"), { kind: "file_edit", ts: T(4), record: { payload: {} } }, { kind: "assistant", ts: T(6), record: {} }] }],
    ["non-activity kinds still count as events", { events: [{ kind: "presence", ts: T(1), record: {} }, { kind: "convergence_event", ts: T(2), record: {} }] }],
    ["a system-role message of another type is not activity", { events: [{ kind: "session_event", type: "message", ts: T(0), record: { payload: { role: "system" } } }, msg(3, "user")] }],
    ["negative durations are ignored", { events: [call(1, "a", "exec", { duration_ms: -5 }), result(9, "a", { duration_ms: -1 }), result(12, null, { tool_name: "exec", duration_ms: -3 })] }],
    ["microsecond durations stay exact", { events: [call(1, "a", "exec"), { ...result(2, "a"), ts: T(2, 123457) }] }],
  ])("%s", async (_name, c) => {
    await expectParity(c);
  });

  test.each<[string, Case]>([
    ["lifecycle interval is work", { events: [], markers: [marker(1, "task_started", "t1"), marker(20, "task_complete", "t1")] }],
    ["lifecycle minus waiting on the same delivery machine", {
      events: [
        call(5, "w", "request_user_input", { machine_id: deliveryMachine(WS, "o-1") }),
        result(12, "w", { machine_id: deliveryMachine(WS, "o-1") }),
      ],
      markers: [marker(1, "task_started", "t1"), marker(20, "task_complete", "t1")],
    }],
    ["waiting on another machine is not subtracted", {
      events: [call(5, "w", "request_user_input", { machine_id: "elsewhere" }), result(12, "w", { machine_id: "elsewhere" })],
      markers: [marker(1, "task_started", "t1"), marker(20, "task_complete", "t1")],
    }],
    ["unmatched lifecycle: two starts, no end, end before start", {
      events: [],
      markers: [marker(1, "task_started", "a"), marker(2, "task_started", "a"), marker(3, "task_complete", "a"), marker(4, "task_started", "b"), marker(9, "task_started", "c"), marker(5, "task_complete", "c")],
    }],
    ["markers without turn or ts", { events: [], markers: [marker(null, "task_started", "x"), marker(3, "task_complete", null), marker(4, "other", "y")] }],
    ["per-origin lifecycle", { events: [], markers: [marker(1, "task_started", "t", "o-a"), marker(5, "task_complete", "t", "o-a"), marker(3, "task_started", "t", "o-b"), marker(9, "task_complete", "t", "o-b")] }],
  ])("%s", async (_name, c) => {
    await expectParity(c);
  });

  test("turns move bounds and count", async () => {
    await expectParity({ events: [msg(10, "assistant")], turns: [{ ts: T(2), role: "user" }, { ts: T(30), role: "assistant" }, { ts: T(40), role: "system" }] });
  });

  test("seeded random sessions", async () => {
    let seed = 0x5eed;
    const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
    const pick = <T,>(xs: T[]) => xs[Math.floor(rnd() * xs.length)]!;
    for (let k = 0; k < 120; k++) {
      const ids = ["a", "b", "c", "d"];
      const machines = ["m1", deliveryMachine(WS, "o-1")];
      const events: TimingEventRow[] = [];
      for (let i = 0; i < 2 + Math.floor(rnd() * 14); i++) {
        const s = Math.floor(rnd() * 120);
        const machine = pick(machines);
        const dur = rnd() < 0.3 ? Math.floor(rnd() * 5000) : null;
        const shape = rnd();
        if (shape < 0.35) events.push(call(s, rnd() < 0.85 ? pick(ids) : null, pick(["exec", "request_user_input", "Bash", "request_user_input_async", "x__AskUserQuestion"]), { machine_id: machine, duration_ms: dur, ts: T(s, Math.floor(rnd() * 999999)) }));
        else if (shape < 0.7) events.push(result(s, rnd() < 0.9 ? pick(ids) : null, { machine_id: machine, duration_ms: dur, tool_name: rnd() < 0.5 ? "exec" : null }));
        else events.push(msg(s, pick(["user", "assistant", "system", "tool_result"]), pick(["session_event", "assistant", "history", "presence"])));
      }
      const markers: TimingMarkerRow[] = [];
      for (let i = 0; i < Math.floor(rnd() * 5); i++) markers.push(marker(rnd() < 0.9 ? Math.floor(rnd() * 120) : null, pick(["task_started", "task_complete", "other"]), pick(["t1", "t2", null])));
      const turns: TimingTurnRow[] = rnd() < 0.3 ? [{ ts: T(Math.floor(rnd() * 120)), role: pick(["user", "assistant", "system"]) }] : [];
      await expectParity({ events, markers, turns });
    }
  }, 120_000);
});

describe("timing input refusals", () => {
  test("an unparseable or non-UTC timestamp is refused, never guessed", () => {
    for (const ts of ["2026-10-01T12:00:00+02:00", "yesterday", "2026-02-30 00:00:00+00"])
      expect(() => port({ events: [{ kind: "session_event", ts, record: {} }] }, "s", "codex")).toThrow(TimingInputError);
  });
});
