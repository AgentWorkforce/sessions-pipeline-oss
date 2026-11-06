/**
 * GET /v1/sessions/:id/timing on the read API: the canary allowlist, the 501
 * fallback contract (never Neon, never a guess), erasure, the deadline, and the
 * exact @relayhistory/cloud-client 0.2.0 `sessionTimingResponse` shape that
 * Cloud validates before serving a 96Bulls answer.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type TimingDeps, type VerifiedIdentity } from "../app.ts";
import { CanonicalReadError, type CanonicalIdentity } from "../canonical.ts";
import type { CanonicalRows } from "../session-timing-source.ts";
import { deliveryMachine } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";

const identity: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const M = deliveryMachine("ws-a", "o-1");
const ROWS: CanonicalRows = {
  legacyCoverage: null,
  rows: [
    { table: "convergence_events", row: { session_id: "s-1", source: "codex", machine_id: M, user_id: "u-1", kind: "session_event", type: "user", ts: "2026-10-01T12:00:00.000000Z", actor_role: "user", record: { payload: { role: "user" } }, tool_calls: [] } },
    { table: "convergence_events", row: { session_id: "s-1", source: "codex", machine_id: M, user_id: "u-1", kind: "tool_call", type: "tool_call", ts: "2026-10-01T12:00:01.000000Z", tool_name: "exec", record: { payload: { kind: "function_call", call_id: "c" } }, tool_calls: [] } },
    { table: "convergence_events", row: { session_id: "s-1", source: "codex", machine_id: M, user_id: "u-1", kind: "session_event", type: "tool_result", ts: "2026-10-01T12:00:03.500000Z", actor_role: "tool_result", record: { payload: { kind: "function_call_output", call_id: "c" } }, tool_calls: [] } },
    { table: "session_markers", row: { session_id: "s-1", source: "codex", origin_id: "o-1", turn_id: "t", marker_kind: "task_started", ts: "2026-10-01T12:00:00.500000Z" } },
    { table: "session_markers", row: { session_id: "s-1", source: "codex", origin_id: "o-1", turn_id: "t", marker_kind: "task_complete", ts: "2026-10-01T12:00:04.000000Z" } },
  ],
};

function api(over: { workspaces?: ReadonlySet<string> | "*"; timing?: Partial<TimingDeps> | null; rows?: CanonicalRows; state?: { pointers: number; live: number } } = {}) {
  const calls: CanonicalIdentity[] = [];
  const logs: Record<string, unknown>[] = [];
  const timing: TimingDeps | undefined =
    over.timing === null
      ? undefined
      : {
          state: async () => over.state ?? { pointers: 1, live: 1 },
          source: async (id) => {
            calls.push(id);
            return over.rows ?? ROWS;
          },
          assumeNoTurns: true,
          ...over.timing,
        };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return identity;
    },
    ch: { database: "relay_search", async command() {}, async insert() {}, async select() { throw new Error("timing never reads ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: "workspaces" in over ? over.workspaces : new Set(["ws-a"]),
    timing,
    log: (f) => logs.push(f),
  });
  const get = (path: string, token: string | null = "good") => app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { get, calls, logs };
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;
const PATH = "/v1/sessions/s-1/timing?workspace=ws-a&source=codex";

/** cloud-client 0.2.0 sessionTimingResponse (contract/recall.ts), strict: exactly these keys and types. */
function expectTimingContract(b: Record<string, any>) {
  expect(Object.keys(b).sort()).toEqual(["correlationId", "coverage", "lastActionAt", "sessionId", "source", "startedAt", "waitingMs", "workMs"]);
  expect(Object.keys(b.coverage).sort()).toEqual([
    "eventCount", "lifecycleIntervalCount", "markerCount", "method", "scope", "turnCount", "unmatchedLifecycleCount", "unmatchedToolCallCount", "waitingIntervalCount", "workIntervalCount",
  ]);
  expect(b.coverage.scope).toBe("stored_session");
  expect(b.coverage.method).toBe("recorded_intervals");
  for (const k of ["eventCount", "turnCount", "markerCount", "workIntervalCount", "waitingIntervalCount", "lifecycleIntervalCount", "unmatchedToolCallCount", "unmatchedLifecycleCount"])
    expect(Number.isInteger(b.coverage[k]) && b.coverage[k] >= 0).toBe(true);
  for (const k of ["startedAt", "lastActionAt"]) expect(b[k] === null || /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(b[k])).toBe(true);
  for (const k of ["workMs", "waitingMs"]) expect(b[k] === null || (typeof b[k] === "number" && b[k] >= 0)).toBe(true);
  expect(typeof b.correlationId).toBe("string");
}

describe("GET /v1/sessions/:id/timing from canonical S3", () => {
  test("serves the timing with tenant from the token, the workspace attested, the strict contract", async () => {
    const { get, calls, logs } = api();
    const r = await get(PATH);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(r.headers.get("cache-control")).toBe("private, no-store");
    const b = await body(r);
    expectTimingContract(b);
    expect(b).toMatchObject({ sessionId: "s-1", source: "codex", startedAt: "2026-10-01T12:00:00.000Z", lastActionAt: "2026-10-01T12:00:04.000Z", workMs: 3500 });
    expect(calls).toEqual([{ orgId: "org-a", workspaceId: "ws-a", source: "codex", sessionId: "s-1" }]);
    expect(logs.find((l) => l.reason === "timing_served")).toMatchObject({ turns: "assumed_none", events: 3 });
  });

  test("source is trimmed and required exactly once, as relayhistory answers (400)", async () => {
    const { get, calls } = api();
    for (const q of ["", "&source=", "&source=%20", "&source=codex&source=claude"]) {
      const r = await get(`/v1/sessions/s-1/timing?workspace=ws-a${q}`);
      expect(r.status, q).toBe(400);
      expect((await body(r)).error.message).toBe("source must be supplied exactly once and be non-empty");
    }
    expect((await get("/v1/sessions/s-1/timing?workspace=ws-a&source=%20codex%20")).status).toBe(200);
    expect(calls.at(-1)?.source).toBe("codex");
  });

  test.each<[string, Parameters<typeof api>[0], string]>([
    ["off the canary allowlist", { workspaces: new Set(["ws-other"]) }, "timing_workspace_not_enabled"],
    ["allowlist unset", { workspaces: undefined }, "timing_workspace_not_enabled"],
    ["no canonical source configured", { timing: null }, "timing_not_configured"],
    ["no pointer row (Neon may hold it)", { state: { pointers: 0, live: 0 } }, "timing_no_pointers"],
    ["no legacy turn evidence and no operator assumption", { timing: { assumeNoTurns: false } }, "timing_turns_unknown"],
    ["the canonical read refuses", { timing: { source: async () => { throw new CanonicalReadError("pointer_bound"); } } }, "timing_canonical_pointer_bound"],
    ["the deadline passes", { timing: { deadlineMs: 5, source: (_id, signal) => new Promise((_, reject) => signal.addEventListener("abort", () => reject(new Error("aborted")))) } }, "timing_canonical_deadline"],
    ["an ambiguous marker user", { rows: { legacyCoverage: null, rows: [
      ...ROWS.rows.filter((r) => r.table !== "convergence_events"),
      { table: "convergence_events", row: { ...ROWS.rows[0]!.row, machine_id: "x", user_id: "u-1" } },
      { table: "convergence_events", row: { ...ROWS.rows[0]!.row, machine_id: "y", user_id: "u-2" } },
    ] } }, "timing_marker_user"],
    ["a row of another session", { rows: { legacyCoverage: null, rows: [{ table: "convergence_events", row: { ...ROWS.rows[0]!.row, session_id: "s-2" } }] } }, "timing_event_scope"],
  ])("%s → 501 %s (Cloud reads relayhistory)", async (_name, over, reason) => {
    const { get } = api(over);
    const r = await get(PATH);
    expect(r.status).toBe(501);
    expect((await body(r)).error).toMatchObject({ code: "unsupported_by_backend", reason });
  });

  test("org-wide (no workspace) stays on relayhistory; another workspace is 403; no token 401", async () => {
    const { get, calls } = api();
    const org = await get("/v1/sessions/s-1/timing?source=codex");
    expect(org.status).toBe(501);
    expect((await body(org)).error.reason).toBe("org_scope");
    expect((await get("/v1/sessions/s-1/timing?workspace=ws-b&source=codex")).status).toBe(403);
    expect((await get(PATH, null)).status).toBe(401);
    expect(calls).toEqual([]);
  });

  test("an erased session (every pointer tombstoned) is the empty timing and reads no object", async () => {
    const { get, calls } = api({ state: { pointers: 2, live: 0 }, timing: { assumeNoTurns: false } });
    const r = await get(PATH);
    expect(r.status).toBe(200);
    const b = await body(r);
    expectTimingContract(b);
    expect(b).toMatchObject({ startedAt: null, lastActionAt: null, workMs: null, waitingMs: null, coverage: { eventCount: 0, turnCount: 0, markerCount: 0 } });
    expect(calls).toEqual([]);
  });

  test("legacy turns count when their stored source is this source, and refuse otherwise", async () => {
    const turn = { session_id: "s-1", source: "codex", ts: "2026-10-01T11:59:00.000000Z", role: "user", id: "x" };
    const ok = api({ rows: { legacyCoverage: { version: 1, tables: ["conversation_turns"] }, rows: [...ROWS.rows, { table: "conversation_turns", row: turn }] }, timing: { assumeNoTurns: false } });
    const b = await body(await ok.get(PATH));
    expect(b).toMatchObject({ startedAt: "2026-10-01T11:59:00.000Z", coverage: { turnCount: 1 } });
    const moved = api({ rows: { legacyCoverage: { version: 1, tables: [] }, rows: [{ table: "conversation_turns", row: { ...turn, legacy_stored_source: "agent-x" } }] } });
    expect((await body(await moved.get(PATH))).error.reason).toBe("timing_turn_attribution");
  });
});
