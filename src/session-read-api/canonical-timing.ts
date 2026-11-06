/** Recorded interval timing over a COMPLETE canonical source projection.
 * Semantics are pinned to vendored 0033_session_timing_cache.sql by oracle tests.
 * This runs in the Node producer; Worker reads the small materialized result.
 */
import {
  deliveryMachine,
  lowerAgentName,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
export interface TimingIdentity {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
}
/** Source data the timing projection cannot interpret; retrying the same source never succeeds. */
export class CanonicalTimingError extends Error {
  readonly code = "canonical_timing_invalid";
  constructor(message: string) {
    super(message);
    this.name = "CanonicalTimingError";
  }
}
type Row = Record<string, unknown>;
type Range = { start: bigint; end: bigint };
type Interval = Range & { owner: string; bucket: "work" | "waiting" };
const object = (v: unknown): Row =>
  v !== null && typeof v === "object" && !Array.isArray(v) ? (v as Row) : {};
const text = (v: unknown): string | null =>
  v == null
    ? null
    : typeof v === "string"
      ? v
      : typeof v === "object"
        ? JSON.stringify(v)
        : String(v);
const nonempty = (v: unknown) => {
  const s = text(v);
  return s === "" ? null : s;
};
/** Postgres times have microsecond precision, including before the Unix epoch. */
export function timingMicros(value: unknown): bigint {
  if (value instanceof Date) value = value.toISOString();
  if (typeof value !== "string")
    throw new CanonicalTimingError("canonical timing timestamp invalid");
  const ms = Date.parse(value),
    fraction = /\.(\d{1,6})(?:Z|[+-]\d\d(?::?\d\d)?)$/u.exec(value)?.[1] ?? "";
  if (!Number.isFinite(ms))
    throw new CanonicalTimingError("canonical timing timestamp invalid");
  return BigInt(ms) * 1000n + BigInt(fraction.padEnd(6, "0").slice(3));
}
function iso(value: bigint | null) {
  if (value === null) return null;
  return new Date(
    Number((value - (((value % 1000n) + 1000n) % 1000n)) / 1000n),
  ).toISOString();
}
const compareText = (a: string, b: string) => {
  const x = Array.from(a),
    y = Array.from(b);
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = x[i]!.codePointAt(0)! - y[i]!.codePointAt(0)!;
    if (d) return d;
  }
  return x.length - y.length;
};
function duration(value: unknown): bigint | null {
  if (value == null) return null;
  if (typeof value === "number" && Number.isSafeInteger(value))
    return BigInt(value);
  if (typeof value === "bigint") return value;
  if (typeof value === "string" && /^-?\d+$/u.test(value)) return BigInt(value);
  throw new CanonicalTimingError("canonical timing duration invalid");
}
function merge(ranges: Range[]): Range[] {
  const sorted = ranges
      .filter((r) => r.end > r.start)
      .sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0)),
    out: Range[] = [];
  for (const r of sorted) {
    const last = out.at(-1);
    if (last && r.start <= last.end) {
      if (r.end > last.end) last.end = r.end;
    } else out.push({ ...r });
  }
  return out;
}
function subtract(range: Range, waits: Range[]): Range[] {
  let parts = [{ ...range }];
  for (const wait of waits) {
    const next: Range[] = [];
    for (const part of parts) {
      if (wait.end <= part.start || wait.start >= part.end) next.push(part);
      else {
        if (wait.start > part.start)
          next.push({ start: part.start, end: wait.start });
        if (wait.end < part.end) next.push({ start: wait.end, end: part.end });
      }
    }
    parts = next;
  }
  return parts;
}
const sum = (ranges: Range[]) =>
  Number(merge(ranges).reduce((n, r) => n + r.end - r.start, 0n)) / 1000;
const CALL = new Set([
  "custom_tool_call",
  "function_call",
  "mcp_tool_call",
  "tool_use",
  "tool-call",
  "tool_call",
]);
const RESULT = new Set([
  "custom_tool_call_output",
  "function_call_output",
  "mcp_tool_call_output",
  "tool_result",
  "tool-result",
  "tool_call_output",
]);
const ACTIVITY = new Set([
  "prompt",
  "user",
  "assistant",
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "trajectory",
  "task",
]);
const CONTROL = new Set([
  "session_meta",
  "session_metadata",
  "metadata",
  "discovery",
  "presence",
  "system",
  "system_message",
]);
const WAIT = /(^|[.:/]|__)(request_user_input|askuserquestion)$/u;
const ASYNC = /(^|[.:/]|__)request_user_input_async$/u;
interface Event {
  row: Row;
  time: bigint;
  owner: string;
  callId: string | null;
  name: string | null;
  role: string;
  eventKind: string;
  isCall: boolean;
  isResult: boolean;
  isActivity: boolean;
  duration: bigint | null;
}
function event(row: Row, id: TimingIdentity): Event {
  const record = object(row.record),
    payload = Object.hasOwn(record, "payload")
      ? object(record.payload)
      : record;
  const tools = Array.isArray(row.tool_calls) ? row.tool_calls : [],
    first = object(tools[0]);
  const eventKind = lowerAgentName(
    nonempty(payload.kind) ?? nonempty(row.type) ?? String(row.kind),
  );
  const role = lowerAgentName(text(payload.role) ?? text(row.actor_role) ?? "");
  const callId =
    nonempty(payload.tool_use_id) ??
    nonempty(payload.toolUseId) ??
    nonempty(payload.call_id) ??
    nonempty(payload.callId) ??
    (tools.length === 1
      ? (text(first.toolUseId) ??
        text(first.tool_use_id) ??
        text(first.call_id) ??
        text(first.id))
      : null);
  const name =
    nonempty(row.tool_name) ??
    nonempty(payload.tool_name) ??
    nonempty(payload.name) ??
    text(first.name);
  return {
    row,
    time: timingMicros(row.ts),
    owner: JSON.stringify([
      row.workspace_id ?? id.workspaceId,
      row.user_id ?? null,
      row.machine_id ?? null,
    ]),
    callId,
    name,
    role,
    eventKind,
    isCall:
      role !== "tool_result" &&
      (CALL.has(eventKind) ||
        (row.kind === "tool_call" && !RESULT.has(eventKind))),
    isResult: RESULT.has(eventKind) || role === "tool_result",
    isActivity:
      ACTIVITY.has(String(row.kind)) &&
      role !== "system" &&
      !CONTROL.has(eventKind),
    duration: duration(row.duration_ms),
  };
}
interface Group {
  owner: string;
  id: string;
  times: Set<string>;
  time: bigint;
  name: string | null;
  duration: bigint | null;
}
function group(map: Map<string, Group>, e: Event) {
  const key = JSON.stringify([e.owner, e.callId]);
  let g = map.get(key);
  if (!g) {
    g = {
      owner: e.owner,
      id: e.callId!,
      times: new Set(),
      time: e.time,
      name: null,
      duration: null,
    };
    map.set(key, g);
  }
  g.times.add(String(e.time));
  if (e.time < g.time) g.time = e.time;
  if (e.name !== null && (g.name === null || compareText(e.name, g.name) > 0))
    g.name = e.name;
  if (
    e.duration !== null &&
    e.duration >= 0n &&
    (g.duration === null || e.duration > g.duration)
  )
    g.duration = e.duration;
}
export function canonicalTiming(
  identity: TimingIdentity,
  input: { events: Row[]; markers: Row[]; turns: Row[] },
) {
  const scoped = (r: Row) =>
    (r.org_id ?? identity.orgId) === identity.orgId &&
    (r.workspace_id ?? identity.workspaceId) === identity.workspaceId &&
    r.source === identity.source &&
    r.session_id === identity.sessionId;
  const events = input.events.filter(scoped).map((r) => event(r, identity)),
    markers = input.markers.filter(scoped),
    turns = input.turns.filter(scoped);
  const calls = new Map<string, Group>(),
    outputs = new Map<string, Group>();
  for (const e of events) {
    if (e.isCall && e.callId !== null && e.callId !== "") group(calls, e);
    if (e.isResult && e.callId !== null && e.callId !== "") group(outputs, e);
  }
  const tools: Interval[] = [];
  let unmatched = 0;
  for (const [key, call] of calls) {
    const output = outputs.get(key),
      name = lowerAgentName(call.name ?? ""),
      waiting = WAIT.test(name),
      async = ASYNC.test(name),
      matched =
        call.times.size === 1 &&
        output?.times.size === 1 &&
        output.time >= call.time;
    if (!matched && !async) unmatched++;
    if (
      !async &&
      (matched ||
        (!waiting &&
          !output &&
          call.times.size === 1 &&
          call.duration !== null))
    ) {
      const start = waiting
        ? call.time
        : output?.duration !== null && output?.duration !== undefined
          ? output.time - output.duration * 1000n
          : call.time;
      const end =
        waiting || (output?.duration !== null && output?.duration !== undefined)
          ? output!.time
          : call.duration !== null
            ? call.time + call.duration * 1000n
            : output!.time;
      tools.push({
        owner: call.owner,
        bucket: waiting ? "waiting" : "work",
        start,
        end,
      });
    }
  }
  for (const e of events) {
    if (e.isCall && (e.callId === null || e.callId === "")) unmatched++;
    if (
      e.duration !== null &&
      e.duration >= 0n &&
      (e.isCall || e.isResult) &&
      e.name !== null &&
      !/(^|[.:/]|__)(request_user_input(_async)?|askuserquestion)$/u.test(
        lowerAgentName(e.name),
      ) &&
      (e.callId === null ||
        e.callId === "" ||
        (e.isResult && !calls.has(JSON.stringify([e.owner, e.callId]))))
    )
      tools.push({
        owner: e.owner,
        bucket: "work",
        start: e.isResult ? e.time - e.duration * 1000n : e.time,
        end: e.isResult ? e.time : e.time + e.duration * 1000n,
      });
  }
  const lifecycle = new Map<
    string,
    { owner: string; starts: bigint[]; ends: bigint[] }
  >();
  for (const m of markers) {
    if (
      !["task_started", "task_complete"].includes(String(m.marker_kind)) ||
      m.turn_id == null ||
      m.turn_id === "" ||
      m.ts == null
    )
      continue;
    const owner = JSON.stringify([
      m.workspace_id ?? identity.workspaceId,
      m.user_id ?? null,
      deliveryMachine(
        String(m.workspace_id ?? identity.workspaceId),
        String(m.origin_id),
      ),
    ]);
    const key = JSON.stringify([owner, m.turn_id]);
    let g = lifecycle.get(key);
    if (!g) {
      g = { owner, starts: [], ends: [] };
      lifecycle.set(key, g);
    }
    g[m.marker_kind === "task_started" ? "starts" : "ends"].push(
      timingMicros(m.ts),
    );
  }
  const lifetimes: Interval[] = [];
  let unmatchedLifecycle = 0;
  for (const g of lifecycle.values()) {
    const start = g.starts.reduce<bigint | null>(
        (n, t) => (n === null || t < n ? t : n),
        null,
      ),
      end = g.ends.reduce<bigint | null>(
        (n, t) => (n === null || t < n ? t : n),
        null,
      );
    if (
      g.starts.length === 1 &&
      g.ends.length === 1 &&
      start !== null &&
      end !== null &&
      end >= start
    )
      lifetimes.push({ owner: g.owner, bucket: "work", start, end });
    else unmatchedLifecycle++;
  }
  const waitsByOwner = new Map<string, Range[]>();
  for (const t of tools)
    if (t.bucket === "waiting") {
      const xs = waitsByOwner.get(t.owner) ?? [];
      xs.push(t);
      waitsByOwner.set(t.owner, xs);
    }
  for (const [owner, ranges] of waitsByOwner)
    waitsByOwner.set(owner, merge(ranges));
  const work = tools.filter((t) => t.bucket === "work"),
    waiting = tools.filter((t) => t.bucket === "waiting"),
    net = lifetimes.flatMap((l) =>
      subtract(l, waitsByOwner.get(l.owner) ?? []),
    );
  const bounds: Array<{ time: bigint; action: boolean }> = [];
  for (const e of events)
    if (e.isActivity || e.isCall || e.isResult)
      bounds.push({
        time: e.time,
        action:
          ["assistant", "tool_result"].includes(e.role) ||
          e.isCall ||
          e.isResult ||
          ["assistant", "file_edit"].includes(String(e.row.kind)) ||
          ["assistant", "assistant_message"].includes(e.eventKind),
      });
  for (const t of turns)
    if (t.role === "user" || t.role === "assistant")
      bounds.push({ time: timingMicros(t.ts), action: t.role === "assistant" });
  for (const m of markers)
    if (
      ["task_started", "task_complete"].includes(String(m.marker_kind)) &&
      m.ts != null
    )
      bounds.push({ time: timingMicros(m.ts), action: true });
  const started = bounds.reduce<bigint | null>(
      (n, b) => (n === null || b.time < n ? b.time : n),
      null,
    ),
    last = bounds
      .filter((b) => b.action)
      .reduce<
        bigint | null
      >((n, b) => (n === null || b.time > n ? b.time : n), null);
  return {
    sessionId: identity.sessionId,
    source: identity.source,
    startedAt: iso(started),
    lastActionAt: iso(last),
    workMs: work.length || lifetimes.length ? sum([...work, ...net]) : null,
    waitingMs: waiting.length ? sum(waiting) : null,
    coverage: {
      scope: "stored_session" as const,
      method: "recorded_intervals" as const,
      eventCount: events.length,
      turnCount: turns.length,
      markerCount: markers.length,
      workIntervalCount: work.length,
      waitingIntervalCount: waiting.length,
      lifecycleIntervalCount: lifetimes.length,
      unmatchedToolCallCount: unmatched,
      unmatchedLifecycleCount: unmatchedLifecycle,
    },
  };
}

/** Unknown legacy coverage cannot produce a complete public timing payload. */
export function canonicalTimingProjection(
  identity: TimingIdentity,
  rows: Array<{ table: string; row: Row }>,
  legacyComplete: boolean,
) {
  if (!legacyComplete) return null;
  const timing = canonicalTiming(identity, {
    events: rows
      .filter((r) => r.table === "convergence_events")
      .map((r) => r.row),
    markers: rows
      .filter((r) => r.table === "session_markers")
      .map((r) => r.row),
    turns: rows
      .filter((r) => r.table === "conversation_turns")
      .map((r) => r.row),
  });
  return {
    table: "session_timing",
    row: {
      org_id: identity.orgId,
      workspace_id: identity.workspaceId,
      source: identity.source,
      session_id: identity.sessionId,
      timing,
    },
  };
}
