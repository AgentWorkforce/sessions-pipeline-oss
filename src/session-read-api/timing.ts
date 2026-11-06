/**
 * Recorded session timing (relayhistory `GET /v1/sessions/:id/timing`) computed
 * from a session's canonical rows, with no Neon.
 *
 * This is a statement-for-statement port of relayhistory's
 * `sessions.session_timing_compute` version 1 (relayhistory-cloud
 * db/migrations/0033_session_timing_cache.sql, vendored verbatim as the test
 * oracle in __tests__/fixtures/relayhistory-session-timing-compute.sql). The
 * parity suite runs that SQL in PGlite over the same rows and requires an
 * identical payload. Keep the CTE names in the comments below aligned with the
 * SQL when changing anything here; any semantic change is a new algorithm
 * version on both sides, never a quiet fix on one.
 *
 * Inputs are the three tables the SQL reads, for ONE (org, workspace, source,
 * session): `convergence_events`, `session_markers` and `conversation_turns`.
 * The read API takes them from the session's canonical S3 projection (the same
 * delivery projection Neon ran; canonical-projection.ts) and, for turns, from
 * the canonical legacy evidence when the export carried it (canonical-legacy.ts).
 *
 * Timestamps are compared and summed in integer microseconds, the resolution of
 * Postgres `timestamptz`, so interval arithmetic is exact. Anything the SQL
 * would refuse (an inverted range) or that this port cannot represent exactly
 * (an unparseable timestamp, a year past 9999) throws TimingInputError and the
 * route answers 501, so the caller reads relayhistory instead of a guess.
 */

export const TIMING_ALGORITHM_VERSION = 1;

export class TimingInputError extends Error {
  constructor(readonly reason: string) {
    super(`session timing input refused: ${reason}`);
    this.name = "TimingInputError";
  }
}

/** A `sessions.convergence_events` row, as the projection emits it. */
export interface TimingEventRow {
  workspace_id?: string | null;
  user_id?: string | null;
  machine_id?: string | null;
  kind: string;
  type?: string | null;
  ts: string;
  actor_role?: string | null;
  tool_name?: string | null;
  duration_ms?: number | string | null;
  record?: unknown;
  tool_calls?: unknown;
}

/** A `sessions.session_markers` row. `origin_id` is the raw delivery origin. */
export interface TimingMarkerRow {
  workspace_id?: string | null;
  user_id?: string | null;
  origin_id: string;
  turn_id?: string | null;
  marker_kind?: string | null;
  ts?: string | null;
}

/** A `sessions.conversation_turns` row (only ts and role are read). */
export interface TimingTurnRow {
  ts: string;
  role: string;
}

export interface SessionTimingPayload {
  sessionId: string;
  source: string;
  startedAt: string | null;
  lastActionAt: string | null;
  workMs: number | null;
  waitingMs: number | null;
  coverage: {
    scope: "stored_session";
    method: "recorded_intervals";
    eventCount: number;
    turnCount: number;
    markerCount: number;
    workIntervalCount: number;
    waitingIntervalCount: number;
    lifecycleIntervalCount: number;
    unmatchedToolCallCount: number;
    unmatchedLifecycleCount: number;
  };
}

type Json = Record<string, unknown>;

/** Postgres `timestamptz` text or ISO, UTC only, to integer epoch microseconds. */
export function timestampMicros(value: unknown): number {
  if (typeof value !== "string") throw new TimingInputError("timestamp");
  const m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?:Z|\+00(?::?00)?)$/u.exec(value);
  if (!m) throw new TimingInputError("timestamp");
  const [, y, mo, d, h, mi, s, f = ""] = m;
  const ms = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s));
  if (!Number.isFinite(ms)) throw new TimingInputError("timestamp");
  const check = new Date(ms);
  if (check.getUTCFullYear() !== Number(y) || check.getUTCMonth() !== Number(mo) - 1 || check.getUTCDate() !== Number(d))
    throw new TimingInputError("timestamp");
  const micros = ms * 1000 + Number(f.padEnd(6, "0"));
  if (!Number.isSafeInteger(micros)) throw new TimingInputError("timestamp");
  return micros;
}

/** `to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`: milliseconds, truncated. */
function formatMicros(micros: number): string {
  const ms = Math.floor(micros / 1000);
  const iso = new Date(ms).toISOString();
  if (!/^\d{4}-/u.test(iso)) throw new TimingInputError("timestamp_range");
  return iso;
}

/** jsonb `->>`: SQL NULL for a missing key or JSON null, the text of anything else. */
function jsonText(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

/** jsonb `obj->>key`, NULL unless `obj` is a JSON object. */
function field(obj: unknown, key: string): string | null {
  return obj !== null && typeof obj === "object" && !Array.isArray(obj) ? jsonText((obj as Json)[key]) : null;
}

const nullif = (value: string | null): string | null => (value === "" ? null : value);
const coalesce = (...values: Array<string | null | undefined>): string | null => {
  for (const v of values) if (v !== null && v !== undefined) return v;
  return null;
};
const lower = (value: string | null): string | null => (value === null ? null : value.toLowerCase());

function durationMs(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(n)) throw new TimingInputError("duration_ms");
  return n;
}

/** `md5(jsonb_build_array(workspace_id, origin_id)::text)`, the delivery machine of an origin. */
export type OriginMachine = (workspaceId: string | null, originId: string) => string;

const CALL_KINDS = new Set(["custom_tool_call", "function_call", "mcp_tool_call", "tool_use", "tool-call", "tool_call"]);
const RESULT_KINDS = new Set(["tool_result", "tool-result", "tool_call_output", "function_call_output", "custom_tool_call_output", "mcp_tool_call_output"]);
const RESULT_KINDS_FOR_IS_RESULT = new Set(["custom_tool_call_output", "function_call_output", "mcp_tool_call_output", "tool_result", "tool-result", "tool_call_output"]);
const ACTIVITY_KINDS = new Set(["prompt", "user", "assistant", "history", "session_event", "tool_call", "file_edit", "trajectory", "task"]);
const NON_ACTIVITY_EVENT_KINDS = new Set(["session_meta", "session_metadata", "metadata", "discovery", "presence", "system", "system_message"]);
const WAIT = /(^|[.:/]|__)(request_user_input|askuserquestion)$/u;
const ASYNC = /(^|[.:/]|__)request_user_input_async$/u;
const WAIT_OR_ASYNC = /(^|[.:/]|__)(request_user_input(_async)?|askuserquestion)$/u;

interface Classified {
  workspace: string | null;
  user: string | null;
  machine: string | null;
  kind: string;
  ts: number;
  durationMs: number | null;
  eventKind: string;
  callId: string | null;
  name: string | null;
  role: string;
  isCall: boolean;
  isResult: boolean;
  isActivity: boolean;
}

type Range = [number, number];

/** `range_agg`: the union of non-empty half-open ranges, merging overlaps and adjacency. */
function union(ranges: Range[]): Range[] {
  const sorted = ranges.filter(([lo, hi]) => lo < hi).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const out: Range[] = [];
  for (const [lo, hi] of sorted) {
    const last = out[out.length - 1];
    if (last && lo <= last[1]) last[1] = Math.max(last[1], hi);
    else out.push([lo, hi]);
  }
  return out;
}

/** Multirange difference `a - b` over unions. */
function subtract(a: Range[], b: Range[]): Range[] {
  const out: Range[] = [];
  for (const [lo0, hi] of a) {
    let lo = lo0;
    for (const [blo, bhi] of b) {
      if (bhi <= lo || blo >= hi) continue;
      if (blo > lo) out.push([lo, blo]);
      lo = Math.max(lo, bhi);
      if (lo >= hi) break;
    }
    if (lo < hi) out.push([lo, hi]);
  }
  return out;
}

/** `tstzrange(lo, hi, '[)')`: Postgres refuses lo > hi. */
function range(lo: number, hi: number): Range {
  if (lo > hi) throw new TimingInputError("range_bounds");
  return [lo, hi];
}

const key = (...parts: Array<string | null>) => JSON.stringify(parts);

export function computeSessionTiming(
  input: { sessionId: string; source: string; events: TimingEventRow[]; markers: TimingMarkerRow[]; turns: TimingTurnRow[] },
  originMachine: OriginMachine,
): SessionTimingPayload {
  // event_rows / normalized / classified
  const classified: Classified[] = input.events.map((row) => {
    const record = row.record;
    const payload =
      record !== null && typeof record === "object" && !Array.isArray(record) && "payload" in (record as Json)
        ? (record as Json).payload
        : record;
    const toolCalls = row.tool_calls;
    const first = Array.isArray(toolCalls) ? toolCalls[0] : undefined;
    const eventKind = lower(coalesce(nullif(field(payload, "kind")), nullif(row.type ?? null), row.kind))!;
    const callId = coalesce(
      nullif(field(payload, "tool_use_id")),
      nullif(field(payload, "toolUseId")),
      nullif(field(payload, "call_id")),
      nullif(field(payload, "callId")),
      Array.isArray(toolCalls) && toolCalls.length === 1
        ? coalesce(field(first, "toolUseId"), field(first, "tool_use_id"), field(first, "call_id"), field(first, "id"))
        : null,
    );
    const name = coalesce(nullif(row.tool_name ?? null), nullif(field(payload, "tool_name")), nullif(field(payload, "name")), field(first, "name"));
    const role = lower(coalesce(field(payload, "role"), row.actor_role ?? null, ""))!;
    const isCall = role !== "tool_result" && (CALL_KINDS.has(eventKind) || (row.kind === "tool_call" && !RESULT_KINDS.has(eventKind)));
    const isResult = RESULT_KINDS_FOR_IS_RESULT.has(eventKind) || role === "tool_result";
    const isActivity = ACTIVITY_KINDS.has(row.kind) && role !== "system" && !NON_ACTIVITY_EVENT_KINDS.has(eventKind);
    return {
      workspace: row.workspace_id ?? null,
      user: row.user_id ?? null,
      machine: row.machine_id ?? null,
      kind: row.kind,
      ts: timestampMicros(row.ts),
      durationMs: durationMs(row.duration_ms),
      eventKind,
      callId,
      name,
      role,
      isCall,
      isResult,
      isActivity,
    };
  });

  // calls / outputs
  interface Group { workspace: string | null; user: string | null; machine: string | null; callId: string; at: number; name: string | null; durationMs: number | null; stamps: Set<number> }
  const group = (rows: Classified[], pickMin: boolean) => {
    const out = new Map<string, Group>();
    for (const r of rows) {
      if (r.callId === null || r.callId === "") continue;
      const k = key(r.workspace, r.user, r.machine, r.callId);
      let g = out.get(k);
      if (!g) {
        g = { workspace: r.workspace, user: r.user, machine: r.machine, callId: r.callId, at: r.ts, name: null, durationMs: null, stamps: new Set() };
        out.set(k, g);
      }
      if (pickMin && r.ts < g.at) g.at = r.ts;
      if (r.name !== null && (g.name === null || r.name > g.name)) g.name = r.name;
      if (r.durationMs !== null && r.durationMs >= 0 && (g.durationMs === null || r.durationMs > g.durationMs)) g.durationMs = r.durationMs;
      g.stamps.add(r.ts);
    }
    return out;
  };
  const calls = group(classified.filter((r) => r.isCall), true);
  const outputs = group(classified.filter((r) => r.isResult), true);

  // paired
  const paired = [...calls.entries()].map(([k, c]) => {
    const o = outputs.get(k);
    const startCount = c.stamps.size;
    const matched: boolean | null = o
      ? startCount === 1 && o.stamps.size === 1 && o.at >= c.at
      : startCount === 1 ? null : false;
    const name = lower(c.name ?? "") ?? "";
    return {
      ...c,
      startCount,
      endedAt: o ? o.at : null,
      resultDurationMs: o ? o.durationMs : null,
      matched,
      isWait: WAIT.test(name),
      isAsync: ASYNC.test(name),
    };
  });

  // tool_intervals
  interface Interval { workspace: string | null; user: string | null; origin: string | null; bucket: "work" | "waiting"; lo: number; hi: number }
  const toolIntervals: Interval[] = [];
  const MS = 1000;
  for (const p of paired) {
    if (p.isAsync) continue;
    if (!(p.matched === true || (!p.isWait && p.endedAt === null && p.startCount === 1 && p.durationMs !== null))) continue;
    const lo = p.isWait ? p.at : p.resultDurationMs !== null ? p.endedAt! - p.resultDurationMs * MS : p.at;
    const hi = p.isWait || p.resultDurationMs !== null ? p.endedAt! : p.durationMs !== null ? p.at + p.durationMs * MS : p.endedAt!;
    const [rlo, rhi] = range(lo, hi);
    toolIntervals.push({ workspace: p.workspace, user: p.user, origin: p.machine, bucket: p.isWait ? "waiting" : "work", lo: rlo, hi: rhi });
  }
  for (const r of classified) {
    if (r.durationMs === null || r.durationMs < 0 || !(r.isCall || r.isResult) || r.name === null) continue;
    if (WAIT_OR_ASYNC.test(r.name.toLowerCase())) continue;
    const orphanResult = r.isResult && !calls.has(key(r.workspace, r.user, r.machine, r.callId));
    if (!(r.callId === null || r.callId === "" || orphanResult)) continue;
    const lo = r.isResult ? r.ts - r.durationMs * MS : r.ts;
    const hi = r.isResult ? r.ts : r.ts + r.durationMs * MS;
    const [rlo, rhi] = range(lo, hi);
    toolIntervals.push({ workspace: r.workspace, user: r.user, origin: r.machine, bucket: "work", lo: rlo, hi: rhi });
  }

  // markers / lifecycle / lifecycle_intervals
  const markers = input.markers.map((m) => ({
    workspace: m.workspace_id ?? null,
    user: m.user_id ?? null,
    origin: originMachine(m.workspace_id ?? null, m.origin_id),
    turnId: m.turn_id ?? null,
    kind: m.marker_kind ?? null,
    ts: m.ts === null || m.ts === undefined ? null : timestampMicros(m.ts),
  }));
  interface Life { workspace: string | null; user: string | null; origin: string; start: number | null; end: number | null; starts: number; ends: number }
  const lifecycle = new Map<string, Life>();
  for (const m of markers) {
    if ((m.kind !== "task_started" && m.kind !== "task_complete") || m.turnId === null || m.turnId === "" || m.ts === null) continue;
    const k = key(m.workspace, m.user, m.origin, m.turnId);
    let l = lifecycle.get(k);
    if (!l) lifecycle.set(k, (l = { workspace: m.workspace, user: m.user, origin: m.origin, start: null, end: null, starts: 0, ends: 0 }));
    if (m.kind === "task_started") {
      l.starts += 1;
      if (l.start === null || m.ts < l.start) l.start = m.ts;
    } else {
      l.ends += 1;
      if (l.end === null || m.ts < l.end) l.end = m.ts;
    }
  }
  const lifecycleIntervals = [...lifecycle.values()]
    .filter((l) => l.starts === 1 && l.ends === 1 && l.end! >= l.start!)
    .map((l) => ({ workspace: l.workspace, user: l.user, origin: l.origin, lo: l.start!, hi: l.end! }));

  // lifecycle_ranges / waiting_ranges / net_ranges / merged_ranges / durations
  const byOrigin = <T extends { workspace: string | null; user: string | null; origin: string | null; lo: number; hi: number }>(rows: T[]) => {
    const out = new Map<string, Range[]>();
    for (const r of rows) {
      const k = key(r.workspace, r.user, r.origin);
      const list = out.get(k) ?? [];
      list.push(range(r.lo, r.hi));
      out.set(k, list);
    }
    return new Map([...out].map(([k, v]) => [k, union(v)]));
  };
  const lifecycleRanges = byOrigin(lifecycleIntervals);
  const waitingRanges = byOrigin(toolIntervals.filter((t) => t.bucket === "waiting"));
  const net: Record<"work" | "waiting", Range[]> = { work: [], waiting: [] };
  for (const [k, spans] of lifecycleRanges) net.work.push(...subtract(spans, waitingRanges.get(k) ?? []));
  for (const t of toolIntervals) net[t.bucket].push([t.lo, t.hi]);
  const sumMs = (spans: Range[]) => union(spans).reduce((total, [lo, hi]) => total + (hi - lo), 0) / MS;

  // turns / bounds
  const turns = input.turns.map((t) => ({ ts: timestampMicros(t.ts), role: t.role }));
  const bounds: Array<{ ts: number; action: boolean }> = [];
  for (const r of classified) {
    if (!(r.isActivity || r.isCall || r.isResult)) continue;
    bounds.push({
      ts: r.ts,
      action: r.role === "assistant" || r.role === "tool_result" || r.isCall || r.isResult || r.kind === "assistant" || r.kind === "file_edit" || r.eventKind === "assistant" || r.eventKind === "assistant_message",
    });
  }
  for (const t of turns) if (t.role === "user" || t.role === "assistant") bounds.push({ ts: t.ts, action: t.role === "assistant" });
  for (const m of markers) if ((m.kind === "task_started" || m.kind === "task_complete") && m.ts !== null) bounds.push({ ts: m.ts, action: true });

  let startedAt: number | null = null;
  let lastActionAt: number | null = null;
  for (const b of bounds) {
    if (startedAt === null || b.ts < startedAt) startedAt = b.ts;
    if (b.action && (lastActionAt === null || b.ts > lastActionAt)) lastActionAt = b.ts;
  }
  const hasWork = toolIntervals.some((t) => t.bucket === "work") || lifecycleIntervals.length > 0;
  const hasWaiting = toolIntervals.some((t) => t.bucket === "waiting");
  return {
    sessionId: input.sessionId,
    source: input.source,
    startedAt: startedAt === null ? null : formatMicros(startedAt),
    lastActionAt: lastActionAt === null ? null : formatMicros(lastActionAt),
    workMs: hasWork ? sumMs(net.work) : null,
    waitingMs: hasWaiting ? sumMs(net.waiting) : null,
    coverage: {
      scope: "stored_session",
      method: "recorded_intervals",
      eventCount: input.events.length,
      turnCount: turns.length,
      markerCount: markers.length,
      workIntervalCount: toolIntervals.filter((t) => t.bucket === "work").length,
      waitingIntervalCount: toolIntervals.filter((t) => t.bucket === "waiting").length,
      lifecycleIntervalCount: lifecycleIntervals.length,
      unmatchedToolCallCount:
        paired.filter((p) => p.matched !== true && !p.isAsync).length +
        classified.filter((r) => r.isCall && (r.callId === null || r.callId === "")).length,
      unmatchedLifecycleCount: [...lifecycle.values()].filter((l) => l.starts !== 1 || l.ends !== 1 || l.end! < l.start!).length,
    },
  };
}
