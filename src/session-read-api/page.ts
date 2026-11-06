/**
 * One event page: candidates, newest versions, tenant recheck, byte budget,
 * erasure filter, then the relayhistory event shape.
 *
 * Pagination never truncates silently (relayhistory-cloud #251): a page that
 * the byte budget cuts carries a cursor at its last kept row, and a page whose
 * candidate scan filled `limit` carries a cursor at the last candidate even when
 * fewer rows survived the newest-version recheck. A page can therefore be short
 * (or empty) with a cursor; it is never short without one while rows remain.
 */
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { encodeCursor, type EventPosition } from "./cursor.ts";
import {
  READ_SETTINGS,
  buildCandidateQuery,
  buildNewestQuery,
  buildPartialSessionsQuery,
  buildWorkspacePartialSessionsQuery,
  PARTIAL_SESSIONS_CAP,
  candidatePosition,
  type CandidateRow,
  type EventQuery,
  type EventRow,
} from "./queries.ts";

/** relayhistory's maxContent marker (lib/recall.ts CONTENT_TRUNCATION_NOTE). */
export const CONTENT_TRUNCATION_NOTE = "…[truncated by maxContent]";

export class TenantMismatchError extends Error {
  constructor() {
    super("a row outside the authorized tenant reached the read path");
    this.name = "TenantMismatchError";
  }
}

export interface SessionKey {
  source: string;
  sessionId: string;
}

/** Supabase-side checks. Only ids cross this boundary. */
export interface Liveness {
  /** The subset of `keys` whose every pointer row is tombstoned (erased). */
  erased(orgId: string, workspaceId: string, keys: SessionKey[]): Promise<Set<string>>;
  /**
   * The session's first activity across its live pointer rows (all harnesses when
   * `source` is omitted), whether any pointer row exists, and whether all are erased.
   */
  sessionStart(orgId: string, workspaceId: string, sessionId: string, source?: string): Promise<{ pointers: number; live: number; createdAtMs: number | null }>;
  /** The subset of `sessionIds` with at least one live pointer row in this workspace (any harness). */
  livePointerSessions(orgId: string, workspaceId: string, sessionIds: string[]): Promise<Set<string>>;
}

export const keyOf = (k: SessionKey) => `${k.source}\u0000${k.sessionId}`;

export interface ShapedEvent {
  eventId: string;
  userId: string | null;
  sessionId: string;
  source: string;
  kind: string;
  type: string;
  ts: string;
  actorName: string | null;
  actorRole: string | null;
  subagentId: string | null;
  machineId: string;
  projectId: string | null;
  trajectoryId: null;
  taskRef: Record<string, unknown>;
  taskTitle: string | null;
  content: string | null;
  contentTruncated: boolean;
  tags: string[];
  toolName: string | null;
  toolStatus: string | null;
  filesTouched: string[];
}

export interface EventPage {
  events: ShapedEvent[];
  nextCursor: string | null;
  /** Rows dropped because their session is erased (telemetry only). */
  erasedDropped: number;
}

const nullable = (v: string | null | undefined) => (v === undefined || v === null || v === "" ? null : v);

function parseTaskRef(raw: string): Record<string, unknown> {
  if (!raw) return {};
  try {
    const v = JSON.parse(raw) as unknown;
    return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

export function shapeRow(row: EventRow, maxContent?: number): ShapedEvent {
  let content: string | null = row.content;
  let contentTruncated = false;
  if (maxContent !== undefined && maxContent > 0 && typeof content === "string" && content.length > maxContent) {
    content = `${content.slice(0, maxContent)}${CONTENT_TRUNCATION_NOTE}`;
    contentTruncated = true;
  }
  return {
    eventId: row.event_id,
    userId: nullable(row.user_id),
    sessionId: row.session_id,
    source: row.source,
    kind: row.kind,
    type: row.type || row.kind,
    ts: new Date(Number(row.ts_ms)).toISOString(),
    actorName: nullable(row.actor_name),
    actorRole: nullable(row.actor_role),
    subagentId: nullable(row.subagent_id),
    machineId: row.machine_id,
    projectId: nullable(row.project_id),
    trajectoryId: null,
    taskRef: parseTaskRef(row.task_ref),
    taskTitle: nullable(row.task_title),
    content,
    contentTruncated,
    tags: Array.isArray(row.tags) ? row.tags : [],
    toolName: nullable(row.tool_name),
    toolStatus: nullable(row.tool_status),
    filesTouched: Array.isArray(row.files_touched) ? row.files_touched : [],
  };
}

const rowPosition = (r: EventRow): EventPosition => ({
  tsMs: Number(r.ts_ms),
  sessionId: r.session_id,
  source: r.source,
  machineId: r.machine_id,
  kind: r.kind,
  eventId: r.event_id,
});

export interface PageDeps {
  ch: ClickHouseClient;
  liveness: Liveness;
  timeoutMs?: number;
}

export async function readEventPage(deps: PageDeps, input: EventQuery): Promise<EventPage> {
  const options = { settings: { ...READ_SETTINGS }, timeoutMs: deps.timeoutMs ?? 3_000 };
  const c = buildCandidateQuery(deps.ch.database, input);
  const candidateRows = await deps.ch.select<CandidateRow>(c.query, { ...options, params: c.params });
  const candidates = candidateRows.map(candidatePosition);
  if (candidates.length === 0) return { events: [], nextCursor: null, erasedDropped: 0 };

  const n = buildNewestQuery(deps.ch.database, input, candidates);
  const rows = await deps.ch.select<EventRow>(n.query, { ...options, params: n.params });

  // Defense in depth: every row must belong to the verified tenant and the
  // requested session/source, whatever the SQL said.
  for (const r of rows) {
    if (r.org_id !== input.orgId || r.workspace_id !== input.workspaceId) throw new TenantMismatchError();
    if (input.sessionId !== undefined && r.session_id !== input.sessionId) throw new TenantMismatchError();
    if (input.source !== undefined && r.source !== input.source) throw new TenantMismatchError();
  }

  let kept = rows;
  let nextCursor: string | null = null;
  const cut = rows.findIndex((r) => Number(r.in_budget) !== 1);
  if (cut > 0) {
    kept = rows.slice(0, cut);
    nextCursor = encodeCursor(rowPosition(kept[kept.length - 1]!));
  } else if (candidates.length >= input.limit) {
    nextCursor = encodeCursor(candidates[candidates.length - 1]!);
  }

  const distinct = new Map<string, SessionKey>();
  for (const r of kept) distinct.set(keyOf({ source: r.source, sessionId: r.session_id }), { source: r.source, sessionId: r.session_id });
  const erased = distinct.size ? await deps.liveness.erased(input.orgId, input.workspaceId, [...distinct.values()]) : new Set<string>();
  const live = kept.filter((r) => !erased.has(keyOf({ source: r.source, sessionId: r.session_id })));
  return {
    events: live.map((r) => shapeRow(r, input.maxContent)),
    nextCursor,
    erasedDropped: kept.length - live.length,
  };
}

/**
 * Partial sessions (`needs_full = 1`, queries.ts buildPartialSessionsQuery):
 * the one session when `sessionId` is given, else any of the org's, capped.
 */
export async function partialSessions(deps: PageDeps, orgId: string, sessionId?: string): Promise<string[]> {
  const q = buildPartialSessionsQuery(deps.ch.database, orgId, sessionId);
  const rows = await deps.ch.select<{ session_id: string }>(q.query, {
    settings: { ...READ_SETTINGS },
    timeoutMs: deps.timeoutMs ?? 3_000,
    params: q.params,
  });
  return rows.map((r) => r.session_id);
}

export interface WorkspacePartials {
  /** Partial sessions that may hold this workspace's rows. */
  sessions: string[];
  /** The org has more partial sessions than one check lists: not decided. */
  overflow: boolean;
}

/**
 * The partial sessions a workspace search must not answer without: those of
 * the token's org that belong to the token's workspace. The state table has
 * no workspace, so a partial session belongs to this workspace when either
 *
 *   - it has a row (any version) in this workspace's `transcript_events`, or
 *   - it has a live pointer row in this workspace's `session_index`.
 *
 * The second covers a session whose passes were all partial and so wrote no
 * row (search-index.ts indexSearchScope writes only the state row then). The
 * pipeline indexes a scope only from its live pointers, and every Neon session
 * reaches S3 through the exporter's pointers, so a session holding content of
 * this workspace has a row here or a live pointer here. A session with neither
 * belongs to another workspace (whose search it still blocks), or is erased
 * here, where the erasure filter drops its rows anyway. Candidate rows alone
 * would not do: the events a partial session is missing are not candidates.
 *
 * The check stops at the first finding: an in-workspace row decides without
 * asking Supabase.
 */
export async function partialSessionsInWorkspace(deps: PageDeps, orgId: string, workspaceId: string): Promise<WorkspacePartials> {
  const q = buildWorkspacePartialSessionsQuery(deps.ch.database, orgId, workspaceId);
  const rows = await deps.ch.select<{ session_id: string; in_workspace: number | string }>(q.query, {
    settings: { ...READ_SETTINGS },
    timeoutMs: deps.timeoutMs ?? 3_000,
    params: q.params,
  });
  if (rows.length > PARTIAL_SESSIONS_CAP) return { sessions: [], overflow: true };
  const inWorkspace = rows.filter((r) => Number(r.in_workspace) === 1).map((r) => r.session_id);
  if (inWorkspace.length > 0) return { sessions: inWorkspace, overflow: false };
  if (rows.length === 0) return { sessions: [], overflow: false };
  const pointed = await deps.liveness.livePointerSessions(orgId, workspaceId, rows.map((r) => r.session_id));
  return { sessions: rows.map((r) => r.session_id).filter((id) => pointed.has(id)), overflow: false };
}
