/**
 * `GET /v1/sessions/:id/turns` and `/metadata` from the session's canonical S3
 * objects (canonical.ts canonicalSnapshot, which merges the legacy Neon export
 * with live relay turns: canonical-turns.ts), shaped exactly as relayhistory
 * lib/turns.ts listConversationTurns / getSessionMetadata answer from Neon
 * `sessions.conversation_turns`. No Neon, no ClickHouse.
 *
 * Exactness, or a TurnsInputError the route turns into a 501 (Cloud reads relayhistory):
 *   - every harness scope read must carry the export's legacy coverage marker
 *     (`turns_unknown`): without it Neon may hold turns S3 has never seen;
 *   - an exported string the export had to cut (`turns_truncated`);
 *   - with `source`, a live row whose public attribution needs Neon's event
 *     tables (an untagged turn stored under its owner) (`turns_attribution`).
 * Rows are de-duplicated by Neon row id across harness scopes (an unattributed
 * legacy turn is exported under every source of its session) and ordered as
 * relayhistory orders them (turn_index, workspace, stored source, id).
 */
import { comparePublicTurnRows } from "../shared/legacy-turn-source.ts";
import type { CanonicalIdentity } from "./canonical.ts";
import type { TenantRunner } from "./liveness.ts";
import type { CanonicalRows } from "./session-timing-source.ts";

export class TurnsInputError extends Error {
  constructor(readonly reason: string) {
    super("turns input refused");
    this.name = "TurnsInputError";
  }
}

/** Harnesses with a pointer for this session id (canonical or raw), live and total counts. Bounded. */
export const SESSION_HARNESSES_SQL = `SELECT p.harness,
       count(*)::int AS pointers,
       (count(*) FILTER (WHERE p.deleted_at IS NULL))::int AS live
  FROM sessions.session_index AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2
   AND (coalesce(p.canonical_session_id, p.session_id) = $3 OR p.session_id = $3)
 GROUP BY p.harness
 ORDER BY p.harness
 LIMIT 5`;
/** Each scope read may hold up to 16 MiB; four bound one request well inside the Worker memory limit. */
export const MAX_TURN_HARNESSES = 4;

export interface HarnessState {
  harness: string;
  pointers: number;
  live: number;
}

export function sqlSessionHarnesses(run: TenantRunner) {
  return async (scope: { orgId: string; workspaceId: string; sessionId: string }): Promise<HarnessState[]> => {
    const { rows } = await run(scope.orgId, scope.workspaceId, (q) =>
      q.query<HarnessState>(SESSION_HARNESSES_SQL, [scope.orgId, scope.workspaceId, scope.sessionId]),
    );
    return rows.map((r) => ({ harness: String(r.harness), pointers: Number(r.pointers), live: Number(r.live) }));
  };
}

export interface TurnResponse {
  id: string;
  sessionId: string;
  sessionOwner: string;
  turnIndex: number;
  role: string;
  content: string;
  actorName: string;
  actorRole: string;
  metadata: unknown;
  ts: string;
}
export interface MetadataResponse {
  nativeCli: string | null;
  nativeResumeId: string | null;
  sessionOwner: string;
  originNode: string | null;
}

const EXPORT_TRUNCATION = "[neon-s3-sync: truncated ";
function truncated(value: unknown): boolean {
  if (typeof value === "string") return value.includes(EXPORT_TRUNCATION);
  if (Array.isArray(value)) return value.some(truncated);
  if (value && typeof value === "object") return Object.entries(value).some(([k, v]) => truncated(k) || truncated(v));
  return false;
}

/**
 * Postgres `timestamptz` text (to_jsonb: `2026-11-05T12:00:00.123456+00:00`) or ISO
 * (live: `...123Z`) as the JSON a JS Date gives: milliseconds, truncated, `Z`.
 */
export function neonTs(value: unknown): string {
  const m = typeof value === "string" ? /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}(?::?\d{2})?)$/u.exec(value) : null;
  if (!m) throw new TurnsInputError("ts");
  const ms = ((m[3] ?? "") + "000").slice(0, 3);
  const zone = m[4] === "Z" ? "Z" : m[4]!.length === 3 ? `${m[4]}:00` : m[4]!.includes(":") ? m[4]! : `${m[4]!.slice(0, 3)}:${m[4]!.slice(3)}`;
  const at = new Date(`${m[1]}T${m[2]}.${ms}${zone}`);
  if (Number.isNaN(at.getTime())) throw new TurnsInputError("ts");
  return at.toISOString();
}

const isLive = (row: Record<string, unknown>) => typeof row.live_revision === "string";

/** relayhistory's public attribution of a live row, when it needs no Neon event table; else null. */
function liveAttribution(row: Record<string, unknown>): string | null {
  const meta = row.metadata as Record<string, unknown>;
  for (const key of ["nativeCli", "source"]) if (typeof meta?.[key] === "string" && (meta[key] as string).length > 0) return meta[key] as string;
  if (row.source !== "" && row.source !== row.session_owner) return String(row.source);
  return null;
}

/**
 * The turn rows of a session in one workspace from its harness snapshots, with
 * relayhistory's `source` selection when given. Throws TurnsInputError.
 */
export function turnRows(identity: Omit<CanonicalIdentity, "source">, snapshots: Array<{ harness: string; snapshot: CanonicalRows }>, source: string | undefined): Record<string, unknown>[] {
  const byId = new Map<string, Record<string, unknown>>();
  for (const { harness, snapshot } of snapshots) {
    if (!snapshot.legacyCoverage) throw new TurnsInputError("turns_unknown");
    for (const { table, row } of snapshot.rows) {
      if (table !== "conversation_turns") continue;
      if (row.session_id !== identity.sessionId || row.workspace_id !== identity.workspaceId || row.org_id !== identity.orgId) throw new TurnsInputError("turn_scope");
      if (truncated(row.content) || truncated(row.metadata) || truncated(row.session_owner) || truncated(row.actor_name)) throw new TurnsInputError("turns_truncated");
      if (source !== undefined) {
        if (harness !== source) continue;
        if (isLive(row) && liveAttribution(row) !== source) throw new TurnsInputError("turns_attribution");
      }
      const id = String(row.id);
      const prior = byId.get(id);
      // The same Neon row exported under two sources is one row; a live record shares its Neon id.
      if (!prior || (isLive(row) && !isLive(prior))) byId.set(id, row);
    }
  }
  // Neon holds one row per (workspace, stored source, turn_index); a live record of a key wins.
  const byKey = new Map<string, Record<string, unknown>>();
  for (const row of byId.values()) {
    const key = JSON.stringify([row.workspace_id, row.legacy_stored_source ?? row.source, Number(row.turn_index)]);
    const prior = byKey.get(key);
    if (!prior || (isLive(row) && (!isLive(prior) || BigInt(String(row.live_revision)) > BigInt(String(prior.live_revision))))) byKey.set(key, row);
  }
  return [...byKey.values()].sort(comparePublicTurnRows);
}

export function turnsResponse(rows: Record<string, unknown>[]): TurnResponse[] {
  return rows.map((r) => {
    if (!r.metadata || typeof r.metadata !== "object" || Array.isArray(r.metadata)) throw new TurnsInputError("metadata");
    return {
      id: String(r.id),
      sessionId: String(r.session_id),
      sessionOwner: String(r.session_owner),
      turnIndex: Number(r.turn_index),
      role: String(r.role),
      content: String(r.content),
      actorName: String(r.actor_name),
      actorRole: String(r.actor_role),
      metadata: r.metadata,
      ts: neonTs(r.ts),
    };
  });
}

/** relayhistory getSessionMetadata: rows by turn_index DESC, then workspace, source, id ASC; null when none. */
export function metadataResponse(rows: Record<string, unknown>[]): MetadataResponse | null {
  if (!rows.length) return null;
  const ordered = [...rows].sort((a, b) => Number(b.turn_index) - Number(a.turn_index) || comparePublicTurnRows({ ...a, turn_index: 0 }, { ...b, turn_index: 0 }));
  const first = (key: string): string | null => {
    for (const r of ordered) {
      const m = r.metadata;
      if (!m || typeof m !== "object" || Array.isArray(m)) continue;
      const v = (m as Record<string, unknown>)[key];
      if (typeof v === "string" && v.length > 0) return v;
    }
    return null;
  };
  return { nativeCli: first("nativeCli"), nativeResumeId: first("nativeResumeId"), sessionOwner: String(ordered[0]!.session_owner), originNode: first("originNode") };
}
