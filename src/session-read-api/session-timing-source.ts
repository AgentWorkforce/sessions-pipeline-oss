/**
 * Where the read API's session timing comes from: the session's canonical S3
 * objects, found through its live Supabase pointers, projected exactly as Neon's
 * delivery projection did (canonical.ts canonicalSnapshot), then timed with the
 * port of relayhistory's algorithm (timing.ts). No Neon, no ClickHouse.
 *
 * Pointers are read as `bulls96_read_api` inside the request's READ ONLY,
 * tenant-bound transaction (liveness.ts TenantRunner). The role's RLS policy
 * admits only the bound org and workspace, and every statement binds them
 * again from verified auth. The query is sessions.session_directory_pointers
 * (20261102010000) without its claim check, which the tenant binding replaces:
 * same canonical-id precedence, live rows only, the same 64-row bound.
 */
import { deliveryMachine } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import type { CanonicalIdentity, CanonicalPointer } from "./canonical.ts";
import type { TenantRunner } from "./liveness.ts";
import {
  computeSessionTiming,
  TimingInputError,
  type SessionTimingPayload,
  type TimingEventRow,
  type TimingMarkerRow,
  type TimingTurnRow,
} from "./timing.ts";

export const SESSION_POINTERS_SQL = `SELECT p.org_id, p.workspace_id, p.harness,
       coalesce(p.canonical_session_id, p.session_id) AS session_id,
       coalesce(p.canonical_machine_id, p.machine_id) AS machine_id, p.user_id,
       p.storage_provider, p.bucket, p.manifest_key, p.content_sha256,
       p.raw_format, p.revision, p.deleted_at
  FROM sessions.session_index AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.harness = $3
   AND (coalesce(p.canonical_session_id, p.session_id) = $4 OR p.session_id = $4)
   AND p.deleted_at IS NULL
 ORDER BY coalesce(p.canonical_machine_id, p.machine_id)
 LIMIT 64`;

/** Whether the session has any pointer row, live or tombstoned (erased = all tombstoned). */
export const SESSION_POINTER_STATE_SQL = `SELECT count(*)::int AS pointers,
       (count(*) FILTER (WHERE p.deleted_at IS NULL))::int AS live
  FROM sessions.session_index AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.harness = $3
   AND (coalesce(p.canonical_session_id, p.session_id) = $4 OR p.session_id = $4)`;

/** Postgres returns bytea as a Buffer through pg; the canonical reader takes `\x…` hex. */
function hexDigest(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Uint8Array) return "\\x" + Array.from(value, (b) => b.toString(16).padStart(2, "0")).join("");
  return "";
}

export function sqlCanonicalPointers(run: TenantRunner) {
  return {
    async pointers(identity: CanonicalIdentity): Promise<CanonicalPointer[]> {
      const { rows } = await run(identity.orgId, identity.workspaceId, (q) =>
        q.query<Record<string, unknown>>(SESSION_POINTERS_SQL, [identity.orgId, identity.workspaceId, identity.source, identity.sessionId]),
      );
      return rows.map((r) => ({ ...(r as unknown as CanonicalPointer), content_sha256: hexDigest(r.content_sha256), deleted_at: null }));
    },
    async state(identity: CanonicalIdentity): Promise<{ pointers: number; live: number }> {
      const { rows } = await run(identity.orgId, identity.workspaceId, (q) =>
        q.query<{ pointers: number; live: number }>(SESSION_POINTER_STATE_SQL, [identity.orgId, identity.workspaceId, identity.source, identity.sessionId]),
      );
      return { pointers: Number(rows[0]?.pointers ?? 0), live: Number(rows[0]?.live ?? 0) };
    },
  };
}

export interface CanonicalRows {
  rows: Array<{ table: string; row: Record<string, unknown> }>;
  legacyCoverage: { version: 1; tables: string[] } | null;
}

export interface TimingOptions {
  /**
   * READ_API_TIMING_ASSUME_NO_TURNS. Neon's timing also reads relay
   * `conversation_turns`, which reach S3 only through the (dormant) legacy
   * export. Without that evidence the answer is exact only for a session that
   * has no turns. Off (the default): such a session is refused (501
   * `timing_turns_unknown`). On: it is timed with no turns; set it only for a
   * workspace a read-only count showed has no conversation_turns.
   */
  assumeNoTurns: boolean;
}

/**
 * Times one session from its canonical rows. Throws TimingInputError for any
 * input this backend cannot time exactly; the route maps it to a 501.
 */
export function timingFromCanonical(identity: CanonicalIdentity, snapshot: CanonicalRows, options: TimingOptions): SessionTimingPayload {
  const ws = identity.workspaceId;
  const events: TimingEventRow[] = [];
  const rawMarkers: Array<Record<string, unknown>> = [];
  const turns: TimingTurnRow[] = [];
  for (const { table, row } of snapshot.rows) {
    if (table === "convergence_events") {
      if (row.session_id !== identity.sessionId || row.source !== identity.source) throw new TimingInputError("event_scope");
      events.push({ ...(row as unknown as TimingEventRow), workspace_id: ws });
    } else if (table === "session_markers") {
      if (row.session_id !== identity.sessionId || row.source !== identity.source) throw new TimingInputError("marker_scope");
      rawMarkers.push(row);
    } else if (table === "conversation_turns") {
      // Neon's timing reads turns by their STORED source; the canonical scope holds
      // the turns publicly attributed to this source. Equal only when they agree.
      const stored = typeof row.legacy_stored_source === "string" ? row.legacy_stored_source : row.source;
      if (stored !== identity.source) throw new TimingInputError("turn_attribution");
      turns.push({ ts: String(row.ts), role: String(row.role) });
    }
  }
  if (!snapshot.legacyCoverage && !options.assumeNoTurns) throw new TimingInputError("turns_unknown");

  // Neon stamps a marker with its delivery's user, which is the user of the
  // events on the same delivery machine. The canonical catalog rows carry no
  // user, so it is resolved from the events and refused when ambiguous.
  const usersByMachine = new Map<string, Set<string | null>>();
  const allUsers = new Set<string | null>();
  for (const e of events) {
    const m = String(e.machine_id);
    const set = usersByMachine.get(m) ?? new Set();
    set.add(e.user_id ?? null);
    usersByMachine.set(m, set);
    allUsers.add(e.user_id ?? null);
  }
  const markers: TimingMarkerRow[] = rawMarkers.map((row) => {
    const origin = String(row.origin_id);
    const onMachine = usersByMachine.get(deliveryMachine(ws, origin));
    let user: string | null;
    if (onMachine && onMachine.size === 1) user = [...onMachine][0]!;
    else if (!onMachine && allUsers.size <= 1) user = allUsers.size ? [...allUsers][0]! : null;
    else throw new TimingInputError("marker_user");
    return {
      workspace_id: ws,
      user_id: user,
      origin_id: origin,
      turn_id: (row.turn_id as string | null) ?? null,
      marker_kind: (row.marker_kind as string | null) ?? null,
      ts: (row.ts as string | null) ?? null,
    };
  });

  return computeSessionTiming(
    { sessionId: identity.sessionId, source: identity.source, events, markers, turns },
    (workspace, origin) => deliveryMachine(workspace ?? ws, origin),
  );
}
