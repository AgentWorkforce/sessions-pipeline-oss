/**
 * Read-only, paged Neon reads for the continuous Neon to S3 export.
 *
 * Every statement runs in its own short `BEGIN READ ONLY` transaction with a
 * statement and lock timeout, and every scan is a keyset page over an existing
 * index, so no read holds a snapshot or a lock for long and the largest
 * session (300k+ events) is read in bounded pages instead of one statement.
 *
 * The stream lines are the ones `buildSessionStream` (relayhistory-export.ts)
 * writes, byte for byte: each line is produced by that function. Within one
 * machine stream the order is RECORD_KINDS order, then `(origin_id,
 * record_id)`, then the undelivered events by `(machine_id, kind, event_id)`.
 * That order is deterministic and pageable; the RelayHistory transform orders
 * survivors by event time itself, so it does not depend on the stream order.
 */
import {
  RECORD_KINDS,
  buildSessionStream,
  type DeliveryRecord,
  type Queryable,
} from "../relayhistory-export.ts";
import { MAX_SCRUB_CHARS } from "@rhc/lib/scrub.ts";
import { canScrubLong } from "../../shared/vendor.ts";

/**
 * Every delivery kind the export carries: #40's RECORD_KINDS plus `trajectory`
 * (an activity kind) and `commit_link` (a catalog kind), which the projection
 * reads and #40's list left out.
 */
export const EXPORT_KINDS = [...RECORD_KINDS, "trajectory", "commit_link"] as const;

/** Kinds whose winner the catalog acceptance ledger settles (as in #78's reader). */
const CATALOG_KINDS = new Set(["session", "relationship", "session_marker", "commit_link"]);

export const DEFAULT_PAGE_ROWS = 500;
export const SWEEP_PAGE_ROWS = 5000;

export interface ReadLimits {
  statementTimeoutMs: number;
  lockTimeoutMs: number;
}

export const DEFAULT_READ_LIMITS: ReadLimits = { statementTimeoutMs: 20_000, lockTimeoutMs: 2_000 };

/** Called after every page: rows and payload bytes read, for the throttle. */
export type PageHook = (rows: number, bytes: number) => Promise<void>;

/**
 * Runs one statement inside `BEGIN READ ONLY` with `SET LOCAL` timeouts, then
 * commits (nothing was written; a read-only transaction cannot write).
 */
export function readOnly(client: Queryable, limits: ReadLimits = DEFAULT_READ_LIMITS): Queryable {
  if (limits.statementTimeoutMs > 20_000) throw new Error("statement timeout must be at most 20 s");
  return {
    async query<T>(sql: string, params?: unknown[]) {
      await client.query("BEGIN READ ONLY");
      try {
        await client.query(`SET LOCAL statement_timeout = ${Math.floor(limits.statementTimeoutMs)}`);
        await client.query(`SET LOCAL lock_timeout = ${Math.floor(limits.lockTimeoutMs)}`);
        await client.query("SET LOCAL TimeZone = 'UTC'");
        const result = await client.query<T>(sql, params);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw error;
      }
    },
  };
}

/**
 * The same guarantees for a dedicated (direct, not pooled) connection in one
 * round trip per statement: every transaction on it is read-only by default,
 * with the statement and lock timeouts and UTC timestamps (the stream carries
 * `received_at` text, so the zone must not vary between runs). Verified, not
 * assumed: refuses to continue unless Postgres reports the session read-only.
 * Do not use through a transaction pooler, where session settings leak.
 */
export async function sessionReadOnly(client: Queryable, limits: ReadLimits = DEFAULT_READ_LIMITS): Promise<Queryable> {
  if (limits.statementTimeoutMs > 20_000) throw new Error("statement timeout must be at most 20 s");
  await client.query(
    `SET default_transaction_read_only = on; SET statement_timeout = ${Math.floor(limits.statementTimeoutMs)};
     SET lock_timeout = ${Math.floor(limits.lockTimeoutMs)}; SET idle_in_transaction_session_timeout = 30000;
     SET TimeZone = 'UTC'`,
  );
  const { rows } = await client.query<{ ro: string; tz: string }>(
    "SELECT current_setting('transaction_read_only') AS ro, current_setting('TimeZone') AS tz",
  );
  if (rows[0]?.ro !== "on" || rows[0]?.tz !== "UTC") throw new Error("the Neon session is not read-only UTC");
  return client;
}

export interface GroupRevision {
  org_id: string;
  session_id: string;
  revision: string;
}

/** `session_revisions` in primary-key order: the change detector (revision bumps on every event insert, update or delete). */
export async function revisionsPage(
  db: Queryable,
  after: { org_id: string; session_id: string } | null,
  limit = SWEEP_PAGE_ROWS,
): Promise<GroupRevision[]> {
  const { rows } = await db.query<GroupRevision>(
    `SELECT org_id, session_id, revision::text AS revision
       FROM sessions.session_revisions
      ${after ? "WHERE (org_id, session_id) > ($2, $3)" : ""}
      ORDER BY org_id, session_id
      LIMIT $1`,
    after ? [limit, after.org_id, after.session_id] : [limit],
  );
  return rows;
}

export interface GroupActivity {
  org_id: string;
  session_id: string;
  last_ms: string;
  events: string;
}

/** Rollups per `(org, session)` in key order, for ordering the backlog newest first. */
export async function activityPage(
  db: Queryable,
  after: { org_id: string; session_id: string } | null,
  limit = SWEEP_PAGE_ROWS,
): Promise<GroupActivity[]> {
  const { rows } = await db.query<GroupActivity>(
    `SELECT org_id, session_id,
            floor(extract(epoch FROM max(last_ts)) * 1000)::bigint::text AS last_ms,
            coalesce(sum(event_count), 0)::text AS events
       FROM sessions.session_rollups
      WHERE workspace_id IS NOT NULL AND source IS NOT NULL
      ${after ? "AND (org_id, session_id) > ($2, $3)" : ""}
      GROUP BY org_id, session_id
      ORDER BY org_id, session_id
      LIMIT $1`,
    after ? [limit, after.org_id, after.session_id] : [limit],
  );
  return rows;
}

/**
 * Sessions in `session_catalog` in key order, as `(org, session)` activity
 * rows. A session can have delivery records (its catalog record, presence,
 * markers, even events not yet projected) and no rollup: rollups come only
 * from convergence events. These must be exported too (8,610 in production
 * on 2026-11-01), so discovery reads the catalog as well as the rollups.
 */
export async function catalogPage(
  db: Queryable,
  after: { org_id: string; workspace_id: string; source: string; session_id: string } | null,
  limit = SWEEP_PAGE_ROWS,
): Promise<(GroupActivity & { workspace_id: string; source: string })[]> {
  const { rows } = await db.query<GroupActivity & { workspace_id: string; source: string }>(
    `SELECT org_id, workspace_id, source, session_id,
            floor(extract(epoch FROM coalesce(last_activity_at, first_activity_at, received_at)) * 1000)::bigint::text AS last_ms,
            '0' AS events
       FROM sessions.session_catalog
      ${after ? "WHERE (org_id, workspace_id, source, session_id) > ($2, $3, $4, $5)" : ""}
      ORDER BY org_id, workspace_id, source, session_id
      LIMIT $1`,
    after ? [limit, after.org_id, after.workspace_id, after.source, after.session_id] : [limit],
  );
  return rows;
}

/**
 * Whether Neon still has anything to export for a group. Legacy-only evidence
 * (conversation turns, links, outcomes) counts only with `includeLegacy`
 * (the eraser's `S3X_ERASER_RECOGNIZES_LEGACY`), so the default guard is the
 * rollup/catalog check the running eraser already uses.
 */
export async function groupExistsInNeon(db: Queryable, orgId: string, sessionId: string, includeLegacy = false): Promise<boolean> {
  const { rows } = await db.query(
    `SELECT 1 FROM sessions.session_rollups WHERE org_id = $1 AND session_id = $2
     UNION ALL
     SELECT 1 FROM sessions.session_catalog WHERE org_id = $1 AND session_id = $2${
       includeLegacy
         ? `
     UNION ALL SELECT 1 FROM sessions.conversation_turns WHERE org_id=$1 AND session_id=$2
     UNION ALL SELECT 1 FROM sessions.session_links WHERE org_id=$1 AND session_id=$2
     UNION ALL SELECT 1 FROM sessions.session_outcomes WHERE org_id=$1 AND session_id=$2`
         : ""
     }
     LIMIT 1`,
    [orgId, sessionId],
  );
  return rows.length > 0;
}

/** One exported unit: a session's records in one workspace and harness. */
export interface SessionUnit {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
  firstMs: number;
  lastMs: number;
  /** Rollup machine ids; the non-delivery ones carry undelivered events. */
  machineIds: string[];
  /** Neon rollup totals, written into the export marker's catalog. */
  totals: Record<string, string | string[]>;
}

/**
 * A group's units, from its rollups (one per workspace and harness; a unit
 * can have several rollup rows, one per project). Rows are summed here, not
 * in SQL, because `array_agg` cannot merge machine-id arrays of different lengths.
 */
export async function sessionUnits(db: Queryable, orgId: string, sessionId: string): Promise<SessionUnit[]> {
  const { rows } = await db.query<{
    workspace_id: string;
    source: string;
    first_ms: string | null;
    last_ms: string | null;
    machine_ids: string[] | null;
    models: string[] | null;
    event_count: string;
    input_tokens: string;
    output_tokens: string;
    reasoning_tokens: string;
    cache_read_tokens: string;
    cache_create_tokens: string;
    cost_usd_micros: string;
  }>(
    `SELECT workspace_id, source,
            floor(extract(epoch FROM first_ts) * 1000)::bigint::text AS first_ms,
            floor(extract(epoch FROM last_ts) * 1000)::bigint::text AS last_ms,
            machine_ids, models,
            coalesce(event_count, 0)::text AS event_count,
            coalesce(input_tokens, 0)::text AS input_tokens,
            coalesce(output_tokens, 0)::text AS output_tokens,
            coalesce(reasoning_tokens, 0)::text AS reasoning_tokens,
            coalesce(cache_read_tokens, 0)::text AS cache_read_tokens,
            coalesce(cache_create_tokens, 0)::text AS cache_create_tokens,
            coalesce(cost_usd_micros, 0)::text AS cost_usd_micros
       FROM sessions.session_rollups
      WHERE org_id = $1 AND session_id = $2 AND workspace_id IS NOT NULL AND source IS NOT NULL
      ORDER BY workspace_id, source
      LIMIT 10000`,
    [orgId, sessionId],
  );
  const SUMS = ["event_count", "input_tokens", "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_create_tokens", "cost_usd_micros"] as const;
  const units = new Map<string, { unit: SessionUnit; sums: Record<string, bigint>; machines: Set<string>; models: Set<string> }>();
  for (const row of rows) {
    const k = JSON.stringify([row.workspace_id, row.source]);
    let u = units.get(k);
    if (!u) {
      u = {
        unit: { orgId, workspaceId: row.workspace_id, source: row.source, sessionId, firstMs: Infinity, lastMs: -Infinity, machineIds: [], totals: {} },
        sums: Object.fromEntries(SUMS.map((f) => [f, 0n])),
        machines: new Set(),
        models: new Set(),
      };
      units.set(k, u);
    }
    if (row.first_ms !== null) u.unit.firstMs = Math.min(u.unit.firstMs, Number(row.first_ms));
    if (row.last_ms !== null) u.unit.lastMs = Math.max(u.unit.lastMs, Number(row.last_ms));
    for (const m of row.machine_ids ?? []) if (m) u.machines.add(m);
    for (const m of row.models ?? []) if (m) u.models.add(m);
    for (const f of SUMS) u.sums[f]! += BigInt(row[f]);
  }
  // Catalog-only units: a (workspace, harness) the catalog names and no rollup covers.
  const catalog = await db.query<{ workspace_id: string; source: string; first_ms: string | null; last_ms: string | null }>(
    `SELECT workspace_id, source,
            floor(extract(epoch FROM coalesce(first_activity_at, received_at)) * 1000)::bigint::text AS first_ms,
            floor(extract(epoch FROM coalesce(last_activity_at, first_activity_at, received_at)) * 1000)::bigint::text AS last_ms
       FROM sessions.session_catalog
      WHERE org_id = $1 AND session_id = $2 AND workspace_id IS NOT NULL AND source IS NOT NULL
      LIMIT 1000`,
    [orgId, sessionId],
  );
  for (const row of catalog.rows) {
    const k = JSON.stringify([row.workspace_id, row.source]);
    if (units.has(k)) continue;
    const first = row.first_ms === null ? 0 : Number(row.first_ms);
    const last = row.last_ms === null ? first : Number(row.last_ms);
    units.set(k, {
      unit: { orgId, workspaceId: row.workspace_id, source: row.source, sessionId, firstMs: first, lastMs: last, machineIds: [], totals: {} },
      sums: Object.fromEntries(SUMS.map((f) => [f, 0n])),
      machines: new Set(),
      models: new Set(),
    });
  }
  return [...units.values()].map(({ unit, sums, machines, models }) => {
    const firstMs = Number.isFinite(unit.firstMs) ? unit.firstMs : 0;
    return {
      ...unit,
      firstMs,
      lastMs: Number.isFinite(unit.lastMs) ? Math.max(unit.lastMs, firstMs) : firstMs,
      machineIds: [...machines].sort(),
      totals: { ...Object.fromEntries(SUMS.map((f) => [f, sums[f]!.toString()])), models: [...models].sort() },
    };
  });
}

/** One stream line, already in `buildSessionStream`'s exact bytes. */
export interface StreamLine {
  /** Neon machine id (unsanitized). */
  machine: string;
  bytes: Uint8Array;
  /** Client feed origin and revision, for the O4 resume marks; absent for `machine:` origins. */
  feed?: { origin: string; revision: number };
  userId: string | null;
  /** String tokens cut to the scrub bound in this line (see `boundStrings`). */
  truncated?: number;
  /** Length (chars) of the longest string cut in this line. */
  truncatedMaxChars?: number;
}

export interface UnitSource {
  /** `fresh` bypasses the fence cache (a changed session must see a tombstone written with its change). */
  fenced(unit: SessionUnit, fresh?: boolean): Promise<boolean>;
  lines(unit: SessionUnit): AsyncIterable<StreamLine>;
}

interface DeliveryRow {
  origin_id: string;
  record_id: string;
  revision: string;
  received_at: string;
  payload_json: string | null;
  user_id: string | null;
  accepted_seq: string | null;
  machine: string;
}

interface EventRow {
  machine_id: string;
  kind: string;
  event_id: string;
  user_id: string | null;
  created_at: string;
  row_json: string;
}

/** Longest JSON string token (escapes included) a stored record may carry: the scrubber's bound, with margin. */
export const MAX_STRING_TOKEN = MAX_SCRUB_CHARS - 4096;
const TRUNCATION_NOTE = (dropped: number) =>
  `\n[neon-s3-sync: truncated ${dropped} characters over the ${MAX_SCRUB_CHARS}-character scrub limit; the full text stays in Neon]`;

/**
 * The sealer (scrub rule set 2) stores strings longer than the scrub bound
 * whole, scrubbed in segments; it refuses only one it cannot segment safely
 * (an unbroken run past the bound with no whitespace, such as a base64 dump).
 * Only such a string token is cut, with an in-band note (and counted in the
 * truncation ledger); every other byte of the payload is kept as Neon's text.
 */
export function boundStrings(json: string): { text: string; truncated: number; maxChars: number } {
  if (json.length <= MAX_STRING_TOKEN) return { text: json, truncated: 0, maxChars: 0 };
  let out = "";
  let last = 0;
  let truncated = 0;
  let maxChars = 0;
  for (let i = 0; i < json.length; i++) {
    if (json[i] !== '"') continue;
    let j = i + 1;
    while (j < json.length && json[j] !== '"') j += json[j] === "\\" ? 2 : 1;
    if (j - i + 1 > MAX_STRING_TOKEN) {
      const source = json.slice(i, j + 1);
      const value = JSON.parse(source) as string;
      // The sealer scrubs both the decoded value and the raw literal.
      if (canScrubLong(value) && canScrubLong(source)) {
        i = j;
        continue;
      }
      maxChars = Math.max(maxChars, value.length);
      let k = MAX_STRING_TOKEN - 512;
      let literal = "";
      for (;;) {
        if (k > 0 && /[\uD800-\uDBFF]/.test(value[k - 1]!)) k -= 1;
        literal = JSON.stringify(value.slice(0, k) + TRUNCATION_NOTE(value.length - k));
        if (literal.length <= MAX_STRING_TOKEN) break;
        k = Math.floor(k * 0.8);
      }
      out += json.slice(last, i) + literal;
      last = j + 1;
      truncated += 1;
    }
    i = j;
  }
  return { text: out + json.slice(last), truncated, maxChars };
}

const encodeLine = (record: DeliveryRecord): Uint8Array => buildSessionStream([record]).bytes;

/** Machine id Neon's delivery projection gives an origin's events. Computed in SQL so the text is Postgres's. */
const DELIVERY_MACHINE_SQL = `'delivery:' || md5(jsonb_build_array(d.workspace_id, d.origin_id)::text)`;

/**
 * The live Neon source: per kind, keyset pages over `delivery_records_lookup`
 * (latest revision per origin is the row itself; deletes are skipped), then
 * the undelivered events per machine over the events primary key and the
 * legacy-delivery partial index.
 *
 * A record_id stored by two origins (rare) is settled like #78's reader: the
 * write the acceptance ledger accepted last wins, then the projected row, then
 * receipt, then origin. If that winner is a delete, no origin exports it.
 */
export const SMALL_SESSION_ROWS = 4000;

/**
 * Deletion fence: `deleted_upload_sessions` tombstones only. A user's reset
 * (`reset_uploaded_sessions`, vendor 0039) tombstones every session it covers
 * and deletes their delivery records (0038 `set_session_uploads`), so the
 * tombstones are the complete fence. A reset leg joined to `delivery_records`
 * could only match sessions delivered *after* the reset, which must be
 * exported, never fenced (#40's `deletionFenced` has that leg; not used here).
 *
 * The table is tiny, so it is read whole at most every `ttlMs`; `fresh`
 * checks the one session directly.
 */
function fenceCache(db: Queryable, ttlMs = 120_000) {
  const LIMIT = 1_000_000;
  let loaded = 0;
  let tombstones = new Set<string>();
  const k = (...parts: string[]) => JSON.stringify(parts);
  const perSession = async (unit: SessionUnit) =>
    (
      await db.query(
        `SELECT 1 FROM sessions.deleted_upload_sessions
          WHERE org_id = $1 AND workspace_id = $2 AND source = $3 AND session_id = $4 LIMIT 1`,
        [unit.orgId, unit.workspaceId, unit.source, unit.sessionId],
      )
    ).rows.length > 0;
  return async (unit: SessionUnit, fresh = false): Promise<boolean> => {
    if (fresh) return perSession(unit);
    if (Date.now() - loaded > ttlMs) {
      const t = await db.query<{ org_id: string; workspace_id: string; source: string; session_id: string }>(
        `SELECT org_id, workspace_id, source, session_id FROM sessions.deleted_upload_sessions LIMIT ${LIMIT + 1}`,
      );
      // Fail closed rather than miss a fence past the cap.
      if (t.rows.length > LIMIT) return perSession(unit);
      tombstones = new Set(t.rows.map((x) => k(x.org_id, x.workspace_id, x.source, x.session_id)));
      loaded = Date.now();
    }
    return tombstones.has(k(unit.orgId, unit.workspaceId, unit.source, unit.sessionId));
  };
}

/** Every `(org, session)` a tombstone covers: their exports are erased. */
export async function fencedGroups(db: Queryable): Promise<{ org_id: string; session_id: string }[]> {
  const { rows } = await db.query<{ org_id: string; session_id: string }>(
    `SELECT DISTINCT org_id, session_id FROM sessions.deleted_upload_sessions LIMIT 100001`,
  );
  if (rows.length > 100_000) throw Object.assign(new Error("too many fenced groups"), { code: "fence_overflow" });
  return rows;
}

/**
 * Deletion tombstones with the identity relayhistory-api's live writer keys
 * its objects by (`m=srv_<sha256(workspace \x1f origin \x1f user)[0:32]>`,
 * WRITE_PATH): the same fence as `fenceCache` and `fencedGroups`,
 * `deleted_upload_sessions` only. A reset writes a tombstone for every
 * session it covers (0039 -> 0038 `set_session_uploads`), and so does the
 * Neon replica for a batch a reset superseded (relayhistory-cloud 0070), so
 * no reset leg is needed, and none may be added: one joined to later
 * deliveries would erase sessions uploaded after the reset.
 */
export interface LiveFencedGroup {
  org_id: string;
  workspace_id: string;
  user_id: string;
  origin_id: string;
  source: string;
  session_id: string;
}

export const LIVE_FENCE_PAGE = 5_000;

/** One keyset page of tombstones, in primary-key order, after `after`. */
export async function fencedLivePage(
  db: Queryable,
  after: LiveFencedGroup | null,
  limit = LIVE_FENCE_PAGE,
): Promise<LiveFencedGroup[]> {
  const cols = "org_id, workspace_id, user_id, origin_id, source, session_id";
  const { rows } = after
    ? await db.query<LiveFencedGroup>(
        `SELECT ${cols} FROM sessions.deleted_upload_sessions
          WHERE (${cols}) > ($1, $2, $3, $4, $5, $6)
          ORDER BY ${cols} LIMIT $7`,
        [after.org_id, after.workspace_id, after.user_id, after.origin_id, after.source, after.session_id, limit],
      )
    : await db.query<LiveFencedGroup>(
        `SELECT ${cols} FROM sessions.deleted_upload_sessions ORDER BY ${cols} LIMIT $1`,
        [limit],
      );
  return rows;
}

interface SmallRow extends DeliveryRow {
  kind: string;
  operation: string;
  has_event: boolean | null;
}

/** Contender order of #78: ledger seat, projected row, receipt, origin (all descending). */
function beats(a: SmallRow, b: SmallRow): boolean {
  const seq = (r: SmallRow) => (r.accepted_seq === null ? null : BigInt(r.accepted_seq));
  const sa = seq(a);
  const sb = seq(b);
  if (sa !== sb) return sb === null ? true : sa === null ? false : sa > sb;
  if (!!a.has_event !== !!b.has_event) return !!a.has_event;
  if (a.received_at !== b.received_at) return a.received_at > b.received_at;
  return a.origin_id > b.origin_id;
}

export function neonUnitSource(
  db: Queryable,
  onPage: PageHook,
  pageRows = DEFAULT_PAGE_ROWS,
  smallRows = SMALL_SESSION_ROWS,
): UnitSource {
  const fenced = fenceCache(db);
  return {
    fenced,
    async *lines(unit) {
      // A session the rollup already shows as large goes straight to pages: the
      // one-statement path sorts and windows the whole session.
      if (Number(unit.totals.event_count ?? 0) > smallRows / 2) {
        yield* pagedLines(db, unit, onPage, pageRows);
        return;
      }
      // Fast path: a session of at most SMALL_SESSION_ROWS delivery rows in one
      // statement (the common case; Neon is ~80 ms away, so round trips dominate).
      const small = await db.query<SmallRow>(
        `WITH d AS MATERIALIZED (
           SELECT * FROM sessions.delivery_records
            WHERE org_id = $1 AND workspace_id = $2 AND kind = ANY ($3::text[]) AND source = $4 AND session_id = $5
            LIMIT $6)
         SELECT d.kind, d.origin_id, d.record_id, d.revision::text AS revision, d.operation,
                d.received_at::text AS received_at,
                CASE WHEN d.operation = 'upsert' THEN d.payload::text END AS payload_json,
                d.user_id,
                (CASE WHEN d.kind = ANY ($7::text[])
                      THEN (SELECT c.accepted_seq FROM sessions.delivery_catalog_acceptance c
                             WHERE c.org_id = d.org_id AND c.workspace_id = d.workspace_id
                               AND c.origin_id = d.origin_id AND c.record_id = d.record_id)
                      ELSE (SELECT p.accepted_seq FROM sessions.delivery_projection_acceptance p
                             WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
                               AND p.origin_id = d.origin_id AND p.record_id = d.record_id)
                 END)::text AS accepted_seq,
                ${DELIVERY_MACHINE_SQL} AS machine,
                CASE WHEN count(*) OVER (PARTITION BY d.kind, d.record_id) > 1 THEN
                  EXISTS (SELECT 1 FROM sessions.convergence_events e
                           WHERE e.org_id = d.org_id AND e.workspace_id = d.workspace_id
                             AND e.delivery_record_id = d.record_id
                             AND e.machine_id = ${DELIVERY_MACHINE_SQL}) END AS has_event
           FROM d
          ORDER BY array_position($3::text[], d.kind), d.origin_id, d.record_id`,
        [unit.orgId, unit.workspaceId, [...EXPORT_KINDS], unit.source, unit.sessionId, smallRows + 1, [...CATALOG_KINDS]],
      );
      await onPage(small.rows.length, small.rows.reduce((n, r) => n + (r.payload_json?.length ?? 0), 0));
      if (small.rows.length <= smallRows) {
        const winner = new Map<string, SmallRow>();
        for (const row of small.rows) {
          const key = JSON.stringify([row.kind, row.record_id]);
          const prior = winner.get(key);
          if (!prior || beats(row, prior)) winner.set(key, row);
        }
        for (const row of small.rows) {
          if (row.operation !== "upsert" || row.payload_json === null) continue;
          if (winner.get(JSON.stringify([row.kind, row.record_id])) !== row) continue;
          yield deliveryLine(row.kind, row);
        }
        yield* undelivered(db, unit, onPage, pageRows);
        return;
      }
      yield* pagedLines(db, unit, onPage, pageRows);
    },
  };
}

function deliveryLine(kind: string, row: DeliveryRow): StreamLine {
  const bounded = boundStrings(row.payload_json!);
  const record: DeliveryRecord = {
    kind,
    origin_id: row.origin_id,
    record_id: row.record_id,
    revision: row.revision,
    operation: "upsert",
    received_at: row.received_at,
    payload: {},
    payload_json: bounded.text,
    accepted_seq: row.accepted_seq,
  };
  const revision = Number(row.revision);
  return {
    machine: row.machine,
    bytes: encodeLine(record),
    ...(bounded.truncated ? { truncated: bounded.truncated, truncatedMaxChars: bounded.maxChars } : {}),
    ...(Number.isSafeInteger(revision) && revision >= 0 ? { feed: { origin: row.origin_id, revision } } : {}),
    userId: row.user_id,
  };
}

/**
 * The large-session path: per kind, keyset pages over `delivery_records_lookup`.
 * Same lines, same order and the same contender rule as the fast path.
 */
async function* pagedLines(db: Queryable, unit: SessionUnit, onPage: PageHook, pageRows: number): AsyncIterable<StreamLine> {
  for (const kind of EXPORT_KINDS) {
    const acceptance = CATALOG_KINDS.has(kind)
      ? "sessions.delivery_catalog_acceptance"
      : "sessions.delivery_projection_acceptance";
    const key = [unit.orgId, unit.workspaceId, kind, unit.source, unit.sessionId];
    let after: [string, string] | null = null;
    for (;;) {
      const { rows }: { rows: DeliveryRow[] } = await db.query<DeliveryRow>(
        `SELECT d.origin_id, d.record_id, d.revision::text AS revision,
                d.received_at::text AS received_at, d.payload::text AS payload_json,
                d.user_id, a.accepted_seq::text AS accepted_seq,
                ${DELIVERY_MACHINE_SQL} AS machine
           FROM sessions.delivery_records d
           LEFT JOIN ${acceptance} a
             ON a.org_id = d.org_id AND a.workspace_id = d.workspace_id
            AND a.origin_id = d.origin_id AND a.record_id = d.record_id
          WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.kind = $3 AND d.source = $4 AND d.session_id = $5
            AND d.operation = 'upsert'
            ${after ? "AND (d.origin_id, d.record_id) > ($7, $8)" : ""}
          ORDER BY d.origin_id, d.record_id
          LIMIT $6`,
        after ? [...key, pageRows, ...after] : [...key, pageRows],
      );
      // Contenders of this page's records on other origins (upserts or tombstones),
      // through delivery_records_record_idx: bounded by the page, not the session.
      const contenders = rows.length
        ? (
            await db.query<SmallRow>(
              `SELECT d.kind, d.origin_id, d.record_id, d.operation, d.received_at::text AS received_at,
                      a.accepted_seq::text AS accepted_seq,
                      CASE WHEN count(*) OVER (PARTITION BY d.record_id) > 1 THEN
                        EXISTS (SELECT 1 FROM sessions.convergence_events e
                                 WHERE e.org_id = d.org_id AND e.workspace_id = d.workspace_id
                                   AND e.delivery_record_id = d.record_id
                                   AND e.machine_id = ${DELIVERY_MACHINE_SQL}) END AS has_event
                 FROM sessions.delivery_records d
                 LEFT JOIN ${acceptance} a
                   ON a.org_id = d.org_id AND a.workspace_id = d.workspace_id
                  AND a.origin_id = d.origin_id AND a.record_id = d.record_id
                WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.record_id = ANY ($6::text[])
                  AND d.kind = $3 AND d.source = $4 AND d.session_id = $5`,
              [...key, [...new Set(rows.map((r) => r.record_id))]],
            )
          ).rows
        : [];
      const winner = new Map<string, SmallRow>();
      for (const c of contenders) {
        const prior = winner.get(c.record_id);
        if (!prior || beats(c, prior)) winner.set(c.record_id, c);
      }
      let bytes = 0;
      for (const row of rows) {
        bytes += row.payload_json?.length ?? 0;
        if (row.payload_json === null) continue;
        const w = winner.get(row.record_id);
        if (w && (w.operation !== "upsert" || w.origin_id !== row.origin_id)) continue;
        yield deliveryLine(kind, row);
      }
      await onPage(rows.length + contenders.length, bytes);
      if (rows.length < pageRows) break;
      const last: DeliveryRow = rows.at(-1)!;
      after = [last.origin_id, last.record_id];
    }
  }
  yield* undelivered(db, unit, onPage, pageRows);
}

/**
 * Events with no delivery record (server-side harnesses, legacy deliveries):
 * the whole event row as a `convergence_event` line, exactly as #40's
 * `undeliveredEvents`, paged per machine.
 */
async function* undelivered(
  db: Queryable,
  unit: SessionUnit,
  onPage: PageHook,
  pageRows: number,
): AsyncIterable<StreamLine> {
  const select = `SELECT e.machine_id, e.kind, e.event_id, e.user_id, e.ingested_at::text AS created_at,
            ((to_jsonb(e) - 'org_id' - 'workspace_id' - 'embedding' - 'embedding_model' - 'embedding_dim'
                - 'content_hash' - 'generated_at' - 'embedding_skip_reason')
              || jsonb_build_object('ts_ms', floor(extract(epoch FROM e.ts) * 1000)::bigint))::text AS row_json
       FROM sessions.convergence_events e`;
  const emit = (row: EventRow): StreamLine => {
    const bounded = boundStrings(row.row_json);
    const record: DeliveryRecord = {
      kind: "convergence_event",
      origin_id: `machine:${row.machine_id}`,
      record_id: row.event_id,
      revision: "1",
      operation: "upsert",
      received_at: row.created_at,
      payload: {},
      payload_json: bounded.text,
    };
    return {
      machine: row.machine_id,
      bytes: encodeLine(record),
      userId: row.user_id,
      ...(bounded.truncated ? { truncated: bounded.truncated, truncatedMaxChars: bounded.maxChars } : {}),
    };
  };
  // Non-delivery machines: the events primary key (org, machine, source, session, kind, event_id).
  for (const machine of unit.machineIds.filter((m) => !m.startsWith("delivery:"))) {
    let after: [string, string] | null = null;
    for (;;) {
      const { rows }: { rows: EventRow[] } = await db.query<EventRow>(
        `${select}
          WHERE e.org_id = $1 AND e.machine_id = $2 AND e.source = $3 AND e.session_id = $4
            AND e.workspace_id = $5 AND e.delivery_record_id IS NULL
            ${after ? "AND (e.kind, e.event_id) > ($7, $8)" : ""}
          ORDER BY e.kind, e.event_id
          LIMIT $6`,
        after
          ? [unit.orgId, machine, unit.source, unit.sessionId, unit.workspaceId, pageRows, ...after]
          : [unit.orgId, machine, unit.source, unit.sessionId, unit.workspaceId, pageRows],
      );
      await onPage(rows.length, rows.reduce((n, r) => n + r.row_json.length, 0));
      for (const row of rows) yield emit(row);
      if (rows.length < pageRows) break;
      after = [rows.at(-1)!.kind, rows.at(-1)!.event_id];
    }
  }
  // Legacy delivery events never linked to their record (partial index
  // convergence_events_legacy_delivery_idx on (org, session, event_id)). The
  // keyset is the full (event_id, machine_id, kind): event ids repeat across
  // machines and kinds, and a page ending between two of them dropped one.
  let afterLegacy: [string, string, string] | null = null;
  for (;;) {
    const { rows }: { rows: EventRow[] } = await db.query<EventRow>(
      `${select}
        WHERE e.org_id = $1 AND e.session_id = $2 AND e.delivery_record_id IS NULL
          AND e.machine_id LIKE 'delivery:%' AND e.workspace_id = $3 AND e.source = $4
          ${afterLegacy !== null ? "AND (e.event_id, e.machine_id, e.kind) > ($6, $7, $8)" : ""}
        ORDER BY e.event_id, e.machine_id, e.kind
        LIMIT $5`,
      afterLegacy !== null
        ? [unit.orgId, unit.sessionId, unit.workspaceId, unit.source, pageRows, ...afterLegacy]
        : [unit.orgId, unit.sessionId, unit.workspaceId, unit.source, pageRows],
    );
    await onPage(rows.length, rows.reduce((n, r) => n + r.row_json.length, 0));
    for (const row of rows) yield emit(row);
    if (rows.length < pageRows) break;
    const last: EventRow = rows.at(-1)!;
    afterLegacy = [last.event_id, last.machine_id, last.kind];
  }
}

/**
 * Lock waiters other than advisory locks (ingest serializes on advisory locks
 * by design, so a handful of those is normal). The exporter pauses when this
 * is above its threshold.
 */
export async function lockWaiters(db: Queryable): Promise<{ total: number; nonAdvisory: number }> {
  const { rows } = await db.query<{ total: string; non_advisory: string }>(
    `SELECT count(*)::text AS total,
            count(*) FILTER (WHERE locktype <> 'advisory')::text AS non_advisory
       FROM pg_locks WHERE NOT granted`,
  );
  return { total: Number(rows[0]?.total ?? 0), nonAdvisory: Number(rows[0]?.non_advisory ?? 0) };
}
