/**
 * Session reconcile and the minute tail that keeps the ClickHouse transcript
 * index in step with Neon.
 *
 * Change source. `sessions.session_revisions` (0024) is bumped by statement
 * triggers on every INSERT, UPDATE and DELETE of `convergence_events`, one row
 * per (org, session). The tail reads the sessions whose `updated_at` passed its
 * watermark (a ~65 ms parallel scan of a ~117k-row table in production,
 * 2026-10-31) and reconciles each one whose revision differs from the one it
 * last reconciled.
 *
 * Reconcile. Neon deletes events outright (delivery revisions retract and
 * re-project a record, session upload deletion, retention), so there is no
 * tombstone to follow. Instead, per session, a fingerprint scan compares Neon's
 * in-window event identities with ClickHouse's: missing or changed events are
 * fetched by primary key and inserted, and ClickHouse rows Neon no longer has
 * (deleted, or moved to another ts/workspace) are removed with one lightweight
 * DELETE. The fingerprint reads content only through `octet_length` and
 * `content_hash`, so the scan never detoasts transcript text.
 */

import type { ClickHouseClient } from "./clickhouse.js";
import { arrayLiteral, tuple } from "./clickhouse.js";
import {
  EVENT_COLUMNS_SQL,
  FINGERPRINT_SQL,
  type IndexRow,
  type NeonEventRow,
  type NeonReader,
  TS_TEXT_SQL,
  toIndexRow,
  tsMillis,
} from "./rows.js";
import {
  EVENTS_TABLE,
  INGEST_STATE_TABLE,
  SCHEMA_SETTINGS,
  SESSION_STATE_TABLE,
  schemaStatements,
} from "./schema.js";

export const STATE_TAIL_WATERMARK = "tail_watermark";
export const STATE_WINDOW_START = "window_start";
export const STATE_REVISION_FLOOR = "revision_floor";
export const STATE_BACKFILL = "backfill";

export const DEFAULT_WINDOW_DAYS = 14;
/** Added to revision_floor before comparing reconcile read-start times. */
export const FLOOR_MARGIN_MS = 2 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;

/** Apply the schema; only sets the text-index settings the server knows. */
export async function migrateClickHouse(ch: ClickHouseClient): Promise<void> {
  const known = await ch.select<{ name: string }>(
    "SELECT name FROM system.settings WHERE name IN {names:Array(String)}",
    { params: { names: Object.keys(SCHEMA_SETTINGS) } },
  );
  const settings: Record<string, string | number> = {};
  for (const { name } of known) settings[name] = SCHEMA_SETTINGS[name]!;
  for (const statement of schemaStatements()) {
    await ch.command(statement, { settings });
  }
  // Read-latency settings: newly written parts (backfill, merges) otherwise
  // serve their first reads from cold marks on object storage (prod
  // 2026-11-01: 0.25-0.45 s WaitMarksLoad, one 6.5 s timeout). Best effort:
  // a server without a setting skips it.
  for (const setting of [
    "prewarm_mark_cache = 1",
    "cache_populated_by_fetch = 1",
  ]) {
    try {
      await ch.command(`ALTER TABLE ${EVENTS_TABLE} MODIFY SETTING ${setting}`);
    } catch (error) {
      // Unknown on this server, or no permission: report the code only.
      console.warn(
        JSON.stringify({
          message: "schema setting skipped",
          setting: setting.split(" ")[0],
          code: (error as { code?: string }).code ?? null,
        }),
      );
    }
  }
}

export async function readState(
  ch: ClickHouseClient,
  name: string,
): Promise<string | null> {
  const rows = await ch.select<{ value: string }>(
    `SELECT argMax(value, updated_at) AS value, count() AS n FROM ${INGEST_STATE_TABLE} WHERE name = {name:String} HAVING n > 0`,
    { params: { name } },
  );
  return rows[0]?.value ?? null;
}

export async function writeState(
  ch: ClickHouseClient,
  name: string,
  value: string,
  now: Date = new Date(),
): Promise<void> {
  await ch.insert(INGEST_STATE_TABLE, [
    { name, value, updated_at: chNow(now) },
  ]);
}

export function chNow(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

export async function windowStart(
  ch: ClickHouseClient,
  now: Date,
): Promise<string> {
  return (
    (await readState(ch, STATE_WINDOW_START)) ??
    new Date(now.getTime() - DEFAULT_WINDOW_DAYS * DAY_MS).toISOString()
  );
}

const KEY_SEP = "\u001f";

interface Identity {
  workspaceId: string;
  machineId: string;
  source: string;
  kind: string;
  eventId: string;
  tsMs: number;
  fingerprint: string;
}

function eventKey(
  i: Pick<Identity, "machineId" | "source" | "kind" | "eventId">,
) {
  return [i.machineId, i.source, i.kind, i.eventId].join(KEY_SEP);
}

export interface ReconcileLimits {
  /** Fingerprint rows per Neon page. */
  pageRows: number;
  /** Stop scanning a session after this many fingerprint rows. */
  maxScanRows: number;
  /** Primary keys per full-row fetch. */
  fetchChunk: number;
}

export const DEFAULT_RECONCILE_LIMITS: ReconcileLimits = {
  pageRows: 10_000,
  maxScanRows: 100_000,
  fetchChunk: 200,
};

export interface SessionTarget {
  orgId: string;
  sessionId: string;
}

export interface ReconcileResult {
  scanned: number;
  inserted: number;
  deleted: number;
  /** False when the scan stopped at maxScanRows (deletions not proven). */
  complete: boolean;
  /** The run's deadline passed mid-session; nothing about it is recorded. */
  timedOut: boolean;
}

async function neonIdentities(
  neon: NeonReader,
  target: SessionTarget,
  from: string,
  limits: ReconcileLimits,
  pastDeadline: () => boolean,
): Promise<{
  rows: Map<string, Identity>;
  complete: boolean;
  timedOut: boolean;
}> {
  const rows = new Map<string, Identity>();
  let cursor: {
    ts: string;
    eventId: string;
    machineId: string;
    source: string;
    kind: string;
  } | null = null;
  for (;;) {
    const params: unknown[] = [target.orgId, target.sessionId, from];
    let keyset = "";
    if (cursor) {
      params.push(
        cursor.ts,
        cursor.eventId,
        cursor.machineId,
        cursor.source,
        cursor.kind,
      );
      keyset =
        "AND ts >= $4::timestamptz AND (ts, event_id, machine_id, source, kind) > ($4::timestamptz, $5, $6, $7, $8)";
    }
    params.push(limits.pageRows);
    const page = await neon.query<{
      workspace_id: string;
      machine_id: string;
      source: string;
      kind: string;
      event_id: string;
      ts_text: string;
      fingerprint: string;
    }>(
      `SELECT workspace_id, machine_id, source, kind, event_id, ${TS_TEXT_SQL} AS ts_text,
        ${FINGERPRINT_SQL} AS fingerprint
       FROM sessions.convergence_events
       WHERE org_id = $1 AND session_id = $2 AND ts >= $3::timestamptz ${keyset}
       ORDER BY ts, event_id, machine_id, source, kind
       LIMIT $${params.length}`,
      params,
    );
    for (const row of page) {
      rows.set(
        eventKey({
          machineId: row.machine_id,
          source: row.source,
          kind: row.kind,
          eventId: row.event_id,
        }),
        {
          workspaceId: row.workspace_id,
          machineId: row.machine_id,
          source: row.source,
          kind: row.kind,
          eventId: row.event_id,
          tsMs: tsMillis(row.ts_text),
          fingerprint: row.fingerprint,
        },
      );
    }
    if (page.length < limits.pageRows)
      return { rows, complete: true, timedOut: false };
    if (rows.size >= limits.maxScanRows)
      return { rows, complete: false, timedOut: false };
    if (pastDeadline()) return { rows, complete: false, timedOut: true };
    const last = page[page.length - 1]!;
    cursor = {
      ts: last.ts_text,
      eventId: last.event_id,
      machineId: last.machine_id,
      source: last.source,
      kind: last.kind,
    };
  }
}

async function clickhouseIdentities(
  ch: ClickHouseClient,
  target: SessionTarget,
  from: string,
  untilMs: number | null,
  limit: number,
): Promise<Identity[]> {
  const rows = await ch.select<{
    workspace_id: string;
    machine_id: string;
    source: string;
    kind: string;
    event_id: string;
    ts_ms: number;
    fingerprint: string;
  }>(
    `SELECT workspace_id, machine_id, source, kind, event_id,
        toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint
     FROM ${EVENTS_TABLE} FINAL
     WHERE org_id = {org:String} AND session_id = {session:String}
       AND ts >= parseDateTime64BestEffort({from:String}, 3, 'UTC')
       ${untilMs === null ? "" : "AND toUnixTimestamp64Milli(ts) <= {until:Int64}"}
     LIMIT {limit:UInt32}`,
    {
      params: {
        org: target.orgId,
        session: target.sessionId,
        from,
        limit,
        ...(untilMs === null ? {} : { until: untilMs }),
      },
      timeoutMs: 20_000,
    },
  );
  return rows.map((row) => ({
    workspaceId: row.workspace_id,
    machineId: row.machine_id,
    source: row.source,
    kind: row.kind,
    eventId: row.event_id,
    tsMs: Number(row.ts_ms),
    fingerprint: row.fingerprint,
  }));
}

/** Fetch full rows for the given identities by Neon primary key. */
export async function fetchEvents(
  neon: NeonReader,
  target: SessionTarget,
  identities: Identity[],
  chunk: number,
  pastDeadline: () => boolean = () => false,
): Promise<{ rows: NeonEventRow[]; timedOut: boolean }> {
  const out: NeonEventRow[] = [];
  for (let i = 0; i < identities.length; i += chunk) {
    if (i > 0 && pastDeadline()) return { rows: out, timedOut: true };
    const slice = identities.slice(i, i + chunk);
    const rows = await neon.query<NeonEventRow>(
      `SELECT ${EVENT_COLUMNS_SQL}
       FROM unnest($3::text[], $4::text[], $5::text[], $6::text[]) AS k(m, src, ki, e)
       JOIN sessions.convergence_events c
         ON c.org_id = $1 AND c.session_id = $2 AND c.machine_id = k.m
        AND c.source = k.src AND c.kind = k.ki AND c.event_id = k.e`,
      [
        target.orgId,
        target.sessionId,
        slice.map((s) => s.machineId),
        slice.map((s) => s.source),
        slice.map((s) => s.kind),
        slice.map((s) => s.eventId),
      ],
    );
    out.push(...rows);
  }
  return { rows: out, timedOut: false };
}

export interface PendingDelete {
  orgId: string;
  sessionId: string;
  identity: Identity;
}

export const DELETE_BYTES_BUDGET = 512 * 1024;

const utf8 = new TextEncoder();

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", utf8.encode(text));
  return [...new Uint8Array(digest)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Delete exact (key, workspace, ts) rows, batched into few mutations.
 *
 * Batches are capped by UTF-8 bytes as well as keys, and event_id is matched
 * by its SHA-256, computed here: the literal sent per key has a fixed-size
 * id member whatever the client-supplied id's length or alphabet, so no key
 * can push a statement past max_query_size (code 62 on every retry). The
 * other members pin the row to one org/session/workspace/machine/source/
 * kind/ms; those are short service-assigned values.
 */
export async function deleteRows(
  ch: ClickHouseClient,
  deletes: PendingDelete[],
  maxKeys = 500,
  maxBytes = DELETE_BYTES_BUDGET,
): Promise<void> {
  const run = async (literals: string[]) => {
    await ch.command(
      `DELETE FROM ${EVENTS_TABLE}
       WHERE (org_id, session_id, workspace_id, machine_id, source, kind, lower(hex(SHA256(event_id))), toUnixTimestamp64Milli(ts))
         IN [${literals.join(",")}]`,
      {
        settings: { lightweight_deletes_sync: 2, max_query_size: 1_048_576 },
        timeoutMs: 60_000,
      },
    );
  };
  let batch: string[] = [];
  let bytes = 0;
  for (const d of deletes) {
    const literal = arrayLiteral([
      tuple(
        d.orgId,
        d.sessionId,
        d.identity.workspaceId,
        d.identity.machineId,
        d.identity.source,
        d.identity.kind,
        await sha256Hex(d.identity.eventId),
        d.identity.tsMs,
      ),
    ]).slice(1, -1);
    const size = utf8.encode(literal).length + 1;
    if (
      batch.length > 0 &&
      (batch.length >= maxKeys || bytes + size > maxBytes)
    ) {
      await run(batch);
      batch = [];
      bytes = 0;
    }
    batch.push(literal);
    bytes += size;
  }
  if (batch.length > 0) await run(batch);
}

/**
 * Make ClickHouse's copy of one session's events at or after `from` equal
 * Neon's. Inserts before deleting, so a moved event is never absent.
 */
export async function reconcileSession(
  neon: NeonReader,
  ch: ClickHouseClient,
  target: SessionTarget,
  from: string,
  options: {
    limits?: ReconcileLimits;
    now?: () => number;
    /** Epoch ms after which the reconcile stops between pages/chunks. */
    deadline?: number;
  } = {},
): Promise<ReconcileResult> {
  const limits = options.limits ?? DEFAULT_RECONCILE_LIMITS;
  const now = options.now ?? Date.now;
  const deadline = options.deadline ?? Number.POSITIVE_INFINITY;
  const pastDeadline = () => now() > deadline;
  const version = now();
  const neonSide = await neonIdentities(
    neon,
    target,
    from,
    limits,
    pastDeadline,
  );
  if (neonSide.timedOut) {
    return {
      scanned: neonSide.rows.size,
      inserted: 0,
      deleted: 0,
      complete: false,
      timedOut: true,
    };
  }

  // A capped scan covers Neon up to its last ts; compare only that range.
  let untilMs: number | null = null;
  if (!neonSide.complete) {
    untilMs = 0;
    for (const row of neonSide.rows.values())
      untilMs = Math.max(untilMs, row.tsMs);
  }
  const chLimit = limits.maxScanRows * 2;
  const chSide = await clickhouseIdentities(ch, target, from, untilMs, chLimit);
  // A truncated ClickHouse read cannot prove the rest is current: deletes for
  // the rows it did return still apply, and the session stays needs_full.
  const chTruncated = chSide.length >= chLimit;
  const chByKey = new Map<string, Identity[]>();
  for (const row of chSide) {
    const key = eventKey(row);
    const list = chByKey.get(key);
    if (list) list.push(row);
    else chByKey.set(key, [row]);
  }

  const toFetch: Identity[] = [];
  for (const [key, row] of neonSide.rows) {
    const existing = chByKey.get(key) ?? [];
    const same = existing.some(
      (c) =>
        c.workspaceId === row.workspaceId &&
        c.tsMs === row.tsMs &&
        c.fingerprint === row.fingerprint,
    );
    if (!same) toFetch.push(row);
  }

  const deletes: PendingDelete[] = [];
  // An incomplete scan cannot prove absence, so it never deletes.
  if (neonSide.complete) {
    for (const [key, list] of chByKey) {
      const current = neonSide.rows.get(key);
      for (const c of list) {
        if (
          !current ||
          current.workspaceId !== c.workspaceId ||
          current.tsMs !== c.tsMs
        ) {
          deletes.push({ ...target, identity: c });
        }
      }
    }
  }

  const fetched = await fetchEvents(
    neon,
    target,
    toFetch,
    limits.fetchChunk,
    pastDeadline,
  );
  const rows: IndexRow[] = fetched.rows.map((row) => toIndexRow(row, version));
  for (let i = 0; i < rows.length; i += 5_000) {
    await ch.insert(EVENTS_TABLE, rows.slice(i, i + 5_000), {
      timeoutMs: 30_000,
      asyncInsert: true,
    });
  }
  // A timed-out fetch keeps the rows it did insert (idempotent) and leaves
  // the deletes and the session state to the next run.
  if (!fetched.timedOut) await deleteRows(ch, deletes);

  return {
    scanned: neonSide.rows.size,
    inserted: rows.length,
    deleted: fetched.timedOut ? 0 : deletes.length,
    complete: neonSide.complete && !chTruncated,
    timedOut: fetched.timedOut,
  };
}

export interface TailOptions {
  now?: () => Date;
  /** Re-read revisions this far behind the watermark (commit lag). */
  lookbackMs?: number;
  /**
   * Revision rows read per run. Rows are small and the lookback re-reads
   * already-current sessions every run, so this must comfortably exceed the
   * number of sessions changed in one lookback window (production: ~100 per
   * 10 minutes; ~1-2k after a post-backfill rewind).
   */
  revisionLimit?: number;
  /** Sessions reconciled per run. */
  maxSessions?: number;
  /** Wall-clock budget for one run. */
  deadlineMs?: number;
  limits?: ReconcileLimits;
  /**
   * Sessions whose last scan saw more than this many events reconcile only
   * their recent tail (recentMs) when they change; they are then marked
   * needs_full and get a full pass from the sweep once fullEveryMs has passed
   * since their last one, whether or not they change again.
   */
  largeSessionRows?: number;
  recentMs?: number;
  fullEveryMs?: number;
  /** Full passes of needs_full sessions per run. */
  sweepSessions?: number;
  /** A run holds a lease this long so minute runs do not overlap. */
  leaseMs?: number;
}

export interface TailSummary {
  changed: number;
  reconciled: number;
  inserted: number;
  deleted: number;
  scanned: number;
  deferred: number;
  swept: number;
  /** Sessions whose reconcile threw; parked as needs_full for the sweep. */
  failed: number;
  /** True when revisionLimit was reached; the next run pages on by keyset. */
  saturated: boolean;
  /** True when another run held the lease and this one did nothing. */
  skipped: boolean;
  watermark: string;
}

export const STATE_TAIL_CURSOR = "tail_cursor";
export const STATE_TAIL_LEASE = "tail_lease";

interface RevisionCursor {
  ts: string;
  orgId: string;
  sessionId: string;
}

interface RevisionRow {
  org_id: string;
  session_id: string;
  revision: string | number;
  updated_text: string;
}

interface SessionState {
  revision: number;
  syncedMs: number;
  fullMs: number;
  scanned: number;
  needsFull: boolean;
}

async function writeSessionState(
  ch: ClickHouseClient,
  target: SessionTarget,
  state: {
    revision: number;
    syncedAt: Date;
    fullAt: Date;
    scanned: number;
    needsFull: boolean;
  },
) {
  await ch.insert(SESSION_STATE_TABLE, [
    {
      org_id: target.orgId,
      session_id: target.sessionId,
      revision: state.revision,
      synced_at: chNow(state.syncedAt),
      full_synced_at: chNow(state.fullAt),
      scanned_rows: state.scanned,
      needs_full: state.needsFull ? 1 : 0,
    },
  ]);
}

/**
 * One tail pass. Bounded by maxSessions and deadlineMs; anything left over is
 * picked up by the next minute's pass because the watermark only advances
 * over sessions that were reconciled (or already current).
 */
export async function runTail(
  neon: NeonReader,
  ch: ClickHouseClient,
  options: TailOptions = {},
): Promise<TailSummary> {
  const now = options.now ?? (() => new Date());
  const started = now();
  const deadline = started.getTime() + (options.deadlineMs ?? 40_000);
  const lookbackMs = options.lookbackMs ?? 10 * 60 * 1000;
  const revisionLimit = options.revisionLimit ?? 20_000;
  const maxSessions = options.maxSessions ?? 200;
  const largeSessionRows = options.largeSessionRows ?? 20_000;
  const recentMs = options.recentMs ?? 6 * 60 * 60 * 1000;
  const fullEveryMs = options.fullEveryMs ?? 60 * 60 * 1000;
  const sweepSessions = options.sweepSessions ?? 5;
  // Longer than any pass can run: the deadline is checked between pages and
  // every request has its own timeout (inserts 30 s, deletes 60 s).
  const leaseMs = options.leaseMs ?? 180_000;

  const summary: TailSummary = {
    changed: 0,
    reconciled: 0,
    inserted: 0,
    deleted: 0,
    scanned: 0,
    deferred: 0,
    swept: 0,
    failed: 0,
    saturated: false,
    skipped: false,
    watermark: "",
  };
  // Best-effort lease (ClickHouse has no compare-and-set): the minute cron
  // must not start a second pass while one is still running.
  const lease = Number((await readState(ch, STATE_TAIL_LEASE)) ?? 0);
  if (lease > started.getTime()) {
    summary.skipped = true;
    return summary;
  }
  const ours = String(started.getTime() + leaseMs);
  await writeState(ch, STATE_TAIL_LEASE, ours, started);
  try {
    return await tailPass(neon, ch, summary, {
      now,
      started,
      deadline,
      lookbackMs,
      revisionLimit,
      maxSessions,
      largeSessionRows,
      recentMs,
      fullEveryMs,
      sweepSessions,
      limits: options.limits,
    });
  } finally {
    // Release only a lease that is still ours.
    try {
      if ((await readState(ch, STATE_TAIL_LEASE)) === ours)
        await writeState(ch, STATE_TAIL_LEASE, "0", now());
    } catch {
      // It expires on its own.
    }
  }
}

async function tailPass(
  neon: NeonReader,
  ch: ClickHouseClient,
  summary: TailSummary,
  o: {
    now: () => Date;
    started: Date;
    deadline: number;
    lookbackMs: number;
    revisionLimit: number;
    maxSessions: number;
    largeSessionRows: number;
    recentMs: number;
    fullEveryMs: number;
    sweepSessions: number;
    limits?: ReconcileLimits;
  },
): Promise<TailSummary> {
  const { now, started, deadline, lookbackMs, revisionLimit, maxSessions } = o;
  const { largeSessionRows, recentMs, fullEveryMs, sweepSessions } = o;
  let watermark = await readState(ch, STATE_TAIL_WATERMARK);
  if (!watermark) {
    watermark = new Date(started.getTime() - lookbackMs).toISOString();
    await writeState(ch, STATE_TAIL_WATERMARK, watermark, started);
  }
  const floor = await readState(ch, STATE_REVISION_FLOOR);
  const from = await windowStart(ch, started);
  summary.watermark = watermark;
  const since = new Date(Date.parse(watermark) - lookbackMs).toISOString();
  // After a saturated page the next run continues by keyset from the last
  // row it covered instead of re-reading the lookback (which would read the
  // same full page forever). Once a page comes back short, the cursor clears
  // and the lookback re-read resumes.
  const cursorText = await readState(ch, STATE_TAIL_CURSOR);
  const cursor = cursorText ? (JSON.parse(cursorText) as RevisionCursor) : null;

  const revisionColumns = `org_id, session_id, revision,
        to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_text`;
  const revisions = cursor
    ? await neon.query<RevisionRow>(
        `SELECT ${revisionColumns}
         FROM sessions.session_revisions
         WHERE (updated_at, org_id, session_id) > ($1::timestamptz, $2, $3)
         ORDER BY updated_at, org_id, session_id
         LIMIT $4`,
        [cursor.ts, cursor.orgId, cursor.sessionId, revisionLimit],
      )
    : await neon.query<RevisionRow>(
        `SELECT ${revisionColumns}
         FROM sessions.session_revisions
         WHERE updated_at > $1::timestamptz
         ORDER BY updated_at, org_id, session_id
         LIMIT $2`,
        [since, revisionLimit],
      );
  // updated_at is the writing transaction's start time, so a transaction
  // still open can commit rows stamped before now. Never move the watermark
  // past the oldest open transaction (the lookback re-reads behind it).
  const [oldest] = await neon.query<{ oldest: string | null }>(
    `SELECT to_char(min(xact_start) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS oldest
     FROM pg_stat_activity
     WHERE xact_start IS NOT NULL AND state <> 'idle' AND pid <> pg_backend_pid()`,
    [],
  );
  const openSinceMs = oldest?.oldest ? Date.parse(oldest.oldest) : Infinity;

  const known = new Map<string, SessionState>();
  for (let i = 0; i < revisions.length; i += 500) {
    const pairs = revisions
      .slice(i, i + 500)
      .map((r) => tuple(r.org_id, r.session_id));
    const rows = await ch.select<{
      org_id: string;
      session_id: string;
      revision: number;
      synced_ms: number;
      full_ms: number;
      scanned: number;
      needs_full: number;
    }>(
      `SELECT org_id, session_id, argMax(revision, synced_at) AS revision,
          toUnixTimestamp64Milli(max(synced_at)) AS synced_ms,
          toUnixTimestamp64Milli(argMax(full_synced_at, synced_at)) AS full_ms,
          argMax(scanned_rows, synced_at) AS scanned,
          argMax(needs_full, synced_at) AS needs_full
       FROM ${SESSION_STATE_TABLE}
       WHERE (org_id, session_id) IN ${arrayLiteral(pairs)}
       GROUP BY org_id, session_id`,
    );
    for (const row of rows) {
      known.set(`${row.org_id}${KEY_SEP}${row.session_id}`, {
        revision: Number(row.revision),
        syncedMs: Number(row.synced_ms),
        fullMs: Number(row.full_ms),
        scanned: Number(row.scanned),
        needsFull: Number(row.needs_full) === 1,
      });
    }
  }

  // The floor is compared with the time a reconcile STARTED reading Neon;
  // the margin absorbs clock skew between the backfill host and the Worker.
  const floorMs = floor ? Date.parse(floor) + FLOOR_MARGIN_MS : 0;
  summary.saturated = revisions.length >= revisionLimit;
  const pastDeadline = () => now().getTime() > deadline;
  let advanceTo: string | null = null;
  let lastCovered: RevisionRow | null = null;
  let blocked = false;
  for (const rev of revisions) {
    const revision = Number(rev.revision);
    const state = known.get(`${rev.org_id}${KEY_SEP}${rev.session_id}`);
    const current =
      state && state.revision === revision && state.syncedMs >= floorMs;
    if (current) {
      if (!blocked) {
        advanceTo = rev.updated_text;
        lastCovered = rev;
      }
      continue;
    }
    summary.changed++;
    if (blocked || summary.reconciled >= maxSessions || pastDeadline()) {
      blocked = true;
      summary.deferred++;
      continue;
    }
    const target = { orgId: rev.org_id, sessionId: rev.session_id };
    const nowMs = now().getTime();
    const partial =
      !!state &&
      state.scanned > largeSessionRows &&
      nowMs - state.fullMs < fullEveryMs;
    const rangeFrom = partial
      ? new Date(Math.max(Date.parse(from), nowMs - recentMs)).toISOString()
      : from;
    const readStart = now();
    let result: ReconcileResult;
    try {
      result = await reconcileSession(neon, ch, target, rangeFrom, {
        limits: o.limits,
        now: () => now().getTime(),
        deadline,
      });
    } catch (error) {
      // One failing session must not wedge the tail: park it for the sweep
      // (which retries it at most once per fullEveryMs) and move on.
      summary.failed++;
      console.error(
        JSON.stringify({
          message: "search ingest session reconcile failed",
          error: (error as { name?: string })?.name ?? "Error",
          code: (error as { code?: string })?.code ?? null,
        }),
      );
      await writeSessionState(ch, target, {
        revision,
        syncedAt: readStart,
        fullAt: now(),
        // 0 marks a failed repair: never counted as harmlessly "capped".
        scanned: 0,
        needsFull: true,
      });
      advanceTo = rev.updated_text;
      lastCovered = rev;
      continue;
    }
    summary.reconciled++;
    summary.inserted += result.inserted;
    summary.deleted += result.deleted;
    summary.scanned += result.scanned;
    if (result.timedOut) {
      if (summary.reconciled === 1) {
        // It had the whole budget and still did not finish: park it for the
        // sweep rather than retrying it first (and blocking) every minute.
        summary.failed++;
        console.error(
          JSON.stringify({
            message: "search ingest session exceeded the run budget",
            scanned: result.scanned,
          }),
        );
        await writeSessionState(ch, target, {
          revision,
          syncedAt: readStart,
          fullAt: now(),
          scanned: 0, // timed out: a pending repair, not a capped scan
          needsFull: true,
        });
        advanceTo = rev.updated_text;
        lastCovered = rev;
        continue;
      }
      // Out of time after other sessions: retry it first next run.
      blocked = true;
      summary.deferred++;
      continue;
    }
    // A recent-only pass, or a full scan cut at maxScanRows, cannot prove
    // older rows are current: the session stays needs_full for the sweep.
    const needsFull = partial || !result.complete;
    await writeSessionState(ch, target, {
      revision,
      syncedAt: readStart,
      fullAt: needsFull ? new Date(state?.fullMs ?? 0) : readStart,
      scanned: partial ? state!.scanned : result.scanned,
      needsFull,
    });
    advanceTo = rev.updated_text;
    lastCovered = rev;
  }

  if (summary.saturated) {
    // Page on by keyset from the last contiguous covered row.
    const at = lastCovered ?? null;
    if (at) {
      await writeState(
        ch,
        STATE_TAIL_CURSOR,
        JSON.stringify({
          ts: at.updated_text,
          orgId: at.org_id,
          sessionId: at.session_id,
        } satisfies RevisionCursor),
        now(),
      );
    }
  } else if (cursor && !blocked) {
    await writeState(ch, STATE_TAIL_CURSOR, "", now());
  } else if (cursor && blocked && lastCovered) {
    await writeState(
      ch,
      STATE_TAIL_CURSOR,
      JSON.stringify({
        ts: lastCovered.updated_text,
        orgId: lastCovered.org_id,
        sessionId: lastCovered.session_id,
      } satisfies RevisionCursor),
      now(),
    );
  }
  if (advanceTo && Date.parse(advanceTo) > openSinceMs) {
    advanceTo = new Date(openSinceMs).toISOString();
  }
  if (advanceTo && Date.parse(advanceTo) > Date.parse(watermark)) {
    await writeState(ch, STATE_TAIL_WATERMARK, advanceTo, now());
    summary.watermark = advanceTo;
  }

  // Sweep: full passes for sessions left needs_full, whether or not they
  // changed again, once fullEveryMs has passed since their last full pass.
  if (!blocked && !pastDeadline() && sweepSessions > 0) {
    const due = await ch.select<{
      org_id: string;
      session_id: string;
      revision: number;
      scanned: number;
    }>(
      `SELECT org_id, session_id, argMax(revision, synced_at) AS revision,
          argMax(scanned_rows, synced_at) AS scanned
       FROM ${SESSION_STATE_TABLE}
       GROUP BY org_id, session_id
       HAVING argMax(needs_full, synced_at) = 1
          AND toUnixTimestamp64Milli(argMax(full_synced_at, synced_at)) < {before:Int64}
       ORDER BY argMax(full_synced_at, synced_at)
       LIMIT {limit:UInt32}`,
      {
        params: {
          before: now().getTime() - fullEveryMs,
          limit: sweepSessions,
        },
      },
    );
    for (const row of due) {
      if (pastDeadline()) break;
      const target = { orgId: row.org_id, sessionId: row.session_id };
      const readStart = now();
      let result: ReconcileResult;
      try {
        result = await reconcileSession(neon, ch, target, from, {
          limits: o.limits,
          now: () => now().getTime(),
          deadline,
        });
      } catch {
        summary.failed++;
        await writeSessionState(ch, target, {
          revision: Number(row.revision),
          syncedAt: readStart,
          fullAt: now(),
          scanned: 0,
          needsFull: true,
        });
        continue;
      }
      if (result.timedOut) {
        // Back off another fullEveryMs rather than retrying every minute.
        await writeSessionState(ch, target, {
          revision: Number(row.revision),
          syncedAt: readStart,
          fullAt: now(),
          // Keep the larger size: a cut-short count must not demote a
          // large session to full scans on its next change.
          scanned: 0, // timed out: a pending repair, not a capped scan
          needsFull: true,
        });
        break;
      }
      summary.swept++;
      summary.inserted += result.inserted;
      summary.deleted += result.deleted;
      summary.scanned += result.scanned;
      // Revision stays as recorded: a change since then is picked up by the
      // revision scan. An incomplete scan backs off another fullEveryMs.
      await writeSessionState(ch, target, {
        revision: Number(row.revision),
        syncedAt: readStart,
        fullAt: readStart,
        scanned: result.scanned,
        needsFull: !result.complete,
      });
    }
  }
  return summary;
}
