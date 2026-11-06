/**
 * Neon to S3 session export, core (docs/relayhistory-export-design.md, P2).
 * Reads RelayHistory's delivery records for one session from Neon and turns
 * them into the session's raw stream: one ndjson line per live record, ordered
 * by event time, split at record boundaries into chunks of at most 4 MiB.
 * Upload goes through the courier protocol (ingest scrub hop, v1 manifest,
 * pipeline notification) in relayhistory-export-upload.ts, which the CLI does
 * not reach while `--execute` is refused. Nothing here writes anywhere; the
 * reader runs on the read-only Neon role.
 */
export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

/** The record kinds a session's stream carries (every delivery kind with a session). */
export const RECORD_KINDS = [
  "session",
  "session_event",
  "tool_call",
  "file_edit",
  "history",
  "session_marker",
  "relationship",
  "presence",
  "source_observation",
  "observation_evidence",
] as const;

export const MAX_CHUNK_BYTES = 4 * 1024 * 1024;

export interface DeliveryRecord {
  kind: string;
  origin_id: string;
  record_id: string;
  revision: string;
  operation: "upsert" | "delete";
  received_at: string;
  /** For reading fields: numbers past a double's precision are rounded here. */
  payload: Record<string, unknown> | null;
  /**
   * The source's own JSON text, written to the stream as is, because `payload`
   * has been through JSON.parse and a bigint or numeric in it would be rounded.
   */
  payload_json?: string | null;
  /**
   * The record's seat in the source's acceptance ledger, which is the order the
   * live projection settles contenders in. The transform needs it for a catalog
   * key that two records contend for, and refuses contenders without it.
   */
  accepted_seq?: string | null;
}

/** Event time of a record, in ms: the payload's own clock, else when it was received. */
export function eventTimeMs(record: DeliveryRecord): number {
  const payload = record.payload ?? {};
  for (const field of ["ts_ms", "timestamp_ms", "created_ms", "spawned_at_ms", "updated_ms", "first_activity_ms", "last_activity_ms"]) {
    const value = payload[field];
    if (typeof value === "number" && Number.isFinite(value) && value > 0) return value;
    if (typeof value === "string" && /^\d{10,16}$/.test(value)) return Number(value);
  }
  return Date.parse(record.received_at);
}

export interface SessionStream {
  bytes: Uint8Array;
  /** Byte offset where each record's line starts, plus the total length at the end. */
  lineStarts: number[];
  records: number;
  firstMs: number | null;
  lastMs: number | null;
}

/**
 * The session's raw stream: live records only (a record whose latest revision
 * is a delete is left out, so deleted content is never exported), ordered by
 * event time then origin and record id, one JSON line each. Deterministic, so
 * a re-export produces identical chunk hashes.
 */
export function buildSessionStream(records: readonly DeliveryRecord[]): SessionStream {
  const live = records
    .filter((record) => record.operation === "upsert" && record.payload !== null)
    .map((record) => ({ record, at: eventTimeMs(record) }))
    .sort(
      (a, b) =>
        a.at - b.at ||
        (a.record.origin_id < b.record.origin_id ? -1 : a.record.origin_id > b.record.origin_id ? 1 : 0) ||
        (a.record.record_id < b.record.record_id ? -1 : a.record.record_id > b.record.record_id ? 1 : 0),
    );
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];
  const lineStarts: number[] = [];
  let length = 0;
  for (const { record } of live) {
    const envelope = JSON.stringify({
      kind: record.kind,
      origin_id: record.origin_id,
      record_id: record.record_id,
      revision: record.revision,
      received_at: record.received_at,
    });
    const payload = record.payload_json ?? JSON.stringify(record.payload);
    const seat = record.accepted_seq
      ? `,"accepted_seq":${JSON.stringify(record.accepted_seq)}`
      : "";
    const line = encoder.encode(`${envelope.slice(0, -1)},"payload":${payload}${seat}}\n`);
    lineStarts.push(length);
    parts.push(line);
    length += line.byteLength;
  }
  lineStarts.push(length);
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return {
    bytes,
    lineStarts,
    records: live.length,
    firstMs: live.length ? live[0]!.at : null,
    lastMs: live.length ? live.at(-1)!.at : null,
  };
}

export interface ChunkRange {
  start: number;
  end: number;
  records: number;
}

/**
 * Split at record boundaries into ranges of at most maxBytes. A single record
 * larger than maxBytes becomes its own range (a chunk never splits a record).
 */
export function chunkRanges(stream: SessionStream, maxBytes = MAX_CHUNK_BYTES): ChunkRange[] {
  const ranges: ChunkRange[] = [];
  const starts = stream.lineStarts;
  let first = 0;
  while (first < starts.length - 1) {
    let last = first + 1;
    while (last < starts.length - 1 && starts[last + 1]! - starts[first]! <= maxBytes) last += 1;
    ranges.push({ start: starts[first]!, end: starts[last]!, records: last - first });
    first = last;
  }
  return ranges;
}

export interface SessionKey {
  org_id: string;
  workspace_id: string;
  source: string;
  session_id: string;
  last_ts: string;
}

/**
 * Sessions newest first, by their latest activity across rollups, keyset-paged
 * on (last_ts, org, workspace, source, session) descending.
 */
export async function sessionsNewestFirst(
  db: Queryable,
  after: SessionKey | null,
  limit: number,
): Promise<SessionKey[]> {
  const { rows } = await db.query<SessionKey>(
    `SELECT org_id, workspace_id, source, session_id, max(last_ts)::text AS last_ts
       FROM sessions.session_rollups
      GROUP BY org_id, workspace_id, source, session_id
     ${after ? "HAVING (max(last_ts), org_id, workspace_id, source, session_id) < ($2::timestamptz, $3, $4, $5, $6)" : ""}
      ORDER BY max(last_ts) DESC, org_id DESC, workspace_id DESC, source DESC, session_id DESC
      LIMIT $1`,
    after
      ? [limit, after.last_ts, after.org_id, after.workspace_id, after.source, after.session_id]
      : [limit],
  );
  return rows;
}

/**
 * A session's delivery records: one row per logical record, the write the live
 * projection keeps. record_id is origin-independent, and the last write accepted
 * from any origin wins, so a tombstone accepted last removes it everywhere. Only
 * the acceptance ledgers know that order (`received_at` is transaction start), so
 * it is settled here, before records split into per-machine streams that no later
 * reader could compare. A record with no ledger seat falls back to the order 0038
 * seeds one in: its projected row, then receipt, then origin.
 */
export async function sessionRecords(db: Queryable, key: SessionKey): Promise<DeliveryRecord[]> {
  const { rows } = await db.query<Omit<DeliveryRecord, "payload">>(
    `SELECT kind, origin_id, record_id, revision, operation, received_at, payload_json, accepted_seq FROM (
       SELECT d.kind, d.origin_id, d.record_id, d.revision::text AS revision, d.operation,
              d.received_at::text AS received_at, d.payload::text AS payload_json,
              s.seq::text AS accepted_seq,
              row_number() OVER (
                PARTITION BY d.record_id
                ORDER BY s.seq DESC NULLS LAST,
                  EXISTS (SELECT 1 FROM sessions.convergence_events e
                           WHERE e.org_id = d.org_id AND e.workspace_id = d.workspace_id
                             AND e.delivery_record_id = d.record_id
                             AND e.machine_id = 'delivery:' || md5(jsonb_build_array(d.workspace_id, d.origin_id)::text)) DESC,
                  d.received_at DESC, d.origin_id DESC
              ) AS place
         FROM sessions.delivery_records d
         LEFT JOIN sessions.delivery_projection_acceptance p
           ON p.org_id = d.org_id AND p.workspace_id = d.workspace_id
          AND p.origin_id = d.origin_id AND p.record_id = d.record_id
         LEFT JOIN sessions.delivery_catalog_acceptance c
           ON c.org_id = d.org_id AND c.workspace_id = d.workspace_id
          AND c.origin_id = d.origin_id AND c.record_id = d.record_id
        CROSS JOIN LATERAL (
          SELECT CASE WHEN d.kind IN ('session', 'relationship', 'session_marker', 'commit_link')
                      THEN c.accepted_seq ELSE p.accepted_seq END AS seq) AS s
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.kind = ANY ($3::text[])
          AND d.source = $4 AND d.session_id = $5
     ) AS ranked WHERE place = 1`,
    [key.org_id, key.workspace_id, [...RECORD_KINDS], key.source, key.session_id],
  );
  return rows.map((row) => ({
    ...row,
    payload: row.payload_json ? (JSON.parse(row.payload_json) as DeliveryRecord["payload"]) : null,
  }));
}

/**
 * Events with no delivery record behind them: sessions uploaded through the
 * ingest API by server-side harnesses (chatgpt, grok-bot, ...), whose content
 * exists only in convergence_events. They join the stream as
 * `convergence_event` lines (the whole event row as the payload), and their
 * machine is the event's own machine_id, so no such session is dropped.
 */
export async function undeliveredEvents(db: Queryable, key: SessionKey): Promise<DeliveryRecord[]> {
  const { rows } = await db.query<{ machine_id: string; event_id: string; created_at: string; row_json: string }>(
    `SELECT e.machine_id, e.event_id, e.ingested_at::text AS created_at,
            ((to_jsonb(e) - 'org_id' - 'workspace_id')
              || jsonb_build_object('ts_ms', floor(extract(epoch FROM e.ts) * 1000)::bigint))::text AS row_json
       FROM sessions.convergence_events e
      WHERE e.org_id = $1 AND e.session_id = $2 AND e.workspace_id = $3 AND e.source = $4
        AND e.delivery_record_id IS NULL`,
    [key.org_id, key.session_id, key.workspace_id, key.source],
  );
  return rows.map((row) => ({
    kind: "convergence_event",
    origin_id: `machine:${row.machine_id}`,
    record_id: row.event_id,
    revision: "1",
    operation: "upsert" as const,
    received_at: row.created_at,
    payload: JSON.parse(row.row_json) as Record<string, unknown>,
    payload_json: row.row_json,
  }));
}

/**
 * The machine each origin's records came from, read from the session's events
 * (which carry machine_id and link back to their delivery record). An origin
 * with no linked event maps to `unknown-origin-<origin_id>`, so its records
 * never merge into another machine's stream.
 */
export async function originMachines(db: Queryable, key: SessionKey): Promise<Map<string, string>> {
  const { rows } = await db.query<{ origin_id: string; machine_id: string }>(
    `SELECT DISTINCT ON (d.origin_id) d.origin_id, e.machine_id
       FROM sessions.convergence_events e
       JOIN sessions.delivery_records d
         ON d.org_id = e.org_id AND d.workspace_id = e.workspace_id AND d.record_id = e.delivery_record_id
      WHERE e.org_id = $1 AND e.session_id = $2 AND e.workspace_id = $3 AND e.source = $4
      ORDER BY d.origin_id, e.machine_id`,
    [key.org_id, key.session_id, key.workspace_id, key.source],
  );
  return new Map(rows.map((row) => [row.origin_id, row.machine_id]));
}

export const unknownOriginMachine = (originId: string) =>
  `unknown-origin-${originId.replace(/[^A-Za-z0-9._-]/g, "_")}`.slice(0, 128);

/** Group a session's records by machine: one exported stream per machine. */
export function byMachine(
  records: readonly DeliveryRecord[],
  machines: ReadonlyMap<string, string>,
): Map<string, DeliveryRecord[]> {
  const groups = new Map<string, DeliveryRecord[]>();
  for (const record of records) {
    const machine = machines.get(record.origin_id) ?? unknownOriginMachine(record.origin_id);
    const list = groups.get(machine) ?? [];
    list.push(record);
    groups.set(machine, list);
  }
  return groups;
}

/**
 * Sessions the export must skip because a user removed them: an upload
 * tombstone for the session, or a reset of its user's workspace history.
 * Neon is the source of truth before T0 (S3-first), so the fence is read there.
 */
export async function deletionFenced(db: Queryable, key: SessionKey): Promise<boolean> {
  const { rows } = await db.query<{ fenced: boolean }>(
    `SELECT EXISTS (SELECT 1 FROM sessions.deleted_upload_sessions d
                     WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.source = $3 AND d.session_id = $4)
         OR EXISTS (SELECT 1 FROM sessions.session_resets r
                      JOIN sessions.delivery_records x
                        ON x.org_id = r.org_id AND x.workspace_id = r.workspace_id AND x.user_id = r.user_id
                     WHERE r.org_id = $1 AND r.workspace_id = $2 AND x.source = $3 AND x.session_id = $4
                       AND x.kind = 'session') AS fenced`,
    [key.org_id, key.workspace_id, key.source, key.session_id],
  );
  return rows[0]?.fenced ?? true;
}

export interface ExportPlanRow {
  session: Omit<SessionKey, "last_ts">;
  machine: string;
  records: number;
  bytes: number;
  chunks: number;
}

/**
 * What exporting one session would produce, per machine stream. Reads only;
 * a fenced session yields nothing.
 */
export async function planSession(db: Queryable, key: SessionKey): Promise<ExportPlanRow[] | "fenced"> {
  if (await deletionFenced(db, key)) return "fenced";
  const [delivered, undelivered, machines] = await Promise.all([
    sessionRecords(db, key),
    undeliveredEvents(db, key),
    originMachines(db, key),
  ]);
  const records = [...delivered, ...undelivered];
  // Undelivered events name their machine directly.
  for (const event of undelivered) machines.set(event.origin_id, event.origin_id.slice("machine:".length));
  const { last_ts: _lastTs, ...session } = key;
  const groups = [...byMachine(records, machines)].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return groups.flatMap(([machine, group]) => {
    const stream = buildSessionStream(group);
    if (stream.records === 0) return [];
    return [{ session, machine, records: stream.records, bytes: stream.bytes.byteLength, chunks: chunkRanges(stream).length }];
  });
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

/**
 * Dry run only for now: walks sessions newest first and reports what the
 * export would write (records, bytes, chunks, fenced sessions). The upload
 * courier exists (relayhistory-export-upload.ts, tested against a local fake
 * ingest only) but `--execute` stays refused, fail closed, until the
 * migration identity grant (O2) and KMS keys (P9) exist.
 *
 * Usage: RELAYHISTORY_SOURCE_URL=... bun run migration:export --limit=100
 */
export async function main() {
  if (process.argv.includes("--execute"))
    throw new Error("--execute is not available yet: the upload needs the O2 identity grant and P9 keys");
  const url = process.env.RELAYHISTORY_SOURCE_URL;
  if (!url) throw new Error("RELAYHISTORY_SOURCE_URL is required");
  const limit = Number(argument("limit") ?? 100);
  if (!Number.isInteger(limit) || limit < 1 || limit > 100_000)
    throw new Error("--limit must be an integer from 1 to 100000");
  const { default: pg } = await import("pg");
  const db = new pg.Client({ connectionString: url, application_name: "96bulls-export-dry-run" });
  await db.connect();
  try {
    await db.query("SET default_transaction_read_only = on");
    const totals = { sessions: 0, fenced: 0, streams: 0, records: 0, bytes: 0, chunks: 0 };
    let after: SessionKey | null = null;
    while (totals.sessions < limit) {
      const page = await sessionsNewestFirst(db, after, Math.min(200, limit - totals.sessions));
      if (page.length === 0) break;
      for (const key of page) {
        totals.sessions += 1;
        const plan = await planSession(db, key);
        if (plan === "fenced") {
          totals.fenced += 1;
          continue;
        }
        for (const row of plan) {
          totals.streams += 1;
          totals.records += row.records;
          totals.bytes += row.bytes;
          totals.chunks += row.chunks;
        }
      }
      after = page.at(-1)!;
    }
    process.stdout.write(`${JSON.stringify({ dryRun: true, ...totals, newestCovered: true }, null, 2)}\n`);
  } finally {
    await db.end();
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls export failed: ${error instanceof Error ? error.message : "unknown error"}`);
    process.exit(1);
  });
