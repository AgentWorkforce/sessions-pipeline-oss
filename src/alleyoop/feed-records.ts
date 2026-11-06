/**
 * The client's change feed as RelayHistory records: the same `origin_id`,
 * `record_id` and `revision` its uploads to Neon carry (relayhistory
 * `crates/ai-hist/src/export.rs`), so the two copies of a record fold in the
 * transform. An `ndjson` stream must carry these rows, not transcript lines,
 * whose identity nothing else shares.
 */
import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

interface FeedKind {
  kind: string;
  table: string;
  session: string;
  key: readonly string[];
  /** The columns a tombstone's `record_key` holds. */
  record: readonly string[];
}

const located = [
  "source",
  "session_id",
  "location",
  "connector_id",
  "connector_instance",
];

/** relayhistory `ChangeKind::table`, for the kinds the export carries. */
const FEED_KINDS: readonly FeedKind[] = [
  {
    kind: "session",
    table: "sessions",
    session: "session_id",
    key: ["source", "session_id"],
    record: ["session_id"],
  },
  {
    kind: "session_event",
    table: "session_events",
    session: "session_id",
    key: ["source", "session_id", "event_uid"],
    record: ["event_uid"],
  },
  {
    kind: "tool_call",
    table: "tool_calls",
    session: "session_id",
    key: ["source", "session_id", "tool_use_id"],
    record: ["tool_use_id"],
  },
  {
    kind: "file_edit",
    table: "file_edits",
    session: "session_id",
    key: ["source", "session_id", "tool_use_id"],
    record: ["tool_use_id"],
  },
  {
    kind: "session_marker",
    table: "session_markers",
    session: "session_id",
    key: ["source", "session_id", "marker_uid"],
    record: ["marker_uid"],
  },
  {
    kind: "relationship",
    table: "session_relationships",
    session: "parent_session_id",
    key: ["source", "parent_session_id", "relationship_uid"],
    record: ["relationship_uid"],
  },
  {
    kind: "history",
    table: "history",
    session: "session_id",
    key: ["source", "timestamp_ms", "prompt"],
    record: ["timestamp_ms", "prompt"],
  },
  {
    kind: "presence",
    table: "session_presences",
    session: "session_id",
    key: ["source", "session_id", "location"],
    record: ["location"],
  },
  {
    kind: "source_observation",
    table: "session_observations",
    session: "session_id",
    key: located,
    record: located.slice(2),
  },
  {
    kind: "observation_evidence",
    table: "observation_evidence",
    session: "session_id",
    key: [...located, "evidence_uid"],
    record: [...located.slice(2), "evidence_uid"],
  },
];

/** The feed's own bookkeeping, never part of a record's payload (`LOCAL_COLUMNS`). */
const localColumn = (table: string, column: string) =>
  column === "revision" ||
  (table === "session_events" && column === "raw_facts_version");

export interface FeedRecord {
  revision: number;
  line: string;
}

export interface FeedRange {
  /** The revision cursor this range continues from. */
  start: number;
  /** The highest revision in it. */
  end: number;
  bytes: Buffer;
}

/** `sha256(json(["kind", ...key]))`, as `make_record` hashes the feed key. */
export function feedRecordId(key: readonly unknown[]): string {
  return createHash("sha256").update(JSON.stringify(key)).digest("hex");
}

function hasTable(db: DatabaseSync, name: string): boolean {
  return (
    db
      .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
      .get(name) !== undefined
  );
}

function originOf(db: DatabaseSync): string | undefined {
  if (!hasTable(db, "change_feed_store")) return undefined;
  const row = db
    .prepare("SELECT epoch FROM change_feed_store WHERE singleton = 1")
    .get() as { epoch: number } | undefined;
  return row ? row.epoch.toString(16).padStart(16, "0") : undefined;
}

/** The store's feed identity, or `undefined` when it has no change feed. */
export function feedOrigin(dbPath: string): string | undefined {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return originOf(db);
  } finally {
    db.close();
  }
}

/** Stored as `stored_value` reports it: a blob is an array of bytes. */
const stored = (value: unknown) =>
  value instanceof Uint8Array ? [...value] : value;

function envelope(
  origin: string,
  kind: string,
  key: readonly unknown[],
  revision: number,
  payload: Record<string, unknown> | null,
): FeedRecord {
  return {
    revision,
    line: JSON.stringify({
      kind,
      origin_id: origin,
      record_id: feedRecordId([kind, ...key]),
      revision: String(revision),
      operation: payload ? "upsert" : "delete",
      received_at: new Date().toISOString(),
      payload,
    }),
  };
}

function liveRows(
  db: DatabaseSync,
  feed: FeedKind,
  origin: string,
  source: string,
  sessionId: string,
  after: number,
): FeedRecord[] {
  if (!hasTable(db, feed.table)) return [];
  const rows = db
    .prepare(
      `SELECT * FROM ${feed.table} WHERE source = ? AND ${feed.session} = ? AND revision > ?`,
    )
    .all(source, sessionId, after) as Record<string, unknown>[];
  return rows.map((row) => {
    const payload = Object.fromEntries(
      Object.entries(row)
        .filter(([column]) => !localColumn(feed.table, column))
        .map(([column, value]) => [column, stored(value)]),
    );
    return envelope(
      origin,
      feed.kind,
      feed.key.map((column) => stored(row[column] ?? null)),
      Number(row.revision),
      payload,
    );
  });
}

function tombstones(
  db: DatabaseSync,
  origin: string,
  source: string,
  sessionId: string,
  after: number,
): FeedRecord[] {
  if (!hasTable(db, "evidence_tombstones")) return [];
  const rows = db
    .prepare(
      "SELECT kind, record_key, revision FROM evidence_tombstones WHERE source = ? AND session_id = ? AND revision > ?",
    )
    .all(source, sessionId, after) as {
    kind: string;
    record_key: string;
    revision: number;
  }[];
  return rows.flatMap((row) => {
    const feed = FEED_KINDS.find((candidate) => candidate.kind === row.kind);
    if (!feed) return [];
    const record: unknown[] =
      feed.record.length === 1
        ? [row.record_key]
        : (JSON.parse(row.record_key) as unknown[]);
    const key = feed.key.map((column) => {
      const at = feed.record.indexOf(column);
      if (at >= 0) return record[at] ?? null;
      return column === "source" ? source : sessionId;
    });
    return [envelope(origin, feed.kind, key, Number(row.revision), null)];
  });
}

/** Every change to one session after `after`, oldest first. */
export function readFeedRecords(
  dbPath: string,
  source: string,
  sessionId: string,
  after: number,
): { origin?: string; records: FeedRecord[] } {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const origin = originOf(db);
    if (!origin) return { records: [] };
    const records = [
      ...FEED_KINDS.flatMap((feed) =>
        liveRows(db, feed, origin, source, sessionId, after),
      ),
      ...tombstones(db, origin, source, sessionId, after),
    ].sort((a, b) => a.revision - b.revision);
    return { origin, records };
  } finally {
    db.close();
  }
}

/** Records cut into chunks at a record boundary, never inside one revision. */
export function feedRanges(
  records: readonly FeedRecord[],
  from: number,
  maxBytes: number,
): FeedRange[] {
  const ranges: FeedRange[] = [];
  let lines: string[] = [];
  let size = 0;
  let start = from;
  let end = from;
  const seal = () => {
    if (lines.length === 0) return;
    ranges.push({ start, end, bytes: Buffer.from(`${lines.join("\n")}\n`) });
    start = end;
    lines = [];
    size = 0;
  };
  for (const record of records) {
    const length = Buffer.byteLength(record.line) + 1;
    if (size > 0 && size + length > maxBytes && record.revision !== end) seal();
    lines.push(record.line);
    size += length;
    end = record.revision;
  }
  seal();
  return ranges;
}
