/**
 * Neon -> ClickHouse row projection, shared by the backfill and the tail so
 * both write byte-identical rows.
 *
 * Every Neon read runs through a `NeonReader`: a short READ ONLY transaction
 * with its own statement and lock timeouts (see neon-reader.ts and the
 * backfill CLI). Nothing here writes to Neon.
 */

/** One read-only statement against Neon; implementations own the transaction. */
export interface NeonReader {
  query<T>(text: string, params: unknown[]): Promise<T[]>;
}

/**
 * Change fingerprint of an event, computed in Neon. It covers every column the
 * index stores except `ts` and `workspace_id` (compared separately because
 * they are part of the ClickHouse sort key) and reads the content only through
 * `octet_length` and the stored `content_hash`, neither of which detoasts the
 * content, so a whole-session fingerprint scan stays cheap.
 */
export const FINGERPRINT_SQL = `left(md5(concat_ws(chr(31),
  type, user_id, coalesce(actor_name, ''), coalesce(actor_role, ''),
  coalesce(subagent_id, ''), coalesce(project_id, ''), coalesce(task_title, ''),
  coalesce(task_ref::text, ''), coalesce(tags::text, ''),
  coalesce(files_touched::text, ''), coalesce(tool_name, ''),
  coalesce(tool_status, ''), coalesce(delivery_record_id, ''),
  coalesce(octet_length(content), -1)::text, coalesce(content_hash, ''),
  extract(epoch FROM ingested_at)::text)), 16)`;

/** Exact UTC text for a timestamptz, used for keyset cursors (microseconds). */
export const TS_TEXT_SQL = `to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;

/** Full event columns for an index row. */
export const EVENT_COLUMNS_SQL = `org_id, workspace_id, machine_id, user_id, source,
  session_id, event_id, kind, type, ${TS_TEXT_SQL} AS ts_text,
  actor_name, actor_role, subagent_id, project_id, task_title,
  task_ref::text AS task_ref, task_ref->>'id' AS task_ref_id,
  tags::text AS tags, files_touched::text AS files_touched,
  tool_name, tool_status, delivery_record_id, content,
  to_char(ingested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS ingested_text,
  ${FINGERPRINT_SQL} AS fingerprint`;

export interface NeonEventRow {
  org_id: string;
  workspace_id: string;
  machine_id: string;
  user_id: string;
  source: string;
  session_id: string;
  event_id: string;
  kind: string;
  type: string;
  ts_text: string;
  actor_name: string | null;
  actor_role: string | null;
  subagent_id: string | null;
  project_id: string | null;
  task_title: string | null;
  task_ref: string | null;
  task_ref_id: string | null;
  tags: string | null;
  files_touched: string | null;
  tool_name: string | null;
  tool_status: string | null;
  delivery_record_id: string | null;
  content: string | null;
  ingested_text: string;
  fingerprint: string;
}

export interface IndexRow {
  org_id: string;
  workspace_id: string;
  ts: string;
  session_id: string;
  source: string;
  machine_id: string;
  kind: string;
  event_id: string;
  type: string;
  user_id: string;
  actor_name: string;
  actor_role: string;
  subagent_id: string;
  project_id: string;
  task_title: string;
  task_ref: string;
  task_ref_id: string;
  tags: string[];
  files_touched: string[];
  tool_name: string;
  tool_status: string;
  delivery_record_id: string;
  content: string;
  content_bytes: number;
  ingested_at: string;
  fingerprint: string;
  version: number;
}

const TS_TEXT = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/;

/** `2026-10-31T07:13:01.123456Z` -> `2026-10-31 07:13:01.123` (ms, truncated). */
export function chTimestamp(tsText: string): string {
  const match = TS_TEXT.exec(tsText);
  if (!match) throw new Error("unexpected timestamp format");
  const ms = (match[3] ?? "").padEnd(3, "0").slice(0, 3);
  return `${match[1]} ${match[2]}.${ms}`;
}

/** Epoch milliseconds (truncated) of a cursor timestamp text. */
export function tsMillis(tsText: string): number {
  const match = TS_TEXT.exec(tsText);
  if (!match) throw new Error("unexpected timestamp format");
  const ms = (match[3] ?? "").padEnd(3, "0").slice(0, 3);
  return Date.parse(`${match[1]}T${match[2]}.${ms}Z`);
}

function stringArray(json: string | null): string[] {
  if (!json) return [];
  try {
    const value: unknown = JSON.parse(json);
    if (!Array.isArray(value)) return [];
    return value.map((item) =>
      typeof item === "string" ? item : JSON.stringify(item),
    );
  } catch {
    return [];
  }
}

const encoder = new TextEncoder();

export function toIndexRow(row: NeonEventRow, version: number): IndexRow {
  const content = row.content ?? "";
  return {
    org_id: row.org_id,
    workspace_id: row.workspace_id,
    ts: chTimestamp(row.ts_text),
    session_id: row.session_id,
    source: row.source,
    machine_id: row.machine_id,
    kind: row.kind,
    event_id: row.event_id,
    type: row.type,
    user_id: row.user_id,
    actor_name: row.actor_name ?? "",
    actor_role: row.actor_role ?? "",
    subagent_id: row.subagent_id ?? "",
    project_id: row.project_id ?? "",
    task_title: row.task_title ?? "",
    task_ref: row.task_ref ?? "{}",
    task_ref_id: row.task_ref_id ?? "",
    tags: stringArray(row.tags),
    files_touched: stringArray(row.files_touched),
    tool_name: row.tool_name ?? "",
    tool_status: row.tool_status ?? "",
    delivery_record_id: row.delivery_record_id ?? "",
    content,
    content_bytes: encoder.encode(content).length,
    ingested_at: chTimestamp(row.ingested_text),
    fingerprint: row.fingerprint,
    version,
  };
}

/** Approximate in-memory size, for byte-bounded insert buffers. */
export function rowBytes(row: IndexRow): number {
  return row.content_bytes + row.task_title.length + 512;
}
