/** Canonical read artifacts v1. Pure protocol: every reference hashes exact stored UTF-8 bytes.
 * Tree ranges are half-open ordinals, never source timestamps or guessed row offsets.
 * User JSON stays inside row/rowJson; codec descriptors are always out of band.
 */
export const ARTIFACT_VERSION = 1 as const;
export const ARTIFACT_TABLES = ["convergence_events", "session_catalog", "session_catalog_agents", "session_relationships", "session_markers", "session_commit_links", "conversation_turns", "session_links", "session_outcomes", "session_directory_event_values"] as const;
export const ARTIFACT_MAX_OBJECT_BYTES = 256 * 1024;
export const ARTIFACT_MAX_ROW_BYTES = 64 * 1024 * 1024;
export const ARTIFACT_MAX_ROWS_PER_PAGE = 1000;
export const ARTIFACT_FANOUT = 128;
export const ARTIFACT_MAX_DEPTH = 4;
export const ARTIFACT_STRING_SEGMENT_CHARS = 65536;
export const ARTIFACT_BLOB_SEGMENT_BYTES = 64 * 1024;
export const ARTIFACT_MAX_SOURCE_BYTES = 256 * 1024 * 1024;
export const ARTIFACT_MAX_SOURCE_RECORDS = 1_000_000;
export const ARTIFACT_MAX_SOURCE_POINTERS = 10_000;
export type ArtifactView = "raw" | "replay" | "bounds";
export interface ArtifactIdentity { orgId: string; workspaceId: string; source: string; sessionId: string }
export interface ArtifactSource { epoch: string; fingerprint: string }
export interface ArtifactRef { key: string; sha256: string; bytes: number }
export interface ArtifactRangeRef extends ArtifactRef {
  /** Offset/count in this tree's declared unit: rows, bytes, or UTF-16 code units. */
  start: number; count: number; kind: "node" | "page" | "segment";
}
export interface ArtifactNode {
  version: 1; kind: "range-node"; identity: ArtifactIdentity; generation: string;
  unit: "rows" | "bytes" | "utf16";
  /** For row trees only; a node cannot be replayed as another table/view. */
  table?: string; view?: ArtifactView;
  start: number; count: number; children: ArtifactRangeRef[];
}
export interface ArtifactBlob {
  codec: "json-utf8-segments-v1";
  /** Exact bytes and hash of JSON.stringify(row), before splitting. */
  bytes: number; sha256: string; root: ArtifactRangeRef;
}
export interface ArtifactString {
  codec: "json-string-segments-v1";
  /** Hash of UTF-8 JSON.stringify(the full original string), including quotes. */
  chars: number; jsonSha256: string; root: ArtifactRangeRef;
}
export interface ArtifactEntry {
  table: string;
  /** Exactly one of row or rowJson. rowJson is segmented JSON for arbitrary arrays/objects. */
  row?: Record<string, unknown>; rowJson?: ArtifactBlob;
  /** Replay's top-level content only; row.content is null until hydrated. */
  content?: ArtifactString;
}
export interface ArtifactPage {
  version: 1; kind: "row-page"; identity: ArtifactIdentity; generation: string;
  table: string; view: ArtifactView; start: number; rows: ArtifactEntry[];
}
export interface ArtifactTableView { count: number; root: ArtifactRangeRef | null }
export interface ArtifactIndex {
  version: 1; kind: "canonical-read-index"; codec: "json-utf8-v1";
  identity: ArtifactIdentity; generation: string; source: ArtifactSource;
  /** All keys must be strictly inside this trusted physical normalized prefix. */
  prefix: string;
  legacyCoverage: null | { version: 1; tables: string[] };
  tables: Record<string, Partial<Record<ArtifactView, ArtifactTableView>>>;
}
export interface ArtifactPublication {
  version: 1; identity: ArtifactIdentity; generation: string; source: ArtifactSource;
  prefix: string; index: ArtifactRef;
}
export interface ArtifactRow { table: string; row: Record<string, unknown> }
/** Materialized views retain current canonical provider semantics. */
export function artifactViewRow(item: ArtifactRow, view: ArtifactView): ArtifactRow {
  if (view === "raw") return item;
  if (view === "bounds") {
    const r = item.row;
    return { table: item.table, row: Object.fromEntries(
      ["org_id", "workspace_id", "source", "session_id", "event_id", "machine_id", "kind", "ts"].map(k => [k, r[k]]),
    ) };
  }
  const { record, ...row } = item.row;
  const payload = record && typeof record === "object" ? (record as {payload?: unknown}).payload : undefined;
  const message = payload && typeof payload === "object" ? (payload as {message_id?: unknown}).message_id : undefined;
  return { table: item.table, row: { ...row, record: { payload: { message_id: typeof message === "string" ? message : null } } } };
}
const orderEncoder = new TextEncoder();
function compareUtf8(a: unknown,b: unknown): number {
  const x=orderEncoder.encode(String(a)),y=orderEncoder.encode(String(b));
  for(let i=0;i<Math.min(x.length,y.length);i++)if(x[i]!==y[i])return x[i]!-y[i]!;
  return x.length-y.length;
}
/** Authoritative projected event timestamps are canonical microsecond strings.
 * Preserve the raw provider's exact byte order, shared by producer and readers. */
export function compareArtifactEventRows(a: Record<string,unknown>,b: Record<string,unknown>): number {
  const x=String(a.ts),y=String(b.ts);
  return x.length-y.length || compareUtf8(x,y) || compareUtf8(a.event_id,b.event_id) || compareUtf8(a.machine_id,b.machine_id) || compareUtf8(a.kind,b.kind);
}
