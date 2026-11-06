import { nodeReadBudget, type NodeReadProfile } from "../session-read-api/node-read-profile.ts";
/** Node-only, write-before-publication materializer. No Worker source-budget lift. */
import { createHash } from "node:crypto";
import {
  ARTIFACT_VERSION, ARTIFACT_TABLES, ARTIFACT_MAX_OBJECT_BYTES, ARTIFACT_MAX_ROWS_PER_PAGE,
  ARTIFACT_FANOUT, ARTIFACT_MAX_DEPTH, ARTIFACT_STRING_SEGMENT_CHARS, ARTIFACT_MAX_ROW_BYTES,
  ARTIFACT_BLOB_SEGMENT_BYTES, artifactViewRow, compareArtifactEventRows,
  type ArtifactIdentity, type ArtifactSource, type ArtifactRef, type ArtifactRangeRef,
  type ArtifactNode, type ArtifactEntry, type ArtifactPage, type ArtifactIndex,
  type ArtifactRow, type ArtifactView, type ArtifactBlob, type ArtifactString,
} from "../session-read-api/canonical-artifact.ts";
import {comparePublicTurnRows} from "../shared/legacy-turn-source.ts";
import { canonicalTimestamp } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { readCapped } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { objectKey, type ObjectStore } from "../../packages/object-store/src/index.ts";
const encoder = new TextEncoder();
export const artifactDigest = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
export class ArtifactBuildError extends Error {
  constructor(readonly code: string) { super(code); this.name = "ArtifactBuildError"; }
}
function refuse(code: string): never { throw new ArtifactBuildError(code); }
const bytesOf = (value: unknown) => encoder.encode(JSON.stringify(value));
const textCompare = (a: unknown, b: unknown) => Buffer.compare(Buffer.from(String(a ?? "")), Buffer.from(String(b ?? "")));
const time = (v: unknown) => v == null ? "" : canonicalTimestamp(String(v));
/** The same event byte order as the raw provider; legacy numeric keys stay numeric. */
export function compareArtifactRows(a: ArtifactRow, b: ArtifactRow): number {
  const x = a.row, y = b.row;
  if (a.table === "session_analysis_requests" || a.table === "session_analyses") return Number(x.ordinal)-Number(y.ordinal);
  if (a.table === "convergence_events") {
    return compareArtifactEventRows(x,y);
  }
  if (a.table === "conversation_turns") return comparePublicTurnRows(x,y);
  if (a.table === "session_links") {
    const t = textCompare(time(x.link_ts ?? x.updated_at ?? x.created_at), time(y.link_ts ?? y.updated_at ?? y.created_at));
    return t || (BigInt(String(x.id)) < BigInt(String(y.id)) ? -1 : BigInt(String(x.id)) > BigInt(String(y.id)) ? 1 : 0);
  }
  if (a.table === "session_outcomes")
    return textCompare(time(x.shipped_at ?? x.ingested_at ?? x.created_at), time(y.shipped_at ?? y.ingested_at ?? y.created_at)) || textCompare(x.commit_sha, y.commit_sha);
  return textCompare(JSON.stringify(x), JSON.stringify(y));
}
export interface MaterializeInput {
  identity: ArtifactIdentity; generation: string; source: ArtifactSource; prefix: string;
  profile?: NodeReadProfile;
  /** Declare empty protocol tables explicitly; missing views remain invalid. */
  additionalTables?: readonly string[];
  rows: ArtifactRow[]; legacyCoverage: ArtifactIndex["legacyCoverage"];
}
export interface MaterializeResult { index: ArtifactRef; document: ArtifactIndex; objects: number; bytes: number }
class Writer {
  readonly written = new Map<string, ArtifactRef>();
  bytes = 0;
  constructor(readonly store: ObjectStore, readonly input: MaterializeInput) {}
  async put(bytes: Uint8Array, extension: "json" | "bin" = "json"): Promise<ArtifactRef> {
    if (!bytes.length || bytes.length > ARTIFACT_MAX_OBJECT_BYTES) refuse("artifact_object_bound");
    const sha256 = artifactDigest(bytes), key = `${this.input.prefix}${sha256}.${extension}`;
    const cached = this.written.get(key); if (cached) return cached;
    // Output expansion is bounded separately from the admitted raw source budget.
    if (this.bytes + bytes.length > nodeReadBudget(this.input.profile).artifactBytes) refuse("artifact_output_bound");
    this.bytes += bytes.length;
    const ack = await this.store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: sha256, contentType: extension === "json" ? "application/json" : "application/octet-stream" });
    if (ack.contentSha256 !== sha256 || ack.size !== bytes.length) refuse("artifact_write_ack_invalid");
    // A put acknowledgement alone cannot distinguish a correct object from an adapter defect.
    const read = await readCapped(await this.store.get(objectKey(key)), bytes.length);
    if (!read || read.length !== bytes.length || artifactDigest(read) !== sha256) refuse("artifact_write_verification_failed");
    const ref = { key, sha256, bytes: bytes.length }; this.written.set(key, ref); return ref;
  }
  json(value: unknown) { return this.put(bytesOf(value)); }
  async tree(children: ArtifactRangeRef[], unit: ArtifactNode["unit"], table?: string, view?: ArtifactView): Promise<ArtifactRangeRef> {
    if (!children.length) refuse("artifact_empty_tree");
    let refs = children, depth = 0;
    while (refs.length > 1) {
      if (++depth > ARTIFACT_MAX_DEPTH) refuse("artifact_depth_bound");
      const next: ArtifactRangeRef[] = [];
      for (let i = 0; i < refs.length; i += ARTIFACT_FANOUT) {
        const group = refs.slice(i, i + ARTIFACT_FANOUT), start = group[0]!.start;
        let count = 0;
        for (const r of group) { if (r.start !== start + count || r.count <= 0) refuse("artifact_range_invalid"); count += r.count; }
        const node: ArtifactNode = { version: 1, kind: "range-node", identity: this.input.identity, generation: this.input.generation, unit, ...(table ? {table} : {}), ...(view ? {view} : {}), start, count, children: group };
        next.push({ ...await this.json(node), kind: "node", start, count });
      }
      refs = next;
    }
    return refs[0]!;
  }
  async blob(row: Record<string, unknown>): Promise<ArtifactBlob> {
    const bytes = bytesOf(row);
    const refs: ArtifactRangeRef[] = [];
    for (let at = 0; at < bytes.length; at += ARTIFACT_BLOB_SEGMENT_BYTES) {
      const part = bytes.subarray(at, at + ARTIFACT_BLOB_SEGMENT_BYTES);
      refs.push({ ...await this.put(part, "bin"), kind: "segment", start: at, count: part.length });
    }
    return { codec: "json-utf8-segments-v1", bytes: bytes.length, sha256: artifactDigest(bytes), root: await this.tree(refs, "bytes") };
  }
  async string(value: string): Promise<ArtifactString> {
    const refs: ArtifactRangeRef[] = [];
    for (let at = 0; at < value.length;) {
      let lo = 1, hi = Math.min(ARTIFACT_STRING_SEGMENT_CHARS,value.length-at);
      // Choose a large bounded prefix. Completing a surrogate pair can reduce
      // encoded bytes slightly, so the final byte check remains authoritative.
      if (bytesOf(value.slice(at,at+hi)).length > ARTIFACT_BLOB_SEGMENT_BYTES) {
        while (lo < hi) { const mid = Math.ceil((lo+hi)/2); if (bytesOf(value.slice(at,at+mid)).length <= ARTIFACT_BLOB_SEGMENT_BYTES) lo=mid; else hi=mid-1; }
      } else lo=hi;
      const part=value.slice(at,at+lo),bytes=bytesOf(part);
      if (!part.length || bytes.length > ARTIFACT_BLOB_SEGMENT_BYTES) refuse("artifact_string_segment_bound");
      refs.push({ ...await this.put(bytes), kind: "segment", start: at, count: part.length });
      at += part.length;
    }
    return { codec: "json-string-segments-v1", chars: value.length, jsonSha256: artifactDigest(bytesOf(value)), root: await this.tree(refs, "utf16") };
  }
  async entry(item: ArtifactRow, view: ArtifactView): Promise<ArtifactEntry> {
    let row = artifactViewRow(item, view).row;
    // Never acknowledge a generation containing a row the public consumer refuses.
    // Measure the hydrated envelope before splitting content or nested objects.
    if (bytesOf({table:item.table,row}).length > ARTIFACT_MAX_ROW_BYTES) refuse("artifact_row_bound");
    let content: ArtifactString | undefined;
    if (view === "replay" && typeof row.content === "string" && bytesOf(row.content).length > 32 * 1024) {
      content = await this.string(row.content); row = { ...row, content: null };
    }
    // Arbitrarily nested arrays/objects use the same bounded byte tree, not string-only paths.
    const inline = { table: item.table, row, ...(content ? {content} : {}) };
    return bytesOf(inline).length <= ARTIFACT_MAX_OBJECT_BYTES / 2 ? inline : { table: item.table, rowJson: await this.blob(row), ...(content ? {content} : {}) };
  }
  async table(items: ArtifactRow[], view: ArtifactView, table: string): Promise<{count: number; root: ArtifactRangeRef | null}> {
    if (!items.length) return { count: 0, root: null };
    const refs: ArtifactRangeRef[] = [];
    let rows: ArtifactEntry[] = [], start = 0, size = 0;
    const writes: Promise<ArtifactRangeRef>[] = [];
    const page = (): ArtifactPage => ({ version: 1, kind: "row-page", identity: this.input.identity, generation: this.input.generation, table, view, start, rows });
    const overhead = bytesOf({ ...page(), start: 2_000_000 }).length;
    const flush = async () => {
      if (!rows.length) return;
      const value=page(),at=start,count=rows.length;
      // At most four page puts/read-backs are in flight, with ordered results.
      const work=this.json(value).then(ref=>({...ref,kind:"page" as const,start:at,count}));
      void work.catch(()=>{}); // handled now; awaited in ordinal order below
      writes.push(work);
      start+=count;rows=[];size=0;
      if(writes.length>=4)refs.push(await writes.shift()!);
    };
    try {
    for (const item of items) {
      const entry = await this.entry(item, view), bytes = bytesOf(entry).length;
      if (rows.length && (rows.length >= ARTIFACT_MAX_ROWS_PER_PAGE || overhead + size + bytes + rows.length > ARTIFACT_MAX_OBJECT_BYTES)) await flush();
      if (overhead + bytes > ARTIFACT_MAX_OBJECT_BYTES) refuse("artifact_entry_bound");
      rows.push(entry); size += bytes;
    }
    await flush();
    refs.push(...await Promise.all(writes));
    } catch(error) { await Promise.allSettled(writes); throw error; }
    return { count: items.length, root: await this.tree(refs, "rows", table, view) };
  }
}
/** Called only after authoritative full-union projection succeeds. Publication is a separate CAS. */
export async function materializeCanonicalArtifacts(store: ObjectStore, input: MaterializeInput): Promise<MaterializeResult> {
  if (!/^[0-9a-f]{64}$/u.test(input.generation) || !/^[0-9a-f]{64}$/u.test(input.source.fingerprint) || !/^[1-9][0-9]*$/u.test(input.source.epoch) || !input.prefix.endsWith("/")) refuse("artifact_identity_invalid");
  if (input.rows.length > nodeReadBudget(input.profile).artifactRows) refuse("artifact_row_bound");
  const writer = new Writer(store, input), groups = new Map<string, ArtifactRow[]>();
  for (const item of input.rows) {
    const group = groups.get(item.table) ?? []; group.push(item); groups.set(item.table, group);
  }
  for (const table of [...ARTIFACT_TABLES, ...(input.additionalTables ?? [])]) if (!groups.has(table)) groups.set(table, []);
  const tables: ArtifactIndex["tables"] = {};
  for (const [table, items] of [...groups].sort(([a], [b]) => textCompare(a, b))) {
    items.sort(compareArtifactRows); tables[table] = {};
    for (const view of (table === "convergence_events" ? ["raw", "replay", "bounds"] : ["raw"]) as ArtifactView[])
      tables[table]![view] = await writer.table(items, view, table);
  }
  const document: ArtifactIndex = { version: ARTIFACT_VERSION, kind: "canonical-read-index", codec: "json-utf8-v1", identity: input.identity, generation: input.generation, source: input.source, prefix: input.prefix, legacyCoverage: input.legacyCoverage, tables };
  const index = await writer.json(document);
  return { index, document, objects: writer.written.size, bytes: writer.bytes };
}
