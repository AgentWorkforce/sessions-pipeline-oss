/** Bounded workerd consumer of immutable, source-acknowledged canonical pages.
 * It never folds raw records, constructs a Neon client, or falls back on misses.
 */
import { canonicalTimestamp } from "../hoopdb/search-query.ts";
import { createHash } from "node:crypto";
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import { CanonicalReadError } from "./canonical.ts";
import {
  compareArtifactEventRows,
  ARTIFACT_VERSION,
  ARTIFACT_MAX_OBJECT_BYTES,
  ARTIFACT_MAX_ROW_BYTES,
  ARTIFACT_MAX_ROWS_PER_PAGE,
  ARTIFACT_FANOUT,
  ARTIFACT_MAX_DEPTH,
  ARTIFACT_BLOB_SEGMENT_BYTES,
  type ArtifactIdentity,
  type ArtifactRef,
  type ArtifactRangeRef,
  type ArtifactIndex,
  type ArtifactNode,
  type ArtifactPage,
  type ArtifactEntry,
  type ArtifactPublication,
  type ArtifactView,
  type ArtifactString,
  type ArtifactBlob,
} from "./canonical-artifact.ts";
export interface ArtifactReadDeps {
  publication(
    identity: ArtifactIdentity,
    signal: AbortSignal,
  ): Promise<unknown>;
  read(key: string, signal: AbortSignal): Promise<ReadableStream<Uint8Array>>;
  signal: AbortSignal;
}
export interface ArtifactQuery {
  table: string;
  view?: "raw" | "replay" | "bounds" | "legacy" | "analysis";
  limit: number;
  order?: "asc" | "desc";
  cursor?: string | null;
  maxContent?: number;
  generation?: string | null;
  since?: string;
  until?: string;
}
const MAX_ROW_BYTES = ARTIFACT_MAX_ROW_BYTES,
  MAX_REPLY_BYTES = 2 * 1024 * 1024,
  MAX_FETCHES = 4096;
const encoder = new TextEncoder();
function fail(reason: string, status: 400 | 503 = 503): never {
  throw new CanonicalReadError(reason, status);
}
const obj = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const count = (v: unknown): v is number =>
  typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const hash = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{64}$/u.test(v);
const sameIdentity = (value: unknown, id: ArtifactIdentity) =>
  obj(value) &&
  value.orgId === id.orgId &&
  value.workspaceId === id.workspaceId &&
  value.source === id.source &&
  value.sessionId === id.sessionId;
const sameSource = (a: unknown, b: unknown) =>
  obj(a) && obj(b) && a.epoch === b.epoch && a.fingerprint === b.fingerprint;
function source(value: unknown, absent = false) {
  if (
    !obj(value) ||
    !hash(value.fingerprint) ||
    !(
      (typeof value.epoch === "string" &&
        /^(0|[1-9]\d*)$/u.test(value.epoch)) ||
      (absent && value.epoch === null)
    )
  )
    fail("artifact_publication");
}
function publication(
  value: unknown,
  id: ArtifactIdentity,
): { exists: false; fingerprint: string } | ArtifactPublication {
  if (Array.isArray(value)) {
    if (value.length !== 1) fail("artifact_publication");
    value = value[0];
  }
  if (
    !obj(value) ||
    value.version !== ARTIFACT_VERSION ||
    !sameIdentity(value.identity, id) ||
    typeof value.exists !== "boolean" ||
    typeof value.current !== "boolean"
  )
    fail("artifact_publication");
  source(value.source, !value.exists);
  if (!value.exists) {
    if (value.current) fail("artifact_publication");
    return {
      exists: false,
      fingerprint: (value.source as { fingerprint: string }).fingerprint,
    };
  }
  if (!value.current) fail("artifact_not_ready");
  if (
    !hash(value.generation) ||
    typeof value.prefix !== "string" ||
    !value.prefix.endsWith("/") ||
    !obj(value.index)
  )
    fail("artifact_publication");
  return {
    version: 1,
    identity: id,
    generation: value.generation,
    source: value.source,
    prefix: value.prefix,
    index: value.index,
  } as unknown as ArtifactPublication;
}
function decode(bytes: Uint8Array): unknown {
  try {
    return JSON.parse(
      new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes),
    );
  } catch {
    return fail("artifact_parse");
  }
}
const cursorEncode = (v: unknown) =>
  btoa(
    Array.from(encoder.encode(JSON.stringify(v)), (b) =>
      String.fromCharCode(b),
    ).join(""),
  );
function cursorDecode(v: string): unknown {
  try {
    return decode(Uint8Array.from(atob(v), (c) => c.charCodeAt(0)));
  } catch {
    return fail("cursor", 400);
  }
}
/** Exact bytes are bounded and authenticated before JSON parsing. Used by both
 * canonical row graphs and the analysis-cache wrapper, never arbitrary caller keys. */
export function assertArtifactReference(
  ref: unknown,
  prefix: string,
  max = ARTIFACT_MAX_OBJECT_BYTES,
): asserts ref is ArtifactRef {
  if (
    !obj(ref) ||
    typeof ref.key !== "string" ||
    !ref.key.startsWith(prefix) ||
    ref.key === prefix ||
    /[\\%?#\u0000-\u0020]/u.test(ref.key) ||
    ref.key.split("/").some((p) => p === "." || p === "..") ||
    !hash(ref.sha256) ||
    !count(ref.bytes) ||
    ref.bytes === 0 ||
    ref.bytes > max
  )
    fail("artifact_reference");
}
export async function readArtifactBytes(
  deps: Pick<ArtifactReadDeps, "read" | "signal">,
  ref: ArtifactRef,
  prefix: string,
  max = ARTIFACT_MAX_OBJECT_BYTES,
) {
  assertArtifactReference(ref, prefix, max);
  const signal = deps.signal;
  if (signal.aborted) fail("deadline");
  const stream = await deps.read(ref.key, deps.signal);
  const reader = stream.getReader();
  let onAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => {
      reject(new CanonicalReadError("deadline"));
      void reader.cancel().catch(() => {});
    };
  });
  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) onAbort();
  const chunks: Uint8Array[] = [];
  let n = 0;
  try {
    for (;;) {
      const chunk = await Promise.race([reader.read(), aborted]);
      if (chunk.done) break;
      n += chunk.value.byteLength;
      if (n > ref.bytes || n > max) fail("artifact_byte_bound");
      chunks.push(chunk.value);
    }
    if (n !== ref.bytes) fail("artifact_byte_bound");
    const data = new Uint8Array(n);
    let offset = 0;
    for (const c of chunks) {
      data.set(c, offset);
      offset += c.byteLength;
    }
    if ((await sha256Hex(data)) !== ref.sha256) fail("artifact_digest");
    return data;
  } finally {
    signal.removeEventListener("abort", onAbort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
export async function readArtifactJson(
  deps: Pick<ArtifactReadDeps, "read" | "signal">,
  ref: ArtifactRef,
  prefix: string,
) {
  return decode(await readArtifactBytes(deps, ref, prefix));
}
interface Cursor {
  version: 1;
  identity: ArtifactIdentity;
  generation: string;
  indexSha256: string;
  table: string;
  view: string;
  order: string;
  maxContent: number | null;
  offset: number;
  since?: string | null;
  until?: string | null;
}
function queryCursor(query: ArtifactQuery, id: ArtifactIdentity) {
  if (!query.cursor) return null;
  if (query.cursor.length > 4096 || !query.cursor.startsWith("artifact-v1:"))
    fail("cursor", 400);
  const c = cursorDecode(query.cursor.slice("artifact-v1:".length));
  if (
    !obj(c) ||
    c.version !== 1 ||
    !sameIdentity(c.identity, id) ||
    !hash(c.generation) ||
    !hash(c.indexSha256) ||
    c.table !== query.table ||
    c.view !== (query.view ?? "raw") ||
    c.order !== (query.order ?? "asc") ||
    c.maxContent !== (query.maxContent ?? null) ||
    (c.since ?? null) !== (query.since ?? null) ||
    (c.until ?? null) !== (query.until ?? null) ||
    !count(c.offset)
  )
    fail("cursor", 400);
  return c as unknown as Cursor;
}
export async function canonicalArtifactPage(
  deps: ArtifactReadDeps,
  identity: ArtifactIdentity,
  query: ArtifactQuery,
) {
  const stop = new AbortController(),
    signal = AbortSignal.any([deps.signal, stop.signal]);
  try {
    const view = query.view ?? "raw",
      order = query.order ?? "asc";
    if (
      !["raw", "replay", "bounds", "legacy", "analysis"].includes(view) ||
      !["asc", "desc"].includes(order) ||
      !Number.isInteger(query.limit) ||
      query.limit < 1 ||
      query.limit > 1000 ||
      (["replay", "bounds"].includes(view) &&
        query.table !== "convergence_events") ||
      (view === "legacy" &&
        !["conversation_turns", "session_links", "session_outcomes"].includes(
          query.table,
        )) ||
      (view === "analysis" &&
        !["session_analysis_requests", "session_analyses"].includes(
          query.table,
        )) ||
      (query.maxContent !== undefined &&
        (view !== "replay" ||
          !count(query.maxContent) ||
          query.maxContent === 0)) ||
      (query.generation != null && !hash(query.generation)) ||
      ((query.since !== undefined || query.until !== undefined) &&
        query.table !== "convergence_events") ||
      (query.since !== undefined && !canonicalTimestamp(query.since)) ||
      (query.until !== undefined && !canonicalTimestamp(query.until))
    )
      fail("parameter", 400);
    const cursor = queryCursor(query, identity),
      lookup = publication(await deps.publication(identity, signal), identity);
    if ("exists" in lookup) {
      if (cursor) fail("cursor", 400);
      if (query.generation && query.generation !== lookup.fingerprint)
        fail("source_generation_changed");
      const fresh = publication(
        await deps.publication(identity, signal),
        identity,
      );
      if (!("exists" in fresh) || fresh.fingerprint !== lookup.fingerprint)
        fail("source_generation_changed");
      return {
        generation: lookup.fingerprint,
        exists: false,
        legacyCoverage: null,
        rows: [],
        nextCursor: null,
      };
    }
    const ack = lookup,
      storageView: ArtifactView = ["legacy", "analysis"].includes(view)
        ? "raw"
        : (view as ArtifactView);
    if (query.generation && query.generation !== ack.generation)
      fail("source_generation_changed");
    if (
      cursor &&
      (cursor.generation !== ack.generation ||
        cursor.indexSha256 !== ack.index.sha256)
    )
      fail("cursor", 400);
    let fetched = 0;
    function checkedRef(ref: unknown, max: number): asserts ref is ArtifactRef {
      assertArtifactReference(ref, ack.prefix, max);
    }
    async function bytes(ref: ArtifactRef, max = ARTIFACT_MAX_OBJECT_BYTES) {
      checkedRef(ref, max);
      if (++fetched > MAX_FETCHES) fail("artifact_fetch_bound");
      return readArtifactBytes(
        { read: (key, signal) => deps.read(key, signal), signal },
        ref,
        ack.prefix,
        max,
      );
    }
    const loaded = decode(await bytes(ack.index));
    if (
      !obj(loaded) ||
      loaded.version !== 1 ||
      loaded.kind !== "canonical-read-index" ||
      loaded.codec !== "json-utf8-v1" ||
      !sameIdentity(loaded.identity, identity) ||
      loaded.generation !== ack.generation ||
      !sameSource(loaded.source, ack.source) ||
      loaded.prefix !== ack.prefix ||
      !obj(loaded.tables)
    )
      fail("artifact_index");
    const index = loaded as unknown as ArtifactIndex;
    if (
      index.legacyCoverage !== null &&
      (!obj(index.legacyCoverage) ||
        index.legacyCoverage.version !== 1 ||
        !Array.isArray(index.legacyCoverage.tables) ||
        JSON.stringify([...index.legacyCoverage.tables].sort()) !==
          JSON.stringify(
            ["conversation_turns", "session_links", "session_outcomes"].sort(),
          ))
    )
      fail("artifact_coverage");
    // The producer materializes session_timing only when legacy turn coverage is
    // known (replay-publication.ts). Without it the table is absent by design:
    // answer like the timing route (501 timing_turns_unknown), never a retryable 503.
    if (
      query.table === "session_timing" &&
      index.legacyCoverage === null &&
      !Object.hasOwn(index.tables, "session_timing")
    )
      throw new CanonicalReadError("timing_turns_unknown", 501);
    const selected = obj(index.tables[query.table])
      ? index.tables[query.table][storageView]
      : undefined;
    if (
      !selected ||
      !count(selected.count) ||
      (selected.count === 0 ? selected.root !== null : selected.root === null)
    )
      fail("artifact_view");
    const sourceTotal = selected.count;
    let total = sourceTotal;
    let offset = cursor?.offset ?? 0;
    if (offset > total) fail("cursor", 400);
    const cache = new Map<string, unknown>();
    async function json(ref: ArtifactRef) {
      checkedRef(ref, ARTIFACT_MAX_OBJECT_BYTES);
      const key = JSON.stringify([ref.key, ref.sha256, ref.bytes]);
      if (cache.has(key)) return cache.get(key);
      const value = decode(await bytes(ref));
      if (cache.size >= 32) cache.delete(cache.keys().next().value!);
      cache.set(key, value);
      return value;
    }
    function range(
      ref: unknown,
      unit: "rows" | "bytes" | "utf16",
    ): asserts ref is ArtifactRangeRef {
      checkedRef(
        ref,
        ref && obj(ref) && ref.kind === "segment"
          ? ARTIFACT_BLOB_SEGMENT_BYTES
          : ARTIFACT_MAX_OBJECT_BYTES,
      );
      if (
        !obj(ref) ||
        !count(ref.start) ||
        !count(ref.count) ||
        ref.count === 0 ||
        !Number.isSafeInteger(ref.start + ref.count) ||
        !["node", unit === "rows" ? "page" : "segment"].includes(
          String(ref.kind),
        )
      )
        fail("artifact_range");
    }
    async function children(
      ref: ArtifactRangeRef,
      unit: "rows" | "bytes" | "utf16",
      depth: number,
      rowView: ArtifactView = storageView,
    ) {
      if (depth >= ARTIFACT_MAX_DEPTH) fail("artifact_depth");
      const value = await json(ref);
      if (
        !obj(value) ||
        value.version !== 1 ||
        value.kind !== "range-node" ||
        !sameIdentity(value.identity, identity) ||
        value.generation !== ack.generation ||
        value.unit !== unit ||
        value.start !== ref.start ||
        value.count !== ref.count ||
        !Array.isArray(value.children) ||
        !value.children.length ||
        value.children.length > ARTIFACT_FANOUT ||
        (unit === "rows" &&
          (value.table !== query.table || value.view !== rowView))
      )
        fail("artifact_node");
      let at = ref.start;
      for (const child of value.children) {
        range(child, unit);
        if (child.start !== at) fail("artifact_range");
        at += child.count;
        if (!Number.isSafeInteger(at)) fail("artifact_range");
      }
      if (at !== ref.start + ref.count) fail("artifact_range");
      return value.children as ArtifactRangeRef[];
    }
    async function pageFor(
      ref: ArtifactRangeRef,
      ordinal: number,
      depth = 0,
      rowView: ArtifactView = storageView,
    ): Promise<ArtifactPage> {
      range(ref, "rows");
      if (ordinal < ref.start || ordinal >= ref.start + ref.count)
        fail("artifact_range");
      if (ref.kind === "node") {
        for (const child of await children(ref, "rows", depth, rowView))
          if (ordinal >= child.start && ordinal < child.start + child.count)
            return pageFor(child, ordinal, depth + 1, rowView);
        return fail("artifact_range");
      }
      const p = await json(ref);
      if (
        !obj(p) ||
        p.version !== 1 ||
        p.kind !== "row-page" ||
        !sameIdentity(p.identity, identity) ||
        p.generation !== ack.generation ||
        p.table !== query.table ||
        p.view !== rowView ||
        p.start !== ref.start ||
        !Array.isArray(p.rows) ||
        !p.rows.length ||
        p.rows.length > ARTIFACT_MAX_ROWS_PER_PAGE ||
        p.rows.length !== ref.count
      )
        fail("artifact_page");
      return p as unknown as ArtifactPage;
    }
    async function leaves(
      ref: ArtifactRangeRef,
      unit: "bytes" | "utf16",
      wanted: number,
      visit: (ref: ArtifactRangeRef) => Promise<void>,
      depth = 0,
    ): Promise<void> {
      range(ref, unit);
      if (ref.start >= wanted) return;
      if (ref.kind === "node") {
        for (const child of await children(ref, unit, depth))
          await leaves(child, unit, wanted, visit, depth + 1);
      } else await visit(ref);
    }
    async function segments(
      root: ArtifactRangeRef,
      unit: "bytes" | "utf16",
      wanted: number,
      visit: (ref: ArtifactRangeRef, data: Uint8Array) => Promise<void>,
    ) {
      const refs: ArtifactRangeRef[] = [];
      await leaves(root, unit, wanted, async (ref) => {
        refs.push(ref);
      });
      const pending = new Map<
        number,
        Promise<{ data: Uint8Array } | { error: unknown }>
      >();
      let next = 0;
      const fill = () => {
        while (next < refs.length && pending.size < 4) {
          const i = next++;
          pending.set(
            i,
            bytes(refs[i]!, ARTIFACT_BLOB_SEGMENT_BYTES).then(
              (data) => ({ data }),
              (error) => ({ error }),
            ),
          );
        }
      };
      fill();
      for (let i = 0; i < refs.length; i++) {
        const result = await pending.get(i)!;
        pending.delete(i);
        fill();
        if ("error" in result) throw result.error;
        await visit(refs[i]!, result.data);
      }
    }
    async function rowJson(blob: ArtifactBlob) {
      if (
        !obj(blob) ||
        blob.codec !== "json-utf8-segments-v1" ||
        !count(blob.bytes) ||
        !blob.bytes ||
        blob.bytes > MAX_ROW_BYTES ||
        !hash(blob.sha256)
      )
        fail("artifact_row_bound");
      range(blob.root, "bytes");
      if (blob.root.start !== 0 || blob.root.count !== blob.bytes)
        fail("artifact_range");
      const data = new Uint8Array(blob.bytes);
      let at = 0;
      await segments(blob.root, "bytes", blob.bytes, async (ref, part) => {
        if (ref.start !== at || ref.bytes !== ref.count) fail("artifact_range");
        data.set(part, at);
        at += part.byteLength;
      });
      if (at !== blob.bytes || (await sha256Hex(data)) !== blob.sha256)
        fail("artifact_digest");
      const value = decode(data);
      if (!obj(value)) fail("artifact_row");
      return value;
    }
    async function content(desc: ArtifactString) {
      if (
        !obj(desc) ||
        desc.codec !== "json-string-segments-v1" ||
        !count(desc.chars) ||
        !desc.chars ||
        !hash(desc.jsonSha256)
      )
        fail("artifact_string");
      range(desc.root, "utf16");
      if (desc.root.start !== 0 || desc.root.count !== desc.chars)
        fail("artifact_range");
      const wanted = Math.min(
        desc.chars,
        query.maxContent === undefined ? desc.chars : query.maxContent + 1,
      );
      if (!count(wanted) || wanted > MAX_ROW_BYTES) fail("artifact_row_bound");
      const parts: string[] = [];
      let at = 0;
      const digest = createHash("sha256");
      digest.update('"');
      let pending = "";
      await segments(desc.root, "utf16", wanted, async (ref, data) => {
        if (ref.start !== at) fail("artifact_range");
        const value = decode(data);
        if (typeof value !== "string" || value.length !== ref.count)
          fail("artifact_string");
        const piece = value.slice(0, wanted - at);
        parts.push(piece);
        at += piece.length;
        if (wanted === desc.chars) {
          let safe = pending + piece;
          pending = "";
          const last = safe.charCodeAt(safe.length - 1);
          if (last >= 0xd800 && last <= 0xdbff) {
            pending = safe.slice(-1);
            safe = safe.slice(0, -1);
          }
          digest.update(JSON.stringify(safe).slice(1, -1));
        }
      });
      if (at !== wanted) fail("artifact_range");
      if (wanted === desc.chars) {
        if (pending) digest.update(JSON.stringify(pending).slice(1, -1));
        digest.update('"');
        if (digest.digest("hex") !== desc.jsonSha256) fail("artifact_digest");
      }
      return parts.join("");
    }
    function rowIdentity(row: Record<string, unknown>) {
      if (
        row.source !== identity.source ||
        (query.table === "session_relationships"
          ? row.parent_session_id
          : row.session_id) !== identity.sessionId ||
        (row.org_id !== undefined && row.org_id !== identity.orgId) ||
        (row.workspace_id !== undefined &&
          row.workspace_id !== identity.workspaceId)
      )
        fail("artifact_row_identity");
    }
    async function hydrate(entry: ArtifactEntry) {
      if (
        !obj(entry) ||
        entry.table !== query.table ||
        Boolean(entry.row) === Boolean(entry.rowJson)
      )
        fail("artifact_entry");
      const row = entry.rowJson ? await rowJson(entry.rowJson) : entry.row;
      if (!obj(row)) fail("artifact_row");
      rowIdentity(row);
      // Never mutate a cached page: a cursor/page boundary can revisit the same row.
      const result = { ...row };
      if (entry.content) {
        if (
          storageView !== "replay" ||
          !Object.hasOwn(result, "content") ||
          result.content !== null
        )
          fail("artifact_string");
        result.content = await content(entry.content);
      } else if (
        view === "replay" &&
        query.maxContent !== undefined &&
        typeof result.content === "string" &&
        result.content.length > query.maxContent
      )
        result.content = result.content.slice(0, query.maxContent + 1);
      return result;
    }
    if (selected.root) {
      range(selected.root, "rows");
      if (selected.root.start !== 0 || selected.root.count !== total)
        fail("artifact_range");
    }
    let firstOrdinal = 0,
      endOrdinal = sourceTotal;
    const since =
        query.since === undefined
          ? undefined
          : canonicalTimestamp(query.since)!,
      until =
        query.until === undefined
          ? undefined
          : canonicalTimestamp(query.until)!;
    const timeCompare = (a: string, b: string) =>
      a.length - b.length || (a < b ? -1 : a > b ? 1 : 0);
    if (since !== undefined || until !== undefined) {
      const bounds = index.tables.convergence_events?.bounds;
      if (
        !bounds ||
        bounds.count !== sourceTotal ||
        (sourceTotal ? !bounds.root : bounds.root !== null)
      )
        fail("artifact_bounds");
      if (bounds.root) {
        range(bounds.root, "rows");
        if (bounds.root.start !== 0 || bounds.root.count !== sourceTotal)
          fail("artifact_bounds");
      }
      async function seek(target: string, inclusive: boolean) {
        let low = 0,
          high = sourceTotal;
        while (low < high) {
          const middle = Math.floor((low + high) / 2),
            p = await pageFor(bounds!.root!, middle, 0, "bounds"),
            entry = p.rows[middle - p.start]!;
          if (
            !obj(entry) ||
            entry.table !== query.table ||
            !obj(entry.row) ||
            entry.rowJson ||
            entry.content
          )
            fail("artifact_bounds");
          rowIdentity(entry.row);
          const ts = entry.row.ts;
          if (typeof ts !== "string" || canonicalTimestamp(ts) !== ts)
            fail("artifact_bounds");
          const comparison = timeCompare(ts, target);
          if (comparison < 0 || (inclusive && comparison === 0))
            low = middle + 1;
          else high = middle;
        }
        return low;
      }
      if (since !== undefined) firstOrdinal = await seek(since, false);
      if (until !== undefined) endOrdinal = await seek(until, true);
      endOrdinal = Math.max(firstOrdinal, endOrdinal);
      total = endOrdinal - firstOrdinal;
      if (offset > total) fail("cursor", 400);
    }
    const rows: Array<{ table: string; row: Record<string, unknown> }> = [];
    let replyBytes = 0;
    while (offset < total && rows.length < query.limit) {
      const ordinal =
        order === "asc" ? firstOrdinal + offset : endOrdinal - 1 - offset;
      const p = await pageFor(selected.root!, ordinal);
      const entry = p.rows[ordinal - p.start]!;
      if (
        rows.length &&
        entry.content &&
        Math.min(
          entry.content.chars,
          query.maxContent === undefined
            ? entry.content.chars
            : query.maxContent + 1,
        ) >
          MAX_REPLY_BYTES - replyBytes - 4096
      )
        break;
      // A rowJson blob is the exact UTF-8 bytes of JSON.stringify(row), so the
      // encoded reply item is strictly larger. Refuse (first row) or stop
      // (later rows) on the declared size before allocating or fetching any
      // segment; the post-hydrate size checks below stay authoritative.
      // Views with a MAX_ROW_BYTES first-row allowance (analysis snapshots are
      // bounded by their own ANALYSIS_MAX_SNAPSHOT_BYTES budget) are exempt.
      if (
        !["replay", "legacy", "analysis"].includes(view) &&
        obj(entry) &&
        obj(entry.rowJson) &&
        count(entry.rowJson.bytes) &&
        entry.rowJson.bytes > MAX_REPLY_BYTES - replyBytes - 4096
      ) {
        if (rows.length) break;
        fail("row_bound");
      }
      const row = await hydrate(entry),
        item = { table: query.table, row },
        size = encoder.encode(JSON.stringify(item)).byteLength;
      if (
        (since !== undefined || until !== undefined) &&
        (typeof row.ts !== "string" ||
          (since !== undefined && timeCompare(row.ts, since) < 0) ||
          (until !== undefined && timeCompare(row.ts, until) > 0))
      )
        fail("artifact_bounds");
      if (rows.length && replyBytes + size > MAX_REPLY_BYTES - 4096) break;
      if (
        !rows.length &&
        size >
          (["replay", "legacy", "analysis"].includes(view)
            ? MAX_ROW_BYTES
            : MAX_REPLY_BYTES - 4096)
      )
        fail("row_bound");
      if (query.table === "convergence_events" && rows.length) {
        const comparison = compareArtifactEventRows(
          rows[rows.length - 1]!.row,
          row,
        );
        if (order === "asc" ? comparison > 0 : comparison < 0)
          fail("artifact_order");
      }
      rows.push(item);
      replyBytes += size;
      offset++;
    }
    const fresh = publication(
      await deps.publication(identity, signal),
      identity,
    );
    if (
      "exists" in fresh ||
      fresh.generation !== ack.generation ||
      fresh.index.key !== ack.index.key ||
      fresh.index.sha256 !== ack.index.sha256 ||
      fresh.index.bytes !== ack.index.bytes ||
      fresh.prefix !== ack.prefix ||
      !sameSource(fresh.source, ack.source)
    )
      fail("source_generation_changed");
    const nextCursor =
      offset < total
        ? "artifact-v1:" +
          cursorEncode({
            version: 1,
            identity,
            generation: ack.generation,
            indexSha256: ack.index.sha256,
            table: query.table,
            view,
            order,
            maxContent: query.maxContent ?? null,
            offset,
            since: query.since ?? null,
            until: query.until ?? null,
          } satisfies Cursor)
        : null;
    return {
      generation: ack.generation,
      exists: true,
      legacyCoverage: index.legacyCoverage,
      rows,
      nextCursor,
    };
  } finally {
    stop.abort();
  }
}
