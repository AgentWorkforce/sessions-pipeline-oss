/**
 * Portable transcript search index contract (SearchIndexAdapter).
 *
 * The index is a DERIVED, rebuildable projection of canonical session content
 * (object storage + HoopDb pointers). Every adapter stores one row per event,
 * keyed by tenant + event identity + ts, and answers the read API's query
 * shape: tenant-scoped keyword/substring search with filters and an exact
 * keyset cursor. Portable matching is defined here and by `matchesQuery`,
 * independently of a backend's locale-specific case matching:
 *
 *   q matches case-insensitively as a literal substring of the first
 *   CONTENT_PREFIX_CHARS characters of `content`, or of `taskTitle`.
 *
 * Page order is (tsMs, sessionId, source, machineId, kind, eventId), ascending
 * or descending, compared as code-point (byte) order. A cursor is the last
 * position served; the next page starts strictly after it.
 *
 * Adapters are trusted code. Tenant ids must come from verified auth: this
 * API filters by them, it does not authenticate them. Errors never carry
 * content, credentials or backend error bodies.
 */

/** Legacy search semantics: q matches the first 4096 characters of content. */
export const CONTENT_PREFIX_CHARS = 4096;
export const MAX_QUERY_CHARS = 500;
export const MAX_PAGE_LIMIT = 1000;
export const MAX_UPSERT_BATCH = 5000;

export interface SearchTenant {
  readonly orgId: string;
  readonly workspaceId: string;
}

/** Event identity within a tenant, plus ts (part of the stored row key). */
export interface SearchPosition {
  readonly tsMs: number;
  readonly sessionId: string;
  readonly source: string;
  readonly machineId: string;
  readonly kind: string;
  readonly eventId: string;
}

export interface SearchEventDocument extends SearchTenant, SearchPosition {
  readonly type: string;
  readonly userId: string;
  readonly actorName: string;
  readonly actorRole: string;
  readonly subagentId: string;
  readonly projectId: string;
  readonly taskTitle: string;
  readonly taskRef: string;
  readonly taskRefId: string;
  readonly tags: readonly string[];
  readonly filesTouched: readonly string[];
  readonly toolName: string;
  readonly toolStatus: string;
  readonly deliveryRecordId: string;
  readonly content: string;
  /** Change fingerprint computed by the projector; compared by reconcile. */
  readonly fingerprint: string;
  /**
   * Monotonic read version (epoch ms of the canonical read). An upsert never
   * replaces a stored row whose version is greater (ClickHouse
   * ReplacingMergeTree semantics), so a slow batch cannot undo a newer one.
   */
  readonly version: number;
}

/**
 * What a query returns: the read API's row shape. `taskRefId` (an index-only
 * filter column) and `deliveryRecordId` are stored but not returned.
 */
export type SearchHit = Omit<SearchEventDocument, "fingerprint" | "version" | "taskRefId" | "deliveryRecordId">;

/** A stored row's identity and fingerprint, as a reconcile scan returns it. */
export interface SearchFingerprint extends SearchPosition {
  readonly workspaceId: string;
  readonly fingerprint: string;
}

/**
 * The rows of one logical session scope: one org + workspace + source
 * (harness), and one or more session ids (a session and its canonical alias).
 */
export interface SearchScope extends SearchTenant {
  readonly source: string;
  readonly sessionIds: readonly string[];
}

export type SearchFilter = "session" | "source" | "kind" | "project" | "since" | "until";

export interface SearchQuery {
  readonly tenant: SearchTenant;
  readonly sessionId?: string;
  readonly source?: string;
  readonly kind?: string;
  readonly projectId?: string;
  /** Literal substring; `%`, `_`, `*`, `?` and `\` carry no wildcard meaning. */
  readonly q?: string;
  /** Inclusive lower bound, epoch ms. */
  readonly sinceMs?: number;
  /** Inclusive upper bound, epoch ms. */
  readonly untilMs?: number;
  readonly order: "asc" | "desc";
  readonly cursor?: SearchPosition;
  readonly limit: number;
  /** Return at most this many code points of content per hit. */
  readonly maxContentChars?: number;
}

export interface SearchPage {
  readonly hits: readonly SearchHit[];
  /** Present when another page may exist: pass it back as `cursor`. */
  readonly next?: SearchPosition;
}

export interface SearchIndexCapabilities {
  readonly backend: string;
  readonly filters: readonly SearchFilter[];
  /** q lengths (code points) the backend's text index prunes; shorter q may scan. */
  readonly minIndexedQueryChars: number;
  /**
   * immediate: an acknowledged upsert/delete is visible to the next query.
   * refresh: visible after the backend refresh interval unless the adapter
   * was configured to wait for refresh.
   */
  readonly readAfterWrite: "immediate" | "refresh";
  /** Erasure is complete (no matching row readable) when eraseScope resolves. */
  readonly synchronousErasure: boolean;
  /** Version-guarded upserts (an older version never replaces a newer row). */
  readonly versionedUpserts: boolean;
}

export interface SearchIndexHealth {
  readonly ok: boolean;
  /** A bounded code, never backend text: ok, unreachable, schema_missing, ... */
  readonly code: string;
}

export interface SearchCallContext {
  readonly signal?: AbortSignal;
}

export interface SearchIndexAdapter {
  readonly id: string;
  readonly backend: string;
  capabilities(): SearchIndexCapabilities;
  /**
   * Create or verify the index's schema/mapping. An explicit operator or
   * migration step: no other call creates storage implicitly. Idempotent.
   */
  ensureSchema(context?: SearchCallContext): Promise<void>;
  /** Insert or replace rows (version-guarded). Batches up to MAX_UPSERT_BATCH. */
  upsert(docs: readonly SearchEventDocument[], context?: SearchCallContext): Promise<{ written: number }>;
  /** Remove exact rows (tenant + position) a reconcile found stale. */
  deleteEvents(
    tenant: SearchTenant,
    positions: readonly SearchPosition[],
    context?: SearchCallContext,
  ): Promise<{ deleted: number }>;
  /** Erasure: remove every row of the scope, at any ts. */
  eraseScope(scope: SearchScope, context?: SearchCallContext): Promise<void>;
  /**
   * Identities + fingerprints of the scope's rows with tsMs >= fromMs, in page
   * order. `truncated` is true when more than `limit` rows exist; a caller
   * must then not delete on the basis of this scan.
   */
  scanFingerprints(
    scope: SearchScope,
    options: { readonly fromMs: number; readonly limit: number },
    context?: SearchCallContext,
  ): Promise<{ rows: readonly SearchFingerprint[]; truncated: boolean }>;
  query(query: SearchQuery, context?: SearchCallContext): Promise<SearchPage>;
  health(context?: SearchCallContext): Promise<SearchIndexHealth>;
}

export type SearchIndexErrorCode =
  | "invalid_argument"
  | "unsupported"
  | "unavailable"
  | "schema_missing"
  | "aborted";

/** Messages never contain content, credentials or backend error text. */
export class SearchIndexError extends Error {
  constructor(
    readonly code: SearchIndexErrorCode,
    readonly retryable = false,
    /** A bounded detail code (e.g. an HTTP status class), never backend text. */
    readonly detail?: string,
  ) {
    super(`search index: ${code}${detail ? ` (${detail})` : ""}`);
    this.name = "SearchIndexError";
  }
}

const ID_MAX = 512;

function nonEmpty(value: unknown, max = ID_MAX): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= max;
}

export function assertTenant(tenant: SearchTenant): void {
  if (!tenant || !nonEmpty(tenant.orgId) || !nonEmpty(tenant.workspaceId)) {
    throw new SearchIndexError("invalid_argument");
  }
}

export function assertPosition(position: SearchPosition): void {
  if (
    !position ||
    !Number.isSafeInteger(position.tsMs) ||
    !nonEmpty(position.sessionId) ||
    !nonEmpty(position.source) ||
    typeof position.machineId !== "string" ||
    position.machineId.length > ID_MAX ||
    !nonEmpty(position.kind) ||
    !nonEmpty(position.eventId)
  ) {
    throw new SearchIndexError("invalid_argument");
  }
}

export function assertDocument(doc: SearchEventDocument): void {
  assertTenant(doc);
  assertPosition(doc);
  if (
    typeof doc.content !== "string" ||
    typeof doc.taskTitle !== "string" ||
    typeof doc.fingerprint !== "string" ||
    !Number.isSafeInteger(doc.version) ||
    doc.version < 0 ||
    !Array.isArray(doc.tags) ||
    !Array.isArray(doc.filesTouched)
  ) {
    throw new SearchIndexError("invalid_argument");
  }
}

export function assertScope(scope: SearchScope): void {
  assertTenant(scope);
  if (
    !nonEmpty(scope.source) ||
    !Array.isArray(scope.sessionIds) ||
    scope.sessionIds.length === 0 ||
    scope.sessionIds.length > 16 ||
    !scope.sessionIds.every((id) => nonEmpty(id))
  ) {
    throw new SearchIndexError("invalid_argument");
  }
}

export function assertQuery(query: SearchQuery): void {
  assertTenant(query.tenant);
  if (!Number.isInteger(query.limit) || query.limit < 1 || query.limit > MAX_PAGE_LIMIT) {
    throw new SearchIndexError("invalid_argument");
  }
  if (query.order !== "asc" && query.order !== "desc") throw new SearchIndexError("invalid_argument");
  if (query.q !== undefined && (typeof query.q !== "string" || query.q.length === 0 || [...query.q].length > MAX_QUERY_CHARS)) {
    throw new SearchIndexError("invalid_argument");
  }
  for (const id of [query.sessionId, query.source, query.kind, query.projectId]) {
    if (id !== undefined && !nonEmpty(id)) throw new SearchIndexError("invalid_argument");
  }
  for (const bound of [query.sinceMs, query.untilMs]) {
    if (bound !== undefined && !Number.isSafeInteger(bound)) throw new SearchIndexError("invalid_argument");
  }
  if (query.maxContentChars !== undefined && (!Number.isInteger(query.maxContentChars) || query.maxContentChars < 0)) {
    throw new SearchIndexError("invalid_argument");
  }
  if (query.cursor !== undefined) assertPosition(query.cursor);
}

/** Code-point comparison (equals UTF-8 byte order, unlike JS `<` on UTF-16). */
export function compareCodePoints(a: string, b: string): number {
  const ai = a[Symbol.iterator]();
  const bi = b[Symbol.iterator]();
  for (;;) {
    const x = ai.next();
    const y = bi.next();
    if (x.done || y.done) return x.done === y.done ? 0 : x.done ? -1 : 1;
    const d = x.value.codePointAt(0)! - y.value.codePointAt(0)!;
    if (d !== 0) return d < 0 ? -1 : 1;
  }
}

/** Ascending page-order comparison of two positions. */
export function comparePositions(a: SearchPosition, b: SearchPosition): number {
  if (a.tsMs !== b.tsMs) return a.tsMs < b.tsMs ? -1 : 1;
  for (const k of ["sessionId", "source", "machineId", "kind", "eventId"] as const) {
    const c = compareCodePoints(a[k], b[k]);
    if (c !== 0) return c;
  }
  return 0;
}

export function positionOf(row: SearchPosition): SearchPosition {
  return {
    tsMs: row.tsMs,
    sessionId: row.sessionId,
    source: row.source,
    machineId: row.machineId,
    kind: row.kind,
    eventId: row.eventId,
  };
}

/** The q semantics every adapter must reproduce exactly (reference oracle). */
export function matchesQuery(doc: Pick<SearchEventDocument, "content" | "taskTitle">, q: string): boolean {
  const needle = q.toLowerCase();
  const prefix = [...doc.content].slice(0, CONTENT_PREFIX_CHARS).join("");
  return prefix.toLowerCase().includes(needle) || doc.taskTitle.toLowerCase().includes(needle);
}

export function truncateContent(content: string, maxChars: number | undefined): string {
  if (maxChars === undefined) return content;
  const cps = [...content];
  return cps.length <= maxChars ? content : cps.slice(0, maxChars).join("");
}
