/**
 * The session read API: an API-compatible subset of relayhistory's /v1 read
 * contract (packages/relayhistory/src/routes/recall.ts), served from ClickHouse
 * `transcript_events` with Supabase for erasure and the history floor.
 *
 * Phase A routes (runbooks/sessions-prod-session-read-api.md):
 *   GET /v1/sessions/:id/events     session replay page
 *   GET /v1/events?session=…        in-session reads (q, since, until, kind)
 *   GET /v1/events?q=…              workspace transcript search
 *
 * Anything this backend cannot answer exactly is `501 unsupported_by_backend`
 * with a `reason`, never a best-effort answer: the caller (Cloud) falls back
 * to relayhistory for it during the transition. A 501 is only ever the answer
 * to a FIRST page (no cursor), where the caller can still fall back cleanly.
 * Once this backend has issued a cursor, the caller cannot restart elsewhere
 * without duplicating pages, so:
 *
 *   - A partial session (`needs_full = 1`) is checked when pagination starts.
 *     A cursor continues the read that check admitted.
 *   - A search page that reaches the history floor is a terminal 200 with a
 *     `continuation` (CONTINUATION below): the rows from the floor up, and
 *     where the existing backend must continue strictly below it.
 *
 * Directory routes (P3b, docs in src/session-read-api/directory.ts):
 *   GET /v1/sessions                session list without `q`, from the Supabase directory
 *   GET /v1/sessions/activity       sessions per UTC day without `q` (span semantics)
 * Both answer 501 `directory_workspace_not_enabled` unless the token's workspace is on
 * READ_API_DIRECTORY_WORKSPACES (the canary allowlist; unset = none), and 501
 * `directory_not_migrated` until migration 20261105029000 is applied. Never Neon.
 *   GET /v1/sessions/:id/timing     recorded timing from the session's canonical S3 objects
 *                                   (session-timing-source.ts, timing.ts), same allowlist.
 *   GET /v1/sessions/:id/turns      the relay session journal from canonical S3 (legacy
 *   GET /v1/sessions/:id/metadata   export + live turns: session-turns-source.ts), same allowlist.
 *   GET /v1/sessions/output and /v1/digest/daily are registered and answer 501 with a
 *   reason naming the store they still need: their relayhistory semantics read tables
 *   that neither ClickHouse, S3 nor the directory holds yet (per-message and commit
 *   facts, session_outcomes, the digest narrative cache), and this backend never
 *   answers approximately.
 *
 * Auth: a RelayAuth assertion JWT minted for THIS service's origin with scope
 * `rth:read` (verifyAgentRelayToken: RS256, JWKS, issuer, exact audience).
 * Org and workspace come only from the verified claims.
 */
import { Hono, type Context } from "hono";
import { ClickHouseError, type ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { CursorError, decodeCursor } from "./cursor.ts";
import { TenantMismatchError, partialSessions, partialSessionsInWorkspace, readEventPage, type Liveness } from "./page.ts";
import type { EventQuery } from "./queries.ts";
import { CanonicalReadError, type CanonicalIdentity } from "./canonical.ts";
import { timingFromCanonical, type CanonicalRows } from "./session-timing-source.ts";
import { TimingInputError } from "./timing.ts";
import { MAX_TURN_HARNESSES, TurnsInputError, metadataResponse, turnRows, turnsResponse, type HarnessState } from "./session-turns-source.ts";
import {
  DIRECTORY_LIST_MAX_LIMIT,
  DirectoryError,
  activityDays,
  decodeListCursor,
  directoryPage,
  type Directory,
  type DirectoryListArgs,
} from "./directory.ts";

export interface VerifiedIdentity {
  userId: string;
  orgId: string;
  workspaceId: string;
  scopes: string[];
}

export type Verifier = (bearer: string) => Promise<VerifiedIdentity>;

export class AuthError extends Error {
  constructor(readonly status: 401 | 403, readonly code: string, message: string) {
    super(message);
    this.name = "AuthError";
  }
}

export interface ReadApiDeps {
  verify: Verifier;
  ch: ClickHouseClient;
  liveness: Liveness;
  /** Epoch ms from which ClickHouse holds complete history; 0 = complete. */
  completeSinceMs: number;
  log?: (fields: Record<string, unknown>) => void;
  timeoutMs?: number;
  /** The Supabase directory (list, activity); absent = those routes answer 501. */
  directory?: Directory;
  /** READ_API_DIRECTORY_WORKSPACES: workspaces the directory routes serve; "*" = all. Absent/empty = none. */
  directoryWorkspaces?: ReadonlySet<string> | "*";
  /** Session timing from canonical S3 (session-timing-source.ts); absent = 501 `timing_not_configured`. */
  timing?: TimingDeps;
  /** Relay session turns and metadata from canonical S3 (session-turns-source.ts); absent = 501 `turns_not_configured`. */
  turns?: TurnsDeps;
}

export interface TurnsDeps {
  /** Harnesses holding a pointer for the session id, with live/total counts (tenant-bound, read role). */
  harnesses(scope: { orgId: string; workspaceId: string; sessionId: string }): Promise<HarnessState[]>;
  /** One harness scope's canonical rows; `signal` aborts its object reads at the deadline. */
  source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows>;
  /** Below Cloud's 4 s read timeout. Default 3000. */
  deadlineMs?: number;
}

export interface TimingDeps {
  /** Pointer rows of the session, live or tombstoned (tenant-bound, read role). */
  state(identity: CanonicalIdentity): Promise<{ pointers: number; live: number }>;
  /** The session's canonical projection; `signal` aborts its object reads at the deadline. */
  source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows>;
  /** READ_API_TIMING_ASSUME_NO_TURNS (session-timing-source.ts TimingOptions). */
  assumeNoTurns: boolean;
  /** Below Cloud's 4 s read timeout, so a slow session falls back cleanly as a 501. Default 3000. */
  deadlineMs?: number;
}

/** READ_API_DIRECTORY_WORKSPACES: unset or blank = none; "*" = every workspace; else comma-separated ids. */
export function parseDirectoryWorkspaces(raw: string | undefined): ReadonlySet<string> | "*" {
  const value = raw?.trim() ?? "";
  if (value === "*") return "*";
  return new Set(value.split(",").map((v) => v.trim()).filter(Boolean));
}

/** relayhistory routes/recall.ts limits for the session list and activity. */
export const SESSIONS_DEFAULT_LIMIT = 50;
export const SESSIONS_MAX_LIMIT = 500;
export const MAX_SESSION_ACTIVITY_DAYS = 92;
const MAX_LIST_SOURCES = 16;
const MAX_LIST_USERS = 20;
const MAX_LIST_ID_LENGTH = 255;
const MAX_AGENT_LENGTH = 200;
const HARNESS_NAME = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
/** Session-list parameters the directory cannot answer: event or text questions. */
const LIST_UNSUPPORTED_PARAMS = ["q", "search", "project", "noProject", "kind", "taskRef", "noTaskRef", "tag", "since", "until"];

/** relayhistory lib/recall.ts limits. */
export const EVENTS_DEFAULT_LIMIT = 200;
export const EVENTS_MAX_LIMIT = 1000;
/** Search pages above this are not served here (Cloud asks for at most 50). */
export const SEARCH_MAX_LIMIT = 100;
export const MAX_Q_CHARS = 500;
const MAX_ID_CHARS = 512;

/**
 * The terminal-page signal for a search cursor page that reached the history
 * floor. `events` holds every match from the floor up (ClickHouse is complete
 * there); the caller continues the same read at relayhistory with `until` set
 * to `until` (inclusive, one microsecond below the floor, so relayhistory's
 * microsecond timestamps neither repeat a row served here nor skip one) and no
 * cursor. Ordering is newest first, so every later relayhistory page is older
 * still and the pagination never revisits this backend. The response carries
 * `X-Sessions-Read-Continuation: history_floor`; a caller that sees the header
 * must finish the page, and must not pass it on as a short final page.
 */
export interface Continuation {
  backend: "relayhistory";
  reason: "history_floor";
  until: string;
}

/** One microsecond below `floorMs`, as an ISO timestamp relayhistory accepts (six fractional digits). */
export function continuationUntil(floorMs: number): string {
  return new Date(floorMs - 1).toISOString().replace(/Z$/, "999Z");
}

class HttpError extends Error {
  constructor(readonly status: number, readonly code: string, message: string, readonly reason?: string) {
    super(message);
  }
}
const unsupported = (reason: string) => new HttpError(501, "unsupported_by_backend", `not served by this backend: ${reason}`, reason);
const bad = (message: string) => new HttpError(400, "bad_request", message);

/** relayhistory clampLimit: truncate, clamp to [1, max]; junk gives the default. */
export function clampLimit(raw: string | undefined, fallback: number, max: number): number {
  if (raw === undefined) return fallback;
  const n = Math.trunc(Number(raw));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(1, n));
}

function positiveInt(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

function parseTime(raw: string | undefined, name: string): number | undefined {
  if (raw === undefined || raw === "") return undefined;
  const t = Date.parse(raw);
  if (Number.isNaN(t)) throw bad(`${name} must be a date`);
  return t;
}

/** Exactly one value or none; a repeated parameter is a 400 like relayhistory's. */
function single(c: Context, name: string): string | undefined {
  const all = c.req.queries(name);
  if (!all || all.length === 0) return undefined;
  if (all.length > 1) throw bad(`${name} must be given at most once`);
  return all[0];
}

/** Parameters this phase never serves: their presence routes the call back to relayhistory. */
const UNSUPPORTED_PARAMS = ["taskRef", "noTaskRef", "tag", "agent", "noProject", "projectContains", "includeMessageId", "search", "kinds"];

function rejectUnsupported(c: Context): void {
  for (const p of UNSUPPORTED_PARAMS) if (c.req.queries(p)?.length) throw unsupported(`param_${p}`);
}

function attestWorkspace(c: Context, identity: VerifiedIdentity): void {
  const all = c.req.queries("workspace");
  // relayhistory serves an org-wide read without `workspace`; this backend is
  // workspace-scoped only, so that read stays there.
  if (!all || all.length === 0) throw unsupported("org_scope");
  if (all.length > 1 || !all[0]) throw bad("workspace must be given exactly once");
  if (all[0] !== identity.workspaceId) throw new HttpError(403, "forbidden", "workspace does not match the token");
}

export function createReadApi(deps: ReadApiDeps) {
  const app = new Hono<{ Variables: { identity: VerifiedIdentity; correlationId: string } }>();
  const log = deps.log ?? (() => {});

  app.use("*", async (c, next) => {
    const incoming = c.req.header("x-correlation-id");
    const correlationId = incoming && /^[A-Za-z0-9._:-]{1,128}$/.test(incoming) ? incoming : crypto.randomUUID();
    c.set("correlationId", correlationId);
    const started = Date.now();
    await next();
    c.header("Cache-Control", "private, no-store");
    c.header("X-Sessions-Read-Backend", "96bulls");
    c.header("X-Correlation-Id", correlationId);
    log({ route: c.req.routePath, status: c.res.status, duration_ms: Date.now() - started, correlation_id: correlationId });
  });

  app.get("/health", (c) => c.json({ ok: true }));

  app.use("/v1/*", async (c, next) => {
    const header = c.req.header("authorization") ?? "";
    const m = /^Bearer\s+(\S+)$/i.exec(header);
    if (!m) return c.json({ error: { code: "missing_authorization", message: "Bearer token required" }, correlationId: c.get("correlationId") }, 401);
    let identity: VerifiedIdentity;
    try {
      identity = await deps.verify(m[1]!);
    } catch (error) {
      const e = error instanceof AuthError ? error : new AuthError(401, "invalid_token", "token is not valid");
      return c.json({ error: { code: e.code, message: e.message }, correlationId: c.get("correlationId") }, e.status);
    }
    if (!identity.orgId || !identity.workspaceId)
      return c.json({ error: { code: "invalid_token", message: "token carries no workspace" }, correlationId: c.get("correlationId") }, 401);
    if (!identity.scopes.includes("rth:read"))
      return c.json({ error: { code: "forbidden", message: "missing required scope: rth:read" }, correlationId: c.get("correlationId") }, 403);
    c.set("identity", identity);
    await next();
    c.header("X-Relayhistory-Workspace-Id", identity.workspaceId);
  });

  const fail = (c: Context, error: unknown) => {
    const correlationId = c.get("correlationId") as string;
    const identity = c.get("identity") as VerifiedIdentity | undefined;
    if (error instanceof HttpError) {
      if (error.status === 501) log({ route: c.req.routePath, reason: error.reason, status: 501, correlation_id: correlationId });
      return c.json({ error: { code: error.code, message: error.message, ...(error.reason ? { reason: error.reason } : {}) }, correlationId }, error.status as never);
    }
    if (error instanceof CursorError) {
      return error.code === "foreign_cursor"
        ? c.json({ error: { code: "unsupported_by_backend", message: error.message, reason: "foreign_cursor" }, correlationId }, 501)
        : c.json({ error: { code: "bad_request", message: error.message }, correlationId }, 400);
    }
    if (error instanceof TenantMismatchError) {
      log({ route: c.req.routePath, reason: "cross_tenant_rows", status: 500, org_id: identity?.orgId, workspace_id: identity?.workspaceId, correlation_id: correlationId });
      return c.json({ error: { code: "internal", message: "read refused" }, correlationId }, 500);
    }
    if (error instanceof ClickHouseError) {
      // 159 TIMEOUT_EXCEEDED: the same answer relayhistory gives a cancelled search.
      const timeout = error.code === "159";
      log({ route: c.req.routePath, reason: timeout ? "clickhouse_timeout" : "clickhouse_error", error_code: error.code ?? String(error.status), status: 503, correlation_id: correlationId });
      return c.json({ error: { code: timeout ? "search_timeout" : "backend_unavailable", message: timeout ? "search took too long" : "read backend unavailable" }, correlationId }, 503);
    }
    log({ route: c.req.routePath, reason: "unavailable", error_code: (error as { name?: string })?.name ?? "error", status: 503, correlation_id: correlationId });
    return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId }, 503);
  };

  const pageDeps = () => ({ ch: deps.ch, liveness: deps.liveness, timeoutMs: deps.timeoutMs });

  /**
   * Session reads: erased → empty page; history older than the floor → 501;
   * a partial session (`needs_full = 1`) → 501 `session_partial` when
   * pagination starts (no cursor), so the caller reads it from relayhistory.
   */
  async function sessionGate(identity: VerifiedIdentity, sessionId: string, source: string | undefined, continuing: boolean): Promise<"empty" | "ok"> {
    const start = await deps.liveness.sessionStart(identity.orgId, identity.workspaceId, sessionId, source);
    if (start.pointers > 0 && start.live === 0) return "empty";
    if (deps.completeSinceMs > 0) {
      if (start.createdAtMs === null) throw unsupported("history_floor_unknown_session");
      if (start.createdAtMs < deps.completeSinceMs) throw unsupported("history_floor");
    }
    if (!continuing && (await partialSessions(pageDeps(), identity.orgId, sessionId)).length > 0) throw unsupported("session_partial");
    return "ok";
  }

  app.get("/v1/sessions/:sessionId/events", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      rejectUnsupported(c);
      for (const p of ["q", "since", "until", "kind", "session", "project"]) if (c.req.queries(p)?.length) throw unsupported(`param_${p}`);
      const sessionId = c.req.param("sessionId");
      if (!sessionId || sessionId.length > MAX_ID_CHARS) throw bad("sessionId is not valid");
      const source = single(c, "source") || undefined;
      const order = single(c, "order") === "desc" ? "desc" : "asc";
      const limit = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
      const maxContent = positiveInt(single(c, "maxContent"));
      const cursor = decodeCursor(single(c, "cursor"));
      if ((await sessionGate(identity, sessionId, source, cursor !== undefined)) === "empty")
        return c.json({ sessionId, events: [], nextCursor: null, correlationId: c.get("correlationId") });
      const page = await readEventPage(
        { ch: deps.ch, liveness: deps.liveness, timeoutMs: deps.timeoutMs },
        { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, order, limit, maxContent, cursor },
      );
      return c.json({ sessionId, events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return fail(c, error);
    }
  });

  app.get("/v1/events", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      rejectUnsupported(c);
      const sessionId = single(c, "session") || undefined;
      const qRaw = single(c, "q");
      const q = qRaw === undefined || qRaw === "" ? undefined : qRaw;
      if (q !== undefined && q.length > MAX_Q_CHARS) throw bad(`q must be at most ${MAX_Q_CHARS} characters`);
      if (sessionId === undefined && q === undefined) throw unsupported("unfiltered_events");
      if (sessionId !== undefined && sessionId.length > MAX_ID_CHARS) throw bad("session is not valid");
      const source = single(c, "source") || undefined;
      const project = single(c, "project") || undefined;
      const kind = single(c, "kind") || undefined;
      const order: "asc" | "desc" = single(c, "order") === "asc" ? "asc" : "desc";
      const sinceMs = parseTime(single(c, "since"), "since");
      const untilMs = parseTime(single(c, "until"), "until");
      const maxContent = positiveInt(single(c, "maxContent"));
      const cursor = decodeCursor(single(c, "cursor"));
      const base: EventQuery = { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, kind, projectId: project, q, sinceMs, untilMs, order, limit: 0, maxContent, cursor };

      if (sessionId !== undefined) {
        base.limit = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
        if ((await sessionGate(identity, sessionId, source, cursor !== undefined)) === "empty")
          return c.json({ events: [], nextCursor: null, correlationId: c.get("correlationId") });
        const page = await readEventPage({ ch: deps.ch, liveness: deps.liveness, timeoutMs: deps.timeoutMs }, base);
        return c.json({ events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
      }

      // Workspace search: newest first only.
      if (order === "asc") throw unsupported("search_ascending");
      const requested = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
      if (requested > SEARCH_MAX_LIMIT) throw unsupported("search_limit");
      base.limit = requested;
      // History floor: search only where ClickHouse is complete; a page that
      // reaches the floor without filling would be missing older rows.
      const belowFloor = deps.completeSinceMs > 0 && (sinceMs === undefined || sinceMs < deps.completeSinceMs);
      if (belowFloor) base.sinceMs = deps.completeSinceMs;
      if (untilMs !== undefined && base.sinceMs !== undefined && untilMs < base.sinceMs) throw unsupported("history_floor");
      // A partial session of this workspace may hold matches this backend
      // lacks, so the search starts at relayhistory. Partial sessions of other
      // workspaces of the org cannot hold this workspace's rows and do not count
      // (page.ts partialSessionsInWorkspace says how one is attributed).
      if (cursor === undefined) {
        const partials = await partialSessionsInWorkspace(pageDeps(), identity.orgId, identity.workspaceId);
        if (partials.overflow) throw unsupported("session_partial_unbounded");
        if (partials.sessions.length > 0) throw unsupported("session_partial");
      }
      const page = await readEventPage(pageDeps(), base);
      if (belowFloor && page.nextCursor === null) {
        // First page: nothing was issued yet, so the whole read falls back.
        if (cursor === undefined) throw unsupported("history_floor");
        // Cursor page: end this backend's part cleanly and hand the rest over.
        const continuation: Continuation = { backend: "relayhistory", reason: "history_floor", until: continuationUntil(deps.completeSinceMs) };
        log({ route: c.req.routePath, reason: "history_floor_continuation", status: 200, correlation_id: c.get("correlationId") });
        // Flags the page so the caller finishes it without parsing every page body.
        c.header("X-Sessions-Read-Continuation", "history_floor");
        return c.json({ events: page.events, nextCursor: null, continuation, correlationId: c.get("correlationId") });
      }
      return c.json({ events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return fail(c, error);
    }
  });

  /** The P3 canary allowlist (READ_API_DIRECTORY_WORKSPACES), shared by the directory and timing routes. */
  function requireCanary(identity: VerifiedIdentity, route: "directory" | "timing" | "turns"): void {
    const allowed = deps.directoryWorkspaces;
    if (!allowed || (allowed !== "*" && !allowed.has(identity.workspaceId))) throw unsupported(`${route}_workspace_not_enabled`);
  }

  /** The canary allowlist, then the directory itself. */
  function directoryFor(identity: VerifiedIdentity): Directory {
    requireCanary(identity, "directory");
    if (!deps.directory) throw unsupported("directory_not_configured");
    return deps.directory;
  }

  const directoryFail = (c: Context, error: unknown) => {
    if (error instanceof DirectoryError) {
      if (error.reason === "agent_filter_not_ready") return fail(c, unsupported("agent_filter_not_ready"));
      if (error.reason === "tenant_mismatch") return fail(c, new TenantMismatchError());
      // A row outside the cloud-client contract (e.g. a title over 90 characters): a first
      // page falls back whole, as relayhistory's directory backend does; a cursor page
      // cannot restart elsewhere, so it is a 503.
      if (error.reason === "parse" && !single(c, "cursor")) return fail(c, unsupported("directory_row_out_of_contract"));
      const correlationId = c.get("correlationId") as string;
      log({ route: c.req.routePath, reason: `directory_${error.reason}`, status: 503, correlation_id: correlationId });
      return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId }, 503);
    }
    return fail(c, error);
  };

  /** Repeated values, deduplicated and empty values dropped (relayhistory readSessionListFilters). */
  const repeated = (c: Context, name: string) => [...new Set((c.req.queries(name) ?? []).filter(Boolean))];

  app.get("/v1/sessions", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      // A workspace off the canary allowlist always falls back, whatever it asked.
      const directory = directoryFor(identity);
      for (const p of LIST_UNSUPPORTED_PARAMS) if (c.req.queries(p)?.length) throw unsupported(`param_${p}`);
      const limit = clampLimit(single(c, "limit"), SESSIONS_DEFAULT_LIMIT, SESSIONS_MAX_LIMIT);
      const args: DirectoryListArgs = { limit };
      const sources = repeated(c, "source");
      const notSources = repeated(c, "notSource");
      for (const [name, values] of [["source", sources], ["notSource", notSources]] as const) {
        if (values.length > MAX_LIST_SOURCES) throw bad(`${name} accepts at most ${MAX_LIST_SOURCES} values`);
        if (values.some((v) => v.length > MAX_LIST_ID_LENGTH)) throw bad(`${name} must be ${MAX_LIST_ID_LENGTH} characters or fewer`);
      }
      const users = repeated(c, "user");
      if (users.length > MAX_LIST_USERS) throw bad(`user accepts at most ${MAX_LIST_USERS} values`);
      if (users.some((u) => u.length > MAX_LIST_ID_LENGTH)) throw bad(`user must be ${MAX_LIST_ID_LENGTH} characters or fewer`);
      const agent = single(c, "agent")?.trim();
      if (agent && agent.length > MAX_AGENT_LENGTH) throw bad(`agent must be ${MAX_AGENT_LENGTH} characters or fewer`);
      const projectContains = single(c, "projectContains") || undefined;
      if (projectContains && projectContains.trim().length > 200) throw bad("projectContains must be 200 characters or fewer");
      const bounds: Record<"lastTsAfter" | "lastTsBefore" | "firstTsAfter" | "firstTsBefore", Date | undefined> = {
        lastTsAfter: undefined, lastTsBefore: undefined, firstTsAfter: undefined, firstTsBefore: undefined,
      };
      for (const field of Object.keys(bounds) as Array<keyof typeof bounds>) {
        const t = parseTime(single(c, field), field);
        if (t !== undefined) bounds[field] = new Date(t);
      }
      if ((bounds.lastTsAfter && bounds.lastTsBefore && bounds.lastTsAfter > bounds.lastTsBefore) ||
          (bounds.firstTsAfter && bounds.firstTsBefore && bounds.firstTsAfter > bounds.firstTsBefore))
        throw bad("an *After bound must not be later than its *Before bound");
      // What the directory cannot answer exactly stays with the current backend.
      if (limit > DIRECTORY_LIST_MAX_LIMIT) throw unsupported("limit");
      if ([...sources, ...notSources].some((v) => !HARNESS_NAME.test(v))) throw unsupported("source_format");
      if (projectContains !== undefined && projectContains.trim() === "") throw unsupported("project_contains");
      const cursor = decodeListCursor(single(c, "cursor"));
      if (cursor === "foreign") throw new CursorError("foreign_cursor");
      if (cursor === "invalid") throw new CursorError("invalid_cursor");
      if (sources.length) args.sources = sources;
      if (notSources.length) args.notSources = notSources;
      if (users.length) args.userIds = users;
      if (agent) args.agent = agent;
      if (projectContains) args.projectContains = projectContains;
      for (const [k, v] of Object.entries(bounds)) if (v) args[k as keyof typeof bounds] = v;
      if (cursor) args.cursor = cursor;
      const rows = await directory.list(identity.orgId, identity.workspaceId, args);
      if (rows === null) throw unsupported("directory_not_migrated");
      const page = directoryPage(rows, identity, limit);
      return c.json({ sessions: page.sessions, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return directoryFail(c, error);
    }
  });

  app.get("/v1/sessions/activity", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      const directory = directoryFor(identity);
      const rawSince = single(c, "since");
      const rawUntil = single(c, "until");
      if (!rawSince || !rawUntil) throw bad("since and until are required");
      const sinceMs = parseTime(rawSince, "since")!;
      const untilMs = parseTime(rawUntil, "until")!;
      if (untilMs < sinceMs) throw bad("until must not be earlier than since");
      const since = new Date(sinceMs);
      const until = new Date(untilMs);
      const days = Math.round((Date.UTC(until.getUTCFullYear(), until.getUTCMonth(), until.getUTCDate()) - Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate())) / 86_400_000) + 1;
      if (days > MAX_SESSION_ACTIVITY_DAYS) throw bad(`since and until may span at most ${MAX_SESSION_ACTIVITY_DAYS} UTC days`);
      const q = single(c, "q");
      if (q && q.length > MAX_Q_CHARS) throw bad(`q must be ${MAX_Q_CHARS} characters or fewer`);
      if (q) throw unsupported("param_q");
      const rows = await directory.activity(identity.orgId, identity.workspaceId, since, until);
      if (rows === null) throw unsupported("directory_not_migrated");
      return c.json({ since: since.toISOString(), until: until.toISOString(), days: activityDays(rows, since, until), correlationId: c.get("correlationId") });
    } catch (error) {
      return directoryFail(c, error);
    }
  });

  /**
   * GET /v1/sessions/:id/timing (relayhistory routes/recall.ts): recorded timing over the
   * whole stored session, from its canonical S3 objects (session-timing-source.ts), never
   * Neon. Same canary allowlist as the directory routes. Every case this backend cannot
   * time exactly is a 501 naming why, and Cloud reads relayhistory instead:
   *   timing_no_pointers       no Supabase pointer row (Neon may still hold the session)
   *   timing_turns_unknown     no legacy turn evidence (READ_API_TIMING_ASSUME_NO_TURNS)
   *   timing_canonical_<r>     the canonical read refused (bounds, digests, generation, deadline)
   *   timing_<r>               an input the port refuses exactly where the SQL would
   * A session whose every pointer is tombstoned is erased: the empty timing, as relayhistory
   * answers once erasure has purged its rows.
   */
  app.get("/v1/sessions/:sessionId/timing", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      requireCanary(identity, "timing");
      const sources = c.req.queries("source") ?? [];
      const source = sources[0]?.trim();
      if (sources.length !== 1 || !source) throw bad("source must be supplied exactly once and be non-empty");
      const timing = deps.timing;
      if (!timing) throw unsupported("timing_not_configured");
      const sessionId = c.req.param("sessionId");
      if (!sessionId || sessionId.length > MAX_ID_CHARS) throw unsupported("timing_session_id");
      const scope: CanonicalIdentity = { orgId: identity.orgId, workspaceId: identity.workspaceId, source, sessionId };
      const state = await timing.state(scope);
      if (state.pointers === 0) throw unsupported("timing_no_pointers");
      const controller = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      let snapshot: CanonicalRows;
      if (state.live === 0) snapshot = { rows: [], legacyCoverage: { version: 1, tables: [] } };
      else {
        try {
          snapshot = await Promise.race([
            timing.source(scope, controller.signal),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => {
                // Settle the race first: aborting can reject the read synchronously.
                reject(new CanonicalReadError("deadline"));
                controller.abort();
              }, timing.deadlineMs ?? 3000);
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
      }
      const payload = timingFromCanonical(scope, snapshot, { assumeNoTurns: timing.assumeNoTurns });
      log({
        route: c.req.routePath,
        reason: state.live === 0 ? "timing_erased" : "timing_served",
        turns: snapshot.legacyCoverage ? "legacy" : "assumed_none",
        events: payload.coverage.eventCount,
        correlation_id: c.get("correlationId"),
      });
      return c.json({ ...payload, correlationId: c.get("correlationId") });
    } catch (error) {
      if (error instanceof CanonicalReadError) return fail(c, unsupported(`timing_canonical_${error.reason}`));
      if (error instanceof TimingInputError) return fail(c, unsupported(`timing_${error.reason}`));
      return fail(c, error);
    }
  });

  // Registered so their 501 names what they still need; see the header comment.
  const notMigrated = (reason: string) => async (c: Context) => {
    try {
      attestWorkspace(c, c.get("identity") as VerifiedIdentity);
      throw unsupported(reason);
    } catch (error) {
      return fail(c, error);
    }
  };
  app.get("/v1/sessions/output", notMigrated("output_not_migrated"));

  /**
   * GET /v1/sessions/:id/turns and /metadata (relayhistory routes/turns.ts): the relay
   * session journal from canonical S3 (session-turns-source.ts), never Neon. Same canary
   * allowlist. `source` is optional, as relayhistory's (its first value; empty = none).
   * Every case not answerable exactly is a 501 Cloud falls back on:
   *   turns_no_pointers           no Supabase pointer (Neon may still hold the turns)
   *   turns_harness_bound         more harness scopes than one read may open
   *   turns_unknown               a scope without the legacy export's coverage marker
   *   turns_truncated             an exported string the export had to cut
   *   turns_attribution           a live untagged turn whose source needs Neon's events
   *   turns_canonical_<r>         the canonical read refused (bounds, digests, deadline)
   * Erased (every pointer tombstoned): no turns, and metadata's 404, as relayhistory
   * answers after erasure purged the rows (0074).
   */
  const sessionTurns = async (c: Context, identity: VerifiedIdentity) => {
    attestWorkspace(c, identity);
    requireCanary(identity, "turns");
    const turns = deps.turns;
    if (!turns) throw unsupported("turns_not_configured");
    const sessionId = c.req.param("sessionId");
    if (!sessionId || sessionId.length > MAX_ID_CHARS) throw unsupported("turns_session_id");
    const source = c.req.query("source") || undefined;
    if (source !== undefined && source.length > MAX_ID_CHARS) throw unsupported("turns_source");
    const scope = { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId };
    const states = await turns.harnesses(scope);
    if (states.length > MAX_TURN_HARNESSES) throw unsupported("turns_harness_bound");
    const selected = source === undefined ? states : states.filter((h) => h.harness === source);
    if (!selected.some((h) => h.pointers > 0)) throw unsupported("turns_no_pointers");
    const live = selected.filter((h) => h.live > 0);
    if (live.length === 0) return { rows: [] as Record<string, unknown>[], erased: true };
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const snapshots = await Promise.race([
        Promise.all(live.map(async (h) => ({ harness: h.harness, snapshot: await turns.source({ ...scope, source: h.harness }, controller.signal) }))),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            reject(new CanonicalReadError("deadline"));
            controller.abort();
          }, turns.deadlineMs ?? 3000);
        }),
      ]);
      return { rows: turnRows(scope, snapshots, source), erased: false };
    } finally {
      clearTimeout(timer);
    }
  };
  const turnsFail = (c: Context, error: unknown) => {
    if (error instanceof CanonicalReadError) return fail(c, unsupported(`turns_canonical_${error.reason}`));
    if (error instanceof TurnsInputError) return fail(c, unsupported(error.reason.startsWith("turns_") ? error.reason : `turns_${error.reason}`));
    // A malformed canonical row (canonical-legacy.ts / canonical-turns.ts) is not answerable exactly.
    if (error instanceof Error && /^canonical (legacy evidence|live turn) invalid$/u.test(error.message)) return fail(c, unsupported("turns_canonical_invalid"));
    return fail(c, error);
  };

  app.get("/v1/sessions/:sessionId/turns", async (c) => {
    try {
      const identity = c.get("identity");
      const { rows, erased } = await sessionTurns(c, identity);
      const body = { sessionId: c.req.param("sessionId"), turns: turnsResponse(rows), correlationId: c.get("correlationId") };
      log({ route: c.req.routePath, reason: erased ? "turns_erased" : "turns_served", turns: body.turns.length, correlation_id: c.get("correlationId") });
      return c.json(body);
    } catch (error) {
      return turnsFail(c, error);
    }
  });

  app.get("/v1/sessions/:sessionId/metadata", async (c) => {
    try {
      const identity = c.get("identity");
      const sessionId = c.req.param("sessionId");
      const { rows, erased } = await sessionTurns(c, identity);
      const metadata = metadataResponse(rows);
      log({ route: c.req.routePath, reason: erased ? "metadata_erased" : metadata ? "metadata_served" : "metadata_none", correlation_id: c.get("correlationId") });
      if (!metadata)
        return c.json({ error: { code: "not_found", message: `No conversation turns found for session ${sessionId}` }, correlationId: c.get("correlationId") }, 404);
      return c.json(metadata);
    } catch (error) {
      return turnsFail(c, error);
    }
  });
  app.get("/v1/digest/daily", notMigrated("digest_not_migrated"));

  app.notFound((c) => c.json({ error: { code: "unsupported_by_backend", message: "route not served by this backend", reason: "route" }, correlationId: c.get("correlationId") }, 501));

  return app;
}
