/**
 * The HTTP surface: auth, workspace attestation, the 501 fallback contract,
 * the history floor, erasure gating and headers. A stub ClickHouse records the
 * queries it is asked; result-level behaviour is in page.clickhouse.test.ts.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { ClickHouseError } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, continuationUntil, createReadApi, type VerifiedIdentity } from "../app.ts";
import { relayAuthVerifier } from "../auth.ts";
import { encodeCursor } from "../cursor.ts";
import { handleUrlEvent, parseCompleteSince, parseReadApiSecret } from "../lambda.ts";
import { PARTIAL_SESSIONS_CAP } from "../queries.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

/**
 * `partial`: the org's partial sessions; `partialInWorkspace`: those of them
 * with a row in the token's workspace (the search check's `in_workspace`).
 */
function stubCh(rows: { candidates?: unknown[]; newest?: unknown[]; partial?: string[]; partialInWorkspace?: string[]; fail?: Error } = {}) {
  const queries: string[] = [];
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      queries.push(query);
      if (rows.fail) throw rows.fail;
      if (query.includes("in_workspace")) {
        const inWs = new Set(rows.partialInWorkspace ?? []);
        return (rows.partial ?? []).map((session_id) => ({ session_id, in_workspace: inWs.has(session_id) ? 1 : 0 })) as T[];
      }
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return (rows.partial ?? []).filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      return ((query.includes("bytes_before") ? rows.newest : rows.candidates) ?? []) as T[];
    },
  };
  /** Queries against transcript_events only (the partial-session checks read the state table). */
  const eventQueries = () => queries.filter((q) => !q.includes("search_ingest_session_state"));
  return { ch, queries, eventQueries };
}

function api(over: { identity?: VerifiedIdentity; completeSinceMs?: number; ch?: ClickHouseClient; liveness?: ReturnType<typeof memoryLiveness>["liveness"]; verify?: (t: string) => Promise<VerifiedIdentity> } = {}) {
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({
    verify: over.verify ?? (async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return over.identity ?? ID;
    }),
    ch: over.ch ?? stubCh().ch,
    liveness: over.liveness ?? memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } }).liveness,
    completeSinceMs: over.completeSinceMs ?? 0,
    log: (f) => logs.push(f),
  });
  const get = (path: string, token: string | null = "good") =>
    app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { app, get, logs };
}

const body = async (r: Response) => (await r.json()) as Record<string, any>;

describe("auth and attestation", () => {
  test("no token, a bad token, a missing scope", async () => {
    const { get } = api();
    expect((await get("/v1/events?workspace=ws-a&q=x", null)).status).toBe(401);
    expect((await get("/v1/events?workspace=ws-a&q=x", "bad")).status).toBe(401);
    const noScope = api({ identity: { ...ID, scopes: ["rth:sync"] } });
    const r = await noScope.get("/v1/events?workspace=ws-a&q=x");
    expect(r.status).toBe(403);
    expect((await body(r)).error.code).toBe("forbidden");
  });

  test("workspace must equal the token's; omitting it is the org-wide read relayhistory keeps", async () => {
    const { get } = api();
    expect((await get("/v1/events?workspace=ws-b&q=x")).status).toBe(403);
    expect((await get("/v1/events?workspace=ws-a&workspace=ws-a&q=x")).status).toBe(400);
    const orgWide = await get("/v1/events?q=x");
    expect(orgWide.status).toBe(501);
    expect((await body(orgWide)).error.reason).toBe("org_scope");
  });

  test("tenant predicates come from the token, never from parameters", async () => {
    const { ch, eventQueries } = stubCh();
    const { get } = api({ ch });
    await get("/v1/events?workspace=ws-a&q=hello&limit=20&org=org-b&org_id=org-b");
    expect(eventQueries()[0]).toContain("org_id = {org:String}");
    expect(eventQueries()[0]).toContain("workspace_id = {ws:String}");
  });
});

describe("the 501 fallback contract", () => {
  test.each([
    ["/v1/events?workspace=ws-a&q=x&taskRef=pull/", "param_taskRef"],
    ["/v1/events?workspace=ws-a&q=x&tag=t", "param_tag"],
    ["/v1/events?workspace=ws-a&q=x&agent=a", "param_agent"],
    ["/v1/events?workspace=ws-a&q=x&projectContains=p", "param_projectContains"],
    ["/v1/events?workspace=ws-a&q=x&noProject=true", "param_noProject"],
    ["/v1/sessions/s-1/events?workspace=ws-a&includeMessageId=true", "param_includeMessageId"],
    ["/v1/events?workspace=ws-a", "unfiltered_events"],
    ["/v1/events?workspace=ws-a&q=x&order=asc", "search_ascending"],
    ["/v1/events?workspace=ws-a&q=x&limit=101", "search_limit"],
    ["/v1/events?workspace=ws-a&q=x&cursor=events-v2:abc", "foreign_cursor"],
    // P3b: registered directory routes; off the canary allowlist (none here) they fall back.
    ["/v1/sessions?workspace=ws-a", "directory_workspace_not_enabled"],
    ["/v1/sessions/s-1/timing?workspace=ws-a&source=codex", "timing_workspace_not_enabled"],
  ])("%s → 501 %s", async (path, reason) => {
    const { get } = api();
    const r = await get(path);
    expect(r.status).toBe(501);
    const b = await body(r);
    expect(b.error.code).toBe("unsupported_by_backend");
    expect(b.error.reason).toBe(reason);
  });

  test("a malformed own cursor is a 400, not a silent first page", async () => {
    const { get } = api();
    expect((await get("/v1/events?workspace=ws-a&q=x&cursor=b96e1:%%%")).status).toBe(400);
  });
});

describe("history floor and erasure", () => {
  const floor = Date.parse("2026-09-01T00:00:00Z");

  test("a session older than the floor, or without a pointer, is not served", async () => {
    const old = api({ completeSinceMs: floor, liveness: memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-08-01T00:00:00Z") } }).liveness });
    expect((await body(await old.get("/v1/sessions/s-1/events?workspace=ws-a"))).error.reason).toBe("history_floor");
    const unknown = api({ completeSinceMs: floor, liveness: memoryLiveness().liveness });
    expect((await body(await unknown.get("/v1/sessions/s-1/events?workspace=ws-a"))).error.reason).toBe("history_floor_unknown_session");
    // Inside the floor it is served.
    const ok = api({ completeSinceMs: floor });
    expect((await ok.get("/v1/sessions/s-1/events?workspace=ws-a")).status).toBe(200);
  });

  test("a search that reaches the floor without filling its page falls back", async () => {
    const { ch, eventQueries } = stubCh({ candidates: [] });
    const { get } = api({ ch, completeSinceMs: floor });
    const r = await get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("history_floor");
    // It searched only from the floor.
    expect(eventQueries()[0]).toContain("{since:Int64}");
    // A search bounded inside the floor is served even when short.
    expect((await get("/v1/events?workspace=ws-a&q=needle&limit=20&since=2026-10-01T00:00:00Z")).status).toBe(200);
    // Without a limit, relayhistory's default (200) is above this backend's search cap.
    expect((await body(await get("/v1/events?workspace=ws-a&q=needle"))).error.reason).toBe("search_limit");
  });

  test("a search cursor page that reaches the floor ends cleanly with a relayhistory continuation, never a 501", async () => {
    const cand = { ts_ms: Date.parse("2026-09-02T00:00:00Z"), session_id: "s-1", source: "codex", machine_id: "m-1", kind: "message", event_id: "e-9" };
    const newest = [{ ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-1", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "needle", in_budget: 1 }];
    const { ch, eventQueries } = stubCh({ candidates: [cand], newest });
    const { get, logs } = api({ ch, completeSinceMs: floor });
    const cursor = encodeCursor({ tsMs: Date.parse("2026-09-03T00:00:00Z"), sessionId: "s-1", source: "codex", machineId: "m-1", kind: "message", eventId: "e-10" });
    const r = await get(`/v1/events?workspace=ws-a&q=needle&limit=20&cursor=${cursor}`);
    expect(r.status).toBe(200);
    const b = await body(r);
    // The rows from the floor up, a terminal page, and where relayhistory takes over.
    expect(b.events.map((e: { eventId: string }) => e.eventId)).toEqual(["e-9"]);
    expect(b.nextCursor).toBeNull();
    expect(b.continuation).toEqual({ backend: "relayhistory", reason: "history_floor", until: "2026-08-31T23:59:59.999999Z" });
    expect(eventQueries()[0]).toContain("{since:Int64}");
    expect(logs.some((l) => l.reason === "history_floor_continuation")).toBe(true);
    expect(r.headers.get("x-sessions-read-continuation")).toBe("history_floor");
    // A full cursor page below no floor keeps its own cursor and no continuation.
    const full = api({ ch: stubCh({ candidates: [cand], newest }).ch, completeSinceMs: floor });
    const f = await body(await full.get(`/v1/events?workspace=ws-a&q=needle&limit=1&cursor=${cursor}`));
    expect(f.nextCursor).toStartWith("b96e1:");
    expect(f.continuation).toBeUndefined();
    expect((await full.get(`/v1/events?workspace=ws-a&q=needle&limit=1&cursor=${cursor}`)).headers.get("x-sessions-read-continuation")).toBeNull();
  });

  test("continuationUntil is one microsecond below the floor, in relayhistory's accepted format", () => {
    expect(continuationUntil(Date.parse("2026-09-01T00:00:00Z"))).toBe("2026-08-31T23:59:59.999999Z");
    expect(continuationUntil(Date.parse("2026-09-01T00:00:00.124Z"))).toBe("2026-09-01T00:00:00.123999Z");
  });

  test("an erased session reads as empty without touching ClickHouse", async () => {
    const live = memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } });
    live.erase({ source: "codex", sessionId: "s-1" });
    const { ch, queries } = stubCh({ candidates: [{ ts_ms: 1, session_id: "s-1", source: "codex", machine_id: "m", kind: "k", event_id: "e" }] });
    const { get } = api({ ch, liveness: live.liveness });
    const r = await get("/v1/sessions/s-1/events?workspace=ws-a&source=codex");
    expect(r.status).toBe(200);
    expect(await body(r)).toMatchObject({ sessionId: "s-1", events: [], nextCursor: null });
    expect(queries).toHaveLength(0);
  });
});

describe("partial sessions (search_ingest_session_state.needs_full = 1)", () => {
  test("a partial session's first page is 501 session_partial, so the caller reads it from relayhistory", async () => {
    const { ch, eventQueries } = stubCh({ partial: ["s-1"] });
    const { get } = api({ ch });
    for (const path of ["/v1/sessions/s-1/events?workspace=ws-a", "/v1/events?workspace=ws-a&session=s-1"]) {
      const r = await get(path);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe("session_partial");
    }
    // Its stale rows were never read.
    expect(eventQueries()).toHaveLength(0);
    // Another, complete session is served.
    const other = api({ ch: stubCh({ partial: ["s-2"] }).ch });
    expect((await other.get("/v1/sessions/s-1/events?workspace=ws-a")).status).toBe(200);
  });

  test("a workspace search starts at relayhistory while a session of its workspace is partial", async () => {
    // A row in this workspace: decided in ClickHouse, Supabase not asked.
    const rowHere = stubCh({ partial: ["s-row", "s-elsewhere"], partialInWorkspace: ["s-row"] });
    const noPointers = memoryLiveness();
    let r = await api({ ch: rowHere.ch, liveness: noPointers.liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial");
    expect(rowHere.eventQueries()).toHaveLength(0);
    expect(noPointers.calls.livePointerSessions).toEqual([]);
    // No row anywhere yet (every pass was partial), but a live pointer here: still this workspace's.
    const pointerHere = stubCh({ partial: ["s-1", "s-elsewhere"] });
    const pointers = memoryLiveness({ createdAt: { "codex\u0000s-1": 1 } });
    r = await api({ ch: pointerHere.ch, liveness: pointers.liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial");
    expect(pointerHere.eventQueries()).toHaveLength(0);
    expect(pointers.calls.livePointerSessions).toEqual([["s-1", "s-elsewhere"]]);
  });

  test("partial sessions of other workspaces of the org do not stop this workspace's search", async () => {
    const { ch, eventQueries } = stubCh({ partial: ["s-elsewhere", "s-erased-here"] });
    // s-erased-here: every pointer here tombstoned, so the erasure filter drops its rows anyway.
    const { liveness, calls } = memoryLiveness({ erased: [{ source: "codex", sessionId: "s-erased-here" }] });
    const r = await api({ ch, liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(200);
    expect(eventQueries()).toHaveLength(1);
    expect(calls.livePointerSessions).toEqual([["s-elsewhere", "s-erased-here"]]);
    expect((await api({ ch: stubCh().ch }).get("/v1/events?workspace=ws-a&q=needle&limit=20")).status).toBe(200);
  });

  test("more partial sessions than one check lists is not decided: the search falls back whole", async () => {
    const many = Array.from({ length: PARTIAL_SESSIONS_CAP + 1 }, (_, i) => `s-other-${i}`);
    const { ch, eventQueries } = stubCh({ partial: many });
    const { liveness, calls } = memoryLiveness();
    const r = await api({ ch, liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial_unbounded");
    expect(eventQueries()).toHaveLength(0);
    expect(calls.livePointerSessions).toEqual([]);
    // Exactly the cap is decided.
    const atCap = stubCh({ partial: many.slice(1) });
    expect((await api({ ch: atCap.ch, liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20")).status).toBe(200);
  });

  test("the partial check is bound to the token's org and checked when pagination starts, not on cursor pages", async () => {
    const { ch, queries } = stubCh({ partial: ["s-1"] });
    const { get } = api({ ch });
    const cursor = encodeCursor({ tsMs: 5, sessionId: "s-1", source: "codex", machineId: "m-1", kind: "message", eventId: "e-1" });
    // A cursor continues the read the first page admitted; a 501 here would end the caller's pagination in an error.
    expect((await get(`/v1/sessions/s-1/events?workspace=ws-a&cursor=${cursor}`)).status).toBe(200);
    expect((await get(`/v1/events?workspace=ws-a&q=needle&limit=20&cursor=${cursor}`)).status).toBe(200);
    expect(queries.some((q) => q.includes("search_ingest_session_state"))).toBe(false);
    await get("/v1/events?workspace=ws-a&q=needle&limit=20&org=org-b");
    const state = queries.find((q) => q.includes("search_ingest_session_state"))!;
    expect(state).toContain("org_id = {org:String}");
    expect(state).toContain("workspace_id = {ws:String}");
    expect(state).toContain("argMax(needs_full, synced_at) = 1");
  });
});

describe("responses", () => {
  test("headers: workspace attestation, no-store, backend marker, correlation id", async () => {
    const { get } = api();
    const r = await get("/v1/sessions/s-1/events?workspace=ws-a");
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(r.headers.get("cache-control")).toBe("private, no-store");
    expect(r.headers.get("x-sessions-read-backend")).toBe("96bulls");
    const b = await body(r);
    expect(b).toEqual({ sessionId: "s-1", events: [], nextCursor: null, correlationId: r.headers.get("x-correlation-id") });
  });

  test("a ClickHouse timeout is relayhistory's 503 search_timeout; other failures 503", async () => {
    const t = api({ ch: stubCh({ fail: new ClickHouseError(500, "159", "TIMEOUT_EXCEEDED") }).ch });
    const r = await t.get("/v1/events?workspace=ws-a&q=x&limit=20");
    expect(r.status).toBe(503);
    expect((await body(r)).error.code).toBe("search_timeout");
    const e = api({ ch: stubCh({ fail: new Error("boom") }).ch });
    expect((await e.get("/v1/events?workspace=ws-a&q=x&limit=20")).status).toBe(503);
  });

  test("logs carry ids and codes only", async () => {
    const { get, logs } = api();
    await get("/v1/events?workspace=ws-a&q=secret%20text&limit=20");
    for (const l of logs) expect(JSON.stringify(l)).not.toContain("secret text");
  });
});

describe("relayAuthVerifier", () => {
  const ORIGIN = "https://read.example.test";
  const JWKS = "https://relayauth.test/.well-known/jwks.json";
  const ISSUER = "https://relayauth.test";
  // One key for the suite: the shared verifier caches the JWKS and refetches an
  // unknown kid only after a minimum interval, so a fresh key per token would make
  // every later token fail for "unknown kid" rather than the reason under test.
  const KID = `k-${crypto.randomUUID()}`;
  const realFetch = globalThis.fetch;
  let privateKey: CryptoKey;
  let jwksFetches = 0;
  beforeAll(async () => {
    const pair = (await crypto.subtle.generateKey(
      { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
      true,
      ["sign", "verify"],
    )) as CryptoKeyPair;
    privateKey = pair.privateKey;
    const jwk = { ...(await crypto.subtle.exportKey("jwk", pair.publicKey)), kid: KID, alg: "RS256", use: "sig" };
    globalThis.fetch = (async (url: string) => {
      if (String(url) !== JWKS) throw new Error(`unexpected fetch ${url}`);
      jwksFetches++;
      return new Response(JSON.stringify({ keys: [jwk] }), { headers: { "content-type": "application/json" } });
    }) as typeof fetch;
  });
  afterAll(() => {
    globalThis.fetch = realFetch;
  });

  async function mint(claims: Record<string, unknown>) {
    const enc = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
    const now = Math.floor(Date.now() / 1000);
    const head = enc({ alg: "RS256", kid: KID, typ: "JWT" });
    const payload = enc({ iss: ISSUER, iat: now, exp: now + 60, ...claims });
    const sig = Buffer.from(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", privateKey, new TextEncoder().encode(`${head}.${payload}`))).toString("base64url");
    return `${head}.${payload}.${sig}`;
  }
  const assertion = (over: Record<string, unknown> = {}) => ({
    sub: "agent_relayhistory_assertion",
    meta: { tokenClass: "relayhistory_assertion" },
    aud: [ORIGIN],
    org: "org-a",
    wks: "ws-a",
    sponsorId: "u-1",
    scopes: ["rth:read"],
    ...over,
  });
  const verifier = () => relayAuthVerifier({ origin: ORIGIN, jwksUrl: JWKS, issuer: ISSUER });
  const refusal = async (token: string) => {
    try {
      await verifier()(token);
    } catch (e) {
      return (e as AuthError).message;
    }
    return "accepted";
  };

  test("an assertion minted for this origin is accepted with its exact tenancy", async () => {
    expect(await verifier()(await mint(assertion()))).toEqual({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] });
    // Proof the key resolves for every later case: each refusal below is for its own reason.
    expect(await refusal(await mint(assertion()))).toBe("accepted");
  });

  test("a token minted for History (another origin) or the legacy audience is refused", async () => {
    for (const aud of [["https://history.example.invalid"], ["relayhistory"], [ORIGIN, "https://history.example.invalid"]])
      expect(await refusal(await mint(assertion({ aud })))).toBe("token is not valid");
  });

  test("the origin binding is checked here too, whatever the shared verifier returns", async () => {
    const stub = (historyOrigin: string | undefined) =>
      relayAuthVerifier({ origin: ORIGIN }, (async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", tokenSubject: "s", scopes: ["rth:read"], claims: { org: "org-a", wks: "ws-a", sponsorId: "u-1" }, historyOrigin })) as never);
    await expect(stub(undefined)("t")).rejects.toThrow("not bound to this service");
    await expect(stub("https://history.example.invalid")("t")).rejects.toThrow("not bound to this service");
    expect((await stub(ORIGIN)("t")).orgId).toBe("org-a");
  });

  test("a non-assertion token for this audience, an expired one, or ambiguous tenancy is refused", async () => {
    expect(await refusal(await mint(assertion({ sub: "agent_x", meta: {} })))).toBe("token is not valid");
    const now = Math.floor(Date.now() / 1000);
    expect(await refusal(await mint(assertion({ exp: now - 120 })))).toBe("token is not valid");
    // An alternative claim name the shared extractor would prefer over `org` / `wks`.
    expect(await refusal(await mint(assertion({ relay_org_id: "org-b" })))).toBe("token tenancy is ambiguous");
    expect(await refusal(await mint(assertion({ relay_workspace_id: "ws-b" })))).toBe("token tenancy is ambiguous");
    expect(await refusal(await mint(assertion({ wks: undefined, workspace_id: "ws-a" })))).toBe("token tenancy is ambiguous");
    expect(jwksFetches).toBeGreaterThan(0);
  });

  test("no configured origin refuses every token", async () => {
    await expect(relayAuthVerifier({ origin: undefined, jwksUrl: JWKS, issuer: ISSUER })(await mint(assertion()))).rejects.toThrow("no origin configured");
  });
});

describe("lambda adapter and configuration", () => {
  test("Function URL events map to GET requests; other methods are 405", async () => {
    const { app } = api();
    const ok = await handleUrlEvent(app, { rawPath: "/v1/sessions/s-1/events", rawQueryString: "workspace=ws-a", headers: { authorization: "Bearer good" }, requestContext: { http: { method: "GET" } } });
    expect(ok.statusCode).toBe(200);
    expect(ok.headers["x-relayhistory-workspace-id"]).toBe("ws-a");
    expect(JSON.parse(ok.body).sessionId).toBe("s-1");
    const post = await handleUrlEvent(app, { rawPath: "/v1/events", headers: {}, requestContext: { http: { method: "POST" } } });
    expect(post.statusCode).toBe(405);
  });

  test("secret and floor parsing never echo values and refuse junk", () => {
    expect(() => parseReadApiSecret("not json")).toThrow("not JSON");
    expect(() => parseReadApiSecret(JSON.stringify({ supabase_url: "postgresql://x", clickhouse: { url: "u", user: "u" } }))).toThrow("clickhouse.password");
    try {
      parseReadApiSecret(JSON.stringify({ supabase_url: "mysql://hunter2", clickhouse: {} }));
    } catch (e) {
      expect(String(e)).not.toContain("hunter2");
    }
    expect(parseCompleteSince("epoch")).toBe(0);
    expect(parseCompleteSince("2026-09-01T00:00:00Z")).toBe(Date.parse("2026-09-01T00:00:00Z"));
    expect(() => parseCompleteSince(undefined)).toThrow();
    expect(() => parseCompleteSince("soon")).toThrow();
  });

  test("own cursors round-trip; the prefix keeps them out of relayhistory", () => {
    const c = encodeCursor({ tsMs: 5, sessionId: "s", source: "codex", machineId: "m", kind: "k", eventId: "e" });
    expect(c.startsWith("b96e1:")).toBe(true);
  });
});
