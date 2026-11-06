/**
 * The directory routes (P3b): GET /v1/sessions and /v1/sessions/activity from the
 * Supabase directory, the canary allowlist, the 501 contract (never Neon), parameter
 * validation as relayhistory's, the row mapping and cursors, and the not-yet-migrated
 * routes. sqlDirectory itself is exercised against PGlite: without migration
 * 20261105029000 it answers null (501 directory_not_migrated), never an empty page.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { AuthError, createReadApi, parseDirectoryWorkspaces, type VerifiedIdentity } from "../app.ts";
import { encodeCursor } from "../cursor.ts";
import {
  DirectoryError,
  activityDates,
  condenseTurn,
  decodeListCursor,
  encodeListCursor,
  sessionFromDirectoryRow,
  sqlDirectory,
  type Directory,
  type DirectoryListArgs,
  type DirectoryListRow,
} from "../directory.ts";
import { poolTenantRunner } from "../liveness.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

function dirRow(over: Partial<DirectoryListRow> & { cursor_ts?: string } = {}): DirectoryListRow & { cursor_ts?: string } {
  return {
    org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: "s-1",
    created_at: new Date("2026-10-01T00:00:00Z"), last_activity_at: new Date("2026-10-02T03:04:05.123Z"),
    cursor_ts: "2026-10-02T03:04:05.123456Z",
    title: "A title", task_title: "Fix the thing", agent_names: [], project_id: "github.com/acme/api",
    project_key: null, project_key_method: null, git_repo: null, git_branch: null, initial_commit: null,
    originator: null, agent_version: null, models: ["gpt-5"], state: "unknown", event_count: "12",
    input_tokens: "100", output_tokens: 7, cost_usd_micros: null, machine_ids: ["m-1"], user_ids: ["u-2", "u-1"],
    parent_session_ids: [], child_session_count: 0, short_summary: null, short_summary_generated_at: null,
    catalog_projected_at: null, projected_at: null,
    ...over,
  };
}

/** A recording Directory stub. */
function stubDirectory(init: { rows?: Array<DirectoryListRow & { cursor_ts?: string }> | null; activity?: Array<{ day: string; harness: string; sessions: number }> | null; fail?: unknown } = {}) {
  const calls: Array<{ fn: string; orgId: string; workspaceId: string; args: unknown }> = [];
  const directory: Directory = {
    async list(orgId, workspaceId, args) {
      calls.push({ fn: "list", orgId, workspaceId, args });
      if (init.fail) throw init.fail;
      return init.rows === undefined ? [dirRow()] : init.rows;
    },
    async activity(orgId, workspaceId, since, until) {
      calls.push({ fn: "activity", orgId, workspaceId, args: { since, until } });
      if (init.fail) throw init.fail;
      return init.activity === undefined ? [] : init.activity;
    },
  };
  return { directory, calls };
}

function api(over: { identity?: VerifiedIdentity; directory?: Directory; workspaces?: ReadonlySet<string> | "*" | undefined } = {}) {
  const logs: Record<string, unknown>[] = [];
  const ch = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select() {
      throw new Error("the directory routes never read ClickHouse");
    },
  };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return over.identity ?? ID;
    },
    ch: ch as never,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: (f) => logs.push(f),
    directory: over.directory,
    directoryWorkspaces: "workspaces" in over ? over.workspaces : new Set(["ws-a"]),
  });
  const get = (path: string, token: string | null = "good") =>
    app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { get, logs };
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;

describe("GET /v1/sessions from the directory", () => {
  test("serves the page with tenant only from the token, relayhistory's shape, and a b96s1 cursor", async () => {
    const { directory, calls } = stubDirectory({ rows: [dirRow(), dirRow({ session_id: "s-2", cursor_ts: "2026-10-01T00:00:00.000001Z" })] });
    const { get } = api({ directory });
    const r = await get("/v1/sessions?workspace=ws-a&limit=1&source=codex&source=claude&notSource=gemini&user=u-1&agent=%20Lead%20&projectContains=acme&lastTsAfter=2026-10-01T00:00:00Z&firstTsBefore=2026-11-01T00:00:00Z");
    expect(r.status).toBe(200);
    expect(r.headers.get("x-sessions-read-backend")).toBe("96bulls");
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    const b = await body(r);
    // Exactly the strict cloud-client 0.2.0 sessionsResponse keys (no ranking, shortSummary, search, agentName).
    expect(Object.keys(b).sort()).toEqual(["correlationId", "nextCursor", "sessions"]);
    expect(b.sessions).toHaveLength(1);
    expect(b.sessions[0]).toEqual({
      title: "A title", sessionId: "s-1", userIds: ["u-1", "u-2"], source: "codex",
      projectId: "github.com/acme/api", machineIds: ["m-1"], firstTs: "2026-10-01T00:00:00.000Z",
      lastTs: "2026-10-02T03:04:05.123Z", workState: null, eventCount: 12, kinds: [], models: ["gpt-5"],
      taskTitle: "Fix the thing", summary: "Fix the thing", summarySource: "task_title", taskRefs: [],
      totalCostUsdMicros: null, totalInputTokens: 100, totalOutputTokens: 7, totalReasoningTokens: 0,
      totalCacheReadTokens: 0, totalCacheCreateTokens: 0, totalCacheCreate5mTokens: null, totalCacheCreate1hTokens: null,
    });
    expect(decodeListCursor(b.nextCursor)).toEqual({ lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "s-1", harness: "codex" });
    const args = calls[0]!.args as DirectoryListArgs;
    expect(calls[0]).toMatchObject({ fn: "list", orgId: "org-a", workspaceId: "ws-a" });
    expect(args).toMatchObject({ limit: 1, sources: ["codex", "claude"], notSources: ["gemini"], userIds: ["u-1"], agent: "Lead", projectContains: "acme" });
    expect(args.lastTsAfter?.toISOString()).toBe("2026-10-01T00:00:00.000Z");
    expect(args.firstTsBefore?.toISOString()).toBe("2026-11-01T00:00:00.000Z");
    // The cursor continues at the directory.
    const next = await get(`/v1/sessions?workspace=ws-a&limit=1&cursor=${encodeURIComponent(b.nextCursor)}`);
    expect(next.status).toBe(200);
    expect((calls[1]!.args as DirectoryListArgs).cursor).toEqual({ lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "s-1", harness: "codex" });
  });

  test("canary allowlist: unset, empty and other workspaces get 501 before anything is read", async () => {
    for (const workspaces of [undefined, new Set<string>(), new Set(["ws-other"])]) {
      const { directory, calls } = stubDirectory();
      const { get } = api({ directory, workspaces });
      const r = await get("/v1/sessions?workspace=ws-a&limit=999999&q=x");
      expect(r.status).toBe(501);
      expect((await body(r)).error).toMatchObject({ code: "unsupported_by_backend", reason: "directory_workspace_not_enabled" });
      const a = await get("/v1/sessions/activity?workspace=ws-a");
      expect((await body(a)).error.reason).toBe("directory_workspace_not_enabled");
      expect(calls).toEqual([]);
    }
    const { directory } = stubDirectory();
    expect((await api({ directory, workspaces: "*" }).get("/v1/sessions?workspace=ws-a")).status).toBe(200);
    expect(parseDirectoryWorkspaces(undefined)).toEqual(new Set());
    expect(parseDirectoryWorkspaces("  ")).toEqual(new Set());
    expect(parseDirectoryWorkspaces("*")).toBe("*");
    expect(parseDirectoryWorkspaces(" 1220c1dc-0ce5-45fe-8b67-e7bb57a2db63 , ws-b,")).toEqual(new Set(["1220c1dc-0ce5-45fe-8b67-e7bb57a2db63", "ws-b"]));
  });

  test("until the migration exists: 501 directory_not_migrated, never an empty page", async () => {
    const { directory } = stubDirectory({ rows: null, activity: null });
    const { get } = api({ directory });
    const r = await get("/v1/sessions?workspace=ws-a");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("directory_not_migrated");
    const a = await get("/v1/sessions/activity?workspace=ws-a&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z");
    expect(a.status).toBe(501);
    expect((await body(a)).error.reason).toBe("directory_not_migrated");
    expect((await body(await api({}).get("/v1/sessions?workspace=ws-a"))).error.reason).toBe("directory_not_configured");
  });

  test("auth, workspace attestation and scope are the event routes'", async () => {
    const { directory, calls } = stubDirectory();
    const { get } = api({ directory });
    expect((await get("/v1/sessions?workspace=ws-a", null)).status).toBe(401);
    expect((await get("/v1/sessions?workspace=ws-a", "bad")).status).toBe(401);
    expect((await get("/v1/sessions?workspace=ws-b")).status).toBe(403);
    const org = await get("/v1/sessions");
    expect(org.status).toBe(501);
    expect((await body(org)).error.reason).toBe("org_scope");
    expect((await api({ directory, identity: { ...ID, scopes: ["rth:sessions:read"] } }).get("/v1/sessions?workspace=ws-a")).status).toBe(403);
    expect(calls).toEqual([]);
  });

  test("what the directory cannot answer exactly is 501 with a reason (first page; the caller falls back)", async () => {
    const { directory, calls } = stubDirectory();
    const { get } = api({ directory });
    const cases: Array<[string, string]> = [
      ["q=x", "param_q"], ["search=hybrid&q=xy", "param_q"], ["project=p", "param_project"], ["noProject=true", "param_noProject"],
      ["kind=tool_call", "param_kind"], ["taskRef=t", "param_taskRef"], ["noTaskRef=true", "param_noTaskRef"], ["tag=x", "param_tag"],
      ["since=2026-10-01T00:00:00Z", "param_since"], ["until=2026-10-01T00:00:00Z", "param_until"],
      ["limit=201", "limit"], ["source=Claude%20Code", "source_format"], ["notSource=X", "source_format"],
      ["projectContains=%20%20", "project_contains"],
      [`cursor=${encodeURIComponent('sessions-v2:{"ts":"2026-10-01T00:00:00Z","source":"codex","sessionId":"s"}')}`, "foreign_cursor"],
      [`cursor=${encodeURIComponent(encodeCursor({ tsMs: 1, sessionId: "s", source: "codex", machineId: "m", kind: "k", eventId: "e" }))}`, "foreign_cursor"],
    ];
    for (const [qs, reason] of cases) {
      const r = await get(`/v1/sessions?workspace=ws-a&${qs}`);
      expect(r.status, qs).toBe(501);
      expect((await body(r)).error, qs).toMatchObject({ code: "unsupported_by_backend", reason });
    }
    expect(calls).toEqual([]);
  });

  test("invalid values are relayhistory's 400s", async () => {
    const { directory, calls } = stubDirectory();
    const { get } = api({ directory });
    const many = (name: string, n: number) => Array.from({ length: n }, (_, i) => `${name}=v${i}`).join("&");
    for (const qs of [
      many("source", 17), many("notSource", 17), many("user", 21), `user=${"u".repeat(256)}`,
      `agent=${"a".repeat(201)}`, `projectContains=${"p".repeat(201)}`, "lastTsAfter=junk", "firstTsBefore=nope",
      "lastTsAfter=2026-10-02T00:00:00Z&lastTsBefore=2026-10-01T00:00:00Z",
      "firstTsAfter=2026-10-02T00:00:00Z&firstTsBefore=2026-10-01T00:00:00Z",
      "cursor=b96s1:garbage", "limit=1&limit=2", "agent=a&agent=b",
    ]) {
      const r = await get(`/v1/sessions?workspace=ws-a&${qs}`);
      expect(r.status, qs).toBe(400);
      expect((await body(r)).error.code, qs).toBe("bad_request");
    }
    expect(calls).toEqual([]);
  });

  test("limit is relayhistory's clamp (default 50, junk 50, below 1 is 1)", async () => {
    const { directory, calls } = stubDirectory({ rows: [] });
    const { get } = api({ directory });
    for (const [qs, limit] of [["", 50], ["limit=junk", 50], ["limit=0", 1], ["limit=200", 200], ["limit=7.9", 7]] as const) {
      expect((await get(`/v1/sessions?workspace=ws-a&${qs}`)).status).toBe(200);
      expect((calls.at(-1)!.args as DirectoryListArgs).limit, qs).toBe(limit);
    }
    const empty = await body(await get("/v1/sessions?workspace=ws-a"));
    expect(empty).toMatchObject({ sessions: [], nextCursor: null });
  });

  test("a row of another tenant fails the read (500), never reaches the response", async () => {
    for (const row of [dirRow({ org_id: "org-b" }), dirRow({ workspace_id: "ws-b" })]) {
      const { directory } = stubDirectory({ rows: [row] });
      const { get, logs } = api({ directory });
      const r = await get("/v1/sessions?workspace=ws-a");
      expect(r.status).toBe(500);
      expect(JSON.stringify(await body(r))).not.toContain("s-1");
      expect(logs.some((l) => l.reason === "cross_tenant_rows")).toBe(true);
    }
  });

  test("directory refusals: agent_filter_not_ready is 501; others are 503 without detail", async () => {
    const notReady = api({ directory: stubDirectory({ fail: new DirectoryError("agent_filter_not_ready") }).directory });
    const r = await notReady.get("/v1/sessions?workspace=ws-a&agent=lead");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("agent_filter_not_ready");
    const outOfContract = api({ directory: stubDirectory({ rows: [dirRow({ title: "t".repeat(91) })] }).directory });
    const first = await outOfContract.get("/v1/sessions?workspace=ws-a");
    expect(first.status).toBe(501);
    expect((await body(first)).error.reason).toBe("directory_row_out_of_contract");
    const cursorPage = await outOfContract.get(`/v1/sessions?workspace=ws-a&cursor=${encodeURIComponent(encodeListCursor({ lastActivityAt: "2026-10-02T00:00:00Z", sessionId: "s", harness: "codex" }))}`);
    expect(cursorPage.status).toBe(503);
    for (const fail of [new DirectoryError("refused"), new Error("connection reset")]) {
      const { get } = api({ directory: stubDirectory({ fail }).directory });
      const e = await get("/v1/sessions?workspace=ws-a");
      expect(e.status).toBe(503);
      expect((await body(e)).error).toEqual({ code: "backend_unavailable", message: "read backend unavailable" });
    }
  });
});

describe("GET /v1/sessions/activity from the directory", () => {
  test("every UTC day zero-filled and ascending, totals the sum of bySource", async () => {
    const { directory, calls } = stubDirectory({
      activity: [
        { day: "2026-10-01", harness: "codex", sessions: 2 },
        { day: "2026-10-01", harness: "claude", sessions: 1 },
        { day: "2026-10-03", harness: "codex", sessions: 4 },
      ],
    });
    const { get } = api({ directory });
    const r = await get("/v1/sessions/activity?workspace=ws-a&since=2026-10-01T10:00:00Z&until=2026-10-04T01:00:00Z");
    expect(r.status).toBe(200);
    expect(await body(r)).toEqual({
      since: "2026-10-01T10:00:00.000Z",
      until: "2026-10-04T01:00:00.000Z",
      days: [
        { date: "2026-10-01", total: 3, bySource: { codex: 2, claude: 1 } },
        { date: "2026-10-02", total: 0, bySource: {} },
        { date: "2026-10-03", total: 4, bySource: { codex: 4 } },
        { date: "2026-10-04", total: 0, bySource: {} },
      ],
      correlationId: expect.any(String),
    });
    expect(calls[0]).toMatchObject({ fn: "activity", orgId: "org-a", workspaceId: "ws-a" });
  });

  test("relayhistory's 400s, q is 501, at most 92 UTC days", async () => {
    const { directory, calls } = stubDirectory();
    const { get } = api({ directory });
    for (const qs of ["", "since=2026-10-01T00:00:00Z", "since=junk&until=2026-10-01T00:00:00Z", "since=2026-10-02T00:00:00Z&until=2026-10-01T00:00:00Z", "since=2026-01-01T00:00:00Z&until=2026-04-03T00:00:00Z"])
      expect((await get(`/v1/sessions/activity?workspace=ws-a&${qs}`)).status, qs).toBe(400);
    expect((await get("/v1/sessions/activity?workspace=ws-a&since=2026-01-01T00:00:00Z&until=2026-04-02T23:59:59Z")).status).toBe(200);
    const q = await get("/v1/sessions/activity?workspace=ws-a&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z&q=x");
    expect(q.status).toBe(501);
    expect((await body(q)).error.reason).toBe("param_q");
    expect(calls).toHaveLength(1);
  });
});

describe("routes not yet migrated answer 501 with the store they need", () => {
  test("output, digest, and timing/turns/metadata without their canonical source", async () => {
    const { get } = api({ directory: stubDirectory().directory });
    for (const [path, reason] of [
      ["/v1/sessions/s-1/timing?workspace=ws-a&source=codex", "timing_not_configured"],
      ["/v1/sessions/output?workspace=ws-a&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z", "output_not_migrated"],
      ["/v1/sessions/s-1/turns?workspace=ws-a", "turns_not_configured"],
      ["/v1/sessions/s-1/metadata?workspace=ws-a", "turns_not_configured"],
      ["/v1/digest/daily?workspace=ws-a&date=2026-10-01", "digest_not_migrated"],
    ] as const) {
      const r = await get(path);
      expect(r.status, path).toBe(501);
      expect((await body(r)).error, path).toMatchObject({ code: "unsupported_by_backend", reason });
      expect((await get(path.replace("ws-a", "ws-b"))).status, path).toBe(403);
      expect((await get(path, null)).status, path).toBe(401);
    }
  });
});

describe("mapping and cursors", () => {
  test("row mapping: catalog only when projected, work state, summary condensing, number parsing", () => {
    const projected = sessionFromDirectoryRow(dirRow({
      catalog_projected_at: new Date(), git_branch: "main", git_repo: "https://github.com/acme/api", parent_session_ids: ["p-1", ""],
      child_session_count: "3", state: "done", task_title: "  ", cost_usd_micros: "42",
    }));
    expect(projected.catalog).toEqual({
      workspaceId: "ws-a", title: "A title", gitBranch: "main", repoUrl: "https://github.com/acme/api", initialCommit: null,
      projectKey: null, projectKeyMethod: null, models: ["gpt-5"], originator: null, agentVersion: null,
      firstActivityAt: "2026-10-01T00:00:00.000Z", lastActivityAt: "2026-10-02T03:04:05.123Z", parentSessionIds: ["p-1"], childSessionCount: 3,
    });
    expect(projected).toMatchObject({ workState: { status: "finished", updatedAt: "2026-10-02T03:04:05.123Z" }, summary: null, summarySource: null, totalCostUsdMicros: 42 });
    expect(sessionFromDirectoryRow(dirRow({ state: "active" })).workState?.status).toBe("active");
    expect(sessionFromDirectoryRow(dirRow({ state: "abandoned" })).workState?.status).toBe("idle");
    expect("catalog" in sessionFromDirectoryRow(dirRow())).toBe(false);
    const long = "word ".repeat(100);
    expect(condenseTurn(long)!.length).toBeLessThanOrEqual(241);
    expect(condenseTurn(long)!.endsWith("…")).toBe(true);
    for (const bad of [dirRow({ event_count: "-1" }), dirRow({ input_tokens: "1.5" }), dirRow({ title: "t".repeat(91) })])
      expect(() => sessionFromDirectoryRow(bad)).toThrow(DirectoryError);
  });

  test("cursor round trip keeps microseconds; anything else is foreign or invalid", () => {
    const c = { lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "s/é 1", harness: "claude" };
    expect(decodeListCursor(encodeListCursor(c))).toEqual(c);
    expect(decodeListCursor(undefined)).toBeUndefined();
    expect(decodeListCursor("sessions-v2:{}")).toBe("foreign");
    expect(decodeListCursor("b96e1:abc")).toBe("foreign");
    expect(decodeListCursor("b96s1:!!!")).toBe("invalid");
    expect(decodeListCursor(encodeListCursor({ ...c, harness: "Bad Harness" }))).toBe("invalid");
    expect(decodeListCursor(encodeListCursor({ ...c, lastActivityAt: "2026-10-02" }))).toBe("invalid");
  });

  test("activityDates spans UTC days inclusively", () => {
    expect(activityDates(new Date("2026-10-31T23:59:59Z"), new Date("2026-11-01T00:00:00Z"))).toEqual(["2026-10-31", "2026-11-01"]);
  });
});

describe("sqlDirectory against Supabase without migration 20261105029000", () => {
  const open: PGlite[] = [];
  afterEach(async () => {
    await Promise.all(open.splice(0).map((db) => db.close()));
  });

  test("reports not installed (null) inside the tenant-bound READ ONLY transaction", async () => {
    const root = join(import.meta.dir, "../../..");
    const read = (p: string) => readFileSync(join(root, p), "utf8");
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    open.push(db);
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await db.exec(read(`supabase/bootstrap/${f}.sql`));
    for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
      if (file.startsWith("20261105029000")) continue;
      await db.exec(read(`supabase/migrations/${file}`));
    }
    await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
    const statements: string[] = [];
    const pool = {
      async connect() {
        await db.exec("SET ROLE bulls96_read_api");
        return {
          query: (text: string, params?: unknown[]) => {
            statements.push(text);
            return db.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>;
          },
          release: () => void db.exec("RESET ROLE"),
        };
      },
    };
    const directory = sqlDirectory(poolTenantRunner(pool));
    expect(await directory.list("org-a", "ws-a", { limit: 50 })).toBeNull();
    expect(await directory.activity("org-a", "ws-a", new Date("2026-10-01T00:00:00Z"), new Date("2026-10-02T00:00:00Z"))).toBeNull();
    expect(statements.filter((s) => s.startsWith("BEGIN"))).toEqual(["BEGIN READ ONLY", "BEGIN READ ONLY"]);
    expect(statements.some((s) => s.includes("list_session_directory_for("))).toBe(false);
  }, 120_000);
});
