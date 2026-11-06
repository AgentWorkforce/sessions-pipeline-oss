/**
 * readEventPage against a real ClickHouse with the production schema. Skipped
 * unless READ_API_TEST_CLICKHOUSE_URL is set (CI sets it; see support.ts).
 *
 * Merges are stopped on the table, so every inserted version stays a separate
 * part row exactly as it does in production before a merge folds it.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { decodeCursor } from "../cursor.ts";
import { recordState } from "../../session-pipeline/search-index.ts";
import { createReadApi } from "../app.ts";
import { CONTENT_TRUNCATION_NOTE, TenantMismatchError, partialSessions, partialSessionsInWorkspace, readEventPage, type PageDeps } from "../page.ts";
import type { EventQuery } from "../queries.ts";
import { CH_URL, freshClickHouse, memoryLiveness, row } from "./support.ts";

const suite = CH_URL ? describe : describe.skip;

let ch: ClickHouseClient & { drop(): Promise<void> };

const tenant = { orgId: "org-a", workspaceId: "ws-a" } as const;
const q = (over: Partial<EventQuery>): EventQuery => ({ ...tenant, order: "asc", limit: 50, ...over });

/**
 * One INSERT per row: an INSERT block is collapsed by ReplacingMergeTree on insert
 * (optimize_on_insert), so versions inserted together would never coexist.
 */
async function insert(rows: ReturnType<typeof row>[]) {
  for (const r of rows) await ch.insert("transcript_events", [r]);
}

/** Physical rows (no dedupe): proves a test really has several versions stored. */
async function physical(session: string): Promise<number> {
  const [r] = await ch.select<{ n: number }>("SELECT count() AS n FROM transcript_events WHERE session_id = {s:String}", { params: { s: session } });
  return Number(r!.n);
}

async function all(deps: PageDeps, input: EventQuery) {
  const ids: string[] = [];
  let cursor = input.cursor;
  for (let i = 0; i < 100; i++) {
    const page = await readEventPage(deps, { ...input, cursor });
    ids.push(...page.events.map((e) => e.eventId));
    if (!page.nextCursor) return ids;
    cursor = decodeCursor(page.nextCursor);
  }
  throw new Error("did not terminate");
}

suite("readEventPage on ClickHouse", () => {
  beforeAll(async () => {
    ch = await freshClickHouse();
    await ch.command("SYSTEM STOP MERGES transcript_events");
  });
  afterAll(async () => {
    await ch?.drop();
  });

  test("a redacted newer version is never matched through its stale predecessor (#242 H1)", async () => {
    await insert([
      row({ session: "h1", id: "e1", ts: "2026-10-01 10:00:00.000", content: "token sk-SECRET-123 here", version: 1 }),
      row({ session: "h1", id: "e1", ts: "2026-10-01 10:00:00.000", content: "token [REDACTED] here", version: 2 }),
    ]);
    expect(await physical("h1")).toBe(2);
    const { liveness } = memoryLiveness();
    const deps = { ch, liveness };
    // Workspace search for the secret: the only matching version is stale.
    const search = await readEventPage(deps, q({ q: "sk-SECRET", order: "desc" }));
    expect(search.events).toEqual([]);
    // In-session search too.
    const inSession = await readEventPage(deps, q({ sessionId: "h1", q: "sk-SECRET" }));
    expect(inSession.events).toEqual([]);
    // A plain session read returns exactly one row, the newest.
    const read = await readEventPage(deps, q({ sessionId: "h1" }));
    expect(read.events.map((e) => e.content)).toEqual(["token [REDACTED] here"]);
    // And searching what the newest version says finds it.
    const now = await readEventPage(deps, q({ q: "redacted", order: "desc" }));
    expect(now.events.map((e) => [e.eventId, e.content])).toEqual([["e1", "token [REDACTED] here"]]);
  });

  test("a project moved by a newer version is filtered on the newest value", async () => {
    await insert([
      row({ session: "p1", id: "p-e1", ts: "2026-10-02 10:00:00.000", project: "old-project", content: "alpha needle", version: 1 }),
      row({ session: "p1", id: "p-e1", ts: "2026-10-02 10:00:00.000", project: "new-project", content: "alpha needle", version: 2 }),
    ]);
    expect(await physical("p1")).toBe(2);
    const deps = { ch, liveness: memoryLiveness().liveness };
    expect((await readEventPage(deps, q({ q: "needle", projectId: "old-project", order: "desc" }))).events).toEqual([]);
    expect((await readEventPage(deps, q({ q: "needle", projectId: "new-project", order: "desc" }))).events.map((e) => e.projectId)).toEqual(["new-project"]);
  });

  test("other tenants are invisible: same session id in another workspace and org", async () => {
    await insert([
      row({ session: "shared", id: "mine", ts: "2026-10-03 10:00:00.000", content: "shared needle mine" }),
      row({ ws: "ws-b", session: "shared", id: "theirs-ws", ts: "2026-10-03 10:00:01.000", content: "shared needle theirs" }),
      row({ org: "org-b", session: "shared", id: "theirs-org", ts: "2026-10-03 10:00:02.000", content: "shared needle theirs" }),
    ]);
    const deps = { ch, liveness: memoryLiveness().liveness };
    expect((await readEventPage(deps, q({ sessionId: "shared" }))).events.map((e) => e.eventId)).toEqual(["mine"]);
    expect((await readEventPage(deps, q({ q: "shared needle", order: "desc" }))).events.map((e) => e.eventId)).toEqual(["mine"]);
  });

  test("a row from another tenant reaching the page is refused, not served", async () => {
    const deps = { ch, liveness: memoryLiveness().liveness };
    const leaky: ClickHouseClient = {
      ...ch,
      database: ch.database,
      command: ch.command,
      insert: ch.insert,
      async select<T>(query: string, options?: Parameters<ClickHouseClient["select"]>[1]) {
        const rows = await ch.select<Record<string, unknown>>(query, options);
        return rows.map((r) => ("org_id" in r ? { ...r, workspace_id: "ws-b" } : r)) as T[];
      },
    };
    await expect(readEventPage({ ...deps, ch: leaky }, q({ sessionId: "shared" }))).rejects.toBeInstanceOf(TenantMismatchError);
  });

  test("pagination is exact in both orders, across one millisecond, with repeated versions", async () => {
    const rows = [];
    for (let i = 0; i < 9; i++) {
      const ts = i < 4 ? "2026-10-04 10:00:00.500" : `2026-10-04 10:00:0${i}.000`;
      // Several versions of every event: they must not fill a page or fake its end.
      for (let v = 1; v <= 3; v++) rows.push(row({ session: "pg", id: `pg-${i}`, ts, version: v, content: `v${v} of ${i}` }));
    }
    await insert(rows);
    expect(await physical("pg")).toBe(27);
    const deps = { ch, liveness: memoryLiveness().liveness };
    // Pages are full while rows remain: stored versions never take a page's slots.
    for (const limit of [2, 3, 4]) expect((await readEventPage(deps, q({ sessionId: "pg", limit }))).events).toHaveLength(limit);
    const asc = await all(deps, q({ sessionId: "pg", limit: 2 }));
    expect(asc).toEqual([...asc].sort((a, b) => Number(a.slice(3)) - Number(b.slice(3)) || 0));
    expect(new Set(asc).size).toBe(9);
    expect(asc).toHaveLength(9);
    const desc = await all(deps, q({ sessionId: "pg", limit: 3, order: "desc" }));
    expect(desc).toEqual([...asc].reverse());
    // Every row is the newest version.
    const page = await readEventPage(deps, q({ sessionId: "pg", limit: 50 }));
    expect(page.events.every((e) => e.content!.startsWith("v3 "))).toBe(true);
    expect(page.nextCursor).toBeNull();
  });

  test("a page the candidate scan filled always carries a cursor, even when rows were dropped", async () => {
    await insert([
      row({ session: "drop", id: "d1", ts: "2026-10-05 10:00:01.000", content: "needle one", version: 1 }),
      row({ session: "drop", id: "d1", ts: "2026-10-05 10:00:01.000", content: "gone", version: 2 }),
      row({ session: "drop", id: "d2", ts: "2026-10-05 10:00:02.000", content: "needle two" }),
      row({ session: "drop", id: "d3", ts: "2026-10-05 10:00:03.000", content: "needle three" }),
    ]);
    expect(await physical("drop")).toBe(4);
    const deps = { ch, liveness: memoryLiveness().liveness };
    const first = await readEventPage(deps, q({ sessionId: "drop", q: "needle", limit: 2 }));
    expect(first.events.map((e) => e.eventId)).toEqual(["d2"]);
    expect(first.nextCursor).not.toBeNull();
    expect(await all(deps, q({ sessionId: "drop", q: "needle", limit: 2 }))).toEqual(["d2", "d3"]);
  });

  test("the byte budget cuts a page with a cursor, never silently (#251)", async () => {
    const big = "x".repeat(900 * 1024);
    await insert([0, 1, 2, 3, 4].map((i) => row({ session: "big", id: `b${i}`, ts: `2026-10-06 10:00:0${i}.000`, content: big })));
    const deps = { ch, liveness: memoryLiveness().liveness };
    const first = await readEventPage(deps, q({ sessionId: "big", limit: 50 }));
    expect(first.events.length).toBeGreaterThan(0);
    expect(first.events.length).toBeLessThan(5);
    expect(first.nextCursor).not.toBeNull();
    expect(await all(deps, q({ sessionId: "big", limit: 50 }))).toEqual(["b0", "b1", "b2", "b3", "b4"]);
    // With maxContent the same rows fit one page, each cut with relayhistory's marker.
    const small = await readEventPage(deps, q({ sessionId: "big", limit: 50, maxContent: 10 }));
    expect(small.events).toHaveLength(5);
    expect(small.events[0]!.content).toBe(`xxxxxxxxxx${CONTENT_TRUNCATION_NOTE}`);
    expect(small.events[0]!.contentTruncated).toBe(true);
    expect(small.nextCursor).toBeNull();
  });

  test("maxContent counts UTF-16 units like relayhistory, also past astral characters", async () => {
    const text = "😀😀😀abc";
    await insert([row({ session: "utf", id: "u1", ts: "2026-10-07 10:00:00.000", content: text })]);
    const deps = { ch, liveness: memoryLiveness().liveness };
    for (const mc of [1, 3, 5, 9, 20]) {
      const [e] = (await readEventPage(deps, q({ sessionId: "utf", maxContent: mc }))).events;
      const expected = text.length > mc ? `${text.slice(0, mc)}${CONTENT_TRUNCATION_NOTE}` : text;
      expect(e!.content).toBe(expected);
    }
  });

  test("lightweight-deleted rows (pipeline erasure) are gone at once", async () => {
    await insert([row({ session: "del", id: "del-1", ts: "2026-10-08 10:00:00.000", content: "erase needle" })]);
    const deps = { ch, liveness: memoryLiveness().liveness };
    expect((await readEventPage(deps, q({ sessionId: "del" }))).events).toHaveLength(1);
    // The lightweight DELETE is a mutation, which needs merges running.
    await ch.command("SYSTEM START MERGES transcript_events");
    await ch.command("DELETE FROM transcript_events WHERE org_id = 'org-a' AND workspace_id = 'ws-a' AND source = 'codex' AND session_id = 'del'", { settings: { lightweight_deletes_sync: 2 } });
    await ch.command("SYSTEM STOP MERGES transcript_events");
    expect((await readEventPage(deps, q({ sessionId: "del" }))).events).toEqual([]);
    expect((await readEventPage(deps, q({ q: "erase needle", order: "desc" }))).events).toEqual([]);
  }, 60_000);

  test("a session tombstoned in Supabase is dropped from search before the pipeline deletes it", async () => {
    await insert([
      row({ session: "gone", id: "g1", ts: "2026-10-09 10:00:00.000", content: "tomb needle" }),
      row({ session: "kept", id: "k1", ts: "2026-10-09 10:00:01.000", content: "tomb needle" }),
    ]);
    const live = memoryLiveness({ erased: [{ source: "codex", sessionId: "gone" }] });
    const page = await readEventPage({ ch, liveness: live.liveness }, q({ q: "tomb needle", order: "desc" }));
    expect(page.events.map((e) => e.eventId)).toEqual(["k1"]);
    expect(page.erasedDropped).toBe(1);
  });

  test("search filters, case-insensitive literal q (with % and _), title match and source", async () => {
    await insert([
      row({ session: "f1", id: "f-1", ts: "2026-10-10 10:00:00.000", content: "100%_DONE literally" }),
      row({ session: "f2", id: "f-2", ts: "2026-10-10 10:00:01.000", content: "nothing", title: "Fix 100%_done bug" }),
      row({ session: "f3", source: "claude", id: "f-3", ts: "2026-10-10 10:00:02.000", content: "100%_done again" }),
      row({ session: "f4", id: "f-4", ts: "2026-10-10 10:00:03.000", content: "100 done" }),
    ]);
    const deps = { ch, liveness: memoryLiveness().liveness };
    expect((await readEventPage(deps, q({ q: "100%_done", order: "desc" }))).events.map((e) => e.eventId)).toEqual(["f-3", "f-2", "f-1"]);
    expect((await readEventPage(deps, q({ q: "100%_done", order: "desc", source: "codex" }))).events.map((e) => e.eventId)).toEqual(["f-2", "f-1"]);
    // Short queries scan without the text index and still match.
    expect((await readEventPage(deps, q({ q: "%_", order: "desc", source: "codex" }))).events.map((e) => e.eventId)).toEqual(["f-2", "f-1"]);
    // Search pagination over newest-first.
    expect(await all(deps, q({ q: "100%_done", order: "desc", limit: 1 }))).toEqual(["f-3", "f-2", "f-1"]);
  });

  test("since/until and kind bound in-session reads; the shape is relayhistory's", async () => {
    await insert([
      row({ session: "w", id: "w1", ts: "2026-10-11 09:00:00.000", kind: "prompt", user: "" }),
      row({ session: "w", id: "w2", ts: "2026-10-11 10:00:00.000" }),
      row({ session: "w", id: "w3", ts: "2026-10-11 11:00:00.000" }),
    ]);
    const deps = { ch, liveness: memoryLiveness().liveness };
    const sinceMs = Date.parse("2026-10-11T10:00:00Z");
    expect((await readEventPage(deps, q({ sessionId: "w", sinceMs }))).events.map((e) => e.eventId)).toEqual(["w2", "w3"]);
    expect((await readEventPage(deps, q({ sessionId: "w", untilMs: sinceMs, order: "desc" }))).events.map((e) => e.eventId)).toEqual(["w2", "w1"]);
    const [prompt] = (await readEventPage(deps, q({ sessionId: "w", kind: "prompt" }))).events;
    expect(prompt).toEqual({
      eventId: "w1", userId: null, sessionId: "w", source: "codex", kind: "prompt", type: "assistant",
      ts: "2026-10-11T09:00:00.000Z", actorName: null, actorRole: "assistant", subagentId: null, machineId: "m-1",
      projectId: null, trajectoryId: null, taskRef: {}, taskTitle: null, content: "content of w1", contentTruncated: false,
      tags: [], toolName: null, toolStatus: null, filesTouched: [],
    });
  });
  test("an event matching both the content and the title branch is one candidate, not two", async () => {
    await insert([
      row({ session: "both", id: "b-1", ts: "2026-10-12 10:00:02.000", content: "dual needle", title: "dual needle title" }),
      row({ session: "both", id: "b-2", ts: "2026-10-12 10:00:01.000", content: "dual needle" }),
      row({ session: "both", id: "b-3", ts: "2026-10-12 10:00:00.000", content: "dual needle" }),
    ]);
    const deps = { ch, liveness: memoryLiveness().liveness };
    // limit 2: a duplicated b-1 would fill the page with one event.
    const first = await readEventPage(deps, q({ q: "dual needle", order: "desc", limit: 2 }));
    expect(first.events.map((e) => e.eventId)).toEqual(["b-1", "b-2"]);
    expect(await all(deps, q({ q: "dual needle", order: "desc", limit: 2 }))).toEqual(["b-1", "b-2", "b-3"]);
  });

  test("partial sessions follow the pipeline's latest state row (needs_full), per org", async () => {
    await ch.command("SYSTEM STOP MERGES search_ingest_session_state");
    const scope = (sessionId: string, orgId = "org-a") => ({ orgId, workspaceId: "ws-a", harness: "codex", sessionId });
    // The pipeline's own writer (search-index.ts recordState): a partial S3 pass writes needs_full = 1 only.
    await recordState(ch, scope("p-partial"), 3, 0, false, new Date("2026-10-13T10:00:00Z"));
    // Partial, then a complete pass: current again.
    await recordState(ch, scope("p-healed"), 1, 0, false, new Date("2026-10-13T10:00:00Z"));
    await recordState(ch, scope("p-healed"), 2, 10, true, new Date("2026-10-13T11:00:00Z"));
    // Complete, then a partial pass: no longer current.
    await recordState(ch, scope("p-regressed"), 1, 10, true, new Date("2026-10-13T10:00:00Z"));
    await recordState(ch, scope("p-regressed"), 2, 0, false, new Date("2026-10-13T11:00:00Z"));
    await recordState(ch, scope("p-other-org", "org-b"), 1, 0, false, new Date("2026-10-13T10:00:00Z"));
    const deps = { ch, liveness: memoryLiveness().liveness };
    expect((await partialSessions(deps, "org-a")).sort()).toEqual(["p-partial", "p-regressed"]);
    expect(await partialSessions(deps, "org-a", "p-partial")).toEqual(["p-partial"]);
    expect(await partialSessions(deps, "org-a", "p-healed")).toEqual([]);
    expect(await partialSessions(deps, "org-a", "p-other-org")).toEqual([]);
    expect(await partialSessions(deps, "org-c")).toEqual([]);
  });

  test("a needs_full session's stale rows are never served as its transcript; a healed one is", async () => {
    await insert([
      row({ session: "nf", id: "nf-1", ts: "2026-10-14 10:00:00.000", content: "stale part" }),
      row({ session: "nf", id: "nf-2", ts: "2026-10-14 10:00:01.000", content: "stale part" }),
    ]);
    const createdAt = { "codex\u0000nf": Date.parse("2026-10-14T09:00:00Z") };
    const orgA = createReadApi({
      verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }),
      ch,
      liveness: memoryLiveness({ createdAt }).liveness,
      completeSinceMs: 0,
    });
    const getA = async (path: string) => orgA.request(path, { headers: { authorization: "Bearer t" } });
    const served = await getA("/v1/sessions/nf/events?workspace=ws-a");
    expect(served.status).toBe(200);
    expect(((await served.json()) as { events: unknown[] }).events).toHaveLength(2);
    await recordState(ch, { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "nf" }, 9, 0, false, new Date("2026-10-14T12:00:00Z"));
    for (const path of ["/v1/sessions/nf/events?workspace=ws-a", "/v1/events?workspace=ws-a&session=nf", "/v1/events?workspace=ws-a&q=stale%20part&limit=20"]) {
      const r = await getA(path);
      expect(r.status).toBe(501);
      expect(((await r.json()) as { error: { reason: string } }).error.reason).toBe("session_partial");
    }
    await recordState(ch, { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "nf" }, 10, 2, true, new Date("2026-10-14T13:00:00Z"));
  });

  test("a search is stopped only by partial sessions of its own workspace, attributed by row or live pointer", async () => {
    await ch.command("SYSTEM STOP MERGES search_ingest_session_state");
    const org = "org-w";
    const scope = (sessionId: string, workspaceId = "ws-a") => ({ orgId: org, workspaceId, harness: "codex", sessionId });
    const t = (h: number) => new Date(`2026-10-20T${String(h).padStart(2, "0")}:00:00Z`);
    await insert([
      row({ org, session: "w-ok", id: "ok-1", ts: "2026-10-20 10:00:00.000", content: "scoped needle ok" }),
      row({ org, ws: "ws-b", session: "w-other", id: "other-1", ts: "2026-10-20 10:00:01.000", content: "scoped needle other" }),
      row({ org, session: "w-here", id: "here-1", ts: "2026-10-20 10:00:02.000", content: "scoped needle here" }),
    ]);
    await recordState(ch, scope("w-ok"), 1, 1, true, t(1));
    // Partial in ws-b (its rows are there), and partial with no row and no pointer in ws-a.
    await recordState(ch, scope("w-other", "ws-b"), 2, 0, false, t(2));
    await recordState(ch, scope("w-gone", "ws-b"), 1, 0, false, t(2));
    const apiFor = (workspaceId: string, liveness: ReturnType<typeof memoryLiveness>["liveness"]) => {
      const app = createReadApi({ verify: async () => ({ userId: "u-1", orgId: org, workspaceId, scopes: ["rth:read"] }), ch, liveness, completeSinceMs: 0 });
      return async () => app.request(`/v1/events?workspace=${workspaceId}&q=scoped%20needle&limit=20`, { headers: { authorization: "Bearer t" } });
    };
    const ids = async (r: Response) => ((await r.json()) as { events: { eventId: string }[] }).events.map((e) => e.eventId);
    const none = memoryLiveness();

    // ws-a: the org has partial sessions, none of them ws-a's. Served, complete.
    expect(await partialSessionsInWorkspace({ ch, liveness: none.liveness }, org, "ws-a")).toEqual({ sessions: [], overflow: false });
    let r = await apiFor("ws-a", none.liveness)();
    expect(r.status).toBe(200);
    expect(await ids(r)).toEqual(["here-1", "ok-1"]);
    // ws-b: w-other's rows are there, so its search falls back.
    r = await apiFor("ws-b", memoryLiveness().liveness)();
    expect(r.status).toBe(501);
    expect(((await r.json()) as { error: { reason: string } }).error.reason).toBe("session_partial");

    // A ws-a session whose every pass was partial has no row at all: its live pointer here attributes it.
    await recordState(ch, scope("w-never"), 1, 0, false, t(3));
    const pointed = memoryLiveness({ createdAt: { "codex\u0000w-never": t(0).getTime() } });
    expect(await partialSessionsInWorkspace({ ch, liveness: pointed.liveness }, org, "ws-a")).toEqual({ sessions: ["w-never"], overflow: false });
    expect(pointed.calls.livePointerSessions).toEqual([["w-gone", "w-never", "w-other"]]);
    expect((await apiFor("ws-a", pointed.liveness)()).status).toBe(501);
    // Erased here (every pointer tombstoned): nothing of it is served, nothing is owed.
    const erasedHere = memoryLiveness({ createdAt: { "codex\u0000w-never": t(0).getTime() }, erased: [{ source: "codex", sessionId: "w-never" }] });
    expect((await apiFor("ws-a", erasedHere.liveness)()).status).toBe(200);
    await recordState(ch, scope("w-never"), 2, 1, true, t(4));

    // w-here indexed earlier, then a partial pass: its row in ws-a decides, Supabase is not asked.
    await recordState(ch, scope("w-here"), 3, 0, false, t(5));
    const asked = memoryLiveness();
    expect(await partialSessionsInWorkspace({ ch, liveness: asked.liveness }, org, "ws-a")).toEqual({ sessions: ["w-here"], overflow: false });
    expect(asked.calls.livePointerSessions).toEqual([]);
    r = await apiFor("ws-a", asked.liveness)();
    expect(r.status).toBe(501);
    expect(((await r.json()) as { error: { reason: string } }).error.reason).toBe("session_partial");
    // Healed: served again.
    await recordState(ch, scope("w-here"), 4, 1, true, t(6));
    r = await apiFor("ws-a", none.liveness)();
    expect(r.status).toBe(200);
    expect(await ids(r)).toEqual(["here-1", "ok-1"]);
  });

  test("the same session id in two workspaces is partial for both", async () => {
    const org = "org-dup";
    await insert([
      row({ org, ws: "ws-a", session: "dup", id: "d-a", ts: "2026-10-21 10:00:00.000" }),
      row({ org, ws: "ws-b", session: "dup", id: "d-b", ts: "2026-10-21 10:00:00.000" }),
    ]);
    // The state table is keyed (org, session): a partial pass in ws-b marks the id partial for ws-a too.
    await recordState(ch, { orgId: org, workspaceId: "ws-b", harness: "codex", sessionId: "dup" }, 1, 0, false, new Date("2026-10-21T11:00:00Z"));
    const deps = { ch, liveness: memoryLiveness().liveness };
    expect(await partialSessionsInWorkspace(deps, org, "ws-a")).toEqual({ sessions: ["dup"], overflow: false });
    expect(await partialSessionsInWorkspace(deps, org, "ws-b")).toEqual({ sessions: ["dup"], overflow: false });
    expect(await partialSessionsInWorkspace(deps, org, "ws-c")).toEqual({ sessions: [], overflow: false });
  });
});
