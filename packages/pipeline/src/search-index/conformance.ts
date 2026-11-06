/**
 * Shared SearchIndexAdapter conformance suite. Every adapter (OSS postgres;
 * cloud clickhouse/opensearch) runs exactly these cases, so "supports the
 * read API" means the same thing for each backend.
 *
 * Usage in a vitest file:
 *
 *   searchIndexConformance("postgres (pglite)", async () => ({ adapter, dispose }));
 *
 * The factory may return one shared index for the whole suite: every case
 * uses its own random org id, so cases never see each other's rows. Adapters
 * with refresh-based visibility must be configured to wait for refresh.
 * Only synthetic data is used.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  CONTENT_PREFIX_CHARS,
  SearchIndexError,
  comparePositions,
  matchesQuery,
  positionOf,
  type SearchEventDocument,
  type SearchHit,
  type SearchIndexAdapter,
  type SearchQuery,
} from "./contracts.js";
import { planScopeReconcile } from "./reconcile.js";

export interface ConformanceTarget {
  adapter: SearchIndexAdapter;
  dispose?: () => Promise<void>;
}

let seq = 0;
const rand = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}${seq++}`;

export function syntheticDoc(over: Partial<SearchEventDocument> & Pick<SearchEventDocument, "orgId">): SearchEventDocument {
  return {
    workspaceId: "ws-a",
    tsMs: 1_767_225_600_000,
    sessionId: "sess-1",
    source: "claude",
    machineId: "m-1",
    kind: "message",
    eventId: `ev-${rand()}`,
    type: "assistant",
    userId: "u-1",
    actorName: "agent",
    actorRole: "assistant",
    subagentId: "",
    projectId: "proj-1",
    taskTitle: "",
    taskRef: "{}",
    taskRefId: "",
    tags: [],
    filesTouched: [],
    toolName: "",
    toolStatus: "",
    deliveryRecordId: "",
    content: "synthetic content",
    fingerprint: "fp-1",
    version: 1,
    ...over,
  };
}

async function allPages(adapter: SearchIndexAdapter, query: Omit<SearchQuery, "cursor">): Promise<SearchHit[]> {
  const out: SearchHit[] = [];
  let cursor = undefined as SearchQuery["cursor"];
  for (let i = 0; i < 1000; i++) {
    const page = await adapter.query({ ...query, cursor });
    out.push(...page.hits);
    if (!page.next) return out;
    cursor = page.next;
  }
  throw new Error("pagination did not terminate");
}

const key = (h: { eventId: string; tsMs: number }) => `${h.tsMs}:${h.eventId}`;

export function searchIndexConformance(name: string, factory: () => Promise<ConformanceTarget>): void {
  describe(`SearchIndexAdapter conformance: ${name}`, () => {
    let target: ConformanceTarget;
    let a: SearchIndexAdapter;
    beforeAll(async () => {
      target = await factory();
      a = target.adapter;
      await a.ensureSchema();
    }, 120_000);
    afterAll(async () => {
      await target?.dispose?.();
    });

    it("declares capabilities, is healthy after ensureSchema, and ensureSchema is idempotent", async () => {
      const caps = a.capabilities();
      expect(caps.backend).toBe(a.backend);
      expect(caps.filters).toEqual(expect.arrayContaining(["session", "source", "kind", "project", "since", "until"]));
      await a.ensureSchema();
      expect(await a.health()).toEqual({ ok: true, code: "ok" });
    });

    it("isolates tenants by org and workspace", async () => {
      const org = `org-${rand()}`;
      const other = `org-${rand()}`;
      await a.upsert([
        syntheticDoc({ orgId: org, content: "needle shared" }),
        syntheticDoc({ orgId: org, workspaceId: "ws-b", content: "needle shared" }),
        syntheticDoc({ orgId: other, content: "needle shared" }),
      ]);
      const page = await a.query({ tenant: { orgId: org, workspaceId: "ws-a" }, q: "needle", order: "desc", limit: 10 });
      expect(page.hits).toHaveLength(1);
      expect(page.hits[0]!.orgId).toBe(org);
      expect(page.hits[0]!.workspaceId).toBe("ws-a");
    });

    it("matches q as a case-insensitive literal substring of the content prefix or task title", async () => {
      const org = `org-${rand()}`;
      const docs = [
        syntheticDoc({ orgId: org, eventId: "e-upper", content: "The QUICK brown fox" }),
        syntheticDoc({ orgId: org, eventId: "e-title", content: "nothing here", taskTitle: "Quick task" }),
        syntheticDoc({ orgId: org, eventId: "e-spaced", content: "q u i c k" }),
        syntheticDoc({ orgId: org, eventId: "e-late", content: `${"x".repeat(CONTENT_PREFIX_CHARS)}quick` }),
        syntheticDoc({ orgId: org, eventId: "e-edge", content: `${"x".repeat(CONTENT_PREFIX_CHARS - 5)}quick` }),
        syntheticDoc({ orgId: org, eventId: "e-pct", content: "rate 100% done_now" }),
        syntheticDoc({ orgId: org, eventId: "e-wild", content: "rate 1000 doneXnow" }),
        syntheticDoc({ orgId: org, eventId: "e-uni", content: "Ünïcode café résumé" }),
      ];
      await a.upsert(docs);
      const ids = async (q: string) =>
        (await a.query({ tenant: { orgId: org, workspaceId: "ws-a" }, q, order: "asc", limit: 50 })).hits
          .map((h) => h.eventId)
          .sort();
      const oracle = (q: string) => docs.filter((d) => matchesQuery(d, q)).map((d) => d.eventId).sort();
      for (const q of ["quick", "QUICK", "k b", "100%", "done_now", "café", "ab", "x".repeat(3) + "quick"]) {
        expect(await ids(q), `q=${JSON.stringify(q)}`).toEqual(oracle(q));
      }
      expect(await ids("quick")).toEqual(["e-edge", "e-title", "e-upper"]);
      expect(await ids("done_now")).toEqual(["e-pct"]);
    });

    it("applies the session, source, kind, project and inclusive time filters", async () => {
      const org = `org-${rand()}`;
      const t = 1_767_225_600_000;
      await a.upsert([
        syntheticDoc({ orgId: org, eventId: "a", tsMs: t, sessionId: "s1", source: "claude", kind: "message", projectId: "p1" }),
        syntheticDoc({ orgId: org, eventId: "b", tsMs: t + 1, sessionId: "s2", source: "claude", kind: "tool", projectId: "p1" }),
        syntheticDoc({ orgId: org, eventId: "c", tsMs: t + 2, sessionId: "s1", source: "codex", kind: "message", projectId: "p2" }),
        syntheticDoc({ orgId: org, eventId: "d", tsMs: t + 3, sessionId: "s1", source: "claude", kind: "message", projectId: "p2" }),
      ]);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const ids = async (f: Partial<SearchQuery>) =>
        (await a.query({ tenant, order: "asc", limit: 50, ...f })).hits.map((h) => h.eventId);
      expect(await ids({ sessionId: "s1" })).toEqual(["a", "c", "d"]);
      expect(await ids({ source: "codex" })).toEqual(["c"]);
      expect(await ids({ kind: "tool" })).toEqual(["b"]);
      expect(await ids({ projectId: "p2" })).toEqual(["c", "d"]);
      expect(await ids({ sinceMs: t + 1, untilMs: t + 2 })).toEqual(["b", "c"]);
      expect(await ids({ sessionId: "s1", source: "claude", projectId: "p2" })).toEqual(["d"]);
    });

    it("pages in exact (ts, session, source, machine, kind, event) order with no gaps or repeats", async () => {
      const org = `org-${rand()}`;
      const t = 1_767_225_600_000;
      const docs: SearchEventDocument[] = [];
      for (let i = 0; i < 23; i++) {
        docs.push(
          syntheticDoc({
            orgId: org,
            tsMs: t + (i % 4), // many ties on ts
            sessionId: `s${i % 3}`,
            machineId: `m${i % 2}`,
            eventId: `e${String(i).padStart(2, "0")}`,
            content: i % 2 ? "match odd" : "match even",
          }),
        );
      }
      await a.upsert(docs);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const expected = [...docs].sort(comparePositions).map(key);
      for (const limit of [1, 5, 7, 23, 100]) {
        expect((await allPages(a, { tenant, order: "asc", limit })).map(key)).toEqual(expected);
        expect((await allPages(a, { tenant, order: "desc", limit })).map(key)).toEqual([...expected].reverse());
      }
      const odd = await allPages(a, { tenant, q: "odd", order: "desc", limit: 4 });
      expect(odd.map(key)).toEqual(docs.filter((d) => d.content.includes("odd")).sort(comparePositions).reverse().map(key));
    });

    it("matches the reference case folding for Unicode content and titles", async () => {
      const org = `org-${rand()}`;
      const docs = [
        syntheticDoc({ orgId: org, eventId: "unicode-i", content: "i" }),
        syntheticDoc({ orgId: org, eventId: "unicode-dot", content: "İ" }),
        syntheticDoc({ orgId: org, eventId: "unicode-sigma", content: "", taskTitle: "ΟΣ" }),
      ];
      await a.upsert(docs);
      for (const q of ["İ", "i\u0307", "οσ", "ος"]) {
        const page = await a.query({ tenant: { orgId: org, workspaceId: "ws-a" }, order: "asc", limit: 10, q });
        expect(page.hits.map((hit) => hit.eventId).sort()).toEqual(
          docs.filter((doc) => matchesQuery(doc, q)).map((doc) => doc.eventId).sort(),
        );
      }
    });

    it("upserts idempotently and never lets an older version replace a newer row", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const base = syntheticDoc({ orgId: org, eventId: "v", content: "version two", fingerprint: "fp-2", version: 2 });
      await a.upsert([base]);
      await a.upsert([base]);
      await a.upsert([{ ...base, content: "version one", fingerprint: "fp-1", version: 1 }]);
      let hits = (await a.query({ tenant, order: "asc", limit: 10 })).hits;
      expect(hits.map((h) => h.content)).toEqual(["version two"]);
      await a.upsert([{ ...base, content: "version three", fingerprint: "fp-3", version: 3 }]);
      hits = (await a.query({ tenant, order: "asc", limit: 10 })).hits;
      expect(hits.map((h) => h.content)).toEqual(["version three"]);
      const scan = await a.scanFingerprints({ ...tenant, source: base.source, sessionIds: [base.sessionId] }, { fromMs: 0, limit: 10 });
      expect(scan.rows.map((r) => r.fingerprint)).toEqual(["fp-3"]);
    });

    it("returns stored fields intact and truncates content to maxContentChars", async () => {
      const org = `org-${rand()}`;
      const doc = syntheticDoc({
        orgId: org,
        content: "héllo wörld",
        tags: ["t1", "t2"],
        filesTouched: ["a.ts"],
        toolName: "Edit",
        toolStatus: "ok",
        taskTitle: "Title",
        taskRef: '{"id":"T-1"}',
        taskRefId: "T-1",
        deliveryRecordId: "rec-1",
      });
      await a.upsert([doc]);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const full = (await a.query({ tenant, order: "asc", limit: 1 })).hits[0]!;
      const { fingerprint: _f, version: _v, taskRefId: _t, deliveryRecordId: _d, ...expected } = doc;
      expect(full).toEqual(expected);
      const cut = (await a.query({ tenant, order: "asc", limit: 1, maxContentChars: 4 })).hits[0]!;
      expect(cut.content).toBe("héll");
    });

    it("deletes exact rows and erases a whole scope at any ts, leaving other scopes", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const keep = syntheticDoc({ orgId: org, sessionId: "keep", content: "erasable text" });
      const s1 = syntheticDoc({ orgId: org, sessionId: "gone", tsMs: 1, content: "erasable text" });
      const s2 = syntheticDoc({ orgId: org, sessionId: "gone-alias", tsMs: 9_999_999_999_999, content: "erasable text" });
      const one = syntheticDoc({ orgId: org, sessionId: "keep", eventId: "drop-me", content: "erasable text" });
      await a.upsert([keep, s1, s2, one]);
      expect(await a.deleteEvents(tenant, [positionOf(one)])).toEqual({ deleted: 1 });
      await a.eraseScope({ ...tenant, source: "claude", sessionIds: ["gone", "gone-alias"] });
      const left = await allPages(a, { tenant, q: "erasable", order: "asc", limit: 10 });
      expect(left.map((h) => h.sessionId)).toEqual(["keep"]);
      const scan = await a.scanFingerprints({ ...tenant, source: "claude", sessionIds: ["gone", "gone-alias"] }, { fromMs: 0, limit: 10 });
      expect(scan.rows).toEqual([]);
    });

    it("scans fingerprints from a ts bound and flags truncation", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const docs = [0, 1, 2, 3].map((i) => syntheticDoc({ orgId: org, tsMs: 1000 + i, eventId: `f${i}`, fingerprint: `fp${i}` }));
      await a.upsert(docs);
      const scope = { ...tenant, source: "claude", sessionIds: ["sess-1"] };
      const all = await a.scanFingerprints(scope, { fromMs: 1001, limit: 10 });
      expect(all.truncated).toBe(false);
      expect(all.rows.map((r) => [r.eventId, r.fingerprint, r.workspaceId])).toEqual([
        ["f1", "fp1", "ws-a"],
        ["f2", "fp2", "ws-a"],
        ["f3", "fp3", "ws-a"],
      ]);
      const cut = await a.scanFingerprints(scope, { fromMs: 0, limit: 2 });
      expect(cut.truncated).toBe(true);
      expect(cut.rows).toHaveLength(2);
    });

    it("converges a scope through planScopeReconcile (insert, change, move, retract)", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const scope = { ...tenant, source: "claude", sessionIds: ["sess-1"] };
      const v1 = [
        syntheticDoc({ orgId: org, eventId: "r1", tsMs: 10, fingerprint: "a", content: "reconcile one" }),
        syntheticDoc({ orgId: org, eventId: "r2", tsMs: 20, fingerprint: "a", content: "reconcile two" }),
        syntheticDoc({ orgId: org, eventId: "r3", tsMs: 30, fingerprint: "a", content: "reconcile three" }),
      ];
      const apply = async (desired: SearchEventDocument[], complete = true) => {
        const scan = await a.scanFingerprints(scope, { fromMs: 0, limit: 100 });
        const plan = planScopeReconcile(desired, scan.rows, { complete: complete && !scan.truncated });
        if (plan.upserts.length) await a.upsert(plan.upserts);
        if (plan.deletes.length) await a.deleteEvents(tenant, plan.deletes);
        return plan;
      };
      expect((await apply(v1)).upserts).toHaveLength(3);
      expect(await apply(v1)).toEqual({ upserts: [], deletes: [] });
      const v2 = [
        { ...v1[0]!, fingerprint: "b", content: "reconcile one edited", version: 2 },
        { ...v1[1]!, tsMs: 25, version: 2 },
      ];
      // An incomplete read inserts but never deletes.
      expect((await apply(v2, false)).deletes).toEqual([]);
      const plan = await apply(v2);
      expect(plan.deletes.map((d) => [d.eventId, d.tsMs]).sort()).toEqual([
        ["r2", 20],
        ["r3", 30],
      ]);
      const rows = await allPages(a, { tenant, order: "asc", limit: 10 });
      expect(rows.map((r) => [r.eventId, r.tsMs, r.content])).toEqual([
        ["r1", 10, "reconcile one edited"],
        ["r2", 25, "reconcile two"],
      ]);
    });

    it("rejects invalid input before touching the backend", async () => {
      const bad = async (p: Promise<unknown>) => {
        const error = await p.then(() => null, (e: unknown) => e);
        expect(error).toBeInstanceOf(SearchIndexError);
        expect((error as SearchIndexError).code).toBe("invalid_argument");
      };
      const tenant = { orgId: "org-x", workspaceId: "ws-a" };
      await bad(a.query({ tenant: { orgId: "", workspaceId: "w" }, order: "asc", limit: 1 }));
      await bad(a.query({ tenant, order: "asc", limit: 0 }));
      await bad(a.query({ tenant, order: "asc", limit: 1, q: "" }));
      await bad(a.query({ tenant, order: "sideways" as "asc", limit: 1 }));
      await bad(a.upsert([{ ...syntheticDoc({ orgId: "org-x" }), tsMs: 1.5 }]));
      await bad(a.eraseScope({ ...tenant, source: "claude", sessionIds: [] }));
    });
  });
}
