import { describe, expect, test } from "bun:test";
import { CanonicalPageAckStore } from "../canonical-ack-store.ts";

const fakeDb = () => {
  const page: Record<string, unknown>[] = [];
  const state: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("canonical_feature_page_ack") && text.startsWith("INSERT")) {
      const dup = page.find((p) => p.entry_revision === params[0] &&
        p.pool === params[2] && p.cursor === params[3]);
      if (dup) return { rows: [] as T[] };
      page.push({ entry_revision: params[0], kind: params[1], pool: params[2],
        cursor: params[3], next_cursor: params[4], page_sha256: params[5] });
      return { rows: [{ n: 1 } as T] };
    }
    if (text.includes("canonical_feature_page_ack"))
      return { rows: page.filter((p) => p.entry_revision === params[0] &&
        p.pool === params[1]) as T[] };
    if (text.includes("canonical_feature_copy_state") && text.startsWith("INSERT")) {
      if (state.some((s) => s.entry_revision === params[0]))
        return { rows: [] as T[] };   // conflict -> no rows -> drift check
      state.push({ entry_revision: params[0], kind: params[1],
        image_key: params[2], source_authority: params[3],
        pointer_set: params[4], profile_binding: params[5],
        target_binding: params[6], dirty: true });
      return { rows: [{ n: 1 } as T] };
    }
    // emulate finish_canonical_copy RPC
    if (text.includes("finish_canonical_copy")) {
      const s = state.find((r) => r.entry_revision === params[0]);
      const ok = !!(s && s.dirty && s.source_authority === params[1] &&
          s.profile_binding === params[2] && s.target_binding === params[5]);
      if (ok) s!.dirty = false;
      return { rows: [{ ok } as T] };
    }
    if (text.includes("canonical_feature_copy_state") && text.includes("SELECT")) {
      const s = state.find((r) => r.entry_revision === params[0]);
      return { rows: s
        ? [{ exact: s.source_authority === params[1] &&
             s.pointer_set === params[2] && s.kind === params[3] &&
             s.image_key === params[4] && s.profile_binding === params[5] &&
             s.target_binding === params[6], dirty: s.dirty } as T] : [] };
    }
    return { rows: [] as T[] };
  };
  return { page, state, query };
};
const AUTH = { authorityId: "u", epoch: "1", fingerprint: "a".repeat(64) };
const PTR = { authorityId: "u", hash: "c".repeat(64), count: "1" };
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: "a".repeat(64) };
const pub = (rev: string) => ({ kind: "publish" as const, revision: rev,
  imageKey: "img", schemaGeneration: "g", sourceHash: "a".repeat(64),
  scope: SCOPE, sourceAuthority: AUTH, pointerSet: PTR });
const ret = (rev: string) => ({ kind: "retire" as const, revision: rev,
  imageKey: "img", schemaGeneration: "g", sourceHash: "b".repeat(64),
  priorKeysetHash: "c".repeat(64), scope: SCOPE,
  priorSourceAuthority: AUTH, pointerSet: PTR });
describe("revision-keyed page ack store", () => {
  test("publish and retire on the SAME image key have disjoint progress", async () => {
    const db = fakeDb();
    const s = new CanonicalPageAckStore({ query: db.query });
    const p = pub("10"), r = ret("11");
    await s.begin(p, "pb", "target"); await s.begin(r, "pb", "target");
    await s.ackPage({ entry: p, kind: "event", cursor: "c1",
      pageSha256: "h1".padEnd(64,"1"), nextCursor: "c2" });
    await s.ackPage({ entry: r, kind: "event", cursor: "c1",
      pageSha256: "h2".padEnd(64,"2"), nextCursor: null });
    // same image key, different revisions — the chains do not collide
    expect(db.page.filter((x) => x.entry_revision === "10").length).toBe(1);
    expect(db.page.filter((x) => x.entry_revision === "11").length).toBe(1);
  });
  test("ack replay is idempotent only when the page matches exactly", async () => {
    const db = fakeDb();
    const s = new CanonicalPageAckStore({ query: db.query });
    const p = pub("10");
    await s.begin(p, "pb", "target");
    await s.ackPage({ entry: p, kind: "event", cursor: "c1",
      pageSha256: "h".padEnd(64,"1"), nextCursor: null });
    // identical replay — no-op
    await s.ackPage({ entry: p, kind: "event", cursor: "c1",
      pageSha256: "h".padEnd(64,"1"), nextCursor: null });
    expect(db.page.length).toBe(1);
    // same cursor, different page hash — refuse
    await expect(s.ackPage({ entry: p, kind: "event", cursor: "c1",
      pageSha256: "h".padEnd(64,"9"), nextCursor: null }))
      .rejects.toThrow("ack_mismatch");
  });
  test("a nonterminal ACK resumes at its durable next cursor", async () => {
    const db = fakeDb();
    const s = new CanonicalPageAckStore({ query: db.query });
    const p = pub("10");
    await s.begin(p, "pb", "target");
    await s.ackPage({ entry: p, kind: "event", cursor: "start",
      pageSha256: "1".repeat(64), nextCursor: "page-2" });
    expect(await s.resumeCursor(p, "event")).toEqual({
      state: "resume", cursor: "page-2",
    });
  });
  test("finish CAS: binds authority+profile, refuses drift/second finish", async () => {
    const db = fakeDb();
    const s = new CanonicalPageAckStore({ query: db.query });
    const p = pub("10");
    await s.begin(p, "pb", "target");
    await s.finish({ entry: p, terminalEvent: "te", terminalBrief: "tb",
      sourceAuthority: AUTH, profileBinding: "pb", targetBinding: "target" });
    // second finish — the row is no longer dirty
    await expect(s.finish({ entry: p, terminalEvent: "te", terminalBrief: "tb",
      sourceAuthority: AUTH, profileBinding: "pb", targetBinding: "target" }))
      .rejects.toThrow("finish_cas");
  });
});
