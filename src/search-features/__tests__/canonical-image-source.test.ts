import { describe, expect, test } from "bun:test";
import { CanonicalImageSource } from "../canonical-image-source.ts";
import { canonicalImageKey } from "../canonical-image-key.ts";
import type { CanonicalJournalEntry } from "../canonical-version.ts";

const GEN = "a".repeat(64), SOURCE = "b".repeat(64);
const UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: GEN };
const AUTH = { authorityId: UID, epoch: "1", fingerprint: SOURCE };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "2" };
const IMAGE = canonicalImageKey(SCOPE, UID);
const entry = (kind: "publish" | "retire"): CanonicalJournalEntry =>
  kind === "publish"
    ? { kind, revision: "10", imageKey: IMAGE, schemaGeneration: GEN,
        sourceHash: SOURCE, scope: SCOPE,
        sourceAuthority: AUTH, pointerSet: PTR }
    : { kind, revision: "10", imageKey: IMAGE, schemaGeneration: GEN,
        sourceHash: SOURCE, priorKeysetHash: PTR.hash, scope: SCOPE,
        priorSourceAuthority: AUTH, pointerSet: PTR };
const fakeMat = (pages: unknown[][]) => {
  let calls = 0;
  return {
    rows: async (_ik: string, _k: "event" | "brief", _c: string | null, _l: number) => {
      const p = pages[calls] ?? [];
      calls += 1;
      return { rows: p as never, nextCursor: calls < pages.length ? `p${calls}` : null };
    },
    priorKeys: async (_ik: string, _k: "event" | "brief", _c: string | null, _l: number) =>
      ({ rows: pages[0] ?? [], nextCursor: null }),
    descriptor: async (ik: string) => ({ imageKey: ik,
      sourceHash: SOURCE, pointerSetHash: PTR.hash, complete: true }),
  };
};
describe("canonical image source", () => {
  test("pages carry cursor, pageSha256, pointerSetHash per the contract", async () => {
    const src = new CanonicalImageSource(fakeMat([[{ a: 1 }], [{ b: 2 }]]));
    const p1 = await src.imagePages(entry("publish"), "event", null);
    expect(p1.rows).toEqual([{ a: 1 }]);
    expect(p1.cursor).toBe("p1");
    expect(p1.pointerSetHash).toBe(PTR.hash);
    expect(p1.pageSha256).toMatch(/^[0-9a-f]{64}$/);
    const p2 = await src.imagePages(entry("publish"), "event", p1.cursor);
    expect(p2.rows).toEqual([{ b: 2 }]);
    expect(p2.cursor).toBeNull();   // terminal
  });
  test("priorKeyPages uses the priorKeys materializer path", async () => {
    const src = new CanonicalImageSource(fakeMat([[{ k: 1 }]]));
    const p = await src.priorKeyPages(entry("retire"), "event", null);
    expect(p.rows).toEqual([{ k: 1 }]);
    expect(p.cursor).toBeNull();
  });
  test("descriptor delegates to the materializer", async () => {
    const src = new CanonicalImageSource(fakeMat([]));
    const d = await src.imageDescriptor(entry("publish"));
    expect(d?.complete).toBe(true);
    expect(d?.imageKey).toBe(IMAGE);
  });
  test("empty workspace remains a valid canonical identity", async () => {
    const src = new CanonicalImageSource(fakeMat([[{ workspaceId: "" }]]));
    const p = await src.imagePages(entry("publish"), "event", null);
    expect((p.rows[0] as { workspaceId: string }).workspaceId).toBe("");
  });
});
