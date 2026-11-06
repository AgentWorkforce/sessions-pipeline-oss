import { createHash } from "node:crypto";
import { describe, expect, test } from "bun:test";
import { copyCanonicalEntry } from "../canonical-cycle.ts";
import { canonicalImageKey } from "../canonical-image-key.ts";
import type {
  CanonicalJournalEntry,
  CanonicalPageAckPort,
} from "../canonical-version.ts";
import { featureVersion } from "../source.ts";

const HASH = "a".repeat(64), GEN = "e".repeat(64);
const UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "org", workspaceId: "ws", harness: "h", sessionId: "s-1",
  generation: GEN };
const AUTH = { authorityId: UID, epoch: "7", fingerprint: HASH };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
const IMAGE = canonicalImageKey(SCOPE, UID);
const REV = "1000001";
const stamp = "2026-10-30T10:00:00.000000Z";

const publish = (rev = REV): CanonicalJournalEntry => ({
  kind: "publish", revision: rev, imageKey: IMAGE,
  schemaGeneration: GEN, sourceHash: HASH, scope: SCOPE,
  sourceAuthority: AUTH, pointerSet: PTR,
});
const retire = (rev = "1000002"): CanonicalJournalEntry => ({
  kind: "retire", revision: rev, imageKey: IMAGE,
  schemaGeneration: GEN, sourceHash: HASH, priorKeysetHash: PTR.hash, scope: SCOPE,
  priorSourceAuthority: AUTH, pointerSet: PTR,
});
const eventRow = {
  orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
  machineId: "m-1", kind: "session_event", eventId: "e-1", ts: stamp,
  ingestedAt: stamp, userId: "u", actorName: null, actorRole: "assistant",
  projectId: null, projectFolded: null, taskTitle: "T", taskTitleFolded: "t",
  content: "c", contentFolded: "c", taskRefText: "{}", taskRefTextFolded: "{}",
  tags: [], embedding: null, embeddingModel: null, contentHash: null,
};
const briefRow = {
  orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
  title: "T", description: "D", titleFolded: "t", descriptionFolded: "d",
  briefCompletedAt: stamp, embedding: null, embeddingModel: null,
  contentHash: "h",
};
const hashRows = (rows: unknown[]) =>
  createHash("sha256").update(JSON.stringify(rows)).digest("hex");
const page = (rows: unknown[], cursor: string | null = null) => ({
  rows, cursor, source: IMAGE, pointerSetHash: PTR.hash,
  pageSha256: hashRows(rows),
});
const images = (over: Partial<Record<string, unknown>> = {}) => ({
  imagePages: async (_e: CanonicalJournalEntry, kind: "event" | "brief", c: string | null) =>
    c === null ? page(kind === "event" ? [eventRow] : [briefRow], "done")
               : page([], null),
  priorKeyPages: async (_e: CanonicalJournalEntry, kind: "event" | "brief", c: string | null) =>
    c === null ? page(kind === "event" ? [{
        orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
        machineId: "m-1", kind: "session_event", eventId: "e-old", ts: stamp }] : [{
        orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" }], "done")
               : page([], null),
  imageDescriptor: async (e: CanonicalJournalEntry) => ({
    imageKey: e.imageKey, sourceHash: HASH,
    pointerSetHash: e.pointerSet.hash, complete: true }),
  ...over,
});
const fakeCH = () => {
  const inserts: Record<string, unknown>[][] = [];
  return { inserts, database: "event_feature_fixture_codex_builtin",
    insert: async (_t: string, rows: Record<string, unknown>[]) => {
      inserts.push(rows);
    }, select: async () => [] };
};
const fakeAcks = () => {
  const acked: string[] = [];
  const finished: unknown[] = [];
  const a: CanonicalPageAckPort = {
    ackPage: async (i) => { acked.push(`${i.kind}:${i.cursor}`); },
    finish: async (i) => { finished.push(i); },
    begin: async () => "started",
    resumeCursor: async () => ({ state: "empty" as const, cursor: null }),
  };
  return { a, acked, finished };
};
describe("canonical copy cycle (paged contract)", () => {
  test("publish drains all pages, acks each, finish CAS", async () => {
    const ch = fakeCH(), { a, acked, finished } = fakeAcks();
    const out = await copyCanonicalEntry(images(), a, ch,
      { checkpoint: "0" }, publish(), "target-a");
    expect(out.kind).toBe("copied");
    expect(out.checkpoint).toBe(REV);
    const rows = ch.inserts.flat();
    expect(rows.every((r) => r.version === featureVersion(REV))).toBe(true);
    expect(acked.length).toBe(4);       // two pages per pool (content + terminal)
    expect(finished.length).toBe(1);
  });
  test("retire pages prior keysets into tombstones", async () => {
    const ch = fakeCH(), { a } = fakeAcks();
    const out = await copyCanonicalEntry(images(), a, ch,
      { checkpoint: "10" }, retire(), "target-a");
    expect(out.kind).toBe("copied");
    const rows = ch.inserts.flat();
    expect(rows.every((r) => r.deleted === 1)).toBe(true);
    expect(rows.some((r) => r.event_id === "e-old")).toBe(true);
  });
  test("stale entry refuses before any write or ack", async () => {
    const ch = fakeCH(), { a, acked } = fakeAcks();
    const out = await copyCanonicalEntry(images(), a, ch,
      { checkpoint: "10" },
      { ...publish(), revision: "10" }, "target-a");
    expect(out).toEqual({ kind: "refused", reason: "stale_revision" });
    expect(ch.inserts.length).toBe(0);
    expect(acked.length).toBe(0);
  });
  test("changed descriptor refuses before any CH write", async () => {
    const ch = fakeCH(), { a, acked } = fakeAcks();
    const out = await copyCanonicalEntry(images({
      imageDescriptor: async () => ({ imageKey: IMAGE, sourceHash: HASH,
        pointerSetHash: "q".repeat(64), complete: true }),
    }), a, ch, { checkpoint: "0" }, publish(), "target-a");
    expect(out).toEqual({ kind: "refused", reason: "image_descriptor_changed" });
    expect(ch.inserts.length).toBe(0);
    expect(acked.length).toBe(0);
  });
  test("changed page identity/hash and invalid cursor refuse", async () => {
    for (const bad of [
      { ...page([eventRow], "done"), source: "other" },
      { ...page([eventRow], "done"), pageSha256: "0".repeat(64) },
      { ...page([eventRow], "done"), cursor: "" },
    ]) {
      const ch = fakeCH(), { a } = fakeAcks();
      const out = await copyCanonicalEntry(images({
        imagePages: async () => bad,
      }), a, ch, { checkpoint: "0" }, publish(), "target-a");
      expect(out).toEqual({ kind: "refused", reason: "image_page_changed" });
      expect(ch.inserts.length).toBe(0);
    }
  });
  test("retire refuses a descriptor from a different prior source", async () => {
    const ch = fakeCH(), { a } = fakeAcks();
    const out = await copyCanonicalEntry(images({
      imageDescriptor: async (e: CanonicalJournalEntry) => ({
        imageKey: e.imageKey, sourceHash: "b".repeat(64),
        pointerSetHash: e.pointerSet.hash, complete: true,
      }),
    }), a, ch, { checkpoint: "0" }, retire(), "target-a");
    expect(out).toEqual({ kind: "refused", reason: "image_descriptor_changed" });
    expect(ch.inserts.length).toBe(0);
  });
});
