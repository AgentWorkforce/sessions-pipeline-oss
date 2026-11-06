import { describe, expect, test } from "bun:test";
import { writeFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { backfillFromIndex } from "../canonical-backfill.ts";
import { manifestIndexImages } from "../canonical-backfill-index.ts";
import type {
  CanonicalBackfillAppendPort,
  CanonicalBackfillImage,
} from "../canonical-backfill.ts";

const KEY = (org: string, ws: string, ses: string, date: string) =>
  `96bulls/${org}/v1/ws=${ws}/m=m/h=chatgpt/d=${date}/s=${ses}/manifest.json`;
const fakeJournal = (seed: string | null): CanonicalBackfillAppendPort & {
  images: CanonicalBackfillImage[];
} => {
  let revision = seed === null ? null : BigInt(seed);
  const seen = new Map<string, string>();
  const images: CanonicalBackfillImage[] = [];
  return {
    images,
    highWater: async () => revision?.toString() ?? null,
    publishOnce: async (image) => {
      const identity = `${image.imageKey}:${image.sourceHash}:${image.pointerSetHash}`;
      const prior = seen.get(identity);
      if (prior) return { revision: prior, inserted: false };
      if (revision === null) throw new Error("unseeded");
      revision += 1n;
      const allocated = revision.toString();
      seen.set(identity, allocated);
      images.push(image);
      return { revision: allocated, inserted: true };
    },
  };
};
const descriptor = async (key: string) => ({
  imageKey: key, sourceHash: "a".repeat(64), schemaGeneration: "g1",
  epoch: "1", fingerprint: "f".repeat(64),
  pointerSetHash: "p".repeat(64), pointerSetCount: 1,
});
describe("canonical backfill driver", () => {
  test("JSONL index → publish journal entries per image key", async () => {
    const dir = mkdtempSync(join(tmpdir(), "bf-"));
    const path = join(dir, "idx.jsonl");
    const lines = [
      { org_id: "o1", session_id: "s-1", revision: 1, error: false,
        keys: [KEY("o1", "w1", "s-1", "2026/10/27")] },
      { org_id: "o1", session_id: "s-2", revision: 1, error: false,
        keys: [KEY("o1", "w1", "s-2", "2026/10/28"),
              KEY("o1", "w1", "s-2b", "2026/10/28")] },
      { org_id: "o1", session_id: "s-err", revision: 1, error: true,
        keys: [KEY("o1", "w1", "s-err", "2026/10/28")] },
    ];
    writeFileSync(path, lines.map((l) => JSON.stringify(l)).join("\n"));
    const journal = fakeJournal("100");
    const out = await backfillFromIndex(journal,
      manifestIndexImages(path, descriptor), "100");
    expect(out.kind).toBe("copied");
    expect(out.entries).toBe(3); // 2 keys in s-2, errored skipped
    expect(out.lastRevision).toBe("103");
    expect(journal.images).toHaveLength(3);
    expect(journal.images.every((image) => image.orgId === "o1")).toBe(true);
  });
  test("an unseeded data-plane backfill fails closed", async () => {
    const dir = mkdtempSync(join(tmpdir(), "bf-"));
    const path = join(dir, "idx.jsonl");
    writeFileSync(path, "");
    const journal = fakeJournal(null);
    expect(await backfillFromIndex(journal,
      manifestIndexImages(path, descriptor), "100"))
      .toEqual({ kind: "refused", reason: "canonical_journal_unseeded" });
    expect(journal.images).toHaveLength(0);
  });
  test("replayed images are idempotent at the owner append boundary", async () => {
    const dir = mkdtempSync(join(tmpdir(), "bf-"));
    const path = join(dir, "idx.jsonl");
    writeFileSync(path, JSON.stringify({ org_id: "o1", session_id: "s-1",
      revision: 1, error: false,
      keys: [KEY("o1", "w1", "s-1", "2026/10/27")] }));
    const journal = fakeJournal("100");
    const first = await backfillFromIndex(journal,
      manifestIndexImages(path, descriptor), "100");
    const replay = await backfillFromIndex(journal,
      manifestIndexImages(path, descriptor), "100");
    expect(first.entries).toBe(1);
    expect(replay.entries).toBe(0);
    expect(replay.lastRevision).toBe(first.lastRevision);
    expect(journal.images).toHaveLength(1);
  });
});
