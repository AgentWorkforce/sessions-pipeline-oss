import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";

/**
 * Every `lib/scrub.ts` that has sealed raw chunks, by `raw.scrub.version`.
 * Stored chunks record the version they were scrubbed with and are never
 * rewritten, so a rule change must come with a new version. When this test
 * fails after editing `lib/scrub.ts`: bump `SCRUB_RULESET_VERSION` in
 * `lib/pipeline/raw-chunks.ts` and add the new version and digest here. Never
 * edit an existing entry.
 */
const SCRUB_RULESETS: Record<number, string> = {
  1: "db9d65c7ab230d67488900203bb78ca8bd3ad16949a0a51b5d3d117d0cff49f8",
  // From version 2 the digest also covers the segmenter in raw-chunks.ts
  // (between the `<scrub-segmenter>` markers): texts past MAX_SCRUB_CHARS are
  // scrubbed whole, in segments, where version 1 refused them.
  // Updated before release (K-1, K-2: key-block segmentation fixes). Version 2 had
  // sealed no chunk: it was only ever on the unmerged #108 branch, and images build from main.
  2: "417d545e6b79b1100e206e7a6add66f24819868b2606e9ca2c45c09be9644ee2",
};

const read = (rel: string) =>
  // Line endings depend on the checkout, not the rules.
  readFileSync(fileURLToPath(new URL(rel, import.meta.url).href), "utf8").replace(/\r\n/g, "\n");

/** The segmenter's source: everything between the markers in raw-chunks.ts. */
function segmenterSource(): string {
  const text = read("../src/lib/pipeline/raw-chunks.ts");
  const m = /\/\/ <scrub-segmenter>[\s\S]*?\/\/ <\/scrub-segmenter>/.exec(text);
  if (!m) throw new Error("raw-chunks.ts lost its <scrub-segmenter> markers");
  return m[0];
}

function scrubSourceDigest(version: number) {
  const scrub = read("../src/lib/scrub.ts");
  // Version 1 predates the segmenter: its digest is lib/scrub.ts alone.
  const source = version >= 2 ? `${scrub}\n${segmenterSource()}` : scrub;
  return createHash("sha256").update(source).digest("hex");
}

describe("scrub ruleset version", () => {
  it("is bumped with every change to lib/scrub.ts", () => {
    const latest = Math.max(...Object.keys(SCRUB_RULESETS).map(Number));
    expect(
      SCRUB_RULESET_VERSION,
      "SCRUB_RULESET_VERSION must be the newest entry in SCRUB_RULESETS",
    ).toBe(latest);
    expect(
      scrubSourceDigest(SCRUB_RULESET_VERSION),
      "lib/scrub.ts or the raw-chunks segmenter changed: bump SCRUB_RULESET_VERSION and add its digest to SCRUB_RULESETS",
    ).toBe(SCRUB_RULESETS[SCRUB_RULESET_VERSION]);
  });
});
