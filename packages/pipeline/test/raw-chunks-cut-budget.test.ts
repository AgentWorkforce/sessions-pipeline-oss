import { describe, expect, it, vi } from "vitest";

// Every context check fails (scrubbing "together" never equals "apart"), so only
// the per-search budget bounds the work before the text is refused.
let calls = 0;
vi.mock("../src/lib/scrub.js", async (orig) => {
  const real = (await orig()) as Record<string, unknown>;
  return {
    ...real,
    scrubText: (s: string) => {
      calls += 1;
      return `${(real.scrubText as (x: string) => string)(s)}#`;
    },
  };
});

describe("segment search budget", () => {
  it("refuses after a bounded number of full checks", async () => {
    const { scrubLong } = await import("../src/lib/pipeline/raw-chunks.js");
    const { MAX_SCRUB_CHARS } = await import("../src/lib/scrub.js");
    const text = "lorem ipsum dolor sit amet ".repeat(Math.ceil((MAX_SCRUB_CHARS + 5000) / 27));
    calls = 0;
    expect(() => scrubLong(text)).toThrow(/scrub limit/);
    // 48 candidates x 3 scrubText calls; without the budget it would be ~2,400 candidates.
    expect(calls).toBeLessThanOrEqual(48 * 3);
  });
});
