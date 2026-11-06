/**
 * oversize.ts: before sealing, a string the pipeline segmenter (`scrubLong`,
 * main's rule set 2) cannot cut becomes one redaction marker, so a delivery
 * batch always lands and nothing unscrubbed is stored. A string it can cut is
 * left for it. The pipeline itself still refuses for every other writer.
 */
import { describe, expect, test } from "bun:test";
import { MAX_SCRUB_CHARS } from "@rhc/lib/scrub.ts";
import { scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";
import { fitForScrub, oversizeRedaction } from "../oversize.ts";
import { liveLine } from "../writer.ts";

const KEY = "AKIAABCDEFGHIJKLMNOP";
const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const base = { schema_version: 1 as const, origin_id: "o", record_id: "r1", revision_id: "r1:1", revision: 1, kind: "tool_call" as const, source: "claude", session_id: "s", operation: "upsert" as const, digest: "d" };
// Canonical JSON evidence has no whitespace: no safe cut for scrubLong.
const unbroken = JSON.stringify({ items: Array.from({ length: 60_000 }, (_, i) => `v${i}`) });

describe("fitForScrub", () => {
  test("the pipeline alone still refuses a string with no safe cut (every other writer)", () => {
    expect(() => scrubRecords(enc(liveLine({ ...base, payload: { input_json: unbroken } }, "t") + "\n"))).toThrow(/scrub limit/);
  });

  test("redacts only that string; the record, its other fields and their scrubbing are kept", () => {
    const fitted = fitForScrub({ ...base, payload: { input_json: unbroken, note: `k ${KEY}`, n: 3 } });
    const out = JSON.parse(dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed));
    expect(out.record_id).toBe("r1");
    expect(out.payload.input_json).toBe(oversizeRedaction(unbroken.length));
    expect(out.payload.n).toBe(3);
    expect(out.payload.note).not.toContain(KEY);
  });

  test("a long text scrubLong can cut is left whole for it, and scrubbed", () => {
    const text = Array.from({ length: 60_000 }, (_, i) => (i % 997 === 0 ? KEY : `w${i}`)).join(" ");
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const fitted = fitForScrub({ ...base, payload: { text } });
    expect(fitted.payload!.text).toBe(text);
    const out = JSON.parse(dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed)).payload.text as string;
    expect(out).not.toContain(KEY);
    expect(out.split(" ").length).toBe(text.split(" ").length);
  });

  test("a long string whose only whitespace is newlines (base64, CSV) is redacted, not refused (re-review High)", () => {
    // Decoded, the newlines are safe cuts; JSON-escaped (what the scrubber also checks), they are not.
    const wrapped = Array.from({ length: 5_000 }, () => "QUJDRA".repeat(13)).join("\n");
    expect(wrapped.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const line = (payload: Record<string, unknown>) => enc(liveLine({ ...base, payload }, "t") + "\n");
    expect(() => scrubRecords(line({ blob: wrapped }))).toThrow(/scrub limit/);
    const fitted = fitForScrub({ ...base, payload: { blob: wrapped } });
    const out = JSON.parse(dec(scrubRecords(line(fitted.payload!)).scrubbed));
    expect(out.payload.blob).toBe(oversizeRedaction(wrapped.length));
  });
});
