import { describe, expect, test } from "bun:test";
import { ALLOWED_LOG_FIELDS, errorFields } from "../log.ts";
import { spanAttrs } from "../telemetry.ts";

describe("telemetry allowlist", () => {
  test("drops anything that could carry content", () => {
    const attrs = spanAttrs({ stage: "summary-short", prompt: "secret text", summary: "x", path: "/home/u", session_uuid: "abc", count: 3 });
    expect(Object.keys(attrs).sort()).toEqual(["count", "session_uuid", "stage"]);
  });
  test("never allowlists content-shaped fields", () => {
    for (const f of ["prompt", "summary", "message", "body", "path", "manifest", "content", "text", "error_message"]) expect(ALLOWED_LOG_FIELDS.has(f)).toBe(false);
  });
  test("scrubs secret-shaped values at the span exporter boundary", () => {
    const planted = "AKIAABCDEFGHIJKLMNOP";
    const attrs = spanAttrs({ harness: planted, session_uuid: "safe-session-id" });
    expect(attrs.harness).not.toBe(planted);
    expect(JSON.stringify(attrs)).not.toContain(planted);
    expect(attrs.session_uuid).toBe("safe-session-id");
  });
  test("error fields are code-only", () => {
    const f = errorFields(Object.assign(new Error("Authorization: Bearer sk-live-123"), { code: "chunk_empty", retryable: false }));
    expect(f).toEqual({ error_code: "chunk_empty", retryable: false });
    expect(JSON.stringify(f)).not.toContain("sk-live");
  });
});
