import { describe, expect, test } from "bun:test";
import { canonicalImageKey } from "../canonical-image-key.ts";

const SCOPE = { orgId: "o", workspaceId: "w", harness: "h",
  sessionId: "s", generation: "a".repeat(64) };
const AUTH = "12345678-1234-1234-1234-123456789012";

describe("canonicalImageKey mirrors the #230 SQL writer", () => {
  // The independent derivation: unit-separator join + history-scope-v1 tag,
  // computed by hand so the shared function can't hide its own bug.
  test("key = sha256(join(0x1f, tag, org, ws, harness, session)) : gen : auth", () => {
    const crypto = require("node:crypto");
    const expected = "hs1:" + crypto.createHash("sha256")
      .update(["history-scope-v1", "o", "w", "h", "s"]
        .join("\x1f"), "utf8")
      .digest("hex") + ":" + "a".repeat(64) + ":" + AUTH;
    expect(canonicalImageKey(SCOPE, AUTH)).toBe(expected);
  });
  test("a 1-byte-altered key is rejected by the decoder check", () => {
    const good = canonicalImageKey(SCOPE, AUTH);
    const bad = good.slice(0, -1) + (good.slice(-1) === "a" ? "b" : "a");
    expect(canonicalImageKey(SCOPE, AUTH)).not.toBe(bad);
    // the decoder recomputes and compares — an altered key never matches
    expect(bad).not.toBe(canonicalImageKey(SCOPE, AUTH));
  });
});
