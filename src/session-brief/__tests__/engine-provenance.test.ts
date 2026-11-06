/**
 * The vendored brief engine is exactly what SOURCE.json says it is: each file's hash, and
 * relayhistory's summary prompt hash (its own provenance.test.ts pins the same value).
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SUMMARY_INSTRUCTIONS } from "../engine/summary-prompt.ts";
import { SESSION_SUMMARY_PROMPT_VERSION } from "../engine/summary-contract.ts";
import { SESSION_BRIEF_ENGINE_VERSION, SESSION_BRIEF_PROMPT_VERSION } from "../versions.ts";

const dir = join(import.meta.dir, "../engine");
const source = JSON.parse(readFileSync(join(dir, "SOURCE.json"), "utf8")) as {
  summary_prompt_sha256: string;
  files: Record<string, { sha256: string; upstream_sha256: string; adaptation: string }>;
};
const sha = (b: string | Buffer) => createHash("sha256").update(b).digest("hex");

describe("vendored relayhistory brief engine", () => {
  test("every vendored file matches its recorded hash; verbatim files match upstream", () => {
    for (const [file, entry] of Object.entries(source.files)) {
      const local = sha(readFileSync(join(dir, file)));
      expect(local, file).toBe(entry.sha256);
      if (entry.adaptation === "verbatim") expect(local, file).toBe(entry.upstream_sha256);
    }
  });

  test("the summary prompt is relayhistory's production prompt, unchanged", () => {
    expect(sha(SUMMARY_INSTRUCTIONS)).toBe(source.summary_prompt_sha256);
    expect(SESSION_SUMMARY_PROMPT_VERSION).toBe("session-summary-v3");
    expect(SESSION_BRIEF_PROMPT_VERSION).toBe("session-summary-v3");
    expect(SESSION_BRIEF_ENGINE_VERSION).toBe("session-summary-v3:96bulls-v1");
  });
});
