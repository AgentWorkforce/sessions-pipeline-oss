import { build } from "esbuild";
import { describe, expect, it } from "vitest";

/**
 * The main entry must run in a Cloudflare Worker: bundle it for a neutral
 * platform with no Node built-ins available. `node:*` imports belong only to
 * the `./fs` entry.
 */
describe("Workers compatibility", () => {
  it("bundles the main entry without Node built-ins", async () => {
    const result = await build({
      entryPoints: [new URL("../src/index.ts", import.meta.url).pathname],
      bundle: true,
      write: false,
      platform: "neutral",
      format: "esm",
      conditions: ["workerd", "worker", "browser"],
      mainFields: ["module", "main"],
      logLevel: "silent",
    });
    expect(result.errors).toEqual([]);
    const code = result.outputFiles[0]!.text;
    expect(code).not.toMatch(/from\s+["']node:/);
    expect(code).not.toMatch(
      /require\(["'](node:)?(fs|path|crypto|stream)["']\)/,
    );
  });
});
