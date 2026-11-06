/**
 * The ingest's entry must bundle for workerd: every import resolves, and the
 * only externals are Node built-ins `nodejs_compat` provides plus
 * `cloudflare:sockets` (pg's socket, through pg-cloudflare, for Hyperdrive).
 */
import { describe, expect, test } from "bun:test";
import { builtinModules } from "node:module";
import { join } from "node:path";

const ROOT = join(import.meta.dir, "../../..");
const NODE = new Set(builtinModules.flatMap((m) => [m, `node:${m}`]));

describe("session ingest Worker bundle", () => {
  test("bundles for workerd with only nodejs_compat built-ins and cloudflare:sockets external", async () => {
    const externals = new Set<string>();
    const result = await Bun.build({
      entrypoints: [join(ROOT, "src/session-ingest/worker.ts")],
      target: "browser",
      format: "esm",
      conditions: ["workerd", "worker", "browser"],
      tsconfig: join(ROOT, "tsconfig.json"),
      throw: false,
      plugins: [
        {
          name: "externals",
          setup(b) {
            b.onResolve({ filter: /.*/ }, (args) => {
              if (NODE.has(args.path) || args.path.startsWith("cloudflare:")) {
                externals.add(args.path);
                return { path: args.path, external: true };
              }
              return undefined;
            });
          },
        },
      ],
    });
    expect(result.logs.filter((l) => l.level === "error").map((l) => l.message)).toEqual([]);
    expect(result.success).toBe(true);
    for (const e of externals) expect(NODE.has(e) || e === "cloudflare:sockets").toBe(true);
    expect(externals.has("cloudflare:sockets")).toBe(true);
    const code = await result.outputs[0]!.text();
    // No AWS SDK, no Lambda runtime, no filesystem store in the request path.
    expect(code).not.toMatch(/@aws-sdk|aws-lambda|createFsStore/);
    // pg reaches Hyperdrive through pg-cloudflare's socket (pg's optional dependency).
    expect(code).toContain("class CloudflareSocket");
  }, 60_000);
});
