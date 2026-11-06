/**
 * The ONE sessions function-boundary generator. Composes every lane manifest
 * (scripts/sessions-function-boundary/lanes/*) into src/hoopdb/sessions-function-acl.json, the Supabase
 * boundary wrapper, migrate-guards.ts, the role verifier SQL and each lane's extra outputs.
 *   bun scripts/sync-sessions-function-boundary.ts          write
 *   bun scripts/sync-sessions-function-boundary.ts --check  exit 1 on any drift; writes nothing
 *   --root=<dir> runs against another checkout (tests).
 * A manifest that disagrees with its SQL, or two lanes claiming one function, always exits 1.
 */
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { BoundaryError, generate, type Generated } from "./sessions-function-boundary/generate.ts";
import { LANES } from "./sessions-function-boundary/lanes/index.ts";
import type { LaneManifest, VerifierTarget } from "./sessions-function-boundary/types.ts";
import { VERIFIERS } from "./sessions-function-boundary/verifiers.ts";

export interface RunOptions {
  root?: string;
  check?: boolean;
  lanes?: readonly LaneManifest[];
  verifiers?: readonly VerifierTarget[];
}
export interface RunResult { drift: string[]; generated: Generated }

export async function run(opts: RunOptions = {}): Promise<RunResult> {
  const root = opts.root ?? fileURLToPath(new URL("..", import.meta.url));
  const read = async (p: string) => {
    try { return await readFile(join(root, p), "utf8"); }
    catch (e) { if ((e as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw e; }
  };
  const generated = await generate(read, opts.lanes ?? LANES, opts.verifiers ?? VERIFIERS);
  const drift: string[] = [];
  for (const [path, text] of Object.entries(generated.outputs).sort(([a], [b]) => (a < b ? -1 : 1))) {
    if ((await read(path)) === text) continue;
    drift.push(path);
    if (!opts.check) await writeFile(join(root, path), text);
  }
  return { drift, generated };
}

if (import.meta.main) {
  const check = process.argv.includes("--check");
  const root = process.argv.find((a) => a.startsWith("--root="))?.slice("--root=".length);
  try {
    const { drift, generated } = await run({ check, root });
    if (check && drift.length) {
      for (const p of drift) console.error("sessions function boundary drift: " + p);
      process.exitCode = 1;
    } else console.log(JSON.stringify({ ...generated.summary, check, written: check ? [] : drift }));
  } catch (e) {
    if (!(e instanceof BoundaryError)) throw e;
    console.error("sessions function boundary: " + e.message);
    process.exitCode = 1;
  }
}
