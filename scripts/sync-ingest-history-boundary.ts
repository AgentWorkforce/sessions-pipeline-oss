/**
 * Thin wrapper kept for existing docs/CI: the 620 ingest-history lane is now one manifest
 * (scripts/sessions-function-boundary/lanes/ingest-history.ts) of the unified generator, which
 * regenerates every lane's shared artifacts, not just this one.
 */
import { BoundaryError } from "./sessions-function-boundary/generate.ts";
import { ingestHistory } from "./sessions-function-boundary/lanes/ingest-history.ts";
import { run } from "./sync-sessions-function-boundary.ts";

const check = process.argv.includes("--check");
try {
  const { drift, generated } = await run({ check });
  if (check && drift.length) {
    for (const p of drift) console.error("history boundary drift: " + p);
    process.exitCode = 1;
  } else {
    const lane = generated.summary.lanes["ingest-history"]!;
    const granted = Object.values(ingestHistory.functions).filter((f) => f.grants.length).length;
    console.log(JSON.stringify({ functions: lane.functions + lane.replaces, publicationRPCs: granted, boundarySHA: generated.summary.boundarySHA, check }));
  }
} catch (e) {
  if (!(e instanceof BoundaryError)) throw e;
  console.error("history boundary: " + e.message);
  process.exitCode = 1;
}
