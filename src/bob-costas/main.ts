import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { bobCostasHandler } from "./handler.ts";
import { summarizerFromEnv } from "./summarizer.ts";
import { keyringFromEnv } from "../shared/keyring.ts";
import { summarySweep } from "./sweep.ts";

const raw = lockerStore();
const summarizer = summarizerFromEnv();
// Summaries are stored encrypted in the Locker; refuse to start without a keyring.
const keyring = await keyringFromEnv();
await runWorker({
  service: "bob-costas",
  stages: ["summary-short", "summary-long"],
  attests: ["summaries"],
  // Queues summaries for indexed sessions that have none (e.g. after migration 0004).
  tick: summarySweep({
    keyring,
    everyMs: Number(process.env.SUMMARY_SWEEP_SECONDS ?? 300) * 1000,
  }),
  handlers: (db) => {
    const deps = {
      db,
      reader: lockerManifestReader(),
      store: raw.store,
      summarizer,
      keyring,
    };
    return {
      "summary-short": bobCostasHandler("short", deps),
      "summary-long": bobCostasHandler("long", deps),
    };
  },
});
