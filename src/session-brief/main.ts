/**
 * The session-brief worker (SERVICE=session-brief). Off unless SESSION_BRIEF_ENABLED=true,
 * and it claims nothing until OPENAI_API_KEY is set: deploying it changes nothing on its own.
 * Reads the scrubbed raw from the Locker (read-only) and writes HoopDb only.
 */
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { log } from "../shared/log.ts";
import { briefConfig, modelFetch, modelKey, modelResponsesUrl } from "./config.ts";
import { createAnalysisTransport } from "./engine/transport.ts";
import { briefTick } from "./worker.ts";

const config = briefConfig();
const key = modelKey();
// OPENAI_BASE_URL is validated even with no key, so a bad value fails at startup.
const fetchModel = modelFetch(modelResponsesUrl());
const transport = key
  ? createAnalysisTransport({ apiKey: () => key, ...(fetchModel ? { fetch: fetchModel } : {}) })
  : null;
log.info("session brief worker", {
  stage: "session-brief",
  ok: config.enabled,
  status: transport ? "model_configured" : "model_not_configured",
});
const tick = briefTick({
  reader: lockerManifestReader(),
  store: lockerStore().store,
  transport,
  config,
});
await runWorker({
  service: "session-brief",
  stages: [],
  handlers: () => ({}),
  tick: async (db, now) => {
    await tick(db, now);
  },
});
