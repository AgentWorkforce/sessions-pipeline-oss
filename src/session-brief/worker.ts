/**
 * The session-brief stage: model-written titles and descriptions, the port of
 * relayhistory-cloud's brief job (runSessionAnalysisTick, kind 'brief') onto 96Bulls.
 *
 * Fed by session-indexed: the HoopDb indexer arms a debounced wakeup per logical session
 * (store.ts armWakeup). Each tick claims due wakeups and, per session:
 *   1. reads the live pointers; none left: done (deletion already removed the brief);
 *   2. applies relayhistory's limits: one attempt per session per 2 minutes (throttle,
 *      not counted), 30 per session per UTC day, and the org's daily pool of distinct
 *      sessions (rate_limited until the next UTC midnight; the previous brief stays);
 *   3. builds the summary input from the scrubbed raw (input.ts); quarantined, empty and
 *      ineligible sessions are recorded without a model call;
 *   4. reuses the stored brief when the input fingerprint is unchanged (no model call);
 *   5. otherwise calls the model through relayhistory's summary prompt and contract, then
 *      scrubs, bounds and records the brief and projects it into the directory.
 * A retryable failure (provider 429/5xx/timeout, a missing chunk) is retried once after a
 * backoff; anything else, or a second failure, records `failed` with its code.
 *
 * Logs carry ids, statuses, counts and controlled error codes only: never transcript
 * text, the model input or output, or a credential.
 */
import { createHash } from "node:crypto";
import { PipelineError, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { log } from "../shared/log.ts";
import { MODEL_PATTERN, type BriefConfig } from "./config.ts";
import { buildBriefInput } from "./input.ts";
import { summarizeSessionInput, type AnalysisModelTransport } from "./engine/model.ts";
import { SessionAnalysisProviderError } from "./engine/transport.ts";
import {
  BriefRejected,
  admit,
  claimWakeups,
  finishWakeup,
  livePointers,
  pruneAttempts,
  pruneStaleWakeups,
  recordBrief,
  rescheduleWakeup,
  scrubBrief,
  storedBrief,
  sweepUnbriefed,
  type ClaimedWakeup,
} from "./store.ts";
import {
  SESSION_BRIEF_ENGINE_VERSION,
  SESSION_BRIEF_PROMPT_VERSION,
} from "./versions.ts";

export interface BriefDeps {
  db: HoopDb;
  reader: ManifestReader;
  store: ObjectStore;
  /** Null when no model is configured: nothing is claimed. */
  transport: AnalysisModelTransport | null;
  config: BriefConfig;
}

export type BriefResult =
  | "generated"
  | "reused"
  | "throttled"
  | "rate_limited"
  | "quarantined"
  | "empty"
  | "ineligible"
  | "retrying"
  | "failed"
  | "gone"
  | "stale";

const fingerprint = (input: unknown) =>
  createHash("sha256").update(JSON.stringify(input)).digest("hex");

/** Retry backoff after the n-th attempt: 60 s, then doubling, at most an hour. */
export const retryDelaySeconds = (attempt: number) =>
  Math.min(3_600, 60 * 2 ** Math.max(0, attempt - 1));

function failureOf(error: unknown): { code: string; retryable: boolean } {
  if (error instanceof SessionAnalysisProviderError)
    return { code: error.code, retryable: error.retryable };
  if (error instanceof BriefRejected) return { code: "brief_scrub_rejected", retryable: false };
  if (error instanceof PipelineError) {
    const code = /^[a-z][a-z0-9_]{0,63}$/.test(error.code) ? error.code : "session_collection_failed";
    return { code, retryable: error.retryable };
  }
  // relayhistory processor: a TypeError is the model's invalid output.
  if (error instanceof TypeError) return { code: "model_output_invalid", retryable: false };
  return { code: "brief_processing_failed", retryable: true };
}

/** One claimed wakeup, start to finish. Never throws for a session-level failure. */
export async function briefSession(deps: BriefDeps, w: ClaimedWakeup): Promise<BriefResult> {
  const { db, config } = deps;
  const pointers = await livePointers(db, w);
  if (!pointers.length) {
    await finishWakeup(db, w, { idleSeconds: config.idleSeconds, maxWaitSeconds: config.maxWaitSeconds });
    return "gone";
  }
  const stored = await storedBrief(db, w);
  if (stored && stored.attemptedAgoSeconds < config.throttleSeconds) {
    await rescheduleWakeup(
      db,
      w,
      { seconds: Math.ceil(config.throttleSeconds - stored.attemptedAgoSeconds) },
      { refundAttempt: true },
    );
    return "throttled";
  }
  const sourceRevisions = Object.fromEntries(pointers.map((p) => [p.machineId, p.revision]));
  const window = { idleSeconds: config.idleSeconds, maxWaitSeconds: config.maxWaitSeconds };
  // After a committed outcome, cleanup is separate: a cleanup failure never rewrites the
  // committed brief (the lease lapses and the wakeup is reclaimed, then reused or throttled).
  const finish = async () => {
    try {
      await finishWakeup(db, w, window);
    } catch (error) {
      log.warn("session brief wakeup cleanup failed", {
        stage: "session-brief",
        session_id: w.sessionId,
        error_code: error instanceof Error ? error.name : "unknown",
      });
    }
  };
  try {
    const built = await buildBriefInput(deps, w, pointers);
    if (built.kind !== "ready") {
      await recordBrief(db, w, built.sourceRevisions, { status: built.kind, error: built.error });
      await finish();
      return built.kind;
    }
    const print = fingerprint(built.input);
    // A brief from another prompt (v2 had no state) is regenerated even for the same input.
    if (stored?.title && stored.briefFingerprint === print && stored.promptVersion === SESSION_BRIEF_PROMPT_VERSION) {
      const { written } = await recordBrief(db, w, built.sourceRevisions, {
        status: "reused",
        fingerprint: print,
      });
      await finish();
      return written ? "reused" : "stale";
    }
    // Only a real model call is admitted and counted: empty, ineligible and reused
    // outcomes above never spend the per-session cap, the org budget or the global ceiling.
    const admission = await admit(db, w, {
      sessionDailyCap: config.sessionDailyCap,
      budget: config.backgroundDailyBudget,
      globalDailyCap: config.globalDailyCap,
    });
    if (!admission.admitted) {
      await recordBrief(db, w, built.sourceRevisions, {
        status: "rate_limited",
        error:
          admission.reason === "capped"
            ? "session_brief_capped"
            : admission.reason === "global"
              ? "session_brief_global_capped"
              : "session_brief_rate_limited",
      });
      await rescheduleWakeup(db, w, { nextUtcDay: true }, { refundAttempt: true });
      return "rate_limited";
    }
    const transport = deps.transport;
    if (!transport) throw new SessionAnalysisProviderError("model_not_configured", false);
    const startedAt = Date.now();
    const summary = await summarizeSessionInput(built.input, transport, config.model);
    const scrubbed = scrubBrief(summary.summary);
    // The provider's reported model id, unless it is not one the column accepts.
    const model = MODEL_PATTERN.test(summary.model) ? summary.model : config.model;
    const { written } = await recordBrief(db, w, built.sourceRevisions, {
      status: "ready",
      text: {
        ...scrubbed,
        fingerprint: print,
        promptVersion: SESSION_BRIEF_PROMPT_VERSION,
        engineVersion: SESSION_BRIEF_ENGINE_VERSION,
        model,
        messageCount: built.input.messages.length,
        shortenedMessages: built.input.messages.filter((m) => m.truncated).length,
        inputTokens: summary.inputTokens,
        outputTokens: summary.outputTokens,
        durationMs: Date.now() - startedAt,
        generatedAt: new Date(),
      },
    });
    await finish();
    log.info("session brief generated", {
      stage: "session-brief",
      org_id: w.orgId,
      workspace_id: w.workspaceId,
      harness: w.harness,
      session_id: w.sessionId,
      model_id: model,
      tokens_in: summary.inputTokens,
      tokens_out: summary.outputTokens,
      count: built.input.messages.length,
      ok: written,
    });
    return written ? "generated" : "stale";
  } catch (error) {
    const failure = failureOf(error);
    if (failure.retryable && w.attempts < config.maxAttempts) {
      await rescheduleWakeup(db, w, { seconds: retryDelaySeconds(w.attempts) }, { error: failure.code });
      log.warn("session brief retrying", {
        stage: "session-brief",
        session_id: w.sessionId,
        error_code: failure.code,
        retryable: true,
        attempt: w.attempts,
      });
      return "retrying";
    }
    await recordBrief(db, w, sourceRevisions, { status: "failed", error: failure.code });
    await finish();
    log.warn("session brief failed", {
      stage: "session-brief",
      session_id: w.sessionId,
      error_code: failure.code,
      retryable: failure.retryable,
      attempt: w.attempts,
    });
    return "failed";
  }
}

/**
 * A `tick` for runWorker. Off unless SESSION_BRIEF_ENABLED=true; with no model configured
 * it prunes and sweeps but claims nothing, so wakeups wait (and expire after
 * SESSION_WAKE_MAX_AGE_HOURS) instead of spending their attempts.
 */
export function briefTick(deps: Omit<BriefDeps, "db">) {
  let lastSweep = 0;
  let lastPrune = 0;
  return async (db: HoopDb, now: Date): Promise<Record<BriefResult, number> | null> => {
    if (!deps.config.enabled) return null;
    const full: BriefDeps = { ...deps, db };
    if (now.getTime() - lastPrune >= 60_000) {
      lastPrune = now.getTime();
      const pruned = await pruneStaleWakeups(db, deps.config.wakeMaxAgeHours);
      await pruneAttempts(db);
      if (pruned) log.info("stale session brief wakeups dropped", { stage: "session-brief", count: pruned });
    }
    if (deps.config.sweepSeconds > 0 && now.getTime() - lastSweep >= deps.config.sweepSeconds * 1000) {
      lastSweep = now.getTime();
      const armed = await sweepUnbriefed(db, 100);
      if (armed) log.info("session brief sweep armed", { stage: "session-brief", count: armed });
    }
    if (!deps.transport) return null;
    const counts = {} as Record<BriefResult, number>;
    const claimed = await claimWakeups(db, deps.config.batch, deps.config.leaseSeconds);
    for (const w of claimed) {
      const result = await briefSession(full, w);
      counts[result] = (counts[result] ?? 0) + 1;
    }
    // Only allowlisted log fields (src/shared/log.ts): the per-outcome map is returned, not logged.
    if (claimed.length)
      log.info("session brief tick", {
        stage: "session-brief",
        claimed: claimed.length,
        failed: counts.failed ?? 0,
        skipped: (counts.throttled ?? 0) + (counts.rate_limited ?? 0),
      });
    return counts;
  };
}
