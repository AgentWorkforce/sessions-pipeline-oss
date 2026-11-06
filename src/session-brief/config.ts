/**
 * Session brief settings, all from env, all off or conservative by default.
 *
 *   SESSION_BRIEF_ENABLED           "true" arms wakeups in the HoopDb indexer and runs the
 *                                   brief loop in the session-brief worker. Anything else: off.
 *   OPENAI_API_KEY                  the model credential (relayhistory's binding name). Unset,
 *                                   blank or ending in "-disabled": no model calls, and the
 *                                   worker claims nothing (a wakeup is never spent without one).
 *   OPENAI_BASE_URL                 default https://api.openai.com/v1: an OpenAI-compatible
 *                                   Responses API base; calls go to <base>/responses. https,
 *                                   loopback, or an in-cluster Service with ALLOW_IN_CLUSTER_HTTP=true.
 *   SESSION_BRIEF_MODEL             default gpt-5.6-luna (relayhistory SESSION_SUMMARY_MODEL_VERSION).
 *   SESSION_BRIEF_IDLE_SECONDS      600: due this long after the newest evidence (0048).
 *   SESSION_BRIEF_MAX_WAIT_SECONDS  3600: never later than this after the first unbriefed evidence.
 *   SESSION_WAKE_MAX_AGE_HOURS      72: a wakeup with no evidence this long is dropped unbriefed.
 *   SESSION_BRIEF_BACKGROUND_DAILY_BUDGET  5000 distinct sessions per org per UTC day (0048 default).
 *   SESSION_BRIEF_SESSION_DAILY_CAP 30 attempts per session per UTC day (session_request_limit).
 *   SESSION_BRIEF_THROTTLE_SECONDS  120: at most one attempt per session per two minutes.
 *   SESSION_BRIEF_MAX_ATTEMPTS      2: a retryable failure is retried once (one requeue).
 *   SESSION_BRIEF_BATCH             5 wakeups per tick; SESSION_BRIEF_LEASE_SECONDS 300.
 *   SESSION_BRIEF_SWEEP_SECONDS     0 (off): how often to queue indexed sessions that have no brief.
 *
 * Values are read once per process; nothing here is logged.
 */
import {
  assertDestinationUrl,
  destinationPolicy,
} from "../shared/destination-url.ts";

export interface BriefConfig {
  enabled: boolean;
  model: string;
  idleSeconds: number;
  maxWaitSeconds: number;
  wakeMaxAgeHours: number;
  backgroundDailyBudget: number;
  sessionDailyCap: number;
  /**
   * Model-call attempts per UTC day across every org (Khaliq, 2026-11-02): a spend
   * ceiling over the per-org budget. Soft by at most the concurrent workers' batch.
   */
  globalDailyCap: number;
  throttleSeconds: number;
  maxAttempts: number;
  batch: number;
  leaseSeconds: number;
  sweepSeconds: number;
}

function int(
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = env[name]?.trim();
  if (!raw || !/^\d{1,9}$/.test(raw)) return fallback;
  const value = Number(raw);
  return value >= min && value <= max ? value : fallback;
}

export const MODEL_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;

export function briefConfig(env: NodeJS.ProcessEnv = process.env): BriefConfig {
  const model = env.SESSION_BRIEF_MODEL?.trim();
  return {
    enabled: env.SESSION_BRIEF_ENABLED === "true",
    model: model && MODEL_PATTERN.test(model) ? model : "gpt-5.6-luna",
    idleSeconds: int(env, "SESSION_BRIEF_IDLE_SECONDS", 600, 0, 86_400),
    maxWaitSeconds: int(env, "SESSION_BRIEF_MAX_WAIT_SECONDS", 3_600, 0, 7 * 86_400),
    wakeMaxAgeHours: int(env, "SESSION_WAKE_MAX_AGE_HOURS", 72, 1, 24 * 365),
    backgroundDailyBudget: int(env, "SESSION_BRIEF_BACKGROUND_DAILY_BUDGET", 5_000, 0, 1_000_000),
    sessionDailyCap: int(env, "SESSION_BRIEF_SESSION_DAILY_CAP", 30, 1, 10_000),
    globalDailyCap: int(env, "SESSION_BRIEF_GLOBAL_DAILY_CAP", 20_000, 0, 10_000_000),
    throttleSeconds: int(env, "SESSION_BRIEF_THROTTLE_SECONDS", 120, 0, 86_400),
    maxAttempts: int(env, "SESSION_BRIEF_MAX_ATTEMPTS", 2, 1, 10),
    batch: int(env, "SESSION_BRIEF_BATCH", 5, 1, 100),
    // Above a full batch of 20 s model calls, so a claim never lapses mid-tick.
    leaseSeconds: int(env, "SESSION_BRIEF_LEASE_SECONDS", 300, 30, 3_600),
    sweepSeconds: int(env, "SESSION_BRIEF_SWEEP_SECONDS", 0, 0, 86_400),
  };
}

/** The model key, or null when model calls are not configured. Never logged. */
export function modelKey(env: NodeJS.ProcessEnv = process.env): string | null {
  const key = env.OPENAI_API_KEY?.trim();
  return !key || key.endsWith("-disabled") ? null : key;
}

export const DEFAULT_MODEL_BASE_URL = "https://api.openai.com/v1";
/** The URL the vendored engine/transport.ts posts to (hash-pinned; not edited here). */
const ENGINE_RESPONSES_URL = `${DEFAULT_MODEL_BASE_URL}/responses`;

/**
 * The Responses endpoint, from OPENAI_BASE_URL (default the hosted OpenAI API).
 * The request carries the model key, so the URL gets the shared transport rule
 * (src/shared/destination-url.ts). Throws at startup on a bad value.
 */
export function modelResponsesUrl(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env.OPENAI_BASE_URL?.trim();
  if (!raw) return ENGINE_RESPONSES_URL;
  const url = assertDestinationUrl("OPENAI_BASE_URL", raw, destinationPolicy(env));
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}/responses`;
}

/**
 * A fetch for createAnalysisTransport that sends the engine's fixed Responses
 * request to `responsesUrl` instead. Undefined when the default is in use, so
 * the hosted path is byte-identical to the vendored engine.
 */
export function modelFetch(
  responsesUrl: string,
  base: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch | undefined {
  if (responsesUrl === ENGINE_RESPONSES_URL) return undefined;
  const redirected = (input: string | URL | Request, init?: RequestInit) => {
    const target = input instanceof Request ? input.url : String(input);
    // Only the engine's one request is rerouted; anything else is a bug.
    if (target !== ENGINE_RESPONSES_URL)
      return Promise.reject(new Error("96bulls: unexpected model request URL"));
    return base(responsesUrl, init);
  };
  return redirected as typeof globalThis.fetch;
}
