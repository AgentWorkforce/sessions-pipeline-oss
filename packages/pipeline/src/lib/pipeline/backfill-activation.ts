/**
 * A backfill the gate refused. Manifests are not stored, so nothing here can
 * replay the write: this stage re-checks the gate on a delay and, once it opens,
 * records that the exporter may try again. Attempts are bounded, so a gate that
 * never opens ends in the DLQ, where a person sees it.
 */
import type { StageHandler, StageMessage } from "./dlq.js";
import { PipelineError } from "./errors.js";
import type { SqlExecutor } from "./queue.js";

export const BACKFILL_ACTIVATION_STAGE = "backfill-activation" as const;

export const BACKFILL_ACTIVATION_DEFAULTS = {
  maxAttempts: 12,
  maxDelaySeconds: 3600,
  baseDelaySeconds: 60,
};

export interface BackfillActivationPayload {
  manifestKey: string;
  grantId: string;
  planDigest: string;
  attempt: number;
}

/** One item per manifest and grant: repeated refusals share a work key. */
export function backfillActivationWorkKey(
  manifestKey: string,
  grantId: string,
) {
  return `${BACKFILL_ACTIVATION_STAGE}:${manifestKey}:${grantId}`;
}

export function backfillActivationDelay(
  attempt: number,
  limits = BACKFILL_ACTIVATION_DEFAULTS,
) {
  return Math.min(
    limits.maxDelaySeconds,
    limits.baseDelaySeconds * 2 ** Math.max(0, attempt - 1),
  );
}

/** Numeric env text with a default; anything else keeps the default. */
export function backfillActivationLimits(env: Record<string, unknown>) {
  const read = (value: unknown, fallback: number) => {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
  };
  return {
    ...BACKFILL_ACTIVATION_DEFAULTS,
    maxAttempts: read(
      env.BACKFILL_ACTIVATION_MAX_ATTEMPTS,
      BACKFILL_ACTIVATION_DEFAULTS.maxAttempts,
    ),
    maxDelaySeconds: read(
      env.BACKFILL_ACTIVATION_MAX_DELAY_SECONDS,
      BACKFILL_ACTIVATION_DEFAULTS.maxDelaySeconds,
    ),
  };
}

export interface BackfillActivationDeps {
  /** Why the gate is still closed; empty when it is open. */
  missing: () => Promise<string[]>;
  /** Tell the exporter this item may be retried. Must be idempotent. */
  ready: (payload: BackfillActivationPayload) => Promise<void>;
  requeue: (message: StageMessage, delaySeconds: number) => Promise<void>;
  limits?: typeof BACKFILL_ACTIVATION_DEFAULTS;
}

export function backfillActivationHandler(
  deps: BackfillActivationDeps,
): StageHandler {
  const limits = deps.limits ?? BACKFILL_ACTIVATION_DEFAULTS;
  return async (message) => {
    const payload = message.payload as BackfillActivationPayload;
    if ((await deps.missing()).length === 0) {
      await deps.ready(payload);
      return;
    }
    if (payload.attempt >= limits.maxAttempts)
      throw new PipelineError(
        "backfill_activation_exhausted",
        "the activation gate did not open in time",
        false,
      );
    await deps.requeue(
      { ...message, payload: { ...payload, attempt: payload.attempt + 1 } },
      backfillActivationDelay(payload.attempt, limits),
    );
  };
}

/** The durable signal E1's exporter reads to retry an item. */
export function postgresBackfillReady(db: SqlExecutor) {
  return async (payload: BackfillActivationPayload) => {
    await db.query(
      `INSERT INTO sessions.backfill_activation_ready
         (manifest_key, grant_id, plan_digest)
       VALUES ($1, $2, $3)
       ON CONFLICT (manifest_key, grant_id) DO NOTHING`,
      [payload.manifestKey, payload.grantId, payload.planDigest],
    );
  };
}
