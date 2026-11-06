/**
 * Session pipeline observability: trace ids, per-stage events, error tracking,
 * per-stage DLQs and replay. See `docs/pipeline-observability.md`.
 *
 * Everything is configured from plain env names, with no Cloud-only binding, so
 * a self-hosted Worker (or any host that can run the queue handler) gets the same
 * behavior by pointing `PIPELINE_EVENTS_URL` at its own PostgREST.
 */
import type { Bindings } from "../../env.js";
import { loggerEnvFromBindings } from "../logger.js";
import { errorReporterFromEnv } from "./errors.js";
import { pipelineEventSinkFromEnv, type PipelineEventSink } from "./events.js";
import type {
  DeadLetterStore,
  StageHandler,
  StageMessage,
  StageQueueDeps,
} from "./dlq.js";
import { cloudflarePipelineQueue, type PipelineQueue } from "./queue.js";
import { QUEUED_STAGES, STAGE_REGISTRY, type QueuedStage } from "./registry.js";
import { postgrestDeadLetterStore } from "./postgrest-dead-letters.js";
import {
  sessionIndexedHandler,
  objectLandedHandler,
  fileNormalizeHandler,
  type SessionPointerIndex,
  type SessionTransform,
} from "./stages.js";
import type { PipelineStage } from "./trace.js";

export * from "./dlq.js";
export * from "./errors.js";
export * from "./events.js";
export * from "./migration-assertions.js";
export * from "./queue.js";
export * from "./registry.js";
export * from "./backfill-activation.js";
export * from "./manifest-readiness.js";
export * from "./parked.js";
export * from "./session-split.js";
export * from "./relayhistory-projection.js";
export * from "./relayhistory-transform.js";
export * from "./stages.js";
export * from "./telemetry-ids.js";
export * from "./trace.js";
export * from "./traces.js";
export { postgrestDeadLetterStore } from "./postgrest-dead-letters.js";
export { postgresDeadLetterStore } from "./postgres-dead-letters.js";

/** Daily, at this UTC minute of the day, off the hour so it avoids other jobs. */
export const PIPELINE_MAINTAIN_MINUTE_UTC = 3 * 60 + 17;

/**
 * Runs `sessions.pipeline_events_maintain()` (monthly partitions ahead, 90-day
 * retention) through PostgREST once a day from the minute cron. Does nothing
 * until `PIPELINE_EVENTS_URL` and `PIPELINE_EVENTS_TOKEN` are configured. The
 * function is idempotent, so a missed or repeated day is harmless.
 */
export async function maintainPipelineEvents(
  env: Bindings,
  now = new Date(),
  fetcher: typeof fetch = fetch,
): Promise<"skipped" | "ok" | "failed"> {
  if (!env.PIPELINE_EVENTS_URL || !env.PIPELINE_EVENTS_TOKEN) return "skipped";
  if (
    now.getUTCHours() * 60 + now.getUTCMinutes() !==
    PIPELINE_MAINTAIN_MINUTE_UTC
  )
    return "skipped";
  const schema = env.PIPELINE_EVENTS_SCHEMA ?? "sessions";
  try {
    const response = await fetcher(
      `${env.PIPELINE_EVENTS_URL.replace(/\/+$/, "")}/rpc/pipeline_events_maintain`,
      {
        method: "POST",
        headers: {
          apikey: env.PIPELINE_EVENTS_TOKEN,
          Authorization: `Bearer ${env.PIPELINE_EVENTS_TOKEN}`,
          "Content-Type": "application/json",
          "Content-Profile": schema,
        },
        body: "{}",
      },
    );
    await response.body?.cancel();
    return response.ok ? "ok" : "failed";
  } catch {
    return "failed";
  }
}

/** Refuses every call, so the DLQ consumer retries instead of acking work it could not keep. */
const unconfiguredDeadLetters: DeadLetterStore = {
  quarantine: async () => {
    throw new Error("dead letter store is not configured");
  },
  list: async () => [],
  claimForReplay: async () => false,
  releaseReplay: async () => {},
  resolve: async () => {},
};

export function deadLetterStoreFromEnv(env: Bindings): DeadLetterStore {
  return env.PIPELINE_EVENTS_URL && env.PIPELINE_EVENTS_TOKEN
    ? postgrestDeadLetterStore({
        url: env.PIPELINE_EVENTS_URL,
        token: env.PIPELINE_EVENTS_TOKEN,
        schema: env.PIPELINE_EVENTS_SCHEMA,
      })
    : unconfiguredDeadLetters;
}

/** The Cloudflare queue producers this deployment binds, by stage. */
export function stageBindings(
  env: Bindings,
): Partial<Record<QueuedStage, Queue<StageMessage>>> {
  const bindings: Partial<Record<QueuedStage, Queue<StageMessage>>> = {};
  for (const stage of QUEUED_STAGES) {
    const queue = env[STAGE_REGISTRY[stage].binding as keyof Bindings];
    if (queue) bindings[stage] = queue as Queue<StageMessage>;
  }
  return bindings;
}

/**
 * The Worker's queue: Cloudflare Queues. A self-hosted runner without them
 * builds `postgresPipelineQueue(sql)` and passes it as `impl.queue`.
 */
export function pipelineQueueFromEnv(env: Bindings): PipelineQueue {
  return cloudflarePipelineQueue(stageBindings(env));
}

export async function sendToStage(
  env: Bindings,
  stage: PipelineStage,
  message: StageMessage,
) {
  await pipelineQueueFromEnv(env).send({ ...message, stage });
}

export interface PipelineImplementations {
  /** Default: `pipelineQueueFromEnv(env)`. */
  queue?: PipelineQueue;
  /** Default: `deadLetterStoreFromEnv(env)`; `postgresDeadLetterStore(sql)` with no PostgREST. */
  deadLetters?: DeadLetterStore;
  index?: SessionPointerIndex;
  transform?: SessionTransform;
  sink?: PipelineEventSink;
  /** Replaces a stage's handler, e.g. a stub stage wired ahead of release. */
  handlers?: Partial<Record<QueuedStage, StageHandler>>;
}

/**
 * Queue deps for this deployment. The pointer index and transform come from
 * their owning workstreams; a stage with no handler (those, or any stub in
 * `STAGE_REGISTRY`) quarantines its messages for replay rather than dropping
 * them.
 */
export function pipelineQueueDeps(
  env: Bindings,
  impl: PipelineImplementations = {},
): StageQueueDeps {
  const transformEnabled =
    env.PIPELINE_FILE_NORMALIZE_ENABLED === "true" && Boolean(impl.transform);
  const deadLetters = impl.deadLetters ?? deadLetterStoreFromEnv(env);
  const queue = impl.queue ?? pipelineQueueFromEnv(env);
  return {
    sink: impl.sink ?? pipelineEventSinkFromEnv(env),
    deadLetters,
    reporter: errorReporterFromEnv(env),
    logger: loggerEnvFromBindings(env),
    handlers: {
      "object-landed": objectLandedHandler({
        senders: {
          send: (stage, message) => queue.send({ ...message, stage }),
        },
        quarantine: (message, at) => deadLetters.quarantine(message, at),
        transformEnabled,
      }),
      ...(impl.index
        ? { "session-indexed": sessionIndexedHandler(impl.index) }
        : {}),
      ...(impl.transform && transformEnabled
        ? { "file-normalize": fileNormalizeHandler(impl.transform) }
        : {}),
      ...impl.handlers,
    },
  };
}
