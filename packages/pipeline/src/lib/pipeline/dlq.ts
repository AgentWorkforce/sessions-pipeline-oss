/**
 * Per-stage queues, dead letters and replay for the session pipeline.
 *
 * Each queued stage (`registry.ts`) has its own
 * queue and DLQ, on the session-events pattern in `session-analysis/events.ts`:
 * the main consumer retries with backoff, the platform moves a message to
 * `<queue>-dlq` once `max_retries` is exhausted, and the DLQ consumer quarantines
 * it in a `DeadLetterStore` and acks. Separate queues are what keep a poisoned
 * normalize from ever holding up raw pointer indexing.
 *
 * Replay re-sends quarantined messages to the stage's main queue. It is dry-run
 * by default, claims each letter before sending (so two operators replaying at
 * once send it once), and keeps the message's `workKey`, so a stage handler that
 * is idempotent by work key (all of them are: content-addressed puts, upserts)
 * applies the work at most once more no matter how many times replay runs.
 *
 * Queue names: `relayhistory-pipeline-<stage with dashes>[-<stage suffix>]`,
 * DLQ `<name>-dlq`. Nothing here creates or changes a live queue.
 */
import {
  PipelineRecorder,
  STORE_TIMEOUT_MS,
  type PipelineEventSink,
} from "./events.js";
import { pipelineErrorInfo, type ErrorReporter } from "./errors.js";
import type { LoggerEnv } from "../logger.js";
import {
  isPipelineStage,
  isTraceId,
  PIPELINE_STAGES,
  uuidv7,
  type PipelineStage,
  type TraceContext,
} from "./trace.js";

export { QUEUED_STAGES, type QueuedStage } from "./registry.js";

export const PIPELINE_QUEUE_PREFIX = "relayhistory-pipeline-";

export function stageQueueName(stage: PipelineStage, suffix = "") {
  return `${PIPELINE_QUEUE_PREFIX}${stage.replace(".", "-")}${suffix}`;
}

/** `{ stage, dlq }` for a pipeline queue name, any stage suffix, or null. */
export function stageForQueue(
  queue: string,
): { stage: PipelineStage; dlq: boolean } | null {
  if (!queue.startsWith(PIPELINE_QUEUE_PREFIX)) return null;
  const rest = queue.slice(PIPELINE_QUEUE_PREFIX.length);
  const dlq = rest.endsWith("-dlq");
  for (const stage of PIPELINE_STAGES) {
    const name = stage.replace(".", "-");
    if (rest === name || rest.startsWith(`${name}-`)) return { stage, dlq };
  }
  return null;
}

export interface StageMessage<P = unknown> {
  v: 1;
  stage: PipelineStage;
  ctx: TraceContext;
  /** Stable for one unit of work across retries and replays, e.g. the object key. */
  workKey: string;
  /**
   * The manifest revision this work is for (monotonic per session, the same
   * number the sessions index fences on). Orders work items in a trace, since
   * an older version can finish after a newer one.
   */
  revision?: number;
  payload: P;
  /** Set when this delivery came from `replayDeadLetters`. */
  replay?: { id: string; at: string };
}

export function isStageMessage(value: unknown): value is StageMessage {
  const message = value as StageMessage | null;
  return Boolean(
    message &&
    message.v === 1 &&
    isPipelineStage(message.stage) &&
    message.ctx &&
    isTraceId(message.ctx.traceId) &&
    typeof message.ctx.orgId === "string" &&
    message.ctx.orgId &&
    typeof message.ctx.workspaceId === "string" &&
    message.ctx.workspaceId &&
    typeof message.workKey === "string" &&
    message.workKey,
  );
}

/**
 * The manifest revision a message's work is for: `revision`, or the one an
 * object notification carries in its payload (`ObjectLandedPayload.revision`),
 * so the storage stage's own outcome sorts by revision like the work it fans out.
 */
export function messageRevision(message: StageMessage): number | undefined {
  if (Number.isSafeInteger(message.revision)) return message.revision;
  const payload = message.payload as { revision?: unknown } | null;
  return payload && Number.isSafeInteger(payload.revision)
    ? (payload.revision as number)
    : undefined;
}

export type DeadLetterState = "dead" | "replaying" | "resolved";

export interface DeadLetter {
  /** `deadLetterId`: one letter per tenant's unit of work, however often it dies. */
  id: string;
  stage: PipelineStage;
  traceId: string;
  workspaceId: string;
  message: StageMessage;
  state: DeadLetterState;
  firstDeadAt: string;
  lastDeadAt: string;
  /** Times this work reached the DLQ, including after failed replays. */
  deadCount: number;
  replayCount: number;
  lastReplayId?: string;
  lastReplayAt?: string;
  resolvedAt?: string;
}

/**
 * `<stage>:<org>:<workspace>:<workKey>`. Work keys are only unique within a
 * tenant (an object key has no org segment), so the tenant is part of the id:
 * one tenant's death or success never touches another's letter.
 */
export function deadLetterId(
  stage: PipelineStage,
  ctx: Pick<TraceContext, "orgId" | "workspaceId">,
  workKey: string,
) {
  return `${stage}:${encodeURIComponent(ctx.orgId)}:${encodeURIComponent(ctx.workspaceId)}:${workKey}`;
}

export interface DeadLetterFilter {
  stage?: PipelineStage;
  traceId?: string;
  workspaceId?: string;
  states?: DeadLetterState[];
  /**
   * Only letters a replay may take: `dead`, or `replaying` with a claim older
   * than `leaseCutoff` (every `replaying` letter when no cutoff, i.e. force).
   * Applied before `limit`, so in-flight claims never crowd out dead letters.
   * Overrides `states`.
   */
  replayable?: { leaseCutoff?: string };
  limit?: number;
}

/** The listed version of a letter a claim is conditional on. */
export interface ClaimSnapshot {
  state: DeadLetterState;
  replayCount: number;
}

function isReplayable(
  letter: DeadLetter,
  replayable: { leaseCutoff?: string },
) {
  if (letter.state === "dead") return true;
  if (letter.state !== "replaying") return false;
  return (
    replayable.leaseCutoff === undefined ||
    (letter.lastReplayAt ?? "") < replayable.leaseCutoff
  );
}

export interface DeadLetterStore {
  /** Upsert by id: a new death resets state to `dead` and bumps `deadCount`. */
  quarantine(message: StageMessage, at: Date): Promise<DeadLetter>;
  list(filter: DeadLetterFilter): Promise<DeadLetter[]>;
  /**
   * Compare-and-set from the listed snapshot to `replaying`. Every claim bumps
   * `replayCount`, so a snapshot taken before anyone else's claim can never
   * win: two replays that listed the same letter send it at most once.
   */
  claimForReplay(
    id: string,
    expected: ClaimSnapshot,
    replayId: string,
    at: Date,
  ): Promise<boolean>;
  /** Undo a claim whose send failed. */
  releaseReplay(id: string, replayId: string): Promise<void>;
  resolve(id: string, at: Date): Promise<void>;
}

export class InMemoryDeadLetterStore implements DeadLetterStore {
  readonly letters = new Map<string, DeadLetter>();

  async quarantine(message: StageMessage, at: Date) {
    const id = deadLetterId(message.stage, message.ctx, message.workKey);
    const existing = this.letters.get(id);
    const letter: DeadLetter = existing
      ? {
          ...existing,
          message: structuredClone(message),
          state: "dead",
          lastDeadAt: at.toISOString(),
          deadCount: existing.deadCount + 1,
        }
      : {
          id,
          stage: message.stage,
          traceId: message.ctx.traceId,
          workspaceId: message.ctx.workspaceId,
          message: structuredClone(message),
          state: "dead",
          firstDeadAt: at.toISOString(),
          lastDeadAt: at.toISOString(),
          deadCount: 1,
          replayCount: 0,
        };
    this.letters.set(id, letter);
    return structuredClone(letter);
  }

  async list(filter: DeadLetterFilter) {
    return [...this.letters.values()]
      .filter(
        (letter) =>
          (!filter.stage || letter.stage === filter.stage) &&
          (!filter.traceId || letter.traceId === filter.traceId) &&
          (!filter.workspaceId || letter.workspaceId === filter.workspaceId) &&
          (filter.replayable
            ? isReplayable(letter, filter.replayable)
            : !filter.states || filter.states.includes(letter.state)),
      )
      .sort((a, b) => a.firstDeadAt.localeCompare(b.firstDeadAt))
      .slice(0, filter.limit ?? 100)
      .map((letter) => structuredClone(letter));
  }

  async claimForReplay(
    id: string,
    expected: ClaimSnapshot,
    replayId: string,
    at: Date,
  ) {
    const letter = this.letters.get(id);
    if (
      !letter ||
      letter.state !== expected.state ||
      letter.replayCount !== expected.replayCount
    )
      return false;
    letter.state = "replaying";
    letter.replayCount += 1;
    letter.lastReplayId = replayId;
    letter.lastReplayAt = at.toISOString();
    return true;
  }

  async releaseReplay(id: string, replayId: string) {
    const letter = this.letters.get(id);
    if (letter?.state === "replaying" && letter.lastReplayId === replayId) {
      letter.state = "dead";
      letter.replayCount -= 1;
    }
  }

  async resolve(id: string, at: Date) {
    const letter = this.letters.get(id);
    if (letter && letter.state !== "resolved") {
      letter.state = "resolved";
      letter.resolvedAt = at.toISOString();
    }
  }
}

/**
 * A stage's work. Returning `{ status: "skipped" }` records the message as
 * handled but not as success (e.g. a chunk notification, which is not a
 * readable session until its manifest lands).
 */
export type StageHandler = (
  message: StageMessage,
  recorder: PipelineRecorder,
  attempt: number,
) => Promise<void | { status: "skipped"; reason: string }>;

export interface StageQueueDeps {
  handlers: Partial<Record<PipelineStage, StageHandler>>;
  sink: PipelineEventSink;
  deadLetters: DeadLetterStore;
  reporter?: ErrorReporter;
  logger?: LoggerEnv;
  /** Seconds before retry `attempt` (1-based). Default 30s doubling, capped at 15 min. */
  retryDelaySeconds?: (attempt: number) => number;
  now?: () => Date;
}

export function defaultRetryDelaySeconds(attempt: number) {
  return Math.min(900, 30 * 2 ** Math.max(0, attempt - 1));
}

type QueueMessage = Pick<
  Message<unknown>,
  "body" | "attempts" | "ack" | "retry"
>;
type QueueBatch = { queue: string; messages: readonly QueueMessage[] };

function plainLog(message: string, fields: Record<string, unknown>) {
  console.warn(JSON.stringify({ source: "pipeline", message, ...fields }));
}

/**
 * Consumer for every `relayhistory-pipeline-*` queue and its DLQ. Messages are
 * handled one at a time and acked or retried individually, so one bad message
 * never retries its neighbours.
 */
export async function handleStageQueue(
  batch: QueueBatch,
  deps: StageQueueDeps,
) {
  const now = deps.now ?? (() => new Date());
  const route = stageForQueue(batch.queue);
  if (!route) {
    plainLog("unknown pipeline queue", { queue: batch.queue });
    for (const message of batch.messages) message.retry({ delaySeconds: 300 });
    return;
  }
  for (const message of batch.messages) {
    if (!isStageMessage(message.body) || message.body.stage !== route.stage) {
      // Malformed work can never succeed; retrying it only fills the DLQ.
      plainLog("pipeline message dropped", {
        queue: batch.queue,
        reason: "malformed",
      });
      message.ack();
      continue;
    }
    const body = message.body;
    const revision = messageRevision(body);
    const recorder = new PipelineRecorder(body.ctx, {
      sink: deps.sink,
      reporter: deps.reporter,
      logger: deps.logger,
      // Lets a trace tell one manifest version's outcome from the next one's.
      attrs: {
        work_key: body.workKey,
        ...(revision !== undefined ? { revision } : {}),
        // A deleted manifest's work, so the trace can say `deleted`.
        ...((body.payload as { deleted?: unknown } | null)?.deleted === true
          ? { deleted: true }
          : {}),
      },
    });
    if (route.dlq) {
      try {
        const letter = await deps.deadLetters.quarantine(body, now());
        await recorder.record(body.stage, "dead_lettered", {
          attempt: message.attempts,
          errorCode: "retries_exhausted",
          attrs: { dead_letter_id: letter.id, dead_count: letter.deadCount },
          key: `${body.workKey}:${letter.deadCount}`,
        });
        message.ack();
      } catch {
        // The DLQ consumer has its own retries; the message is not lost yet.
        plainLog("dead letter quarantine failed", {
          queue: batch.queue,
          trace_id: body.ctx.traceId,
        });
        message.retry({ delaySeconds: 60 });
      }
      await recorder.flush();
      continue;
    }
    const handler = deps.handlers[route.stage];
    if (!handler) {
      // Not configured in this deployment. Queue retries are finite, so retrying
      // would only run the message into the DLQ; quarantine it now instead, where
      // it waits indefinitely and replays once the stage is wired.
      try {
        const letter = await deps.deadLetters.quarantine(body, now());
        await recorder.record(body.stage, "dead_lettered", {
          attempt: message.attempts,
          errorCode: "stage_not_configured",
          attrs: { dead_letter_id: letter.id },
          key: `${body.workKey}:${letter.deadCount}`,
        });
        message.ack();
      } catch {
        message.retry({ delaySeconds: 300 });
      }
      await recorder.flush();
      continue;
    }
    const replayKey = body.replay ? `:replay:${body.replay.id}` : "";
    const started = Date.now();
    try {
      const outcome = await handler(body, recorder, message.attempts);
      await recorder.record(body.stage, outcome ? outcome.status : "ok", {
        attempt: message.attempts,
        attrs: {
          duration_ms: Date.now() - started,
          ...(outcome ? { reason: outcome.reason } : {}),
          ...(body.replay ? { replay_id: body.replay.id } : {}),
        },
        key: `${body.workKey}${replayKey}`,
      });
      if (!body.replay) {
        // The same work may have been quarantined earlier (a transform that could
        // not be enqueued, then enqueued by a redelivered storage event). Done is
        // done: clear any letter for it, best effort.
        await deps.deadLetters
          .resolve(deadLetterId(body.stage, body.ctx, body.workKey), now())
          .catch(() => undefined);
      } else {
        try {
          await deps.deadLetters.resolve(
            deadLetterId(body.stage, body.ctx, body.workKey),
            now(),
          );
        } catch {
          // The work is done but its letter would stay `replaying`, which plain
          // replay skips. Redeliver so resolve runs again; the handler is
          // idempotent by work key, so redoing it is harmless.
          plainLog("dead letter resolve failed", {
            queue: batch.queue,
            trace_id: body.ctx.traceId,
          });
          message.retry({ delaySeconds: 60 });
          await recorder.flush();
          continue;
        }
      }
      message.ack();
    } catch (error) {
      const info = pipelineErrorInfo(error, `${body.stage}_failed`);
      if (!info.retryable) {
        // A permanent failure goes straight to quarantine instead of burning retries.
        try {
          const letter = await deps.deadLetters.quarantine(body, now());
          await recorder.record(body.stage, "dead_lettered", {
            attempt: message.attempts,
            error,
            errorCode: info.code,
            attrs: { dead_letter_id: letter.id, permanent: true },
            key: `${body.workKey}:${letter.deadCount}`,
          });
          message.ack();
        } catch {
          message.retry({ delaySeconds: 60 });
        }
      } else {
        await recorder.record(body.stage, "retrying", {
          attempt: message.attempts,
          error,
          errorCode: info.code,
          attrs: { duration_ms: Date.now() - started },
          key: `${body.workKey}${replayKey}`,
        });
        message.retry({
          delaySeconds: (deps.retryDelaySeconds ?? defaultRetryDelaySeconds)(
            message.attempts,
          ),
        });
      }
    }
    await recorder.flush();
  }
}

/** Waits for `work` to settle, but never longer than `ms`. */
async function settleWithin(work: Promise<unknown>[], ms: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    Promise.allSettled(work),
    new Promise((resolve) => {
      timer = setTimeout(resolve, ms);
    }),
  ]);
  clearTimeout(timer);
}

/** How long a replay claim holds before a plain replay may take the letter again. */
export const REPLAY_LEASE_MS = 60 * 60 * 1000;

export interface ReplayOptions {
  stage?: PipelineStage;
  traceId?: string;
  workspaceId?: string;
  limit?: number;
  /** Default true: report what would be sent and change nothing. */
  dryRun?: boolean;
  /** Also re-send letters whose replay is still in flight (within its lease). */
  force?: boolean;
  now?: Date;
}

export interface ReplayPlanEntry {
  id: string;
  stage: PipelineStage;
  traceId: string;
  workspaceId: string;
  state: DeadLetterState;
  deadCount: number;
  replayCount: number;
}

export interface ReplayResult {
  dryRun: boolean;
  replayId: string;
  considered: number;
  planned: ReplayPlanEntry[];
  sent: string[];
  skipped: {
    id: string;
    reason: "claimed_elsewhere" | "claim_failed" | "send_failed";
  }[];
}

export async function replayDeadLetters(
  deps: {
    deadLetters: DeadLetterStore;
    send: (stage: PipelineStage, message: StageMessage) => Promise<void>;
    sink: PipelineEventSink;
    logger?: LoggerEnv;
  },
  options: ReplayOptions = {},
): Promise<ReplayResult> {
  const dryRun = options.dryRun ?? true;
  const now = options.now ?? new Date();
  const replayId = uuidv7(now.getTime());
  // A `replaying` letter is someone's in-flight replay. It is eligible again
  // with `force`, or once its claim is older than the lease: a claim whose
  // send never happened (the claim call timed out after committing, the
  // process died) would otherwise strand the letter.
  const leaseCutoff = new Date(now.getTime() - REPLAY_LEASE_MS).toISOString();
  const letters = await deps.deadLetters.list({
    stage: options.stage,
    traceId: options.traceId,
    workspaceId: options.workspaceId,
    replayable: options.force ? {} : { leaseCutoff },
    limit: Math.min(Math.max(options.limit ?? 100, 1), 1000),
  });
  const result: ReplayResult = {
    dryRun,
    replayId,
    considered: letters.length,
    planned: letters.map((letter) => ({
      id: letter.id,
      stage: letter.stage,
      traceId: letter.traceId,
      workspaceId: letter.workspaceId,
      state: letter.state,
      deadCount: letter.deadCount,
      replayCount: letter.replayCount,
    })),
    sent: [],
    skipped: [],
  };
  if (dryRun) return result;
  const flushes: Promise<void>[] = [];
  for (const letter of letters) {
    let claimed: boolean;
    try {
      // Conditional on the version we listed: if anyone claimed it since, we lose.
      claimed = await deps.deadLetters.claimForReplay(
        letter.id,
        { state: letter.state, replayCount: letter.replayCount },
        replayId,
        now,
      );
    } catch {
      // The claim may have committed without our seeing the response. Undo
      // our own claim if it exists; if that fails too, the lease frees it.
      await deps.deadLetters
        .releaseReplay(letter.id, replayId)
        .catch(() => undefined);
      result.skipped.push({ id: letter.id, reason: "claim_failed" });
      continue;
    }
    if (!claimed) {
      result.skipped.push({ id: letter.id, reason: "claimed_elsewhere" });
      continue;
    }
    const message: StageMessage = {
      ...letter.message,
      replay: { id: replayId, at: now.toISOString() },
    };
    try {
      await deps.send(letter.stage, message);
    } catch {
      await deps.deadLetters
        .releaseReplay(letter.id, replayId)
        .catch(() => undefined);
      result.skipped.push({ id: letter.id, reason: "send_failed" });
      continue;
    }
    result.sent.push(letter.id);
    const recorder = new PipelineRecorder(letter.message.ctx, {
      sink: deps.sink,
      logger: deps.logger,
    });
    await recorder.record(letter.stage, "replayed", {
      attrs: {
        dead_letter_id: letter.id,
        replay_id: replayId,
        work_key: letter.message.workKey,
      },
      key: `${letter.message.workKey}:${replayId}`,
      now,
    });
    flushes.push(recorder.flush());
  }
  // Log exports never hold up a send: they run beside the loop and get one
  // bounded wait at the end, so a stalled export cannot stall the replay.
  await settleWithin(flushes, STORE_TIMEOUT_MS);
  return result;
}
