/**
 * `PipelineQueue`: how a producer hands a stage its work, whatever carries it.
 *
 * - `cloudflare`: one Cloudflare Queue per stage (binding from `STAGE_REGISTRY`);
 *   the platform retries and moves exhausted messages to `<queue>-dlq`, and the
 *   Worker's `queue()` handler runs `handleStageQueue`.
 * - `postgres`: one table, `sessions.pipeline_jobs`, for every stage. Consumers
 *   claim with `FOR UPDATE SKIP LOCKED` under a lease, so any number of them can
 *   poll one stage without double delivery; an exhausted job is marked `dlq` and
 *   delivered once more as the stage's DLQ, which quarantines it in
 *   `pipeline_dead_letters`. Unlike a Cloudflare DLQ, a dead-letter delivery
 *   that fails is retried until it lands, never dropped.
 * - `memory`: tests and local runs.
 *
 * Both real backends feed the same `handleStageQueue`, so retry, permanent
 * failure, quarantine, events and replay behave the same on either. Messages
 * carry ids and object keys only, never session content.
 */
import {
  handleStageQueue,
  isStageMessage,
  stageQueueName,
  type StageMessage,
  type StageQueueDeps,
} from "./dlq.js";
import { PipelineError } from "./errors.js";
import { createWorkerRuntime, type WorkSource } from "../../runtime/workers.js";
import type { PipelineWork, WorkItemIdentity, WorkOutcome } from "../../runtime/contracts.js";
import { isQueuedStage, STAGE_REGISTRY, type QueuedStage } from "./registry.js";

export interface SendOptions {
  /** Hold the message this long before its first delivery. */
  delaySeconds?: number;
}

export interface PipelineQueue {
  readonly backend: "cloudflare" | "postgres" | "memory";
  /** Routes by `message.stage`, which must be a queued stage. */
  send(message: StageMessage, options?: SendOptions): Promise<void>;
  sendBatch(
    messages: readonly StageMessage[],
    options?: SendOptions,
  ): Promise<void>;
}

/** What the Postgres backend needs: PGlite, `pg` and the Neon pool all fit. */
export interface SqlExecutor {
  query<R = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: R[] }>;
}

function checked(message: StageMessage): QueuedStage {
  if (!isStageMessage(message) || !isQueuedStage(message.stage))
    throw new PipelineError(
      "invalid_stage_message",
      "not a message for a queued pipeline stage",
      false,
    );
  return message.stage;
}

function delayOf(options?: SendOptions) {
  const delay = options?.delaySeconds ?? 0;
  return Number.isFinite(delay) && delay > 0 ? Math.floor(delay) : 0;
}

/** Cloudflare caps a batch at 100 messages. */
const CLOUDFLARE_BATCH = 100;

export function cloudflarePipelineQueue(
  bindings: Partial<Record<QueuedStage, Queue<StageMessage>>>,
): PipelineQueue {
  const producer = (stage: QueuedStage) => {
    const queue = bindings[stage];
    // Retryable: the binding can arrive with the next deploy.
    if (!queue)
      throw new PipelineError(
        "stage_queue_unbound",
        `no queue bound for pipeline stage ${stage}`,
        true,
      );
    return queue;
  };
  return {
    backend: "cloudflare",
    async send(message, options) {
      const delaySeconds = delayOf(options);
      await producer(checked(message)).send(message, {
        contentType: "json",
        ...(delaySeconds ? { delaySeconds } : {}),
      });
    },
    async sendBatch(messages, options) {
      const delaySeconds = delayOf(options);
      const byStage = new Map<QueuedStage, StageMessage[]>();
      for (const message of messages) {
        const stage = checked(message);
        byStage.set(stage, [...(byStage.get(stage) ?? []), message]);
      }
      for (const [stage, group] of byStage) {
        const queue = producer(stage);
        for (let at = 0; at < group.length; at += CLOUDFLARE_BATCH)
          await queue.sendBatch(
            group.slice(at, at + CLOUDFLARE_BATCH).map((body) => ({
              body,
              contentType: "json" as const,
              ...(delaySeconds ? { delaySeconds } : {}),
            })),
          );
      }
    },
  };
}

export interface PostgresQueueOptions {
  /** Schema holding `pipeline_jobs`. Default `sessions`. */
  schema?: string;
}

function table(options: PostgresQueueOptions) {
  const schema = options.schema ?? "sessions";
  if (!/^[a-z_][a-z0-9_]*$/.test(schema))
    throw new Error(`bad pipeline queue schema ${schema}`);
  return `${schema}.pipeline_jobs`;
}

export function postgresPipelineQueue(
  sql: SqlExecutor,
  options: PostgresQueueOptions = {},
): PipelineQueue {
  const jobs = table(options);
  const insert = async (
    messages: readonly StageMessage[],
    send?: SendOptions,
  ) => {
    if (!messages.length) return;
    for (const message of messages) checked(message);
    await sql.query(
      `INSERT INTO ${jobs} (stage, work_key, trace_id, org_id, workspace_id,
                            session_id, machine_id, message, visible_at)
       SELECT m->>'stage', m->>'workKey', (m->'ctx'->>'traceId')::uuid,
              m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
              m->'ctx'->>'sessionId', m->'ctx'->>'machineId', m,
              now() + make_interval(secs => $2::integer)
         FROM jsonb_array_elements($1::jsonb) AS m`,
      [JSON.stringify(messages), delayOf(send)],
    );
  };
  return {
    backend: "postgres",
    send: (message, send) => insert([message], send),
    sendBatch: insert,
  };
}

export interface PostgresConsumerOptions extends PostgresQueueOptions {
  sql: SqlExecutor;
  stage: QueuedStage;
  deps: StageQueueDeps;
  /** Jobs claimed per poll. Default 10. */
  batchSize?: number;
  /**
   * How long a claim holds. A consumer that dies mid-batch leaves its jobs to
   * be claimed again after this; keep it above the slowest handler. Default 300.
   */
  leaseSeconds?: number;
  /** Deliveries before a job is dead-lettered. Default from `STAGE_REGISTRY`. */
  maxAttempts?: number;
}

export interface PollResult {
  claimed: number;
  acked: number;
  retried: number;
  /** Jobs that ran out of attempts and were handed to the DLQ path. */
  exhausted: number;
}

type ClaimedJob = {
  id: string;
  dlq: boolean;
  attempts: number;
  message: unknown;
};

/** A started stage delivery; the original message remains opaque to the runtime. */
export interface PostgresStageWorkItem extends WorkItemIdentity {
  readonly type: "stage-message";
  readonly message: unknown;
  readonly deadLetter: boolean;
}

export interface PostgresStageConsumer {
  readonly source: WorkSource<PostgresStageWorkItem>;
  readonly work: PipelineWork<PostgresStageWorkItem>;
  /** Statistics for the last claim batch, including claims whose lease was lost. */
  result(): PollResult;
}

const emptyPollResult = (): PollResult => ({ claimed: 0, acked: 0, retried: 0, exhausted: 0 });

/**
 * Adapts the existing fenced queue lifecycle to the portable worker runtime.
 * Claiming does not count an attempt. `start` renews waiting claims and starts
 * one delivery; settlement is fenced by that delivery's original lease token.
 * Keep this source/work pair with one binding. Separate consumers may share SQL.
 */
export function postgresStageConsumer(options: PostgresConsumerOptions): PostgresStageConsumer {
  const jobs = table(options);
  const { sql, stage } = options;
  const leaseSeconds = Math.max(1, Math.floor(options.leaseSeconds ?? 300));
  const maxAttempts = Math.max(1, options.maxAttempts ?? STAGE_REGISTRY[stage].maxAttempts);
  type Batch = { leaseId: string; ids: string[]; result: PollResult };
  type Claim = { batch: Batch; index: number; row?: ClaimedJob; fatal?: { error: unknown }; leaveLeased?: boolean };
  // Lease authority belongs to the claim object, never a mutable job id or body.
  const claims = new WeakMap<PostgresStageWorkItem, Claim>();
  let lastResult = emptyPollResult();
  const owned = (item: PostgresStageWorkItem): Claim => {
    const claim = claims.get(item);
    if (!claim) throw new PipelineError("invalid_stage_claim", "unknown stage claim", false);
    return claim;
  };
  const source: WorkSource<PostgresStageWorkItem> = {
    async claim(max) {
      const leaseId = crypto.randomUUID();
      const { rows } = await sql.query<{ id: string }>(
        `WITH next AS (
           SELECT id FROM ${jobs}
            WHERE stage = $1 AND visible_at <= now()
            ORDER BY visible_at, id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
         )
         UPDATE ${jobs} AS j
            SET visible_at = now() + make_interval(secs => $3::integer),
                lease_id = $4::uuid
           FROM next
          WHERE j.id = next.id
         RETURNING j.id::text AS id`,
        [stage, Math.max(1, max), leaseSeconds, leaseId],
      );
      lastResult = { ...emptyPollResult(), claimed: rows.length };
      const batch: Batch = { leaseId, ids: rows.map(r => r.id), result: lastResult };
      return rows.map((row, index) => {
        // These placeholders are never delivered to PipelineWork: start loads
        // the message and counts the actual attempt immediately before handling.
        const item: PostgresStageWorkItem = { type: "stage-message", id: row.id, attempt: 0, message: undefined, deadLetter: false };
        claims.set(item, { batch, index });
        return item;
      });
    },
    async start(item) {
      const claim = owned(item);
      const { batch, index } = claim;
      if (index + 1 < batch.ids.length) await sql.query(
        `UPDATE ${jobs}
            SET visible_at = now() + make_interval(secs => $3::integer)
          WHERE id = ANY($1::bigint[]) AND lease_id = $2::uuid`,
        [batch.ids.slice(index + 1), batch.leaseId, leaseSeconds],
      );
      const { rows: [row] } = await sql.query<ClaimedJob>(
        `UPDATE ${jobs}
            SET dlq = dlq OR attempts >= $4,
                attempts = CASE WHEN NOT dlq AND attempts >= $4 THEN 1
                                  ELSE attempts + 1 END,
                visible_at = now() + make_interval(secs => $3::integer)
          WHERE id = $1::bigint AND lease_id = $2::uuid
          RETURNING id::text AS id, dlq, attempts, message`,
        [batch.ids[index], batch.leaseId, leaseSeconds, maxAttempts],
      );
      if (!row) return undefined;
      claim.row = row;
      const started: PostgresStageWorkItem = { type: "stage-message", id: row.id, attempt: row.attempts, message: row.message, deadLetter: row.dlq };
      claims.set(started, claim);
      return started;
    },
    async settle(item, outcome) {
      const claim = owned(item);
      if (claim.fatal) throw claim.fatal.error;
      if (claim.leaveLeased) return;
      const { row, batch } = claim;
      if (!row) throw new PipelineError("invalid_stage_claim", "stage delivery was not started", false);
      if (outcome.status === "done") {
        const { rows } = await sql.query(`DELETE FROM ${jobs} WHERE id = $1::bigint AND lease_id = $2::uuid RETURNING id`, [row.id, batch.leaseId]);
        batch.result.acked += rows.length;
        return;
      }
      const exhausted = !row.dlq && (outcome.status === "dead" || row.attempts >= maxAttempts);
      const delay = outcome.status === "retry" ? Math.floor(outcome.delaySeconds) : 0;
      const { rows } = await sql.query(
        `UPDATE ${jobs}
            SET visible_at = now() + make_interval(secs => $3::integer),
                lease_id = NULL,
                dlq = dlq OR $4::boolean,
                attempts = CASE WHEN $4::boolean THEN 0 ELSE attempts END
          WHERE id = $1::bigint AND lease_id = $2::uuid
          RETURNING id`,
        [row.id, batch.leaseId, exhausted ? 0 : delay, exhausted],
      );
      if (!rows.length) return;
      if (exhausted) batch.result.exhausted += 1;
      else batch.result.retried += 1;
    },
  };
  const work: PipelineWork<PostgresStageWorkItem> = {
    schedules: [],
    async handle(items) {
      const outcomes = new Map<string, WorkOutcome>();
      for (const item of items) {
        if (item.type !== "stage-message") {
          outcomes.set(item.id, { status: "dead", code: "unsupported_stage_work" });
          continue;
        }
        const claim = owned(item);
        const row = claim.row;
        if (!row) throw new PipelineError("invalid_stage_claim", "stage delivery was not started", false);
        let decision: { retry?: number } | undefined;
        try {
          await handleStageQueue({
            queue: stageQueueName(stage, row.dlq ? "-dlq" : ""),
            messages: [{
              body: row.message,
              attempts: row.attempts,
              ack: () => { decision ??= {}; },
              retry: (retry?: { delaySeconds?: number }) => { decision ??= { retry: retry?.delaySeconds ?? 0 }; },
            }],
          }, options.deps);
        } catch (error) {
          // Preserve the old poller's fatal infrastructure-error boundary:
          // leave this and later claims leased, abort this batch, let recovery
          // redeliver them. The generic runtime's fallback must not ack them.
          claim.fatal = { error };
          throw error;
        }
        if (!decision) {
          claim.leaveLeased = true;
          continue;
        }
        outcomes.set(item.id, decision.retry === undefined
          ? { status: "done" }
          : { status: "retry", delaySeconds: decision.retry, code: "stage_retry" });
      }
      return outcomes;
    },
  };
  return { source, work, result: () => ({ ...lastResult }) };
}

/** Compatibility entry point: the portable worker runtime now drives each delivery. */
export async function pollPostgresStage(options: PostgresConsumerOptions): Promise<PollResult> {
  const consumer = postgresStageConsumer(options);
  const binding = createWorkerRuntime({ source: consumer.source, batchSize: options.batchSize ?? 10 }).bind(consumer.work);
  await binding.tick();
  return consumer.result();
}

/** Everything sent, by stage; `drain` runs it through the real consumer. */
export class MemoryPipelineQueue implements PipelineQueue {
  readonly backend = "memory" as const;
  readonly sent: StageMessage[] = [];

  async send(message: StageMessage) {
    checked(message);
    this.sent.push(structuredClone(message));
  }

  async sendBatch(messages: readonly StageMessage[]) {
    for (const message of messages) await this.send(message);
  }

  take(stage?: QueuedStage) {
    const taken = this.sent.filter((m) => !stage || m.stage === stage);
    for (const message of taken)
      this.sent.splice(this.sent.indexOf(message), 1);
    return taken;
  }
}
