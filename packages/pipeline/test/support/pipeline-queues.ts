/**
 * A local stand-in for Cloudflare Queues with the semantics the pipeline relies
 * on: per-message ack/retry, `attempts` starting at 1, and a message moved to
 * `<queue>-dlq` once it has been retried `maxRetries` times. No network, no live
 * queue.
 */
import {
  handleStageQueue,
  stageQueueName,
  type PipelineStage,
  type StageMessage,
  type StageQueueDeps,
} from "../../src/lib/pipeline/index.js";

type Pending = { queue: string; body: unknown; attempts: number };

export class FakePipelineQueues {
  readonly pending: Pending[] = [];
  readonly sent: { queue: string; body: StageMessage }[] = [];
  readonly dropped: Pending[] = [];

  constructor(
    readonly options: { maxRetries?: number; dlqMaxRetries?: number } = {},
  ) {}

  async send(stage: PipelineStage, body: StageMessage) {
    const queue = stageQueueName(stage);
    // Queues serialize bodies; never hand the consumer the producer's object.
    const copy = structuredClone(body);
    this.sent.push({ queue, body: copy });
    this.pending.push({ queue, body: copy, attempts: 1 });
  }

  /** Deliver until every queue is empty, one message per batch (max_batch_size 1). */
  async drain(deps: StageQueueDeps, maxDeliveries = 1000) {
    let deliveries = 0;
    while (this.pending.length) {
      if (++deliveries > maxDeliveries) throw new Error("queue never drained");
      const next = this.pending.shift()!;
      let outcome: "ack" | "retry" | undefined;
      await handleStageQueue(
        {
          queue: next.queue,
          messages: [
            {
              body: next.body,
              attempts: next.attempts,
              ack: () => {
                outcome ??= "ack";
              },
              retry: () => {
                outcome ??= "retry";
              },
            },
          ],
        },
        deps,
      );
      if (outcome !== "retry") continue;
      const dlq = next.queue.endsWith("-dlq");
      const limit = dlq
        ? (this.options.dlqMaxRetries ?? 3)
        : (this.options.maxRetries ?? 5);
      if (next.attempts <= limit) {
        this.pending.push({ ...next, attempts: next.attempts + 1 });
      } else if (!dlq) {
        this.pending.push({
          queue: `${next.queue}-dlq`,
          body: next.body,
          attempts: 1,
        });
      } else {
        this.dropped.push(next);
      }
    }
    return deliveries;
  }
}
