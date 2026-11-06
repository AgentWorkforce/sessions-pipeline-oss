/**
 * Default PipelineRuntimeAdapter: long-running worker pods (today's
 * hoopdb-indexer / referee / bob-costas tick loops, src/shared/worker.ts).
 *
 * A poll loop claims work from a `WorkSource` (the HoopDb `pipeline_jobs`
 * queue in production), settles each item, and fires schedules in-process on
 * wall-clock boundaries, like the indexer's RECONCILE_EVERY_MINUTES tick.
 * Several replicas may run: the queue's claim is the lease, and schedule
 * work must be safe to run concurrently (it only enqueues idempotent work).
 */
import {
  assertSchedules,
  scheduleItem,
  settle,
  type PipelineRuntimeAdapter,
  type PipelineWork,
  type RuntimeCapabilities,
  type WorkItem,
  type WorkItemIdentity,
  type WorkOutcome,
} from "./contracts.js";

export interface WorkSource<Item extends WorkItemIdentity = WorkItem> {
  /** Claim up to `max` items (lease them); empty when idle. */
  claim(max: number): Promise<readonly Item[]>;
  /**
   * Optional leased-delivery boundary. Renew waiting leases and count an attempt
   * immediately before handling one item. Undefined means the lease was lost.
   * Sources with this hook execute and settle items serially, before starting
   * the next; sources without it retain batch delivery.
   */
  start?(item: Item): Promise<Item | undefined>;
  /** Ack (done), release with a delay (retry) or park (dead). */
  settle(item: Item, outcome: WorkOutcome): Promise<void>;
}

export interface WorkerRuntimeOptions<Item extends WorkItemIdentity = WorkItem> {
  readonly source: WorkSource<Item>;
  readonly batchSize?: number;
  readonly idleMs?: number;
  readonly now?: () => number;
  readonly sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}

export interface WorkerBinding {
  /** Fire schedules, claim and settle one batch. Concurrent calls share the active tick. */
  tick(signal?: AbortSignal): Promise<number>;
  /** Loop until the signal aborts. */
  run(signal: AbortSignal): Promise<void>;
}

const defaultSleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const finish = () => {
      clearTimeout(t);
      signal?.removeEventListener("abort", finish);
      resolve();
    };
    const t = setTimeout(finish, ms);
    signal?.addEventListener("abort", finish, { once: true });
  });

export function createWorkerRuntime<Item extends WorkItemIdentity = WorkItem>(options: WorkerRuntimeOptions<Item>): PipelineRuntimeAdapter<WorkerBinding, Item> {
  const batchSize = options.batchSize ?? 10;
  const idleMs = options.idleMs ?? 1000;
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? defaultSleep;
  const capabilities: RuntimeCapabilities = Object.freeze({
    kind: "workers",
    trigger: "poll",
    maxBatch: batchSize,
    perItemDelay: true,
    partialBatchFailure: true,
    scheduler: "in-process",
    scopeSerialization: "advisory-lock",
    deadLetter: "queue-table",
  });
  return {
    kind: "workers",
    capabilities: () => capabilities,
    bind(work: PipelineWork<Item>): WorkerBinding {
      assertSchedules(work.schedules);
      // Last fired boundary per job: fire once per `everySeconds` window.
      const lastWindow = new Map<string, number>();
      async function tickOnce(signal?: AbortSignal): Promise<number> {
        if (signal?.aborted) return 0;
        const context = { runtime: "workers", ...(signal ? { signal } : {}) };
        const at = now();
        for (const s of work.schedules) {
          if (signal?.aborted) return 0;
          const window = Math.floor(at / (s.everySeconds * 1000));
          if (lastWindow.get(s.job) === window) continue;
          lastWindow.set(s.job, window);
          // A failed schedule run is not retried: the next window runs it again.
          await settle(work, [scheduleItem(s.job, window * s.everySeconds * 1000)], context);
        }
        if (signal?.aborted) return 0;
        const items = await options.source.claim(batchSize);
        if (items.length === 0) return 0;
        if (options.source.start) {
          for (const item of items) {
            // A started delivery drains even if shutdown arrives during start
            // or handle. Unstarted deliveries keep their leases for recovery.
            if (signal?.aborted) break;
            const started = await options.source.start(item);
            if (!started) continue;
            const outcomes = await settle(work, [started], context);
            await options.source.settle(started, outcomes.get(started.id)!);
          }
          return items.length;
        }
        if (signal?.aborted) return items.length;
        const outcomes = await settle(work, items, context);
        for (const item of items) await options.source.settle(item, outcomes.get(item.id)!);
        return items.length;
      }
      let active: Promise<number> | undefined;
      const tick = (signal?: AbortSignal): Promise<number> => {
        if (active) return active;
        active = tickOnce(signal).finally(() => { active = undefined; });
        return active;
      };
      return {
        tick,
        async run(signal) {
          while (!signal.aborted) {
            const handled = await tick(signal);
            if (handled === 0) await sleep(idleMs, signal);
          }
        },
      };
    },
  };
}
