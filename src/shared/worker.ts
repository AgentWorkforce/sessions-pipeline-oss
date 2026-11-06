/**
 * The stage worker runtime every consumer shares: one poll loop per stage over
 * `sessions.pipeline_jobs`, each message handled inside a span that continues
 * the message's traceparent. Persistent worker-runtime bindings own stage
 * delivery; the outer loop retains health, readiness gates and side ticks.
 */
import {
  pipelineQueueDeps,
  postgresStageConsumer,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  type PipelineImplementations,
  type QueuedStage,
  type StageHandler,
  type StageMessage,
  type StageQueueDeps,
  attestReader,
  postgresReadinessStore,
  selfCheckManifestEpochs,
  type ManifestReaderName,
} from "./vendor.ts";
import {
  inTransaction,
  openHoopDb,
  schemaVerdict,
  waitForHoopDb,
  waitForSchema,
  type HoopDb,
} from "./db.ts";
import { ctxOf, enrichMessage } from "./enrich.ts";
import { vendorBindings, integer } from "./env.ts";
import { liveDataPathVerdict, workloadReadyLive } from "./data-path.ts";
import { errorFields, log } from "./log.ts";
import { initTelemetry, shutdownTelemetry, withSpan } from "./telemetry.ts";
import { createServer } from "node:http";
import { setTimeout as idleDelay } from "node:timers/promises";
import { createWorkerRuntime } from "@rhc/runtime/workers.ts";
import { sqlPipelineEventSink } from "../hoopdb/sink.ts";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { validateAdapterEnv } from "@rhc/adapters/registry.ts";

export interface WorkerOptions {
  service: string;
  stages: QueuedStage[];
  /** Builds the handlers once the database is open. */
  handlers: (
    db: HoopDb,
    current: () => StageMessage | undefined,
  ) => Partial<Record<QueuedStage, StageHandler>>;
  /** Extra vendored implementations (e.g. the session index). */
  impl?: (
    db: HoopDb,
    current: () => StageMessage | undefined,
  ) => Omit<
    PipelineImplementations,
    "handlers" | "queue" | "deadLetters" | "sink"
  >;
  /** Manifest readers in this process. They attest at startup, once the key exists. */
  attests?: ManifestReaderName[];
  /** Runs once per loop tick, for side loops such as reconcile. */
  tick?: (db: HoopDb, now: Date) => Promise<void>;
}

/**
 * Attest each reader this process hosts. An unattested reader only keeps the
 * gate closed, so a failure here is logged and never stops the worker.
 */
async function attestReaders(readers: ManifestReaderName[], db: HoopDb) {
  const key = process.env.MANIFEST_READINESS_KEY;
  if (!key) return;
  const store = postgresReadinessStore(db);
  for (const reader of readers) {
    try {
      await attestReader({
        reader,
        key,
        store,
        selfCheck: selfCheckManifestEpochs,
      });
      log.info("manifest reader attested", { reader });
    } catch (err) {
      log.warn("manifest reader did not attest", {
        reader,
        ...errorFields(err),
      });
    }
  }
}

/** Run one complete gated cycle at a time; shutdown drains the current cycle. */
export async function runWorkerLoop(io: {
  signal: AbortSignal;
  idleMs: number;
  tick: () => Promise<number>;
}): Promise<void> {
  while (!io.signal.aborted) {
    const claimed = await io.tick();
    if (claimed === 0 && !io.signal.aborted) {
      try {
        await idleDelay(io.idleMs, undefined, { signal: io.signal });
      } catch (error) {
        if (!io.signal.aborted) throw error;
      }
    }
  }
}

/** Wraps a handler so its work runs in a span on the message's trace, with allowlisted attrs. */
export function traced(
  stage: QueuedStage,
  handler: StageHandler,
): StageHandler {
  return async (message, recorder, attempt) => {
    const ctx = ctxOf(message);
    return withSpan(
      `stage ${stage}`,
      {
        traceparent: ctx.traceparent,
        attrs: {
          stage,
          attempt,
          trace_id: message.ctx.traceId,
          session_uuid: ctx.session_uuid,
          tenant_id: ctx.tenant_id ?? message.ctx.orgId,
          workspace_id: message.ctx.workspaceId,
          harness: message.ctx.harness,
          revision: message.revision,
        },
      },
      async (span) => {
        const started = Date.now();
        try {
          const out = await handler(message, recorder, attempt);
          span.setAttribute(
            "outcome",
            out && typeof out === "object" && out.status === "skipped"
              ? "skipped"
              : "ok",
          );
          log.info("stage handled", {
            stage,
            attempt,
            outcome: out && typeof out === "object" ? out.status : "ok",
            trace_id: message.ctx.traceId,
            session_uuid: ctx.session_uuid,
            duration_ms: Date.now() - started,
          });
          return out;
        } catch (err) {
          const fields = errorFields(err);
          span.setAttribute(
            "outcome",
            fields.retryable === false ? "quarantined" : "retry",
          );
          log.warn("stage failed", {
            stage,
            attempt,
            trace_id: message.ctx.traceId,
            session_uuid: ctx.session_uuid,
            ...fields,
          });
          throw err;
        }
      },
    );
  };
}

/**
 * Worker health, for container and Kubernetes probes (workers have no other port).
 *   GET /healthz  200 while the poll loop is ticking and HoopDb answered within
 *                 the last WORKER_UNHEALTHY_AFTER_FAILURES ticks; 503 otherwise.
 *   GET /readyz   200 once HoopDb answers, the schema ledger is complete and the
 *                 Worker role validation is PASS for this image's generation
 *                 (re-read on every probe, so a failed later validation turns it 503).
 *                 A GCS workload (LOCKER_PROVIDER=gcs or WORKER_DATA_PATH_PROBE=1)
 *                 also requires the data-path settings and refuses HMAC keys.
 *   GET /datapathz  the data-path verdict alone. Unarmed on the S3 default.
 * A rotated HoopDb password therefore turns the worker unhealthy and lets the
 * orchestrator restart it with the new secret, instead of "Running" forever.
 */
export interface WorkerHealth {
  ready: boolean;
  /** Live schema/validation check, set once HoopDb is open; absent means not ready. */
  schemaReady?: () => Promise<boolean>;
  lastTickAt: number;
  consecutiveDbFailures: number;
}

/**
 * Liveness. `lastTickAt` is progress, not just loop turns: it moves on every
 * tick, at the start and end of every handled message, and while the worker
 * waits for HoopDb or the schema, so a long batch, reconcile or migration is
 * never mistaken for a stall.
 */
export function healthVerdict(
  h: WorkerHealth,
  now: number,
  opts: { maxFailures: number; maxTickAgeMs: number },
) {
  if (h.consecutiveDbFailures >= opts.maxFailures)
    return { ok: false, reason: "hoopdb_unreachable" };
  if (now - h.lastTickAt > opts.maxTickAgeMs)
    return { ok: false, reason: "loop_stalled" };
  return { ok: true, reason: h.ready ? "ok" : "starting" };
}

/**
 * The claim gate: the same live check as /readyz (ledger, marker and phase 2). Closed
 * when it is unset, false, or throws. Logged when closed so a paused worker is visible.
 */
export async function gateOpen(
  h: Pick<WorkerHealth, "schemaReady">,
): Promise<boolean> {
  const open = h.schemaReady ? await h.schemaReady().catch(() => false) : false;
  if (!open)
    log.warn(
      "worker paused: live role validation gate closed; claiming nothing",
    );
  return open;
}

/** Readiness: the schema is complete and HoopDb is answering now. */
export function readyVerdict(h: WorkerHealth, opts: { maxFailures: number }) {
  return h.ready && h.consecutiveDbFailures < opts.maxFailures;
}

export function serveHealth(health: WorkerHealth, port: number) {
  const maxFailures = integer("WORKER_UNHEALTHY_AFTER_FAILURES", 5);
  const maxTickAgeMs = integer("WORKER_MAX_TICK_AGE_MS", 120_000);
  const server = createServer((req, res) => {
    if (req.url === "/readyz") {
      void (health.schemaReady ? health.schemaReady() : Promise.resolve(false))
        .catch(() => false)
        .then((schemaOk) =>
          workloadReadyLive(
            schemaOk && readyVerdict(health, { maxFailures }),
            process.env,
          ),
        )
        .then((verdict) => {
          res.writeHead(verdict.ok ? 200 : 503, {
            "content-type": "application/json",
          });
          res.end(JSON.stringify(verdict));
        })
        .catch(() => {
          res.writeHead(503, { "content-type": "application/json" });
          res.end(
            JSON.stringify({ ok: false, missing: ["REDIS_TLS_ROUND_TRIP"] }),
          );
        });
      return;
    }
    if (req.url === "/datapathz") {
      void liveDataPathVerdict(process.env)
        .then((verdict) => {
          res.writeHead(verdict.ok ? 200 : 503, {
            "content-type": "application/json",
          });
          res.end(
            JSON.stringify(
              verdict.ok
                ? { ok: true, armed: verdict.armed }
                : { ok: false, missing: verdict.missing },
            ),
          );
        })
        .catch(() => {
          res.writeHead(503, { "content-type": "application/json" });
          res.end(
            JSON.stringify({ ok: false, missing: ["REDIS_TLS_ROUND_TRIP"] }),
          );
        });
      return;
    }
    if (req.url === "/healthz") {
      const v = healthVerdict(health, Date.now(), {
        maxFailures,
        maxTickAgeMs,
      });
      res.writeHead(v.ok ? 200 : 503, { "content-type": "application/json" });
      res.end(JSON.stringify(v));
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(port, "0.0.0.0", () =>
    log.info("worker health listening", { port }),
  );
  return server;
}

/**
 * One worker tick: every claim path, gated. The live gate (gateOpen: ledger, marker,
 * phase 2 and the sessions-function boundary) runs before EACH stage's claim and before
 * the side tick; shutdown is rechecked after each asynchronous gate. Once closed,
 * nothing further in the tick runs. Exported so a
 * table-driven test can prove the order over every claim path.
 */
export async function workerTick<S>(io: {
  signal?: AbortSignal;
  stages: readonly S[];
  gate: () => Promise<boolean>;
  poll: (stage: S) => Promise<number>;
  tick?: () => Promise<void>;
}): Promise<{ claimed: number; gated: boolean }> {
  let claimed = 0;
  for (const stage of io.stages) {
    if (io.signal?.aborted || !(await io.gate()) || io.signal?.aborted)
      return { claimed, gated: true };
    claimed += await io.poll(stage);
  }
  if (io.tick) {
    if (io.signal?.aborted || !(await io.gate()) || io.signal?.aborted)
      return { claimed, gated: true };
    await io.tick();
  }
  return { claimed, gated: false };
}

export async function runWorker(opts: WorkerOptions): Promise<never> {
  // Fail closed on a mis-set adapter selection (docs/search-and-runtime-adapters.md).
  // Unset (every current install) resolves to postgres + workers: no change.
  validateAdapterEnv(process.env, { runtime: ["workers"] });
  process.env.OTEL_SERVICE_NAME ||= opts.service;
  initTelemetry(opts.service);
  const health: WorkerHealth = {
    ready: false,
    lastTickAt: Date.now(),
    consecutiveDbFailures: 0,
  };
  const healthServer = serveHealth(health, integer("WORKER_HEALTH_PORT", 8081));
  const db = openHoopDb();
  // Waiting for HoopDb or a long migration is progress, not a stall.
  const startupBeat = setInterval(
    () => (health.lastTickAt = Date.now()),
    10_000,
  );
  await waitForHoopDb(db);
  const expected = await expectedSchema();
  await waitForSchema(db, expected);
  // Live on every /readyz probe and before every claim: ledger, marker AND phase 2 now.
  health.schemaReady = async () =>
    (await schemaVerdict(expected, (work) => inTransaction(db, work))).ok;
  clearInterval(startupBeat);
  health.ready = true;
  health.lastTickAt = Date.now();
  const env = vendorBindings() as Parameters<typeof pipelineQueueDeps>[0];
  const rawQueue = postgresPipelineQueue(db);
  let current: StageMessage | undefined;
  const currentRef = () => current;
  // Everything sent from inside a stage carries the ids of the message being handled.
  const queue = {
    backend: rawQueue.backend,
    send: async (
      m: StageMessage,
      options?: Parameters<typeof rawQueue.send>[1],
    ) => rawQueue.send(await enrichMessage(m), options),
    sendBatch: async (
      ms: readonly StageMessage[],
      options?: Parameters<typeof rawQueue.sendBatch>[1],
    ) =>
      rawQueue.sendBatch(
        await Promise.all(ms.map((m) => enrichMessage(m))),
        options,
      ),
  };
  const impl = opts.impl?.(db, currentRef) ?? {};
  const handlers = opts.handlers(db, currentRef);
  const deps: StageQueueDeps = pipelineQueueDeps(env, {
    ...impl,
    queue,
    deadLetters: postgresDeadLetterStore(db),
    sink: sqlPipelineEventSink(db),
    handlers,
  });
  // Every handler, ours or vendored, runs in a span on the message's trace, and
  // the message in flight is tracked so fan-out continues that trace.
  for (const stage of Object.keys(deps.handlers) as QueuedStage[]) {
    const h = traced(stage, deps.handlers[stage]!);
    deps.handlers[stage] = async (message, recorder, attempt) => {
      current = message;
      health.lastTickAt = Date.now(); // progress inside a batch
      try {
        return await h(message, recorder, attempt);
      } finally {
        current = undefined;
        health.lastTickAt = Date.now();
      }
    };
  }
  await attestReaders(opts.attests ?? [], db);
  const idleMs = integer("WORKER_IDLE_MS", 1_000);
  const batchSize = integer("WORKER_BATCH_SIZE", 10);
  const leaseSeconds = integer("WORKER_LEASE_SECONDS", 300);
  log.info("worker started", { count: opts.stages.length });
  const stageBindings = new Map(opts.stages.map(stage => {
    const consumer = postgresStageConsumer({ sql: db, stage, deps, batchSize, leaseSeconds });
    const binding = createWorkerRuntime({ source: consumer.source, batchSize }).bind(consumer.work);
    return [stage, { consumer, binding }] as const;
  }));
  const controller = new AbortController();
  const stop = () => {
    health.ready = false;
    controller.abort();
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  try {
    await runWorkerLoop({
      signal: controller.signal,
      idleMs,
      tick: async () => {
        // One cheap probe per tick: a failing HoopDb (e.g. rotated password) marks the worker unhealthy.
        try {
          await db.query("SELECT 1");
          health.consecutiveDbFailures = 0;
        } catch (err) {
          health.consecutiveDbFailures += 1;
          log.warn("hoopdb probe failed", {
            count: health.consecutiveDbFailures,
            ...errorFields(err),
          });
        }
        health.lastTickAt = Date.now();
        // The live gate, checked again before EVERY claim (each stage's batch) and before
        // the side tick. Current-tick semantics: a verdict holds only as of its final read,
        // so the most a closing gate can leave running is the one batch claimed just before
        // it closed: at most WORKER_BATCH_SIZE messages of one stage, which finish inside
        // their worker runtime tick (acked, retried or dead-lettered). A batch cut short
        // by a crash is re-delivered when its lease (WORKER_LEASE_SECONDS) expires. While
        // the gate is closed the worker claims nothing.
        const r = await workerTick({
          signal: controller.signal,
          stages: opts.stages,
          gate: () => gateOpen(health),
          poll: async (stage) => {
            try {
              const { consumer, binding } = stageBindings.get(stage)!;
              const count = await binding.tick(controller.signal);
              const p = count === 0
                ? { claimed: 0, acked: 0, retried: 0, exhausted: 0 }
                : consumer.result();
              if (p.claimed)
                log.info("poll", {
                  stage,
                  claimed: p.claimed,
                  acked: p.acked,
                  retried: p.retried,
                  exhausted: p.exhausted,
                });
              return p.claimed;
            } catch (err) {
              log.error("poll failed", { stage, ...errorFields(err) });
              return 0;
            }
          },
          ...(opts.tick
            ? {
                tick: async () => {
                  // Long side work (reconcile) keeps the progress clock moving while it runs.
                  const tickBeat = setInterval(
                    () => (health.lastTickAt = Date.now()),
                    10_000,
                  );
                  try {
                    await opts.tick!(db, new Date());
                  } catch (err) {
                    log.error("tick failed", errorFields(err));
                  } finally {
                    clearInterval(tickBeat);
                  }
                },
              }
            : {}),
        });
        return r.claimed;
      },
    });
  } finally {
    health.ready = false;
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
    // No new deliveries can start; the active delivery/side tick has drained.
    // A crash or forced kill still recovers through the unchanged lease expiry.
    // Readiness probes use HoopDb too: drain HTTP requests before ending its pool.
    const serverCleanup = await Promise.allSettled([
      new Promise<void>((resolve, reject) => healthServer.close(error => error ? reject(error) : resolve())),
    ]);
    const cleanup = [...serverCleanup, ...await Promise.allSettled([
      shutdownTelemetry(),
      db.end(),
    ])];
    const failed = cleanup.filter(r => r.status === "rejected");
    for (const result of failed) if (result.status === "rejected") log.warn("worker shutdown failed", errorFields(result.reason));
    if (failed.length) process.exit(1);
  }
  process.exit(0);
}

/** The object-store reader and raw store for this deployment, from env. */
export function rawStoreOrThrow() {
  const env = vendorBindings();
  return env;
}
