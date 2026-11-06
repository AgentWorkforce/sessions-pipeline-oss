/**
 * The stage registry and the `PipelineQueue` backends. The Postgres backend runs
 * against the real `supabase/migrations` applied to local PGlite as
 * `service_role`; the Cloudflare backend against a recording stand-in for the
 * queue binding. Synthetic ids only; no hosted database or live queue.
 */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import type { Bindings } from "../src/env.js";
import {
  cloudflarePipelineQueue,
  InMemoryDeadLetterStore,
  PipelineRecorder,
  STORE_TIMEOUT_MS,
  InMemoryPipelineEventSink,
  isQueuedStage,
  manifestWorkKey,
  MemoryPipelineQueue,
  objectLandedHandler,
  PIPELINE_STAGES,
  PipelineError,
  pipelineErrorInfo,
  pipelineQueueDeps,
  pollPostgresStage,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  postgresStageConsumer,
  type PostgresConsumerOptions,
  QUEUED_STAGES,
  replayDeadLetters,
  sessionIndexedHandler,
  STAGE_REGISTRY,
  stageBindings,
  stageForQueue,
  stageQueueName,
  uuidv7,
  type ObjectLandedPayload,
  type PipelineQueue,
  type QueuedStage,
  type SqlExecutor,
  type RawPointer,
  type StageMessage,
  type StageQueueDeps,
  type TraceContext,
} from "../src/lib/pipeline/index.js";
import { createWorkerRuntime } from "../src/runtime/workers.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const WORKSPACE = "ws_synthetic";
const ORG = "org_synthetic";
const MANIFEST_KEY = `tenant-a/v1/ws=${WORKSPACE}/m=machine-synthetic/h=claude/d=2026/10/25/s=sess-synthetic-1/manifest.json`;

function ctx(traceId = uuidv7()): TraceContext {
  return { traceId, orgId: ORG, workspaceId: WORKSPACE };
}

function message(
  stage: QueuedStage,
  workKey = `${stage}-work`,
  payload: unknown = {},
): StageMessage {
  return { v: 1, stage, ctx: ctx(), workKey, payload };
}

function landed(payload: Partial<ObjectLandedPayload> = {}): StageMessage {
  const body: ObjectLandedPayload = {
    key: MANIFEST_KEY,
    kind: "manifest",
    etag: "etag-1",
    ...payload,
  };
  return {
    v: 1,
    stage: "object-landed",
    ctx: ctx(),
    workKey: manifestWorkKey(body),
    revision: 1,
    payload: body,
  };
}

beforeEach(() => {
  vi.spyOn(console, "info").mockImplementation(() => {});
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
});
afterEach(() => {
  vi.restoreAllMocks();
});

describe("stage registry", () => {
  it("registers the queued stages", () => {
    expect(QUEUED_STAGES).toEqual([
      "object-landed",
      "session-indexed",
      "summary-short",
      "summary-long",
      "status-classify",
      "file-normalize",
      "file-maintenance",
      "backfill-activation",
    ]);
    expect(
      QUEUED_STAGES.filter((stage) => STAGE_REGISTRY[stage].implemented),
    ).toEqual(["object-landed", "session-indexed", "backfill-activation"]);
    const bindings = new Set<string>();
    for (const stage of QUEUED_STAGES) {
      const definition = STAGE_REGISTRY[stage];
      expect(definition.stage).toBe(stage);
      expect(PIPELINE_STAGES).toContain(stage);
      expect(definition.maxAttempts).toBeGreaterThan(0);
      for (const next of definition.next)
        expect(isQueuedStage(next)).toBe(true);
      bindings.add(definition.binding);
    }
    expect(bindings.size).toBe(QUEUED_STAGES.length);
  });

  it("names a queue and a DLQ for every stage and routes them back", () => {
    for (const stage of QUEUED_STAGES) {
      expect(stageForQueue(stageQueueName(stage))).toEqual({
        stage,
        dlq: false,
      });
      expect(stageForQueue(stageQueueName(stage, "-dev-dlq"))).toEqual({
        stage,
        dlq: true,
      });
    }
    expect(isQueuedStage("ingest")).toBe(false);
    expect(isQueuedStage("storage.event")).toBe(false);
  });

  it("quarantines work for a registered stub stage until it is implemented", async () => {
    const queue = new MemoryPipelineQueue();
    const sink = new InMemoryPipelineEventSink();
    const deadLetters = postgresDeadLetterStore(db);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue,
      sink,
      deadLetters,
    });
    for (const stage of QUEUED_STAGES.filter(
      (stage) => !STAGE_REGISTRY[stage].implemented,
    ))
      expect(deps.handlers[stage]).toBeUndefined();
    await postgresPipelineQueue(db).send(message("summary-short"));
    const result = await pollPostgresStage({
      sql: db,
      stage: "summary-short",
      deps,
    });
    expect(result).toMatchObject({ claimed: 1, acked: 1 });
    expect(await deadLetters.list({ stage: "summary-short" })).toEqual([
      expect.objectContaining({
        id: `summary-short:${ORG}:${WORKSPACE}:summary-short-work`,
      }),
    ]);
    expect(sink.events.at(-1)).toMatchObject({
      stage: "summary-short",
      status: "dead_lettered",
      error: { code: "stage_not_configured" },
    });
  });

  it("takes a handler for a stub stage from the deployment", async () => {
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: new MemoryPipelineQueue(),
      handlers: { "summary-short": async () => {} },
    });
    expect(deps.handlers["summary-short"]).toBeTypeOf("function");
  });
});

describe("object-landed fan-out", () => {
  function deps(queue: MemoryPipelineQueue) {
    return pipelineQueueDeps({} as Bindings, {
      queue,
      sink: new InMemoryPipelineEventSink(),
    });
  }

  it("carries provider, bucket and scrub verdict onto the pointer", async () => {
    const queue = new MemoryPipelineQueue();
    const handler = deps(queue).handlers["object-landed"]!;
    const body = landed({ provider: "s3", bucket: "raw", scrubbed: true });
    await handler(body, { record: async () => {} } as never, 1);
    const [indexed] = queue.take("session-indexed");
    expect(indexed!.workKey).toBe(`${MANIFEST_KEY}@etag-1`);
    expect(indexed!.payload).toMatchObject({
      manifestKey: MANIFEST_KEY,
      provider: "s3",
      bucket: "raw",
      scrubbed: true,
    });
    expect((indexed!.payload as RawPointer).deleted).toBeUndefined();
  });

  it("forwards a deleted manifest to the index only, under its own work key", async () => {
    const queue = new MemoryPipelineQueue();
    const handler = deps(queue).handlers["object-landed"]!;
    const body = landed({ deleted: true, etag: undefined });
    expect(body.workKey).toBe(`${MANIFEST_KEY}@deleted`);
    await handler(body, { record: async () => {} } as never, 1);
    expect(queue.take()).toEqual([
      expect.objectContaining({
        stage: "session-indexed",
        workKey: `${MANIFEST_KEY}@deleted`,
        payload: expect.objectContaining({ deleted: true }),
      }),
    ]);
  });

  it("passes the trace context and a skip through the index", async () => {
    const seen: TraceContext[] = [];
    const handler = sessionIndexedHandler({
      async upsertRawPointer(_pointer, context) {
        seen.push(context);
        return { status: "skipped", reason: "stale_revision" };
      },
    });
    const body = message("session-indexed", "k", { manifestKey: MANIFEST_KEY });
    expect(await handler(body, {} as never, 1)).toEqual({
      status: "skipped",
      reason: "stale_revision",
    });
    expect(seen).toEqual([body.ctx]);
  });
});

describe("replay and log exports", () => {
  it("sends every letter even when a log export stalls, and returns within the deadline", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout"] });
    try {
      vi.spyOn(PipelineRecorder.prototype, "flush").mockImplementation(
        () => new Promise<void>(() => {}),
      );
      const deadLetters = new InMemoryDeadLetterStore();
      for (const key of ["one", "two", "three"])
        await deadLetters.quarantine(
          message("session-indexed", key),
          new Date(),
        );
      const sent: string[] = [];
      let done = false;
      const replay = replayDeadLetters(
        {
          deadLetters,
          send: async (_stage, body) => {
            sent.push(body.workKey);
          },
          sink: new InMemoryPipelineEventSink(),
        },
        { dryRun: false },
      ).then((result) => {
        done = true;
        return result;
      });
      await vi.advanceTimersByTimeAsync(0);
      // Every send happened while the exports hang.
      expect(sent.sort()).toEqual(["one", "three", "two"]);
      expect(done).toBe(false);
      await vi.advanceTimersByTimeAsync(STORE_TIMEOUT_MS);
      expect((await replay).sent).toHaveLength(3);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("cloudflare backend", () => {
  class RecordingQueue {
    readonly sends: { body: StageMessage; options: unknown }[] = [];
    readonly batches: { body: StageMessage }[][] = [];
    async send(body: StageMessage, options: unknown) {
      this.sends.push({ body, options });
    }
    async sendBatch(batch: { body: StageMessage }[]) {
      this.batches.push(batch);
    }
  }

  it("sends to the stage's binding and batches by stage in hundreds", async () => {
    const landedQueue = new RecordingQueue();
    const indexedQueue = new RecordingQueue();
    const env = {
      PIPELINE_OBJECT_LANDED: landedQueue,
      PIPELINE_SESSION_INDEXED: indexedQueue,
    } as unknown as Bindings;
    expect(Object.keys(stageBindings(env))).toEqual([
      "object-landed",
      "session-indexed",
    ]);
    const queue = cloudflarePipelineQueue(stageBindings(env));
    await queue.send(message("object-landed"), { delaySeconds: 5 });
    expect(landedQueue.sends[0]!.options).toEqual({
      contentType: "json",
      delaySeconds: 5,
    });
    await queue.sendBatch([
      ...Array.from({ length: 150 }, (_, i) =>
        message("session-indexed", `k${i}`),
      ),
      message("object-landed"),
    ]);
    expect(indexedQueue.batches.map((batch) => batch.length)).toEqual([
      100, 50,
    ]);
    expect(landedQueue.batches.map((batch) => batch.length)).toEqual([1]);
  });

  it("refuses a stage with no binding (retryable) and a malformed message (permanent)", async () => {
    const queue = cloudflarePipelineQueue({});
    const unbound = await queue
      .send(message("summary-long"))
      .catch((error) => error);
    expect(pipelineErrorInfo(unbound, "x")).toMatchObject({
      code: "stage_queue_unbound",
      retryable: true,
    });
    const bad = await queue
      .send({ ...message("object-landed"), stage: "ingest" })
      .catch((error) => error);
    expect(bad).toBeInstanceOf(PipelineError);
    expect(pipelineErrorInfo(bad, "x")).toMatchObject({
      code: "invalid_stage_message",
      retryable: false,
    });
  });
});

let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_directory_search_worker.sql"),
      "utf8",
    ),
  );
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_token_verifier.sql"),
      "utf8",
    ),
  );
  await db.exec(
    readFileSync(
      supabaseFile("bootstrap/session_ingest_roles.sql"),
      "utf8",
    ),
  );
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
  await db.exec("SET ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec(
    "TRUNCATE sessions.pipeline_jobs; TRUNCATE sessions.pipeline_dead_letters; TRUNCATE sessions.pipeline_events",
  );
});

/** Make every waiting job claimable now (backoff and leases skipped). */
const fastForward = () =>
  db.exec("UPDATE sessions.pipeline_jobs SET visible_at = now()");

async function jobs() {
  return (
    await db.query<{
      stage: string;
      dlq: boolean;
      attempts: number;
      work_key: string;
    }>(
      "SELECT stage, dlq, attempts, work_key FROM sessions.pipeline_jobs ORDER BY id",
    )
  ).rows;
}

describe("postgres backend", () => {
  function postgresDeps(
    handlers: StageQueueDeps["handlers"],
  ): StageQueueDeps & { sink: InMemoryPipelineEventSink } {
    return {
      sink: new InMemoryPipelineEventSink(),
      deadLetters: postgresDeadLetterStore(db),
      handlers,
    };
  }

  function stageRuntime(deps: StageQueueDeps, overrides: Partial<PostgresConsumerOptions> = {}) {
    const consumer = postgresStageConsumer({ sql: db, stage: "session-indexed", deps, ...overrides });
    return { consumer, binding: createWorkerRuntime({ source: consumer.source, batchSize: 3 }).bind(consumer.work) };
  }

  it("drains the started delivery on shutdown and recovers unstarted leases after restart", async () => {
    const controller = new AbortController();
    const calls: string[] = [];
    await postgresPipelineQueue(db).sendBatch(["one", "two", "three"].map(key => message("session-indexed", key)));
    const deps = postgresDeps({ "session-indexed": async body => { calls.push(body.workKey); controller.abort(); } });
    const first = stageRuntime(deps);
    await first.binding.tick(controller.signal);
    expect(first.consumer.result()).toEqual({ claimed: 3, acked: 1, retried: 0, exhausted: 0 });
    expect(calls).toEqual(["one"]);
    expect((await jobs()).map(j => [j.work_key, j.attempts])).toEqual([["two", 0], ["three", 0]]);
    const restarted = stageRuntime(postgresDeps({ "session-indexed": async body => { calls.push(body.workKey); } }));
    expect(await restarted.binding.tick()).toBe(0);
    await fastForward();
    expect(await restarted.binding.tick()).toBe(2);
    expect(calls).toEqual(["one", "two", "three"]);
    expect(await jobs()).toEqual([]);
  });

  it("finishes a delivery when shutdown arrives during its start query", async () => {
    const controller = new AbortController();
    let handled = 0;
    await postgresPipelineQueue(db).send(message("session-indexed", "started"));
    const sql: SqlExecutor = { async query(text, params) {
      const result = await db.query(text, params);
      if (text.includes("SET dlq = dlq OR attempts")) controller.abort();
      return result as never;
    } };
    const { binding } = stageRuntime(postgresDeps({ "session-indexed": async () => { handled++; } }), { sql });
    await binding.tick(controller.signal);
    expect(handled).toBe(1);
    expect(await jobs()).toEqual([]);
  });

  it("does not start a leased delivery when shutdown arrives during claim", async () => {
    const controller = new AbortController();
    await postgresPipelineQueue(db).send(message("session-indexed", "claimed"));
    let handled = 0;
    const sql: SqlExecutor = { async query(text, params) {
      const result = await db.query(text, params);
      if (text.startsWith("WITH next AS")) controller.abort();
      return result as never;
    } };
    const { binding } = stageRuntime(postgresDeps({ "session-indexed": async () => { handled++; } }), { sql });
    await binding.tick(controller.signal);
    expect(handled).toBe(0);
    expect((await jobs()).map(j => [j.work_key, j.attempts])).toEqual([["claimed", 0]]);
  });

  it.each([false, true])("does not settle a new owner's lease when the original handler finishes late (retry=%s)", async retry => {
    await postgresPipelineQueue(db).send(message("session-indexed", "late"));
    const old = stageRuntime(postgresDeps({ "session-indexed": async () => {
      await db.exec("UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid()");
      if (retry) throw new Error("transient failure after lease loss");
    } }));
    await old.binding.tick();
    expect(old.consumer.result().acked).toBe(0);
    expect(old.consumer.result().retried).toBe(0);
    expect((await jobs()).map(j => [j.work_key, j.attempts])).toEqual([["late", 1]]);
    await fastForward();
    const next = stageRuntime(postgresDeps({ "session-indexed": async (_body, _recorder, attempt) => { expect(attempt).toBe(2); } }));
    await next.binding.tick();
    expect(await jobs()).toEqual([]);
  });

  it("recovers an unsettled delivery after an acknowledgment failure without spending waiting attempts", async () => {
    await postgresPipelineQueue(db).sendBatch(["one", "two", "three"].map(key => message("session-indexed", key)));
    const seen: string[] = [];
    const deps = postgresDeps({ "session-indexed": async body => { seen.push(body.workKey); } });
    let failAck = true;
    const sql: SqlExecutor = { async query(text, params) {
      if (text.startsWith("DELETE FROM") && failAck) { failAck = false; throw new Error("ack connection lost"); }
      return await db.query(text, params) as never;
    } };
    const worker = stageRuntime(deps, { sql });
    await expect(worker.binding.tick()).rejects.toThrow("ack connection lost");
    expect(seen).toEqual(["one"]);
    expect((await jobs()).map(j => [j.work_key, j.attempts])).toEqual([["one", 1], ["two", 0], ["three", 0]]);
    await fastForward();
    // The same persistent binding resumes after a transient database failure.
    await worker.binding.tick();
    expect(seen).toEqual(["one", "one", "two", "three"]);
    expect(await jobs()).toEqual([]);
  });

  it("carries a manifest from object-landed to the index", async () => {
    const queue = postgresPipelineQueue(db);
    const deadLetters = postgresDeadLetterStore(db);
    const indexed: RawPointer[] = [];
    const deps = postgresDeps({
      "object-landed": objectLandedHandler({
        senders: { send: (stage, body) => queue.send({ ...body, stage }) },
        quarantine: (body, at) => deadLetters.quarantine(body, at),
        transformEnabled: false,
      }),
      "session-indexed": sessionIndexedHandler({
        async upsertRawPointer(pointer) {
          indexed.push(pointer);
        },
      }),
    });
    await queue.send(landed());
    expect(
      await pollPostgresStage({ sql: db, stage: "object-landed", deps }),
    ).toEqual({ claimed: 1, acked: 1, retried: 0, exhausted: 0 });
    expect(await jobs()).toEqual([
      {
        stage: "session-indexed",
        dlq: false,
        attempts: 0,
        work_key: `${MANIFEST_KEY}@etag-1`,
      },
    ]);
    await pollPostgresStage({ sql: db, stage: "session-indexed", deps });
    expect(indexed).toEqual([
      expect.objectContaining({ manifestKey: MANIFEST_KEY }),
    ]);
    expect(await jobs()).toEqual([]);
    expect(
      deps.sink.events.map((event) => `${event.stage}:${event.status}`),
    ).toEqual([
      "file-normalize:skipped",
      "object-landed:ok",
      "session-indexed:ok",
    ]);
  });

  it("holds a delayed send, then a retried job, until it is visible", async () => {
    const queue = postgresPipelineQueue(db);
    await queue.send(message("session-indexed"), { delaySeconds: 600 });
    const deps = postgresDeps({ "session-indexed": async () => {} });
    expect(
      (await pollPostgresStage({ sql: db, stage: "session-indexed", deps }))
        .claimed,
    ).toBe(0);
    await fastForward();
    // No handler and no store: the job is retried with backoff, not lost.
    const stuck = { ...deps, handlers: {} };
    stuck.deadLetters = {
      ...deps.deadLetters,
      quarantine: async () => {
        throw new Error("store down");
      },
    };
    await pollPostgresStage({ sql: db, stage: "session-indexed", deps: stuck });
    expect(
      (await pollPostgresStage({ sql: db, stage: "session-indexed", deps }))
        .claimed,
    ).toBe(0);
  });

  it("claims each job once across consumers and ignores an expired lease's ack", async () => {
    const queue = postgresPipelineQueue(db);
    await queue.sendBatch([
      message("session-indexed", "a"),
      message("session-indexed", "b"),
      message("session-indexed", "c"),
    ]);
    const seen: string[] = [];
    const deps = postgresDeps({
      "session-indexed": async (body) => {
        seen.push(body.workKey);
      },
    });
    const first = await pollPostgresStage({
      sql: db,
      stage: "session-indexed",
      deps,
      batchSize: 2,
    });
    const second = await pollPostgresStage({
      sql: db,
      stage: "session-indexed",
      deps,
      batchSize: 2,
    });
    expect([first.claimed, second.claimed]).toEqual([2, 1]);
    expect(seen.sort()).toEqual(["a", "b", "c"]);

    // Only the lease holder can ack: a stale consumer's DELETE matches nothing.
    await queue.send(message("session-indexed", "d"));
    const { rows } = await db.query<{ id: string }>(
      `UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid(), attempts = 1
        RETURNING id::text AS id`,
    );
    await db.query(
      "DELETE FROM sessions.pipeline_jobs WHERE id = $1::bigint AND lease_id = $2::uuid",
      [rows[0]!.id, crypto.randomUUID()],
    );
    expect(await jobs()).toHaveLength(1);
  });

  it("retries with backoff, then dead-letters into pipeline_dead_letters and replays", async () => {
    const queue = postgresPipelineQueue(db);
    let healthy = false;
    let calls = 0;
    const deps = postgresDeps({
      "session-indexed": async () => {
        calls += 1;
        if (!healthy) throw new Error("connection reset");
      },
    });
    await queue.send(message("session-indexed", "w"));
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const result = await pollPostgresStage({
        sql: db,
        stage: "session-indexed",
        deps,
        maxAttempts: 3,
      });
      expect(result).toMatchObject(
        attempt < 3 ? { retried: 1 } : { exhausted: 1 },
      );
      if (attempt < 3) {
        // Backoff: not visible again until fast-forwarded.
        expect(
          (await pollPostgresStage({ sql: db, stage: "session-indexed", deps }))
            .claimed,
        ).toBe(0);
        await fastForward();
      }
    }
    expect(calls).toBe(3);
    expect(await jobs()).toEqual([
      { stage: "session-indexed", dlq: true, attempts: 0, work_key: "w" },
    ]);
    // The DLQ delivery quarantines and acks; the handler does not run again.
    expect(
      await pollPostgresStage({ sql: db, stage: "session-indexed", deps }),
    ).toMatchObject({ claimed: 1, acked: 1 });
    expect(calls).toBe(3);
    expect(await jobs()).toEqual([]);
    const [letter] = await deps.deadLetters.list({});
    expect(letter).toMatchObject({
      id: `session-indexed:${ORG}:${WORKSPACE}:w`,
      state: "dead",
      deadCount: 1,
    });
    expect(deps.sink.events.at(-1)).toMatchObject({
      status: "dead_lettered",
      error: { code: "retries_exhausted" },
    });

    healthy = true;
    const replay = await replayDeadLetters(
      {
        deadLetters: deps.deadLetters,
        send: (stage, body) => queue.send({ ...body, stage }),
        sink: deps.sink,
      },
      { dryRun: false },
    );
    expect(replay.sent).toEqual([`session-indexed:${ORG}:${WORKSPACE}:w`]);
    // A second replay finds nothing: the letter is claimed.
    expect(
      (
        await replayDeadLetters(
          {
            deadLetters: deps.deadLetters,
            send: (stage, body) => queue.send({ ...body, stage }),
            sink: deps.sink,
          },
          { dryRun: false },
        )
      ).sent,
    ).toEqual([]);
    await pollPostgresStage({ sql: db, stage: "session-indexed", deps });
    expect(calls).toBe(4);
    expect((await deps.deadLetters.list({}))[0]).toMatchObject({
      state: "resolved",
      replayCount: 1,
    });
  });

  it("quarantines a permanent failure on the first delivery", async () => {
    const queue = postgresPipelineQueue(db);
    const deps = postgresDeps({
      "session-indexed": async () => {
        throw new PipelineError("invalid_pointer", "bad pointer", false);
      },
    });
    await queue.send(message("session-indexed", "p"));
    expect(
      await pollPostgresStage({ sql: db, stage: "session-indexed", deps }),
    ).toMatchObject({ claimed: 1, acked: 1 });
    expect(await deps.deadLetters.list({})).toEqual([
      expect.objectContaining({
        id: `session-indexed:${ORG}:${WORKSPACE}:p`,
        state: "dead",
      }),
    ]);
  });

  it("keeps retrying a dead-letter delivery rather than dropping it", async () => {
    const queue = postgresPipelineQueue(db);
    const deps = postgresDeps({
      "session-indexed": async () => {
        throw new Error("down");
      },
    });
    deps.deadLetters = {
      ...deps.deadLetters,
      quarantine: async () => {
        throw new Error("store down");
      },
    };
    await queue.send(message("session-indexed", "x"));
    await pollPostgresStage({
      sql: db,
      stage: "session-indexed",
      deps,
      maxAttempts: 1,
    });
    for (let i = 0; i < 5; i += 1) {
      await fastForward();
      await pollPostgresStage({ sql: db, stage: "session-indexed", deps });
    }
    expect(await jobs()).toEqual([
      { stage: "session-indexed", dlq: true, attempts: 5, work_key: "x" },
    ]);
  });

  it("keeps two tenants' dead letters for the same work key apart", async () => {
    const store = postgresDeadLetterStore(db);
    const a = message("session-indexed", "shared");
    const b = {
      ...message("session-indexed", "shared"),
      ctx: { ...ctx(), orgId: "org_other", workspaceId: WORKSPACE },
    };
    await store.quarantine(a, new Date());
    await store.quarantine(b, new Date());
    const letters = await store.list({});
    expect(letters).toHaveLength(2);
    expect(letters.map((l) => l.deadCount)).toEqual([1, 1]);
    // One tenant's success resolves only its own letter.
    const deps = postgresDeps({ "session-indexed": async () => {} });
    await postgresPipelineQueue(db).send(a);
    await pollPostgresStage({ sql: db, stage: "session-indexed", deps });
    // Both died in the same millisecond, so list order is not the point.
    expect(
      Object.fromEntries(
        (await store.list({})).map((l) => [l.message.ctx.orgId, l.state]),
      ),
    ).toEqual({ [ORG]: "resolved", org_other: "dead" });
  });

  it("dead-letters a job whose consumers keep dying instead of running it forever", async () => {
    const queue = postgresPipelineQueue(db);
    let calls = 0;
    const deps = postgresDeps({
      "session-indexed": async () => {
        calls += 1;
      },
    });
    await queue.send(message("session-indexed", "poison"));
    // Three deliveries started and none finished: each consumer died holding the lease.
    await db.exec(
      "UPDATE sessions.pipeline_jobs SET attempts = 3, lease_id = gen_random_uuid(), visible_at = now()",
    );
    expect(
      await pollPostgresStage({
        sql: db,
        stage: "session-indexed",
        deps,
        maxAttempts: 3,
      }),
    ).toMatchObject({ claimed: 1, acked: 1 });
    expect(calls).toBe(0);
    expect(await jobs()).toEqual([]);
    expect(deps.sink.events.at(-1)).toMatchObject({
      status: "dead_lettered",
      error: { code: "retries_exhausted" },
    });
  });

  it("settles each job before the next starts and keeps the rest of the batch claimed", async () => {
    const queue = postgresPipelineQueue(db);
    await queue.sendBatch([
      message("session-indexed", "one"),
      message("session-indexed", "two"),
      message("session-indexed", "three"),
    ]);
    const seen: { key: string; left: string[]; leased: number }[] = [];
    const deps = postgresDeps({
      "session-indexed": async (body) => {
        const rows = (
          await db.query<{ work_key: string; leased: boolean }>(
            `SELECT work_key, visible_at > now() + interval '200 seconds' AS leased
               FROM sessions.pipeline_jobs ORDER BY id`,
          )
        ).rows;
        seen.push({
          key: body.workKey,
          left: rows.map((r) => r.work_key),
          leased: rows.filter((r) => r.leased).length,
        });
        if (body.workKey === "two")
          // Another consumer took job three after its lease ran out.
          await db.exec(
            "UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid() WHERE work_key = 'three'",
          );
      },
    });
    const result = await pollPostgresStage({
      sql: db,
      stage: "session-indexed",
      deps,
      leaseSeconds: 300,
    });
    expect(seen).toEqual([
      { key: "one", left: ["one", "two", "three"], leased: 3 },
      { key: "two", left: ["two", "three"], leased: 2 },
    ]);
    // Job three's claim was lost, so this consumer left it alone.
    expect(result).toMatchObject({ claimed: 3, acked: 2 });
    expect((await jobs()).map((j) => j.work_key)).toEqual(["three"]);
  });

  it("counts an attempt only when a job starts, not for jobs waiting behind a crash", async () => {
    const queue = postgresPipelineQueue(db);
    await queue.sendBatch([
      message("session-indexed", "first"),
      message("session-indexed", "second"),
      message("session-indexed", "third"),
    ]);
    // The consumer dies while running the first job of its batch.
    const dying = {
      query: async (text: string, params?: unknown[]) => {
        const result = await db.query(text, params);
        if (/SET dlq = dlq OR attempts/.test(text))
          throw new Error("consumer died");
        return result;
      },
    } as SqlExecutor;
    const deps = postgresDeps({ "session-indexed": async () => {} });
    await expect(
      pollPostgresStage({ sql: dying, stage: "session-indexed", deps }),
    ).rejects.toThrow("consumer died");
    expect((await jobs()).map((j) => [j.work_key, j.attempts])).toEqual([
      ["first", 1],
      ["second", 0],
      ["third", 0],
    ]);
  });

  it("uses the deployment's dead letter store inside object-landed too", async () => {
    const deadLetters = postgresDeadLetterStore(db);
    const failing: PipelineQueue = {
      backend: "memory",
      send: async (body) => {
        if (body.stage === "file-normalize") throw new Error("queue down");
      },
      sendBatch: async () => {},
    };
    const deps = pipelineQueueDeps(
      { PIPELINE_FILE_NORMALIZE_ENABLED: "true" } as Bindings,
      {
        queue: failing,
        deadLetters,
        sink: new InMemoryPipelineEventSink(),
        transform: { normalize: async () => ({}) },
      },
    );
    await deps.handlers["object-landed"]!(
      landed(),
      { record: async () => {} } as never,
      1,
    );
    expect((await deadLetters.list({}))[0]).toMatchObject({
      stage: "file-normalize",
      state: "dead",
    });
  });

  it("records the payload's revision on the object-landed outcome", async () => {
    const queue = postgresPipelineQueue(db);
    const deps = postgresDeps({ "object-landed": async () => {} });
    const body = landed({ revision: 7 });
    delete body.revision;
    await queue.send(body);
    await pollPostgresStage({ sql: db, stage: "object-landed", deps });
    expect(deps.sink.events.at(-1)).toMatchObject({
      stage: "object-landed",
      status: "ok",
      attrs: { revision: 7 },
    });
  });

  it("writes the session and machine beside the tenant on jobs and dead letters", async () => {
    const body = {
      ...message("session-indexed", "traced"),
      ctx: { ...ctx(), sessionId: "sess-synthetic-9", machineId: "machine-9" },
    };
    await postgresPipelineQueue(db).send(body);
    await postgresDeadLetterStore(db).quarantine(body, new Date());
    for (const table of ["pipeline_jobs", "pipeline_dead_letters"])
      expect(
        (
          await db.query(
            `SELECT org_id, session_id, machine_id FROM sessions.${table}`,
          )
        ).rows,
      ).toEqual([
        {
          org_id: ORG,
          session_id: "sess-synthetic-9",
          machine_id: "machine-9",
        },
      ]);
  });

  it("fills the session on a later death of a letter that had none", async () => {
    const bare = message("session-indexed", "late-session");
    const store = postgresDeadLetterStore(db);
    await store.quarantine(bare, new Date());
    await store.quarantine(
      {
        ...bare,
        ctx: { ...bare.ctx, sessionId: "sess-late", machineId: "m-late" },
      },
      new Date(),
    );
    // A death without them never clears what is known.
    await store.quarantine(bare, new Date());
    expect(
      (
        await db.query(
          "SELECT session_id, machine_id, dead_count FROM sessions.pipeline_dead_letters",
        )
      ).rows,
    ).toEqual([
      { session_id: "sess-late", machine_id: "m-late", dead_count: 3 },
    ]);
  });

  it("refuses rows the schema does not allow and keeps tenants out", async () => {
    await expect(
      db.query(
        `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
         VALUES ('ingest', 'k', $1, 'o', 'w', '{}')`,
        [uuidv7()],
      ),
    ).rejects.toThrow(/pipeline_jobs_stage_check/);
    await db.exec("RESET ROLE; SET ROLE authenticated");
    try {
      await expect(
        db.query("SELECT 1 FROM sessions.pipeline_jobs"),
      ).rejects.toThrow(/permission denied/);
    } finally {
      await db.exec("RESET ROLE; SET ROLE service_role");
    }
  });
});

describe("20261025000600 on a database with rows under the old stage names", () => {
  it("renames events and dead letters, ids and messages included", async () => {
    const old = new PGlite({ extensions: { btree_gin, pg_trgm } });
    try {
      await old.exec(
        readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"),
      );
      await old.exec(
        readFileSync(
          supabaseFile("bootstrap/session_directory_search_worker.sql"),
          "utf8",
        ),
      );
      await old.exec(
        readFileSync(
          supabaseFile("bootstrap/session_token_verifier.sql"),
          "utf8",
        ),
      );
      await old.exec(
        readFileSync(
          supabaseFile("bootstrap/session_ingest_roles.sql"),
          "utf8",
        ),
      );
      const files = readdirSync(supabaseFile("migrations/")).sort();
      const queue = files.indexOf("20261025000600_pipeline_queue.sql");
      expect(queue).toBeGreaterThan(0);
      for (const file of files.slice(0, queue))
        await old.exec(
          readFileSync(supabaseFile(`migrations/${file}`), "utf8"),
        );
      const traceId = uuidv7();
      await old.query(
        `INSERT INTO sessions.pipeline_events (trace_id, stage, status)
         SELECT $1::uuid, s, 'ok' FROM unnest(ARRAY['storage.event', 'index.upsert', 'transform', 'ingest']) AS s`,
        [traceId],
      );
      await old.query(
        `INSERT INTO sessions.pipeline_dead_letters (id, stage, trace_id, message)
         VALUES ('transform:k@e', 'transform', $1::uuid, '{"v":1,"stage":"transform","workKey":"k@e"}')`,
        [traceId],
      );
      for (const file of files.slice(queue))
        await old.exec(
          readFileSync(supabaseFile(`migrations/${file}`), "utf8"),
        );
      expect(
        (
          await old.query<{ stage: string }>(
            "SELECT stage FROM sessions.pipeline_events ORDER BY stage",
          )
        ).rows.map((row) => row.stage),
      ).toEqual([
        "file-normalize",
        "ingest",
        "object-landed",
        "session-indexed",
      ]);
      expect(
        (
          await old.query(
            "SELECT id, stage, message->>'stage' AS message_stage FROM sessions.pipeline_dead_letters",
          )
        ).rows,
      ).toEqual([
        {
          id: "file-normalize:k@e",
          stage: "file-normalize",
          message_stage: "file-normalize",
        },
      ]);
    } finally {
      await old.close();
    }
    // A second PGlite with every migration: slow beside a parallel suite.
  }, 60_000);
});
