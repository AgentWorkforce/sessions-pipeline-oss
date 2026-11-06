import { Hono } from "hono";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createApp } from "../src/app.js";
import type { AppEnv, AuthContext, Bindings } from "../src/env.js";
import {
  deadLetterId,
  errorReporterFromEnv,
  formatTraceparent,
  InMemoryDeadLetterStore,
  InMemoryPipelineEventSink,
  sessionIndexedHandler,
  isSafeIdentifier,
  isTraceId,
  telemetryContext,
  telemetryFields,
  telemetryIdHash,
  telemetryIdKey,
  telemetryString,
  logOnlyPipelineEventSink,
  manifestWorkKey,
  MAX_TRACE_EVENTS,
  REPLAY_LEASE_MS,
  objectKeyIdentity,
  otlpErrorPayload,
  parseSentryDsn,
  PipelineError,
  pipelineErrorInfo,
  PipelineRecorder,
  postgrestDeadLetterStore,
  postgrestPipelineEventSink,
  replayDeadLetters,
  sentryEnvelope,
  stageForQueue,
  stageQueueName,
  objectLandedHandler,
  summarizeTrace,
  traceIdFromHeaders,
  fileNormalizeHandler,
  uuidv7,
  type PipelineEventSink,
  type PipelineStage,
  type RawPointer,
  type SessionTransform,
  type StageMessage,
  type StageQueueDeps,
  type TraceContext,
} from "../src/lib/pipeline/index.js";
import {
  createPipelineOpsRoutes,
  createPipelineRoutes,
  type PipelineRouteDeps,
} from "../src/routes/pipeline.js";
import { FakePipelineQueues } from "./support/pipeline-queues.js";

const WORKSPACE = "ws_synthetic";
const ORG = "org-synthetic";
const SCOPE = { orgId: ORG, workspaceId: WORKSPACE };
const MACHINE = "machine-synthetic";
const SESSION = "sess-synthetic-1";
const MANIFEST_KEY = `tenant-a/v1/ws=${WORKSPACE}/m=${MACHINE}/h=claude/d=2026/10/25/s=${SESSION}/manifest.json`;
/** Synthetic: a test-only key, never a real secret. */
const TELEMETRY_KEY = "synthetic-telemetry-key-0123456789abcdef";
const CHUNK_KEY = `tenant-a/v1/ws=${WORKSPACE}/m=${MACHINE}/h=claude/d=2026/10/25/s=${SESSION}/raw/0000000000-0-abcdef12.jsonl.zst`;

let info: ReturnType<typeof vi.spyOn>;
let warn: ReturnType<typeof vi.spyOn>;
let errorLog: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  info = vi.spyOn(console, "info").mockImplementation(() => {});
  warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  errorLog = vi.spyOn(console, "error").mockImplementation(() => {});
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function logLines(spy: ReturnType<typeof vi.spyOn>): any[] {
  return spy.mock.calls.map((call: unknown[]) => JSON.parse(String(call[0])));
}

/** A pointer index fake keyed like the real one: one row per manifest key. */
class FakePointerIndex {
  readonly rows = new Map<string, RawPointer>();
  calls = 0;
  failuresLeft = 0;
  async upsertRawPointer(pointer: RawPointer) {
    this.calls += 1;
    if (this.failuresLeft > 0) {
      this.failuresLeft -= 1;
      throw new Error("connection reset: postgres://user:pw@host/db");
    }
    this.rows.set(pointer.manifestKey, pointer);
  }
}

/** A transform that fails until `healthy`, idempotent by input key once it works. */
class FakeTransform implements SessionTransform {
  healthy = false;
  calls = 0;
  readonly outputs = new Map<string, number>();
  async normalize(pointer: RawPointer) {
    this.calls += 1;
    if (!this.healthy)
      throw new Error("model provider said: secret prompt text here");
    this.outputs.set(
      pointer.manifestKey,
      (this.outputs.get(pointer.manifestKey) ?? 0) + 1,
    );
    return {
      outputKey: pointer.manifestKey.replace("manifest.json", "normalized/v1/"),
      revision: 1,
      normalizedVersion: 2,
      contentSha256: "0".repeat(64),
      outputSha256: "0".repeat(64),
      outputBytes: 0,
      convergenceEventCount: 0,
      unprojectedRecords: 0,
      outputReused: false,
      outputDivergent: false,
    };
  }
}

function pipeline(options: { transformEnabled?: boolean } = {}) {
  const queues = new FakePipelineQueues();
  const sink = new InMemoryPipelineEventSink();
  const deadLetters = new InMemoryDeadLetterStore();
  const index = new FakePointerIndex();
  const transform = new FakeTransform();
  const deps: StageQueueDeps = {
    sink,
    deadLetters,
    handlers: {
      "object-landed": objectLandedHandler({
        senders: { send: (stage, message) => queues.send(stage, message) },
        quarantine: (message, at) => deadLetters.quarantine(message, at),
        transformEnabled: options.transformEnabled ?? true,
      }),
      "session-indexed": sessionIndexedHandler(index),
      "file-normalize": fileNormalizeHandler(transform),
    },
  };
  return { queues, sink, deadLetters, index, transform, deps };
}

function landed(traceId: string, key = MANIFEST_KEY): StageMessage {
  return {
    v: 1,
    stage: "object-landed",
    ctx: {
      traceId,
      orgId: ORG,
      workspaceId: WORKSPACE,
      machineId: MACHINE,
      sessionId: SESSION,
      harness: "claude",
    },
    workKey: `${key}@v1`,
    payload: {
      key,
      kind: key.endsWith("manifest.json") ? "manifest" : "chunk",
      version: "v1",
    },
  };
}

describe("trace ids", () => {
  it("mints time-ordered UUIDv7 ids", () => {
    const first = uuidv7(1_700_000_000_000);
    const second = uuidv7(1_700_000_000_001);
    expect(isTraceId(first)).toBe(true);
    expect(first[14]).toBe("7");
    expect(["8", "9", "a", "b"]).toContain(first[19]);
    expect(first < second).toBe(true);
    expect(first.slice(0, 13).replace("-", "")).toBe(
      (1_700_000_000_000).toString(16).padStart(12, "0"),
    );
  });

  it("reads X-Relay-Trace-Id or traceparent and ignores anything malformed", () => {
    const id = uuidv7();
    const headers = (values: Record<string, string>) => (name: string) =>
      values[name];
    expect(
      traceIdFromHeaders(headers({ "X-Relay-Trace-Id": id.toUpperCase() })),
    ).toBe(id);
    expect(
      traceIdFromHeaders(headers({ traceparent: formatTraceparent(id) })),
    ).toBe(id);
    expect(
      traceIdFromHeaders(headers({ "X-Relay-Trace-Id": "rth_at_secret" })),
    ).toBeUndefined();
    expect(
      traceIdFromHeaders(
        headers({ traceparent: `00-${"0".repeat(32)}-${"1".repeat(16)}-01` }),
      ),
    ).toBeUndefined();
    expect(
      traceIdFromHeaders(
        headers({ "X-Relay-Trace-Id": "00000000-0000-0000-0000-000000000000" }),
      ),
    ).toBeUndefined();
  });

  it("recovers tenant, machine, harness and session from a canonical object key", () => {
    expect(objectKeyIdentity(CHUNK_KEY)).toEqual({
      workspaceId: WORKSPACE,
      machineId: MACHINE,
      harness: "claude",
      sessionId: SESSION,
    });
    expect(objectKeyIdentity("tenant-a/v2/whatever")).toBeNull();
  });

  it("names per-stage queues and DLQs and routes them back", () => {
    expect(stageQueueName("object-landed")).toBe(
      "relayhistory-pipeline-object-landed",
    );
    expect(stageQueueName("session-indexed", "-dev-dlq")).toBe(
      "relayhistory-pipeline-session-indexed-dev-dlq",
    );
    expect(stageForQueue("relayhistory-pipeline-file-normalize-dlq")).toEqual({
      stage: "file-normalize",
      dlq: true,
    });
    expect(stageForQueue("relayhistory-pipeline-session-indexed-dev")).toEqual({
      stage: "session-indexed",
      dlq: false,
    });
    expect(stageForQueue("relayhistory-session-events-dlq")).toBeNull();
  });

  it("echoes a well-formed trace id and logs it on the request line", async () => {
    const app = createApp();
    const id = uuidv7();
    const response = await app.request("/health", {
      headers: { "X-Relay-Trace-Id": id },
    });
    expect(response.headers.get("X-Relay-Trace-Id")).toBe(id);
    const request = logLines(info).find(
      (line) => line.message === "request completed",
    );
    expect(request.trace_id).toBe(id);

    const bogus = await app.request("/health", {
      headers: { "X-Relay-Trace-Id": "not a trace" },
    });
    expect(bogus.headers.get("X-Relay-Trace-Id")).toBeNull();
  });
});

describe("error tracking", () => {
  const ctx: TraceContext = {
    traceId: uuidv7(),
    orgId: ORG,
    workspaceId: WORKSPACE,
    machineId: MACHINE,
  };

  it("never forwards a foreign error's message", () => {
    const info = pipelineErrorInfo(
      new Error("password=hunter2 in SQL"),
      "transform_failed",
    );
    expect(info).toEqual({
      code: "transform_failed",
      name: "Error",
      retryable: true,
    });
    expect(JSON.stringify(info)).not.toContain("hunter2");
    expect(
      pipelineErrorInfo(
        new PipelineError("index_conflict", "stale manifest", false),
      ),
    ).toEqual({
      code: "index_conflict",
      name: "PipelineError",
      message: "stale manifest",
      retryable: false,
    });
  });

  it("selects the configured exporter and falls back to console without an endpoint", () => {
    // OTLP is the default; with no endpoint it is console only.
    expect(errorReporterFromEnv({}).kind).toBe("console");
    expect(
      errorReporterFromEnv({
        OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:4318",
      }).kind,
    ).toBe("console+otlp");
    expect(errorReporterFromEnv({ PIPELINE_ERROR_TRACKER: "none" }).kind).toBe(
      "none",
    );
    expect(errorReporterFromEnv({ PIPELINE_ERROR_TRACKER: "otlp" }).kind).toBe(
      "console",
    );
    expect(
      errorReporterFromEnv({
        PIPELINE_ERROR_TRACKER: "otlp",
        OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:4318",
      }).kind,
    ).toBe("console+otlp");
    expect(
      errorReporterFromEnv({
        PIPELINE_ERROR_TRACKER: "sentry",
        SENTRY_DSN: "https://publickey@sentry.example.test/42",
      }).kind,
    ).toBe("console+sentry");
  });

  it("exports OTLP logs carrying the pipeline trace id", async () => {
    const fetchMock = vi.fn(async () => new Response("", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const reporter = errorReporterFromEnv({
      PIPELINE_ERROR_TRACKER: "otlp",
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector.local:4318/",
      OTEL_EXPORTER_OTLP_HEADERS: "x-api-key=synthetic%20value",
    });
    reporter.capture({
      ctx,
      stage: "file-normalize",
      error: pipelineErrorInfo(new Error("x")),
    });
    await reporter.flush();
    const [url, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(url).toBe("http://collector.local:4318/v1/logs");
    expect((init.headers as Record<string, string>)["x-api-key"]).toBe(
      "synthetic value",
    );
    const record = JSON.parse(String(init.body)).resourceLogs[0].scopeLogs[0]
      .logRecords[0];
    expect(record.traceId).toBe(ctx.traceId.replaceAll("-", ""));
    expect(record.attributes).toContainEqual({
      key: "machine_id",
      value: { stringValue: MACHINE },
    });
  });

  it("builds a Sentry envelope from a DSN", () => {
    expect(
      parseSentryDsn("https://abc@o1.ingest.sentry.example.test/9"),
    ).toEqual({
      envelopeUrl: "https://o1.ingest.sentry.example.test/api/9/envelope/",
      publicKey: "abc",
    });
    expect(parseSentryDsn("not a dsn")).toBeNull();
    const [header, item, event] = sentryEnvelope(
      {
        ctx,
        stage: "session-indexed",
        error: pipelineErrorInfo(new Error("x"), "index_failed"),
      },
      "test",
    )
      .split("\n")
      .map((line) => JSON.parse(line));
    expect(item).toEqual({ type: "event" });
    expect(event.event_id).toBe(header.event_id);
    expect(event.tags).toMatchObject({
      stage: "session-indexed",
      error_code: "index_failed",
      workspace_id: WORKSPACE,
    });
    expect(event.contexts.trace.trace_id).toBe(ctx.traceId.replaceAll("-", ""));
  });

  it("swallows exporter transport failures", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("ECONNREFUSED with header x-api-key=secret");
      }),
    );
    const reporter = errorReporterFromEnv({
      PIPELINE_ERROR_TRACKER: "sentry",
      SENTRY_DSN: "https://k@sentry.example.test/1",
    });
    reporter.capture({
      ctx,
      stage: "file-normalize",
      error: pipelineErrorInfo(new Error("x")),
    });
    await expect(reporter.flush()).resolves.toBeUndefined();
    expect(JSON.stringify(warn.mock.calls)).not.toContain("secret");
    expect(
      otlpErrorPayload(
        { ctx, stage: "fetch", error: pipelineErrorInfo(null) },
        "test",
      ),
    ).toBeTruthy();
  });
});

describe("pipeline recorder", () => {
  it("logs trace_id and the machine's keyed token and never throws when the sink fails", async () => {
    const sink: PipelineEventSink = {
      kind: "broken",
      record: async () => {
        throw new Error("sink down");
      },
      byTrace: async () => ({ events: [], truncated: false }),
    };
    const recorder = new PipelineRecorder(
      {
        traceId: uuidv7(),
        orgId: ORG,
        workspaceId: WORKSPACE,
        machineId: MACHINE,
      },
      {
        sink,
        logger: { environment: "test", telemetryIdKey: TELEMETRY_KEY },
      },
    );
    await expect(
      recorder.record("session-indexed", "ok"),
    ).resolves.toMatchObject({
      status: "ok",
    });
    const line = logLines(info).find(
      (entry) => entry.message === "pipeline stage",
    );
    expect(line).toMatchObject({
      trace_id: recorder.ctx.traceId,
      machine_id: telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE),
      stage: "session-indexed",
    });
    expect(
      logLines(warn).some(
        (entry) => entry.message === "pipeline event write failed",
      ),
    ).toBe(true);
  });

  it("dedupes a redelivered transition by idempotency key", async () => {
    const sink = new InMemoryPipelineEventSink();
    const recorder = new PipelineRecorder(
      { traceId: uuidv7(), orgId: ORG, workspaceId: WORKSPACE },
      { sink },
    );
    await recorder.record("object-landed", "ok", { key: "k" });
    await recorder.record("object-landed", "ok", { key: "k" });
    expect(sink.events).toHaveLength(1);
  });
});

describe("trace summary", () => {
  it("folds a transition stored twice into one event", async () => {
    const sink = new InMemoryPipelineEventSink();
    const traceId = uuidv7();
    const recorder = new PipelineRecorder(
      { traceId, orgId: ORG, workspaceId: WORKSPACE },
      { sink },
    );
    const event = await recorder.record("session-indexed", "ok", { key: "k" });
    const later = {
      ...event,
      at: new Date(Date.parse(event.at) + 5).toISOString(),
    };
    const summary = summarizeTrace(traceId, [later, event]);
    expect(summary.events).toHaveLength(1);
    expect(summary.events[0]!.at).toBe(event.at);
    expect(summary.stages["session-indexed"].events).toBe(1);
  });
});

describe("per-stage queues", () => {
  it("indexes the raw pointer even when every transform attempt fails", async () => {
    const { queues, sink, deadLetters, index, transform, deps } = pipeline();
    const traceId = uuidv7();
    await queues.send("object-landed", landed(traceId, CHUNK_KEY));
    await queues.send("object-landed", landed(traceId));
    await queues.drain(deps);

    expect([...index.rows.keys()]).toEqual([MANIFEST_KEY]);
    expect(index.calls).toBe(1);
    // 1 delivery + 5 retries on the main queue, then quarantined from the DLQ.
    expect(transform.calls).toBe(6);
    const letters = await deadLetters.list({});
    expect(letters).toHaveLength(1);
    expect(letters[0]).toMatchObject({
      stage: "file-normalize",
      state: "dead",
      traceId,
      deadCount: 1,
    });

    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.verdict).toBe("indexed");
    expect(summary.landed).toBe(true);
    expect(summary.stages["session-indexed"].status).toBe("ok");
    expect(summary.stages["file-normalize"].status).toBe("dead_lettered");
    expect(summary.answer).toMatch(/^Yes\./);
    expect(summary.answer).toContain("the raw upload is unaffected");
    // No foreign error text reached the event store or the logs.
    expect(JSON.stringify(sink.events)).not.toContain("secret prompt");
    expect(
      JSON.stringify([info.mock.calls, warn.mock.calls, errorLog.mock.calls]),
    ).not.toContain("secret prompt");
  });

  it("indexes the pointer and quarantines the transform while its queue is down", async () => {
    const { queues, sink, deadLetters, index, transform, deps } = pipeline();
    const send = queues.send.bind(queues);
    let transformQueueUp = false;
    queues.send = async (stage, message) => {
      if (stage === "file-normalize" && !transformQueueUp)
        throw new Error("queue unavailable");
      return send(stage, message);
    };
    const traceId = uuidv7();
    await send("object-landed", landed(traceId));
    await queues.drain(deps);
    expect(index.rows.size).toBe(1);
    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.stages["object-landed"].status).toBe("ok");
    expect(summary.stages["file-normalize"]).toMatchObject({
      status: "dead_lettered",
      lastError: { code: "transform_enqueue_failed" },
    });
    expect(summary.verdict).toBe("indexed");
    expect((await deadLetters.list({}))[0]).toMatchObject({
      stage: "file-normalize",
      state: "dead",
    });

    // Once the queue is back, replay recovers the normalization that never got queued.
    transformQueueUp = true;
    transform.healthy = true;
    const result = await replayDeadLetters(
      {
        deadLetters,
        sink,
        send: (stage, message) => queues.send(stage, message),
      },
      { dryRun: false },
    );
    expect(result.sent).toHaveLength(1);
    await queues.drain(deps);
    expect(transform.outputs.get(MANIFEST_KEY)).toBe(1);
    expect((await deadLetters.list({}))[0]!.state).toBe("resolved");
  });

  it("storage event retries when the transform can be neither queued nor quarantined", async () => {
    const { queues, index, deps } = pipeline();
    const send = queues.send.bind(queues);
    queues.send = async (stage, message) => {
      if (stage === "file-normalize") throw new Error("queue unavailable");
      return send(stage, message);
    };
    deps.handlers["object-landed"] = objectLandedHandler({
      senders: { send: (stage, message) => queues.send(stage, message) },
      quarantine: async () => {
        throw new Error("store down");
      },
      transformEnabled: true,
    });
    await send("object-landed", landed(uuidv7()));
    await queues.drain(deps);
    // The pointer still landed; the event was retried rather than acked with the transform lost.
    expect(index.rows.size).toBe(1);
    expect(
      queues.sent.filter((s) => s.queue.includes("session-indexed")).length,
    ).toBeGreaterThan(1);
  });

  it("quarantines messages for a stage this deployment has not wired, instead of burning retries", async () => {
    const { queues, deadLetters, deps } = pipeline({ transformEnabled: false });
    delete deps.handlers["session-indexed"];
    await queues.send("object-landed", landed(uuidv7()));
    const deliveries = await queues.drain(deps);
    expect(deliveries).toBe(2);
    expect((await deadLetters.list({}))[0]).toMatchObject({
      stage: "session-indexed",
      state: "dead",
    });
  });

  it("records a skipped transform when it is not enabled", async () => {
    const { queues, sink, transform, deps } = pipeline({
      transformEnabled: false,
    });
    const traceId = uuidv7();
    await queues.send("object-landed", landed(traceId));
    await queues.drain(deps);
    expect(transform.calls).toBe(0);
    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.stages["file-normalize"].status).toBe("skipped");
    expect(summary.answer).toContain("not enabled");
  });

  it("retries a flaky index upsert and reports a failed trace once it is dead-lettered", async () => {
    const { queues, sink, index, deps } = pipeline({ transformEnabled: false });
    index.failuresLeft = 2;
    const recovered = uuidv7();
    await queues.send("object-landed", landed(recovered));
    await queues.drain(deps);
    expect(index.rows.size).toBe(1);
    const ok = summarizeTrace(
      recovered,
      (await sink.byTrace(SCOPE, recovered)).events,
    );
    expect(ok.stages["session-indexed"]).toMatchObject({
      status: "ok",
      attempts: 3,
    });

    index.rows.clear();
    index.failuresLeft = 100;
    const failed = uuidv7();
    await queues.send("object-landed", landed(failed));
    await queues.drain(deps);
    const summary = summarizeTrace(
      failed,
      (await sink.byTrace(SCOPE, failed)).events,
    );
    expect(summary.verdict).toBe("failed");
    expect(summary.landed).toBe(false);
    expect(summary.answer).toContain("session-indexed dead-lettered");
  });

  it("quarantines a manifest storage event whose workKey is not manifestWorkKey", async () => {
    const { queues, deadLetters, index, deps } = pipeline();
    const message = landed(uuidv7());
    message.workKey = "adapter-specific-id";
    await queues.send("object-landed", message);
    expect(await queues.drain(deps)).toBe(1);
    expect(index.rows.size).toBe(0);
    expect((await deadLetters.list({}))[0]).toMatchObject({
      stage: "object-landed",
      state: "dead",
    });
    expect(manifestWorkKey({ key: "k", kind: "manifest", version: "v9" })).toBe(
      "k@v9",
    );
  });

  it("quarantines a permanently invalid event without burning retries", async () => {
    const { queues, deadLetters, deps } = pipeline();
    const message = landed(uuidv7());
    message.ctx.workspaceId = "ws_other";
    await queues.send("object-landed", message);
    const deliveries = await queues.drain(deps);
    expect(deliveries).toBe(1);
    expect((await deadLetters.list({}))[0]).toMatchObject({
      stage: "object-landed",
      state: "dead",
    });
  });

  it("acks malformed messages instead of retrying them into the DLQ", async () => {
    const { queues, deps } = pipeline();
    queues.pending.push({
      queue: stageQueueName("file-normalize"),
      body: { hello: 1 },
      attempts: 1,
    });
    expect(await queues.drain(deps)).toBe(1);
  });
});

describe("dead letter replay", () => {
  async function deadTransform() {
    const env = pipeline();
    const traceId = uuidv7();
    await env.queues.send("object-landed", landed(traceId));
    await env.queues.drain(env.deps);
    env.queues.sent.length = 0;
    return { ...env, traceId };
  }

  it("is a dry run by default", async () => {
    const { queues, deadLetters, sink } = await deadTransform();
    const result = await replayDeadLetters({
      deadLetters,
      sink,
      send: (stage, message) => queues.send(stage, message),
    });
    expect(result.dryRun).toBe(true);
    expect(result.planned).toHaveLength(1);
    expect(result.sent).toEqual([]);
    expect(queues.sent).toHaveLength(0);
    expect((await deadLetters.list({}))[0]!.state).toBe("dead");
  });

  it("is idempotent: repeated and concurrent replays send each letter once and apply it once", async () => {
    const { queues, deadLetters, sink, transform, deps, traceId } =
      await deadTransform();
    const replay = () =>
      replayDeadLetters(
        {
          deadLetters,
          sink,
          send: (stage, message) => queues.send(stage, message),
        },
        { dryRun: false },
      );
    const [first, second] = await Promise.all([replay(), replay()]);
    const third = await replay();
    expect(first.sent.length + second.sent.length + third.sent.length).toBe(1);
    expect(queues.sent).toHaveLength(1);
    expect(queues.sent[0]!.body.replay).toBeDefined();
    expect(queues.sent[0]!.body.workKey).toBe(`${MANIFEST_KEY}@v1`);

    transform.healthy = true;
    await queues.drain(deps);
    expect(transform.outputs.get(MANIFEST_KEY)).toBe(1);
    expect((await deadLetters.list({}))[0]).toMatchObject({
      state: "resolved",
      replayCount: 1,
    });

    // Nothing is left to replay, with or without force.
    expect((await replay()).sent).toEqual([]);
    expect(
      (
        await replayDeadLetters(
          {
            deadLetters,
            sink,
            send: (stage, message) => queues.send(stage, message),
          },
          { dryRun: false, force: true },
        )
      ).sent,
    ).toEqual([]);
    expect(transform.outputs.get(MANIFEST_KEY)).toBe(1);

    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.stages["file-normalize"].status).toBe("ok");
    expect(summary.answer).toContain("Normalized output is ready");
    expect(
      sink.events.filter((event) => event.status === "replayed"),
    ).toHaveLength(1);
  });

  it("re-quarantines a replay that dies again, and force re-sends a stuck replay", async () => {
    const { queues, deadLetters, sink, deps } = await deadTransform();
    const send = (
      stage: Parameters<typeof queues.send>[0],
      message: StageMessage,
    ) => queues.send(stage, message);
    await replayDeadLetters({ deadLetters, sink, send }, { dryRun: false });
    await queues.drain(deps);
    expect((await deadLetters.list({}))[0]).toMatchObject({
      state: "dead",
      deadCount: 2,
      replayCount: 1,
    });

    await replayDeadLetters({ deadLetters, sink, send }, { dryRun: false });
    queues.pending.length = 0; // the replayed message is lost in flight
    expect(
      (await replayDeadLetters({ deadLetters, sink, send }, { dryRun: false }))
        .sent,
    ).toEqual([]);
    expect(
      (
        await replayDeadLetters(
          { deadLetters, sink, send },
          { dryRun: false, force: true },
        )
      ).sent,
    ).toHaveLength(1);
  });

  it("does not strand a letter when the claim call fails after committing", async () => {
    const { queues, deadLetters, sink, deps } = await deadTransform();
    const claim = deadLetters.claimForReplay.bind(deadLetters);
    deadLetters.claimForReplay = async (...args) => {
      await claim(...args); // commits
      throw new Error("response timed out");
    };
    const result = await replayDeadLetters(
      {
        deadLetters,
        sink,
        send: (stage, message) => queues.send(stage, message),
      },
      { dryRun: false },
    );
    expect(result.skipped).toEqual([
      { id: expect.any(String), reason: "claim_failed" },
    ]);
    expect(queues.sent).toHaveLength(0);
    // Our own claim was released, so the next plain replay picks it up.
    expect((await deadLetters.list({}))[0]!.state).toBe("dead");
    void deps;
  });

  it("lets a plain replay take over a claim older than the lease", async () => {
    const { queues, deadLetters, sink } = await deadTransform();
    const send = (
      stage: Parameters<typeof queues.send>[0],
      message: StageMessage,
    ) => queues.send(stage, message);
    const start = new Date(Date.UTC(2026, 9, 25, 12));
    await replayDeadLetters(
      { deadLetters, sink, send },
      { dryRun: false, now: start },
    );
    queues.sent.length = 0;
    queues.pending.length = 0; // the claim's send was lost
    const soon = new Date(start.getTime() + 10 * 60 * 1000);
    expect(
      (
        await replayDeadLetters(
          { deadLetters, sink, send },
          { dryRun: false, now: soon },
        )
      ).sent,
    ).toEqual([]);
    const later = new Date(start.getTime() + REPLAY_LEASE_MS + 1000);
    expect(
      (
        await replayDeadLetters(
          { deadLetters, sink, send },
          { dryRun: false, now: later },
        )
      ).sent,
    ).toHaveLength(1);
  });

  it("sends an expired claim once when two replays take it over together", async () => {
    const { queues, deadLetters, sink } = await deadTransform();
    const send = (
      stage: Parameters<typeof queues.send>[0],
      message: StageMessage,
    ) => queues.send(stage, message);
    const start = new Date(Date.UTC(2026, 9, 25, 12));
    await replayDeadLetters(
      { deadLetters, sink, send },
      { dryRun: false, now: start },
    );
    queues.sent.length = 0;
    queues.pending.length = 0;
    const later = new Date(start.getTime() + REPLAY_LEASE_MS + 1000);
    const results = await Promise.all([
      replayDeadLetters(
        { deadLetters, sink, send },
        { dryRun: false, now: later },
      ),
      replayDeadLetters(
        { deadLetters, sink, send },
        { dryRun: false, now: later },
      ),
    ]);
    expect(results.flatMap((result) => result.sent)).toHaveLength(1);
    expect(queues.sent).toHaveLength(1);
  });

  it("does not let in-flight claims crowd dead letters out of the replay window", async () => {
    const deadLetters = new InMemoryDeadLetterStore();
    const sink = new InMemoryPipelineEventSink();
    const sent: string[] = [];
    const at = (minutes: number) =>
      new Date(Date.UTC(2026, 9, 25, 12, minutes));
    for (let index = 0; index < 3; index += 1) {
      const message = landed(uuidv7());
      message.workKey = `claimed-${index}`;
      await deadLetters.quarantine(message, at(index));
      await deadLetters.claimForReplay(
        deadLetterId("object-landed", message.ctx, message.workKey),
        { state: "dead", replayCount: 0 },
        uuidv7(),
        at(10),
      );
    }
    const fresh = landed(uuidv7());
    fresh.workKey = "fresh";
    await deadLetters.quarantine(fresh, at(5));
    const result = await replayDeadLetters(
      {
        deadLetters,
        sink,
        send: async (_stage, message) => void sent.push(message.workKey),
      },
      { dryRun: false, limit: 3, now: at(15) },
    );
    expect(sent).toEqual(["fresh"]);
    expect(result.considered).toBe(1);
  });

  it("releases its claim when the send fails", async () => {
    const { deadLetters, sink } = await deadTransform();
    const result = await replayDeadLetters(
      {
        deadLetters,
        sink,
        send: async () => {
          throw new Error("queue down");
        },
      },
      { dryRun: false },
    );
    expect(result.skipped).toEqual([
      { id: expect.any(String), reason: "send_failed" },
    ]);
    expect((await deadLetters.list({}))[0]).toMatchObject({
      state: "dead",
      replayCount: 0,
    });
  });
});

const auth = (
  workspaceId = WORKSPACE,
  scopes = ["rth:read", "rth:sync"],
): AuthContext => ({
  userId: "user-synthetic",
  orgId: "org-synthetic",
  workspaceId,
  tokenSubject: "user-synthetic",
  scopes,
  claims: {},
});

function routeApp(deps: PipelineRouteDeps, who: AuthContext = auth()) {
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("correlationId", "corr-synthetic");
    c.set("auth", who);
    await next();
  });
  app.route("/v1", createPipelineRoutes(deps));
  return app;
}

describe("trace summary hardening", () => {
  const ev = (
    stage: PipelineStage,
    status: "ok" | "dead_lettered" | "started" | "error",
    ms: number,
    attrs: Record<string, unknown>,
    key: string,
  ) => ({
    traceId: "t",
    orgId: ORG,
    workspaceId: WORKSPACE,
    stage,
    status,
    attempt: 1,
    attrs,
    at: new Date(Date.UTC(2026, 9, 27, 6) + ms).toISOString(),
    idempotencyKey: key,
  });

  it("does not let one machine's index success cover another machine's manifest", () => {
    // Revisions count per manifest: machine B's revision 1 is not covered by
    // machine A's revision 1 having indexed.
    const summary = summarizeTrace("t", [
      ev(
        "object-landed",
        "ok",
        0,
        { work_key: "a/manifest.json@v1", revision: 1 },
        "sa",
      ),
      ev(
        "session-indexed",
        "ok",
        10,
        { work_key: "a/manifest.json@v1", revision: 1 },
        "ia",
      ),
      ev(
        "object-landed",
        "ok",
        20,
        { work_key: "b/manifest.json@v1", revision: 1 },
        "sb",
      ),
    ]);
    expect(summary.verdict).toBe("stored");
    expect(summary.landed).toBe(false);
    // Within one manifest, a newer revision's index still covers an older storage event.
    const same = summarizeTrace("t", [
      ev(
        "object-landed",
        "ok",
        0,
        { work_key: "a/manifest.json@v2", revision: 2 },
        "s2",
      ),
      ev(
        "session-indexed",
        "ok",
        10,
        { work_key: "a/manifest.json@v3", revision: 3 },
        "i3",
      ),
    ]);
    expect(same.verdict).toBe("indexed");
  });

  it("lets a later manifest decide even when an earlier one has a higher revision", () => {
    const summary = summarizeTrace("t", [
      ev(
        "object-landed",
        "ok",
        0,
        { work_key: "a/manifest.json@v5", revision: 5 },
        "sa",
      ),
      ev(
        "session-indexed",
        "ok",
        10,
        { work_key: "a/manifest.json@v5", revision: 5 },
        "ia",
      ),
      ev(
        "object-landed",
        "ok",
        20,
        { work_key: "b/manifest.json@v1", revision: 1 },
        "sb",
      ),
    ]);
    expect(summary.verdict).toBe("stored");
    // And its failure is not hidden by the other manifest's success.
    const failed = summarizeTrace("t", [
      ev(
        "object-landed",
        "ok",
        0,
        { work_key: "a/manifest.json@v5", revision: 5 },
        "sa",
      ),
      ev(
        "session-indexed",
        "ok",
        10,
        { work_key: "a/manifest.json@v5", revision: 5 },
        "ia",
      ),
      ev(
        "object-landed",
        "dead_lettered",
        20,
        { work_key: "b/manifest.json@v1", revision: 1 },
        "sb",
      ),
    ]);
    expect(failed.verdict).toBe("failed");
  });

  it("lets a manifest's next version decide after another manifest's work", () => {
    // A, then B, then A again: A's newer version is the newest work.
    const failed = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "a/manifest.json@v1" }, "a1"),
      ev("object-landed", "ok", 10, { work_key: "b/manifest.json@v1" }, "b1"),
      ev("session-indexed", "ok", 15, { work_key: "b/manifest.json@v1" }, "ib"),
      ev(
        "object-landed",
        "dead_lettered",
        20,
        { work_key: "a/manifest.json@v2" },
        "a2",
      ),
    ]);
    expect(failed.verdict).toBe("failed");
    const deleted = summarizeTrace("t", [
      ev("session-indexed", "ok", 0, { work_key: "a/manifest.json@v1" }, "ia1"),
      ev(
        "session-indexed",
        "ok",
        10,
        { work_key: "b/manifest.json@v1" },
        "ib1",
      ),
      ev(
        "session-indexed",
        "ok",
        20,
        { work_key: "a/manifest.json@deleted", deleted: true },
        "ia2",
      ),
    ]);
    expect(deleted.verdict).toBe("deleted");
  });

  it("does not report an upload while any chunk's put failed", () => {
    const summary = summarizeTrace("t", [
      ev("deliver.put", "ok", 0, { chunk: "c1" }, "p1"),
      ev("deliver.put", "error", 10, { chunk: "c2" }, "p2"),
    ]);
    expect(summary.destinations["object-storage"]!.status).toBe("error");
    expect(summary.stages["deliver.put"].status).toBe("error");
    expect(summary.verdict).not.toBe("uploaded");
    // Once the failed chunk is retried successfully, the upload is complete.
    const retried = summarizeTrace("t", [
      ev("deliver.put", "ok", 0, { chunk: "c1" }, "p1"),
      ev("deliver.put", "error", 10, { chunk: "c2" }, "p2"),
      ev("deliver.put", "ok", 20, { chunk: "c2" }, "p3"),
    ]);
    expect(retried.verdict).toBe("uploaded");
  });

  it("says deleted once the index applied a manifest deletion", () => {
    const work = { work_key: "m@deleted:t1", deleted: true };
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, work, "s1"),
      ev("session-indexed", "ok", 10, work, "i1"),
    ]);
    expect(summary.verdict).toBe("deleted");
    expect(summary.landed).toBe(false);
    expect(summary.answer).toMatch(/^Deleted\./);
    // The index's success decides, even if the storage event was never written.
    expect(
      summarizeTrace("t", [ev("session-indexed", "ok", 10, work, "i1")])
        .verdict,
    ).toBe("deleted");
    // Pending, it is only stored.
    expect(
      summarizeTrace("t", [ev("object-landed", "ok", 0, work, "s1")]).verdict,
    ).toBe("stored");
  });

  it("never touches a shared prototype through a destination name", () => {
    const names = ["constructor", "__proto__", "toString", "hasOwnProperty"];
    const summary = summarizeTrace(
      "t",
      names.map((destination, i) =>
        ev("deliver.put", "ok", i, { destination }, `d${i}`),
      ),
    );
    expect(Object.keys(summary.destinations).sort()).toEqual([...names].sort());
    expect(({} as Record<string, unknown>).status).toBeUndefined();
    expect(
      (Object as unknown as Record<string, unknown>).events,
    ).toBeUndefined();
    expect(typeof {}.toString).toBe("function");
  });

  it("is not indexed when a newer manifest failed after an older one indexed", () => {
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "m@1", revision: 1 }, "s1"),
      ev("session-indexed", "ok", 10, { work_key: "m@1", revision: 1 }, "i1"),
      ev(
        "object-landed",
        "dead_lettered",
        20,
        { work_key: "m@2", revision: 2 },
        "s2",
      ),
    ]);
    expect(summary.verdict).toBe("failed");
    expect(summary.landed).toBe(false);
  });

  it("is stored, not indexed, while the newer manifest's pointer is pending", () => {
    // No revisions recorded: first-seen order decides.
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "a" }, "s1"),
      ev("session-indexed", "ok", 10, { work_key: "a" }, "i1"),
      ev("object-landed", "ok", 20, { work_key: "b" }, "s2"),
    ]);
    expect(summary.verdict).toBe("stored");
    expect(summary.answer).toContain("not yet updated for the latest manifest");
  });

  it("is indexed when only a skipped chunk has a storage event recorded", () => {
    // The index finished before the manifest's object-landed/ok was written
    // (or that best-effort write failed); a chunk is not manifest work.
    const summary = summarizeTrace("t", [
      {
        ...ev("object-landed", "ok", 0, { work_key: "c@1" }, "c1"),
        status: "skipped" as const,
        attrs: { work_key: "c@1", reason: "chunk" },
      },
      ev("session-indexed", "ok", 10, { work_key: "m@1", revision: 1 }, "i1"),
    ]);
    expect(summary.verdict).toBe("indexed");
    expect(summary.landed).toBe(true);
  });

  it("stays indexed when the index success is for the newest manifest", () => {
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "m@2", revision: 2 }, "s2"),
      ev("session-indexed", "ok", 10, { work_key: "m@2", revision: 2 }, "i2"),
    ]);
    expect(summary.verdict).toBe("indexed");
  });

  it("rejects a prototype or invalid destination instead of counting it as object storage", async () => {
    const sink = new InMemoryPipelineEventSink();
    const app = routeApp({
      sink: () => sink,
      deadLetters: () => new InMemoryDeadLetterStore(),
      send: async () => {},
    });
    const post = (attrs: Record<string, unknown>) =>
      app.request("/v1/pipeline/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          machineId: MACHINE,
          events: [
            { traceId: uuidv7(), stage: "deliver.put", status: "ok", attrs },
          ],
        }),
      });
    for (const destination of [
      "prototype",
      "constructor",
      "__proto__",
      "has space",
      7,
    ])
      expect((await post({ destination })).status).toBe(400);
    expect(sink.events).toEqual([]);
    expect(
      (await post({ destination: "relayhistory", constructor: 1, bytes: 3 }))
        .status,
    ).toBe(200);
    expect(sink.events[0]!.attrs).toEqual({
      destination: "relayhistory",
      bytes: 3,
    });
  });

  it("does not trust a late older index without revisions", () => {
    // Index for a@ finishes after the storage event for b@ arrived.
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "a" }, "s1"),
      ev("object-landed", "ok", 20, { work_key: "b" }, "s2"),
      ev("session-indexed", "ok", 30, { work_key: "a" }, "i1"),
    ]);
    expect(summary.verdict).toBe("stored");
    expect(summary.landed).toBe(false);
  });

  it("does not let a legacy index success without a revision hide a newer one with it", () => {
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "m@2", revision: 2 }, "s2"),
      ev("session-indexed", "ok", 10, { work_key: "m@1" }, "i1"),
      ev("session-indexed", "ok", 20, { work_key: "m@3", revision: 3 }, "i3"),
    ]);
    expect(summary.verdict).toBe("indexed");
  });

  it("uses revisions when both sides have them, even if the newest index has an older key", () => {
    const summary = summarizeTrace("t", [
      ev("object-landed", "ok", 0, { work_key: "m@2", revision: 2 }, "s2"),
      ev("session-indexed", "ok", 30, { work_key: "m@3", revision: 3 }, "i3"),
    ]);
    expect(summary.verdict).toBe("indexed");
  });
});

describe("trace lookup", () => {
  it("returns every stage of an upload end to end", async () => {
    const { queues, sink, deadLetters, transform, deps } = pipeline();
    transform.healthy = true;
    const traceId = uuidv7();
    const routeDeps: PipelineRouteDeps = {
      sink: () => sink,
      deadLetters: () => deadLetters,
      send: async (_env, stage, message) => queues.send(stage, message),
    };
    const app = routeApp(routeDeps);
    const report = (events: unknown[]) =>
      app.request("/v1/pipeline/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ machineId: MACHINE, events }),
      });
    const reported = await report([
      {
        traceId,
        sessionId: SESSION,
        stage: "ingest",
        status: "ok",
        attrs: { bytes: 4096 },
      },
      {
        traceId,
        sessionId: SESSION,
        stage: "deliver.put",
        status: "ok",
        attrs: { chunk: "0000000000-0-abcdef12", bytes: 1200 },
      },
    ]);
    expect(reported.status).toBe(200);
    await queues.send("object-landed", landed(traceId));
    await queues.drain(deps);
    expect(
      (
        await report([
          { traceId, stage: "fetch", status: "ok", attrs: { duration_ms: 12 } },
        ])
      ).status,
    ).toBe(200);

    const response = await app.request(`/v1/pipeline/traces/${traceId}`);
    expect(response.status).toBe(200);
    const body = await response.json<any>();
    expect(body.verdict).toBe("indexed");
    expect(body.landed).toBe(true);
    expect(body.machineId).toBe(MACHINE);
    for (const stage of [
      "ingest",
      "deliver.put",
      "object-landed",
      "session-indexed",
      "file-normalize",
      "fetch",
    ])
      expect(body.stages[stage].status, stage).toBe("ok");
    expect(body.destinations["object-storage"].status).toBe("ok");
    expect(
      body.events.every(
        (event: { traceId: string }) => event.traceId === traceId,
      ),
    ).toBe(true);
  });

  it("is scoped to the caller's workspace", async () => {
    const sink = new InMemoryPipelineEventSink();
    const traceId = uuidv7();
    await new PipelineRecorder(
      { traceId, orgId: ORG, workspaceId: WORKSPACE },
      { sink },
    ).record("session-indexed", "ok");
    const deps: PipelineRouteDeps = {
      sink: () => sink,
      deadLetters: () => new InMemoryDeadLetterStore(),
      send: async () => {},
    };
    expect(
      (await routeApp(deps).request(`/v1/pipeline/traces/${traceId}`)).status,
    ).toBe(200);
    const other = await routeApp(deps, auth("ws_other")).request(
      `/v1/pipeline/traces/${traceId}`,
    );
    expect(other.status).toBe(404);
    expect((await other.json<any>()).verdict).toBe("not_found");
  });

  it("does not let a client report server stages or skip validation", async () => {
    const sink = new InMemoryPipelineEventSink();
    const app = routeApp({
      sink: () => sink,
      deadLetters: () => new InMemoryDeadLetterStore(),
      send: async () => {},
    });
    const post = (body: unknown) =>
      app.request("/v1/pipeline/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    const traceId = uuidv7();
    expect(
      (
        await post({
          machineId: MACHINE,
          events: [{ traceId, stage: "session-indexed", status: "ok" }],
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await post({
          machineId: MACHINE,
          events: [{ traceId, stage: "fetch", status: "dead_lettered" }],
        })
      ).status,
    ).toBe(400);
    expect(
      (await post({ events: [{ traceId, stage: "fetch", status: "ok" }] }))
        .status,
    ).toBe(400);
    expect(
      (
        await post({
          machineId: MACHINE,
          events: [
            {
              traceId,
              stage: "fetch",
              status: "ok",
              attrs: { note: "free text with spaces", n: 3, "Bad Key": 1 },
            },
          ],
        })
      ).status,
    ).toBe(200);
    expect(sink.events[0]!.attrs).toEqual({ n: 3 });
    expect(sink.events[0]!.workspaceId).toBe(WORKSPACE);
  });

  it("requires rth:read and a configured store", async () => {
    const traceId = uuidv7();
    const deps: PipelineRouteDeps = {
      sink: () => logOnlyPipelineEventSink,
      deadLetters: () => new InMemoryDeadLetterStore(),
      send: async () => {},
    };
    expect(
      (
        await routeApp(deps, auth(WORKSPACE, ["rth:sync"])).request(
          `/v1/pipeline/traces/${traceId}`,
        )
      ).status,
    ).toBe(403);
    const unconfigured = await routeApp(deps).request(
      `/v1/pipeline/traces/${traceId}`,
    );
    expect(unconfigured.status).toBe(503);
    expect((await unconfigured.json<any>()).error.code).toBe("not_configured");
    expect(
      (await routeApp(deps).request("/v1/pipeline/traces/not-a-uuid")).status,
    ).toBe(400);
  });
});

describe("client event reports", () => {
  function reporter(who: AuthContext = auth()) {
    const sink = new InMemoryPipelineEventSink();
    const app = routeApp(
      {
        sink: () => sink,
        deadLetters: () => new InMemoryDeadLetterStore(),
        send: async () => {},
      },
      who,
    );
    const post = (events: unknown[]) =>
      app.request("/v1/pipeline/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ machineId: MACHINE, events }),
      });
    return { sink, app, post };
  }

  it("treats a token hidden inside an identifier as unsafe", () => {
    for (const unsafe of [
      "xghp_abcdefghijklmnopqrstuvwxyz0123456789",
      "machinesk-ant-api03-abcdefghijklmnopqrstuvwxyz0123",
      "9AKIAIOSFODNN7EXAMPLE",
      "host/xoxb-111-222-unit-test-fake-token",
      "ausr@example.com",
    ])
      expect(isSafeIdentifier(unsafe)).toBe(false);
    for (const safe of [
      MACHINE,
      SESSION,
      "0199a000-0000-7000-8000-000000000001",
      `sha256:${"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`,
      "laptop-khaliq.local",
      "object-storage",
      "0000000000-0-abcdef12",
      "01JBXK4ZQ8F3M9V2N6T7R5W1YD",
      "machine_01JBXK4ZQ8F3M9V2N6T7R5W1YD",
      // ULIDs are case-insensitive.
      "01jbxk4zq8f3m9v2n6t7r5w1yd",
      "machine_01jbxk4zq8f3m9v2n6t7r5w1yd",
    ])
      expect(isSafeIdentifier(safe)).toBe(true);
  });

  it("refuses a secret-shaped machine id and drops a secret-shaped session id", async () => {
    const { sink, app } = reporter();
    const traceId = uuidv7();
    const send = (machineId: string, sessionId?: string) =>
      app.request("/v1/pipeline/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          machineId,
          events: [{ traceId, stage: "ingest", status: "ok", sessionId }],
        }),
      });
    const token = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
    expect((await send(token)).status).toBe(400);
    expect((await send("user@example.com")).status).toBe(400);
    expect((await send(MACHINE, token)).status).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(sink.events).toHaveLength(1);
    expect(sink.events[0]!.machineId).toBe(MACHINE);
    expect(sink.events[0]!.sessionId).toBeUndefined();
    const logged = JSON.stringify([
      ...info.mock.calls,
      ...warn.mock.calls,
      ...errorLog.mock.calls,
    ]);
    expect(logged).not.toContain(token);
  });

  it("keeps the destination when a report carries more attrs than the cap", async () => {
    const { sink, post } = reporter();
    const traceId = uuidv7();
    const attrs: Record<string, unknown> = {};
    for (let i = 0; i < 40; i += 1) attrs[`a${i}`] = i;
    attrs.destination = "relayhistory";
    expect(
      (await post([{ traceId, stage: "deliver.put", status: "ok", attrs }]))
        .status,
    ).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(sink.events[0]!.attrs.destination).toBe("relayhistory");
  });

  it("flushes each recorder's log export after recording", async () => {
    const flush = vi.spyOn(PipelineRecorder.prototype, "flush");
    const { post } = reporter();
    const traceId = uuidv7();
    expect(
      (
        await post([
          { traceId, stage: "ingest", status: "ok" },
          { traceId, stage: "fetch", status: "ok" },
        ])
      ).status,
    ).toBe(200);
    expect(flush).toHaveBeenCalledTimes(2);
  });

  it("writes nothing from a batch that fails validation", async () => {
    const { sink, post } = reporter();
    const traceId = uuidv7();
    const response = await post([
      { traceId, stage: "ingest", status: "ok" },
      { traceId, stage: "session-indexed", status: "ok" },
    ]);
    expect(response.status).toBe(400);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(sink.events).toEqual([]);
  });

  it("keeps puts to different destinations and different chunks apart", async () => {
    const { app, post } = reporter();
    const traceId = uuidv7();
    expect(
      (
        await post([
          {
            traceId,
            stage: "deliver.put",
            status: "ok",
            attrs: { destination: "relayhistory", chunk: "c1" },
          },
          {
            traceId,
            stage: "deliver.put",
            status: "ok",
            attrs: { destination: "object-storage", chunk: "c1" },
          },
          { traceId, stage: "deliver.put", status: "ok", attrs: { seq: 1 } },
          { traceId, stage: "deliver.put", status: "ok", attrs: { seq: 2 } },
        ])
      ).status,
    ).toBe(200);
    const body = await (
      await app.request(`/v1/pipeline/traces/${traceId}`)
    ).json<any>();
    expect(body.events).toHaveLength(4);
    expect(body.verdict).toBe("uploaded");
    expect(body.destinations.relayhistory.status).toBe("ok");
  });

  it("never lets attributes override identity fields", async () => {
    const { sink, post } = reporter();
    const traceId = uuidv7();
    await post([
      {
        traceId,
        stage: "fetch",
        status: "ok",
        attrs: {
          workspace_id: "ws_victim",
          trace_id: uuidv7(),
          org_id: "org_victim",
          bytes: 5,
        },
      },
    ]);
    expect(sink.events[0]!.attrs).toEqual({ bytes: 5 });
    const line = logLines(info).find(
      (entry) => entry.message === "pipeline stage",
    );
    expect(line).toMatchObject({
      trace_id: traceId,
      workspace_id: WORKSPACE,
      org_id: ORG,
    });
  });

  it("replaces implausible client timestamps with server time", async () => {
    const { sink, post } = reporter();
    const traceId = uuidv7();
    const recent = new Date(Date.now() - 60_000).toISOString();
    await post([
      { traceId, stage: "ingest", status: "ok", at: "2099-01-01T00:00:00Z" },
      { traceId, stage: "fetch", status: "ok", at: "2001-01-01T00:00:00Z" },
      { traceId, stage: "deliver.put", status: "ok", at: recent },
    ]);
    const byStage = Object.fromEntries(
      sink.events.map((event) => [event.stage, event.at]),
    );
    expect(Date.parse(byStage.ingest!)).toBeLessThanOrEqual(Date.now());
    expect(Date.parse(byStage.ingest!)).toBeGreaterThan(Date.now() - 10_000);
    expect(Date.parse(byStage.fetch!)).toBeGreaterThan(Date.now() - 10_000);
    expect(byStage["deliver.put"]).toBe(recent);
  });

  it("scopes traces by org as well as workspace, and uses `default` for unscoped tokens", async () => {
    const sink = new InMemoryPipelineEventSink();
    const deps: PipelineRouteDeps = {
      sink: () => sink,
      deadLetters: () => new InMemoryDeadLetterStore(),
      send: async () => {},
    };
    const traceId = uuidv7();
    await new PipelineRecorder(
      { traceId, orgId: ORG, workspaceId: WORKSPACE },
      { sink },
    ).record("session-indexed", "ok");
    const otherOrg = { ...auth(), orgId: "org-other" };
    expect(
      (await routeApp(deps, otherOrg).request(`/v1/pipeline/traces/${traceId}`))
        .status,
    ).toBe(404);

    const unscoped = { ...auth(), workspaceId: undefined };
    const report = await routeApp(deps, unscoped).request(
      "/v1/pipeline/events",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          machineId: MACHINE,
          events: [{ traceId, stage: "ingest", status: "ok" }],
        }),
      },
    );
    expect(report.status).toBe(200);
    expect(sink.events.at(-1)).toMatchObject({
      orgId: ORG,
      workspaceId: "default",
    });
  });
});

describe("per-work-item verdicts", () => {
  it("does not call a session stored when only chunks have landed", async () => {
    const { queues, sink, index, deps } = pipeline();
    const traceId = uuidv7();
    await queues.send("object-landed", landed(traceId, CHUNK_KEY));
    await queues.drain(deps);
    expect(index.rows.size).toBe(0);
    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.stages["object-landed"].status).toBe("skipped");
    expect(summary.verdict).toBe("in_progress");
  });

  it("reports a newer manifest version that dead-lettered even after an older one indexed", async () => {
    const { queues, sink, index, deps } = pipeline({ transformEnabled: false });
    const traceId = uuidv7();
    await queues.send("object-landed", landed(traceId));
    await queues.drain(deps);
    index.failuresLeft = 100;
    const v2 = landed(traceId);
    v2.workKey = `${MANIFEST_KEY}@v2`;
    (v2.payload as { version: string }).version = "v2";
    await queues.send("object-landed", v2);
    await queues.drain(deps);
    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.stages["session-indexed"].status).toBe("dead_lettered");
    expect(summary.verdict).toBe("failed");
  });

  it("redelivers a successful replay whose dead letter could not be resolved", async () => {
    const { queues, deadLetters, sink, transform, deps } = pipeline();
    await queues.send("object-landed", landed(uuidv7()));
    await queues.drain(deps);
    await replayDeadLetters(
      {
        deadLetters,
        sink,
        send: (stage, message) => queues.send(stage, message),
      },
      { dryRun: false },
    );
    transform.healthy = true;
    const resolve = deadLetters.resolve.bind(deadLetters);
    let failures = 1;
    deadLetters.resolve = async (id, at) => {
      if (failures-- > 0) throw new Error("store down");
      return resolve(id, at);
    };
    await queues.drain(deps);
    expect((await deadLetters.list({}))[0]!.state).toBe("resolved");
    expect(transform.outputs.get(MANIFEST_KEY)).toBe(2);
  });
});

describe("devin follow-ups", () => {
  it("orders work items by manifest revision, so a late older success cannot mask a newer failure", () => {
    const traceId = uuidv7();
    const at = (ms: number) =>
      new Date(Date.UTC(2026, 9, 25, 12) + ms).toISOString();
    const event = (
      status: "ok" | "dead_lettered",
      revision: number,
      ms: number,
    ) => ({
      traceId,
      orgId: ORG,
      workspaceId: WORKSPACE,
      stage: "session-indexed" as const,
      status,
      attempt: 1,
      attrs: { work_key: `${MANIFEST_KEY}@r${revision}`, revision },
      at: at(ms),
      idempotencyKey: `${revision}:${status}`,
    });
    // v2 dead-letters first; v1's index success arrives later.
    const summary = summarizeTrace(traceId, [
      event("dead_lettered", 2, 0),
      event("ok", 1, 10),
    ]);
    expect(summary.stages["session-indexed"].status).toBe("dead_lettered");
    expect(summary.verdict).toBe("failed");
  });

  it("carries the manifest revision from the storage event to its index and transform messages", async () => {
    const { queues, sink, deps } = pipeline();
    const traceId = uuidv7();
    const message = landed(traceId);
    (message.payload as { revision: number }).revision = 7;
    await queues.send("object-landed", message);
    await queues.drain(deps);
    expect(
      queues.sent
        .filter((s) => s.body.stage !== "object-landed")
        .map((s) => s.body.revision),
    ).toEqual([7, 7]);
    const upsert = sink.events.find(
      (e) => e.stage === "session-indexed" && e.status === "ok",
    );
    expect(upsert!.attrs.revision).toBe(7);
  });

  it("clears a quarantined letter when the same work later succeeds without replay", async () => {
    const { queues, deadLetters, transform, deps } = pipeline();
    const send = queues.send.bind(queues);
    let down = true;
    queues.send = async (stage, message) => {
      if (stage === "file-normalize" && down)
        throw new Error("queue unavailable");
      return send(stage, message);
    };
    const message = landed(uuidv7());
    await send("object-landed", message);
    await queues.drain(deps);
    expect((await deadLetters.list({}))[0]!.state).toBe("dead");
    // The storage event is redelivered once the transform queue is back.
    down = false;
    transform.healthy = true;
    await send("object-landed", message);
    await queues.drain(deps);
    expect((await deadLetters.list({}))[0]!.state).toBe("resolved");
  });

  it("reports a newer version whose transform was skipped instead of an older normalized one", () => {
    const traceId = uuidv7();
    const at = (ms: number) =>
      new Date(Date.UTC(2026, 9, 25, 12) + ms).toISOString();
    const summary = summarizeTrace(traceId, [
      {
        traceId,
        orgId: ORG,
        workspaceId: WORKSPACE,
        stage: "file-normalize",
        status: "ok",
        attempt: 1,
        attrs: { work_key: "m@1", revision: 1 },
        at: at(0),
        idempotencyKey: "a",
      },
      {
        traceId,
        orgId: ORG,
        workspaceId: WORKSPACE,
        stage: "file-normalize",
        status: "skipped",
        attempt: 1,
        attrs: { work_key: "m@2", revision: 2, reason: "not_enabled" },
        at: at(10),
        idempotencyKey: "b",
      },
    ]);
    expect(summary.stages["file-normalize"].status).toBe("skipped");
    expect(summary.answer).not.toContain("Normalized output is ready");
  });

  it("keeps a newer normalized version when an older version's skip is recorded late", async () => {
    const traceId = uuidv7();
    const sink = new InMemoryPipelineEventSink();
    const queues = new FakePipelineQueues();
    const deadLetters = new InMemoryDeadLetterStore();
    const handler = (transformEnabled: boolean) =>
      objectLandedHandler({
        senders: { send: (stage, message) => queues.send(stage, message) },
        quarantine: (message, at) => deadLetters.quarantine(message, at),
        transformEnabled,
      });
    const deps = (transformEnabled: boolean): StageQueueDeps => ({
      sink,
      deadLetters,
      handlers: {
        "object-landed": handler(transformEnabled),
        "session-indexed": async () => {},
        "file-normalize": async () => {},
      },
    });
    const version = (revision: number) => {
      const message = landed(traceId);
      message.workKey = `${MANIFEST_KEY}@r${revision}`;
      Object.assign(message.payload as object, {
        version: `r${revision}`,
        revision,
      });
      return message;
    };
    // Revision 2 is normalized first; revision 1's notification arrives late with transform off.
    await queues.send("object-landed", version(2));
    await queues.drain(deps(true));
    await queues.send("object-landed", version(1));
    await queues.drain(deps(false));
    const summary = summarizeTrace(
      traceId,
      (await sink.byTrace(SCOPE, traceId)).events,
    );
    expect(summary.stages["file-normalize"].status).toBe("ok");
    expect(summary.answer).toContain("Normalized output is ready");
  });

  it("does not mark a trace of exactly the read cap as partial", async () => {
    const traceId = uuidv7();
    const row = (index: number) => ({
      trace_id: traceId,
      org_id: ORG,
      workspace_id: WORKSPACE,
      machine_id: null,
      harness: null,
      session_id: null,
      stage: "fetch",
      status: "ok",
      attempt: 1,
      error: null,
      attrs: {},
      at: new Date(Date.UTC(2026, 9, 25) + index).toISOString(),
      duration_ms: null,
      idempotency_key: `k${index}`,
    });
    const total = (count: number) =>
      vi.fn(async (url: string) => {
        const params = new URL(url).searchParams;
        const offset = Number(params.get("offset"));
        const limit = Number(params.get("limit"));
        return Response.json(
          Array.from(
            { length: Math.max(0, Math.min(limit, count - offset)) },
            (_, i) => row(offset + i),
          ),
        );
      });
    const read = async (count: number) =>
      postgrestPipelineEventSink({
        url: "http://postgrest.local",
        token: "synthetic",
        fetcher: total(count) as unknown as typeof fetch,
      }).byTrace(SCOPE, traceId);
    const exact = await read(MAX_TRACE_EVENTS);
    expect(exact.events).toHaveLength(MAX_TRACE_EVENTS);
    expect(exact.truncated).toBe(false);
    const over = await read(MAX_TRACE_EVENTS + 1);
    expect(over.events).toHaveLength(MAX_TRACE_EVENTS);
    expect(over.truncated).toBe(true);
  });
});

describe("hardening", () => {
  it("drops client error codes that are not identifiers", async () => {
    const info = pipelineErrorInfo(undefined, "DROP TABLE x; -- free text");
    expect(info.code).toBe("stage_failed");
  });

  it("warns when a collector rejects an export and survives malformed OTLP headers", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("denied", { status: 401 })),
    );
    const reporter = errorReporterFromEnv({
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector.local:4318",
      OTEL_EXPORTER_OTLP_HEADERS: "x-api-key=%E0%A4%A,ok=1",
    });
    reporter.capture({
      ctx: { traceId: uuidv7(), orgId: ORG, workspaceId: WORKSPACE },
      stage: "file-normalize",
      error: pipelineErrorInfo(new Error("x")),
    });
    await reporter.flush();
    expect(
      logLines(warn).some((line) => line.message === "error export failed"),
    ).toBe(true);
    expect(JSON.stringify(warn.mock.calls)).not.toContain("denied");
  });
});

describe("long traces", () => {
  it("pages through every stored event and flags a trace past the read cap", async () => {
    const traceId = uuidv7();
    const rows = Array.from({ length: 2500 }, (_, index) => ({
      trace_id: traceId,
      org_id: ORG,
      workspace_id: WORKSPACE,
      machine_id: null,
      harness: null,
      session_id: null,
      stage: index === 2499 ? "session-indexed" : "fetch",
      status: "ok",
      attempt: 1,
      error: null,
      attrs: {},
      at: new Date(Date.UTC(2026, 9, 25) + index).toISOString(),
      duration_ms: null,
      idempotency_key: `k${String(index).padStart(5, "0")}`,
    }));
    const fetcher = vi.fn(async (url: string) => {
      const params = new URL(url).searchParams;
      const offset = Number(params.get("offset"));
      return Response.json(
        rows.slice(offset, offset + Number(params.get("limit"))),
      );
    });
    const sink = postgrestPipelineEventSink({
      url: "http://postgrest.local",
      token: "synthetic",
      fetcher: fetcher as unknown as typeof fetch,
    });
    const { events, truncated } = await sink.byTrace(SCOPE, traceId);
    expect(events).toHaveLength(2500);
    expect(truncated).toBe(false);
    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(new URL(fetcher.mock.calls[0]![0]).searchParams.get("org_id")).toBe(
      `eq.${ORG}`,
    );
    expect(summarizeTrace(traceId, events).verdict).toBe("indexed");

    const capped = summarizeTrace(traceId, events.slice(0, 10), true);
    expect(capped.truncated).toBe(true);
    expect(capped.answer).toMatch(/^Partial:/);
  });
});

describe("dead letter ops routes", () => {
  it("are gated by PIPELINE_OPS_SECRET and replay is a dry run unless asked", async () => {
    const { queues, deadLetters, sink } = await (async () => {
      const env = pipeline();
      await env.queues.send("object-landed", landed(uuidv7()));
      await env.queues.drain(env.deps);
      env.queues.sent.length = 0;
      return env;
    })();
    const app = new Hono<AppEnv>();
    app.route(
      "/v1",
      createPipelineOpsRoutes({
        sink: () => sink,
        deadLetters: () => deadLetters,
        send: async (_env, stage, message) => queues.send(stage, message),
      }),
    );
    const env = { PIPELINE_OPS_SECRET: "ops-synthetic" } as Bindings;
    const call = (
      path: string,
      init: RequestInit = {},
      bindings: Bindings = env,
    ) => app.request(path, init, bindings);
    expect(
      (await call("/v1/internal/pipeline/dead-letters", {}, {} as Bindings))
        .status,
    ).toBe(503);
    expect(
      (
        await call("/v1/internal/pipeline/dead-letters", {
          headers: { Authorization: "Bearer wrong" },
        })
      ).status,
    ).toBe(401);
    const authz = { Authorization: "Bearer ops-synthetic" };
    const list = await (
      await call("/v1/internal/pipeline/dead-letters?stage=file-normalize", {
        headers: authz,
      })
    ).json<any>();
    expect(list.deadLetters).toHaveLength(1);
    expect(list.deadLetters[0].message).toBeUndefined();

    const dry = await (
      await call("/v1/internal/pipeline/dead-letters/replay", {
        method: "POST",
        headers: authz,
      })
    ).json<any>();
    expect(dry).toMatchObject({ dryRun: true, sent: [] });
    expect(queues.sent).toHaveLength(0);
    const real = await (
      await call("/v1/internal/pipeline/dead-letters/replay", {
        method: "POST",
        headers: { ...authz, "Content-Type": "application/json" },
        body: JSON.stringify({ dryRun: false, stage: "file-normalize" }),
      })
    ).json<any>();
    expect(real.sent).toHaveLength(1);
    expect(queues.sent).toHaveLength(1);
  });
});

describe("app wiring", () => {
  it("mounts the ops routes outside user auth and the trace route inside it", async () => {
    const app = createApp();
    const ops = await app.request("/v1/internal/pipeline/dead-letters", {}, {});
    expect(ops.status).toBe(503);
    expect((await ops.json<any>()).error.code).toBe("not_configured");
    const trace = await app.request(`/v1/pipeline/traces/${uuidv7()}`, {}, {});
    expect(trace.status).toBe(401);
  });
});

describe("PostgREST stores", () => {
  it("writes events idempotently and reads them by workspace and trace", async () => {
    const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
      if (init?.method === "POST") return new Response(null, { status: 201 });
      return Response.json([
        {
          trace_id: "t",
          workspace_id: WORKSPACE,
          machine_id: MACHINE,
          session_id: null,
          stage: "ingest",
          status: "ok",
          attempt: 1,
          error: null,
          attrs: {},
          at: "2026-10-25T00:00:00.000Z",
          idempotency_key: "k",
        },
      ]);
    });
    const sink = postgrestPipelineEventSink({
      url: "http://postgrest.local/",
      token: "synthetic-token",
      fetcher: fetcher as unknown as typeof fetch,
    });
    const traceId = uuidv7();
    await new PipelineRecorder(
      { traceId, orgId: ORG, workspaceId: WORKSPACE },
      { sink },
    ).record("ingest", "ok");
    const [insertUrl, insert] = fetcher.mock.calls[0]!;
    expect(insertUrl).toBe("http://postgrest.local/pipeline_events");
    expect((insert!.headers as Record<string, string>).Prefer).toBe(
      "return=minimal",
    );
    expect((insert!.headers as Record<string, string>)["Content-Profile"]).toBe(
      "sessions",
    );
    expect(JSON.parse(String(insert!.body))[0]).toMatchObject({
      trace_id: traceId,
      workspace_id: WORKSPACE,
      stage: "ingest",
    });

    const { events } = await sink.byTrace(SCOPE, traceId);
    const readUrl = new URL(fetcher.mock.calls[1]![0]);
    expect(readUrl.searchParams.get("workspace_id")).toBe(`eq.${WORKSPACE}`);
    expect(readUrl.searchParams.get("trace_id")).toBe(`eq.${traceId}`);
    expect(events[0]).toMatchObject({ machineId: MACHINE, stage: "ingest" });
  });

  it("claims dead letters with a conditional PATCH", async () => {
    const row = { id: "file-normalize:k", state: "dead", replay_count: 0 };
    const fetcher = vi.fn(async (_url: string, init?: RequestInit) =>
      init?.method === "PATCH"
        ? Response.json([{ ...row, state: "replaying" }])
        : Response.json([row]),
    );
    const store = postgrestDeadLetterStore({
      url: "http://postgrest.local",
      token: "synthetic-token",
      fetcher: fetcher as unknown as typeof fetch,
    });
    expect(
      await store.claimForReplay(
        "file-normalize:k",
        { state: "dead", replayCount: 0 },
        uuidv7(),
        new Date(),
      ),
    ).toBe(true);
    // A single conditional PATCH: no read-then-write window.
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0]![1]?.method).toBe("PATCH");
    const patch = new URL(fetcher.mock.calls[0]![0]);
    expect(patch.searchParams.get("state")).toBe("eq.dead");
    expect(patch.searchParams.get("replay_count")).toBe("eq.0");
  });
});

describe("keyed telemetry ids", () => {
  const SESSION_ID = "sess-synthetic-telemetry";
  const ctx = (): TraceContext => ({
    traceId: uuidv7(),
    orgId: ORG,
    workspaceId: WORKSPACE,
    machineId: MACHINE,
    sessionId: SESSION_ID,
  });

  it("hashes deterministically, separates machines from sessions and refuses short keys", () => {
    const token = telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE);
    expect(token).toMatch(/^h1_[0-9a-f]{32}$/);
    expect(telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE)).toBe(token);
    expect(telemetryIdHash(TELEMETRY_KEY, "session", MACHINE)).not.toBe(token);
    expect(
      telemetryIdHash(`${TELEMETRY_KEY}-rotated`, "machine", MACHINE),
    ).not.toBe(token);
    expect(telemetryIdKey("too-short")).toBeUndefined();
    expect(telemetryIdKey(TELEMETRY_KEY)).toBe(TELEMETRY_KEY);
    // No key: the ids are left out, trace and tenant stay.
    expect(telemetryContext(ctx(), undefined)).not.toHaveProperty("machineId");
    expect(telemetryContext(ctx(), undefined)).not.toHaveProperty("sessionId");
  });

  it("keeps raw ids out of logs and error exports but in the stored event", async () => {
    const sink = new InMemoryPipelineEventSink();
    const reported: TraceContext[] = [];
    const recorder = new PipelineRecorder(ctx(), {
      sink,
      logger: { environment: "test", telemetryIdKey: TELEMETRY_KEY },
      reporter: {
        kind: "test",
        capture: (report) => {
          reported.push(report.ctx);
        },
        flush: async () => {},
      },
    });
    await recorder.record("session-indexed", "dead_lettered", {
      errorCode: "boom",
    });
    const machineToken = telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE);
    const sessionToken = telemetryIdHash(TELEMETRY_KEY, "session", SESSION_ID);
    const exported = JSON.stringify([
      ...info.mock.calls,
      ...warn.mock.calls,
      ...errorLog.mock.calls,
      otlpErrorPayload(
        {
          ctx: reported[0]!,
          stage: "session-indexed",
          error: { code: "boom", name: "PipelineError", retryable: false },
        },
        "test",
      ),
      sentryEnvelope(
        {
          ctx: reported[0]!,
          stage: "session-indexed",
          error: { code: "boom", name: "PipelineError", retryable: false },
        },
        "test",
      ),
    ]);
    expect(exported).not.toContain(MACHINE);
    expect(exported).not.toContain(SESSION_ID);
    expect(exported).toContain(machineToken);
    expect(exported).toContain(sessionToken);
    // The trace and the customer stay raw: one session, traceable to its org.
    expect(exported).toContain(recorder.ctx.traceId);
    expect(exported).toContain(ORG);
    // Service-role storage keeps the raw ids beside the same trace id.
    expect(sink.events[0]).toMatchObject({
      traceId: recorder.ctx.traceId,
      orgId: ORG,
      machineId: MACHINE,
      sessionId: SESSION_ID,
    });
  });

  it("hashes the ids inside object keys in attributes and handler logs", async () => {
    const sink = new InMemoryPipelineEventSink();
    const recorder = new PipelineRecorder(ctx(), {
      sink,
      logger: { environment: "test", telemetryIdKey: TELEMETRY_KEY },
      attrs: { work_key: `${MANIFEST_KEY}@etag-1` },
    });
    await recorder.record("session-indexed", "dead_lettered", {
      errorCode: "boom",
      attrs: {
        dead_letter_id: `session-indexed:${ORG}:${WORKSPACE}:${MANIFEST_KEY}@etag-1`,
      },
    });
    recorder.log.info("transform output", {
      output_key: MANIFEST_KEY.replace("manifest.json", "normalized/v1/"),
    });
    const logged = JSON.stringify([...info.mock.calls, ...warn.mock.calls]);
    expect(logged).not.toContain(`m=${MACHINE}`);
    expect(logged).not.toContain(`s=${SESSION}`);
    expect(logged).toContain(
      `m=${telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE)}`,
    );
    expect(logged).toContain(
      `s=${telemetryIdHash(TELEMETRY_KEY, "session", SESSION)}`,
    );
    // Workspace and the rest of the key stay readable.
    expect(logged).toContain(`ws=${WORKSPACE}`);
    // Service-role storage keeps the raw key.
    expect(sink.events[0]!.attrs.work_key).toBe(`${MANIFEST_KEY}@etag-1`);
    // Without a key the segments are blanked, not left raw.
    expect(telemetryString(MANIFEST_KEY, undefined)).not.toContain(MACHINE);
    expect(telemetryString(MANIFEST_KEY, undefined)).toContain("/m=-/");
  });

  it("replaces the trace's own raw ids in any logged field, not only object-key segments", async () => {
    const sink = new InMemoryPipelineEventSink();
    const recorder = new PipelineRecorder(ctx(), {
      sink,
      logger: { environment: "test", telemetryIdKey: TELEMETRY_KEY },
    });
    // A handler logging the raw ids under its own field names.
    recorder.log.info("handler detail", {
      host: MACHINE,
      note: `resumed ${SESSION_ID} after retry`,
      unrelated: "keeps m1 and other text",
    });
    await recorder.record("session-indexed", "ok", {
      attrs: { origin: MACHINE },
    });
    const logged = JSON.stringify(info.mock.calls);
    expect(logged).not.toContain(MACHINE);
    expect(logged).not.toContain(SESSION_ID);
    expect(logged).toContain(
      telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE),
    );
    expect(logged).toContain(
      `resumed ${telemetryIdHash(TELEMETRY_KEY, "session", SESSION_ID)} after retry`,
    );
    expect(logged).toContain("keeps m1 and other text");
    // The stored event keeps the raw attr.
    expect(sink.events[0]!.attrs.origin).toBe(MACHINE);
    // Without a key the ids are blanked, and a short id is replaced only whole.
    expect(
      telemetryFields({ a: `x ${MACHINE} y`, b: "m1", c: "am1b" }, undefined, {
        machineId: MACHINE,
        sessionId: "m1",
      }),
    ).toEqual({ a: "x - y", b: "-", c: "am1b" });
  });

  it("hashes ids at any depth, short ids standing alone, and one token for a shared id", () => {
    const machineToken = telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE);
    const sessionToken = telemetryIdHash(TELEMETRY_KEY, "session", "m1");
    expect(
      telemetryFields(
        {
          nested: { host: MACHINE, list: [`on ${MACHINE}`, { deep: "m1" }] },
          sentence: "resumed m1 after retry",
          glued: "am1b and m1x",
          deep: { a: { b: { c: { d: { e: { f: { g: MACHINE } } } } } } },
        },
        TELEMETRY_KEY,
        { machineId: MACHINE, sessionId: "m1" },
      ),
    ).toEqual({
      nested: {
        host: machineToken,
        list: [`on ${machineToken}`, { deep: sessionToken }],
      },
      sentence: `resumed ${sessionToken} after retry`,
      glued: "am1b and m1x",
      deep: { a: { b: { c: { d: { e: "[nested]" } } } } },
    });
    // The percent-encoded form (object keys, URLs) maps to the same token.
    const slashed = "host/with spaces-01";
    const slashedToken = telemetryIdHash(TELEMETRY_KEY, "machine", slashed);
    expect(
      telemetryFields(
        {
          url: `/v1/x?m=${encodeURIComponent(slashed)}`,
          lowerHex: encodeURIComponent(slashed).replace(/%2F/g, "%2f"),
        },
        TELEMETRY_KEY,
        { machineId: slashed },
      ),
    ).toEqual({ url: `/v1/x?m=${slashedToken}`, lowerHex: slashedToken });
    // Ids that differ only in case keep their own tokens.
    expect(
      telemetryFields(
        { m: "Device-ABCdef-01", s: "device-abcdef-01" },
        TELEMETRY_KEY,
        { machineId: "Device-ABCdef-01", sessionId: "device-abcdef-01" },
      ),
    ).toEqual({
      m: telemetryIdHash(TELEMETRY_KEY, "machine", "Device-ABCdef-01"),
      s: telemetryIdHash(TELEMETRY_KEY, "session", "device-abcdef-01"),
    });
    // A value that is both ids gets the session token, like `session_id`.
    expect(
      telemetryFields({ v: "same-id-0123456789" }, TELEMETRY_KEY, {
        machineId: "same-id-0123456789",
        sessionId: "same-id-0123456789",
      }),
    ).toEqual({
      v: telemetryIdHash(TELEMETRY_KEY, "session", "same-id-0123456789"),
    });
  });

  it("leaves machine and session ids out of telemetry when no key is set", async () => {
    const sink = new InMemoryPipelineEventSink();
    const recorder = new PipelineRecorder(ctx(), {
      sink,
      logger: { environment: "test" },
    });
    await recorder.record("session-indexed", "ok");
    const line = logLines(info).find(
      (entry) => entry.message === "pipeline stage",
    );
    expect(line).toMatchObject({ trace_id: recorder.ctx.traceId, org_id: ORG });
    expect(line).not.toHaveProperty("machine_id");
    expect(line).not.toHaveProperty("session_id");
    expect(sink.events[0]).toMatchObject({ machineId: MACHINE });
  });

  it("gives operators a keyed lookup both ways behind the ops secret", async () => {
    const sink = new InMemoryPipelineEventSink();
    const traced = ctx();
    await new PipelineRecorder(traced, { sink }).record(
      "session-indexed",
      "ok",
    );
    const app = new Hono<AppEnv>();
    app.route(
      "/v1",
      createPipelineOpsRoutes({
        sink: () => sink,
        deadLetters: () => new InMemoryDeadLetterStore(),
        send: async () => {},
      }),
    );
    const authz = { Authorization: "Bearer ops-synthetic" };
    const keyed = {
      PIPELINE_OPS_SECRET: "ops-synthetic",
      PIPELINE_TELEMETRY_ID_KEY: TELEMETRY_KEY,
    } as Bindings;
    const lookup = (bindings: Bindings, headers: Record<string, string>) =>
      app.request(
        "/v1/internal/pipeline/telemetry-ids",
        {
          method: "POST",
          headers: { ...headers, "Content-Type": "application/json" },
          body: JSON.stringify({
            machineIds: [MACHINE],
            sessionIds: [SESSION_ID],
          }),
        },
        bindings,
      );
    expect((await lookup(keyed, {})).status).toBe(401);
    expect(
      (
        await lookup(
          { PIPELINE_OPS_SECRET: "ops-synthetic" } as Bindings,
          authz,
        )
      ).status,
    ).toBe(503);
    const looked = await (await lookup(keyed, authz)).text();
    // Tokens in request order; the raw ids are never echoed back.
    expect(JSON.parse(looked)).toEqual({
      machines: [telemetryIdHash(TELEMETRY_KEY, "machine", MACHINE)],
      sessions: [telemetryIdHash(TELEMETRY_KEY, "session", SESSION_ID)],
    });
    expect(looked).not.toContain(MACHINE);
    expect(looked).not.toContain(SESSION_ID);
    // A secret-shaped id is refused by position, without repeating it.
    const token = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
    const refused = await app.request(
      "/v1/internal/pipeline/telemetry-ids",
      {
        method: "POST",
        headers: { ...authz, "Content-Type": "application/json" },
        body: JSON.stringify({ machineIds: [MACHINE], sessionIds: [token] }),
      },
      keyed,
    );
    expect(refused.status).toBe(400);
    const refusedBody = await refused.text();
    expect(refusedBody).toContain("sessionIds[0]");
    expect(refusedBody).not.toContain(token);
    // A non-string entry is refused at its own position, never dropped.
    const shifted = await app.request(
      "/v1/internal/pipeline/telemetry-ids",
      {
        method: "POST",
        headers: { ...authz, "Content-Type": "application/json" },
        body: JSON.stringify({ machineIds: [null, MACHINE] }),
      },
      keyed,
    );
    expect(shifted.status).toBe(400);
    expect(await shifted.text()).toContain("machineIds[0] is not a string");
    // From a hashed line's raw trace and tenant back to the raw ids.
    const trace = await (
      await app.request(
        `/v1/internal/pipeline/traces/${traced.traceId}?orgId=${ORG}&workspaceId=${WORKSPACE}`,
        { headers: authz },
        keyed,
      )
    ).json<any>();
    expect(trace.events[0]).toMatchObject({
      machineId: MACHINE,
      sessionId: SESSION_ID,
    });
    // Each id is bounded, not just the count.
    expect(
      (
        await app.request(
          "/v1/internal/pipeline/telemetry-ids",
          {
            method: "POST",
            headers: { ...authz, "Content-Type": "application/json" },
            body: JSON.stringify({ machineIds: ["m".repeat(257)] }),
          },
          keyed,
        )
      ).status,
    ).toBe(400);
    expect(
      (
        await app.request(
          `/v1/internal/pipeline/traces/${traced.traceId}`,
          { headers: authz },
          keyed,
        )
      ).status,
    ).toBe(400);
  });
});
