/**
 * Per-stage `pipeline_events` records and the recorder every stage calls.
 *
 * One row per stage transition (`started`, `ok`, `error`, `retrying`,
 * `dead_lettered`, `replayed`, `skipped`), keyed for lookup by `trace_id`. The
 * table lives in the Supabase `sessions` schema owned by the schema workstream;
 * this module only depends on the `PipelineEventSink` interface, so the Worker
 * can run with a log-only sink, an in-memory fake in tests, or a PostgREST
 * writer pointed at any Postgres (hosted Supabase or a self-hosted install).
 *
 * Recording is observability, never control flow: a sink or reporter failure is
 * logged and swallowed, so it can not fail, retry or block the stage it describes.
 */
import { telemetryContext, telemetryFields } from "./telemetry-ids.js";
import {
  createLogger,
  type LogFields,
  type Logger,
  type LoggerEnv,
} from "../logger.js";
import {
  noopErrorReporter,
  pipelineErrorInfo,
  type ErrorReporter,
  type PipelineErrorInfo,
} from "./errors.js";
import {
  safeTraceContext,
  traceLogFields,
  type PipelineStage,
  type PipelineStatus,
  type TraceContext,
} from "./trace.js";

/** The row shape of `sessions.pipeline_events` (snake_case in the table). */
export interface PipelineEvent {
  traceId: string;
  orgId: string;
  workspaceId: string;
  machineId?: string;
  harness?: string;
  sessionId?: string;
  stage: PipelineStage;
  status: PipelineStatus;
  attempt: number;
  error?: PipelineErrorInfo;
  /** Identifiers, counts, sizes, durations. Never content. */
  attrs: Record<string, unknown>;
  at: string;
  /** Copied from `attrs.duration_ms` into its own column when present. */
  durationMs?: number;
  /**
   * Same for every recording of one logical transition, so a redelivered queue
   * message or a replayed dead letter is folded into one event when the trace is
   * read. Not a unique index: the table is partitioned by `at` (see the
   * 20261025000100 migration).
   */
  idempotencyKey: string;
}

/** Who may read a trace: resolved auth's org and workspace, never request input. */
export interface TraceScope {
  orgId: string;
  workspaceId: string;
}

export interface TraceEvents {
  /** Oldest first. */
  events: PipelineEvent[];
  /** True when the trace has more than `MAX_TRACE_EVENTS` events and only the oldest were read. */
  truncated: boolean;
}

export const TRACE_PAGE_SIZE = 1000;
/** Deadline for one store request; recording must never stall the stage it describes. */
export const STORE_TIMEOUT_MS = 5_000;
export const MAX_TRACE_EVENTS = 20 * TRACE_PAGE_SIZE;

export interface PipelineEventSink {
  readonly kind: string;
  record(events: PipelineEvent[]): Promise<void>;
  /** Every event of one trace inside one org and workspace. */
  byTrace(scope: TraceScope, traceId: string): Promise<TraceEvents>;
}

export function pipelineEventKey(
  traceId: string,
  stage: PipelineStage,
  status: PipelineStatus,
  attempt: number,
  discriminator = "",
) {
  return [traceId, stage, status, attempt, discriminator].join(":");
}

/** Test and single-process sink. Drops a repeated idempotency key at write time. */
export class InMemoryPipelineEventSink implements PipelineEventSink {
  readonly kind = "memory";
  readonly events: PipelineEvent[] = [];
  private readonly keys = new Set<string>();

  async record(events: PipelineEvent[]) {
    for (const event of events) {
      if (this.keys.has(event.idempotencyKey)) continue;
      this.keys.add(event.idempotencyKey);
      this.events.push(structuredClone(event));
    }
  }

  async byTrace(scope: TraceScope, traceId: string) {
    const events = this.events
      .filter(
        (event) =>
          event.orgId === scope.orgId &&
          event.workspaceId === scope.workspaceId &&
          event.traceId === traceId,
      )
      .sort((a, b) => a.at.localeCompare(b.at))
      .map((event) => structuredClone(event));
    return {
      events: events.slice(0, MAX_TRACE_EVENTS),
      truncated: events.length > MAX_TRACE_EVENTS,
    };
  }
}

/**
 * The default when no store is configured: every event is a structured log line
 * (the logger already carries trace_id and machine_id), and trace lookup reports
 * the store as unavailable instead of claiming "no events".
 */
export const logOnlyPipelineEventSink: PipelineEventSink = {
  kind: "log",
  record: async () => {},
  byTrace: async () => {
    throw new PipelineStoreUnavailable();
  },
};

export class PipelineStoreUnavailable extends Error {
  constructor() {
    super("pipeline event store is not configured");
    this.name = "PipelineStoreUnavailable";
  }
}

/** A `sessions.pipeline_events` row (20261025000000 + 20261025000100 migrations). */
type Row = {
  trace_id: string;
  workspace_id: string;
  org_id: string | null;
  machine_id: string | null;
  harness: string | null;
  session_id: string | null;
  stage: PipelineStage;
  status: PipelineStatus;
  attempt: number;
  error: PipelineErrorInfo | null;
  attrs: Record<string, unknown>;
  at: string;
  duration_ms: number | null;
  idempotency_key: string | null;
};

export function toRow(event: PipelineEvent): Row {
  return {
    trace_id: event.traceId,
    workspace_id: event.workspaceId,
    org_id: event.orgId,
    machine_id: event.machineId ?? null,
    harness: event.harness ?? null,
    session_id: event.sessionId ?? null,
    stage: event.stage,
    status: event.status,
    attempt: event.attempt,
    error: event.error ?? null,
    attrs: event.attrs,
    at: event.at,
    duration_ms: event.durationMs ?? null,
    idempotency_key: event.idempotencyKey,
  };
}

export function fromRow(row: Row): PipelineEvent {
  return {
    traceId: row.trace_id,
    workspaceId: row.workspace_id,
    orgId: row.org_id ?? "",
    ...(row.machine_id ? { machineId: row.machine_id } : {}),
    ...(row.harness ? { harness: row.harness } : {}),
    ...(row.session_id ? { sessionId: row.session_id } : {}),
    stage: row.stage,
    status: row.status,
    attempt: row.attempt,
    ...(row.error ? { error: row.error } : {}),
    attrs: row.attrs ?? {},
    // PostgREST returns timestamptz as `+00:00`; normalize so ordering is lexical.
    at: new Date(row.at).toISOString(),
    ...(row.duration_ms !== null && row.duration_ms !== undefined
      ? { durationMs: row.duration_ms }
      : {}),
    // Rows written before this column existed fold only with themselves.
    idempotencyKey:
      row.idempotency_key ??
      `${row.trace_id}:${row.stage}:${row.status}:${row.at}`,
  };
}

/**
 * Writer for any PostgREST endpoint exposing `pipeline_events` (Supabase's REST
 * API, or PostgREST beside a self-hosted Postgres) with the service key. Insert
 * is a plain append; duplicates fold by idempotency key on read. Reads always
 * filter by the caller's workspace from resolved auth: the table has no tenant
 * policy, so this filter is the tenant boundary.
 *
 * `fetcher` is injectable so tests use a fake; the token is a runtime secret.
 */
export function postgrestPipelineEventSink(options: {
  url: string;
  token: string;
  schema?: string;
  table?: string;
  fetcher?: typeof fetch;
}): PipelineEventSink {
  const base = `${options.url.replace(/\/+$/, "")}/${options.table ?? "pipeline_events"}`;
  const schema = options.schema ?? "sessions";
  const send = options.fetcher ?? fetch;
  const headers = {
    apikey: options.token,
    Authorization: `Bearer ${options.token}`,
    "Content-Type": "application/json",
    "Accept-Profile": schema,
    "Content-Profile": schema,
  };
  return {
    kind: "postgrest",
    async record(events) {
      if (!events.length) return;
      const response = await send(base, {
        method: "POST",
        headers: { ...headers, Prefer: "return=minimal" },
        body: JSON.stringify(events.map(toRow)),
        // Recording is best effort: never let a stalled store hold a queue message.
        signal: AbortSignal.timeout(STORE_TIMEOUT_MS),
      });
      await response.body?.cancel();
      if (!response.ok)
        throw new Error(`pipeline event insert failed: ${response.status}`);
    },
    async byTrace(scope, traceId) {
      // Pages are ordered by (at, idempotency_key) so offsets are stable across
      // pages even when events share a timestamp.
      const events: PipelineEvent[] = [];
      for (
        let offset = 0;
        offset < MAX_TRACE_EVENTS;
        offset += TRACE_PAGE_SIZE
      ) {
        const query = new URLSearchParams({
          trace_id: `eq.${traceId}`,
          org_id: `eq.${scope.orgId}`,
          workspace_id: `eq.${scope.workspaceId}`,
          order: "at.asc,idempotency_key.asc",
          limit: String(TRACE_PAGE_SIZE),
          offset: String(offset),
        });
        const response = await send(`${base}?${query}`, {
          headers,
          signal: AbortSignal.timeout(STORE_TIMEOUT_MS),
        });
        if (!response.ok) {
          await response.body?.cancel();
          throw new PipelineStoreUnavailable();
        }
        const page = (await response.json()) as Row[];
        events.push(...page.map(fromRow));
        if (page.length < TRACE_PAGE_SIZE) return { events, truncated: false };
      }
      // Exactly MAX_TRACE_EVENTS is complete; only a row beyond it means partial.
      const probe = new URLSearchParams({
        trace_id: `eq.${traceId}`,
        org_id: `eq.${scope.orgId}`,
        workspace_id: `eq.${scope.workspaceId}`,
        select: "trace_id",
        limit: "1",
        offset: String(MAX_TRACE_EVENTS),
      });
      const response = await send(`${base}?${probe}`, {
        headers,
        signal: AbortSignal.timeout(STORE_TIMEOUT_MS),
      });
      if (!response.ok) {
        await response.body?.cancel();
        throw new PipelineStoreUnavailable();
      }
      return {
        events,
        truncated: ((await response.json()) as unknown[]).length > 0,
      };
    },
  };
}

export interface PipelineSinkEnv {
  PIPELINE_EVENTS_URL?: string;
  PIPELINE_EVENTS_TOKEN?: string;
  PIPELINE_EVENTS_SCHEMA?: string;
}

/** PostgREST when both URL and token are configured, otherwise log-only. */
export function pipelineEventSinkFromEnv(
  env: PipelineSinkEnv,
): PipelineEventSink {
  if (env.PIPELINE_EVENTS_URL && env.PIPELINE_EVENTS_TOKEN)
    return postgrestPipelineEventSink({
      url: env.PIPELINE_EVENTS_URL,
      token: env.PIPELINE_EVENTS_TOKEN,
      schema: env.PIPELINE_EVENTS_SCHEMA,
    });
  return logOnlyPipelineEventSink;
}

/** Log/trace identity fields an attribute may never supply. */
export const RESERVED_ATTR_KEYS = new Set([
  "level",
  "source",
  "message",
  "environment",
  "trace_id",
  "org_id",
  "workspace_id",
  "machine_id",
  "session_id",
  "harness",
  "stage",
  "status",
  "attempt",
  "error_code",
  "error_name",
]);

function withoutReservedKeys(attrs: Record<string, unknown> | undefined) {
  if (!attrs) return {};
  return Object.fromEntries(
    Object.entries(attrs).filter(([key]) => !RESERVED_ATTR_KEYS.has(key)),
  );
}

function durationMs(attrs: Record<string, unknown> | undefined) {
  const value = attrs?.duration_ms;
  return typeof value === "number" && Number.isInteger(value) && value >= 0
    ? value
    : undefined;
}

export interface RecordOptions {
  attempt?: number;
  error?: unknown;
  errorCode?: string;
  attrs?: Record<string, unknown>;
  /** Extra idempotency discriminator (e.g. a chunk seq or a replay id). */
  key?: string;
  now?: Date;
}

/**
 * Records stage transitions for one trace: a structured log line carrying
 * trace_id/machine_id, a pipeline_events row, and an error report on failure.
 */
export class PipelineRecorder {
  readonly log: Logger;
  /** The caller's context with unsafe client ids dropped (`safeTraceContext`). */
  readonly ctx: TraceContext;
  /**
   * `ctx` as telemetry carries it: machine and session ids keyed-hashed (or
   * dropped without a key). Log lines and error reports use this; the event
   * row, which goes to service-role storage, keeps the raw ids.
   */
  readonly telemetryCtx: TraceContext;

  constructor(
    ctx: TraceContext,
    private readonly deps: {
      sink: PipelineEventSink;
      reporter?: ErrorReporter;
      logger?: LoggerEnv;
      /** Merged under every event's attrs, e.g. the queue message's `work_key`. */
      attrs?: Record<string, unknown>;
    },
  ) {
    this.ctx = safeTraceContext(ctx);
    this.telemetryCtx = telemetryContext(this.ctx, deps.logger?.telemetryIdKey);
    const key = deps.logger?.telemetryIdKey;
    const log = createLogger(
      deps.logger ?? { environment: "development" },
      "pipeline",
      traceLogFields(this.telemetryCtx),
    );
    // Every field any caller logs (the recorder's own lines and a handler's
    // `recorder.log`) passes through the id redaction first: object-key
    // segments, and this trace's own raw machine and session ids anywhere.
    const redacted =
      (level: "debug" | "info" | "warn" | "error") =>
      (message: string, fields?: LogFields) =>
        log[level](
          message,
          fields &&
            telemetryFields(fields, key, {
              machineId: this.ctx.machineId,
              sessionId: this.ctx.sessionId,
            }),
        );
    this.log = {
      debug: redacted("debug"),
      info: redacted("info"),
      warn: redacted("warn"),
      error: redacted("error"),
      flush: () => log.flush(),
    };
  }

  /** Never throws. Resolves after the sink write settles. */
  async record(
    stage: PipelineStage,
    status: PipelineStatus,
    options: RecordOptions = {},
  ): Promise<PipelineEvent> {
    const attempt = options.attempt ?? 1;
    const attrs = { ...this.deps.attrs, ...options.attrs };
    const failed =
      status === "error" || status === "retrying" || status === "dead_lettered";
    const error =
      options.error !== undefined || (failed && options.errorCode)
        ? pipelineErrorInfo(options.error, options.errorCode)
        : undefined;
    const event: PipelineEvent = {
      traceId: this.ctx.traceId,
      workspaceId: this.ctx.workspaceId,
      orgId: this.ctx.orgId,
      ...(this.ctx.machineId ? { machineId: this.ctx.machineId } : {}),
      ...(this.ctx.harness ? { harness: this.ctx.harness } : {}),
      ...(this.ctx.sessionId ? { sessionId: this.ctx.sessionId } : {}),
      stage,
      status,
      attempt,
      ...(error ? { error } : {}),
      attrs,
      at: (options.now ?? new Date()).toISOString(),
      ...(durationMs(attrs) !== undefined
        ? { durationMs: durationMs(attrs) }
        : {}),
      idempotencyKey: pipelineEventKey(
        this.ctx.traceId,
        stage,
        status,
        attempt,
        options.key,
      ),
    };
    // Attributes first: identity fields set by this recorder always win.
    const fields = {
      ...withoutReservedKeys(attrs),
      stage,
      status,
      attempt,
      ...(error ? { error_code: error.code, error_name: error.name } : {}),
    };
    if (failed) this.log.warn("pipeline stage", fields);
    else this.log.info("pipeline stage", fields);
    if (error && failed) {
      try {
        (this.deps.reporter ?? noopErrorReporter).capture({
          ctx: this.telemetryCtx,
          stage,
          error,
          attempt,
        });
      } catch {
        // Reporting is best effort.
      }
    }
    try {
      await this.deps.sink.record([event]);
    } catch {
      this.log.warn("pipeline event write failed", {
        stage,
        status,
        sink: this.deps.sink.kind,
      });
    }
    return event;
  }

  async flush() {
    await Promise.allSettled([
      this.log.flush(),
      this.deps.reporter?.flush() ?? Promise.resolve(),
    ]);
  }
}
