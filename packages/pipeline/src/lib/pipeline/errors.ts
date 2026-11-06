/**
 * Pluggable error tracking for the session pipeline.
 *
 * A stage failure is reduced to a `PipelineErrorInfo` before any reporter sees
 * it: a stable code, the error class name, and a message only when the error is
 * a `PipelineError` this service constructed. Database, provider and storage
 * exception text is never forwarded (it can carry SQL, keys or history), which
 * matches the rule the rest of this Worker follows for its console logs.
 *
 * Reporters are chosen by `PIPELINE_ERROR_TRACKER`:
 *
 *   - `otlp` (default, vendor neutral for self-hosted installs): OTLP/HTTP JSON
 *     logs to `OTEL_EXPORTER_OTLP_ENDPOINT`, with
 *     `OTEL_EXPORTER_OTLP_HEADERS` (standard OTEL env names), so any collector
 *     (Grafana, Honeycomb, Datadog, a self-hosted otelcol) receives them with the
 *     pipeline `trace_id` set on the record. With no endpoint configured this is
 *     console only, so an unconfigured install still logs every failure.
 *   - `sentry` (optional): the Sentry envelope endpoint derived from `SENTRY_DSN`.
 *   - `console`: one structured error line, nothing leaves the process.
 *   - `none`: drop.
 *
 * An enterprise deployment can also pass its own `ErrorReporter` to the pipeline
 * deps. DSNs and OTLP headers are runtime secrets; none is committed and none is
 * ever logged. Reporting is best effort and never throws into a stage.
 */
import type { PipelineStage, TraceContext } from "./trace.js";
import { newSpanId, traceIdToOtel, traceLogFields } from "./trace.js";

export interface PipelineErrorInfo {
  /** Stable machine-readable code, e.g. `storage_put_failed`, `transform_failed`. */
  code: string;
  /** Error class name (`TypeError`, `PipelineError`); never the message of a foreign error. */
  name: string;
  /** Present only for messages this service authored. */
  message?: string;
  retryable: boolean;
}

/** An error whose code and message are safe to store and export. */
export class PipelineError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable = true,
  ) {
    super(message);
    this.name = "PipelineError";
  }
}

/** Deadline for one export; a slow collector must not hold a queue message. */
export const EXPORT_TIMEOUT_MS = 3_000;

const SAFE_CODE = /^[A-Za-z0-9_.:-]{1,64}$/;
const MAX_MESSAGE = 500;

export function pipelineErrorInfo(
  error: unknown,
  requestedFallback = "stage_failed",
): PipelineErrorInfo {
  // Fallback codes can come from clients; they get the same check as any code.
  const fallbackCode = SAFE_CODE.test(requestedFallback)
    ? requestedFallback
    : "stage_failed";
  if (error instanceof PipelineError) {
    return {
      code: SAFE_CODE.test(error.code) ? error.code : fallbackCode,
      name: error.name,
      message: error.message.slice(0, MAX_MESSAGE),
      retryable: error.retryable,
    };
  }
  const code = (error as { code?: unknown } | null)?.code;
  const name = error instanceof Error ? error.name : typeof error;
  return {
    code:
      typeof code === "string" && SAFE_CODE.test(code) ? code : fallbackCode,
    name: SAFE_CODE.test(name) ? name : "Error",
    retryable: true,
  };
}

export interface ErrorReport {
  ctx: TraceContext;
  stage: PipelineStage;
  error: PipelineErrorInfo;
  attempt?: number;
}

export interface ErrorReporter {
  readonly kind: string;
  capture(report: ErrorReport): void;
  /** Resolves once every export started by this reporter has settled. */
  flush(): Promise<void>;
}

export const noopErrorReporter: ErrorReporter = {
  kind: "none",
  capture() {},
  flush: async () => {},
};

export function consoleErrorReporter(
  environment = "development",
): ErrorReporter {
  return {
    kind: "console",
    capture({ ctx, stage, error, attempt }) {
      console.error(
        JSON.stringify({
          level: "error",
          source: "pipeline",
          message: "pipeline stage failed",
          environment,
          ...traceLogFields(ctx, stage),
          error_code: error.code,
          error_name: error.name,
          retryable: error.retryable,
          ...(attempt !== undefined ? { attempt } : {}),
        }),
      );
    },
    flush: async () => {},
  };
}

/** Reporter that posts each capture with `send`, tracking the promises for `flush`. */
function exportingReporter(
  kind: string,
  send: (report: ErrorReport) => Promise<void>,
): ErrorReporter {
  const pending = new Set<Promise<void>>();
  return {
    kind,
    capture(report) {
      const promise = send(report).catch(() => {
        // Best effort: never include the transport error, which may echo headers.
        console.warn(
          JSON.stringify({
            source: "pipeline",
            message: "error export failed",
            reporter: kind,
          }),
        );
      });
      pending.add(promise);
      void promise.finally(() => pending.delete(promise));
    },
    flush: async () => {
      if (pending.size) await Promise.allSettled([...pending]);
    },
  };
}

/** `k1=v1,k2=v2` per the OTEL spec, values URL-decoded. */
export function parseOtlpHeaders(value: string | undefined) {
  const headers: Record<string, string> = {};
  for (const pair of (value ?? "").split(",")) {
    const index = pair.indexOf("=");
    if (index <= 0) continue;
    try {
      headers[decodeURIComponent(pair.slice(0, index).trim())] =
        decodeURIComponent(pair.slice(index + 1).trim());
    } catch {
      // A malformed escape skips that header; reporting config never fails a request.
    }
  }
  return headers;
}

/** The OTLP/JSON body for one error report, exported so a test can pin its shape. */
export function otlpErrorPayload(
  report: ErrorReport,
  environment: string,
  now = Date.now(),
) {
  const nanos = `${now}000000`;
  const attrs: Record<string, string | number | boolean> = {
    ...traceLogFields(report.ctx, report.stage),
    "exception.type": report.error.name,
    "error.code": report.error.code,
    "error.retryable": report.error.retryable,
    ...(report.error.message
      ? { "exception.message": report.error.message }
      : {}),
    ...(report.attempt !== undefined ? { attempt: report.attempt } : {}),
  };
  return {
    resourceLogs: [
      {
        resource: {
          attributes: [
            {
              key: "service.name",
              value: { stringValue: "relayhistory-pipeline" },
            },
            {
              key: "deployment.environment",
              value: { stringValue: environment },
            },
          ],
        },
        scopeLogs: [
          {
            scope: { name: "relayhistory.pipeline.errors" },
            logRecords: [
              {
                timeUnixNano: nanos,
                observedTimeUnixNano: nanos,
                severityNumber: 17,
                severityText: "ERROR",
                traceId: traceIdToOtel(report.ctx.traceId),
                spanId: newSpanId(),
                body: { stringValue: "pipeline stage failed" },
                attributes: Object.entries(attrs).map(([key, value]) => ({
                  key,
                  value:
                    typeof value === "boolean"
                      ? { boolValue: value }
                      : typeof value === "number"
                        ? { intValue: String(value) }
                        : { stringValue: value },
                })),
              },
            ],
          },
        ],
      },
    ],
  };
}

export function otlpErrorReporter(options: {
  endpoint: string;
  headers?: Record<string, string>;
  environment?: string;
}): ErrorReporter {
  const base = options.endpoint.replace(/\/+$/, "");
  const url = base.endsWith("/v1/logs") ? base : `${base}/v1/logs`;
  return exportingReporter("otlp", async (report) => {
    const response = await fetch(url, {
      method: "POST",
      headers: { ...options.headers, "Content-Type": "application/json" },
      body: JSON.stringify(
        otlpErrorPayload(report, options.environment ?? "development"),
      ),
      signal: AbortSignal.timeout(EXPORT_TIMEOUT_MS),
    });
    await response.body?.cancel();
    // Rejected (401, 429, 5xx) counts as failed so the export warning fires.
    if (!response.ok) throw new Error(`export rejected: ${response.status}`);
  });
}

export interface SentryTarget {
  envelopeUrl: string;
  publicKey: string;
}

/** `https://<key>@<host>[/<path>]/<project>` → the envelope endpoint and key. */
export function parseSentryDsn(dsn: string): SentryTarget | null {
  try {
    const url = new URL(dsn);
    const project = url.pathname.split("/").filter(Boolean).pop();
    if (!url.username || !project) return null;
    const path = url.pathname.slice(0, url.pathname.lastIndexOf("/"));
    return {
      envelopeUrl: `${url.protocol}//${url.host}${path}/api/${project}/envelope/`,
      publicKey: url.username,
    };
  } catch {
    return null;
  }
}

/** The Sentry envelope body for one report, exported so a test can pin its shape. */
export function sentryEnvelope(
  report: ErrorReport,
  environment: string,
  now = new Date(),
) {
  const eventId = crypto.randomUUID().replaceAll("-", "");
  const event = {
    event_id: eventId,
    timestamp: now.getTime() / 1000,
    platform: "javascript",
    level: "error",
    logger: "relayhistory.pipeline",
    environment,
    exception: {
      values: [
        {
          type: report.error.name,
          value: report.error.message ?? report.error.code,
        },
      ],
    },
    tags: {
      stage: report.stage,
      error_code: report.error.code,
      workspace_id: report.ctx.workspaceId,
      ...(report.ctx.machineId ? { machine_id: report.ctx.machineId } : {}),
      ...(report.ctx.harness ? { harness: report.ctx.harness } : {}),
    },
    contexts: {
      trace: {
        trace_id: traceIdToOtel(report.ctx.traceId),
        span_id: newSpanId(),
        op: `pipeline.${report.stage}`,
      },
    },
    extra: {
      pipeline_trace_id: report.ctx.traceId,
      ...(report.ctx.sessionId ? { session_id: report.ctx.sessionId } : {}),
      ...(report.attempt !== undefined ? { attempt: report.attempt } : {}),
    },
  };
  return [
    JSON.stringify({ event_id: eventId, sent_at: now.toISOString() }),
    JSON.stringify({ type: "event" }),
    JSON.stringify(event),
  ].join("\n");
}

export function sentryErrorReporter(options: {
  dsn: string;
  environment?: string;
}): ErrorReporter {
  const target = parseSentryDsn(options.dsn);
  if (!target) return noopErrorReporter;
  return exportingReporter("sentry", async (report) => {
    const response = await fetch(target.envelopeUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-sentry-envelope",
        "X-Sentry-Auth": `Sentry sentry_version=7, sentry_key=${target.publicKey}, sentry_client=relayhistory-pipeline/1`,
      },
      body: sentryEnvelope(report, options.environment ?? "development"),
      signal: AbortSignal.timeout(EXPORT_TIMEOUT_MS),
    });
    await response.body?.cancel();
    // Rejected (401, 429, 5xx) counts as failed so the export warning fires.
    if (!response.ok) throw new Error(`export rejected: ${response.status}`);
  });
}

/** Fan one capture out to several reporters (e.g. console plus OTLP). */
export function composeErrorReporters(
  ...reporters: ErrorReporter[]
): ErrorReporter {
  return {
    kind: reporters.map((reporter) => reporter.kind).join("+"),
    capture(report) {
      for (const reporter of reporters) {
        try {
          reporter.capture(report);
        } catch {
          // One broken reporter must not hide the error from the others.
        }
      }
    },
    flush: async () => {
      await Promise.allSettled(reporters.map((reporter) => reporter.flush()));
    },
  };
}

export interface ErrorTrackerEnv {
  ENVIRONMENT?: string;
  PIPELINE_ERROR_TRACKER?: string;
  OTEL_EXPORTER_OTLP_ENDPOINT?: string;
  OTEL_EXPORTER_OTLP_HEADERS?: string;
  SENTRY_DSN?: string;
}

/**
 * The reporter configured for this deployment. A selected exporter missing its
 * endpoint falls back to console, so a misconfiguration is visible, not silent.
 * Console stays on alongside an exporter: the log line is the record of truth.
 */
export function errorReporterFromEnv(env: ErrorTrackerEnv): ErrorReporter {
  const environment = env.ENVIRONMENT ?? "development";
  const consoleReporter = consoleErrorReporter(environment);
  switch ((env.PIPELINE_ERROR_TRACKER ?? "otlp").toLowerCase()) {
    case "none":
      return noopErrorReporter;
    case "otlp":
      return env.OTEL_EXPORTER_OTLP_ENDPOINT
        ? composeErrorReporters(
            consoleReporter,
            otlpErrorReporter({
              endpoint: env.OTEL_EXPORTER_OTLP_ENDPOINT,
              headers: parseOtlpHeaders(env.OTEL_EXPORTER_OTLP_HEADERS),
              environment,
            }),
          )
        : consoleReporter;
    case "sentry":
      return env.SENTRY_DSN && parseSentryDsn(env.SENTRY_DSN)
        ? composeErrorReporters(
            consoleReporter,
            sentryErrorReporter({ dsn: env.SENTRY_DSN, environment }),
          )
        : consoleReporter;
    default:
      return consoleReporter;
  }
}
