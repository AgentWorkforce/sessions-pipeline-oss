/**
 * OpenTelemetry for every 96bulls service. Spans carry allowlisted attributes
 * only (plan section 12): ids, codes, counts. The trace id of every span is the
 * pipeline trace id (the manifest's UUIDv7), so one revision of one session is
 * one trace from AlleyOop through HoopDb. `session_uuid` is an attribute, never
 * a metric label.
 */
import { context, propagation, trace, SpanStatusCode, type Span, type Tracer } from "@opentelemetry/api";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, BatchSpanProcessor, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { scrubText } from "@rhc/lib/scrub.ts";
import { ALLOWED_LOG_FIELDS, log } from "./log.ts";

let provider: BasicTracerProvider | undefined;
let tracer: Tracer | undefined;

export function initTelemetry(serviceName = process.env.OTEL_SERVICE_NAME ?? "96bulls"): Tracer {
  if (tracer) return tracer;
  const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  const resource = resourceFromAttributes({
    "service.name": serviceName,
    "service.namespace": "96bulls",
    deployment: process.env.DEPLOYMENT ?? "selfhost",
    deployment_id: process.env.DEPLOYMENT_ID ?? "local",
  });
  const processors = endpoint
    ? [
        process.env.OTEL_SYNC_EXPORT === "1"
          ? new SimpleSpanProcessor(new OTLPTraceExporter({ url: `${endpoint.replace(/\/$/, "")}/v1/traces` }))
          : new BatchSpanProcessor(new OTLPTraceExporter({ url: `${endpoint.replace(/\/$/, "")}/v1/traces` }), {
              scheduledDelayMillis: 1_000,
            }),
      ]
    : [];
  provider = new BasicTracerProvider({ resource, spanProcessors: processors });
  trace.setGlobalTracerProvider(provider);
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  tracer = trace.getTracer("96bulls", "0.1.0");
  log.info("telemetry initialised", { ok: Boolean(endpoint) });
  return tracer;
}

export async function shutdownTelemetry(): Promise<void> {
  await provider?.forceFlush().catch(() => {});
  await provider?.shutdown().catch(() => {});
}

export function flushTelemetry(): Promise<void> {
  return provider?.forceFlush().catch(() => {}) ?? Promise.resolve();
}

/** Attribute keys spans may carry. Same allowlist as logs. */
export function spanAttrs(fields: Record<string, unknown>): Record<string, string | number | boolean> {
  const out: Record<string, string | number | boolean> = {};
  for (const [k, v] of Object.entries(fields)) {
    if (!ALLOWED_LOG_FIELDS.has(k) || v === undefined || v === null) continue;
    if (typeof v === "string") out[k] = scrubText(v).slice(0, 256);
    else if (typeof v === "number" || typeof v === "boolean") out[k] = v;
  }
  return out;
}

/**
 * Runs `fn` inside a span that continues the trace named by `traceparent`
 * (W3C). No traceparent: a new root. Attributes are filtered through the allowlist.
 */
export async function withSpan<T>(
  name: string,
  opts: { traceparent?: string; attrs?: Record<string, unknown>; links?: string[] },
  fn: (span: Span) => Promise<T>,
): Promise<T> {
  const t = tracer ?? initTelemetry();
  const parent = opts.traceparent
    ? propagation.extract(context.active(), { traceparent: opts.traceparent })
    : context.active();
  const links = (opts.links ?? [])
    .map((tp) => trace.getSpanContext(propagation.extract(context.active(), { traceparent: tp })))
    .filter((c): c is NonNullable<typeof c> => Boolean(c))
    .map((c) => ({ context: c }));
  return t.startActiveSpan(name, { attributes: spanAttrs(opts.attrs ?? {}), links }, parent, async (span) => {
    try {
      const out = await fn(span);
      span.setStatus({ code: SpanStatusCode.OK });
      return out;
    } catch (err) {
      const e = err as { code?: unknown; retryable?: unknown };
      span.setStatus({ code: SpanStatusCode.ERROR });
      span.setAttributes(spanAttrs({
        error_code: typeof e?.code === "string" ? e.code : "unknown",
        ...(typeof e?.retryable === "boolean" ? { retryable: e.retryable } : {}),
      }));
      throw err;
    } finally {
      span.end();
    }
  });
}

/** The traceparent of the active span, for outgoing headers and queue messages. */
export function currentTraceparent(): string | undefined {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent;
}
