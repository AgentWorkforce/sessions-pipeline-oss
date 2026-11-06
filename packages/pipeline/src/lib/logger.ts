// Stand-in for relayhistory-cloud packages/relayhistory/src/lib/logger.ts.
// Provenance: the file at 8603c141bf7dfcd67687db1e5f02e48c1eb2ef61 (last changed
// upstream in 0715b57; identical at the PR #211 head) with
// integration/relayhistory-cloud/packages/relayhistory/src/lib/logger.ts.patch
// (from PR #210) applied, which adds `telemetryIdKey`. It exists because
// lib/pipeline/{dlq,events,index}.ts and routes/pipeline.ts import it. Below this
// header the original behavior is retained except for the OSS distribution
// requiring both an explicit telemetry host and API key (no default host).

/**
 * Structured logging, on the relaycast engine's convention.
 *
 * Every line is written to the console as one JSON object, so Workers Logs and local
 * `wrangler dev` always have it. When `POSTHOG_API_KEY` is configured the same record
 * is also exported as an OTLP log to PostHog Logs through the telemetry ingest host, best
 * effort: an export failure is swallowed and never fails the request. Callers hand the
 * export promise to `waitUntil` through `pendingLogExports` so it outlives the response.
 *
 * Fields are structured metadata only. Nothing here formats request bodies, query
 * strings, tokens or history content; a caller that passes one has made a mistake the
 * type cannot catch, so keep fields to identifiers, counts and durations.
 */
import { telemetryIdKey } from "./pipeline/telemetry-ids.js";
import type { Bindings } from "../env.js";

export type LogLevel = "debug" | "info" | "warn" | "error";
export type LogFields = Record<string, unknown>;

export const DEFAULT_POSTHOG_HOST = "";
const SERVICE_NAME = "relayhistory-server";
const LOGGER_SCOPE = "relayhistory.server.logger";

const SEVERITY: Record<LogLevel, number> = {
  debug: 5,
  info: 9,
  warn: 13,
  error: 17,
};

export interface LoggerEnv {
  environment: string;
  posthogApiKey?: string;
  posthogHost?: string;
  /** Keys the machine/session id tokens in pipeline telemetry (`telemetry-ids.ts`). */
  telemetryIdKey?: string;
}

export function loggerEnvFromBindings(env: Bindings): LoggerEnv {
  return {
    environment: env.ENVIRONMENT ?? "development",
    posthogApiKey: env.POSTHOG_API_KEY || undefined,
    posthogHost: env.POSTHOG_HOST || undefined,
    telemetryIdKey: telemetryIdKey(env.PIPELINE_TELEMETRY_ID_KEY),
  };
}

export function posthogHost(env: { posthogHost?: string }): string {
  const configured = env.posthogHost ?? DEFAULT_POSTHOG_HOST;
  return configured.endsWith("/") ? configured.slice(0, -1) : configured;
}

type AttributeValue =
  | { stringValue: string }
  | { boolValue: boolean }
  | { intValue: string }
  | { doubleValue: number };

function attributeValue(value: unknown): AttributeValue {
  if (typeof value === "string") return { stringValue: value };
  if (typeof value === "boolean") return { boolValue: value };
  if (typeof value === "number" && Number.isFinite(value))
    return Number.isInteger(value)
      ? { intValue: String(value) }
      : { doubleValue: value };
  if (value === null || value === undefined)
    return { stringValue: String(value) };
  try {
    return { stringValue: JSON.stringify(value) };
  } catch {
    return { stringValue: String(value) };
  }
}

/** The OTLP/JSON body for one log record, exported so a test can pin its shape. */
export function otlpLogPayload(
  env: LoggerEnv,
  level: LogLevel,
  message: string,
  fields: LogFields,
  now = Date.now(),
) {
  const nanos = `${now}000000`;
  return {
    resourceLogs: [
      {
        resource: {
          attributes: [
            { key: "service.name", value: { stringValue: SERVICE_NAME } },
            {
              key: "deployment.environment",
              value: { stringValue: env.environment },
            },
          ],
        },
        scopeLogs: [
          {
            scope: { name: LOGGER_SCOPE },
            logRecords: [
              {
                timeUnixNano: nanos,
                observedTimeUnixNano: nanos,
                severityNumber: SEVERITY[level],
                severityText: level.toUpperCase(),
                body: { stringValue: message },
                attributes: Object.entries(fields).map(([key, value]) => ({
                  key,
                  value: attributeValue(value),
                })),
              },
            ],
          },
        ],
      },
    ],
  };
}

async function exportLog(
  env: LoggerEnv,
  level: LogLevel,
  message: string,
  fields: LogFields,
): Promise<void> {
  try {
    await fetch(`${posthogHost(env)}/i/v1/logs`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.posthogApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(otlpLogPayload(env, level, message, fields)),
    });
  } catch {
    // Best effort. The console copy above is the record of truth.
  }
}

export interface Logger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  error(message: string, fields?: LogFields): void;
  /** Resolves once every export started by this logger has settled. */
  flush(): Promise<void>;
}

export function createLogger(
  env: LoggerEnv,
  source: string,
  base: LogFields = {},
): Logger {
  const pending = new Set<Promise<void>>();
  const write = (level: LogLevel, message: string, fields: LogFields = {}) => {
    const record = {
      level,
      source,
      message,
      environment: env.environment,
      ...base,
      ...fields,
    };
    const line = JSON.stringify(record);
    if (level === "error") console.error(line);
    else if (level === "warn") console.warn(line);
    else if (level === "debug") console.debug(line);
    else console.info(line);
    if (!env.posthogApiKey || !posthogHost(env)) return;
    const { level: _level, message: _message, ...attributes } = record;
    const promise = exportLog(env, level, message, attributes);
    pending.add(promise);
    void promise.finally(() => pending.delete(promise));
  };
  return {
    debug: (message, fields) => write("debug", message, fields),
    info: (message, fields) => write("info", message, fields),
    warn: (message, fields) => write("warn", message, fields),
    error: (message, fields) => write("error", message, fields),
    flush: async () => {
      if (pending.size) await Promise.allSettled([...pending]);
    },
  };
}
