/**
 * One JSON line per event, ids only. Section 12 of the plan: telemetry carries
 * ids, hashes, codes and counts, never session content, prompts, summaries,
 * paths, object bodies or foreign error text. The allowlist is enforced here,
 * at the only place a service writes a log line.
 */
import { scrubText } from "@rhc/lib/scrub.ts";

export const ALLOWED_LOG_FIELDS = new Set([
  "service", "deployment", "deployment_id", "stage", "status", "outcome", "attempt",
  "trace_id", "span_id", "traceparent", "session_uuid", "session_uuid_source",
  "tenant_id", "org_id", "workspace_id", "machine_id", "harness", "session_id",
  "revision", "work_key", "error_code", "retryable", "duration_ms",
  "scrub_ruleset", "scrub_version", "redaction_count",
  "object_sha256", "bytes_raw", "bytes_stored", "chunk_count", "seq", "epoch", "offset",
  "model_id", "tokens_in", "tokens_out",
  "count", "inserted", "deleted", "manifests", "claimed", "acked", "retried", "exhausted", "listed", "enqueued", "skipped", "failed",
  "state", "rule", "kind", "port", "ok", "reason", "key_sha256", "manifest_key_sha256",
  "migration", "ledger_sha", "accepted_as_alias_of",
  "route", "correlation_id",
]);

export type Level = "debug" | "info" | "warn" | "error";
const service = process.env.OTEL_SERVICE_NAME ?? "96bulls";
const deployment = process.env.DEPLOYMENT ?? "selfhost";
const deploymentId = process.env.DEPLOYMENT_ID ?? "local";

function filter(fields: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(fields)) {
    if (!ALLOWED_LOG_FIELDS.has(k)) continue;
    if (v === undefined) continue;
    // Strings are scrubbed again before emit as a backstop; ids never match a rule.
    out[k] = typeof v === "string" ? scrubText(v).slice(0, 256) : v;
  }
  return out;
}

/** The JSON line `emit` writes, for a runtime without process streams (the session read Worker logs it to the console). */
export function logLine(level: Level, message: string, fields: Record<string, unknown>): string {
  return JSON.stringify({
    ts: new Date().toISOString(),
    level,
    service,
    deployment,
    deployment_id: deploymentId,
    message: scrubText(message).slice(0, 200),
    ...filter(fields),
  });
}

function emit(level: Level, message: string, fields: Record<string, unknown>) {
  const line = logLine(level, message, fields);
  if (level === "error" || level === "warn") process.stderr.write(line + "\n");
  else process.stdout.write(line + "\n");
}

export const log = {
  debug: (m: string, f: Record<string, unknown> = {}) => emit("debug", m, f),
  info: (m: string, f: Record<string, unknown> = {}) => emit("info", m, f),
  warn: (m: string, f: Record<string, unknown> = {}) => emit("warn", m, f),
  error: (m: string, f: Record<string, unknown> = {}) => emit("error", m, f),
};

/** The code and retryable flag of an error, never its message. */
export function errorFields(err: unknown): Record<string, unknown> {
  const e = err as { code?: unknown; retryable?: unknown; name?: unknown };
  return {
    error_code: typeof e?.code === "string" ? e.code : typeof e?.name === "string" ? e.name : "unknown",
    ...(typeof e?.retryable === "boolean" ? { retryable: e.retryable } : {}),
  };
}
