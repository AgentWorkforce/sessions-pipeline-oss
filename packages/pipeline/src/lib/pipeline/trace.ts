/**
 * Session pipeline trace context.
 *
 * Every session uploaded through the redesigned pipeline carries one `trace_id`
 * (a UUIDv7, minted on the machine when the session is first seen) from local
 * ingest through object storage, the storage event queue, the optional
 * normalize transform, the pointer index and later fetches. Each stage records
 * itself against that id, so "did my upload land?" is a lookup by one key.
 *
 * The id travels as `X-Relay-Trace-Id` (dashed UUID) or a W3C `traceparent`
 * whose 32-hex trace id is the same UUID without dashes, so an OTEL collector in
 * front of a self-hosted install sees the same trace the pipeline records.
 *
 * Nothing here reads request bodies or session content.
 */
import { scrubText } from "../scrub.js";

/**
 * Span stages (`ingest`, `deliver.put`, `fetch`) are recorded by the client and
 * the request path; the rest are queued stages (see `registry.ts`).
 */
export const PIPELINE_STAGES = [
  "ingest",
  "deliver.put",
  "object-landed",
  "session-indexed",
  "summary-short",
  "summary-long",
  "status-classify",
  "file-normalize",
  "file-maintenance",
  "backfill-activation",
  "fetch",
] as const;
export type PipelineStage = (typeof PIPELINE_STAGES)[number];

export const PIPELINE_STATUSES = [
  "started",
  "ok",
  "error",
  "retrying",
  "dead_lettered",
  "replayed",
  "skipped",
] as const;
export type PipelineStatus = (typeof PIPELINE_STATUSES)[number];

export const TRACE_ID_HEADER = "X-Relay-Trace-Id";
export const TRACEPARENT_HEADER = "traceparent";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TRACEPARENT = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i;

export function isPipelineStage(value: unknown): value is PipelineStage {
  return (
    typeof value === "string" &&
    (PIPELINE_STAGES as readonly string[]).includes(value)
  );
}

/** Any RFC 4122 UUID; v7 is minted here but older clients may send v4. */
export function isTraceId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    UUID.test(value) &&
    value !== "00000000-0000-0000-0000-000000000000"
  );
}

const HEX = Array.from({ length: 256 }, (_, byte) =>
  byte.toString(16).padStart(2, "0"),
);

/** RFC 9562 UUIDv7: 48-bit unix ms, then random bits. Time-ordered for `(trace_id, at)` scans. */
export function uuidv7(now = Date.now()): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  // 48 bits do not fit a 32-bit bitwise op: split into high 16 and low 32.
  const high = Math.floor(now / 0x100000000);
  const low = now >>> 0;
  bytes[0] = (high >>> 8) & 0xff;
  bytes[1] = high & 0xff;
  bytes[2] = low >>> 24;
  bytes[3] = (low >>> 16) & 0xff;
  bytes[4] = (low >>> 8) & 0xff;
  bytes[5] = low & 0xff;
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  let out = "";
  for (let index = 0; index < 16; index += 1) {
    if (index === 4 || index === 6 || index === 8 || index === 10) out += "-";
    out += HEX[bytes[index]!];
  }
  return out;
}

export function traceIdToOtel(traceId: string): string {
  return traceId.replaceAll("-", "").toLowerCase();
}

export function otelToTraceId(hex: string): string {
  const h = hex.toLowerCase();
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/** A random 64-bit span id as 16 hex characters. */
export function newSpanId(): string {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  let out = "";
  for (const byte of bytes) out += HEX[byte];
  return out;
}

export function formatTraceparent(traceId: string, spanId = newSpanId()) {
  return `00-${traceIdToOtel(traceId)}-${spanId}-01`;
}

/**
 * The caller's trace id, from `X-Relay-Trace-Id` first, then `traceparent`.
 * Header values are caller-controlled: anything that is not a well-formed id is
 * ignored rather than echoed into logs or stored.
 */
export function traceIdFromHeaders(
  get: (name: string) => string | undefined | null,
): string | undefined {
  const direct = get(TRACE_ID_HEADER)?.trim();
  if (isTraceId(direct)) return direct.toLowerCase();
  const parent = TRACEPARENT.exec(get(TRACEPARENT_HEADER)?.trim() ?? "");
  if (parent && !/^0+$/.test(parent[1]!)) return otelToTraceId(parent[1]!);
  return undefined;
}

/**
 * The identity every pipeline record, log line and DLQ message carries. Tenant
 * fields come from resolved auth (or the storage event's object path, which the
 * server wrote), never a client body.
 */
export interface TraceContext {
  traceId: string;
  /** Tenant boundary with `workspaceId`: workspace ids are not unique across orgs. */
  orgId: string;
  workspaceId: string;
  machineId?: string;
  sessionId?: string;
  harness?: string;
}

const IDENTIFIER = /^[A-Za-z0-9._:@/=-]{1,256}$/;
// Underscores separate runs too, so a namespaced id (`machine_<ULID>`) is
// judged by its parts, while a token after its prefix (`ghp_<token>`) still is.
const LONG_RUN = /[A-Za-z0-9]{20,}/g;

/**
 * A long run of mixed letters and digits that is not a hex digest: the shape of
 * a random token, wherever it sits. The scrubber's patterns are anchored on word
 * boundaries, so `x` + a token inside one id slips past them; this does not.
 */
function hasTokenShapedRun(value: string) {
  for (const [run] of value.matchAll(LONG_RUN))
    if (
      /[A-Za-z]/.test(run) &&
      /\d/.test(run) &&
      !/^[0-9a-fA-F]+$/.test(run) &&
      // ULIDs are ids, not tokens (Crockford base32, 26 characters, either case).
      !/^[0-9A-HJKMNP-TV-Z]{26}$/i.test(run)
    )
      return true;
  return false;
}

/**
 * An identifier safe to store and log: identifier-shaped, nothing the
 * secret/PII scrubber would redact (a token, key or email passed off as an id),
 * and no token-shaped run (`hasTokenShapedRun`). UUIDs, ULIDs, hex digests
 * and hostnames pass.
 */
export function isSafeIdentifier(value: unknown): value is string {
  return (
    typeof value === "string" &&
    IDENTIFIER.test(value) &&
    !hasTokenShapedRun(value) &&
    scrubText(value) === value
  );
}

/**
 * The context with any unsafe optional id dropped. Machine, session and harness
 * ids can come from a client; they never reach a log line, an export or a row
 * unless they pass `isSafeIdentifier`.
 */
export function safeTraceContext(ctx: TraceContext): TraceContext {
  const { machineId, sessionId, harness, ...rest } = ctx;
  return {
    ...rest,
    ...(isSafeIdentifier(machineId) ? { machineId } : {}),
    ...(isSafeIdentifier(sessionId) ? { sessionId } : {}),
    ...(isSafeIdentifier(harness) ? { harness } : {}),
  };
}

/** The snake_case log fields for a trace context; ids only. */
export function traceLogFields(ctx: TraceContext, stage?: PipelineStage) {
  return {
    trace_id: ctx.traceId,
    org_id: ctx.orgId,
    workspace_id: ctx.workspaceId,
    ...(ctx.machineId ? { machine_id: ctx.machineId } : {}),
    ...(ctx.sessionId ? { session_id: ctx.sessionId } : {}),
    ...(ctx.harness ? { harness: ctx.harness } : {}),
    ...(stage ? { stage } : {}),
  };
}

const OBJECT_SEGMENTS =
  /(?:^|\/)v1\/ws=([^/]+)\/m=([^/]+)\/h=([^/]+)\/d=\d{4}\/\d{2}\/\d{2}\/s=([^/]+)\//;

/**
 * Trace identity recoverable from a canonical object key
 * (`{prefix}/v1/ws=/m=/h=/d=yyyy/mm/dd/s=/...`). The trace id itself lives in
 * the manifest, not the path; storage events carry it from there.
 */
export function objectKeyIdentity(
  key: string,
): Omit<TraceContext, "traceId" | "orgId"> | null {
  const match = OBJECT_SEGMENTS.exec(key);
  if (!match) return null;
  const [, workspaceId, machineId, harness, sessionId] = match.map((part) =>
    part === undefined ? part : decodeURIComponent(part),
  );
  return { workspaceId: workspaceId!, machineId, harness, sessionId };
}
