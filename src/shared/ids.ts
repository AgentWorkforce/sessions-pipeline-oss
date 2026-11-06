/** UUIDv7, W3C traceparent and the small id helpers the pipeline shares. */
import { randomBytes } from "node:crypto";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UUID7_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/** RFC 9562 UUIDv7: 48-bit ms timestamp, then random. Sorts by time. */
export function uuidv7(now = Date.now()): string {
  const b = randomBytes(16);
  const ts = BigInt(now);
  b[0] = Number((ts >> 40n) & 0xffn);
  b[1] = Number((ts >> 32n) & 0xffn);
  b[2] = Number((ts >> 24n) & 0xffn);
  b[3] = Number((ts >> 16n) & 0xffn);
  b[4] = Number((ts >> 8n) & 0xffn);
  b[5] = Number(ts & 0xffn);
  b[6] = (b[6]! & 0x0f) | 0x70;
  b[8] = (b[8]! & 0x3f) | 0x80;
  const h = b.toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

export const isUuid = (s: unknown): s is string => typeof s === "string" && UUID_RE.test(s);
export const isUuidV7 = (s: unknown): s is string => typeof s === "string" && UUID7_RE.test(s);

export function spanId(): string {
  return randomBytes(8).toString("hex");
}

/**
 * The pipeline trace id (a UUIDv7 minted per manifest revision) is also the W3C
 * trace id: 32 hex chars, no dashes. Every span for that revision joins one trace.
 */
export function traceparentFor(traceId: string, span = spanId(), sampled = true): string {
  return `00-${traceId.replace(/-/g, "").toLowerCase()}-${span}-${sampled ? "01" : "00"}`;
}

export function parseTraceparent(tp: string | undefined): { traceId: string; spanId: string; sampled: boolean } | undefined {
  if (!tp) return undefined;
  const m = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i.exec(tp.trim());
  if (!m) return undefined;
  return { traceId: m[1]!.toLowerCase(), spanId: m[2]!.toLowerCase(), sampled: (parseInt(m[3]!, 16) & 1) === 1 };
}

/** `uuidv7` as a bare 32-hex W3C trace id. */
export const hexTraceId = (uuid: string) => uuid.replace(/-/g, "").toLowerCase();
