/**
 * The ingest's answer to `chunk_string_too_long`, before a line is sealed.
 *
 * The pipeline scrubber (main's rule set 2, `scrubLong`) segments any string
 * past `MAX_SCRUB_CHARS` that has a safe whitespace cut, and refuses one that
 * has none, such as canonical `*_json` evidence or a base64 dump. Refusing is
 * right for a client that can resend smaller chunks; a delivery batch cannot
 * be re-cut. So, before sealing, every string in the record that `scrubLong`
 * would refuse becomes one redaction marker: fail closed (nothing unscrubbed
 * is stored), and the record and its other fields still land. Everything
 * else goes to the scrubber untouched, so the pipeline's rule set, its
 * version and its digest are unchanged.
 */
import { MAX_SCRUB_CHARS, exceedsScrubBound } from "@rhc/lib/scrub.ts";
import { canScrubLong } from "@rhc/lib/pipeline/raw-chunks.ts";
import type { PreparedDeliveryRecord } from "./vendor/delivery/prepare.ts";

export function oversizeRedaction(chars: number): string {
  return `[REDACTED: ${chars} characters exceed the ${MAX_SCRUB_CHARS}-character scrub limit and could not be segmented safely]`;
}

/**
 * Whether the scrubber would refuse this string. `scrubStringToken` scrubs the
 * decoded value AND the JSON literal (escapes included), and in the literal a
 * newline or tab is `\n` / `\t`, not whitespace, so a newline-wrapped dump
 * that the value check passes can still have no safe cut. Both must pass.
 */
function refused(value: string): boolean {
  if (!exceedsScrubBound(value) && !exceedsScrubBound(JSON.stringify(value))) return false;
  return !canScrubLong(value) || !canScrubLong(JSON.stringify(value));
}

function fit(value: unknown): unknown {
  if (typeof value === "string") return refused(value) ? oversizeRedaction(value.length) : value;
  if (Array.isArray(value)) return value.map(fit);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, fit(v)]));
  return value;
}

/** The record with every string `scrubLong` cannot take redacted (ids are bounded at 512 bytes, so only the payload). */
export function fitForScrub(record: PreparedDeliveryRecord): PreparedDeliveryRecord {
  if (record.payload === null) return record;
  const payload = fit(record.payload) as Record<string, unknown>;
  return payload === record.payload ? record : { ...record, payload };
}

/**
 * A chunk line (`liveLine`) fitted again. The fan-out seals staged lines that
 * were already scrubbed once, and scrubbing is not idempotent in length
 * (`KEY=)` becomes `KEY=[REDACTED]`), so a string can cross the bound on the
 * second pass: fit it again before it is sealed, or an acknowledged batch
 * dead-letters.
 */
export function fitLine(line: string): string {
  const parsed = JSON.parse(line) as { payload?: unknown };
  if (!parsed.payload || typeof parsed.payload !== "object") return line;
  const payload = fit(parsed.payload);
  return JSON.stringify({ ...parsed, payload });
}
