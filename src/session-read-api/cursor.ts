/**
 * Opaque event-page cursors for the session read API.
 *
 * Namespaced `b96e1:` so a caller (Cloud) can keep pagination sticky: a cursor
 * from this service is never sent to relayhistory, whose reader treats an
 * unknown cursor as "first page" (a silent restart, so duplicated results), and
 * a relayhistory cursor (`events-v2:` or the legacy `<iso>|<id>`) is refused
 * here as `foreign_cursor` instead of being guessed at.
 *
 * The position is the full ClickHouse sort-key suffix after the tenant columns
 * (ts in epoch milliseconds, then session_id, source, machine_id, kind,
 * event_id), so a page boundary is exact even inside one millisecond.
 */

export const CURSOR_PREFIX = "b96e1:";

export interface EventPosition {
  tsMs: number;
  sessionId: string;
  source: string;
  machineId: string;
  kind: string;
  eventId: string;
}

export class CursorError extends Error {
  constructor(readonly code: "invalid_cursor" | "foreign_cursor") {
    super(code === "foreign_cursor" ? "cursor was issued by another backend" : "cursor is not valid");
    this.name = "CursorError";
  }
}

const MAX_CURSOR_CHARS = 4096;

export function encodeCursor(p: EventPosition): string {
  const body = JSON.stringify([p.tsMs, p.sessionId, p.source, p.machineId, p.kind, p.eventId]);
  return CURSOR_PREFIX + Buffer.from(body, "utf8").toString("base64url");
}

/** `undefined` for no cursor. Throws CursorError for a foreign or malformed one. */
export function decodeCursor(raw: string | undefined | null): EventPosition | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  if (!raw.startsWith(CURSOR_PREFIX)) throw new CursorError("foreign_cursor");
  if (raw.length > MAX_CURSOR_CHARS) throw new CursorError("invalid_cursor");
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(raw.slice(CURSOR_PREFIX.length), "base64url").toString("utf8"));
  } catch {
    throw new CursorError("invalid_cursor");
  }
  if (!Array.isArray(value) || value.length !== 6) throw new CursorError("invalid_cursor");
  const [tsMs, sessionId, source, machineId, kind, eventId] = value as unknown[];
  if (typeof tsMs !== "number" || !Number.isSafeInteger(tsMs) || tsMs < 0) throw new CursorError("invalid_cursor");
  for (const s of [sessionId, source, machineId, kind, eventId])
    if (typeof s !== "string" || s.length > 1024) throw new CursorError("invalid_cursor");
  if (!sessionId || !source || !eventId) throw new CursorError("invalid_cursor");
  return { tsMs, sessionId: sessionId as string, source: source as string, machineId: machineId as string, kind: kind as string, eventId: eventId as string };
}
