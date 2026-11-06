/**
 * P5b G2: the Desktop onboarding heartbeat, served by the session ingest Worker over the
 * signed DELIVERY_INGEST binding (relayhistory-cloud forwards when ONBOARDING_BACKEND=ingest).
 * Same request validation and response shape as relayhistory-cloud's routes/onboarding.ts;
 * storage is history.probe_connections (supabase/history-migrations/0002).
 */
export type Run = (text: string, params: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>;

export class ProgressError extends Error {}

const PHASES = ["scanning", "uploading", "watching", "paused", "capture_paused"];
const SOURCES = ["", "local history", "initializing", "claude-history", "claude", "codex", "cursor", "grok",
  // Older probes still report this phase; rejecting it would mark them disconnected.
  "trajectory", "opencode", "muse", "devin", "catalog", "complete"];

/** Only counters and known phases cross this boundary; never paths or content (relayhistory parseProgress). */
export function parseProgress(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new ProgressError("Invalid progress");
  const p = value as Record<string, unknown>;
  const count = (key: string) => {
    const n = p[key];
    if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) throw new ProgressError("Invalid count");
    return n;
  };
  if (typeof p.phase !== "string" || !PHASES.includes(p.phase) || typeof p.source !== "string" ||
      !SOURCES.includes(p.source) || typeof p.backlogComplete !== "boolean")
    throw new ProgressError("Invalid progress");
  const processedFiles = count("processedFiles");
  const totalFiles = p.totalFiles === null ? null : count("totalFiles");
  if (totalFiles !== null && processedFiles > totalFiles) throw new ProgressError("Invalid total");
  return { phase: p.phase, source: p.source, processedFiles, totalFiles, sessionsCaptured: count("sessionsCaptured"),
    recordsUploaded: count("recordsUploaded"), recordsQueued: count("recordsQueued"), backlogComplete: p.backlogComplete };
}

/** Parses the heartbeat body exactly as relayhistory does: empty body or { progress?: … }, at most 4096 bytes. */
export function heartbeatProgress(body: Uint8Array) {
  if (body.byteLength > 4096) return { tooLarge: true as const };
  const text = new TextDecoder().decode(body);
  if (!text) return { progress: null };
  const payload = JSON.parse(text);
  return { progress: payload?.progress != null ? parseProgress(payload.progress) : null };
}

export async function recordHeartbeat(run: Run, t: { orgId: string; workspaceId: string; userId: string }, progress: unknown) {
  const at = (await run("SELECT history.probe_heartbeat($1, $2, $3, $4::jsonb) AS at", [t.orgId, t.workspaceId, t.userId, progress === null ? null : JSON.stringify(progress)])).rows[0]?.at;
  return new Date(at as string | Date).toISOString();
}

export async function readConnection(run: Run, t: { orgId: string; workspaceId: string; userId: string }) {
  const c = (await run("SELECT history.probe_connection($1, $2, $3) AS c", [t.orgId, t.workspaceId, t.userId])).rows[0]?.c as { lastSeenAt: string; progress: unknown } | null | undefined;
  return c ? { lastSeenAt: new Date(c.lastSeenAt).toISOString(), progress: c.progress ?? null } : { lastSeenAt: null, progress: null };
}
