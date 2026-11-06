/**
 * The brief's input: the scrubbed raw of every live machine manifest of one logical
 * session, read from the Locker exactly as Bob Costas reads it (never the unscrubbed
 * source), turned into the engine's NormalizedEvents and then into relayhistory's
 * `summaryInput` (user and assistant text only, 64,000-character budget).
 *
 * Two raw shapes:
 *   - RelayHistory record streams (the Neon export and its live continuation): read with
 *     the pipeline's own `readRelayhistoryExport` and projected with
 *     `projectRelayhistorySession`, the same code that rebuilds Neon's
 *     `convergence_events`. Each projected row then goes through relayhistory's evidence
 *     mapping (`evidence.ts` normalizedEvent: role and kind from kind/actor_role/type),
 *     so a session briefs from the same messages Neon's brief job read.
 *   - Harness transcripts (`jsonl`): user and assistant text parts, Claude- and
 *     Codex-shaped records; sidechain (subagent) and meta records are skipped, as
 *     relayhistory's summary input excludes subagent sessions and tool output.
 *
 * Nothing here logs or returns transcript text except to the caller that builds the
 * model input.
 */
import { zstdDecompressSync } from "node:zlib";
import {
  PipelineError,
  parseSessionKey,
  rawChunkKey,
  readAll,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
} from "../shared/vendor.ts";
import {
  RELAYHISTORY_EXPORT_EXTENSION,
  readRelayhistoryExport,
  relayhistoryExportProvenance,
} from "@rhc/lib/pipeline/relayhistory-transform.ts";
import { projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { NormalizedEvent } from "./engine/normalized-history.ts";
import {
  SESSION_SUMMARY_SOURCES,
  SESSION_SUMMARY_MAX_MESSAGES,
  type SessionSummaryInput,
} from "./engine/summary-contract.ts";
import { summaryInput } from "./engine/summary-input.ts";

/** One live pointer of the logical session, from sessions.session_index. */
export interface BriefPointer {
  machineId: string;
  manifestKey: string;
  revision: number;
}

export type BriefInput =
  | { kind: "ready"; input: SessionSummaryInput; sourceRevisions: Record<string, number> }
  | { kind: "quarantined" | "empty" | "ineligible"; error: string; sourceRevisions: Record<string, number> };

/** Raw bytes read for one session, all machines together. relayhistory-transform's own bound. */
export const MAX_BRIEF_SESSION_BYTES = 256 * 1024 * 1024;

type Row = Record<string, unknown>;

const isObject = (value: unknown): value is Row =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const str = (value: unknown): string | null =>
  typeof value === "string" && value.trim() ? value : null;

/** relayhistory evidence.ts textAnalysisValue, for one string. */
function textForAnalysis(value: string): string {
  return value.replace(
    /data:image\/(?:png|jpeg|gif|webp);base64,[A-Za-z0-9+/]+={0,2}/g,
    "[embedded image omitted from text analysis]",
  );
}

function carriesRecords(manifest: ManifestV1): boolean {
  // relayhistory-transform.ts carriesRecords: a marker, or a backfill epoch.
  return (
    manifest.extensions?.[RELAYHISTORY_EXPORT_EXTENSION] !== undefined ||
    manifest.raw.epochs.some((epoch) => epoch.reason === "backfill")
  );
}

/**
 * relayhistory evidence.ts normalizedEvent, over a projected convergence_events row
 * (snake_case columns instead of the API's camelCase). Only the fields the brief uses.
 */
export function eventFromConvergenceRow(
  row: Row,
  source: string,
  sessionId: string,
): Omit<NormalizedEvent, "id"> | null {
  const ts = Date.parse(String(row.ts ?? ""));
  if (!Number.isFinite(ts)) return null;
  const rawKind = str(row.kind) ?? "event";
  const rawRole = str(row.actor_role);
  const eventType = str(row.type);
  const role =
    rawKind === "prompt" || rawKind === "user"
      ? "user"
      : rawKind === "tool_result" || rawKind === "tool_output"
        ? "tool_result"
        : rawRole === "user" || rawRole === "assistant"
          ? rawRole
          : eventType === "user" || eventType === "assistant"
            ? eventType
            : ["response", "assistant"].includes(rawKind)
              ? "assistant"
              : (rawRole ?? "unknown");
  const kind =
    ["tool_result", "tool_output"].includes(rawKind) && role === "tool_result"
      ? "tool_result"
      : ["user", "assistant"].includes(role) &&
          [
            "prompt",
            "user",
            "response",
            "assistant",
            "message",
            "text",
            "turn",
            "history",
            "session_event",
          ].includes(rawKind)
        ? "text"
        : rawKind;
  const subagent = str(row.subagent_id);
  const content = str(row.content);
  return {
    source,
    sessionId: subagent ? `${sessionId}:subagent:${subagent}` : sessionId,
    eventUid: String(row.event_id ?? ""),
    machineId: str(row.machine_id) ?? undefined,
    sourceKind: rawKind,
    tsMs: ts,
    role,
    kind,
    text: content === null ? null : textForAnalysis(content),
    model: str(row.model),
    tokenUsage: null,
    messageId: null,
    parentId: null,
  };
}

function textParts(content: unknown): string | null {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const parts = content
    .filter(
      (part): part is Row =>
        isObject(part) &&
        ["text", "input_text", "output_text"].includes(String(part.type)) &&
        typeof part.text === "string",
    )
    .map((part) => part.text as string);
  return parts.length ? parts.join("\n") : null;
}

/** One harness transcript record (Claude- or Codex-shaped) as a text event, or null. */
export function eventFromTranscriptRecord(
  record: Row,
  source: string,
  sessionId: string,
  machineId: string,
  index: number,
): Omit<NormalizedEvent, "id"> | null {
  if (record.isSidechain === true || record.isMeta === true) return null;
  const payload = isObject(record.payload) ? record.payload : null;
  const message = isObject(record.message) ? record.message : null;
  let role: unknown;
  let content: unknown;
  if (message) {
    role = message.role ?? record.type;
    content = message.content;
  } else if (payload && payload.type === "message") {
    role = payload.role;
    content = payload.content;
  } else {
    role = record.role;
    content = record.content;
  }
  if (role !== "user" && role !== "assistant") return null;
  const text = textParts(content);
  if (!text?.trim()) return null;
  const ts = Date.parse(String(record.timestamp ?? record.ts ?? ""));
  return {
    source,
    sessionId,
    eventUid: str(record.uuid) ?? `${machineId}:${String(index).padStart(10, "0")}`,
    machineId,
    sourceKind: "transcript",
    tsMs: Number.isFinite(ts) ? ts : 0,
    role,
    kind: "text",
    text: textForAnalysis(text),
    model: message && typeof message.model === "string" ? message.model : null,
    tokenUsage: null,
    messageId: null,
    parentId: null,
  };
}

function location(manifest: ManifestV1, manifestKey: string) {
  const s = manifest.session;
  const prefix = parseSessionKey(manifestKey)?.prefix;
  return {
    ...(prefix ? { prefix } : {}),
    workspaceId: s.workspace_id,
    machineId: s.machine_id,
    harness: s.harness,
    sessionId: s.session_id,
    startDate: s.started_at.slice(0, 10),
  };
}

async function transcriptEvents(
  manifest: ManifestV1,
  manifestKey: string,
  store: ObjectStore,
  budget: { bytes: number },
): Promise<Omit<NormalizedEvent, "id">[]> {
  const loc = location(manifest, manifestKey);
  const events: Omit<NormalizedEvent, "id">[] = [];
  let index = 0;
  for (const chunk of manifest.raw.chunks) {
    budget.bytes += Math.max(chunk.bytes, chunk.stored_raw_bytes ?? 0);
    if (budget.bytes > MAX_BRIEF_SESSION_BYTES)
      throw new PipelineError("session_too_large", "the session exceeds the brief input bound", false);
    const key = rawChunkKey(loc, chunk.seq, chunk.start_offset, chunk.sha256_8, manifest.raw.format);
    let stored: Uint8Array;
    try {
      stored = await readAll(await store.get(key));
    } catch (err) {
      if ((err as { code?: string }).code === "not_found")
        throw new PipelineError("chunk_missing", "a listed chunk is not in object storage", true);
      throw err;
    }
    let text: string;
    try {
      text = zstdDecompressSync(Buffer.from(stored), {
        maxOutputLength: MAX_BRIEF_SESSION_BYTES,
      }).toString("utf8");
    } catch {
      throw new PipelineError("chunk_decompress_failed", "a raw chunk is not valid zstd", false);
    }
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      index += 1;
      let record: unknown;
      try {
        record = JSON.parse(line);
      } catch {
        continue;
      }
      if (!isObject(record)) continue;
      const event = eventFromTranscriptRecord(
        record,
        manifest.session.harness,
        manifest.session.session_id,
        manifest.session.machine_id,
        index,
      );
      if (event) events.push(event);
    }
  }
  return events;
}

async function recordEvents(
  manifest: ManifestV1,
  manifestKey: string,
  store: ObjectStore,
  budget: { bytes: number },
): Promise<Omit<NormalizedEvent, "id">[]> {
  const loc = location(manifest, manifestKey);
  const remaining = MAX_BRIEF_SESSION_BYTES - budget.bytes;
  const records = await readRelayhistoryExport(manifest, store, undefined, {
    maxSessionBytes: remaining,
    ...(loc.prefix ? { prefix: loc.prefix } : {}),
  });
  budget.bytes += manifest.raw.chunks.reduce(
    (n, c) => n + Math.max(c.bytes, c.stored_raw_bytes ?? 0),
    0,
  );
  const projection = projectRelayhistorySession(records, {
    workspaceId: loc.workspaceId,
    source: loc.harness,
    sessionId: loc.sessionId,
    machineId: loc.machineId,
    userId: relayhistoryExportProvenance(manifest).sourceUserId,
  });
  const events: Omit<NormalizedEvent, "id">[] = [];
  for (const { table, row } of projection.rows) {
    if (table !== "convergence_events") continue;
    const event = eventFromConvergenceRow(row, loc.harness, loc.sessionId);
    if (event) events.push(event);
  }
  return events;
}

/**
 * Read every pointer's manifest and raw, and build relayhistory's summary input. A
 * manifest that is not scrubbed quarantines the whole session (nothing is read from it).
 */
export async function buildBriefInput(
  deps: { reader: ManifestReader; store: ObjectStore },
  key: { harness: string; sessionId: string },
  pointers: readonly BriefPointer[],
): Promise<BriefInput> {
  const sourceRevisions: Record<string, number> = {};
  for (const p of pointers) sourceRevisions[p.machineId] = p.revision;
  const manifests: { manifest: ManifestV1; manifestKey: string }[] = [];
  for (const p of pointers) {
    const read = await deps.reader.read(p.manifestKey);
    // The pointer is live but its manifest is not there yet (or was just replaced): retry.
    if (!read)
      throw new PipelineError("manifest_missing", "a live pointer's manifest is missing", true);
    if (!read.manifest.raw.scrub)
      return { kind: "quarantined", error: "session_quarantined", sourceRevisions };
    manifests.push({ manifest: read.manifest, manifestKey: p.manifestKey });
  }
  if (!SESSION_SUMMARY_SOURCES.includes(key.harness))
    return { kind: "ineligible", error: "session_source_unsupported", sourceRevisions };
  const budget = { bytes: 0 };
  const collected: Omit<NormalizedEvent, "id">[] = [];
  for (const { manifest, manifestKey } of manifests) {
    if (manifest.raw.format === "sqlite-rows")
      return { kind: "ineligible", error: "session_format_unsupported", sourceRevisions };
    collected.push(
      ...(carriesRecords(manifest)
        ? await recordEvents(manifest, manifestKey, deps.store, budget)
        : await transcriptEvents(manifest, manifestKey, deps.store, budget)),
    );
  }
  // relayhistory loadSessionSource order: event time, then event uid.
  collected.sort((a, b) => a.tsMs - b.tsMs || a.eventUid.localeCompare(b.eventUid));
  const events: NormalizedEvent[] = collected.map((event, i) => ({ ...event, id: i + 1 }));
  const messages = events.filter(
    (e) =>
      e.sessionId === key.sessionId &&
      (e.role === "user" || e.role === "assistant") &&
      e.kind === "text" &&
      e.text?.trim(),
  );
  if (!messages.length) return { kind: "empty", error: "session_empty", sourceRevisions };
  if (messages.length > SESSION_SUMMARY_MAX_MESSAGES)
    return { kind: "ineligible", error: "session_analysis_ineligible", sourceRevisions };
  try {
    return {
      kind: "ready",
      input: summaryInput(key.harness, key.sessionId, events),
      sourceRevisions,
    };
  } catch {
    // relayhistory request-processor: "Every analyzed session must also be briefable."
    return { kind: "ineligible", error: "session_analysis_ineligible", sourceRevisions };
  }
}
