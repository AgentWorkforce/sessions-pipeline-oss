/**
 * Relay session turns in a canonical scope: the legacy Neon export rows
 * (`legacy_conversation_turn`, canonical-legacy.ts) merged with the live
 * `conversation_turn` records relayhistory forwards since TURNS_WRITE_PATH=dual
 * (sessions-pipeline src/session-ingest/turns.ts). Both become rows of table
 * `conversation_turns` in the Neon column shape.
 *
 * Merge rule, per Neon row key (workspace_id, stored source, turn_index; the
 * session is the scope's): a live record wins over a legacy row, and among live
 * records the highest revision wins (the stream fold already keeps only that).
 * Legacy rows carry no write order, so this is exact only while the writer never
 * returns from dual/forward to Neon-only after the export (docs/relay-session-turns.md).
 */
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import type { CanonicalIdentity } from "./canonical.ts";
import { TURN_KIND, TURN_ORIGIN_ID, TURN_PAYLOAD_KEYS, turnRecordId } from "../session-ingest/turns.ts";

type Row = { table: string; row: Record<string, unknown> };

function fail(): never {
  throw new Error("canonical live turn invalid");
}

/** Live turn records of this scope as Neon-shaped rows; any record outside the scope or shape fails the read. */
export function canonicalLiveTurns(records: RelayhistoryStreamRecord[], identity: CanonicalIdentity): Row[] {
  const rows: Row[] = [];
  for (const record of records) {
    if (record.kind !== TURN_KIND) continue;
    const p = record.payload;
    if (!p || record.origin_id !== TURN_ORIGIN_ID || record.operation !== "upsert") fail();
    const keys = Object.keys(p).sort();
    if (keys.join(",") !== [...TURN_PAYLOAD_KEYS].sort().join(",")) fail();
    if (p.org_id !== identity.orgId || p.workspace_id !== identity.workspaceId || p.source !== identity.source || p.session_id !== identity.sessionId) fail();
    if (typeof p.turn_index !== "number" || !Number.isSafeInteger(p.turn_index) || p.turn_index < 0) fail();
    if (record.record_id !== turnRecordId(identity.workspaceId, identity.source, identity.sessionId, p.turn_index)) fail();
    let metadata: unknown;
    try {
      metadata = JSON.parse(String(p.metadata_json));
    } catch {
      fail();
    }
    if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) fail();
    const { metadata_json: _m, ...rest } = p;
    rows.push({ table: "conversation_turns", row: { ...rest, metadata, live_revision: String(record.revision) } });
  }
  return rows;
}

const turnKey = (row: Record<string, unknown>) =>
  JSON.stringify([row.workspace_id, row.legacy_stored_source ?? row.source, Number(row.turn_index)]);

/** Legacy and live turn rows merged per Neon row key; every other row passes through in order. */
export function mergeTurnRows(legacy: Row[], live: Row[]): Row[] {
  const liveKeys = new Set(live.map((r) => turnKey(r.row)));
  return [...legacy.filter((r) => r.table !== "conversation_turns" || !liveKeys.has(turnKey(r.row))), ...live];
}
