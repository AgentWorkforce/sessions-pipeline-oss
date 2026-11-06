/**
 * Relay session journal turns (relayhistory `POST /v1/sessions/:id/turns`) as
 * delivery records, so they land in the session's S3 stream through the normal
 * buffered ingest instead of Neon `sessions.conversation_turns`.
 *
 * relayhistory-cloud validates the public request exactly as before
 * (routes/turns.ts `parseTurnRequest`), resolves each turn's source and scrubs it
 * the same way, then builds ONE delivery batch per <= 500 turns of this shape and
 * sends it over the signed DELIVERY_INGEST binding to `POST /v1/turns/batches`.
 * The route here accepts only this shape: it is a private boundary, never the
 * public delivery protocol, so `conversation_turn` is NOT a public DeliveryKind
 * and a Desktop batch can never carry one.
 *
 *   origin_id   TURN_ORIGIN_ID (one synthetic origin; never seen by Neon delivery)
 *   kind        "conversation_turn"
 *   source      the turn's resolved source (Neon's `conversation_turns.source`)
 *   session_id  the path session id
 *   record_id   turnRecordId(workspace, source, session, turn_index): one record per
 *               Neon row key, so a re-POST of an index replaces it (higher revision)
 *   revision    relayhistory's microsecond clock at acceptance (later write wins)
 *   payload     the Neon row: id, org_id, workspace_id, source, session_id, turn_index,
 *               session_owner, role, content, actor_name, actor_role, metadata_json,
 *               ts, tagged. No `ts_ms`: a 13-digit Luhn-valid number would be nulled by the
 *               delivery card scrub, so the time travels only as ISO text.
 *
 * Tenancy: payload org/workspace must equal the signed identity's (workspace
 * `default` when the token carries none, as Neon keys turns), and record_id must
 * be the one derived from the payload, so a caller can neither aim a turn at
 * another tenant nor make one row's record replace another's.
 */
import { createHash } from "node:crypto";
import { DeliveryError, DELIVERY_MAPPING_VERSION, MAX_DELIVERY_RECORDS, type HistoryExportBatch } from "./vendor/delivery/delivery-contracts.ts";

export const TURN_KIND = "conversation_turn";
export const TURN_ORIGIN_ID = "relayhistory:conversation_turns:v1";
export const TURN_PAYLOAD_KEYS = [
  "id", "org_id", "workspace_id", "source", "session_id", "turn_index", "session_owner",
  "role", "content", "actor_name", "actor_role", "metadata_json", "ts", "tagged",
] as const;
const ROLES = new Set(["user", "assistant", "system"]);
const ACTOR_ROLES = new Set(["owner", "steerer"]);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const ISO_MS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

/** Neon's turn workspace for a token: the token's workspace, else `default` (relayhistory lib/turns.ts). */
export const turnWorkspace = (identityWorkspace: string) => identityWorkspace || "default";

/** One record per Neon row key (org is the tenant's). Pinned test vector in both repos. */
export function turnRecordId(workspaceId: string, source: string, sessionId: string, turnIndex: number): string {
  return "turn:" + createHash("sha256").update(JSON.stringify([workspaceId, source, sessionId, turnIndex])).digest("hex");
}

function invalid(message: string): never {
  throw new DeliveryError("invalid_delivery", 400, message);
}
const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
function exact(v: Record<string, unknown>, keys: readonly string[]) {
  const got = Object.keys(v).sort(), want = [...keys].sort();
  if (got.length !== want.length || got.some((k, i) => k !== want[i])) invalid("Unexpected or missing field");
}
function identifier(v: unknown): v is string {
  return typeof v === "string" && v.length > 0 && Buffer.byteLength(v) <= 512 && !/[\x00-\x1f\x7f]/.test(v) && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(v);
}
const positive = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v > 0;

/**
 * The private turns request: `{protocolVersion: 1, batch}` with the delivery batch
 * envelope (account, mapping version and digests stay the delivery protocol's) and
 * only `conversation_turn` records of one session. Throws the delivery 400/422.
 */
export function parseTurnBatch(value: unknown, identity: { orgId: string; workspaceId: string }): HistoryExportBatch {
  if (!object(value)) invalid("Turn request must be an object");
  exact(value, ["protocolVersion", "batch"]);
  if (value.protocolVersion !== 1) throw new DeliveryError("unsupported_protocol", 422, "Delivery protocol version is unsupported");
  const batch = value.batch;
  if (!object(batch)) invalid("Turn batch must be an object");
  exact(batch, ["schema_version", "origin_id", "batch_id", "job_id", "generation", "destination_id", "instance_id", "account_id", "mapping_version", "records"]);
  if (batch.schema_version !== 1) throw new DeliveryError("unsupported_schema", 422, "Delivery schema version is unsupported");
  for (const key of ["batch_id", "job_id", "destination_id", "instance_id", "account_id"]) if (!identifier(batch[key])) invalid("Invalid turn batch identifier");
  if (batch.origin_id !== TURN_ORIGIN_ID) invalid("Turn batch origin is fixed");
  if (batch.mapping_version !== DELIVERY_MAPPING_VERSION) throw new DeliveryError("unsupported_mapping", 422, "Delivery mapping version is unsupported");
  if (!positive(batch.generation)) invalid("Delivery generation must be a positive safe integer");
  const records = batch.records;
  if (!Array.isArray(records) || records.length < 1 || records.length > MAX_DELIVERY_RECORDS) invalid(`Turn batch must contain 1..${MAX_DELIVERY_RECORDS} records`);
  const workspace = turnWorkspace(identity.workspaceId);
  const ids = new Set<string>(), revisions = new Set<string>();
  let session: string | undefined;
  for (const r of records as unknown[]) {
    if (!object(r)) invalid("Turn record must be an object");
    exact(r, ["schema_version", "origin_id", "record_id", "revision_id", "revision", "kind", "source", "session_id", "operation", "payload"]);
    if (r.schema_version !== 1) throw new DeliveryError("unsupported_schema", 422, "Delivery schema version is unsupported");
    if (r.kind !== TURN_KIND || r.origin_id !== TURN_ORIGIN_ID || r.operation !== "upsert") invalid("Only conversation turn upserts are accepted");
    if (!positive(r.revision) || !identifier(r.revision_id) || !identifier(r.source) || !identifier(r.session_id)) invalid("Invalid turn record identifier");
    if ((session ??= r.session_id) !== r.session_id) invalid("A turn batch holds one session");
    const p = r.payload;
    if (!object(p)) invalid("Turn payload must be an object");
    exact(p, TURN_PAYLOAD_KEYS);
    if (p.org_id !== identity.orgId || p.workspace_id !== workspace || p.source !== r.source || p.session_id !== r.session_id)
      throw new DeliveryError("delivery_account_mismatch", 403, "Delivery account does not match authenticated tenant");
    if (typeof p.id !== "string" || !UUID.test(p.id)) invalid("Turn id must be a lowercase UUID");
    if (typeof p.turn_index !== "number" || !Number.isSafeInteger(p.turn_index) || p.turn_index < 0 || p.turn_index > 2147483647) invalid("Turn index must be a non-negative integer");
    if (!ROLES.has(p.role as string) || !ACTOR_ROLES.has(p.actor_role as string)) invalid("Invalid turn role");
    for (const k of ["session_owner", "actor_name"]) if (typeof p[k] !== "string" || !(p[k] as string).length) invalid(`Turn ${k} is required`);
    if (typeof p.content !== "string" || typeof p.tagged !== "boolean") invalid("Invalid turn content");
    if (typeof p.ts !== "string" || !ISO_MS.test(p.ts) || !Number.isFinite(Date.parse(p.ts)) || new Date(p.ts).toISOString() !== p.ts) invalid("Turn ts must be canonical ISO-8601 milliseconds");
    let metadata: unknown;
    try {
      metadata = JSON.parse(p.metadata_json as string);
    } catch {
      invalid("Turn metadata_json must be JSON");
    }
    if (typeof p.metadata_json !== "string" || !object(metadata)) invalid("Turn metadata_json must be a JSON object");
    if (r.record_id !== turnRecordId(workspace, r.source as string, r.session_id as string, p.turn_index as number)) invalid("Turn record id does not match its key");
    if (ids.has(r.record_id as string)) invalid("A turn batch holds each turn key once");
    if (revisions.has(r.revision_id as string)) invalid("Duplicate revision identity in batch");
    ids.add(r.record_id as string);
    revisions.add(r.revision_id as string);
  }
  return batch as unknown as HistoryExportBatch;
}
