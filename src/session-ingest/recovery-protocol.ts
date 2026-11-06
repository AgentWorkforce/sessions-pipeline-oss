/**
 * PRIVATE archived-replica recovery wire.
 *
 * A Neon replica archive contains the exact prepared records and original
 * receipt, but not enough of the public upload envelope to recompute them.
 * Recovery therefore carries those immutable prepared values and a fresh
 * current-source custody witness. The request is additionally authenticated by
 * the normal ingest HMAC over its exact bytes; SQL remains the final custody
 * authority.
 */
import {
  COLD_PRIVACY_BYTES,
  CUSTODY_RECORD_BYTES,
  CURRENT_CUSTODY_ISSUER,
  type CurrentCustodyPacket,
  type SourceCustodyWitness,
} from "./custody-protocol.ts";
import type { LedgerTenant } from "./ledger.ts";
import {
  DELIVERY_KINDS,
  DeliveryError,
  MAX_DELIVERY_RECORD_BYTES,
  MAX_DELIVERY_RECORDS,
} from "./vendor/delivery/delivery-contracts.ts";
import {
  canonicalJson,
  digest,
  servicePayload,
  type PreparedDelivery,
  type PreparedDeliveryRecord,
} from "./vendor/delivery/prepare.ts";

export const RECOVERY_PROTOCOL = "relay-ingest-recovery-v1";
export const RECOVERY_CONTENT_TYPE = "application/vnd.relay-ingest-recovery-v1+json";

export interface RecoveryEnvelope {
  protocol: typeof RECOVERY_PROTOCOL;
  prepared: PreparedDelivery;
  custody: CurrentCustodyPacket;
}

const HEX64 = /^[0-9a-f]{64}$/;
const DECIMAL = /^(0|[1-9][0-9]{0,18})$/;
const encoder = new TextEncoder();
const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const invalid = (message = "Invalid recovery delivery"): never => {
  throw new DeliveryError("invalid_delivery", 400, message);
};
function exact(value: Record<string, unknown>, fields: string[]): void {
  const expected = [...fields].sort();
  const actual = Object.keys(value).sort();
  if (actual.length !== expected.length || actual.some((field, index) => field !== expected[index])) invalid();
}
function identifier(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && encoder.encode(value).length <= 512 && !/[\x00-\x1f\x7f]/.test(value) && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(value);
}
function positive(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}
function boundedDecimal(value: unknown): value is string {
  return typeof value === "string" && DECIMAL.test(value) && BigInt(value) <= 9223372036854775807n;
}
function parsedJson(bytes: Uint8Array): unknown {
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes));
  } catch {
    invalid("Recovery request must be valid UTF-8 JSON");
  }
}

async function preparedRecord(value: unknown, originId: string): Promise<PreparedDeliveryRecord> {
  if (!object(value)) invalid();
  const v = value as Record<string, unknown>;
  exact(v, ["schema_version", "origin_id", "record_id", "revision_id", "revision", "kind", "source", "session_id", "operation", "payload", "digest"]);
  if (
    v.schema_version !== 1 || v.origin_id !== originId || !identifier(v.record_id) || !identifier(v.revision_id) ||
    !positive(v.revision) || !DELIVERY_KINDS.includes(v.kind as never) || !identifier(v.source) ||
    !(v.session_id === null || identifier(v.session_id)) || !HEX64.test(String(v.digest)) ||
    !((v.operation === "delete" && v.payload === null) || (v.operation === "upsert" && object(v.payload)))
  ) invalid();
  const record = v as unknown as PreparedDeliveryRecord;
  // Archived records must already be the stable, scrubbed service form. This
  // rejects a recovery sender attempting to smuggle raw/private payload fields.
  let normalized: Record<string, unknown> | null = null;
  try {
    normalized = servicePayload(record);
  } catch {
    invalid();
  }
  if (canonicalJson(normalized) !== canonicalJson(record.payload)) invalid();
  if (encoder.encode(JSON.stringify(record)).length > MAX_DELIVERY_RECORD_BYTES) invalid("Recovery record exceeds the readback byte limit");
  return record;
}

async function preparedDelivery(value: unknown, tenant: LedgerTenant): Promise<PreparedDelivery> {
  if (!object(value)) invalid();
  const v = value as Record<string, unknown>;
  exact(v, ["originId", "batchId", "batchDigest", "receipt", "records"]);
  if (!identifier(v.originId) || !identifier(v.batchId) || !HEX64.test(String(v.batchDigest)) || !Array.isArray(v.records) || v.records.length > MAX_DELIVERY_RECORDS) invalid();
  const rawRecords = v.records as unknown[];
  const records = await Promise.all(rawRecords.map((record) => preparedRecord(record, v.originId as string)));
  const revisions = new Set<string>();
  for (const record of records) {
    if (revisions.has(record.revision_id)) invalid("Duplicate revision identity in recovery batch");
    revisions.add(record.revision_id);
  }
  if (!object(v.receipt)) invalid();
  const receipt = v.receipt as Record<string, unknown>;
  exact(receipt, ["protocolVersion", "receiptId", "batchId", "acceptedRevisionIds", "unsupportedRevisionIds", "acceptanceLevel"]);
  if (
    receipt.protocolVersion !== 1 || receipt.batchId !== v.batchId || receipt.acceptanceLevel !== "durable" ||
    !Array.isArray(receipt.acceptedRevisionIds) || receipt.acceptedRevisionIds.length < Math.max(1, records.length) || receipt.acceptedRevisionIds.length > MAX_DELIVERY_RECORDS ||
    !receipt.acceptedRevisionIds.every(identifier) || new Set(receipt.acceptedRevisionIds).size !== receipt.acceptedRevisionIds.length ||
    !Array.isArray(receipt.unsupportedRevisionIds) || receipt.unsupportedRevisionIds.length !== 0 ||
    typeof receipt.receiptId !== "string" || !/^rhr_[0-9a-f]{64}$/.test(receipt.receiptId)
  ) invalid();
  const accepted = new Set(receipt.acceptedRevisionIds as string[]);
  if (records.some((record) => !accepted.has(record.revision_id))) invalid();
  const expectedReceipt = "rhr_" + await digest([
    "relayhistory-receipt-v1", tenant.orgId, tenant.workspaceId, v.originId, v.batchId, v.batchDigest,
  ]);
  if (receipt.receiptId !== expectedReceipt) invalid("Recovery receipt does not match the authenticated tenant");
  return v as unknown as PreparedDelivery;
}

function custodyPacket(value: unknown, tenant: LedgerTenant, prepared: PreparedDelivery): CurrentCustodyPacket {
  if (!object(value)) invalid();
  const v = value as Record<string, unknown>;
  exact(v, ["witness", "sourceDigest"]);
  if (!HEX64.test(String(v.sourceDigest)) || !object(v.witness)) invalid();
  const witness = v.witness as Record<string, unknown>;
  exact(witness, [
    "version", "issuer", "sourceIdentitySha256", "controlEpoch", "privacyBaselineSha256", "tenant", "originId", "batchId", "batchDigest",
    "sourceControlVersion", "pending", "pendingControl", "privacyVersion", "originalRecordCount", "candidateCount", "records", "privacy",
  ]);
  if (!object(witness.tenant)) invalid();
  const witnessTenant = witness.tenant as Record<string, unknown>;
  exact(witnessTenant, ["orgId", "workspaceId", "userId"]);
  const sameTenant = witnessTenant.orgId === tenant.orgId && witnessTenant.workspaceId === tenant.workspaceId && witnessTenant.userId === tenant.userId;
  if (
    witness.version !== 1 || witness.issuer !== CURRENT_CUSTODY_ISSUER || !HEX64.test(String(witness.sourceIdentitySha256)) ||
    !HEX64.test(String(witness.controlEpoch)) || !HEX64.test(String(witness.privacyBaselineSha256)) || !sameTenant ||
    witness.originId !== prepared.originId || witness.batchId !== prepared.batchId || witness.batchDigest !== prepared.batchDigest ||
    !boundedDecimal(witness.sourceControlVersion) || typeof witness.pending !== "boolean" ||
    !(witness.pendingControl === null || object(witness.pendingControl)) || !boundedDecimal(witness.privacyVersion) ||
    !Number.isSafeInteger(witness.originalRecordCount) || !Number.isSafeInteger(witness.candidateCount) ||
    witness.originalRecordCount !== prepared.receipt.acceptedRevisionIds.length || witness.candidateCount !== prepared.records.length ||
    !Array.isArray(witness.records) || witness.records.length !== prepared.records.length ||
    !(witness.privacy === null || object(witness.privacy))
  ) invalid();
  const custodyRecords = witness.records as unknown[];
  if (Buffer.byteLength(JSON.stringify(custodyRecords), "utf8") > CUSTODY_RECORD_BYTES || witness.privacy !== null && Buffer.byteLength(JSON.stringify(witness.privacy), "utf8") > COLD_PRIVACY_BYTES) invalid();
  const preparedOrdinals = new Set<number>();
  const processingRanks = new Set<number>();
  for (const entry of custodyRecords) {
    if (!object(entry)) invalid();
    const custodyRecord = entry as Record<string, unknown>;
    exact(custodyRecord, ["preparedOrd", "processingRank", "candidateDigest", "recordId", "revision", "revisionId", "candidateFenced", "currentFenced", "current"]);
    if (!Number.isSafeInteger(custodyRecord.preparedOrd) || !Number.isSafeInteger(custodyRecord.processingRank)) invalid();
    const ordinal = custodyRecord.preparedOrd as number;
    const rank = custodyRecord.processingRank as number;
    if (ordinal < 0 || ordinal >= prepared.records.length || rank < 0 || rank >= prepared.records.length || preparedOrdinals.has(ordinal) || processingRanks.has(rank)) invalid();
    preparedOrdinals.add(ordinal);
    processingRanks.add(rank);
    const record = prepared.records[ordinal]!;
    if (
      custodyRecord.candidateDigest !== record.digest || custodyRecord.recordId !== record.record_id || custodyRecord.revision !== String(record.revision) ||
      custodyRecord.revisionId !== record.revision_id || typeof custodyRecord.candidateFenced !== "boolean" || typeof custodyRecord.currentFenced !== "boolean" ||
      !(custodyRecord.current === null || object(custodyRecord.current)) || custodyRecord.candidateFenced === true && record.session_id === null
    ) invalid();
  }
  return v as unknown as CurrentCustodyPacket;
}

/** Decode and bind a recovery envelope to its authenticated tenant. */
export async function decodeRecoveryEnvelope(bytes: Uint8Array, tenant: LedgerTenant): Promise<{ prepared: PreparedDelivery; custody: CurrentCustodyPacket }> {
  const value = parsedJson(bytes);
  if (!object(value)) invalid();
  const v = value as Record<string, unknown>;
  exact(v, ["protocol", "prepared", "custody"]);
  if (v.protocol !== RECOVERY_PROTOCOL) invalid();
  const prepared = await preparedDelivery(v.prepared, tenant);
  const custody = custodyPacket(v.custody, tenant, prepared);
  return { prepared, custody };
}
