/** Protocol 1 matches the local history export contract, not legacy ingest. */
export const DELIVERY_KINDS = [
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "session",
  "presence",
  "relationship",
  "commit_link",
  "source_observation",
  "observation_evidence",
  "session_marker",
  /** Stable joins for source session -> issue -> Garden run -> coding session -> PR. */
  "session_lineage",
  /** Durable lifecycle evidence for an existing-session input turn. */
  "turn_receipt",
] as const;
export type DeliveryKind = (typeof DELIVERY_KINDS)[number];
/**
 * Evidence the service no longer keeps: trajectories and Relaycast messages
 * (`relay` source) are no longer history. Probes released before that change
 * still send them, and a rejected batch never advances the client's cursor, so
 * these records are acknowledged and dropped instead of failing their batch.
 * Remove once no probe sends them.
 */
export const RETIRED_DELIVERY_KINDS = ["trajectory"] as const;
export type RetiredDeliveryKind = (typeof RETIRED_DELIVERY_KINDS)[number];
export const RETIRED_DELIVERY_SOURCES = ["relay", "trajectory"] as const;
export function retiredDeliveryRecord(record: {
  kind?: unknown;
  source?: unknown;
}): boolean {
  return (
    (RETIRED_DELIVERY_KINDS as readonly unknown[]).includes(record.kind) ||
    (RETIRED_DELIVERY_SOURCES as readonly unknown[]).includes(record.source)
  );
}
export interface HistoryExportRecord {
  schema_version: 1;
  origin_id: string;
  record_id: string;
  revision_id: string;
  revision: number;
  /** A `DeliveryKind`, or a retired kind that is acknowledged and dropped. */
  kind: DeliveryKind | RetiredDeliveryKind;
  source: string;
  session_id: string | null;
  operation: "upsert" | "delete";
  payload: Record<string, unknown> | null;
}
export interface HistoryExportBatch {
  schema_version: 1;
  origin_id: string;
  batch_id: string;
  job_id: string;
  generation: number;
  destination_id: string;
  instance_id: string;
  account_id: string;
  mapping_version: string;
  records: HistoryExportRecord[];
}
/** The receipt persisted with a batch and replayed exactly on retry. */
export interface StoredDeliveryReceipt {
  protocolVersion: 1;
  /** Content-addressed server receipt. Stable across replay of the same batch. */
  receiptId: string;
  batchId: string;
  acceptedRevisionIds: string[];
  unsupportedRevisionIds: [];
  acceptanceLevel: "durable";
}
/**
 * The server's current batch limits. Advisory and response-time only: they are
 * not part of the content-addressed receipt and are never persisted with it, so
 * a replayed receipt reports the limits in force when it is replayed.
 */
export interface DeliveryLimits {
  maxRecords: number;
  /** Decoded request bytes, whatever the request's `Content-Encoding`. */
  maxRequestBytes: number;
  /**
   * Request-body codings the server decodes, most preferred first. Added by
   * the route at response time, since it depends on the runtime.
   */
  contentEncodings?: ("zstd" | "gzip")[];
}
export interface DeliveryReceipt extends StoredDeliveryReceipt {
  limits: DeliveryLimits;
}
export interface DeliveryRecordRevisionConflict {
  type: "record_revision";
  originId: string;
  recordId: string;
  submittedRevisionId: string;
  submittedRevision: number;
  submittedDigest: string;
  currentRevisionId: string;
  currentRevision: number;
  currentDigest: string;
}
export interface DeliveryBatchIdConflict {
  type: "batch_id";
  originId: string;
  batchId: string;
  submittedDigest: string;
  currentDigest: string;
}
export type DeliveryConflict =
  | DeliveryRecordRevisionConflict
  | DeliveryBatchIdConflict;
export const MAX_DELIVERY_CONFLICTS = 100;
export const DELIVERY_MAPPING_VERSION = "relayhistory-delivery-v1";
export const MAX_DELIVERY_BYTES = 2_097_152;
export const MAX_DELIVERY_RECORDS = 500;
export const DELIVERY_LIMITS: Readonly<DeliveryLimits> = Object.freeze({
  maxRecords: MAX_DELIVERY_RECORDS,
  maxRequestBytes: MAX_DELIVERY_BYTES,
});
/** Encoded response cap, below the optional helper's 8 MiB reader. */
export const MAX_DELIVERY_PAGE_BYTES = 3_145_728;
export const MAX_DELIVERY_RECORD_BYTES = MAX_DELIVERY_PAGE_BYTES - 16_384;
export class DeliveryError extends Error {
  constructor(
    public readonly code: string,
    public readonly status: 400 | 403 | 409 | 413 | 415 | 422 | 429 | 503,
    message: string,
    public readonly conflict?: DeliveryConflict,
    public readonly conflicts?: DeliveryRecordRevisionConflict[],
    public readonly conflictCount?: number,
  ) {
    super(message);
  }
}
/** Retry-After bounds for a busy batch, in seconds. */
export const DELIVERY_BUSY_RETRY_AFTER = Object.freeze({ min: 2, max: 10 });
/**
 * A batch refused because a delivery lock stayed held past its bound (0059).
 * Nothing was written. The client retries it (429 is retryable, like 503);
 * `Retry-After` is jittered so refused machines do not return together.
 */
export class DeliveryBusyError extends DeliveryError {
  constructor(
    public readonly lock: string,
    public readonly retryAfterSeconds: number,
  ) {
    super(
      "delivery_busy",
      429,
      "Delivery is busy for this workspace; retry later",
    );
  }
}
