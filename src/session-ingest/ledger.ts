import type {CurrentCustodyPacket,CurrentCustodyAck} from "./custody-protocol.ts";
import type {OrderedControl,OrderedControlResult} from "./control-protocol.ts";
import type { BufferedStageSpec, BufferedStageIntent } from "./buffered-protocol.ts";
/**
 * The session ingest's Supabase ledger: everything `accept_delivery_batch`
 * decided in Neon that is not transcript content.
 *
 *   sessions.ingest_batches        (org, ws, origin, batch_id) -> digest, receipt_id, state
 *                                  idempotency and receipts: a retry replays, a reused id 409s
 *   sessions.ingest_record_revisions (org, ws, origin, record_id) -> revision, revision_id, digest
 *                                  the record-revision 409 (0026's checks), no content
 *   sessions.upload_deletions      (org, ws, user, origin, source, session_id) deletion fences
 *   sessions.upload_resets         (org, ws, user, request_id) a user's "delete all" resets
 *   sessions.ingest_session_ownership pending/committed session keys visible to reset
 *   sessions.upload_erasure_rearm  sessions a tombstone reached after an ingest claim: the
 *                                  erasure job sweeps them again
 *
 * Two calls per batch, each one round trip and one short transaction, with
 * short per-(org,workspace,user) row locking for fence decisions (no advisory
 * locks). S3 writes stay outside those transactions:
 *
 *   claim  (before any S3 write) fences, replay/409, stale records, pending claim
 *   commit (after the S3 writes) pending -> committed, written revisions, fence recheck
 *
 * `ingest_record_revisions` holds committed revisions only, so a resumed
 * batch never mistakes its own unfinished write for a stored one. With
 * relayhistory-cloud #250 there is no cross-batch revision conflict (an equal
 * revision is a replay), so nothing needs reserving before the write.
 *
 * The receipt itself is never stored: it is content-addressed (receiptId is a
 * digest of tenant, origin, batch id and batch digest) and its revision ids are
 * the request's own, so a replay with the same digest recomputes it exactly.
 * Nothing client-supplied but ids, revisions and digests reaches this ledger.
 */

export interface LedgerTenant {
  orgId: string;
  workspaceId: string;
  userId: string;
}

/**
 * One record's identity, as 0026's revision checks see it. `digest` is the
 * digest of the submitted record. Records of a deleted session are dropped
 * before the checks (Neon 0038 filters before its revision checks), so each
 * carries its session.
 */
export interface LedgerRecord {
  source: string;
  /** null for a record with no session (never deleted). */
  sessionId: string | null;
  recordId: string;
  revisionId: string;
  revision: number;
  digest: string;
}

export interface ClaimInput {
  custody?:CurrentCustodyPacket;
  tenant: LedgerTenant;
  originId: string;
  batchId: string;
  batchDigest: string;
  receiptId: string;
  /** Distinct (source, session_id) pairs of the batch's session records. */
  sessions: [source: string, sessionId: string][];
  /** Every prepared record (retired kinds already dropped), in batch order. */
  records: LedgerRecord[];
}

export interface RevisionConflict {
  recordId: string;
  submittedRevisionId: string;
  submittedRevision: number;
  submittedDigest: string;
  currentRevisionId: string;
  currentRevision: number;
  currentDigest: string;
}

/**
 * `deleted`: the batch's sessions the user deleted from this origin (their
 * records are acknowledged and not stored, as Neon 0038's deletion filter).
 * `stale`: indexes into `ClaimInput.records` that need no write: the stored
 * revision is at or above the submitted one. relayhistory-cloud #250 (0071):
 * an equal revision is a replay whatever its digest (the stored copy is
 * kept), a lower revision is ignored. This is the replay fast path for old
 * Desktops that regenerate a job and re-send whole histories at old
 * revisions. `resets`: the user's reset count, which the optional Neon
 * replica's guard compares at apply time (relayhistory's `fence.resets`).
 */
type BaseClaimResult =
  /** First sight of this batch: claimed `pending`. */
  | { kind: "claimed"; claimToken: string; deleted: [string, string][]; stale: number[]; resets: number }
  /** A pending claim with this digest (an earlier attempt stopped part way): re-run, every step is idempotent. */
  | { kind: "resume"; claimToken: string; deleted: [string, string][]; stale: number[]; resets: number }
  /**
   * Every record is stale or deleted: the claim committed the batch itself in
   * the same round trip. Nothing is written to S3 and commit is not called.
   */
  | { kind: "replay_fast"; deleted: [string, string][]; stale: number[]; resets: number }
  /** Committed with this digest: answer with the recomputed receipt, write nothing. */
  | { kind: "replay"; receiptId: string }
  /** This batch id holds other content (committed or pending): 409 batch_id. */
  | { kind: "batch_conflict"; currentDigest: string }
  /** 409 record_revision (#250: in-batch reuse of a new revision only): first MAX_DELIVERY_CONFLICTS, and the total. */
  | { kind: "revision_conflict"; conflicts: RevisionConflict[]; count: number };

export type ClaimResult = BaseClaimResult & {custodyAck?:CurrentCustodyAck};

/** A server-issued immutable claim snapshot, carried through object IO. */
export interface ClaimAttempt {
  batchId: string;
  batchDigest: string;
  claimToken: string;
}
export type StagedAttempt = Pick<ClaimAttempt, "batchDigest" | "claimToken">;

export interface CommitInput {
  tenant: LedgerTenant;
  originId: string;
  batchId: string;
  batchDigest: string;
  claimToken: string;
  /** Sessions this batch wrote to S3 (after the deletion filter). */
  sessions: [source: string, sessionId: string][];
  /** Exactly the records written to S3: their revisions become stored (max wins, equal keeps). */
  records: LedgerRecord[];
  /** The m=srv_ machine the sessions live under (for the erasure re-arm). */
  machineId: string;
}

export interface CommitResult {
  /** Sessions a tombstone (or a reset) reached between claim and commit: queued for another erasure sweep. */
  rearmed: number;
}

/**
 * Per-tenant concurrency (replaces Neon's 429 `delivery_busy`, which came from
 * waiting on a per-user advisory lock). A lease per in-flight batch, counted
 * per user and per org, expiring by TTL so a crashed invocation frees itself.
 * A soft limit: two racing acquires can both pass by one.
 */
export interface SlotRequest {
  tenant: LedgerTenant;
  /** Unique per request (the invocation's request id). */
  holder: string;
  maxPerUser: number;
  maxPerOrg: number;
  ttlSeconds: number;
}

export type SlotResult = { ok: true } | { ok: false; retryAfterSeconds: number };

/**
 * Deletion fences and their effect on the revision index (Neon 0038/0039): a
 * disable tombstones the sessions and forgets this user's revisions of them, so an
 * explicit re-enable is written again; an enable lifts the tombstone; a reset
 * tombstones and forgets every session of the user (idempotent by request id).
 */
export interface SelectionInput {
  tenant: LedgerTenant;
  originId: string;
  sessions: [source: string, sessionId: string][];
  enabled: boolean;
}

export type OrderedControlInput =
 | (SelectionInput & {control:OrderedControl & {kind:"selection"};custody?:CurrentCustodyPacket})
 | {tenant:LedgerTenant;requestId:string;control:OrderedControl & {kind:"reset"};custody?:CurrentCustodyPacket};
/** P1 frozen-source custody: the witness relayhistory would build against Neon, built in Supabase. */
export interface FrozenWitnessCandidate { preparedOrd: number; recordId: string; revision: string; revisionId: string; candidateDigest: string; source: string; sessionId: string | null }
export interface FrozenWitnessInput { kind: "batch" | "selection" | "reset"; tenant: LedgerTenant; originId: string | null; batchId: string | null; batchDigest: string | null; originalRecordCount: number; candidates: FrozenWitnessCandidate[] }
export interface SourceControlBeginInput { epoch: string; tenant: LedgerTenant; kind: "selection" | "reset"; payload: unknown }
export interface SourceControlHead { epoch: string; version: string; digest: string; kind: "selection" | "reset"; pending: true; apply: boolean }
export interface SourceControlCompleteInput { epoch: string; tenant: LedgerTenant; version: string; digest: string; kind: "selection" | "reset"; outcome: "applied" | "replay" | "stale" }
export interface Ledger {
  /** Frozen-source custody witness (refuses unless the sealed copy and policy agree). */
  frozenWitness?(input: FrozenWitnessInput): Promise<CurrentCustodyPacket>;
  /** Frozen Neon legacy receipt for a retried pre-cutover batch (refuses while a control is pending). */
  frozenReceipt?(input: { tenant: LedgerTenant; originId: string; batchId: string }): Promise<{ digest: string; receipt: Record<string, unknown> } | null>;
  /** Supabase-first mirror of Neon's control head; relayhistory calls it BEFORE Neon begins. */
  sourceControlBegin?(input: SourceControlBeginInput): Promise<SourceControlHead>;
  /** Mirror completion; relayhistory calls it AFTER Neon completes. */
  sourceControlComplete?(input: SourceControlCompleteInput): Promise<{ completed: true; superseded: boolean }>;
  /** Private verified-identity path. Unsupported stores must fail, never fall back. */
  applyOrderedControl?(input:OrderedControlInput):Promise<OrderedControlResult>;
  claim(input: ClaimInput): Promise<ClaimResult>;
  prepareBufferStage?(input: Pick<CommitInput,"tenant"|"originId"|"batchId"|"batchDigest"|"claimToken"> & BufferedStageSpec): Promise<BufferedStageIntent>;
  commitBuffered?(input: Pick<CommitInput,"tenant"|"originId"|"batchId"|"batchDigest"|"claimToken"> & {stageToken:string}): Promise<CurrentCustodyAck|void>;
  commit(input: CommitInput): Promise<CommitResult>;
  acquireSlot(request: SlotRequest): Promise<SlotResult>;
  releaseSlot(tenant: LedgerTenant, holder: string): Promise<void>;
  setSessionUploads(input: SelectionInput): Promise<void>;
  /** The number of sessions the reset tombstoned (a replayed request id answers its first count). */
  resetUploadedSessions(tenant: LedgerTenant, requestId: string): Promise<number>;
  /** Staged path: the slice's sessions fenced right now (skipped by the fan-out). */
  fanoutFence(tenant: LedgerTenant, originId: string, sessions: [string, string][], attempt: StagedAttempt): Promise<[string, string][]>;
  /** Staged path: the records one slice wrote become stored (a session fenced since is re-armed instead). */
  commitRevisions(tenant: LedgerTenant, originId: string, machineId: string, records: LedgerRecord[], attempt: StagedAttempt): Promise<{ committed: number; rearmed: number }>;
}

/** The ledger is unreachable or refused: the batch answers 503 (retryable), never a fallback. */
export class LedgerUnavailableError extends Error {
  constructor(readonly code: string) {
    super(`session ingest ledger unavailable: ${code}`);
    this.name = "LedgerUnavailableError";
  }
}
