import type {CurrentCustodyPacket,CurrentCustodyAck} from "./custody-protocol.ts";
import {validatedCustodyAck} from "./custody-ack.ts";
/**
 * `POST /v1/delivery/batches` without Neon: every accepted batch is in S3
 * before the response, and there is no fallback path.
 *
 *   prepare (CPU: validate, normalize, scrub payloads, digests, receipt; vendored relayhistory code)
 *   -> claim  (Supabase, one round trip: fences, replay / 409, stale records, pending)
 *   -> write  (S3: scrubbed chunks + manifest CAS + pointer, m=srv_ layout; a batch wider
 *             than one invocation is staged as one object and fanned out by the queue)
 *   -> replica (optional, transition only: one Cloudflare Queue send; Neon applies it later)
 *   -> commit (Supabase, one round trip: committed, written revisions, fence recheck)
 *   -> 200 receipt
 *
 * Status semantics are relayhistory's: 400/403/413 from prepare, 409
 * `delivery_conflict` (batch_id or record_revision, same bodies), 503
 * `delivery_unavailable` for anything transient (ledger or S3 unreachable,
 * replica send failed while required). A 503 is safe to retry: the claim is
 * `pending` until commit, and a retry with the same digest resumes the same
 * idempotent write (create-only chunks and pointers, manifest CAS with the
 * replay extension).
 *
 * Replay fast path (old Desktops re-send whole histories at old revisions):
 * the claim reports which records are already stored at or above their
 * revision (`stale`, #250 rules), and they are not written again. A batch of
 * nothing but stale or deleted records is committed by the claim itself: one
 * Supabase round trip, no S3, no slot, no lock.
 *
 * 429 `delivery_busy` comes only from the per-tenant slot limit, with
 * relayhistory's jittered `Retry-After` (2-10 s), never from waiting on a
 * lock. A slot is taken only for a batch that has something to write, after
 * the claim; a refused batch stays `pending` and its retry resumes it.
 */
import type { Ledger, LedgerTenant, RevisionConflict, SlotRequest } from "./ledger.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import {
  DELIVERY_LIMITS,
  DeliveryBusyError,
  DeliveryError,
  MAX_DELIVERY_CONFLICTS,
  type DeliveryReceipt,
  type DeliveryRecordRevisionConflict,
  type HistoryExportBatch,
} from "./vendor/delivery/delivery-contracts.ts";
import { prepareDelivery, type PreparedDelivery } from "./vendor/delivery/prepare.ts";
import { batchMarkerKey, replicaMessage, sendReplica, stagedReplicaMessage, type ReplicaOutcome, type ReplicaSink } from "./replica.ts";
import { IngestWriteError, assertTenantSegment, groupRecords, liveMachineId, writeBatchObjects, type WriteResult, type WriteTarget } from "./writer.ts";
import type { IngestAcceptanceMode } from "./buffered-protocol.ts";
import { prepareBufferedStage, putBufferedStage, fanoutMessages, stageGroups, type FanoutMessage } from "./staging.ts";

export interface DeliverDeps {
  /** Verified private source packet, bound to this request only. */
  custody?:CurrentCustodyPacket;
  ledger: Ledger;
  acceptanceMode?: IngestAcceptanceMode;
  target: WriteTarget;
  replica?: ReplicaSink;
  now?: () => Date;
  /** Per-tenant in-flight batch limits; unset: no limit. */
  slots?: Omit<SlotRequest, "tenant" | "holder">;
  /** Unique per request (the slot lease holder). */
  requestId?: () => string;
  /**
   * Sessions written inline per request (the Worker subrequest budget). A
   * batch with more is staged (staging.ts) and fanned out by the queue stage.
   * Unset: always inline.
   */
  inlineMaxSessions?: number;
  /** Enqueue fan-out messages (INGEST_FANOUT). Required with `inlineMaxSessions`. */
  fanout?: (messages: FanoutMessage[]) => Promise<void>;
}

export type DeliverOutcome = "accepted" | "accepted_buffered" | "accepted_staged" | "replay" | "replay_fast" | "accepted_all_deleted";

export interface DeliverResult {
  custodyAck?:CurrentCustodyAck;
  receipt: DeliveryReceipt;
  outcome: DeliverOutcome;
  /** Ids, counts and codes only: what the request log line may carry. */
  stats: {
    records: number;
    sessions: number;
    deleted_sessions: number;
    /** Records already stored at or above their revision (not written again). */
    stale: number;
    manifests_written: number;
    replica: ReplicaOutcome;
    rearmed: number;
    timing_ms: Record<string, number>;
  };
}

/** Code only: storage and ledger messages can quote keys. */
export class IngestUnavailable extends DeliveryError {
  constructor(readonly stage: string, readonly cause_code: string) {
    super("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
  }
}

function causeCode(error: unknown): string {
  if (error instanceof LedgerUnavailableError || error instanceof IngestWriteError) return error.code;
  const code = (error as { code?: unknown })?.code;
  if (typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code)) return code;
  const name = (error as { name?: unknown })?.name;
  return typeof name === "string" && /^[A-Za-z]{1,64}$/.test(name) ? name : "Error";
}

const pair = (source: string, sessionId: string | null) => JSON.stringify([source, sessionId ?? ""]);

function withLimits(prepared: PreparedDelivery): DeliveryReceipt {
  return { ...prepared.receipt, limits: { ...DELIVERY_LIMITS } };
}

function revisionConflict(originId: string, conflicts: RevisionConflict[], count: number): DeliveryError {
  const items: DeliveryRecordRevisionConflict[] = conflicts.slice(0, MAX_DELIVERY_CONFLICTS).map((c) => ({ type: "record_revision", originId, ...c }));
  return new DeliveryError("delivery_conflict", 409, "Delivery record revision was reused with different content", items[0], items, count);
}

function batchIdConflict(prepared: PreparedDelivery, currentDigest: string): DeliveryError {
  return new DeliveryError("delivery_conflict", 409, "Delivery batch identity was reused with different content", {
    type: "batch_id",
    originId: prepared.originId,
    batchId: prepared.batchId,
    submittedDigest: prepared.batchDigest,
    currentDigest,
  });
}

async function stage<T>(name: string, work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (error) {
    if (error instanceof DeliveryError) throw error;
    throw new IngestUnavailable(name, causeCode(error));
  }
}

export async function deliverBatch(deps: DeliverDeps, tenant: LedgerTenant, batch: HistoryExportBatch): Promise<DeliverResult> {
  const started = Date.now();
  // 403 delivery_account_mismatch, 413 delivery_too_large, exactly as relayhistory.
  const prepared = await prepareDelivery({ orgId: tenant.orgId, workspaceId: tenant.workspaceId }, batch);
  return deliverPreparedCore(deps, tenant, prepared, started, "prepare");
}

/**
 * Lands an already-prepared relayhistory delivery through the same claim, fence,
 * stale-record, buffered-write and receipt path as a live public batch.
 *
 * This is intentionally a narrow internal boundary. Callers must authenticate
 * and validate the prepared object (including its original receipt and current
 * source custody) before invoking it; the public route continues through
 * `prepareDelivery` above.
 */
export async function deliverPrepared(deps: DeliverDeps, tenant: LedgerTenant, prepared: PreparedDelivery): Promise<DeliverResult> {
  return deliverPreparedCore(deps, tenant, prepared, Date.now(), "precheck");
}

async function deliverPreparedCore(deps: DeliverDeps, tenant: LedgerTenant, prepared: PreparedDelivery, started: number, firstTiming: "prepare" | "precheck"): Promise<DeliverResult> {
  const now = deps.now ?? (() => new Date());
  const timing: Record<string, number> = {};
  let t = started;
  const mark = (name: string) => {
    timing[name] = Date.now() - t;
    t = Date.now();
  };
  await stage("precheck", async () => assertTenantSegment(deps.target.keyRoot, tenant.orgId));
  mark(firstTiming);

  const sessions = [...new Map(prepared.records.filter((r) => r.session_id).map((r) => [pair(r.source, r.session_id), [r.source, r.session_id!] as [string, string]])).values()];
  const claim = await stage("claim", () =>
    deps.ledger.claim({
      tenant,
      ...(deps.custody?{custody:deps.custody}:{}),
      originId: prepared.originId,
      batchId: prepared.batchId,
      batchDigest: prepared.batchDigest,
      receiptId: prepared.receipt.receiptId,
      sessions,
      records: prepared.records.map((r) => ({ source: r.source, sessionId: r.session_id, recordId: r.record_id, revisionId: r.revision_id, revision: r.revision, digest: r.digest })),
    }),
  );
  mark("claim");
  const base = { records: prepared.records.length, sessions: sessions.length, deleted_sessions: 0, stale: 0, manifests_written: 0, replica: "off" as ReplicaOutcome, rearmed: 0 };
  if (claim.kind === "replay") {
    // Content-addressed: the same digest recomputes the same receipt. A mismatch is corruption, never a 200.
    if (claim.receiptId !== prepared.receipt.receiptId) throw new IngestUnavailable("replay", "receipt_mismatch");
    return { custodyAck:claim.custodyAck,receipt: withLimits(prepared), outcome: "replay", stats: { ...base, timing_ms: timing } };
  }
  if (claim.kind === "batch_conflict") throw batchIdConflict(prepared, claim.currentDigest);
  if (claim.kind === "revision_conflict") throw revisionConflict(prepared.originId, claim.conflicts, claim.count);
  if (claim.kind === "replay_fast")
    return { custodyAck:claim.custodyAck,receipt: withLimits(prepared), outcome: "replay_fast", stats: { ...base, deleted_sessions: claim.deleted.length, stale: claim.stale.length, timing_ms: timing } };

  // The deletion filter (records of sessions the user deleted from this origin are
  // acknowledged, not stored) and the replay fast path (records already stored at or
  // above their revision are acknowledged, not written again).
  const deleted = new Set(claim.deleted.map(([s, id]) => pair(s, id)));
  const stale = new Set(claim.stale);
  const keep: number[] = [];
  prepared.records.forEach((r, i) => {
    if (!stale.has(i) && !(r.session_id && deleted.has(pair(r.source, r.session_id)))) keep.push(i);
  });
  const work = () => writeAndCommit(deps, tenant, prepared, sessions, deleted, keep, claim.resets, claim.claimToken, { ...base, stale: stale.size }, timing, mark, now);
  if (!deps.slots) return work();
  const holder = (deps.requestId ?? (() => crypto.randomUUID()))();
  const slot = await stage("slot", () => deps.ledger.acquireSlot({ tenant, holder, ...deps.slots! }));
  // The claim stays pending: the retry resumes it.
  if (!slot.ok) throw new DeliveryBusyError("tenant_slots", slot.retryAfterSeconds);
  try {
    return await work();
  } finally {
    // A lost release expires by TTL.
    await deps.ledger.releaseSlot(tenant, holder).catch(() => {});
  }
}

async function writeAndCommit(
  deps: DeliverDeps,
  tenant: LedgerTenant,
  prepared: PreparedDelivery,
  sessions: [string, string][],
  deleted: Set<string>,
  keep: number[],
  resets: number,
  claimToken: string,
  base: Omit<DeliverResult["stats"], "timing_ms">,
  timing: Record<string, number>,
  mark: (name: string) => void,
  now: () => Date,
): Promise<DeliverResult> {
  const machineId = liveMachineId(tenant.workspaceId, prepared.originId, tenant.userId);
  let written: WriteResult | null = null;
  let staged = false;
  let replicaOutcome: ReplicaOutcome = "off";
  const receivedAt = now();
  const grouped = keep.length > 0 ? groupRecords(keep.map((i) => prepared.records[i]!), receivedAt) : null;
  if (deps.acceptanceMode === "buffered") {
    if (!grouped || !deps.ledger.prepareBufferStage || !deps.ledger.commitBuffered || deps.replica?.mode && deps.replica.mode !== "off")
      throw new IngestUnavailable("buffer", "buffer_not_configured");
    const attempt = {tenant, originId:prepared.originId, batchId:prepared.batchId, batchDigest:prepared.batchDigest, claimToken};
    const preparedStage = await stage("buffer_prepare", async () => prepareBufferedStage(deps.target.keyRoot,
      {claim_token:claimToken,batch_digest:prepared.batchDigest,received_at:receivedAt.toISOString(),org_id:tenant.orgId,workspace_id:tenant.workspaceId,origin_id:prepared.originId,user_id:tenant.userId},
      grouped.groups, grouped.index.map(indexes => indexes.map(i => keep[i]!))));
    const intent = await stage("buffer_intent", () => deps.ledger.prepareBufferStage!({...attempt,...preparedStage.spec}));
    mark("buffer_intent");
    await stage("buffer_put", () => putBufferedStage(deps.target.store,preparedStage));
    mark("buffer_put");
    const ack=await stage("buffer_receipt", () => deps.ledger.commitBuffered!({...attempt,stageToken:intent.stageToken}));
    let custodyAck:CurrentCustodyAck|undefined;
    if(deps.custody){try{custodyAck=validatedCustodyAck(ack,deps.custody)}catch{throw new IngestUnavailable("buffer_receipt","custody_ack_missing")}}
    mark("buffer_receipt");
    return {custodyAck,receipt:withLimits(prepared),outcome:"accepted_buffered",stats:{...base,deleted_sessions:deleted.size,timing_ms:timing}};
  }
  if (grouped && deps.inlineMaxSessions !== undefined && grouped.groups.length > deps.inlineMaxSessions) {
    // Too wide for this invocation: one staged object now, the sessions from the queue.
    if (!deps.fanout) throw new IngestUnavailable("staging", "fanout_not_configured");
    const key = await stage("staging", () =>
      stageGroups(
        deps.target,
        { claim_token: claimToken, batch_digest: prepared.batchDigest, received_at: receivedAt.toISOString(), org_id: tenant.orgId, workspace_id: tenant.workspaceId, origin_id: prepared.originId, user_id: tenant.userId },
        grouped.groups,
      ),
    );
    await stage("fanout", () => deps.fanout!(fanoutMessages(key, grouped.groups.length)));
    staged = true;
    mark("staging");
    if (deps.replica && deps.replica.mode !== "off") {
      const message = stagedReplicaMessage(tenant, prepared, { key, groups: grouped.groups.length, index: grouped.index }, resets, now(), keep);
      replicaOutcome = await stage("replica", () => sendReplica(deps.replica, message, prepared, batchMarkerKey(deps.target.keyRoot, tenant, prepared.originId, prepared.batchId)));
      mark("replica");
    }
  } else if (keep.length > 0) {
    written = await stage("objects", () =>
      writeBatchObjects(deps.target, {
        orgId: tenant.orgId,
        workspaceId: tenant.workspaceId,
        userId: tenant.userId,
        originId: prepared.originId,
        batchDigest: prepared.batchDigest,
        records: keep.map((i) => prepared.records[i]!),
      }),
    );
    mark("objects");
  }

  let replica: ReplicaOutcome = replicaOutcome;
  if (written && deps.replica && deps.replica.mode !== "off") {
    const message = replicaMessage(tenant, prepared, written, resets, now(), keep);
    replica = await stage("replica", () => sendReplica(deps.replica, message, prepared, batchMarkerKey(deps.target.keyRoot, tenant, prepared.originId, prepared.batchId)));
    mark("replica");
  }

  const writtenSessions = sessions.filter(([s, id]) => !deleted.has(pair(s, id)));
  const committed = await stage("commit", () =>
    deps.ledger.commit({
      tenant,
      originId: prepared.originId,
      batchId: prepared.batchId,
      batchDigest: prepared.batchDigest,
      claimToken,
      sessions: writtenSessions,
      machineId,
      // A staged batch's revisions become stored only when the fan-out has written them
      // (staging.ts fanout -> commitRevisions); committing them here would make a
      // dead-lettered slice's records stale forever.
      records: staged ? [] : keep.map((i) => prepared.records[i]!).map((r) => ({ source: r.source, sessionId: r.session_id, recordId: r.record_id, revisionId: r.revision_id, revision: r.revision, digest: r.digest })),
    }),
  );
  mark("commit");
  return {
    receipt: withLimits(prepared),
    outcome: staged ? "accepted_staged" : written ? "accepted" : "accepted_all_deleted",
    stats: {
      ...base,
      deleted_sessions: deleted.size,
      manifests_written: written?.manifests.filter((m) => m.written).length ?? 0,
      replica,
      rearmed: committed.rearmed,
      timing_ms: timing,
    },
  };
}
