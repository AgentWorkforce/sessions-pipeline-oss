/**
 * One SQS batch of S3 object events: pointer first (Supabase), then the
 * search index (ClickHouse) once per session scope the batch touched.
 *
 * At-least-once and idempotent: every step re-reads S3 and is fenced or
 * diffed, so a redelivered or reordered message converges to the same state.
 * Within a batch, events for one key collapse to the latest (S3 sequencer)
 * and events for one session collapse to one search-index pass. A failure
 * fails exactly the messages that fed the failed key or scope (SQS partial
 * batch response); SQS redelivers them after the visibility timeout and moves
 * them to the DLQ after `maxReceiveCount`.
 */
import { errorFields, log } from "../shared/log.ts";
import { parseQueueBody, sequencerAfter, type LandedEvent } from "./s3-events.ts";
import { indexPointer, type PointerDeps, type SessionScope } from "./pointer.ts";
import { indexSearchScope, type SearchIndexDeps } from "./search-index.ts";
import { captureRefusal, classifyReplayFailure, produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { LargeJobState, LargeRoute } from "./large-read-worker.ts";

export interface QueueRecord {
  messageId: string;
  body: string;
}

export interface BatchResponse {
  batchItemFailures: { itemIdentifier: string }[];
}

export interface ConsumerDeps {
  pointer: PointerDeps;
  /** Absent while the search index is switched off: only pointers are written. */
  search?: SearchIndexDeps;
  /** Explicitly absent by default; independent Node artifact publication stage. */
  replay?: ReplayProducerDeps;
  /**
   * Absent by default (SESSION_LARGE_READ_HANDOFF_ENABLED). `delegated: true` means durable
   * canonical-epoch handoff; `jobState` reads the job behind a delegated route, once per routed scope.
   */
  large?: { route: (scope: SessionScope) => Promise<LargeRoute>; jobState?: (scope: SessionScope) => Promise<LargeJobState | null> };
  /**
   * Put a manifest back on the queue after a delay (SQS DelaySeconds): used when
   * another pass holds the session, so the event is neither lost nor failed.
   */
  requeue?: (manifestKey: string) => Promise<void>;
}

const scopeId = (s: SessionScope) => JSON.stringify([s.orgId, s.workspaceId, s.harness, s.sessionId]);

export async function processBatch(records: readonly QueueRecord[], deps: ConsumerDeps): Promise<BatchResponse> {
  const failed = new Set<string>();
  const byKey = new Map<string, { event: LandedEvent; messageIds: string[] }>();
  for (const record of records) {
    const parsed = parseQueueBody(record.body);
    if (parsed.kind === "test") continue;
    if (parsed.kind === "invalid") {
      // Never succeeds on retry; the DLQ keeps it for an operator.
      log.warn("object event unreadable", { stage: "object-landed", reason: "invalid_body" });
      failed.add(record.messageId);
      continue;
    }
    for (const event of parsed.events) {
      const held = byKey.get(event.key);
      if (!held) byKey.set(event.key, { event, messageIds: [record.messageId] });
      else {
        held.messageIds.push(record.messageId);
        if (sequencerAfter(event.sequencer, held.event.sequencer)) held.event = event;
      }
    }
  }

  const scopes = new Map<string, { scope: SessionScope; messageIds: string[]; manifestKey: string }>();
  for (const { event, messageIds } of byKey.values()) {
    const started = Date.now();
    try {
      const outcome = await indexPointer(event, deps.pointer);
      if (outcome.status === "ignored") {
        log.info("object event ignored", { stage: "object-landed", reason: outcome.reason });
        continue;
      }
      log.info("pointer indexed", {
        stage: "session-indexed",
        outcome: outcome.status,
        org_id: outcome.scope.orgId,
        workspace_id: outcome.scope.workspaceId,
        harness: outcome.scope.harness,
        session_id: outcome.scope.sessionId,
        revision: outcome.revision,
        duration_ms: Date.now() - started,
      });
      const id = scopeId(outcome.scope);
      const held = scopes.get(id);
      if (held) held.messageIds.push(...messageIds);
      else scopes.set(id, { scope: outcome.scope, messageIds: [...messageIds], manifestKey: outcome.manifestKey });
    } catch (error) {
      log.error("pointer index failed", { stage: "session-indexed", ...errorFields(error), duration_ms: Date.now() - started });
      for (const id of messageIds) failed.add(id);
    }
  }

  const delegated=new Set<string>();
  if(deps.large&&(deps.search||deps.replay))for(const {scope,messageIds} of scopes.values()){
    if(messageIds.every(id=>failed.has(id)))continue;
    const ids={org_id:scope.orgId,workspace_id:scope.workspaceId,harness:scope.harness,session_id:scope.sessionId};
    let routed:LargeRoute;
    try{routed=await deps.large.route(scope)}
    catch(error){
      // route_session_large_read captures first, so it raises the same refusals as the replay
      // stage (#179). A capacity/superseded refusal is not retryable here: log it and let search
      // and replay apply their own limits. A permanently ambiguous canonical source (55000 with
      // capture's exact message) is deterministic too: retrying only reaches the DLQ with search
      // skipped every time. Anything else (55000 durable handoff unavailable or source epoch
      // missing, 22023, connection errors) fails the batch and is retried.
      const refusal=classifyReplayFailure(error);
      if(refusal){log.warn("large scope handoff refused",{stage:"large-read",...ids,...errorFields(error),reason:refusal.reason,error_code:refusal.code});continue}
      if(captureRefusal(error)==="capture_ambiguous_source"){log.warn("large scope handoff refused",{stage:"large-read",...ids,...errorFields(error),reason:"large_handoff_ambiguous_source",error_code:"55000"});continue}
      log.error("large scope handoff failed",{stage:"large-read",...ids,...errorFields(error)});for(const id of messageIds)failed.add(id);continue;
    }
    if(!routed.delegated)continue;
    delegated.add(scopeId(scope));
    if(!deps.large.jobState)continue;
    // Search/replay are skipped for a delegated scope, so a job that will never run is a gap:
    // `failed` is never claimed again until the source changes. Make it visible.
    try{
      const job=await deps.large.jobState(routed.identity??scope);
      if(job&&job.state!=="failed")log.info("large scope delegated",{stage:"large-read",...ids,state:job.state,attempt:job.attempts});
      else log.warn("large scope job will not be processed",{stage:"large-read",...ids,state:job?.state??"missing",attempt:job?.attempts,reason:job?"large_job_failed":"large_job_missing"});
    }catch(error){log.warn("large scope job state unavailable",{stage:"large-read",...ids,...errorFields(error)})}
  }
  if (deps.search) {
    for (const { scope, messageIds, manifestKey } of scopes.values()) {
      if (delegated.has(scopeId(scope)) || messageIds.every((id) => failed.has(id))) continue;
      const started = Date.now();
      try {
        const result = await indexSearchScope(scope, deps.search);
        if (result.busy) {
          // The running pass may have read S3 before this event's write: run again later.
          if (!deps.requeue) throw new Error("scope busy and no requeue");
          await deps.requeue(manifestKey);
          log.info("search index busy, requeued", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, reason: "scope_busy" });
          continue;
        }
        if (result.partial)
          log.warn("search index partial", { stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId, manifests: result.manifests, reason: "scope_too_large" });
        log.info("search index reconciled", {
          stage: "search-index",
          org_id: scope.orgId,
          workspace_id: scope.workspaceId,
          harness: scope.harness,
          session_id: scope.sessionId,
          manifests: result.manifests,
          count: result.rows,
          inserted: result.inserted,
          deleted: result.deleted,
          ok: result.complete,
          reason: result.reasons.join(",") || undefined,
          duration_ms: Date.now() - started,
        });
      } catch (error) {
        log.error("search index failed", {
          stage: "search-index",
          org_id: scope.orgId,
          session_id: scope.sessionId,
          ...errorFields(error),
          duration_ms: Date.now() - started,
        });
        for (const id of messageIds) failed.add(id);
      }
    }
  }
  if (deps.replay) {
    for (const { scope, messageIds } of scopes.values()) {
      if (delegated.has(scopeId(scope)) || messageIds.every(id => failed.has(id))) continue;
      const started = Date.now();
      try {
        const result = await produceCanonicalReplay(scope, deps.replay);
        log.info("canonical replay materialized", { stage: "replay-pages", org_id: scope.orgId, session_id: scope.sessionId,
          ok: result.published, reason: result.reason, count: result.rows, objects: result.objects, duration_ms: Date.now()-started });
      } catch (error) {
        // Retrying cannot fix a superseded source or a capacity refusal: log it and
        // keep the message (its pointer and search stages already succeeded).
        const refusal = classifyReplayFailure(error);
        const fields = { stage: "replay-pages", org_id: scope.orgId, session_id: scope.sessionId, ok: false,
          ...errorFields(error), duration_ms: Date.now()-started };
        if (refusal?.kind === "superseded") {
          // The newer source write has its own object event, which re-runs this scope.
          log.info("canonical replay superseded", { ...fields, reason: refusal.reason, error_code: refusal.code });
          continue;
        }
        if (refusal?.kind === "capacity") {
          // Oversize scopes go to the large-read path before search/replay run: deps.large
          // (route_session_large_read -> session_large_read_jobs, default off) delegates them
          // above. A capacity refusal here is a scope that was not delegated (handoff off,
          // metadata under the route thresholds, or the route itself refused on capacity);
          // it is logged, not retried.
          log.warn("canonical replay refused", { ...fields, reason: refusal.reason, error_code: refusal.code });
          continue;
        }
        log.error("canonical replay publication failed", fields);
        for (const id of messageIds) failed.add(id);
      }
    }
  }
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
}
