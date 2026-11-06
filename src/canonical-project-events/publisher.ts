import { assertProjectEventClaim, assertProjectEventScope, cloudProjectEventMessage, type ProjectEventClaim, type ProjectEventScope, type CloudProjectEventMessage } from "./protocol.ts";

/** Claim/send is not consumer admission. Only Cloud's separate generation-fenced
 * acknowledgement can finish a durable event after admission at its destination.
 */
export interface CanonicalProjectEventStore {
  claim(input: { schemaGeneration: string; scope: ProjectEventScope; limit: number }): Promise<ProjectEventClaim[]>;
  sent(claim: ProjectEventClaim): Promise<boolean>;
  release(claim: ProjectEventClaim): Promise<boolean>;
}
export interface CloudProjectEventQueue { send(body: CloudProjectEventMessage): Promise<void> }

export async function publishCanonicalProjectEvents(input: {
  store: CanonicalProjectEventStore; queue: CloudProjectEventQueue;
  schemaGeneration: string; scope: ProjectEventScope; limit?: number;
}) {
  const limit = input.limit ?? 20;
  assertProjectEventScope(input.scope);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 50 || !/^[0-9a-f]{64}$/.test(input.schemaGeneration))
    throw new Error("canonical_project_publisher_config_invalid");
  // Snapshot before asynchronous authority calls; caller mutation cannot expand scope.
  const scope = { ...input.scope }, schemaGeneration = input.schemaGeneration;
  const claims = await input.store.claim({ schemaGeneration, scope, limit });
  if (!Array.isArray(claims) || claims.length > limit) throw new Error("canonical_project_claim_limit_invalid");
  // Validate the whole bounded batch before sending anything. Invalid authority
  // is not a legacy fallback and never becomes a message carrying tenant data.
  for (const claim of claims) assertProjectEventClaim(claim, schemaGeneration, scope);
  const result = { claimed: claims.length, sent: 0, superseded: 0 };
  for (let i = 0; i < claims.length; i++) {
    const claim = claims[i]!;
    try {
      await input.queue.send(cloudProjectEventMessage(claim.event));
      if (await input.store.sent(claim)) result.sent++;
      else result.superseded++;
    } catch {
      // A send whose reply was lost may already be delivered. Release is still
      // safe: duplicate delivery must converge at Cloud's durable receipt.
      // Keep source events pending; failed releases recover by bounded leases.
      await Promise.allSettled(claims.slice(i).map(c => input.store.release(c)));
      throw Object.assign(new Error("Canonical project event delivery needs retry"), { code: "canonical_project_send_failed" });
    }
  }
  return result;
}
