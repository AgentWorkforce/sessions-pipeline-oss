import type { SqlExecutor } from "../shared/vendor.ts";
import { assertProjectEventClaim, assertProjectEventScope, type ProjectEventClaim, type ProjectEventScope } from "./protocol.ts";
import type { CanonicalProjectEventStore } from "./publisher.ts";

/** Function-only metadata port; no table queries, live-marker adoption or owner
 * fallback. Production source and scope are pinned by the deployment receipt.
 */
export function canonicalProjectEventStore(sql: SqlExecutor, schemaGeneration: string, requestedScope: ProjectEventScope): CanonicalProjectEventStore {
  assertProjectEventScope(requestedScope);
  if (!/^[0-9a-f]{64}$/.test(schemaGeneration)) throw new Error("canonical_project_generation_invalid");
  const scope = { ...requestedScope };
  const rpc = async (operation: "claim" | "sent" | "release", extra: Record<string, unknown>) => {
    const { rows } = await sql.query<{ result: unknown }>(
      `SELECT sessions.canonical_project_events_${operation}_v1($1::jsonb) AS result`,
      [JSON.stringify({ schemaGeneration, scope, ...extra })]);
    if (rows.length !== 1) throw new Error("canonical_project_authority_invalid");
    return rows[0]!.result;
  };
  const mutate = async (operation: "sent" | "release", claim: ProjectEventClaim) => {
    assertProjectEventClaim(claim, schemaGeneration, scope);
    const value = await rpc(operation, { eventId: claim.event.eventId, generation: claim.event.generation,
      leaseToken: claim.leaseToken, leaseVersion: claim.leaseVersion });
    if (!value || typeof value !== "object" || typeof (value as { ok?: unknown }).ok !== "boolean") throw new Error("canonical_project_authority_invalid");
    return (value as { ok: boolean }).ok;
  };
  return {
    async claim(input) {
      if (input.schemaGeneration !== schemaGeneration || input.scope.orgId !== scope.orgId || input.scope.workspaceId !== scope.workspaceId ||
          !Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 50) throw new Error("canonical_project_scope_invalid");
      const result = await rpc("claim", { limit: input.limit });
      if (!Array.isArray(result) || result.length > input.limit) throw new Error("canonical_project_authority_invalid");
      for (const item of result) assertProjectEventClaim(item, schemaGeneration, scope);
      return result;
    },
    sent: claim => mutate("sent", claim), release: claim => mutate("release", claim),
  };
}
