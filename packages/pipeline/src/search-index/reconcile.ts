import {
  positionOf,
  type SearchEventDocument,
  type SearchFingerprint,
  type SearchPosition,
} from "./contracts.js";

/**
 * Backend-independent scope reconcile, the same rule as
 * src/session-pipeline/search-index.ts applies to ClickHouse:
 *
 *   - upsert every desired row that has no stored row with the same identity
 *     (machine, source, kind, event), session, workspace, ts and fingerprint;
 *   - only when the canonical read AND the index scan are complete, delete
 *     every stored row whose identity is no longer desired or whose session,
 *     workspace or ts moved (an incomplete read never deletes).
 *
 * The caller serializes passes per scope (advisory lock, actor, ...).
 */
export interface ScopeReconcilePlan {
  readonly upserts: readonly SearchEventDocument[];
  readonly deletes: readonly (SearchPosition & { readonly workspaceId: string })[];
}

const identity = (r: { machineId: string; source: string; kind: string; eventId: string }) =>
  JSON.stringify([r.machineId, r.source, r.kind, r.eventId]);

export function planScopeReconcile(
  desired: readonly SearchEventDocument[],
  existing: readonly SearchFingerprint[],
  options: { readonly complete: boolean },
): ScopeReconcilePlan {
  const stored = new Map<string, SearchFingerprint[]>();
  for (const row of existing) stored.set(identity(row), [...(stored.get(identity(row)) ?? []), row]);
  const upserts = desired.filter((doc) => {
    const same = stored.get(identity(doc)) ?? [];
    return !same.some(
      (s) =>
        s.sessionId === doc.sessionId &&
        s.workspaceId === doc.workspaceId &&
        s.tsMs === doc.tsMs &&
        s.fingerprint === doc.fingerprint,
    );
  });
  if (!options.complete) return { upserts, deletes: [] };
  const wanted = new Map(desired.map((doc) => [identity(doc), doc]));
  const deletes: (SearchPosition & { workspaceId: string })[] = [];
  for (const [key, rows] of stored) {
    const current = wanted.get(key);
    for (const row of rows) {
      if (
        !current ||
        current.sessionId !== row.sessionId ||
        current.workspaceId !== row.workspaceId ||
        current.tsMs !== row.tsMs
      ) {
        deletes.push({ ...positionOf(row), workspaceId: row.workspaceId });
      }
    }
  }
  return { upserts, deletes };
}
