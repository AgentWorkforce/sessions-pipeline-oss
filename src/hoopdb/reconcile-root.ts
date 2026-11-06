/**
 * Where the indexer's reconcile tick lists manifests: `<PIPELINE_KEY_PREFIX>/v1`
 * (or `v1`) with a static org; with org-derived prefixes the whole
 * `<PIPELINE_KEY_PREFIX>` root, so every `<base>/<org>/v1` tree is listed
 * (`prefixAllowed` then keeps only exact `<base>/<org>` keys of the org the
 * key names). Undefined for org-derived prefixes without a base: a store root
 * is not listed. Kept out of indexer.ts so it can be tested without starting
 * the worker.
 */
import { reconcileListPrefix } from "../shared/vendor.ts";

export function indexerReconcileRoot(env: { PIPELINE_KEY_PREFIX?: string; PIPELINE_ORG_FROM_PREFIX?: string }): string | undefined {
  return reconcileListPrefix(env);
}
