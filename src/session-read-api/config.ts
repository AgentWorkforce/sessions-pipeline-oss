/**
 * Runtime-neutral configuration parsing shared by the Lambda (lambda.ts) and
 * the Worker (packages/session-read-worker): no AWS or Node imports here.
 */

/** "epoch" → 0 (complete history); otherwise an ISO timestamp. Missing or junk refuses to start. */
export function parseCompleteSince(raw: string | undefined): number {
  if (raw === "epoch") return 0;
  const t = raw ? Date.parse(raw) : Number.NaN;
  if (Number.isNaN(t)) throw new Error('session read api: READ_API_COMPLETE_SINCE must be an ISO timestamp or "epoch"');
  return t;
}
