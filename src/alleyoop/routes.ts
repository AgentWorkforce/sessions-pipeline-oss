/**
 * The ingest's route surface, split from ingest.ts so its authentication can be
 * tested without HoopDb. Only PUBLIC_INGEST_PATHS answer without credentials:
 * health checks and the public JWKS. Every `/v1` route requires a RelayAuth JWT
 * (requireAuth), and the operator routes under `/v1/internal/pipeline/*` require
 * PIPELINE_OPS_SECRET (and answer 503 where it is not configured).
 */
import type { Hono } from "hono";
import { orgAllowlist } from "./org-allowlist.ts";
import { pipelineOpsRoutes, requireAuth, type AppEnv } from "../shared/vendor.ts";
import { requestIdentity } from "../shared/enrich.ts";
import { liveWriteMachineGate } from "./manifests.ts";

/** Paths served without authentication. The public ALB contract depends on this list staying this small. */
export const PUBLIC_INGEST_PATHS = ["/.well-known/jwks.json", "/health", "/healthz", "/readyz"] as const;

/** The tenant org allowlist is #94's module (src/alleyoop/org-allowlist.ts); re-exported for callers. */
export { allowedOrgIdsFromEnv, orgAllowlist } from "./org-allowlist.ts";

export function mountIngestApi(
  app: Hono<AppEnv>,
  routes: { courierManifests: Hono<AppEnv>; pipeline: Hono<AppEnv> },
  v1: Hono<AppEnv>,
  opts: { relayhistoryOrigin?: string | undefined; allowedOrgIds?: readonly string[] | undefined } = {},
) {
  v1.use("*", async (c, next) => requireAuth(c as never, next));
  if (opts.allowedOrgIds?.length) v1.use("*", orgAllowlist(opts.allowedOrgIds));
  // Upload routes on an origin-bound deployment: only assertions minted for
  // this origin (or migration assertions, which are origin-bound by their
  // verifier), whatever the interim bearer flag says.
  v1.use("*", async (c, next) => {
    const path = c.req.path;
    const upload = path === "/v1/pipeline/chunks" || path === "/v1/pipeline/manifests" || path === "/v1/pipeline/manifests/status";
    const auth = c.get("auth") as { historyOrigin?: string; migration?: unknown } | undefined;
    if (upload && opts.relayhistoryOrigin && !auth?.migration && !auth?.historyOrigin)
      return c.json({ error: { code: "origin_bound_assertion_required", message: "upload routes take only assertions minted for this origin" }, correlationId: c.get("correlationId") ?? "" }, 403);
    await next();
  });
  // A live write must be bound to its machine before any route stores anything.
  v1.use("*", liveWriteMachineGate);
  // Every send made while handling this request carries the authenticated user.
  v1.use("*", async (c, next) => {
    const auth = c.get("auth") as { userId?: string } | undefined;
    return requestIdentity.run({ userId: auth?.userId }, () => next());
  });
  // AlleyOop owns authenticated manifest writes and their compare-and-swap
  // fence. Mount before the provenance-tracked pipeline routes it shadows.
  v1.route("/", routes.courierManifests);
  v1.route("/", routes.pipeline);
  app.route("/v1", pipelineOpsRoutes);
  app.route("/v1", v1);
}
