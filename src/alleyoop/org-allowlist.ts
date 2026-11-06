/**
 * Tenant org allowlist for the ingest (TENANT_ORG_IDS, comma-separated).
 *
 * With org-derived key prefixes (PIPELINE_ORG_FROM_PREFIX) the ingest accepts
 * any org a valid RelayAuth token names and files its objects under that org's
 * prefix. A deployment that serves a fixed set of tenants lists them here: a
 * valid token for any other org is refused with 403 `org_not_allowed` before
 * any /v1 route runs. Unset or empty is a no-op (no allowlist), so existing
 * single-tenant and prefix deployments are unchanged.
 */
import type { MiddlewareHandler } from "hono";
import type { AppEnv } from "../shared/vendor.ts";

export function allowedOrgIdsFromEnv(v: string | undefined): string[] {
  return (v ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

/** Runs after authentication: it reads the verified `auth.orgId`. */
export function orgAllowlist(
  allowed: readonly string[],
): MiddlewareHandler<AppEnv> {
  const set = new Set(allowed);
  return async (c, next) => {
    const org = (c.get("auth") as { orgId?: string } | undefined)?.orgId;
    if (!org || !set.has(org))
      return c.json(
        {
          error: {
            code: "org_not_allowed",
            message: "this deployment does not accept that org",
          },
          correlationId: c.get("correlationId") ?? "",
        },
        403,
      );
    await next();
  };
}
