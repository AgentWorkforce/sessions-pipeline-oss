/**
 * Token verification for the session read API: RelayAuth's relayhistory
 * assertion (POST /v1/tokens/relayhistory-assertion), minted by Cloud for this
 * service's own origin. The shared verifier checks RS256 against the JWKS, the
 * issuer and an exact single audience equal to READ_API_ORIGIN; the legacy
 * shared audience and migration assertions are never accepted here.
 */
import { IdentityError, verifyAgentRelayToken } from "@rhc/auth/relayauth.ts";
import { AuthError, type Verifier } from "./app.ts";

export interface AuthConfig {
  /** This service's origin, e.g. https://xyz.lambda-url.us-east-1.on.aws. Unset: every token is refused. */
  origin: string | undefined;
  jwksUrl?: string;
  issuer?: string;
}

export function relayAuthVerifier(config: AuthConfig, verify = verifyAgentRelayToken): Verifier {
  return async (bearer) => {
    if (!config.origin) throw new AuthError(401, "not_configured", "this read API has no origin configured");
    let auth;
    try {
      auth = await verify(bearer, {
        RELAYAUTH_JWKS_URL: config.jwksUrl,
        RELAYAUTH_ISSUER: config.issuer,
        RELAYHISTORY_ORIGIN: config.origin,
      });
    } catch (error) {
      if (error instanceof IdentityError) throw new AuthError(401, "invalid_token", "token is not valid");
      throw error;
    }
    // Only an assertion bound to this origin: the verifier sets historyOrigin then.
    if (auth.historyOrigin !== config.origin) throw new AuthError(401, "invalid_token", "token is not bound to this service");
    if (auth.migration) throw new AuthError(401, "invalid_token", "migration assertions are not accepted");
    // Tenancy is exactly the asserted org, workspace and sponsor: the shared
    // extractor also accepts alternative claim names, which this service never
    // honours.
    const claims = auth.claims ?? {};
    const strip = (v: unknown) => (typeof v === "string" && v.startsWith("relay_") ? v.slice("relay_".length) : v);
    if (
      typeof claims.org !== "string" || typeof claims.wks !== "string" || typeof claims.sponsorId !== "string" ||
      strip(claims.org) !== auth.orgId || strip(claims.wks) !== auth.workspaceId || strip(claims.sponsorId) !== auth.userId
    )
      throw new AuthError(401, "invalid_token", "token tenancy is ambiguous");
    return { userId: auth.userId, orgId: auth.orgId, workspaceId: auth.workspaceId ?? "", scopes: auth.scopes ?? [] };
  };
}
