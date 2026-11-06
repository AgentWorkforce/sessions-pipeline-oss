/**
 * Stand-in for relayhistory-cloud packages/relayhistory/src/middleware/auth.ts.
 *
 * routes/pipeline.ts imports `getAuth` and `requireScope`. Those two are copied
 * from the upstream file at 8603c141bf7dfcd67687db1e5f02e48c1eb2ef61 unchanged.
 * There is NO token verification here (upstream `requireAuth` resolves rth_at_/
 * rth_st_ tokens and RelayAuth assertions against the database): the host app
 * authenticates the request and sets `auth` on the Hono context before these run.
 * `AuthContext` is re-exported from env.ts, where upstream declares it.
 */
import { createMiddleware } from "hono/factory";
import type { AppContext, AppEnv, AuthContext } from "../env.js";

export type { AuthContext };

export function getAuth(c: AppContext): AuthContext {
  const auth = c.get("auth");
  if (!auth) {
    throw new Error("auth context is not available");
  }
  return auth;
}

export function requireScope(scope: string) {
  return createMiddleware<AppEnv>(async (c, next) => {
    const auth = getAuth(c);
    if (!auth.scopes.includes(scope)) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: `missing required scope: ${scope}`,
          },
          correlationId: c.get("correlationId") ?? "",
        },
        403,
      );
    }

    await next();
  });
}

/**
 * 96BullSessions `requireAuth`: the RelayAuth JWT path only (upstream also
 * resolves `rth_at_`/`rth_st_` tokens against Neon, which this repo does not
 * have). Verifies an RS256 JWT against `RELAYAUTH_JWKS_URL` and sets `auth` on
 * the context: with `RELAYHISTORY_ORIGIN` set, only RelayAuth's assertions
 * minted for that origin (and, behind their own flag, migration assertions);
 * `ALLOW_INTERIM_RELAYAUTH_BEARER=true` (local dev only) admits any RelayAuth
 * JWT.
 */
import {
  IdentityError,
  verifyAgentRelayToken,
  verifyMigrationAssertion,
  verifyOriginBoundAssertion,
} from "../auth/relayauth.js";
import {
  migrationKindAllowsRoute,
  migrationPairAuthorized,
  migrationRouteAllowed,
} from "../lib/pipeline/migration-assertions.js";

export const requireAuth = createMiddleware<AppEnv>(async (c, next) => {
  const header = c.req.header("Authorization") ?? "";
  const token = header.startsWith("Bearer ") ? header.slice(7).trim() : "";
  const deny = (status: 401 | 403, code: string, message: string) =>
    c.json(
      { error: { code, message }, correlationId: c.get("correlationId") ?? "" },
      status,
    );
  if (!token)
    return deny(401, "missing_authorization", "missing Authorization header");
  if (token.startsWith("rth_at_") || token.startsWith("rth_st_"))
    return deny(
      401,
      "unsupported_token",
      "opaque relayhistory tokens are not supported by this deployment",
    );
  // As upstream: the broad interim bearer (dev only) verifies any RelayAuth
  // JWT; the dedicated, default-off RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS
  // path admits only migration assertions, and a migration assertion needs
  // that flag on every path (auth/relayauth.ts).
  // 96BullSessions: with RELAYHISTORY_ORIGIN set (hosted and customer
  // installs), the interim bearer off admits RelayAuth's origin-bound History
  // assertion class and nothing else (verifyOriginBoundAssertion); upstream
  // admits the same token at its session exchange.
  const interim = c.env?.ALLOW_INTERIM_RELAYAUTH_BEARER === "true";
  const originBound = !interim && !!c.env?.RELAYHISTORY_ORIGIN?.trim();
  const migrationOnly =
    !interim &&
    !originBound &&
    c.env?.RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS === "true";
  if (!interim && !originBound && !migrationOnly)
    return deny(
      401,
      "auth_not_configured",
      "bearer auth is not configured (set RELAYHISTORY_ORIGIN)",
    );
  let auth: AuthContext;
  try {
    auth = interim
      ? await verifyAgentRelayToken(token, c.env)
      : originBound
        ? await verifyOriginBoundAssertion(token, c.env)
        : await verifyMigrationAssertion(token, c.env);
  } catch (err) {
    if (err instanceof IdentityError) return deny(401, err.code, err.message);
    throw err;
  }
  if (
    auth.migration &&
    migrationRouteAllowed(c.req.method, c.req.path) &&
    !migrationKindAllowsRoute(auth.migration.upload, c.req.method, c.req.path)
  )
    return deny(
      403,
      "migration_token_class_forbidden",
      "this migration token class does not reach this route",
    );
  if (auth.migration && !migrationRouteAllowed(c.req.method, c.req.path))
    return deny(
      403,
      "migration_assertion_route",
      "migration assertions are accepted only by the pipeline upload routes",
    );
  if (auth.migration && !migrationPairAuthorized(auth))
    return deny(
      403,
      "migration_workspace_forbidden",
      "the migration assertion's signed workspace and user are not its grant pair",
    );
  c.set("auth", auth);
  await next();
});
