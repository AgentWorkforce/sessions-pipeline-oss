// Copied from AgentWorkforce/relayhistory-cloud packages/relayhistory/src/auth/relayauth.ts at 6f6d568849c483be8e3a2a5aaeba3da1d4b47de9 (96BullSessions stand-in; changes: an unknown kid refetches the JWKS, see getJwks; verifyOriginBoundAssertion, see MOVE_NOTES.md; and the migration-assertion verifier ported from relayhistory-cloud feat/migration-assertion-ingest, see #43).
import { analyticsFromVerifiedAssertion } from "./analytics-identity.js";
import type { AuthContext, Bindings } from "../env.js";
import {
  parseUploadIdentity,
  type MigrationAssertionClaims,
} from "../lib/pipeline/migration-assertions.js";
import {
  DEFAULT_RELAYAUTH_AUDIENCE,
  DEFAULT_RELAYAUTH_ISSUER,
  DEFAULT_RELAYAUTH_JWKS_URL,
} from "../env.js";

type JwtHeader = {
  alg?: string;
  kid?: string;
};

type JwtClaims = Record<string, unknown> & {
  aud?: string | string[];
  exp?: number;
  iss?: string;
  nbf?: number;
  sub?: string;
};

type JsonWebKeySet = {
  keys?: (JsonWebKey & { kid?: string })[];
};

type IdentityErrorCode =
  | "invalid_token"
  | "invalid_claims"
  | "migration_assertion_expired";

const TOKEN_PREFIXES = ["relay_pa_", "relay_ws_", "relay_id_", "relay_ag_"];
const CLOCK_SKEW_SECONDS = 60;
export const MIGRATION_ASSERTION_SUBJECT = "agent_relayhistory_migration";
const MIGRATION_ASSERTION_CLASS = "migration_assertion";
/**
 * The v2 pre-mint base-read capability (96BullSessions
 * docs/contracts/relayhistory-export-plan-entry-v2-issuer.md section 3): its
 * own subject, token class, scope, audience, lifetime, meta key and jti
 * namespace, so it can never stand in for an upload assertion or the reverse.
 */
export const MIGRATION_BASE_READ_SUBJECT =
  "agent_relayhistory_migration_base_read";
const MIGRATION_BASE_READ_CLASS = "migration_base_read";
const MIGRATION_BASE_READ_SCOPE = "rth:migration_base_read";
const MIGRATION_BASE_READ_MAX_TTL_SECONDS = 30;
const MIGRATION_BASE_READ_JTI_PREFIX = "mbr_";
/** The base-read capability's audience path, after this History's origin. */
export const MIGRATION_BASE_READ_AUDIENCE_PATH = "/v1/pipeline/manifests/base";

interface MigrationClassDef {
  kind: "upload" | "base_read";
  sub: string;
  tokenClass: string;
  scope: string;
  maxTtl: number;
  audience: (origin: string) => string;
  identityKey: "upload" | "read";
}
const UPLOAD_CLASS: MigrationClassDef = {
  kind: "upload",
  sub: MIGRATION_ASSERTION_SUBJECT,
  tokenClass: MIGRATION_ASSERTION_CLASS,
  scope: "rth:sync",
  maxTtl: 60,
  audience: (origin) => origin,
  identityKey: "upload",
};
const BASE_READ_CLASS: MigrationClassDef = {
  kind: "base_read",
  sub: MIGRATION_BASE_READ_SUBJECT,
  tokenClass: MIGRATION_BASE_READ_CLASS,
  scope: MIGRATION_BASE_READ_SCOPE,
  maxTtl: MIGRATION_BASE_READ_MAX_TTL_SECONDS,
  audience: (origin) => `${origin}${MIGRATION_BASE_READ_AUDIENCE_PATH}`,
  identityKey: "read",
};

/**
 * The migration class a token claims by either marker (subject or
 * meta.tokenClass); `null` when its markers name different classes (malformed).
 */
function migrationClassOf(
  claims: Record<string, unknown>,
): MigrationClassDef | null | undefined {
  const tokenClass = metaOf(claims)?.tokenClass;
  const bySub = [UPLOAD_CLASS, BASE_READ_CLASS].find(
    (c) => c.sub === claims.sub,
  );
  const byClass = [UPLOAD_CLASS, BASE_READ_CLASS].find(
    (c) => c.tokenClass === tokenClass,
  );
  if (!bySub && !byClass) return undefined;
  if (bySub && byClass && bySub !== byClass) return null;
  return bySub ?? byClass;
}
/** RelayAuth caps a migration assertion's lifetime (`exp - iat`) at 60 seconds. */
const MIGRATION_ASSERTION_MAX_TTL_SECONDS = 60;
/**
 * Clock skew tolerated on a migration assertion's `iat`, `nbf` and `exp`
 * (ordinary tokens keep 60 seconds). With the 60 second lifetime cap, a
 * migration assertion is accepted for at most `iat + 65` seconds.
 */
export const MIGRATION_CLOCK_SKEW_SECONDS = 5;
const MIGRATION_ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const MIGRATION_NONCE_SHAPE = /^[A-Za-z0-9_-]{16,128}$/;
const PLAN_DIGEST_SHAPE = /^[0-9a-f]{64}$/;
const jwksCache = new Map<string, Promise<JsonWebKeySet>>();
const keyCache = new Map<string, Promise<CryptoKey>>();

export class IdentityError extends Error {
  constructor(
    readonly code: IdentityErrorCode,
    message: string,
  ) {
    super(message);
  }
}

export interface Identity {
  userId: string;
  orgId: string;
  workspaceId: string;
}

export async function verifyAgentRelayToken(
  tokenWithOptionalPrefix: string,
  env: Pick<
    Bindings,
    | "RELAYAUTH_JWKS_URL"
    | "RELAYAUTH_ISSUER"
    | "RELAYAUTH_AUDIENCE"
    | "RELAYHISTORY_ORIGIN"
    | "RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS"
    | "RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS"
    | "RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ"
  >,
): Promise<AuthContext> {
  const token = stripRelayTokenPrefix(tokenWithOptionalPrefix.trim());
  const parts = token.split(".");
  if (parts.length !== 3 || parts.some((part) => part.length === 0)) {
    throw new IdentityError("invalid_token", "invalid jwt format");
  }

  const header = parseJwtPart<JwtHeader>(parts[0], "header");
  if (header.alg !== "RS256") {
    throw new IdentityError("invalid_token", "jwt alg must be RS256");
  }
  if (!header.kid) {
    throw new IdentityError("invalid_token", "missing jwt kid");
  }

  const key = await getVerificationKey(env, header.kid);
  const verified = await crypto.subtle.verify(
    "RSASSA-PKCS1-v1_5",
    key,
    base64UrlToBytes(parts[2]),
    new TextEncoder().encode(`${parts[0]}.${parts[1]}`),
  );
  if (!verified) {
    throw new IdentityError("invalid_token", "invalid jwt signature");
  }

  const claims = parseJwtPart<JwtClaims>(parts[1], "payload");
  const historyOrigin = validateClaims(claims, env);
  const auth = extractAuthContext(claims);
  if (historyOrigin) auth.historyOrigin = historyOrigin;
  if (isMigrationAssertionMarked(claims)) {
    auth.migration = migrationAssertionClaims(claims);
    // Tenancy is exactly the asserted user, org and workspace: no alternative
    // claim name may resolve to something else.
    if (
      auth.userId !== stripRelayPrefix(String(claims.sponsorId)) ||
      auth.orgId !== stripRelayPrefix(String(claims.org)) ||
      auth.workspaceId !== stripRelayPrefix(String(claims.wks))
    ) {
      throw new IdentityError(
        "invalid_claims",
        "migration assertion tenancy is ambiguous",
      );
    }
  }
  auth.analyticsIdentity = analyticsFromVerifiedAssertion(auth);
  return auth;
}

/**
 * The dedicated migration-assertion path (`RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS`):
 * the full RelayAuth verification, then refuses anything that is not of the
 * migration class. A generic RelayAuth bearer is never admitted here.
 */
export async function verifyMigrationAssertion(
  token: string,
  env: Parameters<typeof verifyAgentRelayToken>[1],
): Promise<AuthContext> {
  if (env.RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS !== "true") {
    throw new IdentityError(
      "invalid_claims",
      "migration assertions are not enabled on this History",
    );
  }
  const auth = await verifyAgentRelayToken(token, env);
  if (!auth.migration || !isMigrationAssertionClass(auth.claims)) {
    throw new IdentityError(
      "invalid_token",
      "only RelayAuth migration assertions are accepted on this path",
    );
  }
  return auth;
}

/**
 * 96BullSessions: the deployment path of a History with an origin
 * (`RELAYHISTORY_ORIGIN`), on without the broad interim bearer. Upstream
 * accepts the same token at `/v1/auth/cli/login` and exchanges it for an
 * `rth_` session; the AlleyOop ingest has no session store, so the courier
 * presents the assertion itself on every request.
 *
 * The full RelayAuth verification (RS256 against `RELAYAUTH_JWKS_URL`, exact
 * issuer, expiry), then only RelayAuth's dedicated History assertion class
 * (`sub` agent_relayhistory_assertion and `meta.tokenClass`
 * relayhistory_assertion, which only `/v1/tokens/relayhistory-assertion`
 * signs) whose sole audience is exactly this origin. The legacy shared
 * audience is never admitted here, whatever
 * `RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS` says. A migration assertion still
 * needs its own default-off flag and passes its own contract.
 */
export async function verifyOriginBoundAssertion(
  token: string,
  env: Parameters<typeof verifyAgentRelayToken>[1],
): Promise<AuthContext> {
  const origin = configuredHistoryOrigin(env);
  if (!origin) {
    throw new IdentityError(
      "invalid_claims",
      "origin-bound assertions need RELAYHISTORY_ORIGIN",
    );
  }
  const auth = await verifyAgentRelayToken(token, {
    ...env,
    RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS: "false",
  });
  if (auth.migration) {
    // Reached only with RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS on
    // (validateClaims), and origin-bound by its own audience rule.
    if (!isMigrationAssertionClass(auth.claims)) {
      throw new IdentityError(
        "invalid_token",
        "migration assertion class is malformed",
      );
    }
    return auth;
  }
  if (
    auth.historyOrigin !== origin ||
    soleAudience(auth.claims.aud as JwtClaims["aud"]) !== origin ||
    !isRelayhistoryAssertionClass(auth.claims)
  ) {
    throw new IdentityError(
      "invalid_token",
      "only RelayAuth assertions minted for this History's origin are accepted",
    );
  }
  return auth;
}

export function identityFromAuth(auth: AuthContext): Identity {
  return {
    userId: auth.userId,
    orgId: auth.orgId,
    workspaceId: auth.workspaceId ?? auth.orgId,
  };
}

function stripRelayTokenPrefix(token: string): string {
  const prefix = TOKEN_PREFIXES.find((candidate) =>
    token.startsWith(candidate),
  );
  return prefix ? token.slice(prefix.length) : token;
}

function parseJwtPart<T>(part: string, label: string): T {
  try {
    return JSON.parse(new TextDecoder().decode(base64UrlToBytes(part))) as T;
  } catch {
    throw new IdentityError("invalid_token", `invalid jwt ${label}`);
  }
}

function base64UrlToBytes(value: string): Uint8Array {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64.padEnd(
    base64.length + ((4 - (base64.length % 4)) % 4),
    "=",
  );
  try {
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  } catch {
    throw new IdentityError("invalid_token", "invalid jwt encoding");
  }
}

async function getVerificationKey(
  env: Pick<Bindings, "RELAYAUTH_JWKS_URL" | "RELAYAUTH_ISSUER">,
  kid: string,
): Promise<CryptoKey> {
  const jwksUrl = env.RELAYAUTH_JWKS_URL?.trim() || DEFAULT_RELAYAUTH_JWKS_URL;
  const issuer = env.RELAYAUTH_ISSUER?.trim() || DEFAULT_RELAYAUTH_ISSUER;
  const cacheKey = `${jwksUrl}::${issuer}::${kid}`;
  const cached = keyCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const keyPromise = importVerificationKey(jwksUrl, kid);
  keyCache.set(cacheKey, keyPromise);
  try {
    return await keyPromise;
  } catch (error) {
    keyCache.delete(cacheKey);
    throw error;
  }
}

async function importVerificationKey(
  jwksUrl: string,
  kid: string,
): Promise<CryptoKey> {
  let jwks = await getJwks(jwksUrl);
  let jwk = jwks.keys?.find((candidate) => candidate.kid === kid);
  // 96BullSessions change: a kid we have not seen may be a rotated key. Refetch
  // the set once (rate-limited per URL) instead of rejecting until restart.
  if (
    !jwk &&
    Date.now() - (jwksFetchedAt.get(jwksUrl) ?? 0) > JWKS_REFRESH_MIN_MS
  ) {
    jwksCache.delete(jwksUrl);
    jwks = await getJwks(jwksUrl);
    jwk = jwks.keys?.find((candidate) => candidate.kid === kid);
  }
  if (!jwk) {
    throw new IdentityError("invalid_token", "unknown jwt kid");
  }
  if (jwk.kty !== "RSA") {
    throw new IdentityError("invalid_token", "jwt key must be RSA");
  }

  return crypto.subtle.importKey(
    "jwk",
    jwk,
    {
      name: "RSASSA-PKCS1-v1_5",
      hash: "SHA-256",
    },
    false,
    ["verify"],
  );
}

const jwksFetchedAt = new Map<string, number>();
const JWKS_REFRESH_MIN_MS = 30_000;

async function getJwks(jwksUrl: string): Promise<JsonWebKeySet> {
  const cached = jwksCache.get(jwksUrl);
  if (cached) {
    return cached;
  }

  const jwksPromise = fetch(jwksUrl).then(async (response) => {
    if (!response.ok) {
      throw new IdentityError("invalid_token", "unable to fetch JWKS");
    }
    return (await response.json()) as JsonWebKeySet;
  });
  jwksCache.set(jwksUrl, jwksPromise);
  jwksFetchedAt.set(jwksUrl, Date.now());
  try {
    return await jwksPromise;
  } catch (error) {
    jwksCache.delete(jwksUrl);
    throw error;
  }
}

/**
 * Returns this History's origin when the token was bound to it, `undefined`
 * when it was accepted under the legacy shared audience.
 */
function validateClaims(
  claims: JwtClaims,
  env: Pick<
    Bindings,
    | "RELAYAUTH_ISSUER"
    | "RELAYAUTH_AUDIENCE"
    | "RELAYHISTORY_ORIGIN"
    | "RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS"
    | "RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS"
    | "RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ"
  >,
): string | undefined {
  const issuer = env.RELAYAUTH_ISSUER?.trim() || DEFAULT_RELAYAUTH_ISSUER;
  if (claims.iss !== issuer) {
    throw new IdentityError("invalid_claims", "invalid jwt issuer");
  }
  const historyOrigin = configuredHistoryOrigin(env);
  const migration = isMigrationAssertionMarked(claims);
  const migrationClass = migrationClassOf(claims);
  if (
    migrationClass?.kind === "base_read" &&
    env.RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ !== "true"
  ) {
    // The base read has its own default-off activation gate.
    throw new IdentityError(
      "invalid_claims",
      "migration base reads are not enabled on this History",
    );
  }
  if (migration && env.RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS !== "true") {
    // Dedicated, default-off switch: the broad interim bearer flag alone
    // never admits a migration assertion.
    throw new IdentityError(
      "invalid_claims",
      "migration assertions are not enabled on this History",
    );
  }
  if (migration) {
    // A migration assertion acts for another user, so it is accepted only when
    // minted for this History's origin, never on the shared legacy audience.
    if (
      historyOrigin === undefined ||
      !migrationClass ||
      soleAudience(claims.aud) !== migrationClass.audience(historyOrigin)
    ) {
      throw new IdentityError(
        "invalid_claims",
        "migration assertion must be bound to this History's origin",
      );
    }
  }
  // Only RelayAuth's dedicated assertion endpoint mints for a History origin.
  // Any other token that carries the origin as its audience is not one.
  // Once this History has an origin, every accepted assertion must name
  // exactly one audience: a token that also names another History (or this
  // one next to the shared audience) is ambiguous and refused.
  const boundHere =
    migration ||
    (historyOrigin !== undefined &&
      soleAudience(claims.aud) === historyOrigin &&
      isRelayhistoryAssertionClass(claims));
  if (!boundHere) {
    const legacy = env.RELAYAUTH_AUDIENCE?.trim() || DEFAULT_RELAYAUTH_AUDIENCE;
    const legacyAudience =
      historyOrigin === undefined
        ? audienceMatches(claims.aud, legacy)
        : soleAudience(claims.aud) === legacy;
    if (!acceptsLegacyAudience(env, historyOrigin) || !legacyAudience) {
      throw new IdentityError("invalid_claims", "invalid jwt audience");
    }
  }

  const now = Math.floor(Date.now() / 1000);
  const skew = migration ? MIGRATION_CLOCK_SKEW_SECONDS : CLOCK_SKEW_SECONDS;
  // A migration token's whole shape is validated first, so a malformed one
  // stays terminal (invalid_claims) and only a well-formed, expired one gets
  // the re-mintable migration_assertion_expired.
  if (migration) migrationAssertionClaims(claims, now);
  if (typeof claims.exp !== "number" || claims.exp + skew <= now) {
    // A distinct code for an expired migration assertion: the courier
    // re-mints, while any other 401 stays terminal.
    if (migration && typeof claims.exp === "number")
      throw new IdentityError(
        "migration_assertion_expired",
        "migration assertion is expired; mint a fresh one",
      );
    throw new IdentityError("invalid_claims", "jwt is expired");
  }
  if (typeof claims.nbf === "number" && claims.nbf - skew > now) {
    throw new IdentityError("invalid_claims", "jwt is not active yet");
  }
  if (!stringClaim(claims, "sub")) {
    throw new IdentityError("invalid_claims", "missing jwt subject");
  }
  return boundHere ? historyOrigin : undefined;
}

/**
 * Either marker of RelayAuth's `/v1/tokens/migration-assertion` class. A token
 * with only one of them is not ordinary either: it is refused as malformed.
 */
function isMigrationAssertionMarked(claims: Record<string, unknown>): boolean {
  return migrationClassOf(claims) !== undefined;
}

/**
 * The claims RelayAuth's `/v1/tokens/migration-assertion` alone signs: subject
 * `agent_relayhistory_migration` and `meta.tokenClass` `migration_assertion`.
 * Audience is checked separately.
 */
export function isMigrationAssertionClass(
  claims: Record<string, unknown>,
): boolean {
  const c = migrationClassOf(claims);
  return (
    !!c && claims.sub === c.sub && metaOf(claims)?.tokenClass === c.tokenClass
  );
}

/**
 * The single-use claims of a migration assertion, after checking its exact
 * shape: both class markers, an access token whose only scope is `rth:sync`,
 * a lifetime of at most 60 seconds, well-formed `jti`, nonce, grant and plan
 * digest, a sponsor chain rooted at the signed `sponsorId`, and well-formed
 * `meta.sourceUserId` / `meta.workspaceId`.
 *
 * Whether the signed pair (`wks`, `sponsorId`) equals the grant pair
 * (`meta.workspaceId`, `meta.sourceUserId`) and the upload's target is an
 * authorization decision, not a malformed token: the ingest refuses it with
 * 403 `migration_workspace_forbidden` (see `migrationPairAuthorized`).
 */
function migrationAssertionClaims(
  claims: Record<string, unknown>,
  now = Math.floor(Date.now() / 1000),
): MigrationAssertionClaims {
  const refuse = (message: string): never => {
    throw new IdentityError("invalid_claims", `migration assertion ${message}`);
  };
  const meta = metaOf(claims);
  const def = migrationClassOf(claims);
  if (!def || !isMigrationAssertionClass(claims) || !meta)
    refuse("class is malformed");
  if (claims.token_type !== "access") refuse("must be an access token");
  const scopes = claims.scopes;
  if (
    claims.scope !== undefined ||
    !Array.isArray(scopes) ||
    scopes.length !== 1 ||
    scopes[0] !== def!.scope
  ) {
    refuse(`scopes must be exactly ${def!.scope}`);
  }
  const iat = claims.iat;
  const exp = claims.exp;
  if (
    typeof iat !== "number" ||
    typeof exp !== "number" ||
    exp <= iat ||
    exp - iat > def!.maxTtl ||
    iat - MIGRATION_CLOCK_SKEW_SECONDS > now
  ) {
    refuse("lifetime is invalid");
  }
  const jti = claims.jti;
  const { nonce, grantId, planDigest, sourceUserId, workspaceId } = meta!;
  if (
    typeof sourceUserId !== "string" ||
    !MIGRATION_ID_SHAPE.test(sourceUserId)
  )
    refuse("sourceUserId is malformed");
  if (typeof workspaceId !== "string" || !MIGRATION_ID_SHAPE.test(workspaceId))
    refuse("workspaceId is malformed");
  if (
    typeof jti !== "string" ||
    !MIGRATION_ID_SHAPE.test(jti) ||
    // Separate jti namespaces: only a base-read capability starts with mbr_.
    jti.startsWith(MIGRATION_BASE_READ_JTI_PREFIX) !==
      (def!.kind === "base_read")
  ) {
    refuse("jti is malformed");
  }
  if (typeof nonce !== "string" || !MIGRATION_NONCE_SHAPE.test(nonce)) {
    refuse("nonce is malformed");
  }
  if (typeof grantId !== "string" || !MIGRATION_ID_SHAPE.test(grantId)) {
    refuse("grantId is malformed");
  }
  if (typeof planDigest !== "string" || !PLAN_DIGEST_SHAPE.test(planDigest)) {
    refuse("planDigest is malformed");
  }
  // The signed per-upload identity (relayauth #111), derived by the issuer
  // from the grant's stored plan.
  // An upload assertion carries meta.upload (v1 or v2 commit) and no
  // meta.read; a base-read capability carries meta.read and no meta.upload.
  const other = def!.identityKey === "upload" ? "read" : "upload";
  if (meta![other] !== undefined) refuse(`must not carry meta.${other}`);
  const parsed = parseUploadIdentity(meta![def!.identityKey]);
  const upload =
    parsed &&
    (def!.kind === "base_read") ===
      ((parsed as { kind?: unknown }).kind === "backfill_base_read")
      ? parsed
      : undefined;
  if (!upload) refuse(`${def!.identityKey} identity is malformed`);
  const sponsorId = stringClaim(claims, "sponsorId");
  const chain = claims.sponsorChain;
  if (
    !sponsorId ||
    !Array.isArray(chain) ||
    chain.length !== 2 ||
    chain[0] !== sponsorId ||
    chain[1] !== def!.sub
  ) {
    refuse("sponsor chain is malformed");
  }
  if (!stringClaim(claims, "wks") || !stringClaim(claims, "org")) {
    refuse("workspace or org is missing");
  }
  return {
    jti: jti as string,
    nonce: nonce as string,
    grantId: grantId as string,
    planDigest: planDigest as string,
    upload: upload!,
    capability: def!.kind,
    sourceUserId: sourceUserId as string,
    workspaceId: workspaceId as string,
    // The ledger must keep this row at least until no clock could still
    // accept the token (see migration_assertion_uses retention).
    expiresAt: (exp as number) + MIGRATION_CLOCK_SKEW_SECONDS,
  };
}

function metaOf(
  claims: Record<string, unknown>,
): Record<string, unknown> | undefined {
  const meta = claims.meta;
  return typeof meta === "object" && meta !== null && !Array.isArray(meta)
    ? (meta as Record<string, unknown>)
    : undefined;
}

/**
 * The claims RelayAuth's `/v1/tokens/relayhistory-assertion` alone signs:
 * subject `agent_relayhistory_assertion` and `meta.tokenClass`
 * `relayhistory_assertion`. Audience is checked separately.
 */
export function isRelayhistoryAssertionClass(
  claims: Record<string, unknown>,
): boolean {
  const meta = claims.meta;
  return (
    claims.sub === "agent_relayhistory_assertion" &&
    typeof meta === "object" &&
    meta !== null &&
    (meta as Record<string, unknown>).tokenClass === "relayhistory_assertion"
  );
}

/**
 * `RELAYHISTORY_ORIGIN` in the canonical `URL#origin` form RelayAuth stamps
 * into `aud`. A value that is not a bare origin is a deployment error, and
 * every assertion is refused rather than guessing which part to compare.
 */
export function configuredHistoryOrigin(
  env: Pick<Bindings, "RELAYHISTORY_ORIGIN">,
): string | undefined {
  const raw = env.RELAYHISTORY_ORIGIN?.trim();
  if (!raw) return undefined;
  let url: URL | undefined;
  try {
    url = new URL(raw);
  } catch {
    url = undefined;
  }
  if (!url || raw.replace(/\/$/, "").toLowerCase() !== url.origin) {
    throw new IdentityError(
      "invalid_claims",
      "RELAYHISTORY_ORIGIN must be a bare origin such as https://history.example.com",
    );
  }
  return url.origin;
}

function acceptsLegacyAudience(
  env: Pick<Bindings, "RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS">,
  historyOrigin: string | undefined,
): boolean {
  const flag = env.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS?.trim().toLowerCase();
  if (flag === "true") return true;
  if (flag === "false") return false;
  return historyOrigin === undefined;
}

function soleAudience(audience: JwtClaims["aud"]): string | undefined {
  if (typeof audience === "string") return audience;
  return Array.isArray(audience) && audience.length === 1
    ? audience[0]
    : undefined;
}

function audienceMatches(
  audience: JwtClaims["aud"],
  expected: string,
): boolean {
  if (typeof audience === "string") {
    return audience === expected;
  }
  return Array.isArray(audience) && audience.includes(expected);
}

function extractAuthContext(claims: JwtClaims): AuthContext {
  const userId = firstStringClaim(claims, [
    "sponsorId",
    "sponsor_id",
    "relay_user_id",
    "user_id",
    "userId",
    "uid",
    "sub",
  ]);
  const orgId = firstStringClaim(claims, [
    "relay_org_id",
    "org_id",
    "orgId",
    "organization_id",
    "organizationId",
    "org",
    "organization",
  ]);
  const workspaceId = firstStringClaim(claims, [
    "relay_workspace_id",
    "workspace_id",
    "workspaceId",
    "workspace",
    "wks",
    "ws_id",
    "wsId",
  ]);
  const tokenSubject = stringClaim(claims, "sub");

  if (!userId) {
    throw new IdentityError("invalid_claims", "missing user id claim");
  }
  if (!orgId) {
    throw new IdentityError("invalid_claims", "missing org id claim");
  }
  if (!tokenSubject) {
    throw new IdentityError("invalid_claims", "missing jwt subject");
  }

  return {
    userId: stripRelayPrefix(userId),
    orgId: stripRelayPrefix(orgId),
    workspaceId: workspaceId ? stripRelayPrefix(workspaceId) : undefined,
    tokenSubject,
    scopes: scopeClaims(claims),
    claims,
  };
}

function scopeClaims(claims: Record<string, unknown>): string[] {
  const raw = claims.scope ?? claims.scopes;
  if (typeof raw === "string") {
    return raw.split(/\s+/).filter(Boolean);
  }
  if (Array.isArray(raw)) {
    return raw.filter((item): item is string => typeof item === "string");
  }
  return [];
}

function firstStringClaim(
  claims: Record<string, unknown>,
  keys: string[],
): string | undefined {
  for (const key of keys) {
    const value = stringClaim(claims, key);
    if (value) {
      return value;
    }
  }
  return undefined;
}

function stringClaim(
  claims: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = claims[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function stripRelayPrefix(value: string): string {
  return value.startsWith("relay_") ? value.slice("relay_".length) : value;
}
