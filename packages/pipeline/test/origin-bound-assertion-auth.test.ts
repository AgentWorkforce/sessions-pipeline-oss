/**
 * requireAuth's origin-bound path (RELAYHISTORY_ORIGIN set, the interim bearer
 * off): the production auth of hosted and customer installs. Only RelayAuth's
 * dedicated History assertion class, RS256-signed by a key in the configured
 * JWKS, from the configured issuer, whose sole audience is exactly this origin.
 * Everything else, including an unsigned token and the bearers the interim
 * flag used to admit, is refused. Synthetic ids and a throwaway key only.
 */
import { createHmac, createSign, generateKeyPairSync } from "node:crypto";
import { Hono, type Context } from "hono";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AppEnv, Bindings } from "../src/env.js";
import { requireAuth } from "../src/middleware/auth.js";

const ORIGIN = "https://history.example.test";
const ISSUER = "https://relayauth.example.test";
const KID = "origin-bound-auth-test-key";
const JWKS_URL = "https://relayauth.test/origin-bound-auth/jwks.json";
const ORG = "org-synthetic";
const WORKSPACE = "ws_1";
const USER = "user-synthetic-a";

const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const other = generateKeyPairSync("rsa", { modulusLength: 2048 });
const JWKS = { keys: [{ ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256" }] };

const b64 = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");

function sign(claims: Record<string, unknown>, key = privateKey, kid = KID) {
  const header = b64({ alg: "RS256", kid, typ: "JWT" });
  const body = b64(claims);
  return `${header}.${body}.${createSign("RSA-SHA256").update(`${header}.${body}`).end().sign(key, "base64url")}`;
}

/** Claims as RelayAuth's /v1/tokens/relayhistory-assertion mints them for ORIGIN. */
function assertionClaims(overrides: Record<string, unknown> = {}, meta: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return {
    sub: "agent_relayhistory_assertion",
    org: ORG,
    wks: WORKSPACE,
    scopes: ["rth:sync", "rth:read"],
    sponsorId: USER,
    sponsorChain: [USER, "agent_relayhistory_assertion"],
    token_type: "access",
    iss: ISSUER,
    aud: [ORIGIN],
    iat: now,
    exp: now + 60,
    jti: "tok_synthetic",
    ...overrides,
    meta: { tokenClass: "relayhistory_assertion", historyOrigin: ORIGIN, machineId: "mach-1", ...meta },
  } as Record<string, unknown>;
}

function bindings(overrides: Partial<Bindings> = {}): Bindings {
  // The production composition: no interim bearer, no migration flag.
  return { RELAYAUTH_JWKS_URL: JWKS_URL, RELAYAUTH_ISSUER: ISSUER, RELAYHISTORY_ORIGIN: ORIGIN, ...overrides } as Bindings;
}

beforeEach(() => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    if (String(input) === JWKS_URL) return Response.json(JWKS);
    throw new Error(`unexpected fetch ${String(input)}`);
  }));
});
afterEach(() => {
  vi.unstubAllGlobals();
});

function app() {
  const v1 = new Hono<AppEnv>();
  v1.use("*", requireAuth);
  const echo = (c: Context<AppEnv>) => {
    const auth = c.get("auth");
    return c.json({ userId: auth?.userId, orgId: auth?.orgId, workspaceId: auth?.workspaceId, historyOrigin: auth?.historyOrigin ?? null, scopes: auth?.scopes });
  };
  v1.post("/pipeline/chunks", echo);
  v1.post("/pipeline/manifests", echo);
  const root = new Hono<AppEnv>();
  root.route("/v1", v1);
  return (token: string | null, env: Partial<Bindings> = {}) =>
    root.request("/v1/pipeline/manifests", { method: "POST", headers: token === null ? {} : { Authorization: `Bearer ${token}` } }, bindings(env));
}

const code = async (response: Response) => ((await response.json()) as { error?: { code?: string } }).error?.code;

describe("the origin-bound path (interim bearer off, RELAYHISTORY_ORIGIN set)", () => {
  it("admits a RelayAuth assertion minted for this origin, as its sponsor, tenant and scopes", async () => {
    const response = await app()(sign(assertionClaims()));
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      userId: USER, orgId: ORG, workspaceId: WORKSPACE, historyOrigin: ORIGIN, scopes: ["rth:sync", "rth:read"],
    });
  });

  it("compares the configured origin in its canonical form (trailing slash, case)", async () => {
    for (const configured of [`${ORIGIN}/`, ORIGIN.toUpperCase().replace("HTTPS", "https")]) {
      const response = await app()(sign(assertionClaims()), { RELAYHISTORY_ORIGIN: configured });
      expect(`${configured}: ${response.status}`).toBe(`${configured}: 200`);
    }
  });

  it("refuses an unsigned, alg-swapped, foreign-key, unknown-kid or tampered token (401)", async () => {
    const claims = assertionClaims();
    const unsigned = `${b64({ alg: "none", typ: "JWT" })}.${b64(claims)}.`;
    const noneWithSig = `${b64({ alg: "none", kid: KID, typ: "JWT" })}.${b64(claims)}.c2ln`;
    const hsHeader = b64({ alg: "HS256", kid: KID, typ: "JWT" });
    // HS256 keyed with the public key: the classic RS/HS confusion.
    const hs256 = `${hsHeader}.${b64(claims)}.${createHmac("sha256", publicKey.export({ type: "spki", format: "pem" })).update(`${hsHeader}.${b64(claims)}`).digest("base64url")}`;
    const foreign = sign(claims, other.privateKey);
    const unknownKid = sign(claims, other.privateKey, "not-in-the-jwks");
    const [h, , s] = sign(claims).split(".");
    const tampered = `${h}.${b64({ ...claims, org: "org-someone-else" })}.${s}`;
    for (const [name, token] of Object.entries({ unsigned, noneWithSig, hs256, foreign, unknownKid, tampered })) {
      const response = await app()(token);
      expect(`${name}: ${response.status}`).toBe(`${name}: 401`);
      expect(await code(response)).toBe("invalid_token");
    }
  });

  it("refuses a correctly signed token of any other audience (401)", async () => {
    const audiences: Record<string, unknown> = {
      legacy: ["relayhistory"],
      legacyString: "relayhistory",
      otherHistory: ["https://history.other.test"],
      originAndLegacy: [ORIGIN, "relayhistory"],
      originTwice: [ORIGIN, ORIGIN],
      originWithPath: [`${ORIGIN}/v1`],
      httpScheme: [ORIGIN.replace("https:", "http:")],
      none: undefined,
    };
    for (const [name, aud] of Object.entries(audiences)) {
      const claims = assertionClaims({ aud });
      if (aud === undefined) delete claims.aud;
      // Not even with the legacy switch on: this path never admits the shared audience.
      for (const legacy of [undefined, "true"]) {
        const response = await app()(sign(claims), { RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS: legacy });
        expect(`${name}/${legacy}: ${response.status}`).toBe(`${name}/${legacy}: 401`);
      }
    }
  });

  it("refuses a token for this origin that is not RelayAuth's History assertion class (401)", async () => {
    const variants: Record<string, string> = {
      genericSubject: sign(assertionClaims({ sub: USER })),
      noTokenClass: sign(assertionClaims({}, { tokenClass: undefined })),
      otherTokenClass: sign(assertionClaims({}, { tokenClass: "access" })),
      noMeta: sign({ ...assertionClaims(), meta: undefined }),
      // Either migration marker alone is malformed, and the class needs its own flag.
      migrationSubject: sign(assertionClaims({ sub: "agent_relayhistory_migration" })),
      migrationTokenClass: sign(assertionClaims({}, { tokenClass: "migration_assertion" })),
    };
    for (const [name, token] of Object.entries(variants)) {
      const response = await app()(token);
      expect(`${name}: ${response.status}`).toBe(`${name}: 401`);
    }
  });

  it("refuses a wrong issuer, an expired token and a not-yet-valid token (401 invalid_claims)", async () => {
    const now = Math.floor(Date.now() / 1000);
    for (const [name, claims] of Object.entries({
      issuer: assertionClaims({ iss: "https://relayauth.other.test" }),
      expired: assertionClaims({ iat: now - 600, exp: now - 120 }),
      noExpiry: assertionClaims({ exp: undefined }),
      future: assertionClaims({ nbf: now + 600 }),
    })) {
      if (claims.exp === undefined) delete claims.exp;
      const response = await app()(sign(claims));
      expect(`${name}: ${response.status}`).toBe(`${name}: 401`);
      expect(await code(response)).toBe("invalid_claims");
    }
  });

  it("refuses non-JWT bearers: opaque relayhistory tokens, raw strings and a missing header (401)", async () => {
    const request = app();
    for (const [token, expected] of [
      [null, "missing_authorization"],
      ["rth_at_synthetic", "unsupported_token"],
      ["rth_st_synthetic", "unsupported_token"],
      ["interim-bearer-synthetic", "invalid_token"],
      ["a.b", "invalid_token"],
    ] as const) {
      const response = await request(token);
      expect(`${token}: ${response.status}`).toBe(`${token}: 401`);
      expect(await code(response)).toBe(expected);
    }
  });

  it("fails closed on a malformed RELAYHISTORY_ORIGIN (401, nothing admitted)", async () => {
    for (const configured of ["https://history.example.test/v1", "history.example.test", "not a url"]) {
      const response = await app()(sign(assertionClaims()), { RELAYHISTORY_ORIGIN: configured });
      expect(`${configured}: ${response.status}`).toBe(`${configured}: 401`);
    }
  });
});

describe("regression: no bypass when neither the interim bearer nor an origin is configured", () => {
  it("refuses a valid origin-bound assertion, a legacy token and an unsigned token with 401 auth_not_configured", async () => {
    const claims = assertionClaims();
    for (const [name, token] of Object.entries({
      originBound: sign(claims),
      legacy: sign(assertionClaims({ aud: ["relayhistory"] })),
      unsigned: `${b64({ alg: "none", typ: "JWT" })}.${b64(claims)}.`,
    })) {
      for (const flag of [undefined, "false", "1", "TRUE", "yes"]) {
        const response = await app()(token, { RELAYHISTORY_ORIGIN: undefined, ALLOW_INTERIM_RELAYAUTH_BEARER: flag });
        expect(`${name}/${flag}: ${response.status}`).toBe(`${name}/${flag}: 401`);
        expect(await code(response)).toBe("auth_not_configured");
      }
    }
  });

  it("only the exact interim opt-in (dev) verifies a legacy-audience token", async () => {
    const legacy = sign(assertionClaims({ aud: ["relayhistory"] }));
    expect((await app()(legacy, { RELAYHISTORY_ORIGIN: undefined, ALLOW_INTERIM_RELAYAUTH_BEARER: "true" })).status).toBe(200);
    // An origin turns the legacy audience off even on the interim path.
    expect((await app()(legacy, { ALLOW_INTERIM_RELAYAUTH_BEARER: "true" })).status).toBe(401);
    // And on the origin-bound path the interim flag off refuses it.
    expect((await app()(legacy)).status).toBe(401);
  });
});
