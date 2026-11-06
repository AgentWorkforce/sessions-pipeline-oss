/**
 * The migration-assertion verifier and requireAuth as ported from
 * relayhistory-cloud feat/migration-assertion-ingest (auth/relayauth.ts,
 * middleware/auth.ts): the dedicated default-off flag, the route allowlist,
 * authorization from the signed pair, and the 5 second clock skew. Synthetic
 * ids and a throwaway signing key only.
 */
import { createSign, generateKeyPairSync, randomBytes } from "node:crypto";
import { Hono, type Context } from "hono";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AppEnv, Bindings } from "../src/env.js";
import { requireAuth } from "../src/middleware/auth.js";

const ORIGIN = "https://history.example.test";
const ISSUER = "https://relayauth.dev";
const KID = "migration-auth-test-key";
const JWKS_URL = "https://relayauth.test/migration-auth/jwks.json";
const ORG = "org-synthetic";
const WORKSPACE = "ws_1";
const USER = "user-synthetic-a";
const PLAN = "a".repeat(64);
const GRANT = `mgr_${"0".repeat(32)}`;
/** A canonical upload identity (its derivation is tested against the contract vectors). */
const UPLOAD = {
  grant_id: GRANT,
  plan_digest: PLAN,
  key: "v1/ws=ws_1/m=mach-1/h=claude/d=2026/10/25/s=s-1/manifest.json",
  org_id: ORG,
  workspace_id: WORKSPACE,
  source_user_id: USER,
  session: { machine_id: "mach-1", harness: "claude", session_id: "s-1", started_at: "2026-10-25T00:00:00.000Z" },
  content_sha256: "c".repeat(64),
  entry_sha256: "d".repeat(64),
};

const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const JWKS = { keys: [{ ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256" }] };

function sign(claims: Record<string, unknown>) {
  for (const [key, value] of Object.entries(claims)) if (value === undefined) delete claims[key];
  const b64 = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const header = b64({ alg: "RS256", kid: KID, typ: "JWT" });
  const body = b64(claims);
  const signature = createSign("RSA-SHA256").update(`${header}.${body}`).end().sign(privateKey, "base64url");
  return `${header}.${body}.${signature}`;
}

function migrationClaims(overrides: Record<string, unknown> = {}, meta: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return {
    sub: "agent_relayhistory_migration",
    org: ORG,
    wks: WORKSPACE,
    scopes: ["rth:sync"],
    sponsorId: USER,
    sponsorChain: [USER, "agent_relayhistory_migration"],
    token_type: "access",
    iss: ISSUER,
    aud: [ORIGIN],
    iat: now,
    exp: now + 60,
    jti: `tok_${randomBytes(16).toString("hex")}`,
    ...overrides,
    meta: {
      tokenClass: "migration_assertion",
      grantId: GRANT,
      planDigest: PLAN,
      nonce: randomBytes(24).toString("base64url"),
      sourceUserId: USER,
      actorId: "actor-synthetic",
      actorOrgId: ORG,
      workspaceId: WORKSPACE,
      upload: UPLOAD,
      ...meta,
    },
  } as Record<string, unknown>;
}
const migrationToken = (overrides: Record<string, unknown> = {}, meta: Record<string, unknown> = {}) =>
  sign(migrationClaims(overrides, meta));

function relayhistoryToken() {
  const now = Math.floor(Date.now() / 1000);
  return sign({
    sub: "agent_relayhistory_assertion", org: ORG, wks: WORKSPACE, sponsorId: USER,
    scopes: ["rth:read", "rth:sync"], token_type: "access", iss: ISSUER, aud: [ORIGIN],
    iat: now, exp: now + 60, meta: { tokenClass: "relayhistory_assertion" },
  });
}

function bindings(overrides: Partial<Bindings> = {}): Bindings {
  return {
    ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
    RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS: "true",
    RELAYAUTH_JWKS_URL: JWKS_URL,
    RELAYAUTH_ISSUER: ISSUER,
    RELAYHISTORY_ORIGIN: ORIGIN,
    ...overrides,
  } as Bindings;
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
  const echo = (c: Context<AppEnv>) =>
    c.json({ migration: c.get("auth")?.migration ?? null, userId: c.get("auth")?.userId });
  v1.post("/pipeline/manifests", echo);
  v1.get("/pipeline/manifests/status", echo);
  v1.post("/pipeline/manifests/status", echo);
  v1.post("/pipeline/chunks", echo);
  v1.post("/pipeline/events", echo);
  const root = new Hono<AppEnv>();
  root.route("/v1", v1);
  return (path: string, token: string, env: Partial<Bindings> = {}, method = "POST") =>
    root.request(path, { method, headers: { Authorization: `Bearer ${token}` } }, bindings(env));
}

const code = async (response: Response) => ((await response.json()) as { error?: { code?: string } }).error?.code;

describe("the dedicated migration-assertion flag", () => {
  it("refuses a migration token with the flag off, even with the interim bearer on", async () => {
    const request = app();
    for (const flag of [undefined, "false"]) {
      const response = await request("/v1/pipeline/manifests", migrationToken(), { RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS: flag });
      expect(response.status).toBe(401);
      expect(await code(response)).toBe("invalid_claims");
    }
  });

  it("admits a migration token through the dedicated path with the interim bearer off", async () => {
    const claims = migrationClaims();
    const response = await app()("/v1/pipeline/manifests", sign(claims), { ALLOW_INTERIM_RELAYAUTH_BEARER: undefined });
    expect(response.status).toBe(200);
    const body = (await response.json()) as { migration: Record<string, unknown>; userId: string };
    expect(body.userId).toBe(USER);
    expect(body.migration).toMatchObject({ grantId: GRANT, planDigest: PLAN, sourceUserId: USER, workspaceId: WORKSPACE, expiresAt: (claims.exp as number) + 5 });
  });

  it("never admits a generic RelayAuth bearer through the dedicated path", async () => {
    const request = app();
    // Without an origin, only the explicit interim opt-in verifies a History
    // assertion at all (dev/tests; buildIngestApp defaults it off, see
    // src/alleyoop/__tests__/ingest-auth.test.ts). With an origin, the
    // origin-bound path admits it (test/origin-bound-assertion-auth.test.ts).
    expect((await request("/v1/pipeline/manifests", relayhistoryToken())).status).toBe(200);
    // Without an origin the shared audience verifies, so this reaches the
    // migration path's class check and is refused there.
    const now = Math.floor(Date.now() / 1000);
    const generic = sign({
      sub: "agent_relayhistory_assertion", org: ORG, wks: WORKSPACE, sponsorId: USER,
      scopes: ["rth:read", "rth:sync"], token_type: "access", iss: ISSUER, aud: ["relayhistory"],
      iat: now, exp: now + 60, meta: { tokenClass: "relayhistory_assertion" },
    });
    const refused = await request("/v1/pipeline/manifests", generic, {
      ALLOW_INTERIM_RELAYAUTH_BEARER: undefined,
      RELAYHISTORY_ORIGIN: undefined,
    });
    expect(refused.status).toBe(401);
    expect(await code(refused)).toBe("invalid_token");
  });

  it("refuses everything with neither flag", async () => {
    const response = await app()("/v1/pipeline/manifests", migrationToken(), {
      ALLOW_INTERIM_RELAYAUTH_BEARER: undefined,
      RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS: undefined,
    });
    expect(response.status).toBe(401);
  });
});

describe("where and for whom a migration token is admitted", () => {
  it("only on chunks, the manifest commit and GET manifests/status (403 migration_assertion_route)", async () => {
    const request = app();
    for (const [path, method] of [["/v1/pipeline/events", "POST"], ["/v1/pipeline/manifests/status", "POST"]] as const) {
      const response = await request(path, migrationToken(), {}, method);
      expect(response.status).toBe(403);
      expect(await code(response)).toBe("migration_assertion_route");
    }
    for (const [path, method] of [["/v1/pipeline/chunks", "POST"], ["/v1/pipeline/manifests/status", "GET"]] as const)
      expect((await request(path, migrationToken(), {}, method)).status).toBe(200);
  });

  it("only when the signed wks and sponsorId are the grant pair (403 migration_workspace_forbidden)", async () => {
    const request = app();
    for (const token of [
      migrationToken({}, { workspaceId: "ws_2" }),
      migrationToken({ wks: "ws_2" }),
      migrationToken({}, { sourceUserId: "user-other" }),
      migrationToken({ sponsorId: "user-other", sponsorChain: ["user-other", "agent_relayhistory_migration"] }),
    ]) {
      const response = await request("/v1/pipeline/manifests", token);
      expect(response.status).toBe(403);
      expect(await code(response)).toBe("migration_workspace_forbidden");
    }
  });
});

describe("canonical ids and the upload identity shape", () => {
  it("refuses an identity with a relay_ alias, or none, as malformed; accepts prefixed signed claims", async () => {
    const request = app();
    for (const upload of [
      { ...UPLOAD, source_user_id: `relay_${USER}` },
      { ...UPLOAD, workspace_id: `relay_${WORKSPACE}` },
      { ...UPLOAD, plan_digest: undefined },
      undefined,
    ]) {
      const response = await request("/v1/pipeline/manifests", migrationToken({}, { upload }));
      expect(response.status).toBe(401);
      expect(await code(response)).toBe("invalid_claims");
    }
    const prefixed = migrationToken(
      { sponsorId: `relay_${USER}`, sponsorChain: [`relay_${USER}`, "agent_relayhistory_migration"], wks: `relay_${WORKSPACE}` },
      { sourceUserId: `relay_${USER}`, workspaceId: `relay_${WORKSPACE}` },
    );
    expect((await request("/v1/pipeline/manifests", prefixed)).status).toBe(200);
  });

  it("validates the shape before answering expired: a malformed expired token stays terminal", async () => {
    const now = Math.floor(Date.now() / 1000);
    for (const token of [
      migrationToken({ iat: now - 56, exp: now - 6 }, { nonce: "short" }),
      migrationToken({ iat: now - 56, exp: now - 6 }, { upload: { ...UPLOAD, extra: "x" } }),
      migrationToken({ iat: now - 56, exp: now - 6, token_type: "id" }),
    ]) {
      const response = await app()("/v1/pipeline/manifests", token);
      expect(response.status).toBe(401);
      expect(await code(response)).toBe("invalid_claims");
    }
  });

  it("keeps a bad signature terminal (invalid_token), even when also expired", async () => {
    const now = Math.floor(Date.now() / 1000);
    const [head, body] = migrationToken({ iat: now - 56, exp: now - 6 }).split(".");
    const response = await app()("/v1/pipeline/manifests", `${head}.${body}.${"A".repeat(342)}`);
    expect(response.status).toBe(401);
    expect(await code(response)).toBe("invalid_token");
  });
});

describe("migration clock skew (5 seconds)", () => {
  it("accepts within 5 seconds and refuses beyond", async () => {
    const request = app();
    const now = Math.floor(Date.now() / 1000);
    for (const [overrides, status] of [
      [{ iat: now - 57, exp: now - 2 }, 200],
      [{ iat: now - 56, exp: now - 6 }, "migration_assertion_expired"],
      [{ iat: now + 2, exp: now + 50 }, 200],
      [{ iat: now + 7, exp: now + 60 }, "invalid_claims"],
      [{ nbf: now + 7 }, "invalid_claims"],
      // Past expires_at plus the purge margin: refused before any ledger
      // lookup, so a purged row can never make the token acceptable again.
      [{ iat: now - 400, exp: now - 340 }, "migration_assertion_expired"],
    ] as const) {
      const response = await request("/v1/pipeline/manifests", migrationToken(overrides));
      if (status === 200) expect(response.status).toBe(200);
      else {
        expect(response.status).toBe(401);
        expect(await code(response)).toBe(status);
      }
    }
  });
});
