/**
 * Local auth for the AlleyOop ingest API. Locally (`ALLOW_INTERIM_RELAYAUTH_BEARER`
 * on, no origin), the vendored middleware accepts an RS256 JWT verified against
 * a JWKS URL: `96bulls up` mints a key pair, the ingest server publishes the
 * public key at `/.well-known/jwks.json`, and the courier gets a token.
 * Hosted and customer installs point `RELAYAUTH_JWKS_URL` at RelayAuth and set
 * `RELAYHISTORY_ORIGIN`, which admits only RelayAuth's assertions minted for
 * that origin, with the interim bearer off.
 */
import {
  createPrivateKey,
  createPublicKey,
  createSign,
  generateKeyPairSync,
  randomUUID,
} from "node:crypto";

export interface LocalKeys {
  kid: string;
  privateKeyPem: string;
  jwks: { keys: Array<Record<string, unknown>> };
}

export function generateLocalKeys(): LocalKeys {
  const { privateKey, publicKey } = generateKeyPairSync("rsa", {
    modulusLength: 2048,
  });
  const kid = randomUUID();
  const jwk = publicKey.export({ format: "jwk" }) as Record<string, unknown>;
  return {
    kid,
    privateKeyPem: privateKey.export({
      type: "pkcs8",
      format: "pem",
    }) as string,
    jwks: { keys: [{ ...jwk, kid, alg: "RS256", use: "sig" }] },
  };
}

export function jwksFromPrivateKey(
  privateKeyPem: string,
  kid: string,
): LocalKeys["jwks"] {
  const jwk = createPublicKey(createPrivateKey(privateKeyPem)).export({
    format: "jwk",
  }) as Record<string, unknown>;
  return { keys: [{ ...jwk, kid, alg: "RS256", use: "sig" }] };
}

const b64url = (b: Buffer | string) => Buffer.from(b).toString("base64url");

export interface TokenInput {
  issuer: string;
  audience: string;
  orgId: string;
  workspaceId: string;
  userId: string;
  scopes?: string[];
  ttlSeconds?: number;
  /** Signed as `meta.machineId`: what binds this token to one machine. */
  machineId?: string;
}

/** An RS256 JWT with the claims the vendored `extractAuthContext` reads. */
export function mintToken(
  keys: Pick<LocalKeys, "kid" | "privateKeyPem">,
  input: TokenInput,
  now = Math.floor(Date.now() / 1000),
): string {
  const header = { alg: "RS256", typ: "JWT", kid: keys.kid };
  const claims = {
    iss: input.issuer,
    aud: input.audience,
    sub: `user_${input.userId}`,
    user_id: input.userId,
    org_id: input.orgId,
    workspace_id: input.workspaceId,
    scope: (input.scopes ?? ["rth:sync", "rth:read"]).join(" "),
    iat: now,
    nbf: now - 30,
    exp: now + (input.ttlSeconds ?? 24 * 3600),
    jti: randomUUID(),
    ...(input.machineId ? { meta: { machineId: input.machineId } } : {}),
  };
  const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(claims))}`;
  const signature = createSign("RSA-SHA256")
    .update(signingInput)
    .sign(createPrivateKey(keys.privateKeyPem));
  return `${signingInput}.${b64url(signature)}`;
}
