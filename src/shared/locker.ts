/**
 * The Locker client every service uses.
 *
 * LOCKER_PROVIDER selects the adapter. Unset is S3:
 *   - static keys when LOCKER_ACCESS_KEY_ID/LOCKER_SECRET_ACCESS_KEY are set (local MinIO);
 *   - otherwise EKS Pod Identity: the agent injects AWS_CONTAINER_CREDENTIALS_FULL_URI and
 *     AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE; credentials are short-lived and refreshed
 *     before expiry by the S3 driver's cached provider. No key is stored anywhere.
 * LOCKER_ENDPOINT is optional for S3: unset means AWS S3 in LOCKER_REGION.
 *
 * LOCKER_PROVIDER=gcs uses the GCS adapter and a GKE metadata-server token.
 * HMAC keys are refused. Production AWS S3 does not set LOCKER_PROVIDER.
 */
import { readFile } from "node:fs/promises";
import { createGcsStore, createS3Store, objectStoreManifestReader, type ManifestReader, type RawStore } from "./vendor.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import type { GcsToken } from "../../packages/object-store/src/gcs.ts";
import { optional, required } from "./env.ts";
import { assertLockerSelection, gcsTokenUri, lockerProvider, trustedGcsTokenUri } from "./locker-selection.ts";

export type CredentialSource = "static" | "pod-identity";

export function lockerSettings() {
  return {
    endpoint: process.env.LOCKER_ENDPOINT || undefined,
    bucket: required("LOCKER_BUCKET"),
    region: optional("LOCKER_REGION", "us-east-1"),
  };
}

export function credentialSource(env: NodeJS.ProcessEnv = process.env): CredentialSource {
  if (env.LOCKER_ACCESS_KEY_ID && env.LOCKER_SECRET_ACCESS_KEY) return "static";
  if (env.AWS_CONTAINER_CREDENTIALS_FULL_URI) return "pod-identity";
  throw new Error("96bulls: no Locker credentials: set LOCKER_ACCESS_KEY_ID/LOCKER_SECRET_ACCESS_KEY, or run with EKS Pod Identity");
}

interface ContainerCredentials {
  AccessKeyId: string;
  SecretAccessKey: string;
  Token?: string;
  Expiration?: string;
}

/**
 * The only hosts the credentials token may be sent to: the EKS Pod Identity agent
 * (169.254.170.23, fd00:ec2::23), the ECS endpoint (169.254.170.2) and loopback.
 * Mirrors the AWS SDKs' allowlist, so a misconfigured URI can't capture the token.
 */
const TRUSTED_CREDENTIAL_HOSTS = new Set(["169.254.170.23", "[fd00:ec2::23]", "169.254.170.2", "127.0.0.1", "localhost", "[::1]"]);

export function trustedCredentialsUri(uri: string): boolean {
  try {
    const u = new URL(uri);
    if (u.protocol === "https:") return true;
    return u.protocol === "http:" && TRUSTED_CREDENTIAL_HOSTS.has(u.hostname.startsWith("[") ? u.hostname : u.hostname);
  } catch {
    return false;
  }
}

/** EKS Pod Identity / container credentials endpoint (the same contract the AWS SDKs use). */
export async function podIdentityCredentials(env: NodeJS.ProcessEnv = process.env, fetcher: typeof fetch = fetch): Promise<AwsCredentials> {
  const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
  if (!uri) throw new Error("96bulls: AWS_CONTAINER_CREDENTIALS_FULL_URI is not set");
  if (!trustedCredentialsUri(uri)) throw new Error("96bulls: AWS_CONTAINER_CREDENTIALS_FULL_URI is not a trusted credentials endpoint; refusing to send the token");
  const tokenFile = env.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE;
  const token = tokenFile ? (await readFile(tokenFile, "utf8")).trim() : env.AWS_CONTAINER_AUTHORIZATION_TOKEN;
  const res = await fetcher(uri, { headers: token ? { authorization: token } : {} });
  if (!res.ok) throw Object.assign(new Error(`96bulls: pod identity credentials: http ${res.status}`), { code: "credentials_unavailable", retryable: true });
  const c = (await res.json()) as ContainerCredentials;
  return {
    accessKeyId: c.AccessKeyId,
    secretAccessKey: c.SecretAccessKey,
    ...(c.Token ? { sessionToken: c.Token } : {}),
    ...(c.Expiration ? { expiresAt: new Date(c.Expiration) } : {}),
  };
}

export function lockerCredentials(env: NodeJS.ProcessEnv = process.env): AwsCredentials | (() => Promise<AwsCredentials>) {
  return credentialSource(env) === "static"
    ? { accessKeyId: env.LOCKER_ACCESS_KEY_ID!, secretAccessKey: env.LOCKER_SECRET_ACCESS_KEY! }
    : () => podIdentityCredentials(env);
}

interface GcsMetadataToken {
  access_token?: string;
  expires_in?: number;
}

/** GKE Workload Identity token. The access token is not logged. */
export async function gcsMetadataToken(env: NodeJS.ProcessEnv = process.env, fetcher: typeof fetch = fetch): Promise<GcsToken> {
  const uri = gcsTokenUri(env);
  if (!trustedGcsTokenUri(uri)) {
    throw new Error("96bulls: LOCKER_GCS_TOKEN_URI is not the GKE metadata server or loopback");
  }
  const res = await fetcher(uri, { headers: { "Metadata-Flavor": "Google" } });
  if (!res.ok) throw Object.assign(new Error(`96bulls: GKE metadata token: http ${res.status}`), { code: "credentials_unavailable", retryable: true });
  const body = (await res.json()) as GcsMetadataToken;
  if (!body.access_token) throw new Error("96bulls: GKE metadata server returned no access_token");
  const expiresAt = typeof body.expires_in === "number" ? new Date(Date.now() + body.expires_in * 1000) : undefined;
  return { accessToken: body.access_token, ...(expiresAt ? { expiresAt } : {}) };
}

/** Build the store described by env. Does not cache and does not read process.env unless env is omitted. */
export function buildLockerStore(env: NodeJS.ProcessEnv = process.env): RawStore {
  assertLockerSelection(env);
  const provider = lockerProvider(env);
  const bucket = env.LOCKER_BUCKET;
  if (!bucket) throw new Error("96bulls: missing required env LOCKER_BUCKET");
  if (provider === "gcs") {
    return {
      provider: "gcs",
      bucket,
      store: createGcsStore({
        bucket,
        ...(env.LOCKER_ENDPOINT ? { baseUrl: env.LOCKER_ENDPOINT } : {}),
        token: () => gcsMetadataToken(env),
      }),
    };
  }
  return {
    provider: "s3",
    bucket,
    store: createS3Store({
      bucket,
      region: env.LOCKER_REGION || "us-east-1",
      ...(env.LOCKER_ENDPOINT ? { endpoint: env.LOCKER_ENDPOINT } : {}),
      credentials: lockerCredentials(env),
    }),
  };
}

let cached: RawStore | undefined;

/** The one Locker store for this process. */
export function lockerStore(): RawStore {
  if (cached) return cached;
  cached = buildLockerStore();
  return cached;
}

export function lockerManifestReader(): ManifestReader {
  const raw = lockerStore();
  return objectStoreManifestReader(raw.store, { provider: raw.provider, bucket: raw.bucket });
}
