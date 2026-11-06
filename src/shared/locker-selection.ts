/**
 * Provider selection for the Locker. Unset means S3, which is local MinIO,
 * production AWS S3, and the EKS self-host proof. GCS is selected only by
 * LOCKER_PROVIDER=gcs and uses Workload Identity, never HMAC keys.
 *
 * rawStoreFromEnv follows the same choice: unset and s3 stay S3, and gcs builds
 * the GCS adapter without HMAC keys.
 */

export type LockerProvider = "s3" | "gcs";

const DEFAULT_GCS_TOKEN_URI =
  "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token";

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);
const TRUSTED_GCS_TOKEN_HOSTS = new Set(["169.254.169.254", ...LOOPBACK_HOSTS]);

export function lockerProvider(env: NodeJS.ProcessEnv = process.env): LockerProvider {
  const raw = env.LOCKER_PROVIDER?.trim() || "s3";
  if (raw !== "s3" && raw !== "gcs") {
    throw new Error("96bulls: LOCKER_PROVIDER must be s3 or gcs");
  }
  return raw;
}

export function gcsTokenUri(env: NodeJS.ProcessEnv = process.env): string {
  return env.LOCKER_GCS_TOKEN_URI?.trim() || DEFAULT_GCS_TOKEN_URI;
}

/** Metadata server and loopback only. The token request must not leave the node. */
export function trustedGcsTokenUri(uri: string): boolean {
  try {
    const u = new URL(uri);
    return (
      u.protocol === "http:" &&
      !u.username &&
      !u.password &&
      TRUSTED_GCS_TOKEN_HOSTS.has(u.hostname)
    );
  } catch {
    return false;
  }
}

/** Never send a metadata-derived bearer token to an operator-chosen remote host. */
export function trustedGcsEndpoint(uri: string): boolean {
  try {
    const u = new URL(uri);
    if (u.username || u.password || u.search || u.hash) return false;
    if (LOOPBACK_HOSTS.has(u.hostname))
      return u.protocol === "http:" || u.protocol === "https:";
    return (
      u.protocol === "https:" &&
      u.hostname === "storage.googleapis.com" &&
      !u.port &&
      (u.pathname === "" || u.pathname === "/")
    );
  } catch {
    return false;
  }
}

function endpointHost(endpoint: string | undefined): string | undefined {
  if (!endpoint) return undefined;
  try {
    return new URL(endpoint).hostname.toLowerCase();
  } catch {
    return undefined;
  }
}

function isGcsApiHost(host: string): boolean {
  return host === "storage.googleapis.com" || host.endsWith(".storage.googleapis.com") || host === "storage.google.com";
}

/** Refuse a selection that would disguise GCS as S3 or put HMAC keys on GCS. */
export function assertLockerSelection(env: NodeJS.ProcessEnv = process.env): void {
  const provider = lockerProvider(env);
  const host = endpointHost(env.LOCKER_ENDPOINT);
  if (provider === "s3" && host && isGcsApiHost(host)) {
    throw new Error(
      "96bulls: refusing to reach GCS with the S3 client. Set LOCKER_PROVIDER=gcs. HMAC-as-S3 is not the adapter.",
    );
  }
  if (provider === "gcs" && (env.LOCKER_ACCESS_KEY_ID || env.LOCKER_SECRET_ACCESS_KEY)) {
    throw new Error(
      "96bulls: GCS Locker uses Workload Identity, not HMAC keys. Unset LOCKER_ACCESS_KEY_ID and LOCKER_SECRET_ACCESS_KEY.",
    );
  }
  if (provider === "gcs" && !env.LOCKER_BUCKET?.trim()) {
    throw new Error("96bulls: LOCKER_PROVIDER=gcs requires LOCKER_BUCKET");
  }
  if (provider === "gcs" && env.LOCKER_ENDPOINT) {
    if (!trustedGcsEndpoint(env.LOCKER_ENDPOINT))
      throw new Error(
        "96bulls: GCS LOCKER_ENDPOINT must be the official HTTPS API or loopback emulator",
      );
  }
  if (provider === "gcs" && env.LOCKER_GCS_TOKEN_URI && !trustedGcsTokenUri(env.LOCKER_GCS_TOKEN_URI)) {
    throw new Error("96bulls: LOCKER_GCS_TOKEN_URI is not the GKE metadata server or loopback");
  }
}
