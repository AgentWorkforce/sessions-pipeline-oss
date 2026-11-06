/**
 * Conformance readiness for the GCP data path.
 *
 * Unset stays out of the way: production S3 and local MinIO do not set
 * LOCKER_PROVIDER or WORKER_DATA_PATH_PROBE, and their /readyz stays the
 * HoopDb verdict. A GCS workload must show the bucket, the Cloud SQL
 * connection name, Redis, and Pub/Sub, and must not carry HMAC keys or a
 * database URL. The verdict names settings only. It does not echo values.
 */

import { trustedGcsEndpoint } from "./locker-selection.ts";
import { redisTlsPing, type RedisTlsEnv, type RedisTlsVerdict } from "./redis-tls.ts";

export interface DataPathEnv extends RedisTlsEnv {
  LOCKER_PROVIDER?: string;
  LOCKER_BUCKET?: string;
  LOCKER_ACCESS_KEY_ID?: string;
  LOCKER_SECRET_ACCESS_KEY?: string;
  CLOUD_SQL_CONNECTION_NAME?: string;
  PUBSUB_TOPIC?: string;
  PUBSUB_SUBSCRIPTION?: string;
  LOCKER_ENDPOINT?: string;
  PIPELINE_RAW_PROVIDER?: string;
  PIPELINE_RAW_S3_ENDPOINT?: string;
  PIPELINE_RAW_S3_ACCESS_KEY_ID?: string;
  PIPELINE_RAW_S3_SECRET_ACCESS_KEY?: string;
  WORKER_DATA_PATH_PROBE?: string;
}

export function dataPathProbeEnabled(env: DataPathEnv): boolean {
  return env.WORKER_DATA_PATH_PROBE === "1" || env.LOCKER_PROVIDER?.trim() === "gcs";
}

export type DataPathVerdict = { ok: true; armed: boolean } | { ok: false; armed: true; missing: string[] };

function blank(value: string | undefined): boolean {
  return !value?.trim();
}

export function dataPathVerdict(env: DataPathEnv): DataPathVerdict {
  if (!dataPathProbeEnabled(env)) return { ok: true, armed: false };
  const missing: string[] = [];
  if (env.LOCKER_PROVIDER?.trim() !== "gcs") missing.push("LOCKER_PROVIDER");
  if (blank(env.LOCKER_BUCKET)) missing.push("LOCKER_BUCKET");
  if (env.LOCKER_ACCESS_KEY_ID || env.LOCKER_SECRET_ACCESS_KEY) missing.push("LOCKER_HMAC");
  const pipelineProvider = env.PIPELINE_RAW_PROVIDER?.trim();
  if (pipelineProvider !== "gcs") missing.push("PIPELINE_RAW_PROVIDER");
  if (env.PIPELINE_RAW_S3_ACCESS_KEY_ID || env.PIPELINE_RAW_S3_SECRET_ACCESS_KEY) missing.push("PIPELINE_HMAC");
  if (!blank(env.LOCKER_ENDPOINT) && !trustedGcsEndpoint(env.LOCKER_ENDPOINT!)) missing.push("LOCKER_ENDPOINT");
  if (!blank(env.PIPELINE_RAW_S3_ENDPOINT) && !trustedGcsEndpoint(env.PIPELINE_RAW_S3_ENDPOINT!))
    missing.push("PIPELINE_RAW_S3_ENDPOINT");
  const connection = env.CLOUD_SQL_CONNECTION_NAME?.trim() ?? "";
  if (!connection || connection.includes("://") || connection.includes("@") || connection.includes(" "))
    missing.push("CLOUD_SQL_CONNECTION_NAME");
  if (blank(env.REDIS_HOST) || env.REDIS_HOST!.includes("://") || env.REDIS_HOST!.includes("@")) missing.push("REDIS_HOST");
  if (!/^[0-9]+$/.test(env.REDIS_PORT?.trim() ?? "")) missing.push("REDIS_PORT");
  if (env.REDIS_AUTH_ENABLED !== "false") missing.push("REDIS_AUTH_ENABLED");
  if (env.REDIS_TRANSIT_ENCRYPTION !== "SERVER_AUTHENTICATION") missing.push("REDIS_TRANSIT_ENCRYPTION");
  if (blank(env.REDIS_TLS_CA_PEM) || env.REDIS_TLS_CA_PEM!.includes("PRIVATE KEY")) missing.push("REDIS_TLS_CA_PEM");
  if (env.REDIS_AUTH || env.REDIS_PASSWORD) missing.push("REDIS_SECRET");
  if (blank(env.PUBSUB_TOPIC)) missing.push("PUBSUB_TOPIC");
  if (blank(env.PUBSUB_SUBSCRIPTION)) missing.push("PUBSUB_SUBSCRIPTION");
  return missing.length ? { ok: false, armed: true, missing } : { ok: true, armed: true };
}

/** HoopDb readiness plus the data-path verdict when that probe is armed. */
export function workloadReady(dbReady: boolean, env: DataPathEnv): { ok: true } | { ok: false; missing?: string[] } {
  const data = dataPathVerdict(env);
  if (!dbReady) return data.ok ? { ok: false } : { ok: false, missing: data.missing };
  if (!data.ok) return { ok: false, missing: data.missing };
  return { ok: true };
}

export type LiveDataPathVerdict = DataPathVerdict | { ok: false; armed: true; missing: ["REDIS_TLS_ROUND_TRIP"] };

/** Static boundary checks plus a real CA-validated Redis PING when armed. */
export async function liveDataPathVerdict(
  env: DataPathEnv,
  ping: (env: RedisTlsEnv) => Promise<RedisTlsVerdict> = redisTlsPing,
): Promise<LiveDataPathVerdict> {
  const data = dataPathVerdict(env);
  if (!data.ok || !data.armed) return data;
  const redis = await ping(env);
  return redis.ok ? data : { ok: false, armed: true, missing: ["REDIS_TLS_ROUND_TRIP"] };
}

/** HoopDb readiness plus the live data-path verdict when the GCP probe is armed. */
export async function workloadReadyLive(
  dbReady: boolean,
  env: DataPathEnv,
  ping?: (env: RedisTlsEnv) => Promise<RedisTlsVerdict>,
): Promise<{ ok: true } | { ok: false; missing?: string[] }> {
  const data = await liveDataPathVerdict(env, ping);
  if (!dbReady) return data.ok ? { ok: false } : { ok: false, missing: data.missing };
  if (!data.ok) return { ok: false, missing: data.missing };
  return { ok: true };
}
