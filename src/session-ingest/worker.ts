/**
 * Cloudflare Worker entry for the session ingest (no Lambda on the request
 * path). stacks/cloudflare/session-ingest.ts, docs/session-ingest-design.md.
 *
 *   relayhistory Worker (history.example.invalid, per-org DELIVERY_FORWARD)
 *     --service binding DELIVERY_INGEST--> fetch(): POST /v1/delivery/batches
 *         -> Supabase (Hyperdrive, role bulls96_session_ingest): claim / slot / commit
 *         -> S3 (srv-writer key, SigV4, SSE-KMS): chunks, manifests, pointers or a staged batch
 *         -> NEON_REPLICA queue (optional, transition only)
 *   INGEST_FANOUT queue --> queue(): staged batches written session by session
 *
 * Bindings and settings (the stack declares them; secrets are set out of band):
 *   HYPERDRIVE                 Supabase transaction pooler as bulls96_session_ingest
 *   INGEST_FANOUT              producer + consumer (this Worker)
 *   NEON_REPLICA               optional producer: relayhistory's replica queue
 *   S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY   Secrets Store bindings (`await .get()`): the transcript
 *                              srv-writer user, set out of band from 1Password
 *   INGEST_SIGNING_KEY         Secrets Store: the HMAC key relayhistory signs each request's
 *                              identity with (shared with relayhistory's DELIVERY_INGEST_SIGNING_KEY)
 *   INGEST_S3_BUCKET, INGEST_S3_REGION, INGEST_KEY_ROOT, INGEST_KMS_KEY_ARN
 *   INGEST_MAX_PER_USER / INGEST_MAX_PER_ORG   (defaults 4 / 32)
 *   INGEST_INLINE_MAX_SESSIONS (default 40)
 *   INGEST_REPLICA_MODE        off | queue (= required) | best_effort; required needs NEON_REPLICA
 *
 * Until the S3 key, the KMS key ARN and Hyperdrive are present, every request
 * is 503 `not_configured` and nothing is written.
 */
import pg from "pg";
import type { IngestAcceptanceMode } from "./buffered-protocol.ts";
import { zstdDecompressSync } from "node:zlib";
import { createS3Store, objectKey, type ObjectStore } from "../shared/vendor.ts";
import { log } from "../shared/log.ts";
import { createIngestApp } from "./app.ts";
import { sqlLedger } from "./ledger-sql.ts";
import type { ReplicaMode, ReplicaSink, NeonReplicaMessage } from "./replica.ts";
import { fanout, type FanoutMessage } from "./staging.ts";
import type { WriteTarget } from "./writer.ts";

interface QueueLike<T> {
  send(body: T): Promise<void>;
  sendBatch(messages: { body: T }[]): Promise<void>;
}

export interface IngestWorkerEnv {
  HYPERDRIVE?: { connectionString: string };
  INGEST_FANOUT?: QueueLike<FanoutMessage>;
  NEON_REPLICA?: QueueLike<NeonReplicaMessage>;
  /** Secrets Store bindings (a plain string in tests). */
  S3_ACCESS_KEY_ID?: SecretBinding | string;
  S3_SECRET_ACCESS_KEY?: SecretBinding | string;
  /** Secrets Store: the HMAC key relayhistory signs the identity with (app.ts signRequest). */
  INGEST_SIGNING_KEY?: SecretBinding | string;
  INGEST_S3_BUCKET?: string;
  INGEST_S3_REGION?: string;
  INGEST_KEY_ROOT?: string;
  INGEST_KMS_KEY_ARN?: string;
  INGEST_MAX_PER_USER?: string;
  INGEST_MAX_PER_ORG?: string;
  INGEST_INLINE_MAX_SESSIONS?: string;
  INGEST_REPLICA_MODE?: string;
  INGEST_ACCEPTANCE_MODE?: string;
  INGEST_EXPECT_GENERATION?:string;
  /** P1: relayhistory (default) | shadow | frozen-supabase (sessions.ingest_frozen_source_witness). */
  INGEST_CUSTODY_SOURCE?: string;
}

interface SecretBinding {
  get(): Promise<string>;
}

async function secret(value: SecretBinding | string | undefined): Promise<string | undefined> {
  if (value === undefined || typeof value === "string") return value || undefined;
  try {
    return (await value.get()) || undefined;
  } catch {
    // An unset Secrets Store secret: not configured yet.
    return undefined;
  }
}

interface Ctx {
  waitUntil(work: Promise<unknown>): void;
}

function limit(raw: string | undefined, fallback: number, name: string): number {
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1 || n > 1000) throw new Error(`session ingest: ${name} must be an integer from 1 to 1000`);
  return n;
}

export interface IngestSettings {
  bucket: string;
  region: string;
  keyRoot: string;
  kmsKeyArn: string;
  accessKeyId: string;
  secretAccessKey: string;
  signingKey: string;
  maxPerUser: number;
  maxPerOrg: number;
  inlineMaxSessions: number;
  replicaMode: ReplicaMode;
  acceptanceMode: IngestAcceptanceMode;
  expectedGeneration?:string;
}

/** The settings, or null while any required one is missing (every request is then 503 not_configured). */
export async function ingestSettings(env: IngestWorkerEnv): Promise<IngestSettings | null> {
  const { INGEST_S3_BUCKET: bucket, INGEST_KMS_KEY_ARN: kmsKeyArn } = env;
  if (!bucket || !kmsKeyArn || !env.HYPERDRIVE || !env.INGEST_FANOUT) return null;
  const [accessKeyId, secretAccessKey, signingKey] = await Promise.all([
    secret(env.S3_ACCESS_KEY_ID),
    secret(env.S3_SECRET_ACCESS_KEY),
    secret(env.INGEST_SIGNING_KEY),
  ]);
  if (!accessKeyId || !secretAccessKey || !signingKey) return null;
  const acceptanceMode = env.INGEST_ACCEPTANCE_MODE ?? "canonical";
  if (acceptanceMode !== "canonical" && acceptanceMode !== "buffered") throw new Error("session ingest: acceptance mode invalid");
  if(acceptanceMode==="buffered"&&(!/^[a-f0-9]{64}$/.test(env.INGEST_EXPECT_GENERATION??"")||env.INGEST_KEY_ROOT&&env.INGEST_KEY_ROOT!=="96bulls"))throw new Error("session ingest: buffered generation or prefix invalid");
  const raw = env.INGEST_REPLICA_MODE ?? "off";
  if (!["off", "queue", "required", "best_effort"].includes(raw)) throw new Error("session ingest: INGEST_REPLICA_MODE must be off, queue, required or best_effort");
  const mode: ReplicaMode = raw === "queue" ? "required" : (raw as ReplicaMode);
  if (acceptanceMode === "buffered" && (mode !== "off" || env.INGEST_INLINE_MAX_SESSIONS !== "0")) throw new Error("session ingest: buffered requires inline0 and replicaoff");
  // A replica that must be sent but has no queue would refuse every batch: not configured.
  if (mode !== "off" && !env.NEON_REPLICA) return null;
  return {
    bucket,
    region: env.INGEST_S3_REGION ?? "us-east-1",
    keyRoot: env.INGEST_KEY_ROOT ?? "96bulls",
    kmsKeyArn,
    accessKeyId,
    secretAccessKey,
    signingKey,
    maxPerUser: limit(env.INGEST_MAX_PER_USER, 4, "INGEST_MAX_PER_USER"),
    maxPerOrg: limit(env.INGEST_MAX_PER_ORG, 32, "INGEST_MAX_PER_ORG"),
    inlineMaxSessions: acceptanceMode === "buffered" ? 0 : limit(env.INGEST_INLINE_MAX_SESSIONS, 40, "INGEST_INLINE_MAX_SESSIONS"),
    acceptanceMode,
    ...(acceptanceMode==="buffered"?{expectedGeneration:env.INGEST_EXPECT_GENERATION!}:{}),
    replicaMode: mode,
  };
}

function store(s: IngestSettings): ObjectStore {
  return createS3Store({
    bucket: s.bucket,
    region: s.region,
    credentials: { accessKeyId: s.accessKeyId, secretAccessKey: s.secretAccessKey },
    serverSideEncryption: "aws:kms",
    sseKmsKeyId: s.kmsKeyArn,
  });
}

const NOT_CONFIGURED = () =>
  Response.json({ error: { code: "not_configured", message: "the session ingest is not configured yet" }, correlationId: "" }, { status: 503, headers: { "retry-after": "60" } });

const zstd = (bytes: Uint8Array) => new Uint8Array(zstdDecompressSync(bytes));

export default {
  async fetch(request: Request, env: IngestWorkerEnv, ctx: Ctx): Promise<Response> {
    let settings: IngestSettings | null;
    try {
      settings = await ingestSettings(env);
    } catch {
      // A malformed setting: relayhistory's envelope, retryable, never a bare 500.
      log.error("session ingest settings invalid", { error_code: "settings_invalid" });
      return NOT_CONFIGURED();
    }
    if (!settings) return NOT_CONFIGURED();
    // One Hyperdrive connection per request; Hyperdrive pools the real ones.
    const client = new pg.Client({ connectionString: env.HYPERDRIVE!.connectionString });
    // A dropped socket surfaces on the next query as a coded 503, never as an uncaught error.
    client.on("error", (err: Error) => log.error("session ingest db error", { error_code: (err as { code?: string }).code ?? "db_error" }));
    let connected: Promise<void> | undefined;
    const run = async (text: string, params: unknown[]) => {
      connected ??= client.connect();
      await connected;
      return (await client.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
    };
    const target: WriteTarget = { store: store(settings), keyRoot: settings.keyRoot };
    const replica: ReplicaSink | undefined = settings.replicaMode !== "off" && env.NEON_REPLICA
      ? {
          mode: settings.replicaMode,
          send: (message) => env.NEON_REPLICA!.send(message),
          putDescriptor: async (key, body) => {
            await target.store.put(objectKey(key), body, { ifNoneMatch: true, contentType: "application/json" }).catch((error: { code?: string }) => {
              // Deterministic body: an existing descriptor is this batch's own earlier attempt.
              if (error?.code !== "already_exists" && error?.code !== "precondition_failed") throw error;
            });
          },
        }
      : undefined;
    const custodySource = env.INGEST_CUSTODY_SOURCE?.trim() || "relayhistory";
    if (!["relayhistory", "shadow", "frozen-supabase"].includes(custodySource) || (custodySource !== "relayhistory" && (env.INGEST_ACCEPTANCE_MODE ?? "canonical") !== "buffered"))
      throw new Error("session ingest: INGEST_CUSTODY_SOURCE must be relayhistory, shadow or frozen-supabase (the latter two only in buffered mode)");
    const app = createIngestApp({
      custodySource: custodySource as "relayhistory" | "shadow" | "frozen-supabase",
      signingKey: settings.signingKey,
      deliver: {
        ledger: sqlLedger(run, {acceptanceMode:settings.acceptanceMode,expectedGeneration:settings.expectedGeneration}),
        acceptanceMode:settings.acceptanceMode,
        target,
        replica,
        // Slot TTL: past the Worker's wall-clock budget, so a lost release frees itself.
        slots: { maxPerUser: settings.maxPerUser, maxPerOrg: settings.maxPerOrg, ttlSeconds: 90 },
        inlineMaxSessions: settings.inlineMaxSessions,
        fanout: (messages) => env.INGEST_FANOUT!.sendBatch(messages.map((body) => ({ body }))),
      },
      onboarding: { run },
    });
    try {
      return await app.fetch(request);
    } finally {
      if (connected) ctx.waitUntil(client.end().catch(() => {}));
    }
  },

  /**
   * INGEST_FANOUT: one message, one slice of a staged batch. A failure retries it
   * (then the DLQ: runbooks/session-ingest-rollout.md "Fan-out DLQ redrive"). While
   * the Worker is not configured (a missing or rotated secret) a message waits five
   * minutes per attempt instead of walking to the DLQ in seconds.
   */
  async queue(batch: { messages: { body: unknown; ack(): void; retry(o?: { delaySeconds?: number }): void }[] }, env: IngestWorkerEnv): Promise<void> {
    const settings = await ingestSettings(env).catch(() => null);
    if (settings?.acceptanceMode === "buffered") {
      for (const message of batch.messages) message.retry({delaySeconds:300});
      log.info("session ingest fanout refused", {reason:"buffered_publication_refused"});
      return;
    }
    const client = settings && env.HYPERDRIVE ? new pg.Client({ connectionString: env.HYPERDRIVE.connectionString }) : null;
    client?.on("error", (err: Error) => log.error("session ingest db error", { error_code: (err as { code?: string }).code ?? "db_error" }));
    let connected: Promise<void> | undefined;
    const ledger = client
      ? sqlLedger(async (text, params) => {
          connected ??= client.connect();
          await connected;
          return (await client.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
        })
      : null;
    try {
      for (const message of batch.messages) {
        const body = message.body as FanoutMessage;
        if (!settings || !ledger || body?.kind !== "ingest-fanout") {
          log.error("session ingest fanout refused", { reason: settings ? "unknown_message" : "not_configured" });
          message.retry({ delaySeconds: 300 });
          continue;
        }
        try {
          const written = await fanout({ store: store(settings), keyRoot: settings.keyRoot }, body, zstd, ledger);
          log.info("session ingest fanout", {
            outcome: "ok",
            manifests: written.manifests.filter((m) => m.written).length,
            skipped: written.skipped,
            inserted: written.revisions.committed,
            count: body.to - body.from,
          });
          message.ack();
        } catch (error) {
          log.error("session ingest fanout failed", { error_code: (error as { code?: string }).code ?? "fanout_failed" });
          message.retry({ delaySeconds: 30 });
        }
      }
    } finally {
      if (connected) await client!.end().catch(() => {});
    }
  },
};
