/**
 * The storage-notification adapter: "a manifest landed in object storage" →
 * one object-landed StageMessage on the pipeline queue.
 *
 * Three inputs share one builder (`buildObjectLandedMessage`):
 *   - the manifest POST (`POST /v1/pipeline/manifests`): the delivery
 *     coordinator reports the manifest it just wrote. Authenticated, so the
 *     org and workspace come from the token. Needs no bucket event source.
 *   - Cloudflare R2 event notifications, consumed from
 *     `relayhistory-object-events`. Other native sources (S3 via
 *     SQS/EventBridge, GCS Pub/Sub) plug in as more `StorageEventSource`s.
 *   - the reconcile job (`reconcile.ts`), for stores with no event source
 *     (self-host MinIO): it lists manifests and enqueues the recent ones.
 *
 * All three produce the same work key (`manifestWorkKey`, with the ETag
 * normalized), so a manifest reported twice is one unit of work and the
 * repeat is idempotent.
 *
 * Chunk objects are ignored here: only a manifest makes a session readable.
 * An unscrubbed manifest (no `raw.scrub`) is still enqueued, with
 * `scrubbed: false`, so its pointer is indexed and quarantined.
 *
 * Logging: only codes, stage names and ids. Never a manifest, an object body,
 * or an object-store error message (those can quote manifest values).
 */
import type { Bindings } from "../../env.js";
import type { StageMessage } from "./dlq.js";
import { PipelineError } from "./errors.js";
import {
  ObjectStoreError,
  createGcsStore,
  createS3Store,
  objectKey,
  parseManifest,
  parseSessionKey,
  isManifestRest,
  isPublicationManifestRest,
  parseUploadGenerationPrefix,
  pointerFields,
  type ManifestV1,
  type ObjectStore,
} from "./object-layout.js";
import {
  manifestWorkKey,
  type ObjectLandedPayload,
  type ObjectStoreProvider,
} from "./stages.js";
import { isTraceId, uuidv7, type PipelineStage } from "./trace.js";

/** The queued stage a landed manifest is sent to. */
export const OBJECT_LANDED_STAGE: PipelineStage = "object-landed";

export type StorageProvider = ObjectStoreProvider;

/** A put to object storage, normalized from whatever reported it. */
export interface ObjectLanded {
  provider?: StorageProvider;
  bucket?: string;
  key: string;
  etag?: string;
  /** Backend object version (e.g. GCS generation), when the backend orders by it. */
  version?: string;
  size?: number;
  eventTime?: string;
  /** The object was deleted; there is nothing to read. */
  deleted?: boolean;
}

/** Turns one provider notification into an `ObjectLanded`, or null to ignore it. */
export interface StorageEventSource {
  readonly provider: StorageProvider;
  normalize(body: unknown): ObjectLanded | null;
}

/** `"abc"`, `W/"abc"` and `abc` are the same ETag; keys must not depend on quoting. */
export function normalizeEtag(etag: string | undefined | null) {
  if (!etag) return undefined;
  const bare = etag.trim().replace(/^W\//, "").replace(/^"|"$/g, "");
  return bare || undefined;
}

const R2_PUT_ACTIONS = new Set([
  "PutObject",
  "CopyObject",
  "CompleteMultipartUpload",
]);
const R2_DELETE_ACTIONS = new Set(["DeleteObject", "LifecycleDeletion"]);

/**
 * Cloudflare R2 event notifications:
 * `{ account, action, bucket, object: { key, size, eTag }, eventTime }`.
 * Puts and deletes (including lifecycle deletion); anything else is ignored.
 */
export const r2EventSource: StorageEventSource = {
  provider: "r2",
  normalize(body) {
    const event = body as {
      action?: unknown;
      bucket?: unknown;
      eventTime?: unknown;
      object?: { key?: unknown; size?: unknown; eTag?: unknown };
    } | null;
    if (!event || typeof event !== "object") return null;
    if (typeof event.action !== "string") return null;
    const deleted = R2_DELETE_ACTIONS.has(event.action);
    if (!deleted && !R2_PUT_ACTIONS.has(event.action)) return null;
    const key = event.object?.key;
    if (typeof key !== "string" || !key) return null;
    const etag = normalizeEtag(
      typeof event.object?.eTag === "string" ? event.object.eTag : undefined,
    );
    return {
      provider: "r2",
      key,
      ...(typeof event.bucket === "string" ? { bucket: event.bucket } : {}),
      ...(etag ? { etag } : {}),
      ...(typeof event.object?.size === "number"
        ? { size: event.object.size }
        : {}),
      ...(typeof event.eventTime === "string"
        ? { eventTime: event.eventTime }
        : {}),
      ...(deleted ? { deleted: true } : {}),
    };
  },
};

export interface ManifestRead {
  manifest: ManifestV1;
  etag?: string;
  version?: string;
}

/**
 * Reads and validates a manifest from storage; null when it does not exist
 * (yet). Throws `ObjectStoreError("invalid_argument")` for a malformed one.
 */
export interface ManifestReader {
  readonly provider: StorageProvider;
  readonly bucket?: string;
  read(key: string): Promise<ManifestRead | null>;
}

/** The largest manifest read; a v1 manifest with 100k chunks is ~40 MB. */
const MAX_MANIFEST_BYTES = 64 * 1024 * 1024;

/**
 * Every byte of `stream`, or null once more than `limit` arrive (the stream is
 * cancelled there). Never trusts a declared length, which may be absent.
 */
export async function readCapped(
  stream: ReadableStream<Uint8Array> | null,
  limit: number,
): Promise<Uint8Array | null> {
  if (!stream) return new Uint8Array();
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    size += next.value.byteLength;
    if (size > limit) {
      await reader.cancel();
      return null;
    }
    chunks.push(next.value);
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

/** A manifest reader over any object-store driver (S3, R2's S3 API, MinIO, GCS, Azure, fs). */
export function objectStoreManifestReader(
  store: ObjectStore,
  options: { provider: StorageProvider; bucket?: string; maxBytes?: number },
): ManifestReader {
  return {
    provider: options.provider,
    ...(options.bucket ? { bucket: options.bucket } : {}),
    async read(key) {
      let object;
      try {
        object = await store.getObject(objectKey(key));
      } catch (error) {
        if (error instanceof ObjectStoreError && error.code === "not_found")
          return null;
        throw error;
      }
      // The version comes from the same response as the body (see
      // `ObjectStore.getObject`), so the work key names what was read. The
      // cap is enforced on the bytes read: a store may omit the length.
      const bytes = await readCapped(
        object.body,
        options.maxBytes ?? MAX_MANIFEST_BYTES,
      );
      if (!bytes)
        throw new ObjectStoreError(
          "invalid_argument",
          "manifest",
          "manifest is larger than the reader accepts",
        );
      const text = new TextDecoder().decode(bytes);
      const manifest = await parseManifest(text, key);
      const etag = normalizeEtag(object.meta.version.eTag);
      const version = object.meta.version.version;
      return {
        manifest,
        ...(etag ? { etag } : {}),
        ...(version ? { version } : {}),
      };
    },
  };
}

/**
 * The raw store this deployment reads manifests from and writes scrubbed
 * chunks to, configured from plain env names so a self-hosted install points
 * it at MinIO and the hosted Worker at R2's S3 API. Undefined when unset.
 */
export interface RawStore {
  store: ObjectStore;
  provider: StorageProvider;
  bucket: string;
}

const DEFAULT_GCS_TOKEN_URI =
  "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token";
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);
const TRUSTED_GCS_TOKEN_HOSTS = new Set(["169.254.169.254", ...LOOPBACK_HOSTS]);

function trustedGcsTokenUri(uri: string): boolean {
  try {
    const url = new URL(uri);
    return url.protocol === "http:" && !url.username && !url.password && TRUSTED_GCS_TOKEN_HOSTS.has(url.hostname);
  } catch {
    return false;
  }
}

function trustedGcsEndpoint(uri: string): boolean {
  try {
    const url = new URL(uri);
    if (url.username || url.password || url.search || url.hash) return false;
    if (LOOPBACK_HOSTS.has(url.hostname))
      return url.protocol === "http:" || url.protocol === "https:";
    return (
      url.protocol === "https:" &&
      url.hostname === "storage.googleapis.com" &&
      !url.port &&
      (url.pathname === "" || url.pathname === "/")
    );
  } catch {
    return false;
  }
}

function gcsApiHost(endpoint: string | undefined): boolean {
  if (!endpoint) return false;
  try {
    const host = new URL(endpoint).hostname.toLowerCase().replace(/\.$/, "");
    return host === "storage.googleapis.com" || host.endsWith(".storage.googleapis.com") || host === "storage.google.com";
  } catch {
    return false;
  }
}

async function gcsMetadataToken(uri: string, fetcher: typeof fetch) {
  const res = await fetcher(uri, { headers: { "Metadata-Flavor": "Google" } });
  if (!res.ok) throw new Error(`pipeline: GKE metadata token http ${res.status}`);
  let body: { access_token?: unknown; expires_in?: unknown };
  try {
    body = (await res.json()) as { access_token?: unknown; expires_in?: unknown };
  } catch {
    throw new Error("pipeline: GKE metadata server returned no access_token");
  }
  if (typeof body.access_token !== "string" || !body.access_token)
    throw new Error("pipeline: GKE metadata server returned no access_token");
  const expiresAt = typeof body.expires_in === "number" ? new Date(Date.now() + body.expires_in * 1000) : undefined;
  return { accessToken: body.access_token, ...(expiresAt ? { expiresAt } : {}) };
}

function gcsRawStoreFromEnv(
  env: Pick<
    Bindings,
    | "PIPELINE_RAW_S3_BUCKET"
    | "PIPELINE_RAW_S3_ENDPOINT"
    | "PIPELINE_RAW_S3_ACCESS_KEY_ID"
    | "PIPELINE_RAW_S3_SECRET_ACCESS_KEY"
    | "PIPELINE_RAW_S3_SSE"
    | "PIPELINE_RAW_GCS_TOKEN_URI"
  >,
  fetcher?: typeof fetch,
): RawStore | undefined {
  const bucket = env.PIPELINE_RAW_S3_BUCKET?.trim();
  if (!bucket) {
    adapterLog("raw store misconfigured", { setting: "PIPELINE_RAW_S3_BUCKET", expected: "bucket name" });
    return undefined;
  }
  if (env.PIPELINE_RAW_S3_ACCESS_KEY_ID || env.PIPELINE_RAW_S3_SECRET_ACCESS_KEY) {
    adapterLog("raw store misconfigured", { setting: "PIPELINE_RAW_PROVIDER", expected: "gcs without HMAC keys" });
    return undefined;
  }
  if (env.PIPELINE_RAW_S3_SSE) {
    adapterLog("raw store misconfigured", { setting: "PIPELINE_RAW_S3_SSE", expected: "unset for gcs" });
    return undefined;
  }
  const tokenUri = env.PIPELINE_RAW_GCS_TOKEN_URI?.trim() || DEFAULT_GCS_TOKEN_URI;
  if (!trustedGcsTokenUri(tokenUri)) {
    adapterLog("raw store misconfigured", { setting: "PIPELINE_RAW_GCS_TOKEN_URI", expected: "metadata server or loopback" });
    return undefined;
  }
  let baseUrl: string | undefined;
  if (env.PIPELINE_RAW_S3_ENDPOINT) {
    if (!trustedGcsEndpoint(env.PIPELINE_RAW_S3_ENDPOINT)) {
      adapterLog("raw store misconfigured", {
        setting: "PIPELINE_RAW_S3_ENDPOINT",
        expected: "official GCS HTTPS API or loopback emulator",
      });
      return undefined;
    }
    baseUrl = env.PIPELINE_RAW_S3_ENDPOINT;
  }
  const fetchImpl = fetcher ?? fetch;
  return {
    provider: "gcs",
    bucket,
    store: createGcsStore({
      bucket,
      ...(baseUrl ? { baseUrl } : {}),
      token: () => gcsMetadataToken(tokenUri, fetchImpl),
      ...(fetcher ? { fetch: fetcher } : {}),
    }),
  };
}

export function rawStoreFromEnv(
  env: Pick<
    Bindings,
    | "PIPELINE_RAW_S3_BUCKET"
    | "PIPELINE_RAW_S3_ENDPOINT"
    | "PIPELINE_RAW_S3_REGION"
    | "PIPELINE_RAW_S3_ACCESS_KEY_ID"
    | "PIPELINE_RAW_S3_SECRET_ACCESS_KEY"
    | "PIPELINE_RAW_S3_SSE"
    | "PIPELINE_RAW_PROVIDER"
    | "PIPELINE_RAW_GCS_TOKEN_URI"
  >,
  fetcher?: typeof fetch,
): RawStore | undefined {
  const requested = env.PIPELINE_RAW_PROVIDER?.trim() || undefined;
  if (requested === "gcs") return gcsRawStoreFromEnv(env, fetcher);
  const bucket = env.PIPELINE_RAW_S3_BUCKET;
  const accessKeyId = env.PIPELINE_RAW_S3_ACCESS_KEY_ID;
  const secretAccessKey = env.PIPELINE_RAW_S3_SECRET_ACCESS_KEY;
  if (!bucket || !accessKeyId || !secretAccessKey) return undefined;
  // Unset stays S3. gcs is handled above. Any other name must not be relabeled as S3.
  if (requested && requested !== "s3" && requested !== "r2") {
    adapterLog("raw store misconfigured", {
      setting: "PIPELINE_RAW_PROVIDER",
      expected: "s3 | r2 | gcs",
    });
    return undefined;
  }
  const provider: StorageProvider = requested === "r2" ? "r2" : "s3";
  if (gcsApiHost(env.PIPELINE_RAW_S3_ENDPOINT)) {
    adapterLog("raw store misconfigured", {
      setting: "PIPELINE_RAW_S3_ENDPOINT",
      expected: "s3 or r2 endpoint, not the GCS API",
    });
    return undefined;
  }
  const setting = env.PIPELINE_RAW_S3_SSE;
  const sse =
    setting === "AES256" || setting === "aws:kms" ? setting : undefined;
  // A setting that asks for encryption but names no known mode must not
  // silently mean "no encryption": the store counts as not configured.
  if (setting && !sse) {
    adapterLog("raw store misconfigured", {
      setting: "PIPELINE_RAW_S3_SSE",
      expected: "AES256 | aws:kms",
    });
    return undefined;
  }
  return {
    provider,
    bucket,
    store: createS3Store({
      bucket,
      region:
        env.PIPELINE_RAW_S3_REGION ??
        (provider === "r2" ? "auto" : "us-east-1"),
      ...(env.PIPELINE_RAW_S3_ENDPOINT
        ? { endpoint: env.PIPELINE_RAW_S3_ENDPOINT }
        : {}),
      credentials: { accessKeyId, secretAccessKey },
      ...(sse ? { serverSideEncryption: sse } : {}),
      ...(fetcher ? { fetch: fetcher } : {}),
    }),
  };
}

export function manifestReaderFromEnv(
  env: Bindings,
): ManifestReader | undefined {
  const raw = rawStoreFromEnv(env);
  return raw
    ? objectStoreManifestReader(raw.store, {
        provider: raw.provider,
        bucket: raw.bucket,
      })
    : undefined;
}

/** The org that owns a workspace's objects. Object keys and manifests carry no org. */
export interface OrgResolver {
  readonly kind: string;
  orgFor(location: {
    workspaceId: string;
    prefix?: string;
  }): Promise<string | null>;
}

/** Single-tenant installs: every object belongs to one org. */
export function staticOrgResolver(orgId: string): OrgResolver {
  return { kind: "static", orgFor: async () => orgId };
}

/** Deployments that put the org in the key's deployment prefix (`{org}/v1/...`). */
export const prefixOrgResolver: OrgResolver = {
  kind: "prefix",
  orgFor: async ({ prefix }) => {
    const org = (parseUploadGenerationPrefix(prefix)?.base ?? prefix)?.split("/").filter(Boolean).pop();
    return org || null;
  },
};

/**
 * `PIPELINE_ORG_ID` (single tenant) or `PIPELINE_ORG_FROM_PREFIX=true`. With
 * neither, notifications cannot be attributed and are retried (then left in
 * the object-event DLQ), and only the authenticated manifest POST can enqueue.
 */
export function orgResolverFromEnv(
  env: Pick<Bindings, "PIPELINE_ORG_ID" | "PIPELINE_ORG_FROM_PREFIX">,
): OrgResolver | undefined {
  if (env.PIPELINE_ORG_ID) return staticOrgResolver(env.PIPELINE_ORG_ID);
  if (env.PIPELINE_ORG_FROM_PREFIX === "true") return prefixOrgResolver;
  return undefined;
}

/**
 * The deployment-prefix rule for a tenant: with orgs taken from the prefix,
 * the prefix must be exactly `<PIPELINE_KEY_PREFIX>/<caller's org>` (or the
 * org alone when no base is set); otherwise it must be exactly the
 * configured `PIPELINE_KEY_PREFIX` (or absent when none is configured). Keeps
 * a caller from naming another org's object whose `ws=` happens to match.
 */
export function prefixAllowed(
  env: Pick<Bindings, "PIPELINE_ORG_FROM_PREFIX" | "PIPELINE_KEY_PREFIX">,
  prefix: string | undefined,
  orgId: string,
) {
  const base = (env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
  // With orgs taken from the prefix it must be EXACTLY `<base>/<org>` (or
  // `<org>` with no base) for the caller's org: not another base, no extra
  // segment, not a bare org when a base is configured.
  if (env.PIPELINE_ORG_FROM_PREFIX === "true")
    return (parseUploadGenerationPrefix(prefix)?.base ?? prefix) === (base ? `${base}/${orgId}` : orgId);
  return (parseUploadGenerationPrefix(prefix)?.base ?? prefix ?? "") === base;
}

export type BuildResult =
  | { status: "enqueue"; message: StageMessage<ObjectLandedPayload> }
  | { status: "ignored"; reason: "not_a_session_key" | "not_a_manifest" };

/**
 * The object-landed message for a manifest. Throws a non-retryable
 * `PipelineError` when the manifest contradicts its key or has no valid trace
 * id; such an object can never index correctly, however often it is retried.
 */
export function buildObjectLandedMessage(input: {
  landed: ObjectLanded;
  orgId: string;
  manifest?: ManifestV1;
  /** Identity of the manifest actually read, preferred over the notification's. */
  read?: { etag?: string; version?: string };
}): BuildResult {
  const location = parseSessionKey(input.landed.key);
  if (!location) return { status: "ignored", reason: "not_a_session_key" };
  if (!isManifestRest(location.rest) || (isPublicationManifestRest(location.rest) && !parseUploadGenerationPrefix(location.prefix)))
    return { status: "ignored", reason: "not_a_manifest" };
  const manifest = input.manifest;
  if (!manifest)
    throw new PipelineError(
      "manifest_missing",
      "a manifest event needs its manifest",
    );
  const fields = pointerFields(manifest);
  if (
    fields.workspaceId !== location.workspaceId ||
    fields.machineId !== location.machineId ||
    fields.harness !== location.harness ||
    fields.sessionId !== location.sessionId
  )
    throw new PipelineError(
      "manifest_key_mismatch",
      "manifest session does not match its object key",
      false,
    );
  if (!isTraceId(fields.traceId))
    throw new PipelineError(
      "invalid_trace_id",
      "manifest trace_id is not a UUID",
      false,
    );
  const etag = input.read
    ? normalizeEtag(input.read.etag)
    : normalizeEtag(input.landed.etag);
  const version = input.read ? input.read.version : input.landed.version;
  // Without a version or ETag every revision would share one work key, and an
  // older success could stand in for a newer failure.
  if (!etag && !version)
    throw new PipelineError(
      "manifest_identity_missing",
      "a manifest event needs an ETag or object version",
      false,
    );
  const payload: ObjectLandedPayload = {
    key: input.landed.key,
    kind: "manifest",
    ...(input.landed.provider ? { provider: input.landed.provider } : {}),
    ...(input.landed.bucket ? { bucket: input.landed.bucket } : {}),
    ...(input.landed.size !== undefined ? { size: input.landed.size } : {}),
    ...(etag ? { etag } : {}),
    ...(version ? { version } : {}),
    revision: fields.manifestRevision,
    scrubbed: fields.scrubbed,
  };
  return {
    status: "enqueue",
    message: {
      v: 1,
      stage: OBJECT_LANDED_STAGE,
      ctx: {
        traceId: fields.traceId.toLowerCase(),
        orgId: input.orgId,
        workspaceId: location.workspaceId,
        machineId: location.machineId,
        sessionId: location.sessionId,
        harness: location.harness,
      },
      workKey: manifestWorkKey(payload),
      revision: fields.manifestRevision,
      payload,
    },
  };
}

/**
 * The object-landed message for a deleted manifest. There is no manifest to
 * read, so it carries no revision or scrub state and gets a fresh trace id;
 * the index re-reads the key and drops the session if it is really gone (a
 * re-upload since the delete wins). Each delete event is its own unit of
 * work: its version is `deleted:<event time>`, which no put can share.
 */
export function buildObjectDeletedMessage(input: {
  landed: ObjectLanded;
  orgId: string;
  now?: Date;
}): BuildResult {
  const location = parseSessionKey(input.landed.key);
  if (!location) return { status: "ignored", reason: "not_a_session_key" };
  if (!isManifestRest(location.rest) || (isPublicationManifestRest(location.rest) && !parseUploadGenerationPrefix(location.prefix)))
    return { status: "ignored", reason: "not_a_manifest" };
  const at = input.landed.eventTime ?? (input.now ?? new Date()).toISOString();
  const payload: ObjectLandedPayload = {
    key: input.landed.key,
    kind: "manifest",
    deleted: true,
    ...(input.landed.provider ? { provider: input.landed.provider } : {}),
    ...(input.landed.bucket ? { bucket: input.landed.bucket } : {}),
    version: `deleted:${at}`,
  };
  return {
    status: "enqueue",
    message: {
      v: 1,
      stage: OBJECT_LANDED_STAGE,
      ctx: {
        traceId: uuidv7(input.now?.getTime()),
        orgId: input.orgId,
        workspaceId: location.workspaceId,
        machineId: location.machineId,
        sessionId: location.sessionId,
        harness: location.harness,
      },
      workKey: manifestWorkKey(payload),
      payload,
    },
  };
}

export const OBJECT_EVENTS_QUEUE_PREFIX = "relayhistory-object-events";

/** The object-event DLQ is never processed here; see `handleObjectEventQueue`. */
export function isObjectEventsDlq(queue: string) {
  return queue.startsWith(OBJECT_EVENTS_QUEUE_PREFIX) && queue.endsWith("-dlq");
}

type QueueMessage = Pick<
  Message<unknown>,
  "body" | "attempts" | "ack" | "retry"
>;

export interface ObjectEventQueueDeps {
  source: StorageEventSource;
  reader?: ManifestReader;
  orgs?: OrgResolver;
  /** The deployment-prefix rule (`prefixAllowed`); objects failing it are ignored. */
  allowPrefix?(prefix: string | undefined, orgId: string): boolean;
  send(message: StageMessage): Promise<void>;
}

/** One structured line: codes and ids only, never content. */
export function adapterLog(message: string, fields: Record<string, unknown>) {
  console.warn(
    JSON.stringify({ source: "pipeline.adapter", message, ...fields }),
  );
}

/**
 * Consumer for the object-event queue. Each notification is acked once it is
 * enqueued as object-landed or known to be irrelevant; missing manifests and
 * unconfigured dependencies retry, and a malformed manifest is acked with a
 * log (it can never succeed). Exhausted retries land in this queue's DLQ,
 * which keeps the raw notification for re-drive.
 */
export async function handleObjectEventQueue(
  batch: { queue: string; messages: readonly QueueMessage[] },
  deps: ObjectEventQueueDeps,
) {
  if (isObjectEventsDlq(batch.queue)) {
    // The DLQ holds raw notifications for an operator re-drive. Processing it
    // here would ack them or burn the DLQ's own retries, so it is never
    // consumed: hold everything back as long as the platform allows and say so.
    console.error(
      JSON.stringify({
        source: "pipeline.adapter",
        message: "object-event DLQ must not have a consumer; nothing processed",
        queue: batch.queue,
        count: batch.messages.length,
      }),
    );
    for (const message of batch.messages)
      message.retry({ delaySeconds: 43_200 });
    return;
  }
  for (const message of batch.messages) {
    const landed = deps.source.normalize(message.body);
    if (!landed) {
      message.ack();
      continue;
    }
    const location = parseSessionKey(landed.key);
    if (!location || !isManifestRest(location.rest) || (isPublicationManifestRest(location.rest) && !parseUploadGenerationPrefix(location.prefix))) {
      message.ack();
      continue;
    }
    const bucket =
      landed.bucket ?? (deps.reader?.bucket ? deps.reader.bucket : undefined);
    if (bucket && deps.reader?.bucket && bucket !== deps.reader.bucket) {
      // Another bucket's notification: the index could never read it.
      adapterLog("object event for another bucket", { queue: batch.queue });
      message.ack();
      continue;
    }
    if (!deps.reader || !deps.orgs) {
      adapterLog("object event adapter not configured", { queue: batch.queue });
      message.retry({ delaySeconds: 300 });
      continue;
    }
    try {
      const orgId = await deps.orgs.orgFor(location);
      if (!orgId) {
        adapterLog("object event org unresolved", {
          workspace_id: location.workspaceId,
        });
        message.retry({ delaySeconds: 300 });
        continue;
      }
      if (deps.allowPrefix && !deps.allowPrefix(location.prefix, orgId)) {
        adapterLog("object event outside the deployment prefix", {
          workspace_id: location.workspaceId,
        });
        message.ack();
        continue;
      }
      if (landed.deleted) {
        const built = buildObjectDeletedMessage({
          landed: { ...landed, ...(bucket ? { bucket } : {}) },
          orgId,
        });
        if (built.status === "enqueue") await deps.send(built.message);
        message.ack();
        continue;
      }
      const read = await deps.reader.read(landed.key);
      if (!read) {
        // Notification can precede read-after-write visibility; try again.
        message.retry({ delaySeconds: 30 });
        continue;
      }
      // An older notification for a manifest that has since been overwritten
      // is enqueued with the identity actually read: the newest content wins.
      const built = buildObjectLandedMessage({
        landed: { ...landed, ...(bucket ? { bucket } : {}) },
        orgId,
        manifest: read.manifest,
        read,
      });
      if (built.status === "enqueue") await deps.send(built.message);
      message.ack();
    } catch (error) {
      if (error instanceof PipelineError && !error.retryable) {
        adapterLog("object event rejected", {
          code: error.code,
          workspace_id: location.workspaceId,
        });
        message.ack();
        continue;
      }
      if (
        error instanceof ObjectStoreError &&
        error.code === "invalid_argument"
      ) {
        // Malformed manifest: it can never index. A newer schema
        // ("unsupported") retries instead, since a later deploy can read it.
        adapterLog("object event manifest invalid", {
          workspace_id: location.workspaceId,
        });
        message.ack();
        continue;
      }
      message.retry({ delaySeconds: 60 });
    }
  }
}
