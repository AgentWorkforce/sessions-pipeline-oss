/**
 * The reconcile job: for raw stores with no event notifications (self-hosted
 * MinIO, a plain S3 bucket, a shared directory), it lists manifests and
 * enqueues an object-landed message for each one modified inside the
 * look-back window. It is also the safety net behind the R2 events and the
 * manifest POST: a notification that was lost is picked up on a later run.
 *
 * Re-sending a manifest it already reported is harmless: the work key is the
 * same (`manifestWorkKey` over the key and the ETag read), so the stages treat
 * it as a repeat. It never reports deletes; a deleted manifest simply stops
 * being listed, and the R2 delete event (or a future sweep) handles it.
 *
 * Logs carry counts and codes only, never a key's contents.
 */
import type { Bindings } from "../../env.js";
import type { StageMessage } from "./dlq.js";
import { pipelineQueueFromEnv } from "./index.js";
import { PipelineError } from "./errors.js";
import {
  ObjectStoreError,
  objectKey,
  parseSessionKey,
  type ObjectStore,
} from "./object-layout.js";
import {
  adapterLog,
  buildObjectLandedMessage,
  objectStoreManifestReader,
  orgResolverFromEnv,
  prefixAllowed,
  rawStoreFromEnv,
  type ManifestReader,
  type OrgResolver,
} from "./adapter.js";

export interface ReconcileOptions {
  store: ObjectStore;
  reader: ManifestReader;
  orgs: OrgResolver;
  /** The deployment-prefix rule (`prefixAllowed`); manifests failing it are skipped. */
  allowPrefix?(prefix: string | undefined, orgId: string): boolean;
  send(message: StageMessage): Promise<void>;
  /** Listed recursively; see `reconcileListPrefix`. */
  listPrefix: string;
  /** Only manifests modified at or after this instant are enqueued. */
  modifiedSince: Date;
  /** Upper bound on list pages per run, so one run stays inside a cron tick. */
  maxPages?: number;
  /** Resume a listing a previous run stopped (see `ReconcileResult.nextCursor`). */
  cursor?: string;
}

export interface ReconcileResult {
  listed: number;
  enqueued: number;
  /** Not enqueued for a known reason: invalid, outside the prefix, gone, org unknown. */
  skipped: number;
  /** Failed for a reason a later run can fix (storage or queue unavailable). */
  failed: number;
  /** Set when the page budget ran out before the listing did. */
  nextCursor?: string;
}

export const DEFAULT_RECONCILE_MAX_PAGES = 20;

/**
 * Where manifests live for this deployment: `{PIPELINE_KEY_PREFIX}/v1` (or
 * `v1`) with a static org; with the org taken from the key prefix, everything
 * under `PIPELINE_KEY_PREFIX`, which must then be set (a store root cannot be
 * listed). Undefined when it cannot be determined.
 */
export function reconcileListPrefix(
  env: Pick<Bindings, "PIPELINE_ORG_FROM_PREFIX" | "PIPELINE_KEY_PREFIX">,
): string | undefined {
  const base = (env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
  if (env.PIPELINE_ORG_FROM_PREFIX === "true") return base || undefined;
  return base ? `${base}/v1` : "v1";
}

export async function reconcileManifests(
  options: ReconcileOptions,
): Promise<ReconcileResult> {
  const result: ReconcileResult = {
    listed: 0,
    enqueued: 0,
    skipped: 0,
    failed: 0,
  };
  const maxPages = options.maxPages ?? DEFAULT_RECONCILE_MAX_PAGES;
  const since = options.modifiedSince.getTime();
  let cursor = options.cursor;
  for (let page = 0; page < maxPages; page++) {
    const listing = await options.store.list(
      objectKey(options.listPrefix),
      cursor,
    );
    for (const item of listing.items) {
      if (!item.key.endsWith("/manifest.json")) continue;
      result.listed += 1;
      if (item.lastModified.getTime() < since) continue;
      await reconcileOne(String(item.key), options, result);
    }
    cursor = listing.nextCursor;
    if (!cursor) return result;
  }
  return { ...result, nextCursor: cursor };
}

async function reconcileOne(
  key: string,
  options: ReconcileOptions,
  result: ReconcileResult,
) {
  const location = parseSessionKey(key);
  if (!location || location.rest !== "manifest.json") {
    result.skipped += 1;
    return;
  }
  try {
    const orgId = await options.orgs.orgFor(location);
    if (
      !orgId ||
      (options.allowPrefix && !options.allowPrefix(location.prefix, orgId))
    ) {
      result.skipped += 1;
      return;
    }
    const read = await options.reader.read(key);
    if (!read) {
      // Deleted between the listing and the read.
      result.skipped += 1;
      return;
    }
    const built = buildObjectLandedMessage({
      landed: {
        key,
        provider: options.reader.provider,
        ...(options.reader.bucket ? { bucket: options.reader.bucket } : {}),
      },
      orgId,
      manifest: read.manifest,
      read,
    });
    if (built.status !== "enqueue") {
      result.skipped += 1;
      return;
    }
    await options.send(built.message);
    result.enqueued += 1;
  } catch (error) {
    if (
      (error instanceof PipelineError && !error.retryable) ||
      (error instanceof ObjectStoreError && error.code === "invalid_argument")
    ) {
      adapterLog("reconcile skipped an invalid manifest", {
        code: error instanceof PipelineError ? error.code : "manifest_invalid",
        workspace_id: location.workspaceId,
      });
      result.skipped += 1;
      return;
    }
    result.failed += 1;
  }
}

/** Every this many minutes, on the minute cron. */
export const RECONCILE_EVERY_MINUTES = 5;
export const DEFAULT_RECONCILE_LOOKBACK_MINUTES = 15;

/**
 * The cron entry: with `PIPELINE_RECONCILE_ENABLED=true`, a raw store and an
 * org mapping configured, every `RECONCILE_EVERY_MINUTES` it enqueues the
 * manifests modified in the look-back window (default 15 minutes, three
 * runs, so one failed run loses nothing). Never throws; logs counts only.
 */
export async function runManifestReconcile(
  env: Bindings,
  now: Date,
  send: (message: StageMessage) => Promise<void> = (message) =>
    pipelineQueueFromEnv(env).send(message),
): Promise<ReconcileResult | "skipped"> {
  if (env.PIPELINE_RECONCILE_ENABLED !== "true") return "skipped";
  if (now.getUTCMinutes() % RECONCILE_EVERY_MINUTES !== 0) return "skipped";
  const raw = rawStoreFromEnv(env);
  const orgs = orgResolverFromEnv(env);
  const listPrefix = reconcileListPrefix(env);
  if (!raw || !orgs || !listPrefix) {
    adapterLog("manifest reconcile not configured", {});
    return "skipped";
  }
  const lookback = Number(env.PIPELINE_RECONCILE_LOOKBACK_MINUTES);
  const minutes =
    Number.isFinite(lookback) && lookback > 0
      ? lookback
      : DEFAULT_RECONCILE_LOOKBACK_MINUTES;
  try {
    const result = await reconcileManifests({
      store: raw.store,
      reader: objectStoreManifestReader(raw.store, {
        provider: raw.provider,
        bucket: raw.bucket,
      }),
      orgs,
      allowPrefix: (prefix, orgId) => prefixAllowed(env, prefix, orgId),
      send,
      listPrefix,
      modifiedSince: new Date(now.getTime() - minutes * 60_000),
    });
    adapterLog("manifest reconcile finished", {
      listed: result.listed,
      enqueued: result.enqueued,
      skipped: result.skipped,
      failed: result.failed,
      truncated: Boolean(result.nextCursor),
    });
    return result;
  } catch (error) {
    adapterLog("manifest reconcile failed", {
      code:
        error instanceof ObjectStoreError ? `storage_${error.code}` : "failed",
    });
    return { listed: 0, enqueued: 0, skipped: 0, failed: 1 };
  }
}
