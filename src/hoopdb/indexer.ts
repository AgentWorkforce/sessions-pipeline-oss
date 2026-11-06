/**
 * HoopDb indexer: consumes `object-landed` (vendored handler, fans out to
 * `session-indexed`) and `session-indexed` (our index), and runs the manifest
 * reconcile every five minutes so MinIO, which sends no bucket notifications
 * here, still lands every manifest.
 */
import {
  orgResolverFromEnv,
  prefixAllowed,
  reconcileManifests,
  type Bindings,
} from "../shared/vendor.ts";
import { vendorBindings, integer } from "../shared/env.ts";
import { enrichMessage } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { indexerReconcileRoot } from "./reconcile-root.ts";
import { runWorker } from "../shared/worker.ts";
import { hoopDbSessionIndex } from "./index.ts";
import {expectedSchema}from "./migrate.ts";
import {inTransaction}from "../shared/db.ts";
const publicationSchema=await expectedSchema();
import {
  backfillActivationHandler,
  backfillActivationLimits,
  missingReaders,
  postgresBackfillReady,
  postgresPipelineQueue,
  postgresReadinessStore,
} from "../shared/vendor.ts";
import { withSpan } from "../shared/telemetry.ts";
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";

const env = vendorBindings() as unknown as Bindings;
let lastReconcileMinute = -1;

await runWorker({
  service: "hoopdb-indexer",
  stages: ["object-landed", "session-indexed", "backfill-activation"],
  attests: ["session-index", "hoopdb"],
  impl: (db, current) => ({
    index: hoopDbSessionIndex({
      db,
      reader: lockerManifestReader(),
      queue: postgresPipelineQueue(db),
      current,
      publication:{schemaGeneration:publicationSchema.generation,transact:work=>inTransaction(db,async tx=>{
        await tx.query("SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s'");return work(tx);
      })},
    }),
  }),
  handlers: (db, current) => ({
    // object-landed comes from pipelineQueueDeps; session-indexed from impl.index.
    "backfill-activation": backfillActivationHandler({
      missing: () =>
        missingReaders({
          key: process.env.MANIFEST_READINESS_KEY,
          store: postgresReadinessStore(db),
        }),
      ready: postgresBackfillReady(db),
      requeue: (message, delaySeconds) =>
        postgresPipelineQueue(db).send(message, { delaySeconds }),
      limits: backfillActivationLimits(process.env),
    }),
  }),
  tick: async (db, now) => {
    const every = integer("RECONCILE_EVERY_MINUTES", 5);
    const minute = Math.floor(now.getTime() / 60_000);
    if (minute === lastReconcileMinute || minute % every !== 0) return;
    lastReconcileMinute = minute;
    const raw = lockerStore();
    const orgs = orgResolverFromEnv(env);
    if (!orgs) return;
    const listPrefix = indexerReconcileRoot(env);
    if (!listPrefix) {
      // Org-derived prefixes with no PIPELINE_KEY_PREFIX: the store root cannot be listed.
      log.warn("reconcile not configured", {
        reason: "org_from_prefix_without_base",
      });
      return;
    }
    const reader = lockerManifestReader();
    const queue = postgresPipelineQueue(db);
    const lookback = integer("PIPELINE_RECONCILE_LOOKBACK_MINUTES", 15);
    await withSpan(
      "reconcile manifests",
      { attrs: { stage: "object-landed" } },
      async () => {
        const result = await reconcileManifests({
          store: raw.store,
          reader,
          orgs,
          allowPrefix: (prefix, orgId) => prefixAllowed(env, prefix, orgId),
          send: async (m) => queue.send(await enrichMessage(m, reader)),
          // `<base>/v1` with a static org; with org-derived prefixes the whole
          // `<base>` root, so every `<base>/<org>/v1` is listed (allowPrefix
          // then keeps only exact `<base>/<org>` keys of a known org).
          listPrefix,
          modifiedSince: new Date(now.getTime() - lookback * 60_000),
        });
        log.info("reconcile finished", {
          listed: result.listed,
          enqueued: result.enqueued,
          skipped: result.skipped,
          failed: result.failed,
        });
      },
    );
  },
});
