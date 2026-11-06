/**
 * The pipeline surface the stack builds on, re-exported from one place so an
 * upstream rename touches one file. `@rhc/*` is packages/pipeline/src (the
 * relayhistory-cloud #210 + #211 ports) and `@rhc-object-store` is
 * packages/object-store; see tsconfig paths and MOVE_NOTES.md.
 */
export {
  createPipelineRoutes,
  pipelineOpsRoutes,
  type PipelineRouteDeps,
} from "@rhc/routes/pipeline.ts";
export { requireAuth } from "@rhc/middleware/auth.ts";
export {
  TRACE_ID_HEADER,
  traceIdFromHeaders,
  isTraceId,
  type TraceContext,
} from "@rhc/lib/pipeline/trace.ts";
export {
  postgresPipelineQueue,
  pollPostgresStage,
  postgresStageConsumer,
  type PipelineQueue,
  type PollResult,
  type SqlExecutor,
} from "@rhc/lib/pipeline/queue.ts";
export { postgresDeadLetterStore } from "@rhc/lib/pipeline/postgres-dead-letters.ts";
export {
  pipelineQueueDeps,
  type PipelineImplementations,
} from "@rhc/lib/pipeline/index.ts";
export { PipelineError } from "@rhc/lib/pipeline/errors.ts";
export {
  handleStageQueue,
  type StageHandler,
  type StageMessage,
  type StageQueueDeps,
  type DeadLetterStore,
  isStageMessage,
} from "@rhc/lib/pipeline/dlq.ts";
export {
  type PipelineEvent,
  type PipelineEventSink,
  type TraceScope,
  type TraceEvents,
  toRow,
  fromRow,
  MAX_TRACE_EVENTS,
  PipelineRecorder,
  logOnlyPipelineEventSink,
} from "@rhc/lib/pipeline/events.ts";
export {
  STAGE_REGISTRY,
  type QueuedStage,
} from "@rhc/lib/pipeline/registry.ts";
export type { PipelineStage } from "@rhc/lib/pipeline/trace.ts";
export {
  type RawPointer,
  type SessionPointerIndex,
  type ObjectLandedPayload,
  manifestWorkKey,
  objectLandedHandler,
  sessionIndexedHandler,
} from "@rhc/lib/pipeline/stages.ts";
export {
  buildObjectDeletedMessage,
  buildObjectLandedMessage,
  OBJECT_LANDED_STAGE,
  rawStoreFromEnv,
  manifestReaderFromEnv,
  objectStoreManifestReader,
  orgResolverFromEnv,
  prefixAllowed,
  prefixOrgResolver,
  readCapped,
  type ManifestReader,
  type ObjectLanded,
  type OrgResolver,
  type RawStore,
} from "@rhc/lib/pipeline/adapter.ts";
export {
  telemetryIdHash,
  telemetryIdKey,
} from "@rhc/lib/pipeline/telemetry-ids.ts";
export {
  reconcileListPrefix,
  reconcileManifests,
} from "@rhc/lib/pipeline/reconcile.ts";
export {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  canScrubLong,
  sealRawChunk,
} from "@rhc/lib/pipeline/raw-chunks.ts";
export {
  RELAYHISTORY_EXPORT_EXTENSION,
  RELAYHISTORY_NORMALIZED_VERSION,
  RelayhistoryExportTransform,
  relayhistoryExportProvenance,
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  readRelayhistoryExport,
  type RelayhistoryStreamRecord,
  type ZstdDecompress,
} from "@rhc/lib/pipeline/relayhistory-transform.ts";
export {
  CONVERGENCE_EVENT_KIND,
  projectRelayhistorySession,
  type ProjectionContext,
} from "@rhc/lib/pipeline/relayhistory-projection.ts";
export { scrubText, scrubJson } from "@rhc/lib/scrub.ts";
export type { Bindings, AppEnv } from "@rhc/env.ts";

export {
  createS3Store,
  createGcsStore,
  manifestKey,
  rawChunkKey,
  parseSessionKey,
  adoptionWriteIssues,
  appendBackfillEpoch,
  backfillDigest,
  backfillStatus,
  backfillWriteIssues,
  chunkKeys,
  commitBackfillEpoch,
  computeContentSha256,
  exportWatermarks,
  liveEpoch,
  liveNextOffset,
  MANIFEST_EPOCHS_CAPABILITY,
  writerUnderstandsBackfill,
  validateManifest,
  parseManifest,
  pointerFields,
  objectKey,
  joinKey,
  sessionPrefix,
  manifestLocation,
  readAll,
  type BackfillChunk,
  type BackfillIO,
  type ManifestV1,
  type ChunkEntry,
  type SessionLocation,
  type ObjectStore,
  type ObjectKey,
  type ObjectVersion,
  type RawFormat,
} from "@rhc-object-store";
export { ObjectStoreError, isObjectStoreError } from "@rhc-object-store";
export {
  deleteParkedSession,
  expireParkedSession,
  META_PARKED_AT,
  PARKED_TTL_MS,
  parkedManifestKey,
} from "@rhc/lib/pipeline/parked.ts";
export {
  assertExportComplete,
  detectExportDeferred,
  detectMachineSplits,
  EXPORT_DEFERRED,
  exportCompletionIssues,
  recordExportDeferred,
  resolveExportDeferred,
  SESSION_MACHINE_SPLIT,
  type ExportDeferred,
  type ExportIncomplete,
  type SessionMachineSplit,
} from "@rhc/lib/pipeline/session-split.ts";
export {
  BACKFILL_ACTIVATION_STAGE,
  backfillActivationDelay,
  backfillActivationHandler,
  backfillActivationLimits,
  backfillActivationWorkKey,
  postgresBackfillReady,
  type BackfillActivationPayload,
} from "@rhc/lib/pipeline/backfill-activation.ts";
export {
  attestReader,
  MANIFEST_READERS,
  missingReaders,
  postgresReadinessStore,
  selfCheckManifestEpochs,
  type ManifestReader as ManifestReaderName,
  type ReadinessRow,
  type ReadinessStore,
} from "@rhc/lib/pipeline/manifest-readiness.ts";
