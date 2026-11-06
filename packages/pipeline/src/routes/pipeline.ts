import { Hono } from "hono";
import { createMiddleware } from "hono/factory";
import type { AppContext, AppEnv, Bindings } from "../env.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import { loggerEnvFromBindings } from "../lib/logger.js";
import {
  deadLetterStoreFromEnv,
  errorReporterFromEnv,
  isPipelineStage,
  isSafeIdentifier,
  isTraceId,
  telemetryIdHash,
  PipelineError,
  PipelineRecorder,
  PipelineStoreUnavailable,
  RESERVED_ATTR_KEYS,
  pipelineEventSinkFromEnv,
  replayDeadLetters,
  sendToStage,
  summarizeTrace,
  type DeadLetterStore,
  type PipelineEventSink,
  type PipelineStage,
  type PipelineStatus,
  type RecordOptions,
  type TraceContext,
  type TraceScope,
  type StageMessage,
  type ObjectLandedPayload,
} from "../lib/pipeline/index.js";
import {
  isBackfillIdentity,
  isV2Identity,
  MAX_PLAN_ENTRY_BYTES,
  migrationKindAllowsRoute,
  migrationChunkMatches,
  migrationGrantAllowed,
  migrationManifestProvenance,
  migrationPairAuthorized,
  migrationSessionMatches,
  migrationUploadMatches,
  migrationWorkKey,
  PLAN_ENTRY_LENGTH_HEADER,
  type MigrationAssertionLedger,
  type MigrationChunkResult,
} from "../lib/pipeline/migration-assertions.js";
import { sha256Hex } from "../../../object-store/src/crypto.js";
import {
  buildObjectLandedMessage,
  manifestReaderFromEnv,
  OBJECT_LANDED_STAGE,
  orgResolverFromEnv,
  prefixAllowed,
  rawStoreFromEnv,
  readCapped,
  type BuildResult,
  type ManifestReader,
  type RawStore,
} from "../lib/pipeline/adapter.js";
import {
  ObjectStoreError,
  parseManifest,
  parseSessionKey,
  RAW_FORMATS,
  type ManifestV1,
  type RawFormat,
} from "../lib/pipeline/object-layout.js";
import { sealRawChunk } from "../lib/pipeline/raw-chunks.js";

/** Stages a client may report about itself. Server stages cannot be forged. */
const CLIENT_STAGES: readonly PipelineStage[] = [
  "ingest",
  "deliver.put",
  "fetch",
];
const CLIENT_STATUSES: readonly PipelineStatus[] = [
  "started",
  "ok",
  "error",
  "retrying",
  "skipped",
];
const MAX_REPORTED_EVENTS = 100;
const MAX_TELEMETRY_LOOKUPS = 100;
/** Client attrs that say which destination and which unit of work an event is for. */
const KEY_ATTRS = new Set(["destination", "chunk", "seq", "offset"]);
/** Never accepted as an attr key or a destination name: they name prototype slots. */
const PROTOTYPE_KEYS = new Set(["__proto__", "constructor", "prototype"]);

export interface PipelineRouteDeps {
  /** Authoritative manifest reads for the manifest POST; absent: validate the posted copy. */
  manifests?(env: Bindings): ManifestReader | undefined;
  /** Where scrubbed raw chunks are written; absent: the chunk route answers 503. */
  rawStore?(env: Bindings): RawStore | undefined;
  sink(env: Bindings): PipelineEventSink;
  deadLetters(env: Bindings): DeadLetterStore;
  send(
    env: Bindings,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<void>;
  /**
   * Single-use ledger for RelayAuth migration assertions. It also performs a
   * migration commit's enqueue, in the same transaction as the consumption,
   * in place of `send`. Absent: migration assertions are refused (503).
   */
  migrationLedger?(env: Bindings): MigrationAssertionLedger | undefined;
  /**
   * Whether this deployment admits v2 (backfill epoch) migration assertions.
   * Only a deployment whose manifest commit verifies the append against the
   * stored base (96BullSessions' courier route) sets it; absent, a v2
   * assertion is refused with 403 `migration_backfill_unsupported`.
   */
  migrationBackfill?: boolean;
}

const defaultDeps: PipelineRouteDeps = {
  manifests: (env) => manifestReaderFromEnv(env),
  rawStore: (env) => rawStoreFromEnv(env),
  sink: (env) => pipelineEventSinkFromEnv(env),
  deadLetters: (env) => deadLetterStoreFromEnv(env),
  send: sendToStage,
};

function error(
  c: AppContext,
  status: 400 | 401 | 403 | 404 | 409 | 413 | 503,
  code: string,
  message: string,
) {
  return c.json(
    { error: { code, message }, correlationId: c.get("correlationId") ?? "" },
    status,
  );
}

function waitUntil(c: AppContext, work: Promise<unknown>): void {
  try {
    c.executionCtx.waitUntil(work);
  } catch {
    // No execution context (tests, `app.request`): the promise still runs.
  }
}

/**
 * The trace scope for resolved auth. A token with no workspace claim uses
 * `default`, the same workspace `applyIngest` stores its uploads under, so the
 * trace and the upload it describes stay in one workspace.
 */
function pipelineScope(c: AppContext): TraceScope {
  const auth = getAuth(c);
  return { orgId: auth.orgId, workspaceId: auth.workspaceId || "default" };
}

/** How far a client clock may sit from ours before its `at` is replaced by now. */
const MAX_CLIENT_LAG_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_CLIENT_LEAD_MS = 5 * 60 * 1000;

/**
 * A client-reported time, or now when it is missing or implausible. Accepting a
 * far-future `at` would park the row in the default partition, outside retention.
 */
function clientEventTime(value: unknown, now = Date.now()): Date | undefined {
  if (typeof value !== "string") return undefined;
  const at = Date.parse(value);
  if (
    Number.isNaN(at) ||
    at > now + MAX_CLIENT_LEAD_MS ||
    at < now - MAX_CLIENT_LAG_MS
  )
    return undefined;
  return new Date(at);
}

/**
 * Distinguishes recordings of one stage/status/attempt that are different work:
 * the destination plus the chunk, seq or offset the event is about.
 */
function clientEventKey(attrs: Record<string, unknown>) {
  const unit = attrs.chunk ?? attrs.seq ?? attrs.offset;
  return [
    typeof attrs.destination === "string" ? attrs.destination : "",
    unit === undefined ? "" : String(unit),
  ].join("/");
}

/**
 * Record a server-observed stage for this request's trace, after the response.
 * No trace id on the request means no pipeline row; the request log line still
 * carries machine_id and correlation_id.
 */
export function recordRequestStage(
  c: AppContext,
  stage: PipelineStage,
  status: PipelineStatus,
  context: { machineId?: string; sessionId?: string },
  options: RecordOptions = {},
  sink: PipelineEventSink = pipelineEventSinkFromEnv(c.env ?? {}),
) {
  const traceId = c.get("traceId");
  if (!traceId) return;
  const env = c.env ?? {};
  const recorder = new PipelineRecorder(
    {
      traceId,
      ...pipelineScope(c),
      ...(context.machineId ? { machineId: context.machineId } : {}),
      ...(context.sessionId ? { sessionId: context.sessionId } : {}),
    },
    {
      sink,
      reporter: errorReporterFromEnv(env),
      logger: loggerEnvFromBindings(env),
    },
  );
  waitUntil(
    c,
    recorder.record(stage, status, options).then(() => recorder.flush()),
  );
}

export function createPipelineRoutes(deps: PipelineRouteDeps = defaultDeps) {
  const routes = new Hono<AppEnv>();

  /**
   * Admission for a RelayAuth migration assertion, ahead of any route work:
   * a configured ledger (else 503), the signed pair (`wks`, `sponsorId`)
   * equal to the grant pair (`meta.workspaceId`, `meta.sourceUserId`) (else
   * 403 `migration_workspace_forbidden`), and an assertion not yet consumed
   * (else 409). The handlers then check the upload's target workspace and,
   * at the manifest commit, the plan digest. Ordinary tokens pass through.
   */
  const migrationAdmission = createMiddleware<AppEnv>(async (c, next) => {
    const auth = getAuth(c);
    if (!auth.migration) return next();
    const env = c.env ?? {};
    const ledger = deps.migrationLedger?.(env);
    if (!ledger) return ledgerUnavailable(c);
    if (!migrationPairAuthorized(auth)) return workspaceForbidden(c);
    if (isV2Identity(auth.migration.upload) && !deps.migrationBackfill)
      return error(
        c,
        403,
        "migration_backfill_unsupported",
        "This deployment does not accept backfill-epoch (v2) migration assertions",
      );
    // A base-read capability reaches only the base read, and nothing else does.
    if (
      !migrationKindAllowsRoute(auth.migration.upload, c.req.method, c.req.path)
    )
      return error(
        c,
        403,
        "migration_token_class_forbidden",
        "This migration token class does not reach this route",
      );
    if (!migrationGrantAllowed(env, auth.migration)) return grantForbidden(c);
    // Status: only the signed upload's own manifest key.
    if (c.req.path.endsWith("/pipeline/manifests/status")) {
      const location = parseSessionKey(c.req.query("key") ?? "");
      if (
        !location ||
        location.rest !== "manifest.json" ||
        !migrationSessionMatches(auth, location)
      )
        return uploadForbidden(c);
      // The key's deployment prefix must be this deployment's (static
      // PIPELINE_KEY_PREFIX, or the caller's org with PIPELINE_ORG_FROM_PREFIX),
      // exactly as for the chunk and commit routes.
      if (!prefixAllowed(env, location.prefix, auth.orgId))
        return workspaceForbidden(c);
    }
    let consumed: boolean;
    try {
      consumed = await ledger.consumed(auth.migration);
    } catch {
      return ledgerUnavailable(c);
    }
    if (consumed) return replayed(c);
    await next();
  });
  // The only routes a migration assertion reaches (requireAuth refuses it on
  // every other one). Registered ahead of the handlers, so it runs first.
  routes.use("/pipeline/chunks", migrationAdmission);
  routes.use("/pipeline/manifests", migrationAdmission);
  routes.use("/pipeline/manifests/status", migrationAdmission);
  routes.use("/pipeline/manifests/base", migrationAdmission);
  // This deployment serves no base read (only one with migrationBackfill and
  // a base-fenced commit does): every other token is refused here too.
  routes.get("/pipeline/manifests/base", (c) =>
    error(
      c,
      403,
      "migration_token_class_forbidden",
      "Only a migration base-read capability reaches the base read",
    ),
  );

  function storageReadRequired(c: AppContext) {
    return error(
      c,
      503,
      "migration_storage_read_required",
      "A migration commit needs the server to read the manifest from object storage",
    );
  }

  function uploadForbidden(c: AppContext) {
    return error(
      c,
      403,
      "migration_upload_forbidden",
      "The upload is not the one this migration assertion's signed upload identity names",
    );
  }

  function grantForbidden(c: AppContext) {
    return error(
      c,
      403,
      "migration_grant_forbidden",
      "The migration assertion's grant is not accepted here, or its plan is not that grant's plan",
    );
  }

  function workspaceForbidden(c: AppContext) {
    return error(
      c,
      403,
      "migration_workspace_forbidden",
      "The upload's workspace and user are not the pair this migration assertion was signed for",
    );
  }

  function ledgerUnavailable(c: AppContext) {
    return error(
      c,
      503,
      "migration_ledger_unavailable",
      "The migration assertion ledger is not configured or not available",
    );
  }

  function replayed(c: AppContext) {
    return error(
      c,
      409,
      "migration_assertion_replayed",
      "This migration assertion was already used; retry with a fresh assertion",
    );
  }

  /** "Did my upload land?": every recorded stage of one trace, plus a verdict. */
  routes.get(
    "/pipeline/traces/:traceId",
    requireScope("rth:read"),
    async (c) => {
      const traceId = c.req.param("traceId").toLowerCase();
      if (!isTraceId(traceId))
        return error(c, 400, "bad_request", "traceId must be a UUID");
      try {
        const { events, truncated } = await deps
          .sink(c.env ?? {})
          .byTrace(pipelineScope(c), traceId);
        const summary = summarizeTrace(traceId, events, truncated);
        return c.json(
          { ...summary, correlationId: c.get("correlationId") ?? "" },
          summary.verdict === "not_found" ? 404 : 200,
        );
      } catch (cause) {
        if (cause instanceof PipelineStoreUnavailable)
          return error(
            c,
            503,
            "not_configured",
            "Pipeline event store is not configured; stage events are in logs only",
          );
        return error(
          c,
          503,
          "pipeline_store_unavailable",
          "Pipeline event store is temporarily unavailable",
        );
      }
    },
  );

  /**
   * Client-side stages (local ingest, object storage put, fetch), reported by the
   * delivery coordinator. Workspace comes from auth; machine and session ids are
   * the client's own and are validated as identifiers only.
   */
  routes.post("/pipeline/events", requireScope("rth:sync"), async (c) => {
    let body: { machineId?: unknown; events?: unknown };
    try {
      body = await c.req.json();
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    const machineId = body?.machineId;
    if (typeof machineId !== "string" || !isSafeIdentifier(machineId))
      return error(c, 400, "bad_request", "machineId is required");
    if (
      !Array.isArray(body.events) ||
      !body.events.length ||
      body.events.length > MAX_REPORTED_EVENTS
    )
      return error(
        c,
        400,
        "bad_request",
        `events must include 1 to ${MAX_REPORTED_EVENTS} items`,
      );
    const scope = pipelineScope(c);
    const sink = deps.sink(c.env ?? {});
    const env = c.env ?? {};
    // Validate the whole batch before writing any of it, so a rejected request
    // leaves no partial trace behind.
    const accepted: {
      ctx: TraceContext;
      stage: PipelineStage;
      status: PipelineStatus;
      options: RecordOptions;
    }[] = [];
    for (const [index, raw] of (body.events as unknown[]).entries()) {
      const event = raw as Record<string, unknown> | null;
      if (
        !event ||
        !isTraceId(event.traceId) ||
        !isPipelineStage(event.stage) ||
        !CLIENT_STAGES.includes(event.stage) ||
        typeof event.status !== "string" ||
        !CLIENT_STATUSES.includes(event.status as PipelineStatus) ||
        !validDestination(event.attrs)
      )
        return error(
          c,
          400,
          "bad_request",
          `events[${index}] needs a traceId, a client stage (${CLIENT_STAGES.join(", ")}), a status, and an identifier-shaped attrs.destination if one is given`,
        );
      const sessionId = isSafeIdentifier(event.sessionId)
        ? event.sessionId
        : undefined;
      const attrs = numericAndIdAttrs(event.attrs);
      accepted.push({
        ctx: {
          traceId: event.traceId.toLowerCase(),
          ...scope,
          machineId,
          ...(sessionId ? { sessionId } : {}),
        },
        stage: event.stage,
        status: event.status as PipelineStatus,
        options: {
          attempt:
            Number.isInteger(event.attempt) && (event.attempt as number) > 0
              ? (event.attempt as number)
              : 1,
          attrs,
          errorCode:
            typeof event.errorCode === "string" &&
            /^[A-Za-z0-9_.:-]{1,64}$/.test(event.errorCode)
              ? event.errorCode
              : undefined,
          key: clientEventKey(attrs),
          now: clientEventTime(event.at),
        },
      });
    }
    const recorders = accepted.map(
      ({ ctx }) =>
        new PipelineRecorder(ctx, { sink, logger: loggerEnvFromBindings(env) }),
    );
    await Promise.all(
      accepted.map(({ stage, status, options }, index) =>
        recorders[index]!.record(stage, status, options),
      ),
    );
    // A configured log export must finish after the response, not be cancelled.
    waitUntil(c, Promise.allSettled(recorders.map((r) => r.flush())));
    return c.json({ recorded: recorders.length });
  });

  /**
   * The manifest POST: the delivery coordinator reports a manifest it just
   * wrote. Org and workspace come from auth, and the key's `ws=` must be the
   * caller's workspace. With a raw store configured the server reads the
   * manifest itself (authoritative); otherwise the posted copy is validated
   * against its key. Enqueues one object-landed message; repeats are
   * idempotent by work key. Never logs or echoes manifest content.
   */
  routes.post("/pipeline/manifests", requireScope("rth:sync"), async (c) => {
    let body: {
      key?: unknown;
      etag?: unknown;
      version?: unknown;
      manifest?: unknown;
    };
    try {
      body = await c.req.json();
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    const key = typeof body?.key === "string" ? body.key : "";
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json")
      return error(
        c,
        400,
        "bad_request",
        "key must be a v1 session manifest key",
      );
    const env = c.env ?? {};
    const owned = await callerOwnsLocation(c, location);
    if (owned !== true) return owned;
    const scope = pipelineScope(c);
    let manifest: ManifestV1;
    let read: { etag?: string; version?: string } | undefined;
    const reader = (deps.manifests ?? defaultDeps.manifests!)(env);
    try {
      if (reader) {
        const stored = await reader.read(key);
        if (!stored)
          return error(
            c,
            404,
            "not_found",
            "manifest is not in object storage",
          );
        manifest = stored.manifest;
        read = stored;
      } else {
        manifest = await parseManifest(body.manifest, key);
      }
    } catch (cause) {
      // The validator's message can quote manifest values: never echo it.
      if (
        cause instanceof ObjectStoreError &&
        (cause.code === "invalid_argument" || cause.code === "unsupported")
      )
        return error(c, 400, "bad_request", "manifest failed validation");
      return error(
        c,
        503,
        "storage_unavailable",
        "Object storage is temporarily unavailable",
      );
    }
    let built: BuildResult;
    try {
      built = buildObjectLandedMessage({
        // Read from storage: only the stored identity names the version, the
        // same one a notification or the reconcile job carries. Posted: the
        // client's identity.
        landed: {
          key,
          ...(reader
            ? {
                provider: reader.provider,
                ...(reader.bucket ? { bucket: reader.bucket } : {}),
              }
            : {
                ...(typeof body.etag === "string" ? { etag: body.etag } : {}),
                ...(typeof body.version === "string"
                  ? { version: body.version }
                  : {}),
              }),
        },
        orgId: scope.orgId,
        manifest,
        ...(read ? { read } : {}),
      });
    } catch (cause) {
      if (cause instanceof PipelineError)
        return error(c, 400, "bad_request", cause.message);
      throw cause;
    }
    if (built.status !== "enqueue")
      return error(c, 400, "bad_request", "key is not a manifest");
    const auth = getAuth(c);
    if (auth.migration) {
      // The target (key and manifest workspace, resolved user) must be the
      // signed grant pair, and the manifest must commit to the signed plan.
      if (
        !migrationPairAuthorized(auth, {
          workspaceIds: [location.workspaceId, manifest.session.workspace_id],
        })
      )
        return workspaceForbidden(c);
      // The signed upload identity, recomputed from this key, the manifest's
      // session and chunk list, and the content hash chain over it.
      if (!(await migrationUploadMatches(auth, location, manifest)))
        return uploadForbidden(c);
      if (migrationManifestProvenance(auth, manifest))
        return uploadForbidden(c);
      // The manifest must be the copy the server read from storage (the one
      // just verified); the work key is then derived from signed fields only.
      if (!reader) return storageReadRequired(c);
      built.message.workKey = migrationWorkKey(auth.migration.upload);
      (built.message.payload as ObjectLandedPayload).migration = {
        key: auth.migration.upload.key,
        entry_sha256: auth.migration.upload.entry_sha256,
        ...(isBackfillIdentity(auth.migration.upload)
          ? { base_revision: auth.migration.upload.base.revision }
          : {}),
      };
      // The commit's only durable write is the enqueue: the ledger consumes the
      // assertion and enqueues as one unit, so neither lands without the other.
      // A failure leaves nothing consumed, and the upload may be retried.
      const ledger = deps.migrationLedger?.(env);
      if (!ledger) return ledgerUnavailable(c);
      let outcome: "consumed" | "replayed" | "grant_mismatch";
      try {
        outcome = await ledger.consume(
          {
            jti: auth.migration.jti,
            nonce: auth.migration.nonce,
            grantId: auth.migration.grantId,
            planDigest: auth.migration.planDigest,
            planEntryDigest: auth.migration.upload.entry_sha256,
            expiresAt: auth.migration.expiresAt,
            orgId: scope.orgId,
            workspaceId: scope.workspaceId,
            userId: auth.userId,
          },
          OBJECT_LANDED_STAGE,
          built.message,
        );
      } catch {
        return ledgerUnavailable(c);
      }
      if (outcome === "replayed") return replayed(c);
      if (outcome === "grant_mismatch") return grantForbidden(c);
    } else {
      try {
        await deps.send(env, OBJECT_LANDED_STAGE, built.message);
      } catch {
        return error(
          c,
          503,
          "not_configured",
          "Pipeline queue is not available",
        );
      }
    }
    return c.json(
      {
        workKey: built.message.workKey,
        traceId: built.message.ctx.traceId,
        revision: built.message.revision,
        scrubbed: built.message.payload.scrubbed,
      },
      202,
    );
  });

  /**
   * The scrubbing chunk hop: a client that cannot scrub locally sends the
   * source range of one raw chunk, and the server scrubs it (`lib/scrub.ts`,
   * record by record), compresses it and stores it at its v1 key. The
   * unscrubbed bytes never reach object storage. Returns the chunk entry and
   * `raw.scrub` marker for the client's manifest. Workspace from auth.
   */
  routes.post("/pipeline/chunks", requireScope("rth:sync"), async (c) => {
    const env = c.env ?? {};
    const raw = (deps.rawStore ?? defaultDeps.rawStore!)(env);
    if (!raw)
      return error(
        c,
        503,
        "not_configured",
        "Raw object storage is not configured",
      );
    const q = (name: string) => c.req.query(name) ?? "";
    const format = q("format") as RawFormat;
    // Digits only: a missing or empty position must not become 0.
    const position = (name: string) =>
      /^\d+$/.test(q(name)) ? Number(q(name)) : NaN;
    const seq = position("seq");
    const epoch = position("epoch");
    const startOffset = position("offset");
    const date = q("date");
    if (
      !(RAW_FORMATS as readonly string[]).includes(format) ||
      ![seq, epoch, startOffset].every((n) => Number.isSafeInteger(n)) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(date)
    )
      return error(
        c,
        400,
        "bad_request",
        "format, seq, epoch, offset and date (yyyy-mm-dd) are required",
      );
    const scope = pipelineScope(c);
    const prefix = deploymentPrefix(env, scope.orgId);
    const location = {
      ...(prefix ? { prefix } : {}),
      workspaceId: scope.workspaceId,
      machineId: q("machine"),
      harness: q("harness"),
      sessionId: q("session"),
      startDate: date,
    };
    // The same ownership rule as the manifest POST: a matching ws= is not
    // ownership, and with no org mapping configured the write fails closed.
    const owned = await callerOwnsLocation(c, location);
    if (owned !== true) return owned;
    const chunkAuth = getAuth(c);
    const migration = chunkAuth.migration;
    // The chunk's session key, machine and date must be the signed upload's,
    // before the first write (fresh keys included).
    if (migration && !migrationSessionMatches(chunkAuth, location))
      return uploadForbidden(c);
    // A migration chunk carries its signed plan entry in front of the bytes.
    const entryLength = migration
      ? Number(c.req.header(PLAN_ENTRY_LENGTH_HEADER) ?? NaN)
      : 0;
    if (
      migration &&
      (!Number.isSafeInteger(entryLength) ||
        entryLength < 2 ||
        entryLength > MAX_PLAN_ENTRY_BYTES)
    )
      return uploadForbidden(c);
    const limit = MAX_CHUNK_BYTES + entryLength;
    const declared = Number(c.req.header("content-length") ?? NaN);
    if (declared > limit)
      return error(c, 413, "too_large", "chunk exceeds the size limit");
    // Bounded while reading: the declared length may be absent or wrong.
    const body = await readCapped(c.req.raw.body, limit);
    if (!body || body.byteLength - entryLength > MAX_CHUNK_BYTES)
      return error(c, 413, "too_large", "chunk exceeds the size limit");
    if (migration && body.byteLength <= entryLength) return uploadForbidden(c);
    const source = migration ? body.subarray(entryLength) : body;
    if (migration) {
      // Before anything is stored: the carried entry is the signed one, and
      // this chunk (seq, epoch, offset, size, digest of these bytes) is one
      // of its chunks; so seq < the entry's chunk count.
      const entryText = new TextDecoder().decode(body.subarray(0, entryLength));
      if (
        !(await migrationChunkMatches(
          chunkAuth,
          location,
          { seq, epoch, startOffset, bytes: source },
          entryText,
        ))
      )
        return uploadForbidden(c);
      // At most one chunk per (assertion, seq): a repeat is idempotent, any
      // other chunk at this seq is refused; and a strict per-assertion cap.
      let held: MigrationChunkResult;
      try {
        const ledger = deps.migrationLedger?.(env);
        if (!ledger) return ledgerUnavailable(c);
        held = await ledger.reserveChunk({
          jti: migration.jti,
          seq,
          startOffset,
          sha256: await sha256Hex(source),
          expiresAt: migration.expiresAt,
        });
      } catch {
        return ledgerUnavailable(c);
      }
      if (held === "conflict")
        return error(
          c,
          409,
          "migration_chunk_conflict",
          "Another chunk was already written at this seq under this migration assertion",
        );
      if (held === "limit")
        return error(
          c,
          403,
          "migration_chunk_limit",
          "This migration assertion reached its chunk limit; mint a fresh one",
        );
    }
    const traceId = c.req.header("X-Relay-Trace-Id");
    try {
      const sealed = await sealRawChunk(raw.store, {
        location,
        format,
        seq,
        epoch,
        startOffset,
        source,
        ...(traceId && isTraceId(traceId)
          ? { traceId: traceId.toLowerCase() }
          : {}),
      });
      return c.json(
        {
          key: sealed.key,
          entry: sealed.entry,
          scrub: sealed.scrub,
          created: sealed.created,
        },
        sealed.created ? 201 : 200,
      );
    } catch (cause) {
      if (cause instanceof PipelineError && cause.code === "zstd_unavailable")
        return error(c, 503, "not_supported", cause.message);
      if (cause instanceof PipelineError)
        return error(c, 400, "bad_request", cause.message);
      // Layout validation (ids) is a client error; its message names only ids.
      if (
        cause instanceof ObjectStoreError &&
        cause.code === "invalid_argument"
      )
        return error(
          c,
          400,
          "bad_request",
          "machine, harness or session is not a valid id",
        );
      return error(
        c,
        503,
        "storage_unavailable",
        "Object storage is temporarily unavailable",
      );
    }
  });

  /**
   * Whether the caller owns a parsed key: its `ws=` is the caller's workspace,
   * its deployment prefix is allowed for the caller's org, and the object's
   * owner (by the rule notifications use) is the caller's org. Workspace ids
   * are not unique across orgs, so `ws=` alone is not ownership. With no org
   * mapping configured ownership cannot be established: fail closed.
   */
  async function callerOwnsLocation(
    c: AppContext,
    location: { workspaceId: string; prefix?: string },
  ) {
    const scope = pipelineScope(c);
    const env = c.env ?? {};
    const auth = getAuth(c);
    if (
      auth.migration &&
      !migrationPairAuthorized(auth, { workspaceIds: [location.workspaceId] })
    )
      return workspaceForbidden(c);
    if (
      location.workspaceId !== scope.workspaceId ||
      !prefixAllowed(env, location.prefix, scope.orgId)
    )
      return error(
        c,
        403,
        "forbidden",
        "key is outside the caller's workspace",
      );
    const owners = orgResolverFromEnv(env);
    if (!owners)
      return error(
        c,
        503,
        "not_configured",
        "Object ownership is not configured (PIPELINE_ORG_ID or PIPELINE_ORG_FROM_PREFIX)",
      );
    if ((await owners.orgFor(location)) !== scope.orgId)
      return error(
        c,
        403,
        "forbidden",
        "key is outside the caller's workspace",
      );
    return true as const;
  }

  return routes;
}

/**
 * A destination, when given, must survive the attr filter. Otherwise the event
 * would silently fall back to object storage (`destinationOf`) and count as a
 * put that never happened, so it is rejected instead.
 */
function validDestination(attrs: unknown): boolean {
  if (!attrs || typeof attrs !== "object" || Array.isArray(attrs)) return true;
  if (!("destination" in attrs)) return true;
  const destination = (attrs as Record<string, unknown>).destination;
  return (
    typeof destination === "string" &&
    isSafeIdentifier(destination) &&
    !PROTOTYPE_KEYS.has(destination)
  );
}

/**
 * Client attrs keep only numbers, booleans and identifier-shaped strings, so an
 * event can carry sizes, seqs, offsets, hashes and destination names but never
 * prose or content.
 */
function numericAndIdAttrs(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const attrs: Record<string, unknown> = {};
  // The keys that identify the destination and the work go first, so the cap
  // can never drop them and turn a legacy put into an object storage one.
  const entries = Object.entries(value).sort(
    ([a], [b]) => Number(KEY_ATTRS.has(b)) - Number(KEY_ATTRS.has(a)),
  );
  for (const [key, item] of entries.slice(0, 32)) {
    if (
      !/^[a-z0-9_.]{1,64}$/.test(key) ||
      RESERVED_ATTR_KEYS.has(key) ||
      PROTOTYPE_KEYS.has(key)
    )
      continue;
    if (typeof item === "number" && Number.isFinite(item)) attrs[key] = item;
    else if (typeof item === "boolean") attrs[key] = item;
    else if (
      typeof item === "string" &&
      isSafeIdentifier(item) &&
      !(key === "destination" && PROTOTYPE_KEYS.has(item))
    )
      attrs[key] = item;
  }
  return attrs;
}

/**
 * Operator-only dead letter list and replay. Not under `/v1` auth: it is gated by
 * `PIPELINE_OPS_SECRET` like the other internal routes, and spans workspaces.
 * Replay is a dry run unless the body says `"dryRun": false`.
 */
export function createPipelineOpsRoutes(deps: PipelineRouteDeps = defaultDeps) {
  const routes = new Hono<AppEnv>();

  routes.use("/internal/pipeline/*", async (c, next) => {
    const secret = c.env?.PIPELINE_OPS_SECRET;
    if (!secret)
      return error(c, 503, "not_configured", "PIPELINE_OPS_SECRET is not set");
    if (!(await sameSecret(c.req.header("Authorization"), `Bearer ${secret}`)))
      return error(c, 401, "unauthorized", "Unauthorized");
    await next();
  });

  routes.get("/internal/pipeline/dead-letters", async (c) => {
    const stage = c.req.query("stage");
    if (stage && !isPipelineStage(stage))
      return error(c, 400, "bad_request", "unknown stage");
    const traceId = c.req.query("traceId");
    const letters = await deps.deadLetters(c.env).list({
      ...(stage && isPipelineStage(stage) ? { stage } : {}),
      ...(traceId ? { traceId } : {}),
      states: ["dead", "replaying"],
      limit: Math.min(Number(c.req.query("limit")) || 100, 1000),
    });
    return c.json({
      deadLetters: letters.map(({ message: _message, ...letter }) => letter),
    });
  });

  /**
   * Keyed lookup, raw → telemetry: the tokens that stand for these machine and
   * session ids in logs, OTLP and Sentry, so their telemetry can be searched.
   */
  routes.post("/internal/pipeline/telemetry-ids", async (c) => {
    const key = loggerEnvFromBindings(c.env).telemetryIdKey;
    if (!key)
      return error(
        c,
        503,
        "not_configured",
        "PIPELINE_TELEMETRY_ID_KEY is not set; telemetry carries no machine or session ids",
      );
    let body: { machineIds?: unknown; sessionIds?: unknown };
    try {
      body = await c.req.json();
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    // Positions are the request's: a non-string entry is refused where it is,
    // never dropped (which would shift every later token and error position).
    const ids = (value: unknown, name: string): string[] | string => {
      if (value === undefined) return [];
      if (!Array.isArray(value)) return `${name} must be an array`;
      const bad = value.findIndex((id) => typeof id !== "string");
      return bad < 0 ? (value as string[]) : `${name}[${bad}] is not a string`;
    };
    const machineIds = ids(body?.machineIds, "machineIds");
    const sessionIds = ids(body?.sessionIds, "sessionIds");
    if (typeof machineIds === "string")
      return error(c, 400, "bad_request", machineIds);
    if (typeof sessionIds === "string")
      return error(c, 400, "bad_request", sessionIds);
    if (!machineIds.length && !sessionIds.length)
      return error(c, 400, "bad_request", "machineIds or sessionIds required");
    // Never echoed back, not even in the error: a secret-shaped id is refused
    // by position.
    const unsafe = [
      ...machineIds.map((id, index) => [id, `machineIds[${index}]`] as const),
      ...sessionIds.map((id, index) => [id, `sessionIds[${index}]`] as const),
    ].find(([id]) => !isSafeIdentifier(id));
    if (unsafe)
      return error(
        c,
        400,
        "bad_request",
        `${unsafe[1]} is not an identifier (1-256 identifier characters, no secret-shaped values)`,
      );
    if (machineIds.length + sessionIds.length > MAX_TELEMETRY_LOOKUPS)
      return error(
        c,
        400,
        "bad_request",
        `at most ${MAX_TELEMETRY_LOOKUPS} ids per request`,
      );
    // Tokens in request order, so the response never repeats a raw id.
    const tokens = (kind: "machine" | "session", list: string[]) =>
      list.map((id) => telemetryIdHash(key, kind, id));
    return c.json({
      machines: tokens("machine", machineIds),
      sessions: tokens("session", sessionIds),
    });
  });

  /**
   * Keyed lookup, telemetry → raw: a hashed log line or error report still
   * carries `trace_id`, `org_id` and `workspace_id` raw; this returns that
   * trace's events from service-role storage, raw machine and session ids
   * included. Operators only (the ops secret), any tenant.
   */
  routes.get("/internal/pipeline/traces/:traceId", async (c) => {
    const traceId = c.req.param("traceId").toLowerCase();
    if (!isTraceId(traceId))
      return error(c, 400, "bad_request", "traceId must be a UUID");
    const orgId = c.req.query("orgId");
    const workspaceId = c.req.query("workspaceId");
    if (!orgId || !workspaceId)
      return error(c, 400, "bad_request", "orgId and workspaceId are required");
    try {
      const { events, truncated } = await deps
        .sink(c.env ?? {})
        .byTrace({ orgId, workspaceId }, traceId);
      return c.json(summarizeTrace(traceId, events, truncated));
    } catch (failure) {
      return failure instanceof PipelineStoreUnavailable
        ? error(
            c,
            503,
            "not_configured",
            "Pipeline event store is not configured",
          )
        : error(
            c,
            503,
            "pipeline_store_unavailable",
            "Pipeline event store is temporarily unavailable",
          );
    }
  });

  routes.post("/internal/pipeline/dead-letters/replay", async (c) => {
    let body: Record<string, unknown> = {};
    try {
      const text = await c.req.text();
      body = text ? JSON.parse(text) : {};
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    if (body.stage !== undefined && !isPipelineStage(body.stage))
      return error(c, 400, "bad_request", "unknown stage");
    if (body.traceId !== undefined && !isTraceId(body.traceId))
      return error(c, 400, "bad_request", "traceId must be a UUID");
    const env = c.env;
    const result = await replayDeadLetters(
      {
        deadLetters: deps.deadLetters(env),
        sink: deps.sink(env),
        send: (stage, message) => deps.send(env, stage, message),
        logger: loggerEnvFromBindings(env),
      },
      {
        stage: body.stage as PipelineStage | undefined,
        traceId: body.traceId as string | undefined,
        workspaceId:
          typeof body.workspaceId === "string" ? body.workspaceId : undefined,
        limit: typeof body.limit === "number" ? body.limit : undefined,
        dryRun: body.dryRun !== false,
        force: body.force === true,
      },
    );
    return c.json(result);
  });

  return routes;
}

async function sameSecret(given: string | undefined, expected: string) {
  if (!given) return false;
  const encoder = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(given)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const left = new Uint8Array(a);
  const right = new Uint8Array(b);
  let diff = 0;
  for (let index = 0; index < left.length; index += 1)
    diff |= left[index]! ^ right[index]!;
  return diff === 0;
}

/** The largest source range one chunk POST accepts (the client seals at 8 MiB). */
export const MAX_CHUNK_BYTES = 16 * 1024 * 1024;

/** The key prefix the server writes a tenant's raw objects under. */
function deploymentPrefix(
  env: Pick<Bindings, "PIPELINE_ORG_FROM_PREFIX" | "PIPELINE_KEY_PREFIX">,
  orgId: string,
) {
  const base = (env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
  if (env.PIPELINE_ORG_FROM_PREFIX === "true")
    return base ? `${base}/${orgId}` : orgId;
  return base || undefined;
}

export const pipelineRoutes = createPipelineRoutes();
export const pipelineOpsRoutes = createPipelineOpsRoutes();
