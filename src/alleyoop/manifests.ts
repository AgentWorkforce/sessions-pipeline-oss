import { Hono, type Context, type MiddlewareHandler } from "hono";
import { createMiddleware } from "hono/factory";
import {
  backfillBaseOf,
  isBackfillIdentity,
  isBaseReadIdentity,
  backfillBaseVersion,
  migrationBackfillIssue,
  migrationGrantAllowed,
  migrationManifestProvenance,
  migrationPairAuthorized,
  migrationSessionMatches,
  migrationUploadMatches,
  migrationWorkKey,
  sqlMigrationAssertionLedger,
  type TransactionalSqlExecutor,
  type MigrationAssertionClaims,
  type MigrationAssertionLedger,
  type MigrationConsumeResult,
} from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  backfillWriteIssues,
  adoptionWriteIssues,
  BACKFILL_ACTIVATION_STAGE,
  backfillActivationDelay,
  backfillActivationLimits,
  backfillActivationWorkKey,
  buildObjectLandedMessage,
  deleteParkedSession,
  expireParkedSession,
  isObjectStoreError,
  META_PARKED_AT,
  parkedManifestKey as parkedKey,
  writerUnderstandsBackfill,
  objectKey,
  OBJECT_LANDED_STAGE,
  orgResolverFromEnv,
  parseManifest,
  parseSessionKey,
  postgresPipelineQueue,
  prefixAllowed,
  rawChunkKey,
  readCapped,
  telemetryIdHash,
  telemetryIdKey,
  type AppEnv,
  type Bindings,
  type ManifestReader,
  type ManifestV1,
  type ObjectVersion,
  type ObjectLandedPayload,
  type PipelineStage,
  type RawStore,
  type StageMessage,
  SCRUB_RULESET_VERSION,
} from "../shared/vendor.ts";
import { enrichMessage } from "../shared/enrich.ts";
import { errorFields, log } from "../shared/log.ts";
import { createHash } from "node:crypto";

const sha256Hex = async (text: string) =>
  createHash("sha256").update(text).digest("hex");

/**
 * The ingest's migration ledger over HoopDb: `sessions.migration_assertion_uses`
 * insert, enrichMessage, and the `sessions.pipeline_jobs` insert, all on the
 * same transaction `tx`. `around` wraps the enqueue (the ingest's span).
 */
export function courierMigrationLedger(
  sql: TransactionalSqlExecutor,
  reader: ManifestReader,
  around: (
    stage: PipelineStage,
    message: StageMessage,
    work: () => Promise<void>,
  ) => Promise<void> = (_stage, _message, work) => work(),
): MigrationAssertionLedger {
  return sqlMigrationAssertionLedger(sql, {
    enqueue: (tx, stage, message) =>
      around(stage, message, async () => {
        await postgresPipelineQueue(tx).send(
          await enrichMessage({ ...message, stage }, reader),
        );
      }),
  });
}

const MAX_MANIFEST_BYTES = 8 * 1024 * 1024;
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

interface ManifestRoutesDeps {
  manifests: ManifestReader;
  raw: RawStore;
  send(
    env: Bindings,
    stage: typeof OBJECT_LANDED_STAGE,
    message: StageMessage,
  ): Promise<void>;
  /** Readers with no valid attestation. Absent means none are ready: fail closed. */
  missingReaders?: () => Promise<string[]>;
  /** Records this deployment's first activation once, and returns it. Absent: shut. */
  firstClaimlessActivation?: FirstClaimlessActivation;
  /** The audit and meter for the claimless window. Absent: the window stays shut. */
  recordClaimless?: (use: ClaimlessUse) => Promise<void>;
  /** Durable delayed requeue of a refused backfill. */
  requeueBackfill?: (
    env: Bindings,
    message: StageMessage,
    delaySeconds: number,
  ) => Promise<void>;
  /**
   * Single-use ledger for RelayAuth migration assertions (relayauth#110). For
   * a migration commit it replaces `send`: the ledger row, the enriched
   * message (enrichMessage) and the `pipeline_jobs` insert run in ONE SQL
   * transaction (ingest.ts). Absent: migration assertions get 503
   * `migration_ledger_unavailable`.
   */
  migrationLedger?: MigrationAssertionLedger;
}

/**
 * Every mutating request a live token makes needs a signed machine claim, here,
 * before any route reads a body or stores a byte. It denies by default, so a
 * route added later is covered without knowing about this: only a route listed
 * here can name a machine, and the claim must equal it. Migration assertions are
 * bound to a grant and checked by their own admission. The manifest route also
 * checks the claim against the manifest's key and writer.
 */
export const liveWriteMachineGate: MiddlewareHandler<AppEnv> = async (
  c,
  next,
) => {
  if (c.req.method === "GET" || c.req.method === "HEAD") return next();
  const auth = c.get("auth") as Auth | undefined;
  if (auth?.migration) return next();
  const claim = auth ? machineClaim(auth) : undefined;
  const refused = () =>
    responseError(
      c,
      403,
      "machine_claim_refused",
      "the token is not bound to this machine",
    );
  if (claim === undefined) return refused();
  if (
    c.req.path.endsWith("/pipeline/chunks") &&
    c.req.query("machine") !== claim
  )
    return refused();
  if (c.req.path.endsWith("/pipeline/events")) {
    const body = (await c.req.raw
      .clone()
      .json()
      .catch(() => null)) as { machineId?: unknown } | null;
    if (body?.machineId !== claim) return refused();
  }
  return next();
};

export type FirstClaimlessActivation = (from: string) => Promise<string>;

/**
 * One read through the claimless window. `clientVersion` is what the client
 * says of itself: it is for the meter, and nothing may branch on it.
 */
export interface ClaimlessUse {
  orgId: string;
  caller: string;
  clientVersion: string;
}

async function auditClaimless(
  deps: ManifestRoutesDeps,
  auth: Auth,
  manifest: ManifestV1,
  client: string | undefined,
): Promise<boolean> {
  if (!deps.recordClaimless) return false;
  try {
    await deps.recordClaimless({
      orgId: auth.orgId,
      caller: `${auth.tokenSubject}:${auth.userId}`,
      clientVersion: (client ?? manifest.writer.plugin_version).slice(0, 128),
    });
    return true;
  } catch {
    return false;
  }
}

interface PostedManifest {
  key?: unknown;
  manifest?: unknown;
  previousVersion?: unknown;
}

type Auth = {
  userId: string;
  orgId: string;
  workspaceId?: string;
  historyOrigin?: string;
  tokenSubject: string;
  scopes: string[];
  claims: Record<string, unknown>;
  migration?: MigrationAssertionClaims;
};

const responseError = (
  c: Context<AppEnv>,
  status: 400 | 403 | 404 | 409 | 413 | 503,
  code: string,
  message: string,
) =>
  c.json(
    { error: { code, message }, correlationId: c.get("correlationId") ?? "" },
    status,
  );

function version(value: unknown): ObjectVersion | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return undefined;
  const v = value as Record<string, unknown>;
  const eTag = typeof v.eTag === "string" && v.eTag ? v.eTag : undefined;
  const generation =
    typeof v.version === "string" && v.version ? v.version : undefined;
  return eTag || generation
    ? {
        ...(eTag ? { eTag } : {}),
        ...(generation ? { version: generation } : {}),
      }
    : undefined;
}

function sameVersion(
  expected: ObjectVersion,
  actual: ObjectVersion | undefined,
): boolean {
  if (!actual) return false;
  if (expected.eTag && expected.eTag !== actual.eTag) return false;
  if (expected.version && expected.version !== actual.version) return false;
  return Boolean(expected.eTag || expected.version);
}

const MIGRATION_SUBJECT = "agent_relayhistory_migration";
const MIGRATION_TOKEN_CLASS = "migration_assertion";
const MIGRATION_ID = /^[A-Za-z0-9._:-]{1,255}$/;
const PLAN_DIGEST = /^[0-9a-f]{64}$/;

/**
 * The manifest identity of a verified RelayAuth migration assertion. The
 * assertion verifier has already checked the signature, audience and expiry;
 * this boundary additionally requires the grant and plan bindings E4/O4 uses
 * to fence a backfill epoch.
 */
function migrationWriter(auth: Auth): string | undefined {
  if (auth.tokenSubject !== MIGRATION_SUBJECT) return undefined;
  const meta = auth.claims.meta;
  if (!meta || typeof meta !== "object" || Array.isArray(meta))
    return undefined;
  const values = meta as Record<string, unknown>;
  const grantId = values.grantId;
  const planDigest = values.planDigest;
  if (
    values.tokenClass !== MIGRATION_TOKEN_CLASS ||
    typeof grantId !== "string" ||
    !MIGRATION_ID.test(grantId) ||
    typeof planDigest !== "string" ||
    !PLAN_DIGEST.test(planDigest)
  )
    return undefined;
  return `migration:${grantId}`;
}

const MACHINE_CLAIM = "meta.machineId";

/** The machine a live token is bound to, from a signed claim. */
function machineClaim(auth: Auth): string | undefined {
  const meta = auth.claims.meta;
  if (!meta || typeof meta !== "object" || Array.isArray(meta))
    return undefined;
  const value = (meta as Record<string, unknown>).machineId;
  return typeof value === "string" && value ? value : undefined;
}

/** The longest the legacy window may run, from activation to expiry. */
export const CLAIMLESS_WINDOW_MAX_MS = 72 * 60 * 60 * 1000;

/**
 * The window's configured dates, if it is configured at all. Shut unless a
 * deployment sets `MANIFEST_MACHINE_CLAIM=compat` with
 * `MANIFEST_MACHINE_CLAIM_COMPAT_FROM` and `_UNTIL` at most 72 hours apart, so a
 * new deployment requires the claim and unsetting any of them is the kill switch.
 */
export function claimlessWindowDates(
  env: Bindings,
): { from: string; until: string } | undefined {
  const vars = env as Record<string, unknown>;
  if (vars.MANIFEST_MACHINE_CLAIM !== "compat") return undefined;
  const from = String(vars.MANIFEST_MACHINE_CLAIM_COMPAT_FROM);
  const until = String(vars.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL);
  const start = Date.parse(from);
  const end = Date.parse(until);
  return Number.isFinite(start) &&
    Number.isFinite(end) &&
    end - start <= CLAIMLESS_WINDOW_MAX_MS
    ? { from, until }
    : undefined;
}

/**
 * The legacy window for couriers from before the machine claim. It is read-only
 * and version-only: no write passes through it. There is one activation for the
 * whole database, recorded once: FROM must equal it and UNTIL stay within 72
 * hours of it, so restated dates and new deployment labels open nothing, and if
 * that record cannot be read the window stays shut. While it
 * is open the system is not machine-bound. Delete this and its callers after
 * the rollout.
 */
export async function claimlessWindowOpen(
  env: Bindings,
  firstActivation: FirstClaimlessActivation | undefined,
  now = new Date(),
): Promise<boolean> {
  const dates = claimlessWindowDates(env);
  if (!dates || !firstActivation) return false;
  const at = now.getTime();
  if (at < Date.parse(dates.from) || at >= Date.parse(dates.until))
    return false;
  try {
    const first = Date.parse(await firstActivation(dates.from));
    return (
      first === Date.parse(dates.from) &&
      Date.parse(dates.until) <= first + CLAIMLESS_WINDOW_MAX_MS
    );
  } catch {
    return false;
  }
}

/**
 * A live write's machine binding: claim, key `m=` and writer agree. Every live
 * write needs it, because the claimless window is read-only: an audit row does
 * not make a claimless mutation acceptable. Migration assertions are bound to
 * a grant instead.
 */
function machineClaimIssue(
  manifest: ManifestV1,
  auth: Auth,
): string | undefined {
  if (auth.tokenSubject === MIGRATION_SUBJECT) return undefined;
  const claim = machineClaim(auth);
  if (claim === undefined) return `${MACHINE_CLAIM} is required`;
  return claim === manifest.session.machine_id &&
    claim === manifest.writer.delivery_instance_id
    ? undefined
    : `${MACHINE_CLAIM} does not match the manifest`;
}

/** The session's writer and every reader must be ready for the new epochs. */
async function activationIssue(
  existing: ManifestV1,
  incoming: ManifestV1,
  missing: () => Promise<string[]>,
): Promise<string | undefined> {
  const adds = incoming.raw.epochs.slice(existing.raw.epochs.length);
  const backfill = adds.some((epoch) => epoch.reason === "backfill");
  const adoption = adds.some((epoch) => epoch.reason === "adopted");
  if (!backfill && !adoption) return undefined;
  if (!writerUnderstandsBackfill(adoption ? incoming : existing))
    return "the session's writer is not a known release";
  const readers = await missing();
  return readers.length
    ? `readers are not attested: ${readers.join(", ")}`
    : undefined;
}

/**
 * A live client that meets a cold export but cannot take it over yet: it is not
 * a known release, or the gate is closed. It must not stall on 409s or touch
 * the export, so its manifest is held beside it. Readers never see a parked
 * manifest: nothing is enqueued, and the session stays the export until a
 * capable client takes over and re-reads its source.
 */
async function mustPark(
  existing: ManifestV1,
  incoming: ManifestV1,
  auth: Auth,
  missing: () => Promise<string[]>,
): Promise<boolean> {
  return (
    existing.raw.owner.startsWith("migration:") &&
    auth.tokenSubject !== MIGRATION_SUBJECT &&
    incoming.raw.owner === incoming.writer.delivery_instance_id &&
    incoming.writer.delivery_instance_id === incoming.session.machine_id &&
    (!writerUnderstandsBackfill(incoming) || (await missing()).length > 0)
  );
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => compareText(a, b))
        .map(([key, child]) => [key, canonical(child)]),
    );
  return value;
}

/** Locale-independent Unicode code-point order for canonical JSON keys. */
function compareText(left: string, right: string): number {
  const leftPoints = left[Symbol.iterator]();
  const rightPoints = right[Symbol.iterator]();
  while (true) {
    const leftPoint = leftPoints.next();
    const rightPoint = rightPoints.next();
    if (leftPoint.done || rightPoint.done)
      return leftPoint.done === rightPoint.done ? 0 : leftPoint.done ? -1 : 1;
    const leftValue = leftPoint.value.codePointAt(0)!;
    const rightValue = rightPoint.value.codePointAt(0)!;
    if (leftValue !== rightValue) return leftValue < rightValue ? -1 : 1;
  }
}

function sameJson(left: unknown, right: unknown): boolean {
  return JSON.stringify(canonical(left)) === JSON.stringify(canonical(right));
}

function hasPrefix<T>(before: readonly T[], after: readonly T[]): boolean {
  return (
    after.length >= before.length &&
    before.every((value, index) => sameJson(value, after[index]))
  );
}

function exportMarker(manifest: ManifestV1): boolean {
  const marker = manifest.extensions?.["client.relayhistory_export"];
  return (
    !!marker &&
    typeof marker === "object" &&
    !Array.isArray(marker) &&
    (marker as Record<string, unknown>).version === 1 &&
    (marker as Record<string, unknown>).source === "neon.delivery_records"
  );
}

/**
 * Enforce the raw-owner contract across manifest revisions. Normal live
 * writers may append only as the existing owner. A migration assertion may
 * create a cold export, or append exactly one `backfill` epoch while leaving
 * the live writer's owner token in place. The object-store version remains
 * the serialization fence for a concurrent live append.
 */
function manifestOwnerIssue(
  existing: ManifestV1 | undefined,
  incoming: ManifestV1,
  auth: Auth,
): string | undefined {
  const migration = migrationWriter(auth);
  if (auth.tokenSubject === MIGRATION_SUBJECT && !migration)
    return "migration assertion is not bound to a grant and plan";
  const raw = incoming.raw;
  if (!existing) {
    if (raw.owner !== incoming.writer.delivery_instance_id)
      return "raw owner and delivery writer differ";
    if (migration) {
      if (raw.owner !== migration)
        return "cold export is not owned by its migration grant";
      if (!exportMarker(incoming)) return "cold export marker is missing";
    } else if (
      incoming.extensions?.["client.relayhistory_export"] !== undefined
    )
      return "only a migration writer sets the export marker";
    return undefined;
  }

  if (
    existing.raw.owner.startsWith("migration:") &&
    raw.owner !== existing.raw.owner
  )
    return migration
      ? "an exporter does not take over"
      : adoptionWriteIssues(existing, incoming)[0];

  if (
    incoming.trace_id !== existing.trace_id ||
    incoming.created_at !== existing.created_at ||
    !sameJson(incoming.server, existing.server) ||
    raw.owner !== existing.raw.owner ||
    incoming.writer.delivery_instance_id !==
      existing.writer.delivery_instance_id ||
    raw.format !== existing.raw.format ||
    raw.compression !== existing.raw.compression ||
    !sameJson(raw.scrub, existing.raw.scrub) ||
    (exportMarker(existing) &&
      !sameJson(
        incoming.extensions?.["client.relayhistory_export"],
        existing.extensions?.["client.relayhistory_export"],
      )) ||
    !hasPrefix(existing.raw.epochs, raw.epochs) ||
    !hasPrefix(existing.raw.chunks, raw.chunks)
  )
    return "immutable manifest state changed";

  if (!migration) {
    if (raw.owner !== incoming.writer.delivery_instance_id)
      return "live writer does not own the raw stream";
    if (
      !sameJson(
        incoming.extensions?.["client.relayhistory_export"],
        existing.extensions?.["client.relayhistory_export"],
      )
    )
      return "only a migration writer sets the export marker";
    if (
      raw.epochs
        .slice(existing.raw.epochs.length)
        .some((epoch) => epoch.reason === "backfill")
    )
      return "live writer attempted to open a backfill epoch";
    return backfillWriteIssues(existing, incoming, "live")[0];
  }

  const addedEpochs = raw.epochs.slice(existing.raw.epochs.length);
  const addedChunks = raw.chunks.slice(existing.raw.chunks.length);
  const backfill = addedEpochs[0];
  if (
    !sameJson(incoming.session, existing.session) ||
    !sameJson(incoming.writer, existing.writer) ||
    raw.state !== existing.raw.state ||
    existing.raw.epochs.some((epoch) => epoch.reason === "backfill") ||
    addedEpochs.length !== 1 ||
    !backfill ||
    backfill.reason !== "backfill" ||
    backfill.opened_by !== migration ||
    backfill.first_seq !== existing.raw.chunk_count ||
    addedChunks.length === 0 ||
    addedChunks.some((chunk) => chunk.epoch !== backfill.epoch) ||
    !exportMarker(incoming)
  )
    return "migration is not an append-only backfill epoch";
  const issue = backfillWriteIssues(existing, incoming, migration)[0];
  if (issue) return issue;

  for (const [key, value] of Object.entries(existing.extensions ?? {})) {
    if (key === "client.relayhistory_export") continue;
    if (!sameJson(value, incoming.extensions?.[key]))
      return "migration changed another extension owner";
  }
  for (const key of Object.keys(incoming.extensions ?? {})) {
    if (
      key !== "client.relayhistory_export" &&
      !(key in (existing.extensions ?? {}))
    )
      return "migration added another extension owner's key";
  }
  return undefined;
}

/** Exact replay is idempotent, but it does not bypass migration identity. */
function migrationReplayIssue(manifest: ManifestV1, auth: Auth): boolean {
  const lastEpoch = manifest.raw.epochs.at(-1);
  const expected = manifest.raw.owner.startsWith("migration:")
    ? manifest.raw.owner
    : lastEpoch?.reason === "backfill"
      ? lastEpoch.opened_by
      : undefined;
  return expected !== undefined && migrationWriter(auth) !== expected;
}

/** Ids and keys only: the exporter holds the manifest and rebuilds it. */
function activationMessage(
  key: string,
  manifest: ManifestV1,
  auth: Auth,
): StageMessage {
  const meta = auth.claims.meta as Record<string, unknown>;
  const grantId =
    auth.migration?.grantId ??
    (migrationWriter(auth) ?? "").slice("migration:".length);
  const planDigest = auth.migration?.planDigest ?? String(meta.planDigest);
  return {
    v: 1,
    stage: BACKFILL_ACTIVATION_STAGE,
    ctx: {
      traceId: manifest.trace_id,
      orgId: auth.orgId,
      workspaceId: manifest.session.workspace_id,
    },
    workKey: backfillActivationWorkKey(key, grantId),
    revision: manifest.revision,
    payload: { manifestKey: key, grantId, planDigest, attempt: 1 },
  } as StageMessage;
}

function sessionUuid(manifest: ManifestV1): string | undefined {
  const ext = (manifest.extensions as Record<string, unknown> | undefined)?.[
    "96bulls"
  ];
  if (!ext || typeof ext !== "object") return undefined;
  const value = (ext as Record<string, unknown>).session_uuid;
  return typeof value === "string" && UUID.test(value)
    ? value.toLowerCase()
    : undefined;
}

function manifestMetadata(
  manifest: ManifestV1,
  env: Bindings,
): Record<string, string> {
  const key = telemetryIdKey(env.PIPELINE_TELEMETRY_ID_KEY);
  const uuid = sessionUuid(manifest);
  return {
    "trace-id": manifest.trace_id,
    revision: String(manifest.revision),
    ...(uuid ? { "session-uuid": uuid } : {}),
    ...(key
      ? {
          "machine-token": telemetryIdHash(
            key,
            "machine",
            manifest.session.machine_id,
          ),
          "session-token": telemetryIdHash(
            key,
            "session",
            manifest.session.session_id,
          ),
        }
      : {}),
  };
}

/**
 * Every chunk the manifest lists is a server-sealed object under the same
 * session prefix, INCLUDING the deployment key prefix of the manifest's own
 * key (`prefix`, already checked by `owns`). Without it, a deployment with
 * `PIPELINE_KEY_PREFIX` / `PIPELINE_ORG_FROM_PREFIX` looked for the chunks at
 * the unprefixed path and refused every commit.
 */
export async function chunksAreServerSealed(
  raw: RawStore,
  manifest: ManifestV1,
  prefix: string | undefined,
): Promise<boolean> {
  const scrub = manifest.raw.scrub;
  if (!scrub || scrub.ruleset !== "relayhistory-scrub" || scrub.version < 1)
    return false;
  const location = {
    ...(prefix ? { prefix } : {}),
    workspaceId: manifest.session.workspace_id,
    machineId: manifest.session.machine_id,
    harness: manifest.session.harness,
    sessionId: manifest.session.session_id,
    startDate: manifest.session.started_at.slice(0, 10),
  };
  for (let start = 0; start < manifest.raw.chunks.length; start += 32) {
    const batch = manifest.raw.chunks.slice(start, start + 32);
    const valid = await Promise.all(
      batch.map(async (chunk) => {
        const key = rawChunkKey(
          location,
          chunk.seq,
          chunk.start_offset,
          chunk.sha256,
          manifest.raw.format,
        );
        const meta = await raw.store.head(key);
        if (!meta) return false;
        return (
          meta.size === chunk.stored_bytes &&
          meta.metadata["content-sha256"] === chunk.stored_sha256 &&
          meta.metadata["raw-sha256"] === chunk.raw_sha256 &&
          Number(meta.metadata["raw-bytes"]) === chunk.stored_raw_bytes &&
          meta.metadata["scrub-ruleset"] === scrub.ruleset &&
          // raw.scrub.version is the LOWEST version over the chunks (the courier
          // and the export record the minimum), so a session spanning a rule-set
          // bump lists older and newer chunks: each must be at least the
          // manifest's version and no newer than this server's.
          Number(meta.metadata["scrub-version"]) >= scrub.version &&
          Number(meta.metadata["scrub-version"]) <= SCRUB_RULESET_VERSION &&
          meta.metadata["trace-id"] === manifest.trace_id
        );
      }),
    );
    if (valid.includes(false)) return false;
  }
  return true;
}

async function storedManifest(
  raw: RawStore,
  key: string,
  target = key,
): Promise<
  { manifest: ManifestV1; version: ObjectVersion; text: string } | undefined
> {
  try {
    const object = await raw.store.getObject(objectKey(target));
    const bytes = await readCapped(object.body, MAX_MANIFEST_BYTES);
    if (!bytes) throw new Error("manifest too large");
    const text = new TextDecoder().decode(bytes);
    return {
      manifest: await parseManifest(text, key),
      version: object.meta.version,
      text,
    };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return undefined;
    throw error;
  }
}

/** Reconcile a lost create/CAS race when the winner stored this exact retry. */
async function putManifestOrReplay(
  raw: RawStore,
  key: string,
  text: string,
  auth: Auth,
  put: () => Promise<{ version: ObjectVersion }>,
): Promise<ObjectVersion> {
  try {
    return (await put()).version;
  } catch (error) {
    if (
      !isObjectStoreError(error, "already_exists") &&
      !isObjectStoreError(error, "precondition_failed")
    )
      throw error;
    const winner = await storedManifest(raw, key);
    if (
      !winner ||
      winner.text !== text ||
      migrationReplayIssue(winner.manifest, auth)
    )
      throw error;
    return winner.version;
  }
}

export function createCourierManifestRoutes(deps: ManifestRoutesDeps) {
  const routes = new Hono<AppEnv>();

  const ledgerUnavailable = (c: Context<AppEnv>) =>
    responseError(
      c,
      503,
      "migration_ledger_unavailable",
      "The migration assertion ledger is not configured or not available",
    );
  const workspaceForbidden = (c: Context<AppEnv>) =>
    responseError(
      c,
      403,
      "migration_workspace_forbidden",
      "The upload's workspace and user are not the pair this migration assertion was signed for",
    );
  const grantForbidden = (c: Context<AppEnv>) =>
    responseError(
      c,
      403,
      "migration_grant_forbidden",
      "The migration assertion's grant is not accepted here, or its plan is not that grant's plan",
    );
  const uploadForbidden = (c: Context<AppEnv>) =>
    responseError(
      c,
      403,
      "migration_upload_forbidden",
      "The upload is not the one this migration assertion's signed upload identity names",
    );
  const replayed = (c: Context<AppEnv>) =>
    responseError(
      c,
      409,
      "migration_assertion_replayed",
      "This migration assertion was already used; retry with a fresh assertion",
    );

  /**
   * Migration-assertion admission, the same as relayhistory-cloud's
   * (routes/pipeline.ts) and with the same codes, ahead of any storage call
   * on the manifest commit and the status read (any method; requireAuth
   * already refuses every route but GET status and POST manifests): a ledger
   * (503), the signed pair (403 migration_workspace_forbidden), an accepted
   * grant and plan (403 migration_grant_forbidden), the requested key being
   * the signed upload identity's (status; 403 migration_upload_forbidden),
   * and not yet consumed (409 migration_assertion_replayed; a ledger error is
   * 503). Ordinary tokens pass through.
   */
  const migrationAdmission = createMiddleware<AppEnv>(async (c, next) => {
    const auth = c.get("auth") as Auth;
    if (!auth?.migration) return next();
    const ledger = deps.migrationLedger;
    if (!ledger) return ledgerUnavailable(c);
    if (!migrationPairAuthorized(auth)) return workspaceForbidden(c);
    if (!migrationGrantAllowed(c.env ?? {}, auth.migration))
      return grantForbidden(c);
    if (c.req.path.endsWith("/pipeline/manifests/status")) {
      const location = parseSessionKey(c.req.query("key") ?? "");
      if (
        !location ||
        location.rest !== "manifest.json" ||
        !migrationSessionMatches(auth, location)
      )
        return uploadForbidden(c);
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
  routes.use("/pipeline/manifests", migrationAdmission);
  routes.use("/pipeline/manifests/status", migrationAdmission);

  /**
   * A migration assertion's manifest: the target workspace is the signed pair's
   * (403 migration_workspace_forbidden), and the upload identity recomputed
   * from this key and manifest (session, state, chunk list, content hash
   * chain, entry digest) is the signed one, and the export marker's grant_id,
   * plan_digest, source_user_id and 96bulls.user_id are present and equal to
   * the signed values (403 migration_upload_forbidden). The copy the server
   * reads back must pass the same checks; the work key is then
   * `migrationWorkKey`: the signed key and entry digest.
   */
  const migrationManifestIssue = async (
    c: Context<AppEnv>,
    auth: Auth,
    location: NonNullable<ReturnType<typeof parseSessionKey>>,
    manifest: ManifestV1,
  ) => {
    if (
      !migrationPairAuthorized(auth, {
        workspaceIds: [location.workspaceId, manifest.session.workspace_id],
      })
    )
      return workspaceForbidden(c);
    // v1: the whole session must be the signed entry. v2 (a backfill epoch)
    // is checked against the stored base once it has been read (POST).
    if (
      !isBackfillIdentity(auth.migration!.upload) &&
      !(await migrationUploadMatches(auth, location, manifest))
    )
      return uploadForbidden(c);
    // 96bulls.user_id (the session's user downstream) must be the signed source user.
    if (migrationManifestProvenance(auth, manifest)) return uploadForbidden(c);
    return undefined;
  };

  /**
   * The storage key of a courier request. The deployment prefix is applied
   * SERVER-SIDE from the authenticated org and the deployment config
   * (`PIPELINE_KEY_PREFIX`, `PIPELINE_ORG_FROM_PREFIX`): the courier posts
   * and polls the canonical unprefixed `v1/...` key, exactly as the chunk
   * route chooses the prefix itself. A client-supplied prefix is accepted
   * only when it is exactly the server's (the chunk response's key); any
   * other is refused by `owns`.
   */
  const storageKey = (c: Context<AppEnv>, key: string) => {
    const parsed = parseSessionKey(key);
    if (!parsed || parsed.prefix !== undefined) return key;
    const env = c.env ?? {};
    const auth = c.get("auth") as Auth;
    const base = (env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
    const prefix =
      env.PIPELINE_ORG_FROM_PREFIX === "true"
        ? base
          ? `${base}/${auth.orgId}`
          : auth.orgId
        : base;
    return prefix ? `${prefix}/${key}` : key;
  };

  const owns = async (
    c: Context<AppEnv>,
    location: NonNullable<ReturnType<typeof parseSessionKey>>,
    scope = "rth:sync",
  ) => {
    const auth = c.get("auth") as Auth;
    const env = c.env ?? {};
    if (!auth.scopes.includes(scope))
      return responseError(c, 403, "forbidden", `${scope} scope is required`);
    if (
      auth.migration &&
      !migrationPairAuthorized(auth, { workspaceIds: [location.workspaceId] })
    )
      return workspaceForbidden(c);
    const workspaceId = auth.workspaceId || "default";
    if (
      location.workspaceId !== workspaceId ||
      !prefixAllowed(env, location.prefix, auth.orgId)
    )
      return responseError(
        c,
        403,
        "forbidden",
        "key is outside the caller's workspace",
      );
    const owners = orgResolverFromEnv(env);
    if (!owners)
      return responseError(
        c,
        503,
        "not_configured",
        "Object ownership is not configured",
      );
    if ((await owners.orgFor(location)) !== auth.orgId)
      return responseError(
        c,
        403,
        "forbidden",
        "key is outside the caller's workspace",
      );
    if (env.RELAYHISTORY_ORIGIN && !auth.historyOrigin)
      return responseError(
        c,
        403,
        "forbidden",
        "origin-bound courier assertion required",
      );
    return true as const;
  };

  /**
   * The host courier's manifest write boundary. Ownership is established
   * before any storage call; every referenced chunk must carry AlleyOop's
   * scrub metadata. Revision 1 is create-only, an identical retry is safe,
   * and later revisions require the exact prior object version plus the same
   * delivery writer. The authoritative stored copy is then enqueued.
   */
  routes.post("/pipeline/manifests", async (c) => {
    let body: PostedManifest;
    try {
      body = await c.req.json();
    } catch {
      return responseError(
        c,
        400,
        "bad_request",
        "Request body must be valid JSON",
      );
    }
    const key = storageKey(c, typeof body.key === "string" ? body.key : "");
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json")
      return responseError(
        c,
        400,
        "bad_request",
        "key must be a v1 session manifest key",
      );
    const owned = await owns(c, location);
    if (owned !== true) return owned;
    if (body.manifest === undefined)
      return responseError(c, 400, "bad_request", "manifest is required");
    let manifest: ManifestV1;
    try {
      manifest = await parseManifest(body.manifest, key);
    } catch {
      return responseError(c, 400, "bad_request", "manifest failed validation");
    }
    if (
      c.req.header("X-Session-Id") !== manifest.session.session_id ||
      c.req.header("X-Machine-Id") !== manifest.session.machine_id
    )
      return responseError(
        c,
        400,
        "bad_request",
        "session headers do not match the manifest",
      );
    const auth = c.get("auth") as Auth;
    const migration = auth.migration;
    if (migration) {
      const issue = await migrationManifestIssue(c, auth, location, manifest);
      if (issue) return issue;
    }
    if (machineClaimIssue(manifest, auth))
      return responseError(
        c,
        403,
        "machine_claim_refused",
        "the token is not bound to this machine and writer",
      );
    const encoded = new TextEncoder().encode(JSON.stringify(manifest));
    const encodedText = new TextDecoder().decode(encoded);
    if (encoded.byteLength > MAX_MANIFEST_BYTES)
      return responseError(
        c,
        413,
        "payload_too_large",
        "manifest is too large",
      );
    try {
      if (!(await chunksAreServerSealed(deps.raw, manifest, location.prefix)))
        return responseError(
          c,
          409,
          "chunks_unverified",
          "manifest chunks are not server-sealed",
        );
      const existing = await storedManifest(deps.raw, key);
      // A v2 (backfill) assertion appends exactly one signed backfill epoch
      // to exactly the signed live base; #42's owner fence below still runs.
      // Idempotency first: when the stored object is already byte-identical
      // to this manifest (a retry after a lost commit response, under a fresh
      // assertion with the same base), the append is checked against the
      // signed base only and the commit returns the original success; any
      // other manifest is still held to the stored-base comparison.
      if (migration && isBackfillIdentity(migration.upload)) {
        const identicalRetry = !!existing && existing.text === encodedText;
        if (
          await migrationBackfillIssue(auth, location, existing, manifest, {
            identicalRetry,
          })
        )
          return uploadForbidden(c);
        // On an identical retry the stored object is the appended one, so the
        // stored-base comparison (the only check of the signed base.version)
        // cannot run. Instead the historical `previousVersion` the commit was
        // made against must be present and resolve EXACTLY to the signed
        // base.version, before anything is consumed or written.
        if (identicalRetry) {
          const previous = version(body.previousVersion);
          if (
            !previous ||
            backfillBaseVersion(previous) !== migration.upload.base.version
          )
            return responseError(
              c,
              409,
              "revision_conflict",
              "manifest version or writer does not match",
            );
        }
        if (
          !identicalRetry &&
          (!existing ||
            !sameVersion(existing.version, version(body.previousVersion)))
        )
          return responseError(
            c,
            409,
            "revision_conflict",
            "manifest version or writer does not match",
          );
      }
      // No cron: a session's next write is what expires its parked manifest.
      await expireParkedSession(
        deps.raw.store,
        key,
        manifest.session.machine_id,
        new Date(),
      ).catch((err) => {
        // Best effort: the write proceeds, but a role without s3:DeleteObject
        // (or any store error) must be visible, not silently leave parked data.
        log.warn("parked session expiry failed", errorFields(err));
        return false;
      });
      const missing = deps.missingReaders ?? (async () => ["unconfigured"]);
      if (
        existing &&
        (await mustPark(
          existing.manifest,
          manifest,
          c.get("auth") as Auth,
          missing,
        ))
      ) {
        const target = parkedKey(key, manifest.session.machine_id);
        const held = await storedManifest(deps.raw, key, target);
        if (
          held &&
          held.text !== encodedText &&
          (!sameVersion(held.version, version(body.previousVersion)) ||
            manifest.revision !== held.manifest.revision + 1)
        )
          return responseError(
            c,
            409,
            "revision_conflict",
            "parked manifest version does not match",
          );
        const parked =
          held?.text === encodedText
            ? held.version
            : (
                await deps.raw.store.put(objectKey(target), encoded, {
                  ...(held ? { ifMatch: held.version } : { ifNoneMatch: true }),
                  contentType: "application/json",
                  metadata: {
                    ...manifestMetadata(manifest, c.env ?? {}),
                    [META_PARKED_AT]: new Date().toISOString(),
                  },
                })
              ).version;
        return c.json(
          {
            parked: true,
            revision: manifest.revision,
            scrubbed: true,
            version: parked,
          },
          202,
        );
      }
      let storedVersion: ObjectVersion;
      if (!existing) {
        if (manifest.revision !== 1)
          return responseError(
            c,
            409,
            "revision_conflict",
            "the first manifest revision must be 1",
          );
        if (manifestOwnerIssue(undefined, manifest, c.get("auth") as Auth))
          return responseError(
            c,
            409,
            "owner_conflict",
            "manifest owner or append fence does not match",
          );
        storedVersion = await putManifestOrReplay(
          deps.raw,
          key,
          encodedText,
          c.get("auth") as Auth,
          () =>
            deps.raw.store.put(objectKey(key), encoded, {
              ifNoneMatch: true,
              contentType: "application/json",
              metadata: manifestMetadata(manifest, c.env ?? {}),
            }),
        );
      } else if (existing.text === encodedText) {
        if (migrationReplayIssue(existing.manifest, c.get("auth") as Auth))
          return responseError(
            c,
            409,
            "owner_conflict",
            "manifest owner or append fence does not match",
          );
        storedVersion = existing.version;
      } else {
        const previous = version(body.previousVersion);
        if (
          !sameVersion(existing.version, previous) ||
          manifest.revision !== existing.manifest.revision + 1
        )
          return responseError(
            c,
            409,
            "revision_conflict",
            "manifest version or writer does not match",
          );
        if (
          manifestOwnerIssue(existing.manifest, manifest, c.get("auth") as Auth)
        )
          return responseError(
            c,
            409,
            "owner_conflict",
            "manifest owner or append fence does not match",
          );
        const inactive = await activationIssue(
          existing.manifest,
          manifest,
          missing,
        );
        if (inactive) {
          const limits = backfillActivationLimits(
            (c.env ?? {}) as Record<string, unknown>,
          );
          const delay = backfillActivationDelay(1, limits);
          await deps.requeueBackfill?.(
            c.env ?? {},
            activationMessage(key, manifest, c.get("auth") as Auth),
            delay,
          );
          c.header("Retry-After", String(delay));
          return responseError(c, 409, "activation_required", inactive);
        }
        storedVersion = await putManifestOrReplay(
          deps.raw,
          key,
          encodedText,
          c.get("auth") as Auth,
          () =>
            deps.raw.store.put(objectKey(key), encoded, {
              ifMatch: existing.version,
              contentType: "application/json",
              metadata: manifestMetadata(manifest, c.env ?? {}),
            }),
        );
      }
      // The takeover re-read the source: the parked copy has done its job.
      if (
        existing?.manifest.raw.owner.startsWith("migration:") &&
        !manifest.raw.owner.startsWith("migration:")
      )
        await deleteParkedSession(
          deps.raw.store,
          key,
          manifest.session.machine_id,
        ).catch((err) =>
          log.warn("parked session cleanup failed", errorFields(err)),
        );
      const read = await deps.manifests.read(key);
      if (!read) throw new Error("manifest disappeared after write");
      // The authoritative storage read must agree with what was verified.
      if (migration) {
        const issue = await migrationManifestIssue(
          c,
          auth,
          location,
          read.manifest,
        );
        if (issue) return issue;
        // v2: the stored copy is the appended manifest just written.
        if (
          isBackfillIdentity(migration.upload) &&
          (read.manifest.revision !== manifest.revision ||
            read.manifest.raw.content_sha256 !== manifest.raw.content_sha256)
        )
          return uploadForbidden(c);
      }
      const built = buildObjectLandedMessage({
        landed: {
          key,
          provider: deps.manifests.provider,
          ...(deps.manifests.bucket ? { bucket: deps.manifests.bucket } : {}),
        },
        orgId: (c.get("auth") as Auth).orgId,
        manifest: read.manifest,
        read,
      });
      if (built.status !== "enqueue")
        return responseError(c, 400, "bad_request", "key is not a manifest");
      // Signed-only work key for a migration commit: the signed key and entry digest.
      // Signed-only work key; the payload's `migration` field tells the
      // object-landed consumer to expect it (stages.ts objectLandedWorkKey).
      if (migration) {
        built.message.workKey = migrationWorkKey(migration.upload);
        (built.message.payload as ObjectLandedPayload).migration = {
          key: migration.upload.key,
          entry_sha256: migration.upload.entry_sha256,
          ...(isBackfillIdentity(migration.upload)
            ? { base_revision: migration.upload.base.revision }
            : {}),
        };
      }
      if (migration) {
        // The ledger row and the enriched pipeline_jobs row commit in one SQL
        // transaction, or neither does (fail closed: 503, nothing consumed).
        // The object write above is idempotent: a retry of the identical
        // manifest takes the "identical" branch and enqueues then.
        const ledger = deps.migrationLedger;
        if (!ledger) return ledgerUnavailable(c);
        let outcome: MigrationConsumeResult;
        try {
          outcome = await ledger.consume(
            {
              jti: migration.jti,
              nonce: migration.nonce,
              grantId: migration.grantId,
              planDigest: migration.planDigest,
              planEntryDigest: migration.upload.entry_sha256,
              expiresAt: migration.expiresAt,
              orgId: auth.orgId,
              workspaceId: location.workspaceId,
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
        await deps.send(c.env ?? {}, OBJECT_LANDED_STAGE, built.message);
      }
      return c.json(
        {
          workKey: built.message.workKey,
          traceId: built.message.ctx.traceId,
          revision: built.message.revision,
          scrubbed: built.message.payload.scrubbed,
          version: storedVersion,
        },
        202,
      );
    } catch (error) {
      if (
        isObjectStoreError(error, "already_exists") ||
        isObjectStoreError(error, "precondition_failed")
      )
        return responseError(
          c,
          409,
          "revision_conflict",
          "manifest changed concurrently",
        );
      return responseError(
        c,
        503,
        "storage_unavailable",
        "Object storage is temporarily unavailable",
      );
    }
  });

  /**
   * The v2 pre-mint base read (issuer spec section 4): only the base-read
   * capability, only exactly `GET ...?key=<signed unprefixed key>`, default
   * off (`RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ`). One storage snapshot,
   * validated, then ONE transaction consumes the capability and appends the
   * audit row; the 7 base fields are returned only after it commits.
   */
  // Exactly one trailing slash is stripped (spec section 6): both spellings
  // reach the same handler; no other variant does.
  const baseRead = async (c: Context<AppEnv>) => {
    const env = c.env ?? {};
    if (env.RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ !== "true")
      return responseError(c, 404, "not_found", "no route");
    const auth = c.get("auth") as Auth;
    const migration = auth?.migration;
    if (!migration || !isBaseReadIdentity(migration.upload))
      return responseError(
        c,
        403,
        "migration_token_class_forbidden",
        "only a migration base-read capability may read a base",
      );
    const read = migration.upload;
    const params = [...new URL(c.req.url).searchParams.keys()];
    if (params.length !== 1 || params[0] !== "key")
      return responseError(
        c,
        400,
        "bad_request",
        "exactly one query parameter, key, is allowed",
      );
    if (c.req.query("key") !== read.key) return uploadForbidden(c);
    const ledger = deps.migrationLedger;
    if (!ledger) return ledgerUnavailable(c);
    if (!migrationPairAuthorized(auth)) return workspaceForbidden(c);
    if (!migrationGrantAllowed(env, migration)) return grantForbidden(c);
    const key = storageKey(c, read.key);
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json")
      return uploadForbidden(c);
    if (!migrationSessionMatches(auth, location)) return uploadForbidden(c);
    const owned = await owns(c, location, "rth:migration_base_read");
    if (owned !== true) return owned;
    let snapshot: Awaited<ReturnType<typeof storedManifest>>;
    try {
      snapshot = await storedManifest(deps.raw, key);
    } catch (error) {
      // A stored object that is not a valid manifest (bad JSON, failed
      // validation, an unsupported schema, oversized) can never be a base:
      // 409, not a retryable 503. Only real storage unavailability is 503.
      if (
        isObjectStoreError(error, "invalid_argument") ||
        isObjectStoreError(error, "unsupported") ||
        (error instanceof Error && error.message === "manifest too large")
      )
        return responseError(
          c,
          409,
          "migration_base_unusable",
          "the stored manifest is not a valid manifest",
        );
      return responseError(
        c,
        503,
        "storage_unavailable",
        "Object storage is temporarily unavailable",
      );
    }
    if (!snapshot)
      return responseError(c, 404, "not_found", "no live manifest at this key");
    const session = snapshot.manifest.session;
    if (
      session.machine_id !== read.session.machine_id ||
      session.harness !== read.session.harness ||
      session.session_id !== read.session.session_id ||
      session.started_at !== read.session.started_at
    )
      return uploadForbidden(c);
    const base = backfillBaseOf(snapshot);
    if (typeof base === "string")
      return responseError(c, 409, "migration_base_unusable", base);
    let outcome: MigrationConsumeResult;
    try {
      outcome = await ledger.consumeBaseRead(
        {
          jti: migration.jti,
          nonce: migration.nonce,
          grantId: migration.grantId,
          planDigest: migration.planDigest,
          planEntryDigest: read.entry_sha256,
          orgId: auth.orgId,
          workspaceId: location.workspaceId,
          userId: auth.userId,
          expiresAt: migration.expiresAt,
        },
        // Minimized: the key's digest, never its machine or session ids.
        `${await sha256Hex(read.key)}@base_read@r${base.revision}`,
      );
    } catch {
      return ledgerUnavailable(c);
    }
    if (outcome === "replayed") return replayed(c);
    if (outcome === "grant_mismatch") return grantForbidden(c);
    return c.json({ base }, 200);
  };
  routes.get("/pipeline/manifests/base", baseRead);
  routes.get("/pipeline/manifests/base/", baseRead);

  /**
   * Startup reconciliation: version, scoped to the stored machine/writer. With
   * `manifest=1` also the stored copy from the same read, for a compare-and-swap
   * retry: a writer that lost a race edits this, never its own local rebuild.
   * That form is scoped to the machine only, so a client can see that another
   * writer owns the stream, and fence itself on the stored owner.
   */
  routes.get("/pipeline/manifests/status", async (c) => {
    const key = storageKey(c, c.req.query("key") ?? "");
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json")
      return c.body(null, 400);
    const owned = await owns(c, location);
    if (owned !== true)
      return (c.get("auth") as Auth).migration
        ? owned
        : c.body(null, owned.status as 403 | 503);
    try {
      const stored = await storedManifest(deps.raw, key);
      if (!stored) return c.body(null, 404);
      // A migration assertion reads only the status of a manifest of its own
      // grant, workspace, source user and plan.
      const auth = c.get("auth") as Auth;
      // A v2 (backfill) assertion may read its live base's version (the
      // session, grant and tenancy are already its own; the base has no
      // export marker yet).
      if (auth.migration && isBackfillIdentity(auth.migration.upload)) {
        if (
          !migrationPairAuthorized(auth, {
            workspaceIds: [
              location.workspaceId,
              stored.manifest.session.workspace_id,
            ],
          })
        )
          return workspaceForbidden(c);
      } else if (auth.migration) {
        const issue = await migrationManifestIssue(
          c,
          auth,
          location,
          stored.manifest,
        );
        if (issue) return issue;
      }
      const machine = stored.manifest.session.machine_id;
      // The machine comes from the stored manifest and the signed claim. The
      // query only has to agree: it proves nothing, and a token bound to
      // another machine learns nothing here.
      const claim = machineClaim(auth);
      if (
        c.req.query("machine") !== machine ||
        (claim !== undefined && claim !== machine)
      )
        return c.body(null, 403);
      // Without a claim, the legacy window gives the version alone, and only
      // if the read is on the record: an unaudited read is refused.
      if (claim === undefined && !auth.migration) {
        if (
          !(await claimlessWindowOpen(
            c.env ?? {},
            deps.firstClaimlessActivation,
          ))
        )
          return c.body(null, 403);
        if (
          !(await auditClaimless(
            deps,
            auth,
            stored.manifest,
            c.req.header("X-Relay-Client"),
          ))
        )
          return c.body(null, 503);
      }
      // A migration assertion never reads a client's parked manifest.
      if (
        auth.tokenSubject !== MIGRATION_SUBJECT &&
        stored.manifest.raw.owner.startsWith("migration:") &&
        c.req.query("manifest") !== "1"
      ) {
        const held = await storedManifest(
          deps.raw,
          key,
          parkedKey(key, machine),
        );
        if (held) return c.json({ version: held.version }, 200);
      }
      // The full manifest needs a verified machine claim, never the query's
      // word for it. Without one this stays the version-only, writer-scoped form.
      const bound = c.req.query("manifest") === "1" && claim === machine;
      if (
        !bound &&
        c.req.query("writer") !== stored.manifest.writer.delivery_instance_id
      )
        return c.body(null, 403);
      return c.json(
        {
          version: stored.version,
          ...(bound ? { manifest: stored.manifest } : {}),
        },
        200,
      );
    } catch {
      return c.body(null, 503);
    }
  });

  return routes;
}
