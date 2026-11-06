/**
 * RelayAuth migration assertions (`POST /v1/tokens/migration-assertion`;
 * relayauth #110, with the signed `meta.upload` identity from relayauth #111
 * @2adf414a7767d8cc3e268c6a4bb790d27447e9e4)
 * at the ingest. RelayAuth records neither `jti` nor `nonce`, so this service
 * makes each assertion single-use: chunk requests may reuse it (bounded, see
 * below) until it is consumed, and the manifest commit consumes it once.
 *
 * Authorization comes from the signed assertion itself, never from echoed
 * request or manifest fields, and never from a membership lookup. Live
 * workspace membership is NOT checked here: the grant operator checks it when
 * creating the grant, and relayauth #111 (pinned @2adf414a) mints only for
 * the grant's stored (source user, workspace) pairs:
 *  - the signed `wks` equals `meta.workspaceId` and the signed `sponsorId`
 *    equals `meta.sourceUserId` (else 403 `migration_workspace_forbidden`);
 *  - the signed `meta.upload` (an `UploadIdentity` relayauth #111 derives
 *    from the grant's stored plan; contract and vectors in 96BullSessions
 *    docs/contracts/relayhistory-export-plan-entry-v1.md) must name the
 *    signed grant, plan digest, org, workspace and user (canonical ids, no
 *    `relay_` alias). The ingest RECOMPUTES the identity from the actual
 *    request; any difference is 403 `migration_upload_forbidden`:
 *     - chunks: the request carries the plan entry in front of the chunk
 *       bytes (`X-Relay-Migration-Plan-Entry-Length`); its SHA-256 must be
 *       the signed `entry_sha256`, its key, tenancy and session must be the
 *       request's and the signed ones, and the chunk (seq, epoch, offset,
 *       byte count and the SHA-256 of the actual bytes) must be one of the
 *       entry's chunks. So seq is always below the entry's chunk count, and
 *       no chunk outside the entry is stored;
 *     - status: the requested key, machine, harness, session and date;
 *     - commit: the plan entry derived from the key and the manifest (chunk
 *       list, content hash chain, session, state) must hash to the signed
 *       `entry_sha256`, and the manifest's export marker (`grant_id`,
 *       `plan_digest`, `source_user_id`) and `96bulls.user_id` must be present
 *       and equal to the signed values (`migrationManifestProvenance`). The
 *       server reads the manifest back from storage and verifies that copy;
 *       the work key is `migrationWorkKey`: the signed key and entry digest
 *       (never the storage version or a client-posted value);
 *  - the grant and its plan must be one this ingest accepts
 *    (`RELAYHISTORY_MIGRATION_GRANTS`, else 403 `migration_grant_forbidden`).
 *
 * Chunks: at most one chunk per `(jti, seq)` (an identical repeat is
 * idempotent, any other chunk at that seq is 409 `migration_chunk_conflict`)
 * and at most `MAX_CHUNKS_PER_ASSERTION` chunks per assertion (403
 * `migration_chunk_limit`). The chunk route accepts up to 16 MiB per chunk
 * (the courier seals at 4 MiB), so 256 chunks is up to 4 GiB per assertion,
 * far more than a courier can send within its <= 65 second validity; the
 * courier re-mints for the rest of a session.
 *
 * What is atomic: the ledger row, the manifest commit's durable write (the
 * `object-landed` queue row) and the durable audit event
 * (`sessions.migration_ingest_audit`) are written in one SQL transaction, so
 * a crash leaves all or none. A Cloudflare Queue send cannot join a Postgres
 * transaction, so a deployment that enqueues to Cloudflare has no ledger and
 * refuses migration assertions (503 `migration_ledger_unavailable`).
 *
 * Retention, no cron: every ledger insert first deletes at most
 * `RETENTION_BATCH` replay rows whose `expires_at` (the token's `exp` plus
 * the 5 second skew) passed more than `RETENTION_MARGIN_SECONDS` ago. The
 * verifier refuses an expired token (401 `migration_assertion_expired`, after
 * its shape is validated) before the ledger is consulted, so a purged token
 * is never accepted again. Nonce uniqueness, though, holds only within that
 * window: once a row is purged, a NEW, valid token may carry the same nonce
 * and is accepted. The audit table is durable and is never purged this way.
 *
 * A retry after an ambiguous failure (the commit landed, the response was
 * lost) uses a fresh assertion and enqueues the same work key again; every
 * stage is idempotent by work key, so it is never applied twice. On a replay
 * or a grant mismatch the manifest object may already be written while
 * nothing is enqueued; an idempotent retry with a fresh assertion resolves it.
 */
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import { computeContentSha256 } from "../../../../object-store/src/manifest.js";
import type { AuthContext } from "../../env.js";
import type { StageMessage } from "./dlq.js";
import { manifestKey, type ManifestV1 } from "./object-layout.js";
import { postgresPipelineQueue, type SqlExecutor } from "./queue.js";
import type { PipelineStage } from "./trace.js";

/** What a verified migration assertion carries (`auth.migration`). */
export interface MigrationAssertionClaims {
  jti: string;
  nonce: string;
  grantId: string;
  planDigest: string;
  /**
   * `meta.upload`: the signed identity of the one upload this assertion is
   * for. v1 (`UploadIdentity`, a whole session) or v2
   * (`UploadIdentityV2`, one backfill epoch appended to a live manifest).
   */
  upload: UploadIdentity | UploadIdentityV2 | UploadIdentityBaseRead;
  /**
   * The token class: an upload assertion (`meta.upload`, v1 or v2 commit) or
   * the v2 base-read capability (`meta.read`). Optional for test fixtures;
   * the verifier always sets it.
   */
  capability?: "upload" | "base_read";
  /** `meta.sourceUserId`: the grant pair's user. */
  sourceUserId: string;
  /** `meta.workspaceId`: the grant pair's workspace. */
  workspaceId: string;
  /** Unix seconds after which no verifier accepts the token (`exp` plus skew). */
  expiresAt: number;
}

/** One consumption: the assertion plus the tenancy it was spent on. */
export interface MigrationAssertionUse {
  jti: string;
  nonce: string;
  grantId: string;
  planDigest: string;
  /** `meta.upload.entry_sha256`, stored as `plan_entry_digest`. */
  planEntryDigest: string;
  orgId: string;
  workspaceId: string;
  userId: string;
  /** Unix seconds; stored as `expires_at`, the earliest the row may be purged. */
  expiresAt: number;
}

/** One chunk write under an assertion (`sessions.migration_assertion_chunks`). */
export interface MigrationChunkUse {
  jti: string;
  seq: number;
  startOffset: number;
  /** SHA-256 of the chunk's source bytes (the chunk entry's `sha256`). */
  sha256: string;
  expiresAt: number;
}

/**
 * Whether a migration assertion may act on this target: the signed pair
 * (`wks`, `sponsorId`) equals the grant pair (`meta.workspaceId`,
 * `meta.sourceUserId`), and the target workspace and user (from the upload
 * key, the manifest and the resolved auth context) are that pair. RelayAuth
 * signs only pairs stored in the grant, so this is the whole authorization;
 * there is no membership lookup.
 */
export function migrationPairAuthorized(
  auth: Pick<AuthContext, "claims" | "userId" | "workspaceId" | "migration">,
  target: { workspaceIds?: readonly (string | undefined)[] } = {},
): boolean {
  const migration = auth.migration;
  if (!migration) return false;
  const wks = auth.claims.wks;
  const sponsorId = auth.claims.sponsorId;
  if (
    typeof wks !== "string" ||
    typeof sponsorId !== "string" ||
    normalizeRelayId(wks) !== normalizeRelayId(migration.workspaceId) ||
    normalizeRelayId(sponsorId) !== normalizeRelayId(migration.sourceUserId) ||
    !auth.workspaceId ||
    normalizeRelayId(auth.workspaceId) !== normalizeRelayId(wks) ||
    normalizeRelayId(auth.userId) !== normalizeRelayId(sponsorId)
  )
    return false;
  return (target.workspaceIds ?? []).every(
    (workspaceId) =>
      workspaceId !== undefined &&
      normalizeRelayId(workspaceId) === normalizeRelayId(wks),
  );
}

const GRANT_ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const DIGEST_SHAPE = /^[0-9a-f]{64}$/;

/**
 * The grants this ingest accepts (`RELAYHISTORY_MIGRATION_GRANTS`:
 * comma-separated `grantId=planDigest`, copied from the grant records). A
 * malformed entry is ignored, so it can only refuse, never admit.
 */
export function migrationGrantsFromEnv(env: {
  RELAYHISTORY_MIGRATION_GRANTS?: string;
}): ReadonlyMap<string, string> {
  const grants = new Map<string, string>();
  for (const entry of (env.RELAYHISTORY_MIGRATION_GRANTS ?? "").split(",")) {
    const [grantId, digest, ...rest] = entry.trim().split("=");
    if (
      rest.length === 0 &&
      grantId &&
      digest &&
      GRANT_ID_SHAPE.test(grantId) &&
      DIGEST_SHAPE.test(digest)
    )
      grants.set(grantId, digest);
  }
  return grants;
}

/**
 * Whether the assertion's signed `meta.grantId` is a grant this ingest
 * accepts and its signed `meta.planDigest` is that grant's plan.
 */
export function migrationGrantAllowed(
  env: { RELAYHISTORY_MIGRATION_GRANTS?: string },
  migration: Pick<MigrationAssertionClaims, "grantId" | "planDigest">,
): boolean {
  return (
    migrationGrantsFromEnv(env).get(migration.grantId) === migration.planDigest
  );
}

// --- The upload identity and plan entry (contract v1) -------------------
//
// docs/contracts/relayhistory-export-plan-entry-v1.md in 96BullSessions, with
// cross-repo vectors (test/fixtures/relayhistory-export-plan-entry-v1.vectors.json).
// The shapes are those of the #40 courier (derivePlanContract).

/** The session identity in an entry and in the signed upload identity. */
export interface UploadSession {
  machine_id: string;
  harness: string;
  session_id: string;
  started_at: string;
}

/** One chunk as a plan entry lists it (all known before upload). */
export interface PlanEntryChunk {
  seq: number;
  epoch: number;
  start_offset: number;
  bytes: number;
  /** SHA-256 of the chunk's source bytes (the chunk entry's `sha256`). */
  sha256: string;
  records: number;
}

/** A canonical, grant-bound plan entry: one manifest upload. */
export interface PlanEntry {
  grant_id: string;
  /** The v1 manifest key without any deployment prefix. */
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  state: "open" | "closed";
  chunks: PlanEntryChunk[];
  /** The manifest v1 `content_sha256` hash chain over `chunks`. */
  content_sha256: string;
  /** Sum of the chunks' `bytes`. */
  raw_bytes: number;
}

/**
 * The per-upload identity the issuer derives from the stored plan and signs
 * (`meta.upload`). Ids are in canonical form (no `relay_` prefix).
 */
export interface UploadIdentity {
  grant_id: string;
  /** The grant's plan digest (`exportPlanDigest`); equals the signed `meta.planDigest`. */
  plan_digest: string;
  /** The manifest key (the work key's object); the unprefixed v1 key. */
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  content_sha256: string;
  /** `sha256(canonical(entry))`, lowercase hex. */
  entry_sha256: string;
}

/**
 * One id normalization everywhere: a leading `relay_` is not part of the id
 * (as for every RelayAuth token's user, org and workspace).
 */
export function normalizeRelayId(value: string): string {
  return value.startsWith("relay_") ? value.slice("relay_".length) : value;
}

/**
 * Canonical JSON: object keys sorted (UTF-16 code unit order, as
 * `Array.prototype.sort`), undefined members dropped, no whitespace, every
 * other value as `JSON.stringify` writes it.
 */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .filter((key) => record[key] !== undefined)
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

/** `sha256(canonical(entry))`, lowercase hex. */
export function planEntrySha256(entry: PlanEntry): Promise<string> {
  return sha256Hex(canonicalJson(entry));
}

/**
 * A grant's plan digest (`meta.planDigest`, `RELAYHISTORY_MIGRATION_GRANTS`):
 * `sha256(canonical({v: 1, grant_id, exported_at, manifests}))` with the
 * entries sorted by key.
 */
export function exportPlanDigest(input: {
  grantId: string;
  exportedAt: string;
  entries: readonly PlanEntry[];
}): Promise<string> {
  const manifests = [...input.entries].sort((a, b) =>
    a.key < b.key ? -1 : a.key > b.key ? 1 : 0,
  );
  return sha256Hex(
    canonicalJson({
      v: 1,
      grant_id: input.grantId,
      exported_at: input.exportedAt,
      manifests,
    }),
  );
}

/** The upload identity of an entry under its grant's plan digest. */
export async function uploadIdentityOf(
  entry: PlanEntry,
  planDigest: string,
): Promise<UploadIdentity> {
  return {
    grant_id: entry.grant_id,
    plan_digest: planDigest,
    key: entry.key,
    org_id: entry.org_id,
    workspace_id: entry.workspace_id,
    source_user_id: entry.source_user_id,
    session: entry.session,
    content_sha256: entry.content_sha256,
    entry_sha256: await planEntrySha256(entry),
  };
}

/** The unprefixed v1 manifest key of a session location. */
export function sessionManifestKey(location: {
  workspaceId: string;
  machineId: string;
  harness: string;
  sessionId: string;
  startDate: string;
}): string {
  return manifestKey({
    workspaceId: location.workspaceId,
    machineId: location.machineId,
    harness: location.harness,
    sessionId: location.sessionId,
    startDate: location.startDate,
  });
}

/** The tenancy of an upload, from the signed claims only, normalized. */
function signedTenancy(
  auth: Pick<AuthContext, "claims" | "migration">,
):
  | Pick<
      UploadIdentity,
      "grant_id" | "org_id" | "workspace_id" | "source_user_id"
    >
  | undefined {
  const { org, wks, sponsorId } = auth.claims;
  if (
    !auth.migration ||
    typeof org !== "string" ||
    typeof wks !== "string" ||
    typeof sponsorId !== "string"
  )
    return undefined;
  return {
    grant_id: auth.migration.grantId,
    org_id: normalizeRelayId(org),
    workspace_id: normalizeRelayId(wks),
    source_user_id: normalizeRelayId(sponsorId),
  };
}

/**
 * The plan entry of an actual upload, DERIVED by the ingest: tenancy from the
 * signed claims, key from the upload's key, session and chunk list from the
 * validated manifest, content recomputed over the chunk list. Nothing echoed.
 * `undefined` when the manifest's declared summary disagrees with its chunks.
 */
export async function derivePlanEntry(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  manifest: Pick<ManifestV1, "session" | "raw">,
): Promise<PlanEntry | undefined> {
  const tenancy = signedTenancy(auth);
  return tenancy && planEntryFor(tenancy, location, manifest);
}

/** The plan entry of a manifest under a known tenancy. */
export async function planEntryFor(
  tenancy: Pick<
    UploadIdentity,
    "grant_id" | "org_id" | "workspace_id" | "source_user_id"
  >,
  location: Parameters<typeof sessionManifestKey>[0],
  manifest: Pick<ManifestV1, "session" | "raw">,
): Promise<PlanEntry | undefined> {
  const raw = manifest.raw;
  const content = await computeContentSha256(raw.chunks);
  if (content !== raw.content_sha256 || raw.chunks.length !== raw.chunk_count)
    return undefined;
  return {
    ...tenancy,
    key: sessionManifestKey(location),
    session: {
      machine_id: manifest.session.machine_id,
      harness: manifest.session.harness,
      session_id: manifest.session.session_id,
      started_at: manifest.session.started_at,
    },
    state: raw.state,
    chunks: raw.chunks.map((chunk) => ({
      seq: chunk.seq,
      epoch: chunk.epoch,
      start_offset: chunk.start_offset,
      bytes: chunk.bytes,
      sha256: chunk.sha256,
      records: chunk.records ?? 0,
    })),
    content_sha256: content,
    raw_bytes: raw.chunks.reduce((sum, chunk) => sum + chunk.bytes, 0),
  };
}

/** The signed identity names exactly the signed tenancy and plan (canonical ids). */
function sameTenancy(
  auth: Pick<AuthContext, "migration">,
  upload: UploadIdentity | UploadIdentityV2 | UploadIdentityBaseRead,
  tenancy: Pick<
    UploadIdentity,
    "grant_id" | "org_id" | "workspace_id" | "source_user_id"
  >,
): boolean {
  return (
    upload.grant_id === tenancy.grant_id &&
    upload.plan_digest === auth.migration?.planDigest &&
    upload.org_id === tenancy.org_id &&
    upload.workspace_id === tenancy.workspace_id &&
    upload.source_user_id === tenancy.source_user_id
  );
}

/**
 * Chunk and status requests: the request's session (key, machine, harness,
 * session id, date) and the signed tenancy must be the signed upload's.
 * This binds every key the token may write, fresh ones included, before the
 * first write.
 */
export function migrationSessionMatches(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
): boolean {
  const upload = auth.migration?.upload;
  const tenancy = signedTenancy(auth);
  if (!upload || !tenancy || !sameTenancy(auth, upload, tenancy)) return false;
  return (
    upload.key === sessionManifestKey(location) &&
    upload.session.machine_id === location.machineId &&
    upload.session.harness === location.harness &&
    upload.session.session_id === location.sessionId &&
    upload.session.started_at.slice(0, 10) === location.startDate
  );
}

/**
 * The manifest commit (and a status read of a stored manifest): the entry
 * derived from the actual upload must be the signed one, field by field and
 * by `entry_sha256`.
 */
export async function migrationUploadMatches(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  manifest: Pick<ManifestV1, "session" | "raw">,
): Promise<boolean> {
  const upload = auth.migration?.upload;
  // v1 only: a backfill (v2) identity never matches a whole-session commit.
  if (
    !upload ||
    isV2Identity(upload) ||
    !migrationSessionMatches(auth, location)
  )
    return false;
  const entry = await derivePlanEntry(auth, location, manifest);
  if (!entry) return false;
  const derived = await uploadIdentityOf(entry, upload.plan_digest);
  return (
    canonicalJson(derived.session) === canonicalJson(upload.session) &&
    derived.content_sha256 === upload.content_sha256 &&
    derived.entry_sha256 === upload.entry_sha256
  );
}

/**
 * Parses a signed `meta.upload`; `undefined` when it is not exactly that
 * shape, or when an id is not in canonical form (a `relay_` alias).
 */
export function parseUploadIdentity(
  value: unknown,
): UploadIdentity | UploadIdentityV2 | UploadIdentityBaseRead | undefined {
  if (isRecord(value) && "v" in value) return parseUploadIdentityV2(value);
  const record = (v: unknown) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? (v as Record<string, unknown>)
      : undefined;
  const u = record(value);
  const session = record(u?.session);
  const text = (v: unknown, max = 512) =>
    typeof v === "string" && v.length > 0 && v.length <= max;
  const exactKeys = (v: Record<string, unknown>, keys: string[]) =>
    Object.keys(v).sort().join(",") === [...keys].sort().join(",");
  if (
    !u ||
    !session ||
    // Exactly the contract's fields (as relayauth #111 verifies): nothing
    // unsigned can ride along.
    !exactKeys(u, [
      "grant_id",
      "plan_digest",
      "key",
      "org_id",
      "workspace_id",
      "source_user_id",
      "session",
      "content_sha256",
      "entry_sha256",
    ]) ||
    !exactKeys(session, [
      "machine_id",
      "harness",
      "session_id",
      "started_at",
    ]) ||
    !["grant_id", "key", "org_id", "workspace_id", "source_user_id"].every(
      (k) => text(u[k]),
    ) ||
    ["org_id", "workspace_id", "source_user_id"].some(
      (k) => normalizeRelayId(u[k] as string) !== u[k],
    ) ||
    typeof u.plan_digest !== "string" ||
    !DIGEST_SHAPE.test(u.plan_digest) ||
    !["machine_id", "harness", "session_id", "started_at"].every((k) =>
      text(session[k], 256),
    ) ||
    typeof u.content_sha256 !== "string" ||
    !DIGEST_SHAPE.test(u.content_sha256) ||
    typeof u.entry_sha256 !== "string" ||
    !DIGEST_SHAPE.test(u.entry_sha256)
  )
    return undefined;
  return {
    grant_id: u.grant_id as string,
    plan_digest: u.plan_digest,
    key: u.key as string,
    org_id: u.org_id as string,
    workspace_id: u.workspace_id as string,
    source_user_id: u.source_user_id as string,
    session: {
      machine_id: session.machine_id as string,
      harness: session.harness as string,
      session_id: session.session_id as string,
      started_at: session.started_at as string,
    },
    content_sha256: u.content_sha256,
    entry_sha256: u.entry_sha256,
  };
}

/** The header that carries the plan entry's byte length on a migration chunk request. */
export const PLAN_ENTRY_LENGTH_HEADER = "X-Relay-Migration-Plan-Entry-Length";
/** The largest plan entry a chunk request may carry in front of its bytes. */
export const MAX_PLAN_ENTRY_BYTES = 1024 * 1024;

const isRecord = (v: unknown): v is Record<string, unknown> =>
  !!v && typeof v === "object" && !Array.isArray(v);
const exactKeySet = (v: Record<string, unknown>, keys: readonly string[]) =>
  Object.keys(v).sort().join(",") === [...keys].sort().join(",");
const uint = (v: unknown) => Number.isSafeInteger(v) && (v as number) >= 0;

/**
 * Parses a plan entry carried by a chunk request: its bytes must be exactly
 * `canonicalJson(entry)` of an entry of exactly the contract's shape.
 */
export function parsePlanEntry(text: string): PlanEntry | undefined {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (!isRecord(value) || canonicalJson(value) !== text) return undefined;
  const entryKeys = [
    "grant_id",
    "key",
    "org_id",
    "workspace_id",
    "source_user_id",
    "session",
    "state",
    "chunks",
    "content_sha256",
    "raw_bytes",
  ];
  const chunkKeys = [
    "seq",
    "epoch",
    "start_offset",
    "bytes",
    "sha256",
    "records",
  ];
  const session = value.session;
  const chunks = value.chunks;
  if (
    !exactKeySet(value, entryKeys) ||
    !isRecord(session) ||
    !exactKeySet(session, [
      "machine_id",
      "harness",
      "session_id",
      "started_at",
    ]) ||
    !Object.values(session).every((v) => typeof v === "string") ||
    (value.state !== "open" && value.state !== "closed") ||
    !Array.isArray(chunks) ||
    !chunks.every(
      (c) =>
        isRecord(c) &&
        exactKeySet(c, chunkKeys) &&
        ["seq", "epoch", "start_offset", "bytes", "records"].every((k) =>
          uint(c[k]),
        ) &&
        typeof c.sha256 === "string" &&
        DIGEST_SHAPE.test(c.sha256),
    ) ||
    !["grant_id", "key", "org_id", "workspace_id", "source_user_id"].every(
      (k) => typeof value[k] === "string",
    ) ||
    typeof value.content_sha256 !== "string" ||
    !uint(value.raw_bytes)
  )
    return undefined;
  return value as unknown as PlanEntry;
}

/**
 * A migration chunk request, before anything is stored: the carried plan
 * entry is the signed one (by `entry_sha256`), names this request's key,
 * session and the signed tenancy, its content summary is consistent, and the
 * chunk (seq, epoch, offset, bytes, SHA-256 of the actual bytes) is one of
 * its chunks. Returns the matched chunk, or `undefined`.
 */
export async function migrationChunkMatches(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  chunk: { seq: number; epoch: number; startOffset: number; bytes: Uint8Array },
  entryText: string,
): Promise<PlanEntryChunk | undefined> {
  const upload = auth.migration?.upload;
  const tenancy = signedTenancy(auth);
  if (!upload || !tenancy || !migrationSessionMatches(auth, location))
    return undefined;
  // A base-read capability never writes.
  if (isBaseReadIdentity(upload)) return undefined;
  if (isBackfillIdentity(upload))
    return migrationBackfillChunkMatches(auth, location, chunk, entryText);
  if ((await sha256Hex(entryText)) !== upload.entry_sha256) return undefined;
  const entry = parsePlanEntry(entryText);
  if (
    !entry ||
    entry.key !== upload.key ||
    entry.grant_id !== tenancy.grant_id ||
    entry.org_id !== tenancy.org_id ||
    entry.workspace_id !== tenancy.workspace_id ||
    entry.source_user_id !== tenancy.source_user_id ||
    canonicalJson(entry.session) !== canonicalJson(upload.session) ||
    entry.content_sha256 !== upload.content_sha256 ||
    (await computeContentSha256(entry.chunks)) !== entry.content_sha256 ||
    entry.raw_bytes !== entry.chunks.reduce((n, c) => n + c.bytes, 0)
  )
    return undefined;
  const planned = entry.chunks.find((c) => c.seq === chunk.seq);
  if (
    !planned ||
    planned.epoch !== chunk.epoch ||
    planned.start_offset !== chunk.startOffset ||
    planned.bytes !== chunk.bytes.byteLength ||
    planned.sha256 !== (await sha256Hex(chunk.bytes))
  )
    return undefined;
  return planned;
}

/**
 * The pipeline work key of a migration commit: the SIGNED manifest key and
 * the SIGNED entry digest, `${upload.key}@${upload.entry_sha256}`. Nothing
 * client-posted and not the storage version feed it (the server still reads
 * the manifest back and verifies it before enqueueing). A retry of the same
 * planned upload with a fresh assertion has the same work key.
 */
export function migrationWorkKey(
  upload:
    | Pick<UploadIdentity, "key" | "entry_sha256">
    | Pick<UploadIdentityV2, "v" | "key" | "entry_sha256" | "base">,
): string {
  // v2: one backfill epoch on a base revision. The base revision is signed,
  // so a rebuild on a newer base (after a lost CAS race) is new work.
  if ("v" in upload && upload.v === 2)
    return `${upload.key}@${upload.entry_sha256}@r${upload.base.revision + 1}`;
  return `${upload.key}@${upload.entry_sha256}`;
}

/**
 * Manifest provenance beyond the entry digest (which already binds the chunk
 * list, content, session, state, key, grant, plan and tenancy): the manifest's
 * own source fields must be present and equal to the signed values.
 *  - `extensions["client.relayhistory_export"]`: `grant_id` = the signed
 *    grant, `plan_digest` = the signed plan digest, `source_user_id` = the
 *    signed canonical source user;
 *  - `extensions["96bulls"].user_id` (the session's user downstream) = the
 *    signed canonical source user.
 * Missing or different is a refusal. Returns the problem, or `undefined`.
 */
export function migrationManifestProvenance(
  auth: Pick<AuthContext, "claims" | "migration">,
  manifest: { extensions?: Record<string, unknown> | undefined },
): string | undefined {
  const tenancy = signedTenancy(auth);
  const upload = auth.migration?.upload;
  if (!tenancy || !upload) return "no signed tenancy";
  const marker = manifest.extensions?.["client.relayhistory_export"];
  if (!isRecord(marker)) return "the export marker is missing";
  if (marker.grant_id !== upload.grant_id)
    return "export marker grant_id is not the signed grant";
  if (marker.plan_digest !== upload.plan_digest)
    return "export marker plan_digest is not the signed plan";
  if (marker.source_user_id !== tenancy.source_user_id)
    return "export marker source_user_id is not the signed source user";
  const ext = manifest.extensions?.["96bulls"];
  if (!isRecord(ext)) return "the 96bulls extension is missing";
  if (ext.user_id !== tenancy.source_user_id)
    return "96bulls.user_id is not the signed source user";
  return undefined;
}

// --- Plan entry v2: one backfill epoch on a live manifest -----------------
//
// docs/contracts/relayhistory-export-plan-entry-v2.md in 96BullSessions, with
// vectors. v1 is unchanged. A v2 entry is stored in the grant's plan under
// `backfills`; it lists only the backfill's chunks, independent of where they
// land (#46: `seq` and the epoch number depend on the live manifest at append
// time). The signed v2 identity adds the BASE it appends to, which the issuer
// takes from the mint request and the ingest checks against storage.

/** A backfill chunk as a v2 entry lists it: offsets contiguous from 0. */
export interface BackfillEntryChunk {
  start_offset: number;
  bytes: number;
  /** SHA-256 of the chunk's source bytes. */
  sha256: string;
  records: number;
}

/** A canonical, grant-bound backfill entry (stored in the plan's `backfills`). */
export interface PlanEntryV2 {
  v: 2;
  kind: "backfill";
  grant_id: string;
  /** The live session's manifest key, without any deployment prefix. */
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  /** A backfill epoch needs an ndjson stream (#46). */
  format: "ndjson";
  chunks: BackfillEntryChunk[];
  /** `backfillSha256(chunks)`: #46's `backfillDigest`, independent of seq and epoch. */
  backfill_sha256: string;
  raw_bytes: number;
}

/** The live manifest a backfill appends to, as observed and signed at mint time. */
export interface BackfillBase {
  /** The stored manifest's `revision`; the backfill commits `revision + 1`. */
  revision: number;
  /** The stored object's version: its version id, else its ETag exactly as the store returns it. */
  version: string;
  /** The live `raw.owner` (never `migration:...`); unchanged by the append. */
  owner: string;
  /** The live `writer.delivery_instance_id`; unchanged by the append. */
  writer: string;
  /** The stored `raw.chunk_count`: the backfill epoch's `first_seq`. */
  chunk_count: number;
  /** The stored `raw.epoch`: the backfill epoch is `epoch + 1`. */
  epoch: number;
  /** The stored `raw.content_sha256`. */
  content_sha256: string;
}

/** The signed v2 identity (`meta.upload` with `v: 2`). */
export interface UploadIdentityV2 {
  v: 2;
  kind: "backfill";
  grant_id: string;
  plan_digest: string;
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  backfill_sha256: string;
  /** `sha256(canonical(PlanEntryV2))`. */
  entry_sha256: string;
  base: BackfillBase;
}

/**
 * The pre-mint base-read capability (`meta.upload` with `kind:
 * "backfill_base_read"`): grant-bound to one org, source user, workspace,
 * manifest key, session, grant and plan, with no base. Admitted only on
 * `GET /v1/pipeline/manifests/base`, single use, audited.
 */
export interface UploadIdentityBaseRead {
  v: 2;
  kind: "backfill_base_read";
  grant_id: string;
  plan_digest: string;
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  /** `sha256(canonical(PlanEntryV2))` of the backfill this read is for. */
  entry_sha256: string;
}

type AnyUploadIdentity =
  | UploadIdentity
  | UploadIdentityV2
  | UploadIdentityBaseRead;

/** Any v2 identity: a backfill commit or a base-read capability. */
export function isV2Identity(
  upload: AnyUploadIdentity,
): upload is UploadIdentityV2 | UploadIdentityBaseRead {
  return (upload as { v?: unknown }).v === 2;
}

/** A v2 backfill COMMIT identity (with its signed base). */
export function isBackfillIdentity(
  upload: AnyUploadIdentity,
): upload is UploadIdentityV2 {
  return isV2Identity(upload) && upload.kind === "backfill";
}

/** The v2 pre-mint base-read capability. */
export function isBaseReadIdentity(
  upload: AnyUploadIdentity,
): upload is UploadIdentityBaseRead {
  return isV2Identity(upload) && upload.kind === "backfill_base_read";
}

/** The route a migration identity may use besides admission: base reads only, or everything else. */
export const MANIFEST_BASE_PATH = "/v1/pipeline/manifests/base";

/**
 * Whether this identity kind may call `method path`: a base-read capability
 * only `GET /v1/pipeline/manifests/base`; every other identity never that
 * route. (The route allowlist itself is `migrationRouteAllowed`.)
 */
export function migrationKindAllowsRoute(
  upload: AnyUploadIdentity,
  method: string,
  path: string,
): boolean {
  const baseRead =
    method === "GET" && normalizeRoutePath(path) === MANIFEST_BASE_PATH;
  return isBaseReadIdentity(upload) ? baseRead : !baseRead;
}

/** The base-read capability of a backfill entry (what the issuer signs for `{key, kind: "backfill_base_read"}`). */
export async function baseReadIdentityOf(
  entry: PlanEntryV2,
  planDigest: string,
): Promise<UploadIdentityBaseRead> {
  return {
    v: 2,
    kind: "backfill_base_read",
    grant_id: entry.grant_id,
    plan_digest: planDigest,
    key: entry.key,
    org_id: entry.org_id,
    workspace_id: entry.workspace_id,
    source_user_id: entry.source_user_id,
    session: entry.session,
    entry_sha256: await planEntryV2Sha256(entry),
  };
}

/**
 * The 7 base fields of a stored live manifest, as the base read returns them,
 * or the reason it cannot take a backfill (409 `migration_base_unusable`).
 */
export function backfillBaseOf(stored: {
  manifest: ManifestV1;
  version: { eTag?: string; version?: string };
}): BackfillBase | string {
  const { manifest } = stored;
  const raw = manifest.raw;
  const version = backfillBaseVersion(stored.version);
  if (!version) return "the stored manifest has no version";
  if (raw.owner.startsWith("migration:"))
    return "the session is owned by a migration grant";
  if (raw.epochs.some((e) => (e.reason as string) === "backfill"))
    return "the session already has a backfill epoch";
  if (raw.format !== "ndjson") return "a backfill needs an ndjson stream";
  return {
    revision: manifest.revision,
    version,
    owner: raw.owner,
    writer: manifest.writer.delivery_instance_id,
    chunk_count: raw.chunk_count,
    epoch: raw.epoch,
    content_sha256: raw.content_sha256,
  };
}

/**
 * The v2 entry of a manifest's backfill epoch under a known tenancy (the
 * ingest's commit check and the transform's provenance check): the epoch's
 * chunks in order, every chunk's `records` REQUIRED. `undefined` when there is
 * no single backfill epoch or a chunk lacks `records`.
 */
export async function backfillEntryFor(
  tenancy: Pick<
    UploadIdentity,
    "grant_id" | "org_id" | "workspace_id" | "source_user_id"
  >,
  location: Parameters<typeof sessionManifestKey>[0],
  manifest: Pick<ManifestV1, "session" | "raw">,
): Promise<PlanEntryV2 | undefined> {
  const epochs = manifest.raw.epochs.filter(
    (e) => (e.reason as string) === "backfill",
  );
  if (epochs.length !== 1) return undefined;
  const added = manifest.raw.chunks.filter((c) => c.epoch === epochs[0]!.epoch);
  if (added.length === 0 || added.some((c) => !Number.isSafeInteger(c.records)))
    return undefined;
  const chunks: BackfillEntryChunk[] = added.map((c) => ({
    start_offset: c.start_offset,
    bytes: c.bytes,
    sha256: c.sha256,
    records: c.records as number,
  }));
  return {
    v: 2,
    kind: "backfill",
    ...tenancy,
    key: sessionManifestKey(location),
    session: {
      machine_id: manifest.session.machine_id,
      harness: manifest.session.harness,
      session_id: manifest.session.session_id,
      started_at: manifest.session.started_at,
    },
    format: "ndjson",
    chunks,
    backfill_sha256: await backfillSha256(chunks),
    raw_bytes: chunks.reduce((n, c) => n + c.bytes, 0),
  };
}

/** #46's `backfillDigest`: the content hash chain with every epoch set to 0. */
export function backfillSha256(
  chunks: readonly Pick<
    BackfillEntryChunk,
    "start_offset" | "bytes" | "sha256"
  >[],
): Promise<string> {
  return computeContentSha256(
    chunks.map((c) => ({
      epoch: 0,
      start_offset: c.start_offset,
      bytes: c.bytes,
      sha256: c.sha256,
    })),
  );
}

/** `sha256(canonical(entry))`, lowercase hex. */
export function planEntryV2Sha256(entry: PlanEntryV2): Promise<string> {
  return sha256Hex(canonicalJson(entry));
}

/**
 * A grant's plan digest once it has backfills:
 * `sha256(canonical({v: 2, grant_id, exported_at, manifests, backfills}))`,
 * both lists sorted by key. With no backfills it is exactly the v1 digest.
 */
export function exportPlanDigestV2(input: {
  grantId: string;
  exportedAt: string;
  entries: readonly PlanEntry[];
  backfills: readonly PlanEntryV2[];
}): Promise<string> {
  if (input.backfills.length === 0) return exportPlanDigest(input);
  const byKey = <T extends { key: string }>(list: readonly T[]) =>
    [...list].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return sha256Hex(
    canonicalJson({
      v: 2,
      grant_id: input.grantId,
      exported_at: input.exportedAt,
      manifests: byKey(input.entries),
      backfills: byKey(input.backfills),
    }),
  );
}

/** The signed v2 identity of a backfill entry on an observed base. */
export async function uploadIdentityV2Of(
  entry: PlanEntryV2,
  planDigest: string,
  base: BackfillBase,
): Promise<UploadIdentityV2> {
  return {
    v: 2,
    kind: "backfill",
    grant_id: entry.grant_id,
    plan_digest: planDigest,
    key: entry.key,
    org_id: entry.org_id,
    workspace_id: entry.workspace_id,
    source_user_id: entry.source_user_id,
    session: entry.session,
    backfill_sha256: entry.backfill_sha256,
    entry_sha256: await planEntryV2Sha256(entry),
    base,
  };
}

const V2_IDENTITY_KEYS = [
  "v",
  "kind",
  "grant_id",
  "plan_digest",
  "key",
  "org_id",
  "workspace_id",
  "source_user_id",
  "session",
  "backfill_sha256",
  "entry_sha256",
  "base",
];
const BASE_KEYS = [
  "revision",
  "version",
  "owner",
  "writer",
  "chunk_count",
  "epoch",
  "content_sha256",
];

function parseUploadIdentityV2(
  u: Record<string, unknown>,
): UploadIdentityV2 | UploadIdentityBaseRead | undefined {
  if (u.kind === "backfill_base_read") return parseBaseReadIdentity(u);
  const session = u.session;
  const base = u.base;
  const text = (v: unknown, max = 512) =>
    typeof v === "string" && v.length > 0 && v.length <= max;
  if (
    u.v !== 2 ||
    u.kind !== "backfill" ||
    !exactKeySet(u, V2_IDENTITY_KEYS) ||
    !isRecord(session) ||
    !exactKeySet(session, [
      "machine_id",
      "harness",
      "session_id",
      "started_at",
    ]) ||
    !Object.values(session).every((v) => text(v, 256)) ||
    !isRecord(base) ||
    !exactKeySet(base, BASE_KEYS) ||
    !["grant_id", "key", "org_id", "workspace_id", "source_user_id"].every(
      (k) => text(u[k]),
    ) ||
    ["org_id", "workspace_id", "source_user_id"].some(
      (k) => normalizeRelayId(u[k] as string) !== u[k],
    ) ||
    !["plan_digest", "backfill_sha256", "entry_sha256"].every(
      (k) => typeof u[k] === "string" && DIGEST_SHAPE.test(u[k] as string),
    ) ||
    // Bounds (issuer spec section 5): revision + 1, chunk_count + i (i < 4096)
    // and epoch + 1 (a uint32 in the content chain) must all stay exact.
    !(
      Number.isSafeInteger(base.revision) &&
      (base.revision as number) >= 1 &&
      (base.revision as number) <= Number.MAX_SAFE_INTEGER - 1
    ) ||
    !(
      uint(base.chunk_count) &&
      (base.chunk_count as number) <= Number.MAX_SAFE_INTEGER - 4096
    ) ||
    !(uint(base.epoch) && (base.epoch as number) <= 4294967294) ||
    !text(base.version) ||
    !text(base.owner) ||
    (base.owner as string).startsWith("migration:") ||
    !text(base.writer) ||
    typeof base.content_sha256 !== "string" ||
    !DIGEST_SHAPE.test(base.content_sha256)
  )
    return undefined;
  return u as unknown as UploadIdentityV2;
}

function parseBaseReadIdentity(
  u: Record<string, unknown>,
): UploadIdentityBaseRead | undefined {
  const session = u.session;
  const text = (v: unknown, max = 512) =>
    typeof v === "string" && v.length > 0 && v.length <= max;
  if (
    u.v !== 2 ||
    u.kind !== "backfill_base_read" ||
    !exactKeySet(
      u,
      V2_IDENTITY_KEYS.filter((k) => k !== "backfill_sha256" && k !== "base"),
    ) ||
    !isRecord(session) ||
    !exactKeySet(session, [
      "machine_id",
      "harness",
      "session_id",
      "started_at",
    ]) ||
    !Object.values(session).every((v) => text(v, 256)) ||
    !["grant_id", "key", "org_id", "workspace_id", "source_user_id"].every(
      (k) => text(u[k]),
    ) ||
    ["org_id", "workspace_id", "source_user_id"].some(
      (k) => normalizeRelayId(u[k] as string) !== u[k],
    ) ||
    !["plan_digest", "entry_sha256"].every(
      (k) => typeof u[k] === "string" && DIGEST_SHAPE.test(u[k] as string),
    )
  )
    return undefined;
  return u as unknown as UploadIdentityBaseRead;
}

/** Parses a v2 entry carried by a chunk request: exactly canonical, exactly the shape. */
export function parsePlanEntryV2(text: string): PlanEntryV2 | undefined {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (!isRecord(value) || canonicalJson(value) !== text) return undefined;
  const session = value.session;
  const chunks = value.chunks;
  if (
    value.v !== 2 ||
    value.kind !== "backfill" ||
    value.format !== "ndjson" ||
    !exactKeySet(value, [
      "v",
      "kind",
      "grant_id",
      "key",
      "org_id",
      "workspace_id",
      "source_user_id",
      "session",
      "format",
      "chunks",
      "backfill_sha256",
      "raw_bytes",
    ]) ||
    !isRecord(session) ||
    !exactKeySet(session, [
      "machine_id",
      "harness",
      "session_id",
      "started_at",
    ]) ||
    !Array.isArray(chunks) ||
    chunks.length === 0 ||
    !chunks.every(
      (c) =>
        isRecord(c) &&
        exactKeySet(c, ["start_offset", "bytes", "sha256", "records"]) &&
        uint(c.start_offset) &&
        uint(c.bytes) &&
        (c.bytes as number) > 0 &&
        uint(c.records) &&
        typeof c.sha256 === "string" &&
        DIGEST_SHAPE.test(c.sha256),
    ) ||
    !["grant_id", "key", "org_id", "workspace_id", "source_user_id"].every(
      (k) => typeof value[k] === "string",
    ) ||
    typeof value.backfill_sha256 !== "string" ||
    !uint(value.raw_bytes)
  )
    return undefined;
  // Contiguous from offset 0, as #46 lays a backfill out; every running sum
  // stays a safe integer (an overflow is a refusal, never a wrap).
  if (chunks.length > 4096) return undefined;
  let offset = 0;
  for (const c of chunks as BackfillEntryChunk[]) {
    if (c.start_offset !== offset) return undefined;
    if (c.bytes > Number.MAX_SAFE_INTEGER - offset) return undefined;
    offset += c.bytes;
  }
  if (value.raw_bytes !== offset) return undefined;
  return value as unknown as PlanEntryV2;
}

/** The signed v2 entry fields that must equal the signed identity and tenancy. */
function entryV2MatchesIdentity(
  entry: PlanEntryV2,
  upload: UploadIdentityV2,
  tenancy: Pick<
    UploadIdentity,
    "grant_id" | "org_id" | "workspace_id" | "source_user_id"
  >,
): boolean {
  return (
    entry.key === upload.key &&
    entry.grant_id === tenancy.grant_id &&
    entry.org_id === tenancy.org_id &&
    entry.workspace_id === tenancy.workspace_id &&
    entry.source_user_id === tenancy.source_user_id &&
    canonicalJson(entry.session) === canonicalJson(upload.session) &&
    entry.backfill_sha256 === upload.backfill_sha256
  );
}

/**
 * A v2 (backfill) chunk request: the carried entry hashes to the signed
 * `entry_sha256` and names this key, session and tenancy; the chunk's
 * position is derived from the SIGNED base: `seq = base.chunk_count + i`,
 * `epoch = base.epoch + 1`, at the entry's offset, with the digest of the
 * actual bytes.
 */
async function migrationBackfillChunkMatches(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  chunk: { seq: number; epoch: number; startOffset: number; bytes: Uint8Array },
  entryText: string,
): Promise<PlanEntryChunk | undefined> {
  const upload = auth.migration?.upload;
  const tenancy = signedTenancy(auth);
  if (!upload || !isBackfillIdentity(upload) || !tenancy) return undefined;
  if (!migrationSessionMatches(auth, location)) return undefined;
  if ((await sha256Hex(entryText)) !== upload.entry_sha256) return undefined;
  const entry = parsePlanEntryV2(entryText);
  if (
    !entry ||
    !entryV2MatchesIdentity(entry, upload, tenancy) ||
    (await backfillSha256(entry.chunks)) !== entry.backfill_sha256
  )
    return undefined;
  const index = chunk.seq - upload.base.chunk_count;
  const planned = entry.chunks[index];
  if (
    !planned ||
    chunk.epoch !== upload.base.epoch + 1 ||
    planned.start_offset !== chunk.startOffset ||
    planned.bytes !== chunk.bytes.byteLength ||
    planned.sha256 !== (await sha256Hex(chunk.bytes))
  )
    return undefined;
  return { seq: chunk.seq, epoch: chunk.epoch, ...planned };
}

/** A stored object's version as the v2 base names it: its version id, else its ETag. */
export function backfillBaseVersion(version: {
  eTag?: string;
  version?: string;
}): string | undefined {
  return version.version ?? version.eTag;
}

/**
 * A v2 (backfill) manifest commit. The STORED manifest must be exactly the
 * signed base (revision, version, live owner and writer, chunk count, epoch,
 * content) with no backfill epoch yet; the incoming manifest must be that base
 * plus exactly one backfill epoch whose chunks, re-derived into a v2 entry with
 * the signed tenancy, hash to the signed `entry_sha256` and
 * `backfill_sha256`: `seq = base.chunk_count + i`, `epoch = base.epoch + 1`,
 * `first_seq = base.chunk_count`, `opened_by = migration:<grant_id>`, owner
 * and writer unchanged, ndjson. #42's owner fence still runs in the route.
 * Returns the problem, or `undefined`.
 */
export async function migrationBackfillIssue(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  stored:
    | { manifest: ManifestV1; version: { eTag?: string; version?: string } }
    | undefined,
  incoming: ManifestV1,
  options: {
    /**
     * The stored object is already byte-identical to `incoming` (a retry
     * after a lost commit response): the stored manifest is the appended one,
     * not the base. The append is then verified against the signed base
     * without the stored-base comparison (its revision and version moved).
     */
    identicalRetry?: boolean;
  } = {},
): Promise<string | undefined> {
  const upload = auth.migration?.upload;
  const tenancy = signedTenancy(auth);
  if (!upload || !isBackfillIdentity(upload) || !tenancy)
    return "not a backfill identity";
  if (!migrationSessionMatches(auth, location)) return "not the signed session";
  if (!stored) return "no live manifest to append to";
  const { base } = upload;
  if (!options.identicalRetry) {
    const live = stored.manifest;
    const raw = live.raw;
    if (
      live.revision !== base.revision ||
      backfillBaseVersion(stored.version) !== base.version ||
      raw.owner !== base.owner ||
      raw.owner.startsWith("migration:") ||
      live.writer.delivery_instance_id !== base.writer ||
      raw.chunk_count !== base.chunk_count ||
      raw.chunks.length !== base.chunk_count ||
      raw.epoch !== base.epoch ||
      raw.content_sha256 !== base.content_sha256 ||
      raw.epochs.some((e) => (e.reason as string) === "backfill")
    )
      return "the stored manifest is not the signed base";
    if (
      canonicalJson(incoming.raw.chunks.slice(0, base.chunk_count)) !==
        canonicalJson(raw.chunks) ||
      canonicalJson(incoming.raw.epochs.slice(0, raw.epochs.length)) !==
        canonicalJson(raw.epochs)
    )
      return "the manifest does not keep the base's chunks and epochs";
  }
  // The append itself, against the SIGNED base only (so an identical retry
  // is held to the same rule): the base's chunk list hashes to the signed
  // base content, then exactly one backfill epoch of the signed entry.
  const next = incoming.raw;
  const epoch = base.epoch + 1;
  const prefix = next.chunks.slice(0, base.chunk_count);
  const added = next.chunks.slice(base.chunk_count);
  const backfillEpochs = next.epochs.filter(
    (e) => (e.reason as string) === "backfill",
  );
  const opened = next.epochs.at(-1);
  if (
    incoming.revision !== base.revision + 1 ||
    next.owner !== base.owner ||
    incoming.writer.delivery_instance_id !== base.writer ||
    next.format !== "ndjson" ||
    next.epoch !== epoch ||
    prefix.length !== base.chunk_count ||
    (await computeContentSha256(prefix)) !== base.content_sha256 ||
    backfillEpochs.length !== 1 ||
    !opened ||
    opened !== backfillEpochs[0] ||
    opened.epoch !== epoch ||
    opened.first_seq !== base.chunk_count ||
    opened.opened_by !== `migration:${tenancy.grant_id}` ||
    added.length === 0 ||
    added.some((c, i) => c.seq !== base.chunk_count + i || c.epoch !== epoch) ||
    next.chunk_count !== next.chunks.length ||
    (await computeContentSha256(next.chunks)) !== next.content_sha256
  )
    return "the manifest is not the base plus exactly one signed backfill epoch";
  // Every backfill chunk carries `records` (mandatory, never defaulted).
  const entry = await backfillEntryFor(tenancy, location, incoming);
  if (!entry) return "a backfill chunk lacks records";
  if (
    canonicalJson(entry.session) !== canonicalJson(upload.session) ||
    entry.backfill_sha256 !== upload.backfill_sha256 ||
    (await planEntryV2Sha256(entry)) !== upload.entry_sha256
  )
    return "the backfill is not the signed v2 entry";
  return undefined;
}

/** An executor that can run work in one transaction (a pg Pool client, PGlite). */
export interface TransactionalSqlExecutor extends SqlExecutor {
  transaction<T>(work: (tx: SqlExecutor) => Promise<T>): Promise<T>;
}

/**
 * Routes a migration assertion may call: the courier protocol's chunk hop,
 * manifest commit and manifest status read. Everything else refuses it
 * (including `POST /v1/pipeline/manifests/status`).
 */
const MIGRATION_ROUTES: readonly (readonly [string, string])[] = [
  ["POST", "/v1/pipeline/chunks"],
  ["POST", "/v1/pipeline/manifests"],
  ["GET", "/v1/pipeline/manifests/status"],
  // The v2 pre-mint base read (base-read capabilities only; migrationKindAllowsRoute).
  ["GET", MANIFEST_BASE_PATH],
];

/**
 * Exact route matching for migration tokens: the method is case-sensitive,
 * at most ONE trailing slash is removed, and nothing else is normalized (no
 * `//`, no extra segment, the query never selects a route).
 */
function normalizeRoutePath(path: string): string {
  return path.length > 1 && path.endsWith("/") ? path.slice(0, -1) : path;
}

export function migrationRouteAllowed(method: string, path: string): boolean {
  const normalized = normalizeRoutePath(path);
  return MIGRATION_ROUTES.some(
    ([allowedMethod, allowedPath]) =>
      method === allowedMethod && normalized === allowedPath,
  );
}

// --- The single-use ledger ----------------------------------------------

/** Rows whose `expires_at` passed longer ago than this may be deleted. */
export const RETENTION_MARGIN_SECONDS = 300;
/** At most this many expired rows are deleted per ledger insert. */
export const RETENTION_BATCH = 100;

/**
 * `grant_mismatch`: the grant's earlier uses name another plan or org, so this
 * use is not of the grant it claims (nothing written).
 */
export type MigrationConsumeResult = "consumed" | "replayed" | "grant_mismatch";

/** At most this many chunks per assertion (see the module comment). */
export const MAX_CHUNKS_PER_ASSERTION = 256;

/**
 * `repeat`: this exact chunk was reserved before; `conflict`: another chunk
 * holds the seq; `limit`: the assertion already holds MAX_CHUNKS_PER_ASSERTION.
 */
export type MigrationChunkResult = "reserved" | "repeat" | "conflict" | "limit";

export interface MigrationAssertionLedger {
  /** Whether this `jti` or this `nonce` was already consumed. */
  consumed(
    claims: Pick<MigrationAssertionClaims, "jti" | "nonce">,
  ): Promise<boolean>;
  /**
   * Records one chunk write under an assertion: at most one chunk per
   * `(jti, seq)` and at most MAX_CHUNKS_PER_ASSERTION per `jti`.
   */
  reserveChunk(use: MigrationChunkUse): Promise<MigrationChunkResult>;
  /**
   * Consumes the assertion and enqueues the commit's message as one unit:
   * both are durable or neither is. `replayed` (nothing enqueued) when the
   * `jti` or the `nonce` was consumed before. Throws when the unit failed, in
   * which case nothing was consumed or enqueued.
   */
  consume(
    use: MigrationAssertionUse,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<MigrationConsumeResult>;
  /**
   * Consumes a base-read capability: the replay row and a durable audit row
   * (result `base_read`, `auditKey` as its work key) in one transaction;
   * nothing is enqueued. Same replay and grant rules as `consume`.
   */
  consumeBaseRead(
    use: MigrationAssertionUse,
    auditKey: string,
  ): Promise<MigrationConsumeResult>;
}

/**
 * The ledger over SQL: `sessions.migration_assertion_uses` (jti primary key,
 * nonce unique) and the queue row in one transaction, plus
 * `sessions.migration_assertion_chunks`. `enqueue` defaults to the Postgres
 * pipeline queue on the same transaction; a deployment that enriches
 * messages first passes its own, and must write through `tx`.
 */
export function sqlMigrationAssertionLedger(
  sql: TransactionalSqlExecutor,
  options: {
    schema?: string;
    enqueue?: (
      tx: SqlExecutor,
      stage: PipelineStage,
      message: StageMessage,
    ) => Promise<void>;
  } = {},
): MigrationAssertionLedger {
  const schema = options.schema ?? "sessions";
  if (!/^[a-z_][a-z0-9_]*$/.test(schema))
    throw new Error(`bad migration ledger schema ${schema}`);
  const uses = `${schema}.migration_assertion_uses`;
  const chunks = `${schema}.migration_assertion_chunks`;
  const audit = `${schema}.migration_ingest_audit`;
  const baseReads = `${schema}.migration_base_read_uses`;
  const enqueue =
    options.enqueue ??
    ((tx: SqlExecutor, stage: PipelineStage, message: StageMessage) =>
      postgresPipelineQueue(tx, { schema }).send({ ...message, stage }));
  // Opportunistic, bounded retention on every insert (no cron).
  const purge = (tx: SqlExecutor, table: string, key: string) =>
    tx.query(
      `DELETE FROM ${table} WHERE (${key}) IN (
         SELECT ${key} FROM ${table}
          WHERE expires_at < now() - make_interval(secs => $1)
          ORDER BY expires_at
          LIMIT $2
          FOR UPDATE SKIP LOCKED)`,
      [RETENTION_MARGIN_SECONDS, RETENTION_BATCH],
    );
  return {
    async consumed({ jti, nonce }) {
      const { rows } = await sql.query(
        `SELECT 1 FROM ${uses} WHERE jti = $1 OR nonce = $2 LIMIT 1`,
        [jti, nonce],
      );
      return rows.length > 0;
    },
    reserveChunk(use) {
      return sql.transaction(async (tx) => {
        await purge(tx, chunks, "jti, seq");
        // Serializes this assertion's reservations, so the cap is strict.
        await tx.query("SELECT pg_advisory_xact_lock(hashtext($1))", [use.jti]);
        const held = await tx.query<{
          start_offset: string | number;
          sha256: string;
        }>(
          `SELECT start_offset, sha256 FROM ${chunks} WHERE jti = $1 AND seq = $2`,
          [use.jti, use.seq],
        );
        const row = held.rows[0];
        if (row)
          return Number(row.start_offset) === use.startOffset &&
            row.sha256 === use.sha256
            ? ("repeat" as const)
            : ("conflict" as const);
        const counted = await tx.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM ${chunks} WHERE jti = $1`,
          [use.jti],
        );
        if (Number(counted.rows[0]?.n ?? 0) >= MAX_CHUNKS_PER_ASSERTION)
          return "limit" as const;
        const { rows } = await tx.query(
          `INSERT INTO ${chunks} (jti, seq, start_offset, sha256, expires_at)
           VALUES ($1, $2, $3, $4, to_timestamp($5))
           ON CONFLICT DO NOTHING
           RETURNING jti`,
          [use.jti, use.seq, use.startOffset, use.sha256, use.expiresAt],
        );
        return rows.length ? ("reserved" as const) : ("conflict" as const);
      });
    },
    consume(use, stage, message) {
      return spend(uses, use, message.workKey, "committed", (tx) =>
        enqueue(tx, stage, message),
      );
    },
    consumeBaseRead(use, auditKey) {
      // Its own replay namespace: a base-read jti or nonce never collides
      // with an upload assertion's.
      return spend(baseReads, use, auditKey, "base_read", async () => {});
    },
  };

  function spend(
    table: string,
    use: MigrationAssertionUse,
    workKey: string,
    result: "committed" | "base_read",
    effect: (tx: SqlExecutor) => Promise<void>,
  ): Promise<MigrationConsumeResult> {
    return sql.transaction(async (tx) => {
      await purge(tx, table, "jti");
      // Serializes this grant's consumptions, so two first uses with
      // different plans cannot both pass the consistency check.
      await tx.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
        `migration-grant:${use.grantId}`,
      ]);
      // One grant, one org and one plan: a use that disagrees with the
      // grant's earlier uses is refused, and nothing is written.
      // Across BOTH ledgers (upload uses and base reads): a grant is bound
      // to one plan and one org by whichever of them came first.
      const conflict = await tx.query(
        `SELECT 1 FROM ${uses}
            WHERE grant_id = $1 AND (plan_digest <> $2 OR org_id <> $3)
         UNION ALL
         SELECT 1 FROM ${baseReads}
            WHERE grant_id = $1 AND (plan_digest <> $2 OR org_id <> $3)
         LIMIT 1`,
        [use.grantId, use.planDigest, use.orgId],
      );
      if (conflict.rows.length) return "grant_mismatch" as const;
      // Insert-if-absent on both keys. A concurrent consumer of the same jti
      // or nonce waits on the unique index, then sees the winner's row.
      const { rows } = await tx.query(
        `INSERT INTO ${table} (jti, nonce, grant_id, org_id, workspace_id,
                                user_id, plan_digest, plan_entry_digest,
                                expires_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_timestamp($9))
           ON CONFLICT DO NOTHING
           RETURNING jti`,
        [
          use.jti,
          use.nonce,
          use.grantId,
          use.orgId,
          use.workspaceId,
          use.userId,
          use.planDigest,
          use.planEntryDigest,
          use.expiresAt,
        ],
      );
      if (!rows.length) return "replayed" as const;
      // Throwing here rolls the consumption back with the queue row.
      await effect(tx);
      // The durable audit event, in the same transaction: ids, digests and
      // the work key only (never a token, a signature or chunk bytes).
      await tx.query(
        `INSERT INTO ${audit} (grant_id, plan_digest, entry_sha256, work_key,
                                 org_id, workspace_id, user_id, result)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [
          use.grantId,
          use.planDigest,
          use.planEntryDigest,
          workKey,
          use.orgId,
          use.workspaceId,
          use.userId,
          result,
        ],
      );
      return "consumed" as const;
    });
  }
}

/**
 * An in-process ledger (tests, single-process stacks). The reservation and
 * the send are one unit: a send that throws releases the reservation. The
 * same opportunistic, bounded retention as the SQL ledger.
 */
export class InMemoryMigrationAssertionLedger implements MigrationAssertionLedger {
  readonly uses = new Map<string, MigrationAssertionUse>();
  /** Base-read capabilities: their own replay namespace (jti and nonce). */
  readonly baseReads = new Map<string, MigrationAssertionUse>();
  readonly #baseReadNonces = new Set<string>();

  /** Whether a use disagrees with the grant's binding in EITHER ledger. */
  #grantMismatch(use: MigrationAssertionUse): boolean {
    for (const earlier of [...this.uses.values(), ...this.baseReads.values()])
      if (
        earlier.grantId === use.grantId &&
        (earlier.planDigest !== use.planDigest || earlier.orgId !== use.orgId)
      )
        return true;
    return false;
  }
  readonly chunks = new Map<string, MigrationChunkUse>();
  /** The durable audit events (one per committed consumption). */
  readonly audit: (Omit<
    MigrationAssertionUse,
    "jti" | "nonce" | "expiresAt"
  > & {
    workKey: string;
    result?: "committed" | "base_read";
  })[] = [];
  readonly #nonces = new Map<string, string>();

  constructor(
    private readonly send: (
      stage: PipelineStage,
      message: StageMessage,
    ) => Promise<void>,
    private readonly now: () => number = () => Math.floor(Date.now() / 1000),
  ) {}

  #purge() {
    const cutoff = this.now() - RETENTION_MARGIN_SECONDS;
    let budget = RETENTION_BATCH;
    for (const [jti, use] of this.uses) {
      if (budget <= 0) break;
      if (use.expiresAt < cutoff) {
        this.uses.delete(jti);
        this.#nonces.delete(use.nonce);
        budget--;
      }
    }
    budget = RETENTION_BATCH;
    for (const [key, chunk] of this.chunks) {
      if (budget <= 0) break;
      if (chunk.expiresAt < cutoff) {
        this.chunks.delete(key);
        budget--;
      }
    }
  }

  async consumed({
    jti,
    nonce,
  }: Pick<MigrationAssertionClaims, "jti" | "nonce">) {
    return this.uses.has(jti) || this.#nonces.has(nonce);
  }

  async reserveChunk(use: MigrationChunkUse): Promise<MigrationChunkResult> {
    this.#purge();
    const key = `${use.jti}\u0000${use.seq}`;
    const held = this.chunks.get(key);
    if (!held) {
      let count = 0;
      for (const chunk of this.chunks.values())
        if (chunk.jti === use.jti) count++;
      if (count >= MAX_CHUNKS_PER_ASSERTION) return "limit";
      this.chunks.set(key, use);
      return "reserved";
    }
    return held.startOffset === use.startOffset && held.sha256 === use.sha256
      ? "repeat"
      : "conflict";
  }

  async consumeBaseRead(
    use: MigrationAssertionUse,
    auditKey: string,
  ): Promise<MigrationConsumeResult> {
    this.#purge();
    if (this.baseReads.has(use.jti) || this.#baseReadNonces.has(use.nonce))
      return "replayed";
    if (this.#grantMismatch(use)) return "grant_mismatch";
    this.baseReads.set(use.jti, use);
    this.#baseReadNonces.add(use.nonce);
    this.audit.push({
      grantId: use.grantId,
      planDigest: use.planDigest,
      planEntryDigest: use.planEntryDigest,
      orgId: use.orgId,
      workspaceId: use.workspaceId,
      userId: use.userId,
      workKey: auditKey,
      result: "base_read",
    });
    return "consumed";
  }

  async consume(
    use: MigrationAssertionUse,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<MigrationConsumeResult> {
    this.#purge();
    if (this.uses.has(use.jti) || this.#nonces.has(use.nonce))
      return "replayed";
    if (this.#grantMismatch(use)) return "grant_mismatch";
    // Reserved synchronously, before the first await, so a concurrent consume
    // of the same jti or nonce sees it.
    this.uses.set(use.jti, use);
    this.#nonces.set(use.nonce, use.jti);
    try {
      await this.send(stage, message);
    } catch (cause) {
      this.uses.delete(use.jti);
      this.#nonces.delete(use.nonce);
      throw cause;
    }
    this.audit.push({
      grantId: use.grantId,
      planDigest: use.planDigest,
      planEntryDigest: use.planEntryDigest,
      orgId: use.orgId,
      workspaceId: use.workspaceId,
      userId: use.userId,
      workKey: message.workKey,
    });
    return "consumed";
  }
}
