/**
 * RelayHistory export transform (switch-first E4/O4).
 *
 * Live post-T0 records and the later pre-T0 export share one append-only raw
 * stream. The export is a dedicated `backfill` epoch, so arrival order is not
 * semantic: this reader takes the highest numeric revision of each
 * `(origin_id, record_id)`, applies tombstones, and then orders the surviving
 * records by event time, and projects them as the live triggers would
 * (`relayhistory-projection.ts`). The resulting object is content-addressed by
 * the raw manifest digest; a later manifest cannot overwrite an earlier result.
 */
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import {
  isObjectStoreError,
  manifestLocation,
  normalizedKey,
  objectKey,
  parseSessionKey,
  rawChunkKey,
  type ManifestV1,
  type ObjectStore,
} from "./object-layout.js";
import {
  readCapped,
  type ManifestRead,
  type ManifestReader,
} from "./adapter.js";
import { PipelineError } from "./errors.js";
import {
  backfillEntryFor,
  normalizeRelayId,
  planEntryFor,
  planEntrySha256,
  planEntryV2Sha256,
} from "./migration-assertions.js";
import {
  CONVERGENCE_EVENT_KIND,
  projectRelayhistorySession,
  type ProjectionRow,
} from "./relayhistory-projection.js";
import type {
  NormalizeSkipped,
  NormalizeTarget,
  NormalizedOutput,
  RawPointer,
  SessionTransform,
} from "./stages.js";

/** Bump with any change to the bytes projected from the same input: the object is create-only. */
export const RELAYHISTORY_NORMALIZED_VERSION = 2;
export const RELAYHISTORY_EXPORT_EXTENSION = "client.relayhistory_export";
const MAX_RELAYHISTORY_CHUNK_BYTES = 16 * 1024 * 1024;
/** Everything is held in memory to fold it, so a session past this is refused. */
export const MAX_RELAYHISTORY_SESSION_BYTES = 256 * 1024 * 1024;
/** Projected rows repeat their payloads beside typed columns, so an object can outgrow its raw. */
const NORMALIZED_GROWTH = 4;
const MAX_REVISION = 9_223_372_036_854_775_807n;

export interface RelayhistoryStreamRecord {
  kind: string;
  origin_id: string;
  record_id: string;
  revision: string;
  operation: "upsert" | "delete";
  received_at: string;
  payload: Record<string, unknown> | null;
  /**
   * The record's place in the source's acceptance ledger: the order the live
   * projection settles contenders in. Absent on records with no such ledger.
   */
  accepted_seq?: string;
}

export type ZstdDecompress = (stored: Uint8Array) => Uint8Array;

function fail(code: string, message: string): never {
  throw new PipelineError(code, message, false);
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);

/**
 * Who an exported session belongs to, once its manifest shows it. The ingest
 * checks the marker against the signed assertion and refuses it from any other
 * writer (`manifestOwnerIssue`, `migrationManifestProvenance`); the transform
 * has no assertion, so it requires what that leaves behind: the marker's signed
 * fields, the same user in `96bulls`, and a stream the grant itself opened:
 * as raw owner of a cold export, as the opener of its epoch 0 when a client has
 * since adopted that export, or as the opener of the backfill epoch.
 */
export function relayhistoryExportProvenance(manifest: ManifestV1): {
  grantId: string;
  sourceUserId: string;
} {
  const marker = manifest.extensions?.[RELAYHISTORY_EXPORT_EXTENSION];
  if (
    !isObject(marker) ||
    marker.version !== 1 ||
    marker.source !== "neon.delivery_records"
  )
    fail(
      "relayhistory_export_marker_missing",
      "manifest is not a RelayHistory export",
    );
  const {
    grant_id: grantId,
    plan_digest: planDigest,
    source_user_id: sourceUserId,
  } = marker;
  const bulls = manifest.extensions?.["96bulls"];
  if (
    typeof grantId !== "string" ||
    !grantId ||
    typeof planDigest !== "string" ||
    !/^[0-9a-f]{64}$/.test(planDigest) ||
    typeof sourceUserId !== "string" ||
    !sourceUserId ||
    !isObject(bulls) ||
    bulls.user_id !== sourceUserId
  )
    fail(
      "relayhistory_export_provenance",
      "export marker has no signed provenance",
    );
  const migration = `migration:${grantId}`;
  const { epochs } = manifest.raw;
  const adoptedExport =
    epochs[0]?.opened_by === migration &&
    epochs.some((epoch) => epoch.reason === "adopted");
  if (
    manifest.raw.owner !== migration &&
    !adoptedExport &&
    !epochs.some(
      (epoch) => epoch.reason === "backfill" && epoch.opened_by === migration,
    )
  )
    fail(
      "relayhistory_export_provenance",
      "export marker is not on a stream its grant opened",
    );
  return { grantId, sourceUserId };
}

function carriesRecords(manifest: ManifestV1): boolean {
  return (
    manifest.extensions?.[RELAYHISTORY_EXPORT_EXTENSION] !== undefined ||
    // A "resumed" epoch only ever follows a backfill.
    manifest.raw.epochs.some((epoch) => epoch.reason === "backfill")
  );
}

function stringField(value: Record<string, unknown>, field: string): string {
  const fieldValue = value[field];
  if (typeof fieldValue !== "string" || !fieldValue || fieldValue.length > 1024)
    fail(
      "relayhistory_record_invalid",
      `RelayHistory record ${field} is missing`,
    );
  return fieldValue;
}

/** JSON.parse, with an integer past a double's precision kept exact as a bigint. */
function parseExact(line: string): unknown {
  return (JSON.parse as (text: string, reviver: unknown) => unknown)(
    line,
    (_key: string, value: unknown, context: { source: string }) =>
      typeof value === "number" &&
      !Number.isSafeInteger(value) &&
      /^-?\d+$/.test(context.source)
        ? BigInt(context.source)
        : value,
  );
}

export function parseRelayhistoryRecord(
  line: string,
): RelayhistoryStreamRecord {
  let value: unknown;
  try {
    value = parseExact(line);
  } catch {
    fail(
      "relayhistory_record_invalid",
      "RelayHistory export contains invalid JSON",
    );
  }
  if (!value || typeof value !== "object" || Array.isArray(value))
    fail(
      "relayhistory_record_invalid",
      "RelayHistory export record is not an object",
    );
  const record = value as Record<string, unknown>;
  const revision = stringField(record, "revision");
  const receivedAt = stringField(record, "received_at");
  const operation = record.operation ?? "upsert";
  const payload = record.payload ?? null;
  if (
    !/^(?:0|[1-9][0-9]{0,18})$/.test(revision) ||
    BigInt(revision) > MAX_REVISION
  )
    fail(
      "relayhistory_record_invalid",
      "RelayHistory record revision is not numeric",
    );
  if (!Number.isFinite(Date.parse(receivedAt)))
    fail(
      "relayhistory_record_invalid",
      "RelayHistory record received_at is invalid",
    );
  const acceptedSeq = record.accepted_seq;
  if (
    acceptedSeq !== undefined &&
    (typeof acceptedSeq !== "string" ||
      !/^(?:0|[1-9][0-9]{0,18})$/.test(acceptedSeq) ||
      BigInt(acceptedSeq) > MAX_REVISION)
  )
    fail(
      "relayhistory_record_invalid",
      "RelayHistory record accepted_seq is not numeric",
    );
  if (operation !== "upsert" && operation !== "delete")
    fail(
      "relayhistory_record_invalid",
      "RelayHistory record operation is invalid",
    );
  if (
    payload !== null &&
    (typeof payload !== "object" || Array.isArray(payload))
  )
    fail(
      "relayhistory_record_invalid",
      "RelayHistory record payload is invalid",
    );
  if (operation === "upsert" && payload === null)
    fail("relayhistory_record_invalid", "RelayHistory upsert has no payload");
  return {
    kind: stringField(record, "kind"),
    origin_id: stringField(record, "origin_id"),
    record_id: stringField(record, "record_id"),
    revision,
    operation,
    received_at: receivedAt,
    payload: payload as Record<string, unknown> | null,
    ...(acceptedSeq !== undefined ? { accepted_seq: acceptedSeq } : {}),
  };
}

function eventTime(record: RelayhistoryStreamRecord): number {
  const payload = record.payload ?? {};
  for (const field of [
    "ts_ms",
    "timestamp_ms",
    "created_ms",
    "spawned_at_ms",
    "updated_ms",
    "first_activity_ms",
    "last_activity_ms",
  ]) {
    const value = payload[field];
    if (typeof value === "number" && Number.isFinite(value) && value > 0)
      return value;
    if (typeof value === "string" && /^\d{10,16}$/.test(value)) {
      const parsed = Number(value);
      if (Number.isSafeInteger(parsed)) return parsed;
    }
  }
  return Date.parse(record.received_at);
}

const LEGACY_RECORD_KINDS = new Set([
  "legacy_conversation_turn", "legacy_session_link", "legacy_session_outcome", "legacy_scope",
]);
export const isLegacyRelayhistoryRecord = (record: RelayhistoryStreamRecord) => LEGACY_RECORD_KINDS.has(record.kind);
function recordKey(record: RelayhistoryStreamRecord): string {
  return JSON.stringify(isLegacyRelayhistoryRecord(record)
    ? [record.origin_id, record.record_id, record.kind]
    : [record.origin_id, record.record_id]);
}

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

/**
 * The same revision of one record. `received_at` is excluded: it says when each
 * path stored the record, so the export and the live client legitimately differ.
 */
function sameRecord(
  left: RelayhistoryStreamRecord,
  right: RelayhistoryStreamRecord,
): boolean {
  return relayhistoryRevisionContent(left) === relayhistoryRevisionContent(right);
}
/** Exact same-revision content domain, shared by proof indexes without retaining payloads. */
export function relayhistoryRevisionContent(record: RelayhistoryStreamRecord): string {
  const { received_at: _at, accepted_seq: _seq, ...rest } = record;
  return encoded(canonical(rest));
}

function receivedFirst(
  left: RelayhistoryRevisionSeat,
  right: RelayhistoryRevisionSeat,
): boolean {
  return (
    (Date.parse(left.received_at) - Date.parse(right.received_at) ||
      compareText(left.received_at, right.received_at)) < 0
  );
}

/**
 * record_id is origin-independent, and the live projection keeps one row for it:
 * the last write accepted from any origin, so a tombstone accepted last removes
 * the record whichever origin wrote the upsert. Only the acceptance ledger knows
 * that order: `received_at` is transaction start, so a writer that waited on the
 * lock carries an older stamp than the write it follows. Contenders without
 * `accepted_seq` are refused rather than guessed at.
 */
function newestAcrossOrigins(
  records: Iterable<RelayhistoryStreamRecord>,
): RelayhistoryStreamRecord[] {
  const out: RelayhistoryStreamRecord[] = [];
  const contenders = new Map<string, RelayhistoryStreamRecord[]>();
  for (const record of records) {
    // Server-exported legacy tables have independent primary-key domains and
    // no delivery acceptance ledger. Never make them contend with delivery IDs.
    if (record.kind === CONVERGENCE_EVENT_KIND || isLegacyRelayhistoryRecord(record)) {
      out.push(record);
      continue;
    }
    const group = contenders.get(record.record_id);
    if (group) group.push(record);
    else contenders.set(record.record_id, [record]);
  }
  for (const group of contenders.values()) out.push(group.reduce(lastAccepted));
  return out;
}

/** The later accepted of two contenders, by the source's ledger alone. */
export function lastAccepted(
  left: RelayhistoryStreamRecord,
  right: RelayhistoryStreamRecord,
): RelayhistoryStreamRecord {
  if (left.accepted_seq === undefined || right.accepted_seq === undefined)
    fail(
      "relayhistory_acceptance_missing",
      "contending RelayHistory records have no acceptance order",
    );
  return BigInt(right.accepted_seq) > BigInt(left.accepted_seq) ? right : left;
}

export interface RelayhistoryRevisionSeat {
  revision: string;
  received_at: string;
  accepted_seq?: string;
}
/**
 * The authoritative per-origin revision fold. Evidence indexes may supply a
 * cryptographic content comparison; projection supplies the exact content domain.
 * Tombstones remain seats here: only projection removes them afterwards.
 */
export function chooseRelayhistoryRevision<T extends RelayhistoryRevisionSeat>(
  previous: T | undefined,
  record: T,
  sameContent: (left: T, right: T) => boolean,
): T {
  if (!previous || BigInt(record.revision) > BigInt(previous.revision)) return record;
  if (BigInt(record.revision) !== BigInt(previous.revision)) return previous;
  if (!sameContent(record, previous))
    fail("relayhistory_revision_conflict", "equal RelayHistory revisions contain different records");
  if (record.accepted_seq !== undefined && previous.accepted_seq !== undefined &&
      BigInt(record.accepted_seq) !== BigInt(previous.accepted_seq))
    fail("relayhistory_acceptance_conflict", "copies of one RelayHistory record name different acceptance seats");
  const first = receivedFirst(record, previous) ? record : previous;
  const seq = record.accepted_seq ?? previous.accepted_seq;
  return seq === undefined ? first : { ...first, accepted_seq: seq };
}

/** Latest live records, deterministic regardless of live/backfill arrival order. */
export function normalizeRelayhistoryRecords(
  records: readonly RelayhistoryStreamRecord[],
): RelayhistoryStreamRecord[] {
  const latest = new Map<string, RelayhistoryStreamRecord>();
  for (const record of records) {
    const key = recordKey(record);
    latest.set(key, chooseRelayhistoryRevision(latest.get(key), record, sameRecord));
  }
  return newestAcrossOrigins(latest.values())
    .filter((record) => record.operation === "upsert" && record.payload !== null)
    .sort((left, right) => eventTime(left) - eventTime(right) ||
      compareText(left.origin_id, right.origin_id) || compareText(left.record_id, right.record_id));
}

async function defaultDecompress(
  stored: Uint8Array,
  maxOutputLength: number,
): Promise<Uint8Array> {
  try {
    const zlib = await import("node:zlib");
    if (typeof zlib.zstdDecompressSync !== "function")
      fail(
        "zstd_unavailable",
        "this runtime cannot zstd-decompress raw chunks",
      );
    return new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength }));
  } catch (error) {
    if (error instanceof PipelineError) throw error;
    fail(
      "chunk_decompress_failed",
      "a RelayHistory raw chunk is not valid zstd",
    );
  }
}

/** Read and verify every canonical-record chunk listed by an export manifest. */
export async function readRelayhistoryExport(
  manifest: ManifestV1,
  store: ObjectStore,
  decompress?: ZstdDecompress,
  options: { maxSessionBytes?: number; prefix?: string } = {},
): Promise<RelayhistoryStreamRecord[]> {
  const maxSessionBytes =
    options.maxSessionBytes ?? MAX_RELAYHISTORY_SESSION_BYTES;
  relayhistoryExportProvenance(manifest);
  if (!manifest.raw.scrub)
    fail(
      "relayhistory_export_quarantined",
      "RelayHistory export is not scrubbed",
    );
  if (manifest.raw.format !== "ndjson")
    fail("relayhistory_export_format", "RelayHistory export must use ndjson");
  const declared = manifest.raw.chunks.reduce(
    (total, chunk) =>
      total + Math.max(chunk.bytes, chunk.stored_raw_bytes ?? 0),
    0,
  );
  if (declared > maxSessionBytes)
    fail(
      "relayhistory_session_too_large",
      "RelayHistory session exceeds the transform limit",
    );
  const loc = manifestLocation(manifest, options.prefix);
  const records: RelayhistoryStreamRecord[] = [];
  for (const chunk of manifest.raw.chunks) {
    if (
      typeof chunk.records !== "number" ||
      !Number.isSafeInteger(chunk.records) ||
      chunk.records < 0
    )
      fail(
        "relayhistory_record_count",
        "RelayHistory raw chunk record count is required",
      );
    if (
      chunk.bytes > MAX_RELAYHISTORY_CHUNK_BYTES ||
      chunk.stored_bytes > MAX_RELAYHISTORY_CHUNK_BYTES ||
      (chunk.stored_raw_bytes ?? 0) > MAX_RELAYHISTORY_CHUNK_BYTES
    )
      fail(
        "relayhistory_chunk_too_large",
        "RelayHistory raw chunk exceeds the transform limit",
      );
    const key = rawChunkKey(
      loc,
      chunk.seq,
      chunk.start_offset,
      chunk.sha256,
      manifest.raw.format,
    );
    let stored: Uint8Array | null;
    try {
      stored = await readCapped(await store.get(key), chunk.stored_bytes);
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code === "not_found")
        throw new PipelineError(
          "chunk_missing",
          "a listed raw chunk is missing",
          true,
        );
      throw error;
    }
    if (
      !stored ||
      stored.byteLength !== chunk.stored_bytes ||
      (await sha256Hex(stored)) !== chunk.stored_sha256
    )
      fail(
        "chunk_digest_mismatch",
        "a RelayHistory raw chunk has the wrong stored digest",
      );
    let raw: Uint8Array;
    try {
      raw = decompress
        ? decompress(stored)
        : await defaultDecompress(
            stored,
            chunk.stored_raw_bytes ?? MAX_RELAYHISTORY_CHUNK_BYTES,
          );
    } catch (error) {
      if (error instanceof PipelineError) throw error;
      fail(
        "chunk_decompress_failed",
        "a RelayHistory raw chunk is not valid zstd",
      );
    }
    if (
      raw.byteLength !== chunk.stored_raw_bytes ||
      (await sha256Hex(raw)) !== chunk.raw_sha256
    )
      fail(
        "chunk_digest_mismatch",
        "a RelayHistory raw chunk has the wrong raw digest",
      );
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
        raw,
      );
    } catch {
      fail("relayhistory_record_invalid", "RelayHistory export is not UTF-8");
    }
    let count = 0;
    for (const line of text.split("\n")) {
      if (!line) continue;
      records.push(parseRelayhistoryRecord(line));
      count += 1;
    }
    if (chunk.records !== count)
      fail(
        "relayhistory_record_count",
        "RelayHistory raw chunk record count does not match",
      );
  }
  return normalizeRelayhistoryRecords(records);
}

/** JSON text, with a bigint as the exact number it is. */
function encoded(value: unknown): string {
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return `[${value.map(encoded).join(",")}]`;
  if (isObject(value))
    return `{${Object.entries(value)
      .filter(([, child]) => child !== undefined)
      .map(([key, child]) => `${JSON.stringify(key)}:${encoded(child)}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

function encodedRows(rows: readonly ProjectionRow[]): Uint8Array {
  const lines = rows.map((row) => encoded(canonical(row)));
  return new TextEncoder().encode(lines.length ? `${lines.join("\n")}\n` : "");
}

/**
 * The `convergence_events` lines of a stored normalized object. An object in any
 * other shape, such as the record envelopes earlier builds wrote, is refused:
 * `{table, row}` is the only normalized format.
 */
function convergenceEventLines(stored: Uint8Array): number {
  let count = 0;
  for (const line of new TextDecoder().decode(stored).split("\n")) {
    if (!line) continue;
    const parsed = parsedLine(line);
    if (
      typeof parsed.table !== "string" ||
      !parsed.row ||
      typeof parsed.row !== "object"
    )
      fail(
        "normalized_object_invalid",
        "stored normalized object is not {table, row} lines",
      );
    if (parsed.table === "convergence_events") count += 1;
  }
  return count;
}

function parsedLine(line: string): { table?: unknown; row?: unknown } {
  try {
    return (JSON.parse(line) ?? {}) as { table?: unknown; row?: unknown };
  } catch {
    return {};
  }
}

/**
 * Whether `read` is the manifest version a message was enqueued for. Newer means
 * the message is late, and that version's own event normalizes it. Older means
 * the read lagged the event, which a retry settles.
 */
function currentForTarget(
  read: ManifestRead,
  manifestKey: string,
  target: NormalizeTarget,
  migration: RawPointer["migration"],
): boolean {
  const { revision } = read.manifest;
  if (target.revision !== undefined && revision < target.revision)
    throw new PipelineError(
      "manifest_behind_message",
      "the manifest read is older than the message's revision",
      true,
    );
  if (target.revision !== undefined && revision > target.revision) return false;
  // Migration work is keyed by its plan entry, which is not an object version.
  if (migration) return true;
  const prefix = `${manifestKey}@`;
  const expected = target.workKey?.startsWith(prefix)
    ? target.workKey.slice(prefix.length)
    : "";
  return !expected || expected === read.version || expected === read.etag;
}

/** Whether a migration message's signed plan entry is this manifest's. */
async function isPlannedEntry(
  manifest: ManifestV1,
  migration: NonNullable<RawPointer["migration"]>,
  orgId: string | undefined,
): Promise<boolean> {
  if (orgId === undefined)
    fail("relayhistory_export_provenance", "migration work names no org");
  const provenance = relayhistoryExportProvenance(manifest);
  const tenancy = {
    grant_id: provenance.grantId,
    org_id: normalizeRelayId(orgId),
    workspace_id: normalizeRelayId(manifest.session.workspace_id),
    source_user_id: normalizeRelayId(provenance.sourceUserId),
  };
  const location = {
    workspaceId: manifest.session.workspace_id,
    machineId: manifest.session.machine_id,
    harness: manifest.session.harness,
    sessionId: manifest.session.session_id,
    startDate: new Date(manifest.session.started_at).toISOString().slice(0, 10),
  };
  // v2 (a backfill epoch on a live manifest): the signed entry covers only
  // the backfill epoch's chunks, and the work names the base it appended to.
  if (migration.base_revision !== undefined) {
    if (manifest.revision !== migration.base_revision + 1) return false;
    const backfill = await backfillEntryFor(tenancy, location, manifest);
    return (
      !!backfill &&
      (await planEntryV2Sha256(backfill)) === migration.entry_sha256
    );
  }
  const entry = await planEntryFor(tenancy, location, manifest);
  return !!entry && (await planEntrySha256(entry)) === migration.entry_sha256;
}

/**
 * Concrete file-normalize implementation for RelayHistory export manifests.
 * Non-export manifests are left for their harness-specific normalizers.
 */
export class RelayhistoryExportTransform implements SessionTransform {
  constructor(
    private readonly deps: {
      manifests: ManifestReader;
      store: ObjectStore;
      decompress?: ZstdDecompress;
      maxSessionBytes?: number;
    },
  ) {}

  async normalize(
    pointer: RawPointer,
    target: NormalizeTarget = {},
  ): Promise<NormalizedOutput | NormalizeSkipped | Record<string, never>> {
    if (pointer.deleted) return {};
    const read = await this.deps.manifests.read(pointer.manifestKey);
    if (!read) return {};
    if (!currentForTarget(read, pointer.manifestKey, target, pointer.migration))
      return { skipped: "stale_manifest" };
    const manifest = read.manifest;
    // A harness log is left for its own normalizer. A marker this code cannot
    // read, or a backfill without one, reaches the refusal in the reader below.
    if (!carriesRecords(manifest)) return {};
    if (
      pointer.migration &&
      !(await isPlannedEntry(manifest, pointer.migration, target.orgId))
    )
      return { skipped: "stale_manifest" };
    const prefix = parseSessionKey(pointer.manifestKey)?.prefix;
    const records = await readRelayhistoryExport(
      manifest,
      this.deps.store,
      this.deps.decompress,
      {
        ...(this.deps.maxSessionBytes !== undefined
          ? { maxSessionBytes: this.deps.maxSessionBytes }
          : {}),
        ...(prefix ? { prefix } : {}),
      },
    );
    const location = manifestLocation(manifest, prefix);
    const projection = projectRelayhistorySession(records, {
      workspaceId: location.workspaceId,
      source: location.harness,
      sessionId: location.sessionId,
      machineId: location.machineId,
      userId: relayhistoryExportProvenance(manifest).sourceUserId,
    });
    const body = encodedRows(projection.rows);
    const key = normalizedKey(
      location,
      RELAYHISTORY_NORMALIZED_VERSION,
      `relayhistory-${manifest.raw.content_sha256}.ndjson`,
    );
    let created = false;
    try {
      const ack = await this.deps.store.put(objectKey(key), body, {
        ifNoneMatch: true,
        contentType: "application/x-ndjson",
        metadata: {
          "trace-id": manifest.trace_id,
          "manifest-revision": String(manifest.revision),
          "source-content-sha256": manifest.raw.content_sha256,
          "source-epoch": String(manifest.raw.epoch),
        },
      });
      created = ack.created;
    } catch (error) {
      if (!isObjectStoreError(error, "already_exists")) throw error;
    }
    // Create-only: on a hit the stored object is the output, so it is described
    // from its own bytes. Its metadata names whichever revision wrote it first.
    const stored = created
      ? body
      : await readCapped(
          await this.deps.store.get(objectKey(key)),
          NORMALIZED_GROWTH *
            (this.deps.maxSessionBytes ?? MAX_RELAYHISTORY_SESSION_BYTES),
        );
    if (!stored)
      fail(
        "normalized_object_too_large",
        "stored normalized object exceeds the limit",
      );
    const outputSha256 = await sha256Hex(stored);
    return {
      outputKey: key,
      revision: manifest.revision,
      normalizedVersion: RELAYHISTORY_NORMALIZED_VERSION,
      contentSha256: manifest.raw.content_sha256,
      outputSha256,
      outputBytes: stored.byteLength,
      convergenceEventCount: convergenceEventLines(stored),
      unprojectedRecords: projection.unprojected,
      outputReused: !created,
      outputDivergent: !created && outputSha256 !== (await sha256Hex(body)),
    };
  }
}
