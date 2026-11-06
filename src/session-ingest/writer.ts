/**
 * One delivery batch written to S3 in the 96Bulls pipeline layout: the S3 half
 * of relayhistory-api's `s3_first` write (relayhistory-cloud
 * packages/relayhistory/src/lib/write-path/live-objects.ts at the commit in
 * vendor/delivery/SOURCE.json), run in the ingest Worker with the pipeline's own code.
 *
 *   {keyRoot}/{org}/v1/ws=…/m=srv_…/h=…/d=…/s=…/manifest.json
 *   {keyRoot}/{org}/v1/ws=…/m=srv_…/h=…/d=…/s=…/raw/{seq:010}-{offset}-{sha8}.ndjson.zst
 *   {keyRoot}/{org}/live-sessions/v1/ws=…/m=srv_…/h=…/s=….json   (location pointer)
 *
 * The layout, the machine id (`srv_<sha256(ws \x1f origin \x1f user)[0:32]>`),
 * the chunk line shape, the manifest owner (`LIVE_OWNER`) and the replay
 * extension are relayhistory's exactly, ON PURPOSE: during the transition both
 * writers append to the same session manifests (an old Desktop on relayhistory
 * and a new one here can upload the same session), and the manifest
 * compare-and-swap makes that safe. Only `writer.plugin_version` says which
 * service wrote a revision.
 *
 * What is different from relayhistory's copy, and why:
 *
 * - No exact-scrub precondition. relayhistory refuses (`scrub_not_identity`) a
 *   batch whose lines the pipeline scrubber would change, because its Neon
 *   replica rebuilds Neon rows from the stored lines. Here S3 is the record:
 *   the stored lines are the scrubbed lines, as for every chunk the pipeline
 *   seals, and the batch lands.
 * - A string past the scrub bound that the pipeline segmenter (`scrubLong`,
 *   rule set 2) cannot cut safely is redacted, not refused
 *   before sealing (oversize.ts). The client cannot re-cut a delivery batch,
 *   so refusing it would only move it to a fallback that no longer exists.
 * - No session-count refusal. A batch wider than one invocation's subrequest
 *   budget is staged and fanned out by the queue (staging.ts) instead of
 *   falling back.
 * - A record with no session goes to one per-origin session, `s=_origin`
 *   under harness `_none`, instead of falling back: the batch's own records
 *   still land in S3 and fold by (origin, record, revision) as always.
 *
 * Nothing here logs or puts content in an error message.
 */
import { createHash } from "node:crypto";
import { LAYOUT_VERSION } from "@rhc-object-store";
import { SCRUB_RULESET, sealRawChunk } from "@rhc/lib/pipeline/raw-chunks.ts";
import {
  computeContentSha256,
  isObjectStoreError,
  manifestKey,
  objectKey,
  parseManifest,
  rawChunkKey,
  readAll,
  type ChunkEntry,
  type ManifestV1,
  type ObjectStore,
  type ObjectVersion,
  type SessionLocation,
} from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import type { PreparedDeliveryRecord } from "./vendor/delivery/prepare.ts";
import { fitForScrub } from "./oversize.ts";

/** Shared with relayhistory-api's live writer (see the header). */
export const LIVE_OWNER = "relayhistory-api:delivery";
export const INGEST_PLUGIN_VERSION = "96bulls-session-ingest/1";
/** Manifest extension recording which batches each chunk holds (replay dedupe), as relayhistory. */
export const LIVE_EXTENSION = "relayhistory_live";
/** Authenticated user and pre-layout session identity consumed by the session pipeline. */
export const BULLS_EXTENSION = "96bulls";
const REMEMBERED_BATCHES = 64;
export const CAS_ATTEMPTS = 8;
/** Session groups written at once: a Worker keeps at most six connections open. */
export const INGEST_CONCURRENCY = 6;
export const GROUP_ATTEMPTS = 3;
/** Where records without a session live (one session per origin). */
export const NO_SESSION_HARNESS = "_none";
export const NO_SESSION_ID = "_origin";

export class IngestWriteError extends Error {
  constructor(readonly code: "tenant_not_a_segment" | "foreign_manifest" | "manifest_identity_mismatch" | "manifest_contended" | "pointer_invalid") {
    super(`session ingest write refused: ${code}`);
    this.name = "IngestWriteError";
  }
}

export interface WriteTarget {
  store: ObjectStore;
  keyRoot: string;
  now?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export interface WriteBatch {
  orgId: string;
  workspaceId: string;
  userId: string;
  originId: string;
  batchDigest: string;
  records: PreparedDeliveryRecord[];
}

export interface ManifestWrite {
  key: string;
  manifest: ManifestV1;
  version: ObjectVersion;
  /** False when this batch was already in the session (a replay). */
  written: boolean;
}

export interface WritePart {
  chunkKey: string;
  sha256: string;
  source: string;
  sessionId: string;
  lines: number;
}

export interface WriteResult {
  manifests: ManifestWrite[];
  parts: WritePart[];
  /** For each input record: its part and line (same order as `records`). */
  placed: { part: number; line: number }[];
  machineId: string;
}

const sha256Hex = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export function backoffMs(attempt: number, random: () => number = Math.random, base = 50, cap = 1_000): number {
  return Math.floor(random() * Math.min(cap, base * 2 ** attempt));
}

/** A transient store failure (network, 408/429/5xx such as SlowDown or a KMS throttle). Every retried step is idempotent. */
export function retryableStorageFailure(error: unknown): boolean {
  if (isObjectStoreError(error)) return error.code === "transient";
  if (error instanceof IngestWriteError) return false;
  const e = error as { name?: unknown; code?: unknown } | null;
  // Node network errors (ECONNRESET, ETIMEDOUT, UND_ERR_*) and fetch's TypeError.
  return !!e && (e.name === "TypeError" || (typeof e.code === "string" && /^(E[A-Z]+|UND_ERR_[A-Z_]+)$/.test(e.code)));
}

const SEGMENT = /^[A-Za-z0-9._-]{1,128}$/;

export function assertTenantSegment(keyRoot: string, orgId: string): void {
  if (!SEGMENT.test(orgId) || orgId.startsWith(".") || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot)) throw new IngestWriteError("tenant_not_a_segment");
}

/** relayhistory's `liveMachineId`: per (workspace, origin, user), never the exporter's `delivery_<md5>`. */
export function liveMachineId(workspaceId: string, originId: string, userId: string): string {
  return `srv_${sha256Hex(`${workspaceId}\u001f${originId}\u001f${userId}`).slice(0, 32)}`;
}

/** UUIDv7-shaped id from a seed, as relayhistory's `derivedUuidV7`. */
export function derivedUuidV7(seed: string, ms: number): string {
  const h = sha256Hex(seed);
  const ts = Math.max(0, Math.floor(ms)).toString(16).padStart(12, "0").slice(-12);
  const variant = ((parseInt(h[16]!, 16) & 0x3) | 0x8).toString(16);
  return `${ts.slice(0, 8)}-${ts.slice(8, 12)}-7${h.slice(1, 4)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const TIME_FIELDS = ["ts_ms", "timestamp_ms", "created_ms", "spawned_at_ms", "updated_ms", "first_activity_ms", "last_activity_ms"];

/** One chunk line: the courier's record shape (relayhistory's `liveLine`). */
export function liveLine(record: PreparedDeliveryRecord, receivedAt: string): string {
  return JSON.stringify({
    kind: record.kind,
    origin_id: record.origin_id,
    record_id: record.record_id,
    revision: String(record.revision),
    operation: record.operation,
    received_at: receivedAt,
    payload: record.operation === "upsert" ? record.payload : null,
  });
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function readJson(store: ObjectStore, key: string): Promise<{ text: string; version: ObjectVersion } | null> {
  try {
    const object = await store.getObject(objectKey(key));
    return { text: decoder.decode(await readAll(object.body)), version: object.meta.version };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}

interface Pointer {
  v: 1;
  started_at: string;
  manifest_key: string;
}

async function resolveLocation(
  target: WriteTarget,
  base: Omit<SessionLocation, "startDate">,
  pointerKey: string,
  startedMs: number,
): Promise<{ location: SessionLocation; startedAt: string }> {
  for (let attempt = 0; attempt < 2; attempt++) {
    const stored = await readJson(target.store, pointerKey);
    if (stored) {
      let pointer: Pointer;
      try {
        pointer = JSON.parse(stored.text) as Pointer;
      } catch {
        throw new IngestWriteError("pointer_invalid");
      }
      const started = Date.parse(pointer.started_at);
      if (pointer.v !== 1 || !Number.isFinite(started)) throw new IngestWriteError("pointer_invalid");
      return { location: { ...base, startDate: new Date(started) }, startedAt: new Date(started).toISOString() };
    }
    const startedAt = new Date(startedMs).toISOString();
    const location = { ...base, startDate: new Date(startedMs) };
    const body: Pointer = { v: 1, started_at: startedAt, manifest_key: manifestKey(location) };
    try {
      await target.store.put(objectKey(pointerKey), encoder.encode(JSON.stringify(body)), { ifNoneMatch: true, contentType: "application/json" });
      return { location, startedAt };
    } catch (error) {
      if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
    }
  }
  throw new IngestWriteError("manifest_contended");
}

function rememberedBatches(manifest: ManifestV1 | null): Map<string, number> {
  const ext = manifest?.extensions?.[LIVE_EXTENSION] as { batches?: unknown } | undefined;
  const out = new Map<string, number>();
  if (!Array.isArray(ext?.batches)) return out;
  for (const item of ext.batches) if (Array.isArray(item) && typeof item[0] === "string" && Number.isSafeInteger(item[1])) out.set(item[0], item[1] as number);
  return out;
}

export function admittedIdentity(manifest: ManifestV1 | null, userId: string, sessionId: string): Record<string, unknown> {
  const stored = manifest?.extensions?.[BULLS_EXTENSION];
  if (stored !== undefined && (stored === null || typeof stored !== "object" || Array.isArray(stored)))
    throw new IngestWriteError("manifest_identity_mismatch");
  const identity = (stored ?? {}) as Record<string, unknown>;
  if (("user_id" in identity && identity.user_id !== userId) ||
      ("canonical_session_id" in identity && identity.canonical_session_id !== sessionId))
    throw new IngestWriteError("manifest_identity_mismatch");
  return { ...identity, user_id: userId, canonical_session_id: sessionId };
}

export function nextManifest(args: {
  batches: [string, number][];
  prev: ManifestV1 | null;
  location: SessionLocation;
  startedAt: string;
  sessionFields: ManifestV1["session"];
  entry: ChunkEntry;
  scrubVersion: number;
  traceId: string;
  contentSha256: string;
  chunks: ChunkEntry[];
  now: string;
  identity: Record<string, unknown>;
}): ManifestV1 {
  const { prev, entry, chunks, now } = args;
  const epochs = prev?.raw.epochs ?? [{ epoch: 0, opened_at: now, reason: "initial" as const, first_seq: 0, opened_by: LIVE_OWNER }];
  const scrubVersion = Math.min(prev?.raw.scrub?.version ?? args.scrubVersion, args.scrubVersion);
  return {
    schema_v: 1,
    layout: LAYOUT_VERSION,
    trace_id: prev?.trace_id ?? args.traceId,
    revision: (prev?.revision ?? 0) + 1,
    created_at: prev?.created_at ?? now,
    updated_at: now,
    session: prev?.session ?? args.sessionFields,
    writer: { delivery_instance_id: "96bulls-session-ingest", plugin_version: INGEST_PLUGIN_VERSION },
    raw: {
      owner: LIVE_OWNER,
      state: "open",
      updated_at: now,
      format: "ndjson",
      compression: "zstd",
      chunk_count: chunks.length,
      raw_bytes: chunks.reduce((n, c) => n + c.bytes, 0),
      stored_bytes: chunks.reduce((n, c) => n + c.stored_bytes, 0),
      content_sha256: args.contentSha256,
      epoch: epochs.length - 1,
      epochs,
      next_offset: entry.start_offset + entry.bytes,
      chunks,
      scrub: { ruleset: SCRUB_RULESET, version: scrubVersion },
    },
    ...(prev?.server !== undefined ? { server: prev.server } : {}),
    extensions: {
      ...(prev?.extensions ?? {}),
      [LIVE_EXTENSION]: { batches: args.batches },
      [BULLS_EXTENSION]: args.identity,
    },
  } as ManifestV1;
}

/** One (source, session) group: its chunk lines (`liveLine`) in batch order. */
export interface Group {
  source: string;
  sessionId: string;
  lines: string[];
  /**
   * Per line, what the ledger needs that a line does not hold: the record's
   * revision id, digest, source and session (null for the origin's no-session
   * group). Set by `groupRecords`; carried through a staged object.
   */
  meta?: LineMeta[];
}

export type LineMeta = [revisionId: string, digest: string, source: string, sessionId: string | null];

/** The payload's own clock of a chunk line, else `fallback`. */
export function lineTimeMs(line: string, fallback: number): number {
  try {
    const payload = (JSON.parse(line) as { payload?: Record<string, unknown> | null }).payload ?? {};
    for (const field of TIME_FIELDS) {
      const value = payload[field];
      if (typeof value === "number" && Number.isFinite(value) && value > 0 && value < 8_640_000_000_000_000) return value;
    }
  } catch {
    // A staged line is always JSON; a bad one only loses its clock.
  }
  return fallback;
}

export interface GroupBatch {
  orgId: string;
  workspaceId: string;
  userId: string;
  batchDigest: string;
}

async function writeGroup(
  target: WriteTarget,
  batch: GroupBatch,
  machineId: string,
  group: Group,
  receivedAt: Date,
  source: Uint8Array,
  attempts: { ambiguousManifestPut: boolean },
): Promise<{ manifest: ManifestWrite; chunkKey: string; sha256: string }> {
  const now = target.now ?? (() => new Date());
  const prefix = `${target.keyRoot}/${batch.orgId}`;
  const base = {
    prefix,
    workspaceId: layoutComponent(batch.workspaceId),
    machineId,
    harness: layoutComponent(group.source),
    sessionId: layoutComponent(group.sessionId),
  };
  const pointerKey = `${prefix}/live-sessions/v1/ws=${base.workspaceId}/m=${base.machineId}/h=${base.harness}/s=${base.sessionId}.json`;
  const startedMs = Math.min(...group.lines.map((l) => lineTimeMs(l, receivedAt.getTime())), receivedAt.getTime());
  const { location, startedAt } = await resolveLocation(target, base, pointerKey, startedMs);
  const key = manifestKey(location);
  const traceId = derivedUuidV7(`${LIVE_OWNER}\u001f${key}`, Date.parse(startedAt));

  for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
    const stored = await readJson(target.store, key);
    const prev = stored ? await parseManifest(stored.text, key) : null;
    if (prev && prev.raw.owner !== LIVE_OWNER) throw new IngestWriteError("foreign_manifest");
    const identity = admittedIdentity(prev, batch.userId, group.sessionId);
    // A replayed batch was already appended (by this service or by relayhistory-api).
    const remembered = rememberedBatches(prev);
    const seq = remembered.get(batch.batchDigest);
    const found = seq === undefined ? undefined : prev?.raw.chunks[seq];
    const prior = found && found.seq === seq ? found : undefined;
    if (prev && stored && prior)
      return {
        manifest: { key, manifest: prev, version: stored.version, written: attempts.ambiguousManifestPut },
        sha256: prior.sha256,
        chunkKey: rawChunkKey(location, prior.seq, prior.start_offset, prior.sha256, "ndjson"),
      };
    const sealed = await sealRawChunk(target.store, {
      location,
      format: "ndjson",
      seq: prev?.raw.chunks.length ?? 0,
      epoch: prev?.raw.epoch ?? 0,
      startOffset: prev?.raw.next_offset ?? 0,
      source,
      traceId,
    });
    const chunks = [...(prev?.raw.chunks ?? []), sealed.entry];
    const manifest = nextManifest({
      batches: [...[...remembered].map(([d, at]) => [d, at] as [string, number]), [batch.batchDigest, sealed.entry.seq] as [string, number]].slice(-REMEMBERED_BATCHES),
      prev,
      location,
      startedAt,
      sessionFields: { workspace_id: base.workspaceId, machine_id: base.machineId, harness: base.harness, session_id: base.sessionId, started_at: startedAt },
      entry: sealed.entry,
      scrubVersion: sealed.scrub.version,
      traceId,
      contentSha256: await computeContentSha256(chunks),
      chunks,
      identity,
      now: new Date(Math.max(now().getTime(), Date.parse(prev?.updated_at ?? "0"))).toISOString(),
    });
    await parseManifest(manifest, key);
    try {
      const ack = await target.store.put(
        objectKey(key),
        encoder.encode(JSON.stringify(manifest)),
        stored ? { ifMatch: stored.version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" },
      );
      return { manifest: { key, manifest, version: ack.version, written: true }, chunkKey: sealed.key, sha256: sealed.entry.sha256 };
    } catch (error) {
      if (!isObjectStoreError(error, "precondition_failed") && !isObjectStoreError(error, "already_exists")) {
        attempts.ambiguousManifestPut = true;
        throw error;
      }
      await (target.sleep ?? defaultSleep)(backoffMs(attempt, target.random));
    }
  }
  throw new IngestWriteError("manifest_contended");
}

/**
 * The batch's records as (source, session) groups of chunk lines. Records with
 * no session go to the origin's `_none/_origin` session. `index[g][l]` is the
 * input record at line l of group g.
 */
export function groupRecords(records: PreparedDeliveryRecord[], receivedAt: Date): { groups: Group[]; index: number[][] } {
  const map = new Map<string, { group: Group; index: number[] }>();
  const stamp = receivedAt.toISOString();
  records.forEach((record, i) => {
    const none = record.session_id === null || record.session_id === "";
    const source = none ? NO_SESSION_HARNESS : record.source;
    const sessionId = none ? NO_SESSION_ID : record.session_id!;
    const key = JSON.stringify([source, sessionId]);
    const entry = map.get(key) ?? { group: { source, sessionId, lines: [], meta: [] }, index: [] };
    entry.group.lines.push(liveLine(fitForScrub(record), stamp));
    entry.group.meta!.push([record.revision_id, record.digest, record.source, none ? null : record.session_id!]);
    entry.index.push(i);
    map.set(key, entry);
  });
  const entries = [...map.values()];
  return { groups: entries.map((e) => e.group), index: entries.map((e) => e.index) };
}

/** Groups written to their session manifests, `INGEST_CONCURRENCY` at a time, each retried in place. */
export async function writeGroups(
  target: WriteTarget,
  batch: GroupBatch & { originId: string },
  groups: Group[],
  receivedAt: Date,
): Promise<{ manifests: ManifestWrite[]; parts: WritePart[]; machineId: string }> {
  assertTenantSegment(target.keyRoot, batch.orgId);
  const machineId = liveMachineId(batch.workspaceId, batch.originId, batch.userId);
  const sources = groups.map((g) => encoder.encode(g.lines.join("\n") + "\n"));
  const sleep = target.sleep ?? defaultSleep;
  const writeWithRetry = async (i: number) => {
    const attempts = { ambiguousManifestPut: false };
    for (let attempt = 0; ; attempt++) {
      try {
        return await writeGroup(target, batch, machineId, groups[i]!, receivedAt, sources[i]!, attempts);
      } catch (error) {
        if (attempt + 1 >= GROUP_ATTEMPTS || !retryableStorageFailure(error)) throw error;
        await sleep(backoffMs(attempt + 1, target.random, 100, 2_000));
      }
    }
  };
  const written: Awaited<ReturnType<typeof writeGroup>>[] = new Array(groups.length);
  let next = 0;
  let failed = false;
  await Promise.all(
    Array.from({ length: Math.min(INGEST_CONCURRENCY, groups.length) }, async () => {
      while (!failed && next < groups.length) {
        const i = next++;
        try {
          written[i] = await writeWithRetry(i);
        } catch (error) {
          failed = true;
          throw error;
        }
      }
    }),
  );
  const parts: WritePart[] = groups.map((group, part) => ({ chunkKey: written[part]!.chunkKey, sha256: written[part]!.sha256, source: group.source, sessionId: group.sessionId, lines: group.lines.length }));
  return { manifests: written.map((w) => w.manifest), parts, machineId };
}

/** Every record of the batch, inline: grouped, written, and placed (for the replica). */
export async function writeBatchObjects(target: WriteTarget, batch: WriteBatch): Promise<WriteResult> {
  const receivedAt = (target.now ?? (() => new Date()))();
  const { groups, index } = groupRecords(batch.records, receivedAt);
  const written = await writeGroups(target, batch, groups, receivedAt);
  const placed: { part: number; line: number }[] = new Array(batch.records.length);
  index.forEach((records, part) => records.forEach((recordIndex, line) => (placed[recordIndex] = { part, line })));
  return { ...written, placed };
}
