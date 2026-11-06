/** Authenticated immutable analysis cache. Source SQL owns the public v1 payload;
 * the Worker authenticates scoped references/currentness and never queries Neon. */
import { createHash } from "node:crypto";
import { Buffer } from "node:buffer";
import { CanonicalReadError } from "./canonical.ts";
import {
  canonicalArtifactPage,
  readArtifactJson,
  assertArtifactReference,
  type ArtifactReadDeps,
} from "./canonical-artifact-reader.ts";
import type {
  ArtifactIdentity,
  ArtifactPublication,
  ArtifactSource,
} from "./canonical-artifact.ts";
import {
  ANALYSIS_MAX_SNAPSHOT_BYTES,
  ANALYSIS_FRESH_SECONDS,
  ANALYSIS_MAX_HISTORY,
  analysisSnapshotJson,
  analysisGenerationJson,
  analysisSourceSnapshotJson,
  type CachedBriefTitle,
  type SessionAnalysisIndex,
  type SessionAnalysisSnapshot,
} from "./session-analysis-artifact.ts";
export interface AnalysisCacheDeps extends ArtifactReadDeps {
  now?: () => number;
}
const obj = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const hash = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{64}$/u.test(v);
const identityEqual = (v: unknown, id: ArtifactIdentity) =>
  obj(v) &&
  v.orgId === id.orgId &&
  v.workspaceId === id.workspaceId &&
  v.source === id.source &&
  v.sessionId === id.sessionId;
const sourceEqual = (a: ArtifactSource, b: ArtifactSource) =>
  a.epoch === b.epoch && a.fingerprint === b.fingerprint;
function fail(reason: string): never {
  throw new CanonicalReadError(reason);
}
const digest = (value: string) =>
  createHash("sha256").update(value).digest("hex");
interface Ready extends ArtifactPublication {
  exists: true;
  current: true;
  snapshotAt: string;
  snapshotId: string;
  snapshotSha256: string;
  validUntil: string;
}
interface Absent {
  exists: false;
  fingerprint: string;
  epoch: string | null;
}
function unwrap(value: unknown) {
  if (Array.isArray(value)) {
    if (value.length !== 1) fail("analysis_publication");
    return value[0];
  }
  return value;
}
function parseClaim(
  value: unknown,
  id: ArtifactIdentity,
  now: number,
): Ready | Absent {
  value = unwrap(value);
  if (
    !obj(value) ||
    value.version !== 1 ||
    !identityEqual(value.identity, id) ||
    typeof value.exists !== "boolean" ||
    typeof value.current !== "boolean" ||
    !obj(value.source) ||
    !hash(value.source.fingerprint) ||
    !(
      (value.source.epoch === null && !value.exists) ||
      (typeof value.source.epoch === "string" &&
        /^(0|[1-9]\d*)$/u.test(value.source.epoch))
    )
  )
    fail("analysis_publication");
  if (!value.exists) {
    if (value.current) fail("analysis_publication");
    return {
      exists: false,
      fingerprint: value.source.fingerprint as string,
      epoch: value.source.epoch as string | null,
    };
  }
  if (!value.current) fail("analysis_cache_not_ready");
  if (
    !hash(value.generation) ||
    typeof value.prefix !== "string" ||
    !value.prefix.endsWith("/") ||
    typeof value.snapshotAt !== "string" ||
    typeof value.validUntil !== "string" ||
    typeof value.snapshotId !== "string" ||
    !value.snapshotId ||
    value.snapshotId.length > 128 ||
    !hash(value.snapshotSha256)
  )
    fail("analysis_publication");
  const snapshotAt = Date.parse(value.snapshotAt),
    until = Date.parse(value.validUntil);
  if (
    !Number.isFinite(snapshotAt) ||
    !Number.isFinite(until) ||
    snapshotAt > now + 30_000 ||
    now - snapshotAt > ANALYSIS_FRESH_SECONDS * 1000 ||
    until <= now ||
    until > snapshotAt + ANALYSIS_FRESH_SECONDS * 1000 ||
    until <= snapshotAt
  )
    fail("analysis_cache_expired");
  assertArtifactReference(value.index, value.prefix);
  const claim: Ready = {
    version: 1,
    exists: true,
    current: true,
    identity: id,
    source: value.source as unknown as ArtifactSource,
    generation: value.generation,
    prefix: value.prefix,
    index: value.index,
    snapshotAt: value.snapshotAt,
    snapshotId: value.snapshotId,
    snapshotSha256: value.snapshotSha256,
    validUntil: value.validUntil,
  };
  return claim;
}
function sameClaim(a: Ready, b: Ready) {
  return (
    a.generation === b.generation &&
    a.prefix === b.prefix &&
    sourceEqual(a.source, b.source) &&
    a.index.key === b.index.key &&
    a.index.sha256 === b.index.sha256 &&
    a.index.bytes === b.index.bytes &&
    a.snapshotAt === b.snapshotAt &&
    a.snapshotId === b.snapshotId &&
    a.snapshotSha256 === b.snapshotSha256 &&
    a.validUntil === b.validUntil
  );
}
async function readAnalysisCache(
  deps: AnalysisCacheDeps,
  identity: ArtifactIdentity,
  titleOnly: boolean,
) {
  const stop = new AbortController(),
    signal = AbortSignal.any([deps.signal, stop.signal]),
    now = deps.now ?? Date.now;
  try {
    const ack = parseClaim(
      await deps.publication(identity, signal),
      identity,
      now(),
    );
    if (!ack.exists) {
      const fresh = parseClaim(
        await deps.publication(identity, signal),
        identity,
        now(),
      );
      if (
        fresh.exists ||
        fresh.fingerprint !== ack.fingerprint ||
        fresh.epoch !== ack.epoch
      )
        fail("source_generation_changed");
      if (titleOnly) fail("analysis_title_not_ready");
      return {
        outcome: "not_found" as const,
        payload: { outcome: "not_found" as const },
        generation: ack.fingerprint,
        snapshotAt: null,
      };
    }
    const claim: Ready = ack;
    const read = {
      read: (key: string, signal: AbortSignal) => deps.read(key, signal),
      signal,
    };
    const loaded = await readArtifactJson(read, claim.index, claim.prefix);
    if (
      !obj(loaded) ||
      loaded.version !== 1 ||
      loaded.kind !== "session-analysis-index" ||
      loaded.codec !== "canonical-analysis-graph-v1" ||
      loaded.projection !== "read_session_analysis_v1/0031" ||
      !["ok", "not_found"].includes(String(loaded.outcome)) ||
      !identityEqual(loaded.identity, identity) ||
      !obj(loaded.source) ||
      !sourceEqual(loaded.source as unknown as ArtifactSource, claim.source) ||
      loaded.generation !== claim.generation ||
      loaded.snapshotAt !== claim.snapshotAt ||
      loaded.snapshotId !== claim.snapshotId ||
      loaded.snapshotSha256 !== claim.snapshotSha256 ||
      !obj(loaded.counts)
    )
      fail("analysis_index");
    for (const field of ["requests", "analyses"])
      if (
        typeof loaded.counts[field] !== "number" ||
        !Number.isInteger(loaded.counts[field]) ||
        (loaded.counts[field] as number) < 0 ||
        (loaded.counts[field] as number) > ANALYSIS_MAX_HISTORY
      )
        fail("analysis_history_bound");
    let cachedBriefTitle: CachedBriefTitle | undefined;
    let sourceSnapshotSha256: string | undefined;
    if (
      Object.hasOwn(loaded, "cachedBriefTitle") ||
      Object.hasOwn(loaded, "sourceSnapshotSha256")
    ) {
      if (!obj(loaded.cachedBriefTitle) || !hash(loaded.sourceSnapshotSha256))
        fail("analysis_title_attestation");
      cachedBriefTitle = {
        version: 1,
        complete: true,
        title: loaded.cachedBriefTitle.title as string | null,
      };
      // The shared canonicalizer validates complete/version/trimmed title before
      // metadata can affect generation. Do not copy unknown wrapper properties.
      if (
        loaded.cachedBriefTitle.version !== 1 ||
        loaded.cachedBriefTitle.complete !== true
      )
        fail("analysis_title_attestation");
      let calculated: string;
      try {
        calculated = digest(
          analysisSourceSnapshotJson(claim.snapshotSha256, cachedBriefTitle),
        );
      } catch {
        fail("analysis_title_attestation");
      }
      if (calculated !== loaded.sourceSnapshotSha256)
        fail("analysis_source_snapshot_digest");
      sourceSnapshotSha256 = calculated;
    }
    if (
      digest(
        analysisGenerationJson(
          identity,
          claim.source,
          claim.snapshotAt,
          claim.snapshotId,
          claim.snapshotSha256,
          sourceSnapshotSha256,
        ),
      ) !== claim.generation
    )
      fail("analysis_generation");
    if (
      loaded.outcome === "not_found" &&
      (loaded.counts.requests !== 0 || loaded.counts.analyses !== 0)
    )
      fail("analysis_index");
    assertArtifactReference(loaded.graph, claim.prefix);
    if (titleOnly) {
      if (!cachedBriefTitle || !sourceSnapshotSha256)
        fail("analysis_title_not_ready");
      const fresh = parseClaim(
        await deps.publication(identity, signal),
        identity,
        now(),
      );
      if (!fresh.exists || !sameClaim(claim, fresh))
        fail("source_generation_changed");
      return {
        outcome: "title" as const,
        cachedBriefTitle,
        sourceSnapshotSha256,
        generation: claim.generation,
        snapshotAt: claim.snapshotAt,
        snapshotId: claim.snapshotId,
        validUntil: claim.validUntil,
      };
    }

    const index = loaded as unknown as SessionAnalysisIndex;
    const graphClaim = {
      version: 1,
      exists: true,
      current: true,
      identity,
      generation: claim.generation,
      source: claim.source,
      prefix: claim.prefix,
      index: index.graph,
    };
    let assembled = 0;
    async function table(name: string, expected: number) {
      const payloads: Record<string, unknown>[] = [],
        seen = new Set<string>();
      let cursor: string | null = null;
      for (let page = 0; page <= ANALYSIS_MAX_HISTORY; page++) {
        const result = await canonicalArtifactPage(
          { signal, read: read.read, publication: async () => graphClaim },
          identity,
          {
            table: name,
            view: "analysis",
            limit: ANALYSIS_MAX_HISTORY,
            order: "asc",
            cursor,
            generation: claim.generation,
          },
        );
        for (const { row } of result.rows) {
          if (
            row.org_id !== identity.orgId ||
            row.workspace_id !== identity.workspaceId ||
            row.source !== identity.source ||
            row.session_id !== identity.sessionId ||
            row.ordinal !== payloads.length ||
            !obj(row.payload) ||
            row.payload.sessionId !== identity.sessionId
          )
            fail("analysis_row_identity");
          assembled += Buffer.byteLength(JSON.stringify(row.payload));
          if (assembled > ANALYSIS_MAX_SNAPSHOT_BYTES)
            fail("analysis_response_bound");
          payloads.push(row.payload);
          if (
            payloads.length > expected ||
            payloads.length > ANALYSIS_MAX_HISTORY
          )
            fail("analysis_history_bound");
        }
        if (!result.nextCursor) {
          if (payloads.length !== expected) fail("analysis_history_bound");
          return payloads;
        }
        if (!result.rows.length || seen.has(result.nextCursor))
          fail("cursor_progress");
        seen.add(result.nextCursor);
        cursor = result.nextCursor;
      }
      return fail("analysis_history_bound");
    }
    const requests = await table(
        "session_analysis_requests",
        index.counts.requests,
      ),
      analyses = await table("session_analyses", index.counts.analyses);
    if (index.outcome === "not_found" && (requests.length || analyses.length))
      fail("analysis_index");
    const payload: SessionAnalysisSnapshot["payload"] =
      index.outcome === "not_found"
        ? { outcome: "not_found" }
        : { outcome: "ok", sessionId: identity.sessionId, requests, analyses };
    if (digest(analysisSnapshotJson(payload)) !== claim.snapshotSha256)
      fail("analysis_snapshot_digest");
    const fresh = parseClaim(
      await deps.publication(identity, signal),
      identity,
      now(),
    );
    if (!fresh.exists || !sameClaim(claim, fresh))
      fail("source_generation_changed");
    return {
      outcome: index.outcome,
      payload,
      generation: claim.generation,
      snapshotAt: claim.snapshotAt,
      snapshotId: claim.snapshotId,
      validUntil: claim.validUntil,
    };
  } finally {
    stop.abort();
  }
}

type CacheReadResult = Awaited<ReturnType<typeof readAnalysisCache>>;
export function readSessionAnalysisCache(
  deps: AnalysisCacheDeps,
  identity: ArtifactIdentity,
): Promise<Exclude<CacheReadResult, { outcome: "title" }>> {
  return readAnalysisCache(deps, identity, false) as Promise<
    Exclude<CacheReadResult, { outcome: "title" }>
  >;
}
/** Reads one SHA-bound compact wrapper; never hydrates v1 result graphs. */
export function readSessionAnalysisTitle(
  deps: AnalysisCacheDeps,
  identity: ArtifactIdentity,
): Promise<Extract<CacheReadResult, { outcome: "title" }>> {
  return readAnalysisCache(deps, identity, true) as Promise<
    Extract<CacheReadResult, { outcome: "title" }>
  >;
}
