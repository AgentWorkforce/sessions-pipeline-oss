import type { NodeReadProfile } from "../session-read-api/node-read-profile.ts";
/** Capture/full-union hydration/immutable writes/CAS. All database calls use backend
 * RPCs; the public claim RPC has the existing search-worker JWT policy. */
import * as zlib from "node:zlib";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { CanonicalReadError, canonicalProducerSnapshot, canonicalSourceGeneration, type CanonicalPointer, type CanonicalIdentity } from "../session-read-api/canonical.ts";
import { ARTIFACT_MAX_SOURCE_POINTERS, type ArtifactSource, type ArtifactPublication } from "../session-read-api/canonical-artifact.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { SessionScope } from "./pointer.ts";
import { CanonicalTimingError, canonicalTimingProjection } from "../session-read-api/canonical-timing.ts";
import { artifactDigest, ArtifactBuildError, materializeCanonicalArtifacts } from "./replay-pages.ts";
export interface ReplayCapture {
  exists: boolean; blocked: boolean; identity: CanonicalIdentity; source: ArtifactSource;
  snapshot: { epoch: string; pointers: CanonicalPointer[]; [key: string]: unknown };
}
export interface ReplayProducerDeps {
  profile?: NodeReadProfile;
  publish?: (params: unknown[]) => Promise<boolean>;
  db: SqlExecutor; store: ObjectStore; provider: string; bucket: string;
  prefix: { PIPELINE_ORG_FROM_PREFIX?: string; PIPELINE_KEY_PREFIX?: string };
  decompress?: (bytes: Uint8Array, maxBytes: number) => Uint8Array;
}
/**
 * A replay-stage throw that retrying the same message cannot fix.
 * - superseded: the source moved on (lost source race or lost publication CAS);
 *   the newer source write has its own S3 event, which re-runs the scope.
 * - capacity: the scope exceeds a producer bound or a PostgreSQL capacity guard
 *   (SQLSTATE 54000); the same source refuses the same way on every retry.
 * Anything else (S3 AccessDenied, network, unexpected throws) is null and stays
 * a batch failure. Classified by typed error and code only, never message text.
 */
export interface ReplayRefusal { kind: "superseded" | "capacity"; reason: "replay_source_superseded" | "replay_capacity_refused"; code: string }
const SUPERSEDED_ARTIFACT_CODES = new Set(["artifact_source_changed", "artifact_publish_cas_failed"]);
const CAPACITY_ARTIFACT_CODES = new Set(["artifact_capture_bound", "artifact_row_bound", "artifact_output_bound", "artifact_depth_bound"]);
export function classifyReplayFailure(error: unknown): ReplayRefusal | null {
  const superseded = (code: string): ReplayRefusal => ({ kind: "superseded", reason: "replay_source_superseded", code });
  const capacity = (code: string): ReplayRefusal => ({ kind: "capacity", reason: "replay_capacity_refused", code });
  if (error instanceof ArtifactBuildError) {
    if (SUPERSEDED_ARTIFACT_CODES.has(error.code)) return superseded(error.code);
    if (CAPACITY_ARTIFACT_CODES.has(error.code)) return capacity(error.code);
    return null;
  }
  if (error instanceof CanonicalReadError) {
    if (error.reason === "source_generation_changed") return superseded(error.reason);
    // >ARTIFACT_MAX_SOURCE_POINTERS pointers, or >256 MiB / >1M records (projection "scope_too_large").
    if (error.reason === "pointer_bound") return capacity(error.reason);
    if (error.reason === "canonical_projection_incomplete" && error.details.includes("scope_too_large")) return capacity("scope_too_large");
    return null;
  }
  if (error instanceof CanonicalTimingError) return capacity(error.code);
  // node-postgres / PGlite surface SQLSTATE as `code` (e.g. replay capture pointer/metadata capacity).
  if (error instanceof Error && (error as { code?: unknown }).code === "54000") return capacity("54000");
  return null;
}
/**
 * Deterministic capture_session_replay_pages refusals (migration 20261105026000),
 * classified by SQLSTATE plus the exact RAISE message prefix. The same function
 * raises transient 55000s ("source epoch missing"; route_session_large_read's
 * "durable handoff unavailable") that must stay retryable, so code alone is not enough.
 */
export type CaptureRefusal = "capture_ambiguous_source" | "capture_capacity_exceeded";
const CAPTURE_REFUSAL_MESSAGES: readonly (readonly [code: string, message: string, refusal: CaptureRefusal])[] = [
  ["55000", "replay capture: ambiguous canonical source", "capture_ambiguous_source"],
  ["54000", "replay capture: pointer capacity exceeded", "capture_capacity_exceeded"],
  ["54000", "replay capture: metadata capacity exceeded", "capture_capacity_exceeded"],
];
export function captureRefusal(error: unknown): CaptureRefusal | undefined {
  const e = error as { code?: unknown; message?: unknown } | null;
  if (typeof e?.code !== "string" || typeof e.message !== "string") return undefined;
  for (const [code, message, refusal] of CAPTURE_REFUSAL_MESSAGES) if (e.code === code && e.message.startsWith(message)) return refusal;
  return undefined;
}
export interface ReplayProducerResult { published: boolean; reason?: "absent" | "source_blocked"; publication?: ArtifactPublication; rows?: number; objects?: number; bytes?: number }
const fail = (code: string): never => { throw new ArtifactBuildError(code); };
const scopeOf = (i: CanonicalIdentity): SessionScope => ({ orgId: i.orgId, workspaceId: i.workspaceId, harness: i.source, sessionId: i.sessionId });
async function capture(db: SqlExecutor, scope: SessionScope): Promise<ReplayCapture> {
  const { rows } = await db.query<{captured: ReplayCapture}>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) AS captured", [scope.orgId,scope.workspaceId,scope.harness,scope.sessionId]);
  const c = rows[0]?.captured;
  if (!c || typeof c.exists !== "boolean" || typeof c.blocked !== "boolean" || c.identity?.orgId !== scope.orgId || c.identity?.workspaceId !== scope.workspaceId || c.identity?.source !== scope.harness || typeof c.identity.sessionId !== "string" || !c.identity.sessionId || !/^[0-9a-f]{64}$/u.test(c.source?.fingerprint ?? "") || !Array.isArray(c.snapshot?.pointers)) fail("artifact_capture_invalid");
  if (c.exists && (!/^[1-9][0-9]*$/u.test(c.source.epoch) || c.snapshot.epoch !== c.source.epoch || !c.snapshot.pointers.length || c.snapshot.pointers.length > ARTIFACT_MAX_SOURCE_POINTERS)) fail("artifact_capture_bound");
  if (!c.exists && c.snapshot.pointers.length) fail("artifact_capture_invalid");
  return c;
}
/** Artifact generations are explicitly v1, separate from unchanged raw-provider cursors. */
export function canonicalArtifactGeneration(sourceGeneration: string, source: ArtifactSource): string {
  return artifactDigest(JSON.stringify(["canonical-read-artifact-v1",sourceGeneration,source.epoch,source.fingerprint]));
}
export async function produceCanonicalReplay(scope: SessionScope, deps: ReplayProducerDeps): Promise<ReplayProducerResult> {
  const first = await capture(deps.db, scope);
  if (!first.exists) return { published: false, reason: "absent" };
  if (first.blocked) return { published: false, reason: "source_blocked" };
  const identity = first.identity;
  // Each re-read includes the monotonic source epoch, not merely pointer values.
  const current = async () => {
    const c = await capture(deps.db, scopeOf(identity));
    if (!c.exists || c.blocked || c.source.epoch !== first.source.epoch || c.source.fingerprint !== first.source.fingerprint || JSON.stringify(c.identity) !== JSON.stringify(identity)) fail("artifact_source_changed");
    return c.snapshot.pointers;
  };
  const snapshot = await canonicalProducerSnapshot({
    store: deps.store, provider: deps.provider, bucket: deps.bucket, prefix: deps.prefix,
    pointers: current,
    decompress: deps.decompress ?? ((bytes,maxBytes) => new Uint8Array(zlib.zstdDecompressSync(bytes,{maxOutputLength:maxBytes}))),
  }, identity, deps.profile);
  if (!snapshot.exists) fail("artifact_source_changed");
  const sourceGeneration = canonicalSourceGeneration(first.snapshot.pointers,identity);
  if (snapshot.generation !== sourceGeneration) fail("artifact_source_changed");
  const generation = canonicalArtifactGeneration(sourceGeneration,first.source);
  const anchor = [...first.snapshot.pointers].sort((a,b) => Buffer.compare(Buffer.from(a.manifest_key),Buffer.from(b.manifest_key)))[0]!;
  if (!anchor.manifest_key.endsWith("/manifest.json")) fail("artifact_anchor_invalid");
  const rawSha = anchor.content_sha256.replace(/^\\x/u, "").toLowerCase();
  if (!/^[0-9a-f]{64}$/u.test(rawSha)) fail("artifact_anchor_invalid");
  const prefix = `${anchor.manifest_key.slice(0,-"manifest.json".length)}normalized/read-projection-v1/${rawSha}/${generation}/`;
  const timing = canonicalTimingProjection(identity,snapshot.rows,snapshot.legacyCoverage !== null);
  const rows = timing ? [...snapshot.rows,timing] : snapshot.rows;
  const made = await materializeCanonicalArtifacts(deps.store,{ identity,generation,source:first.source,prefix,rows,profile:deps.profile,legacyCoverage:snapshot.legacyCoverage });
  await current(); // Objects are verified, but stale work still must publish nothing.
  const params = [identity.orgId,identity.workspaceId,identity.source,identity.sessionId,first.source.epoch,first.source.fingerprint,JSON.stringify(first.snapshot),generation,sourceGeneration,prefix,made.index.key,made.index.sha256,made.index.bytes];
  const published = deps.publish ? await deps.publish(params) : (await deps.db.query<{published:boolean}>("SELECT sessions.publish_session_replay_pages($1,$2,$3,$4,$5::bigint,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::integer) AS published",params)).rows[0]?.published;
  if (published !== true) fail("artifact_publish_cas_failed");
  return { published:true, publication:{version:1,identity,generation,source:first.source,prefix,index:made.index}, rows:snapshot.rows.filter(row=>row.table!=="session_directory_event_values").length, objects:made.objects, bytes:made.bytes };
}
