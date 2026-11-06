/**
 * Plants a session through the product's authenticated ingest path, exactly as
 * the AlleyOop courier does: POST /v1/pipeline/chunks (Technical Foul scrubs
 * before storage and seals the chunk), then POST /v1/pipeline/manifests, which
 * writes the v1 manifest to S3 through the relayhistory-cloud storage adapter
 * and enqueues object-landed. Revision 2 appends a chunk and carries the
 * previous object version, as a re-ingest from the courier would.
 *
 * Tokens stay in memory. Nothing here logs a request or response body.
 */
import { createHash } from "node:crypto";
import { computeContentSha256, manifestKey, validateManifest, type ChunkEntry, type ManifestV1, type ObjectVersion } from "../../shared/vendor.ts";
import { EXT } from "../../shared/enrich.ts";
import { hexTraceId, spanId } from "../../shared/ids.ts";
import type { PlantedSession } from "./corpus.ts";

export const PROOF_PLUGIN_VERSION = "96bulls-aws-proof/0.1.0";

export type Fetch = (input: string, init?: RequestInit) => Promise<Response>;

export interface IngestTarget {
  /** e.g. http://127.0.0.1:18080 through `kubectl port-forward`. */
  ingestUrl: string;
  fetch?: Fetch;
}

export interface LandedRevision {
  revision: number;
  manifestKey: string;
  version: ObjectVersion;
  /** Client clock when the 202 arrived; server-side `object-landed` is preferred for latency. */
  landedAtMs: number;
}

export class IngestError extends Error {
  constructor(readonly code: string, readonly status: number) {
    super(`ingest refused: ${code} (${status})`);
  }
}

const date = (iso: string) => iso.slice(0, 10);

export function locationOf(p: PlantedSession) {
  return { ...(p.keyPrefix ? { prefix: p.keyPrefix } : {}), workspaceId: p.tenant.workspaceId, machineId: p.machineId, harness: p.harness, sessionId: p.sessionId, startDate: date(p.startedAt) };
}

async function errorCode(res: Response): Promise<string> {
  const body = (await res.json().catch(() => ({}))) as { error?: { code?: unknown } };
  return typeof body.error?.code === "string" ? body.error.code.slice(0, 64) : `http_${res.status}`;
}

/** Build the v1 manifest for the chunks sealed so far (mirrors src/alleyoop/courier.ts). */
export async function buildManifest(p: PlantedSession, chunks: ChunkEntry[], revision: number, scrub: { ruleset: string; version: number } | undefined, nowIso: string): Promise<ManifestV1> {
  const last = chunks.at(-1);
  if (!last) throw new Error("a manifest needs at least one sealed chunk");
  const closed = revision >= 2;
  const manifest = {
    schema_v: 1,
    layout: "v1",
    trace_id: p.traceId,
    revision,
    created_at: p.startedAt,
    updated_at: nowIso,
    session: {
      workspace_id: p.tenant.workspaceId,
      machine_id: p.machineId,
      harness: p.harness,
      session_id: p.sessionId,
      started_at: p.startedAt,
      ...(closed ? { ended_at: nowIso } : {}),
      cwd_sha256: createHash("sha256").update("/workspace/proof").digest("hex"),
    },
    writer: { delivery_instance_id: p.machineId, plugin_version: PROOF_PLUGIN_VERSION },
    raw: {
      owner: p.machineId,
      state: closed ? "closed" : "open",
      updated_at: nowIso,
      format: "jsonl",
      compression: "zstd",
      chunk_count: chunks.length,
      raw_bytes: chunks.reduce((n, c) => n + c.bytes, 0),
      stored_bytes: chunks.reduce((n, c) => n + c.stored_bytes, 0),
      content_sha256: await computeContentSha256(chunks),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: p.startedAt, reason: "initial", first_seq: 0, opened_by: p.machineId }],
      next_offset: last.start_offset + last.bytes,
      chunks,
      ...(scrub ? { scrub } : {}),
    },
    extensions: { [EXT]: { session_uuid: p.sessionUuid, session_uuid_source: "minted", tenant_id: p.tenant.orgId } },
  } as unknown as ManifestV1;
  const issues = await validateManifest(manifest, manifestKey(locationOf(p)));
  if (issues.length) throw new Error(`proof manifest invalid at ${issues[0]!.path}`);
  return manifest;
}

/** Per-session upload state, so revision 2 can be planted later (stale-summary case). */
export interface UploadState {
  chunks: ChunkEntry[];
  offset: number;
  scrub?: { ruleset: string; version: number };
  previous?: ObjectVersion;
  landed: LandedRevision[];
}

export const newUploadState = (): UploadState => ({ chunks: [], offset: 0, landed: [] });

/**
 * Plant the next revision of one session: seal its new records as one chunk,
 * then land the manifest. `token` is the tenant's RelayAuth access token
 * (rth:sync), minted for this run only.
 */
export async function plantNextRevision(t: IngestTarget, p: PlantedSession, token: string, st: UploadState): Promise<LandedRevision> {
  const f = t.fetch ?? fetch;
  const revision = st.landed.length + 1;
  if (revision > p.revisions.length) throw new Error(`session has only ${p.revisions.length} revisions`);
  const loc = locationOf(p);
  const key = manifestKey(loc);
  const traceparent = `00-${hexTraceId(p.traceId)}-${spanId()}-01`;
  const headers = { authorization: `Bearer ${token}`, "x-relay-trace-id": p.traceId, "x-machine-id": p.machineId, "x-session-id": p.sessionId, traceparent };
  const lines = p.revisions[revision - 1]!.slice(revision === 1 ? 0 : p.revisions[revision - 2]!.length);
  const bytes = new TextEncoder().encode(lines.join("\n") + "\n");
  const q = new URLSearchParams({ format: "jsonl", seq: String(st.chunks.length), epoch: "0", offset: String(st.offset), date: loc.startDate, machine: loc.machineId, harness: loc.harness, session: loc.sessionId });
  const res = await f(`${t.ingestUrl}/v1/pipeline/chunks?${q}`, { method: "POST", headers: { ...headers, "content-type": "application/octet-stream" }, body: bytes });
  if (!res.ok) throw new IngestError(await errorCode(res), res.status);
  const sealed = (await res.json()) as { entry: ChunkEntry; scrub: { ruleset: string; version: number } };
  const chunks = [...st.chunks, sealed.entry];
  const scrub = st.scrub && st.scrub.version < sealed.scrub.version ? st.scrub : sealed.scrub;
  const manifest = await buildManifest(p, chunks, revision, scrub, new Date().toISOString());
  const m = await f(`${t.ingestUrl}/v1/pipeline/manifests`, {
    method: "POST",
    headers: { ...headers, "content-type": "application/json" },
    body: JSON.stringify({ key, manifest, ...(st.previous ? { previousVersion: st.previous } : {}) }),
  });
  if (m.status !== 202) throw new IngestError(await errorCode(m), m.status);
  const body = (await m.json()) as { version?: ObjectVersion };
  if (!body.version?.eTag && !body.version?.version) throw new IngestError("manifest_version_missing", 202);
  // Commit state only after the manifest is acknowledged.
  st.chunks = chunks;
  st.scrub = scrub;
  st.offset += bytes.byteLength;
  st.previous = body.version;
  const landed = { revision, manifestKey: key, version: body.version, landedAtMs: Date.now() };
  st.landed.push(landed);
  return landed;
}

/**
 * Mint a short-lived per-machine token from the in-cluster RelayAuth (reached by
 * port-forward). The bootstrap key comes from the environment, piped from
 * 1Password by the operator; it is never written to disk or the report.
 *
 * RelayAuth #113 signs the canonical `machineId` as `meta.machineId`. A token
 * therefore belongs to exactly one planted machine and cannot be reused for a
 * different courier writer.
 */
const MACHINE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const RELAYAUTH_ASSERTION_TTL_SECONDS = 60;

export async function mintTenantToken(o: { relayauthUrl: string; bootstrapKey: string; orgId: string; workspaceId: string; machineId: string; sponsorId: string; historyOrigin: string; fetch?: Fetch }): Promise<string> {
  if (!MACHINE_ID.test(o.machineId) || o.machineId.startsWith("relay_")) throw new IngestError("machine_id_invalid", 0);
  const f = o.fetch ?? fetch;
  const res = await f(`${o.relayauthUrl}/v1/tokens/relayhistory-assertion`, {
    method: "POST",
    headers: { "x-api-key": o.bootstrapKey, "content-type": "application/json" },
    body: JSON.stringify({ orgId: o.orgId, workspaceId: o.workspaceId, machineId: o.machineId, sponsorId: o.sponsorId, scopes: ["rth:sync", "rth:read"], expiresIn: RELAYAUTH_ASSERTION_TTL_SECONDS, historyOrigin: o.historyOrigin }),
  });
  if (res.status !== 201) throw new IngestError(await errorCode(res), res.status);
  const { accessToken } = (await res.json()) as { accessToken?: string };
  if (!accessToken) throw new IngestError("token_missing", res.status);
  return accessToken;
}
