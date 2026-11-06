import type {FrozenWitnessInput,LedgerTenant} from "./ledger.ts";
import {decodeCustodyEnvelope} from "./custody-wire.ts";
import {validatedCustodyAck} from "./custody-ack.ts";
import {CURRENT_CUSTODY_CONTENT_TYPE,CURRENT_CUSTODY_ACK_HEADER,PRIVATE_CUSTODY_BYTES,type CurrentCustodyPacket} from "./custody-protocol.ts";
import {orderedControl,orderedControlResult,type OrderedControl} from "./control-protocol.ts";
/**
 * The 96Bulls session ingest: relayhistory's delivery write contract
 * (relayhistory-cloud packages/relayhistory/src/routes/delivery.ts), served
 * from AWS with S3 as the record and Supabase as the ledger. No Neon.
 *
 *   POST /v1/delivery/batches   rth:sync   one batch -> receipt (same shapes, same codes)
 *   GET  /v1/delivery/limits    rth:sync   DELIVERY_LIMITS + contentEncodings
 *   POST /v1/turns/batches      rth:sync   relay session turns as records (turns.ts), same receipt
 *   GET  /healthz                          liveness only
 *
 * Every other route is `501 unsupported_by_backend`. Selection, reset and
 * reads stay on relayhistory until their own cutover.
 *
 * Who calls it: only relayhistory's Worker, over a Cloudflare service binding.
 * This Worker has no route, no custom domain and no workers.dev URL (the stack
 * enforces it), so it is unreachable from the internet. relayhistory has
 * already authenticated the client's own bearer (`requireAuth`, `rth:sync`),
 * and passes the verified identity in `x-relay-ingest-identity`. Tenancy comes
 * only from that header, never from the request body.
 *
 * Wire compatibility, line by line with relayhistory: the request body is read
 * bounded by MAX_DELIVERY_BYTES on the wire and decoded (identity, gzip, zstd;
 * 413 / 415 / 400 as `boundedJson`), parsed by the vendored
 * `parseDeliveryRequest`, and answered with the receipt plus `limits`
 * (`contentEncodings` included). Errors are `{ error: { code, message,
 * conflict?, conflicts?, conflictCount? }, correlationId }`, and
 * `X-Correlation-Id` is echoed as relayhistory does.
 */
import { zstdDecompressSync } from "node:zlib";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { Hono, type Context } from "hono";
import { log } from "../shared/log.ts";
import { AuthError, type VerifiedIdentity } from "../session-read-api/app.ts";
import { deliverBatch, deliverPrepared, IngestUnavailable, type DeliverDeps } from "./deliver.ts";
import { DELIVERY_LIMITS, DeliveryBusyError, DeliveryError, MAX_DELIVERY_BYTES, type HistoryExportBatch } from "./vendor/delivery/delivery-contracts.ts";
import { parseDeliveryRequest, requireDeliveryAccount, prepareDelivery } from "./vendor/delivery/prepare.ts";
import { decodeRecoveryEnvelope, RECOVERY_CONTENT_TYPE } from "./recovery-protocol.ts";
import { parseTurnBatch } from "./turns.ts";
import { heartbeatProgress, ProgressError, readConnection, recordHeartbeat, type Run as OnboardingRun } from "../history-db/onboarding.ts";

export type IngestIdentity=VerifiedIdentity & {control?:OrderedControl};

export const SYNC_SCOPE = "rth:sync";
export type ContentEncoding = "zstd" | "gzip";

/** relayhistory-cloud's read scope; only the onboarding status read accepts it here. */
export const READ_SCOPE = "rth:read";
export const IDENTITY_HEADER = "x-relay-ingest-identity";
export const SIGNATURE_HEADER = "x-relay-ingest-signature";
/** How old (or how far ahead) a signature may be. */
export const SIGNATURE_WINDOW_SECONDS = 60;

/**
 * What relayhistory signs (HMAC-SHA256, the shared Secrets Store key
 * `96bulls-session-ingest-signing-key`): the version, the time, the method, the
 * path, the identity header exactly as sent, and the SHA-256 of the body. A
 * sibling Worker that binds this one without the key cannot assert a tenant;
 * one that replays a captured request gets at most the same idempotent write
 * within the window.
 */
export function signingInput(t: number, method: string, path: string, identity: string, body: Uint8Array): string {
  return `v1\n${t}\n${method.toUpperCase()}\n${path}\n${identity}\n${createHash("sha256").update(body).digest("hex")}`;
}

export function signRequest(key: string, t: number, method: string, path: string, identity: string, body: Uint8Array): string {
  return `t=${t},v1=${createHmac("sha256", key).update(signingInput(t, method, path, identity, body)).digest("hex")}`;
}

/** The identity relayhistory verified and signed (`signRequest`). Refuses an unsigned, stale or altered request. */
export function signedBindingIdentity(key: string | undefined, nowSeconds: () => number = () => Math.floor(Date.now() / 1000)) {
  return (request: Request, body: Uint8Array): IngestIdentity => {
    if (!key) throw new AuthError(401, "not_configured", "the session ingest has no signing key");
    const identity = request.headers.get(IDENTITY_HEADER) ?? "";
    const m = /^t=(\d{1,12}),v1=([0-9a-f]{64})$/.exec(request.headers.get(SIGNATURE_HEADER) ?? "");
    if (!identity || !m) throw new AuthError(401, "missing_authorization", "missing Authorization header");
    const t = Number(m[1]);
    if (Math.abs(nowSeconds() - t) > SIGNATURE_WINDOW_SECONDS) throw new AuthError(401, "invalid_token", "invalid or expired token");
    const expected = Buffer.from(signRequest(key, t, request.method, new URL(request.url).pathname, identity, body).slice(-64), "hex");
    const given = Buffer.from(m[2]!, "hex");
    if (expected.length !== given.length || !timingSafeEqual(expected, given)) throw new AuthError(401, "invalid_token", "invalid or expired token");
    return bindingIdentity(request);
  };
}

/** The identity relayhistory verified, as it sends it: base64url JSON { userId, orgId, workspaceId, scopes }. */
export function bindingIdentity(request: Request): IngestIdentity {
  const raw = request.headers.get(IDENTITY_HEADER);
  if (!raw) throw new AuthError(401, "missing_authorization", "missing Authorization header");
  let v: Partial<IngestIdentity> | null = null;
  try {
    v = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")) as Partial<IngestIdentity>;
  } catch {
    // fall through
  }
  const id = (x: unknown) => typeof x === "string" && x.length > 0 && x.length <= 512;
  if (!v || !id(v.userId) || !id(v.orgId) || typeof v.workspaceId !== "string" || !Array.isArray(v.scopes) || !v.scopes.every((s) => typeof s === "string"))
    throw new AuthError(401, "invalid_token", "invalid or expired token");
  let control:OrderedControl|undefined;
  if("control" in v){try{control=orderedControl(v.control)}catch{throw new AuthError(401,"invalid_token","invalid or expired token")}}
  return { userId: v.userId!, orgId: v.orgId!, workspaceId: v.workspaceId, scopes: v.scopes,...(control?{control}:{}) };
}

export function encodeIdentity(identity: IngestIdentity): string {
  return Buffer.from(JSON.stringify(identity), "utf8").toString("base64url");
}

export interface IngestAppDeps {
  /** Default: `signedBindingIdentity(signingKey)`. */
  identify?: (request: Request, body: Uint8Array) => IngestIdentity | Promise<IngestIdentity>;
  /** The shared HMAC key (Secrets Store). Unset with no `identify`: every request is 401 not_configured. */
  signingKey?: string;
  deliver: DeliverDeps;
  /** P5b G2: the onboarding heartbeat on history.probe_connections. Unset: those routes answer 503. */
  onboarding?: { run: OnboardingRun };
  log?: (fields: Record<string, unknown>) => void;
  /**
   * P1 custody source (INGEST_CUSTODY_SOURCE). "relayhistory" (default): only the signed Neon witness
   * relayhistory sends. "shadow": additionally build the Supabase frozen-source witness and log
   * match/mismatch, never changing the answer. "frozen-supabase": when relayhistory sends no witness,
   * build it from the sealed Supabase copy (refuses, never guesses, when unsealed).
   */
  custodySource?: "relayhistory" | "shadow" | "frozen-supabase";
}

type Env = { Variables: { correlationId: string; identity: IngestIdentity; body: Uint8Array; custody:CurrentCustodyPacket|undefined } };

/** Node 24 always has zstd; published most preferred first, as relayhistory. */
export function contentEncodings(): ContentEncoding[] {
  return typeof zstdDecompressSync === "function" ? ["zstd", "gzip"] : ["gzip"];
}

const tooLarge = () => new DeliveryError("delivery_too_large", 413, `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes`);
const unsupported = () => new DeliveryError("unsupported_encoding", 415, "Delivery request Content-Encoding is unsupported");

export interface BodyRead {
  bytes: number;
  wireBytes: number;
  encoding: ContentEncoding | "identity";
}

async function readBounded(stream: ReadableStream<Uint8Array>, limit: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > limit) {
        await reader.cancel();
        throw tooLarge();
      }
      chunks.push(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(size);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

/** relayhistory's `boundedJson`, in Node: the cap bounds wire AND decoded bytes; Content-Length is never trusted. */
export async function boundedJson(request: Request, read: BodyRead): Promise<unknown> {
  const header = request.headers.get("content-encoding")?.trim().toLowerCase() ?? "";
  const encoding: BodyRead["encoding"] = header === "" || header === "identity" ? "identity" : (contentEncodings() as string[]).includes(header) ? (header as ContentEncoding) : (() => { throw unsupported(); })();
  read.encoding = encoding;
  if (!request.body) throw new DeliveryError("invalid_delivery", 400, "Delivery request body is required");
  const wire = await readBounded(request.body, MAX_DELIVERY_BYTES);
  read.wireBytes = wire.byteLength;
  let decoded = wire;
  if (encoding === "gzip") {
    try {
      decoded = await readBounded(new Blob([wire]).stream().pipeThrough(new DecompressionStream("gzip")), MAX_DELIVERY_BYTES);
    } catch (error) {
      if (error instanceof DeliveryError) throw error;
      throw new DeliveryError("invalid_delivery", 400, "Delivery request body is not valid gzip");
    }
  } else if (encoding === "zstd") {
    try {
      decoded = new Uint8Array(zstdDecompressSync(wire, { maxOutputLength: MAX_DELIVERY_BYTES }));
    } catch (error) {
      if (error instanceof RangeError) throw tooLarge();
      throw new DeliveryError("invalid_delivery", 400, "Delivery request body is not valid zstd");
    }
  }
  read.bytes = decoded.byteLength;
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(decoded));
  } catch {
    throw new DeliveryError("invalid_delivery", 400, "Delivery request must be valid UTF-8 JSON");
  }
}

function errorBody(c: Context<Env>, code: string, message: string, extra: Record<string, unknown> = {}) {
  return { error: { code, message, ...extra }, correlationId: c.get("correlationId") ?? "" };
}

function failure(c: Context<Env>, error: unknown) {
  const safe = error instanceof DeliveryError ? error : new DeliveryError("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
  // A busy refusal is retryable and wrote nothing durable; tell the client when (relayhistory's header).
  if (safe instanceof DeliveryBusyError) c.header("Retry-After", String(safe.retryAfterSeconds));
  return c.json(
    errorBody(c, safe.code, safe.message, {
      ...(safe.conflict ? { conflict: safe.conflict } : {}),
      ...(safe.conflicts ? { conflicts: safe.conflicts } : {}),
      ...(safe.conflictCount ? { conflictCount: safe.conflictCount } : {}),
    }),
    safe.status,
  );
}

/** The buffered body as a Request again, for `boundedJson` (decoding per Content-Encoding). */
function bufferedRequest(c: Context<Env>): Request {
  return new Request(c.req.raw.url, { method: c.req.raw.method, headers: c.req.raw.headers, body: c.get("body") });
}

async function jsonOf(c: Context<Env>,read:BodyRead={bytes:0,wireBytes:0,encoding:"identity"}): Promise<unknown> {
  if(c.req.header("content-type")?.split(";")[0]?.trim()!==CURRENT_CUSTODY_CONTENT_TYPE)return boundedJson(bufferedRequest(c),read);
  const decoded=decodeCustodyEnvelope(c.get("body"));c.set("custody",decoded.packet);
  const headers=new Headers(c.req.raw.headers);headers.set("content-encoding",decoded.encoding);headers.delete("content-length");
  return boundedJson(new Request(c.req.raw.url,{method:c.req.raw.method,headers,body:decoded.original}),read);
}
function custodyHeader(c:Context<Env>,ack:unknown){
 const packet=c.get("custody");if(!packet)return;
 const value=JSON.stringify(validatedCustodyAck(ack,packet));
 if(Buffer.byteLength(value)>512)throw new IngestUnavailable("fence","custody_ack_invalid");
 c.header(CURRENT_CUSTODY_ACK_HEADER,value);
}

const invalid = (message: string): never => {
  throw new DeliveryError("invalid_delivery", 400, message);
};
const isObject = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
function exactKeys(v: Record<string, unknown>, keys: string[]) {
  const got = Object.keys(v).sort();
  if (got.length !== keys.length || got.some((k, i) => k !== [...keys].sort()[i])) invalid("Unexpected or missing field");
}
/** relayhistory's `identifier`: 1..512 UTF-8 bytes, no control characters, well-formed. */
function identifier(v: unknown): v is string {
  return typeof v === "string" && v.length > 0 && new TextEncoder().encode(v).length <= 512 && !/[\x00-\x1f\x7f]/.test(v) && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(v);
}

export function parseSelection(body: unknown): { account_id: string; origin_id: string; sessions: { source: string; session_id: string }[]; enabled: boolean } {
  if (!isObject(body)) invalid("Expected upload selection");
  const b = body as Record<string, unknown>;
  exactKeys(b, ["account_id", "origin_id", "sessions", "enabled"]);
  if (!identifier(b.origin_id) || typeof b.enabled !== "boolean" || !Array.isArray(b.sessions) || b.sessions.length < 1 || b.sessions.length > 2048)
    invalid("Invalid upload selection");
  for (const x of b.sessions as unknown[]) {
    if (!isObject(x)) invalid("Invalid session identity");
    exactKeys(x as Record<string, unknown>, ["source", "session_id"]);
    if (!identifier((x as Record<string, unknown>).source) || !identifier((x as Record<string, unknown>).session_id)) invalid("Invalid session identity");
  }
  return b as never;
}

export function parseReset(body: unknown): { account_id: string; request_id: string } {
  if (!isObject(body)) invalid("Expected session reset");
  const b = body as Record<string, unknown>;
  exactKeys(b, ["account_id", "request_id", "confirmation"]);
  if (b.confirmation !== "DELETE ALL SESSIONS" || !identifier(b.request_id)) invalid("Type DELETE ALL SESSIONS to confirm the reset");
  return b as never;
}

export function createIngestApp(deps: IngestAppDeps) {
  const app = new Hono<Env>();
  const emit = deps.log ?? ((fields) => log.info("session ingest", fields));
  const custodyMode = deps.custodySource ?? "relayhistory";
  const frozenWitness = async (input: FrozenWitnessInput): Promise<CurrentCustodyPacket> => {
    if (!deps.deliver.ledger.frozenWitness) throw new IngestUnavailable("fence", "custody_source_unavailable");
    return deps.deliver.ledger.frozenWitness(input);
  };
  const batchWitnessInput = async (tenant: LedgerTenant, batch: Parameters<typeof prepareDelivery>[1]): Promise<FrozenWitnessInput> => {
    const p = await prepareDelivery(tenant, batch);
    return { kind: "batch", tenant, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: (batch as { records: unknown[] }).records.length,
      candidates: p.records.map((r, i) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id ?? null })) };
  };
  const controlWitnessInput = (kind: "selection" | "reset", tenant: LedgerTenant, originId: string | null): FrozenWitnessInput =>
    ({ kind, tenant, originId, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
  /** Shadow compare: never throws, never changes the response. Logs no identifiers or content. */
  const shadow = async (route: string, received: CurrentCustodyPacket, input: () => Promise<FrozenWitnessInput> | FrozenWitnessInput) => {
    if (custodyMode !== "shadow") return;
    try {
      const frozen = await frozenWitness(await input());
      const differing = frozen.sourceDigest === received.sourceDigest ? [] : Object.keys({ ...received.witness, ...frozen.witness }).filter((k) =>
        JSON.stringify((received.witness as unknown as Record<string, unknown>)[k]) !== JSON.stringify((frozen.witness as unknown as Record<string, unknown>)[k])).sort();
      emit({ route: "custody.shadow", source_route: route, outcome: differing.length ? "mismatch" : "match", differing_fields: differing.join(",") || undefined });
    } catch (error) {
      emit({ route: "custody.shadow", source_route: route, outcome: "frozen_unavailable", error_code: (error as { code?: string; cause_code?: string })?.cause_code ?? (error as { code?: string })?.code ?? "error" });
    }
  };

  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    c.set("correlationId", c.req.header("X-Correlation-Id")?.trim().slice(0, 128) || requestId);
    await next();
    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", c.get("correlationId"));
  });

  app.get("/healthz", (c) => c.json({ ok: true }));

  // Identity for /v1, relayhistory's codes: 401 missing_authorization / invalid_token, 403 forbidden.
  const identify = deps.identify ?? signedBindingIdentity(deps.signingKey);
  app.use("/v1/*", async (c, next) => {
    // The body is read once, bounded by the wire cap, so the signature covers exactly what is parsed.
    let body: Uint8Array;
    try {
      const contentType=c.req.header("content-type")?.split(";")[0]?.trim();
      const privateWire=contentType===CURRENT_CUSTODY_CONTENT_TYPE||contentType===RECOVERY_CONTENT_TYPE;
      if(privateWire&&(deps.deliver.acceptanceMode!=="buffered"||!["","identity"].includes(c.req.header("content-encoding")?.trim().toLowerCase()??"")))throw new IngestUnavailable("fence","custody_wire_unavailable");
      body = c.req.raw.body ? await readBounded(c.req.raw.body, privateWire?PRIVATE_CUSTODY_BYTES:MAX_DELIVERY_BYTES) : new Uint8Array();
    } catch (error) {
      return failure(c, error);
    }
    c.set("body", body);
    try {
      c.set("identity", await identify(c.req.raw, body));
    } catch (error) {
      if (error instanceof AuthError) return c.json(errorBody(c, error.code, error.message), error.status);
      emit({ route: "auth", outcome: "error", error_code: "identity_failed" });
      return c.json(errorBody(c, "invalid_token", "invalid or expired token"), 401);
    }
    // The onboarding status read is the only rth:read route here; everything else needs rth:sync.
    const scope = c.req.method === "GET" && new URL(c.req.url).pathname === "/v1/onboarding/connection" ? READ_SCOPE : SYNC_SCOPE;
    if (!c.get("identity").scopes.includes(scope)) return c.json(errorBody(c, "forbidden", `missing required scope: ${scope}`), 403);
    await next();
  });

  // P5b G2: relayhistory-cloud's onboarding heartbeat (same validation and response), stored
  // in history.probe_connections. relayhistory forwards here when ONBOARDING_BACKEND=ingest.
  app.post("/v1/onboarding/heartbeat", async (c) => {
    const identity = c.get("identity");
    if (!deps.onboarding) return c.json(errorBody(c, "not_configured", "onboarding is not configured"), 503);
    if (!identity.workspaceId) return c.json({ error: "Workspace required" }, 403);
    let parsed;
    try {
      parsed = heartbeatProgress(c.get("body"));
    } catch (error) {
      if (error instanceof ProgressError || error instanceof SyntaxError) return c.json({ error: "Invalid progress" }, 400);
      throw error;
    }
    if ("tooLarge" in parsed) return c.json({ error: "Progress too large" }, 413);
    try {
      const lastSeenAt = await recordHeartbeat(deps.onboarding.run, identity, parsed.progress);
      emit({ route: "onboarding.heartbeat", outcome: "ok", org_id: identity.orgId });
      return c.json({ connected: true, lastSeenAt });
    } catch (error) {
      emit({ route: "onboarding.heartbeat", outcome: "error", error_code: (error as { code?: string }).code ?? "onboarding_failed" });
      return c.json(errorBody(c, "onboarding_unavailable", "onboarding is temporarily unavailable"), 503);
    }
  });

  app.get("/v1/onboarding/connection", async (c) => {
    const identity = c.get("identity");
    if (!deps.onboarding) return c.json(errorBody(c, "not_configured", "onboarding is not configured"), 503);
    if (!identity.workspaceId) return c.json({ error: "Workspace required" }, 403);
    try {
      return c.json(await readConnection(deps.onboarding.run, identity));
    } catch (error) {
      emit({ route: "onboarding.connection", outcome: "error", error_code: (error as { code?: string }).code ?? "onboarding_failed" });
      return c.json(errorBody(c, "onboarding_unavailable", "onboarding is temporarily unavailable"), 503);
    }
  });

  app.get("/v1/delivery/limits", (c) => c.json({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings() }));

  // One acceptance path for the public delivery shape and the private turns shape: the
  // same custody (relayhistory witness, shadow, or frozen-supabase), claim, buffer and receipt.
  const acceptBatch = async (c: Context<Env>, route: string, parse: (value: unknown, identity: IngestIdentity) => HistoryExportBatch) => {
    const started = Date.now();
    const identity = c.get("identity");
    const read: BodyRead = { bytes: 0, wireBytes: 0, encoding: "identity" };
    const fields: Record<string, unknown> = { route, org_id: identity.orgId, workspace_id: identity.workspaceId, correlation_id: c.get("correlationId") };
    try {
      const batch = parse(await jsonOf(c, read), identity);
      const tenant = { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      if (!c.get("custody") && custodyMode === "frozen-supabase") c.set("custody", await frozenWitness(await batchWitnessInput(tenant, batch)));
      else if (c.get("custody")) await shadow(route, c.get("custody")!, () => batchWitnessInput(tenant, batch));
      const custody=c.get("custody");
      if(custody&&custody.witness.originalRecordCount!==batch.records.length)throw new IngestUnavailable("fence","custody_record_count");
      const result = await deliverBatch({...deps.deliver,...(custody?{custody}:{})}, { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, batch);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: read.bytes });
      custodyHeader(c,result.custodyAck);
      return c.json({ ...result.receipt, limits: { ...result.receipt.limits, contentEncodings: contentEncodings() } });
    } catch (error) {
      const code = error instanceof DeliveryError ? error.code : "delivery_unavailable";
      emit({
        ...fields,
        outcome: code,
        ...(error instanceof IngestUnavailable ? { stage: error.stage, error_code: error.cause_code } : {}),
        ...(error instanceof DeliveryError && error.conflictCount ? { count: error.conflictCount } : {}),
        duration_ms: Date.now() - started,
        bytes_raw: read.bytes,
      });
      return failure(c, error);
    }
  };

  app.post("/v1/delivery/batches", (c) => acceptBatch(c, "delivery.batches", (value) => parseDeliveryRequest(value)));

  // Relay session journal turns (relayhistory TURNS_WRITE_PATH=dual|forward): turns.ts.
  // Same signed binding, rth:sync and custody as a batch; only the turn record shape.
  app.post("/v1/turns/batches", (c) => acceptBatch(c, "turns.batches", parseTurnBatch));

  // Private archive replay. Unlike the public batch route, this accepts the
  // exact prepared values retained by the old Neon replica archive. It is
  // available only while the receiver is in buffered mode and requires both
  // the normal exact-body HMAC and fresh current-source custody.
  app.post("/v1/delivery/recovery", async (c) => {
    const started = Date.now();
    const identity = c.get("identity");
    const fields: Record<string, unknown> = { route: "delivery.recovery", org_id: identity.orgId, workspace_id: identity.workspaceId, correlation_id: c.get("correlationId") };
    try {
      if (deps.deliver.acceptanceMode !== "buffered" || c.req.header("content-type")?.split(";")[0]?.trim() !== RECOVERY_CONTENT_TYPE)
        throw new IngestUnavailable("recovery", "recovery_wire_unavailable");
      const tenant = { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      const { prepared, custody } = await decodeRecoveryEnvelope(c.get("body"), tenant);
      c.set("custody", custody);
      const result = await deliverPrepared({ ...deps.deliver, custody }, tenant, prepared);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: c.get("body").byteLength });
      custodyHeader(c, result.custodyAck);
      return c.json({ ...result.receipt, limits: { ...result.receipt.limits, contentEncodings: contentEncodings() } });
    } catch (error) {
      const code = error instanceof DeliveryError ? error.code : "delivery_unavailable";
      emit({ ...fields, outcome: code, ...(error instanceof IngestUnavailable ? { stage: error.stage, error_code: error.cause_code } : {}), duration_ms: Date.now() - started, bytes_raw: c.get("body").byteLength });
      return failure(c, error);
    }
  });

  // Deletion fences (relayhistory's selection and reset, written here right after Neon's own, by
  // relayhistory-cloud #265). Same bodies and answers as relayhistory (lib/delivery.ts
  // setSessionUploads / resetUploadedSessions); tenancy only from the signed identity.
  app.post("/v1/delivery/sessions/selection", async (c) => {
    const identity = c.get("identity");
    try {
      const input = parseSelection(await jsonOf(c));
      await requireDeliveryAccount(identity, input.account_id);
      const selection={
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId },
        originId: input.origin_id,
        sessions: input.sessions.map((x):[string,string] => [x.source, x.session_id]),
        enabled: input.enabled,
      };
      let controlAck;
      {const t={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
       if(identity.control&&!c.get("custody")&&custodyMode==="frozen-supabase")c.set("custody",await frozenWitness(controlWitnessInput("selection",t,input.origin_id)));
       else if(identity.control&&c.get("custody"))await shadow("delivery.sessions.selection",c.get("custody")!,()=>controlWitnessInput("selection",t,input.origin_id));}
      const custody=c.get("custody");if(custody&&!identity.control)throw new IngestUnavailable("fence","control_unavailable");
      if(identity.control){
        if(identity.control.kind!=="selection"||!deps.deliver.ledger.applyOrderedControl)throw new IngestUnavailable("fence","control_unavailable");
        const result=orderedControlResult(await deps.deliver.ledger.applyOrderedControl({...selection,control:{...identity.control,kind:"selection"},...(custody?{custody}:{})}),identity.control);
        controlAck=result.control;custodyHeader(c,result.custodyAck);
      }else await deps.deliver.ledger.setSessionUploads(selection);
      emit({ route: "delivery.sessions.selection", outcome: controlAck?"control_"+controlAck.outcome:input.enabled ? "enabled" : "disabled", org_id: identity.orgId, count: input.sessions.length });
      return c.json({ updated: true,...(controlAck?{control:controlAck}:{}) });
    } catch (error) {
      emit({ route: "delivery.sessions.selection", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/delivery/sessions/reset", async (c) => {
    const identity = c.get("identity");
    try {
      const input = parseReset(await jsonOf(c));
      await requireDeliveryAccount(identity, input.account_id);
      const tenant={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      let deleted:number,controlAck;
      {const t={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
       if(identity.control&&!c.get("custody")&&custodyMode==="frozen-supabase")c.set("custody",await frozenWitness(controlWitnessInput("reset",t,null)));
       else if(identity.control&&c.get("custody"))await shadow("delivery.sessions.reset",c.get("custody")!,()=>controlWitnessInput("reset",t,null));}
      const custody=c.get("custody");if(custody&&!identity.control)throw new IngestUnavailable("fence","control_unavailable");
      if(identity.control){
        if(identity.control.kind!=="reset"||!deps.deliver.ledger.applyOrderedControl)throw new IngestUnavailable("fence","control_unavailable");
        const result=orderedControlResult(await deps.deliver.ledger.applyOrderedControl({tenant,requestId:input.request_id,control:{...identity.control,kind:"reset"},...(custody?{custody}:{})}),identity.control);
        deleted=result.deleted!;controlAck=result.control;custodyHeader(c,result.custodyAck);
      }else deleted=await deps.deliver.ledger.resetUploadedSessions(tenant,input.request_id);
      emit({ route: "delivery.sessions.reset", outcome: controlAck?"control_"+controlAck.outcome:"reset", org_id: identity.orgId, deleted });
      return c.json({ reset: true, deleted,...(controlAck?{control:controlAck}:{}) });
    } catch (error) {
      emit({ route: "delivery.sessions.reset", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });

  // Supabase-first mirror of Neon's privacy-control head (P1 option a). relayhistory: begin here,
  // then Neon begin (must return the same {version,digest}), apply, Neon complete, then complete here.
  const controlKind = (v: unknown): "selection" | "reset" => (v === "selection" || v === "reset" ? v : invalid("Invalid control kind"));
  // relayhistory frozen-supabase mode: the two remaining Neon reads on POST /v1/delivery/batches.
  app.post("/v1/custody/legacy-receipt", async (c) => {
    const identity = c.get("identity");
    try {
      if (!deps.deliver.ledger.frozenReceipt) throw new IngestUnavailable("fence", "custody_source_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected batch identity");
      const v = b as Record<string, unknown>;exactKeys(v, ["originId", "batchId"]);
      if (!identifier(v.originId) || !identifier(v.batchId)) invalid("Invalid batch identity");
      const stored = await deps.deliver.ledger.frozenReceipt({ tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, originId: v.originId as string, batchId: v.batchId as string });
      return c.json({ stored });
    } catch (error) {
      emit({ route: "custody.legacy_receipt", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  // 200 {pending:false} when no privacy control is pending for this user, else 503 (fail closed).
  app.post("/v1/custody/pending", async (c) => {
    const identity = c.get("identity");
    try {
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected origin");
      const v = b as Record<string, unknown>;exactKeys(v, ["originId"]);
      if (!identifier(v.originId)) invalid("Invalid origin");
      await frozenWitness({ kind: "batch", tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, originId: v.originId as string,
        batchId: "pending-check", batchDigest: "0".repeat(64), originalRecordCount: 0, candidates: [] });
      return c.json({ pending: false });
    } catch (error) {
      emit({ route: "custody.pending", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/custody/control/begin", async (c) => {
    const identity = c.get("identity");
    try {
      if (!deps.deliver.ledger.sourceControlBegin) throw new IngestUnavailable("fence", "control_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected control");
      exactKeys(b as Record<string, unknown>, ["epoch", "kind", "payload"]);
      const head = await deps.deliver.ledger.sourceControlBegin({ epoch: String((b as Record<string, unknown>).epoch), kind: controlKind((b as Record<string, unknown>).kind), payload: (b as Record<string, unknown>).payload,
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId } });
      emit({ route: "custody.control.begin", outcome: head.apply ? "begun" : "pending_replay", org_id: identity.orgId });
      return c.json(head);
    } catch (error) {
      emit({ route: "custody.control.begin", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/custody/control/complete", async (c) => {
    const identity = c.get("identity");
    try {
      if (!deps.deliver.ledger.sourceControlComplete) throw new IngestUnavailable("fence", "control_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected control completion");
      const v = b as Record<string, unknown>;exactKeys(v, ["epoch", "version", "digest", "kind", "outcome"]);
      if (v.outcome !== "applied" && v.outcome !== "replay" && v.outcome !== "stale") invalid("Invalid control outcome");
      const r = await deps.deliver.ledger.sourceControlComplete({ epoch: String(v.epoch), version: String(v.version), digest: String(v.digest), kind: controlKind(v.kind), outcome: v.outcome as "applied" | "replay" | "stale",
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId } });
      emit({ route: "custody.control.complete", outcome: r.superseded ? "superseded" : "completed", org_id: identity.orgId });
      return c.json(r);
    } catch (error) {
      emit({ route: "custody.control.complete", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });

  app.all("/v1/*", (c) => c.json(errorBody(c, "unsupported_by_backend", `${c.req.method} ${new URL(c.req.url).pathname} is not served by the session ingest`, { reason: "route_not_migrated" }), 501));
  app.notFound((c) => c.json(errorBody(c, "not_found", `No route for ${c.req.method} ${new URL(c.req.url).pathname}`), 404));
  app.onError((_err, c) => c.json(errorBody(c, "internal_error", "Internal server error"), 500));
  return app;
}
