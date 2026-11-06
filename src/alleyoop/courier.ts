/**
 * AlleyOop courier: local history (Concourse) to Locker. Reads sessions from
 * history.db, sends new transcript bytes through the ingest API's scrubbing
 * chunk hop (Technical Foul runs server-side, before any byte reaches the
 * Locker), writes the v1 manifest to the Locker, and tells the pipeline it
 * landed. Mints the session uuid (UUIDv7) once per session and keeps it in a
 * small state file next to the database.
 *
 * Runs once (`COURIER_ONCE=1`) or on an interval. Idempotent: the state file
 * records what was uploaded; a rerun uploads only new bytes.
 */
import { layoutComponent } from "../shared/layout-component.ts";
import { createHash } from "node:crypto";
import {
  mkdir,
  readFile,
  rename,
  stat,
  writeFile,
  open,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import {
  chunkKeys,
  computeContentSha256,
  exportWatermarks,
  liveEpoch,
  liveNextOffset,
  MANIFEST_EPOCHS_CAPABILITY,
  manifestKey,
  validateManifest,
  type ChunkEntry,
  type ManifestV1,
  type ObjectVersion,
  type RawFormat,
  type SessionLocation,
} from "../shared/vendor.ts";
import { flag, integer, optional, required } from "../shared/env.ts";
import {
  assertDestinationUrl,
  destinationPolicy,
} from "../shared/destination-url.ts";
import { mintToken } from "./auth.ts";
import { feedOrigin, feedRanges, readFeedRecords } from "./feed-records.ts";
import { hexTraceId, isUuidV7, spanId, uuidv7 } from "../shared/ids.ts";
import { errorFields, log } from "../shared/log.ts";
import {
  flushTelemetry,
  initTelemetry,
  withSpan,
} from "../shared/telemetry.ts";
import { EXT } from "../shared/enrich.ts";
import {
  CloudSessionAssertionProvider,
  fixedBearerToken,
  mintedBearerToken,
  RelayAuthAssertionProvider,
  secretCommand,
  type BearerTokenProvider,
} from "./relay-auth-token.ts";

export const PLUGIN_VERSION = "96bulls-courier/0.3.0";
const MAX_CHUNK_BYTES = 4 * 1024 * 1024;

interface SessionRow {
  session_id: string;
  source: string;
  raw_path: string | null;
  first_activity_ms: number | null;
  last_activity_ms: number | null;
  cwd: string | null;
  agent_version: string | null;
}

export function sessionsForStartMode<
  T extends Pick<SessionRow, "first_activity_ms" | "last_activity_ms">,
>(rows: T[], startMode: "all" | "new", enrolledAt?: string): T[] {
  if (startMode === "all" || !enrolledAt) return rows;
  const enrolledAtMs = Date.parse(enrolledAt);
  return Number.isFinite(enrolledAtMs)
    ? rows.filter(
        (row) =>
          (row.first_activity_ms ?? row.last_activity_ms ?? -1) >= enrolledAtMs,
      )
    : rows;
}

export type ManifestReconciliation = "verified" | "reset" | "defer";

export function manifestReconciliation(
  status?: number,
): ManifestReconciliation {
  if (status === 200) return "verified";
  if (status === 404) return "reset";
  return "defer";
}

interface SessionState {
  session_uuid: string;
  trace_id: string;
  revision: number;
  uploaded_offset: number;
  chunks: ChunkEntry[];
  created_at: string;
  started_at: string;
  scrub?: { ruleset: string; version: number };
  manifest_key?: string;
  manifest_version?: ObjectVersion;
  /** The stored manifest this session edits: never rebuilt from local state. */
  base?: ManifestV1;
  /** The change-feed epoch `uploaded_offset` belongs to, for `ndjson`. */
  feed_origin?: string;
  /** Another writer committed first: adopt the stored copy before writing again. */
  needs_rebase?: boolean;
  /** The stored manifest is a cold export: the next write takes it over. */
  adopting?: boolean;
  /** Another client owns the raw stream: stop writing; the records stay local. */
  fenced?: boolean;
  closed?: boolean;
  /** Chunks were sealed but the manifest has not been acknowledged yet. */
  pending?: boolean;
  /** Exact manifest replayed until acknowledged; prevents timestamps changing after a committed write lost its response. */
  pending_manifest?: ManifestV1;
  /** CAS precondition paired with pending_manifest so the whole request is replayed unchanged. */
  pending_previous_version?: ObjectVersion;
  /**
   * The ingest refused this session's chunk permanently (a 4xx, e.g. a text it
   * cannot scrub safely). Retried with backoff (1 h, doubling, at most 24 h), so
   * one session never costs the ingest a refused scrub every pass. Codes only.
   */
  refused?: { code: string; at: string; attempts: number };
}

/** Backoff after a permanent chunk refusal: 1 h, doubling, at most 24 h. */
export function refusalBackoffMs(attempts: number): number {
  return Math.min(24, 2 ** Math.max(0, attempts - 1)) * 60 * 60 * 1000;
}

interface CourierState {
  machine_id: string;
  sessions: Record<string, SessionState>;
  /** When set, sessions that already existed before host enrollment stay local. */
  enrolled_at?: string;
}

export function initializeEnrollment(
  state: { enrolled_at?: string },
  startMode: "all" | "new",
  now = new Date(),
): boolean {
  if (startMode !== "new" || state.enrolled_at) return false;
  state.enrolled_at = now.toISOString();
  return true;
}

export interface CourierConfig {
  dbPath: string;
  statePath: string;
  ingestUrl: string;
  /** Fixed/local bearer auth or a JIT RelayAuth assertion provider. */
  token: BearerTokenProvider;
  orgId: string;
  workspaceId: string;
  userId: string;
  machineId?: string;
  harnesses?: string[];
  closeAfterMinutes: number;
  startMode: "all" | "new";
  /** Tokens carry `meta.machineId`, so `machineId` is an identity, not a label. */
  machineBound?: boolean;
  /** Format of new sessions; a stored manifest's format always wins. Default `jsonl`. */
  format?: RawFormat;
  /** Process-local startup checks: persisted upload state is reconciled once, not every pass. */
  verifiedManifests?: Set<string>;
}

export function configFromEnv(): CourierConfig {
  const dbPath = required("CONCOURSE_DB");
  const orgId = required("TENANT_ORG_ID");
  const workspaceId = optional("WORKSPACE_ID", "default");
  const userId = optional("USER_ID", "unknown");
  const startMode = optional("COURIER_START_MODE", "new");
  if (startMode !== "all" && startMode !== "new")
    throw new Error("96bulls: COURIER_START_MODE must be all or new");
  let token: BearerTokenProvider;
  // ALLOW_IN_CLUSTER_HTTP: one policy for every destination below.
  const policy = destinationPolicy();
  const cloudSession = !!process.env.CLOUD_URL;
  const relayAuthSession = !!process.env.RELAYAUTH_URL;
  const ingestUrl = required("INGEST_URL").replace(/\/$/, "");
  if (cloudSession || relayAuthSession) {
    // A durable, machine-bound assertion rides on every upload: the ingest
    // URL gets the same transport rule as the mint URL.
    assertDestinationUrl("INGEST_URL", ingestUrl, policy);
  } else {
    // Local signing key / fixed token are local-only modes (Docker network
    // names such as http://96bulls-ingest:8080); only the URL shape is checked.
    assertDestinationUrl("INGEST_URL", ingestUrl, { allowInClusterHttp: false }, {
      allowAnyHttpHost: true,
    });
  }
  if (cloudSession) {
    token = new CloudSessionAssertionProvider({
      baseUrl: required("CLOUD_URL"),
      workspaceId,
      machineId: required("MACHINE_ID"),
      cloudToken: secretCommand(required("CLOUD_TOKEN_COMMAND_JSON")),
      destinationPolicy: policy,
    });
  } else if (relayAuthSession) {
    token = new RelayAuthAssertionProvider({
      baseUrl: required("RELAYAUTH_URL"),
      historyOrigin: required("RELAYAUTH_HISTORY_ORIGIN"),
      orgId,
      workspaceId,
      machineId: required("MACHINE_ID"),
      sponsorId: userId,
      apiKey: secretCommand(required("RELAYAUTH_API_KEY_COMMAND_JSON")),
      destinationPolicy: policy,
      expiresInSeconds: integer("RELAYAUTH_ASSERTION_TTL_SECONDS", 60),
      refreshSkewSeconds: integer(
        "RELAYAUTH_ASSERTION_REFRESH_SKEW_SECONDS",
        15,
      ),
    });
  } else if (process.env.JWT_PRIVATE_KEY_PEM) {
    if (startMode === "new" && !flag("COURIER_ALLOW_LOCAL_SIGNING_KEY", false))
      throw new Error(
        "96bulls: JWT_PRIVATE_KEY_PEM is local-only; durable host enrollment requires RelayAuth",
      );
    token = mintedBearerToken(() =>
      mintToken(
        {
          kid: required("JWT_KID"),
          privateKeyPem: required("JWT_PRIVATE_KEY_PEM"),
        },
        {
          machineId: required("MACHINE_ID"),
          issuer: optional("AUTH_ISSUER", "https://96bulls.local"),
          audience: optional("AUTH_AUDIENCE", "relayhistory"),
          orgId,
          workspaceId,
          userId,
          ttlSeconds: 3600,
        },
      ),
    );
  } else {
    if (!flag("COURIER_ALLOW_FIXED_TOKEN", false))
      throw new Error(
        "96bulls: ALLEYOOP_TOKEN requires explicit COURIER_ALLOW_FIXED_TOKEN=1",
      );
    token = fixedBearerToken(required("ALLEYOOP_TOKEN"));
  }
  return {
    dbPath,
    statePath: optional(
      "COURIER_STATE",
      join(dirname(dbPath), ".alleyoop-courier.json"),
    ),
    ingestUrl,
    token,
    orgId,
    workspaceId,
    userId,
    ...(process.env.MACHINE_ID ? { machineId: process.env.MACHINE_ID } : {}),
    ...(cloudSession || relayAuthSession || process.env.JWT_PRIVATE_KEY_PEM
      ? { machineBound: true }
      : {}),
    ...(process.env.HARNESSES
      ? {
          harnesses: process.env.HARNESSES.split(",")
            .map((s) => s.trim())
            .filter(Boolean),
        }
      : {}),
    closeAfterMinutes: integer("COURIER_CLOSE_AFTER_MINUTES", 30),
    startMode,
    ...(optional("COURIER_FORMAT", "jsonl") === "ndjson"
      ? { format: "ndjson" as const }
      : {}),
  };
}

async function loadState(
  path: string,
  machineId: string | undefined,
  startMode: "all" | "new",
): Promise<CourierState> {
  try {
    const s = JSON.parse(await readFile(path, "utf8")) as CourierState;
    if (machineId && s.machine_id !== machineId)
      log.warn("machine id differs from state; keeping state's", {});
    if (initializeEnrollment(s, startMode)) await saveState(path, s);
    return s;
  } catch {
    return {
      machine_id:
        machineId ??
        `m-${createHash("sha256").update(uuidv7()).digest("hex").slice(0, 16)}`,
      sessions: {},
      ...(startMode === "new" ? { enrolled_at: new Date().toISOString() } : {}),
    };
  }
}

async function saveState(path: string, state: CourierState) {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.tmp`;
  await writeFile(tmp, JSON.stringify(state, null, 2));
  await rename(tmp, path);
}

/** Reads the session catalogue from history.db (read-only). */
export async function readSessions(
  dbPath: string,
  harnesses?: string[],
): Promise<SessionRow[]> {
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const rows = db
      .prepare(
        "SELECT session_id, source, raw_path, first_activity_ms, last_activity_ms, cwd, agent_version FROM sessions WHERE raw_path IS NOT NULL ORDER BY first_activity_ms",
      )
      .all() as unknown as SessionRow[];
    return harnesses ? rows.filter((r) => harnesses.includes(r.source)) : rows;
  } finally {
    db.close();
  }
}

const isoDate = (ms: number) => new Date(ms).toISOString().slice(0, 10);
// The same map as the Neon export (src/shared/layout-component.ts).
const component = layoutComponent;

/** The next contiguous range of whole records after `from`, capped per chunk. */
async function readNewRecords(
  path: string,
  from: number,
  maxChunkBytes = MAX_CHUNK_BYTES,
): Promise<{ ranges: Array<{ start: number; bytes: Buffer }>; size: number }> {
  const size = (await stat(path)).size;
  if (size <= from) return { ranges: [], size };
  const fh = await open(path, "r");
  try {
    const all = Buffer.alloc(size - from);
    await fh.read(all, 0, all.length, from);
    const lastNl = all.lastIndexOf(0x0a);
    if (lastNl < 0) return { ranges: [], size };
    const usable = all.subarray(0, lastNl + 1);
    const ranges: Array<{ start: number; bytes: Buffer }> = [];
    let cursor = 0;
    while (cursor < usable.length) {
      let end = Math.min(cursor + maxChunkBytes, usable.length);
      if (end < usable.length) {
        const nl = usable.lastIndexOf(0x0a, end - 1);
        if (nl <= cursor)
          throw new Error("96bulls: a single record exceeds the chunk size");
        end = nl + 1;
      }
      ranges.push({
        start: from + cursor,
        bytes: Buffer.from(usable.subarray(cursor, end)),
      });
      cursor = end;
    }
    return { ranges, size };
  } finally {
    await fh.close();
  }
}

type ChunkWithSource = ChunkEntry & {
  source_end?: unknown;
  source_origin?: unknown;
};

/**
 * Where this client's source resumes. `jsonl`: a file offset, `start_offset`.
 * `ndjson`: a feed revision, recorded per chunk as `source_end` beside its
 * `source_origin`, because `start_offset` is then an offset in the stream. A
 * feed whose epoch moved has no position in the stored chunks,
 * and is held, not resumed: see `feed_epoch_moved`. `undefined` when the chunks
 * do not say.
 */
export function sourceResumeOffset(manifest: ManifestV1): number | undefined {
  if (manifest.raw.format !== "ndjson") return liveNextOffset(manifest.raw);
  const tail = chunkKeys(manifest, { currentEpochOnly: true }).at(-1)?.entry as
    ChunkWithSource | undefined;
  if (!tail) return 0;
  if (
    typeof tail.source_end !== "number" ||
    typeof tail.source_origin !== "string"
  )
    return undefined;
  return tail.source_end;
}

/** A condition a person must see; the courier keeps going. */
export type CourierCondition = {
  type: "feed_epoch_moved";
  session_uuid: string;
  from: string;
  to: string;
};

/** Take the stored manifest as the new base: identity, chunks and position. */
function adoptManifest(
  session: SessionState,
  remote: ManifestV1,
  version: ObjectVersion,
  offset: number,
  adopting = false,
) {
  session.adopting = adopting;
  const ext = remote.extensions?.[EXT] as
    { session_uuid?: unknown } | undefined;
  if (typeof ext?.session_uuid === "string")
    session.session_uuid = ext.session_uuid;
  session.trace_id = remote.trace_id;
  session.created_at = remote.created_at;
  session.started_at = remote.session.started_at;
  session.revision = remote.revision;
  session.chunks = remote.raw.chunks;
  session.scrub = remote.raw.scrub;
  session.base = remote;
  session.manifest_version = version;
  session.uploaded_offset = offset;
  session.closed = remote.raw.state === "closed";
  session.needs_rebase = false;
  session.pending = false;
  session.pending_manifest = undefined;
  session.pending_previous_version = undefined;
}

type Rebase = "rebased" | "reset" | "fenced" | "defer";

/**
 * The compare-and-swap retry: read the stored copy and adopt it. Chunks sealed
 * for the lost write stay as orphans, and the next pass re-reads the source
 * from the adopted position under the new `seq`. A pending write that did
 * commit is in the stored copy, so nothing is uploaded twice.
 */
async function rebaseSession(
  cfg: CourierConfig,
  machineId: string,
  session: SessionState,
  mKey: string,
): Promise<Rebase> {
  const q = new URLSearchParams({
    key: mKey,
    machine: machineId,
    writer: machineId,
    manifest: "1",
  });
  const res = await authorizedFetch(
    cfg.token,
    `${cfg.ingestUrl}/v1/pipeline/manifests/status?${q}`,
    { method: "GET", headers: { "x-relay-client": PLUGIN_VERSION } },
  ).catch(() => undefined);
  if (res?.status === 404) return "reset";
  // A 403 here is about the caller's credential, never ownership: only the
  // stored `raw.owner` may fence a session, because fencing is permanent.
  if (res?.status !== 200) return "defer";
  const body = (await res.json().catch(() => ({}))) as {
    version?: ObjectVersion;
    manifest?: ManifestV1;
  };
  if (!body.manifest || (!body.version?.eTag && !body.version?.version))
    return "defer";
  if ((await validateManifest(body.manifest, mKey)).length > 0) return "defer";
  const origin = feedOrigin(cfg.dbPath);
  if (body.manifest.raw.owner.startsWith("migration:")) {
    // Nobody holds this stream. Take it over after what the export already
    // holds: the takeover write is the fence, and a lost race lands back here
    // or fenced.
    const marks = exportWatermarks(body.manifest);
    const exported = Object.keys(marks);
    adoptManifest(
      session,
      body.manifest,
      body.version,
      origin ? (marks[origin] ?? 0) : 0,
      true,
    );
    // A feed the export never saw would put one record under two origins, which
    // has to hold like any other moved epoch.
    session.feed_origin =
      origin && exported.length > 0 && !(origin in marks)
        ? exported[0]
        : origin;
    return "rebased";
  }
  if (body.manifest.raw.owner !== machineId) return "fenced";
  const offset = sourceResumeOffset(body.manifest);
  if (offset === undefined) return "fenced";
  adoptManifest(session, body.manifest, body.version, offset);
  const tail = chunkKeys(body.manifest, { currentEpochOnly: true }).at(-1)
    ?.entry as ChunkWithSource | undefined;
  // What the stored chunks say, not this store's: a moved epoch must be seen.
  session.feed_origin =
    typeof tail?.source_origin === "string" ? tail.source_origin : origin;
  return "rebased";
}

const blankUpload = () => ({
  revision: 0,
  uploaded_offset: 0,
  chunks: [],
  closed: false,
  pending: false,
  pending_manifest: undefined,
  pending_previous_version: undefined,
  scrub: undefined,
  manifest_key: undefined,
  manifest_version: undefined,
  base: undefined,
  needs_rebase: false,
  adopting: false,
});

export async function runCourierOnce(cfg: CourierConfig): Promise<{
  sessions: number;
  uploaded: number;
  chunks: number;
  conditions: CourierCondition[];
}> {
  const state = await loadState(cfg.statePath, cfg.machineId, cfg.startMode);
  // Every key and writer embeds the state's id, and a bound token only matches
  // the claimed one, so continuing would fail each session forever.
  if (cfg.machineBound && state.machine_id !== cfg.machineId)
    throw Object.assign(
      new Error(
        "96bulls: the state's machine id is not the claimed MACHINE_ID",
      ),
      { code: "machine_id_mismatch", retryable: false },
    );
  const discovered = await readSessions(cfg.dbPath, cfg.harnesses);
  const rows = sessionsForStartMode(
    discovered,
    cfg.startMode,
    state.enrolled_at,
  );
  const verified = (cfg.verifiedManifests ??= new Set());
  let uploaded = 0;
  let chunks = 0;
  const conditions: CourierCondition[] = [];
  const deferred = new Set<string>();
  // The ledger follows the Locker, not the other way round: if a recorded manifest is
  // gone (a fresh stack after `down`), the upload state resets while the ids stay.
  for (const [key, st] of Object.entries(state.sessions)) {
    if (!st.manifest_key || (st.revision === 0 && !st.pending_manifest))
      continue;
    if (verified.has(st.manifest_key)) continue;
    const q = new URLSearchParams({
      key: st.manifest_key,
      machine: state.machine_id,
      writer: state.machine_id,
    });
    const head = await authorizedFetch(
      cfg.token,
      `${cfg.ingestUrl}/v1/pipeline/manifests/status?${q}`,
      { method: "GET", headers: { "x-relay-client": PLUGIN_VERSION } },
    ).catch(() => undefined);
    if (head?.status === 403) {
      // The stored writer differs. The session's rebase reads the owner.
      st.needs_rebase = true;
      continue;
    }
    const reconciliation = manifestReconciliation(head?.status);
    if (reconciliation === "verified") {
      const status = (await head!.json().catch(() => ({}))) as {
        version?: ObjectVersion;
      };
      if (!status.version?.eTag && !status.version?.version) {
        deferred.add(key);
        continue;
      }
      st.manifest_version = status.version;
      verified.add(st.manifest_key);
    } else if (reconciliation === "reset") {
      log.info("manifest missing in locker; re-uploading with the same ids", {
        session_uuid: st.session_uuid,
        trace_id: st.trace_id,
      });
      state.sessions[key] = { ...st, ...blankUpload() };
    } else {
      log.warn("manifest status unavailable; keeping durable upload state", {
        session_uuid: st.session_uuid,
        trace_id: st.trace_id,
        status: head?.status,
      });
      deferred.add(key);
    }
  }
  for (const row of rows) {
    const startedMs = row.first_activity_ms ?? row.last_activity_ms;
    if (!row.raw_path || startedMs === null) continue;
    const key = `${row.source}:${row.session_id}`;
    if (deferred.has(key)) continue;
    const now = new Date();
    let s = state.sessions[key];
    if (!s) {
      const startedAt = new Date(startedMs).toISOString();
      s = state.sessions[key] = {
        session_uuid: uuidv7(),
        trace_id: uuidv7(),
        revision: 0,
        uploaded_offset: 0,
        chunks: [],
        created_at: now.toISOString(),
        started_at: startedAt,
      };
      log.info("session first seen; minted ids", {
        harness: row.source,
        session_id: row.session_id,
        session_uuid: s.session_uuid,
        trace_id: s.trace_id,
        session_uuid_source: "minted",
      });
      // The first sealed chunk is create-only and carries the trace id in its
      // metadata. Persist identity before any chunk request so a crash cannot
      // mint a different trace for the same deterministic chunk key.
      await saveState(cfg.statePath, state);
    }
    let session = s;
    if (session.fenced) continue;
    if (session.refused && now.getTime() - Date.parse(session.refused.at) < refusalBackoffMs(session.refused.attempts))
      continue;
    const loc: SessionLocation = {
      workspaceId: cfg.workspaceId,
      machineId: state.machine_id,
      harness: component(row.source),
      sessionId: component(row.session_id),
      startDate: isoDate(startedMs),
    };
    const mKey = manifestKey(loc);
    if (session.needs_rebase || (session.revision > 0 && !session.base)) {
      const rebased = await rebaseSession(cfg, state.machine_id, session, mKey);
      if (rebased === "defer") continue;
      if (rebased === "fenced") {
        session.fenced = true;
        log.warn(
          "raw stream belongs to another writer; stopping uploads for this session",
          { session_uuid: session.session_uuid, trace_id: session.trace_id },
        );
        await saveState(cfg.statePath, state);
        continue;
      }
      if (rebased === "reset")
        session = state.sessions[key] = { ...session, ...blankUpload() };
      await saveState(cfg.statePath, state);
    }
    const uploading = session;
    const format: RawFormat =
      uploading.base?.raw.format ?? cfg.format ?? "jsonl";
    const traceparent = `00-${hexTraceId(session.trace_id)}-${spanId()}-01`;
    const idleMinutes = row.last_activity_ms
      ? (now.getTime() - row.last_activity_ms) / 60_000
      : 0;
    const shouldClose = idleMinutes >= cfg.closeAfterMinutes;
    let result: {
      ranges: Array<{ start: number; end?: number; bytes: Buffer }>;
      size: number;
    };
    if (session.pending_manifest) {
      // Land the exact already-sealed revision before reading or appending more bytes.
      result = { ranges: [], size: session.uploaded_offset };
    } else {
      try {
        if (format === "ndjson") {
          const feed = readFeedRecords(
            cfg.dbPath,
            row.source,
            row.session_id,
            0,
          );
          if (!feed.origin) {
            log.warn("ndjson needs a change feed; this store has none", {
              session_uuid: session.session_uuid,
              trace_id: session.trace_id,
            });
            continue;
          }
          if (session.feed_origin && session.feed_origin !== feed.origin) {
            // A rebuilt store renumbers its feed but keeps its record ids. The
            // same record under two origins has no order the transform can
            // settle, and fails the session for good, so nothing is sent: the
            // session holds here until a person decides.
            conditions.push({
              type: "feed_epoch_moved",
              session_uuid: session.session_uuid,
              from: session.feed_origin,
              to: feed.origin,
            });
            log.warn("change feed epoch moved; holding this session", {
              session_uuid: session.session_uuid,
              trace_id: session.trace_id,
              condition: "feed_epoch_moved",
            });
            continue;
          }
          session.feed_origin = feed.origin;
          const after = session.uploaded_offset;
          result = {
            ranges: feedRanges(
              feed.records.filter((record) => record.revision > after),
              after,
              MAX_CHUNK_BYTES,
            ),
            size: after,
          };
        } else {
          result = await readNewRecords(row.raw_path, session.uploaded_offset);
        }
      } catch (err) {
        log.warn("transcript unreadable", {
          harness: row.source,
          session_id: row.session_id,
          ...errorFields(err),
        });
        continue;
      }
    }
    if (result.size < session.uploaded_offset) {
      log.warn("transcript shrank; epochs are not handled yet, skipping", {
        harness: row.source,
        session_id: row.session_id,
      });
      continue;
    }
    const nothingNew = result.ranges.length === 0;
    if (nothingNew && !session.pending) {
      if (session.revision === 0 || session.adopting) continue;
      if (session.closed || !shouldClose) continue;
    }
    await withSpan(
      "alleyoop upload",
      {
        traceparent,
        attrs: {
          stage: "ingest",
          trace_id: session.trace_id,
          session_uuid: session.session_uuid,
          tenant_id: cfg.orgId,
          workspace_id: cfg.workspaceId,
          harness: row.source,
          session_id: row.session_id,
        },
      },
      async () => {
        const session = uploading;
        const base = session.base;
        // A backfill epoch is sealed: new chunks continue the live offsets in a
        // `resumed` epoch, opened only when there is something to append.
        const resuming =
          !!base &&
          result.ranges.length > 0 &&
          (session.adopting || base.raw.epochs.at(-1)?.reason === "backfill");
        const pendingChunks = session.chunks.slice(base?.raw.chunk_count ?? 0);
        const epoch =
          pendingChunks.at(-1)?.epoch ??
          (base ? base.raw.epoch + (resuming ? 1 : 0) : 0);
        let stream = pendingChunks.at(-1)
          ? pendingChunks.at(-1)!.start_offset + pendingChunks.at(-1)!.bytes
          : base && !session.adopting
            ? liveNextOffset(base.raw)
            : 0;
        for (const range of result.ranges) {
          const seq = session.chunks.length;
          const q = new URLSearchParams({
            format,
            seq: String(seq),
            epoch: String(epoch),
            offset: String(stream),
            date: String(loc.startDate),
            machine: loc.machineId,
            harness: loc.harness,
            session: loc.sessionId,
          });
          const res = await authorizedFetch(
            cfg.token,
            `${cfg.ingestUrl}/v1/pipeline/chunks?${q}`,
            {
              method: "POST",
              headers: {
                "content-type": "application/octet-stream",
                "x-relay-trace-id": session.trace_id,
                "x-machine-id": loc.machineId,
                "x-relay-client": PLUGIN_VERSION,
                "x-session-id": loc.sessionId,
                traceparent,
              },
              body: new Uint8Array(range.bytes),
            },
          );
          if (!res.ok) {
            const body = (await res.json().catch(() => ({}))) as {
              error?: { code?: string };
            };
            throw Object.assign(new Error("chunk hop refused"), {
              code: body.error?.code ?? `http_${res.status}`,
              retryable: res.status >= 500,
              // A 4xx other than auth/rate limits will refuse the same bytes again.
              permanent: res.status >= 400 && res.status < 500 && res.status !== 401 && res.status !== 403 && res.status !== 408 && res.status !== 429,
            });
          }
          const sealed = (await res.json()) as {
            key: string;
            entry: ChunkEntry;
            scrub: { ruleset: string; version: number };
            created: boolean;
          };
          const entry: ChunkEntry =
            format === "ndjson"
              ? {
                  ...sealed.entry,
                  source_end: range.end,
                  source_origin: session.feed_origin,
                }
              : sealed.entry;
          session.chunks.push(entry);
          stream += sealed.entry.bytes;
          session.scrub =
            session.scrub && session.scrub.version < sealed.scrub.version
              ? session.scrub
              : sealed.scrub;
          session.uploaded_offset =
            range.end ?? range.start + range.bytes.length;
          session.pending = true; // sealed chunks exist that no landed manifest lists yet
          chunks += 1;
          log.info("chunk sealed", {
            stage: "deliver.put",
            seq,
            offset: range.start,
            bytes_raw: sealed.entry.bytes,
            bytes_stored: sealed.entry.stored_bytes,
            scrub_ruleset: sealed.scrub.ruleset,
            scrub_version: sealed.scrub.version,
            trace_id: session.trace_id,
            session_uuid: session.session_uuid,
          });
        }
        // The revision and closed flag advance only once the exact manifest has
        // been acknowledged. Persist it before POST so a committed write whose
        // response is lost can be replayed byte-for-byte after a restart.
        let manifest = session.pending_manifest;
        if (!manifest) {
          const revision = session.revision + 1;
          const last = session.chunks.at(-1)!;
          const nowIso = new Date().toISOString();
          const appended = session.chunks.length > (base?.raw.chunk_count ?? 0);
          const epochs =
            base && appended && session.adopting
              ? [
                  ...base.raw.epochs,
                  {
                    epoch,
                    opened_at: nowIso,
                    reason: "adopted" as const,
                    first_seq: base.raw.chunk_count,
                    opened_by: state.machine_id,
                  },
                ]
              : base &&
                  appended &&
                  base.raw.epochs.at(-1)?.reason === "backfill"
                ? [
                    ...base.raw.epochs,
                    {
                      epoch,
                      opened_at: nowIso,
                      reason: "resumed" as const,
                      first_seq: base.raw.chunk_count,
                      opened_by: state.machine_id,
                      resumes_epoch: liveEpoch(base.raw),
                    },
                  ]
                : (base?.raw.epochs ?? [
                    {
                      epoch: 0,
                      opened_at: session.started_at,
                      reason: "initial" as const,
                      first_seq: 0,
                      opened_by: state.machine_id,
                    },
                  ]);
          const { ended_at: _ended, ...baseSession } = base?.session ?? {};
          manifest = {
            ...base,
            schema_v: 1,
            layout: "v1",
            trace_id: session.trace_id,
            revision,
            created_at: session.created_at,
            updated_at: nowIso,
            session: {
              ...baseSession,
              workspace_id: loc.workspaceId,
              machine_id: loc.machineId,
              harness: loc.harness,
              session_id: loc.sessionId,
              started_at: session.started_at,
              ...(shouldClose && row.last_activity_ms
                ? { ended_at: new Date(row.last_activity_ms).toISOString() }
                : {}),
              ...(row.agent_version
                ? { harness_version: row.agent_version }
                : {}),
              ...(row.cwd
                ? {
                    cwd_sha256: createHash("sha256")
                      .update(row.cwd)
                      .digest("hex"),
                  }
                : {}),
            },
            writer: {
              ...base?.writer,
              delivery_instance_id: state.machine_id,
              plugin_version: PLUGIN_VERSION,
              capabilities: [MANIFEST_EPOCHS_CAPABILITY],
            },
            raw: {
              ...base?.raw,
              owner: state.machine_id,
              state: shouldClose ? "closed" : "open",
              updated_at: nowIso,
              format,
              compression: "zstd",
              chunk_count: session.chunks.length,
              raw_bytes: session.chunks.reduce((n, c) => n + c.bytes, 0),
              stored_bytes: session.chunks.reduce(
                (n, c) => n + c.stored_bytes,
                0,
              ),
              content_sha256: await computeContentSha256(session.chunks),
              epoch: epochs.length - 1,
              epochs,
              next_offset: last.start_offset + last.bytes,
              chunks: session.chunks,
              ...(session.scrub ? { scrub: session.scrub } : {}),
            },
            extensions: {
              ...base?.extensions,
              [EXT]: {
                session_uuid: session.session_uuid,
                session_uuid_source: "minted",
                tenant_id: cfg.orgId,
                user_id: cfg.userId,
                canonical_session_id: row.session_id,
                canonical_machine_id: state.machine_id,
              },
            },
          };
        }
        const issues = await validateManifest(manifest, mKey);
        if (issues.length) {
          log.error("manifest invalid", {
            count: issues.length,
            reason: issues[0]!.path,
          });
          throw Object.assign(new Error("manifest invalid"), {
            code: "manifest_invalid",
            retryable: false,
          });
        }
        session.manifest_key = mKey;
        session.pending = true;
        if (!session.pending_manifest)
          session.pending_previous_version = session.manifest_version;
        session.pending_manifest = manifest;
        await saveState(cfg.statePath, state);
        const landed = await authorizedFetch(
          cfg.token,
          `${cfg.ingestUrl}/v1/pipeline/manifests`,
          {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-relay-trace-id": session.trace_id,
              "x-machine-id": loc.machineId,
              "x-relay-client": PLUGIN_VERSION,
              "x-session-id": loc.sessionId,
              traceparent,
            },
            body: JSON.stringify({
              key: mKey,
              manifest,
              ...(session.pending_previous_version
                ? { previousVersion: session.pending_previous_version }
                : {}),
            }),
          },
        );
        if (landed.status !== 202) {
          const b = (await landed.json().catch(() => ({}))) as {
            error?: { code?: string };
          };
          if (
            landed.status === 409 &&
            (b.error?.code === "revision_conflict" ||
              b.error?.code === "owner_conflict")
          ) {
            // Another writer committed first. Replaying these bytes can never
            // land: adopt the stored copy on the next pass.
            session.needs_rebase = true;
            log.info("manifest lost a race; rebasing on the stored copy", {
              session_uuid: session.session_uuid,
              trace_id: session.trace_id,
            });
            return;
          }
          throw Object.assign(new Error("manifest not accepted"), {
            code: b.error?.code ?? `http_${landed.status}`,
            retryable: landed.status >= 500,
          });
        }
        const landedBody = (await landed.json()) as { version?: ObjectVersion };
        if (!landedBody.version?.eTag && !landedBody.version?.version)
          throw Object.assign(
            new Error("manifest response omitted its version"),
            { code: "manifest_version_missing", retryable: true },
          );
        session.revision = manifest.revision;
        session.manifest_version = landedBody.version;
        session.base = manifest;
        session.adopting = false;
        session.closed = manifest.raw.state === "closed";
        session.pending = false;
        session.pending_manifest = undefined;
        session.pending_previous_version = undefined;
        verified.add(mKey);
        uploaded += 1;
        log.info("manifest landed", {
          stage: "object-landed",
          revision: manifest.revision,
          chunk_count: session.chunks.length,
          bytes_raw: manifest.raw.raw_bytes,
          trace_id: session.trace_id,
          session_uuid: session.session_uuid,
          manifest_key_sha256: createHash("sha256")
            .update(mKey)
            .digest("hex")
            .slice(0, 16),
          state: manifest.raw.state,
        });
      },
    ).then(() => {
      if (uploading.refused) delete uploading.refused;
    }).catch((err) => {
      if ((err as { permanent?: unknown }).permanent === true) {
        const attempts = (uploading.refused?.attempts ?? 0) + 1;
        uploading.refused = { code: String((err as { code?: unknown }).code ?? "refused"), at: new Date().toISOString(), attempts };
      }
      log.error("session upload failed", {
        harness: row.source,
        session_id: row.session_id,
        trace_id: session.trace_id,
        session_uuid: session.session_uuid,
        ...errorFields(err),
      });
    });
    await saveState(cfg.statePath, state);
  }
  await saveState(cfg.statePath, state);
  return { sessions: rows.length, uploaded, chunks, conditions };
}

async function authorizedFetch(
  provider: BearerTokenProvider,
  input: string,
  init: RequestInit,
): Promise<Response> {
  const send = async () => {
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${await provider.get()}`);
    return fetch(input, { ...init, headers });
  };
  const response = await send();
  if (response.status !== 401 || !provider.invalidate()) return response;
  return send();
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "alleyoop-courier";
  initTelemetry();
  const cfg = configFromEnv();
  const once = flag("COURIER_ONCE", false);
  const interval = integer("COURIER_INTERVAL_MS", 10_000);
  for (;;) {
    try {
      const r = await runCourierOnce(cfg);
      log.info("courier pass complete", {
        count: r.sessions,
        enqueued: r.uploaded,
        chunk_count: r.chunks,
      });
    } catch (err) {
      log.error("courier pass failed", errorFields(err));
      if (process.env.DEBUG === "1") console.error(err);
    }
    await flushTelemetry();
    if (once) process.exit(0);
    await new Promise((r) => setTimeout(r, interval));
  }
}
