import { appendFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { zstdDecompressSync } from "node:zlib";
import { Hono } from "hono";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  commitBackfillEpoch,
  computeContentSha256,
  isObjectStoreError,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  parseManifest,
  readAll,
  readRelayhistoryExport,
  sealRawChunk,
  type AppEnv,
  type BackfillChunk,
  type BackfillIO,
  type ManifestV1,
  type ObjectVersion,
  type SessionLocation,
} from "../../shared/vendor.ts";
import { runCourierOnce, type CourierConfig } from "../courier.ts";
import { feedRecordId } from "../feed-records.ts";
import {
  createCourierManifestRoutes,
  type ClaimlessUse,
} from "../manifests.ts";

const ORG = "org_test";
const WORKSPACE = "default";
const STARTED = 1_761_730_000_000;
export const EXPORTER = "migration:mgr_export";
export const MARKER = {
  version: 1,
  source: "neon.delivery_records",
  grant_id: "mgr_export",
  plan_digest: "a".repeat(64),
  source_user_id: "user",
};
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";

const authFor = (kind: string | undefined, machineId: string | undefined) => ({
  userId: "user",
  orgId: ORG,
  workspaceId: WORKSPACE,
  tokenSubject:
    kind === "migration"
      ? "agent_relayhistory_migration"
      : "agent_relayhistory_assertion",
  scopes: ["rth:sync"],
  claims:
    kind === "migration"
      ? {
          meta: {
            tokenClass: "migration_assertion",
            grantId: "mgr_export",
            planDigest: "a".repeat(64),
          },
        }
      : machineId
        ? { meta: { machineId } }
        : {},
  historyOrigin: "https://history.example",
});

/** A courier from before the capability: same writes, nothing advertised. */
function asLegacyWriter(init: RequestInit): RequestInit {
  const body = JSON.parse(String(init.body)) as { manifest: ManifestV1 };
  delete body.manifest.writer.capabilities;
  body.manifest.writer.plugin_version = "96bulls-courier/0.2.0";
  return { ...init, body: JSON.stringify(body) };
}

/** One client's courier against the real manifest routes and a real fs store. */
export async function courierWorld(
  options: {
    format?: "jsonl" | "ndjson";
    machineId?: string;
    sessionId?: string;
    root?: string;
    epoch?: number;
  } = {},
) {
  const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-races-"));
  const root = options.root ?? dir;
  const machineId = options.machineId ?? "machine-a";
  const sessionId = options.sessionId ?? "session-a";
  const dbPath = join(dir, "history.db");
  const statePath = join(dir, "state.json");
  const rawPath = join(dir, "session.jsonl");
  await writeFile(rawPath, "");
  const db = new DatabaseSync(dbPath);
  db.exec(
    "CREATE TABLE sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT, revision INTEGER DEFAULT 0)",
  );
  db.exec(
    "CREATE TABLE change_feed_store (singleton INTEGER PRIMARY KEY, epoch INTEGER NOT NULL)",
  );
  db.prepare("INSERT INTO change_feed_store VALUES (1, ?)").run(
    options.epoch ?? 171,
  );
  db.exec(
    "CREATE TABLE session_events (source TEXT, session_id TEXT, event_uid TEXT, ts_ms INTEGER, text TEXT, raw_facts_version INTEGER, revision INTEGER)",
  );
  db.exec(
    "CREATE TABLE evidence_tombstones (kind TEXT, source TEXT, session_id TEXT, record_key TEXT, revision INTEGER)",
  );
  db.prepare(
    "INSERT INTO sessions (session_id, source, raw_path, first_activity_ms, last_activity_ms, cwd, agent_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
  ).run(sessionId, "claude", rawPath, STARTED, STARTED + 100, null, "1.0.0");
  db.close();

  /** The fake issuer and gate: a real token's claim, and attested readers. */
  const gate = {
    missing: [] as string[],
    requeued: [] as { message: unknown; delaySeconds: number }[],
    claimless: [] as ClaimlessUse[],
    auditDown: false,
    activationDown: false,
    firstActivation: undefined as string | undefined,
  };
  const identity: { machineClaim?: string } = { machineClaim: machineId };
  const store = createFsStore({ root });
  const raw = { store, provider: "fs" as const, bucket: "raw" };
  const routes = createCourierManifestRoutes({
    manifests: objectStoreManifestReader(store, {
      provider: "fs",
      bucket: "raw",
    }),
    raw,
    send: async () => {},
    missingReaders: async () => gate.missing,
    firstClaimlessActivation: async (from) => {
      if (gate.activationDown) throw new Error("activation unavailable");
      gate.firstActivation ??= from;
      return gate.firstActivation;
    },
    recordClaimless: async (use) => {
      if (gate.auditDown) throw new Error("audit unavailable");
      gate.claimless.push(use);
    },
    requeueBackfill: async (_env, message, delaySeconds) =>
      void gate.requeued.push({ message, delaySeconds }),
  });
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("correlationId", "test");
    c.set(
      "auth",
      authFor(c.req.header("x-test-auth"), identity.machineClaim) as never,
    );
    await next();
  });
  app.route("/v1", routes);
  const env: Record<string, string> = {
    PIPELINE_ORG_ID: ORG,
    RELAYHISTORY_ORIGIN: "https://history.example",
  };
  const location: SessionLocation = {
    workspaceId: WORKSPACE,
    machineId,
    harness: "claude",
    sessionId,
    startDate: new Date(STARTED).toISOString().slice(0, 10),
  };
  const key = manifestKey(location);

  let revision = 0;
  const counts = { chunkPosts: 0, manifestPosts: 0 };
  /** Runs around a courier manifest POST; returns a replacement response to drop the real one. */
  const hooks: {
    legacyWriter?: boolean;
    /** Replaces the chunk hop's answer (e.g. a permanent 400). */
    chunkResponse?: () => Response | undefined;
    beforeManifest?: () => Promise<void>;
    afterManifest?: (res: Response) => Promise<Response | undefined>;
  } = {};
  const fetchImpl = (async (
    input: string | URL | Request,
    init?: RequestInit,
  ) => {
    const url = new URL(String(input));
    if (url.pathname.endsWith("/pipeline/chunks")) {
      counts.chunkPosts += 1;
      const replaced = hooks.chunkResponse?.();
      if (replaced) return replaced;
      const q = url.searchParams;
      const sealed = await sealRawChunk(store, {
        location: { ...location, startDate: q.get("date")! },
        format: q.get("format") as "jsonl" | "ndjson",
        seq: Number(q.get("seq")),
        epoch: Number(q.get("epoch")),
        startOffset: Number(q.get("offset")),
        source: new Uint8Array(init!.body as Uint8Array),
        traceId: new Headers(init!.headers).get("x-relay-trace-id")!,
      });
      return Response.json(sealed, { status: 201 });
    }
    if (url.pathname.endsWith("/v1/pipeline/manifests")) {
      counts.manifestPosts += 1;
      await hooks.beforeManifest?.();
      const res = await app.request(
        url.pathname,
        hooks.legacyWriter ? asLegacyWriter(init!) : init,
        env,
      );
      return (await hooks.afterManifest?.(res)) ?? res;
    }
    return app.request(`${url.pathname}${url.search}`, init, env);
  }) as typeof fetch;

  const config = (overrides: Partial<CourierConfig> = {}): CourierConfig => ({
    dbPath,
    statePath,
    ingestUrl: "http://ingest.test",
    token: { get: async () => "token", invalidate: () => false },
    orgId: ORG,
    workspaceId: WORKSPACE,
    userId: "user",
    machineId,
    harnesses: ["claude"],
    closeAfterMinutes: 1_000_000,
    startMode: "all",
    ...(options.format ? { format: options.format } : {}),
    ...overrides,
  });

  const post = (body: unknown, auth?: "migration") =>
    app.request(
      "/v1/pipeline/manifests",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": machineId,
          "x-session-id": sessionId,
          ...(auth ? { "x-test-auth": auth } : {}),
        },
        body: JSON.stringify(body),
      },
      env,
    );

  const stored = async (): Promise<
    { manifest: ManifestV1; version: ObjectVersion } | undefined
  > => {
    try {
      const object = await store.getObject(objectKey(key));
      const text = new TextDecoder().decode(await readAll(object.body));
      return {
        manifest: await parseManifest(text, key),
        version: object.meta.version,
      };
    } catch (error) {
      if (isObjectStoreError(error, "not_found")) return undefined;
      throw error;
    }
  };

  const seal = (
    text: string,
    seq: number,
    epoch: number,
    trace: string,
    format: "jsonl" | "ndjson" = "ndjson",
  ) =>
    sealRawChunk(store, {
      location,
      format,
      seq,
      epoch,
      startOffset: 0,
      source: new TextEncoder().encode(text),
      traceId: trace,
    });

  /** The exporter: seals at the planned `seq` and writes through the route. */
  const exportText = (id: string) =>
    `${JSON.stringify({ kind: "session_event", origin_id: "origin-1", record_id: id, revision: "1", received_at: new Date(STARTED).toISOString(), payload: { ts_ms: STARTED, text: id } })}\n`;
  const exporter = async (text: string, maxAttempts = 50) => {
    const { entry } = await seal(text, 900_000, 0, TRACE);
    const { seq: _seq, epoch: _epoch, ...chunk } = entry;
    const versions = new Map<ManifestV1, ObjectVersion>();
    const io: BackfillIO<ObjectVersion> = {
      read: async () => {
        const read = await stored();
        if (read) versions.set(read.manifest, read.version);
        return read;
      },
      seal: async (planned) => {
        const tail = planned.raw.chunks.at(-1)!;
        await seal(text, tail.seq, tail.epoch, planned.trace_id);
      },
      write: async (next, version) => {
        const res = await app.request(
          "/v1/pipeline/manifests",
          {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-machine-id": machineId,
              "x-session-id": sessionId,
              "x-test-auth": "migration",
            },
            body: JSON.stringify({
              key,
              manifest: next,
              previousVersion: version,
            }),
          },
          env,
        );
        if (res.status === 202) return true;
        if (res.status === 409) {
          const code = ((await res.json()) as { error?: { code?: string } })
            .error?.code;
          if (code === "activation_required")
            throw Object.assign(new Error(code), { code });
          return false;
        }
        throw new Error(`export refused: ${res.status} ${await res.text()}`);
      },
    };
    return commitBackfillEpoch(
      io,
      {
        openedBy: EXPORTER,
        openedAt: new Date().toISOString(),
        chunks: [chunk as BackfillChunk],
        marker: MARKER,
      },
      {
        maxAttempts,
        backoff: async () =>
          new Promise((done) => setTimeout(done, Math.random() * 3)),
      },
    );
  };

  /** A cold export: the manifest does not exist yet, and the exporter owns it. */
  const coldExport = async (
    text: string,
    marker: Record<string, unknown> = {},
  ) => {
    const { entry, scrub } = await seal(text, 0, 0, TRACE);
    const at = new Date().toISOString();
    const manifest: ManifestV1 = {
      schema_v: 1,
      layout: "v1",
      trace_id: TRACE,
      revision: 1,
      created_at: at,
      updated_at: at,
      session: {
        workspace_id: WORKSPACE,
        machine_id: machineId,
        harness: "claude",
        session_id: sessionId,
        started_at: new Date(STARTED).toISOString(),
      },
      writer: { delivery_instance_id: EXPORTER, plugin_version: "export" },
      raw: {
        owner: EXPORTER,
        state: "closed",
        updated_at: at,
        format: "ndjson",
        compression: "zstd",
        chunk_count: 1,
        raw_bytes: entry.bytes,
        stored_bytes: entry.stored_bytes,
        content_sha256: await computeContentSha256([entry]),
        epoch: 0,
        epochs: [
          {
            epoch: 0,
            opened_at: at,
            reason: "initial",
            first_seq: 0,
            opened_by: EXPORTER,
          },
        ],
        next_offset: entry.bytes,
        chunks: [entry],
        scrub,
      },
      extensions: {
        "client.relayhistory_export": { ...MARKER, ...marker },
        "96bulls": { user_id: "user" },
      },
    };
    const res = await post({ key, manifest }, "migration");
    if (res.status !== 202)
      throw new Error(`cold export refused: ${res.status}`);
    return manifest;
  };

  /** Every `harness_line` the merged stream holds, with no folding. */
  const liveRecords = async () => {
    const { manifest } = (await stored())!;
    const records = await readRelayhistoryExport(
      manifest,
      store,
      (b) => new Uint8Array(zstdDecompressSync(b)),
    );
    return {
      manifest,
      records,
      lines: records
        .filter((r) => r.kind === "session_event" && r.record_id !== "pre-t0")
        .map((r) => r.payload!.text as string),
    };
  };

  const run = async (overrides?: Partial<CourierConfig>) => {
    const original = globalThis.fetch;
    globalThis.fetch = fetchImpl;
    try {
      return await runCourierOnce(config(overrides));
    } finally {
      globalThis.fetch = original;
    }
  };

  return {
    app,
    env,
    gate,
    identity,
    store,
    rootDir: root,
    key,
    location,
    counts,
    hooks,
    machineId,
    statePath,
    rawPath,
    stored,
    seal,
    post,
    coldExport,
    exporter,
    exportText,
    liveRecords,
    run,
    /** One harness line: in the transcript, and as the row it became. */
    write: async (line: string) => {
      await appendFile(rawPath, `${line}\n`);
      const rw = new DatabaseSync(dbPath);
      revision += 1;
      rw.prepare(
        "INSERT INTO session_events VALUES ('claude', ?, ?, ?, ?, 3, ?)",
      ).run(sessionId, `uid-${revision}`, STARTED + revision, line, revision);
      rw.close();
      return revision;
    },
    /** The legacy window, open until `until`. */
    openClaimlessWindow: (until = new Date(Date.now() + 60_000)) => {
      env.MANIFEST_MACHINE_CLAIM = "compat";
      env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
        until.getTime() - 3_600_000,
      ).toISOString();
      env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL = until.toISOString();
    },
    setEpoch: (epoch: number) => {
      const rw = new DatabaseSync(dbPath);
      rw.prepare("UPDATE change_feed_store SET epoch = ?").run(epoch);
      rw.close();
    },
    /** Delete a row as the client does: a tombstone with its own revision. */
    tombstone: (n: number) => {
      const rw = new DatabaseSync(dbPath);
      revision += 1;
      rw.prepare("DELETE FROM session_events WHERE event_uid = ?").run(
        `uid-${n}`,
      );
      rw.prepare(
        "INSERT INTO evidence_tombstones VALUES ('session_event', 'claude', ?, ?, ?)",
      ).run(sessionId, `uid-${n}`, revision);
      rw.close();
    },
    /** A row as the client uploaded it: what the export holds for that row. */
    exportedRow: (
      n: number,
      line: string,
      origin = (options.epoch ?? 171).toString(16).padStart(16, "0"),
    ) =>
      `${JSON.stringify({
        kind: "session_event",
        origin_id: origin,
        record_id: feedRecordId([
          "session_event",
          "claude",
          sessionId,
          `uid-${n}`,
        ]),
        revision: String(n),
        operation: "upsert",
        received_at: new Date(STARTED).toISOString(),
        payload: {
          source: "claude",
          session_id: sessionId,
          event_uid: `uid-${n}`,
          ts_ms: STARTED + n,
          text: line,
        },
      })}\n`,
    state: async () => JSON.parse(await readFile(statePath, "utf8")),
    close: () => rm(dir, { recursive: true, force: true }),
  };
}
