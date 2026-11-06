/**
 * AlleyOop ingest: the HTTP side of local-to-Locker. The vendored Hono app's
 * pipeline routes (`/v1/pipeline/chunks` = Technical Foul scrub-before-storage,
 * `/v1/pipeline/manifests`, `/v1/pipeline/events`, `/v1/pipeline/traces/:id`)
 * run on Node/Bun with Postgres deps instead of Cloudflare queues. Only the
 * pipeline routes are mounted: the rest of the Worker needs Neon.
 */
import { createServer } from "node:http";
import { Hono } from "hono";
import {
  createPipelineRoutes,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  TRACE_ID_HEADER,
  traceIdFromHeaders,
  type AppEnv,
  type Bindings,
  objectStoreManifestReader,
  type PipelineStage,
  type RawStore,
  type SqlExecutor,
  type StageMessage,
  missingReaders,
  postgresReadinessStore,
} from "../shared/vendor.ts";
import {
  inTransaction,
  openHoopDb,
  waitForHoopDb,
  waitForSchema,
  schemaVerdict,
} from "../shared/db.ts";
import { enrichMessage } from "../shared/enrich.ts";
import { lockerStore } from "../shared/locker.ts";
import { traceparentFor } from "../shared/ids.ts";
import { integer, optional, required, vendorBindings } from "../shared/env.ts";
import { liveDataPathVerdict } from "../shared/data-path.ts";
import {
  assertDestinationUrl,
  destinationPolicy,
} from "../shared/destination-url.ts";
import { healthVerdict, type WorkerHealth } from "../shared/worker.ts";
import { errorFields, log } from "../shared/log.ts";
import { initTelemetry, withSpan } from "../shared/telemetry.ts";
import { sqlPipelineEventSink } from "../hoopdb/sink.ts";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { jwksFromPrivateKey } from "./auth.ts";
import { allowedOrgIdsFromEnv } from "./org-allowlist.ts";
import {
  claimlessWindowDates,
  courierMigrationLedger,
  createCourierManifestRoutes,
  type ClaimlessUse,
} from "./manifests.ts";
import { mountIngestApi } from "./routes.ts";

/** Test seams for buildIngestApp: an already migrated database and a raw store. */
export interface IngestOverrides {
  db?: SqlExecutor & {
    transaction<T>(work: (tx: SqlExecutor) => Promise<T>): Promise<T>;
  };
  raw?: RawStore;
}

/**
 * The production composition of the AlleyOop ingest. Auth (no bypass):
 *  - With `RELAYHISTORY_ORIGIN` set (hosted and customer installs), RelayAuth's
 *    origin-bound History assertion is verified (RS256 against
 *    `RELAYAUTH_JWKS_URL`, exact issuer, sole audience = this origin, the
 *    dedicated token class) and nothing else; no flag is needed.
 *  - `ALLOW_INTERIM_RELAYAUTH_BEARER` is OFF unless the environment sets it to
 *    exactly "true" (an explicit dev/test opt-in; the local Docker stack sets
 *    it). Off, a generic RelayAuth bearer is refused (401), and with no origin
 *    every bearer is refused (401 auth_not_configured).
 *  - Migration assertions go through the dedicated verifier
 *    (`RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS=true`, default off), which
 *    admits only the migration class, under its full contract (signed pair,
 *    recomputed upload identity, chunk bounds, single-use ledger).
 *  - With `RELAYHISTORY_ORIGIN` set, the upload routes take only origin-bound
 *    assertions (or migration assertions), even with the interim bearer on.
 */
/** One row per day, org, caller and client version. Every use is a read. */
function recordClaimlessUse(db: SqlExecutor, use: ClaimlessUse) {
  return db
    .query(
      `INSERT INTO sessions.claimless_uses (day, org_id, surface, caller, client_version)
       VALUES (current_date, $1, 'status', $2, $3)
       ON CONFLICT (day, org_id, surface, caller, client_version)
       DO UPDATE SET uses = sessions.claimless_uses.uses + 1, last_at = now()`,
      [use.orgId, use.caller, use.clientVersion],
    )
    .then(() => undefined);
}

/** One row for the whole database, written once: later dates never replace it. */
async function firstClaimlessActivation(
  db: SqlExecutor,
  from: string,
): Promise<string> {
  const compatClass = "manifest_machine_claim_compat_v1";
  await db.query(
    `INSERT INTO sessions.claimless_window_activations (compat_class, first_activation)
     VALUES ($1, $2) ON CONFLICT (compat_class) DO NOTHING`,
    [compatClass, from],
  );
  const { rows } = await db.query<{ first_activation: string | Date }>(
    "SELECT first_activation FROM sessions.claimless_window_activations WHERE compat_class = $1",
    [compatClass],
  );
  return new Date(rows[0]!.first_activation).toISOString();
}

/**
 * Startup validation of the ingest's own destinations: the JWKS it trusts keys
 * from and the origin assertions must be bound to. Same transport rule as the
 * courier (src/shared/destination-url.ts, ALLOW_IN_CLUSTER_HTTP). The origin's
 * audience comparison stays exact; only which URLs may be named is checked.
 */
export function assertIngestDestinations(env: NodeJS.ProcessEnv = process.env) {
  const policy = destinationPolicy(env);
  const jwksUrl = env.RELAYAUTH_JWKS_URL?.trim();
  if (jwksUrl) assertDestinationUrl("RELAYAUTH_JWKS_URL", jwksUrl, policy);
  const origin = env.RELAYHISTORY_ORIGIN?.trim();
  if (origin) {
    const url = assertDestinationUrl("RELAYHISTORY_ORIGIN", origin, policy, {
      allowPath: false,
    });
    if (origin.replace(/\/$/, "").toLowerCase() !== url.origin)
      throw new Error("96bulls: RELAYHISTORY_ORIGIN must be an exact bare origin");
  }
}

export async function buildIngestApp(overrides: IngestOverrides = {}) {
  assertIngestDestinations();
  const port = integer("INGEST_PORT", 8080);
  const jwks = process.env.JWKS_JSON
    ? (JSON.parse(process.env.JWKS_JSON) as Record<string, unknown>)
    : process.env.JWT_PRIVATE_KEY_PEM
      ? jwksFromPrivateKey(process.env.JWT_PRIVATE_KEY_PEM, required("JWT_KID"))
      : undefined;
  const env = vendorBindings({
    // Explicit opt-in only (dev/tests); never defaulted on.
    ...(process.env.ALLOW_INTERIM_RELAYAUTH_BEARER === "true"
      ? { ALLOW_INTERIM_RELAYAUTH_BEARER: "true" }
      : {}),
    RELAYAUTH_ISSUER: optional("AUTH_ISSUER", "https://96bulls.local"),
    RELAYAUTH_AUDIENCE: optional("AUTH_AUDIENCE", "relayhistory"),
    RELAYAUTH_JWKS_URL: optional(
      "RELAYAUTH_JWKS_URL",
      `http://127.0.0.1:${port}/.well-known/jwks.json`,
    ),
    ...(process.env.PIPELINE_OPS_SECRET
      ? { PIPELINE_OPS_SECRET: process.env.PIPELINE_OPS_SECRET }
      : {}),
    // Hosted/customer installs on a shared RelayAuth issuer: set RELAYHISTORY_ORIGIN so only
    // assertions minted for this deployment's origin are accepted (legacy shared audience refused).
    ...(process.env.RELAYHISTORY_ORIGIN
      ? { RELAYHISTORY_ORIGIN: process.env.RELAYHISTORY_ORIGIN }
      : {}),
    ...(process.env.MANIFEST_MACHINE_CLAIM
      ? { MANIFEST_MACHINE_CLAIM: process.env.MANIFEST_MACHINE_CLAIM }
      : {}),
    ...(process.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM
      ? {
          MANIFEST_MACHINE_CLAIM_COMPAT_FROM:
            process.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM,
        }
      : {}),
    ...(process.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL
      ? {
          MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL:
            process.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL,
        }
      : {}),
    ...(process.env.BACKFILL_ACTIVATION_MAX_ATTEMPTS
      ? {
          BACKFILL_ACTIVATION_MAX_ATTEMPTS:
            process.env.BACKFILL_ACTIVATION_MAX_ATTEMPTS,
        }
      : {}),
    ...(process.env.BACKFILL_ACTIVATION_MAX_DELAY_SECONDS
      ? {
          BACKFILL_ACTIVATION_MAX_DELAY_SECONDS:
            process.env.BACKFILL_ACTIVATION_MAX_DELAY_SECONDS,
        }
      : {}),
    ...(process.env.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS
      ? {
          RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS:
            process.env.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS,
        }
      : {}),
    // RelayAuth migration assertions (relayauth#110): default off. Grants are
    // `grantId=planDigest` pairs (ids and public digests only, no secrets).
    ...(process.env.RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS
      ? {
          RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS:
            process.env.RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS,
        }
      : {}),
    ...(process.env.RELAYHISTORY_MIGRATION_GRANTS
      ? {
          RELAYHISTORY_MIGRATION_GRANTS:
            process.env.RELAYHISTORY_MIGRATION_GRANTS,
        }
      : {}),
    // The v2 pre-mint base read: its own default-off gate (issuer spec section 9).
    ...(process.env.RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ === "true"
      ? { RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ: "true" }
      : {}),
  }) as unknown as Bindings;
  if (!jwks && !process.env.RELAYAUTH_JWKS_URL)
    throw new Error(
      "96bulls: ingest needs JWKS_JSON, JWT_PRIVATE_KEY_PEM or RELAYAUTH_JWKS_URL",
    );

  let db: SqlExecutor;
  let transaction: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
  if (overrides.db) {
    db = overrides.db;
    transaction = overrides.db.transaction.bind(overrides.db);
  } else {
    const hoop = openHoopDb();
    await waitForHoopDb(hoop);
    await waitForSchema(hoop, await expectedSchema());
    db = hoop;
    transaction = (work) => inTransaction(hoop, work);
  }
  const raw = overrides.raw ?? lockerStore();
  const queue = postgresPipelineQueue(db);
  const reader = objectStoreManifestReader(raw.store, {
    provider: raw.provider,
    ...(raw.bucket ? { bucket: raw.bucket } : {}),
  });
  const sink = sqlPipelineEventSink(db);
  const deadLetters = postgresDeadLetterStore(db);
  const send = async (
    _env: Bindings,
    stage: PipelineStage,
    message: StageMessage,
  ) =>
    withSpan(
      "enqueue",
      {
        traceparent: traceparentFor(message.ctx.traceId),
        attrs: { stage, trace_id: message.ctx.traceId },
      },
      async () => {
        await queue.send(await enrichMessage({ ...message, stage }, reader));
      },
    );
  // Migration commits: the ledger row, enrichMessage and the pipeline_jobs
  // insert in ONE transaction (see manifests.ts).
  const migrationLedger = courierMigrationLedger(
    { query: db.query.bind(db), transaction },
    reader,
    (stage, message, work) =>
      withSpan(
        "enqueue",
        {
          traceparent: traceparentFor(message.ctx.traceId),
          attrs: { stage, trace_id: message.ctx.traceId },
        },
        work,
      ),
  );
  const pipeline = createPipelineRoutes({
    manifests: () => reader,
    rawStore: () => raw,
    sink: () => sink,
    deadLetters: () => deadLetters,
    send,
    migrationLedger: () => migrationLedger,
    // The courier route verifies a v2 (backfill) commit against the stored base.
    migrationBackfill: true,
  });
  const readiness = postgresReadinessStore(db);
  // Activation starts at the first start with a window set, used or not.
  const window = claimlessWindowDates(env);
  if (window)
    await firstClaimlessActivation(db, window.from).catch((err) =>
      log.warn("claimless window activation not recorded", errorFields(err)),
    );
  const courierManifests = createCourierManifestRoutes({
    manifests: reader,
    raw,
    send,
    migrationLedger,
    missingReaders: () =>
      missingReaders({
        key: process.env.MANIFEST_READINESS_KEY,
        store: readiness,
      }),
    recordClaimless: (use) => recordClaimlessUse(db, use),
    firstClaimlessActivation: (from) => firstClaimlessActivation(db, from),
    requeueBackfill: (_env, message, delaySeconds) =>
      queue.send(message, { delaySeconds }),
  });

  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    const correlationId = c.req.header("X-Correlation-Id")?.trim() || requestId;
    const traceId = traceIdFromHeaders((name) => c.req.header(name));
    c.set("requestId", requestId);
    c.set("correlationId", correlationId);
    if (traceId) c.set("traceId", traceId);
    await next();
    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", correlationId);
    if (traceId) c.header(TRACE_ID_HEADER, traceId);
  });
  // One allowlisted request line per request (method, route status, duration; never bodies or query values).
  app.use("*", async (c, next) => {
    const started = Date.now();
    await next();
    log.info("request completed", {
      status: c.res.status,
      duration_ms: Date.now() - started,
      trace_id: c.get("traceId"),
    });
  });
  if (jwks) app.get("/.well-known/jwks.json", (c) => c.json(jwks));
  app.get("/health", (c) => c.json({ ok: true, service: "alleyoop-ingest" }));
  // Liveness: same rule as the workers. A background SELECT 1 every 10s; after
  // WORKER_UNHEALTHY_AFTER_FAILURES consecutive failures (e.g. a rotated HoopDb
  // password once sessions reconnect) /healthz turns 503 so the orchestrator restarts us.
  const health: WorkerHealth = {
    ready: true,
    lastTickAt: Date.now(),
    consecutiveDbFailures: 0,
  };
  const maxFailures = integer("WORKER_UNHEALTHY_AFTER_FAILURES", 5);
  const maxTickAgeMs = integer("WORKER_MAX_TICK_AGE_MS", 120_000);
  // One probe at a time, so results apply in order (a late success can't mask newer failures).
  let probing = false;
  setInterval(() => {
    if (probing) return;
    probing = true;
    db.query("SELECT 1")
      .then(() => (health.consecutiveDbFailures = 0))
      .catch(() => (health.consecutiveDbFailures += 1))
      .finally(() => {
        health.lastTickAt = Date.now();
        probing = false;
      });
  }, 10_000).unref();
  app.get("/healthz", (c) => {
    const v = healthVerdict(health, Date.now(), { maxFailures, maxTickAgeMs });
    return c.json(v, v.ok ? 200 : 503);
  });
  // Ready = the schema this image was built for is fully migrated, the Worker role
  // validation marker is PASS for this image's generation, AND phase 2 passes live on
  // this probe (read-only, bounded, uncached), not just "DB answers".
  const expected = await expectedSchema();
  app.get("/readyz", async (c) => {
    try {
      const schema = await schemaVerdict(expected, transaction);
      if (!schema.ok) return c.json(schema, 503);
      const data = await liveDataPathVerdict(process.env);
      if (!data.ok) return c.json({ ok: false, missing: data.missing }, 503);
      return c.json({ ok: true });
    } catch {
      return c.json({ ok: false, reason: "hoopdb_unreachable" }, 503);
    }
  });
  // The v2 base read's default-off gate (issuer spec section 9): with
  // RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ off, everything under
  // /v1/pipeline/manifests/base is exactly 404, for every method and path
  // variant, before any auth (and a base-read capability does not verify).
  app.use("*", async (c, next) => {
    const path = c.req.path;
    const base = "/v1/pipeline/manifests/base";
    if (
      env.RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ !== "true" &&
      (path === base || path.startsWith(`${base}/`))
    )
      return c.json({ error: { code: "not_found", message: "no route" } }, 404);
    await next();
  });
  // Authentication, the tenant org allowlist, the origin rule and the live-write machine
  // gate, in that order, before any /v1 route (routes.ts; tested route by route).
  mountIngestApi(app, { courierManifests, pipeline }, new Hono<AppEnv>(), {
    relayhistoryOrigin: env.RELAYHISTORY_ORIGIN,
    allowedOrgIds: allowedOrgIdsFromEnv(process.env.TENANT_ORG_IDS),
  });
  app.notFound((c) =>
    c.json({ error: { code: "not_found", message: "no route" } }, 404),
  );
  app.onError((err, c) => {
    log.error("request failed", errorFields(err));
    if (process.env.DEBUG === "1") console.error(err);
    return c.json(
      { error: { code: "internal_error", message: "Internal server error" } },
      500,
    );
  });
  return { app, env, port, db };
}

export async function serve() {
  process.env.OTEL_SERVICE_NAME ||= "alleyoop-ingest";
  initTelemetry();
  const { app, env, port } = await buildIngestApp();
  const executionContext = {
    waitUntil: (p: Promise<unknown>) => void p.catch(() => {}),
    passThroughOnException: () => {},
    props: {},
  };
  const server = createServer((req, res) => {
    void (async () => {
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(chunk as Buffer);
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined) continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        const url = `http://${req.headers.host ?? "127.0.0.1"}${req.url ?? "/"}`;
        const body = chunks.length ? Buffer.concat(chunks) : undefined;
        const request = new Request(url, {
          method: req.method,
          headers,
          body:
            req.method === "GET" || req.method === "HEAD" ? undefined : body,
        });
        const response = await app.fetch(
          request,
          env,
          executionContext as unknown as ExecutionContext,
        );
        const out: Record<string, string> = {};
        response.headers.forEach((v, k) => (out[k] = v));
        res.writeHead(response.status, out);
        if (response.body)
          for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>)
            res.write(Buffer.from(chunk));
        res.end();
      } catch (err) {
        log.error("request crashed", errorFields(err));
        if (!res.headersSent)
          res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { code: "internal_error" } }));
      }
    })();
  });
  await new Promise<void>((resolve) => server.listen(port, "0.0.0.0", resolve));
  log.info("alleyoop ingest listening", { port });
  const stop = () => server.close(() => process.exit(0));
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
