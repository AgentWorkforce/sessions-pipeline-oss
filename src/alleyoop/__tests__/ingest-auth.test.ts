/**
 * The REAL production composition, buildIngestApp, end to end: auth defaults,
 * the dedicated migration verifier, and the full migration-assertion contract
 * (signed pair, recomputed upload identity, chunk bounds, single-use ledger).
 * Only the database (local PGlite with every migration) and the raw store
 * (a temp fs store) are substituted. Synthetic ids and a throwaway key only;
 * nothing credential-like is written to disk.
 */
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import {
  createHash,
  createSign,
  generateKeyPairSync,
  randomBytes,
} from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  backfillSha256,
  baseReadIdentityOf,
  canonicalJson,
  planEntryV2Sha256,
  uploadIdentityOf,
  uploadIdentityV2Of,
  type BackfillBase,
  type PlanEntry,
  type PlanEntryV2,
  type UploadIdentity,
  type UploadIdentityBaseRead,
  type UploadIdentityV2,
} from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  computeContentSha256,
  manifestKey,
  pipelineQueueDeps,
  pollPostgresStage,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  sealRawChunk,
  type Bindings,
  type SessionLocation,
  type SqlExecutor,
  attestReader,
  MANIFEST_READERS,
  postgresReadinessStore,
  sessionPrefix,
} from "../../shared/vendor.ts";
import { sqlPipelineEventSink } from "../../hoopdb/sink.ts";
import { buildIngestApp } from "../ingest.ts";
import { expectedMigrationNames, generation } from "../../hoopdb/migrate.ts";
import { mintToken } from "../auth.ts";
import { runCourierOnce, type CourierConfig } from "../courier.ts";
import { mintTenantToken } from "../../proof/aws/ingest.ts";
import { DatabaseSync } from "node:sqlite";
import { writeFile } from "node:fs/promises";
import { indexerReconcileRoot } from "../../hoopdb/reconcile-root.ts";
import {
  objectKey,
  prefixAllowed,
  reconcileManifests,
  objectStoreManifestReader,
  orgResolverFromEnv,
} from "../../shared/vendor.ts";

const ORIGIN = "https://history.example.test";
const ISSUER = "https://relayauth.example.test";
const JWKS_URL = "https://relayauth.example.test/.well-known/jwks.json";
const KID = "ingest-auth-test";
const ORG = "org_ingest";
const WORKSPACE = "ws_ingest";
const USER = "user_ingest_a";
const PLAN = "a".repeat(64);
const GRANT = `mgr_${"1".repeat(32)}`;
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const AT = "2026-10-29T10:00:00.000Z";
const SOURCE = new TextEncoder().encode('{"type":"user","text":"hello"}\n');
const LOCATION: SessionLocation = {
  workspaceId: WORKSPACE,
  machineId: "machine-a",
  harness: "claude",
  sessionId: "session-a",
  startDate: "2026-10-29",
};
const DEFAULT_LOCATION = LOCATION;
const ROOT_DIR = join(import.meta.dir, "../../..");

const { privateKey, publicKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
});
const JWKS = {
  keys: [{ ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256" }],
};

function sign(claims: Record<string, unknown>) {
  for (const [k, v] of Object.entries(claims))
    if (v === undefined) delete claims[k];
  const b64 = (v: unknown) =>
    Buffer.from(JSON.stringify(v)).toString("base64url");
  const head = b64({ alg: "RS256", kid: KID, typ: "JWT" });
  const body = b64(claims);
  return `${head}.${body}.${createSign("RSA-SHA256").update(`${head}.${body}`).end().sign(privateKey, "base64url")}`;
}

const now = () => Math.floor(Date.now() / 1000);

function migrationToken(
  upload: UploadIdentity | UploadIdentityV2,
  overrides: Record<string, unknown> = {},
  meta: Record<string, unknown> = {},
) {
  return sign({
    sub: "agent_relayhistory_migration",
    org: ORG,
    wks: WORKSPACE,
    scopes: ["rth:sync"],
    sponsorId: USER,
    sponsorChain: [USER, "agent_relayhistory_migration"],
    token_type: "access",
    iss: ISSUER,
    aud: [ORIGIN],
    iat: now(),
    exp: now() + 60,
    jti: `tok_${randomBytes(16).toString("hex")}`,
    ...overrides,
    meta: {
      tokenClass: "migration_assertion",
      grantId: GRANT,
      planDigest: PLAN,
      nonce: randomBytes(24).toString("base64url"),
      sourceUserId: USER,
      actorId: "actor",
      actorOrgId: ORG,
      workspaceId: WORKSPACE,
      upload,
      ...meta,
    },
  });
}

/** An origin-bound courier assertion (live delivery), as relayauth mints for this History. */
const liveToken = (machineId = LOCATION.machineId) =>
  sign({
    sub: "agent_relayhistory_assertion",
    org: ORG,
    wks: WORKSPACE,
    sponsorId: USER,
    scopes: ["rth:sync"],
    token_type: "access",
    iss: ISSUER,
    aud: [ORIGIN],
    iat: now(),
    exp: now() + 600,
    meta: { machineId, tokenClass: "relayhistory_assertion" },
  });

/** The v2 base-read capability (issuer spec section 3): its own class, scope, audience, TTL, meta.read and jti namespace. */
function baseReadToken(
  read: UploadIdentityBaseRead,
  overrides: Record<string, unknown> = {},
  meta: Record<string, unknown> = {},
) {
  return sign({
    sub: "agent_relayhistory_migration_base_read",
    org: ORG,
    wks: WORKSPACE,
    scopes: ["rth:migration_base_read"],
    sponsorId: USER,
    sponsorChain: [USER, "agent_relayhistory_migration_base_read"],
    token_type: "access",
    iss: ISSUER,
    aud: [`${ORIGIN}/v1/pipeline/manifests/base`],
    iat: now(),
    exp: now() + 30,
    jti: `mbr_${randomBytes(16).toString("hex")}`,
    ...overrides,
    meta: {
      tokenClass: "migration_base_read",
      grantId: GRANT,
      planDigest: PLAN,
      nonce: randomBytes(24).toString("base64url"),
      sourceUserId: USER,
      actorId: "actor",
      actorOrgId: ORG,
      workspaceId: WORKSPACE,
      read,
      ...meta,
    },
  });
}

/** A generic RelayAuth bearer: a valid JWT of no assertion class (like a relay_pa_ token's JWT). */
const genericToken = (aud: string[] = ["relayhistory"]) =>
  sign({
    sub: USER,
    org: ORG,
    wks: WORKSPACE,
    sponsorId: USER,
    scopes: ["rth:sync", "rth:read"],
    token_type: "access",
    iss: ISSUER,
    aud,
    iat: now(),
    exp: now() + 600,
  });

let db: PGlite;
let root: string;
const savedEnv = { ...process.env };
const realFetch = globalThis.fetch;

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(
    await readFile(join(ROOT_DIR, "supabase/tests/supabase_shim.sql"), "utf8"),
  );
  await db.exec(
    await readFile(
      join(ROOT_DIR, "supabase/bootstrap/session_directory_search_worker.sql"),
      "utf8",
    ),
  );
  await db.exec(
    await readFile(
      join(ROOT_DIR, "supabase/bootstrap/session_token_verifier.sql"),
      "utf8",
    ),
  );
  await db.exec(
    await readFile(
      join(ROOT_DIR, "supabase/bootstrap/session_ingest_roles.sql"),
      "utf8",
    ),
  );
  for (const dir of ["supabase/migrations", "src/hoopdb/migrations"])
    for (const file of (await readdir(join(ROOT_DIR, dir)))
      .filter((f) => f.endsWith(".sql"))
      .sort())
      await db.exec(await readFile(join(ROOT_DIR, dir, file), "utf8"));
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) =>
    String(input) === JWKS_URL
      ? Response.json(JWKS)
      : realFetch(input, init)) as typeof fetch;
}, 120_000);
afterAll(async () => {
  globalThis.fetch = realFetch;
  await db?.close();
});
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-ingest-auth-"));
  // The one activation is append-only by design; each test needs a fresh database's worth.
  await db.exec(`
    ALTER TABLE sessions.claimless_window_activations DISABLE TRIGGER USER;
    DELETE FROM sessions.claimless_window_activations;
    ALTER TABLE sessions.claimless_window_activations ENABLE TRIGGER USER;
    DELETE FROM sessions.claimless_uses;
  `);
  await db.exec(
    "TRUNCATE sessions.migration_assertion_uses; TRUNCATE sessions.migration_assertion_chunks; TRUNCATE sessions.migration_base_read_uses; TRUNCATE sessions.pipeline_jobs",
  );
});
afterEach(async () => {
  for (const k of Object.keys(process.env))
    if (!(k in savedEnv)) delete process.env[k];
  Object.assign(process.env, savedEnv);
  await rm(root, { recursive: true, force: true });
});

const pgExecutor = () => {
  const wrap = (q: { query: PGlite["query"] }): SqlExecutor => ({
    query: async (text, params) => ({
      rows: (await q.query(text, params)).rows as never[],
    }),
  });
  return {
    ...wrap(db),
    transaction: <T>(work: (tx: SqlExecutor) => Promise<T>) =>
      db.transaction((tx) => work(wrap(tx))),
  };
};

const count = async (table: string) =>
  (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`))
    .rows[0]!.n;

/** buildIngestApp under an environment (unset by default: the production defaults). */
async function ingest(
  extra: Record<string, string> = {},
  opts: {
    wrapStore?: (
      store: ReturnType<typeof createFsStore>,
    ) => ReturnType<typeof createFsStore>;
  } = {},
) {
  Object.assign(process.env, {
    LOCKER_BUCKET: "raw",
    TENANT_ORG_ID: ORG,
    AUTH_ISSUER: ISSUER,
    RELAYAUTH_JWKS_URL: JWKS_URL,
    RELAYHISTORY_ORIGIN: ORIGIN,
    ...extra,
  });
  delete process.env.ALLOW_INTERIM_RELAYAUTH_BEARER;
  if (extra.ALLOW_INTERIM_RELAYAUTH_BEARER)
    process.env.ALLOW_INTERIM_RELAYAUTH_BEARER =
      extra.ALLOW_INTERIM_RELAYAUTH_BEARER;
  const store = (opts.wrapStore ?? ((s) => s))(createFsStore({ root }));
  // Every reader attests: the gate has its own tests, these are about the ingest.
  process.env.MANIFEST_READINESS_KEY = "test-readiness-key";
  for (const reader of MANIFEST_READERS)
    await attestReader({
      reader,
      key: "test-readiness-key",
      store: postgresReadinessStore(pgExecutor()),
      selfCheck: async () => {},
      releaseSha: "abc123",
    });
  const { app, env } = await buildIngestApp({
    db: pgExecutor(),
    raw: { store, provider: "fs", bucket: "raw" } as never,
  });
  const req = (path: string, token: string, init: RequestInit = {}) =>
    app.request(
      path,
      {
        ...init,
        headers: {
          authorization: `Bearer ${token}`,
          ...(init.headers as Record<string, string>),
        },
      },
      env,
    );
  /** A chunk request; a migration one carries its plan entry text in front of the bytes. */
  const chunk = (
    token: string,
    location: SessionLocation = LOCATION,
    opts: {
      entry?: string;
      seq?: number;
      bytes?: Uint8Array;
      epoch?: number;
      offset?: number;
      format?: string;
    } = {},
  ) => {
    const bytes = opts.bytes ?? SOURCE;
    const entry =
      opts.entry === undefined
        ? undefined
        : new TextEncoder().encode(opts.entry);
    const body = entry ? Buffer.concat([entry, bytes]) : bytes;
    return req(
      `/v1/pipeline/chunks?machine=${location.machineId}&harness=${location.harness}&session=${location.sessionId}&date=${location.startDate}&seq=${opts.seq ?? 0}&epoch=${opts.epoch ?? 0}&offset=${opts.offset ?? 0}&format=${opts.format ?? "jsonl"}`,
      token,
      {
        method: "POST",
        headers: {
          "x-relay-trace-id": TRACE,
          ...(entry
            ? {
                "x-relay-migration-plan-entry-length": String(entry.byteLength),
              }
            : {}),
        },
        body,
      },
    );
  };
  const commit = (
    token: string,
    manifest: Record<string, unknown>,
    key = manifestKey(LOCATION),
  ) =>
    req("/v1/pipeline/manifests", token, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": (manifest.session as { machine_id: string }).machine_id,
        "x-session-id": (manifest.session as { session_id: string }).session_id,
      },
      body: JSON.stringify({ key, manifest }),
    });
  return { req, chunk, commit, app, env, store };
}

/** The planned upload: its chunk entry (as the server seals it), manifest and signed identity. */
async function planned(
  LOCATION: SessionLocation = DEFAULT_LOCATION,
  owner = `migration:${GRANT}`,
) {
  const scratch = await mkdtemp(join(tmpdir(), "96bulls-plan-"));
  try {
    const sealed = await sealRawChunk(createFsStore({ root: scratch }), {
      location: LOCATION,
      format: "jsonl",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source: SOURCE,
      traceId: TRACE,
    });
    const manifest = {
      schema_v: 1,
      layout: "v1",
      trace_id: TRACE,
      revision: 1,
      created_at: AT,
      updated_at: AT,
      session: {
        workspace_id: WORKSPACE,
        machine_id: LOCATION.machineId,
        harness: LOCATION.harness,
        session_id: LOCATION.sessionId,
        started_at: AT,
      },
      // A cold export is owned by its migration grant (#42's owner fence); a live session by its machine.
      writer: {
        delivery_instance_id: owner,
        plugin_version: "96bulls-courier/0.3.0",
      },
      raw: {
        owner,
        state: "closed",
        updated_at: AT,
        format: "jsonl",
        compression: "zstd",
        chunk_count: 1,
        raw_bytes: sealed.entry.bytes,
        stored_bytes: sealed.entry.stored_bytes,
        content_sha256: await computeContentSha256([sealed.entry]),
        epoch: 0,
        epochs: [
          {
            epoch: 0,
            opened_at: AT,
            reason: "initial",
            first_seq: 0,
            opened_by: owner,
          },
        ],
        next_offset: sealed.entry.bytes,
        chunks: [sealed.entry],
        scrub: sealed.scrub,
      },
      // The export's source fields, as the courier writes them; the ingest
      // requires each to equal the signed values.
      extensions: {
        "96bulls": {
          session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19",
          session_uuid_source: "minted",
          tenant_id: ORG,
          user_id: USER,
        },
        "client.relayhistory_export": {
          version: 1,
          source: "neon.delivery_records",
          exported_at: AT,
          grant_id: GRANT,
          plan_digest: PLAN,
          source_user_id: USER,
        },
      },
    };
    const chunks = [
      {
        seq: 0,
        epoch: 0,
        start_offset: 0,
        bytes: SOURCE.byteLength,
        sha256: createHash("sha256").update(SOURCE).digest("hex"),
        records: sealed.entry.records ?? 0,
      },
    ];
    const entry: PlanEntry = {
      grant_id: GRANT,
      key: manifestKey(LOCATION),
      org_id: ORG,
      workspace_id: WORKSPACE,
      source_user_id: USER,
      session: {
        machine_id: LOCATION.machineId,
        harness: LOCATION.harness,
        session_id: LOCATION.sessionId,
        started_at: AT,
      },
      state: "closed",
      chunks,
      content_sha256: await computeContentSha256(chunks),
      raw_bytes: SOURCE.byteLength,
    };
    return {
      manifest,
      entryText: canonicalJson(entry),
      upload: await uploadIdentityOf(entry, PLAN),
    };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

async function code(response: Response) {
  return ((await response.json()) as { error?: { code?: string } }).error?.code;
}

const MIGRATION_ON = {
  RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS: "true",
  RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${PLAN}`,
};

describe("buildIngestApp auth (production composition)", () => {
  test("/readyz is 503 until the Worker role validation is PASS for this image's generation", async () => {
    const { app, env } = await ingest();
    // This file applies the migrations directly, so record the ledger the migrator
    // would have written: the schema gate passes and only the marker is under test.
    await db.exec(`CREATE TABLE sessions.__96bulls_migrations (
      name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`);
    for (const name of await expectedMigrationNames())
      await db.query(
        "INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ($1, 'x')",
        [name],
      );
    const ready = async () => {
      const r = await app.request("/readyz", {}, env);
      return {
        status: r.status,
        body: (await r.json()) as { reason?: string },
      };
    };
    // No marker table at all (no migrator ran here): not ready.
    expect(await ready()).toEqual({
      status: 503,
      body: { ok: false, reason: "validation_pending" } as never,
    });
    await db.exec(`CREATE TABLE sessions.__96bulls_role_validation (
      name text PRIMARY KEY, generation text NOT NULL,
      status text NOT NULL CHECK (status IN ('pending', 'pass')),
      updated_at timestamptz NOT NULL DEFAULT now())`);
    try {
      const gen = await generation();
      for (const [g, status] of [
        [gen, "pending"],
        ["0".repeat(64), "pass"],
      ] as const) {
        await db.query(
          `INSERT INTO sessions.__96bulls_role_validation (name, generation, status)
           VALUES ('session_directory_search_worker', $1, $2)
           ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, status = EXCLUDED.status`,
          [g, status],
        );
        expect((await ready()).body.reason).toBe("validation_pending");
      }
      // What the migrator does in its validation transaction (this file applies the
      // migrations directly): no sessions function stays PUBLIC-executable.
      await db.exec(
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC",
      );
      await db.query(
        "UPDATE sessions.__96bulls_role_validation SET generation = $1, status = 'pass'",
        [gen],
      );
      // Past the schema gate: whatever /readyz says next, it is not the marker or phase 2.
      expect((await ready()).body.reason).not.toBe("validation_pending");
      expect((await ready()).body.reason).not.toBe("role_validation_failed");
      // Post-PASS drift with no migrator: the live gate closes /readyz on the next probe.
      await db.exec(
        "GRANT SELECT ON sessions.session_index TO session_directory_search_worker",
      );
      try {
        expect(await ready()).toMatchObject({
          status: 503,
          body: { reason: "role_validation_failed", error_code: "55000" },
        });
      } finally {
        await db.exec(
          "REVOKE SELECT ON sessions.session_index FROM session_directory_search_worker",
        );
      }
      expect((await ready()).body.reason).not.toBe("role_validation_failed");
    } finally {
      await db.exec(
        "DROP TABLE sessions.__96bulls_role_validation; DROP TABLE sessions.__96bulls_migrations",
      );
    }
  });

  test("PROOF MINT + REAL ROUTE: RelayAuth request, signed machine claim and zero-side-effect refusals agree", async () => {
    let request:
      | { url: string; body: Record<string, unknown>; key: string | null }
      | undefined;
    const token = await mintTenantToken({
      relayauthUrl: "https://relayauth.example.test",
      bootstrapKey: "synthetic-proof-key",
      orgId: ORG,
      workspaceId: WORKSPACE,
      machineId: LOCATION.machineId,
      sponsorId: USER,
      historyOrigin: ORIGIN,
      fetch: async (url, init) => {
        const body = JSON.parse(String(init?.body)) as { machineId: string };
        request = {
          url,
          body,
          key: new Headers(init?.headers).get("x-api-key"),
        };
        return Response.json(
          { accessToken: liveToken(body.machineId) },
          { status: 201 },
        );
      },
    });
    expect(request).toEqual({
      url: "https://relayauth.example.test/v1/tokens/relayhistory-assertion",
      key: "synthetic-proof-key",
      body: {
        orgId: ORG,
        workspaceId: WORKSPACE,
        machineId: LOCATION.machineId,
        sponsorId: USER,
        scopes: ["rth:sync", "rth:read"],
        expiresIn: 60,
        historyOrigin: ORIGIN,
      },
    });
    const claims = JSON.parse(
      Buffer.from(token.split(".")[1]!, "base64url").toString("utf8"),
    ) as { meta?: { machineId?: string } };
    expect(claims.meta?.machineId).toBe(LOCATION.machineId);

    const i = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const objects = async (location: SessionLocation) =>
      (await i.store.list(objectKey(sessionPrefix(location)))).items.length;
    expect((await i.chunk(token, LOCATION)).status).toBe(201);
    expect(await objects(LOCATION)).toBe(1);

    const other = {
      ...LOCATION,
      machineId: "machine-b",
      sessionId: "session-b",
    };
    const claimless = sign({
      sub: "agent_relayhistory_assertion",
      org: ORG,
      wks: WORKSPACE,
      sponsorId: USER,
      scopes: ["rth:sync"],
      token_type: "access",
      iss: ISSUER,
      aud: [ORIGIN],
      iat: now(),
      exp: now() + 600,
      meta: { tokenClass: "relayhistory_assertion" },
    });
    for (const denied of [claimless, token]) {
      const response = await i.chunk(denied, other);
      expect(response.status).toBe(403);
      expect(await code(response)).toBe("machine_claim_refused");
    }
    expect(await objects(other)).toBe(0);
    for (const table of [
      "sessions.pipeline_jobs",
      "sessions.claimless_uses",
      "sessions.migration_assertion_uses",
    ])
      expect(await count(table)).toBe(0);
  });

  test("REAL ROUTE: a live chunk POST without a matching machine claim is refused and writes nothing", async () => {
    const i = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const claimless = sign({
      sub: "agent_relayhistory_assertion",
      org: ORG,
      wks: WORKSPACE,
      sponsorId: USER,
      scopes: ["rth:sync"],
      token_type: "access",
      iss: ISSUER,
      aud: [ORIGIN],
      iat: now(),
      exp: now() + 600,
      meta: { tokenClass: "relayhistory_assertion" },
    });
    const objects = async () => {
      const prefix = objectKey(sessionPrefix(LOCATION));
      return (await i.store.list(prefix)).items.length;
    };
    for (const [name, token] of [
      ["claimless", claimless],
      ["mismatched", liveToken("another-machine")],
    ] as const) {
      const res = await i.chunk(token, LOCATION);
      expect(`${name}: ${res.status}`).toBe(`${name}: 403`);
      expect(
        ((await res.json()) as { error: { code: string } }).error.code,
      ).toBe("machine_claim_refused");
      expect(await objects()).toBe(0);
    }
    for (const table of [
      "sessions.pipeline_jobs",
      "sessions.claimless_uses",
      "sessions.migration_assertion_uses",
    ])
      expect(await count(table)).toBe(0);

    // Nor with the legacy window open: it is read-only.
    process.env.MANIFEST_MACHINE_CLAIM = "compat";
    process.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
      Date.now() - 1000,
    ).toISOString();
    process.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL = new Date(
      Date.now() + 60_000,
    ).toISOString();
    const open = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    expect((await open.chunk(claimless, LOCATION)).status).toBe(403);
    expect(await objects()).toBe(0);

    expect((await i.chunk(liveToken(), LOCATION)).status).toBe(201);
    expect(await objects()).toBe(1);
  });

  test("REAL ROUTE: a live event report is bound to the claimed machine", async () => {
    const i = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const post = (token: string, machineId: unknown) =>
      i.req("/v1/pipeline/events", token, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ machineId, events: [] }),
      });
    const claimless = sign({
      sub: "agent_relayhistory_assertion",
      org: ORG,
      wks: WORKSPACE,
      sponsorId: USER,
      scopes: ["rth:sync"],
      token_type: "access",
      iss: ISSUER,
      aud: [ORIGIN],
      iat: now(),
      exp: now() + 600,
      meta: { tokenClass: "relayhistory_assertion" },
    });
    const before = await count("sessions.pipeline_events");
    expect((await post(claimless, LOCATION.machineId)).status).toBe(403);
    expect((await post(liveToken(), "another-machine")).status).toBe(403);
    expect(await count("sessions.pipeline_events")).toBe(before);
    // Past the gate, the route's own validation answers.
    expect((await post(liveToken(), LOCATION.machineId)).status).toBe(400);
  });

  test("REAL ROUTE: a mutating path nobody listed is denied without a claim", async () => {
    const i = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const claimless = sign({
      sub: "agent_relayhistory_assertion",
      org: ORG,
      wks: WORKSPACE,
      sponsorId: USER,
      scopes: ["rth:sync"],
      token_type: "access",
      iss: ISSUER,
      aud: [ORIGIN],
      iat: now(),
      exp: now() + 600,
      meta: { tokenClass: "relayhistory_assertion" },
    });
    for (const method of ["POST", "PUT", "PATCH", "DELETE"]) {
      const res = await i.req("/v1/pipeline/some-new-route", claimless, {
        method,
      });
      expect(`${method}: ${res.status}`).toBe(`${method}: 403`);
    }
  });

  test("a locally minted token delivers with the window shut: local dev needs no compat", async () => {
    // As alchemy.run.ts composes a local stage: no origin, the interim bearer on.
    const i = await ingest({
      ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
      RELAYHISTORY_ORIGIN: "",
    });
    const keys = {
      kid: KID,
      privateKeyPem: String(
        privateKey.export({ type: "pkcs8", format: "pem" }),
      ),
    };
    const local = { ...LOCATION, sessionId: "session-local" };
    const mint = (machineId?: string) =>
      mintToken(keys, {
        issuer: ISSUER,
        audience: "relayhistory",
        orgId: ORG,
        workspaceId: WORKSPACE,
        userId: USER,
        ...(machineId ? { machineId } : {}),
      });
    const body = await planned(local, local.machineId);
    const post = (token: string) =>
      i.req("/v1/pipeline/manifests", token, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": local.machineId,
          "x-session-id": local.sessionId,
        },
        body: JSON.stringify({
          key: manifestKey(local),
          manifest: { ...body.manifest, extensions: undefined },
        }),
      });
    expect((await i.chunk(mint(local.machineId), local)).status).toBe(201);
    expect((await post(mint())).status).toBe(403);
    expect((await post(mint("another-machine"))).status).toBe(403);
    expect((await post(mint(local.machineId))).status).toBe(202);
    expect(await count("sessions.claimless_uses")).toBe(0);
  });

  test("the claim switch reaches the route from the real environment, and defaults shut", async () => {
    const shut = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const { manifest } = await planned();
    const claimless = sign({
      sub: "agent_relayhistory_assertion",
      org: ORG,
      wks: WORKSPACE,
      sponsorId: USER,
      scopes: ["rth:sync"],
      token_type: "access",
      iss: ISSUER,
      aud: [ORIGIN],
      iat: now(),
      exp: now() + 600,
      meta: { tokenClass: "relayhistory_assertion" },
    });
    expect((await shut.commit(claimless, manifest)).status).toBe(403);
    expect(await count("sessions.claimless_uses")).toBe(0);

    // The real migrations, composition and meter: a read through an open window.
    process.env.MANIFEST_MACHINE_CLAIM = "compat";
    process.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
      Date.now() - 1000,
    ).toISOString();
    process.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL = new Date(
      Date.now() + 60_000,
    ).toISOString();
    const open = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const liveLocation = { ...LOCATION, sessionId: "session-claimless" };
    const liveKey = manifestKey(liveLocation);
    const live = await planned(liveLocation, liveLocation.machineId);
    expect((await open.chunk(liveToken(), liveLocation)).status).toBe(201);
    const committed = await open.req("/v1/pipeline/manifests", liveToken(), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": liveLocation.machineId,
        "x-session-id": liveLocation.sessionId,
      },
      body: JSON.stringify({
        key: liveKey,
        manifest: { ...live.manifest, extensions: undefined },
      }),
    });
    expect(committed.status).toBe(202);
    const q = new URLSearchParams({
      key: liveKey,
      machine: liveLocation.machineId,
      writer: liveLocation.machineId,
    });
    expect(
      (await open.req(`/v1/pipeline/manifests/status?${q}`, claimless)).status,
    ).toBe(200);
    expect(await count("sessions.claimless_uses")).toBe(1);

    // One row for the whole database, append-only, on the real table: a restart
    // with restated dates finds the window shut, and so does a new label.
    const recorded = await db.query<{
      first_activation: Date;
      compat_class: string;
    }>(
      "SELECT first_activation, compat_class FROM sessions.claimless_window_activations",
    );
    expect(recorded.rows).toHaveLength(1);
    expect(recorded.rows[0]!.compat_class).toBe(
      "manifest_machine_claim_compat_v1",
    );
    const openFrom = process.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM;
    process.env.DEPLOYMENT_ID = "a-new-stage";
    const relabelled = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    expect(
      (await relabelled.req(`/v1/pipeline/manifests/status?${q}`, claimless))
        .status,
    ).toBe(200);
    process.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
      Date.now() + 1000 - 2000,
    ).toISOString();
    const restated = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    expect(
      (await restated.req(`/v1/pipeline/manifests/status?${q}`, claimless))
        .status,
    ).toBe(403);
    expect(
      (await db.query("SELECT 1 FROM sessions.claimless_window_activations"))
        .rows,
    ).toHaveLength(1);
    expect(openFrom).toBeDefined();
    await expect(
      db.query(
        "UPDATE sessions.claimless_window_activations SET first_activation = now()",
      ),
    ).rejects.toThrow(/append-only/);
    await expect(
      db.query("DELETE FROM sessions.claimless_window_activations"),
    ).rejects.toThrow(/append-only/);
    await expect(
      db.query("TRUNCATE sessions.claimless_window_activations"),
    ).rejects.toThrow(/append-only/);
    expect(
      (await db.query("SELECT 1 FROM sessions.claimless_window_activations"))
        .rows,
    ).toHaveLength(1);
  });

  test("default config: a generic RelayAuth bearer is refused (401) on every upload route, and so is a migration token", async () => {
    const { upload, manifest } = await planned();
    const i = await ingest();
    for (const token of [
      genericToken(),
      genericToken([ORIGIN]),
      migrationToken(upload),
    ]) {
      for (const response of [
        await i.chunk(token),
        await i.commit(token, manifest),
      ])
        expect(response.status).toBe(401);
    }
  });

  test("with the migration flag on (interim off): a generic bearer is still 401, a migration token is accepted under its full contract", async () => {
    const { upload, manifest, entryText } = await planned();
    const i = await ingest(MIGRATION_ON);
    for (const response of [
      await i.chunk(genericToken()),
      await i.chunk(genericToken([ORIGIN])),
      await i.commit(genericToken(), manifest),
    ])
      expect(response.status).toBe(401);
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(201);
    const commit = await i.commit(token, manifest);
    expect(commit.status).toBe(202);
    const body = (await commit.json()) as { workKey: string };
    // The work key is <signed manifest key>@<signed entry_sha256>.
    expect(body.workKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
    expect(await count("sessions.migration_assertion_chunks")).toBe(1);
  });

  test("with the interim bearer explicitly on and an origin set: a generic bearer is still refused on the upload routes", async () => {
    const { manifest } = await planned();
    for (const extra of [
      { ALLOW_INTERIM_RELAYAUTH_BEARER: "true" },
      {
        ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
        RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS: "true",
      },
    ] as Record<string, string>[]) {
      const i = await ingest({ ...MIGRATION_ON, ...extra });
      for (const response of [
        await i.chunk(genericToken()),
        await i.commit(genericToken(), manifest),
        await i.req(
          `/v1/pipeline/manifests/status?key=${encodeURIComponent(manifestKey(LOCATION))}`,
          genericToken(),
        ),
      ]) {
        expect([401, 403]).toContain(response.status);
      }
      // With legacy assertions accepted the generic JWT verifies, and the
      // upload-route guard still refuses it.
      if (extra.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS) {
        const legacy = await i.chunk(genericToken());
        expect(legacy.status).toBe(403);
        expect(await code(legacy)).toBe("origin_bound_assertion_required");
      }
    }
  });

  test("default config (interim bearer off, origin set): the REAL courier delivers with RelayAuth's origin-bound assertion; other bearers are refused and write nothing", async () => {
    const i = await ingest();
    const objects = async (location: SessionLocation) =>
      (await i.store.list(objectKey(sessionPrefix(location)))).items.length;
    const unsigned = `${Buffer.from(JSON.stringify({ alg: "none", typ: "JWT" })).toString("base64url")}.${liveToken().split(".")[1]}.`;
    for (const [name, token] of [
      ["unsigned", unsigned],
      ["generic", genericToken()],
      ["generic for this origin", genericToken([ORIGIN])],
      ["other origin", sign({ ...JSON.parse(Buffer.from(liveToken().split(".")[1]!, "base64url").toString("utf8")), aud: ["https://history.other.test"] })],
    ] as const) {
      const response = await i.chunk(token, LOCATION);
      expect(`${name}: ${response.status}`).toBe(`${name}: 401`);
    }
    expect(await objects(LOCATION)).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);

    const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-origin-"));
    const outer = globalThis.fetch;
    try {
      const dbPath = join(dir, "history.db");
      const rawPath = join(dir, "session.jsonl");
      await writeFile(rawPath, '{"type":"user","message":"one"}\n');
      const hist = new DatabaseSync(dbPath);
      hist.exec(
        "CREATE TABLE sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT)",
      );
      hist
        .prepare("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)")
        .run("session-origin", "claude", rawPath, 1_700_000_000_000, 1_700_000_000_100, null, "1.0.0");
      hist.close();
      const ingestUrl = "http://ingest.test";
      globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (!url.startsWith(ingestUrl)) return outer(input, init);
        const u = new URL(url);
        return i.app.request(`${u.pathname}${u.search}`, init, i.env);
      }) as typeof fetch;
      const config = (token: () => string): CourierConfig => ({
        dbPath,
        statePath: join(dir, "state.json"),
        ingestUrl,
        token: { get: async () => token(), invalidate: () => false },
        orgId: ORG,
        workspaceId: WORKSPACE,
        userId: USER,
        machineId: "machine-o",
        harnesses: ["claude"],
        closeAfterMinutes: 1_000_000,
        startMode: "all",
      });
      // Nothing is delivered with a bearer the interim flag used to admit.
      expect(await runCourierOnce(config(() => genericToken()))).toMatchObject({ uploaded: 0 });
      expect(await count("sessions.pipeline_jobs")).toBe(0);
      expect(await runCourierOnce(config(() => liveToken("machine-o")))).toMatchObject({ uploaded: 1, chunks: 1 });
      const { rows } = await db.query<{ key: string }>(
        "SELECT message->'payload'->>'key' AS key FROM sessions.pipeline_jobs",
      );
      expect(rows).toHaveLength(1);
      expect(await i.store.head(objectKey(rows[0]!.key))).toBeTruthy();
    } finally {
      globalThis.fetch = outer;
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("regression: with the interim bearer off and no origin, even a valid origin-bound assertion is refused (401 auth_not_configured)", async () => {
    const i = await ingest({ RELAYHISTORY_ORIGIN: "" });
    for (const token of [liveToken(), genericToken()]) {
      const response = await i.chunk(token, LOCATION);
      expect(response.status).toBe(401);
      expect(await code(response)).toBe("auth_not_configured");
    }
    expect(await count("sessions.pipeline_jobs")).toBe(0);
  });

  test("refuses each contract violation end to end", async () => {
    const { upload, manifest, entryText } = await planned();
    const i = await ingest(MIGRATION_ON);
    const fresh = { ...LOCATION, machineId: "machine-fresh" };
    const otherContent = structuredClone(manifest);
    (otherContent.raw.chunks[0] as { records?: number }).records = 42;
    const cases: [string, number, () => Promise<Response>][] = [
      // An arbitrary fresh machine key, on chunks, status and the commit.
      [
        "migration_upload_forbidden",
        403,
        () => i.chunk(migrationToken(upload), fresh, { entry: entryText }),
      ],
      // Per-chunk: an in-plan key, but bytes or seq outside the signed entry, or no entry.
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.chunk(migrationToken(upload), LOCATION, {
            entry: entryText,
            bytes: new TextEncoder().encode('{"type":"user","text":"evil"}\n'),
          }),
      ],
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.chunk(migrationToken(upload), LOCATION, {
            entry: entryText,
            seq: 1,
          }),
      ],
      [
        "migration_upload_forbidden",
        403,
        () => i.chunk(migrationToken(upload), LOCATION),
      ],
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.chunk(migrationToken(upload), LOCATION, {
            entry: entryText.replace('"closed"', '"open"'),
          }),
      ],
      // Provenance: 96bulls.user_id must be the signed source user.
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.commit(migrationToken(upload), {
            ...manifest,
            extensions: {
              ...manifest.extensions,
              "96bulls": { user_id: "user_ingest_b" },
            },
          }),
      ],
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.commit(migrationToken(upload), {
            ...manifest,
            extensions: undefined,
          }),
      ],
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.commit(migrationToken(upload), {
            ...manifest,
            extensions: { "96bulls": manifest.extensions["96bulls"] },
          }),
      ],
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.req(
            `/v1/pipeline/manifests/status?key=${encodeURIComponent(manifestKey(fresh))}`,
            migrationToken(upload),
          ),
      ],
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.commit(
            migrationToken(upload),
            {
              ...manifest,
              session: { ...manifest.session, machine_id: "machine-fresh" },
            },
            manifestKey(fresh),
          ),
      ],
      // Other content than the signed identity.
      [
        "migration_upload_forbidden",
        403,
        () => i.commit(migrationToken(upload), otherContent),
      ],
      // Another user's identity under this user's token.
      [
        "migration_upload_forbidden",
        403,
        () =>
          i.chunk(
            migrationToken({ ...upload, source_user_id: "user_ingest_b" }),
            LOCATION,
            { entry: entryText },
          ),
      ],
      // Signed pair is not the grant pair.
      [
        "migration_workspace_forbidden",
        403,
        () => i.chunk(migrationToken(upload, {}, { workspaceId: "ws_other" })),
      ],
      // A grant this ingest does not accept.
      [
        "migration_grant_forbidden",
        403,
        () =>
          i.chunk(
            migrationToken(
              upload,
              {},
              {
                grantId: "mgr_other",
                upload: { ...upload, grant_id: "mgr_other" },
              },
            ),
          ),
      ],
      // A relay_ alias in the identity: not canonical.
      [
        "invalid_claims",
        401,
        () =>
          i.chunk(
            migrationToken({ ...upload, source_user_id: `relay_${USER}` }),
          ),
      ],
      // Expired: a distinct, re-mintable code.
      [
        "migration_assertion_expired",
        401,
        () =>
          i.chunk(migrationToken(upload, { iat: now() - 56, exp: now() - 6 })),
      ],
      // A bad signature stays terminal.
      [
        "invalid_token",
        401,
        () => {
          const [h, b] = migrationToken(upload).split(".");
          return i.chunk(`${h}.${b}.${"A".repeat(342)}`);
        },
      ],
      // Outside the three upload routes.
      [
        "migration_assertion_route",
        403,
        () =>
          i.req("/v1/pipeline/events", migrationToken(upload), {
            method: "POST",
            body: "{}",
          }),
      ],
    ];
    for (const [expected, status, run] of cases) {
      const response = await run();
      expect(`${response.status} ${await code(response)}`).toBe(
        `${status} ${expected}`,
      );
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await count("sessions.migration_assertion_chunks")).toBe(0);

    // Replay: the same assertion after its commit is 409 on every route.
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(
      201,
    );
    expect((await i.commit(token, manifest)).status).toBe(202);
    for (const response of [
      await i.commit(token, manifest),
      await i.chunk(token, LOCATION, { entry: entryText }),
    ]) {
      expect(response.status).toBe(409);
      expect(await code(response)).toBe("migration_assertion_replayed");
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });

  test("E2E: route, enqueue, consume object-landed, then session-indexed, for a migration job and a live job", async () => {
    const i = await ingest({
      ...MIGRATION_ON,
      ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
    });
    // Migration upload.
    const { upload, manifest, entryText } = await planned();
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(
      201,
    );
    const migrated = await i.commit(token, manifest);
    expect(migrated.status).toBe(202);
    const migratedKey = ((await migrated.json()) as { workKey: string })
      .workKey;
    expect(migratedKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    // Live upload of another session (origin-bound courier assertion).
    const liveLocation = { ...LOCATION, sessionId: "session-live" };
    const live = await planned(liveLocation, liveLocation.machineId);
    const liveManifest = { ...live.manifest, extensions: undefined };
    expect(
      (await i.chunk(liveToken(liveLocation.machineId), liveLocation)).status,
    ).toBe(201);
    const liveCommit = await i.req(
      "/v1/pipeline/manifests",
      liveToken(liveLocation.machineId),
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": liveLocation.machineId,
          "x-session-id": liveLocation.sessionId,
        },
        body: JSON.stringify({
          key: manifestKey(liveLocation),
          manifest: liveManifest,
        }),
      },
    );
    expect(liveCommit.status).toBe(202);
    const liveKey = ((await liveCommit.json()) as { workKey: string }).workKey;
    expect(liveKey.startsWith(`${manifestKey(liveLocation)}@`)).toBe(true);
    expect(liveKey).not.toBe(
      `${manifestKey(liveLocation)}@${upload.entry_sha256}`,
    );

    // Consume both through the real stage handlers on the same Postgres queue.
    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: postgresPipelineQueue(sql),
      sink: sqlPipelineEventSink(sql),
      deadLetters,
      index: {
        upsertRawPointer: async (pointer: { manifestKey: string }) =>
          void indexed.push(pointer.manifestKey),
      } as never,
    });
    expect(
      await pollPostgresStage({ sql, stage: "object-landed", deps }),
    ).toMatchObject({ claimed: 2, acked: 2, retried: 0, exhausted: 0 });
    const { rows } = await db.query<{ stage: string; work_key: string }>(
      "SELECT stage, work_key FROM sessions.pipeline_jobs ORDER BY id",
    );
    expect(rows.map((r) => r.stage)).toEqual([
      "session-indexed",
      "session-indexed",
    ]);
    expect(rows.map((r) => r.work_key).sort()).toEqual(
      [migratedKey, liveKey].sort(),
    );
    expect(
      await pollPostgresStage({ sql, stage: "session-indexed", deps }),
    ).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(indexed.sort()).toEqual(
      [manifestKey(LOCATION), manifestKey(liveLocation)].sort(),
    );
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await deadLetters.list({ stage: "object-landed" })).toEqual([]);
    const { rows: errors } = await db.query(
      "SELECT 1 FROM sessions.pipeline_events WHERE error->>'code' = 'invalid_work_key' OR status IN ('error', 'dead_lettered')",
    );
    expect(errors).toHaveLength(0);
  });

  test("E2E under a deployment key prefix: the correct prefix is accepted through session-indexed; a wrong or absent prefix is refused, fail-closed", async () => {
    const PREFIX = "deploy-a";
    const i = await ingest({
      ...MIGRATION_ON,
      ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
      PIPELINE_KEY_PREFIX: PREFIX,
    });
    const { upload, manifest, entryText } = await planned();
    const prefixedKey = `${PREFIX}/${manifestKey(LOCATION)}`;
    // Negative first: a wrong or extra client-supplied prefix is refused before
    // any write or consume. (An absent prefix is applied by the server.)
    for (const key of [
      `deploy-b/${manifestKey(LOCATION)}`,
      `${PREFIX}/extra/${manifestKey(LOCATION)}`,
      `${PREFIX}/${PREFIX}/${manifestKey(LOCATION)}`,
    ]) {
      const refused = await i.commit(migrationToken(upload), manifest, key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    // Positive: chunks land under the prefix (server-chosen), the commit's
    // seal check looks there, and the job is consumed through session-indexed.
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(201);
    expect(
      ((await chunk.json()) as { key: string }).key.startsWith(`${PREFIX}/v1/`),
    ).toBe(true);
    const commit = await i.commit(token, manifest, prefixedKey);
    expect(commit.status).toBe(202);
    const workKey = ((await commit.json()) as { workKey: string }).workKey;
    // The signed (unprefixed) key and entry digest; the prefix is deployment config, not identity.
    expect(workKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    // A live session under the same prefix.
    const liveLocation = { ...LOCATION, sessionId: "session-live" };
    const live = await planned(liveLocation, liveLocation.machineId);
    expect(
      (await i.chunk(liveToken(liveLocation.machineId), liveLocation)).status,
    ).toBe(201);
    const liveCommit = await i.req(
      "/v1/pipeline/manifests",
      liveToken(liveLocation.machineId),
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": liveLocation.machineId,
          "x-session-id": liveLocation.sessionId,
        },
        body: JSON.stringify({
          key: `${PREFIX}/${manifestKey(liveLocation)}`,
          manifest: { ...live.manifest, extensions: undefined },
        }),
      },
    );
    expect(liveCommit.status).toBe(202);

    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: postgresPipelineQueue(sql),
      sink: sqlPipelineEventSink(sql),
      deadLetters,
      index: {
        upsertRawPointer: async (pointer: { manifestKey: string }) =>
          void indexed.push(pointer.manifestKey),
      } as never,
    });
    expect(
      await pollPostgresStage({ sql, stage: "object-landed", deps }),
    ).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(
      await pollPostgresStage({ sql, stage: "session-indexed", deps }),
    ).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(indexed.sort()).toEqual(
      [prefixedKey, `${PREFIX}/${manifestKey(liveLocation)}`].sort(),
    );
    expect(await deadLetters.list({ stage: "object-landed" })).toEqual([]);

    // Consumer fail-closed: a migration job whose signed key is not this
    // object's unprefixed key (here: the prefixed key itself, or another
    // session) is refused (invalid_work_key, dead-lettered), never run as live.
    for (const bad of [
      prefixedKey,
      manifestKey({ ...LOCATION, sessionId: "session-other" }),
    ]) {
      await postgresPipelineQueue(sql).send({
        v: 1,
        stage: "object-landed",
        ctx: { traceId: TRACE, orgId: ORG, workspaceId: WORKSPACE },
        workKey: `${bad}@${upload.entry_sha256}`,
        payload: {
          key: prefixedKey,
          kind: "manifest",
          etag: "e1",
          migration: { key: bad, entry_sha256: upload.entry_sha256 },
        },
      } as never);
    }
    const refused = await pollPostgresStage({
      sql,
      stage: "object-landed",
      deps,
    });
    expect(refused.acked + refused.exhausted).toBe(2);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(indexed).toHaveLength(2);
    const letters = await deadLetters.list({ stage: "object-landed" });
    expect(letters).toHaveLength(2);
  });

  test("prefixed GET manifests/status: the correct prefix reads the signed upload's own status; absent or wrong prefixes are refused", async () => {
    const PREFIX = "deploy-a";
    const i = await ingest({ ...MIGRATION_ON, PIPELINE_KEY_PREFIX: PREFIX });
    const { upload, manifest, entryText } = await planned();
    const prefixedKey = `${PREFIX}/${manifestKey(LOCATION)}`;
    const writer = encodeURIComponent(`migration:${GRANT}`);
    const status = (token: string, key: string) =>
      i.req(
        `/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=${LOCATION.machineId}&writer=${writer}`,
        token,
      );
    // Not yet stored: 404 under the correct prefix.
    expect((await status(migrationToken(upload), prefixedKey)).status).toBe(
      404,
    );
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(
      201,
    );
    expect((await i.commit(token, manifest, prefixedKey)).status).toBe(202);
    // A fresh assertion for the same upload reads its status under the prefix.
    const own = await status(migrationToken(upload), prefixedKey);
    expect(own.status).toBe(200);
    expect(
      ((await own.json()) as { version: { eTag?: string } }).version.eTag,
    ).toBeTruthy();
    // The unprefixed key (the courier's shape) is served under the server's prefix.
    expect(
      (await status(migrationToken(upload), manifestKey(LOCATION))).status,
    ).toBe(200);
    // A wrong or extra client prefix: refused (403), never served.
    for (const key of [
      `deploy-b/${manifestKey(LOCATION)}`,
      `${PREFIX}/extra/${manifestKey(LOCATION)}`,
    ]) {
      const refused = await status(migrationToken(upload), key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    // Another session's key under the right prefix: not the signed upload.
    const other = await status(
      migrationToken(upload),
      `${PREFIX}/${manifestKey({ ...LOCATION, sessionId: "session-other" })}`,
    );
    expect(other.status).toBe(403);
    expect(await code(other)).toBe("migration_upload_forbidden");
  });

  test("tenant org allowlist (TENANT_ORG_IDS, org-derived prefixes): a verified token for an unlisted org is refused 403 org_not_allowed before any write; a listed org is accepted", async () => {
    const { upload, manifest, entryText } = await planned();
    const ownKey = `${ORG}/${manifestKey(LOCATION)}`;
    // ORG is not listed: refused on the chunk and the commit, nothing stored or queued.
    const closed = await ingest({
      ...MIGRATION_ON,
      PIPELINE_ORG_FROM_PREFIX: "true",
      TENANT_ORG_ID: "",
      TENANT_ORG_IDS: "org_listed_a,org_listed_b",
    });
    const token = migrationToken(upload);
    const chunk = await closed.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(403);
    expect(await code(chunk)).toBe("org_not_allowed");
    const commit = await closed.commit(token, manifest, ownKey);
    expect(commit.status).toBe(403);
    expect(await code(commit)).toBe("org_not_allowed");
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    // Listed: the same upload goes through.
    const open = await ingest({
      ...MIGRATION_ON,
      PIPELINE_ORG_FROM_PREFIX: "true",
      TENANT_ORG_ID: "",
      TENANT_ORG_IDS: `org_listed_a,${ORG}`,
    });
    const okChunk = await open.chunk(token, LOCATION, { entry: entryText });
    expect(okChunk.status).toBe(201);
    expect((await open.commit(token, manifest, ownKey)).status).toBe(202);
  });

  test("org-derived prefixes (PIPELINE_ORG_FROM_PREFIX through the real composition): the token's own org prefix is accepted end to end; another org's prefix is refused", async () => {
    const i = await ingest({
      ...MIGRATION_ON,
      PIPELINE_ORG_FROM_PREFIX: "true",
      TENANT_ORG_ID: "",
    });
    const { upload, manifest, entryText } = await planned();
    const ownKey = `${ORG}/${manifestKey(LOCATION)}`;
    // A token for org A with a key under org B's prefix: refused before any write.
    for (const key of [
      `org_other/${manifestKey(LOCATION)}`,
      `x/${ORG}/${manifestKey(LOCATION)}`,
    ]) {
      const refused = await i.commit(migrationToken(upload), manifest, key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    // Org B's token (its own identity) cannot write under org A's prefix either.
    const orgBUpload = await uploadIdentityOf(
      { ...(JSON.parse(entryText) as PlanEntry), org_id: "org_other" },
      PLAN,
    );
    const orgB = await i.commit(
      migrationToken(orgBUpload, { org: "org_other" }),
      manifest,
      ownKey,
    );
    expect(orgB.status).toBe(403);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    // The token's own org prefix: chunk under it, commit, consume through session-indexed.
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(201);
    expect(
      ((await chunk.json()) as { key: string }).key.startsWith(`${ORG}/v1/`),
    ).toBe(true);
    const commit = await i.commit(token, manifest, ownKey);
    expect(commit.status).toBe(202);
    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps(
      { PIPELINE_ORG_FROM_PREFIX: "true" } as Bindings,
      {
        queue: postgresPipelineQueue(sql),
        sink: sqlPipelineEventSink(sql),
        deadLetters,
        index: {
          upsertRawPointer: async (pointer: { manifestKey: string }) =>
            void indexed.push(pointer.manifestKey),
        } as never,
      },
    );
    expect(
      await pollPostgresStage({ sql, stage: "object-landed", deps }),
    ).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
    expect(
      await pollPostgresStage({ sql, stage: "session-indexed", deps }),
    ).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
    expect(indexed).toEqual([ownKey]);
    // No dead letter for this upload (the shared test database may hold
    // letters from the forged jobs of the static-prefix test).
    const letters = await deadLetters.list({ stage: "object-landed" });
    expect(
      letters.filter((l) => JSON.stringify(l).includes(`${ORG}/v1/`)),
    ).toEqual([]);
  });

  test("combined base + org-derived prefix: exactly <base>/<caller org> is accepted end to end; foreign, extra and bare prefixes are refused", async () => {
    const BASE = "deploy-a";
    const i = await ingest({
      ...MIGRATION_ON,
      PIPELINE_KEY_PREFIX: BASE,
      PIPELINE_ORG_FROM_PREFIX: "true",
    });
    const { upload, manifest, entryText } = await planned();
    const exact = `${BASE}/${ORG}/${manifestKey(LOCATION)}`;
    for (const key of [
      `deploy-b/${ORG}/${manifestKey(LOCATION)}`, // foreign base
      `${BASE}/extra/${ORG}/${manifestKey(LOCATION)}`, // extra segment
      `${ORG}/${manifestKey(LOCATION)}`, // bare org (no base)
      `${BASE}/org_other/${manifestKey(LOCATION)}`, // another org
      `${BASE}/${manifestKey(LOCATION)}`, // base, no org
    ]) {
      const refused = await i.commit(migrationToken(upload), manifest, key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(
      ((await chunk.json()) as { key: string }).key.startsWith(
        `${BASE}/${ORG}/v1/`,
      ),
    ).toBe(true);
    // The courier's unprefixed key: the server applies exactly <base>/<org>.
    const commit = await i.commit(token, manifest);
    expect(commit.status).toBe(202);
    expect(await i.store.head(objectKey(exact))).toBeTruthy();
    const { rows } = await db.query<{ key: string }>(
      "SELECT message->'payload'->>'key' AS key FROM sessions.pipeline_jobs",
    );
    expect(rows.map((r) => r.key)).toEqual([exact]);
  });

  test("prefixAllowed: exact <base>/<org>, static base, and bare-org rules", () => {
    const org = {
      PIPELINE_ORG_FROM_PREFIX: "true",
      PIPELINE_KEY_PREFIX: "deploy-a",
    };
    expect(prefixAllowed(org, "deploy-a/org_a", "org_a")).toBe(true);
    for (const p of [
      "deploy-b/org_a",
      "deploy-a/extra/org_a",
      "org_a",
      "deploy-a/org_b",
      "deploy-a",
      undefined,
      "deploy-a/org_a/x",
    ])
      expect(`${p}: ${prefixAllowed(org, p, "org_a")}`).toBe(`${p}: false`);
    const bare = { PIPELINE_ORG_FROM_PREFIX: "true" };
    expect(prefixAllowed(bare, "org_a", "org_a")).toBe(true);
    for (const p of ["x/org_a", "org_b", undefined])
      expect(prefixAllowed(bare, p, "org_a")).toBe(false);
    const fixed = { PIPELINE_KEY_PREFIX: "/deploy-a/" };
    expect(prefixAllowed(fixed, "deploy-a", "org_a")).toBe(true);
    for (const p of ["deploy-b", "deploy-a/org_a", undefined])
      expect(prefixAllowed(fixed, p, "org_a")).toBe(false);
    expect(prefixAllowed({}, undefined, "org_a")).toBe(true);
    expect(prefixAllowed({}, "deploy-a", "org_a")).toBe(false);
  });

  for (const [mode, extra, prefix] of [
    ["static prefix", { PIPELINE_KEY_PREFIX: "deploy-a" }, "deploy-a"],
    [
      "org-derived prefix with a base",
      { PIPELINE_KEY_PREFIX: "deploy-a", PIPELINE_ORG_FROM_PREFIX: "true" },
      `deploy-a/${ORG}`,
    ],
  ] as const) {
    test(`the REAL courier (runCourierOnce, unprefixed keys) under a ${mode}: commits, reconciles status and is consumed through session-indexed`, async () => {
      const i = await ingest({
        ...extra,
        ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
      });
      const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-prefix-"));
      try {
        const dbPath = join(dir, "history.db");
        const rawPath = join(dir, "session.jsonl");
        await writeFile(rawPath, '{"type":"user","message":"one"}\n');
        const hist = new DatabaseSync(dbPath);
        hist.exec(
          "CREATE TABLE sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT)",
        );
        hist
          .prepare("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)")
          .run(
            "session-courier",
            "claude",
            rawPath,
            1_700_000_000_000,
            1_700_000_000_100,
            null,
            "1.0.0",
          );
        hist.close();
        const ingestUrl = "http://ingest.test";
        const seenKeys: string[] = [];
        const outer = globalThis.fetch;
        globalThis.fetch = (async (
          input: RequestInfo | URL,
          init?: RequestInit,
        ) => {
          const url = String(input);
          if (!url.startsWith(ingestUrl)) return outer(input, init);
          const u = new URL(url);
          if (u.searchParams.get("key"))
            seenKeys.push(u.searchParams.get("key")!);
          if (init?.body && u.pathname.endsWith("/pipeline/manifests"))
            seenKeys.push(
              (JSON.parse(String(init.body)) as { key: string }).key,
            );
          return i.app.request(`${u.pathname}${u.search}`, init, i.env);
        }) as typeof fetch;
        const config = (): CourierConfig => ({
          dbPath,
          statePath: join(dir, "state.json"),
          ingestUrl,
          token: {
            get: async () => liveToken("machine-c"),
            invalidate: () => false,
          },
          orgId: ORG,
          workspaceId: WORKSPACE,
          userId: USER,
          machineId: "machine-c",
          harnesses: ["claude"],
          closeAfterMinutes: 1_000_000,
          startMode: "all",
        });
        try {
          expect(await runCourierOnce(config())).toMatchObject({
            uploaded: 1,
            chunks: 1,
          });
          // The courier posts the canonical unprefixed key; the server stored it under its prefix.
          expect(seenKeys.every((k) => k.startsWith("v1/"))).toBe(true);
          const { rows } = await db.query<{ key: string }>(
            "SELECT message->'payload'->>'key' AS key FROM sessions.pipeline_jobs",
          );
          expect(rows).toHaveLength(1);
          expect(rows[0]!.key.startsWith(`${prefix}/v1/`)).toBe(true);
          expect(await i.store.head(objectKey(rows[0]!.key))).toBeTruthy();
          // A restarted courier reconciles its recorded (unprefixed) key: verified, not reset.
          expect(
            await runCourierOnce({ ...config(), verifiedManifests: new Set() }),
          ).toMatchObject({ uploaded: 0 });
          const { rows: still } = await db.query(
            "SELECT 1 FROM sessions.pipeline_jobs",
          );
          expect(still).toHaveLength(1);
        } finally {
          globalThis.fetch = outer;
        }
        const sql = pgExecutor();
        const indexed: string[] = [];
        const deadLetters = postgresDeadLetterStore(sql);
        const deps = pipelineQueueDeps(extra as Bindings, {
          queue: postgresPipelineQueue(sql),
          sink: sqlPipelineEventSink(sql),
          deadLetters,
          index: {
            upsertRawPointer: async (pointer: { manifestKey: string }) =>
              void indexed.push(pointer.manifestKey),
          } as never,
        });
        expect(
          await pollPostgresStage({ sql, stage: "object-landed", deps }),
        ).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
        expect(
          await pollPostgresStage({ sql, stage: "session-indexed", deps }),
        ).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
        expect(indexed).toHaveLength(1);
        expect(indexed[0]!.startsWith(`${prefix}/v1/`)).toBe(true);
      } finally {
        await rm(dir, { recursive: true, force: true });
      }
    });
  }

  test("org-derived reconcile scans the <base>/<org> roots and keeps only exact prefixes of a known org", async () => {
    const env = {
      PIPELINE_KEY_PREFIX: "deploy-a",
      PIPELINE_ORG_FROM_PREFIX: "true",
    };
    // The indexer's root (was `<base>/v1` for every mode, which never contains `<base>/<org>/v1`).
    expect(indexerReconcileRoot(env)).toBe("deploy-a");
    expect(indexerReconcileRoot({ PIPELINE_KEY_PREFIX: "deploy-a" })).toBe(
      "deploy-a/v1",
    );
    expect(indexerReconcileRoot({})).toBe("v1");
    expect(
      indexerReconcileRoot({ PIPELINE_ORG_FROM_PREFIX: "true" }),
    ).toBeUndefined();
    const store = createFsStore({ root });
    const { manifest } = await planned();
    const body = new TextEncoder().encode(JSON.stringify(manifest));
    const keys = {
      exact: `deploy-a/${ORG}/${manifestKey(LOCATION)}`,
      foreign: `deploy-b/${ORG}/${manifestKey(LOCATION)}`,
      extra: `deploy-a/extra/${ORG}/${manifestKey(LOCATION)}`,
      bare: `${ORG}/${manifestKey(LOCATION)}`,
    };
    for (const key of Object.values(keys))
      await store.put(objectKey(key), body, {
        contentType: "application/json",
      });
    const sent: { payload: { key: string } }[] = [];
    const result = await reconcileManifests({
      store,
      reader: objectStoreManifestReader(store, {
        provider: "fs",
        bucket: "raw",
      }),
      orgs: orgResolverFromEnv(env)!,
      allowPrefix: (prefix, orgId) => prefixAllowed(env, prefix, orgId),
      send: async (m) => void sent.push(m as never),
      listPrefix: indexerReconcileRoot(env)!,
      modifiedSince: new Date(0),
    });
    expect(sent.map((m) => m.payload.key)).toEqual([keys.exact]);
    expect(result.enqueued).toBe(1);
  });

  // ---- Plan entry v2: the base read, then a backfill epoch (issuer spec #86) ----

  const BASE_READ_ON = {
    ...MIGRATION_ON,
    ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
    RELAYHISTORY_ACCEPT_MIGRATION_BASE_READ: "true",
  };
  const straddle = { ...LOCATION, sessionId: "session-straddle" };
  const ndjsonRecord = (n: number) =>
    new TextEncoder().encode(
      `${JSON.stringify({ kind: "event", record_id: `r${n}`, origin_id: "o1", revision: 1, received_at: AT, payload: { n } })}\n`,
    );

  /** A live ndjson session at `straddle`, its manifest and version, plus the plan's v2 entry for two backfill chunks. */
  async function liveSessionWithPlan(
    i: Awaited<ReturnType<typeof ingest>>,
    location = straddle,
  ) {
    const key = manifestKey(location);
    const liveChunk = await i.chunk(liveToken(), location, {
      bytes: ndjsonRecord(1),
      format: "ndjson",
    });
    expect(liveChunk.status).toBe(201);
    const liveSealed = (await liveChunk.json()) as {
      entry: Record<string, unknown> & { bytes: number; stored_bytes: number };
      scrub: unknown;
    };
    const liveManifest = {
      schema_v: 1,
      layout: "v1",
      trace_id: TRACE,
      revision: 1,
      created_at: AT,
      updated_at: AT,
      session: {
        workspace_id: WORKSPACE,
        machine_id: location.machineId,
        harness: location.harness,
        session_id: location.sessionId,
        started_at: AT,
      },
      writer: {
        delivery_instance_id: location.machineId,
        plugin_version: "96bulls-courier/0.3.0",
      },
      raw: {
        owner: location.machineId,
        state: "open",
        updated_at: AT,
        format: "ndjson",
        compression: "zstd",
        chunk_count: 1,
        raw_bytes: liveSealed.entry.bytes,
        stored_bytes: liveSealed.entry.stored_bytes,
        content_sha256: await computeContentSha256([liveSealed.entry as never]),
        epoch: 0,
        epochs: [
          {
            epoch: 0,
            opened_at: AT,
            reason: "initial",
            first_seq: 0,
            opened_by: location.machineId,
          },
        ],
        next_offset: liveSealed.entry.bytes,
        chunks: [liveSealed.entry],
        scrub: liveSealed.scrub,
      },
      extensions: {
        "96bulls": {
          session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19",
          session_uuid_source: "minted",
          tenant_id: ORG,
          user_id: USER,
        },
      },
    };
    const liveCommit = await i.req("/v1/pipeline/manifests", liveToken(), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": location.machineId,
        "x-session-id": location.sessionId,
      },
      body: JSON.stringify({ key, manifest: liveManifest }),
    });
    expect(liveCommit.status).toBe(202);
    const liveVersion = (
      (await liveCommit.json()) as {
        version: { eTag?: string; version?: string };
      }
    ).version;
    const sources = [ndjsonRecord(100), ndjsonRecord(101)];
    const chunks = sources.map((b, n) => ({
      start_offset: n === 0 ? 0 : sources[0]!.byteLength,
      bytes: b.byteLength,
      sha256: createHash("sha256").update(b).digest("hex"),
      records: 1,
    }));
    const entry: PlanEntryV2 = {
      v: 2,
      kind: "backfill",
      grant_id: GRANT,
      key,
      org_id: ORG,
      workspace_id: WORKSPACE,
      source_user_id: USER,
      session: {
        machine_id: location.machineId,
        harness: location.harness,
        session_id: location.sessionId,
        started_at: AT,
      },
      format: "ndjson",
      chunks,
      backfill_sha256: await backfillSha256(chunks),
      raw_bytes: sources.reduce((n, b) => n + b.byteLength, 0),
    };
    return {
      key,
      liveManifest,
      liveSealed,
      liveVersion,
      sources,
      chunks,
      entry,
      entryText: canonicalJson(entry),
      read: await baseReadIdentityOf(entry, PLAN),
    };
  }

  const getBase = (
    i: Awaited<ReturnType<typeof ingest>>,
    token: string,
    key: string,
    query = "",
  ) =>
    i.req(
      `/v1/pipeline/manifests/base?key=${encodeURIComponent(key)}${query}`,
      token,
    );

  test("v2 base read: exactly the 7 base fields, single use (replay 409), exactly one concurrent winner, audited without the key", async () => {
    const i = await ingest(BASE_READ_ON);
    const p = await liveSessionWithPlan(i);
    const auditBefore = await count("sessions.migration_ingest_audit");
    const token = baseReadToken(p.read);
    const [a, b, c2] = await Promise.all([
      getBase(i, token, p.key),
      getBase(i, token, p.key),
      getBase(i, token, p.key),
    ]);
    const statuses = [a.status, b.status, c2.status].sort();
    expect(statuses).toEqual([200, 409, 409]);
    const winner = [a, b, c2].find((r) => r.status === 200)!;
    const { base } = (await winner.json()) as { base: BackfillBase };
    expect(Object.keys(base).sort()).toEqual([
      "chunk_count",
      "content_sha256",
      "epoch",
      "owner",
      "revision",
      "version",
      "writer",
    ]);
    expect(base).toEqual({
      revision: 1,
      version: String(p.liveVersion.version ?? p.liveVersion.eTag),
      owner: straddle.machineId,
      writer: straddle.machineId,
      chunk_count: 1,
      epoch: 0,
      content_sha256: p.liveManifest.raw.content_sha256,
    });
    for (const r of [a, b, c2].filter((r) => r.status === 409))
      expect(await code(r)).toBe("migration_assertion_replayed");
    // A later replay too.
    expect((await getBase(i, token, p.key)).status).toBe(409);
    // One durable audit row, result base_read, the key's digest instead of the key.
    expect(await count("sessions.migration_ingest_audit")).toBe(
      auditBefore + 1,
    );
    const { rows } = await db.query<Record<string, unknown>>(
      "SELECT * FROM sessions.migration_ingest_audit ORDER BY id DESC LIMIT 1",
    );
    expect(rows[0]).toMatchObject({
      result: "base_read",
      grant_id: GRANT,
      plan_digest: PLAN,
      entry_sha256: p.read.entry_sha256,
      user_id: USER,
    });
    expect(String(rows[0]!.work_key)).toBe(
      `${createHash("sha256").update(p.key).digest("hex")}@base_read@r1`,
    );
    expect(JSON.stringify(rows[0])).not.toContain(straddle.machineId);
    expect(JSON.stringify(rows[0])).not.toContain(token.split(".")[2]!);
    // Its own replay namespace.
    const { rows: own } = await db.query(
      "SELECT 1 FROM sessions.migration_base_read_uses",
    );
    expect(own).toHaveLength(1);
  });

  test("v2 base read refusals: default-off gate, cross-tenant, wrong key, query variants, missing or unusable base (nothing consumed)", async () => {
    // Default off: the capability does not verify, and the route is not served.
    const off = await ingest({
      ...MIGRATION_ON,
      ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
    });
    const p0 = await liveSessionWithPlan(off);
    const offRead = await getBase(off, baseReadToken(p0.read), p0.key);
    expect(offRead.status).toBe(404);
    expect((await getBase(off, liveToken(), p0.key)).status).toBe(404);
    await db.exec("TRUNCATE sessions.pipeline_jobs");

    const i = await ingest(BASE_READ_ON);
    const p = await liveSessionWithPlan(i, {
      ...straddle,
      sessionId: "session-straddle-2",
    });
    const read = p.read;
    const cases: [string, () => Promise<Response>, number][] = [
      [
        "another org's tenancy",
        () => getBase(i, baseReadToken({ ...read, org_id: "org_other" })),
        403,
      ],
      [
        "another user's capability",
        () =>
          getBase(
            i,
            baseReadToken({ ...read, source_user_id: "user_other" }),
            p.key,
          ),
        403,
      ],
      [
        "another workspace",
        () =>
          getBase(
            i,
            baseReadToken(read, {}, { workspaceId: "ws_other" }),
            p.key,
          ),
        403,
      ],
      [
        "a key other than the signed one",
        () => getBase(i, baseReadToken(read), manifestKey(LOCATION)),
        403,
      ],
      [
        "a second query parameter",
        () => getBase(i, baseReadToken(read), p.key, "&machine=x"),
        400,
      ],
      [
        "a client-chosen prefix",
        () => getBase(i, baseReadToken(read), `deploy-x/${p.key}`),
        403,
      ],
      [
        "a grant this ingest does not accept",
        () =>
          getBase(
            i,
            baseReadToken(
              { ...read, grant_id: `mgr_${"9".repeat(32)}` },
              {},
              { grantId: `mgr_${"9".repeat(32)}` },
            ),
            p.key,
          ),
        403,
      ],
      [
        "a malformed meta.read (extra key)",
        () => getBase(i, baseReadToken({ ...read, extra: 1 } as never), p.key),
        401,
      ],
      [
        "meta.upload on a base-read token",
        () => getBase(i, baseReadToken(read, {}, { upload: read }), p.key),
        401,
      ],
      [
        "the upload audience on a base-read token",
        () => getBase(i, baseReadToken(read, { aud: [ORIGIN] }), p.key),
        401,
      ],
      [
        "a TTL above 30 s",
        () => getBase(i, baseReadToken(read, { exp: now() + 45 }), p.key),
        401,
      ],
      [
        "an upload-namespace jti",
        () =>
          getBase(
            i,
            baseReadToken(read, {
              jti: `tok_${randomBytes(8).toString("hex")}`,
            }),
            p.key,
          ),
        401,
      ],
      [
        "a base-read identity inside an upload token's meta.upload",
        () =>
          i.chunk(migrationToken(read as never), straddle, {
            entry: p.entryText,
            seq: 1,
            epoch: 1,
            format: "ndjson",
          }),
        401,
      ],
      [
        "a base-read jti (mbr_) on an upload token",
        async () =>
          i.chunk(
            migrationToken((await planned()).upload, {
              jti: `mbr_${randomBytes(8).toString("hex")}`,
            }),
            LOCATION,
            { entry: (await planned()).entryText },
          ),
        401,
      ],
    ];
    for (const [name, run, status] of cases) {
      const r = await run();
      expect(`${name}: ${r.status}`).toBe(`${name}: ${status}`);
    }
    // No manifest yet at a planned key: 404, nothing consumed.
    const fresh = { ...straddle, sessionId: "session-not-live" };
    const freshEntry = {
      ...p.entry,
      key: manifestKey(fresh),
      session: { ...p.entry.session, session_id: fresh.sessionId },
    };
    expect(
      (
        await getBase(
          i,
          baseReadToken(await baseReadIdentityOf(freshEntry, PLAN)),
          manifestKey(fresh),
        )
      ).status,
    ).toBe(404);
    // A migration-owned (cold-export) session cannot take a backfill: 409 migration_base_unusable.
    const cold = await planned();
    expect(
      (
        await i.chunk(migrationToken(cold.upload), LOCATION, {
          entry: cold.entryText,
        })
      ).status,
    ).toBe(201);
    expect(
      (await i.commit(migrationToken(cold.upload), cold.manifest)).status,
    ).toBe(202);
    const coldEntry = {
      ...p.entry,
      key: manifestKey(LOCATION),
      session: {
        machine_id: LOCATION.machineId,
        harness: LOCATION.harness,
        session_id: LOCATION.sessionId,
        started_at: AT,
      },
    };
    const unusable = await getBase(
      i,
      baseReadToken(await baseReadIdentityOf(coldEntry, PLAN)),
      manifestKey(LOCATION),
    );
    expect(unusable.status).toBe(409);
    expect(await code(unusable)).toBe("migration_base_unusable");
    const { rows } = await db.query(
      "SELECT 1 FROM sessions.migration_base_read_uses",
    );
    expect(rows).toHaveLength(0);
  });

  test("v2 route matrix: a base-read capability reaches only /base; upload, commit and live tokens never reach /base; method, slash and path variants do not widen", async () => {
    const i = await ingest(BASE_READ_ON);
    const p = await liveSessionWithPlan(i, {
      ...straddle,
      sessionId: "session-matrix",
    });
    const read = p.read;
    const commitUpload = await uploadIdentityV2Of(p.entry, PLAN, {
      revision: 1,
      version: '"v"',
      owner: straddle.machineId,
      writer: straddle.machineId,
      chunk_count: 1,
      epoch: 0,
      content_sha256: p.liveManifest.raw.content_sha256,
    });
    const expectStatus = async (
      name: string,
      r: Promise<Response>,
      status: number,
      errorCode?: string,
    ) => {
      const res = await r;
      expect(`${name}: ${res.status}`).toBe(`${name}: ${status}`);
      if (errorCode)
        expect(`${name}: ${await code(res)}`).toBe(`${name}: ${errorCode}`);
    };
    // A base-read capability on every other route.
    await expectStatus(
      "base-read on chunks",
      i.chunk(baseReadToken(read), straddle, {
        entry: p.entryText,
        seq: 1,
        epoch: 1,
        bytes: p.sources[0],
        format: "ndjson",
      }),
      403,
      "migration_token_class_forbidden",
    );
    await expectStatus(
      "base-read on commit",
      i.commit(baseReadToken(read), p.liveManifest, p.key),
      403,
      "migration_token_class_forbidden",
    );
    await expectStatus(
      "base-read on status",
      i.req(
        `/v1/pipeline/manifests/status?key=${encodeURIComponent(p.key)}`,
        baseReadToken(read),
      ),
      403,
      "migration_token_class_forbidden",
    );
    await expectStatus(
      "base-read on events",
      i.req("/v1/pipeline/events", baseReadToken(read), {
        method: "POST",
        body: "{}",
      }),
      403,
      "migration_assertion_route",
    );
    // Upload, v2 commit and live tokens on /base.
    await expectStatus(
      "v1 upload on /base",
      getBase(i, migrationToken((await planned()).upload), p.key),
      403,
      "migration_token_class_forbidden",
    );
    await expectStatus(
      "v2 commit on /base",
      getBase(i, migrationToken(commitUpload), p.key),
      403,
      "migration_token_class_forbidden",
    );
    await expectStatus(
      "live token on /base",
      getBase(i, liveToken(), p.key),
      403,
      "migration_token_class_forbidden",
    );
    // Method, slash and path variants.
    for (const [name, path, method] of [
      [
        "HEAD /base",
        `/v1/pipeline/manifests/base?key=${encodeURIComponent(p.key)}`,
        "HEAD",
      ],
      [
        "POST /base",
        `/v1/pipeline/manifests/base?key=${encodeURIComponent(p.key)}`,
        "POST",
      ],
      [
        "/base//",
        `/v1/pipeline/manifests/base//?key=${encodeURIComponent(p.key)}`,
        "GET",
      ],
      [
        "/base/x",
        `/v1/pipeline/manifests/base/x?key=${encodeURIComponent(p.key)}`,
        "GET",
      ],
      [
        "/status/../base",
        `/v1/pipeline/manifests/status/x?key=${encodeURIComponent(p.key)}`,
        "GET",
      ],
    ] as const) {
      const r = await i.req(path, baseReadToken(read), { method });
      expect(`${name}: ${r.status}`).toBe(`${name}: 403`);
    }
    // One trailing slash is tolerated (the same rule for every route).
    // ...and it is the same route: exactly one trailing slash is stripped.
    expect(
      (
        await i.req(
          `/v1/pipeline/manifests/base/?key=${encodeURIComponent(p.key)}`,
          baseReadToken(read),
        )
      ).status,
    ).toBe(200);
    const { rows } = await db.query(
      "SELECT 1 FROM sessions.migration_base_read_uses",
    );
    expect(rows.length).toBeLessThanOrEqual(1);
  });

  test("v2 backfill end to end: base read, then the backfill epoch on exactly that base, consumed through session-indexed; a base change between read and commit is refused", async () => {
    const i = await ingest(BASE_READ_ON);
    const p = await liveSessionWithPlan(i);
    // 1. The base read.
    const readRes = await getBase(i, baseReadToken(p.read), p.key);
    expect(readRes.status).toBe(200);
    const { base } = (await readRes.json()) as { base: BackfillBase };
    // 2. The commit assertion with exactly that base.
    const upload = await uploadIdentityV2Of(p.entry, PLAN, base);
    expect(upload.entry_sha256).toBe(await planEntryV2Sha256(p.entry));
    expect(upload.entry_sha256).toBe(p.read.entry_sha256);
    const token = migrationToken(upload);
    // Chunk negatives (seq/epoch from the signed base; bytes digested).
    for (const [name, opts] of [
      ["seq inside the live range", { seq: 0, epoch: 1, bytes: p.sources[0] }],
      ["epoch not base.epoch + 1", { seq: 1, epoch: 0, bytes: p.sources[0] }],
      ["other bytes", { seq: 1, epoch: 1, bytes: ndjsonRecord(999) }],
      [
        "chunk 1 at chunk 0's seq",
        { seq: 1, epoch: 1, bytes: p.sources[1], offset: 0 },
      ],
    ] as const) {
      const r = await i.chunk(token, straddle, {
        entry: p.entryText,
        format: "ndjson",
        ...opts,
      });
      expect(`${name}: ${r.status}`).toBe(`${name}: 403`);
    }
    const sealed: Record<string, unknown>[] = [];
    for (const [n, bytes] of p.sources.entries()) {
      const r = await i.chunk(token, straddle, {
        entry: p.entryText,
        seq: base.chunk_count + n,
        epoch: base.epoch + 1,
        offset: p.chunks[n]!.start_offset,
        bytes,
        format: "ndjson",
      });
      expect(r.status).toBe(201);
      sealed.push(
        ((await r.json()) as { entry: Record<string, unknown> }).entry,
      );
    }
    const added = sealed as unknown as {
      bytes: number;
      stored_bytes: number;
      start_offset: number;
    }[];
    const appended = {
      ...p.liveManifest,
      revision: 2,
      raw: {
        ...p.liveManifest.raw,
        chunk_count: 3,
        raw_bytes:
          p.liveManifest.raw.raw_bytes + added.reduce((n, c) => n + c.bytes, 0),
        stored_bytes:
          p.liveManifest.raw.stored_bytes +
          added.reduce((n, c) => n + c.stored_bytes, 0),
        content_sha256: await computeContentSha256([
          p.liveSealed.entry,
          ...sealed,
        ] as never),
        epoch: 1,
        epochs: [
          ...p.liveManifest.raw.epochs,
          {
            epoch: 1,
            opened_at: AT,
            reason: "backfill",
            first_seq: 1,
            opened_by: `migration:${GRANT}`,
          },
        ],
        next_offset: added.at(-1)!.start_offset + added.at(-1)!.bytes,
        chunks: [p.liveSealed.entry, ...sealed],
      },
      extensions: {
        ...p.liveManifest.extensions,
        "client.relayhistory_export": {
          version: 1,
          source: "neon.delivery_records",
          exported_at: AT,
          grant_id: GRANT,
          plan_digest: PLAN,
          source_user_id: USER,
        },
      },
    };
    const post = (
      t: string,
      manifest: unknown,
      previousVersion: unknown = p.liveVersion,
    ) =>
      i.req("/v1/pipeline/manifests", t, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": straddle.machineId,
          "x-session-id": straddle.sessionId,
        },
        body: JSON.stringify({ key: p.key, manifest, previousVersion }),
      });
    // Commit negatives: a base other than the stored one; missing records; a stale CAS.
    const wrongBase = async (b: Partial<BackfillBase>) =>
      migrationToken(
        await uploadIdentityV2Of(p.entry, PLAN, { ...base, ...b }),
      );
    for (const [name, t] of [
      ["base revision", await wrongBase({ revision: 2 })],
      ["base version", await wrongBase({ version: '"stale"' })],
      ["base chunk_count", await wrongBase({ chunk_count: 0 })],
      ["base owner", await wrongBase({ owner: "machine-other" })],
      ["base content", await wrongBase({ content_sha256: "c".repeat(64) })],
    ] as const) {
      const r = await post(t, appended);
      expect(`${name}: ${r.status}`).toBe(`${name}: 403`);
    }
    const noRecords = structuredClone(appended);
    delete (noRecords.raw.chunks[2] as { records?: number }).records;
    const r0 = await post(token, noRecords);
    expect([400, 403]).toContain(r0.status);
    expect((await post(token, appended, { eTag: '"stale"' })).status).toBe(409);
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    // The commit on exactly the read base.
    const commit = await post(token, appended);
    expect(commit.status).toBe(202);
    expect(((await commit.json()) as { workKey: string }).workKey).toBe(
      `${p.key}@${upload.entry_sha256}@r2`,
    );
    // Replay of the commit assertion: 409.
    expect((await post(token, appended)).status).toBe(409);

    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: postgresPipelineQueue(sql),
      sink: sqlPipelineEventSink(sql),
      deadLetters,
      index: {
        upsertRawPointer: async (pointer: { manifestKey: string }) =>
          void indexed.push(pointer.manifestKey),
      } as never,
    });
    expect(
      await pollPostgresStage({ sql, stage: "object-landed", deps }),
    ).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(
      await pollPostgresStage({ sql, stage: "session-indexed", deps }),
    ).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(indexed).toEqual([p.key, p.key]);
  });

  /** A committed v2 backfill (revision 1 + one backfill epoch, 202) and its poster. */
  const committedBackfillV2 = async (
    i: Awaited<ReturnType<typeof ingest>>,
    sessionId: string,
  ) => {
    const location = { ...straddle, sessionId };
    const p = await liveSessionWithPlan(i, location);
    const { base } = (await (
      await getBase(i, baseReadToken(p.read), p.key)
    ).json()) as { base: BackfillBase };
    const upload = await uploadIdentityV2Of(p.entry, PLAN, base);
    const first = migrationToken(upload);
    const sealed: Record<string, unknown>[] = [];
    for (const [n, bytes] of p.sources.entries()) {
      const r = await i.chunk(first, location, {
        entry: p.entryText,
        seq: base.chunk_count + n,
        epoch: base.epoch + 1,
        offset: p.chunks[n]!.start_offset,
        bytes,
        format: "ndjson",
      });
      expect(r.status).toBe(201);
      sealed.push(
        ((await r.json()) as { entry: Record<string, unknown> }).entry,
      );
    }
    const added = sealed as unknown as {
      bytes: number;
      stored_bytes: number;
      start_offset: number;
    }[];
    const appended = {
      ...p.liveManifest,
      revision: 2,
      raw: {
        ...p.liveManifest.raw,
        chunk_count: 3,
        raw_bytes:
          p.liveManifest.raw.raw_bytes + added.reduce((n, c) => n + c.bytes, 0),
        stored_bytes:
          p.liveManifest.raw.stored_bytes +
          added.reduce((n, c) => n + c.stored_bytes, 0),
        content_sha256: await computeContentSha256([
          p.liveSealed.entry,
          ...sealed,
        ] as never),
        epoch: 1,
        epochs: [
          ...p.liveManifest.raw.epochs,
          {
            epoch: 1,
            opened_at: AT,
            reason: "backfill",
            first_seq: 1,
            opened_by: `migration:${GRANT}`,
          },
        ],
        next_offset: added.at(-1)!.start_offset + added.at(-1)!.bytes,
        chunks: [p.liveSealed.entry, ...sealed],
      },
      extensions: {
        ...p.liveManifest.extensions,
        "client.relayhistory_export": {
          version: 1,
          source: "neon.delivery_records",
          exported_at: AT,
          grant_id: GRANT,
          plan_digest: PLAN,
          source_user_id: USER,
        },
      },
    };
    const post = (
      t: string,
      manifest: unknown,
      previousVersion: unknown = p.liveVersion,
    ) =>
      i.req("/v1/pipeline/manifests", t, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": location.machineId,
          "x-session-id": location.sessionId,
        },
        body: JSON.stringify({
          key: p.key,
          manifest,
          previousVersion,
        }),
      });
    const ok = await post(first, appended);
    expect(ok.status).toBe(202);
    const original = (await ok.json()) as { workKey: string };
    return { p, base, upload, appended, post, original };
  };

  test("v2 lost commit response (blocker 1): an identical retry under a fresh assertion with the same base returns the original success; a mismatched replay is refused", async () => {
    const i = await ingest(BASE_READ_ON);
    const { p, base, upload, appended, post, original } =
      await committedBackfillV2(i, "session-lost-response");
    // The response was lost: an identical retry under a FRESH assertion, same base.
    const retry = await post(migrationToken(upload), appended);
    expect(retry.status).toBe(202);
    expect(((await retry.json()) as { workKey: string }).workKey).toBe(
      original.workKey,
    );
    // A mismatched replay (other content than what is stored) is still refused.
    const other = { ...appended, updated_at: "2026-10-29T11:00:00.000Z" };
    expect((await post(migrationToken(upload), other)).status).toBe(403);
    // A replay that is identical but claims another base is refused too.
    const otherBase = migrationToken(
      await uploadIdentityV2Of(p.entry, PLAN, { ...base, chunk_count: 0 }),
    );
    expect((await post(otherBase, appended)).status).toBe(403);
  });

  test("v2 identical retry binds the signed base.version (blocker 1): a changed signed base.version, a missing previousVersion and a mismatched previousVersion are each refused, consuming and writing nothing", async () => {
    const i = await ingest(BASE_READ_ON);
    const { p, base, upload, appended, post } = await committedBackfillV2(
      i,
      "session-retry-version",
    );
    const stored = async () => {
      const object = await i.store.getObject(objectKey(p.key));
      return {
        version: object.meta.version,
        text: await new Response(object.body).text(),
      };
    };
    const snapshot = async () => ({
      uses: await count("sessions.migration_assertion_uses"),
      chunks: await count("sessions.migration_assertion_chunks"),
      baseReads: await count("sessions.migration_base_read_uses"),
      audit: await count("sessions.migration_ingest_audit"),
      jobs: await count("sessions.pipeline_jobs"),
      object: await stored(),
    });
    const before = await snapshot();
    const refused = async (label: string, r: Response) => {
      expect({ label, status: r.status }).toEqual({ label, status: 409 });
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe(
        "revision_conflict",
      );
      expect(await snapshot()).toEqual(before);
    };
    // 1. A fresh assertion whose signed base.version changed (the historical
    //    previousVersion is the real base): refused.
    const changedBase = migrationToken(
      await uploadIdentityV2Of(p.entry, PLAN, {
        ...base,
        version: `${base.version}-changed`,
      }),
    );
    await refused(
      "changed signed base.version",
      await post(changedBase, appended),
    );
    // 2. previousVersion missing.
    await refused(
      "missing previousVersion",
      await post(migrationToken(upload), appended, null),
    );
    // 3. previousVersion that does not resolve to the signed base.version
    //    (here: the appended object's own current version).
    await refused(
      "mismatched previousVersion",
      await post(migrationToken(upload), appended, before.object.version),
    );
    await refused(
      "mismatched previousVersion (arbitrary)",
      await post(migrationToken(upload), appended, { version: "not-the-base" }),
    );
    // The genuine retry (historical previousVersion = signed base) still succeeds.
    expect((await post(migrationToken(upload), appended)).status).toBe(202);
  });

  test("v2 base-read gate (blocker 2): off, everything under /base is exactly 404 for every method and path variant, before auth; on, exactly one trailing slash is stripped", async () => {
    const off = await ingest({
      ...MIGRATION_ON,
      ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
    });
    const p = await liveSessionWithPlan(off, {
      ...straddle,
      sessionId: "session-gate",
    });
    const q = `?key=${encodeURIComponent(p.key)}`;
    for (const path of [
      "/v1/pipeline/manifests/base",
      "/v1/pipeline/manifests/base/",
      "/v1/pipeline/manifests/base//",
      "/v1/pipeline/manifests/base/x",
    ])
      for (const method of ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"])
        for (const token of [
          baseReadToken(p.read),
          liveToken(),
          "",
          "garbage",
        ]) {
          const r = await off.app.request(
            `${path}${q}`,
            {
              method,
              headers: token ? { authorization: `Bearer ${token}` } : {},
            },
            off.env,
          );
          expect(
            `${method} ${path} [${token ? token.slice(0, 6) : "no-token"}]: ${r.status}`,
          ).toBe(
            `${method} ${path} [${token ? token.slice(0, 6) : "no-token"}]: 404`,
          );
        }
    // The token class is unusable elsewhere while the gate is off.
    const elsewhere = await off.req(
      `/v1/pipeline/manifests/status${q}`,
      baseReadToken(p.read),
    );
    expect(elsewhere.status).toBe(401);
    expect(await code(elsewhere)).toBe("invalid_claims");
    // Gate on: `/base/` (one trailing slash) is the same route as `/base`.
    const on = await ingest(BASE_READ_ON);
    const p2 = await liveSessionWithPlan(on, {
      ...straddle,
      sessionId: "session-gate-on",
    });
    const slash = await on.req(
      `/v1/pipeline/manifests/base/?key=${encodeURIComponent(p2.key)}`,
      baseReadToken(p2.read),
    );
    expect(slash.status).toBe(200);
    expect(
      Object.keys(
        ((await slash.json()) as { base: Record<string, unknown> }).base,
      ),
    ).toHaveLength(7);
  });

  test("v2 base read of a malformed stored manifest (blocker 4): 409 migration_base_unusable, not a retryable 503; real storage unavailability stays 503", async () => {
    const down = new Set<string>();
    const i = await ingest(BASE_READ_ON, {
      wrapStore: (store) =>
        new Proxy(store, {
          get(target, prop, receiver) {
            const value = Reflect.get(target, prop, receiver);
            if (prop !== "getObject")
              return typeof value === "function" ? value.bind(target) : value;
            return async (key: Parameters<typeof store.getObject>[0]) => {
              if ([...down].some((d) => String(key).includes(d)))
                throw new Error("storage down");
              return target.getObject(key);
            };
          },
        }),
    });
    const malformed = { ...straddle, sessionId: "session-malformed" };
    const p = await liveSessionWithPlan(i, malformed);
    // Overwrite the stored manifest with something that is not a manifest.
    for (const bytes of [
      "{not json",
      JSON.stringify({ schema_v: 1, layout: "v1" }),
      JSON.stringify({ schema_v: 99 }),
    ]) {
      await i.store.put(objectKey(p.key), new TextEncoder().encode(bytes), {
        contentType: "application/json",
      });
      const r = await getBase(i, baseReadToken(p.read), p.key);
      expect(`${bytes.slice(0, 12)}: ${r.status} ${await code(r)}`).toBe(
        `${bytes.slice(0, 12)}: 409 migration_base_unusable`,
      );
    }
    const { rows: none } = await db.query(
      "SELECT 1 FROM sessions.migration_base_read_uses",
    );
    expect(none).toHaveLength(0);
    // Storage unavailable: 503, nothing consumed.
    const flaky = { ...straddle, sessionId: "session-storage-down" };
    const p2 = await liveSessionWithPlan(i, flaky);
    down.add("session-storage-down");
    const r = await getBase(i, baseReadToken(p2.read), p2.key);
    expect(r.status).toBe(503);
    expect(await code(r)).toBe("storage_unavailable");
    const { rows } = await db.query(
      "SELECT 1 FROM sessions.migration_base_read_uses",
    );
    expect(rows).toHaveLength(0);
  });

  test("v2 race: a live write between the base read and the commit makes the read base stale, and the commit is refused", async () => {
    const i = await ingest(BASE_READ_ON);
    const location = { ...straddle, sessionId: "session-race" };
    const p = await liveSessionWithPlan(i, location);
    const { base } = (await (
      await getBase(i, baseReadToken(p.read), p.key)
    ).json()) as { base: BackfillBase };
    // The live client appends (revision 2) after the base read.
    const second = await i.chunk(liveToken(), location, {
      bytes: ndjsonRecord(2),
      format: "ndjson",
      seq: 1,
      offset: p.liveSealed.entry.bytes,
    });
    expect(second.status).toBe(201);
    const secondEntry = (
      (await second.json()) as {
        entry: Record<string, unknown> & {
          bytes: number;
          stored_bytes: number;
        };
      }
    ).entry;
    const liveV2 = {
      ...p.liveManifest,
      revision: 2,
      raw: {
        ...p.liveManifest.raw,
        chunk_count: 2,
        raw_bytes: p.liveManifest.raw.raw_bytes + secondEntry.bytes,
        stored_bytes:
          p.liveManifest.raw.stored_bytes + secondEntry.stored_bytes,
        content_sha256: await computeContentSha256([
          p.liveSealed.entry,
          secondEntry,
        ] as never),
        next_offset: p.liveSealed.entry.bytes + secondEntry.bytes,
        chunks: [p.liveSealed.entry, secondEntry],
      },
    };
    const liveWrite = await i.req("/v1/pipeline/manifests", liveToken(), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": location.machineId,
        "x-session-id": location.sessionId,
      },
      body: JSON.stringify({
        key: p.key,
        manifest: liveV2,
        previousVersion: p.liveVersion,
      }),
    });
    expect(liveWrite.status).toBe(202);
    // A commit on the stale base: refused before any write, nothing consumed.
    const token = migrationToken(await uploadIdentityV2Of(p.entry, PLAN, base));
    const appended = { ...liveV2, revision: 3 };
    const r = await i.req("/v1/pipeline/manifests", token, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": location.machineId,
        "x-session-id": location.sessionId,
      },
      body: JSON.stringify({
        key: p.key,
        manifest: appended,
        previousVersion: p.liveVersion,
      }),
    });
    expect([403, 409]).toContain(r.status);
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    // A fresh base read now returns revision 2.
    const again = await getBase(i, baseReadToken(p.read), p.key);
    expect(((await again.json()) as { base: BackfillBase }).base.revision).toBe(
      2,
    );
  });

  test("signed work key and durable audit: posted fields cannot alter the work key; one audit row per commit, no secrets", async () => {
    const { upload, manifest, entryText } = await planned();
    const i = await ingest(MIGRATION_ON);
    const auditBefore = await count("sessions.migration_ingest_audit");
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(
      201,
    );
    const commit = await i.req("/v1/pipeline/manifests", token, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": LOCATION.machineId,
        "x-session-id": LOCATION.sessionId,
      },
      body: JSON.stringify({
        key: manifestKey(LOCATION),
        manifest,
        etag: "attacker",
        version: "attacker",
        previousVersion: { eTag: "attacker" },
      }),
    });
    expect(commit.status).toBe(202);
    const { workKey, version } = (await commit.json()) as {
      workKey: string;
      version: { eTag?: string; version?: string };
    };
    // Signed-only: the signed key and entry digest, not the storage version.
    expect(workKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    expect(workKey).not.toContain("attacker");
    expect(workKey).not.toContain(
      String(version.version ?? version.eTag).replace(/"/g, ""),
    );
    expect(await count("sessions.migration_ingest_audit")).toBe(
      auditBefore + 1,
    );
    const { rows } = await db.query<Record<string, unknown>>(
      "SELECT * FROM sessions.migration_ingest_audit ORDER BY id DESC LIMIT 1",
    );
    expect(rows[0]).toMatchObject({
      grant_id: GRANT,
      plan_digest: PLAN,
      entry_sha256: upload.entry_sha256,
      work_key: workKey,
      user_id: USER,
      result: "committed",
    });
    const text = JSON.stringify(rows[0]);
    for (const secret of [
      token,
      token.split(".")[2]!,
      "eyJ",
      new TextDecoder().decode(SOURCE).trim(),
    ])
      expect(text).not.toContain(secret);
  });
});
