/**
 * RelayAuth migration assertions (relayauth#110) at AlleyOop's own manifest
 * route, `POST /v1/pipeline/manifests` (the route the courier hits), and its
 * status read. The admission check, the ledger insert, enrichMessage and the
 * `pipeline_jobs` insert run in ONE SQL transaction: here on local PGlite with
 * every `supabase/migrations` file applied. The error codes are the same as
 * relayhistory-cloud's (routes/pipeline.ts). The signed upload identity
 * (docs/contracts/relayhistory-export-plan-entry-v1.md) is recomputed from the
 * actual key and manifest. Synthetic ids only.
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
import { randomBytes } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  migrationUploadMatches,
  uploadIdentityOf,
  type MigrationAssertionLedger,
  type PlanEntry,
  type TransactionalSqlExecutor,
  type UploadIdentity,
} from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  readAll,
  objectStoreManifestReader,
  sealRawChunk,
  type AppEnv,
  type ManifestV1,
  type SessionLocation,
  type SqlExecutor,
  type StageMessage,
} from "../../shared/vendor.ts";
import {
  courierMigrationLedger,
  createCourierManifestRoutes,
} from "../manifests.ts";

const ORG = "org_test";
const WORKSPACE = "ws_test";
const USER = "user_a";
const MACHINE = "machine-a";
const SESSION = "session-a";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const UUID = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19";
const AT = "2026-10-29T10:00:00.000Z";
const ORIGIN = "https://history.example";
const PLAN = "a".repeat(64);
const OTHER_PLAN = "b".repeat(64);
const GRANT = `mgr_${"0".repeat(32)}`;
const SUPABASE = join(import.meta.dir, "../../../supabase");

let db: PGlite;
let root: string;

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(
    await readFile(join(SUPABASE, "tests/supabase_shim.sql"), "utf8"),
  );
  await db.exec(
    await readFile(
      join(SUPABASE, "bootstrap/session_directory_search_worker.sql"),
      "utf8",
    ),
  );
  await db.exec(
    await readFile(
      join(SUPABASE, "bootstrap/session_token_verifier.sql"),
      "utf8",
    ),
  );
  await db.exec(
    await readFile(
      join(SUPABASE, "bootstrap/session_ingest_roles.sql"),
      "utf8",
    ),
  );
  for (const file of (await readdir(join(SUPABASE, "migrations")))
    .filter((f) => f.endsWith(".sql"))
    .sort())
    await db.exec(await readFile(join(SUPABASE, "migrations", file), "utf8"));
  await db.exec("SET ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-migration-"));
  await db.exec(
    "TRUNCATE sessions.migration_assertion_uses; TRUNCATE sessions.migration_assertion_chunks; TRUNCATE sessions.pipeline_jobs",
  );
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

function executor(): TransactionalSqlExecutor {
  const wrap = (q: { query: PGlite["query"] }): SqlExecutor => ({
    query: async (text, params) => ({
      rows: (await q.query(text, params)).rows as never[],
    }),
  });
  return {
    ...wrap(db),
    transaction: (work) => db.transaction((tx) => work(wrap(tx))),
  };
}

const count = async (table: string) =>
  (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`))
    .rows[0]!.n;

const freshJti = () => `tok_${randomBytes(16).toString("hex")}`;
const freshNonce = () => randomBytes(24).toString("base64url");

type TestAuth = Record<string, unknown>;

/** The plan entry the issuer stores for a manifest (built here, independently of the ingest). */
async function entryOf(
  key: string,
  manifest: ManifestV1,
  overrides: Partial<PlanEntry> = {},
): Promise<PlanEntry> {
  const chunks = manifest.raw.chunks.map((c) => ({
    seq: c.seq,
    epoch: c.epoch,
    start_offset: c.start_offset,
    bytes: c.bytes,
    sha256: c.sha256,
    records: c.records ?? 0,
  }));
  return {
    grant_id: GRANT,
    key,
    org_id: ORG,
    workspace_id: WORKSPACE,
    source_user_id: USER,
    session: {
      machine_id: manifest.session.machine_id,
      harness: manifest.session.harness,
      session_id: manifest.session.session_id,
      started_at: manifest.session.started_at,
    },
    state: manifest.raw.state,
    chunks,
    content_sha256: await computeContentSha256(chunks),
    raw_bytes: chunks.reduce((n, c) => n + c.bytes, 0),
    ...overrides,
  };
}

/** The auth context the verifier sets for a migration assertion (auth/relayauth.ts). */
function migrationAuth(
  upload: UploadIdentity,
  claims: Record<string, unknown> = {},
  migration: Record<string, unknown> = {},
): TestAuth {
  return {
    userId: USER,
    orgId: ORG,
    workspaceId: WORKSPACE,
    tokenSubject: "agent_relayhistory_migration",
    scopes: ["rth:sync"],
    historyOrigin: ORIGIN,
    claims: {
      sub: "agent_relayhistory_migration",
      org: ORG,
      wks: WORKSPACE,
      sponsorId: USER,
      meta: {
        tokenClass: "migration_assertion",
        grantId: GRANT,
        planDigest: PLAN,
      },
      ...claims,
    },
    migration: {
      jti: freshJti(),
      nonce: freshNonce(),
      grantId: GRANT,
      planDigest: PLAN,
      sourceUserId: USER,
      workspaceId: WORKSPACE,
      expiresAt: Math.floor(Date.now() / 1000) + 65,
      upload,
      ...migration,
    },
  };
}

const ordinaryAuth: TestAuth = {
  userId: USER,
  orgId: ORG,
  workspaceId: WORKSPACE,
  tokenSubject: "agent_relayhistory_assertion",
  scopes: ["rth:sync"],
  historyOrigin: ORIGIN,
  claims: { meta: { machineId: MACHINE } },
};

async function fixture(
  options: { ledger?: MigrationAssertionLedger | null } = {},
) {
  const store = createFsStore({ root });
  const raw = { store, provider: "fs" as const, bucket: "raw" };
  const reader = objectStoreManifestReader(store, {
    provider: "fs",
    bucket: "raw",
  });
  const location: SessionLocation = {
    workspaceId: WORKSPACE,
    machineId: MACHINE,
    harness: "claude",
    sessionId: SESSION,
    startDate: "2026-10-29",
  };
  const sealed = await sealRawChunk(store, {
    location,
    format: "jsonl",
    seq: 0,
    epoch: 0,
    startOffset: 0,
    source: new TextEncoder().encode('{"type":"user","text":"hello"}\n'),
    traceId: TRACE,
  });
  const build = async (
    ext: Record<string, unknown> = {},
    session: Partial<ManifestV1["session"]> = {},
    owner = `migration:${GRANT}`,
  ): Promise<ManifestV1> => ({
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision: 1,
    created_at: AT,
    updated_at: AT,
    session: {
      workspace_id: WORKSPACE,
      machine_id: MACHINE,
      harness: "claude",
      session_id: SESSION,
      started_at: AT,
      ...session,
    },
    // A cold export is owned by its migration grant (#42's owner fence).
    writer: { delivery_instance_id: owner, plugin_version: "test" },
    raw: {
      owner,
      state: "open",
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
    extensions: {
      "96bulls": {
        session_uuid: UUID,
        session_uuid_source: "minted",
        user_id: USER,
        ...ext,
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
  });
  const key = manifestKey(location);
  const upload = await uploadIdentityOf(
    await entryOf(key, await build()),
    PLAN,
  );
  const sent: StageMessage[] = [];
  const ledger =
    options.ledger === null
      ? undefined
      : (options.ledger ?? courierMigrationLedger(executor(), reader));
  const routes = createCourierManifestRoutes({
    manifests: reader,
    raw,
    send: async (_env, _stage, message) => void sent.push(message),
    recordClaimless: async () => {},
    ...(ledger ? { migrationLedger: ledger } : {}),
  });
  let auth: TestAuth = ordinaryAuth;
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("correlationId", "test");
    c.set("auth", auth as never);
    await next();
  });
  app.route("/v1", routes);
  const env = {
    PIPELINE_ORG_ID: ORG,
    RELAYHISTORY_ORIGIN: ORIGIN,
    RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${PLAN}`,
  };
  const post = async (
    as: TestAuth,
    manifest: unknown,
    at = key,
    envOverride: Record<string, string | undefined> = {},
  ) => {
    auth = as;
    return app.request(
      "/v1/pipeline/manifests",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": (manifest as ManifestV1).session.machine_id,
          "x-session-id": (manifest as ManifestV1).session.session_id,
        },
        body: JSON.stringify({ key: at, manifest }),
      },
      { ...env, ...envOverride },
    );
  };
  const status = async (
    as: TestAuth,
    envOverride: Record<string, string | undefined> = {},
    at = key,
  ) => {
    auth = as;
    return app.request(
      `/v1/pipeline/manifests/status?key=${encodeURIComponent(at)}&machine=${MACHINE}&writer=${encodeURIComponent(`migration:${GRANT}`)}`,
      {},
      { ...env, ...envOverride },
    );
  };
  /** A migration auth context for this fixture's planned upload. */
  const as = (
    claims: Record<string, unknown> = {},
    migration: Record<string, unknown> = {},
  ) => migrationAuth(upload, claims, migration);
  return {
    store,
    reader,
    location,
    key,
    build,
    sent,
    post,
    status,
    upload,
    as,
  };
}

async function code(response: Response) {
  return ((await response.json()) as { error?: { code?: string } }).error?.code;
}

/** A live writer's manifest: only a migration sets the export marker. */
function liveManifest(manifest: ManifestV1): ManifestV1 {
  const { "client.relayhistory_export": _marker, ...extensions } =
    manifest.extensions ?? {};
  return { ...manifest, extensions };
}

describe("AlleyOop manifest route: migration assertions", () => {
  test("commits the ledger row and the enriched pipeline_jobs row together; a replay is 409 migration_assertion_replayed", async () => {
    const f = await fixture();
    const auth = f.as();
    const manifest = await f.build();
    const first = await f.post(auth, manifest);
    expect(first.status).toBe(202);
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
    // The job carries the enriched ctx (enrichMessage ran inside the transaction).
    const { rows } = await db.query<{
      message: { ctx: { session_uuid?: string; tenant_id?: string } };
    }>("SELECT message FROM sessions.pipeline_jobs");
    expect(rows[0]!.message.ctx.session_uuid).toBe(UUID);
    expect(rows[0]!.message.ctx.tenant_id).toBeTruthy();
    const { rows: uses } = await db.query<Record<string, unknown>>(
      "SELECT grant_id, org_id, workspace_id, user_id, plan_digest, plan_entry_digest FROM sessions.migration_assertion_uses",
    );
    expect(uses).toEqual([
      {
        grant_id: GRANT,
        org_id: ORG,
        workspace_id: WORKSPACE,
        user_id: USER,
        plan_digest: PLAN,
        plan_entry_digest: f.upload.entry_sha256,
      },
    ]);
    // The route's own send is not used for a migration commit.
    expect(f.sent).toHaveLength(0);

    const replay = await f.post(auth, manifest);
    expect(replay.status).toBe(409);
    expect(await code(replay)).toBe("migration_assertion_replayed");
    const sameNonce = f.as(
      {},
      { nonce: (auth.migration as { nonce: string }).nonce },
    );
    const reused = await f.post(sameNonce, manifest);
    expect(reused.status).toBe(409);
    expect(await code(reused)).toBe("migration_assertion_replayed");
    const late = await f.status(auth);
    expect(late.status).toBe(409);
    expect(await code(late)).toBe("migration_assertion_replayed");
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);

    // A fresh assertion retries idempotently (the identical-manifest branch).
    expect((await f.post(f.as(), manifest)).status).toBe(202);
    expect(await count("sessions.migration_assertion_uses")).toBe(2);
  });

  test("a crash after the pipeline_jobs insert, before COMMIT, rolls back both rows (503, nothing consumed)", async () => {
    let crash = true;
    const holder: { reader?: ReturnType<typeof objectStoreManifestReader> } =
      {};
    const crashing: MigrationAssertionLedger = {
      consumed: (claims) =>
        courierMigrationLedger(executor(), holder.reader!).consumed(claims),
      reserveChunk: (use) =>
        courierMigrationLedger(executor(), holder.reader!).reserveChunk(use),
      consume: (use, stage, message) =>
        courierMigrationLedger(
          executor(),
          holder.reader!,
          async (_stage, _message, work) => {
            await work(); // the real enrich + pipeline_jobs insert on the transaction
            if (crash)
              throw new Error("crash between the job insert and COMMIT");
          },
        ).consume(use, stage, message),
    };
    const f = await fixture({ ledger: crashing });
    holder.reader = f.reader;
    const auth = f.as();
    const manifest = await f.build();
    const auditBefore = await count("sessions.migration_ingest_audit");
    const failed = await f.post(auth, manifest);
    expect(failed.status).toBe(503);
    expect(await code(failed)).toBe("migration_ledger_unavailable");
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    // The durable audit row rolls back with them.
    expect(await count("sessions.migration_ingest_audit")).toBe(auditBefore);
    crash = false;
    // Nothing was spent: the same assertion commits the identical manifest once.
    expect((await f.post(auth, manifest)).status).toBe(202);
    expect(await count("sessions.migration_ingest_audit")).toBe(
      auditBefore + 1,
    );
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });

  test("fails closed with 503 migration_ledger_unavailable when the ledger is absent or down", async () => {
    const none = await fixture({ ledger: null });
    for (const response of [
      await none.post(none.as(), await none.build()),
      await none.status(none.as()),
    ]) {
      expect(response.status).toBe(503);
      expect(await code(response)).toBe("migration_ledger_unavailable");
    }
    // Ordinary (live) tokens are unaffected by the ledger. (Checked first: the
    // fixtures below share this test's store, and #42's owner fence then
    // refuses a live write over a migration-owned manifest.)
    expect(
      (
        await none.post(
          ordinaryAuth,
          liveManifest(await none.build({}, {}, MACHINE)),
        )
      ).status,
    ).toBe(202);
    expect(none.sent).toHaveLength(1);
    await rm(root, { recursive: true, force: true });
    const down: MigrationAssertionLedger = {
      consumed: async () => {
        throw new Error("ledger down");
      },
      reserveChunk: async () => {
        throw new Error("ledger down");
      },
      consume: async () => {
        throw new Error("ledger down");
      },
    };
    const f = await fixture({ ledger: down });
    for (const response of [
      await f.post(f.as(), await f.build()),
      await f.status(f.as()),
    ]) {
      expect(response.status).toBe(503);
      expect(await code(response)).toBe("migration_ledger_unavailable");
    }
    const commitDown: MigrationAssertionLedger = {
      consumed: async () => false,
      reserveChunk: async () => "reserved",
      consume: async () => {
        throw new Error("tx failed");
      },
    };
    const g = await fixture({ ledger: commitDown });
    const response = await g.post(g.as(), await g.build());
    expect(response.status).toBe(503);
    expect(await code(response)).toBe("migration_ledger_unavailable");
    expect(g.sent).toHaveLength(0);
  });

  test("returns the relayhistory-cloud codes: workspace pair, grant, and the recomputed upload identity", async () => {
    const f = await fixture();
    const manifest = await f.build();
    const otherKey = manifestKey({ ...f.location, workspaceId: "ws_other" });
    const freshKey = manifestKey({ ...f.location, machineId: "machine-fresh" });
    const otherContent = structuredClone(manifest);
    otherContent.raw.chunks[0]!.records = 41;
    const bUpload = await uploadIdentityOf(
      await entryOf(f.key, manifest, { source_user_id: "user_b" }),
      PLAN,
    );
    const cases: [
      string,
      TestAuth,
      ManifestV1,
      string?,
      Record<string, string | undefined>?,
    ][] = [
      ["migration_workspace_forbidden", f.as({ wks: "ws_other" }), manifest],
      [
        "migration_workspace_forbidden",
        f.as({ sponsorId: "user_b" }),
        manifest,
      ],
      [
        "migration_workspace_forbidden",
        f.as({}, { workspaceId: "ws_other" }),
        manifest,
      ],
      [
        "migration_workspace_forbidden",
        f.as(),
        {
          ...manifest,
          session: { ...manifest.session, workspace_id: "ws_other" },
        },
        otherKey,
      ],
      [
        "migration_grant_forbidden",
        f.as(),
        manifest,
        undefined,
        { RELAYHISTORY_MIGRATION_GRANTS: undefined },
      ],
      [
        "migration_grant_forbidden",
        f.as(),
        manifest,
        undefined,
        { RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${OTHER_PLAN}` },
      ],
      // An arbitrary fresh machine key.
      [
        "migration_upload_forbidden",
        f.as(),
        {
          ...manifest,
          session: { ...manifest.session, machine_id: "machine-fresh" },
        },
        freshKey,
      ],
      // Other content (even with every echoed extension field matching).
      [
        "migration_upload_forbidden",
        f.as(),
        {
          ...otherContent,
          extensions: {
            ...otherContent.extensions,
            "client.relayhistory_export": {
              version: 1,
              source: "neon.delivery_records",
              exported_at: AT,
              upload: f.upload,
            },
          },
        },
      ],
      // A user's token carrying another user's identity, and the reverse.
      ["migration_upload_forbidden", f.as({}, { upload: bUpload }), manifest],
      [
        "migration_upload_forbidden",
        f.as({}, { upload: { ...f.upload, plan_digest: OTHER_PLAN } }),
        manifest,
      ],
      [
        "migration_upload_forbidden",
        f.as({}, { upload: { ...f.upload, grant_id: "mgr_other" } }),
        manifest,
      ],
      // Provenance: 96bulls.user_id must be the signed canonical source user.
      [
        "migration_upload_forbidden",
        f.as(),
        await f.build({ user_id: "user_b" }),
      ],
      [
        "migration_upload_forbidden",
        f.as(),
        await f.build({ user_id: `relay_${USER}` }),
      ],
    ];
    for (const [expected, auth, body, at, envOverride] of cases) {
      const response = await f.post(auth, body, at ?? f.key, envOverride ?? {});
      expect(response.status).toBe(403);
      expect(await code(response)).toBe(expected);
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await f.store.head(objectKey(f.key))).toBeUndefined();
    expect(await f.store.head(objectKey(freshKey))).toBeUndefined();
  });

  test("provenance: the export marker's grant_id, plan_digest, source_user_id and 96bulls.user_id are required and equal to the signed values", async () => {
    const f = await fixture();
    const good = await f.build();
    const marker = good.extensions!["client.relayhistory_export"] as Record<
      string,
      unknown
    >;
    const ext96 = good.extensions!["96bulls"] as Record<string, unknown>;
    const without = (r: Record<string, unknown>, k: string) => {
      const c = { ...r };
      delete c[k];
      return c;
    };
    const cases: [string, Record<string, unknown> | undefined][] = [
      ["no extensions", undefined],
      ["no export marker", { "96bulls": ext96 }],
      [
        "marker without grant_id",
        {
          "96bulls": ext96,
          "client.relayhistory_export": without(marker, "grant_id"),
        },
      ],
      [
        "marker without plan_digest",
        {
          "96bulls": ext96,
          "client.relayhistory_export": without(marker, "plan_digest"),
        },
      ],
      [
        "marker without source_user_id",
        {
          "96bulls": ext96,
          "client.relayhistory_export": without(marker, "source_user_id"),
        },
      ],
      [
        "marker with another grant_id",
        {
          "96bulls": ext96,
          "client.relayhistory_export": {
            ...marker,
            grant_id: `mgr_${"f".repeat(32)}`,
          },
        },
      ],
      [
        "marker with another plan_digest",
        {
          "96bulls": ext96,
          "client.relayhistory_export": { ...marker, plan_digest: OTHER_PLAN },
        },
      ],
      [
        "marker with another source_user_id",
        {
          "96bulls": ext96,
          "client.relayhistory_export": { ...marker, source_user_id: "user_b" },
        },
      ],
      ["no 96bulls extension", { "client.relayhistory_export": marker }],
      [
        "96bulls without user_id",
        {
          "client.relayhistory_export": marker,
          "96bulls": without(ext96, "user_id"),
        },
      ],
      [
        "96bulls with another user_id",
        {
          "client.relayhistory_export": marker,
          "96bulls": { ...ext96, user_id: "user_b" },
        },
      ],
    ];
    for (const [name, extensions] of cases) {
      const manifest = { ...good } as Record<string, unknown>;
      if (extensions === undefined) delete manifest.extensions;
      else manifest.extensions = extensions;
      const response = await f.post(f.as(), manifest);
      expect(`${name}: ${response.status} ${await code(response)}`).toBe(
        `${name}: 403 migration_upload_forbidden`,
      );
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await f.store.head(objectKey(f.key))).toBeUndefined();
    const ok = await f.post(f.as(), good);
    expect(ok.status).toBe(202);
    // The work key is the signed key and entry digest.
    expect(((await ok.json()) as { workKey: string }).workKey).toBe(
      `${f.upload.key}@${f.upload.entry_sha256}`,
    );
  });

  test("composes with #42's owner/backfill fence: both must pass, in either order of failure", async () => {
    const f = await fixture();
    // Identity valid, but the cold export is owned by the machine, not migration:<grant>: #42 refuses.
    const machineOwned = await f.build({}, {}, MACHINE);
    const machineUpload = await uploadIdentityOf(
      await entryOf(f.key, machineOwned),
      PLAN,
    );
    const fenced = await f.post(migrationAuth(machineUpload), machineOwned);
    expect(fenced.status).toBe(409);
    expect(await code(fenced)).toBe("owner_conflict");
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    // #42's fence satisfied but the identity is not the signed one: migration admission refuses.
    const other = await f.post(
      f.as({}, { upload: { ...f.upload, entry_sha256: "e".repeat(64) } }),
      await f.build(),
    );
    expect(other.status).toBe(403);
    expect(await code(other)).toBe("migration_upload_forbidden");
    // Both satisfied: the cold export commits.
    expect((await f.post(f.as(), await f.build())).status).toBe(202);
    // A live (ordinary) writer cannot then take the migration-owned stream
    // over: with the gate closed it is parked beside the export (#52), so the
    // client does not stall and the stored owner does not move.
    const live = await f.build({}, {}, MACHINE);
    const takeover = await f.post(ordinaryAuth, { ...live, revision: 2 });
    expect(takeover.status).toBe(202);
    expect(await takeover.json()).toMatchObject({ parked: true });
    const stored = JSON.parse(
      new TextDecoder().decode(
        await readAll(await f.store.get(objectKey(f.key))),
      ),
    );
    expect(stored.raw.owner).toMatch(/^migration:/);
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
  });

  test("a backfill epoch appended to a live manifest (O4 straddling) is refused by the v1 identity", async () => {
    const f = await fixture();
    // A live session exists at the key.
    const live = await f.build({}, {}, MACHINE);
    expect((await f.post(ordinaryAuth, liveManifest(live))).status).toBe(202);
    // Same content as a new revision without the CAS version: #42 refuses.
    const sameContent = await f.post(f.as(), {
      ...live,
      revision: 2,
    } as ManifestV1);
    expect(sameContent.status).toBe(409);
    // A real backfill (the live chunks plus a backfill epoch's chunk) is not
    // the v1 entry the assertion signed (that covers the whole chunk list):
    // migration admission refuses it before #42's backfill fence.
    const extra = {
      ...live.raw.chunks[0]!,
      seq: 1,
      epoch: 1,
      start_offset: live.raw.chunks[0]!.bytes,
    };
    const chunks = [...live.raw.chunks, extra];
    const backfill = {
      ...live,
      revision: 2,
      raw: {
        ...live.raw,
        chunks,
        chunk_count: 2,
        epoch: 1,
        raw_bytes: live.raw.raw_bytes * 2,
        stored_bytes: live.raw.stored_bytes * 2,
        next_offset: extra.start_offset + extra.bytes,
        content_sha256: await computeContentSha256(chunks),
        epochs: [
          ...live.raw.epochs,
          {
            epoch: 1,
            opened_at: AT,
            reason: "backfill",
            first_seq: 1,
            opened_by: `migration:${GRANT}`,
          },
        ],
      },
    } as unknown as ManifestV1;
    const response = await f.post(f.as(), backfill);
    expect([400, 403]).toContain(response.status);
    if (response.status === 403)
      expect(await code(response)).toBe("migration_upload_forbidden");
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    // And directly: the recomputed identity of a backfill manifest is not the signed one.
    expect(
      await migrationUploadMatches(
        f.as() as never,
        { ...f.location, rest: "manifest.json" } as never,
        backfill,
      ),
    ).toBe(false);
  });

  test("normalizes relay_ consistently: prefixed signed claims match the canonical identity", async () => {
    const f = await fixture();
    const prefixed = f.as(
      {
        sponsorId: `relay_${USER}`,
        wks: `relay_${WORKSPACE}`,
        org: `relay_${ORG}`,
      },
      { sourceUserId: `relay_${USER}`, workspaceId: `relay_${WORKSPACE}` },
    );
    expect(
      (await f.post(prefixed, await f.build({ user_id: USER }))).status,
    ).toBe(202);
    // A different id is a mismatch, prefix or not.
    const other = f.as(
      { sponsorId: "relay_user_x" },
      { sourceUserId: "relay_user_x" },
    );
    const response = await f.post(other, await f.build());
    expect(response.status).toBe(403);
  });

  test("status: admitted only for the signed upload's own key and identity", async () => {
    const f = await fixture();
    expect((await f.status(f.as())).status).toBe(404);
    expect((await f.post(f.as(), await f.build())).status).toBe(200 + 2);
    const own = await f.status(f.as());
    expect(own.status).toBe(200);
    const fresh = await f.status(
      f.as(),
      {},
      manifestKey({ ...f.location, sessionId: "session-fresh" }),
    );
    expect(fresh.status).toBe(403);
    expect(await code(fresh)).toBe("migration_upload_forbidden");
    const otherUpload = await f.status(
      f.as({}, { upload: { ...f.upload, entry_sha256: "c".repeat(64) } }),
    );
    expect(otherUpload.status).toBe(403);
    expect(await code(otherUpload)).toBe("migration_upload_forbidden");
    const grant = await f.status(f.as({}, { grantId: "mgr_other" }));
    expect(grant.status).toBe(403);
    expect(await code(grant)).toBe("migration_grant_forbidden");
  });
});
