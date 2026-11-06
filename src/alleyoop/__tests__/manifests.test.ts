import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  computeContentSha256,
  MANIFEST_EPOCHS_CAPABILITY,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  sealRawChunk,
  type AppEnv,
  type ManifestV1,
  type SessionLocation,
  type StageMessage,
} from "../../shared/vendor.ts";
import { createCourierManifestRoutes } from "../manifests.ts";

const ORG = "org_test";
const WORKSPACE = "ws_test";
const MACHINE = "machine-a";
const SESSION = "session-a";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const UUID = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19";
const AT = "2026-10-29T10:00:00.000Z";
const TELEMETRY_KEY = "test-telemetry-key-at-least-32-bytes-long";

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-manifests-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

async function fixture() {
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
    format: "ndjson",
    seq: 0,
    epoch: 0,
    startOffset: 0,
    source: new TextEncoder().encode('{"type":"user","text":"hello"}\n'),
    traceId: TRACE,
  });
  const build = async (
    revision: number,
    writer = MACHINE,
  ): Promise<ManifestV1> => ({
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision,
    created_at: AT,
    updated_at: new Date(Date.parse(AT) + revision).toISOString(),
    session: {
      workspace_id: WORKSPACE,
      machine_id: MACHINE,
      harness: "claude",
      session_id: SESSION,
      started_at: AT,
      ...(revision > 1 ? { ended_at: AT } : {}),
    },
    writer: {
      delivery_instance_id: writer,
      plugin_version: "96bulls-courier/0.3.0",
      capabilities: [MANIFEST_EPOCHS_CAPABILITY],
    },
    raw: {
      owner: MACHINE,
      state: revision > 1 ? "closed" : "open",
      updated_at: AT,
      format: "ndjson",
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
          opened_by: MACHINE,
        },
      ],
      next_offset: sealed.entry.bytes,
      chunks: [sealed.entry],
      scrub: sealed.scrub,
    },
    extensions: {
      "96bulls": { session_uuid: UUID, session_uuid_source: "minted" },
    },
  });
  const sent: StageMessage[] = [];
  const routes = createCourierManifestRoutes({
    manifests: reader,
    raw,
    send: async (_env, _stage, message) => void sent.push(message),
    missingReaders: async () => [],
    recordClaimless: async () => {},
  });
  const app = new Hono<AppEnv>();
  let auth = {
    userId: "user",
    orgId: ORG,
    workspaceId: WORKSPACE,
    tokenSubject: "agent_relayhistory_assertion",
    scopes: ["rth:sync"],
    claims: { meta: { machineId: MACHINE } },
    historyOrigin: "https://history.example",
  };
  app.use("*", async (c, next) => {
    c.set("correlationId", "test");
    const explicit = c.req.header("x-test-auth");
    c.set(
      "auth",
      explicit === "migration"
        ? {
            userId: "user",
            orgId: ORG,
            workspaceId: WORKSPACE,
            tokenSubject: "agent_relayhistory_migration",
            scopes: ["rth:sync"],
            claims: {
              meta: {
                tokenClass: "migration_assertion",
                grantId: "mgr_export",
                planDigest: "a".repeat(64),
              },
            },
            historyOrigin: "https://history.example",
          }
        : explicit === "malformed"
          ? {
              userId: "user",
              orgId: ORG,
              workspaceId: WORKSPACE,
              tokenSubject: "agent_relayhistory_migration",
              scopes: ["rth:sync"],
              claims: { meta: { tokenClass: "migration_assertion" } },
              historyOrigin: "https://history.example",
            }
          : explicit === "live"
            ? {
                userId: "user",
                orgId: ORG,
                workspaceId: WORKSPACE,
                tokenSubject: "agent_relayhistory_assertion",
                scopes: ["rth:sync"],
                claims: { meta: { machineId: MACHINE } },
                historyOrigin: "https://history.example",
              }
            : auth,
    );
    await next();
  });
  app.route("/v1", routes);
  const env = {
    PIPELINE_ORG_ID: ORG,
    RELAYHISTORY_ORIGIN: "https://history.example",
    PIPELINE_TELEMETRY_ID_KEY: TELEMETRY_KEY,
  };
  const post = (
    body: unknown,
    explicitAuth?: "live" | "migration" | "malformed",
  ) =>
    app.request(
      "/v1/pipeline/manifests",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": MACHINE,
          "x-session-id": SESSION,
          ...(explicitAuth ? { "x-test-auth": explicitAuth } : {}),
        },
        body: JSON.stringify(body),
      },
      env,
    );
  const asLive = () => {
    auth = {
      userId: "user",
      orgId: ORG,
      workspaceId: WORKSPACE,
      tokenSubject: "agent_relayhistory_assertion",
      scopes: ["rth:sync"],
      claims: { meta: { machineId: MACHINE } },
      historyOrigin: "https://history.example",
    };
  };
  const asMigration = () => {
    auth = {
      userId: "user",
      orgId: ORG,
      workspaceId: WORKSPACE,
      tokenSubject: "agent_relayhistory_migration",
      scopes: ["rth:sync"],
      claims: {
        meta: {
          tokenClass: "migration_assertion",
          grantId: "mgr_export",
          planDigest: "a".repeat(64),
        },
      },
      historyOrigin: "https://history.example",
    };
  };
  const appendEpoch = async (
    previous: ManifestV1,
    reason: "backfill" | "rewritten",
    openedBy: string,
    text: string,
  ): Promise<ManifestV1> => {
    const epoch = previous.raw.epoch + 1;
    const seq = previous.raw.chunks.length;
    const sealed = await sealRawChunk(store, {
      location,
      format: "ndjson",
      seq,
      epoch,
      startOffset: 0,
      source: new TextEncoder().encode(`${text}\n`),
      traceId: TRACE,
    });
    const chunks = [...previous.raw.chunks, sealed.entry];
    return {
      ...structuredClone(previous),
      revision: previous.revision + 1,
      updated_at: new Date(Date.parse(previous.updated_at) + 1).toISOString(),
      raw: {
        ...structuredClone(previous.raw),
        // The exporter leaves client activity alone; the owner's write moves it.
        ...(reason === "backfill"
          ? {}
          : {
              updated_at: new Date(
                Date.parse(previous.raw.updated_at) + 1,
              ).toISOString(),
            }),
        chunk_count: chunks.length,
        raw_bytes: chunks.reduce((sum, chunk) => sum + chunk.bytes, 0),
        stored_bytes: chunks.reduce(
          (sum, chunk) => sum + chunk.stored_bytes,
          0,
        ),
        content_sha256: await computeContentSha256(chunks),
        epoch,
        epochs: [
          ...structuredClone(previous.raw.epochs),
          { epoch, opened_at: AT, reason, first_seq: seq, opened_by: openedBy },
        ],
        next_offset: sealed.entry.bytes,
        chunks,
      },
    };
  };
  return {
    store,
    reader,
    location,
    build,
    sent,
    app,
    env,
    post,
    asLive,
    asMigration,
    appendEpoch,
  };
}

describe("AlleyOop courier manifest routes", () => {
  test("creates a server-validated manifest with identity and keyed telemetry metadata", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const manifest = await f.build(1);
    const response = await f.post({ key, manifest });
    expect(response.status).toBe(202);
    const body = (await response.json()) as { version: { eTag?: string } };
    expect(body.version.eTag).toBeTruthy();
    const head = await f.store.head(objectKey(key));
    expect(head?.metadata).toMatchObject({
      "session-uuid": UUID,
      "trace-id": TRACE,
      revision: "1",
    });
    expect(head?.metadata["machine-token"]).toMatch(/^h1_[0-9a-f]{32}$/);
    expect(head?.metadata["session-token"]).toMatch(/^h1_[0-9a-f]{32}$/);
    expect(f.sent).toHaveLength(1);

    const again = await f.post({ key, manifest });
    expect(again.status).toBe(202);
    expect(f.sent).toHaveLength(2);
  });

  test("uses compare-and-swap and writer continuity for later revisions", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const first = await f.post({ key, manifest: await f.build(1) });
    const firstBody = (await first.json()) as { version: { eTag?: string } };

    expect((await f.post({ key, manifest: await f.build(2) })).status).toBe(
      409,
    );
    expect(
      (
        await f.post({
          key,
          manifest: await f.build(2, "machine-b"),
          previousVersion: firstBody.version,
        })
      ).status,
    ).toBe(403);
    const update = await f.post({
      key,
      manifest: await f.build(2),
      previousVersion: firstBody.version,
    });
    expect(update.status).toBe(202);
    const stored = await f.reader.read(key);
    expect(stored?.manifest.revision).toBe(2);
  });

  test("appends one migration-fenced backfill epoch without taking the live owner", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const live = await f.build(1);
    const first = await f.post({ key, manifest: live });
    const firstBody = (await first.json()) as { version: { eTag?: string } };

    f.asMigration();
    const backfill = await f.appendEpoch(
      live,
      "backfill",
      "migration:mgr_export",
      '{"kind":"session_event","origin_id":"o","record_id":"old","revision":"1","received_at":"2026-10-01T00:00:00Z","payload":{"ts_ms":1}}',
    );
    backfill.extensions = {
      ...backfill.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };
    const appended = await f.post({
      key,
      manifest: backfill,
      previousVersion: firstBody.version,
    });
    expect(appended.status).toBe(202);
    const appendedBody = (await appended.json()) as {
      version: { eTag?: string };
    };
    const stored = (await f.reader.read(key))!.manifest;
    expect(stored.raw.owner).toBe(MACHINE);
    expect(stored.writer.delivery_instance_id).toBe(MACHINE);
    expect(stored.raw.epochs.at(-1)).toMatchObject({
      reason: "backfill",
      opened_by: "migration:mgr_export",
    });

    // Exact replay stays idempotent, but cannot be used to bypass the
    // migration assertion and enqueue the export under a live identity.
    f.asLive();
    expect((await f.post({ key, manifest: backfill })).status).toBe(409);
    f.asMigration();
    expect((await f.post({ key, manifest: backfill })).status).toBe(202);

    // The live owner can continue only from the exact post-backfill version.
    f.asLive();
    const resumed = await f.appendEpoch(
      stored,
      "rewritten",
      MACHINE,
      '{"type":"assistant","text":"after T0"}',
    );
    const response = await f.post({
      key,
      manifest: resumed,
      previousVersion: appendedBody.version,
    });
    expect(response.status).toBe(202);
    expect(
      (await f.reader.read(key))?.manifest.raw.epochs.map(
        (epoch) => epoch.reason,
      ),
    ).toEqual(["initial", "backfill", "rewritten"]);
  });

  test("refuses the export marker from a live writer", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const marker = {
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
        grant_id: "mgr_export",
        plan_digest: "a".repeat(64),
        source_user_id: "user",
      },
    };
    const first = await f.build(1, MACHINE);
    first.extensions = { ...first.extensions, ...marker };
    expect((await f.post({ key, manifest: first })).status).toBe(409);

    const plain = await f.build(1, MACHINE);
    const created = await f.post({ key, manifest: plain });
    expect(created.status).toBe(202);
    const { version } = (await created.json()) as {
      version: { eTag?: string };
    };
    const next = await f.build(2, MACHINE);
    next.extensions = { ...next.extensions, ...marker };
    expect(
      (await f.post({ key, manifest: next, previousVersion: version })).status,
    ).toBe(409);
  });

  test("refuses a migration write that moves client-owned raw fields", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const first = await f.build(1);
    const firstBody = (await (
      await f.post({ key, manifest: first })
    ).json()) as { version: object };
    const edits: ((m: ManifestV1) => void)[] = [
      (m) =>
        void (m.raw.updated_at = new Date(
          Date.parse(m.raw.updated_at) + 1000,
        ).toISOString()),
      (m) => void (m.raw.future = "x"),
    ];
    for (const edit of edits) {
      const backfill = await f.appendEpoch(
        first,
        "backfill",
        "migration:mgr_export",
        '{"pre":"t0"}',
      );
      backfill.extensions = {
        ...backfill.extensions,
        "client.relayhistory_export": {
          version: 1,
          source: "neon.delivery_records",
        },
      };
      edit(backfill);
      expect(
        (
          await f.post(
            { key, manifest: backfill, previousVersion: firstBody.version },
            "migration",
          )
        ).status,
      ).toBe(409);
    }
  });

  test("refuses forged and malformed epoch attribution", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const first = await f.build(1);
    const firstBody = (await (
      await f.post({ key, manifest: first })
    ).json()) as { version: object };
    const forged = await f.appendEpoch(
      first,
      "rewritten",
      "migration:mgr_export",
      '{"live":1}',
    );
    expect(
      (
        await f.post({
          key,
          manifest: forged,
          previousVersion: firstBody.version,
        })
      ).status,
    ).toBe(409);
    const stranger = await f.appendEpoch(
      first,
      "rewritten",
      "someone-else",
      '{"live":2}',
    );
    expect(
      (
        await f.post({
          key,
          manifest: stranger,
          previousVersion: firstBody.version,
        })
      ).status,
    ).toBe(409);
    for (const openedBy of [
      "migration:",
      "migration:bad id",
      `migration:${"x".repeat(256)}`,
    ]) {
      const malformed = await f.appendEpoch(
        first,
        "backfill",
        openedBy,
        '{"pre":"t0"}',
      );
      malformed.extensions = {
        ...malformed.extensions,
        "client.relayhistory_export": {
          version: 1,
          source: "neon.delivery_records",
        },
      };
      expect(
        (
          await f.post(
            { key, manifest: malformed, previousVersion: firstBody.version },
            "migration",
          )
        ).status,
      ).toBe(400);
    }
  });

  test("refuses a live write into the sealed backfill epoch, and a backfill on a jsonl stream", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const first = await f.build(1);
    const firstBody = (await (
      await f.post({ key, manifest: first })
    ).json()) as { version: object };
    const backfill = await f.appendEpoch(
      first,
      "backfill",
      "migration:mgr_export",
      '{"pre":"t0"}',
    );
    backfill.extensions = {
      ...backfill.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };
    const jsonl = structuredClone(backfill);
    jsonl.raw.format = "jsonl";
    expect(
      (
        await f.post(
          { key, manifest: jsonl, previousVersion: firstBody.version },
          "migration",
        )
      ).status,
    ).toBe(400);
    const appended = await f.post(
      { key, manifest: backfill, previousVersion: firstBody.version },
      "migration",
    );
    expect(appended.status).toBe(202);
    const appendedBody = (await appended.json()) as { version: object };

    // Offsets are contiguous, so only the two-revision check can refuse this.
    const next = structuredClone(backfill);
    next.revision += 1;
    const tail = next.raw.chunks.at(-1)!;
    const end = tail.start_offset + tail.bytes;
    const extra = await sealRawChunk(f.store, {
      location: f.location,
      format: "ndjson",
      seq: next.raw.chunks.length,
      epoch: tail.epoch,
      startOffset: end,
      source: new TextEncoder().encode('{"after":"t0"}\n'),
      traceId: TRACE,
    });
    next.raw.chunks.push(extra.entry);
    next.raw.chunk_count += 1;
    next.raw.raw_bytes += extra.entry.bytes;
    next.raw.stored_bytes += extra.entry.stored_bytes;
    next.raw.next_offset = end + extra.entry.bytes;
    next.raw.content_sha256 = await computeContentSha256(next.raw.chunks);
    const refused = await f.post({
      key,
      manifest: next,
      previousVersion: appendedBody.version,
    });
    expect(refused.status).toBe(409);
  });

  test("creates a cold export only under the migration grant owner", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const wrongOwner = await f.build(1, "migration:another_grant");
    wrongOwner.raw.owner = "migration:another_grant";
    wrongOwner.raw.epochs[0]!.opened_by = "migration:another_grant";
    wrongOwner.extensions = {
      ...wrongOwner.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };
    expect(
      (await f.post({ key, manifest: wrongOwner }, "migration")).status,
    ).toBe(409);

    const cold = await f.build(1, "migration:mgr_export");
    cold.raw.owner = "migration:mgr_export";
    cold.raw.epochs[0]!.opened_by = "migration:mgr_export";
    cold.extensions = {
      ...cold.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };
    const retries = await Promise.all([
      f.post({ key, manifest: cold }, "migration"),
      f.post({ key, manifest: cold }, "migration"),
    ]);
    expect(retries.map((response) => response.status)).toEqual([202, 202]);
  });

  test("rejects migration rewrites, owner changes, and unbound migration assertions", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const live = await f.build(1);
    const first = await f.post({ key, manifest: live });
    const firstBody = (await first.json()) as { version: { eTag?: string } };
    f.asMigration();
    const backfill = await f.appendEpoch(
      live,
      "backfill",
      "migration:mgr_export",
      '{"kind":"session_event","origin_id":"o","record_id":"old","revision":"1","received_at":"2026-10-01T00:00:00Z","payload":{"ts_ms":1}}',
    );
    backfill.extensions = {
      ...backfill.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };
    const changedOwner = structuredClone(backfill);
    changedOwner.raw.owner = "migration:mgr_export";
    expect(
      (
        await f.post({
          key,
          manifest: changedOwner,
          previousVersion: firstBody.version,
        })
      ).status,
    ).toBe(409);
    const rewritten = structuredClone(backfill);
    rewritten.raw.chunks[0]!.records = 999;
    expect(
      (
        await f.post({
          key,
          manifest: rewritten,
          previousVersion: firstBody.version,
        })
      ).status,
    ).toBe(409);

    // A normal rth:sync assertion cannot use the migration-only epoch reason.
    f.asLive();
    expect(
      (
        await f.post({
          key,
          manifest: backfill,
          previousVersion: firstBody.version,
        })
      ).status,
    ).toBe(409);
    expect(
      (
        await f.post(
          { key, manifest: backfill, previousVersion: firstBody.version },
          "malformed",
        )
      ).status,
    ).toBe(409);
  });

  test("serializes a live append and backfill append on the same object version", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const live = await f.build(1);
    const first = await f.post({ key, manifest: live });
    const firstBody = (await first.json()) as { version: { eTag?: string } };
    const backfill = await f.appendEpoch(
      live,
      "backfill",
      "migration:mgr_export",
      '{"kind":"session_event","origin_id":"o","record_id":"old","revision":"1","received_at":"2026-10-01T00:00:00Z","payload":{"ts_ms":1}}',
    );
    backfill.extensions = {
      ...backfill.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };
    const liveAppend = await f.appendEpoch(
      live,
      "rewritten",
      MACHINE,
      '{"type":"assistant","text":"live"}',
    );

    // Drive the two authenticated attempts concurrently. Exactly one CAS can
    // replace the version they both read; the loser must reread and rebuild.
    const migrationAttempt = f.post(
      { key, manifest: backfill, previousVersion: firstBody.version },
      "migration",
    );
    const liveAttempt = f.post(
      { key, manifest: liveAppend, previousVersion: firstBody.version },
      "live",
    );
    const statuses = (await Promise.all([migrationAttempt, liveAttempt]))
      .map((response) => response.status)
      .sort();
    expect(statuses).toEqual([202, 409]);
    expect((await f.reader.read(key))?.manifest.revision).toBe(2);
  });

  test("treats concurrent byte-identical migration CAS retries as success", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const live = await f.build(1);
    const first = await f.post({ key, manifest: live });
    const firstBody = (await first.json()) as { version: { eTag?: string } };
    const backfill = await f.appendEpoch(
      live,
      "backfill",
      "migration:mgr_export",
      '{"kind":"session_event","origin_id":"o","record_id":"old","revision":"1","received_at":"2026-10-01T00:00:00Z","payload":{"ts_ms":1}}',
    );
    backfill.extensions = {
      ...backfill.extensions,
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    };

    const attempts = await Promise.all([
      f.post(
        { key, manifest: backfill, previousVersion: firstBody.version },
        "migration",
      ),
      f.post(
        { key, manifest: backfill, previousVersion: firstBody.version },
        "migration",
      ),
    ]);
    expect(attempts.map((response) => response.status)).toEqual([202, 202]);
    const bodies = await Promise.all(
      attempts.map(
        (response) =>
          response.json() as Promise<{
            workKey: string;
            version: { eTag?: string };
          }>,
      ),
    );
    expect(bodies[1]).toEqual(bodies[0]);
    expect((await f.reader.read(key))?.manifest).toEqual(backfill);
  });

  test("checks ownership and server-sealed chunks before writing", async () => {
    const f = await fixture();
    const manifest = await f.build(1);
    const foreign = manifestKey({ ...f.location, workspaceId: "ws_other" });
    expect(
      (
        await f.post({
          key: foreign,
          manifest: {
            ...manifest,
            session: { ...manifest.session, workspace_id: "ws_other" },
          },
        })
      ).status,
    ).toBe(403);
    expect(await f.store.head(objectKey(foreign))).toBeUndefined();

    const key = manifestKey(f.location);
    const unsealed = {
      ...manifest,
      raw: { ...manifest.raw, scrub: undefined },
    };
    expect((await f.post({ key, manifest: unsealed })).status).toBe(409);
    expect(await f.store.head(objectKey(key))).toBeUndefined();
  });

  test("returns a version only to the matching machine and writer", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    await f.post({ key, manifest: await f.build(1) });
    const found = await f.app.request(
      `/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=${MACHINE}&writer=${MACHINE}`,
      {},
      f.env,
    );
    expect(found.status).toBe(200);
    expect(
      ((await found.json()) as { version: { eTag?: string } }).version.eTag,
    ).toBeTruthy();
    const wrong = await f.app.request(
      `/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=machine-b&writer=machine-b`,
      {},
      f.env,
    );
    expect(wrong.status).toBe(403);
  });
});
