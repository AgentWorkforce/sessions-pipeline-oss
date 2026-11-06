/**
 * The ingest's write path end to end against a real (filesystem) object store:
 * every batch lands in S3 layout, including the batches relayhistory's
 * s3_first path sends to Neon (scrub_not_identity, chunk_string_too_long,
 * record_without_session, too_many_sessions).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { gzipSync, zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ManifestV1, type ObjectStore } from "../../shared/vendor.ts";
import { MAX_SCRUB_CHARS } from "@rhc/lib/scrub.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, SYNC_SCOPE } from "../app.ts";
import { deliverBatch, IngestUnavailable } from "../deliver.ts";
import { canonicalSnapshot, type CanonicalPointer } from "../../session-read-api/canonical.ts";
import { MAX_REPLICA_MESSAGE_BYTES, type NeonReplicaMessage, type ReplicaSink } from "../replica.ts";
import { DeliveryError, MAX_DELIVERY_BYTES } from "../vendor/delivery/delivery-contracts.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { LIVE_OWNER, NO_SESSION_HARNESS, liveMachineId } from "../writer.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const noSleep = async () => {};

function setup(over: { store?: ObjectStore; replica?: ReplicaSink } = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const store = over.store ?? s.store;
  const ledger = new MemoryLedger();
  const deps = { ledger, target: { store, keyRoot: "96bulls", sleep: noSleep }, replica: over.replica };
  return { store: s.store, ledger, deps };
}

async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.cursor;
  } while (cursor);
  return out.sort();
}

async function readText(store: ObjectStore, key: string): Promise<string> {
  const bytes = await readAll((await store.getObject(objectKey(key))).body);
  return key.endsWith(".zst") ? new TextDecoder().decode(zstdDecompressSync(bytes)) : new TextDecoder().decode(bytes);
}

async function chunks(store: ObjectStore): Promise<string[]> {
  const keys = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith(".ndjson.zst"));
  return Promise.all(keys.map((k) => readText(store, k)));
}

async function onlyManifest(store: ObjectStore): Promise<{ key: string; manifest: ManifestV1 }> {
  const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((key) => key.endsWith("manifest.json"));
  expect(manifests).toHaveLength(1);
  const key = manifests[0]!;
  return { key, manifest: JSON.parse(await readText(store, key)) as ManifestV1 };
}

function identityExtension(manifest: ManifestV1): Record<string, unknown> {
  return manifest.extensions?.["96bulls"] as Record<string, unknown>;
}

async function putManifest(store: ObjectStore, key: string, manifest: ManifestV1): Promise<void> {
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
}

async function canonicalFromWrittenManifest(store: ObjectStore, key: string, manifest: ManifestV1, sessionId: string) {
  const pointer: CanonicalPointer = {
    org_id: TENANT.orgId,
    workspace_id: TENANT.workspaceId,
    harness: "claude",
    session_id: sessionId,
    machine_id: manifest.session.machine_id,
    user_id: TENANT.userId,
    storage_provider: "s3",
    bucket: "test-bucket",
    manifest_key: key,
    content_sha256: manifest.raw.content_sha256,
    raw_format: "ndjson",
    revision: manifest.revision,
    deleted_at: null,
  };
  return canonicalSnapshot(
    {
      store,
      provider: "s3",
      bucket: "test-bucket",
      prefix: { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" },
      pointers: async () => [pointer],
      decompress: (bytes) => new Uint8Array(zstdDecompressSync(bytes)),
    },
    { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId },
  );
}

describe("deliverBatch", () => {
  test("binds the authenticated user to a fresh manifest and canonical pointer read", async () => {
    const { store, deps } = setup();
    await deliverBatch(deps, TENANT, await batch([record({ record_id: "identity-user" })]));
    const { key, manifest } = await onlyManifest(store);

    expect(identityExtension(manifest).user_id).toBe(TENANT.userId);
    const snapshot = await canonicalFromWrittenManifest(store, key, manifest, "sess-1");
    expect(snapshot.rows).toHaveLength(1);
    expect(snapshot.rows[0]).toMatchObject({ row: { user_id: TENANT.userId, session_id: "sess-1" } });
  });

  test("round-trips a layout-encoded session id through the canonical pointer boundary", async () => {
    const { store, deps } = setup();
    await deliverBatch(deps, TENANT, await batch([record({ record_id: "identity-session", session_id: "sess:1" })]));
    const { key, manifest } = await onlyManifest(store);

    expect(manifest.session.session_id).toBe("sess_1");
    expect(identityExtension(manifest).canonical_session_id).toBe("sess:1");
    const snapshot = await canonicalFromWrittenManifest(store, key, manifest, "sess:1");
    expect(snapshot.rows).toHaveLength(1);
    expect(snapshot.rows[0]).toMatchObject({ row: { user_id: TENANT.userId, session_id: "sess:1" } });
  });

  test("preserves existing 96bulls metadata when appending another batch", async () => {
    const { store, deps } = setup();
    await deliverBatch(deps, TENANT, await batch([record({ record_id: "identity-first" })]));
    const first = await onlyManifest(store);
    first.manifest.extensions!["96bulls"] = { ...identityExtension(first.manifest), admitted_marker: { version: 7 } };
    await putManifest(store, first.key, first.manifest);

    await deliverBatch(deps, TENANT, await batch([record({ record_id: "identity-second" })], { batch_id: "identity-batch-2" }));
    const updated = (await onlyManifest(store)).manifest;
    expect(identityExtension(updated)).toMatchObject({
      user_id: TENANT.userId,
      canonical_session_id: "sess-1",
      admitted_marker: { version: 7 },
    });
    expect(updated.raw.chunks).toHaveLength(2);
  });

  for (const [field, conflicting] of [["user_id", "another-user"], ["canonical_session_id", "another-session"]] as const) {
    test(`refuses an existing manifest with conflicting ${field}`, async () => {
      const { store, deps } = setup();
      await deliverBatch(deps, TENANT, await batch([record({ record_id: `identity-${field}-first` })]));
      const first = await onlyManifest(store);
      first.manifest.extensions!["96bulls"] = { ...identityExtension(first.manifest), [field]: conflicting };
      await putManifest(store, first.key, first.manifest);
      const before = await chunks(store);

      const error = await deliverBatch(
        deps,
        TENANT,
        await batch([record({ record_id: `identity-${field}-second` })], { batch_id: `identity-${field}-batch-2` }),
      ).catch((caught) => caught);
      expect(error).toBeInstanceOf(IngestUnavailable);
      expect(error).toMatchObject({ stage: "objects", cause_code: "manifest_identity_mismatch" });
      expect(await chunks(store)).toEqual(before);
    });
  }

  test("lands the batch in the m=srv_ layout with relayhistory's receipt", async () => {
    const { store, ledger, deps } = setup();
    const b = await batch([record({ record_id: "e1" }), record({ record_id: "e2", revision: 3 })]);
    const result = await deliverBatch(deps, TENANT, b);
    const prepared = await prepareDelivery(TENANT, b);
    expect(result.receipt).toEqual({ ...prepared.receipt, limits: { maxRecords: 500, maxRequestBytes: MAX_DELIVERY_BYTES } });
    expect(result.receipt.receiptId).toMatch(/^rhr_[0-9a-f]{64}$/);
    const machine = liveMachineId("ws-a", "origin-1", "user-1");
    const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith("manifest.json"));
    expect(manifests).toHaveLength(1);
    expect(manifests[0]).toContain(`/ws=ws-a/m=${machine}/h=claude/`);
    const manifest = JSON.parse(await readText(store, manifests[0]!));
    expect(manifest.raw.owner).toBe(LIVE_OWNER);
    expect(manifest.writer.plugin_version).toBe("96bulls-session-ingest/1");
    expect(manifest.extensions.relayhistory_live.batches).toEqual([[prepared.batchDigest, 0]]);
    const [lines] = await chunks(store);
    expect(lines!.trim().split("\n").map((l) => JSON.parse(l).record_id)).toEqual(["e1", "e2"]);
    expect(await listAll(store, "96bulls/org-a/live-sessions")).toHaveLength(1);
    expect(ledger.commits).toHaveLength(1);
    expect(ledger.commits[0]!.machineId).toBe(machine);
  });

  test("a replay answers the same receipt and writes nothing", async () => {
    const { store, deps } = setup();
    const b = await batch([record({ record_id: "e1" })]);
    const first = await deliverBatch(deps, TENANT, b);
    const before = await listAll(store, "96bulls");
    const again = await deliverBatch(deps, TENANT, b);
    expect(again.outcome).toBe("replay");
    expect(again.receipt).toEqual(first.receipt);
    expect(await listAll(store, "96bulls")).toEqual(before);
  });

  test("a commit that fails is a 503, and the retry resumes without a second chunk", async () => {
    const { store, ledger, deps } = setup();
    ledger.failCommit = 1;
    const b = await batch([record({ record_id: "e1" })]);
    const failed = await deliverBatch(deps, TENANT, b).catch((e) => e);
    expect(failed).toBeInstanceOf(IngestUnavailable);
    expect((failed as IngestUnavailable).status).toBe(503);
    expect((failed as IngestUnavailable).cause_code).toBe("ECONNRESET");
    const retry = await deliverBatch(deps, TENANT, b);
    expect(retry.outcome).toBe("accepted");
    expect(await chunks(store)).toHaveLength(1);
    expect(retry.stats.manifests_written).toBe(0);
  });

  test("scrub_not_identity: a record id the scrubber rewrites still lands, scrubbed, with the client's receipt", async () => {
    const { store, deps } = setup();
    const b = await batch([record({ record_id: "alice@example.com#1", revision_id: "alice@example.com#1:r1" })]);
    const result = await deliverBatch(deps, TENANT, b);
    expect(result.receipt.acceptedRevisionIds).toEqual(["alice@example.com#1:r1"]);
    const [lines] = await chunks(store);
    expect(lines).not.toContain("alice@example.com");
    expect(JSON.parse(lines!.trim()).record_id).not.toBe("alice@example.com#1");
  });

  test("chunk_string_too_long: a truncated string past the bound lands with its in-band note", async () => {
    const { store, deps } = setup();
    const long = "word ".repeat(Math.ceil((MAX_SCRUB_CHARS + 50_000) / 5));
    const b = await batch([record({ record_id: "e1", payload: { text: long, ts_ms: 1_761_955_200_000 } })]);
    await deliverBatch(deps, TENANT, b);
    const stored = JSON.parse((await chunks(store))[0]!.trim());
    expect(stored.payload.payload_truncated).toBe(1);
    expect(stored.payload.text).toContain("[relayhistory: truncated");
  });

  test("chunk_string_too_long: long *_json evidence with no safe cut still lands (that string redacted, the record kept)", async () => {
    const { store, deps } = setup();
    const items = Array.from({ length: 40_000 }, (_, i) => `item-${i}`);
    const evidence = JSON.stringify({ items });
    expect(evidence.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const b = await batch([record({ record_id: "t1", kind: "tool_call", payload: { input_json: evidence, tool_name: "Read", ts_ms: 1_761_955_200_000 } })]);
    expect((await deliverBatch(deps, TENANT, b)).outcome).toBe("accepted");
    const stored = JSON.parse((await chunks(store))[0]!.trim());
    expect(stored.record_id).toBe("t1");
    expect(stored.payload.tool_name).toBe("Read");
    expect(stored.payload.input_json).toMatch(/^\[REDACTED: \d+ characters exceed/);
  });

  test("record_without_session and many sessions land (no Workers budgets)", async () => {
    const { store, deps } = setup();
    const records = [record({ record_id: "n1", session_id: null, kind: "presence" }), ...Array.from({ length: 60 }, (_, i) => record({ record_id: `s${i}`, session_id: `sess-${i}` }))];
    const result = await deliverBatch(deps, TENANT, await batch(records));
    expect(result.outcome).toBe("accepted");
    const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith("manifest.json"));
    expect(manifests).toHaveLength(61);
    expect(manifests.some((k) => k.includes(`/h=${NO_SESSION_HARNESS}/`) && k.includes("/s=_origin/"))).toBe(true);
  }, 30_000);

  test("records of a deleted session are acknowledged and not stored", async () => {
    const { store, ledger, deps } = setup();
    ledger.deleted.add(JSON.stringify(["claude", "gone"]));
    const b = await batch([record({ record_id: "keep" }), record({ record_id: "drop", session_id: "gone" })]);
    const result = await deliverBatch(deps, TENANT, b);
    expect(result.receipt.acceptedRevisionIds).toEqual(["keep:r1", "drop:r1"]);
    expect(result.stats.deleted_sessions).toBe(1);
    const all = (await chunks(store)).join("");
    expect(all).toContain('"keep"');
    expect(all).not.toContain('"drop"');
    expect(ledger.commits[0]!.sessions).toEqual([["claude", "sess-1"]]);
  });

  test("a reused batch id and a reused revision are relayhistory's 409s", async () => {
    const { ledger, deps } = setup();
    ledger.script.push({ kind: "batch_conflict", currentDigest: "d0" });
    const e1 = (await deliverBatch(deps, TENANT, await batch([record({ record_id: "e1" })])).catch((e) => e)) as DeliveryError;
    expect([e1.status, e1.code, e1.conflict?.type]).toEqual([409, "delivery_conflict", "batch_id"]);
    const c = { recordId: "e1", submittedRevisionId: "e1:r1", submittedRevision: 1, submittedDigest: "x", currentRevisionId: "e1:old", currentRevision: 1, currentDigest: "y" };
    ledger.script.push({ kind: "revision_conflict", conflicts: [c], count: 7 });
    const e2 = (await deliverBatch(deps, TENANT, await batch([record({ record_id: "e1" })], { batch_id: "b2" })).catch((e) => e)) as DeliveryError;
    expect([e2.status, e2.conflictCount, e2.conflicts?.[0]]).toEqual([409, 7, { type: "record_revision", originId: "origin-1", ...c }]);
  });

  test("a store outage is a retryable 503, never a fallback", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const broken = new Proxy(s.store, {
      get(target, prop, recv) {
        if (prop === "put") return async () => { throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" }); };
        return Reflect.get(target, prop, recv);
      },
    }) as ObjectStore;
    const ledger = new MemoryLedger();
    const e = await deliverBatch({ ledger, target: { store: broken, keyRoot: "96bulls", sleep: noSleep } }, TENANT, await batch([record({ record_id: "e1" })])).catch((x) => x);
    expect(e).toBeInstanceOf(IngestUnavailable);
    expect([(e as IngestUnavailable).stage, (e as IngestUnavailable).status]).toEqual(["objects", 503]);
    expect(ledger.commits).toHaveLength(0);
  });
});

describe("Neon replica (transition only)", () => {
  function sink(mode: "required" | "best_effort", fail = false) {
    const sent: NeonReplicaMessage[] = [];
    const descriptors = new Map<string, Uint8Array>();
    const s: ReplicaSink = {
      mode,
      send: async (m) => {
        if (fail) throw new Error("sqs down");
        sent.push(m);
      },
      putDescriptor: async (k, b) => void descriptors.set(k, b),
    };
    return { s, sent, descriptors };
  }

  test("queues relayhistory's message: parts, per-record placement, resets", async () => {
    const r = sink("required");
    const { ledger, deps } = setup({ replica: r.s });
    ledger.resets = 2;
    await deliverBatch(deps, TENANT, await batch([record({ record_id: "e1" }), record({ record_id: "e2", session_id: "sess-2" })]));
    expect(r.sent).toHaveLength(1);
    const m = r.sent[0]!;
    expect([m.kind, m.fence.resets, m.parts.length]).toEqual(["neon-replica", 2, 2]);
    expect(m.records.map((x) => [x[0], x[1], x[4]])).toEqual([[0, 0, "e1"], [1, 0, "e2"]]);
  });

  test("a large batch goes through a descriptor beside the batch marker", async () => {
    const r = sink("required");
    const { deps } = setup({ replica: r.s });
    const records = Array.from({ length: 500 }, (_, i) => record({ record_id: `rec-${i}-${"x".repeat(200)}` }));
    await deliverBatch(deps, TENANT, await batch(records));
    expect(r.sent[0]!.descriptor?.key).toMatch(/^96bulls\/org-a\/live-batches\/v1\/ws=ws-a\/o=[0-9a-f]{32}\/b=[0-9a-f]{32}\.replica\.json$/);
    expect(r.sent[0]!.records).toEqual([]);
    expect(JSON.stringify(r.sent[0]).length).toBeLessThan(MAX_REPLICA_MESSAGE_BYTES);
  });

  test("required: a failed send is a 503 (the batch is pending, the retry re-sends)", async () => {
    const { deps, ledger } = setup({ replica: sink("required", true).s });
    const e = await deliverBatch(deps, TENANT, await batch([record({ record_id: "e1" })])).catch((x) => x);
    expect((e as IngestUnavailable).stage).toBe("replica");
    expect(ledger.commits).toHaveLength(0);
  });

  test("best_effort: a failed send is logged and the batch is acknowledged", async () => {
    const { deps } = setup({ replica: sink("best_effort", true).s });
    const result = await deliverBatch(deps, TENANT, await batch([record({ record_id: "e1" })]));
    expect(result.stats.replica).toBe("failed");
  });
});

describe("HTTP contract", () => {
  // The identity relayhistory verified, as its service binding sends it.
  const good = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE, "rth:read"] });
  const readonly = encodeIdentity({ ...TENANT, scopes: ["rth:read"] });
  function app() {
    const { deps } = setup();
    return createIngestApp({ deliver: deps, log: () => {}, identify: (r) => bindingIdentity(r) });
  }
  const post = async (body: BodyInit, headers: Record<string, string> = {}) =>
    app().fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: { [IDENTITY_HEADER]: good, "x-correlation-id": "corr-1", ...headers } }));

  test("200 receipt with limits.contentEncodings, for identity, gzip and zstd bodies", async () => {
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    for (const [encoded, encoding] of [[body, ""], [gzipSync(body), "gzip"], [zstdCompressSync(Buffer.from(body)), "zstd"]] as const) {
      const res = await post(encoded as BodyInit, encoding ? { "content-encoding": encoding } : {});
      expect(res.status).toBe(200);
      const json = (await res.json()) as { receiptId: string; limits: { contentEncodings: string[] } };
      expect(json.receiptId).toMatch(/^rhr_/);
      expect(json.limits.contentEncodings).toEqual(["zstd", "gzip"]);
      expect(res.headers.get("x-correlation-id")).toBe("corr-1");
    }
  });

  test("relayhistory's error codes and envelope", async () => {
    const none = await app().fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body: "{}" }));
    expect([none.status, ((await none.json()) as { error: { code: string } }).error.code]).toEqual([401, "missing_authorization"]);
    const ro = await post("{}", { [IDENTITY_HEADER]: readonly });
    expect([ro.status, ((await ro.json()) as { error: { message: string } }).error.message]).toEqual([403, "missing required scope: rth:sync"]);
    const bad = await post("{}", { [IDENTITY_HEADER]: Buffer.from('{"orgId":"o"}').toString("base64url") });
    expect([bad.status, ((await bad.json()) as { error: { code: string } }).error.code]).toEqual([401, "invalid_token"]);
    const noScopes = await post("{}", { [IDENTITY_HEADER]: Buffer.from(JSON.stringify({ ...TENANT, scopes: "rth:sync" })).toString("base64url") });
    expect(noScopes.status).toBe(401);
    const junk = await post("{}", { [IDENTITY_HEADER]: "not base64 json" });
    expect(junk.status).toBe(401);
    const enc = await post("{}", { "content-encoding": "br" });
    expect([enc.status, ((await enc.json()) as { error: { code: string } }).error.code]).toEqual([415, "unsupported_encoding"]);
    const big = await post("x".repeat(MAX_DELIVERY_BYTES + 1));
    expect([big.status, ((await big.json()) as { error: { code: string } }).error.code]).toEqual([413, "delivery_too_large"]);
    const bomb = await post(gzipSync("[" + "0,".repeat(MAX_DELIVERY_BYTES) + "0]"), { "content-encoding": "gzip" });
    expect(bomb.status).toBe(413);
    const invalid = await post(JSON.stringify({ protocolVersion: 1, batch: "x" }));
    const ij = (await invalid.json()) as { error: { code: string }; correlationId: string };
    expect([invalid.status, ij.error.code, ij.correlationId]).toEqual([400, "invalid_delivery", "corr-1"]);
    const proto = await post(JSON.stringify({ protocolVersion: 2, batch: {} }));
    expect(proto.status).toBe(422);
  });

  test("limits, and every unmigrated /v1 route is 501 unsupported_by_backend", async () => {
    const a = app();
    const limits = await a.fetch(new Request("https://x/v1/delivery/limits", { headers: { [IDENTITY_HEADER]: good } }));
    expect(await limits.json()).toEqual({ maxRecords: 500, maxRequestBytes: MAX_DELIVERY_BYTES, contentEncodings: ["zstd", "gzip"] });
    const other = await a.fetch(new Request("https://x/v1/sessions", { headers: { [IDENTITY_HEADER]: good } }));
    expect([other.status, ((await other.json()) as { error: { code: string } }).error.code]).toEqual([501, "unsupported_by_backend"]);
  });
});

describe("old Desktops: replay fast path and per-tenant limits", () => {
  test("a regenerated job re-sending a whole history at stored revisions is acked with no S3 write", async () => {
    const { store, ledger, deps } = setup();
    const history = Array.from({ length: 50 }, (_, i) => record({ record_id: `h${i}`, revision: 4 }));
    await deliverBatch(deps, TENANT, await batch(history));
    const before = await listAll(store, "96bulls");
    // gen-N+1 export: a new batch id, the same revisions, changed local bytes (#250: still a replay).
    const regenerated = history.map((r) => ({ ...r, payload: { ...r.payload, added_column: 1 } }));
    const result = await deliverBatch(deps, TENANT, await batch(regenerated, { batch_id: "gen-2", generation: 2 }));
    expect(result.outcome).toBe("replay_fast");
    expect(result.stats.stale).toBe(50);
    expect(result.receipt.acceptedRevisionIds).toEqual(regenerated.map((r) => r.revision_id));
    expect(await listAll(store, "96bulls")).toEqual(before);
    expect(ledger.commits).toHaveLength(1);
  });

  test("a mixed batch writes only the new revisions", async () => {
    const { store, deps } = setup();
    await deliverBatch(deps, TENANT, await batch([record({ record_id: "a", revision: 2 })]));
    const result = await deliverBatch(deps, TENANT, await batch([record({ record_id: "a", revision: 1 }), record({ record_id: "a", revision: 2, revision_id: "a:again" }), record({ record_id: "b", revision: 1 })], { batch_id: "b2" }));
    expect([result.outcome, result.stats.stale]).toEqual(["accepted", 2]);
    const lines = (await chunks(store)).join("").trim().split("\n").map((l) => JSON.parse(l));
    expect(lines.map((l) => `${l.record_id}@${l.revision}`).sort()).toEqual(["a@2", "b@1"]);
  });

  test("a resumed batch rewrites what its failed attempt never committed", async () => {
    const { store, ledger, deps } = setup();
    ledger.failCommit = 1;
    const b = await batch([record({ record_id: "e1" })]);
    await deliverBatch(deps, TENANT, b).catch(() => {});
    const retry = await deliverBatch(deps, TENANT, b);
    expect([retry.outcome, retry.stats.stale]).toEqual(["accepted", 0]);
    expect(await chunks(store)).toHaveLength(1);
  });

  test("over the per-tenant limit: 429 delivery_busy with Retry-After, nothing written, the retry resumes", async () => {
    const { store, ledger, deps } = setup();
    const limited = { ...deps, slots: { maxPerUser: 1, maxPerOrg: 10, ttlSeconds: 90 } };
    ledger.slots.set(TENANT.userId, new Set(["other-request"]));
    const app = createIngestApp({ deliver: limited, log: () => {}, identify: (r) => bindingIdentity(r) });
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    const req = () => app.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }) } }));
    const busy = await req();
    expect([busy.status, ((await busy.json()) as { error: { code: string } }).error.code]).toEqual([429, "delivery_busy"]);
    expect(Number(busy.headers.get("retry-after"))).toBeGreaterThan(0);
    expect(await chunks(store)).toHaveLength(0);
    ledger.slots.clear();
    expect((await req()).status).toBe(200);
    expect(ledger.slots.get(TENANT.userId)?.size ?? 0).toBe(0);
  });

  test("a replay never takes a slot (the fast path is unlimited)", async () => {
    const { ledger, deps } = setup();
    const limited = { ...deps, slots: { maxPerUser: 1, maxPerOrg: 1, ttlSeconds: 90 } };
    const b = await batch([record({ record_id: "e1" })]);
    await deliverBatch(limited, TENANT, b);
    ledger.slots.set(TENANT.userId, new Set(["busy"]));
    expect((await deliverBatch(limited, TENANT, b)).outcome).toBe("replay");
    expect((await deliverBatch(limited, TENANT, await batch([record({ record_id: "e1" })], { batch_id: "regen" }))).outcome).toBe("replay_fast");
  });
});

describe("staging: a batch wider than one Worker invocation", () => {
  test("is staged (scrubbed, one object) before the ack, and the fan-out writes every session idempotently", async () => {
    const { store, deps } = setup();
    const sent: import("../staging.ts").FanoutMessage[] = [];
    const staged = { ...deps, inlineMaxSessions: 5, fanout: async (m: import("../staging.ts").FanoutMessage[]) => void sent.push(...m) };
    // Record ids are not scrubbed by prepareDelivery; the pipeline scrubber must reach the staged copy.
    const records = Array.from({ length: 90 }, (_, i) => record({ record_id: i === 3 ? "alice@example.com#3" : `r${i}`, session_id: `s-${i}` }));
    const result = await deliverBatch(staged, TENANT, await batch(records));
    expect(result.outcome).toBe("accepted_staged");
    const all = await listAll(store, "96bulls");
    const stagedKeys = all.filter((k) => k.endsWith(".staged.ndjson.zst"));
    expect(stagedKeys).toHaveLength(1);
    expect(all.filter((k) => k.endsWith("manifest.json"))).toHaveLength(0);
    const raw = await readText(store, stagedKeys[0]!);
    expect(raw).not.toContain("alice@example.com");
    expect(sent.map((m) => [m.from, m.to])).toEqual([[0, 40], [40, 80], [80, 90]]);
    const { fanout } = await import("../staging.ts");
    const target = { store, keyRoot: "96bulls", sleep: noSleep };
    for (const m of sent) await fanout(target, m, (b) => new Uint8Array(zstdDecompressSync(b)));
    // Redelivered by the queue: nothing new.
    for (const m of sent) await fanout(target, m, (b) => new Uint8Array(zstdDecompressSync(b)));
    const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith("manifest.json"));
    expect(manifests).toHaveLength(90);
    expect((await chunks(store)).length).toBe(90);
    // The retry of the same request finds its own staged object and acks.
    expect((await deliverBatch(staged, TENANT, await batch(records))).outcome).toBe("replay");
  }, 60_000);

  test("a staged batch's replica is sent at request time with [group, line] placements in the staged object", async () => {
    const sent: NeonReplicaMessage[] = [];
    const replica: ReplicaSink = { mode: "required", send: async (m) => void sent.push(m), putDescriptor: async () => {} };
    const { deps } = setup({ replica });
    const staged = { ...deps, inlineMaxSessions: 1, fanout: async () => {} };
    const records = [record({ record_id: "a", session_id: "x" }), record({ record_id: "b", session_id: "y" }), record({ record_id: "c", session_id: "x" })];
    const result = await deliverBatch(staged, TENANT, await batch(records));
    expect([result.outcome, result.stats.replica]).toEqual(["accepted_staged", "sent"]);
    const m = sent[0]!;
    expect(m.staged?.key).toMatch(/\.staged\.ndjson\.zst$/);
    expect([m.parts.length, m.staged?.groups, m.writer]).toEqual([0, 2, "96bulls-session-ingest"]);
    expect(m.records.map((r) => [r[0], r[1], r[4]])).toEqual([[0, 0, "a"], [0, 1, "c"], [1, 0, "b"]]);
  });

  test("a staged retry whose kept records shrank writes every session it acknowledges (review H1)", async () => {
    const { store, ledger, deps } = setup();
    const sent: import("../staging.ts").FanoutMessage[] = [];
    let fail = true;
    const staged = {
      ...deps,
      inlineMaxSessions: 1,
      fanout: async (m: import("../staging.ts").FanoutMessage[]) => {
        if (fail) throw new Error("queue down");
        sent.push(...m);
      },
    };
    const records = [record({ record_id: "a", session_id: "x" }), record({ record_id: "b", session_id: "y" }), record({ record_id: "c", session_id: "z" })];
    const b = await batch(records);
    await deliverBatch(staged, TENANT, b).catch(() => {});
    // Meanwhile another batch commits `a`: on the retry it is stale, so only b and c are kept.
    ledger.revisions.set("a", 1);
    fail = false;
    expect((await deliverBatch(staged, TENANT, b)).outcome).toBe("accepted_staged");
    const { fanout } = await import("../staging.ts");
    for (const m of sent) await fanout({ store, keyRoot: "96bulls", sleep: noSleep }, m, (x) => new Uint8Array(zstdDecompressSync(x)), ledger);
    const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith("manifest.json"));
    expect(manifests.some((k) => k.includes("/s=y/"))).toBe(true);
    expect(manifests.some((k) => k.includes("/s=z/"))).toBe(true);
    // Revisions become stored when the fan-out wrote them, exactly the kept set.
    expect(ledger.commits.at(-1)!.records).toEqual([]);
    expect(ledger.revisionCommits.flat().map((r) => r.recordId).sort()).toEqual(["b", "c"]);
  });

  test("a string the first scrub grows past the bound still lands through the fan-out (re-review High)", async () => {
    const { store } = setup();
    // A string just under the bound that the staging scrub grows past it (`?state=` becomes
    // `?state=[REDACTED]`), with no whitespace to cut at. Built after prepare (which would
    // already redact it), as an exempt image or any non-idempotent match can.
    const grown = "A".repeat(MAX_SCRUB_CHARS - 3 - "?state=)".length) + "?state=)";
    const records = [
      record({ record_id: "img", session_id: "x", payload: { blob: "placeholder", ts_ms: 1_761_955_200_000 } }),
      record({ record_id: "b", session_id: "y" }),
    ];
    const { stageGroups, fanoutMessages, fanout } = await import("../staging.ts");
    const { groupRecords } = await import("../writer.ts");
    const prepared = await prepareDelivery(TENANT, await batch(records));
    prepared.records[0]!.payload = { blob: grown, ts_ms: 1_761_955_200_000 };
    const now = new Date("2026-11-02T00:00:00Z");
    const { groups } = groupRecords(prepared.records, now);
    const target = { store, keyRoot: "96bulls", sleep: noSleep };
    const key = await stageGroups(target, { claim_token:crypto.randomUUID(),batch_digest: prepared.batchDigest, received_at: now.toISOString(), org_id: "org-a", workspace_id: "ws-a", origin_id: "origin-1", user_id: "user-1" }, groups);
    for (const m of fanoutMessages(key, groups.length)) await fanout(target, m, (x) => new Uint8Array(zstdDecompressSync(x)));
    const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith("manifest.json"));
    expect(manifests).toHaveLength(2);

  }, 60_000);

  test("a staged batch is not acknowledged when the fan-out cannot be queued", async () => {
    const { ledger, deps } = setup();
    const staged = { ...deps, inlineMaxSessions: 1, fanout: async () => { throw new Error("queue down"); } };
    const e = await deliverBatch(staged, TENANT, await batch([record({ record_id: "a", session_id: "x" }), record({ record_id: "b", session_id: "y" })])).catch((x) => x);
    expect([(e as IngestUnavailable).stage, (e as IngestUnavailable).status]).toEqual(["fanout", 503]);
    expect(ledger.commits).toHaveLength(0);
  });
});

describe("Worker entry", () => {
  test("is 503 not_configured, writing nothing, until every binding and secret is present", async () => {
    const worker = (await import("../worker.ts")).default;
    const res = await worker.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body: "{}" }), {}, { waitUntil: () => {} });
    expect([res.status, ((await res.json()) as { error: { code: string } }).error.code]).toEqual([503, "not_configured"]);
    // A malformed setting is the same coded 503, not a bare 500.
    const bad = await worker.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body: "{}" }), { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn", INGEST_MAX_PER_USER: "lots" }, { waitUntil: () => {} });
    expect([bad.status, ((await bad.json()) as { error: { code: string } }).error.code]).toEqual([503, "not_configured"]);
    const { ingestSettings } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    expect((await ingestSettings(full))?.replicaMode).toBe("off");
    for (const k of Object.keys(full)) expect(await ingestSettings({ ...full, [k]: undefined })).toBeNull();
    // Secrets Store bindings: read with get(); an unset one is not configured.
    const store = { ...full, S3_ACCESS_KEY_ID: { get: async () => "k" }, S3_SECRET_ACCESS_KEY: { get: async () => { throw new Error("secret not found"); } } };
    expect(await ingestSettings(store)).toBeNull();
    expect((await ingestSettings({ ...store, S3_SECRET_ACCESS_KEY: { get: async () => "s" } }))?.accessKeyId).toBe("k");
    // A required replica without its queue is not configured; "queue" means required.
    expect(await ingestSettings({ ...full, INGEST_REPLICA_MODE: "queue" })).toBeNull();
    expect((await ingestSettings({ ...full, INGEST_REPLICA_MODE: "queue", NEON_REPLICA: {} as never }))?.replicaMode).toBe("required");
  });
});

describe("the binding identity is signed (review Medium)", () => {
  function app() {
    const { deps } = setup();
    return { app: createIngestApp({ deliver: deps, log: () => {}, signingKey: SIGNING_KEY }), deps };
  }
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
  const enc = (s: string) => new TextEncoder().encode(s);

  test("a request signed with the shared key over its exact body is served", async () => {
    const { app: a } = app();
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    const res = await a.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body)) }));
    expect(res.status).toBe(200);
  });

  test("unsigned, wrong key, another body, another path, another identity or a stale time: 401", async () => {
    const { app: a } = app();
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    const send = (headers: Record<string, string>, b = body, path = "/v1/delivery/batches") => a.fetch(new Request(`https://x${path}`, { method: "POST", body: b, headers }));
    expect((await send({ [IDENTITY_HEADER]: encodeIdentity(identity) })).status).toBe(401);
    expect((await send(signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body), "other-key"))).status).toBe(401);
    expect((await send(signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body)), body + " ")).status).toBe(401);
    expect((await send(signedHeaders(identity, "POST", "/v1/delivery/limits", enc(body)))).status).toBe(401);
    const forged = { ...signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body)), [IDENTITY_HEADER]: encodeIdentity({ ...identity, orgId: "org-victim" }) };
    expect((await send(forged)).status).toBe(401);
    expect((await send(signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body), SIGNING_KEY, Math.floor(Date.now() / 1000) - 120))).status).toBe(401);
    // No key configured: refused, never trusted.
    const { deps } = setup();
    const keyless = createIngestApp({ deliver: deps, log: () => {} });
    const res = await keyless.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body)) }));
    expect([res.status, ((await res.json()) as { error: { code: string } }).error.code]).toEqual([401, "not_configured"]);
    void SIGNATURE_HEADER;
  });

  test("archive recovery requires the HMAC over its exact private body and buffered mode", async () => {
    const { deps, ledger } = setup();
    const raw = await batch([record({ record_id: "signed-recovery" })], { batch_id: "signed-recovery" });
    const prepared = await prepareDelivery(TENANT, raw);
    const sourceDigest = "d".repeat(64);
    const custody = {
      sourceDigest,
      witness: {
        version: 1 as const, issuer: "relayhistory-cloud" as const, sourceIdentitySha256: "c".repeat(64), controlEpoch: "e".repeat(64), privacyBaselineSha256: "a".repeat(64),
        tenant: TENANT, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
        privacyVersion: "0", originalRecordCount: raw.records.length, candidateCount: prepared.records.length,
        records: prepared.records.map((r, preparedOrd) => ({ preparedOrd, processingRank: preparedOrd, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
        privacy: { complete: true as const, deletions: [], resets: [] },
      },
    };
    ledger.prepareBufferStage = async () => ({ stageToken: crypto.randomUUID(), expiresAt: new Date(Date.now() + 60_000).toISOString() });
    ledger.commitBuffered = async () => ({ version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true });
    const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
    const path = "/v1/delivery/recovery";
    const headers = { "content-type": RECOVERY_CONTENT_TYPE, ...signedHeaders(identity, "POST", path, enc(body)) };
    const buffered = createIngestApp({ deliver: { ...deps, acceptanceMode: "buffered" }, log: () => {}, signingKey: SIGNING_KEY });
    expect((await buffered.fetch(new Request(`https://x${path}`, { method: "POST", body, headers }))).status).toBe(200);
    expect((await buffered.fetch(new Request(`https://x${path}`, { method: "POST", body: body + " ", headers }))).status).toBe(401);
    const canonical = createIngestApp({ deliver: deps, log: () => {}, signingKey: SIGNING_KEY });
    expect((await canonical.fetch(new Request(`https://x${path}`, { method: "POST", body, headers }))).status).toBe(503);
  });
});

describe("deletion fences written by relayhistory (review High 2 / Medium: fences had no writer)", () => {
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
  const enc = (s: string) => new TextEncoder().encode(s);
  async function post(path: string, value: unknown) {
    const { deps, ledger } = setup();
    const a = createIngestApp({ deliver: deps, log: () => {}, signingKey: SIGNING_KEY });
    const body = JSON.stringify(value);
    const res = await a.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders(identity, "POST", path, enc(body)) }));
    return { res, ledger };
  }
  test("selection: relayhistory's body and answer; the ledger gets this user's sessions", async () => {
    const account_id = (await batch([])).account_id;
    const { res, ledger } = await post("/v1/delivery/sessions/selection", { account_id, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "s1" }], enabled: false });
    expect([res.status, await res.json()]).toEqual([200, { updated: true }]);
    expect(ledger.selections).toEqual([{ tenant: TENANT, originId: "origin-1", sessions: [["claude", "s1"]], enabled: false }]);
  });
  test("selection: another tenant's account id is 403, a malformed body 400, nothing written", async () => {
    const wrong = await post("/v1/delivery/sessions/selection", { account_id: "relayhistory:other", origin_id: "o", sessions: [{ source: "claude", session_id: "s1" }], enabled: false });
    expect(wrong.res.status).toBe(403);
    expect(wrong.ledger.selections).toEqual([]);
    const bad = await post("/v1/delivery/sessions/selection", { account_id: (await batch([])).account_id, origin_id: "o", sessions: [], enabled: false });
    expect(bad.res.status).toBe(400);
  });
  test("reset: needs the typed confirmation, answers relayhistory's shape", async () => {
    const account_id = (await batch([])).account_id;
    const ok = await post("/v1/delivery/sessions/reset", { account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" });
    expect([ok.res.status, await ok.res.json()]).toEqual([200, { reset: true, deleted: 0 }]);
    const no = await post("/v1/delivery/sessions/reset", { account_id, request_id: "req-1", confirmation: "yes" });
    expect(no.res.status).toBe(400);
  });
});

describe("staged fan-out commits revisions only after writing, and honours fences (review Medium)", () => {
  test("the request commits the batch with no revisions; each slice commits what it wrote; a session fenced since is skipped", async () => {
    const { store, ledger, deps } = setup();
    const sent: import("../staging.ts").FanoutMessage[] = [];
    const staged = { ...deps, inlineMaxSessions: 1, fanout: async (m: import("../staging.ts").FanoutMessage[]) => void sent.push(...m) };
    const records = [record({ record_id: "a", session_id: "x" }), record({ record_id: "b", session_id: "y" })];
    await deliverBatch(staged, TENANT, await batch(records));
    expect(ledger.commits.at(-1)!.records).toEqual([]);
    // A re-export before the fan-out ran is NOT acknowledged unwritten.
    expect(ledger.revisions.size).toBe(0);
    ledger.deleted.add(JSON.stringify(["claude", "y"]));
    const { fanout } = await import("../staging.ts");
    for (const m of sent) await fanout({ store, keyRoot: "96bulls", sleep: noSleep }, m, (x) => new Uint8Array(zstdDecompressSync(x)), ledger);
    const manifests = (await listAll(store, "96bulls/org-a/v1")).filter((k) => k.endsWith("manifest.json"));
    expect(manifests.some((k) => k.includes("/s=x/"))).toBe(true);
    expect(manifests.some((k) => k.includes("/s=y/"))).toBe(false);
    expect(ledger.revisionCommits.flat().map((r) => [r.recordId, r.sessionId, r.digest.length])).toEqual([["a", "x", 64]]);
  }, 60_000);
});

describe("signing contract shared with relayhistory-cloud (delivery-forward.ts)", () => {
  test("a fixed vector: relayhistory's test asserts the same string", async () => {
    const { signRequest } = await import("../app.ts");
    expect(signRequest("vector-key", 1800000000, "POST", "/v1/delivery/batches", "eyJpZCI6MX0", new TextEncoder().encode('{"a":1}'))).toBe(
      "t=1800000000,v1=9e40c32831806a6c6f5e24232f926be249a5071c955213dcda3d4e24d2a34c41",
    );
  });
});
