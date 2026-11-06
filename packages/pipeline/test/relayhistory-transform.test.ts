import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { sha256Hex } from "../../object-store/src/crypto.js";
import {
  computeContentSha256,
  manifestKey,
  normalizedKey,
  objectKey,
  rawChunkKey,
  readAll,
  type ChunkEntry,
  type ManifestV1,
  type ObjectStore,
  type SessionLocation,
} from "../src/lib/pipeline/object-layout.js";
import { objectStoreManifestReader } from "../src/lib/pipeline/adapter.js";
import {
  planEntryFor,
  planEntrySha256,
} from "../src/lib/pipeline/migration-assertions.js";
import { projectRelayhistorySession } from "../src/lib/pipeline/relayhistory-projection.js";
import type { NormalizedOutput } from "../src/lib/pipeline/stages.js";
import {
  RelayhistoryExportTransform,
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  readRelayhistoryExport,
  type RelayhistoryStreamRecord,
} from "../src/lib/pipeline/relayhistory-transform.js";

const AT = "2026-10-30T12:00:00.000Z";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const loc: SessionLocation = {
  workspaceId: "workspace",
  machineId: "machine",
  harness: "claude",
  sessionId: "session",
  startDate: "2026-10-30",
};
const record = (
  record_id: string,
  revision: string,
  ts_ms: number,
  overrides: Partial<RelayhistoryStreamRecord> = {},
): RelayhistoryStreamRecord => ({
  kind: "session_event",
  origin_id: "origin",
  record_id,
  revision,
  operation: "upsert",
  received_at: AT,
  payload: { ts_ms, text: `${record_id}-${revision}` },
  ...overrides,
});
const ndjson = (records: readonly RelayhistoryStreamRecord[]) =>
  new TextEncoder().encode(
    `${records.map((value) => JSON.stringify(value)).join("\n")}\n`,
  );

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-relayhistory-transform-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

async function fixture(location: SessionLocation = loc) {
  const store = createFsStore({ root });
  const live = ndjson([
    record("changed", "2", 3_000),
    record("deleted", "3", 4_000, { operation: "delete", payload: null }),
    record("live", "1", 5_000),
  ]);
  const backfill = ndjson([
    record("changed", "1", 1_000),
    record("deleted", "1", 2_000),
    record("historical", "1", 500, {
      payload: { ts_ms: 500, "😀": "astral", "\uE000": "private-use" },
    }),
  ]);
  const chunks: ChunkEntry[] = [];
  for (const [seq, epoch, source] of [
    [0, 0, live],
    [1, 1, backfill],
  ] as const) {
    const stored = new Uint8Array(zstdCompressSync(source));
    const sourceSha = await sha256Hex(source);
    const entry: ChunkEntry = {
      seq,
      epoch,
      start_offset: 0,
      bytes: source.byteLength,
      stored_bytes: stored.byteLength,
      sha256: sourceSha,
      sha256_8: sourceSha.slice(0, 8),
      stored_sha256: await sha256Hex(stored),
      stored_raw_bytes: source.byteLength,
      raw_sha256: sourceSha,
      compression: "zstd",
      records: 3,
    };
    chunks.push(entry);
    await store.put(
      rawChunkKey(location, seq, 0, sourceSha, "ndjson"),
      stored,
      { ifNoneMatch: true },
    );
  }
  const manifest: ManifestV1 = {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision: 2,
    created_at: AT,
    updated_at: AT,
    session: {
      workspace_id: loc.workspaceId,
      machine_id: loc.machineId,
      harness: loc.harness,
      session_id: loc.sessionId,
      started_at: AT,
    },
    writer: { delivery_instance_id: "live-writer", plugin_version: "test" },
    raw: {
      owner: "live-writer",
      state: "open",
      updated_at: AT,
      format: "ndjson",
      compression: "zstd",
      chunk_count: chunks.length,
      raw_bytes: chunks.reduce((sum, chunk) => sum + chunk.bytes, 0),
      stored_bytes: chunks.reduce((sum, chunk) => sum + chunk.stored_bytes, 0),
      content_sha256: await computeContentSha256(chunks),
      epoch: 1,
      epochs: [
        {
          epoch: 0,
          opened_at: AT,
          reason: "initial",
          first_seq: 0,
          opened_by: "live-writer",
        },
        {
          epoch: 1,
          opened_at: AT,
          reason: "backfill",
          first_seq: 1,
          opened_by: "migration:mgr_test",
        },
      ],
      next_offset: backfill.byteLength,
      chunks,
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: {
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
        grant_id: "mgr_test",
        plan_digest: "a".repeat(64),
        source_user_id: "user-1",
      },
      "96bulls": { user_id: "user-1" },
    },
  };
  const key = manifestKey(location);
  await store.put(
    objectKey(key),
    new TextEncoder().encode(JSON.stringify(manifest)),
    {
      ifNoneMatch: true,
      contentType: "application/json",
    },
  );
  const reader = objectStoreManifestReader(store, { provider: "fs" });
  return { store, reader, manifest, key };
}

describe("RelayHistory export transform", () => {
  it("merges live and pre-T0 epochs by revision, applies tombstones, and event-orders", async () => {
    const f = await fixture();
    const records = await readRelayhistoryExport(
      f.manifest,
      f.store,
      (stored) => new Uint8Array(zstdDecompressSync(stored)),
    );
    expect(records.map((value) => [value.record_id, value.revision])).toEqual([
      ["historical", "1"],
      ["changed", "2"],
      ["live", "1"],
    ]);
  });

  it("refuses equal revisions whose content differs", () => {
    expect(() =>
      normalizeRelayhistoryRecords([
        record("same", "7", 1, { payload: { ts_ms: 1, text: "a" } }),
        record("same", "7", 1, { payload: { ts_ms: 1, text: "b" } }),
      ]),
    ).toThrow(/equal RelayHistory revisions/);
  });

  it("folds one revision that the export and the live client stored at different times", () => {
    const exported = record("same", "7", 1, {
      received_at: "2026-10-29T00:00:00.000Z",
    });
    const live = record("same", "7", 1, {
      received_at: "2026-10-30T00:00:00.000Z",
    });
    // No event time in the payload: `received_at` also decides the sort.
    const undated = (received_at: string) =>
      record("undated", "3", 1, { received_at, payload: { text: "no time" } });
    const between = record(
      "between",
      "1",
      Date.parse("2026-10-29T12:00:00.000Z"),
    );
    const early = undated("2026-10-29T00:00:00.000Z");
    const late = undated("2026-10-30T00:00:00.000Z");
    // The same instant written two ways still has one winner.
    const utc = record("spelled", "1", 1, {
      received_at: "2026-10-29T00:00:00.000Z",
    });
    const offset = record("spelled", "1", 1, {
      received_at: "2026-10-29T01:00:00.000+01:00",
    });
    const orders = [
      [exported, live, early, late, between, utc, offset],
      [offset, utc, between, late, early, live, exported],
    ];
    const [first, second] = orders.map((records) =>
      JSON.stringify(normalizeRelayhistoryRecords(records)),
    );
    expect(second).toBe(first);
    expect(
      normalizeRelayhistoryRecords(orders[0]!).map((value) => [
        value.record_id,
        value.received_at,
      ]),
    ).toEqual([
      ["same", exported.received_at],
      ["spelled", utc.received_at],
      ["undated", early.received_at],
      ["between", between.received_at],
    ]);
    expect(() =>
      normalizeRelayhistoryRecords([
        record("same", "7", 1),
        record("same", "7", 1, { kind: "tool_call" }),
      ]),
    ).toThrow(/equal RelayHistory revisions/);
  });

  it("bounds record identities and signed-bigint revisions", () => {
    expect(() =>
      parseRelayhistoryRecord(
        JSON.stringify(record("id", "9223372036854775808", 1)),
      ),
    ).toThrow(/revision is not numeric/);
    expect(() =>
      parseRelayhistoryRecord(JSON.stringify(record("x".repeat(1025), "1", 1))),
    ).toThrow(/record_id is missing/);
  });

  it("writes an idempotent content-addressed normalized object", async () => {
    const f = await fixture();
    const transform = new RelayhistoryExportTransform({
      manifests: f.reader,
      store: f.store,
      decompress: (stored) => new Uint8Array(zstdDecompressSync(stored)),
    });
    const pointer = {
      traceId: TRACE,
      workspaceId: loc.workspaceId,
      manifestKey: f.key,
    };
    const first = (await transform.normalize(pointer)) as NormalizedOutput;
    const second = await transform.normalize(pointer);
    expect(second).toEqual({ ...first, outputReused: true });
    expect(first.outputKey).toContain(f.manifest.raw.content_sha256);
    const body = new TextDecoder().decode(
      await readAll(await f.store.get(objectKey(first.outputKey!))),
    );
    const lines = body
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    expect(lines.map((line) => line.row.delivery_record_id)).toEqual([
      "historical",
      "changed",
      "live",
    ]);
    expect(Object.keys(lines[0]!.row.record.payload)).toEqual([
      "ts_ms",
      "\uE000",
      "😀",
    ]);
  });

  describe("records delivered by two origins", () => {
    const at = (seconds: number) => `2026-10-30 12:00:0${seconds}+00`;
    const copy = (
      origin_id: string,
      text: string,
      received_at: string,
      accepted_seq: string,
    ) => ({
      ...record("same", "1", 1, {
        origin_id,
        received_at,
        payload: { ts_ms: 1, text },
      }),
      accepted_seq,
    });

    it("ranks by the ledger, so a replay or a later stamp can't outrank a later write", () => {
      const lines = [
        copy("origin-a", "from-a", at(3), "1"),
        copy("origin-a", "from-a", at(5), "1"),
        copy("origin-b", "from-b", at(2), "2"),
      ];
      for (const order of [
        lines,
        [...lines].reverse(),
        [lines[1]!, lines[2]!, lines[0]!],
      ])
        expect(
          normalizeRelayhistoryRecords(order).map((value) => value.origin_id),
        ).toEqual(["origin-b"]);
    });

    it("compares sequence numbers exactly, past a double's precision", () => {
      const lines = [
        copy("origin-a", "from-a", at(1), "9007199254740993"),
        copy("origin-b", "from-b", at(1), "9007199254740992"),
      ];
      expect(
        normalizeRelayhistoryRecords(lines).map((value) => value.origin_id),
      ).toEqual(["origin-a"]);
    });

    it("refuses a malformed sequence number", () => {
      expect(() =>
        parseRelayhistoryRecord(
          JSON.stringify({
            ...record("a", "1", 1),
            accepted_seq: "9223372036854775808",
          }),
        ),
      ).toThrow(
        expect.objectContaining({ code: "relayhistory_record_invalid" }),
      );
    });
  });

  describe("provenance", () => {
    const edited = async (change: (manifest: ManifestV1) => void) => {
      const f = await fixture();
      const manifest = structuredClone(f.manifest);
      change(manifest);
      return { manifest, store: f.store };
    };
    const marker = (m: ManifestV1) =>
      m.extensions!["client.relayhistory_export"] as Record<string, unknown>;

    it.each<[string, (m: ManifestV1) => void]>([
      ["no grant", (m) => void delete marker(m).grant_id],
      ["no plan digest", (m) => void delete marker(m).plan_digest],
      [
        "a malformed plan digest",
        (m) => void (marker(m).plan_digest = "not-a-digest"),
      ],
      ["no source user", (m) => void delete marker(m).source_user_id],
      ["no 96bulls user", (m) => void delete m.extensions!["96bulls"]],
      [
        "another 96bulls user",
        (m) => void (m.extensions!["96bulls"] = { user_id: "user-2" }),
      ],
      [
        "a live stream with no backfill",
        (m) => void (m.raw.epochs[1]!.reason = "truncated"),
      ],
      [
        "a backfill another grant opened",
        (m) => void (m.raw.epochs[1]!.opened_by = "migration:mgr_other"),
      ],
    ])("refuses a marker with %s", async (_name, change) => {
      const { manifest, store } = await edited(change);
      await expect(
        readRelayhistoryExport(manifest, store),
      ).rejects.toMatchObject({
        code: "relayhistory_export_provenance",
        retryable: false,
      });
    });

    it("accepts a cold export its grant owns", async () => {
      const { manifest, store } = await edited((m) => {
        m.raw.owner = "migration:mgr_test";
        m.raw.epochs[1]!.opened_by = "migration:mgr_other";
      });
      await expect(
        readRelayhistoryExport(manifest, store),
      ).resolves.toHaveLength(3);
    });
  });

  describe("migration work", () => {
    const entrySha = async (manifest: ManifestV1) =>
      planEntrySha256(
        (await planEntryFor(
          {
            grant_id: "mgr_test",
            org_id: "org",
            workspace_id: "workspace",
            source_user_id: "user-1",
          },
          loc as never,
          manifest,
        ))!,
      );
    const prefixed: SessionLocation = { ...loc, prefix: "tenant/acme" };
    const run = async (location: SessionLocation, digest?: string) => {
      const f = await fixture(location);
      const entry_sha256 = digest ?? (await entrySha(f.manifest));
      const signedKey = manifestKey(loc);
      const transform = new RelayhistoryExportTransform({
        manifests: f.reader,
        store: f.store,
      });
      const output = await transform.normalize(
        {
          traceId: TRACE,
          workspaceId: loc.workspaceId,
          manifestKey: f.key,
          migration: { key: signedKey, entry_sha256 },
        },
        {
          revision: f.manifest.revision,
          workKey: `${signedKey}@${entry_sha256}`,
          orgId: "org",
        },
      );
      return { f, output };
    };

    it.each([
      ["an unprefixed key", loc],
      ["a deployment-prefixed key", prefixed],
    ])(
      "normalizes its own plan entry on %s, reading and writing under that key",
      async (_name, location) => {
        const { f, output } = await run(location);
        const expected = normalizedKey(
          location,
          2,
          `relayhistory-${f.manifest.raw.content_sha256}.ndjson`,
        );
        expect(output).toMatchObject({
          outputKey: expected,
          revision: f.manifest.revision,
        });
        expect(
          await readAll(await f.store.get(objectKey(expected))),
        ).toBeDefined();
      },
    );

    it("skips a manifest that is not the signed plan entry", async () => {
      const { output } = await run(loc, "0".repeat(64));
      expect(output).toEqual({ skipped: "stale_manifest" });
    });

    it("refuses migration work that names no org", async () => {
      const f = await fixture();
      const transform = new RelayhistoryExportTransform({
        manifests: f.reader,
        store: f.store,
      });
      await expect(
        transform.normalize(
          {
            traceId: TRACE,
            workspaceId: loc.workspaceId,
            manifestKey: f.key,
            migration: { key: manifestKey(loc), entry_sha256: "0".repeat(64) },
          },
          { revision: f.manifest.revision },
        ),
      ).rejects.toMatchObject({
        code: "relayhistory_export_provenance",
        retryable: false,
      });
    });

    it("still treats live work's key suffix as an object version", async () => {
      const f = await fixture();
      const transform = new RelayhistoryExportTransform({
        manifests: f.reader,
        store: f.store,
      });
      expect(
        await transform.normalize(
          { traceId: TRACE, workspaceId: loc.workspaceId, manifestKey: f.key },
          { workKey: `${f.key}@${"0".repeat(64)}` },
        ),
      ).toEqual({ skipped: "stale_manifest" });
    });
  });

  describe("reads are capped by what the manifest declares", () => {
    const counted = (store: ObjectStore, bytes: number) => {
      let pulled = 0;
      const capped: ObjectStore = Object.create(store);
      capped.get = async () =>
        new ReadableStream<Uint8Array>({
          pull(controller) {
            pulled += 1024;
            if (pulled > bytes) controller.close();
            else controller.enqueue(new Uint8Array(1024));
          },
        });
      return { capped, pulled: () => pulled };
    };

    it("stops reading a chunk larger than declared", async () => {
      const f = await fixture();
      const { capped, pulled } = counted(f.store, 64 * 1024 * 1024);
      await expect(
        readRelayhistoryExport(f.manifest, capped),
      ).rejects.toMatchObject({ code: "chunk_digest_mismatch" });
      expect(pulled()).toBeLessThan(1024 * 1024);
    });

    it("refuses a reused normalized object past its bound without reading it all", async () => {
      const f = await fixture();
      const total = f.manifest.raw.chunks.reduce(
        (sum, chunk) => sum + chunk.bytes,
        0,
      );
      const key = objectKey(
        normalizedKey(
          loc,
          2,
          `relayhistory-${f.manifest.raw.content_sha256}.ndjson`,
        ),
      );
      await f.store.put(key, new TextEncoder().encode("x"), {
        ifNoneMatch: true,
      });
      const { capped, pulled } = counted(f.store, 64 * 1024 * 1024);
      const store = new Proxy(f.store, {
        get(target, name) {
          if (name === "get")
            return async (k: typeof key, range?: never) =>
              k === key ? capped.get(k, range) : target.get(k, range);
          const value = Reflect.get(target, name, target) as unknown;
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
      const transform = new RelayhistoryExportTransform({
        manifests: f.reader,
        store,
        maxSessionBytes: total,
      });
      await expect(
        transform.normalize({
          traceId: TRACE,
          workspaceId: loc.workspaceId,
          manifestKey: f.key,
        }),
      ).rejects.toMatchObject({
        code: "normalized_object_too_large",
        retryable: false,
      });
      expect(pulled()).toBeLessThan(1024 * 1024);
    });
  });

  describe("session size", () => {
    it("refuses a session past its total before reading any chunk", async () => {
      const f = await fixture();
      let reads = 0;
      const store: ObjectStore = Object.create(f.store);
      store.get = async (...args) => {
        reads += 1;
        return f.store.get(...args);
      };
      const total = f.manifest.raw.chunks.reduce(
        (sum, chunk) => sum + chunk.bytes,
        0,
      );
      await expect(
        readRelayhistoryExport(f.manifest, store, undefined, {
          maxSessionBytes: total - 1,
        }),
      ).rejects.toMatchObject({
        code: "relayhistory_session_too_large",
        retryable: false,
      });
      expect(reads).toBe(0);
    });

    it("reads a session exactly at its total", async () => {
      const f = await fixture();
      const total = f.manifest.raw.chunks.reduce(
        (sum, chunk) => sum + chunk.bytes,
        0,
      );
      await expect(
        readRelayhistoryExport(f.manifest, f.store, undefined, {
          maxSessionBytes: total,
        }),
      ).resolves.toHaveLength(3);
    });
  });

  describe("the output it reports", () => {
    const setup = async () => {
      const f = await fixture();
      const transform = new RelayhistoryExportTransform({
        manifests: f.reader,
        store: f.store,
      });
      const pointer = {
        traceId: TRACE,
        workspaceId: loc.workspaceId,
        manifestKey: f.key,
      };
      const key = normalizedKey(
        loc,
        2,
        `relayhistory-${f.manifest.raw.content_sha256}.ndjson`,
      );
      return { ...f, transform, pointer, key: objectKey(key) };
    };
    const reported = (
      output: Awaited<ReturnType<RelayhistoryExportTransform["normalize"]>>,
    ) => output as NormalizedOutput;
    const eventLines = (bytes: Uint8Array) =>
      new TextDecoder()
        .decode(bytes)
        .trim()
        .split("\n")
        .filter((line) => JSON.parse(line).table === "convergence_events")
        .length;

    it("describes the stored bytes: count, digest and size", async () => {
      const f = await setup();
      const output = reported(await f.transform.normalize(f.pointer));
      const stored = await readAll(await f.store.get(f.key));
      expect(output).toMatchObject({
        outputKey: f.key,
        outputSha256: await sha256Hex(stored),
        outputBytes: stored.byteLength,
        convergenceEventCount: eventLines(stored),
        unprojectedRecords: 0,
        revision: f.manifest.revision,
        normalizedVersion: 2,
        contentSha256: f.manifest.raw.content_sha256,
      });
      expect(output.outputKey).toContain("/normalized/v2/");
      expect(output.convergenceEventCount).toBe(3);
    });

    it("counts only convergence_events lines, and reports records with no projection", () => {
      expect(
        projectRelayhistorySession(
          [
            record("event", "1", 1),
            record("catalog", "1", 2, {
              kind: "session",
              payload: { title: "t" },
            }),
            record("presence", "1", 3, { kind: "presence" }),
          ],
          {
            workspaceId: "workspace",
            source: "claude",
            sessionId: "session",
            machineId: "machine",
            userId: "user-1",
          },
        ),
      ).toMatchObject({ unprojected: 1 });
    });

    it("on a content-addressed hit describes the existing object, not this run or its metadata", async () => {
      const f = await setup();
      const stored = new TextEncoder().encode(
        [
          { table: "session_catalog", row: { title: "t" } },
          { table: "convergence_events", row: { event_id: "only" } },
        ]
          .map((line) => `${JSON.stringify(line)}\n`)
          .join(""),
      );
      await f.store.put(f.key, stored, {
        ifNoneMatch: true,
        metadata: { "convergence-event-count": "99", "manifest-revision": "1" },
      });
      expect(await f.transform.normalize(f.pointer)).toMatchObject({
        outputKey: f.key,
        outputSha256: await sha256Hex(stored),
        outputBytes: stored.byteLength,
        convergenceEventCount: 1,
      });
      expect(await readAll(await f.store.get(f.key))).toEqual(stored);
    });

    it("signals a reused object", async () => {
      const f = await setup();
      expect(await f.transform.normalize(f.pointer)).toMatchObject({
        outputReused: false,
        outputDivergent: false,
      });
      expect(await f.transform.normalize(f.pointer)).toMatchObject({
        outputReused: true,
        outputDivergent: false,
      });
    });

    it("signals a reused object that differs from this run's output", async () => {
      const f = await setup();
      await f.store.put(
        f.key,
        new TextEncoder().encode(
          `${JSON.stringify({ table: "convergence_events", row: { event_id: "x" } })}\n`,
        ),
        { ifNoneMatch: true },
      );
      expect(await f.transform.normalize(f.pointer)).toMatchObject({
        outputReused: true,
        outputDivergent: true,
      });
    });

    const v1Key = (sha: string) =>
      objectKey(normalizedKey(loc, 1, `relayhistory-${sha}.ndjson`));

    it("never reads the retired v1 key", async () => {
      const f = await setup();
      const planted = new TextEncoder().encode("not read\n");
      await f.store.put(v1Key(f.manifest.raw.content_sha256), planted, {
        ifNoneMatch: true,
      });
      expect(await f.transform.normalize(f.pointer)).toMatchObject({
        outputKey: f.key,
        normalizedVersion: 2,
        outputReused: false,
      });
      expect(
        await readAll(await f.store.get(v1Key(f.manifest.raw.content_sha256))),
      ).toEqual(planted);
    });

    it("never writes the retired v1 key", async () => {
      const f = await setup();
      await f.transform.normalize(f.pointer);
      await expect(
        f.store.get(v1Key(f.manifest.raw.content_sha256)),
      ).rejects.toMatchObject({ code: "not_found" });
    });

    describe("the manifest version it was sent for", () => {
      const read = { etag: "etag-now" };
      const bound = async (revision: number) => {
        const f = await setup();
        const transform = new RelayhistoryExportTransform({
          manifests: {
            ...f.reader,
            read: async () => ({ manifest: f.manifest, ...read }),
          } as never,
          store: f.store,
        });
        return {
          ...f,
          bound: transform,
          revision: f.manifest.revision + revision,
        };
      };

      it("no-ops a delayed message when a newer manifest stands, publishing nothing", async () => {
        const f = await bound(-1);
        expect(
          await f.bound.normalize(f.pointer, { revision: f.revision }),
        ).toEqual({ skipped: "stale_manifest" });
        await expect(f.store.get(f.key)).rejects.toMatchObject({
          code: "not_found",
        });
      });

      it("no-ops a same-revision message for another object version", async () => {
        const f = await bound(0);
        expect(
          await f.bound.normalize(f.pointer, {
            revision: f.revision,
            workKey: `${f.pointer.manifestKey}@etag-then`,
          }),
        ).toEqual({ skipped: "stale_manifest" });
        await expect(f.store.get(f.key)).rejects.toMatchObject({
          code: "not_found",
        });
      });

      it("retries when the read lags the message", async () => {
        const f = await bound(1);
        await expect(
          f.bound.normalize(f.pointer, { revision: f.revision }),
        ).rejects.toMatchObject({
          code: "manifest_behind_message",
          retryable: true,
        });
      });

      it("normalizes the version it was sent for", async () => {
        const f = await bound(0);
        expect(
          await f.bound.normalize(f.pointer, {
            revision: f.revision,
            workKey: `${f.pointer.manifestKey}@etag-now`,
          }),
        ).toMatchObject({ outputKey: f.key, revision: f.revision });
      });
    });

    it("caps decompression at the chunk's declared raw size", async () => {
      const f = await setup();
      const manifest = structuredClone(f.manifest);
      manifest.raw.chunks[0]!.stored_raw_bytes = 1;
      await expect(
        readRelayhistoryExport(manifest, f.store),
      ).rejects.toMatchObject({
        code: "chunk_decompress_failed",
        retryable: false,
      });
    });

    it.each([
      [
        "a marker it cannot read",
        { version: 2, source: "neon.delivery_records" },
      ],
      ["a backfill with no marker", undefined],
    ])("refuses %s rather than normalizing nothing", async (_name, marker) => {
      const f = await setup();
      const manifest = structuredClone(f.manifest);
      manifest.extensions = marker
        ? { "client.relayhistory_export": marker }
        : {};
      const transform = new RelayhistoryExportTransform({
        manifests: { ...f.reader, read: async () => ({ manifest }) } as never,
        store: f.store,
      });
      await expect(transform.normalize(f.pointer)).rejects.toMatchObject({
        code: "relayhistory_export_marker_missing",
        retryable: false,
      });
    });

    it("refuses a stored object in the superseded envelope format", async () => {
      const f = await setup();
      await f.store.put(f.key, ndjson([record("old", "1", 1)]), {
        ifNoneMatch: true,
      });
      await expect(f.transform.normalize(f.pointer)).rejects.toMatchObject({
        code: "normalized_object_invalid",
        retryable: false,
      });
    });
  });

  it("refuses a chunk whose stored digest does not match the manifest", async () => {
    const f = await fixture();
    f.manifest.raw.chunks[0]!.stored_sha256 = "f".repeat(64);
    await expect(
      readRelayhistoryExport(
        f.manifest,
        f.store,
        (stored) => new Uint8Array(zstdDecompressSync(stored)),
      ),
    ).rejects.toMatchObject({
      code: "chunk_digest_mismatch",
      retryable: false,
    });
  });

  it("requires every chunk to declare its record count", async () => {
    const f = await fixture();
    delete f.manifest.raw.chunks[0]!.records;
    await expect(
      readRelayhistoryExport(
        f.manifest,
        f.store,
        (stored) => new Uint8Array(zstdDecompressSync(stored)),
      ),
    ).rejects.toMatchObject({
      code: "relayhistory_record_count",
      retryable: false,
    });
  });
});
