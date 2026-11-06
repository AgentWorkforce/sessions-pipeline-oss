/**
 * The continuous Neon to S3 export, end to end against the pipeline's own
 * code: objects it writes are read back through `parseManifest`, AlleyOop's
 * `chunksAreServerSealed`, the RelayHistory transform's `readRelayhistoryExport`
 * and the projection, and the reader's SQL runs against a Neon-shaped PGlite.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import {
  deleteParkedSession,
  objectKey,
  parkedManifestKey,
  parseManifest,
  parseSessionKey,
  readAll,
  readRelayhistoryExport,
  relayhistoryExportProvenance,
  type ObjectStore,
} from "../../shared/vendor.ts";
import { canonicalManifestIdentity } from "../../shared/manifest-identity.ts";
import { chunksAreServerSealed } from "../../alleyoop/manifests.ts";
import { buildSessionStream, type DeliveryRecord, type Queryable } from "../relayhistory-export.ts";
import { erasableByExporter, exportUnit, orgPrefix, writeOnly, type ExportTarget } from "../neon-s3-sync/export-session.ts";
import { storeEraser } from "../neon-s3-erasure/eraser.ts";
import { ErasureJob, DEFAULT_ERASURE_CONFIG } from "../neon-s3-erasure/job.ts";
import { boundStrings, EXPORT_KINDS, fencedGroups, MAX_STRING_TOKEN, neonUnitSource, readOnly, sessionUnits, type SessionUnit, type StreamLine, type UnitSource } from "../neon-s3-sync/neon-reader.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { Syncer, DEFAULT_SYNC_CONFIG } from "../neon-s3-sync/sync.ts";
import { Throttle, parseWindows } from "../neon-s3-sync/throttle.ts";

const dirs: string[] = [];
afterAll(async () => {
  await Promise.all(dirs.map((d) => rm(d, { recursive: true, force: true })));
});
async function tempDir() {
  const d = await mkdtemp(join(tmpdir(), "neon-s3-sync-"));
  dirs.push(d);
  return d;
}

/** An fs store that counts writes and deletes. */
async function countingStore() {
  const inner = createFsStore({ root: await tempDir() });
  const counts = { puts: 0, deletes: 0 };
  const store: ObjectStore = {
    backend: inner.backend,
    head: (k) => inner.head(k),
    get: (k, r) => inner.get(k, r),
    getObject: (k, r) => inner.getObject(k, r),
    list: (p, c) => inner.list(p, c),
    presignGet: (k, t) => inner.presignGet(k, t),
    put: (k, b, o) => {
      counts.puts += 1;
      return inner.put(k, b, o);
    },
    delete: (k) => {
      counts.deletes += 1;
      return inner.delete(k);
    },
  };
  return { store, counts };
}

async function allKeys(store: ObjectStore, prefix = "96bulls"): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.replace(/\/+$/, "")), cursor);
    keys.push(...page.items.map((i) => i.key));
    cursor = page.nextCursor;
  } while (cursor);
  return keys.sort();
}

async function readText(store: ObjectStore, key: string) {
  return new TextDecoder().decode(await readAll(await store.get(objectKey(key))));
}

const T0 = Date.parse("2026-10-20T10:00:00Z");
const unit = (orgId: string, over: Partial<SessionUnit> = {}): SessionUnit => ({
  orgId,
  workspaceId: "11111111-1111-4111-8111-111111111111",
  source: "claude",
  sessionId: "sess:with:colons-1",
  firstMs: T0,
  lastMs: T0 + 3_600_000,
  machineIds: [],
  totals: { event_count: "3", models: ["m1"] },
  ...over,
});

function rec(kind: string, origin: string, id: string, payload: Record<string, unknown>, revision = "1", seq?: string): DeliveryRecord {
  return {
    kind,
    origin_id: origin,
    record_id: id,
    revision,
    operation: "upsert",
    received_at: "2026-10-20 10:00:00+00",
    payload,
    ...(seq ? { accepted_seq: seq } : {}),
  };
}

/** A UnitSource over fixed records, one machine per origin, as the Neon reader would emit. */
function fixedSource(records: DeliveryRecord[], fenced = false): UnitSource {
  return {
    fenced: async () => fenced,
    async *lines() {
      for (const r of records) {
        const line: StreamLine = {
          machine: `delivery:${r.origin_id.replace(/[^a-z0-9]/g, "")}`,
          bytes: buildSessionStream([r]).bytes,
          feed: { origin: r.origin_id, revision: Number(r.revision) },
          userId: "user-1",
        };
        yield line;
      }
    },
  };
}

const baseRecords = () => [
  rec("session", "origin-a", "s-1", { session_id: "sess:with:colons-1", title: "t", first_activity_ms: T0 }, "1", "10"),
  rec("session_event", "origin-a", "e-1", { ts_ms: T0 + 1000, role: "user", text: "hello SECRET_MARKER_ALPHA" }, "2", "11"),
  rec("tool_call", "origin-a", "t-1", { ts_ms: T0 + 2000, name: "Bash", tool_use_id: "tu1" }, "3", "12"),
  rec("session_event", "origin-b", "e-2", { ts_ms: T0 + 3000, role: "assistant", text: "from another machine" }, "1", "13"),
];

const target = (store: ObjectStore, over: Partial<ExportTarget> = {}): ExportTarget => ({
  // The exporter only ever gets a store that cannot delete.
  store: writeOnly(store),
  keyRoot: "96bulls",
  grantId: "neon-s3-sync",
  now: () => new Date("2026-10-31T16:00:00Z"),
  ...over,
});

describe("format: the pipeline's own validators accept what the exporter writes", () => {
  test("manifest validates against its key, chunks are server-sealed, the transform reads and projects them", async () => {
    const { store } = await countingStore();
    const records = baseRecords();
    const r = await exportUnit(target(store, { maxChunkBytes: 300 }), unit("org-a"), fixedSource(records));
    expect(r.manifests.length).toBe(2);
    let total = 0;
    for (const m of r.manifests) {
      expect(m.manifestKey.startsWith("96bulls/org-a/v1/ws=11111111-1111-4111-8111-111111111111/")).toBe(true);
      const text = await readText(store, m.manifestKey);
      const manifest = await parseManifest(text, m.manifestKey);
      const loc = parseSessionKey(m.manifestKey)!;
      expect(loc.prefix).toBe("96bulls/org-a");
      expect(loc.sessionId).toBe("sess_with_colons-1");
      expect(loc.startDate).toBe("2026-10-20");
      // AlleyOop's commit check, with the deployment prefix of the manifest's key.
      expect(await chunksAreServerSealed({ store } as never, manifest, loc.prefix)).toBe(true);
      // Without the prefix (the old bug) the chunks are not found.
      expect(await chunksAreServerSealed({ store } as never, manifest, undefined)).toBe(false);
      expect(relayhistoryExportProvenance(manifest)).toEqual({ grantId: "neon-s3-sync", sourceUserId: "user-1" });
      expect(canonicalManifestIdentity(manifest)).toEqual({
        sessionId: "sess:with:colons-1",
        machineId: expect.any(String),
      });
      const read = await readRelayhistoryExport(manifest, store, (b) => new Uint8Array(zstdDecompressSync(b)), {
        prefix: loc.prefix,
      });
      total += read.length;
      const projection = projectRelayhistorySession(read, {
        workspaceId: loc.workspaceId,
        source: loc.harness,
        sessionId: loc.sessionId,
        machineId: loc.machineId,
        userId: "user-1",
      });
      expect(projection).toBeDefined();
    }
    expect(total).toBe(records.length);
  });

  test("multiple chunks: dense seq, contiguous offsets, hash chain, resume marks", async () => {
    const { store } = await countingStore();
    const r = await exportUnit(target(store, { maxChunkBytes: 200 }), unit("org-a"), fixedSource(baseRecords().slice(0, 3)));
    const manifest = await parseManifest(await readText(store, r.manifests[0]!.manifestKey), r.manifests[0]!.manifestKey);
    expect(manifest.raw.chunk_count).toBeGreaterThan(1);
    expect(manifest.raw.chunks.every((c) => (c as { source_origin?: string }).source_origin === "origin-a")).toBe(true);
    expect((manifest.extensions!["client.relayhistory_export"] as { origin_revisions: unknown }).origin_revisions).toEqual({
      "origin-a": 3,
    });
  });

  test("the stored chunks are scrubbed: a secret in the source never reaches storage", async () => {
    const { store } = await countingStore();
    const secret = "AKIAABCDEFGHIJKLMNOP";
    const records = [rec("session_event", "origin-a", "e-1", { ts_ms: T0, text: `key ${secret}` })];
    const r = await exportUnit(target(store), unit("org-a"), fixedSource(records));
    for (const key of await allKeys(store)) {
      const bytes = await readAll(await store.get(objectKey(key)));
      const text = key.endsWith(".zst") ? new TextDecoder().decode(zstdDecompressSync(bytes)) : new TextDecoder().decode(bytes);
      expect(text.includes(secret)).toBe(false);
    }
    expect(r.manifests[0]!.written).toBe(true);
  });
});

describe("strings over the scrub bound", () => {
  test("only the oversize string is cut, with a note; every other byte is Neon's text; the sealer then accepts it", async () => {
    const huge = "x".repeat(300_000) + '"\\u00e9';
    const json = `{"big": 12345678901234567890, "out": ${JSON.stringify(huge)}, "k": "v"}`;
    const r = boundStrings(json);
    expect(r.truncated).toBe(1);
    expect(r.text.startsWith('{"big": 12345678901234567890, "out": "xxx')).toBe(true);
    expect(r.text.endsWith(', "k": "v"}')).toBe(true);
    const out = (JSON.parse(r.text) as { out: string }).out;
    expect(out).toContain("[neon-s3-sync: truncated");
    expect(JSON.stringify(out).length).toBeLessThanOrEqual(MAX_STRING_TOKEN);
    expect(boundStrings('{"a": "b"}')).toEqual({ text: '{"a": "b"}', truncated: 0, maxChars: 0 });
    expect(r.maxChars).toBe(huge.length);
    // escapes: a token long only because of escapes is bounded by its literal length
    const escaped = `{"e": ${JSON.stringify("\u0001".repeat(60_000))}}`;
    expect(escaped.length).toBeGreaterThan(MAX_STRING_TOKEN);
    expect(boundStrings(escaped).truncated).toBe(1);
  });
});

describe("scrub rule set 2: long strings stored whole", () => {
  test("a long string with safe cuts is kept whole (no cut, no ledger row); only an unsegmentable run is cut", () => {
    const long = "word ".repeat(80_000);
    const kept = boundStrings(`{"out": ${JSON.stringify(long)}}`);
    expect(kept).toEqual({ text: `{"out": ${JSON.stringify(long)}}`, truncated: 0, maxChars: 0 });
    const cut = boundStrings(`{"out": ${JSON.stringify("q".repeat(300_000))}}`);
    expect(cut.truncated).toBe(1);
  });

  test("the whole string is sealed (version 2) and the transform reads it back intact", async () => {
    const { store } = await countingStore();
    const long = `${"word ".repeat(80_000)}AKIAIOSFODNN7EXAMPLE tail`;
    const r = await exportUnit(target(store), unit("org-a"), fixedSource([rec("tool_call", "origin-a", "t", { output: long })]));
    const key = r.manifests[0]!.manifestKey;
    const m = await parseManifest(await readText(store, key), key);
    expect(m.raw.scrub).toEqual({ ruleset: "relayhistory-scrub", version: 2 });
    expect(r.manifests[0]!.truncatedStrings).toBe(0);
    const read = await readRelayhistoryExport(m, store, (b) => new Uint8Array(zstdDecompressSync(b)), { prefix: "96bulls/org-a" });
    const out = (read[0]!.payload as { output: string }).output;
    expect(out.endsWith(" tail")).toBe(true);
    expect(out).not.toContain("AKIAIOSFODNN7EXAMPLE");
    expect(out.length).toBeGreaterThan(400_000);
  });

  test("--reexport-truncated queues every ledgered group as changed", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    state.seeRevisions(1, [{ org_id: "o", session_id: "t", revision: "1" }, { org_id: "o", session_id: "u", revision: "1" }]);
    state.markExported("o", "t", 1, ["96bulls/o/v1/k/manifest.json"], Date.now());
    state.markExported("o", "u", 1, ["96bulls/o/v1/j/manifest.json"], Date.now());
    state.recordTruncations("o", "t", [{ key: "96bulls/o/v1/k/manifest.json", strings: 2, maxChars: 300_000 }]);
    // a ledgered group that is failing past its attempt cap is revived too
    state.seeRevisions(1, [{ org_id: "o", session_id: "f", revision: "1" }]);
    state.recordTruncations("o", "f", [{ key: "96bulls/o/v1/f/manifest.json", strings: 1, maxChars: 300_000 }]);
    for (let i = 0; i < 8; i++) state.markFailed("o", "f", "57014", ["96bulls/o/v1/f/manifest.json"], 1, Date.now());
    expect(state.forceReexportTruncated()).toBe(2);
    expect(state.nextWork(Date.now(), 86_400_000, 10).map((w) => `${w.kind}:${w.session_id}`).sort()).toEqual(["changed:t", "retry:f"]);
    state.close();
  });
});

describe("a session spanning the scrub rule set 1 -> 2 deploy", () => {
  /** The exporter/courier before the bump: same bytes, scrub-version 1 metadata and manifest. */
  function asV1(inner: ObjectStore): ObjectStore {
    return {
      ...inner,
      head: (k) => inner.head(k),
      get: (k, r) => inner.get(k, r),
      getObject: (k, r) => inner.getObject(k, r),
      list: (p, c) => inner.list(p, c),
      presignGet: (k, t) => inner.presignGet(k, t),
      delete: (k) => inner.delete(k),
      put: (k, b, o) => {
        if (String(k).endsWith("manifest.json"))
          b = new TextEncoder().encode(
            new TextDecoder().decode(b).replace('"scrub":{"ruleset":"relayhistory-scrub","version":2}', '"scrub":{"ruleset":"relayhistory-scrub","version":1}'),
          );
        const md = o?.metadata?.["scrub-version"] ? { ...o.metadata, "scrub-version": "1" } : o?.metadata;
        return inner.put(k, b, { ...o, ...(md ? { metadata: md } : {}) });
      },
    };
  }

  test("an export grown after the deploy lists v1 and v2 chunks and still passes AlleyOop's sealed check", async () => {
    const { store } = await countingStore();
    const first = baseRecords().slice(0, 2);
    const r1 = await exportUnit(target(asV1(store), { maxChunkBytes: 200 }), unit("org-a"), fixedSource(first));
    const key = r1.manifests[0]!.manifestKey;
    const m1 = await parseManifest(await readText(store, key), key);
    expect(m1.raw.scrub!.version).toBe(1);
    expect(await chunksAreServerSealed({ store } as never, m1, "96bulls/org-a")).toBe(true);
    await exportUnit(target(store, { maxChunkBytes: 200 }), unit("org-a"), fixedSource([...first, baseRecords()[2]!]));
    const m2 = await parseManifest(await readText(store, key), key);
    expect(m2.raw.scrub!.version).toBe(1); // the lowest over the chunks
    expect(await chunksAreServerSealed({ store } as never, m2, "96bulls/org-a")).toBe(true);
    // A chunk claiming a newer rule set than this server knows is still refused.
    const chunk = m2.raw.chunks.at(-1)!;
    const raw = `${key.slice(0, -"manifest.json".length)}raw/${String(chunk.seq).padStart(10, "0")}-${chunk.start_offset}-${chunk.sha256_8}.ndjson.zst`;
    const object = await store.getObject(objectKey(raw));
    const body = await readAll(object.body);
    await store.delete(objectKey(raw));
    const { "content-sha256": _sha, ...meta } = object.meta.metadata;
    await store.put(objectKey(raw), body, { metadata: { ...meta, "scrub-version": "99" } });
    expect(await chunksAreServerSealed({ store } as never, m2, "96bulls/org-a")).toBe(false);
  });
});

describe("idempotency", () => {
  test("re-exporting an unchanged session writes and deletes nothing", async () => {
    const { store, counts } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    await exportUnit(t, unit("org-a"), fixedSource(baseRecords()));
    const before = { ...counts };
    const keys = await allKeys(store);
    const again = await exportUnit({ ...t, now: () => new Date("2026-11-01T00:00:00Z") }, unit("org-a"), fixedSource(baseRecords()));
    expect(counts).toEqual(before);
    expect(await allKeys(store)).toEqual(keys);
    expect(again.manifests.every((m) => !m.written && m.chunksSealed === 0 && m.chunksReused > 0)).toBe(true);
  });

  test("a changed session is rewritten: new revision, created_at kept, unchanged chunks reused, orphans deleted", async () => {
    const { store } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    const first = await exportUnit(t, unit("org-a"), fixedSource(baseRecords().slice(0, 3)));
    const key = first.manifests[0]!.manifestKey;
    const v1 = await parseManifest(await readText(store, key), key);
    const grown = [...baseRecords().slice(0, 3), rec("file_edit", "origin-a", "f-1", { ts_ms: T0 + 5000, file_path: "x" }, "4")];
    const later = { ...t, now: () => new Date("2026-11-01T00:00:00Z") };
    const second = await exportUnit(later, unit("org-a"), fixedSource(grown));
    await eraseOrphans(store, "org-a", second);
    const v2 = await parseManifest(await readText(store, key), key);
    expect(v2.revision).toBe(v1.revision + 1);
    expect(v2.created_at).toBe(v1.created_at);
    expect(second.manifests[0]!.chunksReused).toBeGreaterThan(0);
    const raw = (await allKeys(store)).filter((k) => k.includes("/raw/"));
    expect(raw.length).toBe(v2.raw.chunk_count);
  });
});

describe("delete propagation", () => {
  test("a record deleted in Neon is gone from every stored object after the next export", async () => {
    const { store } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    await exportUnit(t, unit("org-a"), fixedSource(baseRecords()));
    const without = baseRecords().filter((r) => r.record_id !== "e-1");
    const r = await exportUnit(t, unit("org-a"), fixedSource(without));
    // The exporter wrote the new revision and deleted nothing; the erasure job removes the old chunks.
    await eraseOrphans(store, "org-a", r);
    for (const key of await allKeys(store)) {
      const bytes = await readAll(await store.get(objectKey(key)));
      const text = key.endsWith(".zst") ? new TextDecoder().decode(zstdDecompressSync(bytes)) : new TextDecoder().decode(bytes);
      expect(text.includes("SECRET_MARKER_ALPHA")).toBe(false);
    }
  });

  test("a session hard-deleted from Neon (no units left) loses every object, including pipeline outputs", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const db = fakeDb([unit("org-a")]);
    const syncer = new Syncer(db, state, t, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => fixedSource(baseRecords()));
    state.seeRevisions(1, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "5" }]);
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 5, kind: "recent", events: 3 })).toBe(true);
    const keys = state.exported("org-a", "sess:with:colons-1")!.keys;
    expect(keys.length).toBe(2);
    // a pipeline output next to the session
    await store.put(objectKey(keys[0]!.replace("manifest.json", "normalized/v2/relayhistory-old.ndjson")), new Uint8Array([1]));
    // Neon hard-deleted the session: its rollups are gone and the revision bumped.
    db.units = [];
    state.seeRevisions(2, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "6" }]);
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 6, kind: "changed", events: 0 })).toBe(true);
    expect((await allKeys(store)).length).toBeGreaterThan(0); // the exporter deleted nothing
    await drainErasures(state, store);
    expect(await allKeys(store)).toEqual([]);
    expect(state.exported("org-a", "sess:with:colons-1")!.keys).toEqual([]);
    expect(state.truncationTotals()).toEqual({ sessions: 0, manifests: 0, strings: 0 });
    state.close();
  });

  test("the erasure job erases an exported group that has no rollups left, and leaves the rest", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const gone = await exportUnit(t, unit("o1", { sessionId: "gone" }), fixedSource(baseRecords().slice(0, 1)));
    const kept = await exportUnit(t, unit("o1", { sessionId: "kept" }), fixedSource(baseRecords().slice(0, 1)));
    state.seeRevisions(1, [{ org_id: "o1", session_id: "kept", revision: "1" }]);
    state.markExported("o1", "gone", 1, gone.manifests.map((m) => m.manifestKey), Date.now() - 1000);
    state.markExported("o1", "kept", 1, kept.manifests.map((m) => m.manifestKey), Date.now() - 1000);
    // a group exported after the sweep started is never "gone", even if the sweep missed it
    const fresh = await exportUnit(t, unit("o1", { sessionId: "fresh" }), fixedSource(baseRecords().slice(0, 1)));
    state.markExported("o1", "fresh", 1, fresh.manifests.map((m) => m.manifestKey), Date.now() + 60_000);
    const db = sweepDb([{ org_id: "o1", session_id: "kept", last_ms: "1", events: "1" }]);
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
    await job.sweep();
    await job.drain();
    expect(await allKeys(store, "96bulls/o1/v1")).toEqual(
      [
        ...(await allKeys(store, fresh.manifests[0]!.manifestKey.replace("/manifest.json", ""))),
        ...(await allKeys(store, kept.manifests[0]!.manifestKey.replace("/manifest.json", ""))),
      ].sort(),
    );
    expect(state.exported("o1", "gone")!.keys).toEqual([]);
    // a sweep that read nothing never erases
    const empty = new ErasureJob(sweepDb([]), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
    await empty.sweep();
    expect(state.erasureBacklog().queued).toBe(0);
    state.close();
  });

  test("an exported session Neon has only in the catalog (no rollup) is never erased", async () => {
    const { store } = await countingStore();
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const r = await exportUnit(target(store), unit("o1", { sessionId: "cat-only" }), fixedSource(baseRecords().slice(0, 1)));
    state.markExported("o1", "cat-only", 0, r.manifests.map((m) => m.manifestKey), Date.now() - 1000);
    const job = new ErasureJob(
      sweepDb([{ org_id: "o9", session_id: "x", last_ms: "1", events: "1" }], [], [{ org_id: "o1", session_id: "cat-only" }]),
      state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {},
    );
    await job.sweep();
    await job.drain();
    expect((await allKeys(store)).length).toBeGreaterThan(0);
    expect(state.exported("o1", "cat-only")!.keys.length).toBe(1);
    state.close();
  });

  test("the erasure job erases a group a deletion fence covers", async () => {
    const { store } = await countingStore();
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const r = await exportUnit(target(store), unit("o1", { sessionId: "s1" }), fixedSource(baseRecords().slice(0, 1)));
    state.seeRevisions(1, [{ org_id: "o1", session_id: "s1", revision: "1" }]);
    state.markExported("o1", "s1", 1, r.manifests.map((m) => m.manifestKey), Date.now() - 1000);
    const job = new ErasureJob(sweepDb([{ org_id: "o1", session_id: "s1", last_ms: "1", events: "1" }], [{ org_id: "o1", session_id: "s1" }]), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
    await job.sweep();
    await job.drain();
    expect(await allKeys(store)).toEqual([]);
    state.close();
  });

  test("the eraser refuses keys outside the item's org or session layout; a leased session waits", async () => {
    const { store } = await countingStore();
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const r = await exportUnit(target(store), unit("org-b"), fixedSource(baseRecords().slice(0, 1)));
    const prefixB = r.manifests[0]!.manifestKey.replace("manifest.json", "");
    state.enqueueErasure([
      { key: prefixB, kind: "prefix", org_id: "org-a", reason: "test" },
      { key: "96bulls/org-a/v1/", kind: "prefix", org_id: "org-a", reason: "test" },
    ]);
    const job = await drainErasures(state, store);
    expect(job.counters.refused).toBe(2);
    expect((await allKeys(store, "96bulls/org-b")).length).toBeGreaterThan(0);
    // lease held by the exporter: the erasure is deferred, not done
    const s2 = new SyncState(join(await tempDir(), "s.sqlite"));
    s2.enqueueErasure([{ key: prefixB, kind: "prefix", org_id: "org-b", reason: "test" }]);
    expect(s2.acquireLease(prefixB, "exporter:1")).toBe(true);
    const j2 = new ErasureJob(fakeDb([]), s2, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
    expect(await j2.drain()).toBe(0);
    expect(j2.counters.deferred).toBe(1);
    s2.releaseLeases("exporter:1", [prefixB]);
    expect(await j2.drain()).toBe(1);
    expect(await allKeys(store, "96bulls/org-b")).toEqual([]);
    state.close();
    s2.close();
  });

  test("an orphan the exporter references again is cancelled, never erased", async () => {
    const { store } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const records = baseRecords().slice(0, 3);
    const syncer = new Syncer(fakeDb([unit("org-a")]), state, t, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => fixedSource(records));
    const w = (rev: number) => ({ org_id: "org-a", session_id: "sess:with:colons-1", revision: rev, kind: "changed" as const, events: 3 });
    expect(await syncer.processGroup(w(1))).toBe(true);
    const last = records.pop()!;
    expect(await syncer.processGroup(w(2))).toBe(true);
    expect(state.erasureBacklog().queued).toBeGreaterThan(0);
    records.push(last); // reverted before the eraser ran: the old chunks are referenced again
    expect(await syncer.processGroup(w(3))).toBe(true);
    await drainErasures(state, store);
    const key = state.exported("org-a", "sess:with:colons-1")!.keys[0]!;
    const m = await parseManifest(await readText(store, key), key);
    expect(await chunksAreServerSealed({ store } as never, m, "96bulls/org-a")).toBe(true);
    state.close();
  });

  test("a fenced session (user deletion tombstone) exports nothing", async () => {
    const { store, counts } = await countingStore();
    const r = await exportUnit(target(store), unit("org-a"), fixedSource(baseRecords(), true));
    expect(r).toMatchObject({ fenced: true, manifests: [] });
    expect(counts.puts).toBe(0);
  });
});

describe("ownership and failure bookkeeping", () => {
  test("a failed run records the keys it wrote under, so a later erasure still removes them", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const failing: UnitSource = {
      fenced: async () => false,
      async *lines() {
        for (let i = 0; i < 3; i++)
          yield { machine: "delivery:m1", bytes: buildSessionStream([rec("session_event", "o", `e${i}`, { x: "y".repeat(150) })]).bytes, userId: "u" };
        throw Object.assign(new Error("boom"), { code: "57014" });
      },
    };
    const syncer = new Syncer(fakeDb([unit("org-a")]), state, { ...t, maxChunkBytes: 200 }, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => failing);
    state.seeRevisions(1, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "1" }]);
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 1, kind: "recent", events: 3 })).toBe(false);
    const row = state.exported("org-a", "sess:with:colons-1")!;
    expect(row.error).toBe("57014");
    expect(row.keys).toHaveLength(1);
    expect((await allKeys(store)).length).toBeGreaterThan(0); // sealed chunks, no manifest
    // Neon then hard-deletes the session: the erasure job's sweep no longer sees it and erases the prefix.
    const job = new ErasureJob(sweepDb([{ org_id: "org-z", session_id: "other", last_ms: "1", events: "1" }]), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
    await job.sweep();
    await job.drain();
    expect(await allKeys(store)).toEqual([]);
    state.close();
  });

  test("the truncation ledger records every cut manifest and clears when the group is re-exported whole", async () => {
    const { store } = await countingStore();
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    let huge = true;
    const src: UnitSource = {
      fenced: async () => false,
      async *lines() {
        const payload = huge ? `{"out": ${JSON.stringify("z".repeat(300_000))}}` : `{"out": "short"}`;
        const { boundStrings } = await import("../neon-s3-sync/neon-reader.ts");
        const b = boundStrings(payload);
        const r = rec("tool_call", "o", "t", {});
        yield { machine: "delivery:m", bytes: buildSessionStream([{ ...r, payload_json: b.text }]).bytes, userId: "u", truncated: b.truncated, truncatedMaxChars: b.maxChars };
      },
    };
    const syncer = new Syncer(fakeDb([unit("org-a")]), state, target(store), fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => src);
    state.seeRevisions(1, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "1" }]);
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 1, kind: "recent", events: 1 })).toBe(true);
    expect(state.truncationTotals()).toEqual({ sessions: 1, manifests: 1, strings: 1, maxChars: 300_000 });
    const key = state.exported("org-a", "sess:with:colons-1")!.keys[0]!;
    const m = JSON.parse(await readText(store, key));
    expect(m.extensions["client.relayhistory_export"].catalog.truncated_strings).toBe(1);
    huge = false;
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 2, kind: "changed", events: 1 })).toBe(true);
    expect(state.truncationTotals()).toEqual({ sessions: 0, manifests: 0, strings: 0 });
    state.close();
  });

  test("a failed group is retried at once when its revision moves; a fence forces a re-export", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const now = Date.now();
    state.seeRevisions(1, [{ org_id: "o", session_id: "s", revision: "3" }, { org_id: "o", session_id: "f", revision: "1" }]);
    state.seeActivity(1, [{ org_id: "o", session_id: "s", last_ms: String(now), events: "1" }, { org_id: "o", session_id: "f", last_ms: String(now), events: "1" }]);
    state.markFailed("o", "s", "57014", [], 3, now);
    state.markExported("o", "f", 1, ["96bulls/o/v1/k/manifest.json"], now);
    expect(state.nextWork(now, 86_400_000, 10)).toEqual([]);
    state.seeRevisions(2, [{ org_id: "o", session_id: "s", revision: "4" }]);
    expect(state.nextWork(now, 86_400_000, 10).map((w) => `${w.kind}:${w.session_id}`)).toEqual(["retry:s"]);
    expect(state.forceChanged([{ org_id: "o", session_id: "f" }])).toBe(1);
    expect(state.nextWork(now, 86_400_000, 10).map((w) => `${w.kind}:${w.session_id}`)).toEqual(["changed:f", "retry:s"]);
    state.close();
  });

  test("a fence revives a failure past its attempt cap, once", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    let now = Date.now();
    state.seeRevisions(1, [{ org_id: "o", session_id: "x", revision: "1" }]);
    for (let i = 0; i < 8; i++) state.markFailed("o", "x", "57014", ["96bulls/o/v1/b/manifest.json"], 1, now);
    now += 5 * 60 * 60 * 1000;
    expect(state.nextWork(now, 86_400_000, 10)).toEqual([]);
    expect(state.forceChanged([{ org_id: "o", session_id: "x" }])).toBe(1);
    expect(state.forceChanged([{ org_id: "o", session_id: "x" }])).toBe(0);
    expect(state.nextWork(now, 86_400_000, 10).map((w) => w.session_id)).toEqual(["x"]);
    state.close();
  });

  test("a stale key that is not this exporter's does not stop the group's own erasure", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const own = await exportUnit(t, unit("org-a"), fixedSource(baseRecords().slice(0, 1)));
    const other = await exportUnit(t, unit("org-a", { sessionId: "other" }), fixedSource(baseRecords().slice(0, 1)));
    state.seeRevisions(1, [{ org_id: "org-a", session_id: "sess:with:colons-1", revision: "2" }]);
    // the group's ledger wrongly holds another session's key as well as its own
    state.markExported("org-a", "sess:with:colons-1", 1, [other.manifests[0]!.manifestKey, own.manifests[0]!.manifestKey]);
    const db = fakeDb([]);
    const syncer = new Syncer(db, state, t, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => fixedSource([]));
    // the other session's manifest now belongs to a client: the exporter must not queue it
    const m = JSON.parse(await readText(store, other.manifests[0]!.manifestKey));
    m.raw.owner = m.writer.delivery_instance_id = "client-1";
    await store.put(objectKey(other.manifests[0]!.manifestKey), new TextEncoder().encode(JSON.stringify(m)));
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 2, kind: "changed", events: 0 })).toBe(true);
    await drainErasures(state, store);
    expect(await allKeys(store, own.manifests[0]!.manifestKey.replace("/manifest.json", ""))).toEqual([]);
    expect((await allKeys(store, other.manifests[0]!.manifestKey.replace("/manifest.json", ""))).length).toBeGreaterThan(0);
    expect(state.exported("org-a", "sess:with:colons-1")!.keys).toEqual([]);
    state.close();
  });

  test("a manifest the exporter does not own is neither overwritten nor cleaned up", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const r = await exportUnit(t, unit("org-a"), fixedSource(baseRecords().slice(0, 1)));
    const key = r.manifests[0]!.manifestKey;
    const manifest = JSON.parse(await readText(store, key));
    manifest.raw.owner = manifest.writer.delivery_instance_id = "client-courier-1";
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)));
    const before = await allKeys(store);
    await expect(exportUnit(t, unit("org-a"), fixedSource(baseRecords()))).rejects.toMatchObject({ code: "foreign_manifest" });
    expect(await erasableByExporter(t, "org-a", key)).toBe(false);
    expect(await allKeys(store)).toEqual(before);
  });

  test("a foreign manifest's key is never recorded, so no sweep can hand it to the eraser", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const r = await exportUnit(t, unit("org-a"), fixedSource(baseRecords().slice(0, 1)));
    const key = r.manifests[0]!.manifestKey;
    const m = JSON.parse(await readText(store, key));
    m.raw.owner = m.writer.delivery_instance_id = "client-1";
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(m)));
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const syncer = new Syncer(fakeDb([unit("org-a")]), state, t, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => fixedSource(baseRecords()));
    expect(await syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 1, kind: "recent", events: 1 })).toBe(false);
    expect(state.exported("org-a", "sess:with:colons-1")!.keys).toEqual([]);
    state.close();
  });

  test("a restarted exporter releases its predecessor's leases, never the eraser's", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    expect(state.acquireLease("p1/", "exporter:1")).toBe(true);
    expect(state.acquireLease("p2/", "eraser:9")).toBe(true);
    expect(state.releaseStaleLeases("exporter:", "exporter:2")).toBe(1);
    expect(state.acquireLease("p1/", "exporter:2")).toBe(true);
    expect(state.acquireLease("p2/", "exporter:2")).toBe(false);
    state.close();
  });

  test("keys of a run that died mid-group are recovered at the next start; a lost lease stops the write", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    state.recordPendingKey("o", "s", "96bulls/o/v1/k/manifest.json");
    expect(state.recoverPendingKeys()).toBe(1);
    expect(state.exported("o", "s")).toMatchObject({ error: "interrupted", keys: ["96bulls/o/v1/k/manifest.json"] });
    expect(state.recoverPendingKeys()).toBe(0);
    const { store } = await countingStore();
    await expect(
      exportUnit({ ...target(store), renew: () => false }, unit("org-a"), fixedSource(baseRecords().slice(0, 1))),
    ).rejects.toMatchObject({ code: "lease_lost" });
    expect(await allKeys(store)).toEqual([]);
    state.close();
  });

  test("ids longer than 128 chars that differ only at the end get distinct keys", async () => {
    const { component } = await import("../neon-s3-sync/export-session.ts");
    const base = `pi:${"a".repeat(64)}:pi_session_${"b".repeat(40)}:subagent_1234-5678`;
    const [a, b] = [`${base}:agent-1`, `${base}:agent-2`];
    expect(component(a)).not.toBe(component(b));
    expect(component(a).length).toBe(128);
    expect(component(a)).toMatch(/^[A-Za-z0-9._-]{128}$/);
    expect(component("short:id")).toBe("short_id"); // unchanged for ids that fit
    const { store } = await countingStore();
    await exportUnit(target(store), unit("org-a", { sessionId: a }), fixedSource(baseRecords().slice(0, 1)));
    const r = await exportUnit(target(store), unit("org-a", { sessionId: b }), fixedSource(baseRecords().slice(0, 1)));
    expect(r.manifests[0]!.written).toBe(true);
  });

  test("a manifest of another Neon session at a group's key is never queued for that group's erasure", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const r = await exportUnit(t, unit("org-a", { sessionId: "owner" }), fixedSource(baseRecords().slice(0, 1)));
    const key = r.manifests[0]!.manifestKey;
    expect(await erasableByExporter(t, "org-a", key, "owner")).toBe(true);
    expect(await erasableByExporter(t, "org-a", key, "someone-else")).toBe(false);
  });

  test("a crash mid-group leaves its keys recorded; after a hard delete the erasure job removes the prefix", async () => {
    const { store } = await countingStore();
    const t = target(store, { maxChunkBytes: 200 });
    const path = join(await tempDir(), "s.sqlite");
    const state = new SyncState(path);
    let release!: () => void;
    const hang = new Promise<void>((r) => (release = r));
    const src: UnitSource = {
      fenced: async () => false,
      async *lines() {
        for (let i = 0; i < 3; i++)
          yield { machine: "delivery:m1", bytes: buildSessionStream([rec("session_event", "o", `e${i}`, { x: "y".repeat(150) })]).bytes, userId: "u" };
        await hang; // the process "dies" here: no markFailed, no markExported
      },
    };
    const syncer = new Syncer(fakeDb([unit("org-a")]), state, t, fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false, () => src);
    const running = syncer.processGroup({ org_id: "org-a", session_id: "sess:with:colons-1", revision: 1, kind: "recent", events: 3 });
    await new Promise((r) => setTimeout(r, 200));
    expect((await allKeys(store)).length).toBeGreaterThan(0); // chunks sealed, no manifest
    // A new process on the same state file.
    const after = new SyncState(path);
    expect(after.recoverPendingKeys()).toBe(1);
    after.releaseStaleLeases("exporter:", "exporter:new");
    const job = new ErasureJob(sweepDb([{ org_id: "org-z", session_id: "other", last_ms: "1", events: "1" }]), after, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, () => Date.now() + 60_000, async () => {});
    await job.sweep();
    await job.drain();
    expect(await allKeys(store)).toEqual([]);
    after.close();
    release();
    await running.catch(() => undefined);
    state.close();
  });

  test("two Neon sessions whose ids sanitize to one key do not overwrite each other", async () => {
    const { store } = await countingStore();
    const t = target(store);
    await exportUnit(t, unit("org-a", { sessionId: "a:b" }), fixedSource(baseRecords().slice(0, 1)));
    await expect(exportUnit(t, unit("org-a", { sessionId: "a_b" }), fixedSource(baseRecords()))).rejects.toMatchObject({
      code: "key_collision",
    });
  });
});

describe("prefixes and tenant isolation", () => {
  test("the org prefix is one safe path segment under the key root", () => {
    expect(orgPrefix("96bulls", "4cb2aaaa-0000-4000-8000-000000000000")).toBe("96bulls/4cb2aaaa-0000-4000-8000-000000000000");
    for (const bad of ["", "a/b", "..", ".x", "a b"]) expect(() => orgPrefix("96bulls", bad)).toThrow();
    expect(() => orgPrefix("96Bulls/x", "o")).toThrow();
  });

  test("two orgs with identical workspace and session ids never share or delete each other's objects", async () => {
    const { store } = await countingStore();
    const t = target(store);
    const a = await exportUnit(t, unit("org-a"), fixedSource(baseRecords()));
    const b = await exportUnit(t, unit("org-b"), fixedSource(baseRecords()));
    const keysA = await allKeys(store, "96bulls/org-a/");
    const keysB = await allKeys(store, "96bulls/org-b/");
    expect(keysA.length).toBeGreaterThan(0);
    expect(keysB.length).toBe(keysA.length);
    expect((await allKeys(store)).length).toBe(keysA.length + keysB.length);
    // An org-b key is never erasable for org-a.
    expect(await erasableByExporter(t, "org-a", b.manifests[0]!.manifestKey)).toBe(false);
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    state.enqueueErasure(a.manifests.map((m) => ({ key: m.manifestKey.replace("manifest.json", ""), kind: "prefix" as const, org_id: "org-a", reason: "test" })));
    await drainErasures(state, store);
    state.close();
    expect(await allKeys(store, "96bulls/org-a/")).toEqual([]);
    expect(await allKeys(store, "96bulls/org-b/")).toEqual(keysB);
  });
});

describe("parked-session erasure under a deployment prefix (prefix bug fix)", () => {
  test("deleteParkedSession removes parked chunks under 96bulls/<org>/", async () => {
    const { store } = await countingStore();
    const r = await exportUnit(target(store), unit("org-a"), fixedSource(baseRecords().slice(0, 1)));
    const key = r.manifests[0]!.manifestKey;
    const loc = parseSessionKey(key)!;
    const text = await readText(store, key);
    await store.put(objectKey(parkedManifestKey(key, loc.machineId)), new TextEncoder().encode(text));
    await store.delete(objectKey(key));
    const raw = (await allKeys(store)).filter((k) => k.includes("/raw/"));
    expect(raw.length).toBe(1);
    expect(await deleteParkedSession(store, key, loc.machineId)).toEqual({ manifest: true, chunks: 1 });
    expect(await allKeys(store)).toEqual([]);
  });
});

describe("throttle", () => {
  test("inside a slow window the rate is halved", async () => {
    let now = Date.parse("2026-10-31T14:00:00Z");
    const slept: number[] = [];
    const t = new Throttle({
      rowsPerSec: 100,
      bytesPerSec: 1e9,
      slowWindows: parseWindows("2026-10-31T15:00Z/2026-11-01T00:00Z"),
      slowFactor: 0.5,
      now: () => now,
      sleep: async (ms) => {
        slept.push(ms);
        now += ms;
      },
    });
    expect(t.rates().rowsPerSec).toBe(100);
    await t.take(1100, 0); // 11 s of rows at 100/s, 1 s burst allowed
    expect(slept.at(-1)).toBe(10_000);
    now = Date.parse("2026-10-31T16:00:00Z");
    await t.take(0, 0);
    expect(t.rates()).toMatchObject({ rowsPerSec: 50, slow: true });
    await t.take(550, 0);
    expect(slept.at(-1)).toBe(10_000);
    expect(() => parseWindows("nope")).toThrow();
  });
});

describe("run loop", () => {
  test("lanes pull continuously: a slow group does not hold the others; every group is exported once", async () => {
    const { store } = await countingStore();
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const now = Date.now();
    const ids = Array.from({ length: 40 }, (_, i) => `s${i}`);
    state.seeRevisions(1, ids.map((id) => ({ org_id: "org-a", session_id: id, revision: "1" })));
    state.seeActivity(1, ids.map((id, i) => ({ org_id: "org-a", session_id: id, last_ms: String(now - i * 1000), events: "1" })));
    for (const k of ["act_sweep_done", "rev_sweep_done"]) state.setMeta(k, "1");
    for (const k of ["act_sweep_at", "rev_sweep_at"]) state.setMeta(k, String(now));
    const order: string[] = [];
    const db: Queryable = {
      async query<T>(sql: string, params?: unknown[]) {
        if (sql.includes("pg_locks")) return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
        if (sql.includes("FROM sessions.session_catalog")) return { rows: [] as T[] };
        if (sql.includes("LIMIT 10000"))
          return { rows: [{ workspace_id: "w", source: "claude", first_ms: String(T0), last_ms: String(T0), machine_ids: [], models: [], event_count: "1", input_tokens: "0", output_tokens: "0", reasoning_tokens: "0", cache_read_tokens: "0", cache_create_tokens: "0", cost_usd_metros: "0", cost_usd_micros: "0", _s: params?.[1] }] as T[] };
        throw new Error("unexpected query");
      },
    };
    const source = (): UnitSource => ({
      fenced: async () => false,
      async *lines(u) {
        if (u.sessionId === "s0") {
          // Hold until every other group is done (a batch barrier would deadlock here; 8 s fallback).
          const t0 = Date.now();
          while (order.length < 39 && Date.now() - t0 < 8000) await new Promise((r) => setTimeout(r, 10));
        } // the slow one
        order.push(u.sessionId);
        yield { machine: "delivery:m", bytes: buildSessionStream([rec("session_event", "o", u.sessionId, { a: 1 })]).bytes, userId: "u" };
      },
    });
    // Real (short) timers: an instantly-resolving sleep would spin the sweep loop in microtasks.
    const throttle = new Throttle({ rowsPerSec: 1e9, bytesPerSec: 1e12, slowWindows: [], slowFactor: 1, sleep: (ms) => new Promise((r) => setTimeout(r, Math.min(ms, 5))) });
    const syncer = new Syncer(db, state, target(store), throttle, () => {}, DEFAULT_SYNC_CONFIG, () => false, source, [db, db]);
    await syncer.run({ once: true });
    expect([...order].sort()).toEqual([...ids].sort());
    expect(syncer.counters.groups).toBe(40);
    // the slow group finished last: the other lanes did not wait for it
    expect(order.indexOf("s0")).toBe(39);
    state.close();
  });
});

describe("work order", () => {
  test("changed (past debounce) before recent before history, newest first", async () => {
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const now = Date.parse("2026-10-31T16:00:00Z");
    const day = 86_400_000;
    state.seeRevisions(1, ["old", "new", "mid", "chg"].map((s) => ({ org_id: "o", session_id: s, revision: "2" })));
    state.seeActivity(1, [
      { org_id: "o", session_id: "old", last_ms: String(now - 100 * day), events: "1" },
      { org_id: "o", session_id: "new", last_ms: String(now - day), events: "1" },
      { org_id: "o", session_id: "mid", last_ms: String(now - 5 * day), events: "1" },
      { org_id: "o", session_id: "chg", last_ms: String(now - 50 * day), events: "1" },
    ]);
    state.markExported("o", "chg", 1, ["k"], now - 2 * 3_600_000);
    expect(state.nextWork(now, 14 * day, 10).map((w) => `${w.kind}:${w.session_id}`)).toEqual([
      "changed:chg",
      "recent:new",
      "recent:mid",
      "history:old",
    ]);
    // Just exported: the change waits for its debounce.
    state.markExported("o", "chg", 1, ["k"], now);
    expect(state.nextWork(now, 14 * day, 10).map((w) => w.session_id)).not.toContain("chg");
    state.close();
  });
});

// ---------------------------------------------------------------- Neon-shaped SQL

const NEON_SCHEMA = `
CREATE SCHEMA sessions;
CREATE TABLE sessions.delivery_records (org_id text, workspace_id text, origin_id text, record_id text, revision_id text,
  revision bigint, digest text, kind text, source text, session_id text, operation text, payload jsonb, user_id text,
  received_at timestamptz, PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
CREATE INDEX delivery_records_lookup ON sessions.delivery_records (org_id, workspace_id, kind, source, session_id, origin_id, record_id);
CREATE TABLE sessions.delivery_projection_acceptance (org_id text, workspace_id text, origin_id text, record_id text, accepted_seq bigint, historical boolean,
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
CREATE TABLE sessions.delivery_catalog_acceptance (org_id text, workspace_id text, origin_id text, record_id text, accepted_seq bigint,
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
CREATE TABLE sessions.convergence_events (org_id text, workspace_id text, machine_id text, user_id text, source text, session_id text,
  event_id text, kind text, ts timestamptz, content text, ingested_at timestamptz, delivery_record_id text,
  PRIMARY KEY (org_id, machine_id, source, session_id, kind, event_id));
CREATE TABLE sessions.session_rollups (org_id text, session_id text, workspace_id text, source text, project_id text,
  first_ts timestamptz, last_ts timestamptz, event_count bigint, machine_ids text[], models text[],
  cost_usd_micros bigint, input_tokens bigint, output_tokens bigint, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint);
CREATE TABLE sessions.session_revisions (org_id text, session_id text, revision bigint, updated_at timestamptz, PRIMARY KEY (org_id, session_id));
CREATE TABLE sessions.deleted_upload_sessions (org_id text, workspace_id text, user_id text, origin_id text, source text, session_id text);
CREATE TABLE sessions.session_catalog (org_id text, workspace_id text, source text, session_id text, record_id text, origin_id text,
  user_id text, title text, first_activity_at timestamptz, last_activity_at timestamptz, received_at timestamptz,
  PRIMARY KEY (org_id, workspace_id, source, session_id));
CREATE TABLE sessions.session_resets (org_id text, workspace_id text, user_id text, request_id text, deleted bigint);
`;

describe("Neon reader SQL (Neon-shaped PGlite)", () => {
  test("pages records, settles a contended record by the ledger, skips deletes, adds undelivered events, honours the fence", async () => {
    const pg = new PGlite();
    await pg.exec(NEON_SCHEMA);
    const ws = "w1";
    const ins = (origin: string, id: string, kind: string, op: string, payload: unknown, rev = 1) =>
      pg.query(
        `INSERT INTO sessions.delivery_records VALUES ('o1', $1, $2, $3, 'r', $4, 'd', $5, 'claude', 's1', $6, $7, 'u1', '2026-10-20T10:00:00Z')`,
        [ws, origin, id, rev, kind, op, payload === null ? null : typeof payload === "string" ? payload : JSON.stringify(payload)],
      );
    await ins("oa", "s-1", "session", "upsert", { title: "t" });
    for (let i = 0; i < 7; i++) await ins("oa", `e-${i}`, "session_event", "upsert", `{"ts_ms": ${1000 + i}, "big": 12345678901234567890}`);
    await ins("oa", "gone", "session_event", "delete", null, 2);
    // contended: both origins hold c-1; ledger says ob accepted last
    await ins("oa", "c-1", "session_event", "upsert", { v: "oa" });
    await ins("ob", "c-1", "session_event", "upsert", { v: "ob" });
    await pg.query(`INSERT INTO sessions.delivery_projection_acceptance VALUES ('o1', $1, 'oa', 'c-1', 5, false), ('o1', $1, 'ob', 'c-1', 9, false)`, [ws]);
    // contended where the last accepted write is a tombstone: nobody exports it
    await ins("oa", "c-2", "session_event", "upsert", { v: "x" });
    await ins("ob", "c-2", "session_event", "delete", null);
    await pg.query(`INSERT INTO sessions.delivery_projection_acceptance VALUES ('o1', $1, 'oa', 'c-2', 5, false), ('o1', $1, 'ob', 'c-2', 9, false)`, [ws]);
    // an undelivered server-side event
    await pg.query(
      `INSERT INTO sessions.convergence_events VALUES ('o1', $1, 'relay-events:chatgpt', 'u1', 'claude', 's1', 'ev-1', 'message', '2026-10-20T10:00:00Z', 'hi', '2026-10-20T10:00:01Z', NULL)`,
      [ws],
    );
    await pg.query(
      `INSERT INTO sessions.session_rollups VALUES ('o1', 's1', $1, 'claude', NULL, '2026-10-20T10:00:00Z', '2026-10-20T11:00:00Z', 10,
        ARRAY['delivery:x', 'relay-events:chatgpt'], ARRAY['m'], 1, 2, 3, 4, 5, 6)`,
      [ws],
    );
    // a second rollup row (another project) with a different machine list
    await pg.query(
      `INSERT INTO sessions.session_rollups VALUES ('o1', 's1', $1, 'claude', 'p2', '2026-10-19T09:00:00Z', '2026-10-20T12:00:00Z', 5,
        ARRAY['delivery:x'], ARRAY['m', 'n'], 1, 2, 3, 4, 5, 6)`,
      [ws],
    );
    const db = readOnly(pg as unknown as Queryable);
    const units = await sessionUnits(db, "o1", "s1");
    expect(units).toHaveLength(1);
    expect(units[0]).toMatchObject({
      workspaceId: ws,
      source: "claude",
      machineIds: ["delivery:x", "relay-events:chatgpt"],
      firstMs: Date.parse("2026-10-19T09:00:00Z"),
      lastMs: Date.parse("2026-10-20T12:00:00Z"),
    });
    expect(units[0]!.totals).toMatchObject({ event_count: "15", models: ["m", "n"] });
    let pages = 0;
    // The paged path (forced with a tiny fast-path limit) and the one-statement path give identical lines.
    const source = neonUnitSource(db, async () => void (pages += 1), 3, 2);
    const lines: StreamLine[] = [];
    for await (const l of source.lines(units[0]!)) lines.push(l);
    const fast: StreamLine[] = [];
    for await (const l of neonUnitSource(db, async () => {}).lines(units[0]!)) fast.push(l);
    expect(fast.map((l) => [l.machine, new TextDecoder().decode(l.bytes)])).toEqual(
      lines.map((l) => [l.machine, new TextDecoder().decode(l.bytes)]),
    );
    const parsed = lines.map((l) => JSON.parse(new TextDecoder().decode(l.bytes)) as { record_id: string; origin_id: string; kind: string });
    const ids = parsed.map((p) => `${p.kind}:${p.origin_id}:${p.record_id}`);
    expect(ids).toContain("session:oa:s-1");
    expect(ids.filter((i) => i.startsWith("session_event:oa:e-"))).toHaveLength(7);
    expect(ids).toContain("session_event:ob:c-1");
    expect(ids).not.toContain("session_event:oa:c-1");
    expect(ids.some((i) => i.endsWith(":c-2"))).toBe(false);
    expect(ids.some((i) => i.endsWith(":gone"))).toBe(false);
    expect(ids).toContain("convergence_event:machine:relay-events:chatgpt:ev-1");
    // the delivery machine is Postgres's md5 of [workspace, origin], as Neon's projection names it
    const md5 = (await pg.query<{ m: string }>(`SELECT 'delivery:' || md5(jsonb_build_array('w1', 'oa')::text) AS m`)).rows[0]!.m;
    expect(lines.find((l) => l.feed?.origin === "oa")!.machine).toBe(md5);
    // a bigint payload is written as Neon's own text, not rounded
    expect(new TextDecoder().decode(lines[1]!.bytes)).toContain("12345678901234567890");
    expect(pages).toBeGreaterThan(3);
    expect(await source.fenced(units[0]!)).toBe(false);
    // a tool output longer than the scrub bound is stored (cut, with its note), not refused
    await ins("oa", "huge", "tool_call", "upsert", { output: "y".repeat(400_000) });
    const { store } = await countingStore();
    const exported = await exportUnit(target(store), units[0]!, neonUnitSource(db, async () => {}));
    expect(exported.manifests.reduce((n, m) => n + m.truncatedStrings, 0)).toBe(1);
    for (const m of exported.manifests) await parseManifest(await readText(store, m.manifestKey), m.manifestKey);
    await pg.query(`INSERT INTO sessions.deleted_upload_sessions VALUES ('o1', $1, 'u1', 'oa', 'claude', 's1')`, [ws]);
    // the fence is cached for two minutes; a fresh source reads it again
    expect(await neonUnitSource(db, async () => {}).fenced(units[0]!)).toBe(true);
    expect(await fencedGroups(db)).toEqual([{ org_id: "o1", session_id: "s1" }]);
    expect(await source.fenced(units[0]!, true)).toBe(true); // fresh: bypasses the cached "not fenced"
    // A reset, then a new session from the same user: the reset tombstoned s1 (and
    // deleted its records); s2 was delivered after it and must be exported, never fenced.
    await pg.query(`INSERT INTO sessions.session_resets VALUES ('o1', $1, 'u1', 'r1', 1)`, [ws]);
    await pg.query(
      `INSERT INTO sessions.delivery_records VALUES ('o1', $1, 'oa', 'post-reset', 'r', 1, 'd', 'session', 'claude', 's2', 'upsert', '{"title":"new"}', 'u1', '2026-10-21T10:00:00Z')`,
      [ws],
    );
    const s2 = { ...units[0]!, sessionId: "s2" };
    expect(await neonUnitSource(db, async () => {}).fenced(s2)).toBe(false);
    expect(await neonUnitSource(db, async () => {}).fenced(s2, true)).toBe(false);
    expect(await fencedGroups(db)).toEqual([{ org_id: "o1", session_id: "s1" }]);
    const post = await exportUnit(target(store), s2, neonUnitSource(db, async () => {}));
    expect(post.fenced).toBe(false);
    expect(post.manifests.length).toBe(1);
    await pg.close();
  }, 60_000);

  test("a session with delivery records and a catalog row but no rollup is discovered and exported", async () => {
    const pg = new PGlite();
    await pg.exec(NEON_SCHEMA);
    await pg.query(`INSERT INTO sessions.session_catalog VALUES ('o1', 'w1', 'claude', 'c1', 'r0', 'oa', 'u1', 't', '2026-10-20T10:00:00Z', '2026-10-20T11:00:00Z', '2026-10-20T11:00:00Z')`);
    for (const [i, kind] of ["session", "presence", "session_event"].entries())
      await pg.query(
        `INSERT INTO sessions.delivery_records VALUES ('o1', 'w1', 'oa', $1, 'r', 1, 'd', $2, 'claude', 'c1', 'upsert', '{"a":1}', 'u1', '2026-10-20T10:00:00Z')`,
        [`r${i}`, kind],
      );
    const db = readOnly(pg as unknown as Queryable);
    const { catalogPage } = await import("../neon-s3-sync/neon-reader.ts");
    expect((await catalogPage(db, null)).map((r) => r.session_id)).toEqual(["c1"]);
    const units = await sessionUnits(db, "o1", "c1");
    expect(units).toHaveLength(1);
    expect(units[0]).toMatchObject({ workspaceId: "w1", source: "claude", firstMs: Date.parse("2026-10-20T10:00:00Z") });
    const { store } = await countingStore();
    const r = await exportUnit(target(store), units[0]!, neonUnitSource(db, async () => {}));
    expect(r.manifests).toHaveLength(1);
    const m = await parseManifest(await readText(store, r.manifests[0]!.manifestKey), r.manifests[0]!.manifestKey);
    const read = await readRelayhistoryExport(m, store, (b) => new Uint8Array(zstdDecompressSync(b)), { prefix: "96bulls/o1" });
    expect(read.map((x) => x.kind).sort()).toEqual(["presence", "session", "session_event"]);
    // The exporter's activity sweep sees it.
    const state = new SyncState(join(await tempDir(), "s.sqlite"));
    const syncer = new Syncer(db, state, target(store), fastThrottle(), () => {}, DEFAULT_SYNC_CONFIG, () => false);
    await syncer.sweepActivity();
    expect(state.nextWork(Date.parse("2026-10-21T00:00:00Z"), 14 * 86_400_000, 10).map((w) => w.session_id)).toEqual(["c1"]);
    state.close();
    await pg.close();
  }, 60_000);

  test("exports every kind the projection reads, including trajectory and commit_link", async () => {
    const projected = ["history", "session_event", "tool_call", "file_edit", "trajectory", "session", "relationship", "session_marker", "commit_link"];
    for (const k of projected) expect(EXPORT_KINDS as readonly string[]).toContain(k);
    const pg = new PGlite();
    await pg.exec(NEON_SCHEMA);
    for (const [i, kind] of ["trajectory", "commit_link", "session_event"].entries())
      await pg.query(
        `INSERT INTO sessions.delivery_records VALUES ('o1', 'w1', 'oa', $1, 'r', 1, 'd', $2, 'claude', 's1', 'upsert', '{"a":1}', 'u1', '2026-10-20T10:00:00Z')`,
        [`r${i}`, kind],
      );
    const db = readOnly(pg as unknown as Queryable);
    const u = { ...unit("o1"), workspaceId: "w1", sessionId: "s1" };
    for (const small of [4000, 1]) {
      const kinds: string[] = [];
      for await (const l of neonUnitSource(db, async () => {}, 2, small).lines(u))
        kinds.push((JSON.parse(new TextDecoder().decode(l.bytes)) as { kind: string }).kind);
      expect(kinds.sort()).toEqual(["commit_link", "session_event", "trajectory"]);
    }
    await pg.close();
  }, 60_000);

  test("legacy delivery events sharing an event_id across machines are all read across page boundaries", async () => {
    const pg = new PGlite();
    await pg.exec(NEON_SCHEMA);
    for (const [m, e] of [["delivery:a", "e1"], ["delivery:b", "e1"], ["delivery:a", "e2"]])
      await pg.query(
        `INSERT INTO sessions.convergence_events VALUES ('o1', 'w1', $1, 'u1', 'claude', 's1', $2, 'message', '2026-10-20T10:00:00Z', 'x', '2026-10-20T10:00:01Z', NULL)`,
        [m, e],
      );
    const db = readOnly(pg as unknown as Queryable);
    const u = { ...unit("o1"), workspaceId: "w1", sessionId: "s1" };
    const lines: StreamLine[] = [];
    for await (const l of neonUnitSource(db, async () => {}, 1).lines(u)) lines.push(l);
    expect(lines.length).toBe(3);
    await pg.close();
  }, 60_000);
});

// ---------------------------------------------------------------- helpers

function fakeDb(units: SessionUnit[]): Queryable & { units: SessionUnit[] } {
  const self = {
    units,
    async query<T>(sql: string) {
      if (sql.includes("FROM sessions.session_rollups") && sql.includes("LIMIT 10000")) {
        return {
          rows: self.units.map((u) => ({
            workspace_id: u.workspaceId,
            source: u.source,
            first_ms: String(u.firstMs),
            last_ms: String(u.lastMs),
            machine_ids: u.machineIds,
            models: [],
            event_count: "3",
            input_tokens: "0",
            output_tokens: "0",
            reasoning_tokens: "0",
            cache_read_tokens: "0",
            cache_create_tokens: "0",
            cost_usd_micros: "0",
          })) as T[],
        };
      }
      if (sql.includes("pg_locks")) return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      if (sql.includes("FROM sessions.session_catalog") && sql.includes("LIMIT 1000")) return { rows: [] as T[] };
      throw new Error("unexpected query");
    },
  };
  return self;
}

/** The erasure job over a plain store, draining what is queued. */
async function drainErasures(state: SyncState, store: ObjectStore) {
  const job = new ErasureJob(fakeDb([]), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
  while ((await job.drain()) > 0);
  return job;
}

/** Queue a unit result's orphans as the exporter does, then erase them. */
async function eraseOrphans(store: ObjectStore, org: string, r: { manifests: { orphans: string[] }[] }) {
  const state = new SyncState(join(await tempDir(), "s.sqlite"));
  state.enqueueErasure(r.manifests.flatMap((m) => m.orphans.map((key) => ({ key, kind: "object" as const, org_id: org, reason: "unreferenced" }))));
  await drainErasures(state, store);
  state.close();
}

/** A Neon stand-in for the erasure sweep: rollup activity pages and fenced groups. */
function sweepDb(
  activity: { org_id: string; session_id: string; last_ms: string; events: string }[],
  fenced: { org_id: string; session_id: string }[] = [],
  catalog: { org_id: string; session_id: string }[] = [],
): Queryable {
  return {
    async query<T>(sql: string, params?: unknown[]) {
      if (sql.includes("pg_locks")) return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      if (sql.includes("UNION ALL") && sql.includes("FROM sessions.session_catalog WHERE org_id = $1"))
        return { rows: [...activity, ...catalog].filter((a) => a.org_id === params?.[0] && a.session_id === params?.[1]).map(() => ({})) as T[] };
      if (sql.includes("FROM sessions.session_catalog") && sql.includes("ORDER BY org_id, workspace_id, source, session_id"))
        return { rows: (sql.includes(") > (") ? [] : catalog.map((c) => ({ ...c, workspace_id: "w", source: "claude", last_ms: "1", events: "0" }))) as T[] };
      if (sql.includes("SELECT DISTINCT org_id, session_id FROM sessions.deleted_upload_sessions")) return { rows: fenced as T[] };
      if (sql.includes("FROM sessions.session_rollups") && sql.includes("GROUP BY org_id, session_id"))
        return { rows: (sql.includes("(org_id, session_id) >") ? [] : activity) as T[] };
      throw new Error("unexpected query");
    },
  };
}

function fastThrottle() {
  return new Throttle({ rowsPerSec: 1e9, bytesPerSec: 1e12, slowWindows: [], slowFactor: 1, sleep: async () => {} });
}

// ---------------------------------------------------------------- real S3 API (MinIO), when configured

const minio = process.env.S3X_TEST_MINIO_ENDPOINT;
describe.skipIf(!minio)("against a real S3 API (MinIO, S3X_TEST_MINIO_*)", () => {
  test("write-only exporter, then the versioned eraser removes every version", async () => {
    const { createS3Store } = await import("../../shared/vendor.ts");
    const { createS3Eraser } = await import("../neon-s3-erasure/eraser.ts");
    const creds = { accessKeyId: process.env.S3X_TEST_MINIO_ACCESS_KEY!, secretAccessKey: process.env.S3X_TEST_MINIO_SECRET_KEY! };
    const bucket = process.env.S3X_TEST_MINIO_BUCKET ?? "s3x-test";
    const store = createS3Store({ bucket, region: "us-east-1", endpoint: minio!, credentials: creds });
    const eraser = createS3Eraser({ bucket, region: "us-east-1", endpoint: minio!, credentials: creds });
    const org = `org-${Date.now().toString(36)}`;
    const t = target(store, { maxChunkBytes: 200 });
    const first = await exportUnit(t, unit(org), fixedSource(baseRecords()));
    for (const m of first.manifests) {
      const manifest = await parseManifest(await readText(store, m.manifestKey), m.manifestKey);
      expect(await chunksAreServerSealed({ store } as never, manifest, `96bulls/${org}`)).toBe(true);
    }
    const again = await exportUnit(t, unit(org), fixedSource(baseRecords()));
    expect(again.manifests.every((m) => !m.written && m.chunksSealed === 0)).toBe(true);
    const changed = await exportUnit(t, unit(org), fixedSource(baseRecords().filter((r) => r.record_id !== "e-1")));
    const orphans = changed.manifests.flatMap((m) => m.orphans);
    expect(orphans.length).toBeGreaterThan(0);
    for (const o of orphans) expect(await eraser.eraseObject(o)).toBeGreaterThan(0);
    for (const o of orphans) expect(await store.head(objectKey(o))).toBeUndefined();
    // Every version (the rewritten manifest's noncurrent one too) goes with the prefix.
    for (const m of first.manifests) await eraser.erasePrefix(m.manifestKey.replace("manifest.json", ""));
    expect(await allKeys(store, `96bulls/${org}`)).toEqual([]);
    for (const m of first.manifests) expect(await eraser.erasePrefix(m.manifestKey.replace("manifest.json", ""))).toBe(0);
  });
});

describe("SSE-KMS key id (sessions-prod bucket policy)", () => {
  test("every put names aws:kms and the key ARN, signed", async () => {
    const { createS3Store } = await import("../../shared/vendor.ts");
    const seen: Headers[] = [];
    const key = "arn:aws:kms:us-east-1:111122223333:key/c74376a5-f6ff-4ef0-9a26-543c940094ba";
    const store = createS3Store({
      bucket: "b",
      region: "us-east-1",
      credentials: { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "x" },
      serverSideEncryption: "aws:kms",
      sseKmsKeyId: key,
      fetch: (async (_url: unknown, init?: RequestInit) => {
        seen.push(new Headers(init?.headers));
        return new Response("", { status: 200, headers: { etag: '"e"' } });
      }) as typeof fetch,
    });
    await store.put(objectKey("96bulls/o/v1/x/raw/a.ndjson.zst"), new Uint8Array([1]), { ifNoneMatch: true });
    const h = seen.at(-1)!;
    expect(h.get("x-amz-server-side-encryption")).toBe("aws:kms");
    expect(h.get("x-amz-server-side-encryption-aws-kms-key-id")).toBe(key);
    expect(h.get("authorization")).toContain("x-amz-server-side-encryption-aws-kms-key-id");
  });
});
