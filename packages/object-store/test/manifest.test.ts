import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  EXPORT_EXTENSION,
  MANIFEST_EPOCHS_CAPABILITY,
  adoptionWriteIssues,
  appendBackfillEpoch,
  backfillDigest,
  backfillStatus,
  backfillWriteIssues,
  chunkKeys,
  commitBackfillEpoch,
  computeContentSha256,
  exportWatermarks,
  liveEpoch,
  liveNextOffset,
  manifestKey,
  needsTransform,
  normalizedIsCurrent,
  parseManifest,
  pointerFields,
  quarantined,
  scrubbedForPointer,
  sha256HexForTest as sha256Hex,
  validateManifest,
  writerUnderstandsBackfill,
  type BackfillChunk,
  type BackfillEpoch,
  type BackfillIO,
  type ManifestV1,
} from "./helpers.js";

const loc = {
  workspaceId: "ws_7f3a2c",
  machineId: "mach-linuyc-desktop",
  harness: "claude",
  startDate: "2026-10-25",
  sessionId: "4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718",
};

async function chunk(seq: number, epoch: number, start: number, bytes: number) {
  const sha = await sha256Hex(`chunk-${seq}`);
  return {
    seq,
    epoch,
    start_offset: start,
    bytes,
    stored_bytes: Math.ceil(bytes / 5),
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(`stored-${seq}`),
    compression: "zstd" as const,
    records: 10,
  };
}

/** The spec's example, with real digests so the hash chain verifies. */
async function exampleManifest(): Promise<ManifestV1> {
  const chunks = [
    await chunk(0, 0, 0, 4194304),
    await chunk(1, 0, 4194304, 4194304),
    await chunk(2, 0, 8388608, 1048576),
  ];
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
    revision: 3,
    created_at: "2026-10-25T18:02:11.204Z",
    updated_at: "2026-10-25T18:41:07.990Z",
    session: {
      workspace_id: loc.workspaceId,
      machine_id: loc.machineId,
      harness: loc.harness,
      session_id: loc.sessionId,
      started_at: "2026-10-25T18:01:58.000Z",
    },
    writer: {
      delivery_instance_id: "dest_objstore_01",
      plugin_version: "0.1.0",
    },
    raw: {
      owner: "dest_objstore_01",
      state: "open",
      updated_at: "2026-10-25T18:41:07.990Z",
      format: "jsonl",
      compression: "zstd",
      chunk_count: 3,
      raw_bytes: chunks.reduce((a, c) => a + c.bytes, 0),
      stored_bytes: chunks.reduce((a, c) => a + c.stored_bytes, 0),
      content_sha256: await computeContentSha256(chunks),
      next_offset: 9437184,
      epoch: 0,
      epochs: [
        {
          epoch: 0,
          opened_at: "2026-10-25T18:02:11.204Z",
          reason: "initial",
          first_seq: 0,
          opened_by: "dest_objstore_01",
        },
      ],
      chunks,
    },
  };
}

describe("manifest v1", () => {
  it("accepts a valid manifest and derives pointer fields and chunk keys", async () => {
    const m = await exampleManifest();
    const parsed = await parseManifest(JSON.stringify(m), manifestKey(loc));
    expect(pointerFields(parsed)).toMatchObject({
      manifestRevision: 3,
      rawBytes: 9437184,
      chunkCount: 3,
      rawEpoch: 0,
      contentSha256: m.raw.content_sha256,
    });
    const keys = chunkKeys(parsed).map((c) => c.key);
    expect(keys[1]).toBe(
      `v1/ws=ws_7f3a2c/m=mach-linuyc-desktop/h=claude/d=2026/10/25/s=4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718/raw/0000000001-4194304-${m.raw.chunks[1]!.sha256_8}.jsonl.zst`,
    );
  });

  it("an empty chunk list hashes to 64 zeros", async () => {
    expect(await computeContentSha256([])).toBe("0".repeat(64));
  });

  it("keeps unknown fields (forward compatibility)", async () => {
    const m = { ...(await exampleManifest()), future_field: 1 };
    await expect(parseManifest(m)).resolves.toMatchObject({ future_field: 1 });
  });

  const corruptions: [string, (m: ManifestV1) => void, RegExp][] = [
    ["chunk_count", (m) => (m.raw.chunk_count = 2), /raw\.chunk_count/],
    ["raw_bytes", (m) => (m.raw.raw_bytes += 1), /raw\.raw_bytes/],
    [
      "gap in offsets",
      (m) => (m.raw.chunks[2]!.start_offset += 1),
      /start_offset/,
    ],
    ["seq not dense", (m) => (m.raw.chunks[1]!.seq = 5), /seq/],
    ["sha256_8", (m) => (m.raw.chunks[0]!.sha256_8 = "00000000"), /sha256_8/],
    [
      "hash chain",
      (m) => (m.raw.content_sha256 = "f".repeat(64)),
      /content_sha256/,
    ],
    ["next_offset", (m) => (m.raw.next_offset = 1), /next_offset/],
    ["epoch decreases", (m) => (m.raw.chunks[0]!.epoch = 1), /epoch/],
    ["epoch counter without a record", (m) => (m.raw.epoch = 1), /raw\.epoch/],
    [
      "second initial epoch",
      (m) => m.raw.epochs.push({ ...m.raw.epochs[0]!, epoch: 1 }),
      /reason|raw\.epoch/,
    ],
  ];
  for (const [name, corrupt, pattern] of corruptions) {
    it(`rejects: ${name}`, async () => {
      const m = await exampleManifest();
      corrupt(m);
      await expect(parseManifest(m)).rejects.toThrow(pattern);
    });
  }

  it("accepts a new epoch restarting at offset 0", async () => {
    const m = await exampleManifest();
    const c3 = await chunk(3, 1, 0, 100);
    m.raw.chunks.push(c3);
    m.raw.chunk_count = 4;
    m.raw.raw_bytes += 100;
    m.raw.stored_bytes += c3.stored_bytes;
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-25T18:50:00.000Z",
      reason: "rewritten",
      first_seq: 3,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 100;
    m.raw.content_sha256 = await computeContentSha256(m.raw.chunks);
    const parsed = await parseManifest(m);
    expect(chunkKeys(parsed, { currentEpochOnly: true })).toHaveLength(1);
  });

  it("accepts a migration backfill epoch after live delivery", async () => {
    const m = await exampleManifest();
    const c3 = await chunk(3, 1, 0, 100);
    m.raw.chunks.push(c3);
    m.raw.chunk_count = 4;
    m.raw.raw_bytes += 100;
    m.raw.stored_bytes += c3.stored_bytes;
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-25T18:50:00.000Z",
      reason: "backfill",
      first_seq: 3,
      opened_by: "migration:mgr_export",
    });
    m.raw.next_offset = 100;
    m.raw.content_sha256 = await computeContentSha256(m.raw.chunks);
    m.raw.format = "ndjson";
    await expect(parseManifest(m)).resolves.toMatchObject({
      raw: {
        epoch: 1,
        epochs: [{ reason: "initial" }, { reason: "backfill" }],
      },
    });
  });

  it("opens an epoch with no chunks yet (source emptied)", async () => {
    const m = await exampleManifest();
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-25T18:50:00.000Z",
      reason: "truncated",
      first_seq: 3,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 0;
    await expect(parseManifest(m)).resolves.toBeTruthy();
  });

  it("derives effective state and normalized freshness from both writers' sections", async () => {
    const m = await exampleManifest();
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      state: "active",
      lifecycle: "active",
      normalizedVersion: null,
      normalizedStale: false,
    });
    m.server = {
      updated_at: "2026-10-27T00:00:00.000Z",
      normalized: {
        version: 2,
        source_content_sha256: "0".repeat(64),
        source_epoch: 0,
        built_at: "2026-10-26T00:00:00.000Z",
      },
      lifecycle: "abandoned",
      lifecycle_at: "2026-11-08T00:00:00.000Z",
    };
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      state: "abandoned",
      normalizedVersion: 2,
      normalizedStale: true,
    });
    m.server.normalized!.source_content_sha256 = m.raw.content_sha256;
    m.raw.state = "closed";
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      state: "closed",
      lifecycle: "abandoned",
      normalizedStale: false,
    });
  });

  it("treats normalized output from the previous epoch as stale", async () => {
    const m = await exampleManifest();
    m.server = {
      updated_at: "2026-10-26T00:00:00.000Z",
      normalized: {
        version: 1,
        source_content_sha256: m.raw.content_sha256,
        source_epoch: 0,
        built_at: "2026-10-26T00:00:00.000Z",
      },
      lifecycle: "active",
      lifecycle_at: "2026-10-26T00:00:00.000Z",
    };
    expect(pointerFields(await parseManifest(m)).normalizedStale).toBe(false);
    // Open an empty epoch: same digest, newer epoch.
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-26T01:00:00.000Z",
      reason: "truncated",
      first_seq: 3,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 0;
    const parsed = await parseManifest(m);
    expect(normalizedIsCurrent(parsed)).toBe(false);
    expect(pointerFields(parsed).normalizedStale).toBe(true);
  });

  it("checks first_seq of an epoch that has no chunks yet", async () => {
    const m = await exampleManifest();
    m.raw.epoch = 1;
    m.raw.epochs.push({
      epoch: 1,
      opened_at: "2026-10-26T01:00:00.000Z",
      reason: "truncated",
      first_seq: 99,
      opened_by: "dest_objstore_01",
    });
    m.raw.next_offset = 0;
    await expect(parseManifest(m)).rejects.toThrow(
      /raw\.epochs\[1\]\.first_seq/,
    );
    m.raw.epochs[1]!.first_seq = 3;
    await expect(parseManifest(m)).resolves.toBeTruthy();
  });

  it("validates session.repo and exposes it on the pointer row", async () => {
    const m = await exampleManifest();
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      gitRemoteUrl: null,
      gitRootCommit: null,
    });
    for (const good of [
      "intranet/team/repo",
      "[::1]/x/y",
      "gitlab.example.com/Group/Sub/Repo",
    ]) {
      await expect(
        parseManifest({
          ...m,
          session: { ...m.session, repo: { remote_url: good } },
        }),
      ).resolves.toBeTruthy();
    }
    m.session.repo = {
      remote_url: "github.com/agentworkforce/relayhistory",
      root_commit: "a".repeat(40),
    };
    expect(pointerFields(await parseManifest(m))).toMatchObject({
      gitRemoteUrl: "github.com/agentworkforce/relayhistory",
      gitRootCommit: "a".repeat(40),
    });
    for (const bad of [
      { remote_url: "https://github.com/x/y" },
      { remote_url: "git@github.com:x/y" },
      { remote_url: "GitHub.com/x/y" },
      { remote_url: "github.com:22/x/y" },
      { remote_url: "github.com/" },
      { remote_url: "github.com/x/y?token=abc" },
      { remote_url: "github.com/x/y#frag" },
      { remote_url: "github.com/x:y" },
      { remote_url: "github.com/x/y/" },
      { remote_url: "github.com" },
      { remote_url: "./src" },
      { remote_url: "../src" },
      { root_commit: "ABC" },
      { root_commit: "a".repeat(41) },
    ]) {
      await expect(
        parseManifest({ ...m, session: { ...m.session, repo: bad } }),
      ).rejects.toThrow(/session\.repo/);
    }
  });

  it("accepts a Rust-produced manifest with its committed digest", async () => {
    // Built by relayhistory's Rust manifest helpers; the expected digest is
    // a literal so a change to the hash chain's byte layout cannot pass by
    // being recomputed with the same code.
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const m = await parseManifest(fixture.manifest, fixture.key);
    expect(m.raw.content_sha256).toBe(
      "2f99d12c09716e5f9198013ced2e0ba6392db02031520137ff5b6daf3c53fbb2",
    );
    expect(await computeContentSha256(m.raw.chunks)).toBe(
      "2f99d12c09716e5f9198013ced2e0ba6392db02031520137ff5b6daf3c53fbb2",
    );
    expect(pointerFields(m)).toMatchObject({
      chunkCount: 3,
      rawEpoch: 1,
      normalizedStale: false,
    });
  });

  it("accepts a Rust-produced scrubbed manifest", async () => {
    // relayhistory#551: record_scrub before apply_chunks.
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1-scrubbed.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const m = await parseManifest(fixture.manifest, fixture.key);
    expect(m.raw.content_sha256).toBe(
      "a97482a47131f0a1a196ee3fbfb1eeee7bef52f48d7c9c52a91772049da67e86",
    );
    expect(m.raw.scrub).toEqual({ ruleset: "relayhistory-scrub", version: 1 });
    expect(m.raw.chunks.map((c) => c.stored_raw_bytes)).toEqual([10, 7]);
    expect(pointerFields(m).scrubbed).toBe(true);
    expect(quarantined(m)).toBe(false);
    expect(needsTransform(m)).toBe(true);
  });

  it("quarantines unscrubbed manifests and never transforms them", async () => {
    const m = await parseManifest(await exampleManifest());
    expect(quarantined(m)).toBe(true);
    expect(needsTransform(m)).toBe(false);
    expect(pointerFields(m).scrubbed).toBe(false);
  });

  it("keeps a pointer row quarantined once it saw unscrubbed chunks", async () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1-scrubbed.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const scrubbed = await parseManifest(fixture.manifest, fixture.key);
    const unscrubbed = await parseManifest(await exampleManifest());
    const prev = (
      scrubbed: boolean,
      chunkCount: number,
      scrubQuarantined = false,
    ) => ({ scrubbed, chunkCount, scrubQuarantined });
    const on = { scrubbed: true, scrubQuarantined: false };
    const stuck = { scrubbed: false, scrubQuarantined: true };
    expect(scrubbedForPointer(null, scrubbed)).toEqual(on);
    // A first revision that is unscrubbed with chunks is flagged at once.
    expect(scrubbedForPointer(null, unscrubbed)).toEqual(stuck);
    // Marker written with or before the first chunk: allowed.
    expect(scrubbedForPointer(prev(false, 0), scrubbed)).toEqual(on);
    // Chunks were seen without the marker: stays quarantined.
    expect(scrubbedForPointer(prev(false, 3), scrubbed)).toEqual(stuck);
    // A scrubbed row whose manifest loses the marker is quarantined again.
    expect(scrubbedForPointer(prev(true, 3), unscrubbed)).toEqual(stuck);
    // The flag survives a revision that reset the chunk count.
    expect(scrubbedForPointer(prev(false, 0, true), scrubbed)).toEqual(stuck);
    // A row that already had chunks when the column was added (false, flag unset).
    expect(scrubbedForPointer(prev(false, 4), scrubbed)).toEqual(stuck);
  });

  it("requires stored fields on every chunk once raw.scrub is set", async () => {
    const m = await exampleManifest();
    const scrub = { ruleset: "relayhistory-scrub", version: 1 };
    const stored = await Promise.all(
      m.raw.chunks.map(async (c) => ({
        ...c,
        stored_raw_bytes: c.bytes,
        raw_sha256: await sha256Hex(`scrubbed-${c.seq}`),
      })),
    );
    const ok = await parseManifest({
      ...m,
      raw: { ...m.raw, scrub, chunks: stored },
    });
    expect(pointerFields(ok).scrubbed).toBe(true);

    const missing = stored.map((c, i) =>
      i === 1 ? { ...c, raw_sha256: undefined } : c,
    );
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: missing } }),
    ).rejects.toThrow(/raw\.chunks\[1\]\.raw_sha256: is required/);
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: m.raw.chunks } }),
    ).rejects.toThrow(/raw\.chunks\[0\]\.stored_raw_bytes: is required/);
    for (const bad of [
      { ruleset: "", version: 1 },
      { ruleset: "relayhistory-scrub", version: 0 },
    ])
      await expect(
        parseManifest({ ...m, raw: { ...m.raw, scrub: bad, chunks: stored } }),
      ).rejects.toThrow(/raw\.scrub/);
    const zero = stored.map((c) => ({ ...c, stored_raw_bytes: 0 }));
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: zero } }),
    ).rejects.toThrow(/stored_raw_bytes/);
    const badHex = stored.map((c) => ({ ...c, raw_sha256: "ABC" }));
    await expect(
      parseManifest({ ...m, raw: { ...m.raw, scrub, chunks: badHex } }),
    ).rejects.toThrow(/raw_sha256/);
  });

  it("rejects a manifest whose session does not match its key", async () => {
    const m = await exampleManifest();
    await expect(
      parseManifest(m, manifestKey({ ...loc, harness: "codex" })),
    ).rejects.toThrow(/session\.harness/);
  });

  it("refuses a newer schema_v instead of guessing", async () => {
    const m = { ...(await exampleManifest()), schema_v: 2 };
    await expect(parseManifest(m)).rejects.toMatchObject({
      code: "unsupported",
    });
  });

  it("rejects schema violations", async () => {
    const m = await exampleManifest();
    await expect(
      parseManifest({ ...m, trace_id: "not-a-uuid" }),
    ).rejects.toThrow(/trace_id/);
    await expect(parseManifest("{not json")).rejects.toMatchObject({
      code: "invalid_argument",
    });
  });
});

describe("backfill epochs", () => {
  const EXPORTER = "migration:mgr_export";
  const AT = "2026-10-30T00:00:00.000Z";
  const marker = { version: 1, source: "neon.delivery_records" };

  /** `next` live chunks, as the raw owner appends them. */
  async function ownerAppends(
    m: ManifestV1,
    bytes: number,
    label: string,
  ): Promise<ManifestV1> {
    const n = structuredClone(m);
    const raw = n.raw;
    if (raw.epochs.at(-1)!.reason === "backfill") {
      const epoch = raw.epoch + 1;
      raw.epochs.push({
        epoch,
        opened_at: AT,
        reason: "resumed",
        first_seq: raw.chunk_count,
        opened_by: raw.owner,
        resumes_epoch: liveEpoch(raw),
      });
      raw.epoch = epoch;
    }
    const sha = await sha256Hex(label);
    const entry = {
      ...(await chunk(raw.chunk_count, raw.epoch, liveNextOffset(raw), bytes)),
      sha256: sha,
      sha256_8: sha.slice(0, 8),
    };
    raw.chunks.push(entry);
    raw.chunk_count += 1;
    raw.raw_bytes += entry.bytes;
    raw.stored_bytes += entry.stored_bytes;
    raw.next_offset = entry.start_offset + entry.bytes;
    raw.content_sha256 = await computeContentSha256(raw.chunks);
    n.revision += 1;
    return n;
  }

  async function ndjsonManifest(): Promise<ManifestV1> {
    const m = await exampleManifest();
    m.raw.format = "ndjson";
    return m;
  }

  async function exportChunks(): Promise<BackfillChunk[]> {
    const out: BackfillChunk[] = [];
    for (const [i, [start, bytes]] of [
      [0, 700],
      [700, 300],
    ].entries()) {
      const {
        seq: _seq,
        epoch: _epoch,
        ...c
      } = await chunk(100 + i, 0, start!, bytes!);
      out.push(c);
    }
    return out;
  }

  const backfill = async (openedBy = EXPORTER): Promise<BackfillEpoch> => ({
    openedBy,
    openedAt: AT,
    chunks: await exportChunks(),
    marker,
  });

  /** A manifest cell behind compare-and-swap, as the object store is. */
  function cell(initial: ManifestV1) {
    const state = { manifest: initial, version: 1, writes: 0 };
    const io: BackfillIO<number> = {
      read: async () => ({
        manifest: state.manifest,
        version: state.version,
      }),
      write: async (next, version) => {
        if (version !== state.version) return false;
        await parseManifest(next);
        state.manifest = next;
        state.version += 1;
        state.writes += 1;
        return true;
      },
    };
    return { state, io };
  }

  const noWait = { backoff: async () => {} };

  it("keeps the owner, the raw activity time and the live position", async () => {
    const m = await ndjsonManifest();
    const { state, io } = cell(m);
    const done = await commitBackfillEpoch(io, await backfill(), noWait);
    expect(done).toMatchObject({ status: "committed", attempts: 1 });
    const n = await parseManifest(state.manifest);
    expect(n.raw.owner).toBe(m.raw.owner);
    expect(n.raw.updated_at).toBe(m.raw.updated_at);
    expect(n.writer).toEqual(m.writer);
    expect(n.revision).toBe(m.revision + 1);
    expect(n.raw.epochs[1]).toMatchObject({
      reason: "backfill",
      opened_by: EXPORTER,
      first_seq: 3,
    });
    expect(n.extensions?.[EXPORT_EXTENSION]).toEqual(marker);
    expect(n.raw.next_offset).toBe(1000);
    expect(liveEpoch(n.raw)).toBe(0);
    expect(liveNextOffset(n.raw)).toBe(9437184);
  });

  it("gives the same result however live and backfill commits interleave", async () => {
    const payload = await backfill();
    const liveBytes = [10, 20, 30];
    const digests = new Set<string>();
    for (let backfillAfter = 0; backfillAfter <= 3; backfillAfter += 1) {
      let m = await ndjsonManifest();
      for (let i = 0; i < 3; i += 1) {
        if (i === backfillAfter) m = await appendBackfillEpoch(m, payload);
        m = await ownerAppends(m, liveBytes[i]!, `live-${i}`);
        await parseManifest(m);
      }
      if (backfillAfter === 3) m = await appendBackfillEpoch(m, payload);
      await parseManifest(m);

      const liveChunks = m.raw.chunks.filter(
        (c) => m.raw.epochs[c.epoch]!.reason !== "backfill",
      );
      expect(liveChunks.map((c) => c.bytes).slice(3)).toEqual(liveBytes);
      expect(liveNextOffset(m.raw)).toBe(9437184 + 60);
      expect(
        await backfillStatus(m, await backfillDigest(payload.chunks)),
      ).toBe("present");
      // The live chunks contiguous across `resumed`, whatever the interleaving.
      digests.add(
        await computeContentSha256(liveChunks.map((c) => ({ ...c, epoch: 0 }))),
      );
    }
    expect(digests.size).toBe(1);
  });

  it("current-epoch readers keep the live stream, not the backfill", async () => {
    let m = await ndjsonManifest();
    m = await appendBackfillEpoch(m, await backfill());
    expect(
      chunkKeys(m, { currentEpochOnly: true }).map((c) => c.entry.epoch),
    ).toEqual([0, 0, 0]);
    m = await ownerAppends(m, 10, "after");
    expect(
      chunkKeys(m, { currentEpochOnly: true }).map((c) => c.entry.seq),
    ).toEqual([0, 1, 2, 5]);
    expect(chunkKeys(m)).toHaveLength(6);
  });

  it("makes normalized output stale, because the raw digest and epoch move", async () => {
    const m = await ndjsonManifest();
    m.server = {
      updated_at: AT,
      normalized: {
        version: 1,
        source_content_sha256: m.raw.content_sha256,
        source_epoch: 0,
        built_at: AT,
      },
      lifecycle: "active",
      lifecycle_at: AT,
    };
    expect(normalizedIsCurrent(m)).toBe(true);
    expect(
      normalizedIsCurrent(await appendBackfillEpoch(m, await backfill())),
    ).toBe(false);
  });

  it("retries a lost race on the fresh copy, and seals under the new seq", async () => {
    const { state, io } = cell(await ndjsonManifest());
    const sealedSeqs: number[] = [];
    let raced = 0;
    const racing: BackfillIO<number> = {
      ...io,
      seal: async (planned) => {
        sealedSeqs.push(planned.raw.chunks.at(-1)!.seq);
        // The live client lands between this attempt's read and its write.
        if (raced < 2) {
          raced += 1;
          state.manifest = await ownerAppends(
            state.manifest,
            10,
            `race-${raced}`,
          );
          state.version += 1;
        }
      },
    };
    const done = await commitBackfillEpoch(racing, await backfill(), noWait);
    expect(done).toMatchObject({ status: "committed", attempts: 3 });
    expect(sealedSeqs).toEqual([4, 5, 6]);
    const n = await parseManifest(state.manifest);
    expect(n.raw.chunk_count).toBe(7);
    expect(n.raw.chunks.filter((c) => c.bytes === 10)).toHaveLength(2);
    expect(n.raw.epochs[1]!.first_seq).toBe(5);
  });

  it("gives up after the attempts run out, leaving the manifest alone", async () => {
    const { state, io } = cell(await ndjsonManifest());
    const before = state.manifest;
    const backoffs: number[] = [];
    await expect(
      commitBackfillEpoch(
        { ...io, write: async () => false },
        await backfill(),
        { backoff: async (n) => void backoffs.push(n) },
      ),
    ).rejects.toMatchObject({ code: "precondition_failed" });
    expect(backoffs).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(state.manifest).toBe(before);
  });

  it("a re-export is a no-op by content, wherever the epoch landed", async () => {
    const { state, io } = cell(await ndjsonManifest());
    await commitBackfillEpoch(io, await backfill(), noWait);
    state.manifest = await ownerAppends(state.manifest, 10, "later");
    const revision = state.manifest.revision;
    const writes = state.writes;
    let sealed = 0;
    const again = await commitBackfillEpoch(
      { ...io, seal: async () => void (sealed += 1) },
      await backfill("migration:mgr_rerun"),
      noWait,
    );
    expect(again.status).toBe("already_present");
    expect(state.writes).toBe(writes);
    expect(state.manifest.revision).toBe(revision);
    expect(sealed).toBe(0);
  });

  it("a different re-export stops for a human", async () => {
    const { state, io } = cell(await ndjsonManifest());
    await commitBackfillEpoch(io, await backfill(), noWait);
    const before = state.manifest;
    const changed = await backfill();
    changed.chunks = changed.chunks.slice(0, 1);
    await expect(
      commitBackfillEpoch(io, changed, noWait),
    ).rejects.toMatchObject({ code: "precondition_failed" });
    expect(state.manifest).toBe(before);
  });

  it("survives a client truncate or rewrite, in either order", async () => {
    const truncate = async (
      m: ManifestV1,
      reason: "truncated" | "rewritten",
    ) => {
      const n = structuredClone(m);
      n.raw.epochs.push({
        epoch: n.raw.epoch + 1,
        opened_at: AT,
        reason,
        first_seq: n.raw.chunk_count,
        opened_by: n.raw.owner,
      });
      n.raw.epoch += 1;
      n.raw.next_offset = 0;
      n.revision += 1;
      return n;
    };
    const payload = await backfill();
    const digest = await backfillDigest(payload.chunks);
    for (const reason of ["truncated", "rewritten"] as const) {
      const after = await truncate(
        await appendBackfillEpoch(await ndjsonManifest(), payload),
        reason,
      );
      await parseManifest(after);
      expect(await backfillStatus(after, digest)).toBe("present");
      expect(liveEpoch(after.raw)).toBe(2);
      expect(chunkKeys(after)).toHaveLength(5);

      const before = await appendBackfillEpoch(
        await truncate(await ndjsonManifest(), reason),
        payload,
      );
      await parseManifest(before);
      expect(await backfillStatus(before, digest)).toBe("present");
    }
  });

  it("refuses invalid ownership edits", async () => {
    const m = await ndjsonManifest();
    const good = await appendBackfillEpoch(m, await backfill());
    expect(backfillWriteIssues(m, good, EXPORTER)).toEqual([]);

    const edit = (f: (n: ManifestV1) => void) => {
      const n = structuredClone(good);
      f(n);
      return backfillWriteIssues(m, n, EXPORTER);
    };
    const refusals: [string, (n: ManifestV1) => void][] = [
      ["raw.owner", (n) => void (n.raw.owner = EXPORTER)],
      ["client-owned", (n) => void (n.raw.state = "closed")],
      ["client-owned", (n) => void (n.raw.updated_at = AT)],
      ["writer", (n) => void (n.writer.delivery_instance_id = EXPORTER)],
      ["session", (n) => void (n.session.ended_at = AT)],
      ["existing chunk", (n) => void (n.raw.chunks[0]!.records = 99)],
      ["existing epoch", (n) => void (n.raw.epochs[0]!.opened_by = EXPORTER)],
      ["own epoch", (n) => void (n.raw.epochs[1]!.opened_by = "migration:x")],
      ["extension", (n) => void (n.extensions = { other: 1 })],
    ];
    for (const [needle, f] of refusals)
      expect(edit(f).join("; ")).toContain(needle);

    expect(backfillWriteIssues(good, good, EXPORTER).join("; ")).toContain(
      "already has a backfill",
    );

    // The live side: a backfill epoch is not its to open, grow or drop.
    expect(backfillWriteIssues(m, good, "live").join("; ")).toContain(
      "only the exporter",
    );
    const dropped = structuredClone(m);
    expect(backfillWriteIssues(good, dropped, "live").join("; ")).toContain(
      "existing epoch changed",
    );
    const grown = structuredClone(good);
    const tail = grown.raw.chunks.at(-1)!;
    grown.raw.chunks.push({ ...tail, seq: tail.seq + 1 });
    expect(backfillWriteIssues(good, grown, "live").join("; ")).toContain(
      "sealed",
    );
    expect(
      backfillWriteIssues(good, await ownerAppends(good, 10, "next"), "live"),
    ).toEqual([]);

    await expect(
      commitBackfillEpoch(
        cell(m).io,
        await backfill("dest_not_a_grant"),
        noWait,
      ),
    ).rejects.toMatchObject({ code: "invalid_argument" });
  });

  it("the validator refuses malformed backfill and resumed epochs", async () => {
    const good = await appendBackfillEpoch(
      await ndjsonManifest(),
      await backfill(),
    );
    const cases: [string, (n: ManifestV1) => void][] = [
      [
        "raw.epochs[1].opened_by",
        (n) => void (n.raw.epochs[1]!.opened_by = "x"),
      ],
      ["raw.format", (n) => void (n.raw.format = "jsonl")],
      [
        "raw.epochs[0].resumes_epoch",
        (n) => void (n.raw.epochs[0]!.resumes_epoch = 0),
      ],
      [
        "raw.epochs[1].resumes_epoch",
        (n) => void (n.raw.epochs[1]!.resumes_epoch = 0),
      ],
    ];
    for (const [path, f] of cases) {
      const n = structuredClone(good);
      f(n);
      expect((await validateManifest(n)).map((i) => i.path)).toContain(path);
    }

    const twice = await appendBackfillEpoch(good, await backfill());
    expect((await validateManifest(twice)).map((i) => i.path)).toContain(
      "raw.epochs",
    );

    const resumed = await ownerAppends(good, 10, "next");
    const wrongTarget = structuredClone(resumed);
    wrongTarget.raw.epochs[2]!.resumes_epoch = 1;
    expect((await validateManifest(wrongTarget)).map((i) => i.path)).toContain(
      "raw.epochs[2].resumes_epoch",
    );
    const restart = structuredClone(resumed);
    restart.raw.chunks[5]!.start_offset = 0;
    restart.raw.next_offset = 10;
    restart.raw.content_sha256 = await computeContentSha256(restart.raw.chunks);
    expect((await validateManifest(restart)).map((i) => i.path)).toContain(
      "raw.chunks[5].start_offset",
    );
    const stray = structuredClone(resumed);
    stray.raw.epochs[1]!.reason = "rewritten";
    expect((await validateManifest(stray)).map((i) => i.path)).toContain(
      "raw.epochs[2].reason",
    );
  });

  it("accepts a Rust-produced adopted manifest", async () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1-adopted.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const m = await parseManifest(fixture.manifest, fixture.key);
    expect(m.raw.content_sha256).toBe(
      "0d24239d1b4d38dad7497e7e822d63af4c3869e99082acef2b8dbe1d54961889",
    );
    expect(m.raw.epochs.map((e) => e.reason)).toEqual(["initial", "adopted"]);
    expect(m.raw.owner).toBe("dest_objstore_01");
    expect(m.writer.capabilities).toContain(MANIFEST_EPOCHS_CAPABILITY);
    expect(liveNextOffset(m.raw)).toBe(50);
    expect(chunkKeys(m, { currentEpochOnly: true })).toHaveLength(1);
  });

  it("refuses an adoption of anything but a cold export, and any edit of it", async () => {
    const m = await ndjsonManifest();
    const adopted = structuredClone(m);
    adopted.raw.epochs.push({
      epoch: 1,
      opened_at: AT,
      reason: "adopted",
      first_seq: 3,
      opened_by: "client",
    });
    adopted.raw.epoch = 1;
    adopted.raw.next_offset = 0;
    expect((await validateManifest(adopted)).map((i) => i.path)).toContain(
      "raw.epochs[1].reason",
    );

    const cold = structuredClone(m);
    cold.raw.owner = EXPORTER;
    cold.raw.epochs[0]!.opened_by = EXPORTER;
    const taken = structuredClone(adopted);
    taken.raw.epochs[0]!.opened_by = EXPORTER;
    taken.raw.owner = "client";
    taken.writer.delivery_instance_id = "client";
    expect(await validateManifest(taken)).toEqual([]);
    expect(adoptionWriteIssues(cold, taken)).toEqual([]);
    expect(adoptionWriteIssues(m, taken).join("; ")).toContain("cold export");
    const stolen = structuredClone(taken);
    stolen.raw.owner = "someone-else";
    expect(adoptionWriteIssues(cold, stolen).join("; ")).toContain(
      "writing client",
    );
    const held = structuredClone(taken);
    held.raw.owner = EXPORTER;
    expect((await validateManifest(held)).map((i) => i.path)).toContain(
      "raw.owner",
    );
  });

  it("reads and validates the export marker's watermarks", async () => {
    const m = await ndjsonManifest();
    expect(exportWatermarks(m)).toEqual({});
    const path = `extensions.${EXPORT_EXTENSION}.origin_revisions`;
    const marked = (origin_revisions: unknown) => ({
      ...m,
      extensions: { [EXPORT_EXTENSION]: { ...marker, origin_revisions } },
    });
    expect(await validateManifest(marked({}))).toEqual([]);
    const good = marked({ "00000000000000ab": 41 });
    expect(await validateManifest(good)).toEqual([]);
    expect(exportWatermarks(good)).toEqual({ "00000000000000ab": 41 });
    for (const bad of [
      [],
      "x",
      { "machine:a": 1 },
      { o: -1 },
      { o: "41" },
      { o: 1.5 },
      { o: Number.MAX_SAFE_INTEGER + 1 },
    ])
      expect(
        (await validateManifest(marked(bad))).map((i) => i.path),
      ).toContain(path);
  });

  it("accepts a Rust-produced backfilled and resumed manifest", async () => {
    // Built by relayhistory's Rust helpers, so both validators agree on the
    // epoch rules, and the digest is a literal for the same reason as above.
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/rust-manifest-v1-backfill.json", import.meta.url),
        "utf8",
      ),
    ) as { key: string; manifest: unknown };
    const m = await parseManifest(fixture.manifest, fixture.key);
    expect(m.raw.content_sha256).toBe(
      "796a14cf979d68529e1cd3c9b2c02aa27a73d1054df5c5f035b755a86edc1ec4",
    );
    expect(m.raw.epochs.map((e) => e.reason)).toEqual([
      "initial",
      "backfill",
      "resumed",
    ]);
    expect(liveNextOffset(m.raw)).toBe(160);
  });
});
