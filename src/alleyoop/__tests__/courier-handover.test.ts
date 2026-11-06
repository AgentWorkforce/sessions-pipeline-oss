import { afterEach, describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  computeContentSha256,
  deleteParkedSession,
  expireParkedSession,
  objectKey,
  parseManifest,
  PARKED_TTL_MS,
  rawChunkKey,
  readAll,
  relayhistoryExportProvenance,
  type ManifestV1,
} from "../../shared/vendor.ts";
import {
  assertExportComplete,
  detectMachineSplits,
  SESSION_MACHINE_SPLIT,
} from "../../shared/vendor.ts";
import { courierWorld, EXPORTER, MARKER } from "./courier-harness.ts";

type World = Awaited<ReturnType<typeof courierWorld>>;
const worlds: World[] = [];
const world = async (...args: Parameters<typeof courierWorld>) => {
  const w = await courierWorld(...args);
  worlds.push(w);
  return w;
};
afterEach(async () => {
  await Promise.all(worlds.splice(0).map((w) => w.close()));
});

const ORIGIN = "00000000000000ab";
const line = (n: number) => JSON.stringify({ type: "user", n });
const settle = async (w: World) => {
  for (let i = 0; i < 4; i += 1) await w.run();
};
const liveCount = (m: ManifestV1) =>
  m.raw.chunks
    .filter((c) => c.epoch > 0)
    .reduce((n, c) => n + (c.records ?? 0), 0);

/** A takeover by another writer instance, built on `base` with sealed chunks. */
async function takeover(
  w: World,
  base: ManifestV1,
  owner: string,
  text: string,
  edit?: (m: ManifestV1) => void,
) {
  const m = structuredClone(base);
  const epoch = m.raw.epoch + 1;
  const { entry } = await w.seal(text, m.raw.chunk_count, epoch, m.trace_id);
  m.revision += 1;
  m.writer = {
    delivery_instance_id: owner,
    plugin_version: "96bulls-courier/0.3.0",
    capabilities: ["manifest-epochs-v2"],
  };
  m.raw.owner = owner;
  m.raw.epochs.push({
    epoch,
    opened_at: m.updated_at,
    reason: "adopted",
    first_seq: m.raw.chunk_count,
    opened_by: owner,
  });
  m.raw.epoch = epoch;
  m.raw.chunks.push(entry);
  m.raw.chunk_count += 1;
  m.raw.raw_bytes += entry.bytes;
  m.raw.stored_bytes += entry.stored_bytes;
  m.raw.next_offset = entry.bytes;
  m.raw.content_sha256 = await computeContentSha256(m.raw.chunks);
  edit?.(m);
  return m;
}

describe("handover from a cold export (H1-H5)", () => {
  test("H4: the client takes over and uploads everything it accumulated", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    for (let n = 0; n < 3; n += 1) await w.write(line(n));
    await settle(w);
    const { manifest, lines, records } = await w.liveRecords();
    expect(manifest.raw.owner).toBe(w.machineId);
    expect(manifest.writer.delivery_instance_id).toBe(w.machineId);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual([
      "initial",
      "adopted",
    ]);
    expect(manifest.raw.chunks[0]).toEqual(cold.raw.chunks[0]!);
    expect(manifest.raw.chunks[1]).toMatchObject({ epoch: 1, start_offset: 0 });
    expect(manifest.trace_id).toBe(cold.trace_id);
    expect(manifest.extensions?.["client.relayhistory_export"]).toEqual(MARKER);
    expect(lines.toSorted()).toEqual([line(0), line(1), line(2)].toSorted());
    expect(records.some((r) => r.record_id === "pre-t0")).toBe(true);

    await w.write(line(3));
    await settle(w);
    const after = await w.liveRecords();
    expect(after.lines).toHaveLength(4);
    expect(liveCount(after.manifest)).toBe(4);
    expect(after.manifest.raw.epochs).toHaveLength(2);
  });

  test("H1: racing takeovers on one version: one wins, one adopted epoch", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    const { version } = (await w.stored())!;
    const a = await takeover(w, cold, w.machineId, '{"from":"a"}\n');
    const b = await takeover(w, cold, w.machineId, '{"from":"b"}\n');
    const results = await Promise.all([
      w.post({ key: w.key, manifest: a, previousVersion: version }),
      w.post({ key: w.key, manifest: b, previousVersion: version }),
    ]);
    expect(results.map((r) => r.status).toSorted()).toEqual([202, 409]);
    const m = (await w.stored())!.manifest;
    expect(m.raw.epochs.filter((e) => e.reason === "adopted")).toHaveLength(1);
    expect(m.raw.owner).toBe(w.machineId);
  });

  test("H1: a courier that loses the takeover is fenced and writes nothing more", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    await w.write(line(0));
    await w.run(); // first contact: 409, then the rebase sees the cold export
    await w.write(line(1));
    w.hooks.beforeManifest = async () => {
      w.hooks.beforeManifest = undefined;
      // Another instance, past this route, wins the object compare-and-swap.
      const { version } = (await w.stored())!;
      const won = await takeover(w, cold, "instance-b", '{"from":"b"}\n');
      await w.store.put(
        objectKey(w.key),
        new TextEncoder().encode(JSON.stringify(won)),
        { ifMatch: version, contentType: "application/json" },
      );
    };
    await settle(w);
    expect((await w.state()).sessions["claude:session-a"].fenced).toBe(true);
    const m = (await w.stored())!.manifest;
    expect(m.raw.owner).toBe("instance-b");
    expect(m.raw.chunk_count).toBe(2);
  });

  test("H2: an exporter retry racing the takeover is a no-op in both orders", async () => {
    for (let round = 0; round < 6; round += 1) {
      const w = await world();
      const text = w.exportText("pre-t0");
      await w.coldExport(text);
      await w.write(line(0));
      const [, exported] = await Promise.all([settle(w), w.exporter(text)]);
      expect(exported.status).toBe("already_present");
      await settle(w);
      const { manifest } = await w.liveRecords();
      expect(manifest.raw.epochs.map((e) => e.reason)).toEqual([
        "initial",
        "adopted",
      ]);
      expect(await w.exporter(text)).toMatchObject({
        status: "already_present",
      });
      await expect(w.exporter(w.exportText("different"))).rejects.toMatchObject(
        { code: "precondition_failed" },
      );
    }
  });

  test("H3: refused takeovers", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    const { version } = (await w.stored())!;
    const owner = w.machineId;
    const refused: [string, (m: ManifestV1) => void][] = [
      ["owner is not the writer", (m) => void (m.raw.owner = "someone-else")],
      ["no adopted epoch", (m) => void (m.raw.epochs[1]!.reason = "rewritten")],
      [
        "wrong opened_by",
        (m) => void (m.raw.epochs[1]!.opened_by = "someone-else"),
      ],
      [
        "trace changed",
        (m) => void (m.trace_id = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19"),
      ],
      ["export marker dropped", (m) => void (m.extensions = {})],
    ];
    for (const [name, edit] of refused) {
      const res = await w.post({
        key: w.key,
        manifest: await takeover(w, cold, owner, '{"x":1}\n', edit),
        previousVersion: version,
      });
      expect([name, res.status >= 400 && res.status < 500]).toEqual([
        name,
        true,
      ]);
    }
    const exportedChunk = await takeover(w, cold, owner, '{"x":1}\n');
    exportedChunk.raw.chunks.splice(0, 1);
    exportedChunk.raw.chunks.forEach((c, i) => (c.seq = i));
    expect(
      (
        await w.post({
          key: w.key,
          manifest: exportedChunk,
          previousVersion: version,
        })
      ).status,
    ).toBe(400);

    const asExporter = await takeover(w, cold, owner, '{"x":1}\n');
    expect(
      (
        await w.post(
          { key: w.key, manifest: asExporter, previousVersion: version },
          "migration",
        )
      ).status,
    ).toBe(409);

    // In every mode a takeover needs the claim, and it must match.
    w.identity.machineClaim = undefined;
    w.openClaimlessWindow();
    expect(
      (
        await w.post({
          key: w.key,
          manifest: asExporter,
          previousVersion: version,
        })
      ).status,
    ).toBe(403);
    w.identity.machineClaim = "another-machine";
    expect(
      (
        await w.post({
          key: w.key,
          manifest: asExporter,
          previousVersion: version,
        })
      ).status,
    ).toBe(403);
    expect((await w.stored())!.manifest.raw.owner).toBe(EXPORTER);

    w.identity.machineClaim = owner;
    const won = await takeover(w, cold, owner, '{"x":1}\n');
    expect(
      (await w.post({ key: w.key, manifest: won, previousVersion: version }))
        .status,
    ).toBe(202);
    const second = await takeover(w, won, owner, '{"y":1}\n');
    const { version: now } = (await w.stored())!;
    expect(
      (await w.post({ key: w.key, manifest: second, previousVersion: now }))
        .status,
    ).toBeGreaterThanOrEqual(400);
  });

  test("a live write that moves the owner is refused as owner_conflict, not a stale version", async () => {
    const code = async (res: Response) =>
      ((await res.json()) as { error?: { code?: string } }).error?.code;
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    const { version } = (await w.stored())!;

    // Takeover with the current version, so only ownership can refuse it.
    const noEpoch = await takeover(
      w,
      cold,
      w.machineId,
      '{"x":1}\n',
      (m) => void (m.raw.owner = "someone-else"),
    );
    noEpoch.writer.delivery_instance_id = w.machineId;
    const refused = await w.post({
      key: w.key,
      manifest: noEpoch,
      previousVersion: version,
    });
    expect(refused.status).toBe(409);
    expect(await code(refused)).toBe("owner_conflict");
    const omitted = await w.post({ key: w.key, manifest: noEpoch });
    expect(omitted.status).toBe(409);
    expect(await code(omitted)).toBe("revision_conflict");

    const won = await takeover(w, cold, w.machineId, '{"x":2}\n');
    expect(
      (await w.post({ key: w.key, manifest: won, previousVersion: version }))
        .status,
    ).toBe(202);
    const held = (await w.stored())!;
    const moved = structuredClone(held.manifest);
    moved.revision += 1;
    moved.raw.owner = "someone-else";
    const live = await w.post({
      key: w.key,
      manifest: moved,
      previousVersion: held.version,
    });
    expect(live.status).toBe(409);
    expect(await code(live)).toBe("owner_conflict");
  });

  test("a takeover resumes after the watermark, so exported rows are not sent again", async () => {
    const w = await world();
    const rows = [0, 1].map((n) => w.exportedRow(n + 1, line(n))).join("");
    await w.coldExport(rows, { origin_revisions: { [ORIGIN]: 2 } });
    for (let n = 0; n < 4; n += 1) await w.write(line(n));
    await settle(w);
    const { manifest, lines } = await w.liveRecords();
    expect(lines.toSorted()).toEqual([0, 1, 2, 3].map(line).toSorted());
    expect(liveCount(manifest)).toBe(2);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual([
      "initial",
      "adopted",
    ]);
  });

  test("a feed the export never saw holds instead of taking over", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"), {
      origin_revisions: { "00000000000000ff": 9 },
    });
    await w.write(line(0));
    await w.run();
    await w.run();
    const { conditions } = await w.run();
    expect(conditions).toEqual([
      expect.objectContaining({
        type: "feed_epoch_moved",
        from: "00000000000000ff",
        to: ORIGIN,
      }),
    ]);
    expect((await w.stored())!.manifest).toEqual(cold);
  });

  test("an adopted export still reads as the grant's, through the transform's own check", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    await w.write(line(0));
    await settle(w);
    const { manifest } = await w.liveRecords();
    expect(relayhistoryExportProvenance(manifest)).toEqual({
      grantId: "mgr_export",
      sourceUserId: "user",
    });
    expect(() =>
      relayhistoryExportProvenance({
        ...manifest,
        raw: {
          ...manifest.raw,
          epochs: [
            { ...cold.raw.epochs[0]!, opened_by: "migration:other" },
            ...manifest.raw.epochs.slice(1),
          ],
        },
      }),
    ).toThrow(/stream its grant opened/);
  });

  test("with no watermark, rows sent again fold to one record each", async () => {
    const w = await world();
    const rows = [0, 1].map((n) => w.exportedRow(n + 1, line(n))).join("");
    await w.coldExport(rows);
    for (let n = 0; n < 4; n += 1) await w.write(line(n));
    await settle(w);
    const { manifest, lines } = await w.liveRecords();
    expect(liveCount(manifest)).toBe(4);
    expect(lines.toSorted()).toEqual([0, 1, 2, 3].map(line).toSorted());
  });

  test("H5: a client on another machine id never meets the export", async () => {
    const cold = await world({ machineId: "unknown-origin-7" });
    await cold.coldExport(cold.exportText("pre-t0"));
    const client = await world({
      format: "ndjson",
      machineId: "machine-a",
      root: cold.rootDir,
    });
    await client.write(line(0));
    await settle(client);
    expect((await client.stored())!.manifest.raw.owner).toBe("machine-a");
    expect((await cold.stored())!.manifest.raw.owner).toBe(EXPORTER);
  });
});

describe("a Rust-client manifest at the gated route (activation blocker)", () => {
  // Rust's `Writer.delivery_instance_id` is a destination instance, not a machine
  // id: `dest_objstore_01` beside `mach-1` in this fixture, which its helpers built.
  // A negative proof: that client cannot use this route as it stands.
  test("is refused even with a claim for its own machine", async () => {
    const fixture = JSON.parse(
      await readFile(
        new URL(
          "../../../packages/object-store/test/fixtures/rust-manifest-v1-adopted.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as { key: string; manifest: ManifestV1 };
    expect(fixture.manifest.session.machine_id).toBe("mach-1");
    expect(fixture.manifest.writer.delivery_instance_id).not.toBe("mach-1");
    const w = await world({ machineId: "mach-1", sessionId: "s-adopted" });
    w.identity.machineClaim = "mach-1";
    const res = await w.app.request(
      "/v1/pipeline/manifests",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-machine-id": "mach-1",
          "x-session-id": "s-adopted",
        },
        body: JSON.stringify({ key: fixture.key, manifest: fixture.manifest }),
      },
      w.env,
    );
    expect(res.status).toBe(403);
  });
});

describe("machine splits", () => {
  const db = (rows: unknown[]) => {
    const seen: unknown[][] = [];
    return {
      seen,
      query: async (_sql: string, params?: unknown[]) =>
        (seen.push(params ?? []), { rows }) as never,
    };
  };
  const row = {
    workspace_id: "w",
    harness: "claude",
    session_id: "s",
    machine_ids: ["machine-a", "unknown-origin-7"],
  };

  test("a split is typed, and blocks authoritative completion", async () => {
    const found = db([row]);
    expect(await detectMachineSplits(found, { orgId: "o" })).toEqual([
      {
        type: SESSION_MACHINE_SPLIT,
        orgId: "o",
        workspaceId: "w",
        harness: "claude",
        sessionId: "s",
        machineIds: ["machine-a", "unknown-origin-7"],
      },
    ]);
    expect(found.seen[0]).toEqual(["o", null]);
    await expect(
      assertExportComplete(found, { orgId: "o", workspaceId: "w" }),
    ).rejects.toMatchObject({
      code: "export_incomplete",
      issues: expect.arrayContaining([
        expect.objectContaining({
          type: SESSION_MACHINE_SPLIT,
          sessionId: "s",
        }),
      ]),
    });
    await expect(
      assertExportComplete(db([]), { orgId: "o" }),
    ).resolves.toBeUndefined();
  });
});

describe("gate and old couriers on a cold export (G1, D1)", () => {
  const held = async (w: World) => {
    const key = objectKey(
      w.key.replace(/manifest\.json$/, `parked/${w.machineId}/manifest.json`),
    );
    return parseManifest(
      new TextDecoder().decode(await readAll(await w.store.get(key))),
    );
  };

  test("G1 + D1: a closed gate parks the takeover, and opening it completes it", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    w.gate.missing = ["transform"];
    await w.write(line(0));
    await settle(w);
    await w.write(line(1));
    await settle(w);
    expect((await w.stored())!.manifest).toEqual(cold);
    expect((await held(w)).raw.chunks.length).toBeGreaterThan(1);

    w.gate.missing = [];
    await w.write(line(2));
    await settle(w);
    const { manifest, lines } = await w.liveRecords();
    expect(manifest.raw.owner).toBe(w.machineId);
    expect(lines).toHaveLength(3);
    expect(liveCount(manifest)).toBe(3);
    await expect(held(w)).rejects.toMatchObject({ code: "not_found" });
  });

  test("a parked manifest can be erased, and what the session lists stays", async () => {
    const w = await world();
    const cold = await w.coldExport(w.exportText("pre-t0"));
    w.gate.missing = ["transform"];
    await w.write(line(0));
    await settle(w);
    const parked = await held(w);
    const chunk = parked.raw.chunks.at(-1)!;
    const chunkKey = rawChunkKey(
      w.location,
      chunk.seq,
      chunk.start_offset,
      chunk.sha256,
      parked.raw.format,
    );
    expect(await w.store.head(chunkKey)).toBeDefined();

    expect(await deleteParkedSession(w.store, w.key, w.machineId)).toEqual({
      manifest: true,
      chunks: 1,
    });
    await expect(held(w)).rejects.toMatchObject({ code: "not_found" });
    expect(await w.store.head(chunkKey)).toBeUndefined();
    const kept = cold.raw.chunks[0]!;
    expect(
      await w.store.head(
        rawChunkKey(
          w.location,
          kept.seq,
          kept.start_offset,
          kept.sha256,
          "ndjson",
        ),
      ),
    ).toBeDefined();
    expect(await deleteParkedSession(w.store, w.key, w.machineId)).toEqual({
      manifest: false,
      chunks: 0,
    });
  });

  test("a parked manifest expires on a later write, not by a job", async () => {
    const w = await world();
    await w.coldExport(w.exportText("pre-t0"));
    w.gate.missing = ["transform"];
    await w.write(line(0));
    await settle(w);
    const now = Date.now();
    expect(
      await expireParkedSession(
        w.store,
        w.key,
        w.machineId,
        new Date(now + 1000),
      ),
    ).toBe(false);
    expect(
      await expireParkedSession(
        w.store,
        w.key,
        w.machineId,
        new Date(now + PARKED_TTL_MS + 60_000),
      ),
    ).toBe(true);
    await expect(held(w)).rejects.toMatchObject({ code: "not_found" });
  });

  test("a migration token never reads a client's parked manifest", async () => {
    const w = await world();
    await w.coldExport(w.exportText("pre-t0"));
    w.gate.missing = ["transform"];
    await w.write(line(0));
    await settle(w);
    const q = new URLSearchParams({
      key: w.key,
      machine: w.machineId,
      writer: w.machineId,
    });
    const asClient = await w.app.request(
      `/v1/pipeline/manifests/status?${q}`,
      {},
      w.env,
    );
    expect(asClient.status).toBe(200);
    const asExporter = await w.app.request(
      `/v1/pipeline/manifests/status?${q}`,
      { headers: { "x-test-auth": "migration" } },
      w.env,
    );
    expect(asExporter.status).toBe(403);
  });

  test("D1: a claim-bound client on an unknown release never stalls on a cold export, and an upgrade delivers everything", async () => {
    const w = await world({ format: "ndjson" });
    const cold = await w.coldExport(w.exportText("pre-t0"));
    w.hooks.legacyWriter = true;
    for (let n = 0; n < 3; n += 1) {
      await w.write(line(n));
      await w.run();
    }
    await w.run();
    const state = (await w.state()).sessions["claude:session-a"];
    expect(state.pending).toBeFalsy();
    expect(state.fenced).toBeFalsy();
    expect((await w.stored())!.manifest).toEqual(cold);
    expect((await held(w)).writer.capabilities).toBeUndefined();

    // Nothing reaches readers while parked, and a restart's check still passes.
    const { lines } = await w.liveRecords();
    expect(lines).toEqual([]);

    w.hooks.legacyWriter = false;
    await w.write(line(3));
    await settle(w);
    const after = await w.liveRecords();
    expect(after.manifest.raw.owner).toBe(w.machineId);
    expect(after.lines).toHaveLength(4);
    expect(liveCount(after.manifest)).toBe(4);
  });

  test("a truly claimless courier is refused before parking, and writes nothing", async () => {
    for (const open of [false, true]) {
      const w = await world({ format: "ndjson" });
      const cold = await w.coldExport(w.exportText("pre-t0"));
      w.hooks.legacyWriter = true;
      w.identity.machineClaim = undefined;
      if (open) w.openClaimlessWindow();
      await w.write(line(0));
      await w.run();
      await w.run();
      expect((await w.stored())!.manifest).toEqual(cold);
      await expect(held(w)).rejects.toMatchObject({ code: "not_found" });

      const refused = await w.post({
        key: w.key,
        manifest: { ...cold, revision: 2 },
      });
      expect(refused.status).toBe(403);
      expect(
        ((await refused.json()) as { error: { code: string } }).error.code,
      ).toBe("machine_claim_refused");
    }
  });
});
