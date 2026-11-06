import { afterEach, describe, expect, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import {
  appendBackfillEpoch,
  isStageMessage,
  MANIFEST_EPOCHS_CAPABILITY,
  MANIFEST_READERS,
  objectKey,
  type ManifestV1,
} from "../../shared/vendor.ts";
import { createHash } from "node:crypto";
import { sourceResumeOffset } from "../courier.ts";
import { claimlessWindowOpen } from "../manifests.ts";
import { feedRanges, feedRecordId } from "../feed-records.ts";
import { courierWorld } from "./courier-harness.ts";

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
const line = (n: number) =>
  JSON.stringify({
    type: "user",
    n,
    timestamp: new Date(1_761_730_000_000 + n).toISOString(),
  });

/** Live chunks' record count: folding hides a double upload, this does not. */
const uploadedRecords = (m: ManifestV1) => {
  const backfill = m.raw.epochs.find((e) => e.reason === "backfill")?.epoch;
  return m.raw.chunks
    .filter((c) => c.epoch !== backfill)
    .reduce((n, c) => n + (c.records ?? 0), 0);
};

const settle = async (w: World) => {
  for (let i = 0; i < 4; i += 1) await w.run();
};

describe("courier and exporter races (C1-C3)", () => {
  test("C1: concurrent exporter and courier passes land every line once", async () => {
    for (let round = 0; round < 5; round += 1) {
      const w = await world({ format: "ndjson" });
      await w.write(line(0));
      await w.run();
      const courier = (async () => {
        for (let n = 1; n <= 8; n += 1) {
          await w.write(line(n));
          await w.run();
        }
      })();
      const [, exported] = await Promise.all([
        courier,
        w.exporter(w.exportText("pre-t0")),
      ]);
      expect(exported.status).toBe("committed");
      await settle(w);

      const { manifest, lines, records } = await w.liveRecords();
      expect(manifest.raw.owner).toBe(w.machineId);
      expect(
        manifest.raw.epochs.filter((e) => e.reason === "backfill"),
      ).toHaveLength(1);
      expect(records.some((r) => r.record_id === "pre-t0")).toBe(true);
      expect(lines.toSorted()).toEqual(
        Array.from({ length: 9 }, (_, n) => line(n)).toSorted(),
      );
      expect(uploadedRecords(manifest)).toBe(9);
      expect(
        (await w.state()).sessions["claude:session-a"].needs_rebase,
      ).toBeFalsy();
    }
  });

  test("C1: an exporter commit in the courier's read-to-write window costs one retry", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.write(line(1));
    w.hooks.beforeManifest = async () => {
      w.hooks.beforeManifest = undefined;
      await w.exporter(w.exportText("pre-t0"));
    };
    await w.run();
    expect((await w.state()).sessions["claude:session-a"].needs_rebase).toBe(
      true,
    );
    await w.run();
    await w.run();
    const { manifest, lines } = await w.liveRecords();
    expect(lines).toEqual([line(0), line(1)]);
    expect(uploadedRecords(manifest)).toBe(2);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual([
      "initial",
      "backfill",
      "resumed",
    ]);
  });

  test("C2: a lost response replayed after an exporter commit never uploads twice", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.write(line(1));
    // The write commits, its response is lost, and the exporter lands next.
    w.hooks.afterManifest = async () => {
      w.hooks.afterManifest = undefined;
      await w.exporter(w.exportText("pre-t0"));
      return Response.json(
        { error: { code: "storage_unavailable" } },
        { status: 503 },
      );
    };
    await w.run();
    await settle(w);
    const { manifest, lines } = await w.liveRecords();
    expect(lines).toEqual([line(0), line(1)]);
    expect(uploadedRecords(manifest)).toBe(2);
    expect(manifest.raw.epochs.map((e) => e.reason)).toEqual([
      "initial",
      "backfill",
    ]);
  });

  test("C3: after a backfill the next write resumes the live offsets", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    const before = (await w.stored())!.manifest;
    await w.exporter(w.exportText("pre-t0"));
    await w.run();
    expect((await w.stored())!.manifest.raw.epoch).toBe(1);
    await w.write(line(1));
    await settle(w);

    const { manifest } = await w.liveRecords();
    const tail = manifest.raw.chunks.at(-1)!;
    expect(manifest.raw.epochs[2]).toMatchObject({
      reason: "resumed",
      resumes_epoch: 0,
      opened_by: w.machineId,
    });
    expect(tail.epoch).toBe(2);
    expect(tail.start_offset).toBe(before.raw.next_offset);
    expect(sourceResumeOffset(manifest)).toBe(2);
  });

  test("a jsonl session's backfill is refused and the courier carries on", async () => {
    const w = await world({ format: "jsonl" });
    await w.write(line(0));
    await w.run();
    await expect(w.exporter(w.exportText("pre-t0"), 1)).rejects.toThrow(
      /ndjson/,
    );
    await w.write(line(1));
    await settle(w);
    expect((await w.stored())!.manifest.raw).toMatchObject({
      format: "jsonl",
      chunk_count: 2,
      epoch: 0,
    });
  });
});

describe("courier edits the stored manifest (C4-C7)", () => {
  async function serverWrites(
    w: World,
    edit: (m: ManifestV1 & Record<string, unknown>) => void,
  ) {
    const { manifest, version } = (await w.stored())!;
    const next = structuredClone(manifest) as ManifestV1 &
      Record<string, unknown>;
    edit(next);
    next.revision += 1;
    await w.store.put(
      objectKey(w.key),
      new TextEncoder().encode(JSON.stringify(next)),
      { ifMatch: version, contentType: "application/json" },
    );
  }

  test("C4: the server section, session fields and unknown fields survive", async () => {
    const w = await world({ format: "jsonl" });
    await w.write(line(0));
    await w.run();
    const at = "2026-10-30T00:00:00.000Z";
    await serverWrites(w, (m) => {
      m.server = {
        updated_at: at,
        normalized: null,
        lifecycle: "idle",
        lifecycle_at: at,
      };
      m.future = { keep: true };
      m.session.parent_session_id = "parent";
      m.extensions = { ...m.extensions, "server.note": 1 };
    });
    await w.write(line(1));
    await settle(w);
    const m = (await w.stored())!.manifest as ManifestV1 &
      Record<string, unknown>;
    expect(m.raw.chunk_count).toBe(2);
    expect(m.server?.lifecycle).toBe("idle");
    expect(m.future).toEqual({ keep: true });
    expect(m.session.parent_session_id).toBe("parent");
    expect(m.extensions?.["server.note"]).toBe(1);
  });

  test("C5: ndjson output is feed rows the transform reads", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.write("not json at all");
    await w.run();
    await w.exporter(w.exportText("pre-t0"));
    const { records, lines } = await w.liveRecords();
    expect(lines.toSorted()).toEqual([line(0), "not json at all"].toSorted());
    const live = records.filter((r) => r.record_id !== "pre-t0");
    expect(live.map((r) => r.revision)).toEqual(["1", "2"]);
    expect(live.every((r) => r.origin_id === ORIGIN)).toBe(true);
    expect(live[0]!.payload).not.toHaveProperty("raw_facts_version");
    expect(live[0]!.payload).not.toHaveProperty("revision");
  });

  test("C5: a client delete reaches the stream and removes the record", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.write(line(1));
    await w.run();
    await w.exporter(w.exportText("pre-t0"));
    w.tombstone(1);
    await settle(w);
    const { lines } = await w.liveRecords();
    expect(lines).toEqual([line(1)]);
  });

  test("C5: identity is what the client's export computes", () => {
    // sha256 of the compact JSON feed key, from relayhistory `make_record`.
    expect(feedRecordId(["session_event", "claude", "s", "u"])).toBe(
      createHash("sha256")
        .update('["session_event","claude","s","u"]')
        .digest("hex"),
    );
    const records = [1, 2, 2, 3].map((revision) => ({
      revision,
      line: "x".repeat(10),
    }));
    const ranges = feedRanges(records, 0, 12);
    expect(ranges.map((r) => [r.start, r.end])).toEqual([
      [0, 1],
      [1, 2],
      [2, 3],
    ]);
    expect(ranges[1]!.bytes.toString().trim().split("\n")).toHaveLength(2);
  });

  test("a rebuilt store is a typed condition, and the session holds rather than poison the stream", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.exporter(w.exportText("pre-t0"));
    w.setEpoch(173);
    await w.write(line(1));
    const before = (await w.stored())!.manifest;
    for (let pass = 0; pass < 3; pass += 1) {
      const { conditions } = await w.run();
      expect(conditions).toEqual([
        expect.objectContaining({
          type: "feed_epoch_moved",
          from: ORIGIN,
          to: "00000000000000ad",
        }),
      ]);
    }
    expect((await w.stored())!.manifest).toEqual(before);
    // What is stored stays readable: one record under two origins would not be.
    expect((await w.liveRecords()).lines).toEqual([line(0)]);

    // After a restart the state is gone, but the stored chunks still name the origin.
    const state = await w.state();
    delete state.sessions["claude:session-a"].base;
    delete state.sessions["claude:session-a"].feed_origin;
    await writeFile(w.statePath, JSON.stringify(state));
    expect((await w.run()).conditions).toEqual([
      expect.objectContaining({ type: "feed_epoch_moved" }),
    ]);
    expect((await w.stored())!.manifest).toEqual(before);
  });

  test("C6: state from before this version rebases first, and never rebuilds epoch 0", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.exporter(w.exportText("pre-t0"));
    const state = await w.state();
    delete state.sessions["claude:session-a"].base;
    await writeFile(w.statePath, JSON.stringify(state));
    await w.write(line(1));
    const posts = w.counts.manifestPosts;
    await w.run();
    expect(w.counts.manifestPosts).toBe(posts + 1);
    const m = (await w.stored())!.manifest;
    expect(m.raw.epochs.map((e) => e.reason)).toEqual([
      "initial",
      "backfill",
      "resumed",
    ]);
    expect(m.raw.chunk_count).toBe(3);
  });

  test("C7: a raw stream another writer owns stops the courier", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await serverWrites(w, (m) => {
      m.raw.owner = "another-instance";
      m.writer.delivery_instance_id = "another-instance";
    });
    await w.write(line(1));
    await w.run();
    await w.run();
    const { chunkPosts, manifestPosts } = w.counts;
    await w.write(line(2));
    await w.run();
    await w.run();
    expect(w.counts).toEqual({ chunkPosts, manifestPosts });
    expect((await w.state()).sessions["claude:session-a"].fenced).toBe(true);
    expect((await w.stored())!.manifest.raw.owner).toBe("another-instance");
  });
});

describe("activation gate (G1) and old couriers (D1)", () => {
  const refused = { code: "activation_required" };

  test("G1: refuses while any reader is unattested, and requeues durably", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    for (const reader of MANIFEST_READERS) {
      w.gate.missing = [reader];
      await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(
        refused,
      );
    }
    expect((await w.stored())!.manifest.raw.epochs).toHaveLength(1);
    expect(
      new Set(
        w.gate.requeued.map((r) => (r.message as { workKey: string }).workKey),
      ).size,
    ).toBe(1);
    // The real queue refuses anything else, which a fake here would not show.
    expect(isStageMessage(w.gate.requeued[0]!.message)).toBe(true);
    expect(w.gate.requeued[0]).toMatchObject({
      delaySeconds: 60,
      message: {
        stage: "backfill-activation",
        payload: { attempt: 1, grantId: "mgr_export" },
      },
    });

    w.gate.missing = [];
    expect(await w.exporter(w.exportText("pre-t0"))).toMatchObject({
      status: "committed",
    });
  });

  test("409 activation_required carries Retry-After", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    w.gate.missing = ["transform"];
    const { manifest, version } = (await w.stored())!;
    const sealed = await w.seal(
      w.exportText("pre-t0"),
      1,
      1,
      manifest.trace_id,
    );
    const next = await appendBackfillEpoch(manifest, {
      openedBy: "migration:mgr_export",
      openedAt: new Date().toISOString(),
      chunks: [sealed.entry],
      marker: { version: 1, source: "neon.delivery_records" },
    });
    const res = await w.post(
      { key: w.key, manifest: next, previousVersion: version },
      "migration",
    );
    expect(res.status).toBe(409);
    expect(res.headers.get("Retry-After")).toBe("60");
  });

  test("G1: refuses a session whose writer lacks the capability", async () => {
    const w = await world({ format: "ndjson" });
    w.hooks.legacyWriter = true;
    await w.write(line(0));
    await w.run();
    await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(
      refused,
    );
  });

  test("the courier advertises it on every write", async () => {
    const w = await world({ format: "jsonl" });
    await w.write(line(0));
    await w.run();
    expect((await w.stored())!.manifest.writer.capabilities).toEqual([
      MANIFEST_EPOCHS_CAPABILITY,
    ]);
  });

  test("D1: an old courier keeps delivering while its backfill waits, and an upgrade releases it", async () => {
    const w = await world({ format: "ndjson" });
    w.hooks.legacyWriter = true;
    await w.write(line(0));
    await w.run();
    for (let n = 1; n <= 3; n += 1) {
      await expect(w.exporter(w.exportText("pre-t0"))).rejects.toMatchObject(
        refused,
      );
      await w.write(line(n));
      await w.run();
    }
    const old = (await w.stored())!.manifest;
    expect(old.raw.chunk_count).toBe(4);
    expect(old.raw.epochs).toHaveLength(1);

    w.hooks.legacyWriter = false;
    await w.write(line(4));
    await w.run();
    expect(await w.exporter(w.exportText("pre-t0"))).toMatchObject({
      status: "committed",
    });
    await w.write(line(5));
    await settle(w);
    const { lines, manifest } = await w.liveRecords();
    expect(lines).toHaveLength(6);
    expect(uploadedRecords(manifest)).toBe(6);
  });
});

describe("machine claim", () => {
  test("a bound courier refuses state filed under another machine id", async () => {
    const w = await world({ format: "ndjson", machineId: "m-claimed" });
    await w.write(line(0));
    await w.run();
    const state = await w.state();
    state.machine_id = "m-invented";
    await writeFile(w.statePath, JSON.stringify(state));
    await expect(w.run({ machineBound: true })).rejects.toMatchObject({
      code: "machine_id_mismatch",
    });
    expect((await w.run()).sessions).toBe(1);
  });

  test("rebase needs a claim: without one the full manifest is withheld and the courier defers", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    await w.write(line(1));
    w.hooks.beforeManifest = async () => {
      w.hooks.beforeManifest = undefined;
      await w.exporter(w.exportText("pre-t0"));
    };
    await w.run();
    expect((await w.state()).sessions["claude:session-a"].needs_rebase).toBe(
      true,
    );

    w.identity.machineClaim = undefined;
    w.openClaimlessWindow();
    await settle(w);
    const stuck = (await w.state()).sessions["claude:session-a"];
    expect(stuck.needs_rebase).toBe(true);
    expect(stuck.fenced).toBeFalsy();
    expect(
      (await w.stored())!.manifest.raw.epochs.map((e) => e.reason),
    ).toEqual(["initial", "backfill"]);

    w.identity.machineClaim = w.machineId;
    await settle(w);
    expect((await w.liveRecords()).lines).toHaveLength(2);
  });

  const status = (w: World, query: string) =>
    w.app.request(
      `/v1/pipeline/manifests/status?${new URLSearchParams({ key: w.key, ...Object.fromEntries(new URLSearchParams(query)) })}`,
      {},
      w.env,
    );

  test("the query never proves a machine: only the claim returns the manifest", async () => {
    const w = await world({ format: "ndjson" });
    await w.write(line(0));
    await w.run();
    const q = `machine=${w.machineId}&writer=${w.machineId}&manifest=1`;
    expect(await (await status(w, q)).json()).toHaveProperty("manifest");

    w.identity.machineClaim = undefined;
    w.openClaimlessWindow();
    expect(await (await status(w, q)).json()).not.toHaveProperty("manifest");
    expect(
      (await status(w, `machine=${w.machineId}&writer=someone&manifest=1`))
        .status,
    ).toBe(403);

    w.identity.machineClaim = "another-machine";
    expect((await status(w, q)).status).toBe(403);
    expect(
      (
        await status(
          w,
          `machine=another-machine&writer=${w.machineId}&manifest=1`,
        )
      ).status,
    ).toBe(403);
  });

  describe("the claimless window", () => {
    const setup = async () => {
      const w = await world({ format: "ndjson" });
      await w.write(line(0));
      await w.run();
      w.identity.machineClaim = undefined;
      return { w, version: `machine=${w.machineId}&writer=${w.machineId}` };
    };

    test("is shut by default: a new deployment requires the claim everywhere", async () => {
      const { w, version } = await setup();
      for (const q of [version, `${version}&manifest=1`])
        expect((await status(w, q)).status).toBe(403);
      await w.write(line(1));
      await w.run();
      expect((await w.stored())!.manifest.raw.chunk_count).toBe(1);
      expect(w.gate.claimless).toEqual([]);
    });

    test("open, it gives exactly the version, and audits caller and client", async () => {
      const { w, version } = await setup();
      w.openClaimlessWindow();
      for (const q of [version, `${version}&manifest=1`]) {
        const res = await status(w, q);
        expect(res.status).toBe(200);
        expect(Object.keys((await res.json()) as object)).toEqual(["version"]);
      }
      expect(w.gate.claimless).toHaveLength(2);
      expect(w.gate.claimless[0]).toEqual({
        orgId: "org_test",
        caller: "agent_relayhistory_assertion:user",
        clientVersion: "96bulls-courier/0.3.0",
      });
    });

    test("is read-only: a claimless write is refused with it open, and leaves nothing", async () => {
      const { w } = await setup();
      w.openClaimlessWindow();
      await w.write(line(1));
      await w.run();
      await w.run();
      expect((await w.stored())!.manifest.raw.chunk_count).toBe(1);
      expect((await w.stored())!.manifest.revision).toBe(1);
      expect(w.gate.claimless.every((use) => !("surface" in use))).toBe(true);
    });

    test("client_version is a meter: changing it decides nothing", async () => {
      for (const client of [
        "96bulls-courier/0.3.0",
        "96bulls-courier/0.2.0",
        "admin",
        "x".repeat(500),
      ]) {
        const { w, version } = await setup();
        const ask = (q: string) =>
          w.app.request(
            `/v1/pipeline/manifests/status?${new URLSearchParams({ key: w.key, ...Object.fromEntries(new URLSearchParams(q)) })}`,
            { headers: { "x-relay-client": client } },
            w.env,
          );
        // Shut, it opens nothing. Open, it unlocks nothing and shapes no answer.
        expect((await ask(version)).status).toBe(403);
        w.openClaimlessWindow();
        const res = await ask(`${version}&manifest=1`);
        expect(res.status).toBe(200);
        expect(Object.keys((await res.json()) as object)).toEqual(["version"]);
        expect(w.gate.claimless.at(-1)!.clientVersion).toBe(
          client.slice(0, 128),
        );
      }
    });

    test("an unaudited use is refused, not waved through", async () => {
      const { w, version } = await setup();
      w.openClaimlessWindow();
      w.gate.auditDown = true;
      expect((await status(w, version)).status).toBe(503);
    });

    test("a present, mismatched claim is refused on every form, window open or not", async () => {
      const { w, version } = await setup();
      w.openClaimlessWindow();
      w.identity.machineClaim = "another-machine";
      for (const q of [version, `${version}&manifest=1`])
        expect((await status(w, q)).status).toBe(403);
      expect(w.gate.claimless).toEqual([]);
    });

    test("closes completely: at the sunset, with a bad date, and on the kill switch", async () => {
      const closers: ((w: World) => void)[] = [
        (w) => w.openClaimlessWindow(new Date(Date.now() - 1000)),
        (w) => void (w.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL = "not a date"),
        (w) => void delete w.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL,
        (w) => void delete w.env.MANIFEST_MACHINE_CLAIM,
        (w) => void (w.env.MANIFEST_MACHINE_CLAIM = "required"),
        (w) => void delete w.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM,
        // Over the cap, though the expiry is still in the future.
        (w) =>
          void (w.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
            Date.now() - 72 * 3_600_000,
          ).toISOString()),
        // Not started yet.
        (w) =>
          void (w.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
            Date.now() + 10_000,
          ).toISOString()),
      ];
      for (const close of closers) {
        const { w, version } = await setup();
        w.openClaimlessWindow();
        expect((await status(w, version)).status).toBe(200);
        close(w);
        const before = w.gate.claimless.length;
        for (const q of [version, `${version}&manifest=1`])
          expect((await status(w, q)).status).toBe(403);
        expect(w.gate.claimless).toHaveLength(before);
      }
    });

    describe("the 72-hour cap", () => {
      const from = new Date("2026-11-01T00:00:00.000Z");
      const at = (ms: number) => new Date(from.getTime() + ms);
      const hours = (n: number) => n * 3_600_000;
      const env = (start: number, end: number) =>
        ({
          MANIFEST_MACHINE_CLAIM: "compat",
          MANIFEST_MACHINE_CLAIM_COMPAT_FROM: at(start).toISOString(),
          MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL: at(end).toISOString(),
        }) as never;
      const first = async () => from.toISOString();

      test("holds to the millisecond", async () => {
        expect(
          await claimlessWindowOpen(env(0, hours(72)), first, at(1000)),
        ).toBe(true);
        expect(
          await claimlessWindowOpen(env(0, hours(72) + 1), first, at(1000)),
        ).toBe(false);
        expect(
          await claimlessWindowOpen(env(0, hours(720)), first, at(1000)),
        ).toBe(false);
        expect(
          await claimlessWindowOpen(env(0, hours(72)), first, at(hours(72))),
        ).toBe(false);
      });

      test("a second activation with a different FROM is refused", async () => {
        expect(
          await claimlessWindowOpen(
            env(hours(48), hours(100)),
            first,
            at(hours(50)),
          ),
        ).toBe(false);
        expect(
          await claimlessWindowOpen(
            env(hours(1), hours(60)),
            first,
            at(hours(2)),
          ),
        ).toBe(false);
        expect(
          await claimlessWindowOpen(env(0, hours(60)), first, at(hours(2))),
        ).toBe(true);
      });

      test("fails closed without the record", async () => {
        expect(
          await claimlessWindowOpen(env(0, hours(1)), undefined, at(1)),
        ).toBe(false);
        const down = async () => {
          throw new Error("down");
        };
        expect(await claimlessWindowOpen(env(0, hours(1)), down, at(1))).toBe(
          false,
        );
      });

      test("through the route: the first use fixes it, and a restatement finds it shut", async () => {
        const { w, version } = await setup();
        w.openClaimlessWindow();
        expect((await status(w, version)).status).toBe(200);
        const fixed = w.gate.firstActivation!;
        const until = w.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL;
        w.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = new Date(
          Date.now() - 1000,
        ).toISOString();
        expect((await status(w, version)).status).toBe(403);
        expect(w.gate.firstActivation).toBe(fixed);

        w.env.MANIFEST_MACHINE_CLAIM_COMPAT_FROM = fixed;
        expect(w.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL).toBe(until);
        expect((await status(w, version)).status).toBe(200);

        w.gate.activationDown = true;
        w.openClaimlessWindow();
        expect((await status(w, version)).status).toBe(403);
      });
    });

    test("the courier stays safe when it closes: it defers, never fences", async () => {
      const { w } = await setup();
      await w.exporter(w.exportText("pre-t0"));
      await w.write(line(1));
      await settle(w);
      expect((await w.state()).sessions["claude:session-a"].fenced).toBeFalsy();
      w.identity.machineClaim = w.machineId;
      await settle(w);
      expect((await w.liveRecords()).lines).toHaveLength(2);
    });
  });

  test("POST refuses a mismatched claim with the window open or shut", async () => {
    for (const open of [false, true]) {
      const w = await world({ format: "ndjson" });
      if (open) w.openClaimlessWindow();
      await w.write(line(0));
      w.identity.machineClaim = "another-machine";
      await w.run();
      expect(await w.stored()).toBeUndefined();
      w.identity.machineClaim = w.machineId;
      await settle(w);
      expect((await w.stored())!.manifest.raw.chunk_count).toBe(1);
    }
  });
});
