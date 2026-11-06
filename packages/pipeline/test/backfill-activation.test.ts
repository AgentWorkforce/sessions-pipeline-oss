import { describe, expect, it } from "vitest";
import {
  backfillActivationDelay,
  backfillActivationHandler,
  backfillActivationLimits,
  type BackfillActivationPayload,
} from "../src/lib/pipeline/backfill-activation.js";
import {
  attestReader,
  MANIFEST_READERS,
  missingReaders,
  selfCheckManifestEpochs,
  type ReadinessRow,
  type ReadinessStore,
} from "../src/lib/pipeline/manifest-readiness.js";

const memory = (): ReadinessStore & { rows: ReadinessRow[] } => {
  const rows: ReadinessRow[] = [];
  return {
    rows,
    list: async () => rows,
    put: async (row) => {
      const at = rows.findIndex((r) => r.reader === row.reader);
      if (at >= 0) rows[at] = row;
      else rows.push(row);
    },
  };
};
const KEY = "test-readiness-key";
const attestAll = async (store: ReadinessStore, now?: Date) => {
  for (const reader of MANIFEST_READERS)
    await attestReader({
      reader,
      key: KEY,
      store,
      selfCheck: async () => {},
      releaseSha: "abc123",
      ...(now ? { now } : {}),
    });
};

describe("reader attestation", () => {
  it("opens only when every reader has a valid, fresh row", async () => {
    const store = memory();
    expect(await missingReaders({ key: KEY, store })).toEqual([
      ...MANIFEST_READERS,
    ]);
    await attestReader({
      reader: "transform",
      key: KEY,
      store,
      selfCheck: async () => {},
      releaseSha: "abc123",
    });
    expect(await missingReaders({ key: KEY, store })).not.toContain(
      "transform",
    );
    await attestAll(store);
    expect(await missingReaders({ key: KEY, store })).toEqual([]);
  });

  it("fails closed without the key or the store", async () => {
    const store = memory();
    await attestAll(store);
    expect(await missingReaders({ key: undefined, store })).toHaveLength(
      MANIFEST_READERS.length,
    );
    expect(await missingReaders({ key: KEY, store: undefined })).toHaveLength(
      MANIFEST_READERS.length,
    );
  });

  it("refuses a forged, edited, foreign-key, stale or future row", async () => {
    const store = memory();
    await attestAll(store);
    const edits: ((r: ReadinessRow) => void)[] = [
      (r) => void (r.hmac = "0".repeat(64)),
      (r) => void (r.releaseSha = "other"),
      (r) => void (r.capability = "manifest-epochs-v1"),
    ];
    for (const edit of edits) {
      const row = { ...store.rows[0]! };
      const copy = memory();
      await attestAll(copy);
      edit(copy.rows[0]!);
      expect(row.reader).toBe(copy.rows[0]!.reader);
      expect(await missingReaders({ key: KEY, store: copy })).toEqual([
        row.reader,
      ]);
    }
    expect(await missingReaders({ key: "another-key", store })).toHaveLength(
      MANIFEST_READERS.length,
    );
    const old = memory();
    await attestAll(old, new Date("2026-01-01T00:00:00.000Z"));
    expect(
      await missingReaders({
        key: KEY,
        store: old,
        now: new Date("2026-02-01T00:00:00.000Z"),
      }),
    ).toHaveLength(MANIFEST_READERS.length);
    expect(
      await missingReaders({
        key: KEY,
        store: old,
        now: new Date("2025-12-31T00:00:00.000Z"),
      }),
    ).toHaveLength(MANIFEST_READERS.length);
  });

  it("the self-check passes on this build's validator", async () => {
    await expect(selfCheckManifestEpochs()).resolves.toBeUndefined();
  });

  it("does not attest without proof or from an unreleased build", async () => {
    const store = memory();
    await expect(
      attestReader({
        reader: "fetch",
        key: KEY,
        store,
        selfCheck: async () => {
          throw new Error("cannot parse");
        },
        releaseSha: "abc",
      }),
    ).rejects.toThrow(/cannot parse/);
    await expect(
      attestReader({
        reader: "fetch",
        key: KEY,
        store,
        selfCheck: async () => {},
      }),
    ).rejects.toThrow(/unreleased/);
    expect(store.rows).toEqual([]);
  });
});

describe("backfill-activation stage", () => {
  const payload = (attempt: number): BackfillActivationPayload => ({
    manifestKey: "k",
    grantId: "g",
    planDigest: "d",
    attempt,
  });
  const message = (attempt: number) => ({ payload: payload(attempt) }) as never;
  const run = async (attempt: number, missing: string[]) => {
    const ready: unknown[] = [];
    const requeued: {
      message: { payload: BackfillActivationPayload };
      delay: number;
    }[] = [];
    const handler = backfillActivationHandler({
      missing: async () => missing,
      ready: async (p) => void ready.push(p),
      requeue: async (m, delay) =>
        void requeued.push({ message: m as never, delay }),
    });
    await handler(message(attempt), {} as never, 1);
    return { ready, requeued };
  };

  it("marks the item ready once the gate opens", async () => {
    expect(await run(3, [])).toMatchObject({
      ready: [payload(3)],
      requeued: [],
    });
  });

  it("re-sends with a growing, capped delay while closed", async () => {
    const { requeued, ready } = await run(2, ["transform"]);
    expect(ready).toEqual([]);
    expect(requeued[0]).toMatchObject({
      delay: 120,
      message: { payload: { attempt: 3 } },
    });
    expect([1, 2, 3, 7, 11].map((n) => backfillActivationDelay(n))).toEqual([
      60, 120, 240, 3600, 3600,
    ]);
  });

  it("dead-letters after the attempts run out, non-retryably", async () => {
    await expect(run(12, ["transform"])).rejects.toMatchObject({
      code: "backfill_activation_exhausted",
      retryable: false,
    });
  });

  it("takes limits from config, defaulting on nonsense", () => {
    expect(backfillActivationLimits({})).toMatchObject({
      maxAttempts: 12,
      maxDelaySeconds: 3600,
    });
    expect(
      backfillActivationLimits({
        BACKFILL_ACTIVATION_MAX_ATTEMPTS: "5",
        BACKFILL_ACTIVATION_MAX_DELAY_SECONDS: "x",
      }),
    ).toMatchObject({ maxAttempts: 5, maxDelaySeconds: 3600 });
  });
});
