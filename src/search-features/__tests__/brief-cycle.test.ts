import { test, expect } from "bun:test";
import {
  copyBriefFeaturePage,
  initialBriefFeatureCheckpoint,
} from "../brief-cycle.ts";
import {
  readLegacyCasefoldProfile,
  foldLegacyQuery,
} from "../casefold-source.ts";
import type { FeatureReader } from "../source.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
const ch: ClickHouseClient = {
  database: "fixture",
  async command() {
    throw Error("unexpected CH");
  },
  async insert() {
    throw Error("unexpected CH");
  },
  async select() {
    return [];
  },
};
test("empty initial baseline never claims ready until a committed journal poll closes H", async () => {
  let revision = "3";
  const db: FeatureReader = {
    async query<T>(sql: string) {
      return {
        rows: (sql.startsWith("WITH observed")
          ? [{ p: { observedRevision: revision, rows: [] } }]
          : [{ page: { too_large: false, rows: [] } }]) as T[],
      };
    },
  };
  const first = await copyBriefFeaturePage(
    { db, ch },
    {
      ...initialBriefFeatureCheckpoint(),
      baselineH: "3",
      checkpoint: "3",
      observedRevision: "3",
    },
  );
  expect(first.complete).toBe(false);
  expect(first.next).toMatchObject({
    phase: "journal",
    baselineH: "3",
    checkpoint: "3",
    polled: false,
  });
  revision = "5";
  const poll = await copyBriefFeaturePage({ db, ch }, first.next);
  expect(poll.complete).toBe(true);
  expect(poll.next).toMatchObject({
    checkpoint: "5",
    observedRevision: "5",
    polled: true,
  });
  expect(first.next.checkpoint).toBe("3");
});
test("corrupted resume state cannot skip an initial baseline or manufacture a polled checkpoint", async () => {
  let calls = 0;
  const db: FeatureReader = {
    async query() {
      calls++;
      throw Error("unexpected source");
    },
  };
  for (const state of [
    { ...initialBriefFeatureCheckpoint(), baselineCursor: "skip" },
    { ...initialBriefFeatureCheckpoint(), polled: true },
    { ...initialBriefFeatureCheckpoint(), phase: "journal" as const },
  ])
    await expect(copyBriefFeaturePage({ db, ch }, state)).rejects.toMatchObject(
      { reason: "checkpoint_invalid" },
    );
  expect(calls).toBe(0);
});
test("source fold profile uses bounded readonly metadata pages and preserves exact source mappings, not runtime Unicode", async () => {
  let queries = 0,
    maxPoints = 0;
  const db: FeatureReader = {
    async query<T>(sql: string, params?: unknown[]) {
      queries++;
      expect(sql).not.toContain("sessions.");
      if (sql.includes("pg_database"))
        return {
          rows: [
            {
              datcollate: "fixture",
              datctype: "fixture",
              datlocprovider: "c",
              datlocale: "",
            },
          ] as T[],
        };
      const text = params![0] as string;
      maxPoints = Math.max(maxPoints, Array.from(text).length);
      return { rows: [{ folded: text.replace(/A/g, "a") }] as T[] };
    },
  };
  const profile = await readLegacyCasefoldProfile(db);
  expect(profile.entries).toEqual([["A", "a"]]);
  expect(profile.sourceHash).toMatch(/^[a-f0-9]{64}$/);
  expect(maxPoints).toBeLessThanOrEqual(8192);
  expect(queries).toBeLessThan(150);
  expect(foldLegacyQuery(profile, "Aİ\u{1c89}")).toBe("aİ\u{1c89}");
});
test("contextual/non-libc source fold refuses instead of silently adopting runtime behavior", async () => {
  const db: FeatureReader = {
    async query<T>() {
      return {
        rows: [
          {
            datcollate: "fixture",
            datctype: "fixture",
            datlocprovider: "i",
            datlocale: "icu-fixture",
          },
        ] as T[],
      };
    },
  };
  await expect(readLegacyCasefoldProfile(db)).rejects.toMatchObject({
    reason: "casefold_contextual",
  });
});

test("uninitialized H is refused before any source read or CH write", async () => {
  let reads = 0;
  const db: FeatureReader = {
    async query() {
      reads++;
      throw Error("source should not be touched");
    },
  };
  await expect(
    copyBriefFeaturePage({ db, ch }, initialBriefFeatureCheckpoint()),
  ).rejects.toMatchObject({ reason: "baseline_uninitialized" });
  expect(reads).toBe(0);
});

test("actual-shape builtin C.UTF-8 source is admitted without substituting libc metadata", async () => {
  const db: FeatureReader = {
    async query<T>(sql: string, params?: unknown[]) {
      if (sql.includes("pg_database"))
        return {
          rows: [
            {
              datcollate: "C.UTF-8",
              datctype: "C.UTF-8",
              datlocprovider: "b",
              datlocale: "C.UTF-8",
            },
          ] as T[],
        };
      return { rows: [{ folded: params![0] }] as T[] };
    },
  };
  const p = await readLegacyCasefoldProfile(db);
  expect(p.source).toEqual({
    datcollate: "C.UTF-8",
    datctype: "C.UTF-8",
    datlocprovider: "b",
    datlocale: "C.UTF-8",
  });
});
