import { test, expect } from "bun:test";
import {
  publishBriefDocumentsOnce,
  type BriefDocument,
} from "../brief-documents.ts";
import type { SqlExecutor } from "../../../packages/pipeline/src/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
function setup(deleted = false) {
  const document: BriefDocument = {
    version: 1,
    orgId: "org",
    workspaceId: "ws",
    source: "codex",
    sessionId: "one",
    deleted,
    visible: !deleted,
    title: deleted ? "" : "İΣ title",
    summary: deleted ? "" : "Summary",
    projectId: deleted ? "" : "project",
    lastActivityAt: "2026-11-01T10:00:00.123456Z",
  };
  const writes: readonly object[][] = [],
    calls: { sql: string; params?: unknown[] }[] = [],
    commands: { query: string; options: any }[] = [];
  let bad = false;
  const db: SqlExecutor = {
    async query<T>(sql: string, params?: unknown[]) {
      calls.push({ sql, params });
      if (sql.includes("backfill_"))
        return { rows: [{ r: { processed: 1 } }] as T[] };
      if (sql.includes("claim_"))
        return {
          rows: [
            {
              r: [
                {
                  document,
                  epoch: "9007199254740993",
                  hash: "a".repeat(64),
                  leaseNonce: bad ? "wrong" : params![0],
                },
              ],
            },
          ] as T[],
        };
      if (sql.includes("ack_")) return { rows: [{ n: 1 }] as T[] };
      if (sql.includes("retry_")) return { rows: [] };
      throw Error("unexpected SQL");
    },
  };
  const ch: ClickHouseClient = {
    database: "fixture",
    async insert(_table, rows) {
      (writes as object[][]).push([...rows]);
    },
    async command(query, options) {
      commands.push({ query, options });
    },
    async select() {
      return [];
    },
  };
  return {
    document,
    db,
    ch,
    writes,
    calls,
    commands,
    badNonce: () => (bad = true),
  };
}
test("actual staged publisher preserves exact epoch and Unicode fold before source ACK", async () => {
  const d = setup();
  expect(await publishBriefDocumentsOnce(d)).toEqual({
    claimed: 1,
    published: 1,
    backfilled: 1,
  });
  expect(d.writes[0]![0]).toMatchObject({
    epoch: "9007199254740993",
    title: "İΣ title",
    title_folded: "İΣ title".toLowerCase(),
    last_activity_at: "2026-11-01 10:00:00.123456",
  });
  expect(d.calls.at(-1)!.sql).toContain("ack_");
  expect(JSON.parse(d.calls.at(-1)!.params![1] as string)[0].epoch).toBe(
    "9007199254740993",
  );
});
test("empty workspace remains a valid canonical identity", async () => {
  const d = setup();
  d.document.workspaceId = "";
  expect(await publishBriefDocumentsOnce(d)).toEqual({
    claimed: 1,
    published: 1,
    backfilled: 1,
  });
  expect(d.writes[0]![0]).toMatchObject({ workspace_id: "" });
  expect(JSON.parse(d.calls.at(-1)!.params![1] as string)[0].workspaceId)
    .toBe("");
});
test("tombstone deletes only scoped lower epochs and ACKs after cleanup", async () => {
  const d = setup(true);
  await publishBriefDocumentsOnce(d);
  expect(d.writes[0]![0]).toMatchObject({ deleted: 1, title: "", summary: "" });
  expect(d.commands[0]!.query).toContain("epoch<{e0:UInt64}");
  expect(d.commands[0]!.options.params).toEqual({
    o0: "org",
    w0: "ws",
    h0: "codex",
    s0: "one",
    e0: "9007199254740993",
  });
  expect(d.calls.at(-1)!.sql).toContain("ack_");
});
test("wrong lease nonce and oversized text are retried without CH writes or source ACK", async () => {
  for (const mutate of [
    (d: ReturnType<typeof setup>) => d.badNonce(),
    (d: ReturnType<typeof setup>) =>
      (d.document.summary = "x".repeat(128 * 1024)),
  ]) {
    const d = setup();
    mutate(d);
    await expect(publishBriefDocumentsOnce(d)).rejects.toMatchObject({
      reason: "publication_failed",
    });
    expect(d.writes).toHaveLength(0);
    expect(d.calls.some((c) => c.sql.includes("ack_"))).toBe(false);
    expect(d.calls.at(-1)!.sql).toContain("retry_");
  }
});
test("failed CH acknowledgement cannot become a successful metadata publication", async () => {
  const d = setup();
  d.ch.insert = async () => {
    throw Error("private content");
  };
  await expect(publishBriefDocumentsOnce(d)).rejects.toMatchObject({
    reason: "publication_failed",
  });
  expect(d.calls.some((c) => c.sql.includes("ack_"))).toBe(false);
  expect(d.calls.at(-1)!.sql).toContain("retry_");
});
