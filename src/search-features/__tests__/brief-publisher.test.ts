import { test, expect } from "bun:test";
import {
  briefFeatureRow,
  publishBriefFeatureSnapshots,
} from "../brief-publisher.ts";
import {
  featureVersion,
  type FeatureSnapshot,
  type FeatureReader,
} from "../source.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
const fixture = (): FeatureSnapshot => ({
  reference: {
    kind: "brief",
    key: {
      orgId: "org",
      sessionId: "session",
      source: "codex",
      workspaceId: "ws",
    },
    revision: "2",
    sourceHash: "a".repeat(64),
    deleted: false,
  },
  row: {
    org_id: "org",
    workspace_id: "ws",
    source: "codex",
    session_id: "session",
    title: "İΣ title",
    description: "Description",
    title_folded: "iσ title",
    description_folded: "description",
    brief_completed_at: "2026-11-01T10:00:00.123456Z",
    embedding: JSON.stringify(
      Array.from({ length: 1536 }, (_, n) => (n === 0 ? 1 : 0)),
    ),
    embedding_model: "legacy-model",
    content_hash: "original-content-hash",
  },
});
function setup(s = fixture()) {
  const writes: { table: string; rows: readonly object[] }[] = [],
    commands: { query: string; options: any }[] = [];
  let current = s;
  const db: FeatureReader = {
    async query<T>() {
      const r = current.reference;
      return {
        rows: [
          {
            page: {
              too_large: false,
              rows: [
                {
                  ordinal: 0,
                  kind: r.kind,
                  key: r.key,
                  revision: r.revision,
                  source_hash: r.sourceHash,
                  deleted: r.deleted,
                  payload: current.row,
                  actual_hash: r.sourceHash,
                },
              ],
            },
          },
        ] as T[],
      };
    },
  };
  const ch: ClickHouseClient = {
    database: "fixture",
    async insert(table, rows) {
      writes.push({ table, rows });
    },
    async command(query, options) {
      commands.push({ query, options });
    },
    async select() {
      return [];
    },
  };
  return {
    db,
    ch,
    writes,
    commands,
    set: (v: FeatureSnapshot) => (current = v),
  };
}
test("copy preserves stored model, float32 vector, completion microseconds and exact legacy text", () => {
  const r = briefFeatureRow(fixture());
  expect(r.version).toBe("5");
  expect(r.embedding_model).toBe("legacy-model");
  expect((r.embedding as number[]).length).toBe(1536);
  expect(r.brief_completed_at).toBe("2026-11-01 10:00:00.123456");
  expect(r.title_folded).toBe("iσ title");
  expect(r.content_hash).toBe("original-content-hash");
});
test("baseline uses captured H, without mislabelling current source revision", () => {
  const s = fixture();
  s.reference.revision = "9007199254740993";
  const row = briefFeatureRow(s, "9007199254740992");
  expect(row.version).toBe("18014398509481985");
  expect(row.source_revision).toBe("9007199254740993");
  expect(featureVersion("9223372036854775807")).toBe("18446744073709551615");
});
test("invalid tenant/vector/dimension and payload bounds refuse before CH insert", async () => {
  for (const change of [
    (s: FeatureSnapshot) => (s.row!.workspace_id = "foreign"),
    (s: FeatureSnapshot) => (s.row!.embedding = "[1,0]"),
    (s: FeatureSnapshot) => (s.row!.description = "x".repeat(131072)),
  ]) {
    const s = fixture();
    change(s);
    const d = setup(s);
    await expect(publishBriefFeatureSnapshots(d, [s])).rejects.toThrow();
    expect(d.writes).toHaveLength(0);
  }
});
test("acknowledged current snapshot returns source CAS; a changed source does not complete", async () => {
  const s = fixture(),
    d = setup(s);
  expect(await publishBriefFeatureSnapshots(d, [s])).toEqual([s.reference]);
  expect(d.writes).toHaveLength(1);
  const next = fixture();
  next.reference.revision = "3";
  next.reference.sourceHash = "b".repeat(64);
  d.set(next);
  await expect(publishBriefFeatureSnapshots(d, [s])).rejects.toMatchObject({
    reason: "source_changed",
  });
});
test("failed durable insert never asks for current-source CAS", async () => {
  const d = setup();
  d.ch.insert = async () => {
    throw Error("private source payload");
  };
  let reads = 0;
  d.db.query = async () => {
    reads++;
    throw Error("unexpected");
  };
  await expect(
    publishBriefFeatureSnapshots(d, [fixture()]),
  ).rejects.toMatchObject({ reason: "publication_failed" });
  expect(reads).toBe(0);
});
test("blank tombstone uses exact tenant key and strict upper version cleanup, preserving newer resurrection", async () => {
  const s = fixture();
  s.reference.deleted = true;
  s.reference.revision = "3";
  s.row = null;
  const d = setup(s);
  await publishBriefFeatureSnapshots(d, [s]);
  expect(d.writes[0]!.rows[0]).toMatchObject({
    version: "7",
    deleted: 1,
    title: "",
    description: "",
    embedding: [],
  });
  expect(d.commands).toHaveLength(1);
  expect(d.commands[0]!.query).toContain("version<{v0:UInt64}");
  expect(d.commands[0]!.options.params).toEqual({
    o0: "org",
    w0: "ws",
    h0: "codex",
    s0: "session",
    v0: "7",
  });
  expect(d.commands[0]!.query).toContain("workspace_id={w0:String}");
});
test("duplicate keys, oversize batch and pre-aborted work produce no CH writes", async () => {
  const d = setup();
  await expect(
    publishBriefFeatureSnapshots(d, [fixture(), fixture()]),
  ).rejects.toMatchObject({ reason: "duplicate" });
  await expect(
    publishBriefFeatureSnapshots(d, Array.from({ length: 201 }, fixture)),
  ).rejects.toMatchObject({ reason: "batch_invalid" });
  const c = new AbortController();
  c.abort();
  await expect(
    publishBriefFeatureSnapshots({ ...d, signal: c.signal }, [fixture()]),
  ).rejects.toThrow();
  expect(d.writes).toHaveLength(0);
});

test("cleared/missing vector preserves live legacy keyword text and a null model", () => {
  const s = fixture();
  s.row!.embedding = null;
  s.row!.embedding_model = null;
  const r = briefFeatureRow(s);
  expect(r).toMatchObject({
    deleted: 0,
    title: "İΣ title",
    embedding: [],
    embedding_model: null,
    embedding_dim: 0,
  });
});

test("pgvector maximum finite Float32 text is admitted while true overflow is refused", () => {
  const s = fixture();
  const v = Array(1536).fill(0);
  v[0] = 3.4028235e38;
  s.row!.embedding = JSON.stringify(v);
  expect((briefFeatureRow(s).embedding as number[])[0]).toBe(v[0]);
  v[0] = 3.5e38;
  s.row!.embedding = JSON.stringify(v);
  expect(() => briefFeatureRow(s)).toThrow();
});
