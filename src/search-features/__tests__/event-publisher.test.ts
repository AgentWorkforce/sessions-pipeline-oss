import { test, expect } from "bun:test";
import {
  eventFeatureRow,
  publishEventFeatureSnapshots,
} from "../event-publisher.ts";
import {
  copyEventFeaturePage,
  initialEventFeatureCheckpoint,
  captureEventFeatureBaseline,
} from "../event-cycle.ts";
import type { FeatureSnapshot, FeatureReader } from "../source.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
export const fixture = (): FeatureSnapshot => ({
  reference: {
    kind: "event",
    key: {
      orgId: "org",
      workspaceId: "ws",
      source: "codex",
      sessionId: "s",
      machineId: "m",
      kind: "prompt",
      eventId: "e",
      ts: "2026-11-01T10:00:00.123456Z",
    },
    revision: "9007199254740993",
    sourceHash: "a".repeat(64),
    deleted: false,
  },
  row: {
    org_id: "org",
    workspace_id: "ws",
    source: "codex",
    session_id: "s",
    machine_id: "m",
    kind: "prompt",
    event_id: "e",
    ts: "2026-11-01T10:00:00.123456Z",
    ingested_at: "2026-11-01T10:00:01.123457Z",
    user_id: "u",
    actor_name: null,
    actor_role: null,
    project_id: null,
    project_folded: null,
    task_title: "İ Task",
    task_title_folded: "i task",
    content: "Visible unembedded event",
    content_folded: "visible unembedded event",
    task_ref_text: "{}",
    task_ref_text_folded: "{}",
    tags: [],
    embedding: null,
    embedding_model: null,
    content_hash: null,
  },
});
function setup(s = fixture()) {
  const inserts: Record<string, unknown>[][] = [],
    commands: { query: string; params: unknown }[] = [];
  let current = s,
    reads = 0;
  const db: FeatureReader = {
    async query<T>() {
      reads++;
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
    async insert(_t, rows) {
      inserts.push(rows as Record<string, unknown>[]);
    },
    async command(query, o) {
      commands.push({ query, params: o?.params });
    },
    async select() {
      return [];
    },
  };
  return {
    db,
    ch,
    inserts,
    commands,
    set: (s: FeatureSnapshot) => (current = s),
    reads: () => reads,
  };
}
test("all-event row retains null vector/null project/true microseconds and copied lowercase", () => {
  const row = eventFeatureRow(fixture());
  expect(row.embedding).toEqual([]);
  expect(row.project_id).toBeNull();
  expect(row.ts).toBe("2026-11-01 10:00:00.123456");
  expect(row.ingested_at).toBe("2026-11-01 10:00:01.123457");
  expect(row.task_title_folded).toBe("i task");
  expect(row.version).toBe("18014398509481987");
  const empty = fixture();
  empty.row!.project_id = "";
  empty.row!.project_folded = "";
  expect(eventFeatureRow(empty).project_id).toBe("");
});
test("baseline version uses durable captured H without rounding current source revision", () => {
  const row = eventFeatureRow(fixture(), "9007199254740992");
  expect(row.version).toBe("18014398509481985");
  expect(row.source_revision).toBe("9007199254740993");
});
test("all routing/filter/vector faults refuse before any write", async () => {
  for (const mutate of [
    (s: FeatureSnapshot) => (s.row!.workspace_id = "foreign"),
    (s: FeatureSnapshot) => (s.row!.ts = "2026-11-01T10:00:00.123457Z"),
    (s: FeatureSnapshot) => (s.row!.project_folded = "notnull"),
    (s: FeatureSnapshot) => (s.row!.embedding = "[1,2]"),
  ]) {
    const s = fixture();
    mutate(s);
    const d = setup(s);
    await expect(publishEventFeatureSnapshots(d, [s])).rejects.toThrow();
    expect(d.inserts).toHaveLength(0);
  }
});
test("current source CAS follows acknowledged insert; stale source refuses checkpoint", async () => {
  const s = fixture(),
    d = setup(s);
  expect(await publishEventFeatureSnapshots(d, [s])).toHaveLength(1);
  const next = fixture();
  next.reference.revision = "9007199254740994";
  d.set(next);
  await expect(publishEventFeatureSnapshots(d, [s])).rejects.toMatchObject({
    reason: "source_changed",
  });
});
test("failed CH insert cannot ask for source CAS", async () => {
  const d = setup();
  d.ch.insert = async () => {
    throw new Error("failed");
  };
  await expect(publishEventFeatureSnapshots(d, [fixture()])).rejects.toThrow();
  expect(d.reads()).toBe(0);
});
test("tombstone blanks content and erases only exact old routing versions, preserving newer resurrection", async () => {
  const s = fixture();
  s.reference.deleted = true;
  s.row = null;
  const d = setup(s);
  await publishEventFeatureSnapshots(d, [s]);
  expect(d.inserts[0]![0]!.content).toBeNull();
  expect(d.commands[0]!.query).toContain("version<{v0:UInt64}");
  for (const k of [
    "org_id",
    "workspace_id",
    "source",
    "session_id",
    "machine_id",
    "kind",
    "event_id",
    "ts",
  ])
    expect(d.commands[0]!.query).toContain(k + "=");
  expect(d.commands[0]!.query).toContain(",6,'UTC')");
});
test("uncaptured baseline cannot publish; capture is metadata only until runtime persists it", async () => {
  const d = setup();
  await expect(
    copyEventFeaturePage(d, initialEventFeatureCheckpoint()),
  ).rejects.toMatchObject({ reason: "baseline_not_captured" });
  expect(d.inserts).toHaveLength(0);
  const db: FeatureReader = {
    async query<T>() {
      return { rows: [{ p: { observedRevision: "23", rows: [] } }] as T[] };
    },
  };
  const captured = await captureEventFeatureBaseline(db);
  expect(captured.baselineH).toBe("23");
  expect(captured.polled).toBe(false);
  expect(d.inserts).toHaveLength(0);
});

test("baseline capture rejects invented observations before any source read", async () => {
  const d = setup();
  await expect(
    copyEventFeaturePage(d, {
      ...initialEventFeatureCheckpoint(),
      baselineH: "23",
      checkpoint: "23",
      observedRevision: "24",
    }),
  ).rejects.toMatchObject({ reason: "checkpoint_invalid" });
  expect(d.inserts).toHaveLength(0);
});

test("large mandatory metadata crosses old1MiB bound losslessly; row ceiling remains explicit", () => {
  const s = fixture();
  s.row!.task_ref_text = JSON.stringify({ large: "m".repeat(1_200_000) });
  s.row!.task_ref_text_folded = s.row!.task_ref_text;
  const row = eventFeatureRow(s);
  expect(row.task_ref_text).toBe(s.row!.task_ref_text);
  s.row!.task_ref_text = "m".repeat(17 * 1024 * 1024);
  expect(() => eventFeatureRow(s)).toThrow("Event feature publication refused");
});
