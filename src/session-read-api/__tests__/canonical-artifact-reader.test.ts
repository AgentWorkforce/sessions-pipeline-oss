import { beforeEach, expect, test } from "bun:test";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import {
  canonicalArtifactPage,
  type ArtifactReadDeps,
} from "../canonical-artifact-reader.ts";
import {
  ARTIFACT_TABLES,
  type ArtifactIdentity,
  type ArtifactRangeRef,
  type ArtifactIndex,
  type ArtifactEntry,
} from "../canonical-artifact.ts";
const identity: ArtifactIdentity = {
  orgId: "org-a",
  workspaceId: "ws-a",
  source: "codex",
  sessionId: "会話:1",
};
const generation = "b".repeat(64),
  source = { epoch: "1", fingerprint: "a".repeat(64) },
  prefix =
    "96bulls/org-a/v1/ws=ws-a/m=m/h=codex/d=2026/11/01/s=sess_1/normalized/read-projection-v1/" +
    "c".repeat(64) +
    "/" +
    generation +
    "/";
const encode = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
let objects: Map<string, Uint8Array>,
  ack: any,
  index: ArtifactIndex,
  deps: ArtifactReadDeps,
  readKeys: string[],
  cancelled: number,
  controller: AbortController;
async function put(
  name: string,
  body: Uint8Array,
  start: number,
  count: number,
  kind: ArtifactRangeRef["kind"],
): Promise<ArtifactRangeRef> {
  const key = prefix + name;
  objects.set(key, body);
  return {
    key,
    sha256: await sha256Hex(body),
    bytes: body.byteLength,
    start,
    count,
    kind,
  };
}
async function node(
  name: string,
  unit: "rows" | "bytes" | "utf16",
  children: ArtifactRangeRef[],
  extra: Record<string, unknown> = {},
) {
  const start = children[0]!.start,
    count = children.reduce((n, c) => n + c.count, 0);
  return put(
    name,
    encode({
      version: 1,
      kind: "range-node",
      identity,
      generation,
      unit,
      start,
      count,
      children,
      ...extra,
    }),
    start,
    count,
    "node",
  );
}
async function publish() {
  const ref = await put("index.json", encode(index), 0, 0, "node");
  ack = {
    version: 1,
    exists: true,
    current: true,
    identity,
    generation,
    source,
    prefix,
    index: { key: ref.key, sha256: ref.sha256, bytes: ref.bytes },
  };
}
const original = "prefix😀\ud800tail".replace("\\ud800", "\ud800");
beforeEach(async () => {
  objects = new Map();
  readKeys = [];
  cancelled = 0;
  controller = new AbortController();
  const pieces = [original.slice(0, 7), original.slice(7)],
    strings: ArtifactRangeRef[] = [];
  let at = 0;
  for (let i = 0; i < pieces.length; i++) {
    strings.push(
      await put(
        "string-" + i + ".json",
        encode(pieces[i]),
        at,
        pieces[i]!.length,
        "segment",
      ),
    );
    at += pieces[i]!.length;
  }
  const stringRoot = await node("string-node.json", "utf16", strings);
  const big = {
    source: "codex",
    session_id: identity.sessionId,
    event_id: "e2",
    machine_id: "m",
    kind: "tool_call",
    ts: "2026-11-01T10:00:00.000002Z",
    tool_calls: Array.from({ length: 15000 }, (_, i) => ({
      id: String(i),
      arguments: { kept: true },
    })),
    content: "tools intact",
  };
  const blob = encode(big),
    blobRefs: ArtifactRangeRef[] = [];
  for (let i = 0; i < blob.byteLength; i += 65536)
    blobRefs.push(
      await put(
        "blob-" + i + ".json",
        blob.slice(i, i + 65536),
        i,
        Math.min(65536, blob.byteLength - i),
        "segment",
      ),
    );
  const blobRoot = await node("blob-node.json", "bytes", blobRefs);
  const entries: ArtifactEntry[] = [
    {
      table: "convergence_events",
      row: {
        source: "codex",
        session_id: identity.sessionId,
        event_id: "e1",
        machine_id: "m",
        kind: "assistant",
        ts: "2026-11-01T10:00:00.000001Z",
        content: null,
        task_ref: { id: "task1" },
        input_tokens: 7,
        record: { payload: { message_id: "native1" } },
      },
      content: {
        codec: "json-string-segments-v1",
        chars: original.length,
        jsonSha256: await sha256Hex(encode(original)),
        root: stringRoot,
      },
    },
    {
      table: "convergence_events",
      rowJson: {
        codec: "json-utf8-segments-v1",
        bytes: blob.byteLength,
        sha256: await sha256Hex(blob),
        root: blobRoot,
      },
    },
    {
      table: "convergence_events",
      row: {
        source: "codex",
        session_id: identity.sessionId,
        event_id: "e3",
        machine_id: "m",
        kind: "assistant",
        ts: "2026-11-01T10:00:00.000003Z",
        content: "last",
      },
    },
  ];
  const page1 = await put(
    "page-1.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "replay",
      start: 0,
      rows: entries.slice(0, 2),
    }),
    0,
    2,
    "page",
  );
  const page2 = await put(
    "page-2.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "replay",
      start: 2,
      rows: entries.slice(2),
    }),
    2,
    1,
    "page",
  );
  const root = await node("row-node.json", "rows", [page1, page2], {
    table: "convergence_events",
    view: "replay",
  });
  index = {
    version: 1,
    kind: "canonical-read-index",
    codec: "json-utf8-v1",
    identity,
    generation,
    source,
    prefix,
    legacyCoverage: {
      version: 1,
      tables: ["conversation_turns", "session_links", "session_outcomes"],
    },
    tables: Object.fromEntries(
      ARTIFACT_TABLES.map((table) => [
        table,
        { raw: { count: 0, root: null } },
      ]),
    ),
  };
  index.tables.convergence_events!.replay = { count: 3, root };
  const boundRows = [entries[0]!.row!, big, entries[2]!.row!].map((row) => ({
    table: "convergence_events",
    row: Object.fromEntries(
      ["source", "session_id", "event_id", "machine_id", "kind", "ts"].map(
        (key) => [key, row[key]],
      ),
    ),
  }));
  const boundPage = await put(
    "bounds-page.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "bounds",
      start: 0,
      rows: boundRows,
    }),
    0,
    3,
    "page",
  );
  index.tables.convergence_events!.bounds = { count: 3, root: boundPage };
  await publish();
  deps = {
    signal: controller.signal,
    publication: async () => ack,
    read: async (key) => {
      readKeys.push(key);
      const body = objects.get(key);
      if (!body) throw new Error("unexpected object");
      return new ReadableStream({
        start(c) {
          c.enqueue(body);
          c.close();
        },
        cancel() {
          cancelled++;
        },
      });
    },
  };
});
const query = (extra: Record<string, unknown> = {}) => ({
  table: "convergence_events",
  view: "replay" as const,
  limit: 1,
  ...extra,
});
test("reads ordinal pages with exact native identity and backend/index-bound cursors", async () => {
  const first = await canonicalArtifactPage(deps, identity, query());
  expect(first.rows[0]!.row).toMatchObject({
    event_id: "e1",
    content: original,
    task_ref: { id: "task1" },
    input_tokens: 7,
    record: { payload: { message_id: "native1" } },
  });
  expect(first.nextCursor).toStartWith("artifact-v1:");
  const second = await canonicalArtifactPage(
    deps,
    identity,
    query({ cursor: first.nextCursor }),
  );
  expect(second.rows[0]!.row.event_id).toBe("e2");
  expect((second.rows[0]!.row.tool_calls as unknown[]).length).toBe(15000);
  const third = await canonicalArtifactPage(
    deps,
    identity,
    query({ cursor: second.nextCursor }),
  );
  expect(third.rows[0]!.row.event_id).toBe("e3");
  expect(third.nextCursor).toBeNull();
  const descending = await canonicalArtifactPage(
    deps,
    identity,
    query({ order: "desc" }),
  );
  expect(descending.rows[0]!.row.event_id).toBe("e3");
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({ cursor: first.nextCursor, order: "desc" }),
    ),
  ).rejects.toMatchObject({ status: 400 });
});
test("maxContent prefix skips unused segments and preserves UTF16 split/lone surrogates", async () => {
  const result = await canonicalArtifactPage(
    deps,
    identity,
    query({ maxContent: 6 }),
  );
  expect(result.rows[0]!.row.content).toBe(original.slice(0, 7));
  expect(readKeys.some((k) => k.endsWith("string-0.json"))).toBe(true);
  expect(readKeys.some((k) => k.endsWith("string-1.json"))).toBe(false);
  const complete = await canonicalArtifactPage(deps, identity, query());
  expect(complete.rows[0]!.row.content).toBe(original);
});
test("refuses forged tenant publication before object access", async () => {
  ack.identity = { ...identity, workspaceId: "other" };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_publication" });
  expect(readKeys).toEqual([]);
});
test("refuses absent/stale publication without raw fallback", async () => {
  ack = { version: 1, exists: true, current: false, identity, source };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_not_ready" });
  expect(readKeys).toEqual([]);
  ack = {
    version: 1,
    exists: false,
    current: false,
    identity,
    source: { epoch: null, fingerprint: "d".repeat(64) },
  };
  expect(await canonicalArtifactPage(deps, identity, query())).toMatchObject({
    exists: false,
    rows: [],
    legacyCoverage: null,
    nextCursor: null,
  });
  expect(readKeys).toEqual([]);
});
test("session_timing absent because legacy turns are unknown is a non-retryable 501, not artifact_view", async () => {
  const timingQuery = { table: "session_timing", view: "raw" as const, limit: 10 };
  // Producer shape when legacy coverage is unknown: no session_timing table at all.
  index.legacyCoverage = null;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, timingQuery),
  ).rejects.toMatchObject({ reason: "timing_turns_unknown", status: 501 });
  // Any other missing table keeps the integrity refusal.
  delete (index.tables as Record<string, unknown>).session_markers;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, { ...timingQuery, table: "session_markers" }),
  ).rejects.toMatchObject({ reason: "artifact_view", status: 503 });
  // Known coverage but no session_timing table contradicts the producer: still artifact_view.
  index.legacyCoverage = {
    version: 1,
    tables: ["conversation_turns", "session_links", "session_outcomes"],
  };
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, timingQuery),
  ).rejects.toMatchObject({ reason: "artifact_view", status: 503 });
  // Published with coverage: the materialized timing row is served.
  const timingRow = {
    org_id: identity.orgId,
    workspace_id: identity.workspaceId,
    source: identity.source,
    session_id: identity.sessionId,
    timing: { workMs: 5 },
  };
  const page = await put(
    "timing-page.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "session_timing",
      view: "raw",
      start: 0,
      rows: [{ table: "session_timing", row: timingRow }],
    }),
    0,
    1,
    "page",
  );
  index.tables.session_timing = { raw: { count: 1, root: page } };
  await publish();
  const served = await canonicalArtifactPage(deps, identity, timingQuery);
  expect(served.rows.map((r) => r.row)).toEqual([timingRow]);
  expect(served.nextCursor).toBeNull();
});
test("validates every object digest before parsing or returning content", async () => {
  objects.set(
    prefix + "string-0.json",
    encode(original.slice(0, 7).replace("x", "z")),
  );
  await expect(
    canonicalArtifactPage(deps, identity, query({ maxContent: 6 })),
  ).rejects.toMatchObject({ reason: "artifact_digest" });
});
test("rejects incomplete/gapped ordinal ranges even with a newly acknowledged index hash", async () => {
  const root = index.tables.convergence_events!.replay!.root!;
  const value = JSON.parse(new TextDecoder().decode(objects.get(root.key)!));
  value.children[1].start = 3;
  const replacement = await put("bad-node.json", encode(value), 0, 3, "node");
  index.tables.convergence_events!.replay!.root = replacement;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_range" });
});
test("rejects source changes after hydration and changed index hashes on old cursors", async () => {
  let calls = 0;
  deps.publication = async () =>
    ++calls === 1 ? ack : { ...ack, source: { ...source, epoch: "2" } };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
  deps.publication = async () => ack;
  const first = await canonicalArtifactPage(deps, identity, query());
  index.legacyCoverage = null;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query({ cursor: first.nextCursor })),
  ).rejects.toMatchObject({ status: 400 });
});
test("rejects cross-prefix object references even when digest is valid", async () => {
  ack.index = { ...ack.index, key: "other-tenant/index.json" };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_reference" });
  expect(readKeys).toEqual([]);
});
test("aborts stalled artifact streams without waiting for cancellation to resolve", async () => {
  let seen = false;
  deps.read = async () =>
    new ReadableStream({
      start() {},
      cancel() {
        seen = true;
        return new Promise(() => {});
      },
    });
  const pending = canonicalArtifactPage(deps, identity, query());
  setTimeout(() => controller.abort(), 1);
  await expect(pending).rejects.toMatchObject({ reason: "deadline" });
  expect(seen).toBe(true);
});

test("rechecks authenticated absence and binds expected generation on empty results", async () => {
  const absent = {
    version: 1,
    exists: false,
    current: false,
    identity,
    source: { epoch: null, fingerprint: "d".repeat(64) },
  };
  let calls = 0;
  deps.publication = async () => (++calls === 1 ? absent : ack);
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
  expect(readKeys).toEqual([]);
  deps.publication = async () => absent;
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({ generation: "e".repeat(64) }),
    ),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
});
test("rejects acknowledged reversed event rows using the shared byte comparator", async () => {
  const key = prefix + "page-1.json",
    value = JSON.parse(new TextDecoder().decode(objects.get(key)!));
  value.rows.reverse();
  const page = await put("reversed-page.json", encode(value), 0, 2, "page");
  const root = index.tables.convergence_events!.replay!.root!,
    parent = JSON.parse(new TextDecoder().decode(objects.get(root.key)!));
  parent.children[0] = page;
  index.tables.convergence_events!.replay!.root = await put(
    "reversed-node.json",
    encode(parent),
    0,
    3,
    "node",
  );
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query({ limit: 2 })),
  ).rejects.toMatchObject({ reason: "artifact_order" });
});

test("seeks inclusive precise timestamp ranges through bounds without scanning content", async () => {
  const since = "2026-11-01T10:00:00.000002Z",
    until = since;
  const middle = await canonicalArtifactPage(
    deps,
    identity,
    query({ since, until, limit: 3 }),
  );
  expect(middle.rows.map((x) => x.row.event_id)).toEqual(["e2"]);
  expect(middle.nextCursor).toBeNull();
  expect(readKeys.some((k) => k.endsWith("string-0.json"))).toBe(false);
  readKeys = [];
  const latest = await canonicalArtifactPage(
    deps,
    identity,
    query({ since, order: "desc" }),
  );
  expect(latest.rows[0]!.row.event_id).toBe("e3");
  const older = await canonicalArtifactPage(
    deps,
    identity,
    query({ since, order: "desc", cursor: latest.nextCursor }),
  );
  expect(older.rows[0]!.row.event_id).toBe("e2");
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({
        since: "2026-11-01T10:00:00.000001Z",
        order: "desc",
        cursor: latest.nextCursor,
      }),
    ),
  ).rejects.toMatchObject({ status: 400 });
  expect(
    (
      await canonicalArtifactPage(
        deps,
        identity,
        query({ since: "2026-11-01T10:00:00.000004Z" }),
      )
    ).rows,
  ).toEqual([]);
});
test("refuses missing/misaligned timestamp bounds instead of an unbounded replay scan", async () => {
  delete index.tables.convergence_events!.bounds;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query({ since: "2026-11-01" })),
  ).rejects.toMatchObject({ reason: "artifact_bounds" });
  expect(readKeys.some((k) => k.endsWith("page-1.json"))).toBe(false);
});
async function rawPage(rows: Array<Record<string, unknown>>) {
  // Raw view: every row is stored as a segmented rowJson blob.
  const entries: ArtifactEntry[] = [];
  for (const [n, row] of rows.entries()) {
    const blob = encode(row),
      refs: ArtifactRangeRef[] = [];
    for (let i = 0; i < blob.byteLength; i += 65536)
      refs.push(
        await put(
          `raw-${n}-blob-${i}.json`,
          blob.slice(i, i + 65536),
          i,
          Math.min(65536, blob.byteLength - i),
          "segment",
        ),
      );
    entries.push({
      table: "convergence_events",
      rowJson: {
        codec: "json-utf8-segments-v1",
        bytes: blob.byteLength,
        sha256: await sha256Hex(blob),
        root: await node(`raw-${n}-blob-node.json`, "bytes", refs),
      },
    });
  }
  const page = await put(
    "raw-page.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "raw",
      start: 0,
      rows: entries,
    }),
    0,
    entries.length,
    "page",
  );
  index.tables.convergence_events!.raw = { count: entries.length, root: page };
  await publish();
}
const rawRow = (event: string, ts: string, content: string) => ({
  source: "codex",
  session_id: identity.sessionId,
  event_id: event,
  machine_id: "m",
  kind: "tool_result",
  ts,
  content,
});
test("refuses an oversized raw first row from its declared size before reading any segment", async () => {
  await rawPage([
    rawRow("big", "2026-11-01T10:00:00.000001Z", "x".repeat(6 * 1024 * 1024)),
  ]);
  readKeys = [];
  await expect(
    canonicalArtifactPage(deps, identity, query({ view: "raw" })),
  ).rejects.toMatchObject({ reason: "row_bound", status: 503 });
  expect(readKeys.some((k) => k.includes("raw-page.json"))).toBe(true);
  expect(readKeys.filter((k) => k.includes("-blob-"))).toEqual([]);
});
test("an analysis rowJson first row over the reply cap is read under its own snapshot budget, while a raw one is still refused unread", async () => {
  // #181 serves analysis snapshots of up to 64 MiB through view "analysis"
  // (stored as raw rowJson). The declared-size pre-check is for raw/bounds
  // replies only; it must not refuse analysis rows before they are read.
  const payload = "x".repeat(3 * 1024 * 1024),
    row = {
      org_id: identity.orgId,
      workspace_id: identity.workspaceId,
      source: "codex",
      session_id: identity.sessionId,
      ordinal: 0,
      payload: { sessionId: identity.sessionId, body: payload },
    },
    blob = encode(row),
    refs: ArtifactRangeRef[] = [];
  for (let i = 0; i < blob.byteLength; i += 65536)
    refs.push(
      await put(
        `analysis-0-blob-${i}.json`,
        blob.slice(i, i + 65536),
        i,
        Math.min(65536, blob.byteLength - i),
        "segment",
      ),
    );
  const entry: ArtifactEntry = {
    table: "session_analyses" as ArtifactEntry["table"],
    rowJson: {
      codec: "json-utf8-segments-v1",
      bytes: blob.byteLength,
      sha256: await sha256Hex(blob),
      root: await node("analysis-0-blob-node.json", "bytes", refs),
    },
  };
  const page = await put(
    "analysis-page.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "session_analyses",
      view: "raw",
      start: 0,
      rows: [entry],
    }),
    0,
    1,
    "page",
  );
  (index.tables as Record<string, unknown>).session_analyses = {
    raw: { count: 1, root: page },
  };
  await publish();
  expect(blob.byteLength).toBeGreaterThan(3 * 1024 * 1024);
  readKeys = [];
  const result = await canonicalArtifactPage(
    deps,
    identity,
    query({ table: "session_analyses", view: "analysis" }),
  );
  expect(result.rows).toHaveLength(1);
  expect((result.rows[0]!.row.payload as { body: string }).body).toBe(payload);
  expect(readKeys.filter((k) => k.includes("analysis-0-blob-")).length).toBe(
    refs.length + 1,
  );

  await rawPage([
    rawRow("big", "2026-11-01T10:00:00.000001Z", "x".repeat(3 * 1024 * 1024)),
  ]);
  readKeys = [];
  await expect(
    canonicalArtifactPage(deps, identity, query({ view: "raw" })),
  ).rejects.toMatchObject({ reason: "row_bound", status: 503 });
  expect(readKeys.filter((k) => k.includes("-blob-"))).toEqual([]);
});
test("stops before fetching a later raw row that cannot fit the reply", async () => {
  await rawPage([
    rawRow("small", "2026-11-01T10:00:00.000001Z", "ok"),
    rawRow("big", "2026-11-01T10:00:00.000002Z", "x".repeat(6 * 1024 * 1024)),
  ]);
  readKeys = [];
  const first = await canonicalArtifactPage(
    deps,
    identity,
    query({ view: "raw", limit: 2 }),
  );
  expect(first.rows.map((x) => x.row.event_id)).toEqual(["small"]);
  expect(first.nextCursor).toStartWith("artifact-v1:");
  expect(readKeys.filter((k) => k.includes("raw-1-blob"))).toEqual([]);
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({ view: "raw", limit: 2, cursor: first.nextCursor }),
    ),
  ).rejects.toMatchObject({ reason: "row_bound", status: 503 });
  expect(readKeys.filter((k) => k.includes("raw-1-blob"))).toEqual([]);
});
