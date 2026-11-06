import { beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  readSessionAnalysisCache,
  readSessionAnalysisTitle,
  type AnalysisCacheDeps,
} from "../session-analysis-reader.ts";
import {
  analysisSnapshotJson,
  analysisGenerationJson,
  analysisSourceSnapshotJson,
  type CachedBriefTitle,
} from "../session-analysis-artifact.ts";
const identity = {
    orgId: "org-a",
    workspaceId: "ws-a",
    source: "codex",
    sessionId: "session:one",
  },
  source = { epoch: "9", fingerprint: "a".repeat(64) };
const snapshotAt = "2026-11-03T21:00:00.000000Z",
  validUntil = "2026-11-03T21:02:00.000000Z",
  snapshotId = "snapshot-one",
  prefix = "objects/analysis/";
const bytes = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
const sha = (v: Uint8Array | string) =>
  createHash("sha256").update(v).digest("hex");
let objects: Map<string, Uint8Array>,
  ack: any,
  deps: AnalysisCacheDeps,
  reads: number,
  claims: number;
async function fixture(change?: {
  row?: Record<string, unknown>;
  count?: number;
  outcome?: string;
  snapshotSha?: string;
  title?: CachedBriefTitle;
  sourceDigest?: string;
  empty?: boolean;
}) {
  objects = new Map();
  reads = 0;
  claims = 0;
  const basePayload = {
    outcome: "ok" as const,
    sessionId: identity.sessionId,
    requests: [
      { id: "request-real", sessionId: identity.sessionId, status: "running" },
    ],
    analyses: [
      {
        id: "virtual-v1-id",
        sessionId: identity.sessionId,
        status: "running",
        result: {
          brief: {
            title: "ready while running",
            description: "large preserved",
            metadata: { tools: [{ id: "t" }] },
          },
        },
      },
    ],
  };
  const payload = change?.empty
    ? { outcome: "not_found" as const }
    : basePayload;
  const snapshotSha256 =
    change?.snapshotSha ?? sha(analysisSnapshotJson(payload));
  const sourceSnapshotSha256 = change?.title
    ? (change.sourceDigest ??
      sha(analysisSourceSnapshotJson(snapshotSha256, change.title)))
    : undefined;
  const generation = sha(
    analysisGenerationJson(
      identity,
      source,
      snapshotAt,
      snapshotId,
      snapshotSha256,
      sourceSnapshotSha256,
    ),
  );
  const put = (name: string, v: unknown) => {
    const body = bytes(v),
      key = prefix + name;
    objects.set(key, body);
    return { key, sha256: sha(body), bytes: body.length };
  };
  const tables: Record<string, unknown> = {};
  for (const [table, items] of [
    ["session_analysis_requests", change?.empty ? [] : basePayload.requests],
    ["session_analyses", change?.empty ? [] : basePayload.analyses],
  ] as const) {
    if (!items.length) {
      tables[table] = { raw: { count: 0, root: null } };
      continue;
    }
    const rows = items.map((item, ordinal) => ({
      table,
      row: {
        org_id: identity.orgId,
        workspace_id: identity.workspaceId,
        source: identity.source,
        session_id: identity.sessionId,
        ordinal,
        payload: item,
        ...(table === "session_analyses" ? change?.row : {}),
      },
    }));
    const page = {
      ...put(table + "-page", {
        version: 1,
        kind: "row-page",
        identity,
        generation,
        table,
        view: "raw",
        start: 0,
        rows,
      }),
      kind: "page",
      start: 0,
      count: rows.length,
    };
    const root = {
      ...put(table + "-root", {
        version: 1,
        kind: "range-node",
        identity,
        generation,
        table,
        view: "raw",
        unit: "rows",
        start: 0,
        count: rows.length,
        children: [page],
      }),
      kind: "node",
      start: 0,
      count: rows.length,
    };
    tables[table] = { raw: { count: items.length, root } };
  }
  const graph = put("graph", {
    version: 1,
    kind: "canonical-read-index",
    codec: "json-utf8-v1",
    identity,
    generation,
    source,
    prefix,
    legacyCoverage: null,
    tables,
  });
  const index = put("wrapper", {
    version: 1,
    kind: "session-analysis-index",
    codec: "canonical-analysis-graph-v1",
    projection: "read_session_analysis_v1/0031",
    outcome: change?.empty ? "not_found" : (change?.outcome ?? "ok"),
    ...(change?.title
      ? { cachedBriefTitle: change.title, sourceSnapshotSha256 }
      : {}),
    identity,
    source,
    generation,
    snapshotAt,
    snapshotId,
    snapshotSha256,
    graph,
    counts: {
      requests: change?.empty ? 0 : 1,
      analyses: change?.empty ? 0 : (change?.count ?? 1),
    },
  });
  ack = {
    version: 1,
    exists: true,
    current: true,
    identity,
    source,
    generation,
    snapshotAt,
    snapshotId,
    snapshotSha256,
    validUntil,
    prefix,
    index,
  };
  deps = {
    signal: new AbortController().signal,
    now: () => Date.parse(snapshotAt) + 1000,
    publication: async () => {
      claims++;
      return ack;
    },
    read: async (key) => {
      reads++;
      const body = objects.get(key);
      if (!body) throw new Error("missing object");
      return new ReadableStream({
        start(c) {
          c.enqueue(body);
          c.close();
        },
      });
    },
  };
  return payload;
}
beforeEach(async () => {
  await fixture();
});
test("preserves actual public virtual IDs, array ordering, nested brief and status with final fence", async () => {
  const expected = await fixture();
  const result = await readSessionAnalysisCache(deps, identity);
  expect(result.payload).toEqual(expected);
  expect(claims).toBe(2);
  expect(reads).toBeGreaterThan(0);
});
test("missing/stale/foreign scopes refuse before any S3 bytes", async () => {
  for (const changed of [
    { current: false },
    { identity: { ...identity, workspaceId: "foreign" } },
    { snapshotAt: "2026-11-03T20:00:00Z" },
  ]) {
    await fixture();
    ack = { ...ack, ...changed };
    await expect(
      readSessionAnalysisCache(deps, identity),
    ).rejects.toBeDefined();
    expect(reads).toBe(0);
  }
});
test("absent authority requires unchanged second authenticated absence", async () => {
  const absent = {
    version: 1,
    exists: false,
    current: false,
    identity,
    source: { epoch: null, fingerprint: "f".repeat(64) },
  };
  deps.publication = async () => absent;
  expect((await readSessionAnalysisCache(deps, identity)).outcome).toBe(
    "not_found",
  );
  expect(reads).toBe(0);
  let n = 0;
  deps.publication = async () =>
    ++n === 1
      ? absent
      : { ...absent, source: { epoch: null, fingerprint: "e".repeat(64) } };
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "source_generation_changed",
  });
});
test("private row tenant/source/session and ordinal cannot escape authenticated wrapper", async () => {
  for (const row of [
    { workspace_id: "foreign" },
    { source: "claude" },
    { session_id: "other" },
    { ordinal: 9 },
    { payload: { sessionId: "other" } },
  ]) {
    await fixture({ row });
    await expect(
      readSessionAnalysisCache(deps, identity),
    ).rejects.toMatchObject({
      reason:
        "ordinal" in row || "payload" in row
          ? "analysis_row_identity"
          : "artifact_row_identity",
    });
  }
});
test("count, not-found payload, wrapper digest and snapshot digest are enforced", async () => {
  await fixture({ count: 0 });
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "analysis_history_bound",
  });
  await fixture({ outcome: "not_found" });
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "analysis_index",
  });
  await fixture({ snapshotSha: "d".repeat(64) });
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "analysis_snapshot_digest",
  });
  await fixture();
  objects.set(ack.index.key, bytes({ tampered: true }));
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toBeDefined();
});
test("final current pointer must match every bound snapshot and index field", async () => {
  const original = ack;
  deps.publication = async () =>
    ++claims === 1
      ? original
      : { ...original, index: { ...original.index, sha256: "c".repeat(64) } };
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "source_generation_changed",
  });
});

test("title-only cache reads one bounded wrapper and preserves known-null independently of v1 outcome", async () => {
  const title = {
    version: 1 as const,
    complete: true as const,
    title: "Exact legacy cached title",
  };
  await fixture({ title });
  const result = await readSessionAnalysisTitle(deps, identity);
  expect(result.cachedBriefTitle).toEqual(title);
  expect(reads).toBe(1);
  expect(claims).toBe(2);
  await fixture({ title: { ...title, title: null } });
  expect(
    (await readSessionAnalysisTitle(deps, identity)).cachedBriefTitle.title,
  ).toBeNull();
  await fixture({ title, empty: true });
  expect((await readSessionAnalysisCache(deps, identity)).outcome).toBe(
    "not_found",
  );
  expect(
    (await readSessionAnalysisTitle(deps, identity)).cachedBriefTitle.title,
  ).toBe(title.title);
});
test("legacy wrappers still serve full v1 but cannot fabricate absent title attestation", async () => {
  const expected = await fixture();
  expect((await readSessionAnalysisCache(deps, identity)).payload).toEqual(
    expected,
  );
  await expect(readSessionAnalysisTitle(deps, identity)).rejects.toMatchObject({
    reason: "analysis_title_not_ready",
  });
});
test("title source digest, v2 generation, and final claim fence prevent substitution", async () => {
  const title = {
    version: 1 as const,
    complete: true as const,
    title: "Cached",
  };
  await fixture({ title, sourceDigest: "d".repeat(64) });
  await expect(readSessionAnalysisTitle(deps, identity)).rejects.toMatchObject({
    reason: "analysis_source_snapshot_digest",
  });
  await fixture({ title });
  const original = ack;
  deps.publication = async () =>
    ++claims === 1
      ? original
      : { ...original, index: { ...original.index, sha256: "b".repeat(64) } };
  await expect(readSessionAnalysisTitle(deps, identity)).rejects.toMatchObject({
    reason: "source_generation_changed",
  });
  await fixture({ title });
  const index = JSON.parse(
    new TextDecoder().decode(objects.get(ack.index.key)),
  );
  index.cachedBriefTitle.title = "Tampered";
  const body = bytes(index);
  objects.set(ack.index.key, body);
  ack = {
    ...ack,
    index: { ...ack.index, sha256: sha(body), bytes: body.length },
  };
  await expect(readSessionAnalysisTitle(deps, identity)).rejects.toMatchObject({
    reason: "analysis_source_snapshot_digest",
  });
});
test("v2 full payload remains exact and title-only generation changes with title outside public v1 history", async () => {
  const a = { version: 1 as const, complete: true as const, title: "Old" };
  const expected = await fixture({ title: a });
  const first = await readSessionAnalysisCache(deps, identity);
  expect(first.payload).toEqual(expected);
  await fixture({ title: { ...a, title: "New" } });
  const second = await readSessionAnalysisCache(deps, identity);
  expect(second.payload).toEqual(expected);
  expect(second.generation).not.toBe(first.generation);
});

test("changing title and composite digest cannot reuse an old generation even with a freshly matching index SHA", async () => {
  await fixture({ title: { version: 1, complete: true, title: "Original" } });
  const index = JSON.parse(
    new TextDecoder().decode(objects.get(ack.index.key)),
  );
  index.cachedBriefTitle.title = "Replacement";
  index.sourceSnapshotSha256 = sha(
    analysisSourceSnapshotJson(ack.snapshotSha256, index.cachedBriefTitle),
  );
  const body = bytes(index);
  objects.set(ack.index.key, body);
  ack = {
    ...ack,
    index: { ...ack.index, sha256: sha(body), bytes: body.length },
  };
  await expect(readSessionAnalysisTitle(deps, identity)).rejects.toMatchObject({
    reason: "analysis_generation",
  });
});
