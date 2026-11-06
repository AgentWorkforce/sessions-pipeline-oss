import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import {
  computeContentSha256,
  manifestKey,
  publicationManifestKey,
  uploadGenerationPrefix,
  readAll,
  objectKey,
  rawChunkKey,
  type ChunkEntry,
  type ManifestV1,
} from "../../../packages/object-store/src/index.ts";
import {
  canonicalSnapshot,
  type CanonicalDeps,
  type CanonicalPointer,
} from "../canonical.ts";
import {
  canonicalHandle,
  canonicalPointerReader,
} from "./fixtures/worker-canonical.ts";
const AT = "2026-11-01T10:00:00.000Z",
  TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const identity = {
  orgId: "org_1",
  workspaceId: "ws_1",
  source: "claude",
  sessionId: "sess:1",
};
const location = {
  prefix: "96bulls/org_1",
  workspaceId: "ws_1",
  machineId: "srv_bb",
  harness: "claude",
  sessionId: "sess_1",
  startDate: "2026-11-01",
};
let root: string, deps: CanonicalDeps, pointer: CanonicalPointer;
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "canonical-reader-"));
  const store = createFsStore({ root });
  const lines = [
    {
      kind: "session_event",
      origin_id: "origin",
      record_id: "r1",
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: {
        ts_ms: Date.parse(AT),
        text: "authoritative transcript",
        role: "assistant",
        message_id: "msg-1",
      },
    },
  ];
  lines.push({
    ...lines[0]!,
    record_id: "r2",
    payload: {
      ...lines[0]!.payload,
      text: "second transcript",
      message_id: "msg-2",
    },
  });
  const raw = new TextEncoder().encode(
      lines.map((r) => JSON.stringify(r)).join("\n") + "\n",
    ),
    stored = new Uint8Array(zstdCompressSync(raw)),
    sha = await sha256Hex(raw);
  const chunk: ChunkEntry = {
    seq: 0,
    epoch: 0,
    start_offset: 0,
    bytes: raw.byteLength,
    stored_bytes: stored.byteLength,
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored),
    stored_raw_bytes: raw.byteLength,
    raw_sha256: sha,
    compression: "zstd",
    records: 2,
  };
  const manifest: ManifestV1 = {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision: 1,
    created_at: AT,
    updated_at: AT,
    session: {
      workspace_id: "ws_1",
      machine_id: "srv_bb",
      harness: "claude",
      session_id: "sess_1",
      started_at: AT,
    },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
    raw: {
      owner: "srv_bb",
      state: "open",
      updated_at: AT,
      format: "ndjson",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: raw.byteLength,
      stored_bytes: stored.byteLength,
      content_sha256: await computeContentSha256([chunk]),
      epoch: 0,
      epochs: [
        {
          epoch: 0,
          opened_at: AT,
          reason: "initial",
          first_seq: 0,
          opened_by: "srv_bb",
        },
      ],
      next_offset: raw.byteLength,
      chunks: [chunk],
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: {
      "96bulls": { user_id: "user_1", canonical_session_id: "sess:1" },
    },
  } as ManifestV1;
  await store.put(rawChunkKey(location, 0, 0, sha, "ndjson"), stored, {});
  await store.put(
    objectKey(manifestKey(location)),
    new TextEncoder().encode(JSON.stringify(manifest)),
    {},
  );
  pointer = {
    org_id: "org_1",
    workspace_id: "ws_1",
    harness: "claude",
    session_id: "sess:1",
    machine_id: "srv_bb",
    user_id: "user_1",
    storage_provider: "s3",
    bucket: "bucket",
    manifest_key: manifestKey(location),
    content_sha256: "\\x" + manifest.raw.content_sha256,
    raw_format: "ndjson",
    revision: 1,
    deleted_at: null,
  };
  deps = {
    store,
    provider: "s3",
    bucket: "bucket",
    prefix: {
      PIPELINE_KEY_PREFIX: "96bulls",
      PIPELINE_ORG_FROM_PREFIX: "true",
    },
    pointers: async () => [pointer],
    decompress: (b) => new Uint8Array(zstdDecompressSync(b)),
  };
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});
test("reads canonical records with pointer chunk-chain digest and the shared writer projection", async () => {
  const snapshot = await canonicalSnapshot(deps, identity);
  expect(snapshot.rows).toHaveLength(2);
  expect(snapshot.rows[0]).toMatchObject({
    table: "convergence_events",
    row: {
      session_id: "sess:1",
      source: "claude",
      user_id: "user_1",
      content: "authoritative transcript",
      record: { payload: { message_id: "msg-1" } },
    },
  });
});
test("rejects changed source generation after object hydration", async () => {
  let call = 0;
  deps.pointers = async () => [{ ...pointer, revision: ++call === 1 ? 1 : 2 }];
  await expect(canonicalSnapshot(deps, identity)).rejects.toMatchObject({
    reason: "source_generation_changed",
  });
});
test.each(["org_id", "workspace_id", "harness", "session_id"])(
  "refuses foreign %s before objects",
  async (field) => {
    deps.pointers = async () => [{ ...pointer, [field]: "foreign" }];
    let touched = false;
    deps.store = new Proxy(deps.store, {
      get(target, key) {
        if (key === "getObject") touched = true;
        return Reflect.get(target, key);
      },
    });
    await expect(canonicalSnapshot(deps, identity)).rejects.toMatchObject({
      reason: "tenant_mismatch",
    });
    expect(touched).toBe(false);
  },
);
test("does not treat the RPC truncation boundary as a complete source set", async () => {
  deps.pointers = async () => Array.from({ length: 64 }, () => pointer);
  await expect(canonicalSnapshot(deps, identity)).rejects.toMatchObject({
    reason: "pointer_bound",
  });
});
test("refuses raw digest drift even with unchanged manifest revision", async () => {
  deps.pointers = async () => [{ ...pointer, content_sha256: "a".repeat(64) }];
  await expect(canonicalSnapshot(deps, identity)).rejects.toMatchObject({
    reason: "manifest_mismatch",
  });
});
const token = () =>
  [
    btoa("{}"),
    btoa(
      JSON.stringify({
        role: "session_directory_search_worker",
        app_metadata: { org_id: "org_1", workspace_ids: ["ws_1"] },
      }),
    ),
    "signature",
  ].join(".");
const request = (query = "") =>
  new Request(
    "https://reader.invalid/v1/canonical/sessions/sess%3A1?workspace=ws_1&source=claude" +
      query,
    { headers: { authorization: "Bearer " + token() } },
  );
test("HTTP projection protocol binds tenant, source, canonical ID and pointer generation", async () => {
  const result = await canonicalHandle(
    request("&limit=1"),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  expect(result.status).toBe(200);
  expect(result.headers.get("X-Relayhistory-Workspace-Id")).toBe("ws_1");
  expect(await result.json()).toMatchObject({
    orgId: "org_1",
    workspaceId: "ws_1",
    source: "claude",
    sessionId: "sess:1",
    rows: [
      {
        table: "convergence_events",
        row: { content: "authoritative transcript" },
      },
    ],
  });
});
test("disabled canonical endpoint remains dormant", async () => {
  const result = await canonicalHandle(request(), {}, fetch, deps);
  expect(result.status).toBe(503);
});
test("a syntactically valid forged JWT cannot access any S3 object before RPC verification", async () => {
  const urls: string[] = [];
  const result = await canonicalHandle(
    request(),
    {
      CANONICAL_READ_ENABLED: "true",
      CANONICAL_SUPABASE_URL: "https://metadata.invalid",
      CANONICAL_SUPABASE_API_KEY: "anon",
      PIPELINE_RAW_S3_BUCKET: "bucket",
      PIPELINE_RAW_S3_ACCESS_KEY_ID: "synthetic",
      PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "synthetic",
      PIPELINE_KEY_PREFIX: "96bulls",
      PIPELINE_ORG_FROM_PREFIX: "true",
    },
    async (input, init) => {
      urls.push(String(input));
      expect(new Headers(init?.headers).get("authorization")).toBe(
        "Bearer " + token(),
      );
      return new Response("denied", { status: 401 });
    },
  );
  expect(result.status).toBe(401);
  expect(urls).toEqual([
    "https://metadata.invalid/rest/v1/rpc/session_directory_pointers",
  ]);
});

test("projection pages preserve precise event order and source-bound cursor generation", async () => {
  const first = await canonicalHandle(
    request("&limit=1"),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  const a = (await first.json()) as {
    rows: Array<{ row: { event_id: string } }>;
    nextCursor: string;
  };
  expect(a.rows).toHaveLength(1);
  expect(a.nextCursor).toBeTruthy();
  const second = await canonicalHandle(
    request("&limit=1&cursor=" + encodeURIComponent(a.nextCursor)),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  const b = (await second.json()) as {
    rows: Array<{ row: { event_id: string } }>;
    nextCursor: null;
  };
  expect(b.rows).toHaveLength(1);
  expect(b.rows[0]!.row.event_id).not.toBe(a.rows[0]!.row.event_id);
  expect(b.nextCursor).toBeNull();
  const reversed = await canonicalHandle(
    request("&limit=1&order=desc"),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  expect(((await reversed.json()) as typeof a).rows[0]!.row.event_id).toBe(
    b.rows[0]!.row.event_id,
  );
  const wrongOrder = await canonicalHandle(
    request("&limit=1&order=desc&cursor=" + encodeURIComponent(a.nextCursor)),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  expect(wrongOrder.status).toBe(400);
});

test("oversized successful pointer metadata is cancelled before any S3 read", async () => {
  let cancelled = false;
  const result = await canonicalHandle(
    request(),
    {
      CANONICAL_READ_ENABLED: "true",
      CANONICAL_SUPABASE_URL: "https://metadata.invalid",
      CANONICAL_SUPABASE_API_KEY: "anon",
      PIPELINE_RAW_S3_BUCKET: "bucket",
      PIPELINE_RAW_S3_ACCESS_KEY_ID: "synthetic",
      PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "synthetic",
    },
    async () =>
      new Response(
        new ReadableStream({
          start(c) {
            c.enqueue(new Uint8Array(2 * 1024 * 1024 + 1));
          },
          cancel() {
            cancelled = true;
          },
        }),
      ),
  );
  expect(result.status).toBe(503);
  expect(result.headers.get("X-Session-Read-Reason")).toBe("metadata_bound");
  expect(cancelled).toBe(true);
});

test("stalled successful pointer bodies report deadline and cancel their stream", async () => {
  const controller = new AbortController();
  let cancelled = false;
  const read = canonicalPointerReader(
    {
      CANONICAL_SUPABASE_URL: "https://metadata.invalid",
      CANONICAL_SUPABASE_API_KEY: "anon",
    },
    token(),
    controller.signal,
    async () =>
      new Response(
        new ReadableStream({
          cancel() {
            cancelled = true;
          },
        }),
      ),
  )(identity);
  setTimeout(() => controller.abort(), 1);
  await expect(read).rejects.toMatchObject({ reason: "deadline" });
  expect(cancelled).toBe(true);
});

test("independent: canonical-machine direct export must project", async () => {
  const m: any = await new Response(
    await deps.store.get(objectKey(manifestKey(location))),
  ).json();
  const machine = "machine:1",
    physical = "machine_1",
    updated = { ...location, machineId: physical };
  const row = {
    kind: "convergence_event",
    origin_id: "machine:" + machine,
    record_id: "e1",
    revision: "1",
    operation: "upsert",
    received_at: AT,
    payload: {
      source: "claude",
      session_id: "sess:1",
      machine_id: machine,
      user_id: "user_1",
      event_id: "e1",
      kind: "prompt",
      ts: AT,
      content: "direct transcript",
    },
  };
  const raw = new TextEncoder().encode(JSON.stringify(row) + "\n"),
    stored = new Uint8Array(zstdCompressSync(raw)),
    sha = await sha256Hex(raw);
  const chunk = {
    ...m.raw.chunks[0],
    bytes: raw.byteLength,
    stored_bytes: stored.byteLength,
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored),
    stored_raw_bytes: raw.byteLength,
    raw_sha256: sha,
    records: 1,
  };
  m.session.machine_id = physical;
  m.writer.delivery_instance_id = physical;
  m.raw.owner = physical;
  m.raw.epochs[0].opened_by = physical;
  m.raw.chunks = [chunk];
  m.raw.raw_bytes = raw.byteLength;
  m.raw.stored_bytes = stored.byteLength;
  m.raw.next_offset = raw.byteLength;
  m.raw.content_sha256 = await computeContentSha256([chunk]);
  m.extensions["96bulls"].canonical_machine_id = machine;
  await deps.store.put(rawChunkKey(updated, 0, 0, sha, "ndjson"), stored, {});
  await deps.store.put(
    objectKey(manifestKey(updated)),
    new TextEncoder().encode(JSON.stringify(m)),
    {},
  );
  deps.pointers = async () => [
    {
      ...pointer,
      machine_id: machine,
      manifest_key: manifestKey(updated),
      content_sha256: "\\x" + m.raw.content_sha256,
    },
  ];
  const result = await canonicalSnapshot(deps, identity);
  expect(result.rows[0]?.row.machine_id).toBe(machine);
});
test("independent: pointer owner must fence mutable manifest metadata", async () => {
  const key = objectKey(manifestKey(location));
  const m: any = await new Response(await deps.store.get(key)).json();
  m.extensions["96bulls"].user_id = "user_2";
  await deps.store.put(key, new TextEncoder().encode(JSON.stringify(m)), {});
  await expect(canonicalSnapshot(deps, identity)).rejects.toMatchObject({
    reason: "manifest_mismatch",
  });
});
import { readCanonicalScope } from "../canonical-projection.ts";
import { objectStoreManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
test("independent: actual record count must enforce scope bound when optional count absent", async () => {
  const key = objectKey(manifestKey(location));
  const m: any = await new Response(await deps.store.get(key)).json();
  delete m.raw.chunks[0].records;
  await deps.store.put(key, new TextEncoder().encode(JSON.stringify(m)), {});
  const result = await readCanonicalScope(
    {
      orgId: "org_1",
      workspaceId: "ws_1",
      harness: "claude",
      sessionId: "sess:1",
    },
    [manifestKey(location)],
    {
      store: deps.store,
      reader: objectStoreManifestReader(deps.store, {
        provider: "s3",
        bucket: "bucket",
      }),
      prefixFor: () => "96bulls/org_1",
      decompress: deps.decompress,
    },
    {
      maxScopeBytes: 16 * 1024 * 1024,
      maxScopeEvents: 1,
      maxRows: 500000,
      insertBatch: 5000,
    },
    0,
    "sess:1",
  );
  expect(result.partial).toBe(true);
});

async function replaceCanonicalRecords(records: unknown[]) {
  const m: any = await new Response(
    await deps.store.get(objectKey(manifestKey(location))),
  ).json();
  const raw = new TextEncoder().encode(
    records.map((r) => JSON.stringify(r)).join("\n") + "\n",
  );
  const stored = new Uint8Array(zstdCompressSync(raw)),
    sha = await sha256Hex(raw);
  const chunk = {
    ...m.raw.chunks[0],
    bytes: raw.byteLength,
    stored_bytes: stored.byteLength,
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored),
    stored_raw_bytes: raw.byteLength,
    raw_sha256: sha,
    records: records.length,
  };
  m.raw.chunks = [chunk];
  m.raw.raw_bytes = raw.byteLength;
  m.raw.stored_bytes = stored.byteLength;
  m.raw.next_offset = raw.byteLength;
  m.raw.content_sha256 = await computeContentSha256([chunk]);
  await deps.store.put(rawChunkKey(location, 0, 0, sha, "ndjson"), stored, {});
  await deps.store.put(
    objectKey(manifestKey(location)),
    new TextEncoder().encode(JSON.stringify(m)),
    {},
  );
  pointer = { ...pointer, content_sha256: m.raw.content_sha256 };
}
test("replay pages admit a legally large first event and bind view/content bounds into the cursor", async () => {
  const content = "x".repeat(2 * 1024 * 1024 + 1);
  const fields = {
    source: "claude",
    session_id: "sess:1",
    machine_id: "srv_bb",
    user_id: "user_1",
    kind: "tool_call",
    ts: AT,
    content,
    task_ref: { id: "t1" },
    input_tokens: 7,
    output_tokens: 3,
    tool_calls: [{ id: "call1", arguments: "z".repeat(200000) }],
    tool_results: [{ id: "call1", output: "done" }],
    record: { payload: { message_id: "native1", text: "duplicate" } },
  };
  await replaceCanonicalRecords(
    ["e1", "e2"].map((event_id) => ({
      kind: "convergence_event",
      origin_id: "machine:srv_bb",
      record_id: event_id,
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: { ...fields, event_id },
    })),
  );
  const read = (suffix: string) =>
    canonicalHandle(
      request(suffix),
      { CANONICAL_READ_ENABLED: "true" },
      fetch,
      deps,
    );
  const raw = await read("");
  expect(raw.status).toBe(503);
  const first = await read("&view=replay");
  expect(first.status).toBe(200);
  const a: any = await first.json();
  expect(a.rows).toHaveLength(1);
  expect(a.nextCursor).toBeTruthy();
  expect(a.rows[0].row).toMatchObject({
    ...fields,
    ts: "2026-11-01T10:00:00.000000Z",
    record: { payload: { message_id: "native1" } },
  });
  expect(a.rows[0].row.record.payload.text).toBeUndefined();
  const second = await read(
    "&view=replay&cursor=" + encodeURIComponent(a.nextCursor),
  );
  expect(second.status).toBe(200);
  const b: any = await second.json();
  expect(b.rows[0].row.event_id).toBe("e2");
  expect(b.nextCursor).toBeNull();
  const crossView = await read("&cursor=" + encodeURIComponent(a.nextCursor));
  expect(crossView.status).toBe(400);
  const capped = await read("&view=replay&maxContent=10&limit=1");
  expect(capped.status).toBe(200);
  const c: any = await capped.json();
  expect(c.rows[0].row.content).toBe(content.slice(0, 11));
  expect(c.rows[0].row.tool_calls).toEqual(fields.tool_calls);
  const changedCap = await read(
    "&view=replay&maxContent=11&limit=1&cursor=" +
      encodeURIComponent(c.nextCursor),
  );
  expect(changedCap.status).toBe(400);
  const sameCap = await read(
    "&view=replay&maxContent=10&limit=1&cursor=" +
      encodeURIComponent(c.nextCursor),
  );
  expect(sameCap.status).toBe(200);
});

test("provider exposes scoped legacy table pages and explicit coverage without Neon", async () => {
  const scope = {
    org_id: "org_1",
    workspace_id: "ws_1",
    source: "claude",
    session_id: "sess:1",
  };
  await replaceCanonicalRecords([
    {
      kind: "legacy_conversation_turn",
      origin_id: "legacy:conversation_turns",
      record_id: "legacy:conversation_turns:id",
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: {
        ...scope,
        id: "id",
        turn_index: 0,
        role: "assistant",
        session_owner: "owner",
        content: "legacy body",
        actor_name: "Agent",
        actor_role: "owner",
        ts: AT,
      },
    },
    {
      kind: "legacy_scope",
      origin_id: "legacy:scope",
      record_id: "legacy:scope:coverage",
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: {
        ...scope,
        version: 1,
        tables: ["conversation_turns", "session_links", "session_outcomes"],
      },
    },
  ]);
  const response = await canonicalHandle(
    request("&table=conversation_turns"),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  expect(response.status).toBe(200);
  const result: any = await response.json();
  expect(result.legacyCoverage).toEqual({
    version: 1,
    tables: ["conversation_turns", "session_links", "session_outcomes"],
  });
  expect(result.rows).toHaveLength(1);
  expect(result.rows[0]).toMatchObject({
    table: "conversation_turns",
    row: { id: "id", content: "legacy body" },
  });
});

test("immutable publication snapshot uses existing canonical fold and generation-qualified raw namespace",async()=>{
 const gen="12345678-1234-1234-1234-123456789abc",pub="abcdefab-1234-1234-1234-123456789abc";
 const m=JSON.parse(new TextDecoder().decode(await readAll(await deps.store.get(objectKey(pointer.manifest_key))))) as ManifestV1;
 const next={...location,prefix:uploadGenerationPrefix(location.prefix,gen)};
 for(const entry of m.raw.chunks){
  const bytes=await readAll(await deps.store.get(rawChunkKey(location,entry.seq,entry.start_offset,entry.sha256,"ndjson")));
  await deps.store.put(rawChunkKey(next,entry.seq,entry.start_offset,entry.sha256,"ndjson"),bytes,{});
 }
 const key=publicationManifestKey(next,pub);
 await deps.store.put(key,new TextEncoder().encode(JSON.stringify(m)),{});
 pointer.manifest_key=String(key);
 const result=await canonicalSnapshot(deps,identity);
 expect(result.rows.filter(x=>x.table==="convergence_events").map(x=>x.row.content)).toEqual(["authoritative transcript","second transcript"]);
});

test("bounds view reads large events without transcript payload and cross-table generation is pinned", async () => {
  await replaceCanonicalRecords([
    {
      kind: "session_event",
      origin_id: "origin",
      record_id: "r1",
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: {
        ts_ms: Date.parse(AT),
        text: "x".repeat(3 * 1024 * 1024),
        role: "assistant",
        message_id: "native",
      },
    },
  ]);
  const read = (suffix: string) =>
    canonicalHandle(
      request(suffix),
      { CANONICAL_READ_ENABLED: "true" },
      fetch,
      deps,
    );
  const response = await read("&view=bounds");
  expect(response.status).toBe(200);
  const result: any = await response.json();
  expect(result.exists).toBe(true);
  expect(result.rows).toHaveLength(1);
  expect(result.rows[0].row.content).toBeUndefined();
  expect(result.rows[0].row.record).toBeUndefined();
  expect(result.rows[0].row.ts).toBeTruthy();
  expect(JSON.stringify(result).length).toBeLessThan(1500);
  const different = await read("&view=bounds&generation=" + "b".repeat(64));
  expect(different.status).toBe(503);
  expect(different.headers.get("X-Session-Read-Reason")).toBe(
    "source_generation_changed",
  );
  const wrongView = await read("&view=legacy");
  expect(wrongView.status).toBe(400);
  const wrongContent = await read("&view=bounds&maxContent=10");
  expect(wrongContent.status).toBe(400);
});

test("missing pointer sources attest absence without inventing legacy coverage", async () => {
  deps.pointers = async () => [];
  let objects = 0;
  deps.store = new Proxy(deps.store, {
    get(target, key) {
      if (key === "get" || key === "getObject")
        return async () => {
          objects++;
          throw new Error("unexpected object");
        };
      return Reflect.get(target, key);
    },
  });
  const response = await canonicalHandle(
    request("&view=bounds"),
    { CANONICAL_READ_ENABLED: "true" },
    fetch,
    deps,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    exists: false,
    legacyCoverage: null,
    rows: [],
    nextCursor: null,
  });
  expect(objects).toBe(0);
});
import { canonicalProducerSnapshot, CanonicalReadError } from "../canonical.ts";
test("a directory display refusal omits display rows on the live raw read but refuses the producer with a stable code", async () => {
  const record = (cwd: string) => ({
    kind: "session_event",
    origin_id: "origin",
    record_id: "r1",
    revision: "1",
    operation: "upsert",
    received_at: AT,
    payload: { ts_ms: Date.parse(AT), text: "opening prompt", role: "user", message_id: "msg-1", cwd },
  });
  const display = (rows: Array<{ table: string; row: Record<string, unknown> }>) =>
    rows.filter((r) => r.table === "session_directory_event_values");
  await replaceCanonicalRecords([record("/work/project")]);
  const ok = await canonicalSnapshot(deps, identity, { directoryValues: true });
  expect(display(ok.rows).map((r) => r.row.project_id)).toContain("/work/project");
  expect("displayOmitted" in ok).toBe(false);
  // A >1 MiB project path makes the display value refuse (directory_display_bound).
  await replaceCanonicalRecords([record("/" + "x".repeat(1024 * 1024))]);
  const raw: any = await canonicalSnapshot(deps, identity, { directoryValues: true }).catch((e) => ({ rejected: e?.reason, details: e?.details }));
  // Not requested (raw /canonical of any other table, timing, turns): no display projection ran,
  // so the same refusing fixture reports nothing and the snapshot carries no display table.
  const plain: any = await canonicalSnapshot(deps, identity);
  expect("displayOmitted" in plain).toBe(false);
  expect(display(plain.rows)).toEqual([]);
  expect(raw).toMatchObject({ exists: true });
  expect(display(raw.rows)).toEqual([]);
  expect(raw.rows.some((r: { table: string }) => r.table === "convergence_events")).toBe(true);
  expect((raw as { displayOmitted?: string }).displayOmitted).toBe("directory_display_bound");
  const lines: string[] = [], err = console.error;
  console.error = (line: string) => void lines.push(line);
  let response: Response;
  try {
    response = await canonicalHandle(request("&table=session_directory_event_values"), { CANONICAL_READ_ENABLED: "true" }, fetch, deps);
  } finally { console.error = err; }
  expect(response.status).toBe(200);
  expect(((await response.json()) as any).rows).toEqual([]);
  expect(lines.map((l) => JSON.parse(l))).toEqual([expect.objectContaining({ level: "warn", message: "canonical display values omitted", reason: "directory_display_bound", session_id: "sess:1" })]);
  const refused = await canonicalProducerSnapshot(deps, identity).catch((e) => e);
  expect(refused).toBeInstanceOf(CanonicalReadError);
  expect(refused).toMatchObject({ reason: "canonical_projection_incomplete", details: ["directory_display_bound"] });
});
test("display values are lazy: a raw snapshot without the request is the pre-#184 snapshot, byte for byte", async () => {
  const record = (id: string, text: string, cwd?: string) => ({
    kind: "session_event",
    origin_id: "origin",
    record_id: id,
    revision: "1",
    operation: "upsert",
    received_at: AT,
    payload: { ts_ms: Date.parse(AT), text, role: "user", message_id: `msg-${id}`, ...(cwd ? { cwd } : {}) },
  });
  await replaceCanonicalRecords([record("r1", "opening prompt", "/work/project"), record("r2", "follow up")]);
  const plain = await canonicalSnapshot(deps, identity);
  const requested = await canonicalSnapshot(deps, identity, { directoryValues: true });
  // The opt-in read really projects display values for this fixture...
  expect(requested.rows.some((r) => r.table === "session_directory_event_values")).toBe(true);
  // ...and the default read is exactly the opt-in read minus that table: no new table key,
  // no displayOmitted key, same generation, same rows in the same order.
  expect(Object.keys(plain).sort()).toEqual(["exists", "generation", "legacyCoverage", "rows"]);
  expect(plain.rows.some((r) => r.table === "session_directory_event_values")).toBe(false);
  expect(JSON.stringify(plain)).toBe(JSON.stringify({ ...requested, rows: requested.rows.filter((r) => r.table !== "session_directory_event_values") }));
  // Raw route: other tables never carry display rows; the display table is served on request.
  const rows = async (table: string) => ((await (await canonicalHandle(request(`&table=${table}`), { CANONICAL_READ_ENABLED: "true" }, fetch, deps)).json()) as { rows: unknown[] }).rows;
  expect((await rows("convergence_events")).length).toBe(2);
  expect((await rows("session_directory_event_values")).length).toBeGreaterThan(0);
});
