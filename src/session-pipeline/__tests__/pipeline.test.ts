import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  prefixAllowed,
  prefixOrgResolver,
  rawChunkKey,
  type ChunkEntry,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
  type RelayhistoryStreamRecord,
  type SessionLocation,
} from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import type { IndexRow } from "../vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import { memoryLock } from "./support.ts";
import { RECONCILE_DEADLINE_MS, lambdaEnv, reconcileRecent } from "../lambda.ts";

const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const NOW = new Date("2026-11-01T12:00:00.000Z");
const AT = "2026-11-01T10:00:00.000Z";
const PREFIX = "96bulls/org_1";
const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = (machineId: string): SessionLocation => ({
  prefix: PREFIX,
  workspaceId: scope.workspaceId,
  machineId,
  harness: scope.harness,
  sessionId: scope.sessionId,
  startDate: "2026-11-01",
});
const event = (record_id: string, revision: string, ts_ms: number, text: string, overrides: Partial<RelayhistoryStreamRecord> = {}): RelayhistoryStreamRecord => ({
  kind: "session_event",
  origin_id: "desktop-origin",
  record_id,
  revision,
  operation: "upsert",
  received_at: AT,
  payload: { ts_ms, text, role: "assistant" },
  ...overrides,
});

let root: string;
let store: ObjectStore;
let reader: ManifestReader;

beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-session-pipeline-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

/** Write one manifest (one chunk) for `machine` and return its key. */
async function writeSession(
  machine: string,
  records: RelayhistoryStreamRecord[],
  revision = 1,
  opts: { scrubbed?: boolean; extensions?: Record<string, unknown>; declaredRecords?:number|null; physicalSessionId?:string } = {},
) {
  const location = {...loc(machine),...(opts.physicalSessionId?{sessionId:opts.physicalSessionId}:{})};
  const source = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
  const stored = new Uint8Array(zstdCompressSync(source));
  const sha = await sha256Hex(source);
  const chunk: ChunkEntry = {
    seq: 0,
    epoch: 0,
    start_offset: 0,
    bytes: source.byteLength,
    stored_bytes: stored.byteLength,
    sha256: sha,
    sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored),
    stored_raw_bytes: source.byteLength,
    raw_sha256: sha,
    compression: "zstd",
    records: opts.declaredRecords===null?undefined:opts.declaredRecords??records.length,
  };
  await store.put(rawChunkKey(location, chunk.seq, 0, sha, "ndjson"), stored, {});
  const manifest: ManifestV1 = {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision,
    created_at: AT,
    updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: machine, harness: scope.harness, session_id: location.sessionId, started_at: AT },
    writer: { delivery_instance_id: machine, plugin_version: "test" },
    raw: {
      owner: machine,
      state: "open",
      updated_at: AT,
      format: "ndjson",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: chunk.bytes,
      stored_bytes: chunk.stored_bytes,
      content_sha256: await computeContentSha256([chunk]),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: machine }],
      next_offset: source.byteLength,
      chunks: [chunk],
      ...(opts.scrubbed === false ? {} : { scrub: { ruleset: "relayhistory-scrub", version: 1 } }),
    },
    extensions: {
      "96bulls": { user_id: "user_1" },
      ...(opts.extensions ?? {}),
    },
  } as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return key;
}

/** An in-memory transcript_events table speaking the four statements the indexer sends. */
function fakeClickHouse() {
  const rows: IndexRow[] = [];
  const tsMs = (r: IndexRow) => Date.parse(`${r.ts.replace(" ", "T")}Z`);
  const identity = (r: IndexRow) => [r.org_id, r.workspace_id, tsMs(r), r.session_id, r.source, r.machine_id, r.kind, r.event_id].join("|");
  const final = () => {
    const latest = new Map<string, IndexRow>();
    for (const r of rows) {
      const held = latest.get(identity(r));
      if (!held || r.version >= held.version) latest.set(identity(r), r);
    }
    return [...latest.values()];
  };
  let failInsert = false;
  const commands: string[] = [];
  const state: { session_id: string; needs_full: number }[] = [];
  const sha = (v: string) => createHash("sha256").update(v).digest("hex");
  const ch: ClickHouseClient = {
    database: "test",
    async command(query, options?: { params?: Record<string, unknown> }) {
      commands.push(query);
      if (!query.startsWith("DELETE FROM")) throw new Error(`unexpected command ${query.slice(0, 30)}`);
      if (query.includes("{org:String}")) {
        const p = options!.params!;
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === p.org && r.workspace_id === p.workspace && r.source === p.source && (r.session_id === p.session || r.session_id === p.canonical)) rows.splice(i, 1);
        }
        return;
      }
      const tuples = [...query.matchAll(/\(('(?:[^'\\]|\\.)*'(?:,'(?:[^'\\]|\\.)*')*),(\d+)\)/g)].map((m) =>
        [...m[1]!.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((s) => s[1]!).concat(m[2]!),
      );
      for (const [org, session, ws, machine, source, kind, eventId, ms] of tuples) {
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind && sha(r.event_id) === eventId && tsMs(r) === Number(ms))
            rows.splice(i, 1);
        }
      }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_state")) return [] as T[];
      const p = options!.params!;
      return final()
        .filter((r) => r.org_id === p.org && (r.session_id === p.session || r.session_id === p.canonical) && r.workspace_id === p.workspace && r.source === p.source && tsMs(r) >= Number(p.from))
        .slice(0, Number(p.limit))
        .map((r) => ({ workspace_id: r.workspace_id, session_id:r.session_id, machine_id: r.machine_id, source: r.source, kind: r.kind, event_id: r.event_id, ts_ms: tsMs(r), fingerprint: r.fingerprint })) as T[];
    },
    async insert(table, batch) {
      if (table === SESSION_STATE_TABLE) {
        state.push(...(batch as { session_id: string; needs_full: number }[]));
        return;
      }
      if (failInsert) throw Object.assign(new Error("boom"), { code: "241" });
      expect(table).toBe(EVENTS_TABLE);
      rows.push(...(batch as IndexRow[]));
    },
  };
  return { ch, rows, final, commands, state, fail: (on: boolean) => (failInsert = on) };
}

/** Supabase as the indexer sees it: the live pointer keys of the scope, plus a call log. */
function fakeSupabase(live: () => string[], busy: () => boolean = () => false, canonicalId:()=>string=()=>scope.sessionId) {
  const calls: string[] = [];
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      calls.push(text.trim().replace(/\s+/g, " "));
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: !busy() }] as R[] };
      if (text.includes("event_projection_sources")) return { rows: [{ written: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: live().map((manifest_key) => ({ manifest_key, canonical_id:canonicalId() })) as R[] };
      if(text.includes("AS canonical_id FROM sessions.session_index"))return {rows:[{canonical_id:canonicalId()}] as R[]};
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { tx, calls, transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

function searchDeps(ch: ClickHouseClient, transact: SearchIndexDeps["transact"], limits?: SearchIndexDeps["limits"]): SearchIndexDeps {
  return { transact, reader, store, ch, prefixFor: (org) => `96bulls/${org}`, now: () => NOW, ...(limits ? { limits } : {}) };
}

describe("search index from S3", () => {
  test("direct canonical machine identity survives physical key encoding",async()=>{
    const machine="machine:1";
    const key=await writeSession("machine_1",[event("e1","1",Date.parse(AT),"direct",{kind:"convergence_event",origin_id:"machine:"+machine,payload:{source:scope.harness,session_id:scope.sessionId,machine_id:machine,user_id:"user_1",event_id:"e1",kind:"prompt",ts:AT,content:"direct"}})],1,{extensions:{"96bulls":{user_id:"user_1",canonical_machine_id:machine}}});
    const sb=fakeSupabase(()=>[key]),ch=fakeClickHouse();
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).toMatchObject({complete:true,inserted:1});
    expect(ch.final()[0]?.machine_id).toBe(machine);
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).toMatchObject({complete:true,inserted:0,deleted:0});
  });
  test.each([null,0])("actual record counts enforce the scope bound when declared records is %s",async declaredRecords=>{
    const key=await writeSession("srv_bb",[event("r1","1",Date.parse(AT),"one"),event("r2","1",Date.parse(AT),"two")],1,{declaredRecords});
    const sb=fakeSupabase(()=>[key]),ch=fakeClickHouse();
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact,{maxScopeEvents:1}))).toMatchObject({complete:false,partial:true,reasons:["scope_too_large"],inserted:0});
    expect(ch.final()).toHaveLength(0);
  });

  test("migrates encoded physical delivery IDs to public canonical IDs and reconciles/erases both exactly", async () => {
    const ch=fakeClickHouse();
    let keys=[await writeSession("srv_bb",[event("r1","1",Date.parse(AT),"canonical transcript")])];
    let canonicalId=scope.sessionId;
    const sb=fakeSupabase(()=>keys,()=>false,()=>canonicalId);
    await indexSearchScope(scope,searchDeps(ch.ch,sb.transact));
    expect(ch.final().map(r=>r.session_id)).toEqual([scope.sessionId]);
    canonicalId="sess:1"; // layoutComponent("sess:1") === "sess_1"
    keys=[await writeSession("srv_bb",[event("r1","1",Date.parse(AT),"canonical transcript")],2,{extensions:{"96bulls":{user_id:"user_1",canonical_session_id:canonicalId}}})];
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).toMatchObject({inserted:1,deleted:1,complete:true});
    expect(ch.final().filter(r=>r.session_id===canonicalId).map(r=>r.content)).toEqual(["canonical transcript"]);
    expect(ch.final().filter(r=>r.session_id===scope.sessionId)).toHaveLength(0);
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).toMatchObject({inserted:0,deleted:0,complete:true});
    // A stale physical copy must be removed even when its canonical copy is current.
    ch.rows.push({...ch.final()[0]!,session_id:scope.sessionId});
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).toMatchObject({inserted:0,deleted:1,complete:true});
    expect(ch.final()).toHaveLength(1);
    keys=[];
    expect(await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).toMatchObject({complete:true,reasons:["erased"]});
    expect(ch.final()).toHaveLength(0);
    expect(ch.state.filter(r=>r.session_id===canonicalId).at(-1)?.needs_full).toBe(0);
    expect(ch.state.filter(r=>r.session_id===scope.sessionId).at(-1)?.needs_full).toBe(0);
  });

  test("writes complete canonical event metadata once across duplicate manifest copies", async () => {
    const a = await writeSession("export", [
      event("r1", "1", Date.parse(AT), "text"),
    ]);
    const b = await writeSession("live", [
      event("r1", "1", Date.parse(AT), "text"),
    ]);
    const sb = fakeSupabase(() => [a, b]);
    const ch = fakeClickHouse();
    const writes: unknown[] = [];
    const deps = {
      ...searchDeps(ch.ch, sb.transact),
      writeEvents: async (_tx: unknown, _key: unknown, input: unknown) => {
        writes.push(input);
        return { written: true };
      },
    };
    expect(await indexSearchScope(scope, deps)).toMatchObject({
      complete: true,
    });
    expect(writes).toHaveLength(1);
    expect(writes[0]).toMatchObject({
      events: {
        eventCount: "1",
        kinds: ["session_event"],
        reasoningTokens: "0",
      },
      sources: [
        { manifestKey: a, revision: "1" },
        { manifestKey: b, revision: "1" },
      ],
    });
    await store.delete(objectKey(b));
    writes.length = 0;
    expect(await indexSearchScope(scope, deps)).toMatchObject({
      complete: false,
    });
    expect(writes).toHaveLength(0);
  });
  test("conflicting pointer aliases never bypass admitted canonical manifest identity", async () => {
    const manifest = await writeSession("srv_bb", [event("r1","1",Date.parse(AT),"text")]);
    const sb=fakeSupabase(()=>[manifest],undefined,()=>"unrelated-canonical");const ch=fakeClickHouse();
    const writes:unknown[]=[];
    const deps={...searchDeps(ch.ch,sb.transact),writeEvents:async(_db:unknown,_key:unknown,input:unknown)=>{writes.push(input);return {written:true};}};
    expect(await indexSearchScope(scope,deps)).toMatchObject({complete:false});expect(writes).toHaveLength(0);
  });
  test("writes project/repository catalog only after a complete canonical scope projection", async () => {
    const sessionRecord: RelayhistoryStreamRecord = {
      kind: "session",
      origin_id: "desktop-origin",
      record_id: "session-1",
      revision: "1",
      operation: "upsert",
      received_at: AT,
      payload: {
        project_key: "github.com/acme/widgets",
        project_key_method: "git_remote",
        repo_url: "git@github.com:acme/widgets.git",
        git_branch: "main",
        agent_name: "codex",
        models_json: ["gpt-5"],
      },
    };
    let keys = [
      await writeSession("srv_bb", [sessionRecord], 1, {
        extensions: {
          "96bulls": {
            user_id: "user_1",
            canonical_session_id: "sess:1",
          },
        },
      }),
    ];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys,undefined,()=>"sess:1");
    const writes: Array<{
      key: Parameters<NonNullable<SearchIndexDeps["writeCatalog"]>>[1];
      catalog: Parameters<NonNullable<SearchIndexDeps["writeCatalog"]>>[2];
    }> = [];
    const deps: SearchIndexDeps = {
      ...searchDeps(ch.ch, sb.transact),
      writeCatalog: async (_db, key, catalog) => {
        writes.push({ key, catalog });
        return { written: true };
      },
    };
    expect(await indexSearchScope(scope, deps)).toMatchObject({ complete: true });
    expect(writes).toHaveLength(1);
    expect(writes[0]).toMatchObject({
      key: { sessionId: "sess:1" },
      catalog: {
        projectKey: "github.com/acme/widgets",
        projectId: "acme/widgets",
        gitRepo: "git@github.com:acme/widgets.git",
        gitBranch: "main",
        agentNames: ["codex"],
      },
    });

    keys = [...keys, manifestKey(loc("srv_missing"))];
    expect(await indexSearchScope(scope, deps)).toMatchObject({ complete: false });
    expect(writes).toHaveLength(1);
  });

  test("two manifests carrying the same delivery records index each event once, by its Neon identity", async () => {
    const records = [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "first"), event("r2", "1", Date.parse("2026-11-01T10:00:02Z"), "second")];
    const exporter = await writeSession("delivery_aa", records);
    const live = await writeSession("srv_bb", [...records, event("r3", "1", Date.parse("2026-11-01T10:00:03Z"), "third")]);
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => [exporter, live]);

    const first = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(first).toMatchObject({ manifests: 2, rows: 3, inserted: 3, deleted: 0, complete: true });
    const machines = new Set(ch.final().map((r) => r.machine_id));
    expect(machines.size).toBe(1);
    expect([...machines][0]).toMatch(/^delivery:[0-9a-f]{32}$/);
    expect(ch.final().map((r) => r.content).sort()).toEqual(["first", "second", "third"]);
    expect(ch.final().every((r) => r.org_id === "org_1" && r.user_id === "user_1" && r.fingerprint.startsWith("s3"))).toBe(true);
    // The pointer listing ran under the scope's advisory lock.
    expect(sb.calls.some((c) => c.startsWith("SELECT pg_try_advisory_xact_lock"))).toBe(true);

    // Replay: nothing changes.
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ inserted: 0, deleted: 0, complete: true });
  });

  test("rows of kinds S3 never carries survive a complete pass; erasure still removes them", async () => {
    const ch = fakeClickHouse();
    let keys = [await writeSession("srv_bb", [event("r1", "1", Date.parse(AT), "one")])];
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    // A row written by another ingest path (Neon tail) in a kind the exported stream never carries.
    const base = ch.final()[0]!;
    ch.rows.push({ ...base, kind: "tool_result", event_id: "tail:tr1", fingerprint: "neon-x" });
    // A stale activity-kind row S3 no longer has is still removed.
    ch.rows.push({ ...base, event_id: "tail:stale", fingerprint: "neon-y" });
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: true, deleted: 1 });
    expect(ch.final().some((r) => r.kind === "tool_result" && r.event_id === "tail:tr1")).toBe(true);
    expect(ch.final().some((r) => r.event_id === "tail:stale")).toBe(false);
    keys = [];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(ch.final()).toHaveLength(0);
  });

  test("a retracted record, an edit and an erasure propagate; an incomplete read never deletes", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(ch.final()).toHaveLength(2);

    // Revision 2: r1 edited, r2 deleted at the source.
    keys = [
      await writeSession(
        "srv_bb",
        [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two"), event("r1", "2", t(1), "one, edited"), event("r2", "2", t(2), "two", { operation: "delete", payload: null })],
        2,
      ),
    ];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ inserted: 1, deleted: 1, complete: true });
    expect(ch.final().map((r) => r.content)).toEqual(["one, edited"]);

    // A live pointer whose manifest is gone (its delete is in flight): no deletes.
    const ghost = manifestKey(loc("srv_gone"));
    keys = [...keys, ghost];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: false, deleted: 0, reasons: ["manifest_missing"] });
    expect(ch.final()).toHaveLength(1);

    // Erasure: no live pointer left, every row of the scope goes.
    keys = [];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(ch.final()).toHaveLength(0);
  });

  test("an erasure removes every row of the scope however large it is, without listing (review M2)", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two"), event("r3", "1", t(3), "three")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    // An old row outside the index window, as the Neon backfill may have left.
    ch.rows.push({ ...ch.final()[0]!, event_id: "old", ts: "2026-08-01 00:00:00.000" });
    // Another session's rows are untouched.
    ch.rows.push({ ...ch.final()[0]!, session_id: "other" });
    keys = [];
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact, { maxRows: 1 }));
    expect(result).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(ch.final().map((r) => r.session_id)).toEqual(["other"]);
  });

  test("an unscrubbed manifest is an incomplete read: rows from an earlier revision stay (review M1)", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")], 2, { scrubbed: false })];
    expect(await indexSearchScope(scope, searchDeps(ch.ch, sb.transact))).toMatchObject({ complete: false, deleted: 0, reasons: ["manifest_unscrubbed"] });
    expect(ch.final()).toHaveLength(1);
  });

  test("a manifest whose body names another session is not read and deletes nothing (review L1)", async () => {
    const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    // Overwrite the manifest at srv_bb's key with a body naming machine srv_other.
    const key = keys[0]!;
    const body = JSON.parse(new TextDecoder().decode(await new Response((await store.getObject(objectKey(key))).body).arrayBuffer()));
    body.session.machine_id = "srv_other";
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(body)), { contentType: "application/json" });
    const result = await indexSearchScope(scope, { ...searchDeps(ch.ch, sb.transact), reader: { ...reader, read: async (k) => ({ manifest: JSON.parse(new TextDecoder().decode(await new Response((await store.getObject(objectKey(k))).body).arrayBuffer())) }) } });
    expect(result).toMatchObject({ complete: false, deleted: 0, inserted: 0, reasons: ["manifest_key_mismatch"] });
    expect(ch.final()).toHaveLength(1);
  });

  test("unscrubbed raw is never read or indexed; events outside the window are skipped", async () => {
    const quarantined = await writeSession("srv_q", [event("q1", "1", Date.parse("2026-11-01T10:00:01Z"), "secret-ish")], 1, { scrubbed: false });
    const old = await writeSession("delivery_old", [event("o1", "1", Date.parse("2026-09-01T10:00:00Z"), "too old")]);
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => [quarantined, old]);
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(result).toMatchObject({ inserted: 0, reasons: ["manifest_unscrubbed"] });
    expect(ch.final()).toHaveLength(0);
  });
});

describe("search index: a bad read never deletes (review #2 L-a)", () => {
  const t = (s: number) => Date.parse(`2026-11-01T10:00:0${s}Z`);
  /** Index r1 + r2, then rewrite the session so that r2 is retracted; `spoil` makes that read bad. */
  async function retractedAfter(spoil: (key: string) => Promise<void>, limits?: SearchIndexDeps["limits"]) {
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(ch.final()).toHaveLength(2);
    keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")], 2)];
    await spoil(keys[0]!);
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact, limits));
    return { result, ch };
  }
  const chunkKeyOf = async (key: string) => {
    const listed = await store.list(objectKey(key.replace(/\/manifest\.json$/, "/raw")));
    return listed.items.map((i) => i.key)[listed.items.length - 1]!;
  };

  test("control: a good read deletes the retracted row", async () => {
    const { result, ch } = await retractedAfter(async () => {});
    expect(result).toMatchObject({ complete: true, deleted: 1 });
    expect(ch.final()).toHaveLength(1);
    expect(ch.state.at(-1)).toMatchObject({ needs_full: 0 });
  });

  test("an unreadable manifest (a chunk with the wrong digest)", async () => {
    const { result, ch } = await retractedAfter(async (key) => {
      await store.put(await chunkKeyOf(key), new Uint8Array(zstdCompressSync(new TextEncoder().encode("tampered\n"))), {});
    });
    expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["chunk_digest_mismatch"] });
    expect(ch.final()).toHaveLength(2);
    expect(ch.state.at(-1)).toMatchObject({ needs_full: 1 });
  });

  test("a projection error (a convergence_event line that is not this session's)", async () => {
    const { result, ch } = await retractedAfter(async () => {
      await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("x", "1", t(3), "x", { kind: "convergence_event", origin_id: "machine:elsewhere", payload: { source: "claude", session_id: "other", machine_id: "elsewhere", user_id: "u", event_id: "x", ts: AT } })], 2);
    });
    expect(result).toMatchObject({ complete: false, deleted: 0 });
    expect(result.reasons).toContain("relayhistory_event_identity");
    expect(ch.final()).toHaveLength(2);
  });

  test("a fold error (two copies of one revision that differ)", async () => {
    let keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one"), event("r2", "1", t(2), "two")])];
    const ch = fakeClickHouse();
    const sb = fakeSupabase(() => keys);
    await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    keys = [await writeSession("srv_bb", [event("r1", "1", t(1), "one")], 2), await writeSession("delivery_aa", [event("r1", "1", t(1), "one, but different")])];
    const result = await indexSearchScope(scope, searchDeps(ch.ch, sb.transact));
    expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["relayhistory_revision_conflict"] });
    expect(ch.final().map((r) => r.event_id)).toContain(ch.final().find((r) => r.content === "two")!.event_id);
  });

  test("a truncated ClickHouse read of a live session", async () => {
    const { result, ch } = await retractedAfter(async () => {}, { maxRows: 1 });
    expect(result).toMatchObject({ complete: false, deleted: 0, reasons: ["clickhouse_truncated"] });
    expect(ch.final()).toHaveLength(2);
  });

  test("a session over the size bounds is marked partial: nothing read, written or deleted (new requirement)", async () => {
    for (const limits of [{ maxScopeBytes: 64 }, { maxScopeEvents: 0 }]) {
      const { result, ch } = await retractedAfter(async () => {}, limits);
      expect(result).toMatchObject({ partial: true, complete: false, inserted: 0, deleted: 0, reasons: ["scope_too_large"] });
      expect(ch.final()).toHaveLength(2);
      expect(ch.state.at(-1)).toMatchObject({ session_id: "sess_1", needs_full: 1 });
    }
  });
});

describe("search index: a busy session is requeued, not waited on (review #2 N2)", () => {
  test("a pass that finds the session's search lock held does nothing and re-enqueues its manifest", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const ch = fakeClickHouse();
    let busy = true;
    const sb = fakeSupabase(() => [key], () => busy);
    const requeued: string[] = [];
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const deps: ConsumerDeps = {
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      search: searchDeps(ch.ch, sb.transact),
      requeue: async (k) => void requeued.push(k),
    };
    const put = JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: "01" } } }] });
    expect((await processBatch([{ messageId: "m", body: put }], deps)).batchItemFailures).toEqual([]);
    expect(requeued).toEqual([key]);
    expect(ch.final()).toHaveLength(0);
    busy = false;
    await processBatch([{ messageId: "m2", body: put }], deps);
    expect(ch.final()).toHaveLength(1);
  });
});

describe("SQS consumer", () => {
  const s3Put = (key: string, sequencer: string) =>
    JSON.stringify({
      Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key: encodeURIComponent(key).replace(/%2F/g, "/"), eTag: "e", sequencer } } }],
    });

  function consumer(ch: ClickHouseClient, live: () => string[]) {
    const sb = fakeSupabase(live);
    const db = { ...sb.tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
    const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
    const deps: ConsumerDeps = {
      pointer: { db, lock: memoryLock(), reader, exists: async (k) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver, allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org), fanout: droppedFanout },
      search: searchDeps(ch, sb.transact),
    };
    return { deps, sb };
  }

  // SEARCH_INDEX_ENABLED=1 and SESSION_DESKTOP_CATALOG_ENABLED as lambda.ts wires them into the search deps.
  // Every statement the search stage sends is captured raw (SQL text and parameters).
  function searchOnConsumer(ch: ClickHouseClient, live: () => string[], desktopCatalogEnv: string | undefined) {
    const { deps, sb } = consumer(ch, live);
    const env = lambdaEnv({
      AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
      SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs", SEARCH_INDEX_ENABLED: "1",
      ...(desktopCatalogEnv === undefined ? {} : { SESSION_DESKTOP_CATALOG_ENABLED: desktopCatalogEnv }),
    });
    expect(env.searchEnabled).toBe(true);
    const raw: { text: string; params: unknown[] }[] = [];
    const transact: SearchIndexDeps["transact"] = (work) =>
      sb.transact((tx) => work({ query: (text: string, params?: unknown[]) => (raw.push({ text, params: params ?? [] }), tx.query(text, params)) } as SqlExecutor));
    deps.search = { ...deps.search!, transact, ...(env.desktopCatalogEnabled ? { desktopCatalog: true as const } : {}) };
    return { deps, sb, raw };
  }

  /** The columns a catalog statement writes: the directory UPDATE's SET targets and each companion INSERT's column list. */
  function catalogWrite(raw: { text: string; params: unknown[] }[]) {
    const writes = raw.filter((q) => q.text.includes("INSERT INTO sessions.session_directory_agents"));
    expect(writes).toHaveLength(1);
    const { text, params } = writes[0]!;
    const sql = text.replace(/--[^\n]*/g, "");
    const set = /UPDATE sessions\.session_directory AS d\s+SET([\s\S]*?)\s+FROM live AS l/.exec(sql)![1]!;
    const targets: string[] = [];
    let depth = 0, part = "";
    for (const c of `${set},`) {
      if (c === "(") depth++;
      if (c === ")") depth--;
      if (c === "," && depth === 0) { targets.push(part.split("=")[0]!.trim()); part = ""; } else part += c;
    }
    const insert = (table: string) => new RegExp(`INSERT INTO sessions\\.${table}\\s*\\(([^)]*)\\)`).exec(sql)![1]!.split(",").map((s) => s.trim());
    return { sql, params, directory: targets, agents: insert("session_directory_agents"), relationships: insert("session_directory_relationships") };
  }

  // Pinned from 0c12036's src/hoopdb/session-directory-catalog.ts (unchanged until a49219d^1, the main before #182).
  const PRE_182_DIRECTORY_COLUMNS = ["agent_names", "catalog_models", "project_key", "project_key_method", "initial_commit", "originator", "agent_version", "project_id", "git_repo", "git_branch", "agent_name", "agent_name_normalized", "search_text", "catalog_projected_at"];
  const PRE_182_AGENT_COLUMNS = ["org_id", "workspace_id", "harness", "session_id", "agent_name", "agent_name_normalized", "last_activity_at"];
  const PRE_182_RELATIONSHIP_COLUMNS = ["org_id", "workspace_id", "harness", "parent_session_id", "relationship_uid", "child_session_id"];
  // #182's writer adds only migration 20261105022000 columns.
  const MIGRATION_022000_DIRECTORY_COLUMNS = ["catalog_read_projected_at", "catalog_read_version", "catalog_read_epoch", "catalog_models_complete", "catalog_relationships_complete"];
  const MIGRATION_022000_RELATIONSHIP_COLUMNS = ["child_model", "child_agent_name", "child_agent_type"];

  const catalogSession = (): RelayhistoryStreamRecord[] => [
    event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello"),
    { kind: "session", origin_id: "desktop-origin", record_id: "session-1", revision: "1", operation: "upsert", received_at: AT,
      payload: { project_key: "github.com/acme/widgets", project_key_method: "git_remote", repo_url: "git@github.com:acme/widgets.git", git_branch: "main", agent_name: "codex", models_json: ["gpt-5"] } },
  ];

  test("desktop catalog off: SEARCH_INDEX_ENABLED=1 indexes and runs the pre-#182 catalog writer, column for column", async () => {
    const key = await writeSession("srv_bb", catalogSession());
    for (const flag of [undefined, "0", "true", " 1"]) {
      const ch = fakeClickHouse();
      const { deps, raw } = searchOnConsumer(ch.ch, () => [key], flag);
      expect("desktopCatalog" in deps.search!).toBe(false);
      expect((await processBatch([{ messageId: "m1", body: s3Put(key, "01") }], deps)).batchItemFailures).toEqual([]);
      expect(ch.final().map((r) => r.content)).toEqual(["hello"]);
      expect(ch.state.at(-1)?.needs_full).toBe(0);
      const w = catalogWrite(raw);
      expect(w.directory).toEqual(PRE_182_DIRECTORY_COLUMNS);
      expect(w.agents).toEqual(PRE_182_AGENT_COLUMNS);
      expect(w.relationships).toEqual(PRE_182_RELATIONSHIP_COLUMNS);
      expect(w.sql).toContain("DO UPDATE SET child_session_id = excluded.child_session_id\n");
      for (const column of [...MIGRATION_022000_DIRECTORY_COLUMNS, ...MIGRATION_022000_RELATIONSHIP_COLUMNS, "event_projection_epoch"]) expect(w.sql).not.toContain(column);
      // The pre-#182 parameters: key, agents, models, project key/method, commit, originator, version, id, repo, branch, normalized agents, edge uids, edge children.
      expect(w.params).toEqual(["org_1", "ws_1", "claude", "sess_1", ["codex"], ["gpt-5"], "github.com/acme/widgets", "git_remote", null, null, null, "acme/widgets", "git@github.com:acme/widgets.git", "main", ["codex"], [], []]);
    }
  });

  test("desktop catalog on: SESSION_DESKTOP_CATALOG_ENABLED=1 runs the #182 writer with the 20261105022000 columns", async () => {
    const key = await writeSession("srv_bb", catalogSession());
    const ch = fakeClickHouse();
    const { deps, raw } = searchOnConsumer(ch.ch, () => [key], "1");
    expect(deps.search!.desktopCatalog).toBe(true);
    expect((await processBatch([{ messageId: "m1", body: s3Put(key, "01") }], deps)).batchItemFailures).toEqual([]);
    expect(ch.final().map((r) => r.content)).toEqual(["hello"]);
    const w = catalogWrite(raw);
    expect(w.directory).toEqual([...PRE_182_DIRECTORY_COLUMNS, ...MIGRATION_022000_DIRECTORY_COLUMNS]);
    expect(w.agents).toEqual(PRE_182_AGENT_COLUMNS);
    expect(w.relationships).toEqual([...PRE_182_RELATIONSHIP_COLUMNS, ...MIGRATION_022000_RELATIONSHIP_COLUMNS]);
    expect(w.params).toHaveLength(23);
    expect(w.params.slice(0, 17)).toEqual(["org_1", "ws_1", "claude", "sess_1", ["codex"], ["gpt-5"], "github.com/acme/widgets", "git_remote", null, null, null, "acme/widgets", "git@github.com:acme/widgets.git", "main", ["codex"], [], []]);
    expect(w.params.slice(17)).toEqual([[], [], [], 1, true, true]);
  });

  test("a manifest put upserts the pointer, then indexes its session; duplicates in a batch collapse", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const ch = fakeClickHouse();
    const { deps, sb } = consumer(ch.ch, () => [key]);
    const out = await processBatch(
      [
        { messageId: "m1", body: s3Put(key, "01") },
        { messageId: "m2", body: s3Put(key, "02") },
        { messageId: "m3", body: JSON.stringify({ Event: "s3:TestEvent" }) },
        { messageId: "m4", body: s3Put(`${PREFIX}/v1/ws=ws_1/m=srv_bb/h=claude/d=2026/11/01/s=sess_1/raw/0000000000-0-aaaaaaaa.ndjson.zst`, "03") },
      ],
      deps,
    );
    expect(out.batchItemFailures).toEqual([]);
    expect(sb.calls.filter((c) => c.includes("upsert_session_index"))).toHaveLength(1);
    expect(ch.final().map((r) => r.content)).toEqual(["hello"]);
  });

  test("a delete is checked against S3: an old version deleted keeps the pointer, a gone manifest tombstones it", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const s3Delete = (k: string) =>
      JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectRemoved:Delete", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key: k, versionId: "old", sequencer: "05" } } }] });
    let { deps, sb } = consumer(fakeClickHouse().ch, () => [key]);
    expect((await processBatch([{ messageId: "d1", body: s3Delete(key) }], deps)).batchItemFailures).toEqual([]);
    expect(sb.calls.some((c) => c.includes("upsert_session_index"))).toBe(true);
    expect(sb.calls.some((c) => c.includes("SET deleted_at"))).toBe(false);

    const gone = manifestKey(loc("srv_gone"));
    ({ deps, sb } = consumer(fakeClickHouse().ch, () => []));
    expect((await processBatch([{ messageId: "d2", body: s3Delete(gone) }], deps)).batchItemFailures).toEqual([]);
    expect(sb.calls.some((c) => c.includes("SET deleted_at"))).toBe(true);
    expect(sb.calls.some((c) => c.includes("upsert_session_index"))).toBe(false);
  });

  test("a key outside the deployment prefix is ignored; a failed index fails only its messages", async () => {
    const key = await writeSession("srv_bb", [event("r1", "1", Date.parse("2026-11-01T10:00:01Z"), "hello")]);
    const ch = fakeClickHouse();
    const { deps } = consumer(ch.ch, () => [key]);
    ch.fail(true);
    const out = await processBatch(
      [
        { messageId: "ok", body: s3Put("other/org_1/v1/ws=ws_1/m=x/h=claude/d=2026/11/01/s=s/manifest.json", "01") },
        { messageId: "bad", body: s3Put(key, "02") },
        { messageId: "junk", body: "{" },
      ],
      deps,
    );
    expect(out.batchItemFailures.map((f) => f.itemIdentifier).sort()).toEqual(["bad", "junk"]);
  });
});

describe("reconcile", () => {
  test("enqueues only recent manifests whose pointer is missing or stale", async () => {
    const indexed = await writeSession("srv_ok", [event("r1", "1", 1, "x")]);
    const missing = await writeSession("srv_new", [event("r2", "1", 1, "y")]);
    const listed = await store.list(objectKey(PREFIX));
    const etagOf = (k: string) => listed.items.find((i) => String(i.key) === k)!.version.eTag!.replace(/^"|"$/g, "");
    const sent: string[] = [];
    const db: SqlExecutor = {
      async query<R>(text: string) {
        if (text.includes("indexed_at >=")) return { rows: [] as R[] };
        return { rows: [{ manifest_key: indexed, manifest_etag: etagOf(indexed), deleted: false }] as R[] };
      },
    };
    const summary = await reconcileRecent({ store, db, keyPrefix: "96bulls", since: new Date(Date.now() - 60 * 60_000), send: async (b) => void sent.push(b) });
    expect(summary).toMatchObject({ listed: 2, recent: 2, enqueued: 1, complete: true });
    expect(JSON.parse(sent[0]!).reconcile.key).toBe(missing);
  });

  test("a live pointer whose manifest is gone is enqueued, so a lost or raced delete is repaired (review H1)", async () => {
    const live = await writeSession("srv_ok", [event("r1", "1", 1, "x")]);
    const gone = manifestKey(loc("srv_gone"));
    const sent: string[] = [];
    const db: SqlExecutor = {
      async query<R>(text: string) {
        if (text.includes("indexed_at >=")) return { rows: [{ manifest_key: live }, { manifest_key: gone }] as R[] };
        return { rows: [{ manifest_key: live, manifest_etag: "whatever", deleted: false }] as R[] };
      },
    };
    const summary = await reconcileRecent({ store, db, keyPrefix: "96bulls", since: new Date(0), send: async (b) => void sent.push(b) });
    expect(summary).toMatchObject({ pointersChecked: 2, pointersGone: 1 });
    expect(sent.map((b) => JSON.parse(b).reconcile.key)).toContain(gone);
  });

  test("stops at its deadline, under the function timeout, and returns the cursor for the next link (review M5)", async () => {
    for (const m of ["a", "b", "c"]) await writeSession(`srv_${m}`, [event("r", "1", 1, m)]);
    let clock = 0;
    const pages: (string | undefined)[] = [];
    const paged = {
      head: store.head.bind(store),
      async list(prefix: Parameters<ObjectStore["list"]>[0], cursor?: string) {
        pages.push(cursor);
        clock += 200_000; // each page "takes" 200 s
        const all = await store.list(prefix);
        const at = cursor ? Number(cursor) : 0;
        return { items: all.items.slice(at, at + 1), ...(at + 1 < all.items.length ? { nextCursor: String(at + 1) } : {}) };
      },
    };
    const db: SqlExecutor = { async query<R>() { return { rows: [] as R[] }; } };
    const first = await reconcileRecent({ store: paged, db, keyPrefix: "96bulls", since: new Date(0), send: async () => {}, now: () => clock });
    expect(RECONCILE_DEADLINE_MS).toBeLessThan(300_000);
    expect(first.complete).toBe(false);
    expect(first.nextCursor).toBe("2");
    const second = await reconcileRecent({ store: paged, db, keyPrefix: "96bulls", since: new Date(0), cursor: first.nextCursor, send: async () => {}, now: () => clock });
    expect(pages).toEqual([undefined, "1", "2", "3", "4", "5"].slice(0, pages.length));
    expect(second.pages).toBeGreaterThan(0);
  });
});

const canonical='sess:1';
function aliasDb(liveA:()=>string[],siblings:()=>string[]){
 const base=fakeSupabase(liveA,()=>false,()=>canonical);
 const tx={...base.tx,async query(text:string,params?:unknown[]){
  if(text.includes('FROM sessions.session_index')&&text.includes('deleted_at IS NULL')){
   const selected=text.includes('p.session_id=$4')?liveA():siblings();
   return{rows:selected.map(manifest_key=>({manifest_key,canonical_id:canonical,event_epoch:'0'}))};
  }
  return base.tx.query(text,params);
 }};
 return{tx,transact:async(work:any)=>work(tx)};
}
test('independent: valid same-physical two-machine group survives one machine removal',async()=>{
 const a=await writeSession('machine_a',[event('ra','1',Date.parse(AT),'source A',{origin_id:'origin-a'})],1,{extensions:{'96bulls':{user_id:'user_1',canonical_session_id:canonical}}});
 const b=await writeSession('machine_b',[event('rb','1',Date.parse(AT),'source B',{origin_id:'origin-b'})],1,{extensions:{'96bulls':{user_id:'user_1',canonical_session_id:canonical}}});
 let keys=[a,b];const sb=fakeSupabase(()=>keys,()=>false,()=>canonical),ch=fakeClickHouse();
 expect((await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).complete).toBe(true);
 keys=[b];expect((await indexSearchScope(scope,searchDeps(ch.ch,sb.transact))).complete).toBe(true);
 expect(ch.final().map(r=>r.content)).toEqual(['source B']);
});
test('independent: malformed stale physical alias reconcile must preserve valid canonical sibling',async()=>{
 const b=await writeSession('machine_b',[event('rb','1',Date.parse(AT),'source B',{origin_id:'origin-b'})],1,{extensions:{'96bulls':{user_id:'user_1',canonical_session_id:canonical}}});
 const seed=fakeSupabase(()=>[b],()=>false,()=>canonical),ch=fakeClickHouse();
 expect((await indexSearchScope(scope,searchDeps(ch.ch,seed.transact))).complete).toBe(true);
 const a=await writeSession('machine_a',[event('ra','1',Date.parse(AT),'source A',{origin_id:'origin-a'})],1,{physicalSessionId:'alias_a',extensions:{'96bulls':{user_id:'user_1',canonical_session_id:canonical}}});
 const sb=aliasDb(()=>[a],()=>[a,b]);
 const result=await indexSearchScope({...scope,sessionId:'alias_a'},searchDeps(ch.ch,sb.transact));
 expect(result.complete).toBe(false);
 expect(ch.final().map(r=>r.content)).toContain('source B');
});
test('independent: erased stale physical alias must preserve live canonical sibling',async()=>{
 const b=await writeSession('machine_b',[event('rb','1',Date.parse(AT),'source B',{origin_id:'origin-b'})],1,{extensions:{'96bulls':{user_id:'user_1',canonical_session_id:canonical}}});
 const seed=fakeSupabase(()=>[b],()=>false,()=>canonical),ch=fakeClickHouse();
 expect((await indexSearchScope(scope,searchDeps(ch.ch,seed.transact))).complete).toBe(true);
 const sb=aliasDb(()=>[],()=>[b]);
 await indexSearchScope({...scope,sessionId:'alias_a'},searchDeps(ch.ch,sb.transact));
 expect(ch.final().map(r=>r.content)).toContain('source B');
});
