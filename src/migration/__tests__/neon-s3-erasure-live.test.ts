/**
 * Erasure of relayhistory-api's live objects (WRITE_PATH, relayhistory-cloud #241).
 *
 * The fence SQL runs for real (PGlite) against `deleted_upload_sessions` as
 * the vendored migration 0038 creates it. The keys are what #241's
 * `writeLiveObjects` produced at 85bfb27 for org-1 / ws/1 / origin-1 /
 * user-1 / claude / sess:1, which pins the two key layouts to each other.
 * That a real reset writes these tombstones is proven against the full
 * relayhistory migration chain in #241's test/write-path-migrated.test.ts.
 */
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../shared/vendor.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { inSessionAnyDate, storeEraser } from "../neon-s3-erasure/eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import { liveMachineId, liveTargets } from "../neon-s3-erasure/live.ts";
import { fencedLivePage } from "../neon-s3-sync/neon-reader.ts";
import { SyncState } from "../neon-s3-sync/state.ts";

const M = "srv_47a49913c03b1505faf877017e54ae00";
const DIR = `96bulls/org-1/v1/ws=ws_1/m=${M}/h=claude/d=2026/10/30/s=sess_1/`;
const WORKER_KEYS = [`${DIR}manifest.json`, `${DIR}raw/0000000000-0-e568b589.ndjson.zst`];
const POINTER = `96bulls/org-1/live-sessions/v1/ws=ws_1/m=${M}/h=claude/s=sess_1.json`;
const GROUP = { org_id: "org-1", workspace_id: "ws/1", user_id: "user-1", origin_id: "origin-1", source: "claude", session_id: "sess:1" };

// PGlite boots per test; slower than the 5 s default under load.
setDefaultTimeout(60_000);

/** The sweep runs well after the fixture objects were written (beyond LIVE_SKEW_MS). */
const later = () => Date.now() + 10 * 60 * 1000;

const tmp = () => mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "erasure-live-"));

/** The deployed tombstone table (vendored 0038), plus the rollup sweep the job also runs. */
async function neon(): Promise<{ pg: PGlite; db: Queryable }> {
  const sql = await readFile(new URL("../../../vendor/relayhistory-cloud/migrations/0038_session_upload_deletion.sql", import.meta.url), "utf8");
  const ddl = /CREATE TABLE sessions\.deleted_upload_sessions \([\s\S]*?\n\);/.exec(sql)?.[0];
  if (!ddl) throw new Error("vendored 0038 no longer creates deleted_upload_sessions");
  const pg = new PGlite();
  await pg.exec(`CREATE SCHEMA sessions; ${ddl}
    CREATE TABLE sessions.session_resets (org_id text, workspace_id text, user_id text, request_id text, deleted bigint);
    CREATE TABLE sessions.delivery_records (org_id text, workspace_id text, origin_id text, record_id text, kind text, source text, session_id text, user_id text);`);
  const db: Queryable = {
    async query<T>(text: string, params?: unknown[]) {
      // Only the sweep's other reads are stood in for; the live fence is real SQL.
      if (text.includes("pg_locks")) return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      // #101's catalog walk; before session_rollups (groupExistsInNeon names both).
      if (text.includes("sessions.session_catalog")) return { rows: [] as T[] };
      if (text.includes("FROM sessions.session_rollups"))
        return { rows: (text.includes("(org_id, session_id) >") || text.startsWith("SELECT 1") ? [] : [{ org_id: "org-1", session_id: "keep", last_ms: "1", events: "1" }]) as T[] };
      return (await pg.query<T>(text, params)) as { rows: T[] };
    },
  };
  return { pg, db };
}

const tombstone = (pg: PGlite, g: typeof GROUP) =>
  pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES ($1,$2,$3,$4,$5,$6)", [g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id]);

async function keys(store: ReturnType<typeof createFsStore>) {
  const out: string[] = [];
  let c: string | undefined;
  do {
    const p = await store.list(objectKey("96bulls"), c);
    out.push(...p.items.map((i) => i.key));
    c = p.nextCursor;
  } while (c);
  return out.sort();
}

async function put(store: ReturnType<typeof createFsStore>, ks: string[]) {
  for (const k of ks) await store.put(objectKey(k), new TextEncoder().encode("x"));
}

describe("live (srv_) erasure", () => {
  test("derives #241's exact keys, per user", () => {
    expect(liveMachineId("ws/1", "origin-1", "user-1")).toBe(M);
    expect(liveMachineId("ws/1", "origin-1", "user-2")).not.toBe(M);
    expect(liveTargets("96bulls", GROUP)).toEqual({ base: `96bulls/org-1/v1/ws=ws_1/m=${M}/h=claude/`, sessionSegment: "s=sess_1", pointerKey: POINTER });
  });

  test("matches only the fenced session's directory, on any date", () => {
    const base = `96bulls/org-1/v1/ws=ws_1/m=${M}/h=claude/`;
    expect(inSessionAnyDate(base, "s=sess_1", `${DIR}manifest.json`)).toBe(true);
    expect(inSessionAnyDate(base, "s=sess_1", `${base}d=2026/11/01/s=sess_1/raw/x`)).toBe(true);
    expect(inSessionAnyDate(base, "s=sess_1", `${base}d=2026/10/30/s=sess_10/manifest.json`)).toBe(false);
    expect(inSessionAnyDate(base, "s=sess_1", `${base}x/d=2026/10/30/s=sess_1/manifest.json`)).toBe(false);
  });

  test("a tombstoned session's objects and pointer are erased; another user's copy and neighbours stay", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    const otherUser = liveMachineId("ws/1", "origin-1", "user-2");
    const otherUserCopy = `96bulls/org-1/v1/ws=ws_1/m=${otherUser}/h=claude/d=2026/10/30/s=sess_1/manifest.json`;
    const neighbour = `96bulls/org-1/v1/ws=ws_1/m=${M}/h=claude/d=2026/10/30/s=sess_10/manifest.json`;
    await put(store, [...WORKER_KEYS, POINTER, otherUserCopy, neighbour]);
    await tombstone(pg, GROUP);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    expect(await keys(store)).toEqual([neighbour, otherUserCopy].sort());
    expect(job.counters.liveVersionsDeleted).toBe(3);
    await job.sweep();
    expect(job.counters.liveVersionsDeleted).toBe(3);
    state.close();
    await pg.close();
  });

  test("a session uploaded after the user's reset is not erased (no reset leg)", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await pg.query("INSERT INTO sessions.session_resets VALUES ('org-1','ws/1','user-1','reset-1',3)");
    // The session delivered after the reset: what a reset leg would match.
    await pg.query("INSERT INTO sessions.delivery_records VALUES ('org-1','ws/1','origin-1','r1','session','claude','sess:1','user-1')");
    await put(store, [...WORKER_KEYS, POINTER]);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    expect(await keys(store)).toEqual([...WORKER_KEYS, POINTER].sort());
    state.close();
    await pg.close();
  });

  test("the fence pages by primary key with no cap", async () => {
    const { pg, db } = await neon();
    for (let i = 0; i < 7; i++) await tombstone(pg, { ...GROUP, session_id: `s${i}` });
    const seen: string[] = [];
    let after = null;
    for (;;) {
      const page = await fencedLivePage(db, after, 3);
      if (!page.length) break;
      seen.push(...page.map((r) => r.session_id));
      after = page.at(-1)!;
    }
    expect(seen).toEqual(["s0", "s1", "s2", "s3", "s4", "s5", "s6"]);
    await pg.close();
  });

  test("a dry run erases nothing", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await put(store, [...WORKER_KEYS, POINTER]);
    await tombstone(pg, GROUP);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep({ dryRun: true });
    expect(await keys(store)).toEqual([...WORKER_KEYS, POINTER].sort());
    state.close();
    await pg.close();
  });

  test("re-enabled since the page was read: nothing is erased", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await put(store, [...WORKER_KEYS, POINTER]);
    await tombstone(pg, GROUP);
    // The user re-enables between the page read and this row's erasure.
    const racing: Queryable = {
      async query<T>(text: string, params?: unknown[]) {
        if (text.includes("SELECT 1 FROM sessions.deleted_upload_sessions"))
          await pg.query("DELETE FROM sessions.deleted_upload_sessions");
        return db.query<T>(text, params);
      },
    };
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(racing, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    expect(await keys(store)).toEqual([...WORKER_KEYS, POINTER].sort());
    state.close();
    await pg.close();
  });

  test("objects newer than the re-check (a re-upload) are kept, and so is the pointer", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await put(store, [...WORKER_KEYS, POINTER]);
    await tombstone(pg, GROUP);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    // The sweep's clock is the objects' own: all of them are within the skew margin.
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, Date.now, async () => {});
    await job.sweep();
    expect(await keys(store)).toEqual([...WORKER_KEYS, POINTER].sort());
    state.close();
    await pg.close();
  });

  test("the session ingest's staged batches of a tombstoned machine are erased after the redrive window (review Medium)", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    const staged = `96bulls/org-1/live-batches/v1/ws=ws_1/m=${M}/b=${"a".repeat(32)}-${"b".repeat(16)}.staged.ndjson.zst`;
    const marker = `96bulls/org-1/live-batches/v1/ws=ws_1/m=${M}/b=${"a".repeat(32)}.json`;
    const otherMachine = `96bulls/org-1/live-batches/v1/ws=ws_1/m=${liveMachineId("ws/1", "origin-1", "user-2")}/b=${"c".repeat(32)}-${"d".repeat(16)}.staged.ndjson.zst`;
    await put(store, [...WORKER_KEYS, POINTER, staged, marker, otherMachine]);
    await tombstone(pg, GROUP);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    // Within the window: the session goes at once, the staged batch waits (its fan-out may still be redriven).
    const soon = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await soon.sweep();
    expect(await keys(store)).toEqual([marker, otherMachine, staged].sort());
    // Past it (the session's own objects already gone): the tombstone is revisited for the staged batch.
    const past = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, () => Date.now() + 49 * 60 * 60 * 1000, async () => {});
    await past.sweep();
    expect(await keys(store)).toEqual([marker, otherMachine].sort());
    state.close();
    await pg.close();
  });

  test("a tombstone with no live pointer costs no session listing", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    for (let i = 0; i < 20; i++) await tombstone(pg, { ...GROUP, session_id: `old-${i}` });
    const inner = storeEraser(store);
    const calls = { list: 0, dir: 0 };
    const counting = {
      ...inner,
      listKeys: (p: string) => (calls.list++, inner.listKeys(p)),
      eraseSessionAnyDate: (b: string, s: string, t?: Date) => (calls.dir++, inner.eraseSessionAnyDate(b, s, t)),
    };
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, counting, () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    // Once per (workspace, user, origin) and page: objects, pointers and the session ingest's staged batches.
    expect(calls).toEqual({ list: 3, dir: 0 });
    state.close();
    await pg.close();
  });

  test("chunks left without a pointer (a write that landed after the pointer was erased) are still erased", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await put(store, WORKER_KEYS);
    await tombstone(pg, GROUP);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    expect(await keys(store)).toEqual([]);
    state.close();
    await pg.close();
  });

  test("an orphaned pointer under a tombstone is erased", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await put(store, [POINTER]);
    await tombstone(pg, GROUP);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    expect(await keys(store)).toEqual([]);
    state.close();
    await pg.close();
  });

  test("a failed listing is tried once per group per page", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    for (let i = 0; i < 10; i++) await tombstone(pg, { ...GROUP, session_id: `t${i}` });
    let lists = 0;
    const failing = { ...storeEraser(store), listKeys: async () => { lists++; throw new Error("s3 down"); } };
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(db, state, failing, () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    expect(lists).toBe(1);
    expect(job.counters.liveFenceFailed).toBe(1);
    state.close();
    await pg.close();
  });

  test("live erasure runs even when the discovery walk fails", async () => {
    const { pg, db } = await neon();
    const store = createFsStore({ root: await tmp() });
    await put(store, [...WORKER_KEYS, POINTER]);
    await tombstone(pg, GROUP);
    const broken: Queryable = {
      async query<T>(text: string, params?: unknown[]) {
        if (text.includes("sessions.session_rollups")) throw new Error("rollups unavailable");
        return db.query<T>(text, params);
      },
    };
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    const job = new ErasureJob(broken, state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep().catch(() => {});
    expect(await keys(store)).toEqual([]);
    state.close();
    await pg.close();
  });
});
