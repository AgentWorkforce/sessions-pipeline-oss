import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { appendFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { initializeEnrollment, manifestReconciliation, runCourierOnce, sessionsForStartMode, type CourierConfig } from "../courier.ts";

const temporary: string[] = [];

afterEach(async () => {
  await Promise.all(temporary.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("courier enrollment", () => {
  const rows = [
    { id: "before", first_activity_ms: 100, last_activity_ms: 150 },
    { id: "after", first_activity_ms: 300, last_activity_ms: 350 },
    { id: "last-only", first_activity_ms: null, last_activity_ms: 400 },
    { id: "unknown", first_activity_ms: null, last_activity_ms: null },
  ];

  test("new mode keeps sessions that started after enrollment", () => {
    expect(sessionsForStartMode(rows, "new", new Date(200).toISOString()).map((row) => row.id)).toEqual(["after", "last-only"]);
  });

  test("switching to explicit all mode ignores a persisted enrollment cutoff", () => {
    expect(sessionsForStartMode(rows, "all", new Date(200).toISOString())).toEqual(rows);
  });

  test("new mode adds a cutoff to an existing partial ledger before selection", () => {
    const state: { enrolled_at?: string } = {};
    expect(initializeEnrollment(state, "new", new Date(200))).toBe(true);
    expect(state.enrolled_at).toBe(new Date(200).toISOString());
    expect(sessionsForStartMode(rows, "new", state.enrolled_at).map((row) => row.id)).toEqual(["after", "last-only"]);
    expect(initializeEnrollment(state, "new", new Date(400))).toBe(false);
    expect(state.enrolled_at).toBe(new Date(200).toISOString());
  });
});

describe("courier manifest reconciliation", () => {
  test("only a definitive hit or miss can advance the session", () => {
    expect(manifestReconciliation(200)).toBe("verified");
    expect(manifestReconciliation(404)).toBe("reset");
    expect(manifestReconciliation(401)).toBe("defer");
    expect(manifestReconciliation(503)).toBe("defer");
    expect(manifestReconciliation()).toBe("defer");
  });

  test("persists identity, recovers a fresh Locker, and replays a lost response exactly", async () => {
    const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-replay-"));
    temporary.push(dir);
    const dbPath = join(dir, "history.db");
    const statePath = join(dir, "state.json");
    const rawPath = join(dir, "session.jsonl");
    const first = Buffer.from('{"type":"user","message":"one"}\n');
    const second = Buffer.from('{"type":"assistant","message":"two"}\n');
    await writeFile(rawPath, first);
    const db = new DatabaseSync(dbPath);
    db.exec("CREATE TABLE sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT)");
    db.prepare("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)").run("session-1", "claude", rawPath, 1_700_000_000_000, 1_700_000_000_100, null, "1.0.0");
    db.close();

    let serverVersion: { eTag: string } | undefined;
    let statusCalls = 0;
    let chunkPosts = 0;
    let manifestPosts = 0;
    const posted: string[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input, init) => {
      const url = String(input);
      if (url.includes("/manifests/status")) {
        statusCalls += 1;
        return serverVersion ? Response.json({ version: serverVersion }) : new Response(null, { status: 404 });
      }
      if (url.includes("/pipeline/chunks")) {
        chunkPosts += 1;
        if (chunkPosts === 1) {
          const durable = JSON.parse(await readFile(statePath, "utf8"));
          expect(durable.sessions["claude:session-1"].session_uuid).toMatch(/^[0-9a-f-]{36}$/);
          expect(durable.sessions["claude:session-1"].trace_id).toMatch(/^[0-9a-f-]{36}$/);
          expect(durable.sessions["claude:session-1"].chunks).toEqual([]);
        }
        const query = new URL(url).searchParams;
        const bytes = Buffer.from(init?.body as Uint8Array);
        const sha = createHash("sha256").update(bytes).digest("hex");
        const seq = Number(query.get("seq"));
        const offset = Number(query.get("offset"));
        return Response.json({
          key: `raw/${seq}`,
          created: true,
          scrub: { ruleset: "relayhistory-scrub", version: 1 },
          entry: {
            seq, epoch: 0, start_offset: offset, bytes: bytes.length, sha256: sha, sha256_8: sha.slice(0, 8),
            compression: "zstd", records: 1, stored_bytes: bytes.length, stored_sha256: sha,
            stored_raw_bytes: bytes.length, raw_sha256: sha,
          },
        }, { status: 201 });
      }
      if (url.endsWith("/v1/pipeline/manifests")) {
        manifestPosts += 1;
        posted.push(String(init?.body));
        if (manifestPosts === 1) {
          // The first manifest did not commit. A fresh Locker must make the
          // pending revision reconcile as missing and reseal its chunk.
          return Response.json({ error: { code: "storage_unavailable" } }, { status: 503 });
        }
        if (manifestPosts === 2) {
          serverVersion = { eTag: '"v1"' };
          return Response.json({ version: serverVersion }, { status: 202 });
        }
        if (manifestPosts === 3) {
          // The store committed revision 2, but notification/response failed.
          serverVersion = { eTag: '"v2"' };
          return Response.json({ error: { code: "storage_unavailable" } }, { status: 503 });
        }
        return Response.json({ version: serverVersion }, { status: 202 });
      }
      throw new Error(`unexpected request: ${url}`);
    }) as typeof fetch;

    const config = (): CourierConfig => ({
      dbPath,
      statePath,
      ingestUrl: "http://127.0.0.1:8080",
      token: { get: async () => "token", invalidate: () => false },
      orgId: "org-test",
      workspaceId: "default",
      userId: "user-test",
      machineId: "machine-test",
      harnesses: ["claude"],
      closeAfterMinutes: 1_000_000,
      startMode: "all",
    });

    try {
      expect(await runCourierOnce(config())).toMatchObject({ uploaded: 0, chunks: 1 });
      const firstPending = JSON.parse(await readFile(statePath, "utf8"));
      expect(firstPending.sessions["claude:session-1"]).toMatchObject({ revision: 0, pending: true });
      expect(firstPending.sessions["claude:session-1"].pending_manifest.revision).toBe(1);
      expect(await runCourierOnce(config())).toMatchObject({ uploaded: 1, chunks: 1 });
      expect(statusCalls).toBe(1);
      expect(chunkPosts).toBe(2);
      await appendFile(rawPath, second);
      expect(await runCourierOnce(config())).toMatchObject({ uploaded: 0, chunks: 1 });
      const pending = JSON.parse(await readFile(statePath, "utf8"));
      expect(pending.sessions["claude:session-1"].revision).toBe(1);
      expect(pending.sessions["claude:session-1"].pending_manifest.revision).toBe(2);
      expect(pending.sessions["claude:session-1"].pending_previous_version).toEqual({ eTag: '"v1"' });
      expect(await runCourierOnce(config())).toMatchObject({ uploaded: 1, chunks: 0 });
      expect(posted[3]).toBe(posted[2]);
      const landed = JSON.parse(await readFile(statePath, "utf8"));
      expect(landed.sessions["claude:session-1"]).toMatchObject({ revision: 2, pending: false, manifest_version: serverVersion });
      expect(landed.sessions["claude:session-1"].pending_manifest).toBeUndefined();
      expect(landed.sessions["claude:session-1"].pending_previous_version).toBeUndefined();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
