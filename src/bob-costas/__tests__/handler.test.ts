import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { rawChunkKey, objectKey } from "../../shared/vendor.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { bobCostasHandler } from "../handler.ts";
import { extractiveSummarizer } from "../summarizer.ts";
import { readSummaryObject } from "../summary-objects.ts";

const MIG = (f: string) =>
  readFileSync(join(import.meta.dir, "../../hoopdb/migrations", f), "utf8");

const open: PGlite[] = [];
// PGlite's WASM runtime must be closed, or the test process exits non-zero.
afterEach(async () => {
  while (open.length) await open.pop()!.close();
});

async function setup() {
  const pg = new PGlite();
  open.push(pg);
  await pg.exec(
    `CREATE SCHEMA sessions; CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;`,
  );
  await pg.exec(MIG("0002_session_ext_and_summaries.sql"));
  // A pre-0004 row holding plaintext: the migration must remove it.
  await pg.exec(`INSERT INTO sessions.session_summaries (org_id, workspace_id, harness, session_id, machine_id, kind, trace_id, revision, content_sha256, summary)
                 VALUES ('o','w','claude','old','m','short','01a188e1-ef86-7b16-938a-e91b6b78643b',1,'${"d".repeat(64)}','OLD PLAINTEXT')`);
  await pg.exec(MIG("0004_summary_pointers.sql"));
  const q = async (text: string, params?: unknown[]) => ({
    rows: (await pg.query(text, params as never)).rows as never[],
  });
  const db = {
    query: q,
    pool: { connect: async () => ({ query: q, release() {} }) },
    end: async () => {},
  } as never;
  return { pg, db };
}

function manifest(scrubbed: boolean, chunks: unknown[]) {
  return {
    revision: 2,
    trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b",
    session: {
      workspace_id: "w",
      machine_id: "m",
      harness: "claude",
      session_id: "s1",
      started_at: "2026-10-29T10:00:00Z",
    },
    raw: {
      format: "jsonl",
      content_sha256: "c".repeat(64),
      chunks,
      ...(scrubbed ? { scrub: { ruleset: "r", version: 1 } } : {}),
    },
  };
}

const loc = {
  workspaceId: "w",
  machineId: "m",
  harness: "claude",
  sessionId: "s1",
  startDate: "2026-10-29",
};
const message = { payload: { manifestKey: "k" }, ctx: { orgId: "o" } } as never;
const recorder = { log: { info() {} } } as never;
// These tests cover the encrypted-object path only; the directory projection (G6) is
// tested end to end in src/hoopdb/__tests__/session-directory-short-summary.test.ts.
const noDirectory = {
  write: async () => ({ written: false }),
  clear: async () => false,
};

describe("Bob Costas stores summaries as encrypted Locker objects, pointers in HoopDb", () => {
  it("ready: object summary/short.json.enc decrypts; the row has only the pointer; old plaintext rows are gone", async () => {
    const { pg, db } = await setup();
    expect(
      (
        await pg.query(
          `SELECT count(*)::int AS n FROM sessions.session_summaries`,
        )
      ).rows[0],
    ).toEqual({ n: 0 });
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bch-")) });
    const line = JSON.stringify({
      type: "user",
      message: { role: "user", content: "PLANTED-PROMPT fix the build" },
    });
    await store.put(
      rawChunkKey(loc, 0, 0, "abcdef12", "jsonl"),
      zstdCompressSync(Buffer.from(line + "\n")),
    );
    const m = manifest(true, [
      { seq: 0, start_offset: 0, sha256_8: "abcdef12" },
    ]);
    const keyring = await masterKeyring(
      parseMasterKeys(`k1:${"a".repeat(64)}`),
    );
    const handler = bobCostasHandler("short", {
      db,
      reader: { provider: "fs", read: async () => ({ manifest: m }) } as never,
      store,
      summarizer: extractiveSummarizer,
      directory: noDirectory,
      keyring,
    });
    await handler(message, recorder);
    const row = (
      await pg.query<Record<string, unknown>>(
        `SELECT * FROM sessions.session_summaries`,
      )
    ).rows[0]!;
    expect(row.status).toBe("ready");
    expect(row.object_key).toBe(
      "v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/summary/short.json.enc",
    );
    expect(row.key_id).toBe("hkdf1:k1");
    expect("summary" in row).toBe(false);
    expect("model_id" in row).toBe(false);
    expect(JSON.stringify(row)).not.toContain("PLANTED-PROMPT");
    const rec = await readSummaryObject({
      store,
      keyring,
      ids: { orgId: "o", workspaceId: "w" },
      key: String(row.object_key),
      sha256: String(row.object_sha256),
      kind: "short",
      revision: 2,
    });
    expect(rec?.status).toBe("ready");
    expect(rec?.summary).toContain("PLANTED-PROMPT");
    // A rerun of the same revision and content is a no-op.
    expect(await handler(message, recorder)).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    // After a master rotation the same summary is re-encrypted under the new key, not skipped.
    const rotated = await masterKeyring(
      parseMasterKeys(`k2:${"b".repeat(64)},k1:${"a".repeat(64)}`),
    );
    await bobCostasHandler("short", {
      db,
      reader: { provider: "fs", read: async () => ({ manifest: m }) } as never,
      store,
      summarizer: extractiveSummarizer,
      directory: noDirectory,
      keyring: rotated,
    })(message, recorder);
    const after = (
      await pg.query<Record<string, unknown>>(
        `SELECT key_id, object_sha256 FROM sessions.session_summaries`,
      )
    ).rows[0]!;
    expect(after.key_id).toBe("hkdf1:k2");
    const onlyK2 = await masterKeyring(parseMasterKeys(`k2:${"b".repeat(64)}`));
    expect(
      (
        await readSummaryObject({
          store,
          keyring: onlyK2,
          ids: { orgId: "o", workspaceId: "w" },
          key: String(row.object_key),
          sha256: String(after.object_sha256),
          kind: "short",
          revision: 2,
        })
      )?.summary,
    ).toContain("PLANTED-PROMPT");
  }, 30_000);

  it("unscrubbed: recorded quarantined with no pointer, no object created, raw never read", async () => {
    const { pg, db } = await setup();
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bch-")) });
    const keyring = await masterKeyring(
      parseMasterKeys(`k1:${"a".repeat(64)}`),
    );
    const m = manifest(false, [
      { seq: 0, start_offset: 0, sha256_8: "deadbeef" },
    ]); // chunk absent: reading it would throw
    await bobCostasHandler("long", {
      db,
      reader: { provider: "fs", read: async () => ({ manifest: m }) } as never,
      store,
      summarizer: extractiveSummarizer,
      directory: noDirectory,
      keyring,
    })(message, recorder);
    const row = (
      await pg.query<Record<string, unknown>>(
        `SELECT status, object_key, object_sha256, key_id FROM sessions.session_summaries`,
      )
    ).rows[0]!;
    expect(row).toEqual({
      status: "quarantined",
      object_key: null,
      object_sha256: null,
      key_id: null,
    });
    expect(
      await store.head(
        objectKey(
          "v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/summary/long.json.enc",
        ),
      ),
    ).toBeUndefined();
  }, 30_000);

  it("the pointer constraint refuses a ready row without a pointer", async () => {
    const { pg } = await setup();
    await expect(
      pg.query(`INSERT INTO sessions.session_summaries (org_id, workspace_id, harness, session_id, machine_id, kind, trace_id, revision, content_sha256, status)
                           VALUES ('o','w','claude','x','m','short','01a188e1-ef86-7b16-938a-e91b6b78643b',1,'${"e".repeat(64)}','ready')`),
    ).rejects.toThrow(/session_summaries_pointer/);
  }, 30_000);
});

describe("Bob Costas under org-derived key prefixes (PIPELINE_ORG_FROM_PREFIX)", () => {
  it("reads the chunks and writes the summary under the manifest's own <base>/<org> prefix", async () => {
    const { pg, db } = await setup();
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bch-")) });
    const prefixed = { ...loc, prefix: "96bulls/org_aws_proof_1" };
    const line = JSON.stringify({
      type: "user",
      message: { role: "user", content: "PLANTED-PROMPT tenant one" },
    });
    // Only the prefixed chunk exists, exactly where the ingest stores it.
    await store.put(
      rawChunkKey(prefixed, 0, 0, "abcdef12", "jsonl"),
      zstdCompressSync(Buffer.from(line + "\n")),
    );
    const m = manifest(true, [
      { seq: 0, start_offset: 0, sha256_8: "abcdef12" },
    ]);
    const keyring = await masterKeyring(
      parseMasterKeys(`k1:${"a".repeat(64)}`),
    );
    const msg = {
      payload: {
        manifestKey:
          "96bulls/org_aws_proof_1/v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/manifest.json",
      },
      ctx: { orgId: "org_aws_proof_1" },
    } as never;
    const handler = bobCostasHandler("short", {
      db,
      reader: { provider: "fs", read: async () => ({ manifest: m }) } as never,
      store,
      summarizer: extractiveSummarizer,
      directory: noDirectory,
      keyring,
    });
    // Before the fix this threw chunk_missing: the chunk was looked up at the unprefixed key.
    const outcome = await handler(msg, recorder).then(
      () => "ok",
      (e: { code?: string }) => e.code ?? String(e),
    );
    expect(outcome).toBe("ok");
    const row = (
      await pg.query<Record<string, unknown>>(
        `SELECT * FROM sessions.session_summaries WHERE org_id = 'org_aws_proof_1'`,
      )
    ).rows[0]!;
    expect(row.status).toBe("ready");
    expect(row.object_key).toBe(
      "96bulls/org_aws_proof_1/v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/summary/short.json.enc",
    );
    const rec = await readSummaryObject({
      store,
      keyring,
      ids: { orgId: "org_aws_proof_1", workspaceId: "w" },
      key: String(row.object_key),
      sha256: String(row.object_sha256),
      kind: "short",
      revision: 2,
    });
    expect(rec?.summary).toContain("PLANTED-PROMPT");
  }, 30_000);

  it("rebuilds a ready summary whose pointer is outside the manifest's current prefix (moved session)", async () => {
    const { pg, db } = await setup();
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bch-")) });
    const line = JSON.stringify({
      type: "user",
      message: { role: "user", content: "PLANTED-PROMPT moved" },
    });
    const prefixed = { ...loc, prefix: "96bulls/org_aws_proof_1" };
    for (const l of [loc, prefixed])
      await store.put(
        rawChunkKey(l, 0, 0, "abcdef12", "jsonl"),
        zstdCompressSync(Buffer.from(line + "\n")),
      );
    const m = manifest(true, [
      { seq: 0, start_offset: 0, sha256_8: "abcdef12" },
    ]);
    const keyring = await masterKeyring(
      parseMasterKeys(`k1:${"a".repeat(64)}`),
    );
    const deps = {
      db,
      reader: { provider: "fs", read: async () => ({ manifest: m }) } as never,
      store,
      summarizer: extractiveSummarizer,
      directory: noDirectory,
      keyring,
    };
    const at = (manifestKey: string) =>
      ({
        payload: { manifestKey },
        ctx: { orgId: "org_aws_proof_1" },
      }) as never;
    const unprefixed = "v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/manifest.json";
    await bobCostasHandler("short", deps)(at(unprefixed), recorder);
    // Same revision and content, now under the tenant prefix: not "current", rebuilt beside the new manifest.
    const moved = await bobCostasHandler("short", deps)(
      at(`96bulls/org_aws_proof_1/${unprefixed}`),
      recorder,
    );
    expect(moved).not.toEqual({ status: "skipped", reason: "summary_current" });
    const row = (
      await pg.query<Record<string, unknown>>(
        `SELECT object_key FROM sessions.session_summaries WHERE org_id = 'org_aws_proof_1'`,
      )
    ).rows[0]!;
    expect(row.object_key).toBe(
      "96bulls/org_aws_proof_1/v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/summary/short.json.enc",
    );
    // And a replay at the same place is current again.
    expect(
      await bobCostasHandler("short", deps)(
        at(`96bulls/org_aws_proof_1/${unprefixed}`),
        recorder,
      ),
    ).toEqual({ status: "skipped", reason: "summary_current" });
  }, 30_000);
});
