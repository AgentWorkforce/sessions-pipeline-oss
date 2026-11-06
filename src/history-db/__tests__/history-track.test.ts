import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync, writeFileSync, rmSync, copyFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { generation } from "../../hoopdb/migrate.ts";
import {
  HISTORY_MIGRATIONS,
  historyGeneration,
  historyReady,
  historyRetentionViolations,
  inspectHistory,
  runHistoryMigrations,
  type HistoryClient,
} from "../migrate.ts";

setDefaultTimeout(60_000);
const open: PGlite[] = [];
const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((d) => d.close()));
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});
function client(db: PGlite): HistoryClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
/** A scratch copy of the real history track, so tests can add files safely. */
function scratchTrack(): string {
  const dir = mkdtempSync(join(tmpdir(), "history-track-"));
  dirs.push(dir);
  for (const f of readdirSync(HISTORY_MIGRATIONS)) copyFileSync(join(HISTORY_MIGRATIONS, f), join(dir, f));
  return dir;
}

describe("history migration track is separate from the ingest generation", () => {
  test("adding a history migration leaves the ingest generation() unchanged and moves only the history generation", async () => {
    const ingestBefore = await generation();
    const historyBefore = await historyGeneration();
    const probe = join(HISTORY_MIGRATIONS, "9999_generation_probe.sql");
    writeFileSync(probe, "CREATE TABLE history.generation_probe (org_id text NOT NULL, created_at timestamptz NOT NULL);\n");
    try {
      expect(await generation()).toBe(ingestBefore);
      expect(await historyGeneration()).not.toBe(historyBefore);
    } finally {
      rmSync(probe, { force: true });
    }
    expect(await historyGeneration()).toBe(historyBefore);
  });

  test("every history table is retention-ready (org_id + timestamptz)", async () => {
    expect(await historyRetentionViolations()).toEqual([]);
    const dir = scratchTrack();
    writeFileSync(join(dir, "0900_bad.sql"), "CREATE TABLE history.no_org (id text PRIMARY KEY, created_at timestamptz);\n");
    writeFileSync(join(dir, "0901_bad.sql"), "CREATE TABLE history.no_time (org_id text NOT NULL, n int);\n");
    expect(await historyRetentionViolations(dir)).toEqual(["0900_bad.sql: history.no_org", "0901_bad.sql: history.no_time"]);
  });
});

describe("runHistoryMigrations", () => {
  test("applies, records, marks pass; reruns skip; never touches the ingest ledger", async () => {
    const db = new PGlite(); open.push(db);
    const c = client(db);
    const dir = scratchTrack();
    writeFileSync(join(dir, "0900_example.sql"), "CREATE TABLE history.example (org_id text NOT NULL, seen_at timestamptz NOT NULL, PRIMARY KEY (org_id));\n");
    const files = readdirSync(dir).filter((f) => f.endsWith(".sql")).length;
    const gen = await historyGeneration(dir);
    expect(await historyReady(c, gen)).toBe(false);
    const first = await runHistoryMigrations(c, { dir });
    expect(first).toMatchObject({ applied: files, skipped: 0, generation: gen });
    expect(await historyReady(c, gen)).toBe(true);
    const second = await runHistoryMigrations(c, { dir });
    expect(second).toMatchObject({ applied: 0, skipped: files, generation: gen });
    expect((await db.query("SELECT to_regclass('sessions.__96bulls_migrations') AS t")).rows[0]).toEqual({ t: null });
  });

  test("a ledger that drifted from the approved plan refuses before any write", async () => {
    const db = new PGlite(); open.push(db);
    const c = client(db);
    const dir = scratchTrack();
    const plan = await inspectHistory(c, dir);
    await runHistoryMigrations(c, { dir });
    writeFileSync(join(dir, "0900_late.sql"), "CREATE TABLE history.late (org_id text NOT NULL, at timestamptz NOT NULL);\n");
    await expect(runHistoryMigrations(c, { dir, expectedLedgerSha256: plan.ledgerSha256 })).rejects.toMatchObject({ code: "history_ledger_changed" });
    expect((await db.query("SELECT to_regclass('history.late') AS t")).rows[0]).toEqual({ t: null });
  });

  test("an applied migration whose file changed refuses", async () => {
    const db = new PGlite(); open.push(db);
    const c = client(db);
    const dir = scratchTrack();
    writeFileSync(join(dir, "0900_x.sql"), "CREATE TABLE history.x (org_id text NOT NULL, at timestamptz NOT NULL);\n");
    await runHistoryMigrations(c, { dir });
    writeFileSync(join(dir, "0900_x.sql"), "CREATE TABLE history.x (org_id text NOT NULL, at timestamptz NOT NULL, y int);\n");
    await expect(runHistoryMigrations(c, { dir })).rejects.toMatchObject({ code: "history_migration_changed" });
  });

  test("a failing migration rolls back with no ledger row and leaves readiness pending", async () => {
    const db = new PGlite(); open.push(db);
    const c = client(db);
    const dir = scratchTrack();
    writeFileSync(join(dir, "0900_broken.sql"), "CREATE TABLE history.broken (org_id text NOT NULL, at timestamptz NOT NULL); SELECT nope();\n");
    await expect(runHistoryMigrations(c, { dir })).rejects.toThrow();
    expect((await db.query<{ name: string }>("SELECT name FROM history.__history_migrations ORDER BY name")).rows.map((r) => r.name)).not.toContain("0900_broken.sql");
    expect((await db.query("SELECT to_regclass('history.broken') AS t")).rows[0]).toEqual({ t: null });
    expect(await historyReady(c, await historyGeneration(dir))).toBe(false);
  });
});
