import { afterAll, expect, test } from "bun:test";
import { existsSync, statSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { stateForRun } from "../neon-s3-sync/main.ts";
import { SyncState } from "../neon-s3-sync/state.ts";

const dirs: string[] = [];
afterAll(async () => {
  await Promise.all(dirs.map((d) => rm(d, { recursive: true, force: true })));
});

test("a dry run never writes the real watermark: it works on a private copy that is removed afterwards", async () => {
  const dir = await mkdtemp(join(tmpdir(), "s3x-dry-"));
  dirs.push(dir);
  const real = join(dir, "state.sqlite");
  const live = new SyncState(real);
  live.seeRevisions(1, [{ org_id: "o", session_id: "s", revision: "3" }]);
  live.markExported("o", "s", 3, ["96bulls/o/v1/ws=w/m=m/h=h/d=2026/10/20/s=s/manifest.json"]);
  live.close();
  // The watermark's logical content (bytes can move with WAL checkpoints).
  const dump = () => {
    const d = new DatabaseSync(real, { readOnly: true });
    try {
      return JSON.stringify(["groups", "exported", "meta"].map((t) => d.prepare(`SELECT * FROM ${t} ORDER BY 1, 2`).all()));
    } finally {
      d.close();
    }
  };
  const before = dump();

  const run = stateForRun(false, real, join(dir, "dry-runs"));
  expect(run.path).not.toBe(real);
  const dry = new SyncState(run.path);
  // The copy carries the watermark (a dry run plans from it)...
  expect(dry.exported("o", "s")!.revision).toBe(3);
  // ...and whatever the dry run records stays in the copy.
  dry.markExported("o", "fake", 1, ["96bulls/o/v1/ws=w/m=m/h=h/d=2026/10/20/s=fake/manifest.json"]);
  dry.forceChanged([{ org_id: "o", session_id: "s" }]);
  dry.close();
  run.cleanup();

  expect(existsSync(run.path)).toBe(false);
  expect(dump()).toBe(before);
  const check = new SyncState(real);
  expect(check.exported("o", "fake")).toBeUndefined();
  expect(check.exported("o", "s")!.revision).toBe(3);
  check.close();
});

test("--execute uses the watermark itself; a dry run with no watermark starts empty", async () => {
  const dir = await mkdtemp(join(tmpdir(), "s3x-dry-"));
  dirs.push(dir);
  const real = join(dir, "state.sqlite");
  expect(stateForRun(true, real).path).toBe(real);
  const run = stateForRun(false, real, join(dir, "dry-runs"));
  expect(existsSync(real)).toBe(false);
  const s = new SyncState(run.path);
  expect(s.exported("o", "s")).toBeUndefined();
  s.close();
  run.cleanup();
  expect(existsSync(real)).toBe(false);
  expect(statSync(join(dir, "dry-runs")).mode & 0o777).toBe(0o700);
});
