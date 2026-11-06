/**
 * Host time-zone matrix: the timestamp tests and the search parity suite must pass
 * identically under TZ=UTC and TZ=America/Los_Angeles (a non-UTC zone with DST). Each
 * zone runs in a child `bun test` process, because the zone is fixed at startup.
 */
import { describe, expect, test } from "bun:test";
import { join } from "node:path";

const root = join(import.meta.dir, "../../..");
const FILES = [
  "src/hoopdb/__tests__/search-timestamps.test.ts",
  "src/hoopdb/__tests__/session-directory-search-parity.test.ts",
];

describe("host time-zone matrix", () => {
  for (const tz of ["UTC", "America/Los_Angeles"]) {
    test(`TZ=${tz}: timestamp and parity suites pass`, () => {
      const run = Bun.spawnSync([process.execPath, "test", ...FILES], {
        cwd: root,
        env: { ...process.env, TZ: tz },
        stdout: "pipe",
        stderr: "pipe",
      });
      const output = `${run.stdout.toString()}\n${run.stderr.toString()}`;
      const summary = output
        .split("\n")
        .filter((l) => /^\s*\d+ (pass|fail)$/.test(l))
        .join(", ");
      console.info(`[G6 TZ matrix] TZ=${tz}: exit=${run.exitCode} ${summary}`);
      expect(run.exitCode, output.slice(-2000)).toBe(0);
      expect(output).toMatch(/\b0 fail\b/);
    }, 300_000);
  }
});
