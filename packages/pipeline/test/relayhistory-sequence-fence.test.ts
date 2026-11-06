/**
 * The T0 sequence fence (`src/migration/relayhistory-sequence-fence.ts`) on
 * two in-process Postgres databases standing in for Neon and Supabase.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterEach, beforeEach, expect, it } from "vitest";
import { applyFence, checkFence } from "../../../src/migration/relayhistory-sequence-fence.js";

let neon: PGlite;
let supabase: PGlite;
const DDL = `CREATE SCHEMA sessions;
  CREATE SEQUENCE sessions.used_seq;
  CREATE SEQUENCE sessions.unused_seq START 100;
  CREATE TABLE sessions.links (id bigserial PRIMARY KEY);`;

beforeEach(async () => {
  [neon, supabase] = [new PGlite(), new PGlite()];
  await Promise.all([neon.exec(DDL), supabase.exec(DDL)]);
  await neon.exec("SELECT nextval('sessions.used_seq') FROM generate_series(1, 42); INSERT INTO sessions.links DEFAULT VALUES;");
});
afterEach(async () => {
  await Promise.all([neon.close(), supabase.close()]);
});

const byName = (rows: { name: string }[]) =>
  Object.fromEntries(rows.map((row) => [row.name, row]));

it("fails until every sequence, used or not, is at least the gap above Neon", async () => {
  const before = await checkFence(neon, supabase, 1_000_000n);
  expect(before.passed).toBe(false);
  expect(byName(before.sequences)).toMatchObject({
    used_seq: { required: "1000042", fenced: false, reason: "never called on target" },
    unused_seq: { required: "1000100", fenced: false },
    links_id_seq: { required: "1000001", fenced: false },
  });

  const after = await applyFence(neon, supabase, 1_000_000n);
  expect(after.passed).toBe(true);
  expect((await supabase.query<{ n: string }>("SELECT nextval('sessions.used_seq')::text AS n")).rows).toEqual([
    { n: "1000043" },
  ]);
});

it("re-checks after Neon advances, and the re-apply only raises", async () => {
  await applyFence(neon, supabase, 1_000_000n);
  // Supabase is ahead on one sequence; Neon then writes more before the drain.
  await supabase.exec("SELECT setval('sessions.unused_seq', 5000000)");
  await neon.exec("SELECT nextval('sessions.used_seq') FROM generate_series(1, 10)");
  const drifted = await checkFence(neon, supabase, 1_000_000n);
  expect(byName(drifted.sequences).used_seq).toMatchObject({ fenced: false, reason: "below the fence" });
  const raised = await applyFence(neon, supabase, 1_000_000n);
  expect(raised.passed).toBe(true);
  expect(byName(raised.sequences).unused_seq).toMatchObject({ target: "5000000" });
});

it("fails on a sequence missing from the target and cannot paper over it", async () => {
  await supabase.exec("DROP SEQUENCE sessions.unused_seq");
  const result = await applyFence(neon, supabase, 1_000_000n);
  expect(result.passed).toBe(false);
  expect(byName(result.sequences).unused_seq).toMatchObject({ reason: "missing on target" });
});
