/**
 * Rollback after the flip (`src/migration/relayhistory-reverse.ts`): what
 * Supabase accepted since T0 goes back to Neon, deletions first.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { planTable } from "../../../src/migration/relayhistory-copy.js";
import {
  postT0Deletions,
  replayDeletions,
  reverseTable,
  reverseWriteMode,
} from "../../../src/migration/relayhistory-reverse.js";

const DDL = `CREATE SCHEMA sessions;
  CREATE TABLE sessions.deleted_upload_sessions (
    org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
    origin_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
    PRIMARY KEY (org_id, workspace_id, user_id, origin_id, source, session_id));
  CREATE TABLE sessions.session_resets (
    org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
    request_id text NOT NULL, deleted bigint NOT NULL,
    PRIMARY KEY (org_id, workspace_id, user_id, request_id));
  CREATE TABLE sessions.convergence_events (
    org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
    source text NOT NULL, session_id text NOT NULL, event_id text NOT NULL,
    ts timestamptz NOT NULL, content text, PRIMARY KEY (org_id, event_id));
  CREATE INDEX events_ts ON sessions.convergence_events (ts);`;
const T0 = new Date("2026-10-31T00:00:00Z");

let supabase: PGlite;
let neon: PGlite;
beforeEach(async () => {
  [supabase, neon] = [new PGlite(), new PGlite()];
  await Promise.all([supabase.exec(DDL), neon.exec(DDL)]);
});
afterEach(async () => {
  await Promise.all([supabase.close(), neon.close()]);
});

describe("guard", () => {
  const SUPA = "postgres://postgres:x@db.abcdefghijklmnopqrst.supabase.co:5432/postgres";
  const NEON = "postgres://owner:x@ep-a-1.us-east-1.aws.neon.tech/neondb";
  it("writes only from the named Supabase project to the named Neon host", () => {
    expect(reverseWriteMode({ sourceUrl: SUPA, targetUrl: NEON, execute: false })).toBe("dry-run");
    expect(
      reverseWriteMode({
        sourceUrl: SUPA,
        targetUrl: NEON,
        execute: true,
        expectSupabaseRef: "abcdefghijklmnopqrst",
        expectNeonHost: "ep-a-1.us-east-1.aws.neon.tech",
      }),
    ).toBe("execute");
    for (const [sourceUrl, targetUrl, host] of [
      [NEON, SUPA, "ep-a-1.us-east-1.aws.neon.tech"], // swapped
      [SUPA, NEON, "ep-other.us-east-1.aws.neon.tech"], // wrong Neon host
      [SUPA, "postgres://x@localhost/db", "localhost"], // not Neon
    ] as const)
      expect(() =>
        reverseWriteMode({
          sourceUrl,
          targetUrl,
          execute: true,
          expectSupabaseRef: "abcdefghijklmnopqrst",
          expectNeonHost: host,
        }),
      ).toThrow("refusing to write");
  });
});

it("replays post-T0 deletions on Neon, then brings back rows written since T0", async () => {
  // Neon at T0 (frozen): u1/s1 and u2/s2 history, and an older reset for u3.
  await neon.exec(`INSERT INTO sessions.convergence_events VALUES
      ('o', 'w', 'u1', 'claude', 's1', 'e1', '2026-10-30T10:00:00Z', 'pre'),
      ('o', 'w', 'u2', 'claude', 's2', 'e2', '2026-10-30T11:00:00Z', 'pre'),
      ('o', 'w', 'u3', 'claude', 's3', 'e3', '2026-10-30T12:00:00Z', 'pre');
    INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u3', 'r-old', 0);`);
  // Supabase after T0: u1 deleted s1, u2 reset, u3 wrote a new event and edited e3.
  await supabase.exec(`INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u3', 'r-old', 0), ('o', 'w', 'u2', 'r-new', 1);
    INSERT INTO sessions.deleted_upload_sessions VALUES ('o', 'w', 'u1', 'origin', 'claude', 's1');
    INSERT INTO sessions.convergence_events VALUES
      ('o', 'w', 'u3', 'claude', 's3', 'e3', '2026-10-31T01:00:00Z', 'edited after T0'),
      ('o', 'w', 'u3', 'claude', 's3', 'e4', '2026-10-31T02:00:00Z', 'new after T0'),
      ('o', 'w', 'u3', 'claude', 's3', 'e-old', '2026-10-01T00:00:00Z', 'pre-T0 row, not rewritten');`);

  const deletions = await postT0Deletions(supabase, neon);
  expect(deletions.tombstones).toHaveLength(1);
  expect(deletions.resets).toEqual([
    { org_id: "o", workspace_id: "w", user_id: "u2", request_id: "r-new", deleted: 1 },
  ]);

  const plan = await planTable(supabase, neon, "convergence_events", { newestByTime: true });
  const keyPlan = { ...plan, order: plan.order.slice(1) };
  expect(await replayDeletions(neon, keyPlan, deletions)).toBe(2);
  const copied = await reverseTable(supabase, neon, keyPlan, { since: T0, chunkSize: 1 });
  expect(copied).toMatchObject({ read: 2, written: 2 });

  const rows = (
    await neon.query<{ event_id: string; content: string }>(
      "SELECT event_id, content FROM sessions.convergence_events ORDER BY event_id",
    )
  ).rows;
  expect(rows).toEqual([
    { event_id: "e3", content: "edited after T0" },
    { event_id: "e4", content: "new after T0" },
  ]);
});

it("changes nothing on a dry run", async () => {
  await supabase.exec(`INSERT INTO sessions.convergence_events VALUES
    ('o', 'w', 'u', 'claude', 's', 'e', '2026-10-31T01:00:00Z', 'x')`);
  const plan = await planTable(supabase, neon, "convergence_events");
  expect(await reverseTable(supabase, neon, plan, { since: T0, chunkSize: 10, dryRun: true })).toEqual({
    read: 1,
    written: 0,
  });
  expect((await neon.query("SELECT * FROM sessions.convergence_events")).rows).toEqual([]);
});
