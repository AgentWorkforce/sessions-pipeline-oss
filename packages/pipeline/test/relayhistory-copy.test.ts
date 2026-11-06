/**
 * The T0 copy set (mirror) and history backfill from Neon to Supabase
 * (`src/migration/relayhistory-copy.ts`), between two in-process Postgres
 * databases standing in for Neon (source) and Supabase (target).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Two PGlite databases boot per test; parallel files can push past the 5 s default.
vi.setConfig({ testTimeout: 30_000 });
import {
  copyTable,
  planTable,
  deletionLockSql,
  loadCheckpoint,
  loadFence,
  planHash,
  prepareGuardedTargetSession,
  prepareTargetSession,
  type Queryable,
} from "../../../src/migration/relayhistory-copy.js";

const DDL = `CREATE SCHEMA sessions;
CREATE TABLE sessions.tokens (
  org_id text NOT NULL,
  id bigint NOT NULL,
  secret bytea,
  seen_local timestamp without time zone,
  seen_at timestamptz,
  meta jsonb,
  tags text[],
  label text,
  label_upper text GENERATED ALWAYS AS (upper(label)) STORED,
  PRIMARY KEY (org_id, id)
);`;

let source: PGlite;
let target: PGlite;
beforeEach(async () => {
  [source, target] = [new PGlite(), new PGlite()];
  await Promise.all([source.exec(DDL), target.exec(DDL)]);
});
afterEach(async () => {
  await Promise.all([source.close(), target.close()]);
});

const BIG = "9007199254740993"; // 2^53 + 1: lost if parsed as a JS number
const insert = (db: PGlite, rows: string) =>
  db.exec(
    `INSERT INTO sessions.tokens (org_id, id, secret, seen_local, seen_at, meta, tags, label) VALUES ${rows}`,
  );
const dump = async (db: PGlite) =>
  (
    await db.query<Record<string, unknown>>(
      `SELECT org_id, id::text, encode(secret, 'hex') AS secret, seen_local::text,
              seen_at::text, meta, tags, label, label_upper
         FROM sessions.tokens ORDER BY org_id, id`,
    )
  ).rows;

describe("mirror (T0 copy set)", () => {
  it("makes the target equal to the source across pages, types intact", async () => {
    await insert(
      source,
      `('a', 1, '\\xdeadbeef', '2026-10-30 12:34:56.789', '2026-10-30 12:00:00+00', '{"k":[1,{"x":null}]}', '{x,"y z"}', 'one'),
       ('a', ${BIG}, NULL, NULL, NULL, NULL, NULL, 'big'),
       ('b', 2, '\\x00', '2026-01-01 00:00:00', '2026-01-01 00:00:00+00', '[]', '{}', 'two'),
       ('c', 3, NULL, NULL, NULL, NULL, NULL, 'three')`,
    );
    // Target has a stale copy of one row, a row the source deleted mid-range,
    // and target-only rows past the source's last key.
    await insert(
      target,
      `('a', 1, '\\x01', NULL, NULL, NULL, NULL, 'old'),
       ('a', 5, NULL, NULL, NULL, NULL, NULL, 'gone'),
       ('d', 9, NULL, NULL, NULL, NULL, NULL, 'tail-1'),
       ('e', 9, NULL, NULL, NULL, NULL, NULL, 'tail-2')`,
    );
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    expect(plan.columns).not.toContain("label_upper");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 2,
    });
    expect(await dump(target)).toEqual(await dump(source));
    expect(result).toMatchObject({ complete: true, read: 4, deleted: 3 });

    // A catch-up pass with nothing changed writes nothing.
    const again = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 2,
    });
    expect(again).toMatchObject({ written: 0, deleted: 0, read: 4 });
  });

  it("keeps rows only the target has when the table is shared", async () => {
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'from-neon')`);
    await insert(
      target,
      `('a', 1, NULL, NULL, NULL, NULL, NULL, 'stale'),
       ('p', 7, NULL, NULL, NULL, NULL, NULL, 'pipeline-only')`,
    );
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 10,
      keepTargetOnly: true,
    });
    expect(result).toMatchObject({ written: 1, deleted: 0 });
    expect((await dump(target)).map((row) => row.label)).toEqual([
      "from-neon",
      "pipeline-only",
    ]);
  });

  it("changes nothing on a dry run", async () => {
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'one')`);
    await insert(target, `('z', 1, NULL, NULL, NULL, NULL, NULL, 'stale')`);
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 10,
      dryRun: true,
    });
    expect(result).toMatchObject({ read: 1, written: 0, deleted: 1 });
    expect(await dump(target)).toHaveLength(1);
    expect((await dump(target))[0]).toMatchObject({ org_id: "z" });
  });
});

describe("backfill (history after T0)", () => {
  it("inserts missing rows newest first and never touches rows written after T0", async () => {
    await insert(
      source,
      `('a', 1, NULL, NULL, NULL, NULL, NULL, 'h1'),
       ('a', 2, NULL, NULL, NULL, NULL, NULL, 'h2'),
       ('a', 3, NULL, NULL, NULL, NULL, NULL, 'h3-pre-T0'),
       ('b', 1, NULL, NULL, NULL, NULL, NULL, 'h4')`,
    );
    // Written on Supabase after T0, same key as a source row: fresh data wins.
    await insert(target, `('a', 3, NULL, NULL, NULL, NULL, NULL, 'fresh-after-T0')`);
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const pages: unknown[] = [];
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 2,
      onPage: async (progress) => {
        pages.push(progress.after);
      },
    });
    expect(result).toMatchObject({ complete: true, read: 4, written: 3, deleted: 0 });
    // Newest key first: the first page ends at ('a','3'), then ('a','1').
    expect(pages.slice(0, 2)).toEqual([
      ["a", "3"],
      ["a", "1"],
    ]);
    const labels = (await dump(target)).map((row) => row.label);
    expect(labels).toEqual(["h1", "h2", "fresh-after-T0", "h4"]);
  });

  it("resumes from a checkpoint without rereading finished pages", async () => {
    await insert(
      source,
      `('a', 1, NULL, NULL, NULL, NULL, NULL, 'x'), ('a', 2, NULL, NULL, NULL, NULL, NULL, 'y'),
       ('a', 3, NULL, NULL, NULL, NULL, NULL, 'z')`,
    );
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const resumed = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 10,
      after: ["a", "3"],
    });
    expect(resumed).toMatchObject({ read: 2, written: 2 });
    expect((await dump(target)).map((row) => row.id)).toEqual(["1", "2"]);
  });
});

describe("check (Neon fallback removal criterion)", () => {
  it("counts source rows Supabase lacks, NULL keys included, without writing", async () => {
    const ddl = `CREATE TABLE sessions.rollups (
      org_id text NOT NULL, session_id text NOT NULL, project_id text, n int);
      CREATE UNIQUE INDEX rollups_key ON sessions.rollups (org_id, session_id, project_id) NULLS NOT DISTINCT;`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await source.exec(`INSERT INTO sessions.rollups VALUES
      ('o', 's1', NULL, 1), ('o', 's2', 'p', 2), ('o', 's3', NULL, 3)`);
    // Present (a changed value is fine after T0) and one missing NULL-keyed row.
    await target.exec(`INSERT INTO sessions.rollups VALUES ('o', 's1', NULL, 99), ('o', 's2', 'p', 2)`);
    const plan = await planTable(source as Queryable, target as Queryable, "rollups");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "check",
      chunkSize: 2,
    });
    expect(result).toMatchObject({ read: 3, missing: 1, written: 0 });
    expect((await target.query("SELECT count(*)::int AS n FROM sessions.rollups")).rows).toEqual([
      { n: 2 },
    ]);
    await copyTable(source as Queryable, target as Queryable, plan, { mode: "backfill", chunkSize: 2 });
    expect(
      await copyTable(source as Queryable, target as Queryable, plan, { mode: "check", chunkSize: 2 }),
    ).toMatchObject({ missing: 0 });
  });
});

describe("deletions made on Supabase after T0", () => {
  const FENCE_DDL = `CREATE TABLE sessions.deleted_upload_sessions (
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
      PRIMARY KEY (org_id, event_id));
    CREATE TABLE sessions.session_rollups (
      org_id text NOT NULL, session_id text NOT NULL, n int, PRIMARY KEY (org_id, session_id));`;

  it("never resurrects a deleted upload or a reset user's history", async () => {
    await Promise.all([source.exec(FENCE_DDL), target.exec(FENCE_DDL)]);
    await source.exec(`INSERT INTO sessions.convergence_events VALUES
        ('o', 'w', 'u1', 'claude', 's1', 'e1'), ('o', 'w', 'u1', 'claude', 's1b', 'e1b'),
        ('o', 'w', 'u2', 'claude', 's2', 'e2'), ('o', 'w', 'u3', 'claude', 's3', 'e3');
      INSERT INTO sessions.session_rollups VALUES ('o', 's1', 1), ('o', 's2', 2), ('o', 's3', 3);
      -- u3 reset before T0: already on both sides; their later history stays.
      INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u3', 'r-pre', 0);`);
    await target.exec(`INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u3', 'r-pre', 0);
      -- After T0 on Supabase: u1 deleted session s1, and u2 reset everything.
      INSERT INTO sessions.deleted_upload_sessions VALUES ('o', 'w', 'u1', 'origin', 'claude', 's1');
      INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u2', 'r-post', 1);`);
    await prepareTargetSession(target as Queryable);
    const fence = await loadFence(source as Queryable);
    for (const table of ["convergence_events", "session_rollups"]) {
      const plan = await planTable(source as Queryable, target as Queryable, table);
      await copyTable(source as Queryable, target as Queryable, plan, {
        mode: "backfill",
        chunkSize: 2,
        fence,
      });
    }
    const events = (
      await target.query<{ event_id: string }>(
        "SELECT event_id FROM sessions.convergence_events ORDER BY event_id",
      )
    ).rows.map((row) => row.event_id);
    expect(events).toEqual(["e1b", "e3"]); // s1 deleted, all of u2 reset
    const rollups = (
      await target.query<{ session_id: string }>(
        "SELECT session_id FROM sessions.session_rollups ORDER BY session_id",
      )
    ).rows.map((row) => row.session_id);
    // Without user columns, u2's sessions are matched through Neon's events.
    expect(rollups).toEqual(["s3"]);
  });
});

describe("deletions committed while the backfill runs", () => {
  it("are honoured by every later page, for user-bearing and session-only tables", async () => {
    const ddl = `CREATE TABLE sessions.deleted_upload_sessions (
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
        PRIMARY KEY (org_id, event_id));
      CREATE TABLE sessions.session_rollups (
        org_id text NOT NULL, session_id text NOT NULL, n int, PRIMARY KEY (org_id, session_id));`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await source.exec(`INSERT INTO sessions.convergence_events VALUES
        ('o', 'w', 'u1', 'claude', 's1', 'e1'), ('o', 'w', 'u2', 'claude', 's2', 'e2'),
        ('o', 'w', 'u3', 'claude', 's3', 'e3');
      INSERT INTO sessions.session_rollups VALUES ('o', 's1', 1), ('o', 's2', 2), ('o', 's3', 3);`);
    await prepareTargetSession(target as Queryable);
    // The fence is prepared with no deletions on Supabase yet...
    const fence = await loadFence(source as Queryable);
    // ...then, mid-backfill, u2 resets and u1 deletes s1 on Supabase.
    await target.exec(`INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u2', 'r-late', 1);
      INSERT INTO sessions.deleted_upload_sessions VALUES ('o', 'w', 'u1', 'origin', 'claude', 's1');`);
    const sessionsOf = async (table: string) => {
      const plan = await planTable(source as Queryable, target as Queryable, table);
      await copyTable(source as Queryable, target as Queryable, plan, {
        mode: "backfill",
        chunkSize: 1,
        fence,
      });
      return (
        await target.query<{ session_id: string }>(
          `SELECT session_id FROM sessions.${table} ORDER BY session_id`,
        )
      ).rows.map((row) => row.session_id);
    };
    expect(await sessionsOf("convergence_events")).toEqual(["s3"]);
    expect(await sessionsOf("session_rollups")).toEqual(["s3"]);
  });

  it("locks per user for session-only tables too, through their Neon owners", async () => {
    const plan = {
      table: "session_rollups",
      key: ["org_id", "session_id"],
      order: ['"org_id"', '"session_id"'],
      nullableKey: false,
      columns: ["org_id", "session_id", "n"],
    };
    expect(deletionLockSql(plan)).toMatch(/pg_advisory_xact_lock[\s\S]*jsonb_to_recordset\(\$1/);
    expect(deletionLockSql({ ...plan, table: "patterns", columns: ["org_id", "id"] })).toBeNull();
  });
});

describe("check honours intentional deletions; derived catalog tables are not copied", () => {
  it("does not count rows a post-T0 tombstone or reset removed as missing", async () => {
    const ddl = `CREATE TABLE sessions.deleted_upload_sessions (
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
        PRIMARY KEY (org_id, event_id));`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await source.exec(`INSERT INTO sessions.convergence_events VALUES
      ('o', 'w', 'u1', 'claude', 's1', 'e1'), ('o', 'w', 'u2', 'claude', 's2', 'e2'),
      ('o', 'w', 'u3', 'claude', 's3', 'e3')`);
    // After T0: u1 deleted s1, u2 reset; e3 was simply never backfilled.
    await target.exec(`INSERT INTO sessions.deleted_upload_sessions VALUES ('o', 'w', 'u1', 'origin', 'claude', 's1');
      INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u2', 'r', 1);`);
    const plan = await planTable(source as Queryable, target as Queryable, "convergence_events");
    const fence = await loadFence(source as Queryable);
    const checked = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "check",
      chunkSize: 2,
      fence,
    });
    expect(checked).toMatchObject({ read: 3, missing: 1 });
    // Without the fence every absent row counts.
    expect(
      await copyTable(source as Queryable, target as Queryable, plan, { mode: "check", chunkSize: 2 }),
    ).toMatchObject({ missing: 3 });
  });

  it("refuses to copy catalog-derived tables", async () => {
    const ddl = "CREATE TABLE sessions.session_catalog_agents (org_id text, session_id text, agent_name text, PRIMARY KEY (org_id, session_id));";
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    const plan = await planTable(source as Queryable, target as Queryable, "session_catalog_agents");
    for (const mode of ["backfill", "mirror"] as const)
      await expect(
        copyTable(source as Queryable, target as Queryable, plan, { mode, chunkSize: 5 }),
      ).rejects.toThrow("reprojected by migration:reproject");
  });
});

describe("newest-first order and bound checkpoints", () => {
  it("backfills newest first by an indexed time column, else by key", async () => {
    const ddl = `CREATE TABLE sessions.events (org_id text NOT NULL, id text NOT NULL,
        ts timestamptz NOT NULL, PRIMARY KEY (org_id, id));
      CREATE INDEX events_ts ON sessions.events (ts);`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    // Key order (a, b, c) is not time order (b newest, then c, then a).
    await source.exec(`INSERT INTO sessions.events VALUES
      ('o', 'a', '2026-10-01T00:00:00Z'), ('o', 'b', '2026-10-30T00:00:00Z'), ('o', 'c', '2026-10-15T00:00:00Z')`);
    const timed = await planTable(source as Queryable, target as Queryable, "events", { newestByTime: true });
    expect(timed.timeColumn).toBe("ts");
    const seen: string[] = [];
    await copyTable(source as Queryable, target as Queryable, timed, {
      mode: "backfill",
      chunkSize: 1,
      onPage: async (progress) => {
        if (progress.after) seen.push(String(progress.after.at(-1)));
      },
    });
    // Pages arrive newest first; the completion callback repeats the last position.
    expect(seen.slice(0, 3)).toEqual(["b", "c", "a"]);
    const keyed = await planTable(source as Queryable, target as Queryable, "events");
    expect(keyed.timeColumn).toBeUndefined();
    await expect(
      copyTable(source as Queryable, target as Queryable, timed, { mode: "mirror", chunkSize: 1 }),
    ).rejects.toThrow("mirror pages by key");
  });

  it("binds checkpoints to the databases and the exact plan", async () => {
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    expect(planHash(plan, 500)).toBe(planHash(plan, 500));
    expect(planHash(plan, 500)).not.toBe(planHash(plan, 501));
    expect(planHash(plan, 500)).not.toBe(planHash({ ...plan, columns: plan.columns.slice(1) }, 500));
    const { mkdtempSync, writeFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const { tmpdir } = await import("node:os");
    const file = join(mkdtempSync(join(tmpdir(), "copy-ckpt-")), "c.json");
    writeFileSync(file, JSON.stringify({ version: 2, mode: "backfill", endpointHash: "aaa", tables: {} }));
    await expect(loadCheckpoint(file, "backfill", "bbb")).rejects.toThrow("different databases");
    await expect(loadCheckpoint(file, "check", "aaa")).rejects.toThrow("different version or mode");
    expect((await loadCheckpoint(file, "backfill", "aaa")).tables).toEqual({});
  });
});

describe("target session", () => {
  it("lands rows without firing triggers, so history wakes nothing", async () => {
    await target.exec(`CREATE TABLE sessions.wakeups (id bigint);
      CREATE FUNCTION sessions.wake() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN INSERT INTO sessions.wakeups VALUES (NEW.id); RETURN NEW; END $$;
      CREATE TRIGGER wake AFTER INSERT ON sessions.tokens
        FOR EACH ROW EXECUTE FUNCTION sessions.wake();`);
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'h')`);
    await prepareTargetSession(target as Queryable);
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 10,
    });
    expect((await dump(target)).map((row) => row.label)).toEqual(["h"]);
    expect((await target.query("SELECT * FROM sessions.wakeups")).rows).toEqual([]);
    // A live session (default role) still fires them.
    await target.exec("SET session_replication_role = origin");
    await insert(target, `('b', 2, NULL, NULL, NULL, NULL, NULL, 'live')`);
    expect((await target.query("SELECT id::text FROM sessions.wakeups")).rows).toEqual([
      { id: "2" },
    ]);
  });

  it("fails closed when triggers cannot be disabled", async () => {
    const refusing: Queryable = {
      query: async (sql: string) =>
        sql.startsWith("SHOW")
          ? ({ rows: [{ session_replication_role: "origin" }] } as never)
          : ({ rows: [] } as never),
    };
    await expect(prepareTargetSession(refusing)).rejects.toThrow(
      "could not disable triggers",
    );
  });
});

describe("guarded target session (mirror without session_replication_role)", () => {
  const GUARD = `CREATE FUNCTION sessions.upload_cleanup_ready() RETURNS boolean LANGUAGE sql AS 'SELECT true';
    CREATE FUNCTION sessions.guard_delivery_cleanup() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN IF NOT sessions.upload_cleanup_ready() THEN RAISE EXCEPTION 'upload_cleanup_not_ready'; END IF;
         RETURN COALESCE(NEW, OLD); END $$;
    CREATE TABLE sessions.delivery_origins (org_id text NOT NULL, id bigint NOT NULL, PRIMARY KEY (org_id, id));
    CREATE TRIGGER guard_delivery_origin_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.delivery_origins
      FOR EACH ROW EXECUTE FUNCTION sessions.guard_delivery_cleanup();`;

  it("accepts tables with no triggers or only reviewed guards, then mirrors with live triggers", async () => {
    await Promise.all([source.exec(GUARD), target.exec(GUARD)]);
    await source.exec(`INSERT INTO sessions.delivery_origins VALUES ('a', 1), ('a', 2)`);
    await target.exec(`INSERT INTO sessions.delivery_origins VALUES ('a', 3)`);
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'h')`);
    await prepareGuardedTargetSession(target as Queryable, ["tokens", "delivery_origins"]);
    for (const table of ["tokens", "delivery_origins"]) {
      const plan = await planTable(source as Queryable, target as Queryable, table);
      await copyTable(source as Queryable, target as Queryable, plan, { mode: "mirror", chunkSize: 10 });
    }
    expect((await target.query("SELECT id::text FROM sessions.delivery_origins ORDER BY id")).rows).toEqual([
      { id: "1" },
      { id: "2" },
    ]);
    expect((await dump(target)).map((row) => row.label)).toEqual(["h"]);
    expect((await target.query("SHOW session_replication_role")).rows).toEqual([
      { session_replication_role: "origin" },
    ]);
  });

  it("refuses a table with a trigger that is not a reviewed guard", async () => {
    await target.exec(`CREATE TABLE sessions.wakeups (id bigint);
      CREATE FUNCTION sessions.wake() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN INSERT INTO sessions.wakeups VALUES (NEW.id); RETURN NEW; END $$;
      CREATE TRIGGER wake AFTER INSERT ON sessions.tokens FOR EACH ROW EXECUTE FUNCTION sessions.wake();`);
    await expect(prepareGuardedTargetSession(target as Queryable, ["tokens"])).rejects.toThrow(
      "not reviewed guards: tokens.wake",
    );
  });

  it("refuses a guard name bound to a different function", async () => {
    await target.exec(`CREATE FUNCTION sessions.upload_cleanup_ready() RETURNS boolean LANGUAGE sql AS 'SELECT true';
      CREATE TABLE sessions.delivery_origins (org_id text NOT NULL, id bigint NOT NULL, PRIMARY KEY (org_id, id));
      CREATE FUNCTION sessions.wake() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
      CREATE TRIGGER guard_delivery_origin_cleanup BEFORE INSERT ON sessions.delivery_origins
        FOR EACH ROW EXECUTE FUNCTION sessions.wake();`);
    await expect(
      prepareGuardedTargetSession(target as Queryable, ["delivery_origins"]),
    ).rejects.toThrow("not reviewed guards: delivery_origins.guard_delivery_origin_cleanup");
  });

  it("ignores disabled and replica-only triggers, which do not fire for this session", async () => {
    await target.exec(`CREATE FUNCTION sessions.wake() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
      CREATE TRIGGER off_t BEFORE INSERT ON sessions.tokens FOR EACH ROW EXECUTE FUNCTION sessions.wake();
      CREATE TRIGGER replica_t BEFORE INSERT ON sessions.tokens FOR EACH ROW EXECUTE FUNCTION sessions.wake();
      ALTER TABLE sessions.tokens DISABLE TRIGGER off_t;
      ALTER TABLE sessions.tokens ENABLE REPLICA TRIGGER replica_t;`);
    await expect(prepareGuardedTargetSession(target as Queryable, ["tokens"])).resolves.toBeUndefined();
  });

  it("refuses an ALWAYS trigger, which fires even under replica", async () => {
    await target.exec(`CREATE FUNCTION sessions.wake() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
      CREATE TRIGGER always_t BEFORE INSERT ON sessions.tokens FOR EACH ROW EXECUTE FUNCTION sessions.wake();
      ALTER TABLE sessions.tokens ENABLE ALWAYS TRIGGER always_t;`);
    await expect(prepareGuardedTargetSession(target as Queryable, ["tokens"])).rejects.toThrow(
      "tokens.always_t",
    );
  });

  it("refuses tables that take part in a foreign key, as parent or child", async () => {
    await target.exec(`CREATE TABLE sessions.children (org_id text, id bigint,
      FOREIGN KEY (org_id, id) REFERENCES sessions.tokens (org_id, id) ON DELETE CASCADE);`);
    await expect(prepareGuardedTargetSession(target as Queryable, ["tokens"])).rejects.toThrow(
      "foreign key: tokens",
    );
    await expect(prepareGuardedTargetSession(target as Queryable, ["children"])).rejects.toThrow(
      "foreign key: children",
    );
  });

  it("refuses the guard's table until upload cleanup is ready", async () => {
    await target.exec(GUARD.replace("'SELECT true'", "'SELECT false'"));
    await expect(
      prepareGuardedTargetSession(target as Queryable, ["delivery_origins"]),
    ).rejects.toThrow("upload cleanup rollout ready");
  });

  it("refuses delivery_origins when its required guard is missing", async () => {
    await target.exec(GUARD.split("CREATE TRIGGER")[0]);
    await expect(
      prepareGuardedTargetSession(target as Queryable, ["delivery_origins"]),
    ).rejects.toThrow("needs these guards enabled: delivery_origins.guard_delivery_origin_cleanup");
  });

  it("refuses delivery_origins when its required guard is disabled", async () => {
    await target.exec(GUARD);
    await target.exec("ALTER TABLE sessions.delivery_origins DISABLE TRIGGER guard_delivery_origin_cleanup");
    await expect(
      prepareGuardedTargetSession(target as Queryable, ["delivery_origins"]),
    ).rejects.toThrow("needs these guards enabled: delivery_origins.guard_delivery_origin_cleanup");
  });

  it("refuses delivery_origins when its required guard is replica-only", async () => {
    await target.exec(GUARD);
    await target.exec("ALTER TABLE sessions.delivery_origins ENABLE REPLICA TRIGGER guard_delivery_origin_cleanup");
    await expect(
      prepareGuardedTargetSession(target as Queryable, ["delivery_origins"]),
    ).rejects.toThrow("needs these guards enabled");
  });

  it("does not require the guard for tables it does not belong to", async () => {
    await expect(prepareGuardedTargetSession(target as Queryable, ["tokens"])).resolves.toBeUndefined();
  });

  it("refuses a session already in replica", async () => {
    await target.exec("SET session_replication_role = replica");
    await expect(prepareGuardedTargetSession(target as Queryable, ["tokens"])).rejects.toThrow(
      "session_replication_role = origin",
    );
  });
});

describe("tables keyed by a unique index with nullable columns", () => {
  it("backfills every row, NULL keys included, and refuses to mirror", async () => {
    const ddl = `CREATE TABLE sessions.rollups (
      org_id text NOT NULL, session_id text NOT NULL, project_id text, n int);
      CREATE UNIQUE INDEX rollups_key ON sessions.rollups (org_id, session_id, project_id) NULLS NOT DISTINCT;`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await source.exec(`INSERT INTO sessions.rollups VALUES
      ('o', 's1', NULL, 1), ('o', 's1', 'p', 2), ('o', 's1', '', 3),
      ('o', 's2', NULL, 4), ('o', 's3', 'p', 5), ('o', 's3', NULL, 6)`);
    // Written after T0 with a NULL in its key: must survive.
    await target.exec(`INSERT INTO sessions.rollups VALUES ('o', 's2', NULL, 40)`);
    const plan = await planTable(source as Queryable, target as Queryable, "rollups");
    expect(plan.nullableKey).toBe(true);
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 2,
    });
    expect(result).toMatchObject({ read: 6, written: 5 });
    const rows = (
      await target.query<{ n: number }>("SELECT n FROM sessions.rollups ORDER BY n")
    ).rows.map((row) => row.n);
    expect(rows).toEqual([1, 2, 3, 5, 6, 40]);
    await expect(
      copyTable(source as Queryable, target as Queryable, plan, {
        mode: "mirror",
        chunkSize: 2,
      }),
    ).rejects.toThrow("mirror needs a key without NULLs");
  });

  it("refuses a table with no usable key", async () => {
    const ddl = `CREATE TABLE sessions.loose (a text, b text);
      CREATE UNIQUE INDEX loose_key ON sessions.loose (a, b);`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await expect(
      planTable(source as Queryable, target as Queryable, "loose"),
    ).rejects.toThrow("no primary or usable unique key");
  });
});

describe("plans fail closed", () => {
  it("refuses a target missing a source column or with a different key", async () => {
    await target.exec("ALTER TABLE sessions.tokens DROP COLUMN tags");
    await expect(
      planTable(source as Queryable, target as Queryable, "tokens"),
    ).rejects.toThrow("target lacks 1 source column");
    await target.exec(`ALTER TABLE sessions.tokens ADD COLUMN tags text[];
      ALTER TABLE sessions.tokens DROP CONSTRAINT tokens_pkey;
      ALTER TABLE sessions.tokens ADD PRIMARY KEY (id, org_id);`);
    await expect(
      planTable(source as Queryable, target as Queryable, "tokens"),
    ).rejects.toThrow("keys differ");
  });
});
