/**
 * The V1 projector precedence contract (approved; provenance in
 * docs/session-search-index-contract.md, "Projector precedence"), driven through
 * projectSessionDirectory from crafted session_index pointer rows. In-process
 * PGlite with every supabase migration; synthetic data.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import {
  eraseSessionDirectory,
  propagatePointerTombstone,
  writeShortSummary,
} from "../session-directory-summary.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const KEY = { orgId: "o", workspaceId: "w", harness: "codex", sessionId: "s" };

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  return db;
}

type Pointer = {
  machine: string;
  user?: string;
  created: string;
  last: string;
  revision?: number;
  title?: string;
  state?: string;
  events?: number;
  inputTokens?: number;
  indexed?: string;
  canonicalSession?: string;
  canonicalMachine?: string;
};

async function upsertPointer(db: PGlite, p: Pointer) {
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        title, state, event_count, input_tokens, storage_provider, bucket, manifest_key, trace_id,
        revision, indexed_at, deleted_at, canonical_session_id, canonical_machine_id)
     VALUES ('o', 'w', 'codex', 's', $1, $2, $3::timestamptz, $4::timestamptz, $5, $6, $7, $8,
             's3', 'bucket', 'key/' || $1, '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', $9,
             coalesce($10::timestamptz, now()), NULL, $11, $12)
     ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
       user_id = excluded.user_id, created_at = excluded.created_at,
       last_activity_at = excluded.last_activity_at, title = excluded.title, state = excluded.state,
       event_count = excluded.event_count, input_tokens = excluded.input_tokens,
       revision = excluded.revision, indexed_at = excluded.indexed_at, deleted_at = NULL,
       canonical_session_id = excluded.canonical_session_id,
       canonical_machine_id = excluded.canonical_machine_id`,
    [
      p.machine,
      p.user ?? `u-${p.machine}`,
      p.created,
      p.last,
      p.title ?? `title ${p.machine}`,
      p.state ?? "active",
      p.events ?? 1,
      p.inputTokens ?? 0,
      p.revision ?? 1,
      p.indexed ?? null,
      p.canonicalSession ?? null,
      p.canonicalMachine ?? null,
    ],
  );
}

async function tombstonePointer(db: PGlite, machine: string, at = "now()") {
  await db.query(
    `UPDATE sessions.session_index SET deleted_at = ${at}, indexed_at = ${at}
      WHERE org_id = 'o' AND workspace_id = 'w' AND harness = 'codex' AND session_id = 's' AND machine_id = $1`,
    [machine],
  );
}

type Row = Record<string, unknown>;
const row = async (db: PGlite): Promise<Row | undefined> =>
  (
    await db.query<Row>(
      "SELECT * FROM sessions.session_directory WHERE session_id = 's'",
    )
  ).rows[0];
const members = async (db: PGlite, table: "users" | "machines") =>
  (
    await db.query<{ v: string; at: Date }>(
      `SELECT ${table === "users" ? "user_id" : "machine_id"} AS v, last_activity_at AS at
       FROM sessions.session_directory_${table} WHERE session_id = 's' ORDER BY 1`,
    )
  ).rows;
const iso = (v: unknown) => (v as Date).toISOString();

describe("V1 projector precedence", () => {
  test("stable historical created_at: the earliest ever projected, kept after its pointer is tombstoned", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
    });
    await upsertPointer(db, {
      machine: "m2",
      created: "2026-10-01T11:00:00Z",
      last: "2026-10-01T13:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    expect(iso((await row(db))!.created_at)).toBe("2026-10-01T10:00:00.000Z");
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    expect(iso((await row(db))!.created_at)).toBe("2026-10-01T10:00:00.000Z");
  });

  test("latest live pointer wins every scalar and counter, without summing", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      title: "older",
      state: "done",
      events: 5,
      inputTokens: 500,
    });
    await upsertPointer(db, {
      machine: "m2",
      created: "2026-10-01T11:00:00Z",
      last: "2026-10-01T13:00:00Z",
      title: "newer",
      state: "active",
      events: 3,
      inputTokens: 300,
    });
    await projectSessionDirectory(db, KEY);
    let current = (await row(db))!;
    expect([
      current.title,
      current.state,
      current.event_count,
      current.input_tokens,
    ]).toEqual(["newer", "active", 3, 300]);
    expect(iso(current.last_activity_at)).toBe("2026-10-01T13:00:00.000Z");
    // The winner's pointer is tombstoned: the remaining live pointer wins, still not summed.
    await tombstonePointer(db, "m2");
    await projectSessionDirectory(db, KEY);
    current = (await row(db))!;
    expect([
      current.title,
      current.state,
      current.event_count,
      current.input_tokens,
    ]).toEqual(["older", "done", 5, 500]);
    expect(iso(current.last_activity_at)).toBe("2026-10-01T12:00:00.000Z");
  });

  test("deterministic ties: latest activity, then highest revision, then highest machine_id", async () => {
    const db = await database();
    const same = "2026-10-01T12:00:00Z";
    await upsertPointer(db, {
      machine: "m1",
      created: same,
      last: same,
      revision: 3,
      title: "m1 rev 3",
    });
    await upsertPointer(db, {
      machine: "m2",
      created: same,
      last: same,
      revision: 2,
      title: "m2 rev 2",
    });
    await projectSessionDirectory(db, KEY);
    expect((await row(db))!.title).toBe("m1 rev 3");
    await upsertPointer(db, {
      machine: "m2",
      created: same,
      last: same,
      revision: 3,
      title: "m2 rev 3",
    });
    await projectSessionDirectory(db, KEY);
    expect((await row(db))!.title).toBe("m2 rev 3");
    // Repeating the projection changes nothing.
    const before = await row(db);
    await projectSessionDirectory(db, KEY);
    const after = await row(db);
    expect({ ...after, projected_at: null }).toEqual({
      ...before,
      projected_at: null,
    });
  });

  test("memberships are the union of LIVE pointers and carry the row's last_activity_at", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      user: "u1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
    });
    await upsertPointer(db, {
      machine: "m2",
      user: "u2",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T13:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    let current = (await row(db))!;
    expect(current.user_ids).toEqual(["u1", "u2"]);
    expect(current.machine_ids).toEqual(["m1", "m2"]);
    expect(
      (await members(db, "users")).map((m) => [m.v, m.at.toISOString()]),
    ).toEqual([
      ["u1", "2026-10-01T13:00:00.000Z"],
      ["u2", "2026-10-01T13:00:00.000Z"],
    ]);
    await tombstonePointer(db, "m2");
    await projectSessionDirectory(db, KEY);
    current = (await row(db))!;
    expect(current.user_ids).toEqual(["u1"]);
    expect(current.machine_ids).toEqual(["m1"]);
    expect((await members(db, "users")).map((m) => m.v)).toEqual(["u1"]);
    expect((await members(db, "machines")).map((m) => m.v)).toEqual(["m1"]);
  });

  test("uses canonical Kimi-style ids and excludes the unknown user placeholder", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "machine_encoded",
      user: "unknown",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      canonicalSession: "kimi:s",
      canonicalMachine: "machine:encoded",
    });
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: true,
      tombstoned: false,
    });
    const canonical = (
      await db.query<Row>(
        "SELECT * FROM sessions.session_directory WHERE session_id = 'kimi:s'",
      )
    ).rows[0]!;
    expect(canonical.machine_ids).toEqual(["machine:encoded"]);
    expect(canonical.user_ids).toEqual([]);
    expect(
      await db.query(
        "SELECT 1 FROM sessions.session_directory WHERE session_id = 's'",
      ),
    ).toMatchObject({ rows: [] });
    expect(
      (
        await db.query<{ machine_id: string }>(
          "SELECT machine_id FROM sessions.session_directory_machines WHERE session_id = 'kimi:s'",
        )
      ).rows,
    ).toEqual([{ machine_id: "machine:encoded" }]);
    expect(
      (
        await db.query(
          "SELECT 1 FROM sessions.session_directory_users WHERE session_id = 'kimi:s'",
        )
      ).rows,
    ).toEqual([]);
    expect(await propagatePointerTombstone(db, KEY)).toEqual({
      found: true,
      tombstoned: false,
    });
    expect(
      await writeShortSummary(db, {
        ...KEY,
        summary: "Canonical summary",
        source: "bob-costas/extractive/v1",
        generatedAt: new Date("2026-10-01T12:30:00Z"),
      }),
    ).toEqual({ written: true, scrubVersion: 2 });
    expect(
      (
        await db.query<{ short_summary: string }>(
          "SELECT short_summary FROM sessions.session_directory WHERE session_id = 'kimi:s'",
        )
      ).rows,
    ).toEqual([{ short_summary: "Canonical summary" }]);

    await db.query(
      "UPDATE sessions.session_index SET user_id = 'user-real' WHERE session_id = 's'",
    );
    await projectSessionDirectory(db, KEY);
    expect(
      (
        await db.query<{ user_id: string }>(
          "SELECT user_id FROM sessions.session_directory_users WHERE session_id = 'kimi:s'",
        )
      ).rows,
    ).toEqual([{ user_id: "user-real" }]);

    await tombstonePointer(db, "machine_encoded");
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: false,
      tombstoned: true,
    });
    expect(
      (
        await db.query<{ session_id: string; machine_id: string }>(
          "SELECT session_id, machine_id FROM sessions.session_directory_resurrection_fences WHERE session_id = 'kimi:s'",
        )
      ).rows,
    ).toEqual([{ session_id: "kimi:s", machine_id: "machine:encoded" }]);
  });

  test("hard-erases a canonical directory row through its physical session key", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "machine_encoded",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      canonicalSession: "kimi:s",
      canonicalMachine: "machine:encoded",
    });
    await projectSessionDirectory(db, KEY);
    expect(await eraseSessionDirectory(db, KEY)).toBe(1);
    expect((await db.query("SELECT 1 FROM sessions.session_directory")).rows).toEqual([]);
  });

  test("keeps a legacy physical-id row until a controlled rebuild removes it", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    await db.query(
      `UPDATE sessions.session_index
          SET canonical_session_id = 'kimi:s', canonical_machine_id = 'machine:one'
        WHERE session_id = 's'`,
    );
    await projectSessionDirectory(db, KEY);
    expect(
      (
        await db.query<{ session_id: string }>(
          "SELECT session_id FROM sessions.session_directory ORDER BY session_id",
        )
      ).rows,
    ).toEqual([{ session_id: "s" }]);
    expect((await row(db))!.machine_ids).toEqual(["machine:one"]);
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    expect(
      (
        await db.query<{ session_id: string; machine_id: string }>(
          "SELECT session_id, machine_id FROM sessions.session_directory_resurrection_fences",
        )
      ).rows,
    ).toEqual([{ session_id: "s", machine_id: "machine:one" }]);
  });

  test("fails closed when one physical scope claims two canonical session ids", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      canonicalSession: "kimi:one",
    });
    await upsertPointer(db, {
      machine: "m2",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T13:00:00Z",
      canonicalSession: "kimi:two",
    });
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: false,
      tombstoned: false,
    });
    expect(
      (await db.query("SELECT 1 FROM sessions.session_directory")).rows,
    ).toEqual([]);
  });

  /** Tombstone the logical row by deleting every pointer, then return its fences. */
  async function tombstoneAll(db: PGlite, machines: string[]) {
    for (const m of machines) await tombstonePointer(db, m);
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: false,
      tombstoned: true,
    });
    return (
      await db.query<{ machine_id: string; revision: string }>(
        `SELECT machine_id, revision::text FROM sessions.session_directory_resurrection_fences
          WHERE session_id = 's' ORDER BY machine_id`,
      )
    ).rows.map((r) => [r.machine_id, Number(r.revision)]);
  }

  test("tombstone when no live pointer remains, recording each pointer's revision as a fence", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      user: "u1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      revision: 4,
    });
    await upsertPointer(db, {
      machine: "m2",
      user: "u2",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T11:00:00Z",
      revision: 2,
    });
    await projectSessionDirectory(db, KEY);
    expect(await tombstoneAll(db, ["m1", "m2"])).toEqual([
      ["m1", 4],
      ["m2", 2],
    ]);
    const current = (await row(db))!;
    expect(current.deleted_at).not.toBeNull();
    expect([
      current.user_ids,
      current.machine_ids,
      current.search_text,
    ]).toEqual([[], [], ""]);
    expect(await members(db, "users")).toEqual([]);
  });

  test("a deleted pointer never revives: un-deleting it at its fenced revision does not resurrect", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      revision: 3,
    });
    await projectSessionDirectory(db, KEY);
    await tombstoneAll(db, ["m1"]);
    // The same pointer row cleared of its tombstone, even re-indexed later, is still revision 3.
    await db.query(`UPDATE sessions.session_index SET deleted_at = NULL, indexed_at = now() + interval '1 hour'
      WHERE session_id = 's' AND machine_id = 'm1'`);
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: false,
      tombstoned: false,
    });
    expect((await row(db))!.deleted_at).not.toBeNull();
  });

  test("a stale or equal revision cannot revive", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      revision: 5,
    });
    await projectSessionDirectory(db, KEY);
    await tombstoneAll(db, ["m1"]);
    for (const revision of [4, 5]) {
      await upsertPointer(db, {
        machine: "m1",
        created: "2026-10-01T10:00:00Z",
        last: "2026-10-02T12:00:00Z",
        revision,
        indexed: "2099-01-01T00:00:00Z",
      });
      expect(
        await projectSessionDirectory(db, KEY),
        `revision ${revision}`,
      ).toEqual({ projected: false, tombstoned: false });
      expect(
        (await row(db))!.deleted_at,
        `revision ${revision}`,
      ).not.toBeNull();
    }
  });

  test("a new, strictly higher revision resurrects; so does a machine that had no pointer", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      user: "u1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      revision: 5,
    });
    await projectSessionDirectory(db, KEY);
    await tombstoneAll(db, ["m1"]);
    await upsertPointer(db, {
      machine: "m1",
      user: "u1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-31T09:30:00Z",
      revision: 6,
      title: "back again",
    });
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: true,
      tombstoned: false,
    });
    let current = (await row(db))!;
    expect(current.deleted_at).toBeNull();
    expect(current.title).toBe("back again");
    expect(iso(current.created_at)).toBe("2026-10-01T10:00:00.000Z");
    expect(current.short_summary).toBeNull();
    expect((await members(db, "users")).map((m) => m.v)).toEqual(["u1"]);

    // Tombstoned again: the fence rises to 6; then a machine with no fence comes back.
    expect(await tombstoneAll(db, ["m1"])).toEqual([["m1", 6]]);
    await upsertPointer(db, {
      machine: "m9",
      user: "u9",
      created: "2026-11-01T09:00:00Z",
      last: "2026-11-01T09:30:00Z",
      revision: 1,
    });
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: true,
      tombstoned: false,
    });
    current = (await row(db))!;
    expect(current.deleted_at).toBeNull();
    expect(current.machine_ids).toEqual(["m9"]);
    expect(iso(current.created_at)).toBe("2026-10-01T10:00:00.000Z");
  });

  test("erasure deletes the fences with the row", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    await tombstoneAll(db, ["m1"]);
    await db.query(
      "DELETE FROM sessions.session_directory WHERE session_id = 's'",
    );
    expect(
      (
        await db.query(
          "SELECT 1 FROM sessions.session_directory_resurrection_fences",
        )
      ).rows,
    ).toEqual([]);
  });
});
