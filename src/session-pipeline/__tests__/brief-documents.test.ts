import { afterEach, expect, test } from "bun:test";
import {
  replayDatabase,
  addReplayPointer,
  REPLAY_FIXTURE_IDENTITY as i,
} from "./replay-fixture.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
const open: Awaited<ReturnType<typeof replayDatabase>>[] = [];
afterEach(async () => {
  for (const x of open.splice(0)) await x.pg.close();
});
const nonce = "09090909-0909-4909-8909-090909090909";
async function fresh() {
  const x = await replayDatabase();
  open.push(x);
  await addReplayPointer(x.db, {
    key: "96bulls/org_replay/v1/ws=ws_replay/m=machine_0/h=claude/d=2026/11/01/s=large_replay/manifest.json",
    digest: "a".repeat(64),
    physicalSession: "large_replay",
    physicalMachine: "machine_0",
    canonicalMachine: "machine:0",
  });
  await projectSessionDirectory(x.db, {
    orgId: i.orgId,
    workspaceId: i.workspaceId,
    harness: i.source,
    sessionId: "large_replay",
  });
  await x.db.query("SELECT sessions.set_session_brief_documents_live(true)");
  return x;
}
async function backfill(x: Awaited<ReturnType<typeof fresh>>, limit = 200) {
  return (
    await x.db.query<{ r: any }>(
      "SELECT sessions.backfill_session_brief_documents($1) r",
      [limit],
    )
  ).rows[0]!.r;
}
async function claim(x: Awaited<ReturnType<typeof fresh>>) {
  return (
    await x.db.query<{ r: any[] }>(
      "SELECT sessions.claim_session_brief_documents($1::uuid,200) r",
      [nonce],
    )
  ).rows[0]!.r;
}
async function ack(x: Awaited<ReturnType<typeof fresh>>, items: any[]) {
  return (
    await x.db.query<{ n: number }>(
      "SELECT sessions.ack_session_brief_documents($1::uuid,$2::jsonb) n",
      [
        nonce,
        JSON.stringify(
          items.map((v) => ({
            orgId: v.document.orgId,
            workspaceId: v.document.workspaceId,
            source: v.document.source,
            sessionId: v.document.sessionId,
            epoch: v.epoch,
            hash: v.hash,
          })),
        ),
      ],
    )
  ).rows[0]!.n;
}
test("brief-document queue accepts an empty workspace but rejects a missing or null workspace", async () => {
  const x = await replayDatabase();
  open.push(x);
  const document = {
    version: 1,
    orgId: "org_empty_workspace",
    workspaceId: "",
    source: "codex",
    sessionId: "session_empty_workspace",
  };
  await x.db.query(
    "SELECT sessions.queue_session_brief_document($1::jsonb)",
    [JSON.stringify(document)],
  );
  expect(
    (
      await x.db.query<{ n: number }>(
        "SELECT count(*)::int n FROM sessions.session_brief_document_outbox WHERE org_id=$1 AND workspace_id='' AND harness=$2 AND session_id=$3",
        [document.orgId, document.source, document.sessionId],
      )
    ).rows[0]!.n,
  ).toBe(1);
  for (const invalid of [
    (({ workspaceId: _workspaceId, ...rest }) => rest)(document),
    { ...document, workspaceId: null },
  ]) {
    await expect(
      x.db.query("SELECT sessions.queue_session_brief_document($1::jsonb)", [
        JSON.stringify(invalid),
      ]),
    ).rejects.toMatchObject({ code: "22023" });
  }
});
test("brief documents remain dormant until the owner activates publication", async () => {
  const x = await replayDatabase();
  open.push(x);
  expect(
    (
      await x.db.query<{ live: boolean; scopes: number; outbox: number }>(
        `SELECT c.live,
          (SELECT count(*)::int FROM sessions.session_brief_document_scopes) scopes,
          (SELECT count(*)::int FROM sessions.session_brief_document_outbox) outbox
         FROM sessions.session_brief_document_control c WHERE c.singleton`,
      )
    ).rows[0],
  ).toEqual({ live: false, scopes: 0, outbox: 0 });
  await addReplayPointer(x.db, {
    key: "96bulls/org_replay/v1/ws=ws_replay/m=machine_0/h=claude/d=2026/11/01/s=dormant/manifest.json",
    digest: "d".repeat(64),
    physicalSession: "dormant",
    physicalMachine: "machine_0",
    canonicalMachine: "machine:0",
  });
  await projectSessionDirectory(x.db, {
    orgId: i.orgId,
    workspaceId: i.workspaceId,
    harness: i.source,
    sessionId: "dormant",
  });
  await x.pg.exec("UPDATE sessions.session_directory SET title='dormant write'");
  expect(
    (
      await x.db.query<{ scopes: number; outbox: number }>(
        `SELECT
          (SELECT count(*)::int FROM sessions.session_brief_document_scopes) scopes,
          (SELECT count(*)::int FROM sessions.session_brief_document_outbox) outbox`,
      )
    ).rows[0],
  ).toEqual({ scopes: 0, outbox: 0 });
  await x.pg.exec("BEGIN; SET LOCAL ROLE service_role");
  await expect(
    x.pg.query("SELECT sessions.set_session_brief_documents_live(true)"),
  ).rejects.toMatchObject({ code: "42501" });
  await x.pg.exec("ROLLBACK");
  await x.db.query("SELECT sessions.set_session_brief_documents_live(true)");
  expect(
    (
      await x.db.query<{ live: boolean; scopes: number; outbox: number }>(
        `SELECT c.live,
          (SELECT count(*)::int FROM sessions.session_brief_document_scopes) scopes,
          (SELECT count(*)::int FROM sessions.session_brief_document_outbox) outbox
         FROM sessions.session_brief_document_control c WHERE c.singleton`,
      )
    ).rows[0],
  ).toEqual({ live: true, scopes: 1, outbox: 0 });
});
test("off-interval updates and deletions stay incomplete until a fresh bounded backfill", async () => {
  const x = await fresh();
  await backfill(x);
  await ack(x, await claim(x));

  expect(
    (
      await x.db.query<{ value: boolean }>(
        "SELECT sessions.set_session_brief_documents_live(false) value",
      )
    ).rows[0]!.value,
  ).toBe(false);
  // Repeating either state is a true no-op, not another cursor reset.
  expect(
    (
      await x.db.query<{ value: boolean }>(
        "SELECT sessions.set_session_brief_documents_live(false) value",
      )
    ).rows[0]!.value,
  ).toBe(false);
  await expect(backfill(x)).rejects.toMatchObject({ code: "55000" });
  await x.pg.exec(
    "UPDATE sessions.session_directory SET title='changed while publication was off'",
  );
  expect(
    (
      await x.db.query<{ initialized: boolean }>(
        "SELECT initialized FROM sessions.session_brief_document_scopes",
      )
    ).rows[0]!.initialized,
  ).toBe(false);

  await x.db.query("SELECT sessions.set_session_brief_documents_live(true)");
  await x.db.query("SELECT sessions.set_session_brief_documents_live(true)");
  expect((await backfill(x)).complete).toBe(true);
  const changed = await claim(x);
  expect(changed).toHaveLength(1);
  expect(changed[0].document.title).toBe(
    "changed while publication was off",
  );
  await ack(x, changed);

  await x.db.query("SELECT sessions.set_session_brief_documents_live(false)");
  await x.pg.exec("DELETE FROM sessions.session_directory");
  await x.db.query("SELECT sessions.set_session_brief_documents_live(true)");
  expect((await backfill(x)).complete).toBe(true);
  const deleted = await claim(x);
  expect(deleted).toHaveLength(1);
  expect(deleted[0].document).toMatchObject({
    deleted: true,
    title: "",
    summary: "",
    projectId: "",
    lastActivityAt: "1970-01-01T00:00:00.000000Z",
  });
});
test("relevant directory writes queue metadata once; publisher acknowledges exact epoch/hash", async () => {
  const x = await fresh();
  const b = await backfill(x);
  expect(b.complete).toBe(true);
  await x.pg.exec("UPDATE sessions.session_directory SET title='Alpha title'");
  const items = await claim(x);
  expect(items).toHaveLength(1);
  expect(items[0].document.title).toBe("Alpha title");
  expect(items[0].document).not.toHaveProperty("record");
  expect(await ack(x, items)).toBe(1);
  expect(
    (
      await x.pg.query<{ n: number }>(
        "SELECT count(*)::int n FROM sessions.session_brief_document_outbox WHERE epoch<>published_epoch",
      )
    ).rows[0]!.n,
  ).toBe(0);
  await x.pg.exec(
    "UPDATE sessions.session_directory SET projected_at=clock_timestamp()",
  );
  expect(await claim(x)).toHaveLength(0);
});
test("source mutation invalidates claimed epoch and cannot be acknowledged by older publication", async () => {
  const x = await fresh();
  await backfill(x);
  const old = await claim(x);
  await x.pg.exec("UPDATE sessions.session_directory SET title='new winner'");
  expect(await ack(x, old)).toBe(0);
  const current = await claim(x);
  expect(BigInt(current[0].epoch)).toBeGreaterThan(BigInt(old[0].epoch));
  expect(current[0].document.title).toBe("new winner");
  expect(await ack(x, current)).toBe(1);
});
test("hard erasure queues blank-text higher epoch tombstone and prevents stale ack", async () => {
  const x = await fresh();
  await backfill(x);
  await x.pg.exec(
    "UPDATE sessions.session_directory SET title='secret prior title'",
  );
  const before = await claim(x);
  await x.pg.exec("DELETE FROM sessions.session_directory");
  expect(await ack(x, before)).toBe(0);
  const erased = await claim(x);
  expect(erased).toHaveLength(1);
  expect(erased[0].document.deleted).toBe(true);
  expect(erased[0].document.title).toBe("");
  expect(erased[0].document.summary).toBe("");
  expect(BigInt(erased[0].epoch)).toBeGreaterThan(BigInt(before[0].epoch));
});
test("canonical/source identity change leaves a tombstone under the old key and a live new key", async () => {
  const x = await fresh();
  await backfill(x);
  await ack(x, await claim(x));
  await x.pg.exec(
    "WITH old AS(DELETE FROM sessions.session_directory RETURNING *) INSERT INTO sessions.session_directory SELECT (jsonb_populate_record(NULL::sessions.session_directory,to_jsonb(old)||jsonb_build_object('session_id','new:canonical','harness','codex'))).* FROM old",
  );
  const items = await claim(x);
  expect(items).toHaveLength(2);
  expect(
    items.find((v) => v.document.sessionId === i.sessionId)?.document.deleted,
  ).toBe(true);
  expect(
    items.find((v) => v.document.sessionId === "new:canonical")?.document
      .source,
  ).toBe("codex");
  expect(
    items.find((v) => v.document.sessionId === "new:canonical")?.document
      .deleted,
  ).toBe(false);
});
test("backend claims and direct table access are unavailable to the public search reader", async () => {
  const x = await fresh();
  await x.pg.exec("SET ROLE session_directory_search_worker");
  await expect(claim(x)).rejects.toMatchObject({ code: "42501" });
  await expect(
    x.pg.query("SELECT * FROM sessions.session_brief_document_outbox"),
  ).rejects.toMatchObject({ code: "42501" });
  await x.pg.exec("RESET ROLE");
});
async function eventReady(x: Awaited<ReturnType<typeof fresh>>) {
  await x.pg.exec(
    "UPDATE sessions.session_directory SET event_count=1,event_first_ts=created_at,event_last_ts='2026-11-01T10:00:00.123456Z',event_projection_version=1,event_projection_ready=true,event_projection_sources=(SELECT jsonb_object_agg(manifest_key,revision::text) FROM sessions.session_index),event_projected_at=clock_timestamp();UPDATE sessions.session_directory_event_pending SET dirty=false",
  );
}
async function reader<T>(
  x: Awaited<ReturnType<typeof fresh>>,
  work: () => Promise<T>,
  workspaces = [i.workspaceId],
) {
  await x.pg.exec(
    "INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://replay.invalid','replay-test') ON CONFLICT DO NOTHING;BEGIN;SET LOCAL ROLE session_directory_search_worker",
  );
  const now = Math.floor(Date.now() / 1000);
  try {
    await x.pg.query("SELECT set_config('request.jwt.claims',$1,true)", [
      JSON.stringify({
        role: "session_directory_search_worker",
        iss: "https://replay.invalid",
        aud: "replay-test",
        iat: now,
        exp: now + 30,
        app_metadata: { org_id: i.orgId, workspace_ids: workspaces },
      }),
    ]);
    return await work();
  } finally {
    await x.pg.exec("ROLLBACK");
  }
}
test("authority returns only exact authorized tuples, current hash and event timestamps", async () => {
  const x = await fresh();
  await eventReady(x);
  await backfill(x);
  const c = await claim(x);
  expect(c[0].document.lastActivityAt).toBe("2026-11-01T10:00:00.123456Z");
  expect(c[0].document.visible).toBe(true);
  await ack(x, c);
  const result = await reader(x, () =>
    x.pg.query<any>(
      "SELECT * FROM sessions.session_brief_document_authority($1,$2,$3)",
      [i.workspaceId, [i.source], [i.sessionId]],
    ),
  );
  expect(result.rows).toHaveLength(1);
  expect(result.rows[0].complete).toBe(true);
  expect(result.rows[0].documents).toHaveLength(1);
  expect(result.rows[0].documents[0].hash).toBe(c[0].hash);
  expect(result.rows[0].documents[0].directory.session_id).toBe(i.sessionId);
  await expect(
    reader(x, () =>
      x.pg.query(
        "SELECT * FROM sessions.session_brief_document_authority($1)",
        ["foreign-workspace"],
      ),
    ),
  ).rejects.toMatchObject({ code: "42501" });
});
test("dirty metadata and uninitialized backfill refuse complete authority even for empty candidates", async () => {
  const x = await fresh();
  await eventReady(x);
  let a = (
    await reader(x, () =>
      x.pg.query<any>(
        "SELECT * FROM sessions.session_brief_document_authority($1)",
        [i.workspaceId],
      ),
    )
  ).rows[0];
  expect(a.complete).toBe(false);
  expect(a.initialized).toBe(false);
  await backfill(x);
  await ack(x, await claim(x));
  await x.pg.exec(
    "UPDATE sessions.session_directory SET title='changed after search capture'",
  );
  a = (
    await reader(x, () =>
      x.pg.query<any>(
        "SELECT * FROM sessions.session_brief_document_authority($1)",
        [i.workspaceId],
      ),
    )
  ).rows[0];
  expect(a.initialized).toBe(true);
  expect(a.complete).toBe(false);
  expect(a.documents).toEqual([]);
});
test("backfill cursor is server owned and exhausts bounded pages without losing earlier new keys", async () => {
  const x = await fresh();
  for (const n of [2, 3]) {
    const physical = "large_replay" + n;
    await addReplayPointer(x.db, {
      key: `96bulls/org_replay/v1/ws=ws_replay/m=machine_0/h=claude/d=2026/11/01/s=${physical}/manifest.json`,
      digest: "a".repeat(64),
      physicalSession: physical,
      physicalMachine: "machine_0",
      canonicalSession: "large:replay" + n,
      canonicalMachine: "machine:0",
    });
    await projectSessionDirectory(x.db, {
      orgId: i.orgId,
      workspaceId: i.workspaceId,
      harness: i.source,
      sessionId: physical,
    });
  }
  await x.pg.exec("DELETE FROM sessions.session_brief_document_outbox");
  expect((await backfill(x, 1)).complete).toBe(false);
  expect((await backfill(x, 1)).complete).toBe(false);
  expect((await backfill(x, 1)).complete).toBe(true);
  const initial = await claim(x);
  expect(initial).toHaveLength(3);
  await ack(x, initial);
  await x.db.query("SELECT sessions.set_session_brief_documents_live(false)");
  await x.pg.exec("DELETE FROM sessions.session_directory");
  await x.db.query("SELECT sessions.set_session_brief_documents_live(true)");
  expect((await backfill(x, 1)).complete).toBe(false);
  expect((await backfill(x, 1)).complete).toBe(false);
  expect((await backfill(x, 1)).complete).toBe(true);
  const tombstones = await claim(x);
  expect(tombstones).toHaveLength(3);
  expect(tombstones.every((item) => item.document.deleted === true)).toBe(true);
});
test("expired document lease cannot be acknowledged from a held transaction", async () => {
  const x = await fresh();
  await backfill(x);
  const c = await claim(x);
  await x.pg.exec(
    "BEGIN;UPDATE sessions.session_brief_document_outbox SET lease_expires_at=clock_timestamp()+interval '50 milliseconds'",
  );
  try {
    await new Promise((r) => setTimeout(r, 80));
    expect(await ack(x, c)).toBe(0);
  } finally {
    await x.pg.exec("ROLLBACK");
  }
});
