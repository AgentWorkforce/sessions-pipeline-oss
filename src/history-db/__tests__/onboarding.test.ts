import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../../session-ingest/app.ts";
import { generation } from "../../hoopdb/migrate.ts";
import { historyRetentionViolations, runHistoryMigrations, type HistoryClient } from "../migrate.ts";
import type { Run } from "../onboarding.ts";

setDefaultTimeout(60_000);
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((d) => d.close())); });
function client(db: PGlite): HistoryClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const TENANT = { userId: "user-1", orgId: "org-1", workspaceId: "ws-1" };
const PROGRESS = { phase: "uploading", source: "claude", processedFiles: 3, totalFiles: 10, sessionsCaptured: 2, recordsUploaded: 40, recordsQueued: 5, backlogComplete: false };

async function setup() {
  const db = new PGlite(); open.push(db);
  await runHistoryMigrations(client(db));
  const run: Run = async (text, params) => (await db.query(text, params)) as { rows: Record<string, unknown>[] };
  const app = createIngestApp({ deliver: {} as never, log: () => {}, identify: (r) => bindingIdentity(r), onboarding: { run } });
  const call = (method: string, path: string, scopes: string[], body?: string, tenant = TENANT) =>
    app.fetch(new Request(`https://x${path}`, { method, body, headers: { [IDENTITY_HEADER]: encodeIdentity({ ...tenant, scopes }) } }));
  return { db, call };
}

describe("onboarding heartbeat on history.probe_connections (P5b G2)", () => {
  test("heartbeat upserts per (org, workspace, user) and the connection read returns it", async () => {
    const { db, call } = await setup();
    const first = await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], JSON.stringify({ progress: PROGRESS }));
    expect(first.status).toBe(200);
    const a = await first.json();
    expect(a.connected).toBe(true);
    expect(Number.isFinite(Date.parse(a.lastSeenAt))).toBe(true);
    const second = await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], "");
    expect(second.status).toBe(200);
    expect((await db.query("SELECT count(*)::int AS n, max(progress::text) AS p FROM history.probe_connections")).rows[0]).toEqual({ n: 1, p: null });
    const read = await call("GET", "/v1/onboarding/connection", ["rth:read"]);
    expect(read.status).toBe(200);
    expect(await read.json()).toMatchObject({ progress: null });
    const other = await call("GET", "/v1/onboarding/connection", ["rth:read"], undefined, { ...TENANT, userId: "user-2" });
    expect(await other.json()).toEqual({ lastSeenAt: null, progress: null });
  });

  test("same validation as relayhistory: bad progress 400, oversize 413, no workspace 403, scopes enforced", async () => {
    const { call } = await setup();
    expect((await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], JSON.stringify({ progress: { ...PROGRESS, phase: "nope" } }))).status).toBe(400);
    expect((await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], "{not json")).status).toBe(400);
    expect((await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], JSON.stringify({ pad: "x".repeat(5000) }))).status).toBe(413);
    expect((await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], "", { ...TENANT, workspaceId: "" })).status).toBe(403);
    expect((await call("POST", "/v1/onboarding/heartbeat", ["rth:read"], "")).status).toBe(403);
    expect((await call("GET", "/v1/onboarding/connection", [])).status).toBe(403);
  });

  test("owner-only import never overwrites a newer heartbeat; the table is retention-ready; ingest generation unchanged", async () => {
    const { db, call } = await setup();
    await call("POST", "/v1/onboarding/heartbeat", [SYNC_SCOPE], "");
    const n = (await db.query<{ n: number }>("SELECT history.probe_connections_import($1::jsonb) AS n", [JSON.stringify([
      { ...{ org_id: "org-1", workspace_id: "ws-1", user_id: "user-1" }, last_seen_at: "2026-01-01T00:00:00Z", progress: null },
      { org_id: "org-1", workspace_id: "ws-1", user_id: "user-9", last_seen_at: "2026-01-01T00:00:00Z", progress: PROGRESS },
    ])])).rows[0]!.n;
    expect(n).toBe(1);
    expect((await db.query("SELECT user_id, last_seen_at > '2026-06-01' AS newer FROM history.probe_connections ORDER BY user_id")).rows)
      .toEqual([{ user_id: "user-1", newer: true }, { user_id: "user-9", newer: false }]);
    expect(await historyRetentionViolations()).toEqual([]);
    // The ingest generation never depends on the history track (proven in history-track.test.ts).
    expect(typeof (await generation())).toBe("string");
  });

  test("without the onboarding dependency the routes answer 503, never touching delivery", async () => {
    const app = createIngestApp({ deliver: {} as never, log: () => {}, identify: (r) => bindingIdentity(r) });
    const res = await app.fetch(new Request("https://x/v1/onboarding/heartbeat", { method: "POST", headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }) } }));
    expect(res.status).toBe(503);
  });
});
