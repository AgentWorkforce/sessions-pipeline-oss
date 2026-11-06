/**
 * GET /v1/sessions/:id/turns and /metadata from canonical rows, checked against
 * Neon itself: the rows live in a PGlite `sessions.conversation_turns` with Neon's
 * columns, relayhistory's ORDER BY and its JS Date serialization are the oracle,
 * and the legacy records are exactly what the export writes (`to_jsonb(t)` text).
 * Live `conversation_turn` records (relayhistory TURNS_WRITE_PATH) must replace
 * the legacy row of their key. Route behavior: canary, 501 reasons, erasure,
 * the strict cloud-client 0.2.0 turnsResponse / sessionMetadataResponse shapes.
 */
import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { canonicalLegacy } from "../canonical-legacy.ts";
import { canonicalLiveTurns, mergeTurnRows } from "../canonical-turns.ts";
import { AuthError, createReadApi, type TurnsDeps, type VerifiedIdentity } from "../app.ts";
import { CanonicalReadError, type CanonicalIdentity } from "../canonical.ts";
import type { CanonicalRows } from "../session-timing-source.ts";
import { metadataResponse, neonTs, turnRows, turnsResponse } from "../session-turns-source.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../../session-ingest/turns.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

let db: PGlite;
beforeAll(async () => {
  db = new PGlite();
  await db.exec(`SET TimeZone='UTC'; CREATE SCHEMA sessions; CREATE TABLE sessions.conversation_turns(
    id uuid PRIMARY KEY, org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
    turn_index integer NOT NULL, session_owner text NOT NULL, role text NOT NULL, content text NOT NULL,
    actor_name text NOT NULL, actor_role text NOT NULL, metadata jsonb NOT NULL DEFAULT '{}', ts timestamptz NOT NULL,
    UNIQUE(org_id, workspace_id, source, session_id, turn_index))`);
});
afterAll(async () => db.close());

const ORG = "org-a", WS = "ws-a", SESSION = "sess-1";
/** relayhistory listConversationTurns, as SQL: same filter and ORDER BY (Neon is C.UTF-8). */
async function oracle(source?: string) {
  const { rows } = await db.query<any>(
    `SELECT id::text, session_id, session_owner, turn_index, role, content, actor_name, actor_role, metadata, ts
       FROM sessions.conversation_turns WHERE org_id=$1 AND workspace_id=$2 AND session_id=$3 ${source ? "AND source=$4" : ""}
      ORDER BY turn_index, workspace_id COLLATE "C", source COLLATE "C", id`,
    source ? [ORG, WS, SESSION, source] : [ORG, WS, SESSION],
  );
  // drizzle hands back a Date; the route's JSON is its toISOString().
  return rows.map((r) => ({ id: r.id, sessionId: r.session_id, sessionOwner: r.session_owner, turnIndex: r.turn_index, role: r.role, content: r.content,
    actorName: r.actor_name, actorRole: r.actor_role, metadata: r.metadata, ts: new Date(r.ts).toISOString() }));
}
/** relayhistory getSessionMetadata over the same rows (its TS logic, verbatim semantics). */
async function metadataOracle() {
  const { rows } = await db.query<any>(
    `SELECT session_owner, metadata FROM sessions.conversation_turns WHERE org_id=$1 AND workspace_id=$2 AND session_id=$3
      ORDER BY turn_index DESC, workspace_id COLLATE "C", source COLLATE "C", id`, [ORG, WS, SESSION]);
  if (!rows.length) return null;
  const first = (k: string) => { for (const r of rows) { const v = r.metadata?.[k]; if (typeof v === "string" && v.length > 0) return v; } return null; };
  return { nativeCli: first("nativeCli"), nativeResumeId: first("nativeResumeId"), sessionOwner: rows[0].session_owner, originNode: first("originNode") };
}
/** The export's legacy records for one unit (legacy-source.ts: to_jsonb(t) || {id, source, legacy_stored_source}). */
async function exported(unitSource: string): Promise<RelayhistoryStreamRecord[]> {
  const { rows } = await db.query<any>(
    `SELECT id::text AS key,(to_jsonb(t)||jsonb_build_object('id',id::text,'source',$4::text,'legacy_stored_source',t.source))::text AS payload_json
       FROM sessions.conversation_turns t WHERE org_id=$1 AND workspace_id=$2 AND session_id=$3 AND source=$4 ORDER BY id`, [ORG, WS, SESSION, unitSource]);
  return [
    ...rows.map((r) => ({ kind: "legacy_conversation_turn", origin_id: "legacy:conversation_turns", record_id: "legacy:conversation_turns:" + r.key, revision: "1", operation: "upsert", received_at: "2026-11-01T00:00:00Z", payload: JSON.parse(r.payload_json) }) as RelayhistoryStreamRecord),
    { kind: "legacy_scope", origin_id: "legacy:scope", record_id: "legacy:scope:coverage", revision: "1", operation: "upsert", received_at: "2026-11-01T00:00:00Z",
      payload: { version: 1, org_id: ORG, workspace_id: WS, source: unitSource, session_id: SESSION, tables: ["conversation_turns", "session_links", "session_outcomes"] } } as RelayhistoryStreamRecord,
  ];
}
/** A live record exactly as the ingest stores it (turns.ts), after the pipeline's revision fold. */
function live(source: string, index: number, over: Record<string, unknown> = {}, revision = 1_793_000_000_000_000 + index): RelayhistoryStreamRecord {
  return { kind: TURN_KIND, origin_id: TURN_ORIGIN_ID, record_id: turnRecordId(WS, source, SESSION, index), revision: String(revision), operation: "upsert", received_at: "2026-11-05T12:00:00Z",
    payload: { id: crypto.randomUUID(), org_id: ORG, workspace_id: WS, source, session_id: SESSION, turn_index: index, session_owner: "user-a", role: "assistant",
      content: `live ${index}`, actor_name: "Ada", actor_role: "owner", metadata_json: JSON.stringify({ nativeCli: source }), ts: "2026-11-05T12:00:00.123Z", tagged: true, ...over } } as RelayhistoryStreamRecord;
}
function snapshot(source: string, records: RelayhistoryStreamRecord[]): { harness: string; snapshot: CanonicalRows } {
  const identity: CanonicalIdentity = { orgId: ORG, workspaceId: WS, source, sessionId: SESSION };
  const legacy = canonicalLegacy(records, identity);
  return { harness: source, snapshot: { rows: mergeTurnRows(legacy.rows, canonicalLiveTurns(records, identity)), legacyCoverage: legacy.coverage } };
}
const scope = { orgId: ORG, workspaceId: WS, sessionId: SESSION };

describe("parity with Neon conversation_turns", () => {
  test("seeded random sessions: same rows, order, ts and metadata as relayhistory", async () => {
    let seed = 7;
    const rnd = (n: number) => ((seed = (seed * 1103515245 + 12345) % 2147483648), seed % n);
    for (let round = 0; round < 40; round++) {
      await db.exec("TRUNCATE sessions.conversation_turns");
      const sources = ["claude", "codex", "user-a"].slice(0, 1 + rnd(3));
      for (const source of sources)
        for (let i = 0; i < 1 + rnd(6); i++) {
          const meta = rnd(3) === 0 ? {} : { nativeCli: source === "user-a" ? "claude" : source, ...(rnd(2) ? { nativeResumeId: `r-${rnd(9)}` } : {}), ...(rnd(2) ? { originNode: `n${rnd(3)}` } : {}) };
          const us = String(rnd(1_000_000)).padStart(6, "0");
          await db.query(`INSERT INTO sessions.conversation_turns VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
            [crypto.randomUUID(), ORG, WS, source, SESSION, rnd(4) * 2 + i * 10, rnd(2) ? "user-a" : "owner é", ["user", "assistant", "system"][rnd(3)], `c${round}-${i} "q" é\n`, "Ada", rnd(2) ? "owner" : "steerer", JSON.stringify(meta), `2026-11-0${1 + rnd(5)} 1${rnd(9)}:0${rnd(9)}:00.${us}+00`]);
        }
      const snaps = await Promise.all(sources.map(async (s) => snapshot(s, await exported(s))));
      const rows = turnRows(scope, snaps, undefined);
      expect(turnsResponse(rows)).toEqual(await oracle());
      expect(metadataResponse(rows)).toEqual(await metadataOracle());
      for (const s of sources) expect(turnsResponse(turnRows(scope, snaps, s))).toEqual(await oracle(s));
    }
  });

  test("a live record replaces its key's legacy row; other rows unchanged; later revision wins", async () => {
    await db.exec("TRUNCATE sessions.conversation_turns");
    for (const i of [0, 1])
      await db.query(`INSERT INTO sessions.conversation_turns VALUES($1,$2,$3,'claude',$4,$5,'user-a','user',$6,'Ada','owner','{"nativeCli":"claude"}','2026-11-01 10:00:00.5+00')`,
        [crypto.randomUUID(), ORG, WS, SESSION, i, `legacy ${i}`]);
    const records = [...(await exported("claude")), live("claude", 1, { content: "replaced" }), live("claude", 2)];
    const turns = turnsResponse(turnRows(scope, [snapshot("claude", records)], undefined));
    expect(turns.map((t) => [t.turnIndex, t.content])).toEqual([[0, "legacy 0"], [1, "replaced"], [2, "live 2"]]);
    // The per-harness merge and the cross-scope key rule agree on the newest live record.
    const a = live("claude", 3, { content: "old" }, 10), b = live("claude", 3, { content: "new" }, 20);
    const rows = turnRows(scope, [{ harness: "claude", snapshot: { legacyCoverage: { version: 1, tables: [] }, rows: [...canonicalLiveTurns([a], { ...scope, source: "claude" }), ...canonicalLiveTurns([b], { ...scope, source: "claude" })] } }], undefined);
    expect(turnsResponse(rows).map((t) => t.content)).toEqual(["new"]);
  });

  test("a live record outside its scope or with a forged record id fails the read", () => {
    const id = { ...scope, source: "claude" };
    expect(() => canonicalLiveTurns([live("claude", 0, { org_id: "org-b" })], id)).toThrow("canonical live turn invalid");
    expect(() => canonicalLiveTurns([{ ...live("claude", 0), record_id: turnRecordId(WS, "claude", SESSION, 9) }], id)).toThrow();
    expect(() => canonicalLiveTurns([live("claude", 0, { metadata_json: "[]" })], id)).toThrow();
  });

  test("ts: Postgres text and ISO become JS Date JSON (ms, truncated, Z)", () => {
    expect(neonTs("2026-11-05T12:00:00.123999+00:00")).toBe("2026-11-05T12:00:00.123Z");
    expect(neonTs("2026-11-05 12:00:00+00")).toBe("2026-11-05T12:00:00.000Z");
    expect(neonTs("2026-11-05T05:00:00.5-07:00")).toBe("2026-11-05T12:00:00.500Z");
    expect(neonTs("2026-11-05T12:00:00.123Z")).toBe("2026-11-05T12:00:00.123Z");
    expect(() => neonTs("yesterday")).toThrow();
  });
});

describe("exactness refusals", () => {
  const cover = { version: 1 as const, tables: ["conversation_turns", "session_links", "session_outcomes"] };
  test("no legacy coverage, a cut export string, an untagged live turn under source: refused", () => {
    expect(() => turnRows(scope, [{ harness: "claude", snapshot: { legacyCoverage: null, rows: [] } }], undefined)).toThrow("turns input refused");
    const cut = { table: "conversation_turns", row: { id: "x", org_id: ORG, workspace_id: WS, source: "claude", session_id: SESSION, turn_index: 0, content: "a\n[neon-s3-sync: truncated 9 characters over the 262144-character scrub limit; the full text stays in Neon]", metadata: {} } };
    expect(() => turnRows(scope, [{ harness: "claude", snapshot: { legacyCoverage: cover, rows: [cut] } }], undefined)).toThrow();
    const untagged = canonicalLiveTurns([live("user-a", 0, { source: "user-a", metadata_json: "{}", tagged: false })], { ...scope, source: "user-a" });
    expect(() => turnRows(scope, [{ harness: "user-a", snapshot: { legacyCoverage: cover, rows: untagged } }], "user-a")).toThrow();
    // Without `source` no attribution is needed: the row is served.
    expect(turnRows(scope, [{ harness: "user-a", snapshot: { legacyCoverage: cover, rows: untagged } }], undefined)).toHaveLength(1);
  });
});

describe("routes", () => {
  const identity: VerifiedIdentity = { userId: "u-1", orgId: ORG, workspaceId: WS, scopes: ["rth:read"] };
  const cover = { version: 1 as const, tables: ["conversation_turns", "session_links", "session_outcomes"] };
  function api(over: { workspaces?: ReadonlySet<string> | "*"; turns?: Partial<TurnsDeps> | null } = {}) {
    const calls: CanonicalIdentity[] = [];
    const logs: Record<string, unknown>[] = [];
    const turns: TurnsDeps | undefined = over.turns === null ? undefined : {
      harnesses: async () => [{ harness: "claude", pointers: 1, live: 1 }, { harness: "codex", pointers: 1, live: 1 }],
      source: async (id) => {
        calls.push(id);
        return snapshot(id.source, [live(id.source, id.source === "claude" ? 0 : 1, { metadata_json: JSON.stringify({ nativeCli: id.source, originNode: `node-${id.source}` }) }),
          { kind: "legacy_scope", origin_id: "legacy:scope", record_id: "legacy:scope:coverage", revision: "1", operation: "upsert", received_at: "2026-11-01T00:00:00Z",
            payload: { version: 1, org_id: ORG, workspace_id: WS, source: id.source, session_id: SESSION, tables: cover.tables } } as RelayhistoryStreamRecord]).snapshot;
      },
      ...over.turns,
    };
    const app = createReadApi({
      verify: async (t) => { if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid"); return identity; },
      ch: { database: "relay_search", async command() {}, async insert() {}, async select() { throw new Error("turns never read ClickHouse"); } },
      liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
      completeSinceMs: 0,
      directoryWorkspaces: "workspaces" in over ? over.workspaces : new Set([WS]),
      turns,
      log: (f) => logs.push(f),
    });
    const get = (path: string) => app.request(path, { headers: { authorization: "Bearer good" } });
    return { get, calls, logs };
  }
  const body = async (r: Response) => (await r.json()) as any;
  const T = `/v1/sessions/${SESSION}/turns?workspace=${WS}`, M = `/v1/sessions/${SESSION}/metadata?workspace=${WS}`;

  test("serves the strict turnsResponse and sessionMetadataResponse shapes", async () => {
    const { get, calls } = api();
    const r = await get(T);
    expect(r.status).toBe(200);
    const b = await body(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "sessionId", "turns"]);
    expect(b.turns.map((t: any) => [t.turnIndex, t.metadata.nativeCli])).toEqual([[0, "claude"], [1, "codex"]]);
    for (const t of b.turns) expect(Object.keys(t).sort()).toEqual(["actorName", "actorRole", "content", "id", "metadata", "role", "sessionId", "sessionOwner", "ts", "turnIndex"]);
    expect(calls.map((c) => c.source).sort()).toEqual(["claude", "codex"]);
    const m = await get(M);
    expect(await body(m)).toEqual({ nativeCli: "codex", nativeResumeId: null, sessionOwner: "user-a", originNode: "node-codex" });
    // `source` selects one harness scope only.
    const only = api();
    expect((await body(await only.get(`${T}&source=claude`))).turns).toHaveLength(1);
    expect(only.calls.map((c) => c.source)).toEqual(["claude"]);
  });

  test("501 reasons Cloud falls back on; never an approximate answer", async () => {
    const reason = async (r: Response) => [r.status, (await body(r)).error?.reason];
    expect(await reason(await api({ workspaces: new Set() }).get(T))).toEqual([501, "turns_workspace_not_enabled"]);
    expect(await reason(await api({ turns: null }).get(T))).toEqual([501, "turns_not_configured"]);
    expect(await reason(await api().get(`/v1/sessions/${SESSION}/turns`))).toEqual([501, "org_scope"]);
    expect(await reason(await api({ turns: { harnesses: async () => [] } }).get(T))).toEqual([501, "turns_no_pointers"]);
    expect(await reason(await api().get(`${T}&source=gemini`))).toEqual([501, "turns_no_pointers"]);
    expect(await reason(await api({ turns: { harnesses: async () => Array.from({ length: 5 }, (_, i) => ({ harness: `h${i}`, pointers: 1, live: 1 })) } }).get(T))).toEqual([501, "turns_harness_bound"]);
    expect(await reason(await api({ turns: { source: async () => ({ rows: [], legacyCoverage: null }) } }).get(T))).toEqual([501, "turns_unknown"]);
    expect(await reason(await api({ turns: { source: async () => { throw new CanonicalReadError("manifest_mismatch"); } } }).get(M))).toEqual([501, "turns_canonical_manifest_mismatch"]);
    expect(await reason(await api({ turns: { source: () => new Promise(() => {}), deadlineMs: 20 } }).get(T))).toEqual([501, "turns_canonical_deadline"]);
  });

  test("erased (every pointer tombstoned): no turns and metadata 404, without reading S3", async () => {
    const { get, calls } = api({ turns: { harnesses: async () => [{ harness: "claude", pointers: 2, live: 0 }] } });
    expect((await body(await get(T))).turns).toEqual([]);
    const m = await get(M);
    expect(m.status).toBe(404);
    expect((await body(m)).error.code).toBe("not_found");
    expect(calls).toEqual([]);
  });
});
