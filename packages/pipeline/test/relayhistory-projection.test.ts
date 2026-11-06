/**
 * The export projection against the real one: the same records go through the
 * vendored migrations (delivery triggers live, as after the 0029 rollout) and
 * through `projectRelayhistorySession`, and the rows must be equal.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { REPROJECTED_TABLES } from "../../../src/migration/relayhistory-copy.js";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  canonicalTimestamp,
  lowerAgentName,
  projectRelayhistorySession,
  type ProjectionRow,
} from "../src/lib/pipeline/relayhistory-projection.js";
import {
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  type RelayhistoryStreamRecord,
} from "../src/lib/pipeline/relayhistory-transform.js";

const MIGRATIONS = fileURLToPath(
  new URL("../../../vendor/relayhistory-cloud/migrations/", import.meta.url)
    .href,
);
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");

const WORKSPACE = "workspace-1";
const SESSION = "session-1";
let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { pg_trgm } });
  await db.exec(`SET search_path = pg_catalog, public;
    CREATE SCHEMA IF NOT EXISTS sessions;
    CREATE TABLE IF NOT EXISTS sessions.__migrations (
      name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
  for (const file of readdirSync(MIGRATIONS)
    .filter((name) => name.endsWith(".sql"))
    .sort())
    await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
  // Production's 0029 rollout: each stage's indexes, then its live trigger.
  const { rows } = await db.query<{ index_name: string; definition: string }>(
    "SELECT index_name, definition FROM sessions.delivery_rollout_indexes()",
  );
  for (const index of rows)
    await db.exec(`CREATE INDEX ${index.index_name} ${index.definition}`);
  await db.exec(`SET session_replication_role = replica;
    UPDATE sessions.upload_cleanup_rollout SET phase = 5 WHERE singleton;
    SET session_replication_role = origin;`);
  await db.exec(`SELECT sessions.activate_delivery_catalog();
    SELECT sessions.activate_delivery_projection_v2();
    SET search_path = public`);
}, 120_000);
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec(`SET session_replication_role = replica;
    TRUNCATE sessions.delivery_records, sessions.convergence_events, sessions.session_catalog,
      sessions.session_catalog_agents, sessions.session_relationships, sessions.session_markers,
      sessions.session_commit_links, sessions.session_rollups CASCADE;
    SET session_replication_role = origin;`);
});

type Line = Omit<RelayhistoryStreamRecord, "operation"> & {
  operation?: "upsert" | "delete";
};

/** Deliver one revision the way accept_delivery_batch does, at its `received_at`. */
async function deliver(line: Line, source: string) {
  await db.query(
    `INSERT INTO sessions.delivery_records
       (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source,
        session_id, operation, payload, user_id, received_at)
     VALUES ('org', $1, $2, $3, $3 || ':' || $4, $4::bigint, 'digest', $5, $6, $7, $8, $9::jsonb, 'u', $10::timestamptz)
     ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE SET
       revision = excluded.revision, revision_id = excluded.revision_id, kind = excluded.kind,
       operation = excluded.operation, payload = excluded.payload, received_at = excluded.received_at`,
    [
      WORKSPACE,
      line.origin_id,
      line.record_id,
      line.revision,
      line.kind,
      source,
      SESSION,
      line.operation ?? "upsert",
      line.payload === null ? null : JSON.stringify(line.payload),
      line.received_at,
    ],
  );
}

const TIMES = new Set([
  "ts",
  "ingested_at",
  "received_at",
  "first_activity_at",
  "last_activity_at",
  "spawned_at",
  "updated_at",
  "linked_at",
]);
/** A stored row in the projection's shape: no tenant or local bookkeeping columns. */
function stored(row: Record<string, unknown>, drop: readonly string[]) {
  return Object.fromEntries(
    Object.entries(row)
      .filter(([key]) => !drop.includes(key))
      .map(([key, value]) => [
        key,
        TIMES.has(key) && typeof value === "string"
          ? canonicalTimestamp(value)
          : value,
      ]),
  );
}

const TENANT = ["org_id", "workspace_id", "user_id"];
/** Filled after projection, by the enrichment jobs. */
const ENRICHMENT = [
  "embedding",
  "embedding_dim",
  "embedding_model",
  "embedding_skip_reason",
  "content_hash",
];
const TABLES: Record<
  Exclude<ProjectionRow["table"], "convergence_events">,
  string[]
> = {
  session_catalog: TENANT,
  // `last_ts` comes from session_rollups, never from the export projection.
  session_catalog_agents: [...TENANT, "updated_at", "last_ts"],
  session_relationships: TENANT,
  session_markers: TENANT,
  session_commit_links: TENANT,
};

async function liveRows(): Promise<ProjectionRow[]> {
  const rows: ProjectionRow[] = [];
  for (const [table, drop] of Object.entries(TABLES)) {
    const result = await db.query<{ row: Record<string, unknown> }>(
      `SELECT to_jsonb(t) AS row FROM sessions.${table} t ORDER BY to_jsonb(t)::text`,
    );
    for (const { row } of result.rows)
      rows.push({
        table: table as ProjectionRow["table"],
        row: stored(row, drop),
      });
  }
  const events = await db.query<{ row: Record<string, unknown> }>(
    `SELECT to_jsonb(e) AS row FROM sessions.convergence_events e ORDER BY e.ts, e.event_id`,
  );
  for (const { row } of events.rows)
    rows.push({
      table: "convergence_events",
      row: stored(row, [...TENANT, ...ENRICHMENT]),
    });
  return rows;
}

const byTable = (
  rows: readonly ProjectionRow[],
  table: ProjectionRow["table"],
) => rows.filter((row) => row.table === table).map((row) => row.row);

const context = (source = "claude") => ({
  workspaceId: WORKSPACE,
  source,
  sessionId: SESSION,
  machineId: "machine:1",
  userId: "u",
});

const CATALOG_KINDS = [
  "session",
  "relationship",
  "session_marker",
  "commit_link",
];

/** Each line with its seat in the ledger the live projection settles that kind by. */
async function withAcceptance(lines: Line[]): Promise<Line[]> {
  const seat = async (table: string) => {
    const { rows } = await db.query<{
      origin_id: string;
      record_id: string;
      seq: string;
    }>(
      `SELECT origin_id, record_id, accepted_seq::text AS seq FROM sessions.${table}`,
    );
    return new Map(
      rows.map((row) => [
        JSON.stringify([row.origin_id, row.record_id]),
        row.seq,
      ]),
    );
  };
  const activity = await seat("delivery_projection_acceptance");
  const catalog = await seat("delivery_catalog_acceptance");
  return lines.map((line) => {
    const key = JSON.stringify([line.origin_id, line.record_id]);
    const accepted_seq = (
      CATALOG_KINDS.includes(line.kind) ? catalog : activity
    ).get(key);
    return accepted_seq === undefined ? line : { ...line, accepted_seq };
  });
}

/** Exactly what the live triggers leave, in the readers' event order. */
async function expectLiveEqual(lines: Line[], source = "claude") {
  for (const line of lines) await deliver(line, source);
  const sequenced = await withAcceptance(lines);
  const projected = projectRelayhistorySession(
    normalizeRelayhistoryRecords(
      sequenced.map((line) => parseRelayhistoryRecord(JSON.stringify(line))),
    ),
    context(source),
  );
  const live = await liveRows();
  for (const table of [
    ...Object.keys(TABLES),
    "convergence_events",
  ] as ProjectionRow["table"][])
    expect(byTable(projected.rows, table), table).toEqual(byTable(live, table));
  return projected;
}

const RECEIVED = "2026-10-30 12:00:00.123456+00";
const T = 1_793_000_000_000;
const line = (
  record_id: string,
  kind: string,
  payload: Record<string, unknown>,
  overrides: Partial<Line> = {},
): Line => ({
  kind,
  origin_id: "origin-a",
  record_id,
  revision: "1",
  received_at: RECEIVED,
  payload,
  ...overrides,
});

describe("export projection equals the live projection", () => {
  it("projects every event kind with its typed columns", async () => {
    await expectLiveEqual([
      line("h", "history", {
        prompt: "do it",
        ts_ms: T,
        project_key: "github.com/acme/repo",
      }),
      line("user", "session_event", {
        role: "user",
        text: "hello",
        timestamp_ms: T + 1,
        project_key: "acme/repo",
        model: "opus",
        provider: "",
        subagent_session_id: "sub-1",
      }),
      line("tool", "tool_call", {
        name: "Read",
        target: "a.ts",
        args_json: '{"path":"a.ts"}',
        is_error: 0,
        tool_use_id: "t1",
        ts_ms: T + 2,
        project_key: "/home/me/repo",
        project_key_method: "path",
        cwd: "/home/me/repo",
      }),
      line("tool-bad", "tool_call", {
        name: "Bash",
        args_json: "not json",
        is_error: "1",
        updated_ms: T + 3,
        ts_ms: 1,
      }),
      line("tool-bare", "tool_call", { ts_ms: T + 4 }),
      line("edit", "file_edit", {
        file_path: "a.ts",
        ts_ms: T + 5,
        project_id: "p",
        project: "q",
      }),
      line("edit-empty", "file_edit", { file_path: "", ts_ms: T + 6 }),
    ]);
  });

  it("withholds control and sidechain rows and has no row for evidence kinds", async () => {
    const { rows, unprojected } = await expectLiveEqual([
      line("keep", "session_event", { role: "user", text: "x", ts_ms: T }),
      line("control", "session_event", {
        role: "user",
        control_kind: "hook",
        ts_ms: T,
      }),
      line("meta", "session_event", { role: "user", is_meta: 1, ts_ms: T }),
      line("sidechain", "session_event", {
        role: "user",
        is_sidechain: "true",
        ts_ms: T,
      }),
      line("assistant-sidechain", "session_event", {
        role: "assistant",
        is_sidechain: 1,
        ts_ms: T,
      }),
      line("presence", "presence", { ts_ms: T }),
      line("observation", "source_observation", { ts_ms: T }),
      line("evidence", "observation_evidence", { ts_ms: T }),
    ]);
    expect(byTable(rows, "convergence_events")).toHaveLength(2);
    expect(unprojected).toBe(3);
  });

  it("settles a timeless tool to its earliest parent event, else its delivery time", async () => {
    await expectLiveEqual([
      line("tool", "tool_call", { name: "Read", tool_use_id: "t1" }),
      line("edit", "file_edit", { file_path: "a", tool_use_id: "t1" }),
      line("own-time", "tool_call", {
        name: "Read",
        tool_use_id: "t1",
        ts_ms: T + 9,
      }),
      line("orphan", "tool_call", { name: "Read", tool_use_id: "t2" }),
      line("no-id", "tool_call", { name: "Read" }),
      line("parent-late", "session_event", {
        role: "assistant",
        tool_use_id: "t1",
        ts_ms: T + 5,
      }),
      line("parent-early", "session_event", {
        role: "assistant",
        tool_use_id: "t1",
        ts_ms: T + 1,
      }),
    ]);
  });

  it("counts a request's usage once, on its smallest record", async () => {
    const usage = JSON.stringify({
      input_tokens: 10,
      output_tokens: 20,
      cache_read_input_tokens: 3,
      cache_creation: {
        ephemeral_5m_input_tokens: 4,
        ephemeral_1h_input_tokens: 5,
      },
      cost_usd: 0.0123455,
    });
    const assistant = (extra: Record<string, unknown>) => ({
      role: "assistant",
      ts_ms: T,
      message_id: "m",
      ...extra,
    });
    await expectLiveEqual([
      line(
        "b",
        "session_event",
        assistant({ request_id: "r1", token_json: usage }),
      ),
      line(
        "a",
        "session_event",
        assistant({ request_id: "r1", token_json: usage }),
      ),
      line(
        "c",
        "session_event",
        assistant({ request_id: "r2", token_json: usage }),
      ),
      line(
        "d",
        "session_event",
        assistant({ request_id: "r2", token_json: '{"input_tokens":1}' }),
      ),
      line("e", "session_event", assistant({ token_json: usage })),
      line(
        "f",
        "session_event",
        assistant({ provider_message_id: "pm", token_json: "{broken" }),
      ),
      line(
        "g",
        "session_event",
        assistant({ request_span: "sp", token_json: '{"input_tokens":-1}' }),
      ),
    ]);
  });

  it.each([
    [
      "codex",
      {
        input_tokens: 10,
        cached_input_tokens: 4,
        output_tokens: 2,
        reasoning_output_tokens: 1,
      },
    ],
    ["codex", { input_tokens: 1, cached_input_tokens: 4 }],
    [
      "muse",
      {
        input_tokens: 10,
        cached_tokens: 4,
        reasoning_tokens: 2,
        total_cost_usd: 1.5,
      },
    ],
    [
      "grok",
      { usage: { promptTokens: 9, cachedReadTokens: 2, completionTokens: 1 } },
    ],
    ["grok", { input_tokens: 9 }],
    ["chatgpt", { input_tokens: 9 }],
  ])("normalizes %s usage %j", async (source, token) => {
    await expectLiveEqual(
      [
        line("a", "session_event", {
          role: "assistant",
          ts_ms: T,
          message_id: "m",
          token_json: JSON.stringify(token),
        }),
      ],
      source,
    );
  });

  it("projects the catalog tables, the newest record winning its key", async () => {
    await expectLiveEqual([
      line("s-old", "session", {
        session_id: SESSION,
        title: "old",
        agent_name: "old-agent",
      }),
      line(
        "s-new",
        "session",
        {
          session_id: SESSION,
          title: "new",
          cwd: "/r",
          git_branch: "main",
          repo_url: "git@github.com:acme/repo.git",
          initial_commit: "abc",
          first_prompt: "hi",
          models_json: '["opus"]',
          workspace_roots_json: "not json",
          project_key: "acme/repo",
          first_activity_ms: T,
          last_activity_ms: T + 10,
          agent_name: "Worker-1",
        },
        { received_at: "2026-10-30 12:00:01+00" },
      ),
      line("rel", "relationship", {
        relationship_uid: "r1",
        child_session_id: "child",
        relationship: "subagent",
        spawn_depth: 1,
        child_has_events: 1,
        spawned_at_ms: T,
        updated_ms: T + 1,
      }),
      line("rel-bool", "relationship", {
        relationship_uid: "r2",
        child_has_events: false,
        spawn_depth: 1.5,
      }),
      line("mark", "session_marker", {
        marker_uid: "m1",
        kind: "compaction",
        subkind: "auto",
        ts_ms: T,
        text: "t",
        payload_json: '{"a":1}',
      }),
      line("mark-bad", "session_marker", { kind: "no-uid" }),
      line("commit", "commit_link", {
        commit_sha: "deadbeef",
        match_method: "trailer",
        confidence: 0.12345,
        files_json: '["a"]',
        created_at_ms: T,
      }),
    ]);
  });

  it("drops the agent name when the newest catalog record has none", async () => {
    const { rows } = await expectLiveEqual([
      line("s-old", "session", { agent_name: "worker" }),
      line(
        "s-new",
        "session",
        { title: "t" },
        { received_at: "2026-10-30 12:00:01+00" },
      ),
    ]);
    expect(byTable(rows, "session_catalog_agents")).toEqual([]);
  });

  it("keeps the latest revision whatever order the lines arrive in", async () => {
    const lines = [
      line("x", "session_event", { role: "user", text: "first", ts_ms: T }),
      line(
        "x",
        "session_event",
        { role: "user", text: "second", ts_ms: T + 1 },
        { revision: "2" },
      ),
    ];
    for (const value of lines) await deliver(value, "claude");
    const live = await liveRows();
    const projected = projectRelayhistorySession(
      normalizeRelayhistoryRecords(
        [...lines]
          .reverse()
          .map((value) => parseRelayhistoryRecord(JSON.stringify(value))),
      ),
      context(),
    );
    expect(byTable(projected.rows, "convergence_events")).toEqual(
      byTable(live, "convergence_events"),
    );
    expect(byTable(live, "convergence_events")).toHaveLength(1);
  });

  it("keeps one row for a record delivered by two origins, the newest", async () => {
    await expectLiveEqual([
      line("x", "session_event", { role: "user", text: "a", ts_ms: T }),
      line(
        "x",
        "session_event",
        { role: "user", text: "b", ts_ms: T },
        { origin_id: "origin-b", received_at: "2026-10-30 12:00:01+00" },
      ),
    ]);
  });

  it("lets a tombstone from another origin remove the record", async () => {
    const lines: Line[] = [
      line("gone", "session_event", { role: "user", text: "a", ts_ms: T }),
      line("kept", "session_event", { role: "user", text: "b", ts_ms: T }),
    ];
    const tombstone = line(
      "gone",
      "session_event",
      {},
      {
        origin_id: "origin-b",
        operation: "delete",
        payload: null,
        received_at: "2026-10-30 12:00:01+00",
      },
    );
    const { rows } = await expectLiveEqual([...lines, tombstone]);
    expect(
      byTable(rows, "convergence_events").map((row) => row.delivery_record_id),
    ).toEqual(["kept"]);
  });
});

describe("acceptance order, not delivery time, settles contenders", () => {
  const LATER = "2026-10-30 12:00:09+00";
  const EARLIER = "2026-10-30 12:00:01+00";

  it("keeps the origin accepted last even when its received_at is earlier", async () => {
    const { rows } = await expectLiveEqual([
      line(
        "x",
        "session_event",
        { role: "user", text: "first-accepted", ts_ms: T },
        { received_at: LATER },
      ),
      line(
        "x",
        "session_event",
        { role: "user", text: "last-accepted", ts_ms: T },
        { origin_id: "origin-b", received_at: EARLIER },
      ),
    ]);
    expect(
      byTable(rows, "convergence_events").map((row) => row.content),
    ).toEqual(["last-accepted"]);
  });

  it("lets a tombstone accepted last win even when its received_at is earlier", async () => {
    const { rows } = await expectLiveEqual([
      line(
        "x",
        "session_event",
        { role: "user", text: "gone", ts_ms: T },
        { received_at: LATER },
      ),
      line(
        "x",
        "session_event",
        {},
        {
          origin_id: "origin-b",
          operation: "delete",
          payload: null,
          received_at: EARLIER,
        },
      ),
    ]);
    expect(byTable(rows, "convergence_events")).toEqual([]);
  });

  it("settles a catalog key the same way", async () => {
    const { rows } = await expectLiveEqual([
      line(
        "s-1",
        "session",
        { title: "first-accepted" },
        { received_at: LATER },
      ),
      line(
        "s-2",
        "session",
        { title: "last-accepted" },
        { received_at: EARLIER },
      ),
    ]);
    expect(byTable(rows, "session_catalog").map((row) => row.title)).toEqual([
      "last-accepted",
    ]);
  });

  it("is the same in every arrival order", async () => {
    const lines = [
      line(
        "x",
        "session_event",
        { role: "user", text: "a", ts_ms: T },
        { received_at: LATER },
      ),
      line(
        "x",
        "session_event",
        { role: "user", text: "b", ts_ms: T },
        { origin_id: "origin-b", received_at: EARLIER },
      ),
      line(
        "x",
        "session_event",
        {},
        {
          origin_id: "origin-c",
          operation: "delete",
          payload: null,
          received_at: EARLIER,
        },
      ),
      line(
        "y",
        "session_event",
        { role: "user", text: "c", ts_ms: T },
        { received_at: LATER },
      ),
    ];
    const expected = await expectLiveEqual(lines);
    const sequenced = await withAcceptance(lines);
    const orders = sequenced.flatMap((first, i) =>
      sequenced
        .filter((_, j) => j !== i)
        .flatMap((second) => [
          [
            first,
            second,
            ...sequenced.filter((v) => v !== first && v !== second),
          ],
        ]),
    );
    for (const order of orders)
      expect(
        projectRelayhistorySession(
          normalizeRelayhistoryRecords(
            order.map((value) =>
              parseRelayhistoryRecord(JSON.stringify(value)),
            ),
          ),
          context(),
        ).rows,
      ).toEqual(expected.rows);
  });

  it.each([
    ["two origins of one record", "x", "x", "session_event", "origin-b"],
    ["two records of one catalog key", "s-1", "s-2", "session", "origin-a"],
  ])(
    "refuses %s with no ledger seat, never guessing",
    (_name, first, second, kind, origin_id) => {
      const lines = [
        line(first, kind, { role: "user", ts_ms: T }),
        line(
          second,
          kind,
          { role: "user", ts_ms: T },
          { origin_id, received_at: LATER },
        ),
      ];
      expect(() =>
        projectRelayhistorySession(
          normalizeRelayhistoryRecords(
            lines.map((value) =>
              parseRelayhistoryRecord(JSON.stringify(value)),
            ),
          ),
          context(),
        ),
      ).toThrow(
        expect.objectContaining({
          code: "relayhistory_acceptance_missing",
          retryable: false,
        }),
      );
    },
  );

  it("refuses copies of one write naming different seats, in either order", () => {
    const seat = (accepted_seq: string) => ({
      ...line("x", "session_event", { role: "user", ts_ms: T }),
      accepted_seq,
    });
    for (const order of [
      [seat("7"), seat("8")],
      [seat("8"), seat("7")],
    ])
      expect(() =>
        normalizeRelayhistoryRecords(
          order.map((value) => parseRelayhistoryRecord(JSON.stringify(value))),
        ),
      ).toThrow(
        expect.objectContaining({
          code: "relayhistory_acceptance_conflict",
          retryable: false,
        }),
      );
  });

  it("does not let a re-exported copy lose its ledger seat", () => {
    const seated = {
      ...line("x", "session_event", { role: "user", ts_ms: T }),
      accepted_seq: "7",
    };
    const plain = line(
      "x",
      "session_event",
      { role: "user", ts_ms: T },
      { received_at: EARLIER },
    );
    const other = {
      ...line(
        "x",
        "session_event",
        { role: "user", ts_ms: T },
        { origin_id: "origin-b" },
      ),
      accepted_seq: "8",
    };
    const kept = normalizeRelayhistoryRecords(
      [seated, plain, other].map((value) =>
        parseRelayhistoryRecord(JSON.stringify(value)),
      ),
    );
    expect(
      kept.map((record) => [record.origin_id, record.accepted_seq]),
    ).toEqual([["origin-b", "8"]]);
  });
});

describe("usage counters past a double's precision", () => {
  const MAX = "9223372036854775807";
  const usage = (input: string) =>
    `{"input_tokens":${input},"output_tokens":1}`;
  const assistant = (token_json: string) =>
    line("a", "session_event", {
      role: "assistant",
      ts_ms: T,
      message_id: "m",
      request_id: "r",
      token_json,
    });

  async function liveTokens() {
    const { rows } = await db.query<{ input: string; output: string }>(
      "SELECT input_tokens::text AS input, output_tokens::text AS output FROM sessions.convergence_events",
    );
    return rows[0]!;
  }

  it.each([
    [MAX, MAX, "1"],
    ["9007199254740993", "9007199254740993", "1"],
    ["9223372036854775808", "0", "0"],
    ["1e2", "100", "1"],
    ["1.5", "0", "0"],
    ["-1", "0", "0"],
    ["1e400", "0", "0"],
  ])("input %s matches the column", async (input, stored, output) => {
    const lines = [assistant(usage(input))];
    await deliver(lines[0]!, "claude");
    expect(await liveTokens()).toEqual({ input: stored, output });
    const [row] = projectRelayhistorySession(
      normalizeRelayhistoryRecords(
        (await withAcceptance(lines)).map((value) =>
          parseRelayhistoryRecord(JSON.stringify(value)),
        ),
      ),
      context(),
    ).rows;
    expect(String(row!.row.input_tokens)).toBe(stored);
    expect(String(row!.row.output_tokens)).toBe(output);
  });

  it("rounds a cost past a double's precision as the numeric column does", async () => {
    const lines = [assistant('{"cost_usd":1234567890.1234565}')];
    await deliver(lines[0]!, "claude");
    const { rows } = await db.query<{ cost: string }>(
      "SELECT cost_usd_micros::text AS cost FROM sessions.convergence_events",
    );
    const [row] = projectRelayhistorySession(
      normalizeRelayhistoryRecords(
        (await withAcceptance(lines)).map((value) =>
          parseRelayhistoryRecord(JSON.stringify(value)),
        ),
      ),
      context(),
    ).rows;
    expect(String(row!.row.cost_usd_micros)).toBe(rows[0]!.cost);
  });
});

describe("timestamps at Postgres's edge", () => {
  const MAX_MS = 8_640_000_000_000_000;

  it("writes the largest accepted time as Postgres does, and sorts it last", async () => {
    const { rows } = await expectLiveEqual([
      line("edge", "session_event", { role: "user", ts_ms: MAX_MS }),
      line("past", "session_event", { role: "user", ts_ms: MAX_MS + 1 }),
      line("zero", "session_event", { role: "user", ts_ms: 0 }),
      line("now", "session_event", { role: "user", ts_ms: T }),
      line("s", "session", {
        first_activity_ms: MAX_MS,
        last_activity_ms: MAX_MS + 1,
      }),
    ]);
    const events = byTable(rows, "convergence_events");
    expect(events.map((row) => row.delivery_record_id)).toEqual([
      "zero",
      "now",
      "past",
      "edge",
    ]);
    expect(events.at(-1)!.ts).toBe("275760-09-13T00:00:00.000000Z");
    expect(byTable(rows, "session_catalog")[0]).toMatchObject({
      first_activity_at: "275760-09-13T00:00:00.000000Z",
      last_activity_at: null,
    });
  });

  it("settles a tool to a parent past year 9999 by time, not text", async () => {
    await expectLiveEqual([
      line("tool", "tool_call", { name: "Read", tool_use_id: "t1" }),
      // Year 10000 and year 9999: as text the later one sorts first.
      line("late", "session_event", {
        role: "assistant",
        tool_use_id: "t1",
        ts_ms: 253402300800000,
      }),
      line("early", "session_event", {
        role: "assistant",
        tool_use_id: "t1",
        ts_ms: 253402300799000,
      }),
    ]);
  });
});

describe("agent name lowering", () => {
  const COLLATIONS = ["default", "pg_c_utf8", "C.utf8"];
  const lowered = async (collation: string, values: string[]) =>
    (
      await db.query<{ value: string }>(
        `SELECT lower(v COLLATE "${collation}") AS value
           FROM unnest($1::text[]) WITH ORDINALITY AS t(v, n) ORDER BY n`,
        [values],
      )
    ).rows.map((row) => row.value);

  it.each(COLLATIONS)(
    "matches lower() on awkward names under %s",
    async (collation) => {
      const names = [
        "\u0130stanbul",
        "\u0131",
        "STRASSE",
        "\u00df",
        "\u1E9E",
        "\u0391\u03A3",
        "E\u0301",
        "\u00C9",
      ];
      expect(names.map(lowerAgentName)).toEqual(
        await lowered(collation, names),
      );
    },
  );

  // Only pg_c_utf8: the libc collations here are wasm's, not the target's glibc.
  // Pairs newer than Postgres's Unicode table are the runtime's to lower first,
  // and a U+FEFF result is lost to the client's decoder, not to lower().
  it("matches pg_c_utf8 on every code point its Unicode table knows", async () => {
    const newer = (code: number) =>
      [0xa7ce, 0xa7d2, 0xa7d4, 0xfeff].includes(code) ||
      (code >= 0x16ea0 && code <= 0x16eb8);
    const points: string[] = [];
    for (let code = 1; code <= 0x10ffff; code += 1)
      if ((code < 0xd800 || code > 0xdfff) && !newer(code))
        points.push(String.fromCodePoint(code));
    const theirs = await lowered("pg_c_utf8", points);
    expect(
      points.filter((point, index) => lowerAgentName(point) !== theirs[index]),
    ).toEqual([]);
  }, 120_000);
});

describe("agent_name_normalized is the target's to set", () => {
  const MIGRATION_TOOLS = fileURLToPath(
    new URL("../../../src/migration/", import.meta.url).href,
  );

  it("is replaced by the trigger whatever a writer sends, and last_ts with it", async () => {
    await db.query(
      `INSERT INTO sessions.session_catalog_agents
         (org_id, workspace_id, source, session_id, agent_name, agent_name_normalized, last_ts, record_id)
       VALUES ('org', $1, 'claude', 'no-rollup', 'Worker', 'from-the-export', now(), 'r')`,
      [WORKSPACE],
    );
    const { rows } = await db.query(
      "SELECT agent_name_normalized, last_ts FROM sessions.session_catalog_agents",
    );
    expect(rows).toEqual([{ agent_name_normalized: "worker", last_ts: null }]);
  });

  it("is never written around the trigger by the migration tools", () => {
    // The trigger-off copy skips the table; reprojection runs with triggers on.
    expect(REPROJECTED_TABLES.has("session_catalog_agents")).toBe(true);
    const reproject = readFileSync(
      `${MIGRATION_TOOLS}relayhistory-reproject.ts`,
      "utf8",
    );
    expect(reproject).not.toMatch(/session_replication_role/);
    for (const file of readdirSync(MIGRATION_TOOLS).filter((name) =>
      name.endsWith(".ts"),
    ))
      expect(readFileSync(MIGRATION_TOOLS + file, "utf8"), file).not.toMatch(
        /\b(DISABLE|ENABLE\s+REPLICA)\s+TRIGGER\b/i,
      );
  });
});

describe("export projection", () => {
  const session = context("chatgpt");

  it("maps convergence_event lines directly and merges them in event order", () => {
    const row = {
      machine_id: session.machineId,
      user_id: session.userId,
      source: "chatgpt",
      session_id: SESSION,
      event_id: "ev",
      kind: "message",
      type: "user",
      ts: "2026-10-30T12:00:00.5+00:00",
      content: "hi",
      record: { any: "thing" },
      ingested_at: "2026-10-30T12:00:01+00:00",
      ts_ms: Date.parse("2026-10-30T12:00:00.5Z"),
      org_id: "org",
      workspace_id: WORKSPACE,
      embedding: "[1]",
    };
    const { rows } = projectRelayhistorySession(
      [
        parseRelayhistoryRecord(
          JSON.stringify({
            ...line("ev", "convergence_event", row),
            origin_id: `machine:${session.machineId}`,
          }),
        ),
        parseRelayhistoryRecord(
          JSON.stringify(
            line("early", "session_event", {
              role: "user",
              ts_ms: Date.parse("2026-10-30T12:00:00Z"),
            }),
          ),
        ),
      ],
      session,
    );
    expect(rows.map(({ row: event }) => event.ts)).toEqual([
      "2026-10-30T12:00:00.000000Z",
      "2026-10-30T12:00:00.500000Z",
    ]);
    const { org_id, workspace_id, embedding, ts_ms, ...direct } = row;
    expect(rows[1]).toEqual({
      table: "convergence_events",
      row: {
        ...direct,
        ts: "2026-10-30T12:00:00.500000Z",
        ingested_at: "2026-10-30T12:00:01.000000Z",
      },
    });
  });

  it.each([
    ["source", { source: "claude" }],
    ["session_id", { session_id: "other" }],
    ["machine_id", { machine_id: "other" }],
    ["user_id", { user_id: "other" }],
    ["event_id", { event_id: "other" }],
  ])(
    "refuses a convergence_event line naming another %s",
    (_field, changed) => {
      const row = {
        machine_id: session.machineId,
        user_id: session.userId,
        source: session.source,
        session_id: SESSION,
        event_id: "ev",
        ts: "2026-10-30T12:00:00Z",
        ...changed,
      };
      expect(() =>
        projectRelayhistorySession(
          [
            parseRelayhistoryRecord(
              JSON.stringify({
                ...line("ev", "convergence_event", row),
                origin_id: `machine:${session.machineId}`,
              }),
            ),
          ],
          session,
        ),
      ).toThrow(
        expect.objectContaining({
          code: "relayhistory_event_identity",
          retryable: false,
        }),
      );
    },
  );

  it("refuses a convergence_event line from another machine's origin", () => {
    const row = {
      machine_id: session.machineId,
      user_id: session.userId,
      source: session.source,
      session_id: SESSION,
      event_id: "ev",
      ts: "2026-10-30T12:00:00Z",
    };
    expect(() =>
      projectRelayhistorySession(
        [
          parseRelayhistoryRecord(
            JSON.stringify({
              ...line("ev", "convergence_event", row),
              origin_id: "machine:other",
            }),
          ),
        ],
        session,
      ),
    ).toThrow(expect.objectContaining({ code: "relayhistory_event_identity" }));
  });

  it("keeps a direct row's bigint columns exact through the object", async () => {
    const MAX = "9223372036854775807";
    const row = `{"machine_id":"machine:1","user_id":"u","source":"chatgpt","session_id":"${SESSION}","event_id":"ev","ts":"2026-10-30T12:00:00Z","input_tokens":${MAX},"cost_usd_micros":9007199254740993}`;
    const text = `{"kind":"convergence_event","origin_id":"machine:machine:1","record_id":"ev","revision":"1","received_at":"${RECEIVED}","payload":${row}}`;
    const { rows } = projectRelayhistorySession(
      normalizeRelayhistoryRecords([
        parseRelayhistoryRecord(text),
        parseRelayhistoryRecord(text),
      ]),
      session,
    );
    expect(rows[0]!.row).toMatchObject({
      input_tokens: BigInt(MAX),
      cost_usd_micros: 9007199254740993n,
    });
  });

  it("refuses a direct row with a malformed timestamp, without retry", () => {
    const row = {
      machine_id: session.machineId,
      user_id: session.userId,
      source: session.source,
      session_id: SESSION,
      event_id: "ev",
      ts: "not a time",
    };
    expect(() =>
      projectRelayhistorySession(
        [
          parseRelayhistoryRecord(
            JSON.stringify({
              ...line("ev", "convergence_event", row),
              origin_id: `machine:${session.machineId}`,
            }),
          ),
        ],
        session,
      ),
    ).toThrow(
      expect.objectContaining({
        code: "relayhistory_event_invalid",
        retryable: false,
      }),
    );
  });

  it("emits nothing for a stream with no deletes to apply and no records", () => {
    expect(projectRelayhistorySession([], session)).toEqual({
      rows: [],
      unprojected: 0,
    });
  });

  it("orders events by time then event id, not by stream order", () => {
    const { rows } = projectRelayhistorySession(
      normalizeRelayhistoryRecords(
        [
          line("late", "session_event", { role: "user", ts_ms: T + 1 }),
          line("b", "session_event", { role: "user", ts_ms: T }),
          line("a", "session_event", { role: "user", ts_ms: T }),
        ].map((value) => parseRelayhistoryRecord(JSON.stringify(value))),
      ),
      session,
    );
    expect(rows.map(({ row }) => row.delivery_record_id)).toEqual([
      "a",
      "b",
      "late",
    ]);
  });
});
