/**
 * Search parity for sessions.search_session_directory: every filter the RelayHistory
 * read surface exposes, alone and composed with text and the cursor, paged to the end
 * and compared with an oracle computed from the generated rows (never from the
 * engine). Unsupported combinations must be typed errors, never empty results.
 * In-process PGlite only; synthetic data.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { auto_explain } from "@electric-sql/pglite/contrib/auto_explain";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  SESSION_DIRECTORY_LIST_COLUMNS,
  SESSION_DIRECTORY_QUERIES,
  SESSION_DIRECTORY_SEARCH_RPC,
} from "../search-index-contract.ts";
import {
  searchRpcRequest,
  type SessionSearchRequest,
} from "../search-query.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;
const TOKEN_ISSUER = "https://issuer.test/session-directory";

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { auto_explain, btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  // The production PostgreSQL session is UTC whatever the client host's zone; in-process
  // PGlite would inherit the host TZ, so pin it to model production.
  await db.exec("SET TIME ZONE 'UTC'");
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
  );
  return db;
}

type Row = {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  created_at: Date;
  last_activity_at: Date;
  title: string;
  agent_name: string | null;
  project_id: string | null;
  state: string;
  short_summary: string | null;
  deleted: boolean;
  users: string[];
  machines: string[];
};

const WORDS = [
  "queue",
  "ledger",
  "pelican",
  "harbor",
  "falcon",
  "quartz",
  "meadow",
];
const HARNESSES = ["codex", "claude", "opencode"];
const STATES = ["active", "done", "abandoned", "unknown"];
const BASE = Date.parse("2026-09-01T00:00:00Z");

/** Deterministic, with deliberate keyset ties (same last_activity_at, same session id). */
function generate(): Row[] {
  const rows: Row[] = [];
  const tenants: Array<[string, string, number]> = [
    ["org-a", "ws-a", 240],
    ["org-a", "ws-b", 80],
    ["org-b", "ws-a", 60],
  ];
  let n = 0;
  for (const [org, ws, count] of tenants) {
    for (let i = 0; i < count; i += 1, n += 1) {
      const word = WORDS[n % WORDS.length]!;
      // Every 10th row shares the previous row's timestamp; every 15th reuses its session id.
      const tick = i % 10 === 0 && i > 0 ? i - 1 : i;
      const sessionNo = i % 15 === 0 && i > 0 ? i - 1 : i;
      const created = BASE + i * 3_600_000;
      rows.push({
        org_id: org,
        workspace_id: ws,
        harness: HARNESSES[i % 3]!,
        session_id: `s-${String(sessionNo).padStart(4, "0")}`,
        created_at: new Date(created),
        last_activity_at: new Date(
          BASE + tick * 3_600_000 + 7_200_000 + (i % 7) * 0,
        ),
        title: `Session ${word} ${i}`,
        agent_name: i % 4 === 0 ? null : `Agent-${word}-${i % 13}`,
        project_id:
          i % 5 === 0
            ? null
            : i % 9 === 0
              ? `alpha-proj-${i % 3}`
              : `proj-${i % 7}`,
        state: STATES[i % 4]!,
        short_summary:
          i % 3 === 0
            ? `Notes about the ${WORDS[(n + 2) % WORDS.length]} reconciler`
            : null,
        deleted: i % 17 === 16,
        users: i % 9 === 0 ? [`u-${i % 5}`, "u-shared"] : [`u-${i % 5}`],
        machines: i % 6 === 0 ? [`m-${i % 4}`, "m-shared"] : [`m-${i % 4}`],
      });
    }
  }
  // Drop exact key duplicates produced by the session-id reuse on the same harness.
  const seen = new Set<string>();
  return rows.filter((r) => {
    const k = `${r.org_id}/${r.workspace_id}/${r.harness}/${r.session_id}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

const searchDocument = (r: Row) =>
  [r.title, r.agent_name, r.project_id, r.short_summary]
    .map((v) => v?.trim())
    .filter((v) => v)
    .join(" ");

async function load(db: PGlite, rows: Row[]) {
  for (const r of rows) {
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, agent_name,
          agent_name_normalized, project_id, state, machine_ids, user_ids, short_summary,
          short_summary_scrub_ruleset, short_summary_scrub_version, short_summary_source, short_summary_generated_at,
          search_text, deleted_at)
       VALUES ($1, $2, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8::text, lower(btrim($8::text)), $9, $10, $11, $12, $13::text,
               CASE WHEN $13::text IS NULL THEN NULL ELSE 'relayhistory-scrub' END,
               CASE WHEN $13::text IS NULL THEN NULL ELSE 1 END,
               CASE WHEN $13::text IS NULL THEN NULL ELSE 'bob-costas/v1' END,
               CASE WHEN $13::text IS NULL THEN NULL ELSE $6::timestamptz END,
               sessions.session_directory_search_text($7::text, NULL, $8::text, $9::text, NULL, NULL, NULL, $13::text),
               CASE WHEN $14::boolean THEN $6::timestamptz END)`,
      [
        r.org_id,
        r.workspace_id,
        r.harness,
        r.session_id,
        r.created_at.toISOString(),
        r.last_activity_at.toISOString(),
        r.title,
        r.agent_name,
        r.project_id,
        r.state,
        r.machines,
        r.users,
        r.short_summary,
        r.deleted,
      ],
    );
    for (const [table, column, values] of [
      ["users", "user_id", r.users],
      ["machines", "machine_id", r.machines],
    ] as const) {
      for (const v of values) {
        await db.query(
          `INSERT INTO sessions.session_directory_${table} (org_id, workspace_id, harness, session_id, ${column}, last_activity_at)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [
            r.org_id,
            r.workspace_id,
            r.harness,
            r.session_id,
            v,
            r.last_activity_at.toISOString(),
          ],
        );
      }
    }
  }
  await db.exec("ANALYZE");
}

/** The read surface's semantics, applied to the generated rows. */
function oracle(
  rows: Row[],
  org: string,
  claimed: string[],
  req: SessionSearchRequest,
): string[] {
  const agent = req.agent?.trim().toLowerCase();
  const out = rows.filter(
    (r) =>
      r.org_id === org &&
      !r.deleted &&
      (req.workspaceId
        ? r.workspace_id === req.workspaceId
        : claimed.includes(r.workspace_id)) &&
      (req.q === undefined ||
        searchDocument(r).toLowerCase().includes(req.q.toLowerCase())) &&
      (req.source === undefined || r.harness === req.source) &&
      (req.projectId === undefined || r.project_id === req.projectId) &&
      (req.projectContains === undefined ||
        (r.project_id ?? "")
          .toLowerCase()
          .includes(req.projectContains.toLowerCase())) &&
      (!req.noProject || r.project_id === null) &&
      (agent === undefined ||
        ((req.agentMatch ?? "exact") === "exact"
          ? r.agent_name?.toLowerCase() === agent
          : !!r.agent_name?.toLowerCase().startsWith(agent))) &&
      (req.state === undefined || r.state === req.state) &&
      (req.since === undefined ||
        r.last_activity_at.getTime() >= Date.parse(req.since)) &&
      (req.until === undefined ||
        r.last_activity_at.getTime() <= Date.parse(req.until)) &&
      (req.sessionId === undefined || r.session_id === req.sessionId) &&
      (req.userId === undefined || r.users.includes(req.userId)) &&
      (req.machineId === undefined || r.machines.includes(req.machineId)),
  );
  const cmp = (a: string, b: string) => (a < b ? 1 : a > b ? -1 : 0);
  out.sort(
    (a, b) =>
      b.last_activity_at.getTime() - a.last_activity_at.getTime() ||
      cmp(a.session_id, b.session_id) ||
      cmp(a.harness, b.harness) ||
      (req.workspaceId ? 0 : cmp(a.workspace_id, b.workspace_id)),
  );
  return out.map(identity);
}
const identity = (r: {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
}) => `${r.org_id}/${r.workspace_id}/${r.harness}/${r.session_id}`;

const ARG_TYPES: Record<string, string> = {
  p_no_project: "boolean",
  p_limit: "integer",
  p_since: "timestamptz",
  p_until: "timestamptz",
  p_cursor_last_activity_at: "timestamptz",
};
const claimsFor = (org: string, workspaces: unknown[]) => {
  const iat = Math.floor(Date.now() / 1000);
  return JSON.stringify({
    role: WORKER,
    iss: TOKEN_ISSUER,
    aud: SESSION_DIRECTORY_SEARCH_RPC.audience,
    iat,
    exp: iat + 60,
    app_metadata: { org_id: org, workspace_ids: workspaces },
  });
};

/** Calls the RPC as PostgREST would: SET ROLE Worker, claims set, named arguments. */
async function rpc(
  db: PGlite,
  claims: string,
  body: Record<string, unknown>,
  onNotice?: (m: string) => void,
) {
  const keys = Object.keys(body);
  const args = keys
    .map((k, i) => `${k} => $${i + 1}::${ARG_TYPES[k] ?? "text"}`)
    .join(", ");
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    claims,
  ]);
  await db.exec(`SET ROLE ${WORKER}`);
  try {
    return (
      await db.query<Record<string, unknown>>(
        `SELECT * FROM sessions.search_session_directory(${args})`,
        keys.map((k) => body[k]),
        onNotice
          ? ({
              onNotice: (n: { message: string }) => onNotice(n.message),
            } as never)
          : undefined,
      )
    ).rows;
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

/** Pages through the RPC with the keyset cursor until the end. */
async function pageAll(
  db: PGlite,
  claims: string,
  req: SessionSearchRequest,
  limit: number,
) {
  const seen: string[] = [];
  let cursor: SessionSearchRequest["cursor"];
  for (let page = 0; page < 200; page += 1) {
    const built = searchRpcRequest({
      ...req,
      limit,
      ...(cursor ? { cursor } : {}),
    });
    if (!built.ok) throw new Error(`refused: ${built.code} ${built.reason}`);
    const rows = await rpc(db, claims, built.body);
    expect(rows.length).toBeLessThanOrEqual(limit + 1);
    const pageRows = rows.slice(0, limit);
    seen.push(...pageRows.map((r) => identity(r as never)));
    if (rows.length <= limit) return seen;
    const last = pageRows.at(-1)!;
    cursor = {
      lastActivityAt: (last.last_activity_at as Date).toISOString(),
      sessionId: String(last.session_id),
      harness: String(last.harness),
      ...(req.workspaceId ? {} : { workspaceId: String(last.workspace_id) }),
    };
  }
  throw new Error("pagination did not terminate");
}

let db: PGlite;
let rows: Row[];
const A = claimsFor("org-a", ["ws-a", "ws-b"]);

beforeAll(async () => {
  db = await database();
  rows = generate();
  await load(db, rows);
}, 120_000);
afterAll(async () => {
  await db?.close();
});

const SHAPES: Array<[string, SessionSearchRequest]> = [
  ["workspace recent", { workspaceId: "ws-a" }],
  ["org recent (claimed workspaces)", {}],
  ["q", { workspaceId: "ws-a", q: "pelican" }],
  ["q org-wide", { q: "reconciler" }],
  ["source", { workspaceId: "ws-a", source: "claude" }],
  ["q + source", { workspaceId: "ws-a", q: "ledger", source: "codex" }],
  ["project exact", { workspaceId: "ws-a", projectId: "proj-3" }],
  [
    "project exact + q",
    { workspaceId: "ws-a", projectId: "proj-3", q: "session" },
  ],
  ["project contains", { workspaceId: "ws-a", projectContains: "ALPHA-proj" }],
  [
    "project contains + q + state",
    {
      workspaceId: "ws-a",
      projectContains: "proj",
      q: "harbor",
      state: "done",
    },
  ],
  [
    "project exact + project contains (ANDed)",
    {
      workspaceId: "ws-a",
      projectId: "alpha-proj-0",
      projectContains: "proj-0",
    },
  ],
  ["no project", { workspaceId: "ws-a", noProject: true }],
  ["no project + q", { workspaceId: "ws-a", noProject: true, q: "falcon" }],
  ["since", { workspaceId: "ws-a", since: "2026-09-05T00:00:00.000Z" }],
  ["until", { workspaceId: "ws-a", until: "2026-09-04T00:00:00.000Z" }],
  [
    "since + until + q",
    {
      workspaceId: "ws-a",
      since: "2026-09-03T00:00:00.000Z",
      until: "2026-09-08T00:00:00.000Z",
      q: "session",
    },
  ],
  [
    "since + until org-wide + source",
    {
      since: "2026-09-02T00:00:00.000Z",
      until: "2026-09-06T00:00:00.000Z",
      source: "codex",
    },
  ],
  [
    "harness + state",
    { workspaceId: "ws-a", source: "opencode", state: "active" },
  ],
  ["state org-wide", { state: "abandoned" }],
  [
    "agent exact (case-insensitive)",
    { workspaceId: "ws-a", agent: "  AGENT-queue-7 " },
  ],
  [
    "agent exact + q",
    { workspaceId: "ws-a", agent: "agent-pelican-2", q: "session" },
  ],
  [
    "agent prefix",
    { workspaceId: "ws-a", agent: "agent-qu", agentMatch: "prefix" },
  ],
  [
    "agent prefix + since + until",
    {
      workspaceId: "ws-a",
      agent: "agent-",
      agentMatch: "prefix",
      since: "2026-09-03T00:00:00.000Z",
      until: "2026-09-09T00:00:00.000Z",
    },
  ],
  [
    "agent prefix + q + state",
    {
      workspaceId: "ws-a",
      agent: "agent-l",
      agentMatch: "prefix",
      q: "ledger",
      state: "unknown",
    },
  ],
  ["user", { workspaceId: "ws-a", userId: "u-2" }],
  ["user + q", { workspaceId: "ws-a", userId: "u-shared", q: "session" }],
  ["machine", { workspaceId: "ws-a", machineId: "m-1" }],
  [
    "machine + state + source",
    {
      workspaceId: "ws-a",
      machineId: "m-shared",
      state: "active",
      source: "codex",
    },
  ],
  [
    "user + machine (ANDed)",
    { workspaceId: "ws-a", userId: "u-shared", machineId: "m-shared" },
  ],
  [
    "user + agent prefix + q",
    {
      workspaceId: "ws-a",
      userId: "u-1",
      agent: "agent-",
      agentMatch: "prefix",
      q: "session",
    },
  ],
  ["session id (org-wide)", { sessionId: "s-0014" }],
  [
    "session id + workspace + source",
    { workspaceId: "ws-a", sessionId: "s-0014", source: "claude" },
  ],
  [
    "everything at once",
    {
      workspaceId: "ws-a",
      q: "session",
      source: "codex",
      projectContains: "proj",
      agent: "agent-",
      agentMatch: "prefix",
      state: "done",
      userId: "u-3",
      machineId: "m-3",
      since: "2026-09-02T00:00:00.000Z",
      until: "2026-09-12T00:00:00.000Z",
    },
  ],
];

describe("search RPC parity with the read surface", () => {
  for (const [name, req] of SHAPES) {
    test(`${name}: every page, in order, equals the oracle`, async () => {
      const expected = oracle(rows, "org-a", ["ws-a", "ws-b"], req);
      for (const limit of [7, 50]) {
        expect(await pageAll(db, A, req, limit)).toEqual(expected);
      }
    });
  }

  test("the shapes are not vacuous: most return rows and several span pages", () => {
    const sizes = SHAPES.map(
      ([, req]) => oracle(rows, "org-a", ["ws-a", "ws-b"], req).length,
    );
    expect(sizes.filter((n) => n > 0).length).toBeGreaterThanOrEqual(
      SHAPES.length - 3,
    );
    expect(sizes.filter((n) => n > 7).length).toBeGreaterThanOrEqual(15);
  });

  test("tenancy: org-b with the same workspace id never sees org-a, and unclaimed workspaces are refused", async () => {
    const B = claimsFor("org-b", ["ws-a"]);
    const hits = await pageAll(
      db,
      B,
      { workspaceId: "ws-a", q: "session" },
      50,
    );
    expect(hits.length).toBeGreaterThan(0);
    expect(hits.every((h) => h.startsWith("org-b/"))).toBe(true);
    const onlyA = claimsFor("org-a", ["ws-a"]);
    const orgWide = await pageAll(db, onlyA, {}, 50);
    expect(orgWide.every((h) => h.startsWith("org-a/ws-a/"))).toBe(true);
    await expect(
      rpc(db, onlyA, { p_workspace_id: "ws-b" }),
    ).rejects.toMatchObject({ code: "42501" });
  });
});

describe("workspace scope stays inside the claim", () => {
  // Claims cover only ws-a; org-a also has ws-b rows that match every probe below.
  const onlyA = claimsFor("org-a", ["ws-a"]);
  const wsBRow = () =>
    rows.find(
      (r) => r.org_id === "org-a" && r.workspace_id === "ws-b" && !r.deleted,
    )!;

  test("p_workspace_id NULL sees exactly the claimed workspaces, never the rest of the org", async () => {
    const b = wsBRow();
    const probes: SessionSearchRequest[] = [
      {},
      { q: "session" },
      { q: b.title },
      { sessionId: b.session_id },
      { state: b.state as never },
      { source: b.harness },
      { since: "2026-09-01T00:00:00.000Z" },
      { until: "2026-12-31T00:00:00.000Z" },
    ];
    for (const req of probes) {
      // The probe does match ws-b rows when ws-b is claimed, so an empty answer is meaningful.
      expect(
        oracle(rows, "org-a", ["ws-a", "ws-b"], req).some((id) =>
          id.startsWith("org-a/ws-b/"),
        ),
        JSON.stringify(req),
      ).toBe(true);
      const hits = await pageAll(db, onlyA, req, 50);
      expect(
        hits.every((id) => id.startsWith("org-a/ws-a/")),
        JSON.stringify(req),
      ).toBe(true);
      expect(hits).toEqual(oracle(rows, "org-a", ["ws-a"], req));
    }
  });

  test("an organization-wide cursor naming an unclaimed workspace cannot widen the page", async () => {
    // Inside the time range both workspaces share, so a leak would show on this page.
    const at = "2026-09-03T12:00:00.000Z";
    expect(
      rows.some(
        (r) =>
          r.org_id === "org-a" &&
          r.workspace_id === "ws-b" &&
          !r.deleted &&
          r.last_activity_at.getTime() < Date.parse(at),
      ),
    ).toBe(true);
    const hits = await rpc(db, onlyA, {
      p_cursor_last_activity_at: at,
      p_cursor_session_id: "s-9999",
      p_cursor_harness: "zzz",
      p_cursor_workspace_id: "ws-b",
      p_limit: 50,
    });
    expect(hits.length).toBeGreaterThan(0);
    expect(hits.every((h) => h.workspace_id === "ws-a")).toBe(true);
  });

  test("a non-NULL p_workspace_id outside the claim is refused for every filter shape", async () => {
    for (const body of [
      { p_workspace_id: "ws-b" },
      { p_workspace_id: "ws-b", p_query: "session" },
      { p_workspace_id: "ws-b", p_session_id: wsBRow().session_id },
      { p_workspace_id: "ws-b", p_user_id: "u-1" },
      {
        p_workspace_id: "ws-b",
        p_agent: "agent-queue-1",
        p_agent_match: "prefix",
      },
      { p_workspace_id: "WS-A" },
      { p_workspace_id: " ws-a" },
      { p_workspace_id: "" },
    ]) {
      const error = (await rpc(db, onlyA, body).then(
        () => null,
        (c: unknown) => c,
      )) as { code?: string } | null;
      expect(error?.code, JSON.stringify(body)).toBe("42501");
    }
  });

  test("non-string or empty workspace claims are refused", async () => {
    for (const set of [[7, null, { id: "ws-b" }, "ws-a"], []]) {
      const error = await rpc(db, claimsFor("org-a", set), {}).then(() => null, (c: unknown) => c) as { code?: string } | null;
      expect(error?.code).toBe("42501");
    }
  });
});

describe("short_summary reachability", () => {
  // Where an authenticated end user's short_summary is returned: by the RPC, which the
  // Worker calls with that user's tenant claims. The direct table surface omits it by
  // grant, and asking for it there is a loud permission error, never a silent drop.
  const summaries = () =>
    new Map(
      rows
        .filter((r) => r.org_id === "org-a")
        .map((r) => [identity(r), r.short_summary]),
    );

  test("the RPC returns short_summary on every list shape, with or without text", async () => {
    const expected = summaries();
    const shapes: SessionSearchRequest[] = [
      { workspaceId: "ws-a" },
      {},
      { workspaceId: "ws-a", q: "reconciler" },
      { workspaceId: "ws-a", agent: "agent-", agentMatch: "prefix" },
      { workspaceId: "ws-a", projectId: "proj-3" },
      { workspaceId: "ws-a", userId: "u-2" },
      { workspaceId: "ws-a", machineId: "m-1" },
      { sessionId: "s-0003" },
    ];
    let withSummary = 0;
    for (const req of shapes) {
      const built = searchRpcRequest(req);
      if (!built.ok) throw new Error(built.reason);
      const hits = await rpc(db, A, built.body);
      expect(hits.length, JSON.stringify(req)).toBeGreaterThan(0);
      for (const hit of hits) {
        expect(Object.keys(hit)).toContain("short_summary");
        expect(hit.short_summary, identity(hit as never)).toBe(
          expected.get(identity(hit as never)) ?? null,
        );
        if (hit.short_summary !== null) withSummary += 1;
      }
    }
    expect(withSummary).toBeGreaterThan(20);
  });

  test("an authenticated direct read cannot see short_summary, and asking for it fails loudly", async () => {
    expect(SESSION_DIRECTORY_LIST_COLUMNS).not.toContain(
      "short_summary" as never,
    );
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
      JSON.stringify({
        role: "authenticated",
        app_metadata: { org_id: "org-a", workspace_ids: ["ws-a"] },
      }),
    ]);
    await db.exec("SET ROLE authenticated");
    try {
      const list = await db.query<Record<string, unknown>>(
        SESSION_DIRECTORY_QUERIES.workspaceRecent,
        ["org-a", "ws-a", 50],
      );
      expect(list.rows.length).toBeGreaterThan(0);
      expect(Object.keys(list.rows[0]!)).toEqual([
        ...SESSION_DIRECTORY_LIST_COLUMNS,
      ]);
      for (const sql of [
        "SELECT short_summary FROM sessions.session_directory",
        "SELECT * FROM sessions.session_directory",
        "SELECT session_id FROM sessions.session_directory WHERE short_summary ILIKE '%reconciler%'",
      ]) {
        const error = (await db.query(sql).then(
          () => null,
          (c: unknown) => c,
        )) as { code?: string } | null;
        expect(error?.code, sql).toBe("42501");
      }
    } finally {
      await db.exec("RESET ROLE");
      await db.query("SELECT set_config('request.jwt.claims', '', false)");
    }
  });
});

describe("typed refusals, never empty results", () => {
  const CASES: Array<
    [string, SessionSearchRequest, Record<string, unknown>, "22023" | "0A000"]
  > = [
    [
      "org-wide agent",
      { agent: "agent-queue-1" },
      { p_agent: "agent-queue-1" },
      "0A000",
    ],
    [
      "org-wide project",
      { projectId: "proj-1" },
      { p_project_id: "proj-1" },
      "0A000",
    ],
    [
      "org-wide project contains",
      { projectContains: "proj" },
      { p_project_contains: "proj" },
      "0A000",
    ],
    [
      "org-wide no project",
      { noProject: true },
      { p_no_project: true },
      "0A000",
    ],
    ["org-wide user", { userId: "u-1" }, { p_user_id: "u-1" }, "0A000"],
    [
      "org-wide machine",
      { machineId: "m-1" },
      { p_machine_id: "m-1" },
      "0A000",
    ],
    [
      "no project + project",
      { workspaceId: "ws-a", noProject: true, projectId: "proj-1" },
      { p_workspace_id: "ws-a", p_no_project: true, p_project_id: "proj-1" },
      "22023",
    ],
    [
      "no project + project contains",
      { workspaceId: "ws-a", noProject: true, projectContains: "proj" },
      {
        p_workspace_id: "ws-a",
        p_no_project: true,
        p_project_contains: "proj",
      },
      "22023",
    ],
    [
      "since after until",
      {
        workspaceId: "ws-a",
        since: "2026-09-09T00:00:00Z",
        until: "2026-09-01T00:00:00Z",
      },
      {
        p_workspace_id: "ws-a",
        p_since: "2026-09-09T00:00:00Z",
        p_until: "2026-09-01T00:00:00Z",
      },
      "22023",
    ],
    [
      "limit 0",
      { workspaceId: "ws-a", limit: 0 },
      { p_workspace_id: "ws-a", p_limit: 0 },
      "22023",
    ],
    [
      "limit 51",
      { workspaceId: "ws-a", limit: 51 },
      { p_workspace_id: "ws-a", p_limit: 51 },
      "22023",
    ],
    [
      "two-character q",
      { workspaceId: "ws-a", q: "ab" },
      { p_workspace_id: "ws-a", p_query: "ab" },
      "22023",
    ],
    [
      "two-character project contains",
      { workspaceId: "ws-a", projectContains: "pr" },
      { p_workspace_id: "ws-a", p_project_contains: "pr" },
      "22023",
    ],
    [
      "two-character agent prefix",
      { workspaceId: "ws-a", agent: "ag", agentMatch: "prefix" },
      { p_workspace_id: "ws-a", p_agent: "ag", p_agent_match: "prefix" },
      "22023",
    ],
    [
      "NBSP-padded agent prefix",
      { workspaceId: "ws-a", agent: "a g", agentMatch: "prefix" },
      { p_workspace_id: "ws-a", p_agent: "a g", p_agent_match: "prefix" },
      "22023",
    ],
    [
      "blank agent",
      { workspaceId: "ws-a", agent: "   " },
      { p_workspace_id: "ws-a", p_agent: "   " },
      "22023",
    ],
    [
      "unknown state",
      { workspaceId: "ws-a", state: "paused" as never },
      { p_workspace_id: "ws-a", p_state: "paused" },
      "22023",
    ],
    [
      "invalid harness",
      { workspaceId: "ws-a", source: "Not A Harness" },
      { p_workspace_id: "ws-a", p_harness: "Not A Harness" },
      "22023",
    ],
    [
      "org-wide cursor without workspace",
      {
        cursor: {
          lastActivityAt: "2026-09-05T00:00:00.000Z",
          sessionId: "s-0001",
          harness: "codex",
        },
      },
      {
        p_cursor_last_activity_at: "2026-09-05T00:00:00.000Z",
        p_cursor_session_id: "s-0001",
        p_cursor_harness: "codex",
      },
      "22023",
    ],
    [
      "cursor for another workspace",
      {
        workspaceId: "ws-a",
        cursor: {
          lastActivityAt: "2026-09-05T00:00:00.000Z",
          sessionId: "s-0001",
          harness: "codex",
          workspaceId: "ws-b",
        },
      },
      {
        p_workspace_id: "ws-a",
        p_cursor_last_activity_at: "2026-09-05T00:00:00.000Z",
        p_cursor_session_id: "s-0001",
        p_cursor_harness: "codex",
        p_cursor_workspace_id: "ws-b",
      },
      "22023",
    ],
  ];
  for (const [name, req, body, code] of CASES) {
    test(`${name}: ${code} in TypeScript and in SQL`, async () => {
      const built = searchRpcRequest(req);
      expect(built.ok).toBe(false);
      expect(!built.ok && built.code).toBe(code);
      const error = (await rpc(db, A, body).then(
        () => null,
        (caught: unknown) => caught,
      )) as { code?: string } | null;
      expect(error?.code).toBe(code);
    });
  }

  test("agent trimming matches the RPC's btrim: spaces only", async () => {
    // A tab-only agent is not blank to lower(btrim(...)); TypeScript must not refuse it.
    const built = searchRpcRequest({ workspaceId: "ws-a", agent: "\t" });
    expect(built.ok).toBe(true);
    expect(built.ok && built.body.p_agent).toBe("\t");
    expect(await rpc(db, A, { p_workspace_id: "ws-a", p_agent: "\t" })).toEqual(
      [],
    );
  });

  test("an unparseable cursor timestamp is a typed 22023 in TypeScript", () => {
    const built = searchRpcRequest({
      workspaceId: "ws-a",
      cursor: { lastActivityAt: "bad-date", sessionId: "s", harness: "codex" },
    });
    expect(built.ok).toBe(false);
    expect(!built.ok && built.code).toBe("22023");
  });

  test("an unparseable since or until is a typed 22023 in TypeScript", () => {
    for (const field of ["since", "until"] as const) {
      const built = searchRpcRequest({
        workspaceId: "ws-a",
        [field]: "not-a-date",
      });
      expect(built.ok).toBe(false);
      expect(!built.ok && built.code).toBe("22023");
    }
  });

  test("SQL-only refusals: incomplete cursor, unknown agent match", async () => {
    for (const body of [
      { p_workspace_id: "ws-a", p_cursor_session_id: "s-0001" },
      {
        p_workspace_id: "ws-a",
        p_agent: "agent-queue-1",
        p_agent_match: "fuzzy",
      },
    ]) {
      expect(
        (
          (await rpc(db, A, body).then(
            () => null,
            (c: unknown) => c,
          )) as { code?: string } | null
        )?.code,
      ).toBe("22023");
    }
  });
});

describe("search RPC plans as the Worker identity (PGlite, 30,000 rows)", () => {
  let big: PGlite;
  type Plan = { [key: string]: unknown; Plans?: Plan[] };
  const walk = (plan: Plan): Plan[] => [
    plan,
    ...(plan.Plans ?? []).flatMap(walk),
  ];

  beforeAll(async () => {
    big = await database();
    // A large tenant (org-0/ws-0, 22,500 rows) beside 7,500 rows of other tenants, with
    // the benchmark's agent naming (agent-<word>-<0..49>) and memberships.
    await big.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, agent_name, agent_name_normalized,
         project_id, state, machine_ids, user_ids, search_text)
      SELECT CASE WHEN n % 4 = 0 THEN 'org-' || (n % 7 + 1) ELSE 'org-0' END,
             CASE WHEN n % 4 = 0 THEN 'ws-' || (n % 7 + 1) ELSE 'ws-0' END,
             (ARRAY['codex','claude','opencode'])[n % 3 + 1], 's-' || n,
             '2026-09-01T00:00:00Z'::timestamptz + n * interval '1 minute',
             '2026-09-01T00:00:00Z'::timestamptz + n * interval '1 minute' + interval '30 minutes',
             'Session ' || n,
             CASE WHEN n % 10 < 7 THEN 'agent-' || (ARRAY['queue','ledger','pelican','harbor','falcon','quartz','meadow','cobalt','ember','lumen','orbit','prism','rune','sable','tidal','umber','vivid','willow','xenon','zephyr'])[n % 20 + 1] || '-' || (n % 50) END,
             CASE WHEN n % 10 < 7 THEN 'agent-' || (ARRAY['queue','ledger','pelican','harbor','falcon','quartz','meadow','cobalt','ember','lumen','orbit','prism','rune','sable','tidal','umber','vivid','willow','xenon','zephyr'])[n % 20 + 1] || '-' || (n % 50) END,
             CASE WHEN n % 8 = 3 THEN NULL WHEN n % 1000 = 1 THEN 'zebra-crossing-project' ELSE 'project-' || (n % 40) END,
             (ARRAY['active','done','abandoned','unknown'])[n % 4 + 1],
             ARRAY['m-' || (n % 30)], ARRAY['u-' || (n % 60)], ''
        FROM generate_series(1, 30000) AS n;
      UPDATE sessions.session_directory SET search_text = sessions.session_directory_search_text(
        title, task_title, agent_name, project_id, git_repo, git_branch, model, short_summary);
      INSERT INTO sessions.session_directory_users
      SELECT org_id, workspace_id, harness, session_id, user_ids[1], last_activity_at FROM sessions.session_directory;
      INSERT INTO sessions.session_directory_machines
      SELECT org_id, workspace_id, harness, session_id, machine_ids[1], last_activity_at FROM sessions.session_directory;
      ANALYZE;
    `);
    await big.exec(`LOAD 'auto_explain';
      SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on; SET auto_explain.log_buffers = on;
      SET auto_explain.log_format = 'json'; SET auto_explain.log_nested_statements = on;`);
  }, 120_000);
  afterAll(async () => {
    await big?.close();
  });

  async function innerPlan(req: SessionSearchRequest) {
    const built = searchRpcRequest(req);
    if (!built.ok) throw new Error(built.reason);
    const messages: string[] = [];
    await big.exec("SET client_min_messages = log");
    let rows: Record<string, unknown>[];
    try {
      rows = await rpc(big, claimsFor("org-0", ["ws-0"]), built.body, (m) =>
        messages.push(m),
      );
    } finally {
      await big.exec("SET client_min_messages = notice");
    }
    const entry = messages
      .filter((m) => m.includes('"Query Text"'))
      .map(
        (m) =>
          JSON.parse(m.slice(m.indexOf("{"))) as {
            "Query Text": string;
            Plan: Plan;
          },
      )
      .find((e) =>
        /FROM sessions\.session_directory(_users|_machines)? AS /.test(
          e["Query Text"],
        ),
      );
    if (!entry) throw new Error("no inner plan captured");
    const nodes = walk(entry.Plan);
    const scans = nodes.filter(
      (n) =>
        /Scan$/.test(String(n["Node Type"])) &&
        n["Node Type"] !== "Bitmap Index Scan",
    );
    const examined = scans.reduce(
      (t, n) =>
        t +
        Number(n["Actual Rows"] ?? 0) * Number(n["Actual Loops"] ?? 1) +
        Number(n["Rows Removed by Filter"] ?? 0) +
        Number(n["Rows Removed by Index Recheck"] ?? 0),
      0,
    );
    console.info(
      `[G6 parity plan] ${JSON.stringify(req)} ${nodes.map((n) => [n["Node Type"], n["Index Name"], n["Index Cond"]].filter(Boolean).join(" | ")).join("  ->  ")} returned=${rows.length} examined=${examined}`,
    );
    return { nodes, rows, examined, sql: entry["Query Text"] };
  }

  // The tenant's agents for word index 1 are agent-ledger-{1,11,21,31,41}.
  const PLAN_CASES: Array<
    [string, SessionSearchRequest, string | string[], string | null]
  > = [
    [
      "agent exact",
      { workspaceId: "ws-0", agent: "agent-ledger-21" },
      "session_directory_agent_recent_idx",
      "agent_name_normalized",
    ],
    [
      "agent prefix (selective)",
      { workspaceId: "ws-0", agent: "agent-ledger-1", agentMatch: "prefix" },
      "session_directory_agent_recent_idx",
      "agent_name_normalized",
    ],
    [
      "agent prefix + q",
      {
        workspaceId: "ws-0",
        agent: "agent-ledger-1",
        agentMatch: "prefix",
        q: "session",
      },
      "session_directory_agent_recent_idx",
      "agent_name_normalized",
    ],
    [
      "project contains (rare)",
      { workspaceId: "ws-0", projectContains: "zebra-cross" },
      "session_directory_tenant_project_trgm_idx",
      "project_id",
    ],
    [
      "project contains (2.5% of rows)",
      { workspaceId: "ws-0", projectContains: "project-37" },
      [
        "session_directory_tenant_project_trgm_idx",
        "session_directory_workspace_recent_idx",
      ],
      null,
    ],
    [
      "user",
      { workspaceId: "ws-0", userId: "u-7" },
      "session_directory_users_recent_idx",
      "user_id",
    ],
    [
      "machine",
      { workspaceId: "ws-0", machineId: "m-7" },
      "session_directory_machines_recent_idx",
      "machine_id",
    ],
    [
      "session id org-wide",
      { sessionId: "s-12345" },
      "session_directory_session_lookup_idx",
      "session_id",
    ],
    ["org recent", {}, "session_directory_org_recent_idx", null],
    [
      "workspace recent",
      { workspaceId: "ws-0" },
      "session_directory_workspace_recent_idx",
      null,
    ],
    [
      "project exact",
      { workspaceId: "ws-0", projectId: "project-5" },
      "session_directory_project_recent_idx",
      "project_id",
    ],
    [
      "no project",
      { workspaceId: "ws-0", noProject: true },
      "session_directory_no_project_recent_idx",
      null,
    ],
    [
      "state",
      { workspaceId: "ws-0", state: "done" },
      "session_directory_state_recent_idx",
      "state",
    ],
    [
      "harness",
      { workspaceId: "ws-0", source: "claude" },
      "session_directory_harness_recent_idx",
      "harness",
    ],
  ];
  for (const [name, req, index, condColumn] of PLAN_CASES) {
    test(`${name}: ${[index].flat().join(" or ")}, no seq scan, row amplification within the gate`, async () => {
      const { nodes, rows, examined } = await innerPlan(req);
      expect(nodes.some((n) => n["Node Type"] === "Seq Scan")).toBe(false);
      const scan = nodes.find((n) =>
        [index].flat().includes(String(n["Index Name"])),
      );
      expect(scan, `${index} not used`).toBeDefined();
      expect(rows.length, "the plan case must return rows").toBeGreaterThan(0);
      if (condColumn)
        expect(
          String(scan!["Index Cond"] ?? scan!["Recheck Cond"] ?? ""),
        ).toContain(condColumn);
      if (rows.length > 0)
        expect(examined / rows.length).toBeLessThanOrEqual(100);
    });
  }
});

describe("p_until keeps the legacy direct-list semantics", () => {
  // The legacy filter is #226's PostgREST `last_activity_at=lte.<until>`, i.e.
  // `last_activity_at <= until::timestamptz`: inclusive, on last_activity_at.
  let u: PGlite;
  const T0 = "2026-09-10 12:00:00+00";
  const ROWS: Array<[string, string, string]> = [
    // [session, created_at, last_activity_at]
    ["at-until", "2026-09-10 11:00:00+00", "2026-09-10 12:00:00+00"],
    [
      "one-microsecond-after",
      "2026-09-10 11:00:00+00",
      "2026-09-10 12:00:00.000001+00",
    ],
    [
      "one-microsecond-before",
      "2026-09-10 11:00:00+00",
      "2026-09-10 11:59:59.999999+00",
    ],
    [
      "created-before-active-after",
      "2026-09-10 10:00:00+00",
      "2026-09-10 13:00:00+00",
    ],
    ["long-before", "2026-09-01 00:00:00+00", "2026-09-02 00:00:00+00"],
  ];
  beforeAll(async () => {
    u = await database();
    for (const [session, created, last] of ROWS) {
      await u.query(
        `INSERT INTO sessions.session_directory
           (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, search_text)
         VALUES ('org-u', 'ws-u', 'codex', $1, $2::timestamptz, $3::timestamptz, 'Session', 'Session')`,
        [session, created, last],
      );
    }
  }, 60_000);
  afterAll(async () => {
    await u?.close();
  });

  const legacy = async (until: string) =>
    (
      await u.query<{ session_id: string }>(
        `SELECT session_id FROM sessions.session_directory
          WHERE org_id = 'org-u' AND workspace_id = 'ws-u' AND deleted_at IS NULL
            AND last_activity_at <= $1::timestamptz`,
        [until],
      )
    ).rows
      .map((r) => r.session_id)
      .sort();
  const viaRpc = async (until: string) =>
    (
      await rpc(u, claimsFor("org-u", ["ws-u"]), {
        p_workspace_id: "ws-u",
        p_until: until,
      })
    )
      .map((r) => String(r.session_id))
      .sort();

  test("inclusive, on last_activity_at, at microsecond precision", async () => {
    const expected = ["at-until", "long-before", "one-microsecond-before"];
    expect(await legacy(T0)).toEqual(expected);
    expect(await viaRpc(T0)).toEqual(expected);
    // A row created before until but last active after it is excluded: the column
    // is last_activity_at, not created_at.
    expect(await viaRpc(T0)).not.toContain("created-before-active-after");
    // Moving until by one microsecond moves exactly the boundary row.
    for (const until of [
      "2026-09-10 12:00:00.000001+00",
      "2026-09-10 11:59:59.999999+00",
    ]) {
      expect(await viaRpc(until), until).toEqual(await legacy(until));
    }
    expect(await viaRpc("2026-09-10 12:00:00.000001+00")).toContain(
      "one-microsecond-after",
    );
    expect(await viaRpc("2026-09-10 11:59:59.999999+00")).not.toContain(
      "at-until",
    );
  });

  test("the same instant in any UTC offset, or none (session time zone), gives the same rows", async () => {
    // A no-offset input is read in the session time zone, as PostgREST's would be;
    // this engine's (and Supabase's) is UTC-equivalent.
    const offset = (
      await u.query<{ o: number }>(
        "SELECT EXTRACT(TIMEZONE FROM now())::int AS o",
      )
    ).rows[0]!.o;
    expect(offset).toBe(0);
    for (const until of [
      "2026-09-10T12:00:00Z",
      "2026-09-10T14:00:00+02:00",
      "2026-09-10T07:00:00-05:00",
      "2026-09-10T12:00:00",
      "2026-09-10T12:00:00.000Z",
    ]) {
      expect(await viaRpc(until), until).toEqual(await legacy(until));
      expect(await viaRpc(until), until).toEqual([
        "at-until",
        "long-before",
        "one-microsecond-before",
      ]);
    }
  });

  test("searchRpcRequest sends until as canonical UTC and since > until is still refused", async () => {
    const built = searchRpcRequest({
      workspaceId: "ws-u",
      until: "2026-09-10T14:00:00+02:00",
    });
    expect(built.ok && built.body.p_until).toBe("2026-09-10T12:00:00.000000Z");
    const error = (await rpc(u, claimsFor("org-u", ["ws-u"]), {
      p_workspace_id: "ws-u",
      p_since: "2026-09-10T13:00:00Z",
      p_until: "2026-09-10T12:00:00Z",
    }).then(
      () => null,
      (c: unknown) => c,
    )) as { code?: string } | null;
    expect(error?.code).toBe("22023");
  });
});
