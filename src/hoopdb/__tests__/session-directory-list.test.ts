/**
 * The session list RPC (20261101050000): every filter, their combinations, the keyset
 * cursor, the half-open timestamp bounds, tenant isolation, the role and claim pins it
 * shares with the search RPC, the catalog writer and projector, the query plans of the
 * main filter shapes, and up/down/up. In-process PGlite with every supabase migration.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_DIRECTORY_SEARCH_RPC } from "../search-index-contract.ts";
import {
  directoryCatalogFromProjection,
  writeDirectoryCatalog,
} from "../session-directory-catalog.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;
const UP = read("supabase/migrations/20261101050000_session_directory_list.sql");
const DOWN = read(
  "supabase/rollbacks/20261101050000_session_directory_list.down.sql",
);
const TOKEN_ISSUER = "https://issuer.test/session-directory";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(options: { catalogReady?: boolean } = {}): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
  );
  // The operator step that unlocks p_agent once the catalog backfill is done.
  if (options.catalogReady ?? true)
    await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
  return db;
}

const claims = (org: string, workspaces: unknown[], role = WORKER) => {
  const iat = Math.floor(Date.now() / 1000);
  return JSON.stringify({
    role,
    iss: TOKEN_ISSUER,
    aud: SESSION_DIRECTORY_SEARCH_RPC.audience,
    iat,
    exp: iat + 60,
    app_metadata: { org_id: org, workspace_ids: workspaces },
  });
};

type Row = Record<string, unknown>;
type ListArgs = {
  workspace?: string | null;
  sources?: string[];
  notSources?: string[];
  projectContains?: string;
  users?: string[];
  agent?: string;
  lastTsAfter?: string;
  lastTsBefore?: string;
  firstTsAfter?: string;
  firstTsBefore?: string;
  keys?: Array<[string, string]>;
  cursor?: [string, string, string];
  limit?: number;
};

async function list(
  db: PGlite,
  args: ListArgs,
  options: { org?: string; workspaces?: unknown[]; role?: string } = {},
): Promise<Row[]> {
  const workspace = args.workspace === undefined ? "ws-a" : args.workspace;
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    claims(
      options.org ?? "org-a",
      options.workspaces ?? [workspace ?? "ws-a"],
      options.role ?? WORKER,
    ),
  ]);
  await db.exec(`SET ROLE ${options.role ?? WORKER}`);
  try {
    return (
      await db.query<Row>(
        `SELECT * FROM sessions.list_session_directory(
           p_workspace_id => $1, p_sources => $2, p_project_contains => $3, p_user_ids => $4,
           p_agent => $5, p_last_ts_after => $6, p_last_ts_before => $7,
           p_first_ts_after => $8, p_first_ts_before => $9,
           p_session_harnesses => $14, p_session_ids => $15, p_not_sources => $16,
           p_cursor_last_activity_at => $10, p_cursor_session_id => $11, p_cursor_harness => $12,
           p_limit => $13)`,
        [
          workspace,
          args.sources ?? null,
          args.projectContains ?? null,
          args.users ?? null,
          args.agent ?? null,
          args.lastTsAfter ?? null,
          args.lastTsBefore ?? null,
          args.firstTsAfter ?? null,
          args.firstTsBefore ?? null,
          args.cursor?.[0] ?? null,
          args.cursor?.[1] ?? null,
          args.cursor?.[2] ?? null,
          args.limit ?? 50,
          args.keys ? args.keys.map(([h]) => h) : null,
          args.keys ? args.keys.map(([, id]) => id) : null,
          args.notSources ?? null,
        ],
      )
    ).rows;
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

const ids = (rows: Row[]) => rows.map((row) => row.session_id);
const code = async (p: Promise<unknown>) =>
  (
    (await p.then(
      () => null,
      (e: unknown) => e,
    )) as { code?: string } | null
  )?.code;

type Session = {
  org?: string;
  workspace?: string;
  harness?: string;
  id: string;
  created: string;
  last: string;
  project?: string | null;
  projectKey?: string | null;
  users?: string[];
  agents?: string[];
  model?: string | null;
  catalogModels?: string[];
};

/** Seed directory rows the way the projector and catalog writer leave them. */
async function seed(db: PGlite, sessions: Session[]) {
  for (const s of sessions) {
    const org = s.org ?? "org-a";
    const workspace = s.workspace ?? "ws-a";
    const harness = s.harness ?? "codex";
    const users = s.users ?? [];
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
          project_id, project_key, user_ids, agent_names, model, catalog_models, catalog_projected_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CASE WHEN $13 THEN now() END)`,
      [
        org,
        workspace,
        harness,
        s.id,
        s.created,
        s.last,
        s.project ?? null,
        s.projectKey ?? null,
        users,
        s.agents ?? [],
        s.model ?? null,
        s.catalogModels ?? [],
        (s.agents?.length ?? 0) > 0 || (s.catalogModels?.length ?? 0) > 0,
      ],
    );
    for (const user of users)
      await db.query(
        "INSERT INTO sessions.session_directory_users VALUES ($1, $2, $3, $4, $5, $6)",
        [org, workspace, harness, s.id, user, s.last],
      );
    for (const agent of s.agents ?? [])
      await db.query(
        `INSERT INTO sessions.session_directory_agents
           (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
         VALUES ($1, $2, $3, $4, $5, lower(btrim($5)), $6)`,
        [org, workspace, harness, s.id, agent, s.last],
      );
  }
}

const FIXTURE: Session[] = [
  {
    id: "s1",
    harness: "codex",
    created: "2026-10-01T00:00:00Z",
    last: "2026-10-05T12:00:00Z",
    project: "AgentWorkforce/cloud",
    users: ["u-1"],
    agents: ["Helm-Chief", "reviewer"],
    model: "gpt-5",
    catalogModels: ["gpt-5", "o4"],
  },
  {
    id: "s2",
    harness: "claude",
    created: "2026-10-02T00:00:00Z",
    last: "2026-10-04T00:00:00Z",
    project: "AgentWorkforce/relayhistory-cloud",
    users: ["u-2"],
    agents: ["worker-7"],
  },
  {
    id: "s3",
    harness: "opencode",
    created: "2026-10-03T00:00:00Z",
    last: "2026-10-03T00:00:00Z",
    project: null,
    projectKey: "github.com/acme/widgets",
    users: ["u-1", "u-3"],
  },
  {
    id: "s4",
    harness: "codex",
    created: "2026-09-20T00:00:00Z",
    last: "2026-10-02T00:00:00Z",
    project: "personal/scratch",
    users: ["u-3"],
    agents: ["helm-chief"],
  },
  // Another workspace of the same org, and another org: never in ws-a's lists.
  {
    id: "s1",
    workspace: "ws-b",
    harness: "codex",
    created: "2026-10-01T00:00:00Z",
    last: "2026-10-06T00:00:00Z",
    project: "AgentWorkforce/cloud",
    users: ["u-9"],
    agents: ["helm-chief"],
  },
  {
    id: "x1",
    org: "org-b",
    workspace: "ws-a",
    harness: "codex",
    created: "2026-10-01T00:00:00Z",
    last: "2026-10-07T00:00:00Z",
    project: "AgentWorkforce/cloud",
    users: ["u-1"],
    agents: ["helm-chief"],
  },
];

describe("list_session_directory filters", () => {
  test("lists one workspace newest first with every listing column", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    const rows = await list(db, {});
    expect(ids(rows)).toEqual(["s1", "s2", "s3", "s4"]);
    expect(rows.every((row) => row.org_id === "org-a" && row.workspace_id === "ws-a")).toBe(true);
    // Models are the catalog models united with the pointer model, sorted and distinct.
    expect(rows[0]!.models).toEqual(["gpt-5", "o4"]);
    expect(rows[0]!.agent_names).toEqual(["Helm-Chief", "reviewer"]);
    expect(rows[2]!.models).toEqual([]);
    expect(rows[0]!.catalog_projected_at).not.toBeNull();
    expect(rows[2]!.catalog_projected_at).toBeNull();
  });

  test("source is exact and repeatable (OR)", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect(ids(await list(db, { sources: ["codex"] }))).toEqual(["s1", "s4"]);
    expect(ids(await list(db, { sources: ["claude", "opencode"] }))).toEqual(["s2", "s3"]);
    expect(ids(await list(db, { sources: ["cod"] }))).toEqual([]);
    // notSource excludes (repeatable), and combines with source.
    expect(ids(await list(db, { notSources: ["codex"] }))).toEqual(["s2", "s3"]);
    expect(ids(await list(db, { notSources: ["codex", "opencode"] }))).toEqual(["s2"]);
    expect(ids(await list(db, { sources: ["codex", "claude"], notSources: ["claude"] }))).toEqual(["s1", "s4"]);
  });

  test("projectContains is a case-insensitive substring of the project id, key or repository, LIKE-literal", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    await db.query("UPDATE sessions.session_directory SET git_repo = 'github.com/acme/gadgets' WHERE session_id = 's2' AND workspace_id = 'ws-a'");
    expect(ids(await list(db, { projectContains: "GADGETS" }))).toEqual(["s2"]);
    expect(ids(await list(db, { projectContains: "agentworkforce/" }))).toEqual(["s1", "s2"]);
    expect(ids(await list(db, { projectContains: "ACME/WID" }))).toEqual(["s3"]);
    // One or two characters are accepted (Neon's ILIKE accepts them too).
    expect(ids(await list(db, { projectContains: "sc" }))).toEqual(["s4"]);
    // % and _ are literal, not wildcards.
    expect(ids(await list(db, { projectContains: "%" }))).toEqual([]);
    expect(ids(await list(db, { projectContains: "cloud_" }))).toEqual([]);
  });

  test("user is exact and repeatable (OR), and never widens the workspace", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect(ids(await list(db, { users: ["u-1"] }))).toEqual(["s1", "s3"]);
    expect(ids(await list(db, { users: ["u-2", "u-3"] }))).toEqual(["s2", "s3", "s4"]);
    // u-9 has sessions only in ws-b of the same org: nothing in ws-a.
    expect(ids(await list(db, { users: ["u-9"] }))).toEqual([]);
    expect(ids(await list(db, { users: ["u-9", "u-404"] }))).toEqual([]);
  });

  test("agent is a case-insensitive exact name over every agent of the session", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect(ids(await list(db, { agent: "HELM-CHIEF" }))).toEqual(["s1", "s4"]);
    expect(ids(await list(db, { agent: "  reviewer " }))).toEqual(["s1"]);
    expect(ids(await list(db, { agent: "helm" }))).toEqual([]);
  });

  test("agent is refused (0A000 agent_filter_not_ready) until the catalog backfill is marked complete", async () => {
    const db = await database({ catalogReady: false });
    await seed(db, FIXTURE);
    expect(await code(list(db, { agent: "helm-chief" }))).toBe("0A000");
    // Every other filter is served meanwhile.
    expect(ids(await list(db, { users: ["u-1"] }))).toEqual(["s1", "s3"]);
    await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
    expect(ids(await list(db, { agent: "helm-chief" }))).toEqual(["s1", "s4"]);
  });

  test("timestamp bounds are half-open: after inclusive, before exclusive", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect(ids(await list(db, { lastTsAfter: "2026-10-04T00:00:00Z" }))).toEqual(["s1", "s2"]);
    expect(ids(await list(db, { lastTsBefore: "2026-10-04T00:00:00Z" }))).toEqual(["s3", "s4"]);
    expect(
      ids(await list(db, { lastTsAfter: "2026-10-03T00:00:00Z", lastTsBefore: "2026-10-04T00:00:00Z" })),
    ).toEqual(["s3"]);
    expect(ids(await list(db, { firstTsAfter: "2026-10-02T00:00:00Z" }))).toEqual(["s2", "s3"]);
    expect(ids(await list(db, { firstTsBefore: "2026-10-01T00:00:00Z" }))).toEqual(["s4"]);
    // An empty interval is valid and empty; an inverted one is refused.
    expect(
      ids(await list(db, { firstTsAfter: "2026-10-02T00:00:00Z", firstTsBefore: "2026-10-02T00:00:00Z" })),
    ).toEqual([]);
    expect(
      await code(list(db, { lastTsAfter: "2026-10-05T00:00:00Z", lastTsBefore: "2026-10-04T00:00:00Z" })),
    ).toBe("22023");
  });

  test("filters combine (AND) across every driving shape", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect(ids(await list(db, { agent: "helm-chief", users: ["u-3"] }))).toEqual(["s4"]);
    expect(ids(await list(db, { agent: "helm-chief", sources: ["codex"], lastTsAfter: "2026-10-03T00:00:00Z" }))).toEqual(["s1"]);
    expect(ids(await list(db, { users: ["u-1"], projectContains: "widgets" }))).toEqual(["s3"]);
    expect(ids(await list(db, { users: ["u-1", "u-3"], sources: ["codex"] }))).toEqual(["s1", "s4"]);
    expect(
      ids(await list(db, { sources: ["codex", "claude"], projectContains: "agentworkforce", firstTsBefore: "2026-10-02T00:00:00Z" })),
    ).toEqual(["s1"]);
  });

  test("a batch of (harness, session id) keys hydrates exactly those sessions, ANDed with filters", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    expect(ids(await list(db, { keys: [["codex", "s4"], ["claude", "s2"], ["claude", "s1"], ["codex", "x1"]] }))).toEqual(["s2", "s4"]);
    expect(ids(await list(db, { keys: [["codex", "s4"], ["claude", "s2"]], agent: "helm-chief" }))).toEqual(["s4"]);
    // Another workspace's session with the same key is never hydrated.
    expect(ids(await list(db, { keys: [["codex", "s1"]], users: ["u-9"] }))).toEqual([]);
    expect(ids(await list(db, { limit: 200 }))).toEqual(["s1", "s2", "s3", "s4"]);
  });

  test("the keyset cursor pages every shape without repeats or gaps", async () => {
    const db = await database();
    const many: Session[] = Array.from({ length: 25 }, (_, n) => ({
      id: `p-${String(n).padStart(2, "0")}`,
      harness: n % 2 ? "codex" : "claude",
      created: "2026-10-01T00:00:00Z",
      // Ties on last_activity_at are broken by session_id, then harness.
      last: `2026-10-${String(10 + (n % 5)).padStart(2, "0")}T00:00:00Z`,
      users: ["u-1"],
      agents: ["a"],
    }));
    await seed(db, many);
    for (const shape of [{}, { users: ["u-1"] }, { agent: "a" }, { users: ["u-1", "u-2"] }] as ListArgs[]) {
      const seen: unknown[] = [];
      let cursor: [string, string, string] | undefined;
      for (;;) {
        const page = await list(db, { ...shape, limit: 4, ...(cursor ? { cursor } : {}) });
        const visible = page.slice(0, 4);
        seen.push(...ids(visible));
        if (page.length <= 4) break;
        const last = visible[3]!;
        cursor = [
          (last.last_activity_at as Date).toISOString(),
          last.session_id as string,
          last.harness as string,
        ];
      }
      expect(seen).toHaveLength(25);
      expect(new Set(seen).size).toBe(25);
    }
  });
});

describe("pagination with a drifted companion order key (H1)", () => {
  test("agent and user pages follow the directory row's key: no repeat, no skip, no loop", async () => {
    const db = await database();
    const sessions: Session[] = Array.from({ length: 5 }, (_, n) => ({
      id: `s${n + 1}`,
      created: "2026-10-01T00:00:00Z",
      last: `2026-10-0${5 - n}T00:00:00Z`,
      users: ["u-1", "u-2"],
      agents: ["bot"],
    }));
    await seed(db, sessions);
    // s4 moves to the newest activity; its companions lag behind (a writer that crashed
    // before its companion update, or one that read an older row).
    await db.exec(`
      UPDATE sessions.session_directory SET last_activity_at = '2026-10-06T00:00:00Z' WHERE session_id = 's4';
      UPDATE sessions.session_directory_agents SET last_activity_at = '2026-10-02T00:00:00Z' WHERE session_id = 's4';
      UPDATE sessions.session_directory_users SET last_activity_at = '2026-10-02T00:00:00Z' WHERE session_id = 's4';
    `);
    const expected = ids(await list(db, {}));
    expect(expected).toEqual(["s4", "s1", "s2", "s3", "s5"]);
    for (const shape of [{ agent: "bot" }, { users: ["u-1"] }, { users: ["u-1", "u-2"] }] as ListArgs[]) {
      const seen: unknown[] = [];
      let cursor: [string, string, string] | undefined;
      for (let page = 0; page < 10; page++) {
        const rows = await list(db, { ...shape, limit: 2, ...(cursor ? { cursor } : {}) });
        const visible = rows.slice(0, 2);
        seen.push(...ids(visible));
        if (rows.length <= 2) break;
        const last = visible[1]!;
        cursor = [(last.last_activity_at as Date).toISOString(), last.session_id as string, last.harness as string];
      }
      expect(seen, JSON.stringify(shape)).toEqual(expected);
    }
  });

  test("moving a directory row's last_activity_at moves every companion in the same statement", async () => {
    const db = await database();
    await seed(db, [{ id: "s1", created: "2026-10-01T00:00:00Z", last: "2026-10-02T00:00:00Z", users: ["u-1"], agents: ["bot"] }]);
    await db.exec(`INSERT INTO sessions.session_directory_machines VALUES ('org-a', 'ws-a', 'codex', 's1', 'm-1', '2026-10-02T00:00:00Z')`);
    await db.exec("UPDATE sessions.session_directory SET last_activity_at = '2026-10-09T00:00:00Z' WHERE session_id = 's1'");
    const keys = await db.query<{ t: Date }>(`
      SELECT last_activity_at AS t FROM sessions.session_directory_users
      UNION ALL SELECT last_activity_at FROM sessions.session_directory_machines
      UNION ALL SELECT last_activity_at FROM sessions.session_directory_agents`);
    expect(keys.rows.map((row) => row.t.toISOString())).toEqual(Array(3).fill("2026-10-09T00:00:00.000Z"));
  });
});

describe("list_session_directory tenancy and refusals", () => {
  test("the org and workspace come only from the claims", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    // A claim for ws-a cannot read ws-b.
    expect(await code(list(db, { workspace: "ws-b" }, { workspaces: ["ws-a"] }))).toBe("42501");
    // ws-b's own claim reads only ws-b.
    expect(ids(await list(db, { workspace: "ws-b" }))).toEqual(["s1"]);
    // org-b's ws-a is a different tenant from org-a's ws-a.
    expect(ids(await list(db, {}, { org: "org-b" }))).toEqual(["x1"]);
    expect(ids(await list(db, { users: ["u-1"] }, { org: "org-b" }))).toEqual(["x1"]);
  });

  test("only the Worker role with valid claims may call it", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    for (const role of ["authenticated", "anon", "service_role"])
      expect(await code(list(db, {}, { role }))).toBe("42501");
    // Claims for another role are refused even under the Worker role.
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [claims("org-a", ["ws-a"], "authenticated")]);
    await db.exec(`SET ROLE ${WORKER}`);
    try {
      expect(await code(db.query("SELECT * FROM sessions.list_session_directory('ws-a')"))).toBe("42501");
      // The statement builder and the project text helper are owner-only.
      expect(
        await code(db.query("SELECT sessions.session_directory_list_sql(0, false, 0, false, false, false, false, false, false, false, false)")),
      ).toBe("42501");
    } finally {
      await db.exec("RESET ROLE");
    }
  });

  test("invalid values are refused with 22023", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    const refused: ListArgs[] = [
      { workspace: null },
      { limit: 0 },
      { limit: 201 },
      { keys: [] },
      { keys: [["Codex", "s1"]] },
      { keys: Array.from({ length: 201 }, (_, n) => ["codex", `s${n}`] as [string, string]) },
      { sources: ["Codex"] },
      { sources: Array.from({ length: 17 }, (_, n) => `h${n}`) },
      { notSources: ["Codex"] },
      { users: [""] },
      { users: Array.from({ length: 21 }, (_, n) => `u${n}`) },
      { agent: "   " },
      { projectContains: " " },
      { projectContains: "x".repeat(201) },
    ];
    for (const args of refused)
      expect(await code(list(db, args, { workspaces: ["ws-a"] })), JSON.stringify(args)).toBe(
        args.workspace === null ? "22023" : "22023",
      );
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [claims("org-a", ["ws-a"])]);
    await db.exec(`SET ROLE ${WORKER}`);
    try {
      expect(
        await code(db.query("SELECT * FROM sessions.list_session_directory('ws-a', p_cursor_session_id => 's1')")),
      ).toBe("22023");
    } finally {
      await db.exec("RESET ROLE");
    }
  });
});

describe("session_directory_activity", () => {
  async function activity(db: PGlite, ws: string, since: string, until: string, workspaces = [ws]) {
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [claims("org-a", workspaces)]);
    await db.exec(`SET ROLE ${WORKER}`);
    try {
      return (
        await db.query<{ day: Date | string; harness: string; sessions: number | string }>(
          "SELECT * FROM sessions.session_directory_activity($1, $2, $3)",
          [ws, since, until],
        )
      ).rows.map((row) => [
        typeof row.day === "string" ? row.day : row.day.toISOString().slice(0, 10),
        row.harness,
        Number(row.sessions),
      ]);
    } finally {
      await db.exec("RESET ROLE");
    }
  }

  test("a row with created_at after last_activity_at spans the two in order and never hides other days", async () => {
    const db = await database();
    await seed(db, [
      { id: "bad", created: "2026-10-05T00:00:00Z", last: "2026-10-02T00:00:00Z" },
      { id: "ok", harness: "claude", created: "2026-10-01T00:00:00Z", last: "2026-10-06T00:00:00Z" },
    ]);
    const days = await activity(db, "ws-a", "2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z");
    const totals = new Map<string, number>();
    for (const [day, , n] of days) totals.set(day as string, (totals.get(day as string) ?? 0) + (n as number));
    expect([...totals]).toEqual([
      ["2026-10-01", 1], ["2026-10-02", 2], ["2026-10-03", 2],
      ["2026-10-04", 2], ["2026-10-05", 2], ["2026-10-06", 1],
    ]);
  });

  test("a deleted session counts on no day", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    await db.exec("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 's2' AND workspace_id = 'ws-a'");
    const days = await activity(db, "ws-a", "2026-10-02T00:00:00Z", "2026-10-04T23:59:59Z");
    expect(days.some(([, harness]) => harness === "claude")).toBe(false);
    expect(ids(await list(db, {}))).toEqual(["s1", "s3", "s4"]);
  });

  test("counts each session on every UTC day of its span, clipped to the window, per harness", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    // s1 codex 10-01..10-05, s2 claude 10-02..10-04, s3 opencode 10-03, s4 codex 09-20..10-02.
    expect(await activity(db, "ws-a", "2026-10-02T00:00:00Z", "2026-10-04T23:59:59Z")).toEqual([
      ["2026-10-02", "claude", 1],
      ["2026-10-02", "codex", 2],
      ["2026-10-03", "claude", 1],
      ["2026-10-03", "codex", 1],
      ["2026-10-03", "opencode", 1],
      ["2026-10-04", "claude", 1],
      ["2026-10-04", "codex", 1],
    ]);
    // Another workspace's sessions never count; its claim is refused for ws-a.
    expect(await activity(db, "ws-b", "2026-10-06T00:00:00Z", "2026-10-06T23:59:59Z")).toEqual([
      ["2026-10-06", "codex", 1],
    ]);
    expect(await code(activity(db, "ws-a", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", ["ws-b"]))).toBe("42501");
    expect(await code(activity(db, "ws-a", "2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z"))).toBe("22023");
    expect(await code(activity(db, "ws-a", "2025-01-01T00:00:00Z", "2026-01-02T00:00:00Z"))).toBe("22023");
  });
});

describe("catalog writer and projector", () => {
  const key = { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s-1" };

  async function pointer(db: PGlite, machine: string, deleted = false, last = "2026-10-05T00:00:00Z") {
    await db.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision,
          project_id, model, deleted_at)
       VALUES ('org-a', 'ws-a', 'codex', 's-1', $1, 'u-' || $1, '2026-10-01T00:00:00Z', $3, 's3', 'bucket',
               'v1/' || $1, decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2,
               'AgentWorkforce/cloud', 'gpt-5', CASE WHEN $2 THEN now() END)
       ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id)
       DO UPDATE SET deleted_at = excluded.deleted_at, last_activity_at = excluded.last_activity_at,
                     revision = sessions.session_index.revision + 1`,
      [machine, deleted, last],
    );
  }

  test("derives agents, models, project key and edges from projection rows; drops local paths", () => {
    const catalog = directoryCatalogFromProjection([
      { table: "session_catalog", row: { models: ["o4", "gpt-5", "", 3], project_key: "github.com/acme/widgets", project_key_method: "git_remote", repo_url: "git@github.com:acme/widgets.git", git_branch: "main", originator: "codex_cli", agent_version: "1.2.3" } },
      { table: "session_catalog_agents", row: { agent_name: "Helm-Chief" } },
      { table: "session_catalog_agents", row: { agent_name: "helm-chief " } },
      { table: "session_catalog_agents", row: { agent_name: "reviewer" } },
      { table: "session_relationships", row: { relationship_uid: "r2", child_session_id: null } },
      { table: "session_relationships", row: { relationship_uid: "r1", child_session_id: "child-1" } },
    ]);
    expect(catalog.agentNames).toEqual(["helm-chief", "reviewer"]);
    expect(catalog.models).toEqual(["gpt-5", "o4"]);
    expect(catalog.projectKey).toBe("github.com/acme/widgets");
    expect(catalog.projectId).toBe("acme/widgets");
    expect(catalog.gitRepo).toBe("git@github.com:acme/widgets.git");
    expect(catalog.gitBranch).toBe("main");
    expect(catalog.relationships).toEqual([
      { relationshipUid: "r1", childSessionId: "child-1" },
      { relationshipUid: "r2", childSessionId: null },
    ]);
    for (const [key, method] of [["/Users/alice/src/app", "git_remote"], ["C:\\work\\app", null], ["app", "path"]] as const)
      expect(
        directoryCatalogFromProjection([{ table: "session_catalog", row: { project_key: key, project_key_method: method } }]).projectKey,
      ).toBeNull();
  });

  test("the writer feeds the list's agent filter, models, project key and parent/child counts", async () => {
    const db = await database();
    await pointer(db, "m1");
    await projectSessionDirectory(db as never, key);
    await pointer(db, "m1");
    await db.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, raw_format, trace_id, revision)
       VALUES ('org-a', 'ws-a', 'codex', 'child-1', 'm1', 'u-m1', '2026-10-02T00:00:00Z', '2026-10-02T00:00:00Z',
               's3', 'bucket', 'v1/c', 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19', 1)`,
    );
    await projectSessionDirectory(db as never, { ...key, sessionId: "child-1" });
    expect(
      await writeDirectoryCatalog(db as never, key, {
        agentNames: ["Helm-Chief"],
        models: ["o4"],
        projectKey: "github.com/acme/widgets",
        projectKeyMethod: "git_remote",
        projectId: "acme/widgets",
        gitRepo: "git@github.com:acme/widgets.git",
        gitBranch: "main",
        initialCommit: null,
        originator: null,
        agentVersion: null,
        relationships: [
          { relationshipUid: "r1", childSessionId: "child-1" },
          { relationshipUid: "r2", childSessionId: null },
        ],
      }),
    ).toEqual({ written: true });
    const rows = await list(db, { agent: "helm-chief" });
    expect(ids(rows)).toEqual(["s-1"]);
    expect(rows[0]!.models).toEqual(["gpt-5", "o4"]);
    expect(rows[0]!.project_id).toBe("acme/widgets");
    expect(rows[0]!.git_repo).toBe("git@github.com:acme/widgets.git");
    expect(rows[0]!.git_branch).toBe("main");
    expect(rows[0]!.child_session_count).toBe(2);
    expect(ids(await list(db, { projectContains: "acme" }))).toEqual(["s-1"]);
    const child = (await list(db, {})).find((row) => row.session_id === "child-1")!;
    expect(child.parent_session_ids).toEqual(["s-1"]);

    // A new pointer activity moves the agent row's order key with the logical row.
    await pointer(db, "m1", false, "2026-10-09T00:00:00Z");
    await projectSessionDirectory(db as never, key);
    const agent = await db.query<{ last_activity_at: Date }>(
      "SELECT last_activity_at FROM sessions.session_directory_agents WHERE session_id = 's-1'",
    );
    expect(agent.rows[0]!.last_activity_at.toISOString()).toBe("2026-10-09T00:00:00.000Z");

    // Tombstone: the row, its catalog, agents and edges leave every list.
    await pointer(db, "m1", true);
    await projectSessionDirectory(db as never, key);
    expect(ids(await list(db, { agent: "helm-chief" }))).toEqual([]);
    const after = await list(db, {});
    expect(ids(after)).toEqual(["child-1"]);
    expect(after[0]!.parent_session_ids).toEqual([]);
    const tomb = await db.query<Row>(
      "SELECT agent_names, catalog_models, project_key, catalog_projected_at FROM sessions.session_directory WHERE session_id = 's-1'",
    );
    expect(tomb.rows[0]).toEqual({ agent_names: [], catalog_models: [], project_key: null, catalog_projected_at: null });
    expect(
      (await db.query("SELECT 1 FROM sessions.session_directory_agents WHERE session_id = 's-1'")).rows,
    ).toHaveLength(0);
    // A catalog write to a tombstone is refused.
    expect(
      (await writeDirectoryCatalog(db as never, key, {
        agentNames: ["x"], models: [], projectKey: null, projectKeyMethod: null,
        projectId: null, gitRepo: null, gitBranch: null,
        initialCommit: null, originator: null, agentVersion: null, relationships: [],
      })).written,
    ).toBe(false);
  });

  const EMPTY = {
    agentNames: [] as string[], models: [] as string[], projectKey: null, projectKeyMethod: null,
    projectId: null, gitRepo: null, gitBranch: null,
    initialCommit: null, originator: null, agentVersion: null,
    relationships: [] as Array<{ relationshipUid: string; childSessionId: string | null }>,
  };
  const dirKey = (sessionId: string) => ({ orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId });

  test("an erased child leaves no edge, id or count on a live parent, by tombstone or by delete (M2)", async () => {
    const db = await database();
    await seed(db, [
      { id: "parent", created: "2026-10-01T00:00:00Z", last: "2026-10-05T00:00:00Z" },
      { id: "kid-a", created: "2026-10-02T00:00:00Z", last: "2026-10-03T00:00:00Z" },
      { id: "kid-b", created: "2026-10-02T00:00:00Z", last: "2026-10-03T00:00:00Z" },
    ]);
    await writeDirectoryCatalog(db as never, dirKey("parent"), {
      ...EMPTY,
      relationships: [
        { relationshipUid: "ra", childSessionId: "kid-a" },
        { relationshipUid: "rb", childSessionId: "kid-b" },
      ],
    });
    const parent = async () => (await list(db, {})).find((row) => row.session_id === "parent")!;
    expect((await parent()).child_session_count).toBe(2);
    // Tombstone kid-a (any writer), hard-delete kid-b (erasure).
    await db.exec("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 'kid-a'");
    await db.exec("DELETE FROM sessions.session_directory WHERE session_id = 'kid-b'");
    expect((await parent()).child_session_count).toBe(0);
    const edges = await db.query("SELECT child_session_id FROM sessions.session_directory_relationships");
    expect(edges.rows).toEqual([]);
    // A re-delivered parent cannot bring the tombstoned child's id back.
    await writeDirectoryCatalog(db as never, dirKey("parent"), {
      ...EMPTY,
      relationships: [{ relationshipUid: "ra", childSessionId: "kid-a" }],
    });
    expect((await parent()).child_session_count).toBe(0);
  });

  test("the catalog writer is one statement and never outlives a tombstone, in either order (M3)", async () => {
    const db = await database();
    await seed(db, [
      { id: "p", created: "2026-10-01T00:00:00Z", last: "2026-10-05T00:00:00Z" },
      { id: "c", created: "2026-10-02T00:00:00Z", last: "2026-10-03T00:00:00Z" },
    ]);
    // One statement: atomic on any executor, with the live row locked FOR UPDATE first.
    let statements = 0;
    const counting = {
      query: (text: string, params?: unknown[]) => {
        statements++;
        expect(text).toMatch(/FOR UPDATE/);
        return db.query(text, params);
      },
    };
    const write = { ...EMPTY, agentNames: ["bot"], relationships: [{ relationshipUid: "r", childSessionId: "c" }] };
    expect((await writeDirectoryCatalog(counting as never, dirKey("p"), write)).written).toBe(true);
    expect(statements).toBe(1);
    // Tombstone after the write: its triggers remove the agents and edges.
    await db.exec("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 'p'");
    const left = async () =>
      (
        await db.query(`SELECT 1 FROM sessions.session_directory_agents WHERE session_id = 'p'
                        UNION ALL SELECT 1 FROM sessions.session_directory_relationships WHERE parent_session_id = 'p'`)
      ).rows.length;
    expect(await left()).toBe(0);
    expect((await list(db, {})).find((row) => row.session_id === "c")!.parent_session_ids).toEqual([]);
    // Tombstone before the write: nothing is written.
    expect((await writeDirectoryCatalog(db as never, dirKey("p"), write)).written).toBe(false);
    expect(await left()).toBe(0);
  });
});

type Plan = { [key: string]: unknown; Plans?: Plan[]; "Index Name"?: string };
const walk = (plan: Plan): Plan[] => [plan, ...(plan.Plans ?? []).flatMap(walk)];

describe("list_session_directory query plans", () => {
  test("each main filter shape uses its index", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         project_id, project_key, user_ids, agent_names)
      SELECT 'o', CASE WHEN n % 2 = 0 THEN 'w' ELSE 'other-w' END,
             CASE WHEN n % 200 = 0 THEN 'gemini' ELSE (ARRAY['codex','claude','opencode'])[1 + n % 3] END, 's-' || n,
             '2026-01-01T00:00:00Z'::timestamptz + n * interval '1 minute',
             '2026-01-01T00:00:00Z'::timestamptz + n * interval '1 minute',
             CASE WHEN n % 500 = 0 THEN 'needle-project-' || n ELSE 'project-' || (n % 50) END,
             NULL,
             CASE WHEN n % 1000 = 0 THEN ARRAY['rare-' || n] ELSE ARRAY['u-' || (n % 200), 'u-' || ((n + 7) % 200)] END,
             ARRAY['agent-' || (n % 300)]
        FROM generate_series(1, 20000) AS n;
      -- Other tenants' rows: the directory is one multi-tenant table, so a seq scan's cost
      -- grows with every tenant while a companion probe's does not.
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at)
      SELECT 'other-org-' || (n % 50), 'w', 'codex', 'o-' || n, now(), now()
        FROM generate_series(1, 60000) AS n;
      INSERT INTO sessions.session_directory_users
      SELECT d.org_id, d.workspace_id, d.harness, d.session_id, u, d.last_activity_at
        FROM sessions.session_directory AS d, unnest(d.user_ids) AS u;
      INSERT INTO sessions.session_directory_agents
      SELECT d.org_id, d.workspace_id, d.harness, d.session_id, a, lower(a), d.last_activity_at
        FROM sessions.session_directory AS d, unnest(d.agent_names) AS a;
      ANALYZE sessions.session_directory;
      ANALYZE sessions.session_directory_users;
      ANALYZE sessions.session_directory_agents;
    `);
    const statement = async (flags: unknown[]) =>
      (
        await db.query<{ sql: string }>(
          "SELECT sessions.session_directory_list_sql($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) AS sql",
          flags,
        )
      ).rows[0]!.sql;
    const literal = (value: unknown): string =>
      value === null
        ? "NULL"
        : Array.isArray(value)
          ? `ARRAY[${value.map(literal).join(", ")}]::text[]`
          : typeof value === "number"
            ? String(value)
            : `'${String(value).replaceAll("'", "''")}'`;
    const params = (overrides: Record<number, unknown>) => {
      const values: unknown[] = ["o", "w", null, null, null, null, null, null, null, null, null, null, null, 51, null, null, null];
      for (const [index, value] of Object.entries(overrides)) values[Number(index) - 1] = value;
      return values.map(literal).join(", ");
    };
    const off = [0, false, 0, false, false, false, false, false, false, false, false];
    const flags = (overrides: Record<number, unknown>) => {
      const values = [...off];
      for (const [index, value] of Object.entries(overrides)) values[Number(index)] = value;
      return values;
    };
    // The planner picks the recency walk when a filter value is common (a page is found
    // in a few hundred index rows), so each shape is checked with a selective value: that
    // is the case its index exists for.
    // Builder flags: 0 source count, 1 project, 2 user count, 3 agent, 4 last after, 5 last
    // before, 6 first after, 7 first before, 8 keys, 9 cursor, 10 excluded sources.
    const cases: Array<[string, unknown[], string, string | string[]]> = [
      ["workspace", flags({}), params({}), "session_directory_workspace_recent_idx"],
      ["one source", flags({ 0: 1 }), params({ 3: ["gemini"] }), "session_directory_harness_recent_idx"],
      ["project substring", flags({ 1: true }), params({ 4: "%needle-project%" }), "session_directory_tenant_project_text_trgm_idx"],
      ["one user", flags({ 2: 1 }), params({ 5: ["rare-2000"] }), "session_directory_users_recent_idx"],
      ["several users", flags({ 2: 2 }), params({ 5: ["rare-2000", "rare-4000"] }), "session_directory_users_recent_idx"],
      ["agent", flags({ 3: true }), params({ 6: "agent-42" }), "session_directory_agents_recent_idx"],
      ["agent + cursor", flags({ 3: true, 9: true }), params({ 6: "agent-42", 11: "2026-01-10T00:00:00Z", 12: "s-1", 13: "codex" }), "session_directory_agents_recent_idx"],
      ["batch keys", flags({ 8: true }), params({ 15: ["codex", "claude"], 16: ["s-8", "s-9"] }), ["session_directory_pkey", "session_directory_session_lookup_idx"]],
    ];
    const types =
      "text, text, text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, timestamptz, text, text, integer, text[], text[], text[]";
    for (const [name, builderFlags, values, expected] of cases) {
      const sql = await statement(builderFlags);
      await db.exec(`PREPARE list_shape (${types}) AS ${sql}`);
      try {
        const result = await db.query<Record<string, unknown>>(
          `EXPLAIN (FORMAT JSON) EXECUTE list_shape(${values})`,
        );
        const plan = (result.rows[0]!["QUERY PLAN"] as Array<{ Plan: Plan }>)[0]!.Plan;
        const nodes = walk(plan);
        const used = nodes.map((node) => node["Index Name"]).filter(Boolean);
        expect(
          (Array.isArray(expected) ? expected : [expected]).some((index) => used.includes(index)),
          `${name}: ${used.join(", ")}`,
        ).toBe(true);
        // Never a sequential scan of the directory for a page.
        expect(
          nodes.some((node) => node["Node Type"] === "Seq Scan" && node["Relation Name"] === "session_directory"),
          name,
        ).toBe(false);
      } finally {
        await db.exec("DEALLOCATE list_shape");
      }
    }
  });
});

describe("migration up/down/up", () => {
  test("the down path removes exactly what the up added and the up reapplies", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    // Newer migrations depending on catalog columns must roll back first.
    await db.exec(read("supabase/rollbacks/20261105023000_session_analysis_publications.down.sql"));
    await db.exec(read("supabase/rollbacks/20261105022000_session_desktop_catalog.down.sql"));
    await db.exec(read("supabase/rollbacks/20261105020000_session_replay_pages.down.sql"));
    await db.exec(read("supabase/rollbacks/20261104021000_session_directory_event_metadata.down.sql"));
    await db.exec(DOWN);
    expect(
      (await db.query("SELECT to_regprocedure('sessions.list_session_directory(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)') AS f")).rows,
    ).toEqual([{ f: null }]);
    expect((await db.query("SELECT to_regclass('sessions.session_directory_agents') AS t")).rows).toEqual([{ t: null }]);
    // The directory rows themselves survive the down.
    expect((await db.query("SELECT count(*)::int AS n FROM sessions.session_directory")).rows).toEqual([{ n: 6 }]);
    await db.exec(UP);
    await db.exec(read("supabase/migrations/20261104021000_session_directory_event_metadata.sql"));
    await db.exec(read("supabase/migrations/20261105020000_session_replay_pages.sql"));
    await db.exec(read("supabase/migrations/20261105022000_session_desktop_catalog.sql"));
    await db.exec(read("supabase/migrations/20261105023000_session_analysis_publications.sql"));
    expect(ids(await list(db, {}))).toEqual(["s1", "s2", "s3", "s4"]);
  });
});
