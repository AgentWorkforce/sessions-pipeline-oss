import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  AGENT_PREFIX_BENCHMARK_GATE,
  SEARCH_INDEX_CATALOG_SQL,
  SESSION_DIRECTORY_LIST_COLUMNS,
  SESSION_DIRECTORY_QUERIES,
  AGENT_PREFIX_CANDIDATE_BUDGET,
  AgentPrefixRequiresRpcError,
  directAgentPrefix,
  auditSearchIndexes,
  type SearchIndexCatalogRow,
} from "../search-index-contract.ts";

const migration = readFileSync(
  join(
    import.meta.dir,
    "../../../supabase/migrations/20261030010000_session_directory.sql",
  ),
  "utf8",
);
const shortSummaryMigration = readFileSync(
  join(
    import.meta.dir,
    "../../../supabase/migrations/20261030020000_session_directory_short_summary.sql",
  ),
  "utf8",
);
const shim = readFileSync(
  join(import.meta.dir, "../../../supabase/tests/supabase_shim.sql"),
  "utf8",
);
const bootstrap = readFileSync(
  join(
    import.meta.dir,
    "../../../supabase/bootstrap/session_directory_search_worker.sql",
  ),
  "utf8",
);
const tokenBootstrap = readFileSync(
  join(
    import.meta.dir,
    "../../../supabase/bootstrap/session_token_verifier.sql",
  ),
  "utf8",
);
const open: PGlite[] = [];

afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(shim);
  await db.exec(bootstrap);
  await db.exec(tokenBootstrap);
  await db.exec(readFileSync(join(import.meta.dir, "../../../supabase/bootstrap/session_ingest_roles.sql"), "utf8"));
  await db.exec(`
    CREATE SCHEMA sessions;
    CREATE FUNCTION sessions.jwt_org_id() RETURNS text LANGUAGE sql STABLE AS $$
      SELECT nullif(current_setting('request.jwt.claims', true), '')::jsonb
        #>> '{app_metadata,org_id}'
    $$;
    CREATE FUNCTION sessions.jwt_workspace_ids() RETURNS text[] LANGUAGE sql STABLE AS $$
      SELECT coalesce(array_agg(value #>> '{}'), '{}'::text[])
        FROM jsonb_array_elements(coalesce(
          nullif(current_setting('request.jwt.claims', true), '')::jsonb
            #> '{app_metadata,workspace_ids}',
          '[]'::jsonb
        ))
    $$;
    CREATE FUNCTION sessions.member_of(text, text[]) RETURNS boolean
      LANGUAGE sql IMMUTABLE AS $$ SELECT $1 = ANY ($2) $$;
    -- 20261025000000_sessions_index.sql establishes this prerequisite before
    -- either directory migration runs in production.
    GRANT USAGE ON SCHEMA sessions TO authenticated;
  `);
  await db.exec(migration);
  await db.exec(shortSummaryMigration);
  return db;
}

/** The executable audit covers every current index, so this fixture applies
 * every migration rather than the historical directory-only subset below. */
async function fullDatabase(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(shim);
  await db.exec(bootstrap);
  await db.exec(tokenBootstrap);
  await db.exec(readFileSync(join(import.meta.dir, "../../../supabase/bootstrap/session_ingest_roles.sql"), "utf8"));
  const root = join(import.meta.dir, "../../../supabase/migrations");
  for (const file of readdirSync(root).filter(file => file.endsWith(".sql")).sort())
    await db.exec(readFileSync(join(root, file), "utf8"));
  return db;
}

async function enterAuthenticated(
  db: PGlite,
  org = "o",
  workspaces = ["w"],
): Promise<void> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    JSON.stringify({
      role: "authenticated",
      app_metadata: { org_id: org, workspace_ids: workspaces },
    }),
  ]);
  await db.exec("SET ROLE authenticated");
}

async function leaveAuthenticated(db: PGlite): Promise<void> {
  await db.exec("RESET ROLE");
}

type Plan = {
  [key: string]: unknown;
  Plans?: Plan[];
  "Node Type"?: string;
  "Index Name"?: string;
  "Actual Rows"?: number;
  "Rows Removed by Filter"?: number;
};

function walk(plan: Plan): Plan[] {
  return [plan, ...(plan.Plans ?? []).flatMap(walk)];
}

async function explain(
  db: PGlite,
  sql: string,
  params: unknown[],
): Promise<Plan> {
  const result = await db.query<Record<string, unknown>>(
    `EXPLAIN (ANALYZE, FORMAT JSON) ${sql}`,
    params,
  );
  const envelope = result.rows[0]?.["QUERY PLAN"] as
    Array<{ Plan: Plan }> | undefined;
  if (!envelope?.[0]?.Plan) throw new Error("PostgreSQL returned no JSON plan");
  return envelope[0].Plan;
}

describe("session_directory migration", () => {
  test("matches the executable catalog audit and starts rollout incomplete", async () => {
    const db = await fullDatabase();
    const rows = await db.query<SearchIndexCatalogRow>(
      SEARCH_INDEX_CATALOG_SQL,
    );
    const report = auditSearchIndexes(rows.rows, {
      directoryProjectionComplete: true,
    });
    expect(report.indexes.filter((index) => !index.ready)).toEqual([]);
    const rollout = await db.query<{ completed_at: Date | null }>(
      "SELECT completed_at FROM sessions.session_directory_rollout",
    );
    expect(rollout.rows).toEqual([{ completed_at: null }]);
  });

  test("enforces one logical row with normalized user and machine membership", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized, machine_ids, user_ids, search_text)
      VALUES
        ('o', 'w', 'codex', 's', '2026-10-30T00:00:00Z', '2026-10-30T01:00:00Z',
         'Helm-Chief', 'helm-chief', ARRAY['m1', 'm2'], ARRAY['u1'], 'helm-chief repo model');
      INSERT INTO sessions.session_directory_users VALUES ('o', 'w', 'codex', 's', 'u1', '2026-10-30T01:00:00Z');
      INSERT INTO sessions.session_directory_machines VALUES
        ('o', 'w', 'codex', 's', 'm1', '2026-10-30T01:00:00Z'),
        ('o', 'w', 'codex', 's', 'm2', '2026-10-30T01:00:00Z');
    `);
    expect(
      (await db.query("SELECT * FROM sessions.session_directory")).rows,
    ).toHaveLength(1);
    expect(
      (await db.query("SELECT * FROM sessions.session_directory_machines"))
        .rows,
    ).toHaveLength(2);
    await expect(
      db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized)
      VALUES ('o', 'w', 'codex', 'bad', now(), now(), 'Mixed', 'MIXED')
    `),
    ).rejects.toThrow();
  });

  test("authenticated list queries expose only their column-granted metadata contract", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized, search_text)
      VALUES
        ('o', 'w', 'codex', 's', '2026-10-30T00:00:00Z', '2026-10-30T01:00:00Z',
         'Helm-Chief', 'helm-chief', 'private rpc-only text');
    `);

    for (const sql of Object.values(SESSION_DIRECTORY_QUERIES)) {
      expect(sql).not.toMatch(/\b(?:d\.)?\*/);
    }

    await enterAuthenticated(db);
    try {
      const result = await db.query<Record<string, unknown>>(
        SESSION_DIRECTORY_QUERIES.workspaceRecent,
        ["o", "w", 50],
      );
      expect(result.rows).toHaveLength(1);
      expect(Object.keys(result.rows[0] ?? {})).toEqual([
        ...SESSION_DIRECTORY_LIST_COLUMNS,
      ]);
      await expect(
        db.query("SELECT short_summary FROM sessions.session_directory"),
      ).rejects.toThrow();
      await expect(
        db.query("SELECT search_text FROM sessions.session_directory"),
      ).rejects.toThrow();
    } finally {
      await leaveAuthenticated(db);
    }
  });
});

describe("session_directory query plans", () => {
  test("uses the declared ordered indexes for list and membership filters", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         project_id, state, search_text)
      SELECT 'o', CASE WHEN n % 2 = 0 THEN 'w' ELSE 'other-w' END,
             CASE WHEN n % 4 < 2 THEN 'codex' ELSE 'claude' END, 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n % 10 = 0 THEN NULL ELSE 'project-' || (n % 100) END,
             CASE WHEN n % 3 = 0 THEN 'done' ELSE 'active' END,
             CASE WHEN n % 100 = 0 THEN 'needle project model' ELSE 'ordinary metadata' END
        FROM generate_series(1, 10000) AS n;
      INSERT INTO sessions.session_directory_users
      SELECT org_id, workspace_id, harness, session_id, 'u-' || (substring(session_id from 3)::integer % 100), last_activity_at
        FROM sessions.session_directory;
      INSERT INTO sessions.session_directory_machines
      SELECT org_id, workspace_id, harness, session_id, 'm-' || (substring(session_id from 3)::integer % 100), last_activity_at
        FROM sessions.session_directory;
      ANALYZE sessions.session_directory;
      ANALYZE sessions.session_directory_users;
      ANALYZE sessions.session_directory_machines;
    `);

    const cases: Array<[string, string, unknown[], string]> = [
      [
        "workspace",
        SESSION_DIRECTORY_QUERIES.workspaceRecent,
        ["o", "w", 50],
        "session_directory_workspace_recent_idx",
      ],
      [
        "harness",
        SESSION_DIRECTORY_QUERIES.harnessRecent,
        ["o", "w", "codex", 50],
        "session_directory_harness_recent_idx",
      ],
      [
        "project",
        SESSION_DIRECTORY_QUERIES.projectRecent,
        ["o", "w", "project-12", 50],
        "session_directory_project_recent_idx",
      ],
      [
        "no project",
        SESSION_DIRECTORY_QUERIES.withoutProjectRecent,
        ["o", "w", 50],
        "session_directory_no_project_recent_idx",
      ],
      [
        "created",
        SESSION_DIRECTORY_QUERIES.createdRange,
        ["o", "w", "2026-10-01", "2026-11-01", 50],
        "session_directory_created_recent_idx",
      ],
      [
        "state",
        SESSION_DIRECTORY_QUERIES.stateRecent,
        ["o", "w", "done", 50],
        "session_directory_state_recent_idx",
      ],
      [
        "user",
        SESSION_DIRECTORY_QUERIES.userRecent,
        ["o", "w", "u-12", 50],
        "session_directory_users_recent_idx",
      ],
      [
        "machine",
        SESSION_DIRECTORY_QUERIES.machineRecent,
        ["o", "w", "m-12", 50],
        "session_directory_machines_recent_idx",
      ],
    ];
    for (const [name, sql, params, expectedIndex] of cases) {
      const nodes = walk(await explain(db, sql, params));
      expect(
        nodes.some((node) => node["Index Name"] === expectedIndex),
        name,
      ).toBe(true);
    }
  });

  test("uses the ordered agent index through authenticated RLS for exact lookups; prefixes need the RPC", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized)
      SELECT 'o', 'w', 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n < 5000 THEN 'age-' || n ELSE 'helm-' || n END,
             CASE WHEN n < 5000 THEN 'age-' || n ELSE 'helm-' || n END
        FROM generate_series(1, 10000) AS n;
      ANALYZE sessions.session_directory;
    `);

    await enterAuthenticated(db);
    try {
      const exact = walk(
        await explain(db, SESSION_DIRECTORY_QUERIES.agentExact, [
          "o",
          "w",
          "helm-9999",
          50,
        ]),
      );
      expect(
        exact.some(
          (node) => node["Index Name"] === "session_directory_agent_recent_idx",
        ),
      ).toBe(true);

      // Prefix is RPC-only: the direct surface refuses it before any SQL.
      expect(() => directAgentPrefix("helm-99")).toThrow(
        AgentPrefixRequiresRpcError,
      );
    } finally {
      await leaveAuthenticated(db);
    }
  });

  test("the RPC refuses a broad three-character prefix by its candidate budget instead of scanning", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized)
      SELECT 'o', 'w', 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'other-' || n END,
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'other-' || n END
        FROM generate_series(1, 10000) AS n;
      ANALYZE sessions.session_directory;
    `);
    await db.query(
      "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
      ["https://issuer.test/session-directory", "session-directory-search"],
    );
    const asWorker = async (agent: string) => {
      const iat = Math.floor(Date.now() / 1000);
      await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
        JSON.stringify({
          role: "session_directory_search_worker",
          iss: "https://issuer.test/session-directory",
          aud: "session-directory-search",
          iat,
          exp: iat + 60,
          app_metadata: { org_id: "o", workspace_ids: ["w"] },
        }),
      ]);
      await db.exec("SET ROLE session_directory_search_worker");
      try {
        return await db.query(
          "SELECT * FROM sessions.search_session_directory(p_workspace_id => 'w', p_agent => $1, p_agent_match => 'prefix')",
          [agent],
        );
      } finally {
        await db.exec("RESET ROLE");
      }
    };
    // 5,100 candidates exceed the 5,000 budget: a typed refusal, not a tenant scan.
    const error = (await asWorker("age").then(
      () => null,
      (e: unknown) => e,
    )) as { code?: string; message?: string } | null;
    expect(error?.code).toBe("54000");
    expect(error?.message).toMatch(/agent_prefix_too_broad/);
    expect(AGENT_PREFIX_CANDIDATE_BUDGET).toBe(
      AGENT_PREFIX_BENCHMARK_GATE.pageSize *
        AGENT_PREFIX_BENCHMARK_GATE.maxIndexRowsExaminedPerResult,
    );
    // A longer prefix within the budget is served.
    const served = await asWorker("age-51");
    expect(served.rows.length).toBeGreaterThan(0);
  });
});
