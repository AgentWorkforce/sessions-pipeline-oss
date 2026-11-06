/**
 * Direct authenticated list queries under the real RLS policy (every supabase migration
 * applied, so jwt_org_id/jwt_workspace_ids/member_of are the production definitions),
 * and the RPC-only agent prefix (helm-chart-chief ruling on #38): the direct surface
 * refuses every prefix with a typed requires-RPC error and runs no prefix SQL; the
 * tenant-bound RPC serves prefixes under a bounded candidate budget and refuses a
 * broader prefix as agent_prefix_too_broad. In-process PGlite; synthetic data.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { auto_explain } from "@electric-sql/pglite/contrib/auto_explain";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  AGENT_PREFIX_CANDIDATE_BUDGET,
  AgentPrefixRequiresRpcError,
  NonAsciiAgentFilterError,
  SESSION_DIRECTORY_LIST_COLUMNS,
  SESSION_DIRECTORY_QUERIES,
  agentExactValue,
  directAgentPrefix,
  normalizeAgentName,
} from "../search-index-contract.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const WORKER = "session_directory_search_worker";
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
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, "session-directory-search"],
  );
  return db;
}

async function asAuthenticated<T>(
  db: PGlite,
  run: () => Promise<T>,
  org = "o",
  workspaces = ["w"],
): Promise<T> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    JSON.stringify({
      role: "authenticated",
      app_metadata: { org_id: org, workspace_ids: workspaces },
    }),
  ]);
  await db.exec("SET ROLE authenticated");
  try {
    return await run();
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

async function insertAgents(
  db: PGlite,
  names: string[],
  org = "o",
  workspace = "w",
) {
  for (const [i, name] of names.entries()) {
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, agent_name, agent_name_normalized)
       VALUES ($1, $2, 'codex', $3, now(), now() + $4 * interval '1 second', $5::text, lower(btrim($5::text)))`,
      [org, workspace, `s-${String(i).padStart(4, "0")}`, i, name],
    );
  }
}

/** The tenant-bound RPC as PostgREST would call it for the same tenant. */
async function rpcAgent(
  db: PGlite,
  agent: string,
  match: "exact" | "prefix",
  onNotice?: (message: string) => void,
): Promise<string[]> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    JSON.stringify({
      role: WORKER,
      iss: TOKEN_ISSUER,
      aud: "session-directory-search",
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 60,
      app_metadata: { org_id: "o", workspace_ids: ["w"] },
    }),
  ]);
  await db.exec(`SET ROLE ${WORKER}`);
  try {
    const { rows } = await db.query<{ agent_name: string }>(
      "SELECT * FROM sessions.search_session_directory(p_workspace_id => 'w', p_agent => $1, p_agent_match => $2)",
      [agent, match],
      onNotice
        ? ({
            onNotice: (n: { message: string }) => onNotice(n.message),
          } as never)
        : undefined,
    );
    return rows.map((r) => r.agent_name).sort();
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}
const code = async (p: Promise<unknown>) =>
  (
    (await p.then(
      () => null,
      (e: unknown) => e,
    )) as { code?: string } | null
  )?.code;

describe("leakproof operator flags the design depends on", () => {
  test("textlike and texticlike are not leakproof; texteq and the text_pattern_ops bounds are", async () => {
    const db = await database();
    try {
      const { rows } = await db.query<{
        proname: string;
        proleakproof: boolean;
      }>(
        `SELECT proname, proleakproof FROM pg_proc
          WHERE proname IN ('textlike', 'texticlike', 'texteq', 'text_pattern_ge', 'text_pattern_lt') ORDER BY proname`,
      );
      expect(rows).toEqual([
        { proname: "text_pattern_ge", proleakproof: true },
        { proname: "text_pattern_lt", proleakproof: true },
        { proname: "texteq", proleakproof: true },
        { proname: "texticlike", proleakproof: false },
        { proname: "textlike", proleakproof: false },
      ]);
    } finally {
      await db.close();
    }
  });
});

describe("direct list queries", () => {
  test("name only the column-granted metadata and never a wildcard", () => {
    const grant =
      /GRANT SELECT \(([^)]*)\) ON sessions\.session_directory TO authenticated/.exec(
        read(
          "supabase/migrations/20261030020000_session_directory_short_summary.sql",
        ),
      );
    expect(grant).not.toBeNull();
    const granted = grant![1]!
      .split(",")
      .map((c) => c.trim())
      .filter(Boolean);
    expect(granted).toHaveLength(24);
    expect([...SESSION_DIRECTORY_LIST_COLUMNS]).toEqual(granted);
    for (const [name, sql] of Object.entries(SESSION_DIRECTORY_QUERIES)) {
      expect(sql, name).not.toMatch(/\*/);
      expect(sql, name).not.toMatch(/\b(short_summary|search_text)\b/);
    }
  });

  test("exact stays direct with neither LIKE nor a range; there is no direct prefix query", () => {
    expect(SESSION_DIRECTORY_QUERIES.agentExact).toMatch(
      /agent_name_normalized = \$3/,
    );
    expect(Object.keys(SESSION_DIRECTORY_QUERIES)).not.toContain("agentPrefix");
    for (const [name, sql] of Object.entries(SESSION_DIRECTORY_QUERIES)) {
      expect(sql, name).not.toMatch(/\bLIKE\b|~>=~|~<~/);
    }
  });

  test("every direct agent prefix is a typed requires-RPC refusal, before any SQL", () => {
    for (const value of [
      "helm-99",
      "age",
      "a_b",
      "ab%",
      "ab\\",
      "ΟΔΟΣ",
      "",
      "  ",
    ]) {
      expect(() => directAgentPrefix(value), value).toThrow(
        AgentPrefixRequiresRpcError,
      );
    }
  });
});

describe("agent filters: exact direct, prefix through the RPC", () => {
  let db: PGlite;
  const NAMES = [
    "a_bc",
    "axbc",
    "a_b",
    "ab%x",
    "abcx",
    "ab\\y",
    "abzy",
    "ab%",
    "ab\\",
    "café-one",
    "cafe-two",
    "cafг",
    "日本語-agent",
    "日本-x",
    "Ägent-Nord",
    "ägent-süd",
    "ΟΔΟΣ-Agent",
    "İstanbul-X",
    "Helm-Chief",
  ];
  beforeAll(async () => {
    db = await database();
    await insertAgents(db, NAMES);
    await db.exec("ANALYZE sessions.session_directory");
  }, 60_000);
  afterAll(async () => {
    await db?.close();
  });

  test("RPC prefixes match %, _ and backslash literally", async () => {
    expect(await rpcAgent(db, "a_b", "prefix")).toEqual(["a_b", "a_bc"]);
    expect(await rpcAgent(db, "ab%", "prefix")).toEqual(["ab%", "ab%x"]);
    expect(await rpcAgent(db, "ab\\", "prefix")).toEqual(["ab\\", "ab\\y"]);
    expect(await rpcAgent(db, "caf", "prefix")).toEqual(
      ["cafe-two", "café-one", "cafг"].sort(),
    );
    expect(await rpcAgent(db, "日本語", "prefix")).toEqual(["日本語-agent"]);
    expect(await code(rpcAgent(db, "ab", "prefix"))).toBe("22023");
  });

  test("an uppercase non-ASCII prefix finds its rows through the RPC; the direct path refuses it", async () => {
    for (const [name, prefix] of [
      ["ΟΔΟΣ-Agent", "ΟΔΟΣ"],
      ["İstanbul-X", "İSTANBUL"],
    ] as const) {
      // PostgreSQL lower() and JavaScript toLowerCase() disagree here, so only the RPC,
      // which lowers with the same SQL lower() as the stored value, is exact.
      const sqlLower = (
        await db.query<{ v: string }>("SELECT lower(btrim($1)) AS v", [prefix])
      ).rows[0]!.v;
      expect(sqlLower, name).not.toBe(prefix.toLowerCase());
      expect(() => directAgentPrefix(prefix)).toThrow(
        AgentPrefixRequiresRpcError,
      );
      expect(await rpcAgent(db, prefix, "prefix"), name).toEqual([name]);
    }
    expect(await rpcAgent(db, "ÄGENT", "prefix")).toEqual(
      ["Ägent-Nord", "ägent-süd"].sort(),
    );
  });

  test("exact: a non-ASCII name is refused on the direct path and found through the RPC", async () => {
    for (const [name, input] of [
      ["ΟΔΟΣ-Agent", "ΟΔΟΣ-AGENT"],
      ["İstanbul-X", "İSTANBUL-X"],
    ] as const) {
      const sqlLower = (
        await db.query<{ v: string }>("SELECT lower(btrim($1)) AS v", [input])
      ).rows[0]!.v;
      const jsLower = input.trim().toLowerCase();
      expect(sqlLower, name).not.toBe(jsLower);
      // A JavaScript-normalized exact value would silently miss the stored row...
      const naive = await asAuthenticated(
        db,
        async () =>
          (
            await db.query(SESSION_DIRECTORY_QUERIES.agentExact, [
              "o",
              "w",
              jsLower,
              50,
            ])
          ).rows,
      );
      expect(naive, name).toEqual([]);
      // ...so the direct helpers refuse it, pointing at the RPC's exact match,
      expect(() => agentExactValue(input)).toThrow(NonAsciiAgentFilterError);
      expect(() => agentExactValue(input)).toThrow(/p_agent_match => 'exact'/);
      expect(() => normalizeAgentName(input)).toThrow(NonAsciiAgentFilterError);
      // which finds it.
      expect(await rpcAgent(db, input, "exact"), name).toEqual([name]);
    }
    // ASCII exact values pass straight through and match directly.
    const direct = await asAuthenticated(
      db,
      async () =>
        (
          await db.query<{ agent_name: string }>(
            SESSION_DIRECTORY_QUERIES.agentExact,
            ["o", "w", agentExactValue(" Helm-CHIEF "), 50],
          )
        ).rows,
    );
    expect(direct.map((r) => r.agent_name)).toEqual(["Helm-Chief"]);
    expect(() => agentExactValue("   ")).toThrow(RangeError);
  });

  test("property: RPC prefix results equal plain LIKE semantics for generated prefixes", async () => {
    const alphabet = [
      "a",
      "b",
      "_",
      "%",
      "\\",
      "é",
      "日",
      "z",
      String.fromCodePoint(0x10ffff),
    ];
    let seed = 42;
    const rand = () => {
      seed = (seed * 1103515245 + 12345) % 2 ** 31;
      return seed / 2 ** 31;
    };
    const pick = () => alphabet[Math.floor(rand() * alphabet.length)]!;
    const generated = [
      ...new Set(
        Array.from({ length: 160 }, () =>
          Array.from({ length: 3 + Math.floor(rand() * 4) }, pick).join(""),
        ),
      ),
    ];
    const prop = await database();
    try {
      await insertAgents(prop, generated);
      const prefixes = [
        ...new Set([
          ...generated
            .slice(0, 40)
            .map((n) => Array.from(n).slice(0, 3).join("")),
          ...Array.from({ length: 40 }, () =>
            Array.from({ length: 3 }, pick).join(""),
          ),
        ]),
      ];
      let nonEmpty = 0;
      for (const prefix of prefixes) {
        const viaRpc = await rpcAgent(prop, prefix, "prefix");
        const like = (
          await prop.query<{ agent_name: string }>(
            `SELECT agent_name FROM sessions.session_directory
            WHERE org_id = 'o' AND workspace_id = 'w' AND deleted_at IS NULL
              AND agent_name_normalized LIKE $1 ESCAPE '\\'`,
            [`${prefix.replace(/[\\%_]/g, "\\$&")}%`],
          )
        ).rows
          .map((r) => r.agent_name)
          .sort();
        expect(viaRpc, JSON.stringify(prefix)).toEqual(like.slice(0, 51));
        expect(like, JSON.stringify(prefix)).toEqual(
          generated.filter((n) => n.startsWith(prefix)).sort(),
        );
        if (viaRpc.length) nonEmpty += 1;
      }
      expect(nonEmpty).toBeGreaterThan(20);
    } finally {
      await prop.close();
    }
  }, 120_000);
});

type Plan = { [key: string]: unknown; Plans?: Plan[] };
const walk = (plan: Plan): Plan[] => [
  plan,
  ...(plan.Plans ?? []).flatMap(walk),
];

describe("plans under the real RLS policy and the RPC budget (10,500 rows)", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, agent_name, agent_name_normalized)
      SELECT 'o', 'w', 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'helm-' || n END,
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'helm-' || n END
        FROM generate_series(1, 10000) AS n;
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, agent_name, agent_name_normalized)
      SELECT 'other', 'w', 'codex', 'x-' || n, now(), now(), 'helm-' || n, 'helm-' || n FROM generate_series(1, 500) AS n;
      ANALYZE sessions.session_directory;
    `);
  }, 60_000);
  afterAll(async () => {
    await db?.close();
  });

  async function explain(sql: string, params: unknown[]) {
    const result = await db.query<Record<string, unknown>>(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`,
      params,
    );
    const plan = (result.rows[0]?.["QUERY PLAN"] as Array<{ Plan: Plan }>)[0]!
      .Plan;
    return { nodes: walk(plan), returned: Number(plan["Actual Rows"] ?? 0) };
  }

  test("exact under authenticated RLS: an agent_recent_idx equality Index Cond (owner baseline alongside)", async () => {
    const args = ["o", "w", "helm-9999", 50];
    const owner = await explain(SESSION_DIRECTORY_QUERIES.agentExact, args);
    const user = await asAuthenticated(db, () =>
      explain(SESSION_DIRECTORY_QUERIES.agentExact, args),
    );
    const shape = (p: typeof owner) =>
      p.nodes
        .map((n) =>
          [n["Node Type"], n["Index Name"], n["Index Cond"]]
            .filter(Boolean)
            .join(" | "),
        )
        .join("  ->  ");
    console.info(
      `[G6 RLS plan] exact helm-9999\n  owner:         ${shape(owner)}\n  authenticated: ${shape(user)}`,
    );
    const scan = user.nodes.find(
      (n) => n["Index Name"] === "session_directory_agent_recent_idx",
    );
    expect(String(scan?.["Index Cond"])).toContain(
      "agent_name_normalized = 'helm-9999'",
    );
    expect(user.nodes.some((n) => n["Node Type"] === "Seq Scan")).toBe(false);
    expect(user.returned).toBe(1);
  });

  test("worst-case prefix 'age': refused by the RPC budget after reading at most budget + 1 index rows", async () => {
    await db.exec(`LOAD 'auto_explain';
      SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on;
      SET auto_explain.log_format = 'json'; SET auto_explain.log_nested_statements = on;
      SET client_min_messages = log;`);
    const messages: string[] = [];
    let failure: { code?: string; message?: string } | null = null;
    try {
      await rpcAgent(db, "age", "prefix", (m) => messages.push(m));
    } catch (e) {
      failure = e as { code?: string; message?: string };
    } finally {
      await db.exec(
        "SET auto_explain.log_min_duration = -1; SET client_min_messages = notice;",
      );
    }
    expect(failure?.code).toBe("54000");
    expect(failure?.message).toMatch(/agent_prefix_too_broad/);
    const probe = messages
      .filter((m) => m.includes('"Query Text"'))
      .map(
        (m) =>
          JSON.parse(m.slice(m.indexOf("{"))) as {
            "Query Text": string;
            Plan: Plan;
          },
      )
      .find((e) =>
        /SELECT count\(\*\) FROM \(SELECT 1 FROM sessions\.session_directory/.test(
          e["Query Text"],
        ),
      );
    expect(probe).toBeDefined();
    const nodes = walk(probe!.Plan);
    console.info(
      `[G6 RPC budget probe] age: ${nodes.map((n) => [n["Node Type"], n["Index Name"], `rows=${n["Actual Rows"]}`].filter(Boolean).join(" | ")).join("  ->  ")}`,
    );
    expect(nodes.some((n) => n["Node Type"] === "Seq Scan")).toBe(false);
    const scans = nodes.filter((n) => /Scan$/.test(String(n["Node Type"])));
    expect(scans.map((n) => n["Index Name"])).toEqual([
      "session_directory_agent_recent_idx",
    ]);
    for (const scan of scans) {
      expect(
        Number(scan["Actual Rows"]) +
          Number(scan["Rows Removed by Filter"] ?? 0),
      ).toBeLessThanOrEqual(AGENT_PREFIX_CANDIDATE_BUDGET + 1);
    }
    // The page query never ran: the refusal came first.
    expect(
      messages.some((m) => m.includes("ORDER BY d.last_activity_at DESC")),
    ).toBe(false);
  });

  test("a prefix within the budget is served by the RPC, and the caller's planner settings are restored", async () => {
    expect((await rpcAgent(db, "age-50", "prefix")).length).toBeGreaterThan(0);
    expect((await rpcAgent(db, "helm-99", "prefix")).length).toBe(51);
    const settings = await db.query<{ s: string; b: string; o: string }>(
      "SELECT current_setting('enable_seqscan') AS s, current_setting('enable_bitmapscan') AS b, current_setting('enable_sort') AS o",
    );
    expect(settings.rows[0]).toEqual({ s: "on", b: "on", o: "on" });
  });

  test("a cross-tenant read returns 0 rows for every direct list query", async () => {
    const params: Record<string, unknown[]> = {
      workspaceRecent: ["o", "w", 50],
      harnessRecent: ["o", "w", "codex", 50],
      projectRecent: ["o", "w", "p", 50],
      withoutProjectRecent: ["o", "w", 50],
      agentExact: ["o", "w", "helm-9999", 50],
      createdRange: ["o", "w", "2000-01-01", "2100-01-01", 50],
      stateRecent: ["o", "w", "unknown", 50],
      userRecent: ["o", "w", "u", 50],
      machineRecent: ["o", "w", "m", 50],
    };
    expect(Object.keys(params).sort()).toEqual(
      Object.keys(SESSION_DIRECTORY_QUERIES).sort(),
    );
    const own = await asAuthenticated(
      db,
      async () =>
        (
          await db.query(
            SESSION_DIRECTORY_QUERIES.workspaceRecent,
            params.workspaceRecent,
          )
        ).rows,
    );
    expect(own.length).toBe(50);
    for (const [org, workspaces] of [
      ["other", ["w"]],
      ["o", ["not-w"]],
      ["", ["w"]],
    ] as const) {
      for (const [name, sql] of Object.entries(SESSION_DIRECTORY_QUERIES)) {
        const rows = await asAuthenticated(
          db,
          async () => (await db.query(sql, params[name]!)).rows,
          org,
          [...workspaces],
        );
        expect(rows, `${name} as ${org}/${workspaces}`).toEqual([]);
      }
    }
  });
});
