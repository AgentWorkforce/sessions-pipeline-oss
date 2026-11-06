/**
 * Expand phase (20261031001000): claim-bound pointer and readiness functions that
 * replace RelayHistory #226's last direct reads. Additive only: nothing is revoked and
 * the direct surface is unchanged here. In-process PGlite with every supabase migration.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  SESSION_DIRECTORY_POINTERS_RPC,
  SESSION_DIRECTORY_READINESS_RPC,
  SESSION_DIRECTORY_SEARCH_RPC,
} from "../search-index-contract.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;
const UP = read(
  "supabase/migrations/20261031001000_session_directory_reads_expand.sql",
);
const DOWN = read(
  "supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql",
);
const CANONICAL_UP = read(
  "supabase/migrations/20261102010000_session_directory_canonical_ids.sql",
);
const CANONICAL_DOWN = read(
  "supabase/rollbacks/20261102010000_session_directory_canonical_ids.down.sql",
);
const FUNCTIONS = [
  "sessions.session_directory_pointers(text, text, text)",
  "sessions.session_directory_readiness()",
  "sessions.session_directory_worker_scope(text)",
];

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
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
  );
  return db;
}

async function pointer(
  db: PGlite,
  org: string,
  workspace: string,
  machine: string,
  deleted = false,
) {
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
     VALUES ($1, $2, 'codex', 's-1', $3, 'u-' || $3, now(), now(), 's3', 'bucket', 'v1/' || $3,
             decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2,
             CASE WHEN $4 THEN now() END)`,
    [org, workspace, machine, deleted],
  );
}

const TOKEN_ISSUER = "https://issuer.test/session-directory";
const claims = (
  org: string,
  workspaces: unknown[],
  role = WORKER,
  overrides: Record<string, unknown> = {},
) => {
  const iat = Math.floor(Date.now() / 1000);
  return JSON.stringify({
    role,
    iss: TOKEN_ISSUER,
    aud: SESSION_DIRECTORY_SEARCH_RPC.audience,
    iat,
    exp: iat + 60,
    app_metadata: { org_id: org, workspace_ids: workspaces },
    ...overrides,
  });
};

async function asRole<T>(
  db: PGlite,
  role: string,
  claimsJson: string | null,
  run: () => Promise<T>,
): Promise<T> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    claimsJson ?? "",
  ]);
  await db.exec(`SET ROLE ${role}`);
  try {
    return await run();
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}
const pointers = (
  db: PGlite,
  claimsJson: string | null,
  ws: string | null,
  harness = "codex",
  session = "s-1",
  role = WORKER,
) =>
  asRole(
    db,
    role,
    claimsJson,
    async () =>
      (
        await db.query<Record<string, unknown>>(
          "SELECT * FROM sessions.session_directory_pointers($1, $2, $3)",
          [ws, harness, session],
        )
      ).rows,
  );
const code = async (p: Promise<unknown>) =>
  (
    (await p.then(
      () => null,
      (e: unknown) => e,
    )) as { code?: string } | null
  )?.code;

describe("token pins shared with the search RPC", () => {
  test("pointers and readiness refuse a token the search RPC would refuse", async () => {
    const db = await database();
    await pointer(db, "org-a", "ws-a", "m-1");
    const iat = Math.floor(Date.now() / 1000);
    const refusedClaims = [
      claims("org-a", ["ws-a"], WORKER, { iss: "https://other.test" }),
      claims("org-a", ["ws-a"], WORKER, { aud: "relayhistory" }),
      claims("org-a", ["ws-a"], WORKER, { aud: [SESSION_DIRECTORY_SEARCH_RPC.audience] }),
      claims("org-a", ["ws-a"], WORKER, { iat, exp: iat + 3600 }),
      claims("org-a", ["ws-a"], WORKER, { iat: iat + 3600, exp: iat + 3660 }),
      claims("org-a", ["ws-a"], WORKER, { iat: undefined }),
      claims("org-a", Array.from({ length: 65 }, (_, i) => `ws-${i}`)),
      claims("org-a", ["ws-a", "bad id"]),
    ];
    for (const c of refusedClaims) {
      expect(await code(pointers(db, c, "ws-a")), c).toBe("42501");
      expect(
        await code(
          asRole(db, WORKER, c, () =>
            db.query("SELECT * FROM sessions.session_directory_readiness()"),
          ),
        ),
        c,
      ).toBe("42501");
    }
    expect(await pointers(db, claims("org-a", ["ws-a"]), "ws-a")).toHaveLength(1);
  });

  test("with no token policy row, nothing is served", async () => {
    const db = await database();
    await pointer(db, "org-a", "ws-a", "m-1");
    await db.query("DELETE FROM sessions.session_directory_search_token_policy");
    expect(await code(pointers(db, claims("org-a", ["ws-a"]), "ws-a"))).toBe(
      "42501",
    );
  });
});

describe("session_directory_pointers", () => {
  test("returns exactly #226's pointer columns for live pointers in the claimed workspace, ordered by machine", async () => {
    const db = await database();
    for (const m of ["m-3", "m-1", "m-2"])
      await pointer(db, "org-a", "ws-a", m);
    await pointer(db, "org-a", "ws-a", "m-0", true);
    const rows = await pointers(db, claims("org-a", ["ws-a"]), "ws-a");
    expect(rows.map((r) => r.machine_id)).toEqual(["m-1", "m-2", "m-3"]);
    expect(Object.keys(rows[0]!)).toEqual([
      ...SESSION_DIRECTORY_POINTERS_RPC.returns,
    ]);
    expect(
      rows.every((r) => r.deleted_at === null && r.org_id === "org-a"),
    ).toBe(true);
    expect(Number(rows[0]!.revision)).toBe(2);
  });

  test("looks up and returns canonical session and machine ids while preserving the physical manifest pointer", async () => {
    const db = await database();
    await pointer(db, "org-a", "ws-a", "delivery_abc");
    await db.query(
      `UPDATE sessions.session_index
          SET canonical_session_id = 'kimi:session',
              canonical_machine_id = 'delivery:abc'
        WHERE org_id = 'org-a' AND workspace_id = 'ws-a'`,
    );
    const rows = await pointers(
      db,
      claims("org-a", ["ws-a"]),
      "ws-a",
      "codex",
      "kimi:session",
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      session_id: "kimi:session",
      machine_id: "delivery:abc",
      manifest_key: "v1/delivery_abc",
    });
    expect(
      await pointers(
        db,
        claims("org-a", ["ws-a"]),
        "ws-a",
        "codex",
        "s-1",
      ),
    ).toEqual(rows);
  });

  test("caps the result at 64 rows", async () => {
    const db = await database();
    for (let i = 0; i < 70; i += 1)
      await pointer(db, "org-a", "ws-a", `m-${String(i).padStart(2, "0")}`);
    expect(await pointers(db, claims("org-a", ["ws-a"]), "ws-a")).toHaveLength(
      SESSION_DIRECTORY_POINTERS_RPC.maxRows,
    );
  });

  test("cross-tenant: another org with the same ids sees nothing, and unclaimed or forged scopes are refused", async () => {
    const db = await database();
    await pointer(db, "org-a", "ws-a", "m-1");
    await pointer(db, "org-b", "ws-a", "m-9");
    expect(
      (await pointers(db, claims("org-b", ["ws-a"]), "ws-a")).map(
        (r) => r.machine_id,
      ),
    ).toEqual(["m-9"]);
    expect(await pointers(db, claims("org-c", ["ws-a"]), "ws-a")).toEqual([]);
    for (const [label, c, ws] of [
      ["unclaimed workspace", claims("org-a", ["ws-b"]), "ws-a"],
      ["no claims", null, "ws-a"],
      ["malformed claims", "{not json", "ws-a"],
      ["wrong role claim", claims("org-a", ["ws-a"], "authenticated"), "ws-a"],
      ["empty org", claims("", ["ws-a"]), "ws-a"],
      ["non-string workspaces", claims("org-a", [7, { id: "ws-a" }]), "ws-a"],
    ] as const) {
      expect(await code(pointers(db, c, ws)), label).toBe("42501");
    }
    for (const [label, ws, harness, session] of [
      ["missing workspace", null, "codex", "s-1"],
      ["invalid harness", "ws-a", "Not A Harness", "s-1"],
      ["empty session", "ws-a", "codex", ""],
    ] as const) {
      expect(
        await code(
          pointers(db, claims("org-a", ["ws-a"]), ws, harness, session),
        ),
        label,
      ).toBe("22023");
    }
  });

  test("only the Worker can execute, and a later broad grant is refused at call time", async () => {
    const db = await database();
    await pointer(db, "org-a", "ws-a", "m-1");
    for (const fn of FUNCTIONS) {
      const executors = await db.query<{ rolname: string }>(
        `SELECT r.rolname FROM pg_roles AS r, pg_proc AS p
          WHERE p.oid = $1::regprocedure AND r.oid <> p.proowner
            AND has_function_privilege(r.oid, p.oid, 'EXECUTE') ORDER BY 1`,
        [fn],
      );
      expect(
        executors.rows.map((r) => r.rolname),
        fn,
      ).toEqual(fn.includes("worker_scope") ? [] : [WORKER]);
      const meta = (
        await db.query<{ prosecdef: boolean; proconfig: string[] }>(
          "SELECT prosecdef, proconfig FROM pg_proc WHERE oid = $1::regprocedure",
          [fn],
        )
      ).rows[0]!;
      expect(meta.proconfig, fn).toContain(
        "search_path=pg_catalog, sessions, pg_temp",
      );
      expect(meta.prosecdef, fn).toBe(!fn.includes("worker_scope"));
    }
    for (const role of ["authenticated", "anon", "service_role"]) {
      expect(
        await code(
          pointers(db, claims("org-a", ["ws-a"]), "ws-a", "codex", "s-1", role),
        ),
        role,
      ).toBe("42501");
    }
    await db.exec(`CREATE ROLE relayhistory_app; GRANT USAGE ON SCHEMA sessions TO relayhistory_app;
      GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO relayhistory_app;`);
    expect(
      await code(
        pointers(
          db,
          claims("org-a", ["ws-a"]),
          "ws-a",
          "codex",
          "s-1",
          "relayhistory_app",
        ),
      ),
    ).toBe("42501");
    // The Worker still has no table privileges: the functions add no direct surface.
    expect(
      await code(
        asRole(db, WORKER, claims("org-a", ["ws-a"]), () =>
          db.query("SELECT * FROM sessions.session_index"),
        ),
      ),
    ).toBe("42501");
  });
});

describe("session_directory_readiness", () => {
  const readiness = (db: PGlite, c: string | null, role = WORKER) =>
    asRole(
      db,
      role,
      c,
      async () =>
        (
          await db.query<{ completed_at: Date | null }>(
            "SELECT * FROM sessions.session_directory_readiness()",
          )
        ).rows,
    );

  test("returns only completed_at for a valid Worker caller, and refuses everyone else", async () => {
    const db = await database();
    const ok = claims("org-a", ["ws-a"]);
    expect(await readiness(db, ok)).toEqual([{ completed_at: null }]);
    expect(Object.keys((await readiness(db, ok))[0]!)).toEqual([
      ...SESSION_DIRECTORY_READINESS_RPC.returns,
    ]);
    await db.query(
      "UPDATE sessions.session_directory_rollout SET completed_at = '2026-11-01T00:00:00Z'",
    );
    expect((await readiness(db, ok))[0]!.completed_at?.toISOString()).toBe(
      "2026-11-01T00:00:00.000Z",
    );
    for (const [label, c] of [
      ["no claims", null],
      ["empty workspace list", claims("org-a", [])],
      ["wrong role claim", claims("org-a", ["ws-a"], "anon")],
    ] as const) {
      expect(await code(readiness(db, c)), label).toBe("42501");
    }
    expect(await code(readiness(db, ok, "authenticated"))).toBe("42501");
  });
});

describe("expand migration", () => {
  test("is additive: it revokes nothing and leaves the direct grants and the search RPC as they were", async () => {
    const db = await database();
    // The direct authenticated list surface still exists (contract phase is later).
    const granted = await db.query<{ ok: boolean }>(
      "SELECT has_column_privilege('authenticated', 'sessions.session_directory', 'project_id', 'SELECT') AS ok",
    );
    expect(granted.rows[0]!.ok).toBe(true);
    const sql = UP.replace(/--[^\n]*/g, "");
    // Table revokes touch only the table this migration creates (its default lockdown).
    expect(
      [
        ...sql.matchAll(
          /\bREVOKE\s+(?:SELECT|ALL)\s+ON\s+(?:TABLE\s+)?(sessions\.\w+)/gi,
        ),
      ].map((r) => r[1]),
    ).toEqual(["sessions.session_directory_resurrection_fences"]);
    expect(sql).not.toMatch(/\bCREATE\s+INDEX\b/i);
    expect(sql).not.toMatch(/\bDROP\b/i);
    // The only ALTER enables RLS on the table this migration creates.
    expect(sql.match(/\bALTER\s+\w+\s+[\w.]+/gi)).toEqual([
      "ALTER TABLE sessions.session_directory_resurrection_fences",
    ]);
    expect(sql).not.toMatch(/search_session_directory/);
  });

  test("down drops only what up added, and up applies again", async () => {
    const db = await database();
    await db.exec(DOWN);
    for (const fn of [
      "session_directory_pointers",
      "session_directory_readiness",
      "session_directory_worker_scope",
    ]) {
      expect(
        (await db.query("SELECT 1 FROM pg_proc WHERE proname = $1", [fn])).rows,
        fn,
      ).toEqual([]);
    }
    expect(
      (
        await db.query(
          "SELECT 1 FROM pg_proc WHERE proname = 'search_session_directory'",
        )
      ).rows,
    ).toHaveLength(1);
    await db.exec(UP);
    await pointer(db, "org-a", "ws-a", "m-1");
    expect(await pointers(db, claims("org-a", ["ws-a"]), "ws-a")).toHaveLength(
      1,
    );
  });
});

describe("canonical identity migration", () => {
  test("down restores physical pointer reads and up applies again", async () => {
    const db = await database();
    await pointer(db, "org-a", "ws-a", "delivery_abc");
    await db.exec(CANONICAL_DOWN);
    expect(
      (
        await db.query(
          `SELECT 1 FROM pg_attribute
            WHERE attrelid = 'sessions.session_index'::regclass
              AND attname = 'canonical_session_id' AND NOT attisdropped`,
        )
      ).rows,
    ).toEqual([]);
    expect(
      await pointers(db, claims("org-a", ["ws-a"]), "ws-a"),
    ).toHaveLength(1);
    await db.exec(CANONICAL_UP);
    await db.query(
      `UPDATE sessions.session_index
          SET canonical_session_id = 'kimi:session', canonical_machine_id = 'delivery:abc'`,
    );
    expect(
      await pointers(
        db,
        claims("org-a", ["ws-a"]),
        "ws-a",
        "codex",
        "kimi:session",
      ),
    ).toHaveLength(1);
  });
});
