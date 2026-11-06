/**
 * G6: short_summary on sessions.session_directory and its one search path,
 * sessions.search_session_directory. In-process PGlite only, with every
 * supabase/migrations file applied in name order after the Supabase role shim; no
 * hosted database is involved. Synthetic ids and text only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { auto_explain } from "@electric-sql/pglite/contrib/auto_explain";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  objectKey,
  rawChunkKey,
  type ManifestV1,
} from "../../shared/vendor.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { createHash } from "node:crypto";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { bobCostasHandler } from "../../bob-costas/handler.ts";
import { extractiveSummarizer } from "../../bob-costas/summarizer.ts";
import {
  FORMAT_RANGES,
  MARK_RANGES,
  SEPARATOR_RANGES,
  normalizeSearchQuery,
  pgClass,
  searchRpcRequest,
} from "../search-query.ts";
import type { HoopDb } from "../../shared/db.ts";
import { hoopDbSessionIndex } from "../index.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import {
  SESSION_DIRECTORY_SEARCH_BOUNDARY,
  SESSION_DIRECTORY_SEARCH_RPC,
  SESSION_DIRECTORY_SEARCH_SQL,
} from "../search-index-contract.ts";
import {
  SHORT_SUMMARY_MAX_CHARS,
  ShortSummaryRejected,
  clearShortSummary,
  eraseSessionDirectory,
  scrubShortSummary,
  writeShortSummary,
} from "../session-directory-summary.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const MIGRATION = "20261030020000_session_directory_short_summary.sql";
const DOWN = read(
  `supabase/rollbacks/${MIGRATION.replace(/\.sql$/, ".down.sql")}`,
);
const UP = read(`supabase/migrations/${MIGRATION}`);
// Later migrations that depend on G6 (its role and functions): rolled back first.
const EXPAND = "20261031001000_session_directory_reads_expand.sql";
const EXPAND_UP = read(`supabase/migrations/${EXPAND}`);
const EXPAND_DOWN = read(
  `supabase/rollbacks/${EXPAND.replace(/\.sql$/, ".down.sql")}`,
);
// 20261101050000 (the list RPC) grants the Worker EXECUTE too, so a rollback past G6
// unwinds it first and reapplies it last.
const LIST = "20261101050000_session_directory_list.sql";
const LIST_UP = read(`supabase/migrations/${LIST}`);
const LIST_DOWN = read(`supabase/rollbacks/${LIST.replace(/\.sql$/, ".down.sql")}`);
// The session briefs migration uses the expand phase's Worker scope and G6's Worker role.
const BRIEFS = "20261102000000_session_briefs.sql";
const BRIEFS_UP = read(`supabase/migrations/${BRIEFS}`);
const BRIEFS_DOWN = read(
  `supabase/rollbacks/${BRIEFS.replace(/\.sql$/, ".down.sql")}`,
);
const BRIEF_STATE = "20261102020000_session_brief_state.sql";
const BRIEF_STATE_UP = read(`supabase/migrations/${BRIEF_STATE}`);
const BRIEF_STATE_DOWN = read(
  `supabase/rollbacks/${BRIEF_STATE.replace(/\.sql$/, ".down.sql")}`,
);
const EVENTS = "20261104021000_session_directory_event_metadata.sql";
const EVENTS_UP = read(`supabase/migrations/${EVENTS}`);
const EVENTS_DOWN = read(`supabase/rollbacks/${EVENTS.replace(/\.sql$/, ".down.sql")}`);
const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;
// Newer additive consumers of the event/list columns unwind in strict reverse
// dependency order. Their explicit down files are part of the schema contract.
const LATER_PHASES=readdirSync(join(root,"supabase/rollbacks")).filter(f=>f.endsWith(".down.sql")&&f>EVENTS.replace(/\.sql$/,".down.sql")).sort();

const TOKEN_ISSUER = "https://issuer.test/session-directory";
const RPC_SIGNATURE = "sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)";

// Synthetic secret-shaped values, assembled at runtime so no literal sits on disk.
const FAKE_RELAY_TOKEN = ["relay", "ag", "x".repeat(24)].join("_");
const FAKE_HOME = ["", "Users", "alice", "proj"].join("/");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(prelude = ""): Promise<PGlite> {
  const db = new PGlite({ extensions: { auto_explain, btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  if (prelude) await db.exec(prelude);
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
  );
  return db;
}

/** The full HoopDb plan (as src/hoopdb/migrate.ts orders it): supabase, then 96bulls. */
async function hoopDatabase(): Promise<PGlite> {
  const db = await database();
  for (const file of readdirSync(join(root, "src/hoopdb/migrations"))
    .filter((f) => f.endsWith(".sql"))
    .sort()) {
    await db.exec(read(`src/hoopdb/migrations/${file}`));
  }
  return db;
}

const KEY = {
  orgId: "org-a",
  workspaceId: "ws-a",
  harness: "codex",
  sessionId: "s-1",
};
const AT = new Date("2026-10-30T12:00:00Z");

async function insertRow(
  db: PGlite,
  org = "org-a",
  workspace = "ws-a",
  session = "s-1",
) {
  await db.query(
    `INSERT INTO sessions.session_directory
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
        title, agent_name, agent_name_normalized, project_id, search_text)
     VALUES ($1, $2, 'codex', $3, $4, $4, 'Fix flaky queue test', 'Helm', 'helm', 'proj-1',
             sessions.session_directory_search_text('Fix flaky queue test', NULL, 'Helm', 'proj-1',
               NULL, NULL, NULL, NULL))`,
    [org, workspace, session, AT.toISOString()],
  );
}

async function summarize(
  db: PGlite,
  key: typeof KEY,
  summary: string,
  generatedAt = AT,
) {
  return writeShortSummary(db, {
    ...key,
    summary,
    source: "bob-costas/v1",
    generatedAt,
  });
}

type Row = Record<string, unknown>;
async function row(db: PGlite, session = "s-1"): Promise<Row | undefined> {
  return (
    await db.query<Row>(
      "SELECT * FROM sessions.session_directory WHERE session_id = $1",
      [session],
    )
  ).rows[0];
}

const claims = (
  org: string,
  workspaces: unknown[],
  role: string = WORKER,
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

/** Calls the RPC exactly as PostgREST would: SET ROLE to the Worker, claims GUC set. */
async function search(
  db: PGlite,
  claimsJson: string | null,
  workspace: string,
  query: string,
  onNotice?: (message: string) => void,
): Promise<Row[]> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    claimsJson ?? "",
  ]);
  await db.exec(`SET ROLE ${WORKER}`);
  try {
    const result = await db.query<Row>(
      "SELECT * FROM sessions.search_session_directory($1, $2)",
      [workspace, query],
      onNotice
        ? ({
            onNotice: (notice: { message: string }) => onNotice(notice.message),
          } as never)
        : undefined,
    );
    return result.rows;
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

describe("search token pins", () => {
  const now = () => Math.floor(Date.now() / 1000);
  const refused = async (db: PGlite, claimsJson: string, workspace: string | null = "ws-a") => {
    await expect(search(db, claimsJson, workspace as string, "flaky")).rejects.toMatchObject({ code: "42501" });
  };

  test("accepts a token from the pinned issuer and audience", async () => {
    const db = await database();
    await insertRow(db);
    expect(await search(db, claims("org-a", ["ws-a"]), "ws-a", "flaky")).toHaveLength(1);
  });

  test("refuses another issuer or audience, including an array audience", async () => {
    const db = await database();
    await insertRow(db);
    await refused(db, claims("org-a", ["ws-a"], WORKER, { iss: "https://relayauth.dev" }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { iss: undefined }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { aud: "relayhistory" }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { aud: undefined }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { aud: [SESSION_DIRECTORY_SEARCH_RPC.audience] }));
  });

  test("refuses a missing, malformed, long or zero lifetime, and a far-future iat", async () => {
    const db = await database();
    await insertRow(db);
    const t = now();
    await refused(db, claims("org-a", ["ws-a"], WORKER, { exp: undefined }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { iat: undefined }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { exp: String(t + 60) }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { iat: t, exp: t + 61 }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { iat: t, exp: t }));
    await refused(db, claims("org-a", ["ws-a"], WORKER, { iat: t + 3600, exp: t + 3660 }));
  });

  test("refuses everything when no policy row exists, and the migration seeds none", async () => {
    const db = await database();
    await insertRow(db);
    await db.query("DELETE FROM sessions.session_directory_search_token_policy");
    await refused(db, claims("org-a", ["ws-a"]));
    expect(UP).not.toMatch(/INSERT INTO sessions\.session_directory_search_token_policy/);
  });

  test("the policy has one row and no role but the owner can read it", async () => {
    const db = await database();
    await expect(db.query(
      "INSERT INTO sessions.session_directory_search_token_policy (singleton, issuer, audience) VALUES (false, 'https://x', 'y')",
    )).rejects.toThrow();
    for (const role of ["anon", "authenticated", "service_role", WORKER]) {
      const allowed = await db.query<{ ok: boolean }>(
        "SELECT has_table_privilege($1, 'sessions.session_directory_search_token_policy', 'SELECT') AS ok",
        [role],
      );
      expect(allowed.rows[0]!.ok).toBe(false);
    }
  });

  const ids = (count: number) => Array.from({ length: count }, (_, index) => `ws-${index}`);

  test("accepts 1 to 64 unique workspaces, for one workspace and for the claimed set", async () => {
    const db = await database();
    await insertRow(db, "org-a", "ws-0", "s-0");
    await insertRow(db, "org-a", "ws-63", "s-63");
    await insertRow(db, "org-a", "ws-64", "s-64");
    expect(await search(db, claims("org-a", ids(64)), "ws-63", "flaky")).toHaveLength(1);
    const scoped = await search(db, claims("org-a", ids(64)), null as unknown as string, "flaky");
    expect(scoped.map((hit) => hit.workspace_id).sort()).toEqual(["ws-0", "ws-63"]);
    expect(await search(db, claims("org-a", ["x".repeat(128)]), "x".repeat(128), "flaky")).toEqual([]);
  });

  test("refuses a bad workspace set, for one workspace and for the claimed set", async () => {
    const db = await database();
    await insertRow(db);
    const badSets: unknown[][] = [
      [],
      ids(65),
      ["ws-a", "ws-a"],
      ["ws-a", 7],
      ["ws-a", null],
      ["ws-a", { id: "ws-b" }],
      ["ws-a", ["ws-b"]],
      ["ws-a", ""],
      ["ws-a", " ws-b"],
      ["ws-a", "ws\nb"],
      ["ws-a", "x".repeat(129)],
    ];
    for (const set of badSets) {
      await refused(db, claims("org-a", set));
      await refused(db, claims("org-a", set), null);
    }
  });

  test("the claimed set never reaches an unclaimed workspace or another org", async () => {
    const db = await database();
    await insertRow(db, "org-a", "ws-a", "s-a");
    await insertRow(db, "org-a", "ws-b", "s-b");
    await insertRow(db, "org-a", "ws-c", "s-c");
    await insertRow(db, "org-b", "ws-a", "s-other-org");
    const claimed = claims("org-a", ["ws-a", "ws-c"]);
    const hits = await search(db, claimed, null as unknown as string, "flaky");
    expect(hits.map((hit) => hit.session_id).sort()).toEqual(["s-a", "s-c"]);
    await refused(db, claimed, "ws-b");
    await refused(db, claimed, "");
    await refused(db, claimed, "WS-A");
  });
});

describe("short_summary writer", () => {
  test("stores only scrubbed text and records the scrub ruleset and version", async () => {
    const db = await database();
    await insertRow(db);
    const summary = `Rotated ${FAKE_RELAY_TOKEN} and edited ${FAKE_HOME}/queue.ts to fix retries.`;
    const result = await summarize(db, KEY, summary);
    expect(result).toEqual({
      written: true,
      scrubVersion: SCRUB_RULESET_VERSION,
    });
    const stored = await row(db);
    expect(stored?.short_summary).toBe(
      "Rotated [REDACTED] and edited ~/proj/queue.ts to fix retries.",
    );
    expect(String(stored?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(String(stored?.search_text)).not.toContain(FAKE_RELAY_TOKEN);
    expect(stored?.short_summary_scrub_ruleset).toBe(SCRUB_RULESET);
    expect(stored?.short_summary_scrub_version).toBe(SCRUB_RULESET_VERSION);
    expect(stored?.short_summary_source).toBe("bob-costas/v1");
    expect((stored?.short_summary_generated_at as Date).toISOString()).toBe(
      AT.toISOString(),
    );
  });

  test("refuses over-length text in the writer and in the database", async () => {
    const db = await database();
    await insertRow(db);
    const exact = "a".repeat(SHORT_SUMMARY_MAX_CHARS);
    expect(scrubShortSummary(exact).text).toBe(exact);
    // Counted in code points, as PostgreSQL char_length does: 1000 emoji fit.
    expect(
      scrubShortSummary("\u{1F402}".repeat(SHORT_SUMMARY_MAX_CHARS)).text,
    ).toHaveLength(2000);

    const over = "a".repeat(SHORT_SUMMARY_MAX_CHARS + 1);
    expect(() => scrubShortSummary(over)).toThrow(
      new ShortSummaryRejected("over_length"),
    );
    await expect(summarize(db, KEY, over)).rejects.toThrow(
      "short_summary rejected: over_length",
    );
    expect((await row(db))?.short_summary).toBeNull();

    // A writer that bypasses the TypeScript path still hits the CHECK.
    await expect(
      db.query(
        `UPDATE sessions.session_directory SET short_summary = $1, short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 1, short_summary_source = 'x', short_summary_generated_at = now()`,
        [over],
      ),
    ).rejects.toThrow(/session_directory_short_summary_length/);
  });

  test("refuses when the scrub fails or is not stable, and writes nothing", async () => {
    const db = await database();
    await insertRow(db);
    const input = {
      ...KEY,
      summary: "a normal summary",
      source: "bob-costas/v1",
      generatedAt: AT,
    };
    await expect(
      writeShortSummary(db, input, () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("short_summary rejected: scrub_failed");
    await expect(
      writeShortSummary(db, input, (value) => `${value}!`),
    ).rejects.toThrow("short_summary rejected: scrub_unstable");
    await expect(
      writeShortSummary(db, input, () => undefined as unknown as string),
    ).rejects.toThrow("short_summary rejected: scrub_failed");
    await expect(
      writeShortSummary(db, { ...input, summary: "   " }),
    ).rejects.toThrow("short_summary rejected: empty");
    await expect(
      writeShortSummary(db, { ...input, source: "Not A Source" }),
    ).rejects.toThrow("short_summary rejected: invalid_source");
    expect((await row(db))?.short_summary).toBeNull();
  });

  test("the database rejects summary text that lacks scrub provenance labels", async () => {
    const db = await database();
    await insertRow(db);
    await expect(
      db.query(
        "UPDATE sessions.session_directory SET short_summary = 'unscrubbed text'",
      ),
    ).rejects.toThrow(/session_directory_short_summary_provenance/);
    await expect(
      db.query(
        `UPDATE sessions.session_directory SET short_summary = 'x', short_summary_scrub_ruleset = 'something-else',
         short_summary_scrub_version = 1, short_summary_source = 'x', short_summary_generated_at = now()`,
      ),
    ).rejects.toThrow(/session_directory_short_summary_scrub_ruleset/);
    await expect(
      db.query(
        `UPDATE sessions.session_directory SET short_summary = 'x', short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 0, short_summary_source = 'x', short_summary_generated_at = now()`,
      ),
    ).rejects.toThrow(/session_directory_short_summary_scrub_version/);
  });

  test("an older summary never replaces a newer one", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "newer");
    const older = await summarize(
      db,
      KEY,
      "older",
      new Date(AT.getTime() - 1000),
    );
    expect(older.written).toBe(false);
    expect((await row(db))?.short_summary).toBe("newer");
  });

  test("a rejected newer summary clears the older one; a rejected older one leaves a newer one", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "Old heron migration notes");
    const a = claims("org-a", ["ws-a"]);
    expect(await search(db, a, "ws-a", "heron")).toHaveLength(1);
    const later = new Date(AT.getTime() + 1000);
    await expect(
      summarize(db, KEY, "y".repeat(SHORT_SUMMARY_MAX_CHARS + 1), later),
    ).rejects.toThrow("short_summary rejected: over_length");
    const cleared = await row(db);
    expect(cleared?.short_summary).toBeNull();
    expect(cleared?.short_summary_source).toBeNull();
    expect(cleared?.search_text).toBe("Fix flaky queue test Helm proj-1");
    expect(await search(db, a, "ws-a", "heron")).toEqual([]);

    // The fence: a refused summary older than the stored one does not clear it.
    await summarize(db, KEY, "Fresh egret notes", later);
    await expect(
      writeShortSummary(db, {
        ...KEY,
        summary: "   ",
        source: "bob-costas/v1",
        generatedAt: AT,
      }),
    ).rejects.toThrow("short_summary rejected: empty");
    expect((await row(db))?.short_summary).toBe("Fresh egret notes");
    expect(await search(db, a, "ws-a", "egret")).toHaveLength(1);
  });
});

describe("short_summary in search_text", () => {
  test("is part of the declared boundary and of search_text, and is found through the RPC", async () => {
    expect(SESSION_DIRECTORY_SEARCH_BOUNDARY.fields).toContain("short_summary");
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "Migrated the zebra-crossing scheduler");
    expect((await row(db))?.search_text).toBe(
      "Fix flaky queue test Helm proj-1 Migrated the zebra-crossing scheduler",
    );
    const hits = await search(
      db,
      claims("org-a", ["ws-a"]),
      "ws-a",
      "ZEBRA-cross",
    );
    expect(hits.map((hit) => hit.session_id)).toEqual(["s-1"]);
    expect(Object.keys(hits[0]!)).toEqual([
      ...SESSION_DIRECTORY_SEARCH_RPC.returns,
    ]);
    expect(hits[0]!.short_summary).toBe(
      "Migrated the zebra-crossing scheduler",
    );

    expect(await clearShortSummary(db, KEY)).toBe(true);
    expect((await row(db))?.search_text).toBe(
      "Fix flaky queue test Helm proj-1",
    );
    expect(
      await search(db, claims("org-a", ["ws-a"]), "ws-a", "zebra"),
    ).toEqual([]);
  });
});

describe("search RPC authorization", () => {
  test("claims for tenant A never return tenant B; missing or forged claims are refused", async () => {
    const db = await database();
    await insertRow(db, "org-a", "ws-a", "s-a");
    await insertRow(db, "org-b", "ws-b", "s-b");
    await insertRow(db, "org-a", "ws-hidden", "s-hidden");
    for (const [org, workspace, session] of [
      ["org-a", "ws-a", "s-a"],
      ["org-b", "ws-b", "s-b"],
      ["org-a", "ws-hidden", "s-hidden"],
    ]) {
      await summarize(
        db,
        {
          orgId: org!,
          workspaceId: workspace!,
          harness: "codex",
          sessionId: session!,
        },
        `walrus summary for ${session}`,
      );
    }
    const a = claims("org-a", ["ws-a"]);
    expect(
      (await search(db, a, "ws-a", "walrus")).map((hit) => hit.session_id),
    ).toEqual(["s-a"]);

    const refused = async (claimsJson: string | null, workspace: string) =>
      expect(search(db, claimsJson, workspace, "walrus")).rejects.toThrow(
        "tenant claims missing or invalid",
      );
    // Tenant A naming B's workspace, or its own org's unlisted workspace.
    await refused(a, "ws-b");
    await refused(a, "ws-hidden");
    // B's workspace id under A's org claim still cannot reach B's rows.
    await refused(claims("org-a", ["ws-b"]), "ws-x");
    expect(
      await search(db, claims("org-a", ["ws-b"]), "ws-b", "walrus"),
    ).toEqual([]);
    // Missing, malformed, wrong-role, and structurally forged claims.
    await refused(null, "ws-a");
    await refused("{not json", "ws-a");
    await refused("[]", "ws-a");
    await refused(claims("org-a", ["ws-a"], "authenticated"), "ws-a");
    await refused(claims("org-a", ["ws-a"], "service_role"), "ws-a");
    await refused(claims("", ["ws-a"]), "ws-a");
    await refused(
      JSON.stringify({
        role: WORKER,
        app_metadata: { org_id: "org-a", workspace_ids: "ws-a" },
      }),
      "ws-a",
    );
    await refused(
      JSON.stringify({
        role: WORKER,
        app_metadata: { org_id: ["org-a"], workspace_ids: ["ws-a"] },
      }),
      "ws-a",
    );
    await refused(
      JSON.stringify({
        role: WORKER,
        org_id: "org-a",
        workspace_ids: ["ws-a"],
      }),
      "ws-a",
    );
  });

  test("the Worker role has no table access, no login, no BYPASSRLS, and a statement timeout", async () => {
    const db = await database();
    const role = (
      await db.query<Row>(
        "SELECT rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolconfig FROM pg_roles WHERE rolname = $1",
        [WORKER],
      )
    ).rows[0];
    expect(role).toEqual({
      rolcanlogin: false,
      rolinherit: false,
      rolbypassrls: false,
      rolsuper: false,
      rolconfig: ["statement_timeout=2s"],
    });
    const privileged = await db.query<{ table_name: string }>(
      `SELECT c.relname AS table_name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm')
          AND (has_table_privilege($1, c.oid, 'SELECT') OR has_table_privilege($1, c.oid, 'INSERT')
            OR has_table_privilege($1, c.oid, 'UPDATE') OR has_table_privilege($1, c.oid, 'DELETE')
            OR has_any_column_privilege($1, c.oid, 'SELECT'))`,
      [WORKER],
    );
    expect(privileged.rows).toEqual([]);
    await db.exec(`SET ROLE ${WORKER}`);
    try {
      await expect(
        db.query("SELECT search_text FROM sessions.session_directory"),
      ).rejects.toThrow(/permission denied/);
      await expect(
        db.query("SELECT * FROM sessions.session_index"),
      ).rejects.toThrow(/permission denied/);
    } finally {
      await db.exec("RESET ROLE");
    }
  });

  test("the Worker role is NOLOGIN, which the RPC's session_user fallback relies on", async () => {
    // The role guard accepts session_user = the Worker when no SET ROLE is active. That
    // is only safe while nobody can log in as the Worker directly; PostgREST must reach
    // it through authenticator's SET ROLE. Changing this to LOGIN needs a new review.
    const db = await database();
    const role = await db.query<{ rolcanlogin: boolean }>(
      "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1",
      [WORKER],
    );
    expect(role.rows).toEqual([{ rolcanlogin: false }]);
  });

  test("no role in pg_roles except the Worker and the owner can execute the search RPC", async () => {
    // Default privileges and a pre-existing runtime role, present before the migration runs.
    const db = await database(`CREATE ROLE relayhistory_app;
      ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO relayhistory_app;
      ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;`);
    const executors = await db.query<{ rolname: string }>(
      `SELECT r.rolname FROM pg_roles AS r, pg_proc AS p
        WHERE p.oid = 'sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)'::regprocedure
          AND r.rolname <> $1 AND r.oid <> p.proowner
          AND has_function_privilege(r.oid, p.oid, 'EXECUTE')
        ORDER BY r.rolname`,
      [WORKER],
    );
    expect(executors.rows).toEqual([]);
    const checked = await db.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM pg_roles",
    );
    expect(checked.rows[0]!.n).toBeGreaterThan(5);
  });

  test("a runner's later GRANT EXECUTE ON ALL FUNCTIONS is refused at call time by the role guard", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "osprey summary");
    // As relayhistory-verify-schema.test.ts's RUNTIME_ROLE grants it, after the migrations.
    await db.exec(`CREATE ROLE relayhistory_app;
      GRANT USAGE ON SCHEMA sessions TO relayhistory_app;
      GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO relayhistory_app;`);
    const granted = await db.query<{ ok: boolean }>(
      "SELECT has_function_privilege('relayhistory_app', 'sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)', 'EXECUTE') AS ok",
    );
    expect(granted.rows[0]?.ok).toBe(true);
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
      claims("org-a", ["ws-a"]),
    ]);
    await db.exec("SET ROLE relayhistory_app");
    try {
      await expect(
        db.query(
          "SELECT * FROM sessions.search_session_directory('ws-a', 'osprey')",
        ),
      ).rejects.toThrow("caller is not the search worker role");
    } finally {
      await db.exec("RESET ROLE");
    }
    expect(
      await search(db, claims("org-a", ["ws-a"]), "ws-a", "osprey"),
    ).toHaveLength(1);
  });

  test("only the Worker role may execute the RPC, and end users cannot filter search_text directly", async () => {
    const db = await database();
    const fn =
      "sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)";
    for (const grantee of ["anon", "authenticated", "service_role"]) {
      const allowed = await db.query<{ ok: boolean }>(
        "SELECT has_function_privilege($1, $2, 'EXECUTE') AS ok",
        [grantee, fn],
      );
      expect(allowed.rows[0]?.ok, grantee).toBe(false);
    }
    const publicExec = await db.query(
      `SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
        WHERE p.oid = $1::regprocedure AND a.grantee = 0 AND a.privilege_type = 'EXECUTE'`,
      [fn],
    );
    expect(publicExec.rows).toEqual([]);
    await insertRow(db);
    await db.exec(`SET ROLE authenticated;
      SELECT set_config('request.jwt.claims', '{"app_metadata":{"org_id":"org-a","workspace_ids":["ws-a"]}}', false);`);
    try {
      await expect(
        db.query(
          "SELECT * FROM sessions.search_session_directory('ws-a', 'flaky')",
        ),
      ).rejects.toThrow(/permission denied/);
      await expect(
        db.query(
          "SELECT session_id FROM sessions.session_directory WHERE search_text ILIKE '%flaky%'",
        ),
      ).rejects.toThrow(/permission denied/);
      await expect(
        db.query("SELECT short_summary FROM sessions.session_directory"),
      ).rejects.toThrow(/permission denied/);
      // RLS-scoped listing columns stay readable.
      expect(
        (await db.query("SELECT session_id FROM sessions.session_directory"))
          .rows,
      ).toEqual([{ session_id: "s-1" }]);
    } finally {
      await db.exec("RESET ROLE");
    }
  });
});

type Plan = { [key: string]: unknown; Plans?: Plan[] };
const walk = (plan: Plan): Plan[] => [
  plan,
  ...(plan.Plans ?? []).flatMap(walk),
];

type CapturedPlan = {
  queryText: string;
  plan: Plan;
  nodes: Plan[];
  durationMs: number;
};

/** auto_explain output (ANALYZE, BUFFERS, FORMAT JSON) of the RPC's inner statement. */
async function explainRpc(
  db: PGlite,
  claimsJson: string,
  workspace: string,
  query: string,
) {
  await db.exec(`LOAD 'auto_explain';
    SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on;
    SET auto_explain.log_buffers = on; SET auto_explain.log_timing = on;
    SET auto_explain.log_format = 'json'; SET auto_explain.log_nested_statements = on;
    SET client_min_messages = log;`);
  const messages: string[] = [];
  try {
    const rows = await search(db, claimsJson, workspace, query, (message) =>
      messages.push(message),
    );
    const captured: CapturedPlan[] = messages
      .filter((message) => message.includes('"Query Text"'))
      .map((message) => {
        const duration = Number(/duration: ([\d.]+) ms/.exec(message)?.[1]);
        const json = JSON.parse(message.slice(message.indexOf("{"))) as {
          "Query Text": string;
          Plan: Plan;
        };
        return {
          queryText: json["Query Text"],
          plan: json.Plan,
          nodes: walk(json.Plan),
          durationMs: duration,
        };
      });
    const squash = (sql: string) => sql.replace(/\s+/g, " ").trim();
    const inner = captured.find(
      (entry) =>
        squash(entry.queryText) === squash(SESSION_DIRECTORY_SEARCH_SQL),
    );
    if (!inner)
      throw new Error(
        "auto_explain did not capture the RPC's search statement",
      );
    return { rows, inner };
  } finally {
    await db.exec(
      "SET auto_explain.log_min_duration = -1; SET client_min_messages = notice;",
    );
  }
}

/** Every auto_explain plan logged while the Worker calls the RPC, whether or not it throws. */
async function capturedStatements(
  db: PGlite,
  claimsJson: string,
  workspace: string,
  query: string,
) {
  await db.exec(`LOAD 'auto_explain';
    SET auto_explain.log_min_duration = 0; SET auto_explain.log_nested_statements = on;
    SET auto_explain.log_format = 'json'; SET client_min_messages = log;`);
  const messages: string[] = [];
  let error: unknown;
  try {
    await search(db, claimsJson, workspace, query, (message) =>
      messages.push(message),
    );
  } catch (caught) {
    error = caught;
  } finally {
    await db.exec(
      "SET auto_explain.log_min_duration = -1; SET client_min_messages = notice;",
    );
  }
  const texts = messages
    .filter((m) => m.includes('"Query Text"'))
    .map((m) =>
      (JSON.parse(m.slice(m.indexOf("{"))) as { "Query Text": string })[
        "Query Text"
      ]
        .replace(/\s+/g, " ")
        .trim(),
    );
  return {
    error,
    ranSearch: texts.includes(
      SESSION_DIRECTORY_SEARCH_SQL.replace(/\s+/g, " ").trim(),
    ),
  };
}

describe("migration role collision", () => {
  /** Every migration before G6, so a pre-existing role can be prepared against real tables. */
  async function beforeG6(): Promise<PGlite> {
    const db = new PGlite({ extensions: { auto_explain, btree_gin, pg_trgm } });
    open.push(db);
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
      // Only the migrations before G6: later ones (the expand phase) need G6's role.
      if (file < MIGRATION) await db.exec(read(`supabase/migrations/${file}`));
    }
    return db;
  }
  const applied = async (db: PGlite) =>
    (
      await db.query(
        `SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'sessions' AND table_name = 'session_directory' AND column_name = 'short_summary'`,
      )
    ).rows.length > 0;

  const BOOTSTRAP = read(
    "supabase/bootstrap/session_directory_search_worker.sql",
  );
  const TOKEN_BOOTSTRAP = read(
    "supabase/bootstrap/session_token_verifier.sql",
  );
  /** The role exactly as the bootstrap makes it. */
  const SPEC = `CREATE ROLE ${WORKER} NOLOGIN NOINHERIT; ALTER ROLE ${WORKER} SET statement_timeout = '2s';`;
  const failure = async (run: Promise<unknown>) =>
    (await run.then(
      () => null,
      (caught: unknown) => caught,
    )) as { code?: string; message?: string } | null;
  const nothingApplied = async (db: PGlite) => {
    expect(await applied(db)).toBe(false);
    expect(
      (
        await db.query(
          "SELECT 1 FROM pg_proc WHERE proname = 'search_session_directory'",
        )
      ).rows,
    ).toEqual([]);
    // The extension is the migration's first DDL after the guard.
    expect(
      (await db.query("SELECT 1 FROM pg_extension WHERE extname = 'btree_gin'"))
        .rows,
    ).toEqual([]);
  };
  const settings = async (db: PGlite) =>
    (
      await db.query<Row>(
        `SELECT s.setdatabase, s.setconfig FROM pg_db_role_setting AS s
          JOIN pg_roles AS r ON r.oid = s.setrole WHERE r.rolname = $1`,
        [WORKER],
      )
    ).rows;
  const memberships = async (db: PGlite) =>
    (
      await db.query<Row>(
        `SELECT g.rolname AS role, u.rolname AS member, m.admin_option
           FROM pg_auth_members AS m
           JOIN pg_roles AS g ON g.oid = m.roleid
           JOIN pg_roles AS u ON u.oid = m.member
          WHERE g.rolname = $1 OR u.rolname = $1`,
        [WORKER],
      )
    ).rows;

  test("an absent role fails closed with 42704 before any DDL; the migration never creates it", async () => {
    const db = await beforeG6();
    const error = await failure(db.exec(UP));
    expect(error?.code).toBe("42704");
    expect(error?.message).toContain(
      "role session_directory_search_worker does not exist; this migration does not create it",
    );
    await nothingApplied(db);
    expect(
      (await db.query("SELECT 1 FROM pg_roles WHERE rolname = $1", [WORKER]))
        .rows,
    ).toEqual([]);
  });

  test("the bootstrap creates the role exactly to spec and is idempotent; the migration then applies and leaves no membership or ADMIN OPTION", async () => {
    const db = await beforeG6();
    await db.exec(BOOTSTRAP);
    await db.exec(TOKEN_BOOTSTRAP);
    await db.exec(BOOTSTRAP);
    await db.exec(TOKEN_BOOTSTRAP);
    const spec = async () =>
      (
        await db.query<Row>(
          `SELECT rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolcreaterole, rolcreatedb, rolreplication
             FROM pg_roles WHERE rolname = $1`,
          [WORKER],
        )
      ).rows;
    const expected = [
      {
        rolcanlogin: false,
        rolinherit: false,
        rolbypassrls: false,
        rolsuper: false,
        rolcreaterole: false,
        rolcreatedb: false,
        rolreplication: false,
      },
    ];
    const timeout = [{ setdatabase: 0, setconfig: ["statement_timeout=2s"] }];
    expect(await spec()).toEqual(expected);
    expect(await settings(db)).toEqual(timeout);
    expect(await memberships(db)).toEqual([]);

    await db.exec(UP);
    await db.exec(EXPAND_UP);
    expect(await applied(db)).toBe(true);
    // Neither the migration owner nor anyone else holds membership or ADMIN OPTION.
    expect(await memberships(db)).toEqual([]);
    expect(await spec()).toEqual(expected);
    expect(await settings(db)).toEqual(timeout);
    // Bootstrap again after the migration: still a no-op that verifies.
    await db.exec(BOOTSTRAP);
    await db.exec(TOKEN_BOOTSTRAP);
    expect(await memberships(db)).toEqual([]);
  });

  test("the bootstrap refuses an existing role that differs (55000) and changes nothing", async () => {
    const db = await beforeG6();
    await db.exec(
      `CREATE ROLE ${WORKER} NOLOGIN NOINHERIT; ALTER ROLE ${WORKER} SET statement_timeout = '2000ms';`,
    );
    const error = await failure(db.exec(BOOTSTRAP));
    expect(error?.code).toBe("55000");
    expect(error?.message).toContain(
      "role settings must be exactly statement_timeout=2s (found: statement_timeout=2000ms)",
    );
    expect(await settings(db)).toEqual([
      { setdatabase: 0, setconfig: ["statement_timeout=2000ms"] },
    ]);
  });

  test("the bootstrap refuses a non-superuser creator on PostgreSQL 16+ (42501) and creates nothing", async () => {
    const db = await beforeG6();
    const version = (
      await db.query<{ v: number }>(
        "SELECT current_setting('server_version_num')::int AS v",
      )
    ).rows[0]!.v;
    await db.exec(
      "CREATE ROLE g6_bootstrapper NOLOGIN CREATEROLE; SET ROLE g6_bootstrapper",
    );
    const error = await failure(db.exec(BOOTSTRAP));
    await db.exec("RESET ROLE");
    if (version >= 160000) {
      expect(error?.code).toBe("42501");
      expect(error?.message).toContain("g6_bootstrapper may not create it");
      expect(
        (await db.query("SELECT 1 FROM pg_roles WHERE rolname = $1", [WORKER]))
          .rows,
      ).toEqual([]);
    } else {
      expect(error).toBeNull();
      expect(await memberships(db)).toEqual([]);
    }
  });

  test("a bootstrapped role with a NOINHERIT authenticator is reused unaltered", async () => {
    const db = await beforeG6();
    await db.exec(`${SPEC}
      CREATE ROLE authenticator NOLOGIN NOINHERIT; GRANT ${WORKER} TO authenticator;`);
    await db.exec(UP);
    expect(await applied(db)).toBe(true);
    expect(await settings(db)).toEqual([
      { setdatabase: 0, setconfig: ["statement_timeout=2s"] },
    ]);
    expect(await memberships(db)).toEqual([
      {
        role: WORKER,
        member: "authenticator",
        admin_option: false,
      },
    ]);
  });

  const SETTINGS = "role settings must be exactly statement_timeout=2s";
  const UNSAFE: Array<[string, string, string]> = [
    ["LOGIN", `${SPEC} ALTER ROLE ${WORKER} LOGIN`, "LOGIN"],
    ["INHERIT", `${SPEC} ALTER ROLE ${WORKER} INHERIT`, "INHERIT"],
    ["BYPASSRLS", `${SPEC} ALTER ROLE ${WORKER} BYPASSRLS`, "BYPASSRLS"],
    ["SUPERUSER", `${SPEC} ALTER ROLE ${WORKER} SUPERUSER`, "SUPERUSER"],
    ["CREATEROLE", `${SPEC} ALTER ROLE ${WORKER} CREATEROLE`, "CREATEROLE"],
    ["CREATEDB", `${SPEC} ALTER ROLE ${WORKER} CREATEDB`, "CREATEDB"],
    ["REPLICATION", `${SPEC} ALTER ROLE ${WORKER} REPLICATION`, "REPLICATION"],
    [
      "no statement_timeout",
      `CREATE ROLE ${WORKER} NOLOGIN NOINHERIT`,
      `${SETTINGS} (found: none)`,
    ],
    [
      "an equivalent spelling, 2000ms",
      `CREATE ROLE ${WORKER} NOLOGIN NOINHERIT; ALTER ROLE ${WORKER} SET statement_timeout = '2000ms'`,
      `${SETTINGS} (found: statement_timeout=2000ms)`,
    ],
    [
      "an equivalent spelling, bare 2000",
      `CREATE ROLE ${WORKER} NOLOGIN NOINHERIT; ALTER ROLE ${WORKER} SET statement_timeout = 2000`,
      `${SETTINGS} (found: statement_timeout=2000)`,
    ],
    [
      "a different timeout",
      `CREATE ROLE ${WORKER} NOLOGIN NOINHERIT; ALTER ROLE ${WORKER} SET statement_timeout = '5s'`,
      `${SETTINGS} (found: statement_timeout=5s)`,
    ],
    [
      "an extra role setting",
      `${SPEC} ALTER ROLE ${WORKER} SET search_path = public`,
      `${SETTINGS} (found: statement_timeout=2s, search_path=public)`,
    ],
    [
      "a database-specific setting",
      `${SPEC}
      DO $g$ BEGIN EXECUTE format('ALTER ROLE ${WORKER} IN DATABASE %I SET work_mem = ''64kB''', current_database()); END $g$`,
      SETTINGS,
    ],
    [
      "member of another role",
      `${SPEC} GRANT service_role TO ${WORKER}`,
      "member of service_role",
    ],
    [
      "granted to an inheriting role",
      `${SPEC} CREATE ROLE app_reader NOLOGIN INHERIT;
      GRANT ${WORKER} TO app_reader`,
      "granted to app_reader",
    ],
    [
      "granted to an INHERIT authenticator",
      `${SPEC} CREATE ROLE authenticator NOLOGIN INHERIT;
      GRANT ${WORKER} TO authenticator`,
      "granted to authenticator",
    ],
    [
      "granted to authenticator WITH ADMIN OPTION",
      `${SPEC} CREATE ROLE authenticator NOLOGIN NOINHERIT;
      GRANT ${WORKER} TO authenticator WITH ADMIN OPTION`,
      "granted to authenticator",
    ],
    [
      "ADMIN OPTION held by a migration owner (the PG16+ CREATEROLE creator grant)",
      `${SPEC} CREATE ROLE hoop NOLOGIN NOINHERIT;
      GRANT ${WORKER} TO hoop WITH ADMIN OPTION, INHERIT FALSE, SET FALSE`,
      "granted to hoop",
    ],
    [
      "an inheriting grant to a NOINHERIT authenticator (PG16+)",
      `${SPEC} CREATE ROLE authenticator NOLOGIN NOINHERIT;
      GRANT ${WORKER} TO authenticator WITH INHERIT TRUE`,
      "granted WITH INHERIT to authenticator",
    ],
    [
      "owns an object",
      `${SPEC} CREATE TABLE public.g6_owned (x int);
      ALTER TABLE public.g6_owned OWNER TO ${WORKER}`,
      "owns objects",
    ],
    [
      "table privilege",
      `${SPEC} GRANT SELECT ON sessions.session_index TO ${WORKER}`,
      "table, view or sequence privileges",
    ],
    [
      "column privilege",
      `${SPEC} GRANT SELECT (org_id) ON sessions.session_directory TO ${WORKER}`,
      "column privileges",
    ],
    [
      "function privilege",
      `${SPEC} GRANT EXECUTE ON FUNCTION sessions.jwt_org_id() TO ${WORKER}`,
      "function privileges",
    ],
    [
      "schema privilege",
      `${SPEC} GRANT CREATE ON SCHEMA sessions TO ${WORKER}`,
      "schema privileges",
    ],
    [
      "database privilege",
      `${SPEC}
      DO $g$ BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO ${WORKER}', current_database()); END $g$`,
      "database privileges",
    ],
    [
      "default privileges",
      `${SPEC}
      ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO ${WORKER}`,
      "default privileges",
    ],
  ];
  for (const [name, setup, problem] of UNSAFE) {
    test(`fails closed with 55000, before any DDL, when the pre-existing role has: ${name}`, async () => {
      const db = await beforeG6();
      await db.exec(setup);
      const error = await failure(db.exec(UP));
      expect(error?.code).toBe("55000");
      expect(error?.message).toContain(
        "role session_directory_search_worker does not match the bootstrap spec",
      );
      expect(error?.message).toContain(problem);
      await nothingApplied(db);
    });
  }
});

describe("search RPC query bounds", () => {
  // Inputs that look like 3+ characters to btrim/char_length but carry fewer than
  // three meaningful ones.
  const INVALID: Array<[string, string]> = [
    ["empty", ""],
    ["one char", "a"],
    ["two chars", "ab"],
    ["ASCII-padded two", "  ab  "],
    ["NBSP between two", "a\u00a0b"],
    ["NBSP padding", "\u00a0\u00a0ab\u00a0"],
    ["en/em/thin spaces", "\u2002a\u2003b\u2009\u200a"],
    ["ideographic space", "\u3000ab\u3000"],
    ["ZWSP", "a\u200bb\u200b"],
    ["ZWNJ/ZWJ", "\u200ca\u200db\u200d"],
    ["BOM", "\ufeffab\ufeff"],
    ["tab/newline", "\ta\nb\r"],
    ["soft hyphen", "a\u00adb"],
    ["combining-only", "\u0301\u0301\u0301\u0301"],
    ["two bases plus marks", "a\u0301\u0302b\u0303\u0304"],
    ["enclosing marks", "a\u20dd\u20ddb\u20dd"],
    ["tag characters", "ab\u{e0041}\u{e0042}"],
    ["over 200", "x".repeat(201)],
    ["over 200 after collapsing", `${"x ".repeat(100)}y`],
  ];

  test("TypeScript and SQL refuse the same inputs, with 22023, and the inner query never runs", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "Caf\u00e9 menu cut 50%_off for the launch");
    const a = claims("org-a", ["ws-a"]);
    for (const [name, query] of INVALID) {
      expect(normalizeSearchQuery(query).ok, name).toBe(false);
      expect(searchRpcRequest({ workspaceId: "ws-a", q: query }).ok, name).toBe(
        false,
      );
      const sql = await db.query<{ q: string | null }>(
        "SELECT sessions.session_directory_search_query($1) AS q",
        [query],
      );
      expect(sql.rows[0]?.q, name).toBeNull();
      const { error, ranSearch } = await capturedStatements(
        db,
        a,
        "ws-a",
        query,
      );
      expect((error as { code?: string })?.code, name).toBe("22023");
      expect(ranSearch, name).toBe(false);
    }
    // Control: a valid query is captured, so the capture itself works.
    expect((await capturedStatements(db, a, "ws-a", "menu")).ranSearch).toBe(
      true,
    );
  });

  test("valid queries normalize identically in TypeScript and SQL and match literally", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "Caf\u00e9 menu cut 50%_off for the launch");
    const a = claims("org-a", ["ws-a"]);
    for (const query of [
      "abc",
      "  cafe\u0301 ",
      "menu\u00a0cut",
      "\u200bmenu\u3000cut\ufeff",
      "50%_off",
      "x".repeat(200),
    ]) {
      const ts = normalizeSearchQuery(query);
      const sql = await db.query<{ q: string | null }>(
        "SELECT sessions.session_directory_search_query($1) AS q",
        [query],
      );
      expect(ts.ok && ts.query, JSON.stringify(query)).toBe(sql.rows[0]!.q!);
    }
    expect(await search(db, a, "ws-a", "  cafe\u0301 ")).toHaveLength(1);
    expect(await search(db, a, "ws-a", "menu\u00a0cut")).toHaveLength(1);
    expect(
      await search(db, a, "ws-a", "\u200bmenu\u3000cut\ufeff"),
    ).toHaveLength(1);
    expect(await search(db, a, "ws-a", "50%_off")).toHaveLength(1);
    expect(await search(db, a, "ws-a", "5_%")).toEqual([]);
    expect(await search(db, a, "ws-a", "menu%launch")).toEqual([]);
  });

  test("the migration embeds exactly the TypeScript Unicode classes", () => {
    expect(UP).toContain(`'[${pgClass(FORMAT_RANGES)}]'`);
    expect(UP).toContain(`'[${pgClass(SEPARATOR_RANGES)}]+'`);
    expect(UP).toContain(`'[ ${pgClass(MARK_RANGES)}]'`);
  });
});

const sum = (nodes: Plan[], key: string) =>
  nodes.reduce((total, node) => total + Number(node[key] ?? 0), 0);

describe("search RPC query plan as the Worker identity", () => {
  // 30,000 rows: a large tenant (org-0/ws-0, 22,500 rows) beside 7,500 rows of six
  // other tenants. Two in every 1,000 summaries hold the selective needle; every
  // summary holds the common term.
  async function populated(): Promise<PGlite> {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title,
         short_summary, short_summary_scrub_ruleset, short_summary_scrub_version,
         short_summary_source, short_summary_generated_at)
      SELECT CASE WHEN n % 4 = 0 THEN 'org-' || (n % 7 + 1) ELSE 'org-0' END,
             CASE WHEN n % 4 = 0 THEN 'ws-' || (n % 7 + 1) ELSE 'ws-0' END, 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             'Session ' || n,
             CASE WHEN n % 1000 IN (0, 1) THEN 'Rebuilt the quokka ledger reconciler, session ' || n
                  ELSE 'Refactored module ' || (n % 997) || ' and updated tests for case ' || n END,
             'relayhistory-scrub', 1, 'bob-costas/v1', now()
        FROM generate_series(1, 30000) AS n;
      UPDATE sessions.session_directory
         SET search_text = sessions.session_directory_search_text(
               title, task_title, agent_name, project_id, git_repo, git_branch, model, short_summary);
      ANALYZE sessions.session_directory;
    `);
    return db;
  }

  test("a selective term uses the tenant-scoped trigram index within the gates", async () => {
    const db = await populated();
    const { rows, inner } = await explainRpc(
      db,
      claims("org-0", ["ws-0"]),
      "ws-0",
      "quokka ledger",
    );
    console.info(`[G6 plan: selective] ${JSON.stringify(inner.plan)}`);
    expect(rows).toHaveLength(30);
    expect(
      rows.every(
        (hit) => hit.org_id === "org-0" && hit.workspace_id === "ws-0",
      ),
    ).toBe(true);
    expect(
      inner.nodes.some(
        (node) =>
          node["Index Name"] === "session_directory_tenant_search_trgm_idx",
      ),
    ).toBe(true);
    expect(inner.nodes.some((node) => node["Node Type"] === "Seq Scan")).toBe(
      false,
    );
    // Amplification: heap rows examined (kept + removed by recheck/filter) per row returned.
    const heap = inner.nodes.find(
      (node) => node["Node Type"] === "Bitmap Heap Scan",
    )!;
    const examined =
      Number(heap["Actual Rows"]) +
      Number(heap["Rows Removed by Index Recheck"] ?? 0) +
      Number(heap["Rows Removed by Filter"] ?? 0);
    expect(examined / rows.length).toBeLessThanOrEqual(2);
    // Indicative PGlite bounds, not production SLOs.
    expect(
      sum(inner.nodes.slice(0, 1), "Shared Hit Blocks") +
        sum(inner.nodes.slice(0, 1), "Shared Read Blocks"),
    ).toBeLessThanOrEqual(500);
    expect(inner.durationMs).toBeLessThan(1000);
  });

  test("a common term is capped at 51 rows without a seq scan and within the gates", async () => {
    const db = await populated();
    const { rows, inner } = await explainRpc(
      db,
      claims("org-0", ["ws-0"]),
      "ws-0",
      "Refactored module",
    );
    console.info(`[G6 plan: common] ${JSON.stringify(inner.plan)}`);
    expect(rows).toHaveLength(SESSION_DIRECTORY_SEARCH_RPC.maxRows);
    expect(inner.nodes.some((node) => node["Node Type"] === "Seq Scan")).toBe(
      false,
    );
    const scan = inner.nodes.find(
      (node) => typeof node["Index Name"] === "string",
    )!;
    expect([
      "session_directory_tenant_search_trgm_idx",
      "session_directory_workspace_recent_idx",
    ]).toContain(scan["Index Name"] as string);
    const examined =
      sum(
        inner.nodes.filter(
          (node) =>
            /Scan$/.test(String(node["Node Type"])) &&
            node["Node Type"] !== "Bitmap Index Scan",
        ),
        "Actual Rows",
      ) +
      sum(inner.nodes, "Rows Removed by Filter") +
      sum(inner.nodes, "Rows Removed by Index Recheck");
    expect(examined / rows.length).toBeLessThanOrEqual(100);
    expect(
      sum(inner.nodes.slice(0, 1), "Shared Hit Blocks") +
        sum(inner.nodes.slice(0, 1), "Shared Read Blocks"),
    ).toBeLessThanOrEqual(2000);
    expect(inner.durationMs).toBeLessThan(2000);
  });
});

describe("short_summary deletion propagation", () => {
  const pointer = (db: PGlite, machine: string) =>
    db.query(
      `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, trace_id)
     VALUES ('org-a', 'ws-a', 'codex', 's-1', $1, 'u1', now(), now(), 's3', 'bucket', 'key/' || $1,
             '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18')`,
      [machine],
    );
  const hoopDb = (db: PGlite) =>
    ({
      query: async (text: string, params?: unknown[]) => ({
        rows: (await db.query(text, params)).rows,
      }),
    }) as unknown as HoopDb;

  test("the indexer's pointer-tombstone path clears the summary, then tombstones the row", async () => {
    const db = await database();
    await insertRow(db);
    await pointer(db, "m1");
    await pointer(db, "m2");
    await summarize(db, KEY, "heron migration notes");
    const index = hoopDbSessionIndex({
      db: hoopDb(db),
      reader: {} as never,
      queue: {} as never,
      current: () => undefined,
    });
    const deleted = (machine: string) =>
      index.upsertRawPointer(
        {
          deleted: true,
          workspaceId: "ws-a",
          harness: "codex",
          sessionId: "s-1",
          machineId: machine,
          manifestKey: `key/${machine}`,
        } as never,
        { orgId: "org-a" } as never,
      );

    // One machine's manifest deleted: the summary goes at once, the row stays live.
    await deleted("m1");
    let current = await row(db);
    expect(current?.deleted_at).toBeNull();
    expect(current?.short_summary).toBeNull();
    expect(current?.short_summary_scrub_version).toBeNull();
    // Re-projected from the remaining live pointer (m2 has no title/project), keeping
    // the agent name, without the summary.
    expect(current?.machine_ids).toEqual(["m2"]);
    // Catalog-derived project identity survives a pointer whose scalar is null.
    expect(current?.search_text).toBe("Helm proj-1");
    expect(
      await search(db, claims("org-a", ["ws-a"]), "ws-a", "heron"),
    ).toEqual([]);

    // The last machine deleted: the logical row is tombstoned and holds no text.
    await deleted("m2");
    current = await row(db);
    expect(current?.deleted_at).not.toBeNull();
    expect(current?.search_text).toBe("");
    expect(await search(db, claims("org-a", ["ws-a"]), "ws-a", "Helm")).toEqual(
      [],
    );
    // The writer never puts a summary back on a tombstone.
    expect(
      (await summarize(db, KEY, "heron again", new Date(AT.getTime() + 1000)))
        .written,
    ).toBe(false);
  });

  test("the trigger backstop clears a summary when any writer sets deleted_at directly", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "heron migration notes");
    await db.query(
      "UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 's-1'",
    );
    const tombstone = await row(db);
    for (const column of [
      "short_summary",
      "short_summary_scrub_ruleset",
      "short_summary_scrub_version",
      "short_summary_source",
      "short_summary_generated_at",
    ])
      expect(tombstone?.[column]).toBeNull();
    expect(tombstone?.search_text).toBe("");
    // A direct insert of a tombstone that carries a summary is also cleared.
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, deleted_at,
          short_summary, short_summary_scrub_ruleset, short_summary_scrub_version,
          short_summary_source, short_summary_generated_at, search_text)
       VALUES ('org-a', 'ws-a', 'codex', 's-2', now(), now(), now(),
               'heron', 'relayhistory-scrub', 1, 'bob-costas/v1', now(), 'heron')`,
    );
    const leaks = await db.query(
      "SELECT 1 FROM sessions.session_directory WHERE short_summary ILIKE '%heron%' OR search_text ILIKE '%heron%'",
    );
    expect(leaks.rows).toEqual([]);
  });

  test("erasure deletes the row, the summary and its memberships", async () => {
    const db = await database();
    for (const [workspace, session] of [
      ["ws-a", "s-1"],
      ["ws-a", "s-2"],
      ["ws-keep", "s-3"],
    ]) {
      await insertRow(db, "org-a", workspace!, session!);
      await summarize(
        db,
        {
          orgId: "org-a",
          workspaceId: workspace!,
          harness: "codex",
          sessionId: session!,
        },
        `ibis summary ${session}`,
      );
    }
    await db.exec(
      `INSERT INTO sessions.session_directory_users VALUES ('org-a', 'ws-a', 'codex', 's-1', 'u1', now())`,
    );
    expect(await eraseSessionDirectory(db, KEY)).toBe(1);
    expect(await row(db, "s-1")).toBeUndefined();
    expect(
      (await db.query("SELECT 1 FROM sessions.session_directory_users")).rows,
    ).toEqual([]);
    expect(
      await eraseSessionDirectory(db, { orgId: "org-a", workspaceId: "ws-a" }),
    ).toBe(1);
    const left = await db.query<{ session_id: string }>(
      "SELECT session_id FROM sessions.session_directory WHERE short_summary ILIKE '%ibis%' OR search_text ILIKE '%ibis%'",
    );
    expect(left.rows).toEqual([{ session_id: "s-3" }]);
  });
});

describe("short_summary end to end through the pipeline", () => {
  const manifest = (): ManifestV1 =>
    ({
      schema_v: 1,
      layout: "v1",
      trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
      revision: 1,
      created_at: "2026-10-29T07:00:00.000Z",
      updated_at: "2026-10-29T07:00:01.000Z",
      session: {
        workspace_id: "ws-e2e",
        machine_id: "m-1",
        harness: "codex",
        session_id: "s-e2e",
        started_at: "2026-10-29T07:00:00.000Z",
      },
      writer: { delivery_instance_id: "m-1", plugin_version: "test" },
      raw: {
        owner: "m-1",
        state: "open",
        updated_at: "2026-10-29T07:00:01.000Z",
        format: "jsonl",
        compression: "zstd",
        chunk_count: 1,
        raw_bytes: 12,
        stored_bytes: 10,
        content_sha256: "1".repeat(64),
        epoch: 0,
        epochs: [
          {
            epoch: 0,
            opened_at: "2026-10-29T07:00:00.000Z",
            reason: "initial",
            first_seq: 0,
            opened_by: "m-1",
          },
        ],
        next_offset: 12,
        chunks: [
          {
            seq: 0,
            epoch: 0,
            start_offset: 0,
            bytes: 12,
            stored_bytes: 10,
            sha256: "2".repeat(64),
            sha256_8: "22222222",
            stored_sha256: "3".repeat(64),
            compression: "zstd",
          },
        ],
        scrub: { ruleset: "relayhistory-scrub", version: 1 },
      },
      extensions: {
        "96bulls": {
          session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19",
          session_uuid_source: "minted",
        },
      },
    }) as unknown as ManifestV1;

  async function pipeline() {
    const db = await hoopDatabase();
    const q = async (text: string, params?: unknown[]) => ({
      rows: (await db.query(text, params as never)).rows as never[],
    });
    const hoop = {
      query: q,
      pool: { connect: async () => ({ query: q, release() {} }) },
      end: async () => {},
    } as unknown as HoopDb;
    const m = manifest();
    // A second machine of the same logical session has its own manifest and uuid.
    const m2 = {
      ...m,
      session: { ...m.session, machine_id: "m-2" },
      extensions: {
        "96bulls": {
          session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c1a",
          session_uuid_source: "minted",
        },
      },
    } as unknown as ManifestV1;
    const reader = {
      provider: "fs",
      bucket: "locker",
      read: async (key: string) => ({
        manifest: key.includes("/m=m-2/") ? m2 : m,
        etag: "e1",
      }),
    } as never;
    const pointer = {
      traceId: m.trace_id,
      workspaceId: "ws-e2e",
      machineId: "m-1",
      harness: "codex",
      sessionId: "s-e2e",
      manifestKey:
        "v1/ws=ws-e2e/m=m-1/h=codex/d=2026/10/29/s=s-e2e/manifest.json",
      provider: "fs",
      bucket: "locker",
    };
    const ctx = {
      traceId: m.trace_id,
      orgId: "org-a",
      workspaceId: "ws-e2e",
      machineId: "m-1",
      harness: "codex",
      sessionId: "s-e2e",
    };
    const sent: unknown[] = [];
    const index = hoopDbSessionIndex({
      db: hoop,
      reader,
      queue: {
        backend: "memory",
        send: async (msg: unknown) => {
          sent.push(msg);
        },
        sendBatch: async () => {},
      } as never,
      current: () => undefined,
    });
    const store = createFsStore({
      root: mkdtempSync(join(tmpdir(), "g6-e2e-")),
    });
    const line = JSON.stringify({
      type: "user",
      message: {
        role: "user",
        content: `Fix the pelican ledger; token ${FAKE_RELAY_TOKEN}`,
      },
    });
    await store.put(
      rawChunkKey(
        {
          workspaceId: "ws-e2e",
          machineId: "m-1",
          harness: "codex",
          sessionId: "s-e2e",
          startDate: "2026-10-29",
        },
        0,
        0,
        "22222222",
        "jsonl",
      ),
      zstdCompressSync(Buffer.from(`${line}\n`)),
    );
    const keyring = await masterKeyring(
      parseMasterKeys(`k1:${"a".repeat(64)}`),
    );
    const summaryShort = (
      directory?: Parameters<typeof bobCostasHandler>[1]["directory"],
      storeOverride?: typeof store,
      keyringOverride?: typeof keyring,
      summarizerOverride?: typeof extractiveSummarizer,
    ) =>
      bobCostasHandler("short", {
        db: hoop,
        reader,
        store: storeOverride ?? store,
        summarizer: summarizerOverride ?? extractiveSummarizer,
        keyring: keyringOverride ?? keyring,
        ...(directory ? { directory } : {}),
      })(
        { payload: pointer, ctx: { orgId: "org-a" } } as never,
        { log: { info() {} } } as never,
      );
    return {
      db,
      hoop,
      index,
      pointer,
      ctx,
      sent,
      summaryShort,
      store,
      keyring,
    };
  }

  test("indexer projects the row, Bob Costas writes the scrubbed summary, the RPC is tenant-bound, deletion clears it", async () => {
    const { db, index, pointer, ctx, sent, summaryShort } = await pipeline();
    // 1. Projector create path: the indexer writes session_index and the directory row.
    await index.upsertRawPointer(pointer as never, ctx as never);
    let current = await row(db, "s-e2e");
    expect(current?.org_id).toBe("org-a");
    expect(current?.machine_ids).toEqual(["m-1"]);
    expect(current?.short_summary).toBeNull();
    expect(sent.length).toBeGreaterThan(0);
    // The same workspace and session ids under another org, to prove the org boundary.
    await insertRow(db, "org-b", "ws-e2e", "s-e2e-b");
    await summarize(
      db,
      {
        orgId: "org-b",
        workspaceId: "ws-e2e",
        harness: "codex",
        sessionId: "s-e2e-b",
      },
      "pelican ledger for org b",
    );

    // 2. Bob Costas short summary over the scrubbed raw: the real production callsite.
    await summaryShort();
    current = await row(db, "s-e2e");
    expect(String(current?.short_summary)).toContain("pelican ledger");
    expect(String(current?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(String(current?.search_text)).toContain("pelican ledger");
    expect(String(current?.search_text)).not.toContain(FAKE_RELAY_TOKEN);
    expect(current?.short_summary_scrub_ruleset).toBe(SCRUB_RULESET);
    expect(current?.short_summary_scrub_version).toBe(SCRUB_RULESET_VERSION);
    expect(current?.short_summary_source).toBe("bob-costas/extractive/v1");

    // 3. The Worker RPC: tenant A finds it; tenant B (same workspace id) sees only its own.
    const hitsA = await search(
      db,
      claims("org-a", ["ws-e2e"]),
      "ws-e2e",
      "pelican ledger",
    );
    expect(hitsA.map((hit) => [hit.org_id, hit.session_id])).toEqual([
      ["org-a", "s-e2e"],
    ]);
    const hitsB = await search(
      db,
      claims("org-b", ["ws-e2e"]),
      "ws-e2e",
      "pelican ledger",
    );
    expect(hitsB.map((hit) => [hit.org_id, hit.session_id])).toEqual([
      ["org-b", "s-e2e-b"],
    ]);

    // 4. A re-index of the same manifest keeps the summary in search_text.
    await index.upsertRawPointer(pointer as never, ctx as never);
    expect(String((await row(db, "s-e2e"))?.search_text)).toContain(
      "pelican ledger",
    );

    // 5. Deletion through the indexer clears it end to end.
    await index.upsertRawPointer(
      { ...pointer, deleted: true } as never,
      ctx as never,
    );
    current = await row(db, "s-e2e");
    expect(current?.deleted_at).not.toBeNull();
    expect(current?.short_summary).toBeNull();
    expect(current?.search_text).toBe("");
    expect(
      await search(db, claims("org-a", ["ws-e2e"]), "ws-e2e", "pelican ledger"),
    ).toEqual([]);
    // Re-sending the same manifest revision is fenced by upsert_session_index, so the
    // pointer stays tombstoned and the row is not resurrected (only a later live pointer
    // resurrects; see session-directory-precedence.test.ts).
    await index.upsertRawPointer(pointer as never, ctx as never);
    expect((await row(db, "s-e2e"))?.deleted_at).not.toBeNull();
  }, 60_000);

  test("deleting every pointer tombstones the row, deletes memberships, and the RPC returns nothing", async () => {
    const { db, index, pointer, ctx, summaryShort } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    const second = {
      ...pointer,
      machineId: "m-2",
      manifestKey: pointer.manifestKey.replace("/m=m-1/", "/m=m-2/"),
    };
    await index.upsertRawPointer(
      second as never,
      { ...ctx, machineId: "m-2" } as never,
    );
    await summaryShort();
    let current = await row(db, "s-e2e");
    expect(current?.machine_ids).toEqual(["m-1", "m-2"]);
    expect(String(current?.short_summary)).toContain("pelican ledger");
    const members = async () =>
      (
        await db.query(
          `SELECT 'u' FROM sessions.session_directory_users WHERE session_id = 's-e2e'
       UNION ALL SELECT 'm' FROM sessions.session_directory_machines WHERE session_id = 's-e2e'`,
        )
      ).rows.length;
    expect(await members()).toBe(2); // placeholders are not user memberships
    const a = claims("org-a", ["ws-e2e"]);
    expect(await search(db, a, "ws-e2e", "pelican")).toHaveLength(1);

    await index.upsertRawPointer(
      { ...pointer, deleted: true } as never,
      ctx as never,
    );
    current = await row(db, "s-e2e");
    expect(current?.deleted_at).toBeNull();
    expect(current?.short_summary).toBeNull();
    expect(current?.machine_ids).toEqual(["m-2"]);
    expect(await search(db, a, "ws-e2e", "pelican")).toEqual([]);

    await index.upsertRawPointer(
      { ...second, deleted: true } as never,
      { ...ctx, machineId: "m-2" } as never,
    );
    current = await row(db, "s-e2e");
    expect(current?.deleted_at).not.toBeNull();
    expect(current?.short_summary).toBeNull();
    expect(current?.search_text).toBe("");
    expect(current?.machine_ids).toEqual([]);
    expect(current?.user_ids).toEqual([]);
    expect(await members()).toBe(0);
    for (const term of ["pelican", "ledger", "codex", "s-e2e"])
      expect(await search(db, a, "ws-e2e", term)).toEqual([]);

    // The projector alone also tombstones when no live pointer remains.
    await insertRow(db, "org-a", "ws-e2e", "s-orphan");
    expect(
      await projectSessionDirectory(db, {
        orgId: "org-a",
        workspaceId: "ws-e2e",
        harness: "codex",
        sessionId: "s-orphan",
      }),
    ).toEqual({ projected: false, tombstoned: true });
    expect((await row(db, "s-orphan"))?.deleted_at).not.toBeNull();
    expect(await search(db, a, "ws-e2e", "flaky")).toEqual([]);
  }, 60_000);

  test("re-running summary-short after directory.write throws is idempotent", async () => {
    const { db, index, pointer, ctx, summaryShort } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    const failing = {
      write: async () => {
        throw new Error("directory unavailable");
      },
      clear: async () => false,
    };
    // First run publishes the encrypted object and pointer, then fails on the directory.
    await expect(summaryShort(failing)).rejects.toThrow(
      "directory unavailable",
    );
    expect((await row(db, "s-e2e"))?.short_summary).toBeNull();
    const pointers = await db.query(
      "SELECT status FROM sessions.session_summaries WHERE session_id = 's-e2e' AND kind = 'short'",
    );
    expect(pointers.rows).toEqual([{ status: "ready" }]);

    // The retry sees a current summary and reconciles the directory from the object.
    expect(await summaryShort()).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    const first = await row(db, "s-e2e");
    expect(String(first?.short_summary)).toContain("pelican ledger");
    expect(String(first?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(String(first?.search_text)).toContain("pelican ledger");

    // Further reruns change nothing.
    expect(await summaryShort()).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    const again = await row(db, "s-e2e");
    expect(again?.short_summary).toBe(first?.short_summary);
    expect(again?.short_summary_generated_at).toEqual(
      first?.short_summary_generated_at,
    );
    expect(
      await search(db, claims("org-a", ["ws-e2e"]), "ws-e2e", "pelican ledger"),
    ).toHaveLength(1);
  }, 60_000);

  test("a retry replaces an older revision's summary left behind by a failed directory write", async () => {
    const { db, index, pointer, ctx, summaryShort } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    const key = {
      orgId: "org-a",
      workspaceId: "ws-e2e",
      harness: "codex",
      sessionId: "s-e2e",
    };
    // Revision N-1's summary is in the directory.
    const older = new Date("2026-10-01T00:00:00Z");
    await writeShortSummary(db, {
      ...key,
      summary: "Stale osprey notes from an older revision",
      source: "bob-costas/extractive/v0",
      generatedAt: older,
    });
    const a = claims("org-a", ["ws-e2e"]);
    expect(await search(db, a, "ws-e2e", "osprey")).toHaveLength(1);

    // Revision N's object and pointer are published, then the directory write fails.
    const failing = {
      write: async () => {
        throw new Error("directory unavailable");
      },
      clear: async () => false,
    };
    await expect(summaryShort(failing)).rejects.toThrow(
      "directory unavailable",
    );
    expect(String((await row(db, "s-e2e"))?.short_summary)).toContain("osprey");

    // The retry finds the summary current but the row holds the old text: it republishes.
    expect(await summaryShort()).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    const fixed = await row(db, "s-e2e");
    expect(String(fixed?.short_summary)).toContain("pelican ledger");
    expect(String(fixed?.short_summary)).not.toContain("osprey");
    expect(String(fixed?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(String(fixed?.search_text)).not.toContain("osprey");
    expect(fixed?.short_summary_source).toBe("bob-costas/extractive/v1");
    expect(fixed?.short_summary_scrub_ruleset).toBe(SCRUB_RULESET);
    expect(fixed?.short_summary_scrub_version).toBe(SCRUB_RULESET_VERSION);
    expect(
      (fixed?.short_summary_generated_at as Date).getTime(),
    ).toBeGreaterThan(older.getTime());
    expect(await search(db, a, "ws-e2e", "osprey")).toEqual([]);
    expect(await search(db, a, "ws-e2e", "pelican ledger")).toHaveLength(1);

    // A further retry changes nothing.
    expect(await summaryShort()).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    const again = await row(db, "s-e2e");
    expect(again?.short_summary).toBe(fixed?.short_summary);
    expect(again?.short_summary_generated_at).toEqual(
      fixed?.short_summary_generated_at,
    );
    expect(again?.search_text).toBe(fixed?.search_text);
  }, 60_000);

  test("a corrupt current summary object clears the directory summary and fails non-retryably", async () => {
    const { db, index, pointer, ctx, summaryShort, store } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    await summaryShort();
    expect(String((await row(db, "s-e2e"))?.short_summary)).toContain(
      "pelican ledger",
    );
    const published = (
      await db.query<{ object_key: string }>(
        "SELECT object_key FROM sessions.session_summaries WHERE session_id = 's-e2e' AND kind = 'short'",
      )
    ).rows[0]!;
    await store.put(
      objectKey(published.object_key),
      new TextEncoder().encode("not an envelope"),
    );

    const error = (await summaryShort().then(
      () => null,
      (caught: unknown) => caught,
    )) as { code?: string; retryable?: boolean } | null;
    expect(error?.code).toBe("summary_object_integrity");
    expect(error?.retryable).toBe(false);
    expect((await row(db, "s-e2e"))?.short_summary).toBeNull();
    expect(String((await row(db, "s-e2e"))?.search_text)).not.toContain(
      "pelican",
    );
    expect(
      await search(db, claims("org-a", ["ws-e2e"]), "ws-e2e", "pelican ledger"),
    ).toEqual([]);
  }, 60_000);

  test("a store error on the retry read clears the directory summary, then rethrows it", async () => {
    const { db, index, pointer, ctx, summaryShort, store } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    await summaryShort();
    expect(String((await row(db, "s-e2e"))?.short_summary)).toContain(
      "pelican ledger",
    );
    const down = new Error("store unavailable");
    const failingStore = new Proxy(store, {
      get(target, prop, receiver) {
        if (prop === "getObject")
          return async () => {
            throw down;
          };
        return Reflect.get(target, prop, receiver);
      },
    });
    const error = (await summaryShort(undefined, failingStore).then(
      () => null,
      (caught: unknown) => caught,
    )) as { code?: string; retryable?: boolean; message?: string } | null;
    // readSummaryObject wraps the store failure as the retryable summary_store_unavailable.
    expect(error?.code).toBe("summary_store_unavailable");
    expect(error?.retryable).toBe(true);
    expect((await row(db, "s-e2e"))?.short_summary).toBeNull();
    expect(
      await search(db, claims("org-a", ["ws-e2e"]), "ws-e2e", "pelican ledger"),
    ).toEqual([]);
    // Once the store is back, the retry republishes.
    expect(await summaryShort()).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    expect(String((await row(db, "s-e2e"))?.short_summary)).toContain(
      "pelican ledger",
    );
  }, 60_000);

  test("Bob Costas: a rejected new summary clears the older searchable one", async () => {
    const { db, index, pointer, ctx, summaryShort } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    const key = {
      orgId: "org-a",
      workspaceId: "ws-e2e",
      harness: "codex",
      sessionId: "s-e2e",
    };
    await writeShortSummary(db, {
      ...key,
      summary: "Stale osprey notes",
      source: "bob-costas/extractive/v0",
      generatedAt: new Date("2026-10-01T00:00:00Z"),
    });
    const a = claims("org-a", ["ws-e2e"]);
    expect(await search(db, a, "ws-e2e", "osprey")).toHaveLength(1);
    const overLong = {
      ...extractiveSummarizer,
      summarize: async () => ({
        text: "z".repeat(SHORT_SUMMARY_MAX_CHARS + 1),
        modelId: "extractive/v1",
        tokensIn: 0,
        tokensOut: 0,
      }),
    } as typeof extractiveSummarizer;
    // The refusal is logged by reason and the stage completes (the encrypted object is published).
    await summaryShort(undefined, undefined, undefined, overLong);
    const current = await row(db, "s-e2e");
    expect(current?.short_summary).toBeNull();
    expect(String(current?.search_text)).not.toContain("osprey");
    expect(await search(db, a, "ws-e2e", "osprey")).toEqual([]);
  }, 60_000);

  /** Captures the service's stderr log lines (warn/error) while `run` executes. */
  async function stderrLines(run: () => Promise<unknown>): Promise<string[]> {
    const lines: string[] = [];
    const original = process.stderr.write.bind(process.stderr);
    process.stderr.write = ((chunk: string | Uint8Array) => {
      lines.push(String(chunk));
      return true;
    }) as typeof process.stderr.write;
    try {
      await run().catch(() => undefined);
    } finally {
      process.stderr.write = original;
    }
    return lines.flatMap((chunk) => chunk.split("\n")).filter(Boolean);
  }

  test("an integrity dead-letter logs session, revision and the object key hash", async () => {
    const { db, index, pointer, ctx, summaryShort, store } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    await summaryShort();
    const { object_key } = (
      await db.query<{ object_key: string }>(
        "SELECT object_key FROM sessions.session_summaries WHERE session_id = 's-e2e' AND kind = 'short'",
      )
    ).rows[0]!;
    await store.put(
      objectKey(object_key),
      new TextEncoder().encode("not an envelope"),
    );
    const lines = await stderrLines(() => summaryShort());
    const line = lines
      .map((l) => JSON.parse(l) as Record<string, unknown>)
      .find((entry) => entry.error_code === "summary_object_integrity");
    expect(line).toMatchObject({
      level: "warn",
      stage: "summary-short",
      retryable: false,
      org_id: "org-a",
      workspace_id: "ws-e2e",
      harness: "codex",
      session_id: "s-e2e",
      machine_id: "m-1",
      revision: 1,
      key_sha256: createHash("sha256").update(object_key).digest("hex"),
    });
    // The path itself is never logged.
    expect(lines.join("\n")).not.toContain(object_key);
  }, 60_000);

  test("a retired key id on the retry read is non-retryable, clears the summary and cannot loop", async () => {
    const { db, index, pointer, ctx, summaryShort, keyring } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    await summaryShort();
    expect(String((await row(db, "s-e2e"))?.short_summary)).toContain(
      "pelican ledger",
    );
    // The pointer still names hkdf1:k1 as current, but this deployment no longer holds k1
    // for decryption: keyById comes from the real keyring implementation, built without it.
    const retiredOnly = await masterKeyring(
      parseMasterKeys(`k2:${"b".repeat(64)}`),
    );
    const rotatedAway = {
      currentKey: keyring.currentKey,
      keyById: retiredOnly.keyById,
    } as typeof keyring;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const error = (await summaryShort(undefined, undefined, rotatedAway).then(
        () => null,
        (caught: unknown) => caught,
      )) as { code?: string; retryable?: boolean } | null;
      expect(error?.code).toBe("keyring_unknown_key");
      expect(error?.retryable).toBe(false);
    }
    expect((await row(db, "s-e2e"))?.short_summary).toBeNull();
    expect(
      await search(db, claims("org-a", ["ws-e2e"]), "ws-e2e", "pelican ledger"),
    ).toEqual([]);
  }, 60_000);

  test("a failing clear after a read failure does not mask the original error", async () => {
    const { db, index, pointer, ctx, summaryShort, store } = await pipeline();
    await index.upsertRawPointer(pointer as never, ctx as never);
    await summaryShort();
    const failingStore = new Proxy(store, {
      get(target, prop, receiver) {
        if (prop === "getObject")
          return async () => {
            throw new Error("store unavailable");
          };
        return Reflect.get(target, prop, receiver);
      },
    });
    const brokenDirectory = {
      write: async () => ({ written: false }),
      clear: async () => {
        throw new Error("directory clear failed");
      },
    };
    let error: { code?: string; retryable?: boolean; message?: string } | null =
      null;
    const lines = await stderrLines(async () => {
      error = (await summaryShort(brokenDirectory, failingStore).then(
        () => null,
        (caught: unknown) => caught,
      )) as never;
    });
    expect(error!.code).toBe("summary_store_unavailable");
    expect(error!.retryable).toBe(true);
    const entries = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    const readFailed = entries.findIndex(
      (entry) => entry.error_code === "summary_store_unavailable",
    );
    const clearFailed = entries.findIndex(
      (entry) => entry.error_code === "summary_directory_clear_failed",
    );
    expect(readFailed).toBeGreaterThanOrEqual(0);
    expect(clearFailed).toBeGreaterThan(readFailed);
    // The directory was not cleared (its clear failed), and the retry still repairs it.
    expect(await summaryShort()).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    expect(String((await row(db, "s-e2e"))?.short_summary)).toContain(
      "pelican ledger",
    );
  }, 60_000);

  test("deployment prefix (#88) with the directory projection (#38): a prefixed manifest publishes, reconciles on retry from the prefixed object, and rebuilds when moved", async () => {
    const db = await hoopDatabase();
    const q = async (text: string, params?: unknown[]) => ({
      rows: (await db.query(text, params as never)).rows as never[],
    });
    const hoop = {
      query: q,
      pool: { connect: async () => ({ query: q, release() {} }) },
      end: async () => {},
    } as unknown as HoopDb;
    const m = manifest();
    const reader = {
      provider: "fs",
      bucket: "locker",
      read: async () => ({ manifest: m, etag: "e1" }),
    } as never;
    const tail = "v1/ws=ws-e2e/m=m-1/h=codex/d=2026/10/29/s=s-e2e";
    const prefixA = "96bulls/org-a";
    const prefixB = "96bulls/org-a-moved";
    const pointerAt = (prefix: string) => ({
      traceId: m.trace_id,
      workspaceId: "ws-e2e",
      machineId: "m-1",
      harness: "codex",
      sessionId: "s-e2e",
      manifestKey: `${prefix}/${tail}/manifest.json`,
      provider: "fs",
      bucket: "locker",
    });
    const ctx = {
      traceId: m.trace_id,
      orgId: "org-a",
      workspaceId: "ws-e2e",
      machineId: "m-1",
      harness: "codex",
      sessionId: "s-e2e",
    };
    const index = hoopDbSessionIndex({
      db: hoop,
      reader,
      queue: {
        backend: "memory",
        send: async () => {},
        sendBatch: async () => {},
      } as never,
      current: () => undefined,
    });
    await index.upsertRawPointer(pointerAt(prefixA) as never, ctx as never);
    expect((await row(db, "s-e2e"))?.short_summary).toBeNull();

    // Chunks exist only under the deployment prefixes, as the ingest stores them.
    const store = createFsStore({
      root: mkdtempSync(join(tmpdir(), "g6-prefix-")),
    });
    const line = JSON.stringify({
      type: "user",
      message: {
        role: "user",
        content: `Fix the pelican ledger; token ${FAKE_RELAY_TOKEN}`,
      },
    });
    for (const prefix of [prefixA, prefixB]) {
      await store.put(
        rawChunkKey(
          {
            prefix,
            workspaceId: "ws-e2e",
            machineId: "m-1",
            harness: "codex",
            sessionId: "s-e2e",
            startDate: "2026-10-29",
          },
          0,
          0,
          "22222222",
          "jsonl",
        ),
        zstdCompressSync(Buffer.from(`${line}\n`)),
      );
    }
    const keyring = await masterKeyring(
      parseMasterKeys(`k1:${"a".repeat(64)}`),
    );
    const run = (
      prefix: string,
      directory?: Parameters<typeof bobCostasHandler>[1]["directory"],
    ) =>
      bobCostasHandler("short", {
        db: hoop,
        reader,
        store,
        summarizer: extractiveSummarizer,
        keyring,
        ...(directory ? { directory } : {}),
      })(
        { payload: pointerAt(prefix), ctx: { orgId: "org-a" } } as never,
        { log: { info() {} } } as never,
      );
    const objectKey = async () =>
      (
        await db.query<{ object_key: string }>(
          "SELECT object_key FROM sessions.session_summaries WHERE session_id = 's-e2e' AND kind = 'short'",
        )
      ).rows[0]?.object_key;

    // 1. Published under prefix A; the directory write fails after the pointer commits.
    const failing = {
      write: async () => {
        throw new Error("directory unavailable");
      },
      clear: async () => false,
    };
    await expect(run(prefixA, failing)).rejects.toThrow(
      "directory unavailable",
    );
    expect(await objectKey()).toBe(`${prefixA}/${tail}/summary/short.json.enc`);
    expect((await row(db, "s-e2e"))?.short_summary).toBeNull();

    // 2. The retry is "current" only because the pointer is the object beside this
    //    manifest (#88). #38's reconcile then reads that prefixed object and publishes.
    expect(await run(prefixA)).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
    let current = await row(db, "s-e2e");
    expect(String(current?.short_summary)).toContain("pelican ledger");
    expect(String(current?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(current?.short_summary_source).toBe("bob-costas/extractive/v1");
    expect(
      await search(db, claims("org-a", ["ws-e2e"]), "ws-e2e", "pelican ledger"),
    ).toHaveLength(1);

    // 3. The same revision under prefix B is not current (the pointer is outside the
    //    manifest's prefix): rebuilt beside the new manifest and republished.
    const firstGenerated = (
      current?.short_summary_generated_at as Date
    ).getTime();
    const moved = await run(prefixB);
    expect(moved).not.toEqual({ status: "skipped", reason: "summary_current" });
    expect(await objectKey()).toBe(`${prefixB}/${tail}/summary/short.json.enc`);
    current = await row(db, "s-e2e");
    expect(String(current?.short_summary)).toContain("pelican ledger");
    expect(
      (current?.short_summary_generated_at as Date).getTime(),
    ).toBeGreaterThanOrEqual(firstGenerated);
    // And now prefix B is the current one.
    expect(await run(prefixB)).toEqual({
      status: "skipped",
      reason: "summary_current",
    });
  }, 60_000);
});

describe("short_summary rollback", () => {
  test("down removes every summary column, copy and the RPC, keeps the bootstrapped role at its spec, and up applies again", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "pelican rollback check");
    // Reverse dependency order: event metadata also depends on G6 and the list/catalog phases.
    for(const file of [...LATER_PHASES].reverse())await db.exec(read(`supabase/rollbacks/${file}`));
    await db.exec(EVENTS_DOWN);
    await db.exec(BRIEF_STATE_DOWN);
    await db.exec(BRIEFS_DOWN);
    await db.exec(LIST_DOWN);
    await db.exec(EXPAND_DOWN);
    await db.exec(DOWN);
    const columns = await db.query(
      `SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'sessions' AND table_name = 'session_directory' AND column_name LIKE 'short_summary%'`,
    );
    expect(columns.rows).toEqual([]);
    const objects = await db.query(
      `SELECT 1 FROM pg_class WHERE relname = 'session_directory_tenant_search_trgm_idx'
       UNION ALL SELECT 1 FROM pg_proc WHERE proname IN
         ('session_directory_search_text', 'session_directory_tombstone_clears_summary', 'search_session_directory')
       UNION ALL SELECT 1 FROM pg_trigger WHERE tgname = 'session_directory_tombstone_clears_summary'`,
    );
    expect(objects.rows).toEqual([]);
    // The migration never created the role, so its down does not drop it; it only
    // revokes what the up granted, leaving the role exactly as the bootstrap made it.
    expect(
      (
        await db.query<Row>(
          `SELECT has_schema_privilege($1, 'sessions', 'USAGE') AS usage,
                  (SELECT setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole
                    WHERE r.rolname = $1) AS setconfig`,
          [WORKER],
        )
      ).rows,
    ).toEqual([{ usage: false, setconfig: ["statement_timeout=2s"] }]);
    expect(
      (
        await db.query(
          "SELECT 1 FROM pg_class WHERE relname = 'session_directory_metadata_trgm_idx'",
        )
      ).rows,
    ).toHaveLength(1);
    expect((await row(db))?.search_text).toBe(
      "Fix flaky queue test Helm proj-1",
    );

    await db.exec(UP);
    await db.exec(EXPAND_UP);
    await db.exec(LIST_UP);
    await db.exec(BRIEFS_UP);
    await db.exec(BRIEF_STATE_UP);
    await db.exec(EVENTS_UP);
    for(const file of LATER_PHASES)await db.exec(read(`supabase/migrations/${file.replace(/\.down\.sql$/,".sql")}`));
    await db.query(
      "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
      [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
    );
    await summarize(db, KEY, "pelican again");
    expect((await row(db))?.short_summary).toBe("pelican again");
    expect(
      await search(db, claims("org-a", ["ws-a"]), "ws-a", "pelican"),
    ).toHaveLength(1);
  });
});

describe("Worker role validation in two phases", () => {
  const PHASE1 = read("supabase/bootstrap/session_directory_search_worker.sql");
  const PHASE2 = read(
    "supabase/bootstrap/session_directory_search_worker_verify.sql",
  );
  const outcome = async (db: PGlite, sql: string) =>
    (await db.exec(sql).then(
      () => null,
      (caught: unknown) => caught,
    )) as { code?: string; message?: string } | null;
  const PHASE2_PREFIX =
    "verify session_directory_search_worker: the role does not match the approved grants";

  test("phase 1 alone: the bare role passes before any schema object or grant exists", async () => {
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    open.push(db);
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    expect(await outcome(db, PHASE1)).toBeNull();
    expect(await outcome(db, PHASE1)).toBeNull();
    expect(
      (await db.query("SELECT 1 FROM pg_namespace WHERE nspname = 'sessions'"))
        .rows,
    ).toEqual([]);
  });

  test("phase 2 alone, before the migrations: fails, naming every missing RPC and grant", async () => {
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    open.push(db);
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    await db.exec(PHASE1);
    await db.exec("CREATE SCHEMA sessions");
    const error = await outcome(db, PHASE2);
    expect(error?.code).toBe("55000");
    expect(error?.message).toContain(PHASE2_PREFIX);
    const manifest = JSON.parse(
      read("src/hoopdb/sessions-function-acl.json"),
    ) as { functions: Record<string, string[]> };
    const workerSignatures = Object.entries(manifest.functions)
      .filter(([, acl]) => acl.includes(`${WORKER}:EXECUTE`))
      .map(([signature]) => signature);
    for (const sig of workerSignatures)
      expect(error?.message).toContain(`approved RPC missing: ${sig}`);
    expect(error?.message).toContain("missing USAGE on schema sessions");
  });

  test("phase 2 after the full migration: the approved grants pass, both phases rerun cleanly, and the role holds exactly them", async () => {
    const db = await hoopDatabase();
    expect(await outcome(db, PHASE2)).toBeNull();
    expect(await outcome(db, PHASE1)).toBeNull();
    expect(await outcome(db, PHASE2)).toBeNull();
    const fns = await db.query<{ fn: string }>(
      `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p, aclexplode(p.proacl) a
        JOIN pg_roles r ON r.oid = a.grantee WHERE r.rolname = $1 ORDER BY 1`,
      [WORKER],
    );
    const manifest=JSON.parse(read("src/hoopdb/sessions-function-acl.json")) as {functions:Record<string,string[]>};
    const signatures=Object.entries(manifest.functions).filter(([,acl])=>acl.includes(WORKER+":EXECUTE")).map(([signature])=>signature);
    const approved=await db.query<{fn:string}>("SELECT to_regprocedure(sig)::text fn FROM unnest($1::text[]) sig ORDER BY 1",[signatures]);
    expect(fns.rows).toEqual(approved.rows);
    expect(fns.rows).toHaveLength(signatures.length);
  });

  test("phase 2 ignores PUBLIC-derived EXECUTE: only aclitems naming the role count", async () => {
    const db = await hoopDatabase();
    await db.exec(
      "GRANT EXECUTE ON FUNCTION sessions.session_directory_worker_scope(text) TO PUBLIC",
    );
    expect(await outcome(db, PHASE2)).toBeNull();
  });

  test("phase 2 accepts the separately approved authenticator binding, and only that shape", async () => {
    const db = await hoopDatabase();
    await db.exec(
      `CREATE ROLE authenticator NOLOGIN NOINHERIT; GRANT ${WORKER} TO authenticator`,
    );
    expect(await outcome(db, PHASE2)).toBeNull();
    await db.exec(`REVOKE ${WORKER} FROM authenticator`);
    await db.exec(`GRANT ${WORKER} TO authenticator WITH ADMIN OPTION`);
    const error = await outcome(db, PHASE2);
    expect(error?.code).toBe("55000");
    expect(error?.message).toContain("granted to authenticator");
  });

  const DRIFT: Array<[string, string, string]> = [
    [
      "an unapproved table privilege",
      `GRANT SELECT ON sessions.session_directory TO ${WORKER}`,
      "has privileges on table sessions.session_directory",
    ],
    [
      "a column privilege",
      `GRANT SELECT (org_id) ON sessions.session_directory TO ${WORKER}`,
      "has privileges on column org_id of table sessions.session_directory",
    ],
    [
      "EXECUTE on a non-approved function",
      `GRANT EXECUTE ON FUNCTION sessions.session_directory_worker_scope(text) TO ${WORKER}`,
      "has privileges on function sessions.session_directory_worker_scope(text)",
    ],
    [
      "a default privilege",
      `ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO ${WORKER}`,
      "has privileges on default privileges",
    ],
    [
      "a sequence privilege",
      `CREATE SEQUENCE sessions.g6_drift_seq; GRANT USAGE ON SEQUENCE sessions.g6_drift_seq TO ${WORKER}`,
      "has privileges on sequence sessions.g6_drift_seq",
    ],
    [
      "a privilege on schema sessions other than USAGE",
      `GRANT CREATE ON SCHEMA sessions TO ${WORKER}`,
      "schema sessions: privileges other than USAGE",
    ],
    [
      "an approved RPC WITH GRANT OPTION",
      `GRANT EXECUTE ON FUNCTION sessions.session_directory_readiness() TO ${WORKER} WITH GRANT OPTION`,
      "sessions.session_directory_readiness(): privileges other than EXECUTE",
    ],
    [
      "an owned object",
      `CREATE TABLE public.g6_drift (x int); ALTER TABLE public.g6_drift OWNER TO ${WORKER}`,
      "owns table g6_drift",
    ],
    [
      "a database privilege",
      `DO $g$ BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO ${WORKER}', current_database()); END $g$`,
      "has privileges on database",
    ],
    [
      "an RLS policy naming the role",
      `CREATE POLICY g6_drift ON sessions.session_directory FOR SELECT TO ${WORKER} USING (true)`,
      "is named in policy",
    ],
    [
      "a missing approved RPC",
      "DROP FUNCTION sessions.session_directory_readiness()",
      "approved RPC missing: sessions.session_directory_readiness()",
    ],
    [
      "a redefined approved RPC (a different signature)",
      `DROP FUNCTION sessions.session_directory_pointers(text, text, text);
      CREATE FUNCTION sessions.session_directory_pointers(p_workspace_id text, p_session_id text)
        RETURNS integer LANGUAGE sql AS 'SELECT 1';
      GRANT EXECUTE ON FUNCTION sessions.session_directory_pointers(text, text) TO ${WORKER}`,
      "approved RPC redefined: sessions.session_directory_pointers(text,text)",
    ],
    [
      "a redefined brief-document authority RPC",
      `CREATE FUNCTION sessions.session_brief_document_authority(p_workspace_id text)
        RETURNS integer LANGUAGE sql AS 'SELECT 1';
      GRANT EXECUTE ON FUNCTION sessions.session_brief_document_authority(text) TO ${WORKER}`,
      "approved RPC redefined: sessions.session_brief_document_authority(text)",
    ],
    [
      "a revoked approved grant",
      `REVOKE EXECUTE ON FUNCTION sessions.session_directory_readiness() FROM ${WORKER}`,
      "missing EXECUTE on sessions.session_directory_readiness()",
    ],
    [
      "a revoked schema USAGE",
      `REVOKE USAGE ON SCHEMA sessions FROM ${WORKER}`,
      "missing USAGE on schema sessions",
    ],
  ];
  for (const [name, change, problem] of DRIFT) {
    test(`phase 2 after the full migration fails closed (55000) on ${name}; phase 1 does not look at grants`, async () => {
      const db = await hoopDatabase();
      expect(await outcome(db, PHASE2)).toBeNull();
      await db.exec(change);
      const error = await outcome(db, PHASE2);
      expect(error?.code).toBe("55000");
      expect(error?.message).toContain(PHASE2_PREFIX);
      expect(error?.message).toContain(problem);
      // Phase 1 is the bare-role check; grants are phase 2's.
      expect(await outcome(db, PHASE1)).toBeNull();
    });
  }
});
