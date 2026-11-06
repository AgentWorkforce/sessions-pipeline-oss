import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { beforeAll, describe, expect, it } from "vitest";
import {
  assertExportComplete,
  detectExportDeferred,
  exportCompletionIssues,
  recordExportDeferred,
  resolveExportDeferred,
} from "../src/lib/pipeline/session-split.js";

const supabase = (path: string) =>
  fileURLToPath(new URL(`../../../supabase/${path}`, import.meta.url).href);
let pg: PGlite;
const db = {
  query: async <R>(text: string, params?: unknown[]) => {
    const { rows } = await pg.query<R>(text, params);
    return { rows };
  },
};
const key = (s: string) =>
  `v1/ws=w/m=m/h=claude/d=2026/10/30/s=${s}/manifest.json`;
const scope = { orgId: "o" };

beforeAll(async () => {
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readFileSync(supabase("tests/supabase_shim.sql"), "utf8"));
  await pg.exec(
    readFileSync(
      supabase("bootstrap/session_directory_search_worker.sql"),
      "utf8",
    ),
  );
  await pg.exec(
    readFileSync(
      supabase("bootstrap/session_token_verifier.sql"),
      "utf8",
    ),
  );
  await pg.exec(
    readFileSync(
      supabase("bootstrap/session_ingest_roles.sql"),
      "utf8",
    ),
  );
  for (const file of readdirSync(supabase("migrations/")).sort())
    await pg.exec(readFileSync(supabase(`migrations/${file}`), "utf8"));
});

describe("export completion", () => {
  it("a deferred straddling session is incomplete until its records land", async () => {
    await expect(assertExportComplete(db, scope)).resolves.toBeUndefined();
    await recordExportDeferred(db, {
      orgId: "o",
      workspaceId: "w",
      manifestKey: key("a"),
      reason: "straddling_session",
    });
    await recordExportDeferred(db, {
      orgId: "o",
      workspaceId: "w",
      manifestKey: key("a"),
      reason: "straddling_session",
    });
    expect(await detectExportDeferred(db, scope)).toEqual([
      {
        type: "export_deferred",
        workspaceId: "w",
        manifestKey: key("a"),
        reason: "straddling_session",
      },
    ]);
    await expect(assertExportComplete(db, scope)).rejects.toMatchObject({
      code: "export_incomplete",
      issues: [{ type: "export_deferred", manifestKey: key("a") }],
    });

    await resolveExportDeferred(db, { orgId: "o", manifestKey: key("a") });
    await expect(assertExportComplete(db, scope)).resolves.toBeUndefined();

    // A later deferral of the same session counts again.
    await recordExportDeferred(db, {
      orgId: "o",
      workspaceId: "w",
      manifestKey: key("a"),
      reason: "straddling_session",
    });
    expect(await detectExportDeferred(db, scope)).toHaveLength(1);
    await resolveExportDeferred(db, { orgId: "o", manifestKey: key("a") });
  });

  it("is scoped to one org and, when asked, one workspace", async () => {
    await recordExportDeferred(db, {
      orgId: "other",
      workspaceId: "w",
      manifestKey: key("b"),
      reason: "straddling_session",
    });
    await recordExportDeferred(db, {
      orgId: "o",
      workspaceId: "w2",
      manifestKey: key("c"),
      reason: "straddling_session",
    });
    expect(await detectExportDeferred(db, scope)).toHaveLength(1);
    expect(
      await detectExportDeferred(db, { orgId: "o", workspaceId: "w" }),
    ).toEqual([]);
    expect(await detectExportDeferred(db, { orgId: "other" })).toHaveLength(1);
    await resolveExportDeferred(db, { orgId: "o", manifestKey: key("c") });
  });

  it("reports splits from the real session_index query alongside deferrals", async () => {
    const columns = (
      await pg.query<{
        column_name: string;
        is_nullable: string;
        column_default: string | null;
        data_type: string;
      }>(
        `SELECT column_name, is_nullable, column_default, data_type FROM information_schema.columns
        WHERE table_schema = 'sessions' AND table_name = 'session_index'`,
      )
    ).rows.filter((c) => c.is_nullable === "NO" && c.column_default === null);
    const insert = async (machine: string, session = "split") => {
      const values: Record<string, unknown> = {
        org_id: "o",
        workspace_id: "w",
        harness: "claude",
        session_id: session,
        machine_id: machine,
        storage_provider: "s3",
      };
      for (const c of columns) {
        if (c.column_name in values) continue;
        values[c.column_name] = c.data_type.includes("timestamp")
          ? "2026-10-30T00:00:00Z"
          : /int|numeric/.test(c.data_type)
            ? 1
            : c.data_type === "uuid"
              ? "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"
              : c.data_type === "boolean"
                ? false
                : "x";
      }
      const names = Object.keys(values);
      await pg.query(
        `INSERT INTO sessions.session_index (${names.join(",")}) VALUES (${names.map((_, i) => `$${i + 1}`).join(",")})`,
        Object.values(values),
      );
    };
    await insert("machine-a");
    await insert("unknown-origin-7");
    // The same session id on two real machines is legitimate.
    await insert("machine-a", "two-machines");
    await insert("machine-b", "two-machines");
    await recordExportDeferred(db, {
      orgId: "o",
      workspaceId: "w",
      manifestKey: key("d"),
      reason: "straddling_session",
    });

    const issues = await exportCompletionIssues(db, { orgId: "o" });
    expect(issues.map((i) => i.type).sort()).toEqual([
      "export_deferred",
      "session_machine_split",
    ]);
    expect(
      issues.find((i) => i.type === "session_machine_split"),
    ).toMatchObject({
      sessionId: "split",
      machineIds: ["machine-a", "unknown-origin-7"],
    });
  });
});
