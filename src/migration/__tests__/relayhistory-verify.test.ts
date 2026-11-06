import { describe, expect, test } from "bun:test";
import {
  ALLOWED_SCHEMA_DIFFERENCES,
  allowedDifferences,
  isPerDatabaseState,
  missingObjects,
  missingPrivileges,
  schemaDifferences,
  addFingerprint,
  chunkFingerprintSql,
  chunkUpperBoundSql,
  fingerprintSql,
  quoteIdent,
  selectUsableKey,
  type CatalogObjects,
  type ColumnDefinition,
  type RoleAccess,
} from "../relayhistory-verify.ts";

describe("relayhistory migration verifier", () => {
  test("quotes catalog identifiers and hashes only source columns", () => {
    expect(quoteIdent('odd"name')).toBe('"odd""name"');
    const sql = fingerprintSql("sessions", "machines", ["org_id", "workspace_id"]);
    expect(sql).toContain('SELECT "org_id", "workspace_id" FROM "sessions"."machines"');
    expect(sql).toContain("hashtextextended");
  });

  test("builds deterministic composite-key ranges without an unbounded table scan", () => {
    const upper = chunkUpperBoundSql("sessions", "events", ["org_id", "event_id"], true);
    expect(upper).toContain('("org_id", "event_id") > ($1, $2)');
    expect(upper).toContain('ORDER BY "org_id", "event_id" LIMIT $3');
    expect(upper).toContain('ORDER BY "org_id" DESC, "event_id" DESC LIMIT 1');

    const fingerprint = chunkFingerprintSql(
      "sessions",
      "events",
      ["org_id", "event_id", "content"],
      ["org_id", "event_id"],
      true,
    );
    expect(fingerprint).toContain('("org_id", "event_id") > ($1, $2)');
    expect(fingerprint).toContain('("org_id", "event_id") <= ($3, $4)');
    expect(fingerprint).not.toContain("FROM (SELECT *");
  });

  test("fails closed when source and target lack the same primary key", () => {
    expect(selectUsableKey([], [])).toBeNull();
    expect(selectUsableKey(["id"], [])).toBeNull();
    expect(selectUsableKey(["org_id", "id"], ["id", "org_id"])).toBeNull();
    expect(selectUsableKey(["org_id", "id"], ["org_id", "id"])).toEqual(["org_id", "id"]);
  });

  test("combines chunk fingerprints without losing 64-bit precision", () => {
    expect(addFingerprint(
      { count: "9007199254740992", hash1: "9223372036854775807", hash2: "-5" },
      { count: "2", hash1: "10", hash2: "12" },
    )).toEqual({ count: "9007199254740994", hash1: "9223372036854775817", hash2: "7" });
  });
});

describe("per-database state", () => {
  test("skips the runner ledger and rollout progress, nothing else", () => {
    expect(isPerDatabaseState("__migrations")).toBe(true);
    expect(isPerDatabaseState("session_rollup_rollout")).toBe(true);
    expect(isPerDatabaseState("delivery_rollout")).toBe(true);
    expect(isPerDatabaseState("session_rollups")).toBe(false);
    expect(isPerDatabaseState("auth_sessions")).toBe(false);
  });
});

const column = (overrides: Partial<ColumnDefinition> = {}): ColumnDefinition => ({
  type: "text",
  notNull: false,
  default: null,
  generated: "",
  identity: "",
  collation: "default",
  ...overrides,
});

/** A small legacy-shaped catalog; each call is a fresh, independently mutable copy. */
const catalog = (extra: Partial<Record<"index" | "trigger", string>> = {}): CatalogObjects => ({
  indexes: new Map([
    ["convergence_events_content_prefix_trgm_idx", "CREATE INDEX ... USING gin"],
    ...(extra.index ? [[extra.index, "x"] as [string, string]] : []),
  ]),
  constraints: new Map([["machines.machines_pkey", "PRIMARY KEY (org_id, machine_id)"]]),
  functions: new Set(["sessions.accept_delivery_batch(jsonb)", "sessions.rollup_sum(bigint)"]),
  triggers: new Map([
    ["delivery_records.delivery_session_projection", "CREATE TRIGGER ..."],
    ...(extra.trigger ? [[extra.trigger, "y"] as [string, string]] : []),
  ]),
  columns: new Map([
    ["machines", new Map([
      ["org_id", column({ notNull: true })],
      ["workspace_id", column({ notNull: true })],
      ["hostname", column()],
    ])],
    ["jobs", new Map([
      ["id", column({ type: "bigint", notNull: true, identity: "a", collation: null })],
      ["name", column({ collation: "C" })],
      ["upper_name", column({ default: "upper(name)", generated: "s" })],
      ["state", column({ type: "sessions.state", notNull: true, default: "'queued'::sessions.state", collation: null })],
    ])],
  ]),
  // Aggregates have a signature but no pg_get_functiondef.
  functionDefinitions: new Map([["sessions.accept_delivery_batch(jsonb)", "CREATE OR REPLACE FUNCTION ... $1 ..."]]),
  tables: new Map([
    ["machines", { rowSecurity: false, forceRowSecurity: false }],
    ["jobs", { rowSecurity: true, forceRowSecurity: false }],
    // The shared legacy auth table: Neon leaves RLS off; the Supabase path
    // enables it as the reviewed hardening.
    ["auth_sessions", { rowSecurity: false, forceRowSecurity: false }],
  ]),
  policies: new Map([["jobs.jobs_org", '{"cmd":"SELECT","qual":"(org_id = \'a\'::text)"}']]),
  types: new Map([
    ["state", '{"kind":"e","labels":["queued","done"]}'],
    ["label", '{"kind":"d","base":"text","notNull":true}'],
    ["pair", '{"kind":"c","attributes":["left_id bigint","right_id text"]}'],
  ]),
});

describe("schema objects beyond columns", () => {

  test("passes a target that has everything, extra objects included", () => {
    expect(missingObjects(catalog(), catalog({ index: "si_org_recent" }))).toEqual([]);
  });

  test("fails on a missing or redefined trigger, index, constraint or function", () => {
    const target = catalog();
    target.triggers.delete("delivery_records.delivery_session_projection");
    target.indexes.set("convergence_events_content_prefix_trgm_idx", "CREATE INDEX ... USING btree");
    target.constraints.clear();
    target.functions.clear();
    expect(missingObjects(catalog(), target)).toEqual([
      "different index convergence_events_content_prefix_trgm_idx",
      "missing constraint machines.machines_pkey",
      "missing function sessions.accept_delivery_batch(jsonb)",
      "missing function sessions.rollup_sum(bigint)",
      "missing trigger delivery_records.delivery_session_projection",
    ]);
  });
});

describe("schema definitions, not just names", () => {
  test("passes an identical target and a superset target", () => {
    expect(schemaDifferences(catalog(), catalog())).toEqual([]);
    const target = catalog();
    target.columns.get("machines")!.set("client_version", column());
    target.columns.set("pipeline_events", new Map([["trace_id", column({ type: "uuid", notNull: true })]]));
    target.tables.set("pipeline_events", { rowSecurity: true, forceRowSecurity: false });
    target.policies.set("machines.machines_member_read", "{}");
    target.types.set("pipeline_stage", "{}");
    target.functionDefinitions.set("sessions.member_of(text,text[])", "CREATE ...");
    expect(schemaDifferences(catalog(), target)).toEqual([]);
  });

  test("fails on each column attribute that differs, and on a missing column or table", () => {
    const target = catalog();
    const jobs = target.columns.get("jobs")!;
    jobs.set("id", column({ type: "integer", notNull: true, identity: "d", collation: null }));
    jobs.set("name", column({ collation: "POSIX" }));
    jobs.set("upper_name", column());
    jobs.set("state", column({ type: "sessions.state", notNull: true, collation: null }));
    target.columns.get("machines")!.set("hostname", column({ notNull: true }));
    target.columns.get("machines")!.delete("org_id");
    expect(schemaDifferences(catalog(), target)).toEqual([
      "different column collation jobs.name: source C, target POSIX",
      "different column default jobs.state: source 'queued'::sessions.state, target none",
      "different column default jobs.upper_name: source upper(name), target none",
      "different column generated jobs.upper_name: source s, target none",
      "different column identity jobs.id: source a, target d",
      "different column nullability machines.hostname: source nullable, target NOT NULL",
      "different column type jobs.id: source bigint, target integer",
      "missing column machines.org_id",
    ]);
    const looser = catalog();
    looser.columns.get("machines")!.set("org_id", column());
    expect(schemaDifferences(catalog(), looser)).toEqual([
      "different column nullability machines.org_id: source NOT NULL, target nullable",
    ]);
    const noTable = catalog();
    noTable.tables.delete("jobs");
    noTable.columns.delete("jobs");
    expect(schemaDifferences(catalog(), noTable)).toEqual(["missing table jobs"]);
  });

  test("fails on a redefined function body, not on a function only the target has", () => {
    const target = catalog();
    target.functionDefinitions.set("sessions.accept_delivery_batch(jsonb)", "CREATE OR REPLACE FUNCTION ... $2 ...");
    expect(schemaDifferences(catalog(), target)).toEqual([
      "different function definition sessions.accept_delivery_batch(jsonb)",
    ]);
  });

  test("fails on row security, forced row security and policy drift", () => {
    const target = catalog();
    target.tables.set("jobs", { rowSecurity: false, forceRowSecurity: true });
    target.policies.set("jobs.jobs_org", '{"cmd":"ALL","qual":"true"}');
    const missingPolicy = catalog();
    missingPolicy.policies.clear();
    expect(schemaDifferences(catalog(), target)).toEqual([
      "different forced row security jobs: source disabled, target enabled",
      "different policy jobs.jobs_org",
      "different row security jobs: source enabled, target disabled",
    ]);
    expect(schemaDifferences(catalog(), missingPolicy)).toEqual(["missing policy jobs.jobs_org"]);
  });

  test("fails on enum, domain and composite type drift", () => {
    const target = catalog();
    target.types.set("state", '{"kind":"e","labels":["done","queued"]}');
    target.types.set("label", '{"kind":"d","base":"text","notNull":false}');
    target.types.delete("pair");
    expect(schemaDifferences(catalog(), target)).toEqual([
      "different type label",
      "different type state",
      "missing type pair",
    ]);
  });
});

describe("allowlist", () => {
  // The 96Bulls target as it is meant to be: RLS on machines, workspace default.
  const bullsTarget = () => {
    const target = catalog();
    target.tables.set("machines", { rowSecurity: true, forceRowSecurity: false });
    target.tables.set("auth_sessions", { rowSecurity: true, forceRowSecurity: false });
    target.columns.get("machines")!.set("workspace_id", column({ notNull: true, default: "'default'::text" }));
    return target;
  };

  test("holds exactly the three reviewed differences", () => {
    expect([...ALLOWED_SCHEMA_DIFFERENCES].sort()).toEqual([
      "different column default machines.workspace_id: source none, target 'default'::text",
      "different row security auth_sessions: source disabled, target enabled",
      "different row security machines: source disabled, target enabled",
    ]);
  });

  test("passes the intended target and reports what it allowed", () => {
    expect(missingObjects(catalog(), bullsTarget())).toEqual([]);
    expect(allowedDifferences(catalog(), bullsTarget())).toEqual([...ALLOWED_SCHEMA_DIFFERENCES].sort());
  });

  test("allows nothing near it: another default, forced RLS, another table or attribute", () => {
    const otherDefault = bullsTarget();
    otherDefault.columns.get("machines")!.set("workspace_id", column({ notNull: true, default: "'ws'::text" }));
    expect(missingObjects(catalog(), otherDefault)).toEqual([
      "different column default machines.workspace_id: source none, target 'ws'::text",
    ]);
    const forced = bullsTarget();
    forced.tables.set("machines", { rowSecurity: true, forceRowSecurity: true });
    expect(missingObjects(catalog(), forced)).toEqual([
      "different forced row security machines: source disabled, target enabled",
    ]);
    const nullable = bullsTarget();
    nullable.columns.get("machines")!.set("workspace_id", column({ default: "'default'::text" }));
    expect(missingObjects(catalog(), nullable)).toEqual([
      "different column nullability machines.workspace_id: source NOT NULL, target nullable",
    ]);
    const otherTable = catalog();
    otherTable.tables.set("jobs", { rowSecurity: true, forceRowSecurity: true });
    otherTable.columns.get("machines")!.set("hostname", column({ default: "'default'::text" }));
    expect(missingObjects(catalog(), otherTable)).toEqual([
      "different column default machines.hostname: source none, target 'default'::text",
      "different forced row security jobs: source disabled, target enabled",
    ]);
  });
});

describe("runtime-role grants", () => {
  const role = (name: string, privileges: string[], rowSecurityExempt: string[] = []): RoleAccess => ({
    name,
    exists: true,
    privileges: new Set(privileges),
    rowSecurityExempt: new Set(rowSecurityExempt),
  });
  const neonGrants = [
    "USAGE on schema sessions",
    "SELECT on table machines",
    "INSERT on table machines",
    "USAGE on sequence jobs_id_seq",
    "EXECUTE on function sessions.accept_delivery_batch(jsonb)",
  ];

  test("passes when the target role holds every source privilege, and more", () => {
    expect(missingPrivileges(
      role("neon_app", neonGrants),
      role("supabase_app", [...neonGrants, "TRUNCATE on table machines"]),
      catalog(),
      catalog(),
    )).toEqual([]);
  });

  test("fails on every privilege the target role lacks", () => {
    expect(missingPrivileges(
      role("neon_app", neonGrants),
      role("supabase_app", ["SELECT on table machines"]),
      catalog(),
      catalog(),
    )).toEqual([
      "target role supabase_app lacks EXECUTE on function sessions.accept_delivery_batch(jsonb)",
      "target role supabase_app lacks INSERT on table machines",
      "target role supabase_app lacks USAGE on schema sessions",
      "target role supabase_app lacks USAGE on sequence jobs_id_seq",
    ]);
  });

  test("fails when either role does not exist", () => {
    const absent = { ...role("ghost", []), exists: false };
    expect(missingPrivileges(absent, role("supabase_app", neonGrants), catalog(), catalog()))
      .toEqual(["source role ghost does not exist"]);
    expect(missingPrivileges(role("neon_app", neonGrants), absent, catalog(), catalog()))
      .toEqual(["target role ghost does not exist"]);
  });

  test("the allowlisted machines RLS still needs a target role that bypasses it", () => {
    const target = catalog();
    target.tables.set("machines", { rowSecurity: true, forceRowSecurity: false });
    expect(missingPrivileges(role("neon_app", neonGrants), role("supabase_app", neonGrants), catalog(), target))
      .toEqual(["target role supabase_app is subject to row security on machines; source role neon_app is not"]);
    expect(missingPrivileges(
      role("neon_app", neonGrants),
      role("supabase_app", neonGrants, ["machines"]),
      catalog(),
      target,
    )).toEqual([]);
    // jobs has RLS on both sides and neither role bypasses it: no regression.
    expect(missingPrivileges(
      role("neon_app", neonGrants, ["jobs"]),
      role("supabase_app", neonGrants, ["machines"]),
      catalog(),
      target,
    )).toEqual(["target role supabase_app is subject to row security on jobs; source role neon_app is not"]);
  });
});
