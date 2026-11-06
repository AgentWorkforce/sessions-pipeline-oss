/**
 * The migrator's read-only guards. Each runs under the migrator's advisory lock before
 * any write (before the ledger or marker tables are created, before the pending marker
 * upsert, before any step), and the runtime-role allowlist runs again after the last
 * migration, before phase 2 can mark PASS. Every refusal names each violation and writes
 * nothing.
 */
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MIGRATION_SENTINELS } from "./migration-sentinels.ts";

export interface GuardClient {
  query<R = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: R[] }>;
}
export interface GuardStep {
  name: string;
  path: string;
  /** A step's own sentinel (test-injected steps); otherwise MIGRATION_SENTINELS[name]. */
  sentinel?: { evidence: string; sql: string };
}

export function guardError(code: string, message: string): Error {
  return Object.assign(new Error(`96bulls: ${message}`), { code });
}

// 20261030020000's only post-release change replaces its transient role guard with
// the catalog-exact PG16+ Supabase creator-edge check. It changes no persistent
// object. Existing databases can therefore retain the one known historical ledger
// checksum while fresh databases execute and record the reviewed file below. Binding
// both hashes prevents this exception from silently covering a future edit.
export const MIGRATION_CHECKSUM_ALIASES = [
  {
    migration: "upstream:20261030020000_session_directory_short_summary.sql",
    ledger_sha:
      "c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4",
    accepted_as_alias_of:
      "b0ddc7de8c4cff9454638c52563f37f1e1380ed577e2a7535f67ddc2c0b758ce",
  },
] as const;

export function migrationChecksumMatches(
  name: string,
  recorded: string,
  actual: string,
): boolean {
  return (
    recorded === actual ||
    MIGRATION_CHECKSUM_ALIASES.some(
      (a) =>
        a.migration === name &&
        a.ledger_sha === recorded &&
        a.accepted_as_alias_of === actual,
    )
  );
}

export function migrationChecksumAlias(
  name: string,
  recorded: string,
  actual: string,
): (typeof MIGRATION_CHECKSUM_ALIASES)[number] | undefined {
  return MIGRATION_CHECKSUM_ALIASES.find(
    (a) =>
      a.migration === name &&
      a.ledger_sha === recorded &&
      a.accepted_as_alias_of === actual,
  );
}

// ---------------------------------------------------------------------------
// Runtime role: an exact least-privilege allowlist
// ---------------------------------------------------------------------------

export interface RuntimeRoleConfig {
  mode: "local" | "selfhosted" | "cloudsql" | "supabase";
  role: string;
  /** HOOPDB_RUNTIME_ROLE_LOGIN: whether the role logs in itself (true) or is reached through a workload role (false). */
  login?: boolean;
  /** HOOPDB_RUNTIME_WORKLOAD_ROLE: the one role allowed to be a member of the runtime role (no ADMIN OPTION). */
  workload?: string;
}

export interface RuntimeTables {
  ledger: string;
  marker: string;
}

/** The exact database surface shared by the 96Bulls service pods. */
export const RUNTIME_APPLICATION_TABLE_PRIVILEGES = {
  "sessions.session_index": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.machines": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.pipeline_events": ["SELECT", "INSERT", "DELETE"],
  "sessions.pipeline_dead_letters": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.pipeline_jobs": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.repos": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.repo_names": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_index_ext": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_summaries": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_verdicts": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_users": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_machines": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_rollout": ["SELECT"],
  "sessions.session_directory_resurrection_fences": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_agents": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_relationships": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_event_times": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_directory_event_pending": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.manifest_readiness": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.export_deferred_sessions": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.claimless_uses": ["SELECT", "INSERT", "UPDATE"],
  "sessions.backfill_activation_ready": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.migration_assertion_uses": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.migration_assertion_chunks": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.migration_ingest_audit": ["SELECT", "INSERT"],
  "sessions.migration_base_read_uses": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.claimless_window_activations": ["SELECT", "INSERT"],
  "sessions.session_briefs": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_brief_wakeups": ["SELECT", "INSERT", "UPDATE", "DELETE"],
  "sessions.session_brief_attempts": ["SELECT", "INSERT", "UPDATE", "DELETE"],
} as const satisfies Record<string, readonly string[]>;

export const RUNTIME_APPLICATION_SEQUENCES = [
  "sessions.pipeline_jobs_id_seq",
  "sessions.migration_ingest_audit_id_seq",
  "sessions.session_verdicts_id_seq",
] as const;

export const RUNTIME_APPLICATION_RLS_TABLES = [
  "sessions.session_index",
  "sessions.machines",
  "sessions.pipeline_events",
  "sessions.pipeline_dead_letters",
  "sessions.pipeline_jobs",
  "sessions.repos",
  "sessions.repo_names",
  "sessions.session_index_ext",
  "sessions.session_summaries",
  "sessions.session_verdicts",
  "sessions.session_directory",
  "sessions.session_directory_users",
  "sessions.session_directory_machines",
  "sessions.session_directory_resurrection_fences",
  "sessions.session_directory_agents",
  "sessions.session_directory_relationships",
  "sessions.session_directory_event_times",
  "sessions.session_directory_event_pending",
  "sessions.manifest_readiness",
  "sessions.export_deferred_sessions",
  "sessions.claimless_uses",
  "sessions.backfill_activation_ready",
  "sessions.migration_assertion_uses",
  "sessions.migration_assertion_chunks",
  "sessions.migration_ingest_audit",
  "sessions.migration_base_read_uses",
  "sessions.claimless_window_activations",
  "sessions.session_briefs",
  "sessions.session_brief_wakeups",
  "sessions.session_brief_attempts",
] as const;

export const RUNTIME_APPLICATION_FUNCTIONS = [
  "sessions.ack_session_brief_documents(uuid,jsonb)",
  "sessions.applied_repo_evidence(jsonb,jsonb)",
  "sessions.backfill_session_brief_documents(integer)",
  "sessions.canonical_remote(text)",
  "sessions.capture_session_replay_pages(text,text,text,text)",
  "sessions.claim_session_analysis_cache()",
  "sessions.claim_session_brief_documents(uuid,integer)",
  "sessions.claim_session_large_read(uuid,integer)",
  "sessions.clean_root_commit(text)",
  "sessions.discover_session_large_reads()",
  "sessions.enqueue_session_analysis_cache(text,text,text,text)",
  "sessions.enqueue_session_large_read(text,text,text,text)",
  "sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text)",
  "sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid)",
  "sessions.heartbeat_session_large_read(text,text,text,text,uuid)",
  "sessions.jwt_org_id()",
  "sessions.jwt_workspace_ids()",
  "sessions.legacy_remote(text)",
  "sessions.lock_repo_catalog(text)",
  "sessions.member_of(text,text[])",
  "sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid)",
  "sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)",
  "sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)",
  "sessions.repo_backfill_report()",
  "sessions.repo_backfill_step(integer,text)",
  "sessions.repo_group(text,uuid)",
  "sessions.repo_live(text,uuid)",
  "sessions.repo_visibility_ids(text,uuid)",
  "sessions.repos_maintain(integer)",
  "sessions.resolve_repo(text,text,text,timestamp with time zone)",
  "sessions.retry_session_brief_documents(uuid,text)",
  "sessions.retry_session_large_read(text,text,text,text,uuid,text)",
  "sessions.route_session_large_read(text,text,text,text)",
  "sessions.session_directory_project_text(text,text,text)",
  "sessions.session_directory_search_text(text,text,text,text,text,text,text,text)",
  "sessions.session_index_batch(jsonb)",
  "sessions.session_index_scrub_sticky()",
  "sessions.session_publication_is_managed(text,text,text,text,text)",
  "sessions.session_publication_lock(jsonb)",
  "sessions.session_publication_probe(jsonb)",
  "sessions.session_publication_register_object(jsonb)",
  "sessions.set_session_state(text,text,text,text,text,text,timestamp with time zone)",
  "sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamp with time zone,text)",
  "sessions.upsert_machine(text,text,text,text,text,text,timestamp with time zone)",
  "sessions.upsert_session_index(jsonb)",
] as const;

/**
 * The pods use one reviewed, multi-tenant backend identity. This policy is deliberately
 * all-row for that identity: tenant isolation for user-facing requests remains in the
 * authenticated RPCs and application filters, while this role is constrained by exact
 * table/function ACLs, NOBYPASSRLS, no ownership, and no DDL. It must never be granted
 * to a user-facing or otherwise untrusted database role.
 */
export const RUNTIME_APPLICATION_POLICY = "bulls96_trusted_backend";

export interface RoleGrantEdge {
  rolname: string;
  grantor: string;
  grantor_super: boolean;
  admin_option: boolean;
  inherit_option: boolean;
  set_option: boolean;
}

export interface DatabaseTempEvidence {
  effective: boolean;
  direct: boolean;
  public: boolean;
  public_grantable: boolean;
}

/** The one unavoidable PG16+ Supabase creator edge. No names are configurable. */
export function isSupabaseCreatorEdge(
  cfg: Pick<RuntimeRoleConfig, "mode">,
  serverVersion: number,
  edge: RoleGrantEdge,
): boolean {
  return (
    cfg.mode === "supabase" &&
    serverVersion >= 160000 &&
    edge.rolname === "postgres" &&
    edge.grantor === "supabase_admin" &&
    edge.grantor_super &&
    edge.admin_option &&
    !edge.inherit_option &&
    !edge.set_option
  );
}

/**
 * The one optional workload membership. On PG16+ both per-grant options must keep
 * ordinary privilege inheritance and SET ROLE enabled; on PG15 the query projects
 * the member role's INHERIT attribute and the platform's implicit SET capability.
 * A hosted non-superuser creator is frozen to postgres. Every other grantor must be
 * a catalog-verified superuser.
 */
export function isRuntimeWorkloadEdge(
  cfg: Pick<RuntimeRoleConfig, "mode" | "workload">,
  edge: RoleGrantEdge,
  exactSupabaseCreatorPresent = false,
): boolean {
  return (
    cfg.workload !== undefined &&
    edge.rolname === cfg.workload &&
    !edge.admin_option &&
    edge.inherit_option &&
    edge.set_option &&
    (edge.grantor_super ||
      (cfg.mode === "supabase" &&
        exactSupabaseCreatorPresent &&
        edge.grantor === "postgres"))
  );
}

/** Supabase's frozen database baseline leaves TEMPORARY granted to PUBLIC. */
export function isSupabasePublicTemp(
  cfg: Pick<RuntimeRoleConfig, "mode">,
  evidence: DatabaseTempEvidence,
): boolean {
  return (
    cfg.mode === "supabase" &&
    evidence.effective &&
    !evidence.direct &&
    evidence.public &&
    !evidence.public_grantable
  );
}

/**
 * Approved for the runtime role, and nothing else:
 *   USAGE on schema sessions; SELECT on the ledger and the readiness marker; the exact
 *   application table/function/sequence ACL and RLS policies declared above;
 *   CONNECT on the current database, which is REQUIRED (effective), as it is for the
 *   workload role when one is configured.
 * Refused, among everything else: effective TEMPORARY on the database, except the exact
 * PUBLIC-derived Supabase baseline (see the runbook), and EXECUTE on any unlisted
 * sessions function. Trigger functions remain excluded because triggers fire without
 * their caller holding EXECUTE.
 *
 * Coverage, stated exactly:
 *   current database  effective privileges in every non-system schema: tables, views,
 *                     sequences (any privilege), SECURITY DEFINER functions (EXECUTE),
 *                     every sessions function (EXECUTE), CREATE on any schema, CREATE
 *                     and TEMPORARY on the database; default privileges in sessions.
 *   every database    direct references naming the role (pg_shdepend): ownership,
 *                     ACL entries, default ACLs, policies.
 *   not covered       PUBLIC-derived privileges in other databases (for example
 *                     CONNECT to another database through PUBLIC: keep one database
 *                     per cluster, or revoke PUBLIC CONNECT there), and SECURITY INVOKER
 *                     functions outside sessions, which run with the caller's own
 *                     privileges (extension functions are reviewed with the extension).
 * phase "pre" (before any write) ignores PUBLIC-derived EXECUTE on sessions functions:
 * the migrator revokes that in the same transaction as validation. phase "post" counts
 * every EXECUTE.
 */
export async function runtimeRoleViolations(
  client: GuardClient,
  cfg: RuntimeRoleConfig,
  tables: RuntimeTables,
  phase: "pre" | "post" = "post",
): Promise<string[]> {
  const out: string[] = [];
  const serverVersion = Number(
    (
      await client.query<{ server_version_num: string }>(
        "SELECT current_setting('server_version_num') AS server_version_num",
      )
    ).rows[0]?.server_version_num ?? "0",
  );
  const role = (
    await client.query<{
      oid: number;
      rolsuper: boolean;
      rolcreatedb: boolean;
      rolcreaterole: boolean;
      rolreplication: boolean;
      rolbypassrls: boolean;
      rolcanlogin: boolean;
    }>(
      "SELECT oid::int AS oid, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname = $1",
      [cfg.role],
    )
  ).rows[0];
  if (!role) return [`role ${cfg.role} does not exist`];
  for (const [flag, label] of [
    ["rolsuper", "SUPERUSER"],
    ["rolcreatedb", "CREATEDB"],
    ["rolcreaterole", "CREATEROLE"],
    ["rolreplication", "REPLICATION"],
    ["rolbypassrls", "BYPASSRLS"],
  ] as const)
    if (role[flag]) out.push(label);
  if (cfg.login === undefined)
    out.push(
      "HOOPDB_RUNTIME_ROLE_LOGIN must be set for a dedicated runtime role",
    );
  else if (role.rolcanlogin !== cfg.login)
    out.push(
      role.rolcanlogin
        ? "LOGIN (configured NOLOGIN)"
        : "NOLOGIN (configured LOGIN)",
    );

  for (const m of (
    await client.query<{ rolname: string }>(
      "SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE m.member = $1::oid ORDER BY 1",
      [role.oid],
    )
  ).rows)
    out.push(`member of ${m.rolname}`);
  const members =
    serverVersion >= 160000
      ? (
          await client.query<{
            rolname: string;
            grantor: string;
            grantor_super: boolean;
            admin_option: boolean;
            inherit_option: boolean;
            set_option: boolean;
          }>(
            `SELECT r.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super,
                  m.admin_option, m.inherit_option, m.set_option
             FROM pg_auth_members m
             JOIN pg_roles r ON r.oid = m.member
             JOIN pg_roles g ON g.oid = m.grantor
            WHERE m.roleid = $1::oid ORDER BY 1, 2`,
            [role.oid],
          )
        ).rows
      : (
          await client.query<{
            rolname: string;
            grantor: string;
            grantor_super: boolean;
            admin_option: boolean;
            inherit_option: boolean;
            set_option: boolean;
          }>(
            `SELECT r.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super,
                  m.admin_option, r.rolinherit AS inherit_option, true AS set_option
             FROM pg_auth_members m
             JOIN pg_roles r ON r.oid = m.member
             JOIN pg_roles g ON g.oid = m.grantor
            WHERE m.roleid = $1::oid ORDER BY 1, 2`,
            [role.oid],
          )
        ).rows;
  const supabaseCreatorPresent = members.some((m) =>
    isSupabaseCreatorEdge(cfg, serverVersion, m),
  );
  for (const m of members) {
    // PostgreSQL 16+ necessarily gives a non-superuser CREATEROLE creator an ADMIN
    // membership in every custom role it creates. Supabase's bootstrap superuser is
    // the recorded grantor, while postgres receives neither INHERIT nor SET. This one
    // catalog-exact edge is compatible only with Supabase and may coexist only with
    // the separately validated workload edge. No name is environment-configurable.
    const supabaseCreatorEdge = isSupabaseCreatorEdge(cfg, serverVersion, m);
    if (
      supabaseCreatorEdge ||
      isRuntimeWorkloadEdge(cfg, m, supabaseCreatorPresent)
    )
      continue;
    out.push(
      `granted to ${m.rolname} by ${m.grantor} with ADMIN=${m.admin_option} INHERIT=${m.inherit_option} SET=${m.set_option}`,
    );
  }

  const applicationTables = Object.keys(RUNTIME_APPLICATION_TABLE_PRIVILEGES);
  const ids = (
    await client.query<{
      db: number;
      sessions: number | null;
      ledger: number | null;
      marker: number | null;
      relations: number[];
      functions: number[];
      policies: number[];
    }>(
      `SELECT (SELECT oid::int FROM pg_database WHERE datname = current_database()) AS db,
              to_regnamespace('sessions')::oid::int AS sessions,
              to_regclass($1)::oid::int AS ledger, to_regclass($2)::oid::int AS marker,
              ARRAY(SELECT to_regclass(x)::oid::int FROM unnest($3::text[]) x WHERE to_regclass(x) IS NOT NULL) AS relations,
              ARRAY(SELECT to_regprocedure(x)::oid::int FROM unnest($4::text[]) x WHERE to_regprocedure(x) IS NOT NULL) AS functions,
              ARRAY(SELECT p.oid::int FROM pg_policy p WHERE p.polname = $5 AND p.polrelid = ANY(
                ARRAY(SELECT to_regclass(x)::oid FROM unnest($6::text[]) x WHERE to_regclass(x) IS NOT NULL))) AS policies`,
      [
        tables.ledger,
        tables.marker,
        [...applicationTables, ...RUNTIME_APPLICATION_SEQUENCES],
        [...RUNTIME_APPLICATION_FUNCTIONS],
        RUNTIME_APPLICATION_POLICY,
        [...RUNTIME_APPLICATION_RLS_TABLES],
      ],
    )
  ).rows[0]!;
  // Every owner, ACL, policy and initial-privilege reference naming the role, in every
  // database of the cluster (pg_shdepend). Only the approved ACL entries may remain.
  for (const d of (
    await client.query<{
      dbid: number;
      cls: string;
      objid: number;
      objsubid: number;
      deptype: string;
      descr: string;
    }>(
      `SELECT d.dbid::int AS dbid, d.classid::regclass::text AS cls, d.objid::int AS objid, d.objsubid, d.deptype::text AS deptype,
              CASE WHEN d.dbid IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
                   THEN pg_describe_object(d.classid, d.objid, d.objsubid)
                   ELSE format('an object in database %I', db.datname) END AS descr
         FROM pg_shdepend d LEFT JOIN pg_database db ON db.oid = d.dbid
        WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = $1::oid
        ORDER BY 6`,
      [role.oid],
    )
  ).rows) {
    const approved =
      d.deptype === "a" &&
      d.objsubid === 0 &&
      ((d.dbid === ids.db &&
        d.cls === "pg_namespace" &&
        d.objid === ids.sessions) ||
        (d.dbid === ids.db &&
          d.cls === "pg_class" &&
          (d.objid === ids.ledger ||
            d.objid === ids.marker ||
            ids.relations.includes(d.objid))) ||
        (d.dbid === ids.db &&
          d.cls === "pg_proc" &&
          ids.functions.includes(d.objid)) ||
        (d.dbid === 0 && d.cls === "pg_database" && d.objid === ids.db));
    const approvedPolicy =
      d.deptype === "r" &&
      d.dbid === ids.db &&
      d.cls === "pg_policy" &&
      ids.policies.includes(d.objid);
    if (approved || approvedPolicy) continue;
    const verb =
      d.deptype === "o"
        ? "owns"
        : d.deptype === "a"
          ? "has privileges on"
          : d.deptype === "r"
            ? "is named in policy"
            : "is referenced by";
    out.push(`${verb} ${d.descr}`);
  }
  // Exact privileges on the approved objects (direct aclitems).
  const direct = await client.query<{
    what: string;
    privilege_type: string;
    is_grantable: boolean;
  }>(
    `SELECT 'schema sessions' AS what, a.privilege_type, a.is_grantable FROM pg_namespace n, aclexplode(n.nspacl) a
      WHERE n.nspname = 'sessions' AND a.grantee = $1::oid
     UNION ALL
     SELECT c.oid::regclass::text, a.privilege_type, a.is_grantable FROM pg_class c, aclexplode(c.relacl) a
      WHERE c.oid IN (coalesce(to_regclass($2), 0), coalesce(to_regclass($3), 0)) AND a.grantee = $1::oid
     UNION ALL
     SELECT 'database ' || d.datname, a.privilege_type, a.is_grantable FROM pg_database d, aclexplode(d.datacl) a
      WHERE d.datname = current_database() AND a.grantee = $1::oid`,
    [role.oid, tables.ledger, tables.marker],
  );
  for (const p of direct.rows) {
    const ok =
      p.what === "schema sessions"
        ? p.privilege_type === "USAGE"
        : p.what.startsWith("database ")
          ? p.privilege_type === "CONNECT"
          : p.privilege_type === "SELECT";
    if (!ok || p.is_grantable)
      out.push(
        `${p.what}: ${p.privilege_type}${p.is_grantable ? " WITH GRANT OPTION" : ""} is not approved`,
      );
  }
  // Effective privileges, however obtained (PUBLIC, memberships, defaults), in every
  // non-system schema of the current database.
  const NONSYSTEM = `n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'`;
  const db = (
    await client.query<{
      create: boolean;
      temp: boolean;
      direct_temp: boolean;
      public_temp: boolean;
      public_temp_grantable: boolean;
      connect: boolean;
      wl_connect: boolean | null;
    }>(
      `SELECT has_database_privilege($1::oid, current_database(), 'CREATE') AS create,
              has_database_privilege($1::oid, current_database(), 'TEMPORARY') AS temp,
              EXISTS (SELECT 1 FROM pg_database x, aclexplode(coalesce(x.datacl, acldefault('d', x.datdba))) a
                       WHERE x.datname = current_database() AND a.grantee = $1::oid
                         AND a.privilege_type = 'TEMPORARY') AS direct_temp,
              EXISTS (SELECT 1 FROM pg_database x, aclexplode(coalesce(x.datacl, acldefault('d', x.datdba))) a
                       WHERE x.datname = current_database() AND a.grantee = 0
                         AND a.privilege_type = 'TEMPORARY') AS public_temp,
              EXISTS (SELECT 1 FROM pg_database x, aclexplode(coalesce(x.datacl, acldefault('d', x.datdba))) a
                       WHERE x.datname = current_database() AND a.grantee = 0
                         AND a.privilege_type = 'TEMPORARY' AND a.is_grantable) AS public_temp_grantable,
              has_database_privilege($1::oid, current_database(), 'CONNECT') AS connect,
              (SELECT has_database_privilege(r.oid, current_database(), 'CONNECT') FROM pg_roles r WHERE r.rolname = $2) AS wl_connect`,
      [role.oid, cfg.workload ?? null],
    )
  ).rows[0]!;
  if (db.create) out.push("CREATE on the database");
  if (
    db.temp &&
    !isSupabasePublicTemp(cfg, {
      effective: db.temp,
      direct: db.direct_temp,
      public: db.public_temp,
      public_grantable: db.public_temp_grantable,
    })
  )
    out.push(
      "TEMPORARY on the database is not the exact PUBLIC-derived Supabase baseline (revoke it from PUBLIC outside Supabase, see the runbook)",
    );
  if (!db.connect) out.push("no CONNECT on the database (required)");
  if (cfg.workload && db.wl_connect !== true)
    out.push(
      `workload role ${cfg.workload} has no CONNECT on the database (required)`,
    );
  for (const sc of (
    await client.query<{ nspname: string }>(
      `SELECT n.nspname FROM pg_namespace n WHERE ${NONSYSTEM} AND has_schema_privilege($1::oid, n.oid, 'CREATE') ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(`CREATE on schema ${sc.nspname}`);
  // Schema USAGE: an exact allowlist. sessions is approved; public is PostgreSQL's own
  // PUBLIC default (non-mutating: it only lets the role resolve names there, and every
  // object in public is checked above and below). Any other non-system schema is refused.
  // Type USAGE through PUBLIC (PostgreSQL's default for every type) is accepted as
  // non-mutating; a type grant naming the role is a pg_shdepend reference, refused above.
  for (const sc of (
    await client.query<{ nspname: string }>(
      `SELECT n.nspname FROM pg_namespace n WHERE ${NONSYSTEM} AND n.nspname NOT IN ('sessions', 'public')
          AND has_schema_privilege($1::oid, n.oid, 'USAGE') ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(
      `USAGE on schema ${sc.nspname} (only sessions and public are accepted)`,
    );
  // PostgreSQL 17+ MAINTAIN (VACUUM, ANALYZE, REINDEX, REFRESH, CLUSTER, LOCK): refused on
  // any relation. The privilege does not exist before 17, so the check is version-guarded.
  if (
    Number(
      (
        await client.query<{ v: string }>(
          "SELECT current_setting('server_version_num') AS v",
        )
      ).rows[0]?.v,
    ) >= 170000
  )
    for (const m of (
      await client.query<{ rel: string }>(
        `SELECT c.oid::regclass::text AS rel FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE ${NONSYSTEM} AND c.relkind IN ('r', 'p', 'm')
            -- sessions becomes reachable when this same migrator grants the approved
            -- USAGE, so preflight must treat it as reachable already.
            AND (n.nspname = 'sessions' OR has_schema_privilege($1::oid, n.oid, 'USAGE'))
            AND CASE WHEN c.relkind IN ('r', 'p', 'm') THEN has_table_privilege($1::oid, c.oid, 'MAINTAIN') ELSE false END ORDER BY 1`,
        [role.oid],
      )
    ).rows)
      out.push(`MAINTAIN on ${m.rel}`);
  const effectiveTables = (
    await client.query<{ rel: string; privs: string }>(
      `SELECT c.oid::regclass::text AS rel,
              concat_ws(',', CASE WHEN has_table_privilege($1::oid, c.oid, 'SELECT') THEN 'SELECT' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'INSERT') THEN 'INSERT' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'UPDATE') THEN 'UPDATE' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'DELETE') THEN 'DELETE' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'TRUNCATE') THEN 'TRUNCATE' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'REFERENCES') THEN 'REFERENCES' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'TRIGGER') THEN 'TRIGGER' END,
                CASE WHEN NOT has_table_privilege($1::oid, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') AND has_any_column_privilege($1::oid, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') THEN 'column' END) AS privs
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE ${NONSYSTEM} AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND (n.nspname = 'sessions' OR has_schema_privilege($1::oid, n.oid, 'USAGE')) ORDER BY 1`,
      [role.oid],
    )
  ).rows;
  const expectedTablePrivileges = new Map<string, readonly string[]>([
    [tables.ledger, ["SELECT"]],
    [tables.marker, ["SELECT"]],
    ...Object.entries(RUNTIME_APPLICATION_TABLE_PRIVILEGES),
  ]);
  for (const t of effectiveTables) {
    const actual = t.privs ? t.privs.split(",") : [];
    const expected = expectedTablePrivileges.get(t.rel) ?? [];
    const unexpected = actual.filter((p) => !expected.includes(p));
    const missing = phase === "post" ? expected.filter((p) => !actual.includes(p)) : [];
    if (unexpected.length) out.push(`${unexpected.join(",")} on ${t.rel}`);
    if (missing.length) out.push(`missing ${missing.join(",")} on ${t.rel}`);
  }
  const effectiveSequences = (
    await client.query<{ rel: string; privs: string }>(
      `SELECT c.oid::regclass::text AS rel,
              concat_ws(',', CASE WHEN has_sequence_privilege($1::oid, c.oid, 'USAGE') THEN 'USAGE' END,
                CASE WHEN has_sequence_privilege($1::oid, c.oid, 'SELECT') THEN 'SELECT' END,
                CASE WHEN has_sequence_privilege($1::oid, c.oid, 'UPDATE') THEN 'UPDATE' END) AS privs
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE ${NONSYSTEM} AND c.relkind = 'S'
          AND (n.nspname = 'sessions' OR has_schema_privilege($1::oid, n.oid, 'USAGE'))
          -- CASE: AND order is not guaranteed, and the function errors on a non-sequence.
          AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege($1::oid, c.oid, 'USAGE, SELECT, UPDATE') ELSE false END ORDER BY 1`,
      [role.oid],
    )
  ).rows;
  for (const q of effectiveSequences) {
    const approved = RUNTIME_APPLICATION_SEQUENCES.includes(
      q.rel as (typeof RUNTIME_APPLICATION_SEQUENCES)[number],
    );
    if (!approved && q.privs) out.push(`privileges on sequence ${q.rel}`);
    if (approved && q.privs !== "USAGE")
      out.push(`${q.privs} on sequence ${q.rel} (only USAGE is approved)`);
  }
  if (phase === "post")
    for (const sequence of RUNTIME_APPLICATION_SEQUENCES)
      if (!effectiveSequences.some((q) => q.rel === sequence && q.privs === "USAGE"))
        out.push(`missing USAGE on sequence ${sequence}`);
  const effectiveFunctions = (
    await client.query<{ fn: string }>(
      `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE ${NONSYSTEM} AND (n.nspname = 'sessions' OR p.prosecdef)
          AND has_function_privilege($1::oid, p.oid, 'EXECUTE')
          -- Before any write, PUBLIC-derived EXECUTE on sessions functions is left to the
          -- revoke that runs in the validation transaction.
          AND NOT ($2 = 'pre' AND n.nspname = 'sessions'
                   AND (p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')))
        ORDER BY 1`,
      [role.oid, phase],
    )
  ).rows;
  for (const fnr of effectiveFunctions)
    if (!RUNTIME_APPLICATION_FUNCTIONS.includes(fnr.fn as (typeof RUNTIME_APPLICATION_FUNCTIONS)[number]))
      out.push(`EXECUTE on ${fnr.fn}`);
  if (phase === "post")
    for (const fn of RUNTIME_APPLICATION_FUNCTIONS)
      if (!effectiveFunctions.some((row) => row.fn === fn))
        out.push(`missing EXECUTE on ${fn}`);

  const grantable = (
    await client.query<{ what: string }>(
      `SELECT c.oid::regclass::text || ':' || a.privilege_type AS what
         FROM pg_class c, aclexplode(c.relacl) a WHERE a.grantee = $1::oid AND a.is_grantable
       UNION ALL
       SELECT p.oid::regprocedure::text || ':EXECUTE' FROM pg_proc p, aclexplode(p.proacl) a
        WHERE a.grantee = $1::oid AND a.is_grantable
       ORDER BY 1`,
      [role.oid],
    )
  ).rows;
  for (const row of grantable) out.push(`${row.what} WITH GRANT OPTION is not approved`);

  const policies = (
    await client.query<{
      rel: string;
      name: string;
      permissive: boolean;
      command: string;
      roles: number[];
      using_expr: string | null;
      check_expr: string | null;
    }>(
      `SELECT p.polrelid::regclass::text AS rel, p.polname AS name, p.polpermissive AS permissive,
              p.polcmd::text AS command, p.polroles::oid[]::int[] AS roles,
              pg_get_expr(p.polqual, p.polrelid) AS using_expr,
              pg_get_expr(p.polwithcheck, p.polrelid) AS check_expr
         FROM pg_policy p WHERE $1::oid = ANY(p.polroles) ORDER BY 1, 2`,
      [role.oid],
    )
  ).rows;
  for (const policy of policies) {
    const exact =
      RUNTIME_APPLICATION_RLS_TABLES.includes(policy.rel as (typeof RUNTIME_APPLICATION_RLS_TABLES)[number]) &&
      policy.name === RUNTIME_APPLICATION_POLICY &&
      policy.permissive && policy.command === "*" &&
      policy.roles.length === 1 && policy.roles[0] === role.oid &&
      policy.using_expr === "true" && policy.check_expr === "true";
    if (!exact) out.push(`unapproved policy ${policy.name} on ${policy.rel}`);
  }
  if (phase === "post")
    for (const rel of RUNTIME_APPLICATION_RLS_TABLES)
      if (!policies.some((p) => p.rel === rel && p.name === RUNTIME_APPLICATION_POLICY))
        out.push(`missing policy ${RUNTIME_APPLICATION_POLICY} on ${rel}`);
  // Detection only: a per-schema default-privileges GRANT in sessions naming PUBLIC or the
  // role is refused. This is NOT a schema-scoped fix for PostgreSQL's built-in PUBLIC
  // EXECUTE on new functions: per-schema ALTER DEFAULT PRIVILEGES cannot revoke a global
  // or built-in default. That residual is handled by the per-run revoke, the live gate
  // and the activation gate (runbook "Function boundary").
  for (const d of (
    await client.query<{ objtype: string }>(
      `SELECT d.defaclobjtype::text AS objtype FROM pg_default_acl d, aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace = coalesce(to_regnamespace('sessions')::oid, 0) AND a.grantee IN (0, $1::oid) ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(
      `default privileges in schema sessions grant PUBLIC or the runtime role (${d.objtype})`,
    );
  return out;
}

// ---------------------------------------------------------------------------
// One migration authority: a contiguous ledger prefix and no unledgered footprint
// ---------------------------------------------------------------------------

export interface InventoryObject {
  kind:
    "relation" | "index" | "function" | "type" | "trigger" | "policy" | "role";
  name: string;
  /** The table, for triggers and policies. */
  on?: string;
}

const IDENT = `"?[A-Za-z_][\\w$]*"?(?:\\."?[A-Za-z_][\\w$]*"?)?`;
const norm = (n: string) =>
  n
    .split(".")
    .map((p) => (p.startsWith('"') ? p.slice(1, -1) : p.toLowerCase()))
    .join(".");

/**
 * Removes SQL comments, but never inside a single-quoted string ('' escapes) or a
 * dollar-quoted body ($tag$...$tag$): a literal such as '\.git/*$' must not start a
 * comment. Block comments nest, as in PostgreSQL.
 */
export function stripComments(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i]!;
    const next = sql[i + 1];
    if (c === "-" && next === "-") {
      const end = sql.indexOf("\n", i);
      i = end < 0 ? sql.length : end;
      out += " ";
    } else if (c === "/" && next === "*") {
      let depth = 1;
      i += 2;
      while (i < sql.length && depth > 0) {
        if (sql[i] === "/" && sql[i + 1] === "*") (depth++, (i += 2));
        else if (sql[i] === "*" && sql[i + 1] === "/") (depth--, (i += 2));
        else i++;
      }
      out += " ";
    } else if (c === "'") {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === "'" && sql[j + 1] === "'") j += 2;
        else if (sql[j] === "'") break;
        else j++;
      }
      out += sql.slice(i, j + 1);
      i = j + 1;
    } else if (c === "$") {
      const tag = /^\$[A-Za-z_]*\$/.exec(sql.slice(i))?.[0];
      if (tag) {
        const end = sql.indexOf(tag, i + tag.length);
        const stop = end < 0 ? sql.length : end + tag.length;
        out += sql.slice(i, stop);
        i = stop;
      } else ((out += c), i++);
    } else ((out += c), i++);
  }
  return out;
}

/**
 * Every object a migration file creates, read from its CREATE statements (comments
 * stripped; temporary objects, schemas and extensions excluded). A test checks that
 * the union covers every object a full migration leaves in the sessions schema.
 */
export function inventoryOf(sql: string): InventoryObject[] {
  const text = stripComments(sql);
  const out: InventoryObject[] = [];
  const add = (o: InventoryObject) => {
    if (!o.name.includes("%")) out.push(o);
  };
  const main = new RegExp(
    `\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:UNIQUE\\s+)?(TEMP\\s+|TEMPORARY\\s+|UNLOGGED\\s+)?(MATERIALIZED\\s+VIEW|TABLE|VIEW|SEQUENCE|INDEX|FUNCTION|PROCEDURE|TYPE|ROLE)\\s+(?:CONCURRENTLY\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?(${IDENT})`,
    "gi",
  );
  for (const m of text.matchAll(main)) {
    const temp = (m[1] ?? "").trim().toUpperCase();
    if (temp === "TEMP" || temp === "TEMPORARY") continue;
    const kind = m[2]!.toUpperCase().replace(/\s+/g, " ");
    const name = m[3]!;
    if (kind === "INDEX" && name.toUpperCase() === "ON") continue; // unnamed index
    add({
      kind:
        kind === "INDEX"
          ? "index"
          : kind === "FUNCTION" || kind === "PROCEDURE"
            ? "function"
            : kind === "TYPE"
              ? "type"
              : kind === "ROLE"
                ? "role"
                : "relation",
      name: norm(name),
    });
  }
  for (const m of text.matchAll(
    new RegExp(
      `\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:CONSTRAINT\\s+)?TRIGGER\\s+(${IDENT})[\\s\\S]*?\\bON\\s+(${IDENT})`,
      "gi",
    ),
  ))
    add({ kind: "trigger", name: norm(m[1]!), on: norm(m[2]!) });
  for (const m of text.matchAll(
    new RegExp(`\\bCREATE\\s+POLICY\\s+(${IDENT})\\s+ON\\s+(${IDENT})`, "gi"),
  ))
    add({ kind: "policy", name: norm(m[1]!), on: norm(m[2]!) });
  return out;
}

const key = (o: InventoryObject) =>
  `${o.kind}:${o.name}${o.on ? `@${o.on}` : ""}`;

async function exists(
  client: GuardClient,
  o: InventoryObject,
): Promise<boolean> {
  const [schema, name] = o.name.includes(".")
    ? o.name.split(".")
    : ["public", o.name];
  const one = async (sql: string, params: unknown[]) =>
    Boolean((await client.query<{ e: boolean }>(sql, params)).rows[0]?.e);
  try {
    switch (o.kind) {
      case "relation":
        return one("SELECT to_regclass($1) IS NOT NULL AS e", [o.name]);
      case "index":
        return o.name.includes(".")
          ? one("SELECT to_regclass($1) IS NOT NULL AS e", [o.name])
          : one(
              "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1 AND relkind IN ('i', 'I')) AS e",
              [o.name],
            );
      case "function":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1 AND p.proname = $2) AS e",
          [schema, name],
        );
      case "type":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = $1 AND t.typname = $2) AS e",
          [schema, name],
        );
      case "trigger":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = $1 AND tgrelid = to_regclass($2)) AS e",
          [o.name, o.on],
        );
      case "policy":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_policy WHERE polname = $1 AND polrelid = to_regclass($2)) AS e",
          [o.name, o.on],
        );
      case "role":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1) AS e",
          [o.name],
        );
    }
  } catch {
    return true; // can't look: fail closed, as if the object were there
  }
}

/**
 * Refuses, before any write, unless the database is either empty of this migration
 * set or exactly a ledgered prefix of each migration lane. Upstream and 96Bulls are
 * independent append-only lanes: a later upstream release must remain deployable after
 * an earlier image has already recorded the then-current 96Bulls prefix.
 *   ledger_unknown_step   the ledger names a schema step this image doesn't have;
 *   ledger_gap            one lane isn't a contiguous prefix of its ordered steps;
 *   migration_changed     a ledgered step's file changed;
 *   unledgered_schema     an object that an unledgered step creates already exists.
 * Nothing is ever imported into, or marked in, the ledger.
 */
export async function sequencePreflight(
  client: GuardClient,
  schemaSteps: GuardStep[],
  ledger: Map<string, string>,
): Promise<void> {
  const names = schemaSteps.map((s) => s.name);
  const known = new Set(names);
  const isSchema = (n: string) =>
    n.startsWith("upstream:") || n.startsWith("96bulls:");
  const unknown = [...ledger.keys()]
    .filter((n) => isSchema(n) && !known.has(n))
    .sort();
  if (unknown.length)
    throw guardError(
      "ledger_unknown_step",
      `the ledger records ${unknown.join(", ")}, which this image doesn't have; nothing is marked, see the runbook decision point`,
    );
  const lanes = ["upstream:", "96bulls:"] as const;
  const laneNames = (lane: (typeof lanes)[number]) =>
    names.filter((name) => name.startsWith(lane));
  const recordedUpstream = laneNames("upstream:").filter((name) =>
    ledger.has(name),
  );
  const recordedBulls = laneNames("96bulls:").filter((name) =>
    ledger.has(name),
  );
  // Every historical image placed the upstream lane before the 96Bulls lane. A lone
  // 96Bulls row therefore cannot be a legitimate old prefix and remains fail-closed.
  if (recordedBulls.length > 0 && recordedUpstream.length === 0)
    throw guardError(
      "ledger_gap",
      `${recordedBulls[0]} is recorded without any upstream migration; nothing is marked, see the runbook decision point`,
    );
  for (const lane of lanes) {
    const ordered = laneNames(lane);
    let k = 0;
    while (k < ordered.length && ledger.has(ordered[k]!)) k++;
    const beyond = ordered.slice(k).find((name) => ledger.has(name));
    if (beyond)
      throw guardError(
        "ledger_gap",
        `${ordered[k]} is not recorded but ${beyond} is; ${lane.slice(0, -1)} migrations must be one contiguous prefix, nothing is marked, see the runbook decision point`,
      );
  }
  const sql = await Promise.all(
    schemaSteps.map((s) => readFile(s.path, "utf8")),
  );
  for (let i = 0; i < names.length; i++) {
    if (!ledger.has(names[i]!)) continue;
    const sha = createHash("sha256").update(sql[i]!).digest("hex");
    if (!migrationChecksumMatches(names[i]!, ledger.get(names[i]!)!, sha))
      throw guardError(
        "migration_changed",
        `migration ${names[i]} changed after it was applied; add a new migration instead`,
      );
  }
  const seen = new Set<string>();
  for (let i = 0; i < names.length; i++) {
    if (!ledger.has(names[i]!)) continue;
    for (const o of inventoryOf(sql[i]!)) seen.add(key(o));
  }
  const found: string[] = [];
  for (let i = 0; i < names.length; i++) {
    if (ledger.has(names[i]!)) continue;
    for (const o of inventoryOf(sql[i]!)) {
      const id = key(o);
      if (seen.has(id)) continue;
      seen.add(id);
      if (await exists(client, o))
        found.push(
          `${o.kind} ${o.name}${o.on ? ` on ${o.on}` : ""} (${names[i]})`,
        );
    }
  }
  // Per-step catalog sentinels: they see ALTER-only and replacement steps too.
  for (let i = 0; i < names.length; i++) {
    if (ledger.has(names[i]!)) continue;
    const sentinel = schemaSteps[i]!.sentinel ?? MIGRATION_SENTINELS[names[i]!];
    if (!sentinel)
      throw guardError(
        "sentinel_missing",
        `${names[i]} has no catalog sentinel (src/hoopdb/migration-sentinels.ts); refusing`,
      );
    let applied: boolean;
    try {
      applied = Boolean(
        (await client.query<{ applied: boolean }>(sentinel.sql)).rows[0]
          ?.applied,
      );
    } catch {
      applied = true; // can't look: fail closed, as if the step had run
    }
    if (applied) found.push(`sentinel ${sentinel.evidence} (${names[i]})`);
  }
  if (found.length)
    throw guardError(
      "unledgered_schema",
      `objects of unrecorded migrations already exist: ${found.slice(0, 5).join("; ")}${found.length > 5 ? `; and ${found.length - 5} more` : ""}. Nothing is imported or marked, see the runbook decision point`,
    );
}

// ---------------------------------------------------------------------------
// Durable function boundary (activation gate)
// ---------------------------------------------------------------------------

/**
 * Event-trigger mode (supabase only): the reviewed trigger installed by
 * supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql. The gate verifies it
 * by its full catalog definition against these frozen constants, never by name alone.
 */
export const SESSIONS_FUNCTION_EVENT_TRIGGER = "hoopdb_sessions_function_boundary";
export const SESSIONS_FUNCTION_EVENT_TRIGGER_FUNCTION =
  "hoopdb_boundary.sessions_function_boundary()";
export const SESSIONS_FUNCTION_EVENT_TRIGGER_SCHEMA = "hoopdb_boundary";
/** The exact tag set (sorted): every command that can create a routine or change its ACL, owner or security. */
export const SESSIONS_FUNCTION_EVENT_TRIGGER_TAGS: readonly string[] =
  Object.freeze([
    "ALTER AGGREGATE",
    "ALTER DEFAULT PRIVILEGES",
    "ALTER EXTENSION",
    "ALTER FUNCTION",
    "ALTER PROCEDURE",
    "ALTER ROUTINE",
    "ALTER SCHEMA",
    "CREATE AGGREGATE",
    "CREATE EXTENSION",
    "CREATE FUNCTION",
    "CREATE PROCEDURE",
    "CREATE TYPE",
    "GRANT",
    "REVOKE",
  ]);
/**
 * sha256 (hex) of the trigger function's prosrc with its one variable part, the runtime
 * role literal (`v_runtime text := '<role>';`), put back to the placeholder
 * `__RUNTIME_ROLE__`: exactly the reviewed body in the bootstrap file.
 */
export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =
  "8fcf0f6f2876c08d96dd667f69f6fa1f3a52c8cb7ba0b431e4ddac08243cb434";
export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROCONFIG: readonly string[] =
  Object.freeze(["search_path=pg_catalog, pg_temp"]);
const RUNTIME_LITERAL = /v_runtime text := '([a-z_][a-z0-9_]{0,62})';/g;
/** The runtime role written into a trigger body, and the body normalized back to the reviewed text. */
export function normalizeEventTriggerBody(prosrc: string): {
  runtimeRole: string | null;
  sha256: string;
} {
  const found = [...prosrc.matchAll(RUNTIME_LITERAL)];
  const normalized =
    found.length === 1
      ? prosrc.replace(RUNTIME_LITERAL, "v_runtime text := '__RUNTIME_ROLE__';")
      : prosrc;
  return {
    runtimeRole: found.length === 1 ? found[0]![1]! : null,
    sha256: createHash("sha256").update(normalized).digest("hex"),
  };
}

/** The event trigger's catalog definition as the gate read it. Names, booleans and hashes only. */
export interface EventTriggerEvidence {
  name: string;
  present: boolean;
  event: string | null;
  tags: string[];
  tags_exact: boolean;
  enabled: string | null;
  enabled_ok: boolean;
  owner: string | null;
  owner_is_migration_owner: boolean;
  function: string | null;
  function_is_reviewed: boolean;
  function_owner: string | null;
  function_owner_is_migration_owner: boolean;
  schema_owner_is_migration_owner: boolean;
  prosrc_sha256: string | null;
  prosrc_exact: boolean;
  proconfig: string[];
  proconfig_exact: boolean;
  security_definer: boolean | null;
  volatility: string | null;
  language: string | null;
  returns: string | null;
  function_acl: string[];
  function_public_execute: boolean;
  /** The runtime role written into the body (allowed on RUNTIME_APPLICATION_FUNCTIONS); must equal HOOPDB_RUNTIME_ROLE. */
  runtime_role_setting: string | null;
  runtime_role_setting_matches: boolean;
  /** Schema hoopdb_boundary is owned by the migration owner and grants nobody else anything. */
  schema_private: boolean;
  verified: boolean;
}

/** Every other event trigger in the database (informational; final-state checks still apply). */
export interface OtherEventTrigger {
  name: string;
  event: string;
  enabled: string;
  owner: string;
  function: string;
}
export type FunctionBoundaryMode = "global-default-revoke" | "event-trigger";

/**
 * SQL (role oid as $1): the role's GLOBAL function default (pg_default_acl, defaclnamespace
 * = 0, defaclobjtype 'f') normalized as sorted "<self>|PUBLIC|role:PRIVILEGE[*]" entries
 * ("*" = grant option). No row means PostgreSQL's built-in default, reported as
 * ["<built-in>"] (owner and PUBLIC EXECUTE).
 */
const FUNCTION_DEFAULT_ROW_SQL = `coalesce((
    SELECT coalesce((SELECT array_agg(CASE WHEN a.grantee = 0 THEN 'PUBLIC' WHEN a.grantee = $1::oid THEN '<self>' ELSE (SELECT rolname FROM pg_roles WHERE oid = a.grantee) END
                     || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END ORDER BY 1)
      FROM aclexplode(d.defaclacl) a), '{}'::text[])
      FROM pg_default_acl d
     WHERE d.defaclrole = $1::oid AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'), ARRAY['<built-in>'])`;
/** The only exact-safe global function default: the role's own EXECUTE, no grant option, nothing else. */
export const EXACT_SAFE_FUNCTION_DEFAULT = ["<self>:EXECUTE"];

/**
 * Supabase platform superusers: the platform's trust root, which the migrator cannot
 * constrain (Supabase's postgres is not a superuser and cannot alter their defaults).
 * Fixed and code-reviewed; no environment variable changes it. Keep it minimal; add a
 * name only with a Supabase docs citation.
 *   supabase_admin: "An internal role Supabase uses for administrative tasks, such as
 *   running upgrades and automations."
 *   (https://supabase.com/docs/guides/database/postgres/roles)
 * A listed role is excluded from creator coverage ONLY in supabase mode AND only when
 * pg_roles.rolsuper is true at gate time; never the migration owner. A listed name that is
 * not a superuser, or any other superuser, stays covered.
 */
export const SUPABASE_PLATFORM_SUPERUSERS: readonly string[] = Object.freeze([
  "supabase_admin",
]);

/**
 * Google documents cloudsqladmin as a Cloud SQL system user with database superuser
 * privileges that customers cannot delete or modify. It is excluded only in cloudsql
 * mode, only while the catalog still marks it SUPERUSER, and never when it is the
 * migration owner. No environment variable can extend this list.
 * https://cloud.google.com/sql/docs/postgres/users#system-users
 */
export const CLOUDSQL_PLATFORM_SUPERUSERS: readonly string[] = Object.freeze([
  "cloudsqladmin",
]);

/** A fixed provider platform superuser excluded from creator coverage, with its attributes. */
export interface ExcludedPlatformSuperuser {
  role: string;
  rolsuper: boolean;
  rolinherit: boolean;
  rolcreaterole: boolean;
  rolcreatedb: boolean;
  rolcanlogin: boolean;
  rolreplication: boolean;
  rolbypassrls: boolean;
  function_default: string[];
}

/** One role able to create functions in sessions, with its global function default. Names and booleans only. */
export interface SessionsCreator {
  role: string;
  superuser: boolean;
  is_migration_owner: boolean;
  function_default: string[];
  function_default_exact: boolean;
}

/** Names and booleans only. */
export interface FunctionBoundaryEvidence {
  migration_owner: string;
  /**
   * (B) is false by design in this repository: the search Worker's RPCs are called
   * through PostgREST with Content-Profile sessions (src/proof/aws/checks.ts, #73), so
   * sessions is an exposed schema, and authenticated and service_role hold USAGE on it.
   */
  sessions_unexposed_and_no_untrusted_usage: false;
  /** The migration owner's global function default, normalized; exact-safe only if EXACT_SAFE_FUNCTION_DEFAULT. */
  owner_function_default: string[];
  owner_function_default_exact: boolean;
  /**
   * Pre-creation creator coverage: every role with effective CREATE on schema sessions
   * (has_schema_privilege, membership included, superusers included); before sessions
   * exists, the migration owner and every superuser. Each must have an exact-safe global
   * function default, or a function it creates later would be PUBLIC-executable.
   */
  sessions_create_roles: SessionsCreator[];
  sessions_create_roles_covered: boolean;
  /**
   * The HOOPDB_DB_MODE the gate ran under ("unspecified" for --activation-check without
   * it: full coverage). Only provider-specific modes exclude their fixed platform
   * superusers.
   */
  db_mode: "local" | "selfhosted" | "cloudsql" | "supabase" | "unspecified";
  /**
   * Provider mode only: CREATE-capable roles left out of sessions_create_roles because
   * the catalog shows rolsuper AND the name is in that provider's fixed platform list.
   * Always empty in local, selfhosted and unspecified modes.
   */
  excluded_platform_superusers: ExcludedPlatformSuperuser[];
  /**
   * Schema-scoped function defaults IN sessions (any role) that grant anyone other than
   * the defining role, as "role>grantee:PRIVILEGE[*]". They add to the global default, so
   * any entry makes future sessions functions executable by that grantee: must be empty.
   */
  sessions_schema_function_defaults: string[];
  /** Every existing sessions function is owned by the migration owner. */
  sessions_functions_owned_by_owner: boolean;
  event_trigger_present: boolean;
  /** Event-trigger mode: the reviewed trigger's full catalog definition. */
  event_trigger: EventTriggerEvidence;
  other_event_triggers: OtherEventTrigger[];
  mode: FunctionBoundaryMode | "none";
  satisfied: boolean;
}

/**
 * PostgreSQL gives every new function PUBLIC EXECUTE by built-in default, and a
 * per-schema ALTER DEFAULT PRIVILEGES cannot revoke that. So a function created in
 * sessions after a migrate, outside the migrator, is directly invocable by any role with
 * USAGE on sessions (authenticated, service_role, and through PostgREST RPC) until the
 * next migrate's revoke, unless its creator's global function default is exact-safe.
 * The migrator's function-boundary gate (and --activation-check) therefore requires the
 * mode the deployment selects (HOOPDB_FUNCTION_BOUNDARY_MODE, configuration, not a human
 * sign-off) to be proven by the catalog. It applies outside local mode; only
 * explicit local mode is ungated:
 *   "global-default-revoke": every sessions creator, the migration owner included, has
 *       a global function default of exactly {<self>:EXECUTE} (no other grantee, no grant
 *       option). Installed in selfhosted by the privileged
 *       supabase/bootstrap/hoopdb_function_default_revoke.sql (the database is dedicated
 *       to HoopDb). On Supabase the owner is postgres, so the operator decides (#98).
 *   "event-trigger" (supabase mode only): the reviewed ddl_command_end trigger
 *       (SESSIONS_FUNCTION_EVENT_TRIGGER) must match every frozen constant above (event,
 *       exact tags, enabled O/A, owner = migration owner, function identity, owner,
 *       prosrc sha256, proconfig, SECURITY INVOKER, volatility, language, not
 *       PUBLIC-executable). The migration owner is covered by the trigger instead of its
 *       global default; every other creator still needs an exact default. Other event
 *       triggers are reported: any DDL they run fires the reviewed trigger too, and the
 *       live gate keeps refusing PUBLIC-executable sessions functions.
 * (B) cannot be claimed here (see the evidence field). Follow-up: a dedicated sessions
 * object owner, which needs an ownership transfer of live objects (activation dossier #98).
 */
export async function functionBoundaryEvidence(
  client: GuardClient,
  mode: FunctionBoundaryMode | undefined,
  dbMode: "local" | "selfhosted" | "cloudsql" | "supabase" | undefined = undefined,
  runtimeRole: string | undefined = undefined,
): Promise<FunctionBoundaryEvidence> {
  const head = (
    await client.query<{
      owner: string;
      owner_oid: number;
      event_trigger: boolean;
      owned: boolean;
      sessions: boolean;
    }>(
      `SELECT current_user AS owner, (SELECT oid::int FROM pg_roles WHERE rolname = current_user) AS owner_oid,
              EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = $1) AS event_trigger,
              NOT EXISTS (SELECT 1 FROM pg_proc p
                           WHERE p.pronamespace = coalesce(to_regnamespace('sessions')::oid, 0)
                             AND p.proowner <> (SELECT oid FROM pg_roles WHERE rolname = current_user)) AS owned,
              to_regnamespace('sessions') IS NOT NULL AS sessions`,
      [SESSIONS_FUNCTION_EVENT_TRIGGER],
    )
  ).rows[0]!;
  const roles = (
    await client.query<{
      oid: number;
      rolname: string;
      rolsuper: boolean;
      rolinherit: boolean;
      rolcreaterole: boolean;
      rolcreatedb: boolean;
      rolcanlogin: boolean;
      rolreplication: boolean;
      rolbypassrls: boolean;
    }>(
      `SELECT oid::int AS oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls
         FROM pg_roles WHERE ${head.sessions ? "has_schema_privilege(oid, 'sessions', 'CREATE')" : "rolsuper OR rolname = current_user"}
        ORDER BY rolname`,
    )
  ).rows;
  const defaultOf = async (oid: number) =>
    (
      await client.query<{ d: string[] }>(
        `SELECT ${FUNCTION_DEFAULT_ROW_SQL} AS d`,
        [oid],
      )
    ).rows[0]!.d;
  const exact = (d: string[]) =>
    JSON.stringify(d) === JSON.stringify(EXACT_SAFE_FUNCTION_DEFAULT);
  const creators: SessionsCreator[] = [];
  const excluded: ExcludedPlatformSuperuser[] = [];
  for (const r of roles) {
    const d = await defaultOf(r.oid);
    const providerSuperusers =
      dbMode === "supabase"
        ? SUPABASE_PLATFORM_SUPERUSERS
        : dbMode === "cloudsql"
          ? CLOUDSQL_PLATFORM_SUPERUSERS
          : [];
    if (
      providerSuperusers.length > 0 &&
      r.rolsuper === true &&
      r.oid !== head.owner_oid &&
      providerSuperusers.includes(r.rolname)
    ) {
      excluded.push({
        role: r.rolname,
        rolsuper: r.rolsuper,
        rolinherit: r.rolinherit,
        rolcreaterole: r.rolcreaterole,
        rolcreatedb: r.rolcreatedb,
        rolcanlogin: r.rolcanlogin,
        rolreplication: r.rolreplication,
        rolbypassrls: r.rolbypassrls,
        function_default: d,
      });
      continue;
    }
    creators.push({
      role: r.rolname,
      superuser: r.rolsuper,
      is_migration_owner: r.oid === head.owner_oid,
      function_default: d,
      function_default_exact: exact(d),
    });
  }
  const ownerDefault = await defaultOf(head.owner_oid);
  const schemaDefaults = (
    await client.query<{ e: string }>(
      `SELECT r.rolname || '>' || CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE (SELECT rolname FROM pg_roles WHERE oid = a.grantee) END
              || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END AS e
         FROM pg_default_acl d JOIN pg_roles r ON r.oid = d.defaclrole, aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace = coalesce(to_regnamespace('sessions')::oid, 0) AND d.defaclnamespace <> 0
          AND d.defaclobjtype = 'f' AND a.grantee <> d.defaclrole
        ORDER BY 1`,
    )
  ).rows.map((x) => x.e);
  const et = await eventTriggerEvidence(client, head.owner_oid, runtimeRole);
  const others = (
    await client.query<OtherEventTrigger>(
      `SELECT e.evtname AS name, e.evtevent AS event, e.evtenabled::text AS enabled,
              (SELECT rolname FROM pg_roles WHERE oid = e.evtowner) AS owner, e.evtfoid::regprocedure::text AS function
         FROM pg_event_trigger e WHERE e.evtname <> $1 ORDER BY 1`,
      [SESSIONS_FUNCTION_EVENT_TRIGGER],
    )
  ).rows;
  // In event-trigger mode the trigger covers the migration owner; every other creator
  // still needs its own exact global default (supautils skips the trigger for
  // superusers and reserved roles, and other roles are aborted only after the fact).
  const eventTriggerMode = mode === "event-trigger" && dbMode === "supabase";
  const covered =
    creators.length > 0 &&
    creators.every(
      (c) =>
        c.function_default_exact || (eventTriggerMode && c.is_migration_owner),
    );
  const satisfied =
    (mode === "global-default-revoke"
      ? exact(ownerDefault)
      : eventTriggerMode && et.verified) &&
    covered &&
    schemaDefaults.length === 0 &&
    head.owned;
  return {
    migration_owner: head.owner,
    sessions_unexposed_and_no_untrusted_usage: false,
    owner_function_default: ownerDefault,
    owner_function_default_exact: exact(ownerDefault),
    sessions_create_roles: creators,
    sessions_create_roles_covered: covered,
    db_mode: dbMode ?? "unspecified",
    excluded_platform_superusers: excluded,
    sessions_schema_function_defaults: schemaDefaults,
    sessions_functions_owned_by_owner: head.owned,
    event_trigger_present: head.event_trigger,
    event_trigger: et,
    other_event_triggers: others,
    mode: mode ?? "none",
    satisfied,
  };
}

/** The reviewed event trigger's catalog definition, compared with the frozen constants. */
export async function eventTriggerEvidence(
  client: GuardClient,
  ownerOid: number,
  runtimeRole: string | undefined,
): Promise<EventTriggerEvidence> {
  const r = (
    await client.query<{
      present: boolean;
      event: string | null;
      tags: string[] | null;
      enabled: string | null;
      owner: string | null;
      owner_ok: boolean | null;
      function: string | null;
      function_ok: boolean | null;
      function_owner: string | null;
      function_owner_ok: boolean | null;
      schema_owner_ok: boolean | null;
      prosrc: string | null;
      proconfig: string[] | null;
      security_definer: boolean | null;
      volatility: string | null;
      language: string | null;
      returns: string | null;
      function_acl: string[] | null;
      function_public_execute: boolean | null;
      schema_private: boolean | null;
    }>(
      `SELECT e.oid IS NOT NULL AS present, e.evtevent AS event,
              (SELECT array_agg(t ORDER BY t) FROM unnest(e.evttags) t) AS tags,
              e.evtenabled::text AS enabled,
              (SELECT rolname FROM pg_roles WHERE oid = e.evtowner) AS owner,
              e.evtowner = $2::oid AS owner_ok,
              e.evtfoid::regprocedure::text AS function,
              e.evtfoid = to_regprocedure($3) AS function_ok,
              (SELECT rolname FROM pg_roles WHERE oid = p.proowner) AS function_owner,
              p.proowner = $2::oid AS function_owner_ok,
              (SELECT n.nspowner = $2::oid FROM pg_namespace n WHERE n.nspname = $4) AS schema_owner_ok,
              p.prosrc,
              p.proconfig, p.prosecdef AS security_definer, p.provolatile::text AS volatility,
              (SELECT lanname FROM pg_language WHERE oid = p.prolang) AS language,
              p.prorettype::regtype::text AS returns,
              CASE WHEN p.oid IS NULL THEN NULL WHEN p.proacl IS NULL THEN ARRAY['<default>'] ELSE (
                SELECT array_agg(CASE WHEN a.grantee = 0 THEN 'PUBLIC' WHEN a.grantee = p.proowner THEN '<owner>' ELSE (SELECT rolname FROM pg_roles WHERE oid = a.grantee) END
                                 || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END ORDER BY 1)
                  FROM aclexplode(p.proacl) a) END AS function_acl,
              CASE WHEN p.oid IS NULL THEN NULL ELSE (p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0)) END AS function_public_execute,
              (SELECT n.nspowner = $2::oid
                      AND (n.nspacl IS NULL OR NOT EXISTS (SELECT 1 FROM aclexplode(n.nspacl) a WHERE a.grantee <> n.nspowner))
                 FROM pg_namespace n WHERE n.nspname = $4) AS schema_private
         FROM (SELECT 1) one
         LEFT JOIN pg_event_trigger e ON e.evtname = $1
         LEFT JOIN pg_proc p ON p.oid = e.evtfoid`,
      [
        SESSIONS_FUNCTION_EVENT_TRIGGER,
        ownerOid,
        SESSIONS_FUNCTION_EVENT_TRIGGER_FUNCTION,
        SESSIONS_FUNCTION_EVENT_TRIGGER_SCHEMA,
      ],
    )
  ).rows[0]!;
  // Sorted here, not in SQL: the comparison must not depend on the database collation.
  const tags = [...(r.tags ?? [])].sort();
  const proconfig = r.proconfig ?? [];
  const body = r.prosrc === null ? null : normalizeEventTriggerBody(r.prosrc);
  const runtimeSetting = body?.runtimeRole ?? null;
  const e = {
    name: SESSIONS_FUNCTION_EVENT_TRIGGER,
    present: r.present === true,
    event: r.event,
    tags,
    tags_exact:
      JSON.stringify(tags) ===
      JSON.stringify([...SESSIONS_FUNCTION_EVENT_TRIGGER_TAGS].sort()),
    enabled: r.enabled,
    // Exactly ALWAYS: an origin (O) trigger is skipped in replica mode.
    enabled_ok: r.enabled === "A",
    owner: r.owner,
    owner_is_migration_owner: r.owner_ok === true,
    function: r.function,
    function_is_reviewed: r.function_ok === true,
    function_owner: r.function_owner,
    function_owner_is_migration_owner: r.function_owner_ok === true,
    schema_owner_is_migration_owner: r.schema_owner_ok === true,
    prosrc_sha256: body?.sha256 ?? null,
    prosrc_exact: body?.sha256 === SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256,
    proconfig,
    proconfig_exact:
      JSON.stringify(proconfig) ===
      JSON.stringify(SESSIONS_FUNCTION_EVENT_TRIGGER_PROCONFIG),
    security_definer: r.security_definer,
    volatility: r.volatility,
    language: r.language,
    returns: r.returns,
    function_acl: r.function_acl ?? [],
    function_public_execute: r.function_public_execute !== false,
    runtime_role_setting: runtimeSetting,
    runtime_role_setting_matches:
      runtimeSetting !== null && runtimeSetting === runtimeRole,
    schema_private: r.schema_private === true,
  };
  const verified =
    e.present &&
    e.event === "ddl_command_end" &&
    e.tags_exact &&
    e.enabled_ok &&
    e.owner_is_migration_owner &&
    e.function_is_reviewed &&
    e.function_owner_is_migration_owner &&
    e.schema_owner_is_migration_owner &&
    e.prosrc_exact &&
    e.proconfig_exact &&
    e.security_definer === false &&
    e.volatility === "v" &&
    e.language === "plpgsql" &&
    e.returns === "event_trigger" &&
    !e.function_public_execute &&
    e.runtime_role_setting_matches &&
    e.schema_private;
  return { ...e, verified };
}

/** The committed ACL manifest (src/hoopdb/sessions-function-acl.json). */
export async function sessionsFunctionAclManifest(): Promise<
  Record<string, string[]>
> {
  return (
    JSON.parse(
      await readFile(new URL("./sessions-function-acl.json", import.meta.url), "utf8"),
    ) as { functions: Record<string, string[]> }
  ).functions;
}

/**
 * Run-time exact-manifest check (every non-local migrate, inside the validation
 * transaction, after the runtime application ACL is installed): every sessions
 * function's ACL must equal the manifest, plus `<runtime role>:EXECUTE` on
 * RUNTIME_APPLICATION_FUNCTIONS. Returns the differences (empty when exact).
 */
export async function sessionsFunctionAclDrift(
  client: GuardClient,
  runtimeRole: string,
): Promise<string[]> {
  const manifest = await sessionsFunctionAclManifest();
  const expected: Record<string, string[]> = Object.fromEntries(
    Object.entries(manifest).map(([fn, entries]) => [
      fn,
      (RUNTIME_APPLICATION_FUNCTIONS as readonly string[]).includes(fn)
        ? [...entries, `${runtimeRole}:EXECUTE`]
        : entries,
    ]),
  );
  const actual = await sessionsFunctionAcl(client);
  const norm = (xs: string[] | undefined) => JSON.stringify([...(xs ?? [])].sort());
  const drift: string[] = [];
  for (const fn of new Set([...Object.keys(expected), ...Object.keys(actual)]))
    if (norm(expected[fn]) !== norm(actual[fn]))
      drift.push(
        `${fn}: ${actual[fn] ? actual[fn]!.join(",") : "missing"} (expected ${expected[fn] ? expected[fn]!.join(",") : "absent"})`,
      );
  return drift;
}

/** Read-only preview of the exact-ACL drift check, split by what the migrator would do. */
export interface FunctionAclPreview {
  /** The runtime role whose RUNTIME_APPLICATION_FUNCTIONS grants are expected (null: none). */
  runtime_role: string | null;
  /** Grants present but not expected ("fn: entry"; "<default>" is the built-in PUBLIC EXECUTE): the migrator refuses these. */
  function_acl_drift_extra: string[];
  /**
   * Grants expected but absent that the migrator adds: the runtime role's
   * RUNTIME_APPLICATION_FUNCTIONS grants (installRuntimeApplicationAcl), and
   * "fn: <function>" for a manifest function a pending migration still has to create.
   */
  function_acl_missing: string[];
  /**
   * Grants expected but absent that nothing re-adds: a non-runtime manifest grant on an
   * existing function, or a manifest function missing with no migration pending. The
   * migrator refuses these (function_acl_drift).
   */
  function_acl_missing_refused: string[];
  /**
   * PUBLIC EXECUTE the migrator would revoke before its check (its REVOKE ... FROM PUBLIC
   * on every sessions function), so not refused; reported because such a function is
   * PUBLIC-executable now.
   */
  function_acl_public_revoked: string[];
}

/**
 * The same comparison as sessionsFunctionAclDrift (manifest plus the runtime application
 * ACL the migrator installs), read-only and split into extras and missing grants. Missing
 * runtime grants are expected before the first migrate; only extras would be refused.
 */
export async function sessionsFunctionAclPreview(
  client: GuardClient,
  runtimeRole: string | undefined,
  pendingMigrations = false,
): Promise<FunctionAclPreview> {
  const manifest = await sessionsFunctionAclManifest();
  const actual = await sessionsFunctionAcl(client);
  const extra: string[] = [];
  const missing: string[] = [];
  const missingRefused: string[] = [];
  const publicRevoked: string[] = [];
  // Multiset counts, as the migrator's drift check compares sorted arrays: the same
  // grantee:privilege from two grantors counts twice and is a surplus.
  const counts = (xs: string[]) => {
    const m = new Map<string, number>();
    for (const x of xs) m.set(x, (m.get(x) ?? 0) + 1);
    return m;
  };
  for (const fn of [...new Set([...Object.keys(manifest), ...Object.keys(actual)])].sort()) {
    const runtimeEntry =
      runtimeRole && (RUNTIME_APPLICATION_FUNCTIONS as readonly string[]).includes(fn)
        ? `${runtimeRole}:EXECUTE`
        : null;
    const expectedList = [...(manifest[fn] ?? []), ...(runtimeEntry ? [runtimeEntry] : [])];
    const raw = actual[fn];
    if (raw === undefined) {
      if (manifest[fn])
        (pendingMigrations ? missing : missingRefused).push(`${fn}: <function>`);
      continue;
    }
    // Model the migrator's REVOKE EXECUTE ... FROM PUBLIC: the built-in default becomes
    // owner-only, and PUBLIC EXECUTE entries go away.
    const have: string[] = [];
    for (const e of raw)
      if (e === "<default>") {
        publicRevoked.push(`${fn}: <default>`);
        have.push("<owner>:EXECUTE");
      } else if (e.startsWith("PUBLIC:EXECUTE")) publicRevoked.push(`${fn}: ${e}`);
      else have.push(e);
    const want = counts(expectedList);
    const got = counts(have);
    for (const [e, n] of [...got].sort())
      for (let i = want.get(e) ?? 0; i < n; i++) extra.push(`${fn}: ${e}`);
    for (const [e, n] of [...want].sort())
      for (let i = got.get(e) ?? 0; i < n; i++)
        // Only the runtime grants are (re)installed by the migrator; any other missing
        // manifest grant on an existing function is refused.
        (e === runtimeEntry ? missing : missingRefused).push(`${fn}: ${e}`);
  }
  return {
    runtime_role: runtimeRole ?? null,
    function_acl_drift_extra: extra,
    function_acl_missing: missing,
    function_acl_missing_refused: missingRefused,
    function_acl_public_revoked: publicRevoked,
  };
}

/**
 * Every sessions function's ACL after a migrate, normalized for comparison with the
 * committed manifest (src/hoopdb/sessions-function-acl.json): grantee names (PUBLIC for
 * the pseudo-role; <owner> for the function owner), privilege, "*" for GRANT OPTION.
 * A NULL ACL (built-in default: owner plus PUBLIC EXECUTE) is reported as ["<default>"].
 */
export async function sessionsFunctionAcl(
  client: GuardClient,
): Promise<Record<string, string[]>> {
  const rows = (
    await client.query<{ fn: string; entries: string[] | null }>(
      `SELECT p.oid::regprocedure::text AS fn,
              CASE WHEN p.proacl IS NULL THEN NULL ELSE (
                SELECT array_agg(
                         CASE WHEN a.grantee = 0 THEN 'PUBLIC' WHEN a.grantee = p.proowner THEN '<owner>' ELSE (SELECT rolname FROM pg_roles WHERE oid = a.grantee) END
                         || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END ORDER BY 1)
                  FROM aclexplode(p.proacl) a) END AS entries
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sessions' ORDER BY 1`,
    )
  ).rows;
  return Object.fromEntries(
    rows.map((r) => [r.fn, r.entries ?? ["<default>"]]),
  );
}
