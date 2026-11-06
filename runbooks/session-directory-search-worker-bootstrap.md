# Bootstrapping and validating the session directory search Worker role

`supabase/migrations/20261030020000_session_directory_short_summary.sql` needs the role
`session_directory_search_worker` to exist before it runs, and it neither creates nor
alters that role. Creating a role needs CREATEROLE, and on PostgreSQL 16+ setting a role
parameter also needs ADMIN OPTION on the role. A least-privilege migration owner (CNPG
`hoop`) has neither. So a privileged step provisions the role, and the migration only
verifies it. If the role is absent, the migration fails with `42704`. If it differs from
the spec, the migration fails with `55000`, naming every violation. Either way it fails
before any DDL, so nothing is applied.

The migration's guard runs only once, because the ledger then skips the file. So every
deploy runs two separate checks:

| Phase        | File                                                            | When                                                 | Run as                                                     | Checks                                                                                           |
| ------------ | --------------------------------------------------------------- | ---------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| 1. bootstrap | `supabase/bootstrap/session_directory_search_worker.sql`        | before every migrate                                 | a superuser; PG15 CREATEROLE; or the Supabase-only wrapper | provisions the bare role if absent; otherwise verifies the role spec and the exact creator edge. |
| 2. verify    | `supabase/bootstrap/session_directory_search_worker_verify.sql` | after every migrate, once all migrations are applied | any role that can read the catalogs (`hoop` is enough)     | the role spec plus the exact direct-grant allowlist. Read-only.                                  |

Nothing here runs automatically against a real database. Every apply needs the database operator's
explicit approval.

## Interface (stable; deploy-time provisioning is not wired yet: helm-charts #24 owns the Job)

**Deploy order:** phase 1 (superuser), then `hoopdb-migrate` (which runs phase 2 and records PASS; see the readiness marker below). Each step uses
`psql -v ON_ERROR_STOP=1 -X -f <file>` against the HoopDb or Supabase database, and the
deploy stops at the first non-zero exit. Both files are single atomic `DO` blocks.

Role spec (both phases):

| Item     | Value                                                                                                                                                                                                                                                                                                 |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Role     | `session_directory_search_worker`                                                                                                                                                                                                                                                                     |
| Attrs    | `NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`                                                                                                                                                                                                                     |
| Settings | exactly one row in `pg_db_role_setting`: `setdatabase = 0`, `setconfig = {statement_timeout=2s}`. Other spellings (`2000ms`, `2000`), other values, extra settings and per-database settings are all refused.                                                                                         |
| Members  | it is a member of no role. Allowed members are the separately approved `authenticator` binding, and only in `supabase` mode on PG16+, the unavoidable `postgres` creator edge: grantor `supabase_admin` with `rolsuper = true`, ADMIN true, INHERIT false and SET false. Every other edge is refused. |

Phase 1, `session_directory_search_worker.sql`:

| Item    | Value                                                                                                                     |
| ------- | ------------------------------------------------------------------------------------------------------------------------- |
| Does    | creates the role to spec if it is absent; otherwise verifies it. It never alters an existing role.                        |
| Run as  | a superuser; a PG15 CREATEROLE role; or Supabase PG16+ `postgres` through `session_directory_search_worker_supabase.sql`. |
| Success | exit 0: the role exists to spec.                                                                                          |
| Failure | `42501`: this caller may not create the role. `55000`: an existing role differs. Nothing is changed.                      |

Phase 2, `session_directory_search_worker_verify.sql`:

| Item                               | Value                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Required direct grants             | exactly: `USAGE` on schema `sessions`, and `EXECUTE` on each approved RPC, none `WITH GRANT OPTION`.                                                                                                                                                                                                                                                                                                                        |
| Approved RPCs (this migration set) | `sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)` (from `20261030020000`); `sessions.session_directory_pointers(text, text, text)` and `sessions.session_directory_readiness()` (from `20261031001000`); `sessions.list_session_directory(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)` and `sessions.session_directory_activity(text, timestamptz, timestamptz)` (from `20261101050000`); `sessions.session_brief(text, text, text)` (from `20261102000000`). A migration that adds, removes or re-signs one updates the list in the same change. |
| Missing or redefined               | an approved RPC that is missing, or a function of the same name with another signature, fails. The allowlist never shrinks to match the database.                                                                                                                                                                                                                                                                           |
| Everything else                    | no direct reference to the role (`pg_shdepend`) in any database of the cluster; effective privileges are checked in the current database only: no owned objects; no table, column, sequence, function, schema, database, type, language, large object, parameter, foreign server or tablespace privileges; no default privileges; no RLS policies.                                                                          |
| One database per cluster           | roles are cluster-wide, and phase 2 can't see other databases' catalogs. So any reference to the role from another database of the same cluster (for example a second migrated copy) fails closed. Keep the migration set in one database per cluster, as the CNPG HoopDb cluster and a Supabase project do.                                                                                                                |
| PUBLIC                             | only aclitems that name this role count. Privileges it gets through PUBLIC are not its own grants and are ignored.                                                                                                                                                                                                                                                                                                          |
| Success                            | exit 0. It changes nothing.                                                                                                                                                                                                                                                                                                                                                                                                 |
| Failure                            | `55000`, naming every violation. Nothing is changed; the deploy must stop and an operator decides the fix.                                                                                                                                                                                                                                                                                                                  |

Phase 1 creates no membership when a superuser runs it. On PostgreSQL 16+, Supabase's
non-superuser CREATEROLE `postgres` necessarily receives ADMIN on a custom role, recorded
with grantor `supabase_admin`. The Supabase wrapper binds `hoopdb.db_mode=supabase`; phase
1, the migration guard, phase 2 and the live gate then accept only that frozen catalog
shape. `postgres` cannot inherit or SET the dedicated role, but ADMIN plus CREATEROLE
means it can grant or drop it. That residual administrative capability is explicit and
is never accepted in local or selfhosted mode. A grant made after PASS is caught by the
next live Worker phase-2 probe; a runtime-role grant is caught by the next migrator
preflight.

## Readiness (what makes a reader ready)

A reader is ready only when all three hold:

1. the ledger is complete;
2. the marker in `sessions.__96bulls_role_validation` is `'pass'` for the reader's own
   migration-set generation;
3. phase 2 passes **live**, on this check.

The marker is generation, coordination and audit evidence, never the sole authority. A
grant added after a PASS fails the next check with no migrator involved. The same rule
gates `waitForSchema` (startup), worker startup, every worker `/readyz` probe, every
ingest `/readyz` probe, and the worker's claim loop before it takes new work.

The check (`schemaVerdict` in `src/shared/db.ts`) is one transaction, `READ ONLY`
and pinned to `READ COMMITTED`, in this order:

1. the ledger (`schema_pending` if incomplete);
2. the marker (`validation_pending` unless `'pass'` for this generation; phase 2 is
   then skipped, so nothing becomes ready without it);
3. phase 2 (`role_validation_failed` with its SQLSTATE on any error); then the live
   `sessions` function boundary: no function EXECUTE-able through PUBLIC or by the runtime
   role (`55000`; see "Function boundary");
4. the marker again, as the **final read** (`validation_pending` unless still `'pass'`
   for this generation).

`READ COMMITTED` is deliberate. Every statement takes a fresh snapshot, so the final read
sees a migrator that set the marker to `'pending'` while phase 2 was running. Under
`REPEATABLE READ` it would reuse the first snapshot and miss that change.

- **One source:** it executes `supabase/bootstrap/session_directory_search_worker_verify.sql`,
  the same file the migrator runs and `mark_pass.sql` includes with `\ir`.
- **Bounded:** the transaction has its own `statement_timeout`
  (`HOOPDB_ROLE_GATE_TIMEOUT_MS`, default 2000). Values outside 1..10000, or non-integers,
  close the gate (`gate_timeout_invalid`) rather than running unbounded.
- **No lock, no cache:** it never takes the migrator's advisory lock, so a long migrate
  never blocks a probe, and it runs on every probe and before every claim.
- **Its own role:** it runs as the role the service connects as (`HOOPDB_URL`). That
  role needs SELECT on the ledger and the marker table. Phase 2 reads only PUBLIC-readable
  catalogs: `pg_roles`, `pg_auth_members`, `pg_db_role_setting`, `pg_shdepend`,
  `pg_namespace`, `pg_proc`, `pg_database` and `pg_default_acl`.
- **Fails closed:** any error closes the gate, is logged with its SQLSTATE and stage
  (`live role validation gate closed`) and makes the reader not ready. That covers a
  violation (`55000`), an unreadable catalog (`42501`), a timeout (`57014`) and a lost
  connection.

**Current-tick semantics.** A verdict holds as of its final read and only for the one
action it gates: one probe response, or one claim. The worker checks the gate again
before every stage's claim and before its side tick, so a gate that closes mid-tick stops
the next claim.

The most a closing gate can leave running is the batch claimed just before it closed:
at most `WORKER_BATCH_SIZE` messages (default 10) of one stage. That batch runs to
completion inside its poll call (acked, retried or dead-lettered). If the worker
crashes, the batch is redelivered when its lease (`WORKER_LEASE_SECONDS`, default 300)
expires. So the window is one batch's processing time. While the gate is closed the
worker claims nothing, and it resumes on the first check that passes.

Record format, the contract for #75's external verifier:

| Column       | Type and value                                                                                                                                                                                                                                            |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`       | `text PRIMARY KEY`, always `'session_directory_search_worker'`                                                                                                                                                                                            |
| `generation` | `text NOT NULL`: sha256 hex of the migration set (every `supabase/migrations` and `src/hoopdb/migrations` file by name and sha256, plus the phase 2 verifier). Print it with `bun src/hoopdb/migrate.ts --generation` from the same image as the readers. |
| `status`     | `text NOT NULL`, `'pending'` or `'pass'`                                                                                                                                                                                                                  |
| `updated_at` | `timestamptz NOT NULL`                                                                                                                                                                                                                                    |

Transitions:

1. The migrator takes `pg_advisory_lock(961996, 1)`, so concurrent migrators run one at a
   time. Before any migration step, it writes `('pending', <its generation>)` and
   commits. From that moment, every reader is non-ready.
2. Phase 2 and the change to `'pass'` happen in one transaction, still under the lock, and
   only for the generation that is pending. Nothing else writes `'pass'`.
3. Anything else leaves the row `'pending'`: a crash mid-migration, a migration failure, a
   phase 2 failure, or a failure between the last migration commit and phase 2. The
   next migrator for that image starts again at step 1.
4. A `'pass'` for a different generation (a stale earlier PASS, or another image's)
   is not ready for these readers.

**With the in-process verifier off** (`HOOPDB_ROLE_VERIFY=false`), the migrator still
writes `'pending'` but never `'pass'`. Only an external verifier can then make the
readers ready, by running, after migrate:

```sh
GEN=$(bun src/hoopdb/migrate.ts --generation)
psql -v ON_ERROR_STOP=1 -X -v generation="$GEN" \
  -f supabase/bootstrap/session_directory_search_worker_mark_pass.sql
```

In one transaction, that script:

- takes the same advisory lock (`pg_advisory_xact_lock(961996, 1)`);
- runs phase 2;
- sets `'pass'` only if the row is `'pending'` for exactly `$GEN`.

Otherwise it fails with `55000` and changes nothing.

Kubernetes Job and pod ordering is defense in depth only. Correctness rests on the
marker: a reader started early, or a migrator that died, simply stays non-ready.

## Database mode, runtime role and one migration authority

`src/hoopdb/migrate.ts` resolves an explicit profile **before opening any connection**. It
never infers anything from the hostname. An invalid profile writes nothing anywhere.

| Variable                        | Contract                                                                                                                                                                                                                                                                                                          |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HOOPDB_DB_MODE`                | required, no default: `local` (Docker stack), `selfhosted` (operator-controlled Postgres), `cloudsql` (Google Cloud SQL) or `supabase`. Anything else: `db_mode_invalid`.                                                                                                                                         |
| `HOOPDB_SUPABASE_SHIM`          | `local`, `selfhosted` and `cloudsql`: must be set explicitly to `true` or `false` (`shim_unset` otherwise). `supabase`: only unset or `false`. `true` is refused (`shim_forbidden`), because `supabase_shim.sql` must never run on a Supabase project.                                                            |
| `HOOPDB_ROLE_BOOTSTRAP`         | defaults to the shim choice. In `supabase` mode it is refused if forced on (`bootstrap_forbidden`); the bootstrap is an operator step there.                                                                                                                                                                      |
| `HOOPDB_RUNTIME_ROLE`           | required (`runtime_role_unset`): the role the services connect as.                                                                                                                                                                                                                                                |
| `HOOPDB_RUNTIME_ROLE_LOGIN`     | required in `selfhosted`, `cloudsql` and `supabase` (`runtime_role_login_unset`): `true` if the runtime role logs in itself, `false` if the services log in as a workload role that is a member of it.                                                                                                            |
| `HOOPDB_RUNTIME_WORKLOAD_ROLE`  | optional: the one role allowed to be a member of the runtime role (without ADMIN OPTION).                                                                                                                                                                                                                         |
| `HOOPDB_FUNCTION_BOUNDARY_MODE` | required in `selfhosted`, `cloudsql` and `supabase`: `global-default-revoke`, or `event-trigger` (supabase only; refused elsewhere with `function_boundary_mode_unsupported`). The catalog must prove the selected control, see "Function boundary". The old `HOOPDB_FUNCTION_BOUNDARY_APPROVED` name is refused. |

**Runtime role: dedicated, exact least privilege.** It is checked under the migrator's
advisory lock before any write (before the ledger and marker tables are created, before
the pending marker upsert, before any step), and again after the last migration, before
phase 2 can mark PASS.

- **Missing role:** `runtime_role_missing`.
- **The migrator itself (owner-as-runtime):** accepted only in `local` (the Docker
  stack, where one superuser runs everything). Refused in `selfhosted`, `cloudsql` and `supabase`
  (`runtime_role_unsafe`).
- **Who creates the dedicated role:**
  - `selfhosted` (CNPG): the privileged
    `supabase/bootstrap/hoopdb_runtime_role.sql`, run as a superuser (CNPG `postgres`)
    before migrate on every deploy, like the Worker bootstrap. It is idempotent, creates
    the role with no privileges and never sets a password; the credential is provisioned
    separately.
  - `cloudsql`: the Cloud SQL Admin API creates the dedicated LOGIN user while assigning
    the already-empty `anon` compatibility role, preventing the API's automatic
    `cloudsqlsuperuser` membership. Before migrations grant `anon` anything and before
    the user can reach a workload, the Admin API replaces its database-role list with
    an empty list (`assign-roles --database-roles= --revoke-existing-roles`). This is an
    API operation, not a SQL `REVOKE`: PostgreSQL 17 records the API grant under a
    provider identity that the customer migration owner cannot impersonate. The
    migrator then verifies the resulting LOGIN-only role and its exact
    database/schema/object privileges. Its password and URL are stored in 1Password and
    supplied only through the runtime Secret.
  - `supabase`: pre-provisioned by an operator and verified by the migrator. On PG16+
    the exact unavoidable `postgres` creator edge is accepted only when its grantor is
    the frozen superuser `supabase_admin`, ADMIN is true, INHERIT and SET are false, no
    other unconfigured member exists, and the database mode is exactly `supabase`.

**The allowlist** (`runtimeRoleViolations` in `src/hoopdb/migrate-guards.ts`). Every
violation is named:

- LOGIN or NOLOGIN exactly as `HOOPDB_RUNTIME_ROLE_LOGIN` says. NOSUPERUSER, NOCREATEDB,
  NOCREATEROLE, NOREPLICATION, NOBYPASSRLS.
- A member of no role. Its only ordinary member may be `HOOPDB_RUNTIME_WORKLOAD_ROLE`,
  with ADMIN false, INHERIT/SET true, and a catalog-superuser grantor. In exact
  `supabase` mode on PG16+, the workload grantor may instead be frozen `postgres` only
  when the same target carries the exact `postgres`/`supabase_admin` creator edge. That
  creator edge may coexist with the configured workload edge; every mutation or second
  ordinary member is refused.
- Owns nothing. No CREATE on any schema or on the database, however it is obtained
  (PUBLIC or memberships included).
- **CONNECT is required** (effective) for the runtime role, and for the workload role
  when one is configured. A LOGIN role that can't connect is refused.
- **TEMPORARY is refused**, except for one Supabase-only platform baseline: effective
  TEMPORARY must come solely from PUBLIC, the role must have no direct TEMPORARY ACL or
  PUBLIC GRANT OPTION. A configured workload does not change this database-wide PUBLIC
  baseline. Local, selfhosted and cloudsql still refuse every effective TEMPORARY grant.
  This is compatibility, not a
  service requirement: no service creates temporary objects or relies on runtime
  `search_path`; see `docs/evidence/runtime-temp-usage.md`.
- **Approved, and nothing else:** `USAGE` on schema `sessions`; read-only migration
  ledger/marker access; `CONNECT`; and the immutable application table, identity-sequence,
  callable-function and RLS-policy allowlists in `src/hoopdb/migrate-guards.ts`. The
  migrator installs those grants only after all schema steps and validates the resulting
  catalog before PASS. There is no ownership, DDL, `TRUNCATE`, `REFERENCES`, `TRIGGER`,
  `MAINTAIN`, GRANT OPTION, or `BYPASSRLS`.
- **Trusted backend, not a tenant database role:** the runtime credential is shared by
  the ingest and worker pods, which must process rows for every tenant. Its role-scoped
  `bulls96_trusted_backend` RLS policies are therefore deliberately all-row. Tenant
  isolation for user-facing traffic is enforced by the authenticated RPCs and required
  application filters; the runtime role must never be inherited by or exposed to an end
  user. The database boundary still removes the former owner capability: no ownership,
  DDL, `BYPASSRLS`, unlisted function execution, or privileges outside the reviewed ACL.
- **No EXECUTE on an unlisted `sessions` function; trigger functions are never listed.**
  A trigger fires without its caller holding EXECUTE (a real PostgreSQL test shows it).
  In the same transaction as phase 2 and the PASS write, the migrator runs
  `REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC` on every run. That is
  idempotent; if validation fails, the revoke rolls back with it (a test proves the state is
  unchanged). It fixes existing functions only, not future ones; see "Function boundary"
  below for the residual and how activation fails closed on it. A per-schema
  default-privileges GRANT in `sessions` naming PUBLIC or the runtime role is detected and
  refused. That is detection only, not a fix: per-schema ALTER DEFAULT PRIVILEGES cannot
  revoke PostgreSQL's global or built-in PUBLIC EXECUTE.
- **Schema and type USAGE:** effective USAGE is accepted only on `sessions` (approved) and
  `public`. `public` gets it from PostgreSQL's own PUBLIC default; it is non-mutating,
  because it only lets the role resolve names, and every object in `public` is checked
  above. USAGE on any other non-system schema is refused, PUBLIC-derived included. Type
  USAGE through PUBLIC (PostgreSQL's default for every type, needed to read typed columns)
  is accepted as non-mutating; a type grant naming the role is a direct reference, refused.
- **Relation reachability:** effective PUBLIC-derived table, view, sequence and
  PostgreSQL 17+ MAINTAIN grants are
  effective only when the role also has USAGE on the containing schema. The guard uses
  that catalog rule instead of hard-coding extension relations. A PUBLIC relation grant
  in an inaccessible extension schema is inert; adding schema USAGE makes the same grant
  fail. A direct relation or column ACL naming the runtime role is always refused through
  `pg_shdepend`, even when the schema is inaccessible. `sessions` is treated as reachable during preflight because the migrator itself
  grants the approved USAGE later in the same run. Direct relation ACLs in other
  databases remain refused because their schema catalogs cannot be evaluated here.
- **Coverage, stated exactly:**
  - In the current database: effective privileges in every non-system schema. That covers
    tables, views and sequences (any privilege), EXECUTE on SECURITY DEFINER functions
    anywhere, EXECUTE on every `sessions` function, CREATE on any schema, and CREATE,
    TEMPORARY and CONNECT on the database. PUBLIC-derived grants are included.
  - In every database, direct references only: those naming the role (`pg_shdepend`): ownership, ACL
    entries, default ACLs, policies.
  - **Not covered:** PUBLIC-derived privileges in other databases (for example CONNECT to
    another database through PUBLIC: keep one HoopDb database per cluster, or revoke
    PUBLIC CONNECT on the others); and SECURITY INVOKER functions outside `sessions`,
    which run with the caller's own (already validated) privileges. Extension functions
    are reviewed with the extension.
- **Out of scope here:** the services' own runtime DML grants (ingest, workers) belong to
  #98. A runtime role holding them is refused until this allowlist is extended in a
  reviewed change.

The migrator grants exactly `USAGE` on `sessions` and `SELECT` on the two tables,
idempotently.

**Database ACL contract.**

- **selfhosted:** `hoopdb_runtime_role.sql`, run connected to the HoopDb database, also
  runs `REVOKE TEMPORARY ON DATABASE <db> FROM PUBLIC` and
  `GRANT CONNECT ON DATABASE <db> TO <runtime role>`.
  - **Database-wide side effect:** every unrelated role in that database that relied on
    PUBLIC for TEMPORARY loses it. The owner and superusers keep it. A real PostgreSQL
    test shows an unrelated role losing it.
  - Grant TEMPORARY explicitly to any other role that needs it.
- **supabase:** the migrator never changes the database ACL. It accepts Supabase's frozen
  PUBLIC TEMPORARY baseline only when catalog evidence proves there is no direct role
  grant and no PUBLIC GRANT OPTION. A direct TEMPORARY grant, a membership-derived grant,
  or any non-Supabase mode fails closed. The runtime role must still hold effective
  CONNECT. The residual is explicit: the role can create temporary objects because the
  hosted platform grants that capability to every role, although no 96Bulls runtime path
  uses it.

**One migration authority and the full ordered sequence (decision point).** Also under the
lock and before any write, the migrator refuses:

- `mixed_migration_authority`: `supabase_migrations.schema_migrations` (the
  `supabase db push` history) records any of these migrations, or exists but can't be
  read.
- `ledger_unknown_step`: the ledger names a schema step this image doesn't have.
- `ledger_gap`: the ledger is not one contiguous prefix of the ordered steps (upstream,
  then 96bulls). A first 96bulls row with no upstream rows is refused, never backfilled.
- `migration_changed`: a recorded step's file changed.
- `unledgered_schema`: the catalog sentinel of an unrecorded step is already true, or an object that an unrecorded step creates already exists. The
  inventory is every table, view, sequence, index, function, trigger, policy and role in
  the migration files' CREATE statements (`inventoryOf`; a test proves it covers
  everything a full migration leaves in `sessions`). So a database where only
  `sessions.session_directory` exists is refused before any write, not after seven
  ledger rows.

**Per-step sentinels** (`src/hoopdb/migration-sentinels.ts`). Each step has one
read-only catalog predicate, false before the step and true from it on. This catches
ALTER-only and replacement steps that the CREATE inventory can't see. Tests prove the table
names every step, and that each sentinel is false before its step and true after it.

| Step                                                           | Sentinel                                                                                                                |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `upstream:20261025000000_sessions_index.sql`                   | table sessions.session_index                                                                                            |
| `upstream:20261025000100_repo_identity.sql`                    | table sessions.repos                                                                                                    |
| `upstream:20261025000200_scrub_quarantine.sql`                 | function sessions.session_index_scrub_sticky                                                                            |
| `upstream:20261025000300_upsert_keep_on_null.sql`              | replacement: sessions.upsert_session_index keeps stored values on NULL (its body has coalesce(excluded.title, s.title)) |
| `upstream:20261025000600_pipeline_queue.sql`                   | table sessions.pipeline_jobs                                                                                            |
| `upstream:20261030000000_relayhistory_machines_compat.sql`     | ALTER-only: column sessions.machines.cursors_json                                                                       |
| `upstream:20261030010000_session_directory.sql`                | table sessions.session_directory                                                                                        |
| `upstream:20261030020000_session_directory_short_summary.sql`  | column sessions.session_directory.short_summary                                                                         |
| `upstream:20261031000000_migration_assertion_ledger.sql`       | table sessions.migration_assertion_uses                                                                                 |
| `upstream:20261031000100_manifest_readiness.sql`               | table sessions.manifest_readiness                                                                                       |
| `upstream:20261031001000_session_directory_reads_expand.sql`   | table sessions.session_directory_resurrection_fences                                                                    |
| `upstream:20261101000000_migration_ingest_audit_base_read.sql` | table sessions.migration_base_read_uses                                                                                 |
| `upstream:20261101000200_claimless_window_activations.sql`     | table sessions.claimless_window_activations                                                                             |
| `upstream:20261101050000_session_directory_list.sql`           | table sessions.session_directory_agents                                                                                 |
| `upstream:20261102000000_session_briefs.sql`                   | table sessions.session_briefs                                                                                           |
| `upstream:20261102010000_session_directory_canonical_ids.sql`  | column sessions.session_index.canonical_session_id                                                                      |
| `upstream:20261105024000_session_brief_documents.sql`          | table sessions.session_brief_document_outbox                                                                            |
| `upstream:20261104022000_session_ingest_ledger.sql`            | table sessions.ingest_batches                                                                                           |
| `upstream:20261104021000_session_directory_event_metadata.sql` | table sessions.session_directory_event_times |
| `upstream:20261105020000_session_replay_pages.sql` | table sessions.session_replay_publications |
| `upstream:20261105021000_session_large_read_jobs.sql` | table sessions.session_large_read_jobs |
| `upstream:20261105022000_session_desktop_catalog.sql` | column sessions.session_directory.catalog_read_version |
| `upstream:20261105023000_session_analysis_publications.sql` | table sessions.session_analysis_publications |
| `upstream:20261102020000_session_brief_state.sql`              | ALTER-only: column sessions.session_briefs.state                                                                        |
| `upstream:20261104020000_auth_sessions.sql`                    | table sessions.auth_sessions                                                                                            |
| `upstream:20261104020100_auth_session_verify.sql`              | table sessions.auth_token_verifier_policy                                                                               |
| `upstream:20261104030000_ingest_fence_ownership.sql`           | table sessions.ingest_session_ownership                                                                                  |
| `upstream:20261104040000_ingest_s3_proof_seed.sql` | table sessions.ingest_seed_captures |
| `upstream:20261104050000_buffered_upload_receipts.sql` | table sessions.ingest_stage_intents |
| `upstream:20261104051000_ingest_buffer_gc.sql` | column sessions.ingest_stage_intents.gc_lease_token |
| `upstream:20261104052000_ordered_ingest_controls.sql` | column sessions.ingest_buffer_admission.control_epoch |
| `upstream:20261105010000_frozen_source_custody.sql` | table sessions.ingest_frozen_source_completeness |
| `upstream:20261104055000_lazy_current_custody.sql` | table sessions.ingest_lazy_policy |
| `upstream:20261104054000_ingest_custody_baseline.sql` | table sessions.ingest_custody_imports |
| `upstream:20261104053000_ingest_first_ack_seats.sql` | table sessions.ingest_receipt_seat_clock |
| `upstream:20261105025000_canonical_feature_journal.sql` | table sessions.canonical_feature_journal |
| `upstream:20261105026000_canonical_history_lineage.sql` | table sessions.ingest_history_carriers |
| `upstream:20261105030000_canonical_feature_consumer_acks.sql` | table sessions.canonical_feature_copy_state |
| `upstream:20261104060000_ingest_canonical_publication.sql` | table `sessions.ingest_publication_heads` |
| `96bulls:0001_96bulls_observability.sql`                       | column sessions.pipeline_jobs.traceparent                                                                               |
| `96bulls:0002_session_ext_and_summaries.sql`                   | table sessions.session_index_ext                                                                                        |
| `96bulls:0003_fanout_ledger.sql`                               | ALTER-only: column sessions.session_index_ext.fanout_revision                                                           |
| `96bulls:0004_summary_pointers.sql`                            | DELETE + ALTER: column sessions.session_summaries.object_key (summary dropped)                                          |

For each ALTER or replacement step (`20261025000300`, `20261030000000`,
`20261102010000`, `20261102020000`, `0003`, `0004`),
real PostgreSQL 15 and 17 tests cover two states. Honestly applied and ledgered is
accepted with no false positive. Applied but unledgered is refused before any write.

It never imports a backlog into the ledger and never marks a step applied. Resolving any of
these is an explicit operator decision, recorded in the activation dossier (#98):

1. choose one authority for this database (`hoopdb-migrate`, or the Supabase CLI, never
   both);
2. reconcile by hand and review the result: for example, recreate a disposable database;
   or verify each object against the file and have the owner write ledger rows in a
   reviewed change;
3. only then rerun the migrator.

**Evidence.** The `pg-proof` workflow (`.github/workflows/pg-proof.yml`) runs
`scripts/pg-proof/role-proof.ts` on the exact head against real PostgreSQL 15 and 17
service containers. It uploads `pg-proof-pg15` and `pg-proof-pg17` artifacts, each a JSON
file (head, server version, each case's pass/fail) plus its log.

## Function boundary (threat, residual, activation gate)

**Threat.** PostgreSQL gives every new function PUBLIC EXECUTE by built-in default, and a
per-schema `ALTER DEFAULT PRIVILEGES` cannot revoke that. Where no owner-scoped control is
installed, a function created in `sessions` after a migrate, outside the migrator (a
hand-run DDL, a future tool), is **directly invocable** until the next migrate's revoke.
Anyone with USAGE on `sessions` can call it, by database credential or through PostgREST
RPC. Real PostgreSQL tests show both sides:

- with the selfhosted control installed, such a function is **not** invocable (42501);
- with the built-in default in place, an unrelated role calls it.

**What does fail closed.**

- **The live gate** (`SESSIONS_FUNCTION_GATE_SQL` in `src/shared/db.ts`, part of every
  readiness check and every worker claim) refuses with `55000` when any `sessions`
  function is EXECUTE-able through PUBLIC, or by the runtime role unless it owns the
  function (owner-run local). Readiness goes 503 at the next probe and the worker stops
  claiming (tested on PG15 and PG17).
- **The ACL manifest** (`src/hoopdb/sessions-function-acl.json`) is the exact expected
  ACL of every `sessions` function after a clean migrate. CI compares it on PGlite and on
  real PostgreSQL 15 and 17.

**Who reaches `sessions`, by which path** (from the repository, not from a live query):

| Path                                                                            | Roles                                                                                                                                                                                                                                                                                                          | Evidence                                                                              |
| ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| PostgREST RPC, `Content-Profile: sessions` (so `sessions` is an exposed schema) | the search Worker, via `authenticator` role switching                                                                                                                                                                                                                                                          | `src/proof/aws/checks.ts` (`/rpc/search_session_directory`), #73 real-PostgREST tests |
| `authenticator` memberships                                                     | `session_directory_search_worker` (NOINHERIT, no ADMIN: the Worker contract); on Supabase also the platform's `anon`, `authenticated`, `service_role`                                                                                                                                                          | migration 20261030020000 role guard; Supabase platform defaults                       |
| USAGE on `sessions`                                                             | `authenticated`, `service_role` (20261025000000), `session_directory_search_worker` (20261030020000); `anon` has none                                                                                                                                                                                          | the migrations; PGlite mapping                                                        |
| EXECUTE (effective, after a clean migrate)                                      | Worker: only the exact JWT-scoped read RPCs in the manifest. `authenticated`: `jwt_org_id`, `jwt_workspace_ids`, `member_of`, `repo_group`, `repo_live`, `repo_visibility_ids`. `service_role`: only the exact runtime RPCs in the manifest. `anon`: none. PUBLIC: none. | `src/hoopdb/sessions-function-acl.json` (exact, CI-checked)                           |

So the "(B) `sessions` is unexposed and no untrusted role has USAGE" alternative **cannot
hold**: exposure is by design, and `authenticated` holds USAGE. Exposure is not the
problem. The problem is PUBLIC EXECUTE on future functions. Each intended RPC has explicit
grants to the exact API roles and PUBLIC stays revoked.

**Function-boundary gate (selfhosted and supabase; only explicit local mode is ungated).**
Before any write, the migrator refuses with `function_boundary_unproven` unless the catalog
proves the control that `HOOPDB_FUNCTION_BOUNDARY_MODE` selects. That variable is
configuration chosen by the reviewed deployment, not a human approval: the catalog control
itself is the proof. The old name, `HOOPDB_FUNCTION_BOUNDARY_APPROVED`, is refused, so stale
config fails closed.

- **`global-default-revoke` (the only mode implemented):**
  `ALTER DEFAULT PRIVILEGES FOR ROLE <role> REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`
  in the HoopDb database, for the migration owner and for every other role that can
  create functions in `sessions`.
  - **selfhosted:** the privileged, idempotent
    `supabase/bootstrap/hoopdb_function_default_revoke.sql`
    (`-v migration_owner=hoop`, run as a superuser, in the HoopDb database) installs it
    before migrate. It sets the global function default of the owner **and of every
    superuser** (a superuser can always CREATE in `sessions`). That is global for those
    roles: every function they create later in that database, in any schema, starts
    executable by its owner only. That is safe only because the selfhosted database is
    dedicated to HoopDb. Before any write it refuses (nothing changed) when a target's
    existing default grants anything beyond the built-in owner/PUBLIC EXECUTE (a poisoned
    default, for example EXECUTE to `authenticated` or a grant option), or when another
    role can CREATE in `sessions` without an exact-safe default of its own, or a
    `sessions`-scoped function default grants another role. Its
    postcondition re-checks every owner, superuser and `sessions` creator. Real PG15/17
    tests show:
    - a new owner function in `sessions` and in a second schema is not
      PUBLIC-executable;
    - after the bootstrap, functions created later in `sessions` by the owner and by a
      superuser are not executable (`has_function_privilege`) by PUBLIC (a fresh role),
      `authenticated` or the runtime role;
    - both poisoned-default variants fail the bootstrap with `pg_default_acl` unchanged,
      and `--activation-check` and migrate refuse;
    - the intended grants survive the revoke and the next migrate: the Worker's three RPCs,
      `authenticated`'s six helpers and `service_role`'s grants, all checked against the
      ACL manifest;
    - the AWS CNPG and GCP fixture env resolves to this profile (the catalog still
      decides).
  - **supabase:** the owner is `postgres`, so the same statement would reach every future
    postgres-created function in every schema of the project. It is never run from here.
    An operator decides it (#98), and until then supabase migrate stays refused. The
    operator path (dossier 4.2, each live step needs Khaliq's OK): `postgres`'s own
    `ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`
    (blast radius: every schema), then `--activation-check` with
    `HOOPDB_DB_MODE=supabase`, then migrate.
  - **Supabase platform trust root (supabase mode only):** Supabase's `postgres` is not
    a superuser and cannot alter `supabase_admin`'s defaults. So in
    `HOOPDB_DB_MODE=supabase` a CREATE-capable role is left out of creator coverage only
    when BOTH `pg_roles.rolsuper` is true at gate time AND its name is in the
    code-reviewed `SUPABASE_PLATFORM_SUPERUSERS` constant (`supabase_admin` only,
    cited from the Supabase roles docs). No environment variable changes the list. Any
    other superuser (for example `ops_admin`), a listed name that is not a superuser,
    and the migration owner stay covered. Excluded roles are listed with their attributes
    in `excluded_platform_superusers`. `selfhosted` keeps the full check, superusers
    included (tested on PG15 and PG17).
  - **Google Cloud SQL (`cloudsql` mode):** Google documents `cloudsqladmin` as an
    immutable system user with database-superuser privileges. The fixed
    `CLOUDSQL_PLATFORM_SUPERUSERS` allowlist therefore excludes exactly
    `cloudsqladmin`, only while its catalog row is still SUPERUSER and never when it is
    the migration owner. No environment setting extends the list. The customer
    migration owner runs `cloudsql_owner_function_default_revoke.sql`; every ordinary
    role with effective CREATE in `sessions`, including the owner, remains covered by
    the catalog gate. `cloudsql_migration_roles.sql` uses and drops a disposable
    CREATEROLE trampoline; dropping it removes PostgreSQL 16+ automatic creator
    memberships and leaves `anon`, `authenticated`, `service_role`, and the NOLOGIN
    Worker membership-free. The Cloud SQL `service_role` is NOBYPASSRLS and exists only
    to satisfy shared migration grants; the GCP runtime never connects as it. The
    canonical verifier then proves the Worker role and grants.
- **Exact default ACL:** the migration owner's `pg_default_acl` row with
  `defaclnamespace = 0` and `defaclobjtype = 'f'` must normalize to EXACTLY
  `{<self>:EXECUTE}`: owner EXECUTE, no grant option, no other grantee. Merely "PUBLIC
  revoked" is not enough: a default that also grants `authenticated` (or anyone) EXECUTE,
  or carries a grant option, refuses the gate. No row means the built-in default (PUBLIC
  EXECUTE) and refuses too.
- **Creator coverage, before any function exists:** every role with effective CREATE on
  `sessions` (`has_schema_privilege`, so membership, the schema owner and superusers
  count) must be the migration owner or have its own exact-safe global function default.
  A CREATE-capable role that has not created anything yet still refuses the gate (tested
  on PG15 and PG17). Separately, every existing `sessions` function must be owned by the
  owner (`sessions_functions_owned_by_owner`).
- **Run-time exact manifest (every non-local mode: `global-default-revoke` in
  `selfhosted` and `cloudsql`, `event-trigger` in `supabase`):** after the last
  migration, inside the validation transaction with phase 2 and the PASS write, every
  `sessions` function's ACL must equal `src/hoopdb/sessions-function-acl.json` plus
  `<runtime role>:EXECUTE` on `RUNTIME_APPLICATION_FUNCTIONS` (#116)
  (`sessionsFunctionAclDrift` in `src/hoopdb/migrate-guards.ts`). Any difference, an
  extra grantee such as `authenticated` or a missing manifest grant, refuses with
  `function_acl_drift`: the transaction rolls back, nothing is marked PASS and the
  readiness marker stays `pending`, so readers stay not ready. Local (Docker dev) mode
  has no such check. No Cloud SQL-specific grantee (for example `cloudsqlsuperuser` or
  an IAM role) is granted on `sessions` functions by the repository: the trampoline
  bootstrap (`cloudsql_migration_roles.sql`) creates roles only, and the Cloud SQL
  pg-proof path matches the manifest. A live Cloud SQL instance is NOT OBSERVED. Tested on
  PG15 and PG17 (selfhosted extra grant refused, marker pending, clean migrate passes)
  and in the unit-test mode matrix.
- **Read-only preview:** outside local mode, with `HOOPDB_RUNTIME_ROLE` set,
  `--activation-check` runs the same comparison read-only and reports
  `function_acl_drift_extra` (grants the migrator would refuse, duplicate grantors
  included; exit 1), `function_acl_missing_refused` (missing manifest grants nothing
  re-adds, such as `service_role` on an existing function; exit 1), `function_acl_missing`
  (only what the migrator adds: the runtime role's #116 grants, and functions a pending
  migration creates; exit 0) and `function_acl_public_revoked` (PUBLIC EXECUTE the migrator
  revokes first; exit 0). Known gap, on the safe side: when a pending migration would itself
  re-grant a missing grant on an **existing** function, the preview still lists it under
  `function_acl_missing_refused` (exit 1), although the migrate would pass. The preview can
  only over-refuse, never under-refuse. In that case, read the pending migration and
  proceed only on its reviewed grants.
- **No schema-scoped grant in `sessions`:** a function default defined `IN SCHEMA sessions`
  (by any role) adds to the global default, so any entry granting a role other than its
  own (for example `authenticated`) refuses the gate and the bootstrap
  (`sessions_schema_function_defaults` must be empty; tested on PG15 and PG17).
- **`event-trigger` (supabase only):** a reviewed `ddl_command_end` event trigger scoped
  to `sessions`, with no project-wide `ALTER DEFAULT PRIVILEGES`. Accepted only with
  `HOOPDB_DB_MODE=supabase`; `selfhosted`, `cloudsql` and `local` refuse it
  (`function_boundary_mode_unsupported`) and keep `global-default-revoke`.
  - **Install:** `supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql`, run
    as `postgres` after the runtime-role and Worker bootstraps and before migrate
    (`-v runtime_role=bulls96_runtime`). Transactional and mode-bound. It creates schema
    `hoopdb_boundary` (no PUBLIC access), the SECURITY INVOKER plpgsql function
    `hoopdb_boundary.sessions_function_boundary()` (`search_path = pg_catalog, pg_temp`,
    everything fully qualified) and the trigger `hoopdb_sessions_function_boundary` on
    the exact tag set: `CREATE`/`ALTER` `FUNCTION`, `PROCEDURE`, `AGGREGATE`,
    `ALTER ROUTINE`, `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`, `CREATE`/`ALTER
EXTENSION`, `ALTER SCHEMA`, `CREATE TYPE`. It sets the trigger `ENABLE ALWAYS`
    (`'A'`): an `'O'` trigger is skipped in sessions with `session_replication_role =
replica` (confirmed on PG 17.11), as dump and restore tooling sets. Idempotent; an
    existing trigger (including one only `'O'`) or runtime role that differs is refused
    (operator decides). If `CREATE EVENT TRIGGER` is refused (no
    supautils), it fails closed with 55000 and creates nothing.
  - **Adding a `sessions` function:** a migration that adds or changes a `sessions`
    function must, in the same change, update `src/hoopdb/sessions-function-acl.json`, the
    trigger's embedded manifest, its SECURITY DEFINER list (and
    `RUNTIME_APPLICATION_FUNCTIONS` if it is a runtime function), and
    `SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256`; the unit tests fail otherwise.
  - **What it enforces, at fire time** (it filters by the schema NAME `sessions`, which
    need not exist at install): routines created in `sessions` get PUBLIC revoked (the
    range and multirange constructors of a `CREATE TYPE ... AS RANGE` are checked too:
    PostgreSQL makes the bootstrap superuser their owner, so that command is aborted
    instead of leaving PUBLIC-executable functions that would block every later function
    GRANT); then
    the command is aborted (42501) when a touched `sessions` routine is owned by anyone
    but the migration owner, holds an ACL entry outside the embedded manifest (equal to
    `src/hoopdb/sessions-function-acl.json`; owner-only for an unmanifested signature;
    the runtime role written into the function only on `RUNTIME_APPLICATION_FUNCTIONS`),
    or is SECURITY DEFINER outside the frozen list; or when a `sessions`-scoped function
    default grants another role. A function GRANT/REVOKE, an extension or schema change
    rechecks every `sessions` routine. While migrations run the check is "subset of the
    manifest"; exact equality is checked after every non-local migrate (see "Run-time exact
    manifest" below), and the live gate still refuses PUBLIC-executable
    `sessions` functions. Blast radius: a function GRANT
    anywhere in the database is refused while `sessions` itself violates the manifest.
  - **Gate (more than its name):** before any write the migrator (and
    `--activation-check`) compares the catalog with frozen constants in
    `src/hoopdb/migrate-guards.ts`: `evtevent = ddl_command_end`, the exact sorted tags,
    `evtenabled` exactly `A` (ALWAYS), `evtowner` and the function owner = the migration owner, the
    function oid = `hoopdb_boundary.sessions_function_boundary()`, the prosrc sha256
    (normalized: the runtime-role literal put back to its placeholder), `proconfig`
    exactly the pinned search_path, SECURITY INVOKER, volatile, plpgsql, returns
    `event_trigger`, not PUBLIC-executable, the runtime role in the body =
    `HOOPDB_RUNTIME_ROLE`, and schema `hoopdb_boundary` private. Any drift:
    `function_boundary_unproven`. The trigger covers the migration owner (its global
    default may stay built-in); every other CREATE-capable role still needs its own exact
    default, and the platform trust root rule above still applies. Other event triggers
    are listed (`other_event_triggers`): a GRANT a hostile trigger runs is itself DDL and
    fires the reviewed trigger, which aborts it (tested).
  - **Supabase behaviour (from docs and supautils, NOT OBSERVED on the project):**
    Supabase documents that `postgres` can create event triggers
    (https://supabase.com/docs/guides/database/postgres/event-triggers). supautils
    (https://github.com/supabase/supautils, "Privileged Role", "Non-Superuser Event
    Triggers") creates the object as the superuser and then changes its owner to the
    privileged role, so the gate expects `evtowner = postgres` and fails closed otherwise.
    Privileged-role triggers are skipped for superusers and reserved roles, so DDL run as
    `supabase_admin` is not covered: consistent with the platform trust root. They also do
    not fire for `CREATE EXTENSION` (supautils issue 123); the post-migrate manifest check
    and the live gate remain. If `postgres` may set `session_replication_role` (supautils
    `privileged_role_allowed_configs`; the README's example; not verified for the
    project), a session in `replica` mode would skip an `O` trigger, which is why the
    trigger is `A` and the gate accepts only `A`. In every skipped case (superusers,
    reserved roles, `CREATE EXTENSION`) only the live gate (PUBLIC or the runtime role)
    and the next migrate's checks catch a bad ACL. An owner or superuser could still run
    `ALTER EVENT TRIGGER ... DISABLE`; the next migrate's gate refuses it and the live
    gate keeps refusing PUBLIC-executable functions. Whether supautils permits
    `CREATE EVENT TRIGGER` and `ALTER EVENT TRIGGER ... ENABLE ALWAYS` for `postgres` on
    the project is NOT OBSERVED; if `CREATE` is refused the bootstrap fails closed.
  - **Tests:** PGlite (profile binding, a name-only trigger refused, fresh full migrate,
    behaviour, 11 drift cases, other creators, frozen constants vs the file) and real
    PG15/17 on the Supabase-shaped cluster (`supabase_admin` OID 10, non-superuser
    `postgres`; supautils emulated for the install only): no supautils fails closed;
    fresh full migrate passes; GRANT to PUBLIC, `anon` or `authenticated` (unmanifested),
    SECURITY DEFINER, `OWNER TO`, a `sessions` default, another role's function, and a
    hostile second trigger are aborted; CREATE OR REPLACE keeps the ACL; other schemas are
    unaffected; disabled, replaced body, function or trigger owner changed, missing tag,
    SECURITY DEFINER, PUBLIC-executable, a function setting added, schema opened, and a
    runtime-role mismatch each fail the gate before any write. Also: CREATE PROCEDURE and
    CREATE AGGREGATE (owner-only, grants outside the manifest aborted), `SET SCHEMA` into
    `sessions` and RENAME away from or into a manifest signature, `CREATE EXTENSION ...
SCHEMA sessions` and `ALTER EXTENSION ... SET SCHEMA sessions` (plain PostgreSQL,
    where the trigger fires), `CREATE TYPE ... AS RANGE`, replica mode (`A` still aborts a
    PUBLIC grant; an `O` trigger is skipped there and the gate refuses it), and a missing
    manifest grant refused by the next migrate (`function_acl_drift`). Residual: the
    harness emulates supautils by making `postgres` a superuser for the install only, so
    it does not reproduce supautils' skips for superusers and reserved roles; on plain
    PostgreSQL the trigger fires for superusers too.

The evidence is names, booleans and hashes only (null where absent):

- `migration_owner`;
- `sessions_unexposed_and_no_untrusted_usage`, always false here;
- `owner_function_default` (the normalized owner row: `<self>`, `PUBLIC` or
  `role:PRIVILEGE`, `*` marking a grant option; `<built-in>` when no row exists) and
  `owner_function_default_exact`;
- `sessions_create_roles`: each role able to CREATE in `sessions`, with `superuser`,
  `is_migration_owner`, its normalized `function_default` and
  `function_default_exact`; then `sessions_create_roles_covered`;
- `db_mode` (`unspecified` when `--activation-check` runs without `HOOPDB_DB_MODE`:
  full coverage) and `excluded_platform_superusers` (supabase mode only: each role's
  name, `rolsuper`, `rolinherit`, `rolcreaterole`, `rolcreatedb`, `rolcanlogin`,
  `rolreplication`, `rolbypassrls` and its function default);
- `sessions_schema_function_defaults` (`role>grantee:PRIVILEGE` entries; must be empty);
- `sessions_functions_owned_by_owner`;
- `event_trigger_present`, `event_trigger` (the full definition above, with `verified`)
  and `other_event_triggers`;
- `mode`;
- `satisfied`.

`bun src/hoopdb/migrate.ts --activation-check` prints it and exits 1 if unproven (set
`HOOPDB_DB_MODE` to the deployment's mode; an invalid value exits 1 with
`db_mode_invalid`). The
real-PG proof JSON carries the same evidence for a bootstrapped selfhosted database
(`function_boundary_evidence`). The live gate (readiness and worker claims) checks
existing functions only; the default-ACL and creator evidence is enforced by the migrator
and `--activation-check`.

**Claim paths.** The worker's only claim paths are each stage's `pollPostgresStage` and
the side tick, all through `workerTick`, which runs the live gate (sessions-function
check included) before each one. A table-driven test proves the order. The ingest and the
courier claim no queue work.

**Durable follow-up (open):** a dedicated `sessions` object owner, so an owner-scoped
global default revoke has no blast radius outside HoopDb. That needs an ownership transfer
of live objects, so it is out of scope for #99. The schema-specific event trigger above is
the reviewed alternative for Supabase. Tracked in #98.

## CNPG (self-hosted HoopDb)

- **Selfhosted bootstraps, every deploy, before migrate, as the CNPG superuser, in the HoopDb
  database:** `hoopdb_runtime_role.sql` (the dedicated runtime role; it also revokes PUBLIC
  TEMPORARY) and `hoopdb_function_default_revoke.sql` (the exact global function default for the owner and every superuser).
  Not wired yet: the Job is helm-charts #24's.
- **Phase 1, every deploy (required for existing clusters):** a Job ordered before
  `hoopdb-migrate`, for example a pre-migrate step in the same release. It runs phase 1 as
  the cluster superuser `postgres` (the CNPG superuser secret, `enableSuperuserAccess`),
  never as `hoop`, against the HoopDb database. It must succeed before migrate starts.
- **Phase 1, new clusters (optional extra):** `bootstrap.initdb.postInitApplicationSQLRefs`
  pointing at a ConfigMap built from the same file. CNPG runs it once, as `postgres`, at
  init. It never reruns, so the Job above is still the source of truth.
- **Phase 2, every deploy:** `hoopdb-migrate` itself runs it after the last migration (the
  `verify:session_directory_search_worker` step, `HOOPDB_ROLE_VERIFY`, on by default). With `HOOPDB_ROLE_VERIFY=false`, a
  separate post-migrate Job must run `session_directory_search_worker_mark_pass.sql` (not
  the bare verifier, which records nothing), or no reader becomes ready.

A CNPG managed role (`spec.managed.roles`) can express the attributes but not the
role-level `statement_timeout`, so it is not enough on its own.

`hoopdb-migrate` (`src/hoopdb/migrate.ts`) runs both checks on **every** migrate, and never
records either in the ledger:

- **Phase 1** is the `bootstrap:session_directory_search_worker` step, which runs before
  the migrations when the profile enables it (`HOOPDB_ROLE_BOOTSTRAP`, default the
  explicit `HOOPDB_SUPABASE_SHIM` choice; never in `supabase` mode, see below). As non-superuser `hoop`, it only verifies an
  already-provisioned role and fails closed with `42501` if the role is absent, so the
  deploy Job above must run first. On the local Docker stack, the migrator is the database
  superuser and the step creates the role.
- **Phase 2** is the `verify:` step, which runs after the migrations in every stage.

## Supabase (hosted)

On Supabase, `postgres` is not a superuser. Its behavior depends on the server version:

1. Read the version: `SHOW server_version_num;`.
2. **Below 160000 (PostgreSQL 15):** an operator runs the command above as `postgres`
   (CREATEROLE). Creating the role grants `postgres` nothing.
3. **160000 or above:** run the transactional Supabase entry point as `postgres`:
   `psql -v ON_ERROR_STOP=1 -X -f supabase/bootstrap/session_directory_search_worker_supabase.sql`.
   It binds the exact mode, creates the role and verifies the automatic edge in one
   transaction. A different member/grantor, non-super grantor, SET/INHERIT option,
   missing ADMIN option or any additional membership fails with `55000` and rolls back.
4. Check the role with the queries below, then apply the migrations with `hoopdb-migrate`.
   It writes the pending marker, runs phase 2 and records PASS. Migrations applied any
   other way write no marker, so no reader becomes ready (fail closed).
5. With `HOOPDB_ROLE_VERIFY=false`, an operator runs `mark_pass.sql` (above) as `postgres`
   after the migrations. It must exit 0 before the deploy counts as done.

## Checks

```sql
SELECT rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolcreaterole, rolcreatedb, rolreplication
  FROM pg_roles WHERE rolname = 'session_directory_search_worker';
SELECT setdatabase, setconfig FROM pg_db_role_setting s
  JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'session_directory_search_worker';
SELECT g.rolname AS role, u.rolname AS member, x.rolname AS grantor,
       x.rolsuper AS grantor_super, m.admin_option, m.inherit_option, m.set_option
  FROM pg_auth_members m
  JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member
  JOIN pg_roles x ON x.oid = m.grantor
 WHERE 'session_directory_search_worker' IN (g.rolname, u.rolname);
SELECT d.deptype, pg_describe_object(d.classid, d.objid, d.objsubid) FROM pg_shdepend d
 WHERE d.refclassid = 'pg_authid'::regclass
   AND d.refobjid = 'session_directory_search_worker'::regrole;
```

Expected results:

- every attribute is `f`;
- exactly one settings row, `0 | {statement_timeout=2s}`;
- selfhosted/PG15: no membership rows until the separate authenticator binding;
- Supabase PG16+: exactly the `postgres`/`supabase_admin` creator edge described above.
- after the migrations, `pg_shdepend` lists only `a` rows: schema `sessions` and the three
  approved functions. Before `20261030020000`, it lists none.

## Rollback and removal

The migration's down path
(`supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql`) revokes
what the migration granted and leaves the role at its spec. Removing the role is a
separate operator step, done as a superuser after every down that references it:
`DROP OWNED BY session_directory_search_worker; DROP ROLE session_directory_search_worker;`
in each database.

## Rehearsal databases

The HoopDb ledger records each migration's sha256. The only historical checksum alias is
the guard-only `20261030020000` change: an existing ledger row holding
`c223b77e4c875cbc36499e6ca1c8e521b564ebf25facd87313b3e58335918de4` is accepted and
left unchanged, while a fresh database executes and records
`b0ddc7de8c4cff9454638c52563f37f1e1380ed577e2a7535f67ddc2c0b758ce`.
Both hashes, the migration name and the fact that this is the sole alias are pinned by a
test. Any third checksum or any other migration name fails closed. The alias is emitted
in migration logs and `--activation-check`; generation remains based on the current file.

### Auth verifier append-only upgrade and membership

The vendored auth source filenames are preserved in their provenance comments.
This repository applies them as `20261104020000_auth_sessions.sql` and
`20261104020100_auth_session_verify.sql`, after existing upstream migrations.
Do not restore the earlier insertion timestamps: a populated upstream migration
ledger must remain a contiguous prefix. The migrator never renames/imports an
applied ledger row or bypasses its checksum/sequence guard. These auth migrations
are unreleased here; an installation with the earlier draft filenames already
recorded requires explicit operator reconciliation, not an automatic ledger edit.

The verifier bootstrap, migration role guard and phase-2 verifier all require a
switch-only authenticator membership: no ADMIN OPTION, and on PostgreSQL16+
`INHERIT FALSE, SET TRUE`; on PostgreSQL15 the authenticator member role must be
`NOINHERIT`. Existing unsafe membership is refused without changing it. Grants or
role changes remain operator actions. T0's target setup runs both privileged role
bootstraps before its sorted migration files.

`scripts/pg-proof/auth-upgrade-proof.ts` exercises the actual T0 setup, the pinned
21-step pre-auth base ledger plus preserved data, no-op retry, all three guards,
and effective inherited-vs-switch privileges on disposable PG15/16/17. It refuses
existing verifier/authenticator/directory roles and requires explicit disposable
cluster opt-in. The new proof runs before the general role proof in CI.
