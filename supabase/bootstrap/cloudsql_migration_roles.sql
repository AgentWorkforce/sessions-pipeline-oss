-- Cloud SQL migration compatibility roles on PostgreSQL 17+.
--
-- Cloud SQL exposes no customer SUPERUSER. PostgreSQL 16+ records an automatic ADMIN
-- membership, granted by the bootstrap superuser, when a non-superuser CREATEROLE user
-- creates a role. PostgreSQL documents that the creator cannot revoke that grant.
-- https://www.postgresql.org/docs/17/role-attributes.html
--
-- Use a disposable CREATEROLE trampoline instead: it creates the six fixed NOLOGIN
-- roles (the session ingest's two included: 20261102010000 verifies them and never
-- creates a role), then the migration owner drops the trampoline. DROP ROLE removes the
-- trampoline's automatic memberships, leaving every target membership-free. The
-- trampoline and every target are created in this one transaction, so any refusal rolls
-- the entire operation back.
--
-- service_role is deliberately NOBYPASSRLS. GCP has no Supabase/PostgREST service-role
-- runtime: this role exists only because the shared schema grants reference it. Giving a
-- customer-created role BYPASSRLS is both unsupported by Cloud SQL and unnecessary.
--
-- Run directly as the Cloud SQL migration owner before the first migrate and before the
-- canonical session_directory_search_worker.sql role verifier. Idempotent after a clean
-- success; it never repairs or replaces an existing role.
DO $cloudsql_migration_roles$
DECLARE
  v_owner name := current_user;
  v_seed constant name := 'bulls96_cloudsql_role_seed';
  v_role name;
  v_row record;
  v_problems text[] := '{}';
  v_missing boolean := false;
BEGIN
  SELECT * INTO v_row FROM pg_roles WHERE rolname = v_owner;
  IF v_row.rolsuper OR NOT v_row.rolcreaterole THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'Cloud SQL role bootstrap requires a non-superuser CREATEROLE migration owner';
  END IF;
  IF NOT pg_has_role(v_owner, 'cloudsqlsuperuser', 'MEMBER') THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'Cloud SQL role bootstrap requires cloudsqlsuperuser membership';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_seed) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('Cloud SQL role bootstrap: reserved trampoline role %I already exists', v_seed),
      HINT = 'Nothing was changed. Inspect and remove the stale role as a separate operator action.';
  END IF;

  FOREACH v_role IN ARRAY ARRAY[
    'anon'::name,
    'authenticated'::name,
    'service_role'::name,
    'session_directory_search_worker'::name,
    'session_token_verifier'::name,
    'bulls96_session_ingest'::name,
    'bulls96_session_ingest_seeder'::name
  ] LOOP
    SELECT * INTO v_row FROM pg_roles WHERE rolname = v_role;
    IF NOT FOUND THEN
      v_missing := true;
      CONTINUE;
    END IF;
    IF v_row.rolcanlogin THEN v_problems := array_append(v_problems, format('%I has LOGIN', v_role)::text); END IF;
    IF v_row.rolinherit THEN v_problems := array_append(v_problems, format('%I has INHERIT', v_role)::text); END IF;
    IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, format('%I has BYPASSRLS', v_role)::text); END IF;
    IF v_row.rolsuper THEN v_problems := array_append(v_problems, format('%I has SUPERUSER', v_role)::text); END IF;
    IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, format('%I has CREATEROLE', v_role)::text); END IF;
    IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, format('%I has CREATEDB', v_role)::text); END IF;
    IF v_row.rolreplication THEN v_problems := array_append(v_problems, format('%I has REPLICATION', v_role)::text); END IF;
    IF EXISTS (
      SELECT 1 FROM pg_auth_members
       WHERE roleid = v_row.oid OR member = v_row.oid
    ) THEN
      v_problems := array_append(v_problems, format('%I has a role membership', v_role)::text);
    END IF;
    IF v_role = 'session_directory_search_worker' AND (
      (SELECT count(*) FROM pg_db_role_setting WHERE setrole = v_row.oid) <> 1 OR
      NOT EXISTS (
        SELECT 1 FROM pg_db_role_setting
         WHERE setrole = v_row.oid AND setdatabase = 0
           AND setconfig = ARRAY['statement_timeout=2s']::text[]
      )
    ) THEN
      v_problems := array_append(v_problems, 'session_directory_search_worker has unexpected role settings'::text);
    END IF;
    IF v_role <> 'session_directory_search_worker' AND EXISTS (
      SELECT 1 FROM pg_db_role_setting WHERE setrole = v_row.oid
    ) THEN
      v_problems := array_append(v_problems, format('%I has role settings', v_role)::text);
    END IF;
  END LOOP;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL role bootstrap refused (nothing changed): ' || array_to_string(v_problems, '; ');
  END IF;

  IF v_missing THEN
    CREATE ROLE bulls96_cloudsql_role_seed
      NOLOGIN NOINHERIT CREATEROLE NOBYPASSRLS NOSUPERUSER NOCREATEDB NOREPLICATION;
    -- The automatic membership has SET FALSE. An ADMIN member may add SET TRUE; this
    -- does not try to modify or revoke the bootstrap-superuser grant.
    EXECUTE format(
      'GRANT bulls96_cloudsql_role_seed TO %I WITH INHERIT FALSE, SET TRUE',
      v_owner
    );
    SET LOCAL ROLE bulls96_cloudsql_role_seed;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
      CREATE ROLE anon NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
      CREATE ROLE authenticated NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
      CREATE ROLE service_role NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_directory_search_worker') THEN
      CREATE ROLE session_directory_search_worker
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
      ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_token_verifier') THEN
      CREATE ROLE session_token_verifier
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest') THEN
      CREATE ROLE bulls96_session_ingest
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest_seeder') THEN
      CREATE ROLE bulls96_session_ingest_seeder
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    RESET ROLE;
    DROP ROLE bulls96_cloudsql_role_seed;
  END IF;

  -- Recheck the security property that the trampoline exists to establish.
  IF EXISTS (
    SELECT 1
      FROM pg_auth_members membership
      JOIN pg_roles target ON target.oid IN (membership.roleid, membership.member)
     WHERE target.rolname IN (
       'anon', 'authenticated', 'service_role', 'session_directory_search_worker',
       'session_token_verifier',
       'bulls96_session_ingest', 'bulls96_session_ingest_seeder'
     )
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL role bootstrap left a target role membership';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_seed) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL role bootstrap left its trampoline role behind';
  END IF;
END
$cloudsql_migration_roles$;
