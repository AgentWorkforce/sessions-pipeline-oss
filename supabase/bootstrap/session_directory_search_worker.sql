-- Phase 1: privileged bootstrap for the session directory search Worker role.
--
-- Two phases, run by every deploy:
--   phase 1 (this file)  before migrate: provision the bare role, or verify that it
--                        matches the role spec. It does not look at grants: before
--                        20261030020000 there are none, and afterwards they belong to
--                        phase 2.
--   phase 2              after all migrations:
--                        supabase/bootstrap/session_directory_search_worker_verify.sql
--                        checks the role spec plus the exact direct-grant allowlist.
--
-- Interface (stable; the deploy-time provisioning in self-hosted-lead's #75 runs this):
--   role      session_directory_search_worker
--   attrs     NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
--   settings  exactly one, role-wide: statement_timeout=2s (pg_db_role_setting,
--             setdatabase = 0, setconfig = {statement_timeout=2s})
--   members   member of none. The authenticator binding is separate. Supabase PG16+
--             additionally has the exact postgres creator edge documented below.
--   run as    a superuser: CNPG `postgres` (the superuser secret, or
--             postInitApplicationSQL for a new cluster); a non-superuser CREATEROLE
--             role on PostgreSQL 15 and older; or the exact Supabase PostgreSQL 16+
--             compatibility path in session_directory_search_worker_supabase.sql.
--             That path accepts only postgres's unavoidable ADMIN/no-INHERIT/no-SET
--             creator edge when its grantor is the frozen superuser supabase_admin.
--   when      before every migrate. It must run before the migration run that first
--             applies supabase/migrations/20261030020000_session_directory_short_summary.sql
--             (the migration fails closed, 42704, if the role is absent).
--   exit      success: the role exists exactly as specified (created now or already).
--             failure: SQLSTATE 42501 (caller may not create it) or 55000 (an existing
--             role differs; nothing is changed). It never alters an existing role.
--
-- Idempotent and atomic: one DO block. Run it with
--   psql -v ON_ERROR_STOP=1 -X -f supabase/bootstrap/session_directory_search_worker.sql
-- connected to the HoopDb / Supabase database. Roles are cluster-wide, so once per
-- cluster is enough, but running it in each database is harmless.
--
-- Runbook: runbooks/session-directory-search-worker-bootstrap.md. Not a migration:
-- the migration runner applies supabase/migrations only (local plain Postgres runs this
-- as its `bootstrap:` step; see src/hoopdb/migrate.ts).
DO $bootstrap$
DECLARE
  v_oid oid;
  v_me record;
  v_problems text[] := '{}';
  v_row record;
  v_settings text;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT oid INTO v_oid FROM pg_roles WHERE rolname = 'session_directory_search_worker';

  IF v_oid IS NULL THEN
    SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
    IF NOT v_me.rolsuper
       AND NOT (v_me.rolcreaterole AND v_version < 160000)
       AND NOT (v_mode = 'supabase' AND v_version >= 160000
                AND current_user = 'postgres' AND v_me.rolcreaterole
                AND EXISTS (SELECT 1 FROM pg_roles
                             WHERE rolname = 'supabase_admin' AND rolsuper)) THEN
      RAISE EXCEPTION USING ERRCODE = '42501',
        MESSAGE = format('bootstrap session_directory_search_worker: %I may not create it', current_user),
        HINT = 'Run as a superuser, or use the reviewed Supabase wrapper. The Supabase path is catalog-bound to postgres and supabase_admin.';
    END IF;
    CREATE ROLE session_directory_search_worker
      NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
    SELECT oid INTO v_oid FROM pg_roles WHERE rolname = 'session_directory_search_worker';
  END IF;

  -- Verify, whether created just now or already present. Any failure rolls back the
  -- whole block, including a role created above.
  SELECT * INTO v_row FROM pg_roles WHERE oid = v_oid;
  IF v_row.rolcanlogin THEN v_problems := array_append(v_problems, 'LOGIN'::text); END IF;
  IF v_row.rolinherit THEN v_problems := array_append(v_problems, 'INHERIT'::text); END IF;
  IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF v_row.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF v_row.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;

  IF (SELECT count(*) FROM pg_db_role_setting WHERE setrole = v_oid) <> 1
     OR NOT EXISTS (SELECT 1 FROM pg_db_role_setting
                     WHERE setrole = v_oid AND setdatabase = 0
                       AND setconfig = ARRAY['statement_timeout=2s']::text[]) THEN
    SELECT coalesce(string_agg(
             CASE WHEN s.setdatabase = 0 THEN '' ELSE format('database %s: ', s.setdatabase) END
               || array_to_string(s.setconfig, ', '), '; '), 'none')
      INTO v_settings
      FROM pg_db_role_setting AS s WHERE s.setrole = v_oid;
    v_problems := array_append(v_problems,
      format('role settings must be exactly statement_timeout=2s (found: %s)', v_settings)::text);
  END IF;

  FOR v_row IN SELECT r.rolname FROM pg_auth_members AS m JOIN pg_roles AS r ON r.oid = m.roleid
                WHERE m.member = v_oid LOOP
    v_problems := array_append(v_problems, format('member of %I', v_row.rolname)::text);
  END LOOP;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT r.rolname, r.rolinherit, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS r ON r.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1 ORDER BY r.rolname, g.rolname' USING v_oid LOOP
      IF v_mode = 'supabase'
         AND v_row.rolname = 'postgres'
         AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- unavoidable PG16+ Supabase custom-role creator edge
      ELSIF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
    FOR v_row IN EXECUTE
      'SELECT r.rolname FROM pg_auth_members AS m JOIN pg_roles AS r ON r.oid = m.member
        WHERE m.roleid = $1 AND m.inherit_option' USING v_oid LOOP
      v_problems := array_append(v_problems, format('granted WITH INHERIT to %I', v_row.rolname)::text);
    END LOOP;
  ELSE
    FOR v_row IN SELECT r.rolname, r.rolinherit, m.admin_option FROM pg_auth_members AS m
                   JOIN pg_roles AS r ON r.oid = m.member
                  WHERE m.roleid = v_oid LOOP
      IF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'bootstrap session_directory_search_worker: the role does not match the spec: '
                || array_to_string(v_problems, ', '),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
END
$bootstrap$;
