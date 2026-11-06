-- Phase 1: privileged bootstrap for the session ingest roles. Before migrate.
--
-- The migration (supabase/migrations/20261104022000_session_ingest_ledger.sql) never
-- creates or alters a role: a CNPG/Cloud SQL migration owner has no CREATEROLE. It
-- verifies these roles (42704 if absent, 55000 if they differ) and grants EXECUTE on
-- the ingest functions to them. This file provisions them, the pattern of
-- supabase/bootstrap/session_directory_search_worker.sql:
--
--   roles     bulls96_session_ingest         the ingest Worker's ledger login (made
--                                            LOGIN after migrate by session_ingest_role.sql)
--             bulls96_canonical_feature_producer  the 610 producer: read-only on the canonical
--                                            feature journal and the frozen pointer pages
--             bulls96_session_ingest_seeder  the revision seed (granted to an operator
--                                            login for scripts/session-ingest-seed.ts)
--   attrs     NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
--   members   member of none.
--   run as    a superuser (CNPG `postgres`); a CREATEROLE role on PostgreSQL 15 and
--             older (Cloud SQL's migration_owner); or Supabase's `postgres` on 16+.
--   when      before every migrate (src/hoopdb/migrate.ts runs it as its bootstrap step
--             where the migrator may; on Supabase the runbook runs it as `postgres`).
--   exit      success: both roles exist (created now or already) and are not elevated.
--             failure: 42501 (caller may not create them) or 55000 (an existing role is
--             elevated or a member of a role; nothing is changed). It never alters an
--             existing role, so it is safe after session_ingest_role.sql made one LOGIN.
--
-- Idempotent and atomic (one DO block). Roles are cluster-wide.
DO $bootstrap$
DECLARE
  v_name text;
  v_me record;
  r pg_roles%ROWTYPE;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  FOREACH v_name IN ARRAY ARRAY['bulls96_session_ingest', 'bulls96_session_ingest_seeder', 'bulls96_canonical_feature_producer'] LOOP
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
    IF NOT FOUND THEN
      SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
      IF NOT v_me.rolsuper
         AND NOT (v_me.rolcreaterole AND v_version < 160000)
         AND NOT (v_version >= 160000 AND current_user = 'postgres' AND v_me.rolcreaterole
                  AND (v_mode = 'supabase'
                       OR EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin' AND rolsuper))) THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
          MESSAGE = format('bootstrap %s: %I may not create it', v_name, current_user),
          HINT = 'Run as a superuser, a CREATEROLE role on PostgreSQL 15 or older, or Supabase''s postgres.';
      END IF;
      EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', v_name);
      SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
    END IF;
    IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
       OR (v_name = 'bulls96_session_ingest_seeder' AND r.rolcanlogin) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('bootstrap %s: the role exists elevated (or the seeder can log in)', v_name),
        HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('bootstrap %s: the role is a member of another role', v_name),
        HINT = 'Nothing was changed. Revoke the membership by hand, then rerun.';
    END IF;
  END LOOP;
END
$bootstrap$;
