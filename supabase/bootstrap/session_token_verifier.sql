-- Phase 1: privileged bootstrap for the auth token verifier role. Before migrate.
--
-- The migration (supabase/migrations/20261104020100_auth_session_verify.sql) never
-- creates or alters a role: a CNPG/Cloud SQL migration owner has no CREATEROLE. It
-- verifies this role (42704 if absent, 55000 if it differs) and grants EXECUTE on
-- sessions.auth_session_verify to it. This file provisions it, the pattern of
-- supabase/bootstrap/session_ingest_roles.sql:
--
--   role      session_token_verifier  the second-service verifier for relayhistory
--                                     bearer tokens (96BullSessions
--                                     supabase/migrations/20261101000100; relayhistory-cloud
--                                     docs/auth-backend.md). Reached only through a
--                                     PostgREST JWT whose role claim names it; it
--                                     never logs in.
--   attrs     NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
--   members   member of none. Members OF the role may only be the separately
--             approved `authenticator` edge (PostgREST's switch-to, granted as a
--             deployment step without ADMIN OPTION or inherited privileges;
--             PG16+ requires INHERIT FALSE and SET TRUE; PG15 requires the
--             authenticator role NOINHERIT) and, on Supabase PG16+, the
--             unavoidable `postgres` creator edge (grantor supabase_admin, ADMIN
--             without INHERIT or SET).
--   run as    a superuser (CNPG `postgres`); a CREATEROLE role on PostgreSQL 15 and
--             older (Cloud SQL's migration_owner); or Supabase's `postgres` on 16+.
--   when      before every migrate (src/hoopdb/migrate.ts runs it as its bootstrap
--             step where the migrator may; on Supabase the operator runs it as
--             `postgres` before the migrate that carries 20261104020100).
--   exit      success: the role exists (created now or already) and is not elevated.
--             failure: 42501 (caller may not create it) or 55000 (an existing role
--             is elevated or a member of another role; nothing is changed). It
--             never alters an existing role.
--
-- Idempotent and atomic (one DO block). Roles are cluster-wide.
DO $bootstrap$
DECLARE
  r pg_roles%ROWTYPE;
  v_row record;
  v_me record;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = 'session_token_verifier';
  IF NOT FOUND THEN
    SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
    IF NOT v_me.rolsuper
       AND NOT (v_me.rolcreaterole AND v_version < 160000)
       AND NOT (v_version >= 160000 AND current_user = 'postgres' AND v_me.rolcreaterole
                AND (v_mode = 'supabase'
                     OR EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin' AND rolsuper))) THEN
      RAISE EXCEPTION USING ERRCODE = '42501',
        MESSAGE = 'bootstrap session_token_verifier: ' || current_user || ' may not create it',
        HINT = 'Run as a superuser, a CREATEROLE role on PostgreSQL 15 or older, or Supabase''s postgres.';
    END IF;
    CREATE ROLE session_token_verifier NOLOGIN NOINHERIT NOBYPASSRLS
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    SELECT * INTO r FROM pg_roles WHERE rolname = 'session_token_verifier';
  END IF;
  IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb
     OR r.rolreplication OR r.rolcanlogin THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'bootstrap session_token_verifier: the role exists elevated (or it can log in)',
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
  -- A member of no role.
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'bootstrap session_token_verifier: the role is a member of another role',
      HINT = 'Nothing was changed. Revoke the membership by hand, then rerun.';
  END IF;
  -- Members OF the verifier: only the separately approved authenticator edge
  -- (never ADMIN or inherited privileges) and the Supabase PG16+ postgres creator edge.
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS u ON u.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1' USING r.oid LOOP
      IF v_row.rolname = 'postgres' AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- Supabase PG16+ creator edge
      ELSIF v_row.rolname = 'authenticator' AND NOT v_row.admin_option
         AND NOT v_row.inherit_option AND v_row.set_option THEN
        NULL; -- separately approved PostgREST switch-to edge
      ELSE
        RAISE EXCEPTION USING ERRCODE = '55000',
          MESSAGE = format('bootstrap session_token_verifier: unexpected member %I', v_row.rolname),
          HINT = 'Nothing was changed. Revoke the member by hand, then rerun.';
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT u.rolname, u.rolinherit, m.admin_option FROM pg_auth_members AS m
                   JOIN pg_roles AS u ON u.oid = m.member WHERE m.roleid = r.oid LOOP
      IF NOT (v_row.rolname = 'authenticator' AND NOT v_row.admin_option AND NOT v_row.rolinherit) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
          MESSAGE = format('bootstrap session_token_verifier: unexpected member %I', v_row.rolname),
          HINT = 'Nothing was changed. Revoke the member by hand, then rerun.';
      END IF;
    END LOOP;
  END IF;
END
$bootstrap$;
