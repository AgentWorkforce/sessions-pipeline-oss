-- The session ingest's Supabase login (src/session-ingest/ledger-sql.ts).
--
-- The ingest (POST /v1/delivery/batches, the session ingest Worker) keeps its idempotency, revision
-- and deletion-fence ledger in Supabase. It reaches that ledger only through two
-- SECURITY DEFINER functions (plus the slot pair), so this role may call those and nothing else: no
-- table, no other function, no writes of its own.
--
--   role      created NOLOGIN by supabase/bootstrap/session_ingest_roles.sql BEFORE migrate
--             (the migration never creates a role; it verifies it and grants USAGE on
--             sessions and EXECUTE on the ingest functions, so the function ACL manifest
--             is exact after every migrate). This file, after migrate, makes it LOGIN
--             NOINHERIT with a 5s statement timeout and a 2s lock timeout.
--   guard     every ingest function refuses any caller whose role is not bulls96_session_ingest,
--             so a later blanket grant to another role cannot reach them.
--
-- Idempotent. Run as `postgres` (Supabase) after the HoopDb migrations; set the
-- password separately (never in this file), then verify:
--
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_ingest_role.sql
--   psql -X -d <the HoopDb database> -c '\password bulls96_session_ingest'
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_ingest_role_verify.sql
\set ON_ERROR_STOP 1
BEGIN;
DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'role bulls96_session_ingest does not exist: run supabase/bootstrap/session_ingest_roles.sql, then migrate, first';
  END IF;
END
$role$;
-- Supabase's postgres is not a superuser, and PostgreSQL 16+ lets only a superuser name SUPERUSER or
-- REPLICATION in ALTER ROLE, even as NOSUPERUSER/NOREPLICATION (and BYPASSRLS needs BYPASSRLS). The migration
-- (session_ingest_roles.sql) created the role with the safe attributes; here only LOGIN NOINHERIT are asserted, and
-- session_ingest_role_verify.sql fails if the role has SUPERUSER, CREATEDB, CREATEROLE, REPLICATION,
-- BYPASSRLS or INHERIT.
ALTER ROLE bulls96_session_ingest LOGIN NOINHERIT;
ALTER ROLE bulls96_session_ingest SET statement_timeout = '5s';
ALTER ROLE bulls96_session_ingest SET lock_timeout = '2s';
ALTER ROLE bulls96_session_ingest SET idle_in_transaction_session_timeout = '10s';
COMMIT;
