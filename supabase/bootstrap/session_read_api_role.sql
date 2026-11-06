-- The session read API's Supabase role (src/session-read-api/liveness.ts).
--
-- The read API serves transcript reads from ClickHouse. It asks Supabase
-- whether a session is erased (every pointer row tombstoned) and when it began
-- (the history floor), and, for session timing, where the session's canonical
-- S3 objects are (session-timing-source.ts SESSION_POINTERS_SQL, the query of
-- sessions.session_directory_pointers). So this role may read the id/time and
-- object-pointer columns of the pointer table and nothing else: no content, no
-- titles, no metadata, no other table, no function, no writes.
--
--   role      LOGIN NOINHERIT NOBYPASSRLS; no SUPERUSER, CREATEDB, CREATEROLE or
--             REPLICATION; read-only transactions; statement_timeout 2s.
--   grants    USAGE on schema sessions; SELECT on sessions.session_index of
--             (org_id, workspace_id, harness, session_id, created_at, deleted_at)
--             and the pointer columns (canonical_session_id, machine_id,
--             canonical_machine_id, user_id, storage_provider, bucket,
--             manifest_key, content_sha256, raw_format, revision).
--             Narrowing back is a REVOKE of the pointer columns; re-running an
--             older copy of this file does not remove them.
--   RLS       session_index already has RLS on. The policy below admits only rows
--             of the org and workspace that the current transaction bound with
--             set_config('bulls96_read.org_id' / 'bulls96_read.workspace_id', ..., true)
--             (TENANT_SETTINGS_SQL). A setting never set on the connection is NULL;
--             after a transaction-local set_config ends it reads as '' on that pooled
--             connection. Neither matches a row: org_id and workspace_id are never NULL or ''.
--
-- Idempotent. Run as `postgres` (Supabase) after the HoopDb migrations; set the
-- password separately (never in this file), then verify:
--
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_role.sql
--   psql -X -d <the HoopDb database> -c '\password bulls96_read_api'
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_role_verify.sql
\set ON_ERROR_STOP 1
BEGIN;
DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    CREATE ROLE bulls96_read_api LOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  END IF;
END
$role$;
-- Supabase's postgres is not a superuser, and PostgreSQL 16+ lets only a superuser name SUPERUSER or
-- REPLICATION in ALTER ROLE, even as NOSUPERUSER/NOREPLICATION (and BYPASSRLS needs BYPASSRLS). CREATE ROLE above
-- sets the safe attributes; here only LOGIN NOINHERIT are re-asserted, and session_read_api_role_verify.sql fails
-- if an existing role has SUPERUSER, CREATEDB, CREATEROLE, REPLICATION, BYPASSRLS or INHERIT.
ALTER ROLE bulls96_read_api LOGIN NOINHERIT;
ALTER ROLE bulls96_read_api SET statement_timeout = '2s';
ALTER ROLE bulls96_read_api SET default_transaction_read_only = on;

GRANT USAGE ON SCHEMA sessions TO bulls96_read_api;
GRANT SELECT (org_id, workspace_id, harness, session_id, created_at, deleted_at,
              canonical_session_id, machine_id, canonical_machine_id, user_id,
              storage_provider, bucket, manifest_key, content_sha256, raw_format, revision)
  ON sessions.session_index TO bulls96_read_api;

DROP POLICY IF EXISTS session_index_read_api ON sessions.session_index;
CREATE POLICY session_index_read_api ON sessions.session_index
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (
    org_id = current_setting('bulls96_read.org_id', true)
    AND workspace_id = current_setting('bulls96_read.workspace_id', true)
  );
COMMIT;
