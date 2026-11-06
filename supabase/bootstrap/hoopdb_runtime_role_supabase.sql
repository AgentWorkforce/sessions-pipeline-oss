-- Supabase-only transactional entry point for the dedicated HoopDb runtime role.
--
-- Run as the hosted non-superuser CREATEROLE migration owner `postgres`. PostgreSQL
-- 16+ records an unavoidable ADMIN/no-INHERIT/no-SET creator edge granted by the
-- platform superuser `supabase_admin`; the included bootstrap accepts only that exact
-- catalog shape. Any failure rolls back role creation and CONNECT together. Unlike the
-- selfhosted path, this wrapper deliberately preserves Supabase's PUBLIC TEMPORARY
-- baseline; the migrator validates its exact provenance before writing.
--
--   psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime \
--     -v runtime_login=true [-v workload_role=the_login_role] \
--     -d <the HoopDb database> \
--     -f supabase/bootstrap/hoopdb_runtime_role_supabase.sql
\set ON_ERROR_STOP 1
BEGIN;
SELECT set_config('hoopdb.db_mode', 'supabase', true);
\ir hoopdb_runtime_role.sql
COMMIT;
