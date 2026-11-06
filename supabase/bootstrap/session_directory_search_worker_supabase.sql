-- Supabase-only transactional entry point for the Worker role bootstrap.
--
-- PostgreSQL 16+ records the unavoidable custom-role creator edge when Supabase's
-- non-superuser `postgres` creates the role. The included bootstrap accepts that edge
-- only when this exact mode is bound and the catalogs prove the frozen Supabase shape.
-- Any failure rolls the CREATE ROLE and its setting back together.
\set ON_ERROR_STOP 1
BEGIN;
SELECT set_config('hoopdb.db_mode', 'supabase', true);
\ir session_directory_search_worker.sql
COMMIT;
