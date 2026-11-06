-- Down path for supabase/migrations/20261101050000_session_directory_list.sql.
-- Additive up, so the down only drops what it added. Exercised by
-- src/hoopdb/__tests__/session-directory-list.test.ts (up, down, up).
-- Code only: running it against a hosted database needs the user's OK.

BEGIN;
DROP FUNCTION IF EXISTS sessions.session_directory_activity(text, timestamptz, timestamptz);
DROP FUNCTION IF EXISTS sessions.list_session_directory(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer);
DROP FUNCTION IF EXISTS sessions.session_directory_list_sql(
  integer, boolean, integer, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean);
DROP INDEX IF EXISTS sessions.session_directory_tenant_project_text_trgm_idx;
DROP FUNCTION IF EXISTS sessions.session_directory_project_text(text, text, text);
DROP TRIGGER IF EXISTS session_directory_forget_catalog_on_delete ON sessions.session_directory;
DROP TRIGGER IF EXISTS session_directory_forget_catalog_on_tombstone ON sessions.session_directory;
DROP FUNCTION IF EXISTS sessions.session_directory_forget_catalog();
DROP TRIGGER IF EXISTS session_directory_companions_follow ON sessions.session_directory;
DROP FUNCTION IF EXISTS sessions.session_directory_companions_follow();
DROP TABLE IF EXISTS sessions.session_directory_catalog_rollout;
DROP TRIGGER IF EXISTS session_directory_tombstone_clears_catalog ON sessions.session_directory;
DROP FUNCTION IF EXISTS sessions.session_directory_tombstone_clears_catalog();
DROP TABLE IF EXISTS sessions.session_directory_relationships;
DROP TABLE IF EXISTS sessions.session_directory_agents;
ALTER TABLE sessions.session_directory
  DROP COLUMN IF EXISTS catalog_projected_at,
  DROP COLUMN IF EXISTS agent_version,
  DROP COLUMN IF EXISTS originator,
  DROP COLUMN IF EXISTS initial_commit,
  DROP COLUMN IF EXISTS project_key_method,
  DROP COLUMN IF EXISTS project_key,
  DROP COLUMN IF EXISTS catalog_models,
  DROP COLUMN IF EXISTS agent_names;
COMMIT;
