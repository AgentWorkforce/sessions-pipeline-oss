-- Down path for supabase/migrations/20261030020000_session_directory_short_summary.sql.
--
-- Lives outside supabase/migrations/ on purpose: the tests apply every file in that
-- directory in name order, and a down file there would undo the up. Exercised by
-- src/hoopdb/__tests__/session-directory-short-summary.test.ts (up, down, up).
--
-- Rolling back removes every copy of summary text from the directory (search_text
-- is rebuilt without it before the columns are dropped), removes the search RPC and
-- the Worker role's USAGE on `sessions`, and restores #38's index and grants. It does
-- not drop the role: the migration never created it. The role belongs to the
-- privileged bootstrap (supabase/bootstrap/session_directory_search_worker.sql), and
-- removing it is a separate operator step in
-- runbooks/session-directory-search-worker-bootstrap.md. btree_gin stays installed
-- (CREATE EXTENSION IF NOT EXISTS in the up), as 20261030010000 treats pg_trgm.
--
-- Run later downs first, in reverse migration order (20261031001000 builds on this
-- one). After this down the role is back to the bootstrap spec, so the up can rerun.
--
-- Code only: running it against a hosted database needs the user's OK.

BEGIN;

REVOKE SELECT ON sessions.session_directory FROM authenticated;
GRANT SELECT ON sessions.session_directory TO authenticated;

DROP FUNCTION IF EXISTS sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer);
DROP TABLE IF EXISTS sessions.session_directory_search_token_policy;
DROP FUNCTION IF EXISTS sessions.session_directory_search_query(text);
DO $role$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_directory_search_worker') THEN
    REVOKE ALL ON SCHEMA sessions FROM session_directory_search_worker;
  END IF;
END
$role$;

DROP INDEX IF EXISTS sessions.session_directory_tenant_search_trgm_idx;
DROP INDEX IF EXISTS sessions.session_directory_tenant_project_trgm_idx;
DROP TRIGGER IF EXISTS session_directory_tombstone_clears_summary ON sessions.session_directory;
DROP FUNCTION IF EXISTS sessions.session_directory_tombstone_clears_summary();

UPDATE sessions.session_directory
   SET search_text = sessions.session_directory_search_text(
         title, task_title, agent_name, project_id, git_repo, git_branch, model, NULL)
 WHERE short_summary IS NOT NULL;

DROP FUNCTION IF EXISTS sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text);

ALTER TABLE sessions.session_directory
  DROP CONSTRAINT IF EXISTS session_directory_short_summary_not_on_tombstone,
  DROP CONSTRAINT IF EXISTS session_directory_short_summary_provenance,
  DROP COLUMN IF EXISTS short_summary_generated_at,
  DROP COLUMN IF EXISTS short_summary_source,
  DROP COLUMN IF EXISTS short_summary_scrub_version,
  DROP COLUMN IF EXISTS short_summary_scrub_ruleset,
  DROP COLUMN IF EXISTS short_summary;

CREATE INDEX IF NOT EXISTS session_directory_project_trgm_idx
  ON sessions.session_directory USING gin (project_id public.gin_trgm_ops)
  WHERE project_id IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS session_directory_metadata_trgm_idx
  ON sessions.session_directory USING gin (search_text public.gin_trgm_ops)
  WHERE search_text <> '' AND deleted_at IS NULL;

COMMENT ON COLUMN sessions.session_directory.search_text IS
  'Metadata-only search document; MUST NOT contain transcript or event content.';

COMMIT;
