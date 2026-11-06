-- Down path for supabase/migrations/20261031001000_session_directory_reads_expand.sql.
-- Additive up, so the down only drops what it added. Exercised by
-- src/hoopdb/__tests__/session-directory-reads-expand.test.ts (up, down, up).
-- Code only: running it against a hosted database needs the user's OK.

BEGIN;
DROP FUNCTION IF EXISTS sessions.session_directory_readiness();
DROP FUNCTION IF EXISTS sessions.session_directory_pointers(text, text, text);
DROP FUNCTION IF EXISTS sessions.session_directory_worker_scope(text);
DROP TRIGGER IF EXISTS session_directory_record_fences ON sessions.session_directory;
DROP FUNCTION IF EXISTS sessions.session_directory_record_fences();
DROP TABLE IF EXISTS sessions.session_directory_resurrection_fences;
COMMIT;
