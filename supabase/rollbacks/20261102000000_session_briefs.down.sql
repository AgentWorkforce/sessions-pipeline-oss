-- Down path for supabase/migrations/20261102000000_session_briefs.sql.
-- Additive up, so the down only drops what it added. Briefs are derived data: the brief
-- worker rebuilds them from the scrubbed raw after an up. Revert the code first: the
-- directory projector reads session_briefs for its title precedence. Brief titles and
-- descriptions already projected into session_directory stay until the next projection
-- (title) or summary write (short_summary) replaces them. Exercised by
-- src/session-brief/__tests__/migration.test.ts (up, down, up).
-- Code only: running it against a hosted database needs the user's OK.

BEGIN;
DROP FUNCTION IF EXISTS sessions.session_brief(text, text, text);
DROP TRIGGER IF EXISTS session_briefs_forget_deleted_directory ON sessions.session_directory;
DROP TRIGGER IF EXISTS session_briefs_forget_tombstoned_directory ON sessions.session_directory;
DROP TRIGGER IF EXISTS session_briefs_forget_deleted_pointer ON sessions.session_index;
DROP TRIGGER IF EXISTS session_briefs_forget_tombstoned_pointer ON sessions.session_index;
DROP FUNCTION IF EXISTS sessions.session_briefs_forget();
DROP TABLE IF EXISTS sessions.session_brief_attempts;
DROP TABLE IF EXISTS sessions.session_brief_wakeups;
DROP TABLE IF EXISTS sessions.session_briefs;
COMMIT;
