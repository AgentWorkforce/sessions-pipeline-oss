-- Preserve only verified canonical analytics attribution across opaque session rotation.
-- Existing sessions remain NULL until a new verified login; no inferred backfill.
ALTER TABLE sessions.auth_sessions ADD COLUMN IF NOT EXISTS analytics_identity jsonb;
