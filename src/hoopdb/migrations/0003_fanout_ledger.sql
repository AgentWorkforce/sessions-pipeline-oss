-- 96bulls 0003: make the session-indexed stage recoverable on retry.
-- The vendored upsert fences by revision, so a retry after a partial fan-out
-- would see "already applied" and stop. The fan-out keeps its own ledger here.
ALTER TABLE sessions.session_index_ext
  ADD COLUMN IF NOT EXISTS fanout_revision bigint NOT NULL DEFAULT 0;
