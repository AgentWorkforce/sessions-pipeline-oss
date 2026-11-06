-- Plan entry v2's pre-mint base read (96BullSessions
-- docs/contracts/relayhistory-export-plan-entry-v2-issuer.md sections 3, 4, 8):
-- the base-read capability is its own token class with its own single-use
-- replay namespace, and each read is audited like a commit. Code only: applying
-- it to a hosted project needs Khaliq's go.

-- Replay rows of base-read capabilities (jti `mbr_...`), apart from upload
-- assertions' `migration_assertion_uses`. Same shape, same delete-on-insert
-- retention (a bounded batch past `expires_at` plus 5 minutes).
CREATE TABLE sessions.migration_base_read_uses (
  jti text PRIMARY KEY CHECK (jti ~ '^mbr_' AND length(jti) BETWEEN 5 AND 128),
  nonce text NOT NULL UNIQUE CHECK (nonce ~ '^[A-Za-z0-9_-]{16,128}$'),
  grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  plan_entry_digest text NOT NULL CHECK (plan_entry_digest ~ '^[0-9a-f]{64}$'),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX migration_base_read_uses_expires_idx
  ON sessions.migration_base_read_uses (expires_at);

ALTER TABLE sessions.migration_base_read_uses ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_base_read_uses FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.migration_base_read_uses TO service_role;

-- The audit's result vocabulary gains 'base_read' (append-only as before).
ALTER TABLE sessions.migration_ingest_audit
  DROP CONSTRAINT migration_ingest_audit_result_check;
ALTER TABLE sessions.migration_ingest_audit
  ADD CONSTRAINT migration_ingest_audit_result_check
  CHECK (result IN ('committed', 'base_read'));
