-- Single-use ledger for RelayAuth migration assertions (relayauth #110, with
-- the signed `meta.upload` identity of relayauth #111;
-- `POST /v1/tokens/migration-assertion`; O2 of the Neon-to-S3 export).
--
-- RelayAuth records neither `jti` nor `meta.nonce`, so the ingest makes each
-- assertion single-use: the manifest commit (`POST /v1/pipeline/manifests`)
-- inserts one row here, insert-if-absent on both keys, in the same transaction
-- as its `sessions.pipeline_jobs` row. A conflict on either key is a replay
-- (409 `migration_assertion_replayed`), and a failed enqueue rolls the
-- consumption back with it. See relayhistory-cloud
-- `packages/relayhistory/src/lib/pipeline/migration-assertions.ts`.
--
-- Ids and a plan digest only: no token, no signature, no session content.
-- Service-role only: RLS on with no policy, and no grant to tenants. Code only:
-- applying it to a hosted project needs Khaliq's go.
--
-- Plan binding: `plan_entry_digest` is the signed `meta.upload.entry_sha256`
-- (the canonical plan entry digest, 96BullSessions
-- docs/contracts/relayhistory-export-plan-entry-v1.md), which the ingest
-- recomputed from the upload itself before inserting.
--
-- Chunk uses: `migration_assertion_chunks` holds at most one chunk per
-- (jti, seq) and at most 256 per jti. The ingest admits only a chunk that is
-- one of the signed entry's chunks (so seq < the entry's chunk count), checked
-- before anything is stored.
--
-- Replay retention (no cron, nothing scheduled): every ledger insert first
-- deletes at most 100 rows (per table) whose `expires_at` passed more than 5
-- minutes ago. `expires_at` is the token's exp plus the ingest's 5 second
-- migration skew. The verifier refuses an expired token (401
-- `migration_assertion_expired`) before it touches the ledger, so a purged
-- token is never accepted again. Nonce uniqueness holds only within that
-- window: after a purge, a NEW valid token may reuse a nonce.
--
-- Durable audit: `migration_ingest_audit` gets one append-only row per
-- committed upload, in the same transaction as the ledger row and the
-- pipeline job. Ids, digests and the work key only. Retention policy: keep
-- for the export's lifetime plus 400 days; removal only by a reviewed
-- migration run by the owner role (the trigger refuses UPDATE and DELETE).

CREATE TABLE sessions.migration_assertion_uses (
  jti text PRIMARY KEY CHECK (length(jti) BETWEEN 1 AND 128),
  nonce text NOT NULL UNIQUE CHECK (nonce ~ '^[A-Za-z0-9_-]{16,128}$'),
  grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  plan_entry_digest text NOT NULL CHECK (plan_entry_digest ~ '^[0-9a-f]{64}$'),
  -- The token's exp plus skew: the earliest moment the row may be purged.
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz NOT NULL DEFAULT now()
);
-- Audit: every upload a grant committed, in order.
CREATE INDEX migration_assertion_uses_grant_idx
  ON sessions.migration_assertion_uses (grant_id, consumed_at);

ALTER TABLE sessions.migration_assertion_uses ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_assertion_uses FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.migration_assertion_uses TO service_role;

-- Retention scans.
CREATE INDEX migration_assertion_uses_expires_idx
  ON sessions.migration_assertion_uses (expires_at);

-- One chunk per (assertion, seq) while the assertion is live.
CREATE TABLE sessions.migration_assertion_chunks (
  jti text NOT NULL CHECK (length(jti) BETWEEN 1 AND 128),
  seq bigint NOT NULL CHECK (seq >= 0),
  start_offset bigint NOT NULL CHECK (start_offset >= 0),
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  expires_at timestamptz NOT NULL,
  reserved_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (jti, seq)
);
CREATE INDEX migration_assertion_chunks_expires_idx
  ON sessions.migration_assertion_chunks (expires_at);

ALTER TABLE sessions.migration_assertion_chunks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_assertion_chunks FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.migration_assertion_chunks TO service_role;

-- Durable, append-only audit of committed migration uploads.
CREATE TABLE sessions.migration_ingest_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  at timestamptz NOT NULL DEFAULT now(),
  grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  entry_sha256 text NOT NULL CHECK (entry_sha256 ~ '^[0-9a-f]{64}$'),
  work_key text NOT NULL CHECK (length(work_key) BETWEEN 1 AND 2048),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  result text NOT NULL CHECK (result IN ('committed'))
);
CREATE INDEX migration_ingest_audit_grant_idx
  ON sessions.migration_ingest_audit (grant_id, at);

CREATE FUNCTION sessions.migration_ingest_audit_append_only()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  RAISE EXCEPTION 'migration_ingest_audit is append-only';
END
$$;
CREATE TRIGGER migration_ingest_audit_append_only
  BEFORE UPDATE OR DELETE ON sessions.migration_ingest_audit
  FOR EACH ROW EXECUTE FUNCTION sessions.migration_ingest_audit_append_only();

ALTER TABLE sessions.migration_ingest_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_ingest_audit FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT ON sessions.migration_ingest_audit TO service_role;
