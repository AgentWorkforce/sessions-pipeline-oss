-- 96bulls 0002: the session uuid at the customer level, and Bob Costas output.

-- One row per indexed session with the client-minted session uuid. Kept beside
-- session_index (same primary key) so the vendored upsert function is unchanged.
CREATE TABLE IF NOT EXISTS sessions.session_index_ext (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  machine_id text NOT NULL,
  -- UUIDv7 minted once by Concourse/AlleyOop when the session first appears; immutable.
  session_uuid uuid NOT NULL,
  session_uuid_source text NOT NULL DEFAULT 'minted'
    CHECK (session_uuid_source IN ('minted', 'reused', 'derived')),
  tenant_id text NOT NULL,
  user_id text,
  first_trace_id uuid NOT NULL,
  last_trace_id uuid NOT NULL,
  last_revision bigint NOT NULL DEFAULT 1,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, harness, session_id, machine_id),
  UNIQUE (session_uuid)
);
ALTER TABLE sessions.session_index_ext ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_index_ext FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_index_ext TO service_role;

-- Bob Costas: short and long summaries per session, idempotent by content hash.
-- Summary text is session data (scrubbed at storage), stored here and never emitted as telemetry.
CREATE TABLE IF NOT EXISTS sessions.session_summaries (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  machine_id text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('short', 'long')),
  session_uuid uuid,
  trace_id uuid NOT NULL,
  revision bigint NOT NULL,
  -- The raw.content_sha256 the summary was built from; a repeat is a no-op.
  content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
  summary text NOT NULL,
  model_id text NOT NULL DEFAULT 'extractive/v1',
  tokens_in integer NOT NULL DEFAULT 0,
  tokens_out integer NOT NULL DEFAULT 0,
  generated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, harness, session_id, machine_id, kind)
);
ALTER TABLE sessions.session_summaries ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_summaries FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_summaries TO service_role;

-- Referee: every verdict, not just the latest state on session_index.
CREATE TABLE IF NOT EXISTS sessions.session_verdicts (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  machine_id text NOT NULL,
  session_uuid uuid,
  trace_id uuid NOT NULL,
  revision bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('active', 'done', 'abandoned', 'unknown')),
  rule text NOT NULL,
  model_id text NOT NULL DEFAULT 'rules/v1',
  classified_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, workspace_id, harness, session_id, machine_id, trace_id, revision)
);
ALTER TABLE sessions.session_verdicts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_verdicts FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_verdicts TO service_role;
