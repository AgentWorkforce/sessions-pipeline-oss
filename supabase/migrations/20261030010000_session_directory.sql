-- Logical-session metadata directory for the S3-content architecture (decision 0002).
--
-- sessions.session_index remains the physical manifest/pointer table: one row per
-- machine manifest. This directory is one row per logical session so keyset pages
-- cannot repeat a session that exists on multiple machines. A later projector owns
-- population and rollout; this migration deliberately creates no trigger, backfill,
-- or write path.

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE TABLE sessions.session_directory (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL CHECK (harness ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255),

  created_at timestamptz NOT NULL,
  last_activity_at timestamptz NOT NULL,
  title text CHECK (char_length(title) <= 200),
  task_title text CHECK (char_length(task_title) <= 200),
  agent_name text CHECK (char_length(agent_name) <= 200),
  -- Written by the projector as lower(btrim(agent_name)). Keeping this materialized
  -- makes exact and prefix queries use one stable text_pattern_ops index.
  agent_name_normalized text CHECK (
    agent_name_normalized IS NULL OR
    (agent_name IS NOT NULL AND agent_name_normalized = lower(btrim(agent_name)))
  ),
  project_id text,
  git_repo text,
  git_branch text,
  model text,
  state text NOT NULL DEFAULT 'unknown'
    CHECK (state IN ('active', 'done', 'abandoned', 'unknown')),

  event_count bigint NOT NULL DEFAULT 0 CHECK (event_count >= 0),
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  cost_usd_micros bigint CHECK (cost_usd_micros >= 0),

  -- Convenience output only. Indexed membership filtering uses the normalized
  -- companion tables below, avoiding GIN-plus-sort plans for recent-session pages.
  machine_ids text[] NOT NULL DEFAULT '{}',
  user_ids text[] NOT NULL DEFAULT '{}',

  -- Metadata only: title/task title, agent, project, repository, branch and model.
  -- Transcript/event content is canonical in S3 and MUST NOT be copied here.
  search_text text NOT NULL DEFAULT '' CHECK (char_length(search_text) <= 8192),
  projection_version smallint NOT NULL DEFAULT 1 CHECK (projection_version >= 1),
  projected_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,

  CONSTRAINT session_directory_pkey
    PRIMARY KEY (org_id, workspace_id, harness, session_id)
);

COMMENT ON TABLE sessions.session_directory IS
  'Rebuildable logical-session metadata directory. Canonical session content remains in object storage.';
COMMENT ON COLUMN sessions.session_directory.search_text IS
  'Metadata-only search document; MUST NOT contain transcript or event content.';

CREATE TABLE sessions.session_directory_users (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  user_id text NOT NULL CHECK (char_length(user_id) BETWEEN 1 AND 255),
  last_activity_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, harness, session_id, user_id),
  FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.session_directory (org_id, workspace_id, harness, session_id)
    ON DELETE CASCADE
);

CREATE TABLE sessions.session_directory_machines (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  machine_id text NOT NULL CHECK (char_length(machine_id) BETWEEN 1 AND 255),
  last_activity_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, harness, session_id, machine_id),
  FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.session_directory (org_id, workspace_id, harness, session_id)
    ON DELETE CASCADE
);

-- Every page is ordered by a total logical-session key. Tombstones remain directly
-- addressable but never occur in lists.
CREATE INDEX session_directory_workspace_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, last_activity_at DESC, session_id DESC, harness DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX session_directory_org_recent_idx
  ON sessions.session_directory
  (org_id, last_activity_at DESC, session_id DESC, harness DESC, workspace_id DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX session_directory_harness_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, harness, last_activity_at DESC, session_id DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX session_directory_project_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, project_id, last_activity_at DESC, session_id DESC, harness DESC)
  WHERE project_id IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX session_directory_no_project_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, last_activity_at DESC, session_id DESC, harness DESC)
  WHERE project_id IS NULL AND deleted_at IS NULL;

CREATE INDEX session_directory_agent_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, agent_name_normalized text_pattern_ops,
   last_activity_at DESC, session_id DESC, harness DESC)
  WHERE agent_name_normalized IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX session_directory_created_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, created_at DESC, session_id DESC, harness DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX session_directory_state_recent_idx
  ON sessions.session_directory
  (org_id, workspace_id, state, last_activity_at DESC, session_id DESC, harness DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX session_directory_session_lookup_idx
  ON sessions.session_directory (org_id, session_id);

CREATE INDEX session_directory_project_trgm_idx
  ON sessions.session_directory USING gin (project_id public.gin_trgm_ops)
  WHERE project_id IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX session_directory_metadata_trgm_idx
  ON sessions.session_directory USING gin (search_text public.gin_trgm_ops)
  WHERE search_text <> '' AND deleted_at IS NULL;

CREATE INDEX session_directory_users_recent_idx
  ON sessions.session_directory_users
  (org_id, workspace_id, user_id, last_activity_at DESC, session_id DESC, harness DESC);

CREATE INDEX session_directory_machines_recent_idx
  ON sessions.session_directory_machines
  (org_id, workspace_id, machine_id, last_activity_at DESC, session_id DESC, harness DESC);

-- The index audit remains fail-closed until the later projector/backfill lane marks
-- this singleton complete. Creating this row is not permission to populate it.
CREATE TABLE sessions.session_directory_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.session_directory_rollout (singleton) VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

ALTER TABLE sessions.session_directory ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_directory_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_directory_machines ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_directory_rollout ENABLE ROW LEVEL SECURITY;

CREATE POLICY session_directory_member_read ON sessions.session_directory
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id())
         AND sessions.member_of(workspace_id, (SELECT sessions.jwt_workspace_ids())));

CREATE POLICY session_directory_users_member_read ON sessions.session_directory_users
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id())
         AND sessions.member_of(workspace_id, (SELECT sessions.jwt_workspace_ids())));

CREATE POLICY session_directory_machines_member_read ON sessions.session_directory_machines
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id())
         AND sessions.member_of(workspace_id, (SELECT sessions.jwt_workspace_ids())));

REVOKE ALL ON sessions.session_directory, sessions.session_directory_users,
  sessions.session_directory_machines, sessions.session_directory_rollout
  FROM PUBLIC, anon, authenticated;
GRANT SELECT ON sessions.session_directory, sessions.session_directory_users,
  sessions.session_directory_machines TO authenticated;
GRANT ALL ON sessions.session_directory, sessions.session_directory_users,
  sessions.session_directory_machines, sessions.session_directory_rollout TO service_role;
