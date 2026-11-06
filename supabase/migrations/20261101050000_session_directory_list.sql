-- The session LIST and its metadata filters, served from the logical-session directory
-- (neon-retire, Khaliq 2026-11-01: Supabase serves session metadata lists, filters and the
-- directory; ClickHouse serves transcript text). RelayHistory's `GET /v1/sessions` (no
-- `q`) and the dashboard's harness:/project:/person:/mine/updated:/created:/agent filters
-- read sessions.list_session_directory instead of Neon's session_rollups.
--
-- What this adds:
--   1. Catalog dimensions on the logical row that sessions.session_index cannot carry
--      (agent names, catalog models, project key and method, initial commit, originator,
--      agent version, parent sessions, child count). The pointer row still owns title,
--      project, repository, branch, the latest model, counts, tokens and cost; the
--      catalog columns are written by src/hoopdb/session-directory-catalog.ts from the
--      delivered `session` and `relationship` records (the same records relayhistory's
--      project_delivery_catalog_row reads). A brief title reaches `title` through the
--      brief writer's projector precedence (sessions.session_briefs, feat/model-session-brief);
--      this migration does not depend on that table.
--   2. sessions.session_directory_agents: one row per (session, agent), because a session
--      can carry several agents. Ordered by the logical row's last_activity_at, as the
--      user and machine companions are, so an agent filter is an ordered index walk.
--      sessions.session_directory_relationships: the parent -> child edges a parent's
--      `relationship` records deliver. A child learns its parents only from other
--      sessions' records, so parents and the child count are read from the edges, not
--      stored on either row.
--   3. Indexes for the list filters that no existing index serves: the agent companion
--      index (lower(agent) exact) and a tenant trigram index over the project id and key
--      for `projectContains`. Agent and user filters are membership filters over their
--      companions (session_directory_agents_recent_idx, session_directory_users_recent_idx);
--      the page is always ordered and paged on the directory row's own key. A
--      tenant-prefixed GIN over user_ids was rejected: its org_id/workspace_id posting
--      lists cover the whole tenant and the planner never picks it.
--   6. Triggers that keep the companions' order keys in step with the directory row and
--      remove a tombstoned or deleted session's agents and edges (as parent and as child),
--      and sessions.session_directory_catalog_rollout, which gates p_agent until the
--      catalog backfill is complete. The
--      (org, workspace, harness, last_activity_at DESC) index the source filter needs is
--      session_directory_harness_recent_idx (20261030010000), and the created range uses
--      session_directory_created_recent_idx.
--   4. sessions.list_session_directory: SECURITY DEFINER, fixed SQL fragments with bound
--      values, tenant only from the signed claims through
--      sessions.session_directory_worker_scope (20261031001000: the same role guard,
--      issuer, audience, lifetime and workspace pins as sessions.search_session_directory),
--      EXECUTE for session_directory_search_worker alone. The statement text comes from
--      sessions.session_directory_list_sql, owner-only, so tests can EXPLAIN the exact SQL.
--   5. sessions.session_directory_activity: sessions per UTC day and harness (span
--      semantics; see the function), under the same pins and grant.
--
-- Timestamp semantics (half-open, so a dashboard day operator maps without overlap):
--   last_ts_after   inclusive  last_activity_at >= value
--   last_ts_before  exclusive  last_activity_at <  value
--   first_ts_after  inclusive  created_at       >= value
--   first_ts_before exclusive  created_at       <  value
--
-- Backfill: rows projected before this migration have empty catalog columns and no agent
-- rows until their next catalog write. Rebuilding them is an operator step
-- (docs/session-directory-list.md, "Backfill"); this migration writes no data.
--
-- Code only; nothing here is applied by this repository. Down path:
-- supabase/rollbacks/20261101050000_session_directory_list.down.sql.

ALTER TABLE sessions.session_directory
  ADD COLUMN agent_names text[] NOT NULL DEFAULT '{}'
    CONSTRAINT session_directory_agent_names_bound CHECK (cardinality(agent_names) <= 64),
  ADD COLUMN catalog_models text[] NOT NULL DEFAULT '{}'
    CONSTRAINT session_directory_catalog_models_bound CHECK (cardinality(catalog_models) <= 64),
  ADD COLUMN project_key text
    CONSTRAINT session_directory_project_key_length CHECK (char_length(project_key) BETWEEN 1 AND 512),
  ADD COLUMN project_key_method text
    CONSTRAINT session_directory_project_key_method_length CHECK (char_length(project_key_method) BETWEEN 1 AND 64),
  ADD COLUMN initial_commit text
    CONSTRAINT session_directory_initial_commit_length CHECK (char_length(initial_commit) BETWEEN 1 AND 64),
  ADD COLUMN originator text
    CONSTRAINT session_directory_originator_length CHECK (char_length(originator) BETWEEN 1 AND 200),
  ADD COLUMN agent_version text
    CONSTRAINT session_directory_agent_version_length CHECK (char_length(agent_version) BETWEEN 1 AND 200),
  -- Set by the catalog writer. NULL means no catalog was delivered: the list then omits
  -- `catalog`, as relayhistory does when a session has no catalog row.
  ADD COLUMN catalog_projected_at timestamptz;

COMMENT ON COLUMN sessions.session_directory.agent_names IS
  'Agent names from the delivered session catalog, sorted and distinct. Filter through sessions.session_directory_agents.';
COMMENT ON COLUMN sessions.session_directory.catalog_models IS
  'Models the delivered session catalog lists. The list RPC returns them united with the pointer model.';

CREATE TABLE sessions.session_directory_agents (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  agent_name text NOT NULL CHECK (char_length(agent_name) BETWEEN 1 AND 200),
  agent_name_normalized text NOT NULL CHECK (agent_name_normalized = lower(btrim(agent_name))),
  last_activity_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, harness, session_id, agent_name_normalized),
  FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.session_directory (org_id, workspace_id, harness, session_id)
    ON DELETE CASCADE
);
COMMENT ON TABLE sessions.session_directory_agents IS
  'One row per (logical session, agent). last_activity_at is the directory row''s, kept equal by the projector.';

CREATE INDEX session_directory_agents_recent_idx
  ON sessions.session_directory_agents
  (org_id, workspace_id, agent_name_normalized, last_activity_at DESC, session_id DESC, harness DESC);

ALTER TABLE sessions.session_directory_agents ENABLE ROW LEVEL SECURITY;
-- RPC-only, like search_text: no authenticated policy or grant.
REVOKE ALL ON sessions.session_directory_agents FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_directory_agents TO service_role;

CREATE TABLE sessions.session_directory_relationships (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  parent_session_id text NOT NULL,
  relationship_uid text NOT NULL CHECK (char_length(relationship_uid) BETWEEN 1 AND 255),
  -- NULL until the child is linked (relayhistory's unlinked relationship rows).
  child_session_id text CHECK (char_length(child_session_id) BETWEEN 1 AND 255),
  PRIMARY KEY (org_id, workspace_id, harness, parent_session_id, relationship_uid),
  FOREIGN KEY (org_id, workspace_id, harness, parent_session_id)
    REFERENCES sessions.session_directory (org_id, workspace_id, harness, session_id)
    ON DELETE CASCADE
);
CREATE INDEX session_directory_relationships_child_idx
  ON sessions.session_directory_relationships
  (org_id, workspace_id, harness, child_session_id)
  WHERE child_session_id IS NOT NULL;
ALTER TABLE sessions.session_directory_relationships ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_relationships FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_directory_relationships TO service_role;

-- A tombstone keeps no catalog dimensions: they describe the deleted content. The
-- projector deletes the agent and relationship rows with the other memberships.
CREATE FUNCTION sessions.session_directory_tombstone_clears_catalog()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $tombstone$
BEGIN
  IF NEW.deleted_at IS NOT NULL THEN
    NEW.agent_names := '{}';
    NEW.catalog_models := '{}';
    NEW.project_key := NULL;
    NEW.project_key_method := NULL;
    NEW.initial_commit := NULL;
    NEW.originator := NULL;
    NEW.agent_version := NULL;
    NEW.catalog_projected_at := NULL;
  END IF;
  RETURN NEW;
END
$tombstone$;

CREATE TRIGGER session_directory_tombstone_clears_catalog
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_tombstone_clears_catalog();

-- Companions follow the logical row's order key inside the directory row's own
-- transaction, whichever writer moves it (projector, catalog writer, erasure, a future
-- path). The list no longer orders by companion keys (H1), but
-- sessions.search_session_directory still drives users/machines from them, so this
-- keeps that RPC's pages consistent too.
CREATE FUNCTION sessions.session_directory_companions_follow()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $follow$
BEGIN
  UPDATE sessions.session_directory_users AS m SET last_activity_at = NEW.last_activity_at
   WHERE m.org_id = NEW.org_id AND m.workspace_id = NEW.workspace_id
     AND m.harness = NEW.harness AND m.session_id = NEW.session_id
     AND m.last_activity_at IS DISTINCT FROM NEW.last_activity_at;
  UPDATE sessions.session_directory_machines AS m SET last_activity_at = NEW.last_activity_at
   WHERE m.org_id = NEW.org_id AND m.workspace_id = NEW.workspace_id
     AND m.harness = NEW.harness AND m.session_id = NEW.session_id
     AND m.last_activity_at IS DISTINCT FROM NEW.last_activity_at;
  UPDATE sessions.session_directory_agents AS m SET last_activity_at = NEW.last_activity_at
   WHERE m.org_id = NEW.org_id AND m.workspace_id = NEW.workspace_id
     AND m.harness = NEW.harness AND m.session_id = NEW.session_id
     AND m.last_activity_at IS DISTINCT FROM NEW.last_activity_at;
  RETURN NULL;
END
$follow$;

CREATE TRIGGER session_directory_companions_follow
  AFTER UPDATE OF last_activity_at ON sessions.session_directory
  FOR EACH ROW WHEN (OLD.last_activity_at IS DISTINCT FROM NEW.last_activity_at)
  EXECUTE FUNCTION sessions.session_directory_companions_follow();

-- Erasure and tombstones reach every catalog row that names the session, whatever writer
-- sets deleted_at or deletes the row: its agent rows, the edges it delivered as a parent,
-- and the edges other (live) parents hold naming it as their child. A row delete
-- cascades the first two through the foreign keys; child edges have no foreign key, so
-- they are removed here in both cases.
CREATE FUNCTION sessions.session_directory_forget_catalog()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
DECLARE
  k sessions.session_directory%ROWTYPE;
BEGIN
  IF TG_OP = 'DELETE' THEN k := OLD; ELSE k := NEW; END IF;
  DELETE FROM sessions.session_directory_relationships AS r
   WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id AND r.harness = k.harness
     AND (r.child_session_id = k.session_id OR r.parent_session_id = k.session_id);
  DELETE FROM sessions.session_directory_agents AS a
   WHERE a.org_id = k.org_id AND a.workspace_id = k.workspace_id
     AND a.harness = k.harness AND a.session_id = k.session_id;
  RETURN NULL;
END
$forget$;

CREATE TRIGGER session_directory_forget_catalog_on_tombstone
  AFTER INSERT OR UPDATE OF deleted_at ON sessions.session_directory
  FOR EACH ROW WHEN (NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_directory_forget_catalog();
CREATE TRIGGER session_directory_forget_catalog_on_delete
  AFTER DELETE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_forget_catalog();

-- The agent filter matches only rows the catalog writer has written. Until the catalog
-- backfill is complete (docs/session-directory-list.md, "Backfill") the list refuses
-- p_agent (0A000 agent_filter_not_ready) rather than answer an empty list for sessions
-- whose agents were never projected; relayhistory-cloud falls back to Neon on it.
-- Marking it complete is an operator step, like session_directory_rollout.
CREATE TABLE sessions.session_directory_catalog_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.session_directory_catalog_rollout (singleton) VALUES (true)
ON CONFLICT (singleton) DO NOTHING;
ALTER TABLE sessions.session_directory_catalog_rollout ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_catalog_rollout FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_directory_catalog_rollout TO service_role;

-- The project text `projectContains` matches: the project id, the catalog project key and
-- the repository (the dashboard's `project:` matches the project id, name and key; the
-- repository carries the name). Values are joined by a newline, so only a needle that itself
-- contains a newline could match across two of the same row's values.
CREATE FUNCTION sessions.session_directory_project_text(
  p_project_id text, p_project_key text, p_git_repo text
)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $project_text$
  SELECT coalesce(p_project_id, '') || E'\n' || coalesce(p_project_key, '')
    || E'\n' || coalesce(p_git_repo, '')
$project_text$;

CREATE INDEX session_directory_tenant_project_text_trgm_idx
  ON sessions.session_directory
  USING gin (org_id, workspace_id,
             sessions.session_directory_project_text(project_id, project_key, git_repo) public.gin_trgm_ops)
  WHERE deleted_at IS NULL AND (project_id IS NOT NULL OR project_key IS NOT NULL OR git_repo IS NOT NULL);

-- The statement for one filter shape. Fixed fragments only; every value is a bound
-- parameter:
--   $1 org  $2 workspace  $3 sources text[]  $4 project LIKE pattern  $5 users text[]
--   $6 agent (normalized)  $7 last_ts_after  $8 last_ts_before  $9 first_ts_after
--   $10 first_ts_before  $11 cursor last_activity_at  $12 cursor session_id
--   $13 cursor harness  $14 row limit (page + 1)  $15 key harnesses text[]
--   $16 key session ids text[] (a batch lookup: pairs by position)  $17 excluded sources
-- The page is always ordered, and the cursor always compared, on the directory row's
-- (last_activity_at, session_id, harness); companions are membership filters only.
CREATE FUNCTION sessions.session_directory_list_sql(
  p_source_count integer, p_has_project boolean, p_user_count integer, p_has_agent boolean,
  p_has_last_after boolean, p_has_last_before boolean,
  p_has_first_after boolean, p_has_first_before boolean, p_has_keys boolean, p_has_cursor boolean,
  p_has_not_sources boolean
) RETURNS text LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, pg_temp AS $list_sql$
DECLARE
  v_users integer := coalesce(p_user_count, 0);
  v_where text;
BEGIN
  -- One sort key for the order and the cursor: the logical row's
  -- (last_activity_at, session_id, harness). The companions (agents, users) are
  -- membership filters only, never the order, so a companion row whose copied
  -- last_activity_at lags the directory row can neither repeat nor skip a session
  -- across pages (H1, #124 review). The planner may still drive from a companion's
  -- index to find a selective agent's or user's sessions, then sort them.
  v_where := ' WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL'
    || CASE WHEN p_has_agent THEN
         ' AND (d.org_id, d.workspace_id, d.harness, d.session_id) IN (SELECT a.org_id, a.workspace_id, a.harness, a.session_id'
         || ' FROM sessions.session_directory_agents AS a'
         || ' WHERE a.org_id = $1 AND a.workspace_id = $2 AND a.agent_name_normalized = $6)'
       ELSE '' END
    || CASE WHEN v_users = 1 THEN
         ' AND (d.org_id, d.workspace_id, d.harness, d.session_id) IN (SELECT u.org_id, u.workspace_id, u.harness, u.session_id'
         || ' FROM sessions.session_directory_users AS u'
         || ' WHERE u.org_id = $1 AND u.workspace_id = $2 AND u.user_id = ($5)[1])'
            WHEN v_users > 1 THEN
         ' AND (d.org_id, d.workspace_id, d.harness, d.session_id) IN (SELECT u.org_id, u.workspace_id, u.harness, u.session_id'
         || ' FROM sessions.session_directory_users AS u'
         || ' WHERE u.org_id = $1 AND u.workspace_id = $2 AND u.user_id = ANY ($5))'
       ELSE '' END
    -- One source is an ordered equality on session_directory_harness_recent_idx.
    || CASE WHEN coalesce(p_source_count, 0) = 1 THEN ' AND d.harness = ($3)[1]'
            WHEN coalesce(p_source_count, 0) > 1 THEN ' AND d.harness = ANY ($3)' ELSE '' END
    || CASE WHEN p_has_not_sources THEN ' AND NOT (d.harness = ANY ($17::text[]))' ELSE '' END
    || CASE WHEN p_has_project THEN
         ' AND (d.project_id IS NOT NULL OR d.project_key IS NOT NULL OR d.git_repo IS NOT NULL)'
         || ' AND sessions.session_directory_project_text(d.project_id, d.project_key, d.git_repo) ILIKE $4 ESCAPE ''\'''
       ELSE '' END
    || CASE WHEN p_has_last_after THEN ' AND d.last_activity_at >= $7' ELSE '' END
    || CASE WHEN p_has_last_before THEN ' AND d.last_activity_at < $8' ELSE '' END
    || CASE WHEN p_has_first_after THEN ' AND d.created_at >= $9' ELSE '' END
    || CASE WHEN p_has_first_before THEN ' AND d.created_at < $10' ELSE '' END
    || CASE WHEN p_has_keys THEN
         ' AND (d.harness, d.session_id) IN (SELECT k.h, k.s FROM unnest($15::text[], $16::text[]) AS k(h, s))'
       ELSE '' END
    || CASE WHEN p_has_cursor THEN
         ' AND (d.last_activity_at, d.session_id, d.harness) < ($11, $12, $13)'
       ELSE '' END;
  RETURN 'SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,'
    || ' d.title, d.task_title, d.agent_names, d.project_id, d.project_key, d.project_key_method,'
    || ' d.git_repo, d.git_branch, d.initial_commit, d.originator, d.agent_version,'
    || ' ARRAY(SELECT DISTINCT x FROM unnest(d.catalog_models || ARRAY[d.model]) AS x'
    ||       ' WHERE x IS NOT NULL AND x <> '''' ORDER BY x) AS models,'
    || ' d.state, d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,'
    || ' d.machine_ids, d.user_ids,'
    -- Parents: live sessions whose delivered relationships name this one as their child
    -- (a tombstoned parent's edges are deleted with it).
    || ' ARRAY(SELECT DISTINCT r.parent_session_id FROM sessions.session_directory_relationships AS r'
    ||       ' WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness'
    ||       ' AND r.child_session_id = d.session_id ORDER BY 1 LIMIT 64) AS parent_session_ids,'
    -- Children: relationship rows naming this session as parent, linked or not (an
    -- erased child's edge is deleted with it).
    || ' (SELECT count(*) FROM sessions.session_directory_relationships AS r'
    ||       ' WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness'
    ||       ' AND r.parent_session_id = d.session_id)::integer AS child_session_count,'
    || ' d.short_summary, d.short_summary_generated_at, d.catalog_projected_at, d.projected_at'
    || ' FROM sessions.session_directory AS d' || v_where
    || ' ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT $14';
END
$list_sql$;

-- The tenant-bound session list. One workspace (the claims must list it), newest
-- activity first, keyset cursor (last_activity_at, session_id, harness), at most
-- p_limit + 1 rows so the caller learns whether another page exists. Errors:
--   42501  caller is not the Worker role; or claims missing, malformed, or not covering
--          the workspace (from sessions.session_directory_worker_scope)
--   22023  an invalid value: no workspace, limit outside 1..200, more than 16 sources or
--          excluded sources, or a malformed one, more than 20 users or an empty one, an agent or project
--          substring outside 1..200 characters, an after later than its before, session
--          keys that are not 1 to 200 valid (harness, session id) pairs, or an incomplete
--          cursor
-- p_session_harnesses / p_session_ids: a batch lookup by key (page hydration for hybrid
-- search), ANDed with every other filter. The short summary and its generation time are
-- returned as search_session_directory returns them to the same role.
CREATE FUNCTION sessions.list_session_directory(
  p_workspace_id text,
  p_sources text[] DEFAULT NULL,
  p_not_sources text[] DEFAULT NULL,
  p_project_contains text DEFAULT NULL,
  p_user_ids text[] DEFAULT NULL,
  p_agent text DEFAULT NULL,
  p_last_ts_after timestamptz DEFAULT NULL,
  p_last_ts_before timestamptz DEFAULT NULL,
  p_first_ts_after timestamptz DEFAULT NULL,
  p_first_ts_before timestamptz DEFAULT NULL,
  p_session_harnesses text[] DEFAULT NULL,
  p_session_ids text[] DEFAULT NULL,
  p_cursor_last_activity_at timestamptz DEFAULT NULL,
  p_cursor_session_id text DEFAULT NULL,
  p_cursor_harness text DEFAULT NULL,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  created_at timestamptz, last_activity_at timestamptz,
  title text, task_title text, agent_names text[], project_id text,
  project_key text, project_key_method text, git_repo text, git_branch text,
  initial_commit text, originator text, agent_version text, models text[],
  state text, event_count bigint, input_tokens bigint, output_tokens bigint,
  cost_usd_micros bigint, machine_ids text[], user_ids text[],
  parent_session_ids text[], child_session_count integer,
  short_summary text, short_summary_generated_at timestamptz,
  catalog_projected_at timestamptz, projected_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $list$
DECLARE
  v_org text;
  v_sources text[];
  v_not_sources text[];
  v_users text[];
  v_agent text;
  v_project text;
  v_has_cursor boolean;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: workspace is required';
  END IF;
  -- Role guard and signed-claims pins; refuses (42501) before any value is read.
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;

  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 200 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: limit must be 1 to 200';
  END IF;
  IF (p_session_harnesses IS NULL) <> (p_session_ids IS NULL)
     OR (p_session_ids IS NOT NULL AND (
          cardinality(p_session_ids) NOT BETWEEN 1 AND 200
          OR cardinality(p_session_harnesses) <> cardinality(p_session_ids)
          OR EXISTS (SELECT 1 FROM unnest(p_session_harnesses, p_session_ids) AS k(h, s)
                      WHERE h IS NULL OR h !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
                         OR s IS NULL OR char_length(s) NOT BETWEEN 1 AND 255))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023',
      MESSAGE = 'session list: session keys must be 1 to 200 (harness, session id) pairs';
  END IF;
  IF p_sources IS NOT NULL AND cardinality(p_sources) > 0 THEN
    IF cardinality(p_sources) > 16 OR EXISTS (
         SELECT 1 FROM unnest(p_sources) AS s(v)
          WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$') THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: sources must be 1 to 16 valid harness names';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_sources FROM unnest(p_sources) AS s(v);
  END IF;
  IF p_not_sources IS NOT NULL AND cardinality(p_not_sources) > 0 THEN
    IF cardinality(p_not_sources) > 16 OR EXISTS (
         SELECT 1 FROM unnest(p_not_sources) AS s(v)
          WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$') THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: not_sources must be 1 to 16 valid harness names';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_not_sources FROM unnest(p_not_sources) AS s(v);
  END IF;
  IF p_user_ids IS NOT NULL AND cardinality(p_user_ids) > 0 THEN
    IF cardinality(p_user_ids) > 20 OR EXISTS (
         SELECT 1 FROM unnest(p_user_ids) AS u(v)
          WHERE v IS NULL OR char_length(v) NOT BETWEEN 1 AND 255) THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: users must be 1 to 20 ids of 1 to 255 characters';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_users FROM unnest(p_user_ids) AS u(v);
  END IF;
  IF p_agent IS NOT NULL THEN
    v_agent := lower(btrim(p_agent));
    IF v_agent = '' OR char_length(v_agent) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: agent must be 1 to 200 characters';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM sessions.session_directory_catalog_rollout AS r
                    WHERE r.singleton AND r.completed_at IS NOT NULL) THEN
      RAISE EXCEPTION USING ERRCODE = '0A000',
        MESSAGE = 'session list: agent_filter_not_ready: the directory catalog backfill is not complete';
    END IF;
  END IF;
  IF p_project_contains IS NOT NULL THEN
    v_project := btrim(p_project_contains);
    IF v_project = '' OR char_length(v_project) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: project_contains must be 1 to 200 characters';
    END IF;
  END IF;
  IF (p_last_ts_after IS NOT NULL AND p_last_ts_before IS NOT NULL AND p_last_ts_after > p_last_ts_before)
     OR (p_first_ts_after IS NOT NULL AND p_first_ts_before IS NOT NULL AND p_first_ts_after > p_first_ts_before) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: an after bound is later than its before bound';
  END IF;
  v_has_cursor := p_cursor_last_activity_at IS NOT NULL OR p_cursor_session_id IS NOT NULL
    OR p_cursor_harness IS NOT NULL;
  IF v_has_cursor AND (p_cursor_last_activity_at IS NULL OR p_cursor_session_id IS NULL
       OR p_cursor_harness IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: cursor is incomplete';
  END IF;

  RETURN QUERY EXECUTE sessions.session_directory_list_sql(
      coalesce(cardinality(v_sources), 0), v_project IS NOT NULL, coalesce(cardinality(v_users), 0),
      v_agent IS NOT NULL, p_last_ts_after IS NOT NULL, p_last_ts_before IS NOT NULL,
      p_first_ts_after IS NOT NULL, p_first_ts_before IS NOT NULL, p_session_ids IS NOT NULL,
      v_has_cursor, v_not_sources IS NOT NULL)
  USING v_org, p_workspace_id, v_sources,
        CASE WHEN v_project IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_project, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        v_users, v_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before,
        p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_limit + 1,
        p_session_harnesses, p_session_ids, v_not_sources;
END
$list$;

-- Distinct sessions per UTC day and harness for `GET /v1/sessions/activity` (no `q`).
-- SPAN semantics: a session counts on every UTC day from its first to its last activity,
-- clipped to the window. The directory does not know which days between them had events,
-- so a session idle mid-span still counts; relayhistory's Neon path counts only days with
-- an event. Exact per-day activity is an event question (ClickHouse). One pass over the
-- workspace's sessions overlapping the window (session_directory_workspace_recent_idx
-- range on last_activity_at), then a running sum of +1/-1 day deltas: O(sessions + days).
-- Days are UTC dates from p_since through p_until, at most 366. Same refusals as the
-- list (42501 from the scope helper; 22023 for a missing or inverted window).
CREATE FUNCTION sessions.session_directory_activity(
  p_workspace_id text, p_since timestamptz, p_until timestamptz
)
RETURNS TABLE (day date, harness text, sessions bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $activity$
DECLARE
  v_org text;
  v_first date;
  v_last date;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: workspace is required';
  END IF;
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF p_since IS NULL OR p_until IS NULL OR p_until < p_since THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: since and until are required, since first';
  END IF;
  v_first := (p_since AT TIME ZONE 'UTC')::date;
  v_last := (p_until AT TIME ZONE 'UTC')::date;
  IF v_last - v_first + 1 > 366 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: at most 366 UTC days';
  END IF;
  RETURN QUERY
    WITH spans AS MATERIALIZED (
      -- least/greatest: a row whose created_at is after its last_activity_at (nothing
      -- forbids it) spans the two in order instead of adding a negative delta.
      SELECT d.harness,
             greatest((least(d.created_at, d.last_activity_at) AT TIME ZONE 'UTC')::date, v_first) AS lo,
             least((greatest(d.created_at, d.last_activity_at) AT TIME ZONE 'UTC')::date, v_last) AS hi
        FROM sessions.session_directory AS d
       WHERE d.org_id = v_org AND d.workspace_id = p_workspace_id AND d.deleted_at IS NULL
         AND greatest(d.created_at, d.last_activity_at) >= p_since
         AND least(d.created_at, d.last_activity_at) <= p_until
    ), deltas AS (
      SELECT s.harness, s.lo AS day, 1::bigint AS delta FROM spans AS s
      UNION ALL
      SELECT s.harness, s.hi + 1, -1::bigint FROM spans AS s WHERE s.hi < v_last
    ), per_day AS (
      SELECT x.harness, x.day, sum(x.delta) AS delta FROM deltas AS x GROUP BY x.harness, x.day
    ), grid AS (
      SELECT h.harness, g.day::date AS day
        FROM (SELECT DISTINCT s.harness FROM spans AS s) AS h,
             generate_series(v_first::timestamp, v_last::timestamp, interval '1 day') AS g(day)
    ), running AS (
      SELECT g.day, g.harness,
             sum(coalesce(p.delta, 0)) OVER (PARTITION BY g.harness ORDER BY g.day)::bigint AS sessions
        FROM grid AS g
        LEFT JOIN per_day AS p ON p.harness = g.harness AND p.day = g.day
    )
    SELECT r.day, r.harness, r.sessions FROM running AS r WHERE r.sessions > 0
     ORDER BY r.day, r.harness;
END
$activity$;

REVOKE ALL ON FUNCTION sessions.session_directory_tombstone_clears_catalog() FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_directory_companions_follow()
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_forget_catalog()
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_project_text(text, text, text) FROM PUBLIC;
-- Every writer of a directory row evaluates the trigram index expression, so the writers
-- (service_role here; the runtime role through RUNTIME_APPLICATION_FUNCTIONS) execute it.
GRANT EXECUTE ON FUNCTION sessions.session_directory_project_text(text, text, text) TO service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_list_sql(
  integer, boolean, integer, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.list_session_directory(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_activity(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_activity(text, timestamptz, timestamptz)
  TO session_directory_search_worker;
GRANT EXECUTE ON FUNCTION sessions.list_session_directory(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer)
  TO session_directory_search_worker;

-- As in 20261030020000 and 20261031001000: strip EXECUTE from every grantee present at
-- creation (default privileges, a pre-existing runtime role), whatever the order. A grant
-- added later is still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_directory_tombstone_clears_catalog()'::regprocedure,
    'sessions.session_directory_companions_follow()'::regprocedure,
    'sessions.session_directory_forget_catalog()'::regprocedure,
    'sessions.session_directory_list_sql(integer, boolean, integer, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)'::regprocedure,
    'sessions.list_session_directory(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)'::regprocedure,
    'sessions.session_directory_activity(text, timestamptz, timestamptz)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'session_directory_search_worker'
                  AND v_fn IN ('sessions.list_session_directory(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)'::regprocedure,
                               'sessions.session_directory_activity(text, timestamptz, timestamptz)'::regprocedure))
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
