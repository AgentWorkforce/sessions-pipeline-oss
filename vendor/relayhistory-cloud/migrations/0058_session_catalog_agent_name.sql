-- Catalog each delivered session's agent name, so a session can be looked up by
-- the name other agents address it by.
--
-- The client (relayhistory, SESSION_CATALOG_CONTRACT_VERSION 7) catalogs Claude's
-- `agent-name` record -- an Agent Relay worker's name, e.g.
-- `agent-relay-social-animation` -- apart from the title (where a custom or AI
-- title outranks it). Session change records carry it, so the delivered `session`
-- payload has `agent_name`. Before this, no table stored it, and GET /v1/sessions
-- had nothing to filter on.
--
-- It lives in its own table, session_catalog_agents, not in a new session_catalog
-- column. The first attempt (ALTER TABLE session_catalog ADD COLUMN, deploy run
-- 40462096092) needed ACCESS EXCLUSIVE on session_catalog. Delivery batches write
-- that table all the time and can run for tens of seconds, so the lock was not
-- granted within the runner's 30 s lock_timeout. Meanwhile every new catalog
-- writer queued behind the request. The deploy rolled back, and the migration
-- was never applied anywhere, so it is rewritten here in place.
--
-- This version takes no lock on any existing table. It creates a new (empty)
-- table and its index, and replaces project_delivery_catalog_row (0022) so the
-- projection also writes the name. Replacing a function locks neither its callers
-- nor their tables. A short local lock_timeout bounds every lock this migration
-- could still meet, and the runner's value is restored at the end.
--
-- One row per catalog key that has a name, kept by the projection with the same
-- rule as the catalog row: newest record wins, and a record without a name
-- removes it. Other paths that delete catalog rows (retraction, cleanup) do not
-- know this table. Lookups therefore join session_catalog, and a leftover row
-- for a removed catalog key never matches. Sessions delivered before the client
-- sent the field have no row until their catalog record is delivered again
-- (fix-forward only, no backfill).

SELECT set_config('relayhistory.migration_saved_lock_timeout', current_setting('lock_timeout'), true),
       set_config('lock_timeout', '5s', true);

CREATE TABLE sessions.session_catalog_agents (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  agent_name text NOT NULL,
  -- The catalog record that set it.
  record_id text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, source, session_id)
);

-- GET /v1/sessions?agent= and hybrid search's agent filter (agentSessionsClause in
-- src/lib/recall.ts, which must keep lower(agent_name) identical).
CREATE INDEX session_catalog_agents_name_idx
  ON sessions.session_catalog_agents (org_id, lower(agent_name), workspace_id);

CREATE OR REPLACE FUNCTION sessions.project_delivery_catalog_row(winner sessions.delivery_records)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_row$
DECLARE
  k jsonb := sessions.delivery_catalog_key(winner.kind, winner.source, winner.session_id, winner.operation, winner.payload);
  p jsonb := winner.payload;
  target_session text := k->>2;
  confidence_value numeric;
  agent text;
BEGIN
  IF k IS NULL THEN RETURN; END IF;

  IF winner.kind = 'session' THEN
    INSERT INTO sessions.session_catalog (
      org_id, workspace_id, source, session_id, record_id, origin_id, user_id,
      title, cwd, git_branch, repo_url, initial_commit, first_prompt, models,
      originator, agent_version, workspace_roots, project_key, project_key_method,
      discovery_state, first_activity_at, last_activity_at, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session, winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'title'),
      sessions.delivery_catalog_text(p->'cwd'),
      sessions.delivery_catalog_text(p->'git_branch'),
      sessions.delivery_catalog_text(p->'repo_url'),
      sessions.delivery_catalog_text(p->'initial_commit'),
      sessions.delivery_catalog_text(p->'first_prompt'),
      sessions.delivery_catalog_json(p->'models_json'),
      sessions.delivery_catalog_text(p->'originator'),
      sessions.delivery_catalog_text(p->'agent_version'),
      sessions.delivery_catalog_json(p->'workspace_roots_json'),
      sessions.delivery_catalog_text(p->'project_key'),
      sessions.delivery_catalog_text(p->'project_key_method'),
      sessions.delivery_catalog_text(p->'discovery_state'),
      sessions.delivery_catalog_time(p->'first_activity_ms'),
      sessions.delivery_catalog_time(p->'last_activity_ms'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, session_id) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      title = excluded.title, cwd = excluded.cwd, git_branch = excluded.git_branch,
      repo_url = excluded.repo_url, initial_commit = excluded.initial_commit,
      first_prompt = excluded.first_prompt, models = excluded.models,
      originator = excluded.originator, agent_version = excluded.agent_version,
      workspace_roots = excluded.workspace_roots, project_key = excluded.project_key,
      project_key_method = excluded.project_key_method, discovery_state = excluded.discovery_state,
      first_activity_at = excluded.first_activity_at, last_activity_at = excluded.last_activity_at,
      received_at = excluded.received_at;

    -- The session's agent name, by the catalog row's rule: this (newest) record's
    -- value replaces it, and a record without one removes it.
    agent := sessions.delivery_catalog_text(p->'agent_name');
    IF agent IS NULL THEN
      DELETE FROM sessions.session_catalog_agents AS a
       WHERE a.org_id = winner.org_id AND a.workspace_id = winner.workspace_id
         AND a.source = winner.source AND a.session_id = target_session;
    ELSE
      INSERT INTO sessions.session_catalog_agents AS a
        (org_id, workspace_id, source, session_id, agent_name, record_id)
      VALUES (winner.org_id, winner.workspace_id, winner.source, target_session, agent, winner.record_id)
      ON CONFLICT (org_id, workspace_id, source, session_id) DO UPDATE SET
        agent_name = excluded.agent_name, record_id = excluded.record_id, updated_at = now()
      WHERE (a.agent_name, a.record_id) IS DISTINCT FROM (excluded.agent_name, excluded.record_id);
    END IF;

  ELSIF winner.kind = 'relationship' THEN
    INSERT INTO sessions.session_relationships (
      org_id, workspace_id, source, parent_session_id, relationship_uid, record_id, origin_id, user_id,
      child_session_id, relationship, identity_status, child_agent_type, child_agent_name,
      child_model, spawn_depth, evidence_kind, child_has_events, spawned_at, updated_at, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session,
      k->>3, winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'child_session_id'),
      sessions.delivery_catalog_text(p->'relationship'),
      sessions.delivery_catalog_text(p->'identity_status'),
      sessions.delivery_catalog_text(p->'child_agent_type'),
      sessions.delivery_catalog_text(p->'child_agent_name'),
      sessions.delivery_catalog_text(p->'child_model'),
      sessions.delivery_catalog_integer(p->'spawn_depth'),
      sessions.delivery_catalog_text(p->'evidence_kind'),
      CASE WHEN jsonb_typeof(p->'child_has_events') = 'boolean' THEN (p->>'child_has_events')::boolean
           ELSE sessions.delivery_catalog_integer(p->'child_has_events') <> 0 END,
      sessions.delivery_catalog_time(p->'spawned_at_ms'),
      sessions.delivery_catalog_time(p->'updated_ms'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, parent_session_id, relationship_uid) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      child_session_id = excluded.child_session_id, relationship = excluded.relationship,
      identity_status = excluded.identity_status, child_agent_type = excluded.child_agent_type,
      child_agent_name = excluded.child_agent_name, child_model = excluded.child_model,
      spawn_depth = excluded.spawn_depth, evidence_kind = excluded.evidence_kind,
      child_has_events = excluded.child_has_events, spawned_at = excluded.spawned_at,
      updated_at = excluded.updated_at, received_at = excluded.received_at;

  ELSIF winner.kind = 'session_marker' THEN
    INSERT INTO sessions.session_markers (
      org_id, workspace_id, source, session_id, marker_uid, record_id, origin_id, user_id,
      marker_kind, subkind, ts, message_id, parent_id, turn_id, text, payload, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session,
      k->>3, winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'kind'),
      sessions.delivery_catalog_text(p->'subkind'),
      sessions.delivery_catalog_time(p->'ts_ms'),
      sessions.delivery_catalog_text(p->'message_id'),
      sessions.delivery_catalog_text(p->'parent_id'),
      sessions.delivery_catalog_text(p->'turn_id'),
      sessions.delivery_catalog_text(p->'text'),
      sessions.delivery_catalog_json(p->'payload_json'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, session_id, marker_uid) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      marker_kind = excluded.marker_kind, subkind = excluded.subkind, ts = excluded.ts,
      message_id = excluded.message_id, parent_id = excluded.parent_id, turn_id = excluded.turn_id,
      text = excluded.text, payload = excluded.payload, received_at = excluded.received_at;

  ELSIF winner.kind = 'commit_link' THEN
    -- The local store keeps confidence as 0..1; the service stores basis points.
    confidence_value := CASE WHEN jsonb_typeof(p->'confidence') = 'number' THEN (p->>'confidence')::numeric END;
    INSERT INTO sessions.session_commit_links (
      org_id, workspace_id, source, session_id, commit_sha, match_method, record_id, origin_id, user_id,
      repo, branch, note_ref, confidence_basis_points, files, numstat, evidence, linked_at, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session,
      k->>3, k->>4,
      winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'repo'),
      sessions.delivery_catalog_text(p->'branch'),
      sessions.delivery_catalog_text(p->'note_ref'),
      CASE WHEN confidence_value BETWEEN 0 AND 1 THEN round(confidence_value * 10000)::integer END,
      sessions.delivery_catalog_json(p->'files_json'),
      sessions.delivery_catalog_json(p->'numstat_json'),
      sessions.delivery_catalog_json(p->'evidence_json'),
      sessions.delivery_catalog_time(p->'created_at_ms'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, session_id, commit_sha, match_method) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      repo = excluded.repo, branch = excluded.branch, note_ref = excluded.note_ref,
      confidence_basis_points = excluded.confidence_basis_points, files = excluded.files,
      numstat = excluded.numstat, evidence = excluded.evidence, linked_at = excluded.linked_at,
      received_at = excluded.received_at;
  END IF;
END;
$catalog_row$;

-- Restore the runner's value (unchanged if nothing was saved, e.g. statements run
-- outside one transaction).
SELECT set_config('lock_timeout', COALESCE(NULLIF(
  current_setting('relayhistory.migration_saved_lock_timeout', true), ''), current_setting('lock_timeout')), true);
