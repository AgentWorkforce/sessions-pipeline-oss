-- P5b G2: Desktop onboarding heartbeat (relayhistory-cloud POST /v1/onboarding/heartbeat,
-- the busiest relayhistory route) moves off Neon sessions.probe_connections. One row per
-- (org, workspace, user): when the probe last checked in and its counters-only progress.
-- Reached only through the two SECURITY DEFINER functions below (the ingest Worker's role);
-- the import function is owner-only, for the one-time Neon -> Supabase backfill.
CREATE TABLE history.probe_connections (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  last_seen_at timestamptz NOT NULL,
  progress jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, user_id)
);
REVOKE ALL ON history.probe_connections FROM PUBLIC;
CREATE INDEX probe_connections_org_seen ON history.probe_connections (org_id, last_seen_at);

CREATE FUNCTION history.probe_heartbeat(p_org text, p_ws text, p_user text, p_progress jsonb)
RETURNS timestamptz LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, history AS $heartbeat$
  INSERT INTO history.probe_connections (org_id, workspace_id, user_id, last_seen_at, progress)
  VALUES (p_org, p_ws, p_user, clock_timestamp(), p_progress)
  ON CONFLICT (org_id, workspace_id, user_id)
    DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at, progress = EXCLUDED.progress, updated_at = now()
  RETURNING last_seen_at;
$heartbeat$;

CREATE FUNCTION history.probe_connection(p_org text, p_ws text, p_user text)
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, history AS $connection$
  SELECT jsonb_build_object('lastSeenAt', last_seen_at, 'progress', progress)
  FROM history.probe_connections WHERE org_id = p_org AND workspace_id = p_ws AND user_id = p_user;
$connection$;

-- Owner-only backfill: rows [{org_id, workspace_id, user_id, last_seen_at, progress}]; a newer
-- heartbeat already recorded here always wins over the copied Neon value.
CREATE FUNCTION history.probe_connections_import(p_rows jsonb)
RETURNS integer LANGUAGE sql VOLATILE
SET search_path = pg_catalog, history AS $import$
  WITH r AS (
    SELECT x->>'org_id' AS org_id, x->>'workspace_id' AS workspace_id, x->>'user_id' AS user_id,
           (x->>'last_seen_at')::timestamptz AS last_seen_at, x->'progress' AS progress
    FROM jsonb_array_elements(p_rows) x
  ), up AS (
    INSERT INTO history.probe_connections (org_id, workspace_id, user_id, last_seen_at, progress)
    SELECT org_id, workspace_id, user_id, last_seen_at, CASE WHEN progress = 'null'::jsonb THEN NULL ELSE progress END FROM r
    ON CONFLICT (org_id, workspace_id, user_id) DO UPDATE
      SET last_seen_at = EXCLUDED.last_seen_at, progress = EXCLUDED.progress, updated_at = now()
      WHERE history.probe_connections.last_seen_at < EXCLUDED.last_seen_at
    RETURNING 1
  ) SELECT count(*)::integer FROM up;
$import$;

REVOKE ALL ON FUNCTION history.probe_heartbeat(text, text, text, jsonb),
  history.probe_connection(text, text, text), history.probe_connections_import(jsonb) FROM PUBLIC;
DO $grant$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest') THEN
    GRANT USAGE ON SCHEMA history TO bulls96_session_ingest;
    GRANT EXECUTE ON FUNCTION history.probe_heartbeat(text, text, text, jsonb),
      history.probe_connection(text, text, text) TO bulls96_session_ingest;
  END IF;
END
$grant$;
