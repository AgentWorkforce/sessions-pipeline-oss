-- Plan-stable legacy retraction for every delivered activity row.
--
-- project_delivery_activity_row calls retract_delivery_projection for every
-- activity record it accepts, including brand-new ones. Its first DELETE looks
-- for a pre-0023 projection (delivery_record_id IS NULL) by the full primary
-- key. When the session is absent from convergence_events' statistics (any
-- session that started after the last ANALYZE, and autoanalyze has been off
-- on that table since the 2026-10-24 stopgap), the planner rates
-- the (org_id, session_id, workspace_id) index (CONCURRENT_INDEXES) as
-- cheap as the primary key and picks it. That probe reads
-- every row of the session, so a batch costs O(records x session rows).
-- Measured on a local copy of the schema, 500-record batches of one session
-- grew from 1.4 s to 2.5 s by 10,000 rows, with 22 M index tuples read for
-- 2,000 records; retract_delivery_projection was 62% of the batch.
-- Every second of that is spent holding the org and workspace delivery locks.
--
-- The added predicate is always true here (machine is 'delivery:' || md5),
-- so this function returns exactly what it did. It lets the planner match
-- a partial index (CONCURRENT_INDEXES) of only pre-0023
-- delivery projections (no new write enters it). Until that index exists
-- (it is built after commit by apply-neon-migration.mjs, only where
-- RELAYHISTORY_LEGACY_RETRACT_INDEX=1), the plan is the same as before.
--
-- Only this function is replaced: no table is created, altered or scanned,
-- no trigger changes, no lock changes key or order, and no write hold is
-- touched.

CREATE OR REPLACE FUNCTION sessions.retract_delivery_projection(
  p_org text, p_workspace text, p_origin text, p_record_id text, p_kind text, p_source text, p_session text
) RETURNS void LANGUAGE plpgsql AS $retract$
DECLARE
  machine text := 'delivery:' || md5(jsonb_build_array(p_workspace, p_origin)::text);
  prior record;
BEGIN
  -- Projections written before 0023 carry no delivery_record_id (0011/0014 event ids).
  IF p_session IS NOT NULL THEN
    DELETE FROM sessions.convergence_events
     WHERE org_id = p_org AND machine_id = machine AND source = p_source
       AND session_id = p_session AND kind = p_kind AND delivery_record_id IS NULL
       AND machine_id LIKE 'delivery:%'
       AND event_id IN (p_record_id, machine || ':' || p_record_id);
  END IF;
  FOR prior IN
    DELETE FROM sessions.convergence_events
     WHERE org_id = p_org AND workspace_id = p_workspace
       AND delivery_record_id = p_record_id AND kind = p_kind
    RETURNING source, session_id, request_key, kind AS projected_kind, record->'payload'->>'tool_use_id' AS tool_use_id
  LOOP
    PERFORM sessions.settle_delivery_request(p_org, p_workspace, prior.source, prior.session_id, prior.request_key);
    IF prior.projected_kind = 'session_event' THEN
      PERFORM sessions.settle_delivery_tool_ts(p_org, p_workspace, prior.source, prior.session_id, prior.tool_use_id);
    END IF;
  END LOOP;
END;
$retract$;
