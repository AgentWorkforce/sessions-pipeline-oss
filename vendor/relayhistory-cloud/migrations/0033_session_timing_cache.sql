-- Lazily computed recorded timing. NULL workspace is an organization-wide cache
-- key, never a sentinel that can collide with a real workspace ID. Invalidation
-- stays inside each writer's transaction and performs no timing computation.
-- Existing sessions need no backfill: the first read creates and fills the row.
CREATE TABLE sessions.session_timing_cache (
  org_id text NOT NULL,
  session_id text NOT NULL,
  source text NOT NULL,
  workspace_id text,
  input_revision bigint NOT NULL DEFAULT 0,
  cached_revision bigint,
  algorithm_version integer,
  payload jsonb,
  computed_at timestamptz,
  CONSTRAINT session_timing_cache_key UNIQUE NULLS NOT DISTINCT
    (org_id, session_id, source, workspace_id),
  CONSTRAINT session_timing_cache_revision_check CHECK
    (input_revision >= 0 AND (cached_revision IS NULL OR cached_revision <= input_revision))
);

-- A cold fill is serialized per exact key, not across a whole organization.
-- JSON encoding keeps tenant separators and NULL scopes unambiguous.
CREATE FUNCTION sessions.session_timing_cache_lock_key(
  p_org_id text, p_session_id text, p_source text, p_workspace_id text
) RETURNS bigint LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, sessions, pg_temp AS $lock_key$
  SELECT hashtextextended('session-timing:' || jsonb_build_array(
    p_org_id, p_session_id, p_source, p_workspace_id)::text, 0)
$lock_key$;

-- Version 1: one snapshot, all stored input records, no transcript bodies sent
-- to the Worker. Keep invalidation's relevant-column projections aligned with
-- this function when changing the algorithm, and bump the cached reader version.
CREATE FUNCTION sessions.session_timing_compute(
  p_org_id text, p_session_id text, p_source text, p_workspace_id text DEFAULT NULL
) RETURNS jsonb LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $compute$
  SELECT jsonb_build_object(
    'sessionId', p_session_id, 'source', p_source,
    'startedAt', to_char(r."startedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'lastActionAt', to_char(r."lastActionAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'workMs', r."workMs", 'waitingMs', r."waitingMs",
    'coverage', jsonb_build_object(
      'scope', 'stored_session', 'method', 'recorded_intervals',
      'eventCount', r."eventCount", 'turnCount', r."turnCount", 'markerCount', r."markerCount",
      'workIntervalCount', r."workIntervalCount", 'waitingIntervalCount', r."waitingIntervalCount",
      'lifecycleIntervalCount', r."lifecycleIntervalCount",
      'unmatchedToolCallCount', r."unmatchedToolCallCount", 'unmatchedLifecycleCount', r."unmatchedLifecycleCount"
    )
  ) FROM (

    WITH event_rows AS MATERIALIZED (
      SELECT workspace_id, user_id, machine_id, event_id, kind, type, ts,
        actor_role, tool_name, duration_ms,
        coalesce(record->'payload', record) AS payload, tool_calls
      FROM sessions.convergence_events
      WHERE org_id = p_org_id AND session_id = p_session_id AND source = p_source
        AND (p_workspace_id IS NULL OR workspace_id = p_workspace_id)
    ), normalized AS (
      SELECT *, lower(coalesce(nullif(payload->>'kind', ''), nullif(type, ''), kind)) AS event_kind,
        coalesce(nullif(payload->>'tool_use_id', ''), nullif(payload->>'toolUseId', ''),
          nullif(payload->>'call_id', ''), nullif(payload->>'callId', ''),
          CASE WHEN jsonb_array_length(CASE WHEN jsonb_typeof(tool_calls) = 'array' THEN tool_calls ELSE '[]'::jsonb END) = 1
            THEN coalesce(tool_calls->0->>'toolUseId', tool_calls->0->>'tool_use_id', tool_calls->0->>'call_id', tool_calls->0->>'id') END) AS call_id,
        coalesce(nullif(tool_name, ''), nullif(payload->>'tool_name', ''),
          nullif(payload->>'name', ''), tool_calls->0->>'name') AS name,
        lower(coalesce(payload->>'role', actor_role, '')) AS role
      FROM event_rows
    ), classified AS (
      SELECT *,
        (role <> 'tool_result' AND (event_kind IN ('custom_tool_call','function_call','mcp_tool_call','tool_use','tool-call','tool_call') OR (kind='tool_call' AND event_kind NOT IN ('tool_result','tool-result','tool_call_output','function_call_output','custom_tool_call_output','mcp_tool_call_output')))) AS is_call,
        (event_kind IN ('custom_tool_call_output','function_call_output','mcp_tool_call_output','tool_result','tool-result','tool_call_output') OR role = 'tool_result') AS is_result,
        (kind IN ('prompt','user','assistant','history','session_event','tool_call','file_edit','trajectory','task') AND role <> 'system' AND event_kind NOT IN ('session_meta','session_metadata','metadata','discovery','presence','system','system_message')) AS is_activity
      FROM normalized
    ), calls AS (
      SELECT workspace_id, user_id, machine_id, call_id, min(ts) AS started_at,
        max(name) AS name, max(duration_ms) FILTER (WHERE duration_ms >= 0) AS duration_ms,
        count(DISTINCT ts) AS start_count
      FROM classified WHERE is_call AND call_id IS NOT NULL AND call_id <> ''
      GROUP BY workspace_id, user_id, machine_id, call_id
    ), outputs AS (
      SELECT workspace_id, user_id, machine_id, call_id, min(ts) AS ended_at,
        max(duration_ms) FILTER (WHERE duration_ms >= 0) AS duration_ms,
        count(DISTINCT ts) AS end_count
      FROM classified WHERE is_result AND call_id IS NOT NULL AND call_id <> ''
      GROUP BY workspace_id, user_id, machine_id, call_id
    ), paired AS (
      SELECT c.*, o.ended_at, o.duration_ms AS result_duration_ms,
        (c.start_count = 1 AND o.end_count = 1 AND o.ended_at >= c.started_at) AS matched,
        lower(coalesce(c.name,'')) ~ '(^|[.:/]|__)(request_user_input|askuserquestion)$' AS is_wait,
        lower(coalesce(c.name,'')) ~ '(^|[.:/]|__)request_user_input_async$' AS is_async
      FROM calls c LEFT JOIN outputs o ON c.workspace_id IS NOT DISTINCT FROM o.workspace_id AND c.user_id IS NOT DISTINCT FROM o.user_id AND c.machine_id IS NOT DISTINCT FROM o.machine_id AND c.call_id=o.call_id
    ), tool_intervals AS (
      SELECT workspace_id, user_id, machine_id AS origin_id, CASE WHEN is_wait THEN 'waiting' ELSE 'work' END AS bucket,
        CASE WHEN is_wait THEN started_at
          WHEN result_duration_ms IS NOT NULL THEN ended_at - result_duration_ms * interval '1 millisecond'
          ELSE started_at END AS start_at,
        CASE WHEN is_wait OR result_duration_ms IS NOT NULL THEN ended_at
          WHEN duration_ms IS NOT NULL THEN started_at + duration_ms * interval '1 millisecond'
          ELSE ended_at END AS end_at
      FROM paired WHERE NOT is_async AND (matched OR
        (NOT is_wait AND ended_at IS NULL AND start_count=1 AND duration_ms IS NOT NULL))
      UNION ALL
      SELECT workspace_id,user_id,machine_id,'work',
        CASE WHEN is_result THEN ts - duration_ms * interval '1 millisecond' ELSE ts END,
        CASE WHEN is_result THEN ts ELSE ts + duration_ms * interval '1 millisecond' END
      FROM classified WHERE duration_ms >= 0 AND (is_call OR is_result)
        AND name IS NOT NULL
        AND lower(name) !~ '(^|[.:/]|__)(request_user_input(_async)?|askuserquestion)$'
        AND (call_id IS NULL OR call_id='' OR (is_result AND NOT EXISTS (
          SELECT 1 FROM calls c WHERE c.workspace_id IS NOT DISTINCT FROM classified.workspace_id
            AND c.user_id IS NOT DISTINCT FROM classified.user_id
            AND c.machine_id IS NOT DISTINCT FROM classified.machine_id AND c.call_id=classified.call_id
        )))
    ), markers AS MATERIALIZED (
      SELECT workspace_id,user_id,'delivery:' || md5(jsonb_build_array(workspace_id,origin_id)::text) AS origin_id,turn_id,marker_kind,ts
      FROM sessions.session_markers
      WHERE org_id = p_org_id AND session_id = p_session_id AND source = p_source
        AND (p_workspace_id IS NULL OR workspace_id = p_workspace_id)
    ), lifecycle AS (
      SELECT workspace_id,user_id,origin_id,turn_id,
        min(ts) FILTER (WHERE marker_kind='task_started') AS start_at,
        min(ts) FILTER (WHERE marker_kind='task_complete') AS end_at,
        count(*) FILTER (WHERE marker_kind='task_started') AS starts,
        count(*) FILTER (WHERE marker_kind='task_complete') AS ends
      FROM markers WHERE marker_kind IN ('task_started','task_complete') AND turn_id IS NOT NULL AND turn_id <> '' AND ts IS NOT NULL
      GROUP BY workspace_id,user_id,origin_id,turn_id
    ), lifecycle_intervals AS (
      SELECT workspace_id,user_id,origin_id,'work' AS bucket,start_at,end_at FROM lifecycle
      WHERE starts=1 AND ends=1 AND end_at >= start_at
    ), intervals AS (
      SELECT * FROM tool_intervals UNION ALL SELECT * FROM lifecycle_intervals
    ), lifecycle_ranges AS (
      SELECT workspace_id,user_id,origin_id,range_agg(tstzrange(start_at,end_at,'[)')) AS spans
      FROM lifecycle_intervals GROUP BY workspace_id,user_id,origin_id
    ), waiting_ranges AS (
      SELECT workspace_id,user_id,origin_id,range_agg(tstzrange(start_at,end_at,'[)')) AS spans
      FROM tool_intervals WHERE bucket='waiting' GROUP BY workspace_id,user_id,origin_id
    ), net_ranges AS (
      SELECT 'work' AS bucket, unnest(l.spans - coalesce(w.spans,'{}'::tstzmultirange)) AS span
      FROM lifecycle_ranges l LEFT JOIN waiting_ranges w
        ON l.workspace_id IS NOT DISTINCT FROM w.workspace_id
        AND l.user_id IS NOT DISTINCT FROM w.user_id AND l.origin_id IS NOT DISTINCT FROM w.origin_id
      UNION ALL SELECT bucket,tstzrange(start_at,end_at,'[)') FROM tool_intervals
    ), merged_ranges AS (
      SELECT bucket,unnest(range_agg(span)) AS span FROM net_ranges GROUP BY bucket
    ), durations AS (
      SELECT bucket,sum(extract(epoch FROM (upper(span)-lower(span))) * 1000) AS ms
      FROM merged_ranges GROUP BY bucket
    ), turns AS MATERIALIZED (
      SELECT ts,role FROM sessions.conversation_turns
      WHERE org_id=p_org_id AND session_id=p_session_id AND source=p_source
        AND (p_workspace_id IS NULL OR workspace_id = p_workspace_id)
    ), bounds AS (
      SELECT ts, (role IN ('assistant','tool_result') OR is_call OR is_result OR kind IN ('assistant','file_edit') OR event_kind IN ('assistant','assistant_message')) AS is_action
      FROM classified WHERE is_activity OR is_call OR is_result
      UNION ALL SELECT ts,role='assistant' FROM turns WHERE role IN ('user','assistant')
      UNION ALL SELECT ts,true FROM markers WHERE marker_kind IN ('task_started','task_complete') AND ts IS NOT NULL
    )
    SELECT (SELECT min(ts) FROM bounds) AS "startedAt",
      (SELECT max(ts) FROM bounds WHERE is_action) AS "lastActionAt",
      CASE WHEN EXISTS(SELECT 1 FROM intervals WHERE bucket='work') THEN coalesce((SELECT ms FROM durations WHERE bucket='work'),0) END AS "workMs",
      CASE WHEN EXISTS(SELECT 1 FROM intervals WHERE bucket='waiting') THEN coalesce((SELECT ms FROM durations WHERE bucket='waiting'),0) END AS "waitingMs",
      (SELECT count(*) FROM event_rows) AS "eventCount",
      (SELECT count(*) FROM turns) AS "turnCount",
      (SELECT count(*) FROM markers) AS "markerCount",
      (SELECT count(*) FROM tool_intervals WHERE bucket='work') AS "workIntervalCount",
      (SELECT count(*) FROM tool_intervals WHERE bucket='waiting') AS "waitingIntervalCount",
      (SELECT count(*) FROM lifecycle_intervals) AS "lifecycleIntervalCount",
      ((SELECT count(*) FROM paired WHERE matched IS NOT TRUE AND NOT is_async) +
       (SELECT count(*) FROM classified WHERE is_call AND (call_id IS NULL OR call_id=''))) AS "unmatchedToolCallCount",
      (SELECT count(*) FROM lifecycle WHERE starts<>1 OR ends<>1 OR end_at < start_at) AS "unmatchedLifecycleCount"
  
  ) AS r
$compute$;

CREATE FUNCTION sessions.session_timing_cached(
  p_org_id text, p_session_id text, p_source text, p_workspace_id text DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $cached$
DECLARE
  -- Increment whenever compute semantics change; mismatched rows refill lazily.
  v_algorithm_version constant integer := 1;
  v_row sessions.session_timing_cache%ROWTYPE;
  v_revision bigint;
  v_payload jsonb;
BEGIN
  SELECT * INTO v_row FROM sessions.session_timing_cache
   WHERE org_id=p_org_id AND session_id=p_session_id AND source=p_source
     AND workspace_id IS NOT DISTINCT FROM p_workspace_id;
  IF FOUND AND v_row.algorithm_version=v_algorithm_version
     AND v_row.cached_revision=v_row.input_revision AND v_row.payload IS NOT NULL THEN
    RETURN v_row.payload;
  END IF;

  PERFORM pg_advisory_xact_lock(sessions.session_timing_cache_lock_key(
    p_org_id,p_session_id,p_source,p_workspace_id));
  INSERT INTO sessions.session_timing_cache(org_id,session_id,source,workspace_id)
    VALUES(p_org_id,p_session_id,p_source,p_workspace_id)
    ON CONFLICT ON CONSTRAINT session_timing_cache_key DO NOTHING;
  -- Recheck after the lock: another reader may have completed this exact fill.
  SELECT * INTO STRICT v_row FROM sessions.session_timing_cache
   WHERE org_id=p_org_id AND session_id=p_session_id AND source=p_source
     AND workspace_id IS NOT DISTINCT FROM p_workspace_id;
  IF v_row.algorithm_version=v_algorithm_version
     AND v_row.cached_revision=v_row.input_revision AND v_row.payload IS NOT NULL THEN
    RETURN v_row.payload;
  END IF;

  -- Capture BEFORE computing. A writer can commit between these statements or
  -- during the aggregate. Never stamp that result with the writer's newer revision.
  v_revision := v_row.input_revision;
  v_payload := sessions.session_timing_compute(p_org_id,p_session_id,p_source,p_workspace_id);
  UPDATE sessions.session_timing_cache
     SET payload=v_payload, cached_revision=v_revision,
         algorithm_version=v_algorithm_version, computed_at=clock_timestamp()
   WHERE org_id=p_org_id AND session_id=p_session_id AND source=p_source
     AND workspace_id IS NOT DISTINCT FROM p_workspace_id
     AND input_revision=v_revision;
  -- A racing invalidation may prevent publication. This response still represents
  -- the aggregate's consistent snapshot; the next read recomputes the dirty row.
  RETURN v_payload;
END;
$cached$;

-- Inserts/deletes always change coverage or bounds. Updates compare only inputs
-- the algorithm reads: embedding, usage, content and other unrelated enrichment
-- must not turn every cached session dirty. EXCEPT operates on identity-bearing
-- projections, so two otherwise identical rows cannot hide a changed input.
CREATE FUNCTION sessions.invalidate_session_timing() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $invalidate$
BEGIN
  IF TG_OP='TRUNCATE' THEN
    UPDATE sessions.session_timing_cache SET input_revision=input_revision+1;
    RETURN NULL;
  END IF;
  IF TG_OP='INSERT' THEN
    INSERT INTO sessions.session_timing_cache AS cache
      (org_id,session_id,source,workspace_id,input_revision)
    SELECT DISTINCT k.org_id,k.session_id,k.source,scope.workspace_id,1
      FROM (SELECT org_id, session_id, source, workspace_id FROM new_timing_rows) AS k
      CROSS JOIN LATERAL (VALUES(k.workspace_id),(NULL::text)) AS scope(workspace_id)
      ORDER BY k.org_id,k.session_id,k.source,scope.workspace_id NULLS FIRST
    ON CONFLICT ON CONSTRAINT session_timing_cache_key DO UPDATE
      SET input_revision=cache.input_revision+1;
  ELSIF TG_OP='DELETE' THEN
    INSERT INTO sessions.session_timing_cache AS cache
      (org_id,session_id,source,workspace_id,input_revision)
    SELECT DISTINCT k.org_id,k.session_id,k.source,scope.workspace_id,1
      FROM (SELECT org_id, session_id, source, workspace_id FROM old_timing_rows) AS k
      CROSS JOIN LATERAL (VALUES(k.workspace_id),(NULL::text)) AS scope(workspace_id)
      ORDER BY k.org_id,k.session_id,k.source,scope.workspace_id NULLS FIRST
    ON CONFLICT ON CONSTRAINT session_timing_cache_key DO UPDATE
      SET input_revision=cache.input_revision+1;
  ELSIF TG_TABLE_NAME='convergence_events' THEN
    WITH new_inputs AS (
      SELECT org_id,
        session_id,
        source,
        workspace_id,
        event_id,
        machine_id,
        user_id,
        kind,
        type,
        ts,
        actor_role,
        tool_name,
        duration_ms,
        jsonb_build_array(
          (coalesce(record->'payload',record))->'kind',
          (coalesce(record->'payload',record))->'role',
          (coalesce(record->'payload',record))->'tool_use_id',
          (coalesce(record->'payload',record))->'toolUseId',
          (coalesce(record->'payload',record))->'call_id',
          (coalesce(record->'payload',record))->'callId',
          (coalesce(record->'payload',record))->'tool_name',
          (coalesce(record->'payload',record))->'name'
        ) AS timing_payload,
        jsonb_build_array(
          jsonb_array_length(CASE WHEN jsonb_typeof(tool_calls)='array' THEN tool_calls ELSE '[]'::jsonb END),
          tool_calls->0->'toolUseId',
          tool_calls->0->'tool_use_id',
          tool_calls->0->'call_id',
          tool_calls->0->'id',
          tool_calls->0->'name'
        ) AS timing_tools
        FROM new_timing_rows
    ), old_inputs AS (
      SELECT org_id,
        session_id,
        source,
        workspace_id,
        event_id,
        machine_id,
        user_id,
        kind,
        type,
        ts,
        actor_role,
        tool_name,
        duration_ms,
        jsonb_build_array(
          (coalesce(record->'payload',record))->'kind',
          (coalesce(record->'payload',record))->'role',
          (coalesce(record->'payload',record))->'tool_use_id',
          (coalesce(record->'payload',record))->'toolUseId',
          (coalesce(record->'payload',record))->'call_id',
          (coalesce(record->'payload',record))->'callId',
          (coalesce(record->'payload',record))->'tool_name',
          (coalesce(record->'payload',record))->'name'
        ) AS timing_payload,
        jsonb_build_array(
          jsonb_array_length(CASE WHEN jsonb_typeof(tool_calls)='array' THEN tool_calls ELSE '[]'::jsonb END),
          tool_calls->0->'toolUseId',
          tool_calls->0->'tool_use_id',
          tool_calls->0->'call_id',
          tool_calls->0->'id',
          tool_calls->0->'name'
        ) AS timing_tools
        FROM old_timing_rows
    ), changed AS (
      (TABLE new_inputs EXCEPT TABLE old_inputs)
      UNION ALL (TABLE old_inputs EXCEPT TABLE new_inputs)
    )
    INSERT INTO sessions.session_timing_cache AS cache
      (org_id,session_id,source,workspace_id,input_revision)
    SELECT DISTINCT k.org_id,k.session_id,k.source,scope.workspace_id,1
      FROM changed AS k
      CROSS JOIN LATERAL (VALUES(k.workspace_id),(NULL::text)) AS scope(workspace_id)
      ORDER BY k.org_id,k.session_id,k.source,scope.workspace_id NULLS FIRST
    ON CONFLICT ON CONSTRAINT session_timing_cache_key DO UPDATE
      SET input_revision=cache.input_revision+1;
  ELSIF TG_TABLE_NAME='conversation_turns' THEN
    WITH new_inputs AS (
      SELECT org_id,
        session_id,
        source,
        workspace_id,
        id,
        ts,
        role
        FROM new_timing_rows
    ), old_inputs AS (
      SELECT org_id,
        session_id,
        source,
        workspace_id,
        id,
        ts,
        role
        FROM old_timing_rows
    ), changed AS (
      (TABLE new_inputs EXCEPT TABLE old_inputs)
      UNION ALL (TABLE old_inputs EXCEPT TABLE new_inputs)
    )
    INSERT INTO sessions.session_timing_cache AS cache
      (org_id,session_id,source,workspace_id,input_revision)
    SELECT DISTINCT k.org_id,k.session_id,k.source,scope.workspace_id,1
      FROM changed AS k
      CROSS JOIN LATERAL (VALUES(k.workspace_id),(NULL::text)) AS scope(workspace_id)
      ORDER BY k.org_id,k.session_id,k.source,scope.workspace_id NULLS FIRST
    ON CONFLICT ON CONSTRAINT session_timing_cache_key DO UPDATE
      SET input_revision=cache.input_revision+1;
  ELSIF TG_TABLE_NAME='session_markers' THEN
    WITH new_inputs AS (
      SELECT org_id,
        session_id,
        source,
        workspace_id,
        marker_uid,
        user_id,
        origin_id,
        turn_id,
        marker_kind,
        ts
        FROM new_timing_rows
    ), old_inputs AS (
      SELECT org_id,
        session_id,
        source,
        workspace_id,
        marker_uid,
        user_id,
        origin_id,
        turn_id,
        marker_kind,
        ts
        FROM old_timing_rows
    ), changed AS (
      (TABLE new_inputs EXCEPT TABLE old_inputs)
      UNION ALL (TABLE old_inputs EXCEPT TABLE new_inputs)
    )
    INSERT INTO sessions.session_timing_cache AS cache
      (org_id,session_id,source,workspace_id,input_revision)
    SELECT DISTINCT k.org_id,k.session_id,k.source,scope.workspace_id,1
      FROM changed AS k
      CROSS JOIN LATERAL (VALUES(k.workspace_id),(NULL::text)) AS scope(workspace_id)
      ORDER BY k.org_id,k.session_id,k.source,scope.workspace_id NULLS FIRST
    ON CONFLICT ON CONSTRAINT session_timing_cache_key DO UPDATE
      SET input_revision=cache.input_revision+1;
  ELSE
    RAISE EXCEPTION 'unexpected session timing invalidation table %', TG_TABLE_NAME;
  END IF;
  RETURN NULL;
END;
$invalidate$;

CREATE TRIGGER convergence_events_timing_insert
  AFTER INSERT ON sessions.convergence_events
  REFERENCING NEW TABLE AS new_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER convergence_events_timing_update
  AFTER UPDATE ON sessions.convergence_events
  REFERENCING OLD TABLE AS old_timing_rows NEW TABLE AS new_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER convergence_events_timing_delete
  AFTER DELETE ON sessions.convergence_events
  REFERENCING OLD TABLE AS old_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER convergence_events_timing_truncate
  AFTER TRUNCATE ON sessions.convergence_events
  
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER conversation_turns_timing_insert
  AFTER INSERT ON sessions.conversation_turns
  REFERENCING NEW TABLE AS new_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER conversation_turns_timing_update
  AFTER UPDATE ON sessions.conversation_turns
  REFERENCING OLD TABLE AS old_timing_rows NEW TABLE AS new_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER conversation_turns_timing_delete
  AFTER DELETE ON sessions.conversation_turns
  REFERENCING OLD TABLE AS old_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER conversation_turns_timing_truncate
  AFTER TRUNCATE ON sessions.conversation_turns
  
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER session_markers_timing_insert
  AFTER INSERT ON sessions.session_markers
  REFERENCING NEW TABLE AS new_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER session_markers_timing_update
  AFTER UPDATE ON sessions.session_markers
  REFERENCING OLD TABLE AS old_timing_rows NEW TABLE AS new_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER session_markers_timing_delete
  AFTER DELETE ON sessions.session_markers
  REFERENCING OLD TABLE AS old_timing_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();

CREATE TRIGGER session_markers_timing_truncate
  AFTER TRUNCATE ON sessions.session_markers
  
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.invalidate_session_timing();
