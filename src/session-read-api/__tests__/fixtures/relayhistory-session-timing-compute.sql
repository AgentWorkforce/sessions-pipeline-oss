-- Vendored parity oracle: sessions.session_timing_compute v1, verbatim from
-- AgentWorkforce/relayhistory-cloud packages/relayhistory/db/migrations/0033_session_timing_cache.sql at c7de216335a1633b80314c3fd0000a48da9ff29d
-- (lines between CREATE FUNCTION sessions.session_timing_compute and its $compute$ end).
-- Test-only: the read API's TS port (src/session-read-api/timing.ts) must equal it.
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
