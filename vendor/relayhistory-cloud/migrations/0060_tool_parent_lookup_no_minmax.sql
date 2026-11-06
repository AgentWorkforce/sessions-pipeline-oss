-- Look up a tool call's parent event through the tool_use_id index, never by
-- walking the organization's timeline.
--
-- settle_delivery_tool_ts (0031) and project_delivery_activity_row (0038) find
-- the earliest session event naming a tool_use_id with `SELECT min(parent.ts)`.
-- On a custom plan Postgres rewrites a bare min(ts) as `ORDER BY ts LIMIT 1`
-- and, for an organization with many rows, costs it on the (org_id, source, ts)
-- index below the (org_id, workspace_id, source, session_id, tool_use_id) probe:
-- it expects a match early, then filters every row of the organization's
-- source history (heap fetches included) when the parent is late or absent.
-- Retractions always plan this way: retract_delivery_projection runs with
-- plan_cache_mode = force_custom_plan (0054), which holds for the
-- settle_delivery_tool_ts calls it makes.
--
-- Local PG17, one 174,000-event codex session (2026-10-27): custom plan
-- 32,850-39,709 buffers per lookup on org_source_ts_idx against 4 on
-- delivery_tool_use_idx. A 500-record revision batch read 5.06 M buffers
-- (19.7 s), 4.64 M of them in this lookup (118 org walks). Organizations with
-- fewer rows plan the index probe, which is why batches into small sessions
-- stayed fast.
--
-- Selecting the rows in an OFFSET 0 subquery keeps the aggregate above a plain
-- scan, so the planner picks an access path by its filters alone. The result
-- is the same minimum. No data, index or lock change; both functions keep
-- their signatures, and neither had a SET or grant to carry over.
CREATE OR REPLACE FUNCTION sessions.settle_delivery_tool_ts(
  p_org text, p_workspace text, p_source text, p_session text, p_tool_use_id text
) RETURNS void LANGUAGE plpgsql AS $tool_ts$
DECLARE
  parent_ts timestamptz;
BEGIN
  IF p_tool_use_id IS NULL THEN RETURN; END IF;
  SELECT min(parents.ts) INTO parent_ts FROM (
    SELECT parent.ts FROM sessions.convergence_events AS parent
     WHERE parent.org_id = p_org AND parent.workspace_id = p_workspace AND parent.source = p_source
       AND parent.session_id = p_session AND parent.kind = 'session_event'
       AND parent.delivery_record_id IS NOT NULL
       AND parent.record->'payload'->>'tool_use_id' = p_tool_use_id
    OFFSET 0) AS parents;
  UPDATE sessions.convergence_events AS tool SET ts = COALESCE(parent_ts, tool.ingested_at)
   WHERE tool.org_id = p_org AND tool.workspace_id = p_workspace AND tool.source = p_source
     AND tool.session_id = p_session AND tool.kind IN ('tool_call', 'file_edit')
     AND tool.delivery_record_id IS NOT NULL
     AND tool.record->'payload'->>'tool_use_id' = p_tool_use_id
     AND sessions.delivery_activity_ms(tool.record->'payload') IS NULL
     AND tool.ts IS DISTINCT FROM COALESCE(parent_ts, tool.ingested_at);
END;
$tool_ts$;

-- 0038's function, unchanged except for the parent lookup.

CREATE OR REPLACE FUNCTION sessions.project_delivery_activity_row(
  p_new sessions.delivery_records, p_old sessions.delivery_records, p_operation text
) RETURNS void LANGUAGE plpgsql AS $projection$
DECLARE
  activity_kinds text[] := ARRAY['history', 'session_event', 'tool_call', 'file_edit', 'trajectory'];
  winner sessions.delivery_records%ROWTYPE;
  winner_machine text;
  stale_kind text;
  p jsonb;
  activity timestamptz;
  milliseconds numeric;
  content_text text;
  request_identity text;
  tool_use text;
  tool_error boolean;
  tool_state text;
  args_value jsonb;
  tool_call_json jsonb;
BEGIN
  -- A revision can reclassify a record or move it: the write that did so is the
  -- newest statement about its prior identity, so that projection goes, exactly as
  -- a tombstone would remove it, before any early return for the new kind.
  -- An update from babysitter evidence (session_lineage, turn_receipt) to babysitter
  -- evidence cannot change any projection: the row was never projected and, having
  -- been evidence already, any cross-kind retraction ran when it first arrived. It
  -- returns before the lock, so expiry (which row-locks those rows first, across
  -- workspaces, then updates them) never waits on the workspace lock a delivery
  -- batch already holds. A newly inserted evidence row still takes the lock below
  -- and retracts another origin's activity projection of the same record.
  IF p_operation = 'UPDATE' AND p_new.kind IN ('session_lineage', 'turn_receipt')
    AND p_old.kind IN ('session_lineage', 'turn_receipt') THEN
    RETURN;
  END IF;
  -- Every other write takes the workspace lock before reading any projection, so
  -- the cross-kind check below cannot race another origin's write of the same
  -- record (both would otherwise see nothing and both insert). The lock is the one
  -- the catalog projection takes, held until commit; a batch holds only this one.
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || p_new.org_id || chr(31) || p_new.workspace_id, 0));
  IF p_operation = 'UPDATE' AND p_old.kind = ANY (activity_kinds)
    AND (p_old.org_id, p_old.workspace_id, p_old.record_id, p_old.kind)
      IS DISTINCT FROM (p_new.org_id, p_new.workspace_id, p_new.record_id, p_new.kind) THEN
    PERFORM sessions.retract_delivery_projection(
      p_old.org_id, p_old.workspace_id, p_old.origin_id, p_old.record_id, p_old.kind, p_old.source, p_old.session_id);
  END IF;
  -- The same record_id may arrive from another origin under a new kind. That write
  -- is also the newest statement about the record, so any projection of it under
  -- another kind goes too, whichever origin wrote it (retract settles its group).
  IF EXISTS (
    SELECT 1 FROM sessions.convergence_events
     WHERE org_id = p_new.org_id AND workspace_id = p_new.workspace_id
       AND delivery_record_id = p_new.record_id AND kind <> p_new.kind
  ) THEN
    FOR stale_kind IN
      SELECT DISTINCT kind FROM sessions.convergence_events
       WHERE org_id = p_new.org_id AND workspace_id = p_new.workspace_id
         AND delivery_record_id = p_new.record_id AND kind <> p_new.kind
    LOOP
      PERFORM sessions.retract_delivery_projection(
        p_new.org_id, p_new.workspace_id, p_new.origin_id, p_new.record_id, stale_kind, p_new.source, p_new.session_id);
    END LOOP;
  END IF;
  IF NOT (p_new.kind = ANY (activity_kinds)) THEN RETURN; END IF;
  -- Every origin of one workspace settles shared projections (cross-origin records,
  -- request groups) under the lock taken above, so concurrent origins cannot both win.

  -- The newest delivery write of this logical record, from any origin, wins, and
  -- the arriving row is that write: the lock above is held until commit and each
  -- statement takes a fresh snapshot, so whoever acquires it later wrote later.
  -- record_id is origin-independent. received_at cannot order origins: it is now()
  -- in accept_delivery_batch, the transaction start, so a writer that waited on
  -- the lock carries an older stamp than the write it follows.
  winner := p_new;
  winner_machine := 'delivery:' || md5(jsonb_build_array(p_new.workspace_id, p_new.origin_id)::text);
  PERFORM sessions.retract_delivery_projection(
    p_new.org_id, p_new.workspace_id, p_new.origin_id, p_new.record_id, p_new.kind, p_new.source, p_new.session_id);

  p := winner.payload;
  IF winner.operation = 'upsert' AND winner.session_id IS NOT NULL
    -- Control rows are harness plumbing, not activity (sourcing-contract "Control rows").
    AND NOT (winner.kind = 'session_event' AND (
      NULLIF(p->>'control_kind', '') IS NOT NULL OR COALESCE(p->>'is_meta', '') IN ('1', 'true')))
    -- A Claude sidechain user row is the delegating agent's prompt or tool result
    -- to a subagent, not a human turn (0037).
    AND NOT sessions.delivery_sidechain_user_row(winner.kind, p)
  THEN
    tool_use := NULLIF(p->>'tool_use_id', '');
    activity := winner.received_at;
    milliseconds := sessions.delivery_activity_ms(p);
    IF milliseconds IS NOT NULL THEN
      activity := to_timestamp((milliseconds / 1000)::double precision);
    ELSIF winner.kind IN ('tool_call', 'file_edit') AND tool_use IS NOT NULL THEN
      -- OFFSET 0: see settle_delivery_tool_ts below (0060).
      SELECT COALESCE(min(parent_ts), activity) INTO activity FROM (
        SELECT parent.ts AS parent_ts FROM sessions.convergence_events AS parent
         WHERE parent.org_id = winner.org_id AND parent.workspace_id = winner.workspace_id
           AND parent.source = winner.source AND parent.session_id = winner.session_id
           AND parent.kind = 'session_event' AND parent.delivery_record_id IS NOT NULL
           AND parent.record->'payload'->>'tool_use_id' = tool_use
        OFFSET 0) AS parents;
    END IF;
    content_text := CASE winner.kind
      WHEN 'history' THEN p->>'prompt'
      WHEN 'session_event' THEN p->>'text'
      WHEN 'tool_call' THEN concat_ws(' ', p->>'name', p->>'target')
      WHEN 'file_edit' THEN p->>'file_path'
      WHEN 'trajectory' THEN COALESCE(p->>'task_description', p->>'search_text')
      ELSE NULL END;
    -- The request identity `session_requests` groups by: namespace-qualified,
    -- verbatim, only for assistant rows with a record id.
    request_identity := CASE WHEN winner.kind = 'session_event' AND p->>'role' = 'assistant' AND NULLIF(p->>'message_id', '') IS NOT NULL THEN
      CASE
        WHEN NULLIF(p->>'request_id', '') IS NOT NULL THEN 'request-id:' || (p->>'request_id')
        WHEN NULLIF(p->>'provider_message_id', '') IS NOT NULL THEN 'provider-message-id:' || (p->>'provider_message_id')
        WHEN NULLIF(p->>'request_span', '') IS NOT NULL THEN 'request-span:' || (p->>'request_span')
        ELSE 'record-id:' || (p->>'message_id')
      END END;
    IF winner.kind = 'tool_call' THEN
      tool_error := CASE WHEN (p->>'is_error') IN ('1', 'true') THEN true WHEN (p->>'is_error') IN ('0', 'false') THEN false END;
      tool_state := CASE tool_error WHEN true THEN 'error' WHEN false THEN 'success' END;
      BEGIN
        args_value := (p->>'args_json')::jsonb;
      EXCEPTION WHEN others THEN
        args_value := NULL;
      END;
      SELECT COALESCE(jsonb_object_agg(field.key, field.value), '{}'::jsonb) INTO tool_call_json
        FROM jsonb_each(jsonb_build_object(
          'toolUseId', tool_use, 'name', p->>'name', 'target', p->>'target',
          'args', args_value, 'isError', tool_error, 'status', tool_state)) AS field
       WHERE field.value <> 'null'::jsonb;
    END IF;
    INSERT INTO sessions.convergence_events (
      org_id, workspace_id, machine_id, user_id, source, session_id, event_id,
      kind, type, ts, actor_role, subagent_id, project_id, content, task_title, model, provider,
      tool_name, tool_status, tool_calls, files_touched, delivery_record_id, request_key,
      cost_usd_micros, record, ingested_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner_machine, winner.user_id, winner.source, winner.session_id,
      winner_machine || ':' || winner.record_id,
      winner.kind, COALESCE(p->>'role', winner.kind), activity,
      CASE WHEN winner.kind = 'history' THEN 'user' ELSE p->>'role' END,
      -- Only a subagent session id marks subagent activity: root assistant events
      -- can carry their own agent_id.
      NULLIF(p->>'subagent_session_id', ''),
      sessions.delivery_project_id(p), content_text,
      CASE WHEN winner.kind = 'trajectory' THEN p->>'task_title' ELSE NULL END,
      p->>'model', NULLIF(p->>'provider', ''),
      CASE WHEN winner.kind = 'tool_call' THEN p->>'name' END,
      tool_state,
      CASE WHEN winner.kind = 'tool_call' THEN jsonb_build_array(tool_call_json) ELSE '[]'::jsonb END,
      CASE WHEN winner.kind = 'file_edit' AND NULLIF(p->>'file_path', '') IS NOT NULL
        THEN jsonb_build_array(p->>'file_path') ELSE '[]'::jsonb END,
      winner.record_id, request_identity, NULL,
      jsonb_build_object('deliveryRecordId', winner.record_id, 'payload', p), winner.received_at
    );
    PERFORM sessions.settle_delivery_request(winner.org_id, winner.workspace_id, winner.source, winner.session_id, request_identity);
    IF winner.kind = 'session_event' AND tool_use IS NOT NULL THEN
      PERFORM sessions.settle_delivery_tool_ts(winner.org_id, winner.workspace_id, winner.source, winner.session_id, tool_use);
    END IF;
  END IF;
  RETURN;
END;
$projection$;
