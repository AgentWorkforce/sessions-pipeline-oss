-- Claude sidechain user rows are not user activity.
--
-- A Claude `isSidechain` user row is a delegating agent's prompt (or tool result)
-- to a subagent. RelayHistory 0.37.0 stores it as `is_sidechain` evidence and never
-- as a prompt, user turn or `history` (ai-hist USER_TURN_ROW_FILTER). Such rows
-- reach a parent session's delivery stream from transcripts that inline sidechain
-- traffic and from `agent-*.jsonl` sidecars whose records name no `agentId`; a
-- sidecar that does name one is delivered under the child's own session id. v2
-- projected every one of them as a `user` message, so briefs, timing and rollups
-- read delegated prompts as the human's.
--
-- 1. The v2 projection keeps them in delivery_records and does not project them,
--    as it already does control rows. Sidechain assistant output and tool results
--    stay activity.
-- 2. A database that applied an earlier 0023 runs v2 in place as
--    project_delivery_session() (0029 marks its activity stage complete). The v2
--    defined here is the one the trigger runs there from now on.
-- 3. Rows v2 projected before this migration are removed afterwards, by the
--    `sidechain` stage of scripts/rollout-delivery-projection.mjs, in short
--    batches under the workspace lock uploads take (the 0029 pattern), never in
--    the deploy transaction.

-- What 1. withholds: role `user`, `is_sidechain` 1/true.
CREATE FUNCTION sessions.delivery_sidechain_user_row(p_kind text, p_payload jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $sidechain$
  SELECT p_kind = 'session_event' AND p_payload->>'role' = 'user'
     AND COALESCE(p_payload->>'is_sidechain', '') IN ('1', 'true')
$sidechain$;

-- 2. Before v2 exists here, record where an earlier 0023 left the activity stage:
-- once it does, 0029's detection would otherwise read the stage as never started
-- and replay every retained record.
DO $earlier_v2$
BEGIN
  IF to_regprocedure('sessions.project_delivery_session_v2()') IS NULL THEN
    INSERT INTO sessions.delivery_rollout (stage, activated_snapshot, completed_at)
    VALUES ('activity', pg_current_snapshot(), now())
    ON CONFLICT (stage) DO NOTHING;
  END IF;
END;
$earlier_v2$;

CREATE OR REPLACE FUNCTION sessions.project_delivery_session_v2() RETURNS trigger LANGUAGE plpgsql AS $projection$
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
  IF TG_OP = 'UPDATE' AND NEW.kind IN ('session_lineage', 'turn_receipt')
    AND OLD.kind IN ('session_lineage', 'turn_receipt') THEN
    RETURN NEW;
  END IF;
  -- Every other write takes the workspace lock before reading any projection, so
  -- the cross-kind check below cannot race another origin's write of the same
  -- record (both would otherwise see nothing and both insert). The lock is the one
  -- the catalog projection takes, held until commit; a batch holds only this one.
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || NEW.org_id || chr(31) || NEW.workspace_id, 0));
  IF TG_OP = 'UPDATE' AND OLD.kind = ANY (activity_kinds)
    AND (OLD.org_id, OLD.workspace_id, OLD.record_id, OLD.kind)
      IS DISTINCT FROM (NEW.org_id, NEW.workspace_id, NEW.record_id, NEW.kind) THEN
    PERFORM sessions.retract_delivery_projection(
      OLD.org_id, OLD.workspace_id, OLD.origin_id, OLD.record_id, OLD.kind, OLD.source, OLD.session_id);
  END IF;
  -- The same record_id may arrive from another origin under a new kind. That write
  -- is also the newest statement about the record, so any projection of it under
  -- another kind goes too, whichever origin wrote it (retract settles its group).
  IF EXISTS (
    SELECT 1 FROM sessions.convergence_events
     WHERE org_id = NEW.org_id AND workspace_id = NEW.workspace_id
       AND delivery_record_id = NEW.record_id AND kind <> NEW.kind
  ) THEN
    FOR stale_kind IN
      SELECT DISTINCT kind FROM sessions.convergence_events
       WHERE org_id = NEW.org_id AND workspace_id = NEW.workspace_id
         AND delivery_record_id = NEW.record_id AND kind <> NEW.kind
    LOOP
      PERFORM sessions.retract_delivery_projection(
        NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id, stale_kind, NEW.source, NEW.session_id);
    END LOOP;
  END IF;
  IF NOT (NEW.kind = ANY (activity_kinds)) THEN RETURN NEW; END IF;
  -- Every origin of one workspace settles shared projections (cross-origin records,
  -- request groups) under the lock taken above, so concurrent origins cannot both win.

  -- The newest delivery write of this logical record, from any origin, wins, and
  -- the arriving row is that write: the lock above is held until commit and each
  -- statement takes a fresh snapshot, so whoever acquires it later wrote later.
  -- record_id is origin-independent. received_at cannot order origins: it is now()
  -- in accept_delivery_batch, the transaction start, so a writer that waited on
  -- the lock carries an older stamp than the write it follows.
  winner := NEW;
  winner_machine := 'delivery:' || md5(jsonb_build_array(NEW.workspace_id, NEW.origin_id)::text);
  PERFORM sessions.retract_delivery_projection(
    NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id, NEW.kind, NEW.source, NEW.session_id);

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
      SELECT COALESCE(min(parent.ts), activity) INTO activity FROM sessions.convergence_events AS parent
       WHERE parent.org_id = winner.org_id AND parent.workspace_id = winner.workspace_id
         AND parent.source = winner.source AND parent.session_id = winner.session_id
         AND parent.kind = 'session_event' AND parent.delivery_record_id IS NOT NULL
         AND parent.record->'payload'->>'tool_use_id' = tool_use;
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
  RETURN NEW;
END;
$projection$;


-- 2. Where the activity stage is complete but the trigger still runs
-- project_delivery_session(), that function is an earlier 0023's v2: point the
-- trigger at this one, in place as 0029 does. Before activation the trigger runs
-- 0014 and the rollout points it here once v2's indexes are valid.
DO $repoint$
BEGIN
  IF EXISTS (
    SELECT 1 FROM sessions.delivery_rollout
     WHERE stage = 'activity' AND completed_at IS NOT NULL
  ) AND EXISTS (
    SELECT 1 FROM pg_trigger AS t
      JOIN pg_proc AS p ON p.oid = t.tgfoid
     WHERE t.tgrelid = 'sessions.delivery_records'::regclass
       AND t.tgname = 'delivery_session_projection'
       AND p.proname = 'project_delivery_session'
  ) THEN
    CREATE OR REPLACE TRIGGER delivery_session_projection AFTER INSERT OR UPDATE ON sessions.delivery_records
      FOR EACH ROW EXECUTE FUNCTION sessions.project_delivery_session_v2();
  END IF;
END;
$repoint$;

-- 3. The rollout's stages, with `sidechain` added. It needs no index and has no
-- earlier form, so it has work until its row is complete.
CREATE OR REPLACE FUNCTION sessions.delivery_rollout_stage_requires_indexes(p_stage text)
RETURNS boolean LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $requires_indexes$
DECLARE
  completed timestamptz;
BEGIN
  SELECT completed_at INTO completed
    FROM sessions.delivery_rollout WHERE stage = p_stage;
  IF FOUND THEN RETURN completed IS NULL; END IF;
  IF p_stage = 'catalog' THEN
    RETURN NOT EXISTS (
      SELECT 1 FROM pg_trigger
       WHERE tgrelid = 'sessions.delivery_records'::regclass
         AND tgname = 'delivery_session_catalog_insert');
  ELSIF p_stage = 'activity' THEN
    RETURN to_regprocedure('sessions.project_delivery_session_v2()') IS NOT NULL;
  ELSIF p_stage = 'sidechain' THEN
    RETURN true;
  END IF;
  RAISE EXCEPTION 'unknown_delivery_rollout_stage' USING ERRCODE = 'P0001';
END;
$requires_indexes$;

-- Stage 4a. Nothing to switch on (1. is live at commit); records the stage.
CREATE FUNCTION sessions.activate_delivery_sidechain_cleanup()
RETURNS boolean LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $activate_sidechain$
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.delivery_rollout WHERE stage = 'sidechain') THEN
    RETURN false;
  END IF;
  INSERT INTO sessions.delivery_rollout (stage, activated_snapshot)
  VALUES ('sidechain', pg_current_snapshot());
  RETURN true;
END;
$activate_sidechain$;

-- Stage 4b. Walk up to p_limit projected records after the cursor, through
-- convergence_events_delivery_record_idx, and remove the sidechain user rows among
-- them; returns how many records were walked, so a batch's work is bounded however
-- few of them match. A batch that ends at a workspace's last record returns fewer
-- than p_limit with work still left, and one whose workspace an upload emptied
-- before its lock was granted returns 0; the stage is done when its row is
-- complete, which the caller reads after a 0.
--
-- A batch stays within one workspace. A delete takes its session's rollup lock
-- (0030, keyed by org and session, not workspace) after the workspace lock, as an
-- upload's projection does; holding a second workspace's lock meanwhile could
-- wait on an upload there that waits on that rollup lock.
--
-- The rollout runs this stage after `activity`, when every retained record is a
-- v2 projection. A record written meanwhile is projected by the v2 above and holds
-- nothing to remove; one that re-arrives after its row was removed here is not
-- projected again.
CREATE FUNCTION sessions.delivery_sidechain_cleanup_step(p_limit integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $sidechain_step$
DECLARE
  progress sessions.delivery_rollout%ROWTYPE;
  scope record;
  item record;
  removed record;
  processed integer := 0;
  last_record jsonb;
BEGIN
  SELECT * INTO progress FROM sessions.delivery_rollout WHERE stage = 'sidechain' FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'delivery_rollout_not_activated' USING ERRCODE = 'P0001';
  END IF;
  IF progress.completed_at IS NOT NULL THEN RETURN 0; END IF;
  SELECT e.org_id, e.workspace_id INTO scope
    FROM sessions.convergence_events AS e
   WHERE e.delivery_record_id IS NOT NULL
     AND (progress.cursor IS NULL
       OR (e.org_id, e.workspace_id, e.delivery_record_id)
          > (progress.cursor->>0, progress.cursor->>1, progress.cursor->>2))
   ORDER BY e.org_id, e.workspace_id, e.delivery_record_id
   LIMIT 1;
  IF NOT FOUND THEN
    UPDATE sessions.delivery_rollout SET completed_at = now(), updated_at = now()
     WHERE stage = 'sidechain';
    RETURN 0;
  END IF;
  -- The lock v2 takes, and the only workspace lock this batch takes.
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || scope.org_id || chr(31) || scope.workspace_id, 0));
  FOR item IN
    SELECT DISTINCT e.delivery_record_id
      FROM sessions.convergence_events AS e
     WHERE e.org_id = scope.org_id AND e.workspace_id = scope.workspace_id
       AND e.delivery_record_id IS NOT NULL
       AND (progress.cursor IS NULL
         OR (e.org_id, e.workspace_id, e.delivery_record_id)
            > (progress.cursor->>0, progress.cursor->>1, progress.cursor->>2))
     ORDER BY 1
     LIMIT p_limit
  LOOP
    FOR removed IN
      DELETE FROM sessions.convergence_events
       WHERE org_id = scope.org_id AND workspace_id = scope.workspace_id
         AND delivery_record_id = item.delivery_record_id
         AND sessions.delivery_sidechain_user_row(kind, record->'payload')
      RETURNING source, session_id, record->'payload'->>'tool_use_id' AS tool_use_id
    LOOP
      -- As retract_delivery_projection does: a tool row timed by this event
      -- takes its next earliest event.
      PERFORM sessions.settle_delivery_tool_ts(
        scope.org_id, scope.workspace_id, removed.source, removed.session_id, removed.tool_use_id);
    END LOOP;
    processed := processed + 1;
    last_record := jsonb_build_array(scope.org_id, scope.workspace_id, item.delivery_record_id);
  END LOOP;
  UPDATE sessions.delivery_rollout
     SET cursor = COALESCE(last_record, cursor), updated_at = now()
   WHERE stage = 'sidechain';
  RETURN processed;
END;
$sidechain_step$;
