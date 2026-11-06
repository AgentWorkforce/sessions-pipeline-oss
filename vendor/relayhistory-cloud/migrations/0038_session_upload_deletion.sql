-- Keep immutable copy ownership after a record is replaced or retired. These
-- identities contain no transcript payload and are pruned after explicit cleanup.
CREATE TABLE sessions.uploaded_support_identities (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
  origin_id text NOT NULL, source text NOT NULL, session_id text NOT NULL, record_id text NOT NULL,
  PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id,record_id)
);
-- History-sized ownership recovery/cache invalidation is a post-migration rollout.
-- A monotonic completion flag keeps hot paths read-only after activation.
CREATE TABLE sessions.upload_cleanup_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  phase integer NOT NULL DEFAULT 0 CHECK(phase BETWEEN 0 AND 5),
  cursor text[] NOT NULL DEFAULT ARRAY[]::text[],
  item_id text, support_offset integer NOT NULL DEFAULT 0,
  legacy boolean NOT NULL DEFAULT false, item_stage text NOT NULL DEFAULT 'support'
);
INSERT INTO sessions.upload_cleanup_rollout(singleton) VALUES(true);
-- Completion is monotonic even for direct SQL; normal deliveries never lock it.
CREATE FUNCTION sessions.guard_upload_cleanup_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $cleanup_checkpoint$
BEGIN
  IF NEW.phase<OLD.phase THEN RAISE EXCEPTION 'upload cleanup cannot regress'; END IF;
  RETURN NEW;
END;
$cleanup_checkpoint$;
CREATE TRIGGER guard_upload_cleanup_checkpoint BEFORE UPDATE ON sessions.upload_cleanup_rollout
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_upload_cleanup_checkpoint();
CREATE TABLE sessions.upload_cleanup_origins (
  org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,machine_id text NOT NULL,
  PRIMARY KEY(org_id,machine_id)
);
CREATE TABLE sessions.upload_cleanup_tombstones (
  org_id text NOT NULL,machine_id text NOT NULL,source text NOT NULL,event_id text NOT NULL,
  workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
  PRIMARY KEY(org_id,machine_id,source,event_id)
);
CREATE FUNCTION sessions.upload_cleanup_ready() RETURNS boolean LANGUAGE sql STABLE AS $cleanup_ready$
  SELECT COALESCE((SELECT phase=5 FROM sessions.upload_cleanup_rollout WHERE singleton),false);
$cleanup_ready$;
CREATE FUNCTION sessions.require_upload_cleanup_ready() RETURNS void LANGUAGE plpgsql AS $cleanup_guard$
BEGIN
  IF NOT sessions.upload_cleanup_ready() THEN RAISE EXCEPTION 'upload_cleanup_not_ready' USING ERRCODE='P0001'; END IF;
END;
$cleanup_guard$;
-- Enforce the maintenance hold on direct SQL and every old/new function alias,
-- not merely the current Worker route. No checkpoint row lock on the ready path.
CREATE FUNCTION sessions.guard_delivery_cleanup() RETURNS trigger LANGUAGE plpgsql AS $cleanup_delivery_guard$
BEGIN
  PERFORM sessions.require_upload_cleanup_ready();
  RETURN COALESCE(NEW,OLD);
END;
$cleanup_delivery_guard$;
CREATE TRIGGER guard_delivery_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.delivery_records
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_delivery_cleanup();
CREATE TRIGGER guard_delivery_origin_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.delivery_origins
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_delivery_cleanup();
-- Other event ingests remain available. A direct or old projection writer may
-- not insert a delivery event behind the phase-2 cursor during maintenance.
CREATE FUNCTION sessions.guard_upload_projection_cleanup() RETURNS trigger LANGUAGE plpgsql AS $cleanup_projection_guard$
DECLARE old_delivery boolean:=false; new_delivery boolean:=false;
BEGIN
  IF TG_OP<>'INSERT' THEN old_delivery:=OLD.delivery_record_id IS NOT NULL OR starts_with(OLD.machine_id,'delivery:'); END IF;
  IF TG_OP<>'DELETE' THEN new_delivery:=NEW.delivery_record_id IS NOT NULL OR starts_with(NEW.machine_id,'delivery:'); END IF;
  IF old_delivery OR new_delivery THEN PERFORM sessions.require_upload_cleanup_ready(); END IF;
  RETURN COALESCE(NEW,OLD);
END;
$cleanup_projection_guard$;
CREATE TRIGGER guard_upload_projection_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.convergence_events
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_upload_projection_cleanup();
CREATE FUNCTION sessions.guard_memory_cleanup() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,sessions,pg_temp AS $cleanup_memory_guard$
BEGIN
  IF NOT sessions.upload_cleanup_ready() AND current_user<>(SELECT pg_catalog.pg_get_userbyid(proowner) FROM pg_catalog.pg_proc WHERE oid='sessions.upload_cleanup_step(integer)'::regprocedure) THEN
    RAISE EXCEPTION 'upload_cleanup_not_ready' USING ERRCODE='P0001';
  END IF;
  RETURN COALESCE(NEW,OLD);
END;
$cleanup_memory_guard$;
CREATE TRIGGER guard_claim_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.neighborhood_claims FOR EACH ROW EXECUTE FUNCTION sessions.guard_memory_cleanup();
CREATE TRIGGER guard_edge_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.neighborhood_edges FOR EACH ROW EXECUTE FUNCTION sessions.guard_memory_cleanup();
CREATE TRIGGER guard_pattern_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.patterns FOR EACH ROW EXECUTE FUNCTION sessions.guard_memory_cleanup();
CREATE TRIGGER guard_reflex_feedback_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.pattern_hits FOR EACH ROW EXECUTE FUNCTION sessions.guard_memory_cleanup();

CREATE FUNCTION sessions.upload_cleanup_step(p_limit integer) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $cleanup_step$
DECLARE state sessions.upload_cleanup_rollout%ROWTYPE; r record; k jsonb; n integer:=0; budget integer; removed integer; edge record;
BEGIN
  IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 500 THEN RAISE EXCEPTION 'invalid cleanup batch size'; END IF;
  budget:=p_limit;
  SELECT * INTO state FROM sessions.upload_cleanup_rollout WHERE singleton FOR UPDATE;
  IF state.phase=5 THEN RETURN jsonb_build_object('complete',true,'phase',5,'processed',0); END IF;
  -- Each call handles at most one phase and at most budget primitive items.
  IF state.phase=0 THEN
    FOR r IN EXECUTE CASE WHEN cardinality(state.cursor)=0
      THEN 'SELECT * FROM sessions.delivery_origins ORDER BY org_id,workspace_id,origin_id LIMIT $1'
      ELSE 'SELECT * FROM sessions.delivery_origins WHERE (org_id,workspace_id,origin_id)>(($2::text[])[1],($2::text[])[2],($2::text[])[3]) ORDER BY org_id,workspace_id,origin_id LIMIT $1' END USING budget,state.cursor LOOP
      INSERT INTO sessions.upload_cleanup_origins VALUES(r.org_id,r.workspace_id,r.origin_id,'delivery:'||md5(jsonb_build_array(r.workspace_id,r.origin_id)::text));
      state.cursor:=ARRAY[r.org_id,r.workspace_id,r.origin_id]; n:=n+1;
    END LOOP;
  ELSIF state.phase=1 THEN
    FOR r IN EXECUTE CASE WHEN cardinality(state.cursor)=0
      THEN 'SELECT * FROM sessions.delivery_records ORDER BY org_id,workspace_id,origin_id,record_id LIMIT $1'
      ELSE 'SELECT * FROM sessions.delivery_records WHERE (org_id,workspace_id,origin_id,record_id)>(($2::text[])[1],($2::text[])[2],($2::text[])[3],($2::text[])[4]) ORDER BY org_id,workspace_id,origin_id,record_id LIMIT $1' END USING budget,state.cursor LOOP
      IF r.session_id IS NOT NULL THEN
        INSERT INTO sessions.uploaded_support_identities VALUES(r.org_id,r.workspace_id,r.user_id,r.origin_id,r.source,r.session_id,r.record_id) ON CONFLICT DO NOTHING;
      ELSIF r.operation='delete' THEN
        INSERT INTO sessions.upload_cleanup_tombstones VALUES(r.org_id,'delivery:'||md5(jsonb_build_array(r.workspace_id,r.origin_id)::text),r.source,'delivery:'||md5(jsonb_build_array(r.workspace_id,r.origin_id)::text)||':'||r.record_id,r.workspace_id,r.user_id,r.origin_id,r.record_id);
      END IF;
      state.cursor:=ARRAY[r.org_id,r.workspace_id,r.origin_id,r.record_id]; n:=n+1;
    END LOOP;
  ELSIF state.phase=2 THEN
    FOR r IN EXECUTE CASE WHEN cardinality(state.cursor)=0
      THEN 'SELECT * FROM sessions.convergence_events ORDER BY org_id,machine_id,source,session_id,kind,event_id LIMIT $1'
      ELSE 'SELECT * FROM sessions.convergence_events WHERE (org_id,machine_id,source,session_id,kind,event_id)>(($2::text[])[1],($2::text[])[2],($2::text[])[3],($2::text[])[4],($2::text[])[5],($2::text[])[6]) ORDER BY org_id,machine_id,source,session_id,kind,event_id LIMIT $1' END USING budget,state.cursor LOOP
      IF r.delivery_record_id IS NOT NULL THEN
        INSERT INTO sessions.uploaded_support_identities
          SELECT r.org_id,r.workspace_id,r.user_id,o.origin_id,r.source,r.session_id,r.delivery_record_id
          FROM sessions.upload_cleanup_origins o WHERE o.org_id=r.org_id AND o.machine_id=r.machine_id AND o.workspace_id=r.workspace_id
          ON CONFLICT DO NOTHING;
      END IF;
      state.cursor:=ARRAY[r.org_id,r.machine_id,r.source,r.session_id,r.kind,r.event_id]; n:=n+1;
    END LOOP;
  ELSIF state.phase=3 THEN
    -- A claim with arbitrarily many citations/edges uses inner durable cursors.
    -- The cursor limits citation count per transaction. JSONB access can
    -- detoast the entire stored claim on each step; the size preflight and
    -- statement timeout bound that cost but do not make it constant-time.
    WHILE n<budget LOOP
      IF state.item_id IS NULL THEN
        EXECUTE CASE WHEN cardinality(state.cursor)=0 THEN 'SELECT id FROM sessions.neighborhood_claims ORDER BY id LIMIT 1'
          ELSE 'SELECT id FROM sessions.neighborhood_claims WHERE id>($1::text[])[1] ORDER BY id LIMIT 1' END INTO state.item_id USING state.cursor;
        IF state.item_id IS NULL THEN EXIT; END IF;
        state.support_offset:=0;state.legacy:=false;state.item_stage:='support'; n:=n+1;
        IF n=budget THEN EXIT; END IF;
      END IF;
      SELECT * INTO r FROM sessions.neighborhood_claims WHERE id=state.item_id;
      IF NOT FOUND THEN state.cursor:=ARRAY[state.item_id];state.item_id:=NULL;n:=n+1;CONTINUE;END IF;
      IF jsonb_array_length(r.support_event_ids)>10000 OR octet_length(r.support_event_ids::text)>2097152 THEN
        RAISE EXCEPTION 'upload_cleanup_oversized_claim' USING ERRCODE='P0001';
      END IF;
      IF state.item_stage='support' AND state.support_offset<jsonb_array_length(r.support_event_ids) THEN
        k:=r.support_event_ids->state.support_offset;
        IF k->>'machineId' ~ '^delivery:[0-9a-f]{32}$' AND starts_with(k->>'eventId',(k->>'machineId')||':') THEN state.legacy:=true; END IF;
        -- Exact indexed lookup; at most one owner per projection key. Recover
        -- identity before any citation is lost, in this same checkpoint chain.
        INSERT INTO sessions.uploaded_support_identities
          SELECT t.org_id,t.workspace_id,t.user_id,t.origin_id,t.source,k->>'sessionId',t.record_id
          FROM sessions.upload_cleanup_tombstones t
          WHERE t.org_id=r.org_id AND t.machine_id=k->>'machineId' AND t.source=k->>'source' AND t.event_id=k->>'eventId'
            AND length(k->>'sessionId') BETWEEN 1 AND 512 ON CONFLICT DO NOTHING;
        state.support_offset:=state.support_offset+1;n:=n+1;
      ELSIF state.legacy THEN
        state.item_stage:='edges';
        removed:=0;
        FOR edge IN SELECT ctid FROM sessions.neighborhood_edges WHERE org_id=r.org_id AND claim_id=r.id LIMIT budget-n LOOP
          DELETE FROM sessions.neighborhood_edges WHERE ctid=edge.ctid;
          removed:=removed+1;
        END LOOP;
        n:=n+removed;
        IF removed=0 THEN
          DELETE FROM sessions.neighborhood_claims WHERE id=r.id;
          state.cursor:=ARRAY[r.id];state.item_id:=NULL;n:=n+1;
        END IF;
      ELSE
        state.cursor:=ARRAY[r.id];state.item_id:=NULL;n:=n+1;
      END IF;
    END LOOP;
  ELSIF state.phase=4 THEN
    WHILE n<budget LOOP
      IF state.item_id IS NULL THEN
        EXECUTE CASE WHEN cardinality(state.cursor)=0 THEN 'SELECT id FROM sessions.patterns ORDER BY id LIMIT 1'
          ELSE 'SELECT id FROM sessions.patterns WHERE id>($1::text[])[1] ORDER BY id LIMIT 1' END INTO state.item_id USING state.cursor;
        IF state.item_id IS NULL THEN EXIT; END IF;
        n:=n+1; IF n=budget THEN EXIT; END IF;
      END IF;
      SELECT * INTO r FROM sessions.patterns WHERE id=state.item_id;
      IF NOT FOUND THEN state.cursor:=ARRAY[state.item_id];state.item_id:=NULL;n:=n+1;CONTINUE;END IF;
      IF r.id ~ '^[0-9a-f]{64}$' AND r.kind='mistake' AND r.scope IN ('individual','team')
        AND ((r.scope='individual' AND r.subject_user_id IS NOT NULL) OR (r.scope='team' AND r.subject_user_id IS NULL))
        AND r.body IS NULL AND r.evidence='{}'::jsonb AND r.sample_size>=1
        AND starts_with(r.statement,'Reverted commit ') AND (starts_with(r.title,'Mistake: ') OR starts_with(r.title,'Reverted change in ')) THEN
        -- Bound feedback cleanup before deleting its pattern. The pending
        -- feedback guard rejects inserts/updates, permitting rollout deletes.
        removed:=0;
        FOR edge IN SELECT ctid FROM sessions.pattern_hits WHERE org_id=r.org_id AND pattern_id=r.id ORDER BY ts LIMIT budget-n LOOP
          DELETE FROM sessions.pattern_hits WHERE ctid=edge.ctid;
          removed:=removed+1;
        END LOOP;
        n:=n+removed;
        IF removed=0 THEN DELETE FROM sessions.patterns WHERE id=r.id;state.cursor:=ARRAY[r.id];state.item_id:=NULL;n:=n+1; END IF;
      ELSE state.cursor:=ARRAY[r.id];state.item_id:=NULL;n:=n+1;
      END IF;
    END LOOP;
  END IF;
  IF n=0 AND state.item_id IS NULL THEN state.phase:=state.phase+1;state.cursor:=ARRAY[]::text[]; END IF;
  UPDATE sessions.upload_cleanup_rollout SET phase=state.phase,cursor=state.cursor,item_id=state.item_id,support_offset=state.support_offset,legacy=state.legacy,item_stage=state.item_stage WHERE singleton;
  RETURN jsonb_build_object('complete',state.phase=5,'phase',state.phase,'processed',n);
END;
$cleanup_step$;
REVOKE ALL ON FUNCTION sessions.upload_cleanup_step(integer) FROM PUBLIC;
CREATE FUNCTION sessions.remember_uploaded_support_identity() RETURNS trigger LANGUAGE plpgsql AS $support_identity$
BEGIN
  IF TG_OP='UPDATE' AND (OLD.org_id,OLD.workspace_id,OLD.user_id,OLD.origin_id,OLD.source,OLD.session_id,OLD.record_id) IS NOT DISTINCT FROM (NEW.org_id,NEW.workspace_id,NEW.user_id,NEW.origin_id,NEW.source,NEW.session_id,NEW.record_id) THEN RETURN NEW; END IF;
  IF TG_OP IN ('UPDATE','DELETE') AND OLD.session_id IS NOT NULL THEN
    INSERT INTO sessions.uploaded_support_identities VALUES(OLD.org_id,OLD.workspace_id,OLD.user_id,OLD.origin_id,OLD.source,OLD.session_id,OLD.record_id) ON CONFLICT DO NOTHING;
  END IF;
  IF TG_OP IN ('INSERT','UPDATE') AND NEW.session_id IS NOT NULL THEN
    INSERT INTO sessions.uploaded_support_identities VALUES(NEW.org_id,NEW.workspace_id,NEW.user_id,NEW.origin_id,NEW.source,NEW.session_id,NEW.record_id) ON CONFLICT DO NOTHING;
  END IF;
  RETURN COALESCE(NEW,OLD);
END;
$support_identity$;
CREATE TRIGGER remember_uploaded_support_identity BEFORE INSERT OR UPDATE OR DELETE ON sessions.delivery_records
  FOR EACH ROW EXECUTE FUNCTION sessions.remember_uploaded_support_identity();

-- Shared SQL token: observed alongside candidate content, never supplied by a
-- model. Reject writeback if ownership or content changed during extraction.
CREATE FUNCTION sessions.memory_support_generation(p_user text,p_workspace text,p_content text,p_record jsonb,p_ts timestamptz)
RETURNS text LANGUAGE sql IMMUTABLE AS $support_generation$
  SELECT md5(jsonb_build_array(p_user,p_workspace,p_content,p_record,extract(epoch FROM p_ts))::text);
$support_generation$;
CREATE TABLE sessions.memory_claim_support_owners (
  org_id text NOT NULL, claim_id text NOT NULL REFERENCES sessions.neighborhood_claims(id) ON DELETE CASCADE,
  workspace_id text NOT NULL, user_id text NOT NULL,
  machine_id text NOT NULL, source text NOT NULL, session_id text NOT NULL, kind text NOT NULL, event_id text NOT NULL,
  generation text NOT NULL,
  PRIMARY KEY(org_id,claim_id,workspace_id,user_id,machine_id,source,session_id,kind,event_id,generation)
);
-- Pre-migration claims have no observed owner/generation. A current replacement
-- projection cannot prove their earlier attribution; never backfill that guess.

-- Extract 0037's sidechain-aware v2 activity projection into a callable row helper.
-- Settlement can project retained evidence without locking another origin's row:
-- updating that row while holding the workspace lock can deadlock a live upload.
-- A transaction's received_at is not its cross-origin acceptance order. Record
-- positions after the projection lock, without rewriting retained hot rows.
CREATE SEQUENCE sessions.delivery_projection_acceptance_seq START 1099511627776;
CREATE TABLE sessions.delivery_projection_acceptance (
  org_id text NOT NULL, workspace_id text NOT NULL,
  origin_id text NOT NULL, record_id text NOT NULL,
  accepted_seq bigint NOT NULL,
  historical boolean NOT NULL DEFAULT false,
  PRIMARY KEY (org_id,workspace_id,origin_id,record_id),
  FOREIGN KEY (org_id,workspace_id,origin_id,record_id)
    REFERENCES sessions.delivery_records(org_id,workspace_id,origin_id,record_id)
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE FUNCTION sessions.record_delivery_projection_acceptance(p_new sessions.delivery_records)
RETURNS void LANGUAGE plpgsql AS $acceptance$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || p_new.org_id || chr(31) || p_new.workspace_id, 0));
  -- Pre-ledger ordering is irrecoverable. Lazily seed only this record's old
  -- contenders, preserving its known current projection when available. Other
  -- historical survivors have a deterministic timestamp/origin fallback, not
  -- a claim of exact old arrival order. Live positions start above these seeds.
  INSERT INTO sessions.delivery_projection_acceptance
    (org_id,workspace_id,origin_id,record_id,accepted_seq,historical)
    SELECT r.org_id,r.workspace_id,r.origin_id,r.record_id,
      row_number() OVER (ORDER BY
        EXISTS (SELECT 1 FROM sessions.convergence_events e
          WHERE e.org_id=r.org_id AND e.workspace_id=r.workspace_id
            AND e.delivery_record_id=r.record_id
            AND e.machine_id='delivery:' || md5(jsonb_build_array(r.workspace_id,r.origin_id)::text)),
        r.received_at,r.origin_id), true
    FROM sessions.delivery_records r
    WHERE r.org_id=p_new.org_id AND r.workspace_id=p_new.workspace_id AND r.record_id=p_new.record_id
    ON CONFLICT (org_id,workspace_id,origin_id,record_id) DO NOTHING;
  INSERT INTO sessions.delivery_projection_acceptance
    (org_id,workspace_id,origin_id,record_id,accepted_seq,historical)
    VALUES (p_new.org_id,p_new.workspace_id,p_new.origin_id,p_new.record_id,
      nextval('sessions.delivery_projection_acceptance_seq'),false)
    ON CONFLICT (org_id,workspace_id,origin_id,record_id) DO UPDATE
      SET accepted_seq=excluded.accepted_seq,historical=false;
END;
$acceptance$;

CREATE FUNCTION sessions.project_delivery_activity_row(
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
  RETURN;
END;
$projection$;

CREATE OR REPLACE FUNCTION sessions.project_delivery_session_v2() RETURNS trigger LANGUAGE plpgsql AS $projection$
BEGIN
  -- Preserve the existing evidence-only expiry fast path: it touches no
  -- projection and must not wait on a workspace while holding evidence rows.
  IF NOT (TG_OP='UPDATE' AND NEW.kind IN ('session_lineage','turn_receipt')
      AND OLD.kind IN ('session_lineage','turn_receipt')) THEN
    PERFORM sessions.record_delivery_projection_acceptance(NEW);
  END IF;
  PERFORM sessions.project_delivery_activity_row(NEW, OLD, TG_OP);
  RETURN NEW;
END;
$projection$;

-- Keep only a consent fence after deleting this user's copy from this origin.
-- The same origin lock used by delivery makes deletion atomic with in-flight batches.
CREATE TABLE sessions.deleted_upload_sessions (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
  origin_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
  PRIMARY KEY (org_id, workspace_id, user_id, origin_id, source, session_id)
);
ALTER FUNCTION sessions.accept_delivery_batch(text,text,text,text,text,text,jsonb,jsonb)
  RENAME TO accept_delivery_batch_before_session_deletion;
CREATE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE allowed jsonb;
BEGIN
  PERFORM sessions.require_upload_cleanup_ready();
  PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  INSERT INTO sessions.delivery_origins VALUES (p_org,p_workspace,p_origin) ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
  SELECT COALESCE(jsonb_agg(r), '[]'::jsonb) INTO allowed
    FROM jsonb_array_elements(p_records) r
    WHERE NOT EXISTS (SELECT 1 FROM sessions.deleted_upload_sessions d
      WHERE d.org_id=p_org AND d.workspace_id=p_workspace AND d.user_id=p_user
        AND d.origin_id=p_origin AND d.source=r->>'source' AND d.session_id=r->>'session_id');
  -- Receipt identity remains tied to the original request, including suppressed records.
  RETURN sessions.accept_delivery_batch_before_session_deletion(p_org,p_workspace,p_user,p_origin,p_batch,p_digest,allowed,p_receipt);
END;
$delivery$;
-- Canonical reset identities include historical uploaded support whose accepted
-- tombstone omitted the session ID. The retained record owner and exact projected
-- citation key prove ownership; no offline/unseen or other-owner identity is inferred.
CREATE FUNCTION sessions.owned_uploaded_session_identities(p_org text,p_workspace text,p_user text)
RETURNS TABLE(origin_id text,source text,session_id text) LANGUAGE sql STABLE AS $uploaded_identities$
  SELECT DISTINCT r.origin_id,r.source,r.session_id FROM sessions.uploaded_support_identities r
    WHERE r.org_id=p_org AND r.workspace_id=p_workspace AND r.user_id=p_user AND r.session_id IS NOT NULL
  UNION
  SELECT DISTINCT r.origin_id,r.source,k->>'sessionId'
    FROM sessions.delivery_records r JOIN sessions.neighborhood_claims c ON c.org_id=r.org_id
      CROSS JOIN LATERAL jsonb_array_elements(c.support_event_ids) k
    WHERE r.org_id=p_org AND r.workspace_id=p_workspace AND r.user_id=p_user
      AND r.session_id IS NULL AND r.operation='delete'
      AND k->>'machineId'='delivery:' || md5(jsonb_build_array(r.workspace_id,r.origin_id)::text)
      AND k->>'source'=r.source
      AND k->>'eventId'='delivery:' || md5(jsonb_build_array(r.workspace_id,r.origin_id)::text) || ':' || r.record_id
      AND length(k->>'sessionId') BETWEEN 1 AND 512;
$uploaded_identities$;
CREATE FUNCTION sessions.set_session_uploads(
  p_org text, p_workspace text, p_user text, p_origin text, p_sessions jsonb, p_enabled boolean
) RETURNS void LANGUAGE plpgsql AS $selection$
DECLARE item jsonb; sid text; src text; had_copy boolean; removed_ids text[]; survivor sessions.delivery_records; owned_jobs uuid[]; removed_event_ids text[]; removed_claims text[]; removed_patterns text[];
BEGIN
  PERFORM sessions.require_upload_cleanup_ready();
  -- Admission uses this org-wide mutex before inserting a request. Hold it
  -- before origin/workspace locks so no new collector can enter the gap between
  -- locking existing requests and deleting their source. Reset uses this order too.
  IF NOT p_enabled THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  END IF;
  INSERT INTO sessions.delivery_origins VALUES (p_org,p_workspace,p_origin) ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
  FOR item IN SELECT value FROM jsonb_array_elements(p_sessions) LOOP
    src := item->>'source'; sid := item->>'session_id';
    IF p_enabled THEN
      DELETE FROM sessions.deleted_upload_sessions WHERE org_id=p_org AND workspace_id=p_workspace
        AND user_id=p_user AND origin_id=p_origin AND source=src AND session_id=sid;
    ELSE
      SELECT EXISTS(SELECT 1 FROM sessions.uploaded_support_identities WHERE org_id=p_org AND workspace_id=p_workspace
        AND origin_id=p_origin AND user_id=p_user AND source=src AND session_id=sid) INTO had_copy;
      INSERT INTO sessions.deleted_upload_sessions VALUES(p_org,p_workspace,p_user,p_origin,src,sid) ON CONFLICT DO NOTHING;
      SELECT array_agg(record_id) INTO removed_ids FROM sessions.delivery_records
        WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin AND user_id=p_user
          AND source=src AND session_id=sid;
      -- Projection IDs are stable per retained delivery record, including an
      -- earlier kind/tombstone whose projection is already gone. Derive only
      -- this caller's selected record IDs, preserving other users on the origin.
      SELECT array_agg('delivery:' || md5(jsonb_build_array(p_workspace,p_origin)::text) || ':' || r.record_id)
        INTO removed_event_ids FROM sessions.uploaded_support_identities r
        WHERE r.org_id=p_org AND r.workspace_id=p_workspace AND r.origin_id=p_origin AND r.user_id=p_user
          AND r.source=src AND r.session_id=sid;
      -- A tombstone may omit its session identity. Its retained owner/record ID
      -- still proves copy ownership; claim matching below also requires this sid.
      -- Update first so all projection triggers remove events, catalogs, rollups and caches.
      UPDATE sessions.delivery_records SET operation='delete',payload=NULL
        WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin AND user_id=p_user
          AND source=src AND session_id=sid;
      -- Keep the session fence, not payloads/revision records. Explicit re-enable backfills
      -- unchanged revisions using fresh batch IDs; stale receipts remain idempotent.
      DELETE FROM sessions.delivery_records WHERE org_id=p_org AND workspace_id=p_workspace
        AND origin_id=p_origin AND user_id=p_user AND source=src AND session_id=sid;
      -- Activity projection treats record IDs as shared across origins. Tombstoning
      -- one copy retracts that shared projection; replay the newest remaining statement (including tombstones)
      -- while holding the same workspace lock as delivery. Do not modify its timestamp.
      FOR survivor IN
        SELECT DISTINCT ON (r.record_id) r.*
        FROM sessions.delivery_records r
        LEFT JOIN sessions.delivery_projection_acceptance a
          ON (a.org_id,a.workspace_id,a.origin_id,a.record_id)=(r.org_id,r.workspace_id,r.origin_id,r.record_id)
        WHERE r.org_id=p_org AND r.workspace_id=p_workspace
          AND r.record_id=ANY(removed_ids)
        ORDER BY r.record_id,a.accepted_seq DESC NULLS LAST,r.received_at DESC,r.origin_id DESC
      LOOP
        PERFORM sessions.project_delivery_activity_row(survivor, NULL, 'INSERT');
      END LOOP;
      -- Derived summaries can retain transcript text from any removed copy.
      -- Invalidate affected dependencies while preserving independent copies.
      -- Claim identities/support merge at org scope. Remove the whole derived
      -- statement when it cites this exact deleted copy; pruning citations alone
      -- cannot remove leaked text. Independent claims/tenants remain intact.
      SELECT array_agg(c.id) INTO removed_claims FROM sessions.neighborhood_claims c
        WHERE c.org_id=p_org AND EXISTS (
          SELECT 1 FROM sessions.memory_claim_support_owners b WHERE b.org_id=p_org AND b.claim_id=c.id
            AND b.workspace_id=p_workspace AND b.user_id=p_user
            AND b.machine_id='delivery:' || md5(jsonb_build_array(p_workspace,p_origin)::text)
            AND b.source=src AND b.session_id=sid AND b.event_id=ANY(removed_event_ids));
      -- An exact retained citation -> caller-owned tombstone link also proves
      -- this historical uploaded copy when no session-qualified record remains.
      -- Reuse the same scoped cache/lease invalidation, never an unproven session.
      had_copy := had_copy OR COALESCE(array_length(removed_claims,1),0)>0;
      DELETE FROM sessions.neighborhood_edges WHERE org_id=p_org AND claim_id=ANY(removed_claims);
      DELETE FROM sessions.neighborhood_claims WHERE org_id=p_org AND id=ANY(removed_claims);
      DELETE FROM sessions.uploaded_support_identities WHERE org_id=p_org AND workspace_id=p_workspace
        AND user_id=p_user AND origin_id=p_origin AND source=src AND session_id=sid;
      IF had_copy THEN
        SELECT array_agg(p.id) INTO removed_patterns FROM sessions.patterns p WHERE p.org_id=p_org AND (
          EXISTS (SELECT 1 FROM sessions.reflex_pattern_events e WHERE e.org_id=p_org AND e.pattern_id=p.id
            AND e.workspace_id=p_workspace AND e.user_id=p_user AND e.machine_id='delivery:' || md5(jsonb_build_array(p_workspace,p_origin)::text)
            AND e.source=src AND e.session_id=sid AND e.event_id=ANY(removed_event_ids)));
        DELETE FROM sessions.pattern_hits WHERE org_id=p_org AND pattern_id=ANY(removed_patterns);
        DELETE FROM sessions.patterns WHERE org_id=p_org AND id=ANY(removed_patterns);
        -- session_outcomes belongs to independent legacy ingest, not delivery.
        -- Sharing a session/source/user does not prove ownership by this copy.
        -- Delivery commit associations are removed by their own projection path.
        DELETE FROM sessions.daily_digests WHERE org_id=p_org AND workspace_id=p_workspace;
        DELETE FROM sessions.memory_jobs WHERE org_id=p_org AND workspace_id=p_workspace
          AND machine_id='delivery:' || md5(jsonb_build_array(p_workspace,p_origin)::text) AND source=src AND session_id=sid;
      END IF;
      IF had_copy THEN
        -- A collector locks its request before creating/linking jobs. Wait for
        -- existing collectors before taking the job snapshot; after they commit,
        -- cleanup must see their new references and jobs, not a stale empty set.
        PERFORM 1 FROM sessions.session_analysis_requests
          WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND requested_by_user_id=p_user AND session_id=sid
          ORDER BY id FOR UPDATE;
        SELECT array_agg(id) INTO owned_jobs FROM sessions.session_analysis_jobs
          WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND source_provider=src
            AND requested_by_user_id=p_user AND session_id=sid;
        -- Source-less requests for a surviving provider may reference a removed
        -- provider's cache. Detach only this caller's references before cleanup.
        -- Invalidate affected requests rather than requeueing historical rows: the
        -- active unique key is org/session-wide and another request may be pending.
        -- A source-less running collector may already hold deleted evidence even
        -- before job linkage, so revoke this caller's in-flight lease as well.
        -- Normal future admission can request analysis of retained provider evidence.
        UPDATE sessions.session_analysis_requests SET
          analysis_job_id=CASE WHEN analysis_job_id=ANY(owned_jobs) THEN NULL ELSE analysis_job_id END,
          brief_job_id=CASE WHEN brief_job_id=ANY(owned_jobs) THEN NULL ELSE brief_job_id END,
          status='failed',source_revision=NULL,lease_token=NULL,lease_expires_at=NULL,
          last_error='Uploaded analysis source removed',updated_at=now()
          WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND requested_by_user_id=p_user
            AND session_id=sid AND (status='running' OR analysis_job_id=ANY(owned_jobs) OR brief_job_id=ANY(owned_jobs));
        -- Requests predate provider-qualified identities and carry no source.
        -- Preserve them while another source with the same ID still has evidence.
        IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND session_id=sid)
          AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND session_id=sid) THEN
          DELETE FROM sessions.session_analysis_requests WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND requested_by_user_id=p_user AND session_id=sid;
        END IF;
        -- A shared job is another requestor's retained analysis copy. Preserve
        -- its foreign-key targets; deleting this caller's primary upload must
        -- neither delete another requestor's request nor fail the whole reset.
        DELETE FROM sessions.session_analysis_jobs AS j WHERE j.id=ANY(owned_jobs)
          AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_requests r
            WHERE r.analysis_job_id=j.id OR r.brief_job_id=j.id);
        IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND source=src AND session_id=sid)
          AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND source=src AND session_id=sid) THEN
        DELETE FROM sessions.session_links WHERE org_id=p_org AND COALESCE(workspace_id,'')=p_workspace AND source=src AND session_id=sid;
        END IF;
      END IF;
    END IF;
  END LOOP;
END;
$selection$;

-- Neon HTTP cannot hold an interactive transaction. Keep live citation validation
-- and the claim write in one statement, serialized with deletion before row locks.
CREATE FUNCTION sessions.upsert_live_memory_claim(p_org text,p_workspace text,p_id text,p_subject text,p_claim jsonb,p_now timestamptz)
RETURNS boolean LANGUAGE plpgsql AS $memory_claim$
DECLARE written integer;
BEGIN
  IF NOT sessions.upload_cleanup_ready() THEN RETURN false; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  IF jsonb_array_length(p_claim->'support')=0 OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_claim->'support') k WHERE NOT EXISTS (
      SELECT 1 FROM sessions.convergence_events e WHERE e.org_id=p_org
        AND e.machine_id=k->>'machineId' AND e.source=k->>'source' AND e.session_id=k->>'sessionId'
        AND e.kind=k->>'kind' AND e.event_id=k->>'eventId'
        AND sessions.memory_support_generation(e.user_id,e.workspace_id,e.content,e.record,e.ts)=
          p_claim->'observedGenerations'->>concat_ws(chr(1),k->>'machineId',k->>'source',k->>'sessionId',k->>'kind',k->>'eventId'))) THEN RETURN false; END IF;
  INSERT INTO sessions.neighborhood_claims
    (id,org_id,workspace_id,source_project,subject_project,kind,statement,impact_on_subject,
     entities,evidence_class,confidence,status,valid_from,observed_from,last_confirmed_at,
     support_event_ids,extractor_version,profile_version)
    VALUES(p_id,p_org,p_workspace,p_claim->>'sourceProject',p_subject,p_claim->>'kind',p_claim->>'statement',p_claim->>'impactOnSubject',
      COALESCE(p_claim->'entities','[]'),COALESCE(p_claim->>'evidenceClass','observed'),COALESCE((p_claim->>'confidence')::double precision,0.5),'candidate',
      (p_claim->>'validFrom')::timestamptz,(p_claim->>'observedFrom')::timestamptz,p_now,
      p_claim->'support',p_claim->>'extractorVersion',p_claim->>'profileVersion')
    ON CONFLICT(id) DO UPDATE SET
      support_event_ids=(SELECT COALESCE(jsonb_agg(DISTINCT elem),'[]'::jsonb)
        FROM jsonb_array_elements(sessions.neighborhood_claims.support_event_ids || excluded.support_event_ids) elem),
      last_confirmed_at=p_now,updated_at=p_now
    WHERE sessions.neighborhood_claims.org_id=p_org;
  GET DIAGNOSTICS written=ROW_COUNT;
  IF written>0 THEN
    -- Bind only this validated incoming citation set; retained older bindings
    -- must not acquire a replacement owner's attribution on a later merge.
    INSERT INTO sessions.memory_claim_support_owners
      SELECT DISTINCT p_org,p_id,e.workspace_id,e.user_id,e.machine_id,e.source,e.session_id,e.kind,e.event_id,
        sessions.memory_support_generation(e.user_id,e.workspace_id,e.content,e.record,e.ts)
      FROM jsonb_array_elements(p_claim->'support') k JOIN sessions.convergence_events e
        ON e.org_id=p_org AND e.machine_id=k->>'machineId' AND e.source=k->>'source'
          AND e.session_id=k->>'sessionId' AND e.kind=k->>'kind' AND e.event_id=k->>'eventId'
      ON CONFLICT DO NOTHING;
  END IF;
  RETURN written>0;
END;
$memory_claim$;

-- Reflex patterns retain both outcome-only and event-derived text. Private full
-- provenance is separate from the bounded public display citation list.
CREATE FUNCTION sessions.reflex_outcome_generation(p_row sessions.session_outcomes)
RETURNS text LANGUAGE sql IMMUTABLE AS $reflex_outcome_generation$
  SELECT md5(((to_jsonb(p_row)-'shipped_at'-'reverted_at'-'created_at'-'ingested_at') ||
    jsonb_build_object('shipped_at',extract(epoch FROM p_row.shipped_at),'reverted_at',extract(epoch FROM p_row.reverted_at),
      'created_at',extract(epoch FROM p_row.created_at),'ingested_at',extract(epoch FROM p_row.ingested_at)))::text);
$reflex_outcome_generation$;
CREATE FUNCTION sessions.reflex_event_generation(p_row sessions.convergence_events)
RETURNS text LANGUAGE sql IMMUTABLE AS $reflex_event_generation$
  SELECT md5(jsonb_build_array(sessions.memory_support_generation(p_row.user_id,p_row.workspace_id,p_row.content,p_row.record,p_row.ts),
    p_row.task_title,p_row.project_id)::text);
$reflex_event_generation$;
CREATE TABLE sessions.reflex_pattern_outcomes (
  org_id text NOT NULL,pattern_id text NOT NULL REFERENCES sessions.patterns(id) ON DELETE CASCADE,
  workspace_id text NOT NULL,user_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,commit_sha text NOT NULL,generation text NOT NULL,
  PRIMARY KEY(org_id,pattern_id,workspace_id,user_id,source,session_id,commit_sha,generation)
);
CREATE TABLE sessions.reflex_pattern_events (
  org_id text NOT NULL,pattern_id text NOT NULL REFERENCES sessions.patterns(id) ON DELETE CASCADE,
  workspace_id text NOT NULL,user_id text NOT NULL,machine_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,kind text NOT NULL,event_id text NOT NULL,generation text NOT NULL,
  PRIMARY KEY(org_id,pattern_id,workspace_id,user_id,machine_id,source,session_id,kind,event_id,generation)
);
CREATE FUNCTION sessions.upsert_reflex_pattern(p_org text,p_id text,p_pattern jsonb)
RETURNS boolean LANGUAGE plpgsql AS $reflex_pattern$
DECLARE accepted boolean;
BEGIN
  IF NOT sessions.upload_cleanup_ready() THEN RETURN false; END IF;
  -- Obtain the mutex before the validation/write snapshot, so a learner waiting
  -- for deletion cannot validate its pre-wait source rows.
  PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  WITH valid AS MATERIALIZED (
    SELECT 1 WHERE jsonb_array_length(p_pattern->'outcomes')>0
      AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_pattern->'outcomes') k WHERE NOT EXISTS (
        SELECT 1 FROM sessions.session_outcomes o WHERE o.org_id=p_org AND o.workspace_id=k->>'workspaceId'
          AND o.user_id=k->>'userId' AND o.source=k->>'source' AND o.session_id=k->>'sessionId' AND o.commit_sha=k->>'commitSha'
          AND sessions.reflex_outcome_generation(o)=k->>'generation'))
      AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_pattern->'events') k WHERE NOT EXISTS (
        SELECT 1 FROM sessions.convergence_events e WHERE e.org_id=p_org AND e.workspace_id=k->>'workspaceId' AND e.user_id=k->>'userId'
          AND e.machine_id=k->>'machineId' AND e.source=k->>'source' AND e.session_id=k->>'sessionId' AND e.kind=k->>'kind' AND e.event_id=k->>'eventId'
          AND sessions.reflex_event_generation(e)=k->>'generation'))
  ), written AS (
    INSERT INTO sessions.patterns(id,org_id,workspace_id,scope,subject_user_id,project_id,kind,title,statement,sample_size,confidence_basis_points,
      support_event_ids,evidence,status,first_observed_at,last_observed_at,created_at,updated_at)
      SELECT p_id,p_org,p_pattern->>'workspaceId',p_pattern->>'scope',p_pattern->>'subjectUserId',p_pattern->>'projectId','mistake',
        p_pattern->>'title',p_pattern->>'statement',(p_pattern->>'sampleSize')::integer,(p_pattern->>'confidence')::integer,
        p_pattern->'supportEventIds','{}'::jsonb,p_pattern->>'status',(p_pattern->>'firstObservedAt')::timestamptz,
        (p_pattern->>'lastObservedAt')::timestamptz,now(),now() FROM valid
      ON CONFLICT(id) DO UPDATE SET workspace_id=excluded.workspace_id,scope=excluded.scope,subject_user_id=excluded.subject_user_id,
        project_id=excluded.project_id,title=excluded.title,statement=excluded.statement,sample_size=excluded.sample_size,
        confidence_basis_points=excluded.confidence_basis_points,support_event_ids=excluded.support_event_ids,evidence=excluded.evidence,
        status=excluded.status,first_observed_at=excluded.first_observed_at,last_observed_at=excluded.last_observed_at,updated_at=excluded.updated_at
      WHERE sessions.patterns.org_id=p_org RETURNING id
  ) SELECT EXISTS(SELECT 1 FROM written) INTO accepted;
  IF accepted THEN
    -- The accepted row replaces the pattern's evidence, rather than merging it.
    -- Replace both private sets under the same mutex and transaction; rejected
    -- learner inputs must leave the existing pattern and bindings untouched.
    DELETE FROM sessions.reflex_pattern_outcomes WHERE org_id=p_org AND pattern_id=p_id;
    DELETE FROM sessions.reflex_pattern_events WHERE org_id=p_org AND pattern_id=p_id;
    INSERT INTO sessions.reflex_pattern_outcomes
      SELECT DISTINCT p_org,p_id,k->>'workspaceId',k->>'userId',k->>'source',k->>'sessionId',k->>'commitSha',k->>'generation'
      FROM jsonb_array_elements(p_pattern->'outcomes') k;
    INSERT INTO sessions.reflex_pattern_events
      SELECT DISTINCT p_org,p_id,k->>'workspaceId',k->>'userId',k->>'machineId',k->>'source',k->>'sessionId',k->>'kind',k->>'eventId',k->>'generation'
      FROM jsonb_array_elements(p_pattern->'events') k;
  END IF;
  RETURN accepted;
END;
$reflex_pattern$;

-- Existing admission contracts, with evidence checked under their existing mutex.
-- Unchanged v1 contract; the in-flight check follows the projection above and the
-- daily budget counts distinct sessions, shared with session_brief_v2.
CREATE OR REPLACE FUNCTION sessions.request_session_analysis_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request_analysis$
DECLARE
  existing sessions.session_analysis_requests%ROWTYPE;
  active record;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  -- Check evidence only after deletion/admission serialization has settled.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  SELECT * INTO active FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
   WHERE status IN ('queued', 'running')
   ORDER BY request_created_at DESC, request_id DESC LIMIT 1;
  IF active.request_id IS NOT NULL THEN
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', active.request_id, 'sessionId', active.session_id, 'status', active.status,
      'analysisId', active.job_id, 'error', NULL,
      'createdAt', active.request_created_at, 'updatedAt', active.request_updated_at));
  END IF;

  -- The per-session limits shared with session_brief_v2: a throttled request
  -- returns the session's latest request.
  IF sessions.session_request_limit(p_org_id, p_session_id) = 'throttled' THEN
    SELECT * INTO active FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
     ORDER BY request_created_at DESC, request_id DESC LIMIT 1;
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', active.request_id, 'sessionId', active.session_id, 'status', active.status,
      'analysisId', active.job_id, 'error', active.error,
      'createdAt', active.request_created_at, 'updatedAt', active.request_updated_at));
  END IF;
  IF sessions.session_request_limit(p_org_id, p_session_id) = 'capped'
     OR sessions.sessions_admitted_today(p_org_id, p_session_id) >= 20 THEN
    RETURN jsonb_build_object('outcome', 'rate_limited');
  END IF;

  INSERT INTO sessions.session_analysis_requests (org_id, workspace_id, session_id, requested_by_user_id)
    VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id)
    RETURNING * INTO existing;
  RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
    'id', existing.id, 'sessionId', existing.session_id, 'status', existing.status,
    'analysisId', NULL, 'error', NULL,
    'createdAt', existing.created_at, 'updatedAt', existing.updated_at));
END;
$request_analysis$;
-- Preserve the six-argument admission API; read-only project workers use the
-- explicit seventh argument and cannot fan out additional model jobs.
CREATE OR REPLACE FUNCTION sessions.session_brief_v2(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text,
  p_brief_engine text, p_analysis_engine text, p_admit boolean
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_brief$
DECLARE
  current_revision text;
  engine text;
  brief record;
  analysis record;
  brief_fresh boolean;
  analysis_fresh boolean;
  latest sessions.session_analysis_requests%ROWTYPE;
  brief_job sessions.session_analysis_jobs%ROWTYPE;
  analysis_job sessions.session_analysis_jobs%ROWTYPE;
  brief_busy boolean;
  analysis_busy boolean;
  brief_due boolean;
  analysis_due boolean;
  request_limit text;
  recent_failure text;
  exhausted boolean;
  retryable boolean;
  admitted_id uuid;
  limited boolean := false;
  request_status text;
  brief_error text;
  analysis_error text;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = ''
    OR p_brief_engine IS NULL OR btrim(p_brief_engine) = ''
    OR p_analysis_engine IS NULL OR btrim(p_analysis_engine) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  -- Check evidence only after deletion/admission serialization has settled.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  engine := p_brief_engine || '#' || p_analysis_engine;
  -- Shared with request_session_analysis_v1 so both admission paths serialize.
  current_revision := sessions.current_session_revision(p_org_id, p_session_id);

  SELECT * INTO brief FROM sessions.cached_session_job(p_org_id, p_session_id, 'brief');
  SELECT * INTO analysis FROM sessions.cached_session_job(p_org_id, p_session_id, 'analysis');
  brief_fresh := brief.job_id IS NOT NULL AND brief.revision IS NOT DISTINCT FROM current_revision
    AND brief.engine = p_brief_engine;
  analysis_fresh := analysis.job_id IS NOT NULL AND analysis.revision IS NOT DISTINCT FROM current_revision
    AND analysis.engine = p_analysis_engine;

  SELECT * INTO latest FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;
  SELECT * INTO brief_job FROM sessions.session_analysis_jobs
   WHERE id = latest.brief_job_id AND org_id = p_org_id AND session_id = p_session_id;
  SELECT * INTO analysis_job FROM sessions.session_analysis_jobs
   WHERE id = latest.analysis_job_id AND org_id = p_org_id AND session_id = p_session_id;
  -- Busy means any job of that kind is in flight for the session, not only the
  -- latest request's, so a deferred analysis is not re-admitted on every read.
  brief_busy := COALESCE(latest.status IN ('queued', 'running'), false)
    OR EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                WHERE org_id = p_org_id AND session_id = p_session_id
                  AND kind = 'brief' AND status IN ('queued', 'running'));
  analysis_busy := COALESCE(latest.status IN ('queued', 'running'), false)
    OR EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                WHERE org_id = p_org_id AND session_id = p_session_id
                  AND kind = 'analysis' AND status IN ('queued', 'running'));

  exhausted := EXISTS (SELECT 1 FROM sessions.session_analysis_admissions
                        WHERE org_id = p_org_id AND day = (now() AT TIME ZONE 'UTC')::date
                          AND requests >= 20);

  -- Another attempt for the same revision and engine is made only when it can
  -- change the result: a transient failure after a cooldown, a failed job that
  -- still has its single requeue, a deferred analysis, or a refused analysis once
  -- model admission has room again.
  retryable := COALESCE(latest.id IS NULL
    OR latest.source_revision IS DISTINCT FROM current_revision
    -- The claim records the handling worker's engine, so every finished request
    -- has one; a request from before 0024 retries once under the current engine.
    OR latest.engine IS DISTINCT FROM engine
    OR (latest.status = 'failed'
        AND latest.last_error IN ('session_collection_failed', 'request_lease_expired')
        AND latest.updated_at <= now() - interval '1 hour')
    OR (latest.status = 'resolved' AND (
          (brief_job.status = 'failed' AND brief_job.requeues < 1)
       OR (analysis_job.status = 'failed' AND analysis_job.requeues < 1)
       OR (latest.analysis_job_id IS NULL AND latest.last_error IS NULL)
       OR (latest.analysis_job_id IS NULL AND NOT exhausted
           AND latest.updated_at <= now() - interval '1 hour'))), false);

  -- Each kind is judged on its own: a slow analysis never holds back a brief. An
  -- analysis starts at most once per 30 minutes per session and never while the
  -- org's model admission is exhausted.
  brief_due := NOT brief_fresh AND NOT brief_busy;
  analysis_due := NOT analysis_fresh AND NOT analysis_busy AND NOT exhausted
    AND NOT sessions.session_analysis_cooling(p_org_id, p_session_id);
  -- A throttled read returns the stale result with a non-polling status.
  request_limit := sessions.session_request_limit(p_org_id, p_session_id);

  IF p_admit AND (brief_due OR analysis_due) AND retryable AND request_limit IS DISTINCT FROM 'throttled' THEN
    IF request_limit = 'capped' OR sessions.sessions_admitted_today(p_org_id, p_session_id) >= 20 THEN
      limited := true;
    ELSE
      INSERT INTO sessions.session_analysis_requests
        (org_id, workspace_id, session_id, requested_by_user_id, source_revision, engine)
        VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, current_revision, engine)
        RETURNING * INTO latest;
      admitted_id := latest.id;
      brief_job := NULL;
      analysis_job := NULL;
      -- Only the kinds that were due are now in flight; the other keeps its state.
      brief_busy := brief_busy OR brief_due;
      analysis_busy := analysis_busy OR analysis_due;
    END IF;
  END IF;

  request_status := CASE
    WHEN latest.status <> 'resolved' THEN latest.status
    WHEN brief_job.status = 'failed' OR analysis_job.status = 'failed' THEN 'failed'
    WHEN brief_job.status = 'running' OR analysis_job.status = 'running' THEN 'running'
    WHEN brief_job.status = 'queued' OR analysis_job.status = 'queued' THEN 'queued'
    ELSE 'completed' END;
  brief_error := CASE WHEN latest.status = 'failed' THEN latest.last_error
                      WHEN brief_job.status = 'failed' THEN brief_job.last_error END;
  -- An analysis deferred by a failed run's cooldown reports that failure.
  SELECT last_error INTO recent_failure FROM sessions.session_analysis_jobs
   WHERE org_id = p_org_id AND session_id = p_session_id AND kind = 'analysis'
     AND last_error IS DISTINCT FROM 'superseded' AND status IN ('completed', 'failed')
   ORDER BY COALESCE(completed_at, updated_at) DESC, id DESC LIMIT 1;
  analysis_error := CASE WHEN latest.status = 'failed' THEN latest.last_error
                         WHEN analysis_job.status = 'failed' THEN analysis_job.last_error
                         WHEN latest.status = 'resolved' AND latest.analysis_job_id IS NULL
                           THEN COALESCE(latest.last_error, recent_failure) END;

  RETURN jsonb_build_object(
    'outcome', 'ok',
    'sessionId', p_session_id,
    'admittedRequestId', admitted_id,
    'status', CASE
      WHEN brief_fresh THEN 'ready'
      WHEN brief_busy THEN CASE WHEN brief.job_id IS NULL THEN 'pending' ELSE 'refreshing' END
      WHEN limited THEN 'rate_limited'
      WHEN brief_error IS NOT NULL OR brief.job_id IS NULL THEN 'failed'
      ELSE 'stale' END,
    'stale', brief.job_id IS NOT NULL AND NOT brief_fresh,
    'briefId', brief.job_id,
    'brief', brief.result,
    'generatedAt', brief.completed_at,
    'error', CASE WHEN brief_fresh THEN NULL ELSE brief_error END,
    'analysis', jsonb_build_object(
      'status', CASE
        WHEN analysis_fresh THEN 'ready'
        WHEN analysis_busy THEN CASE WHEN analysis.job_id IS NULL THEN 'pending' ELSE 'refreshing' END
        WHEN limited OR exhausted OR analysis_error = 'session_analysis_rate_limited' THEN 'rate_limited'
        WHEN analysis_error IS NOT NULL THEN 'failed'
        WHEN analysis.job_id IS NULL THEN 'pending'
        ELSE 'stale' END,
      'stale', analysis.job_id IS NOT NULL AND NOT analysis_fresh,
      'id', analysis.job_id,
      'result', analysis.result,
      'generatedAt', analysis.completed_at,
      'error', CASE WHEN analysis_fresh THEN NULL ELSE analysis_error END),
    'request', CASE WHEN latest.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', latest.id, 'sessionId', latest.session_id, 'status', request_status,
      'analysisId', latest.analysis_job_id,
      'error', COALESCE(brief_error, analysis_error),
      'createdAt', latest.created_at, 'updatedAt', latest.updated_at) END);
END;
$session_brief$;
