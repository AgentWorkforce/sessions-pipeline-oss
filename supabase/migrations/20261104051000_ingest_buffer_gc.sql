-- Durable buffered-object redaction leases. Transcript bytes stay in S3.
--040500 owns intent/outbox admission. This migration never grants direct tables.
ALTER TABLE sessions.ingest_stage_intents
 ADD COLUMN gc_lease_token uuid,
 ADD COLUMN gc_lease_until timestamptz,
 ADD COLUMN gc_lease_version bigint,
 ADD COLUMN gc_generation text CHECK(gc_generation IS NULL OR gc_generation ~ '^[0-9a-f]{64}$'),
 ADD COLUMN gc_checked_at timestamptz,
 ADD COLUMN gc_keep_groups jsonb,
 ADD COLUMN gc_replacement_stage_token uuid REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
 ADD COLUMN gc_barrier_version_id text,
 ADD COLUMN gc_outbox_version bigint,
 ADD CONSTRAINT ingest_stage_gc_lease_shape CHECK(
  (gc_lease_token IS NULL AND gc_lease_until IS NULL AND gc_lease_version IS NULL) OR
  (gc_lease_token IS NOT NULL AND gc_lease_until IS NOT NULL AND gc_lease_version IS NOT NULL AND gc_lease_version>0)),
 ADD CONSTRAINT ingest_stage_gc_groups CHECK(gc_keep_groups IS NULL OR jsonb_typeof(gc_keep_groups)='array');
CREATE INDEX ingest_stage_gc_scan_idx ON sessions.ingest_stage_intents(
 coalesce(gc_checked_at,created_at),stage_token) WHERE cleanup_state<>'redacted';

-- Caller already owns user/batch/intent locks. This helper never invents a fresh
-- generation. All rows reference original claim ordinals and original epochs.
CREATE FUNCTION sessions.ingest_buffer_gc_kept(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $kept$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 v_reset bigint;v_records jsonb;v_pairs jsonb;v_fenced jsonb;g record;o jsonb;r jsonb;
 v_keep jsonb:='[]';v_live int;v_total int;v_count int;v_distinct int;
 v_source text;v_session text;v_pair jsonb;
BEGIN
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 IF ROW(a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest,a.reset_epoch)
   IS DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest,s.reset_epoch)
   OR jsonb_typeof(a.records) IS DISTINCT FROM 'array' OR jsonb_array_length(a.records)>500
   OR jsonb_typeof(s.group_ordinals) IS DISTINCT FROM 'array' OR jsonb_array_length(s.group_ordinals) NOT BETWEEN 1 AND 500
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: original authority malformed';END IF;
 SELECT count(*),count(DISTINCT x->>'ord'),coalesce(jsonb_object_agg(x->>'ord',x),'{}')
 INTO v_count,v_distinct,v_records FROM jsonb_array_elements(a.records)x;
 IF v_count<>v_distinct THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: duplicate original ordinal';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(s.group_ordinals)x
   WHERE jsonb_typeof(x) IS DISTINCT FROM 'array' OR jsonb_array_length(x) NOT BETWEEN 1 AND 500)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: group malformed';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(s.group_ordinals)gg CROSS JOIN LATERAL jsonb_array_elements(gg)x
   WHERE jsonb_typeof(x) IS DISTINCT FROM 'number' OR x::text!~'^(0|[1-9][0-9]{0,2})$'
    OR NOT(v_records ? x::text))
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: unknown original ordinal';END IF;
 SELECT count(*),count(DISTINCT x::text) INTO v_count,v_distinct
 FROM jsonb_array_elements(s.group_ordinals)gg CROSS JOIN LATERAL jsonb_array_elements(gg)x;
 IF v_count<>v_distinct OR v_count>500
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: duplicate group ordinal';END IF;
 SELECT count(*) INTO v_reset FROM sessions.upload_resets
 WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND user_id=s.user_id;
 IF v_reset<>s.reset_epoch THEN RETURN '[]';END IF;
 SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_array(x->>'source',x->>'session_id')),'[]') INTO v_pairs
 FROM jsonb_array_elements(a.records)x WHERE x->>'session_id' IS NOT NULL;
 v_fenced:=sessions.ingest_attempt_fenced(s.org_id,s.workspace_id,s.user_id,s.origin_id,a.epochs,v_pairs);
 FOR g IN SELECT value,ordinality-1 AS idx FROM jsonb_array_elements(s.group_ordinals) WITH ORDINALITY LOOP
  v_live:=0;v_total:=0;v_pair:=NULL;
  FOR o IN SELECT value FROM jsonb_array_elements(g.value) LOOP
   r:=v_records->(o::text);
   IF v_pair IS NULL THEN v_pair:=jsonb_build_array(r->>'source',r->>'session_id');
   ELSIF v_pair IS DISTINCT FROM jsonb_build_array(r->>'source',r->>'session_id')
    AND NOT(v_pair->>1 IS NULL AND r->>'session_id' IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: group crosses source session';
   END IF;
   v_total:=v_total+1;
   IF r->>'session_id' IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced)x
      WHERE x->>0=r->>'source' AND x->>1=r->>'session_id') THEN v_live:=v_live+1;END IF;
  END LOOP;
  IF v_live=v_total THEN v_keep:=v_keep||to_jsonb(g.idx);
  ELSIF v_live<>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: mixed group cannot be dropped';END IF;
 END LOOP;
 RETURN v_keep;
END $kept$;

-- Every multirow operation takes USER FENCE, BATCH, then INTENT/OUTBOX. The first
-- identity read is unlocked and repeated after the locks. No stage→user wait.
CREATE FUNCTION sessions.ingest_buffer_gc_locked(p_stage uuid,p_lease uuid)
RETURNS sessions.ingest_stage_intents
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $locked$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;v_user text;v_org text;v_ws text;v_generation text;
BEGIN
 SELECT org_id,workspace_id,user_id,gc_generation INTO v_org,v_ws,v_user,v_generation FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 IF v_generation IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: unknown lease generation';END IF;
 PERFORM sessions.ingest_require_generation(v_generation);
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: unknown ticket';END IF;
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 PERFORM 1 FROM sessions.ingest_batches WHERE org_id=v_org AND workspace_id=v_ws
  AND origin_id=(SELECT origin_id FROM sessions.ingest_stage_intents WHERE stage_token=p_stage)
  AND batch_id=(SELECT batch_id FROM sessions.ingest_stage_intents WHERE stage_token=p_stage) FOR UPDATE;
 SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage FOR UPDATE;
 IF NOT FOUND OR ROW(s.org_id,s.workspace_id,s.user_id) IS DISTINCT FROM ROW(v_org,v_ws,v_user)
  OR s.gc_generation IS DISTINCT FROM v_generation OR p_lease IS NULL OR s.gc_lease_token IS DISTINCT FROM p_lease OR s.gc_lease_version IS DISTINCT FROM s.row_version
  OR s.gc_lease_until IS NULL OR clock_timestamp()>=s.gc_lease_until OR s.cleanup_state='redacted'
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: stale lease';END IF;
 RETURN s;
END $locked$;

CREATE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE v_limit int;v_generation text;candidate record;s sessions.ingest_stage_intents%ROWTYPE;
 child sessions.ingest_stage_intents%ROWTYPE;parent sessions.ingest_stage_intents%ROWTYPE;
 v_keep jsonb;v_phase text;v_ticket jsonb;v_tickets jsonb:='[]';v_has_outbox boolean;v_abort boolean;v_token uuid;v_outbox_version bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 -- Marker first: metadata operations never wait for it while holding user/table
 -- locks. The migrator's PENDING update waits for this short operation to finish.
 PERFORM sessions.ingest_require_generation(v_generation);
 v_limit:=sessions.ingest_bounded(p->'limit',1,100);
 FOR candidate IN SELECT stage_token,org_id,workspace_id,user_id,origin_id,batch_id
  FROM sessions.ingest_stage_intents
  WHERE cleanup_state<>'redacted' AND (gc_lease_until IS NULL OR gc_lease_until<=clock_timestamp())
  ORDER BY coalesce(gc_checked_at,created_at),stage_token LIMIT v_limit*4
 LOOP
  PERFORM sessions.ingest_lock_fences(candidate.org_id,candidate.workspace_id,candidate.user_id);
  PERFORM 1 FROM sessions.ingest_batches WHERE org_id=candidate.org_id AND workspace_id=candidate.workspace_id
   AND origin_id=candidate.origin_id AND batch_id=candidate.batch_id FOR UPDATE;
  SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=candidate.stage_token FOR UPDATE SKIP LOCKED;
  IF NOT FOUND OR s.cleanup_state='redacted' OR s.gc_lease_until>clock_timestamp() THEN CONTINUE;END IF;
  IF ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id) IS DISTINCT FROM
    ROW(candidate.org_id,candidate.workspace_id,candidate.user_id,candidate.origin_id,candidate.batch_id)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: identity changed';END IF;
  -- A prospective child cannot be independently aborted during its parent's copy.
  IF s.parent_stage_token IS NOT NULL THEN
   SELECT * INTO parent FROM sessions.ingest_stage_intents WHERE stage_token=s.parent_stage_token;
   -- Prospective bytes have a deterministic key. They remain owned through
   -- repeated parent crashes; retiring them merely because a lease expired would
   -- make that same replacement key permanently unusable after its barrier.
   IF parent.gc_replacement_stage_token=s.stage_token AND parent.cleanup_state='live' AND parent.receipt_state='acknowledged'
    AND ROW(parent.org_id,parent.workspace_id,parent.user_id,parent.origin_id,parent.batch_id,parent.claim_token,parent.digest)
      IS NOT DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.claim_token,s.digest)
    AND EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=parent.org_id AND o.workspace_id=parent.workspace_id
      AND o.user_id=parent.user_id AND o.origin_id=parent.origin_id AND o.batch_id=parent.batch_id AND o.digest=parent.digest
      AND o.state='buffered' AND o.active_stage_token=parent.stage_token)
   THEN
    UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
    CONTINUE;
   END IF;
  END IF;
  PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
   AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
  SELECT row_version INTO v_outbox_version FROM sessions.ingest_buffer_outbox WHERE active_stage_token=s.stage_token;
  v_has_outbox:=FOUND;
  v_abort:=false;
  IF s.cleanup_state='retired' THEN
   IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: current object cannot be cleaned';END IF;
   v_keep:='[]';v_phase:='cleanup';
  ELSE
   v_keep:=sessions.ingest_buffer_gc_kept(s.stage_token);
   IF s.receipt_state='preparing' THEN
    IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: preparing current object';END IF;
    v_abort:=s.expires_at<=clock_timestamp() OR jsonb_array_length(v_keep)<>jsonb_array_length(s.group_ordinals);
    IF NOT v_abort THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     CONTINUE;
    END IF;
    v_keep:='[]';v_phase:='cleanup';
   ELSE
    IF NOT v_has_outbox THEN
     -- Acknowledged but unreferenced intent is a known retired object.
     v_abort:=true;v_keep:='[]';v_phase:='cleanup';
    ELSIF jsonb_array_length(v_keep)=jsonb_array_length(s.group_ordinals) THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     CONTINUE;
    ELSE v_phase:='plan';END IF;
   END IF;
  END IF;
  -- A stale prospective child is never adopted into another keep set.
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token FOR UPDATE;
   IF NOT FOUND OR child.cleanup_state<>'live' OR child.group_ordinals IS DISTINCT FROM
     (SELECT coalesce(jsonb_agg(s.group_ordinals->(x::text)::int ORDER BY ordinality),'[]')
        FROM jsonb_array_elements(v_keep) WITH ORDINALITY q(x,ordinality))
   THEN
    IF FOUND AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE active_stage_token=child.stage_token) THEN
     UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1,
      gc_lease_token=NULL,gc_lease_until=NULL,gc_lease_version=NULL WHERE stage_token=child.stage_token;
    END IF;
    s.gc_replacement_stage_token:=NULL;
   END IF;
  END IF;
  v_token:=gen_random_uuid();
  UPDATE sessions.ingest_stage_intents SET gc_generation=v_generation,gc_lease_token=v_token,gc_lease_until=clock_timestamp()+interval '5 minutes',
   row_version=row_version+1,gc_lease_version=row_version+1,gc_checked_at=clock_timestamp(),gc_keep_groups=v_keep,
   gc_replacement_stage_token=s.gc_replacement_stage_token,gc_outbox_version=v_outbox_version,
   cleanup_state=CASE WHEN v_abort THEN 'retired' ELSE cleanup_state END
   WHERE stage_token=s.stage_token RETURNING * INTO s;
  v_ticket:=jsonb_build_object('stageToken',s.stage_token,'claimToken',s.claim_token,'leaseToken',v_token,
   'rowVersion',s.row_version::text,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'keepGroupIndexes',v_keep,'receiptState',s.receipt_state,'phase',v_phase,
   'resetEpoch',s.reset_epoch::text,'cleanupState',s.cleanup_state);
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token;
   v_ticket:=v_ticket||jsonb_build_object('replacement',jsonb_build_object('stageToken',child.stage_token,
    'key',child.stage_key,'sha256',child.stage_sha256,'bytes',child.stage_bytes,'groupOrdinals',child.group_ordinals));
  END IF;
  v_tickets:=v_tickets||jsonb_build_array(v_ticket);
  IF jsonb_array_length(v_tickets)>=v_limit THEN EXIT;END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_tickets)t JOIN sessions.ingest_stage_intents i
   ON i.stage_token=(t->>'stageToken')::uuid WHERE i.gc_lease_token IS DISTINCT FROM (t->>'leaseToken')::uuid
    OR i.gc_lease_until<=clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: claim lease expired';END IF;
 IF octet_length(v_tickets::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='buffer gc: ticket byte bound';END IF;
 RETURN jsonb_build_object('tickets',v_tickets);
END $claim$;

CREATE FUNCTION sessions.ingest_buffer_gc_prepare(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $prepare$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;c sessions.ingest_stage_intents%ROWTYPE;
 v_groups jsonb;v_key text;v_sha text;v_bytes bigint;v_old text[];v_new text[];v_token uuid;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','leaseToken','key','sha256','bytes','groupOrdinals']);
 s:=sessions.ingest_buffer_gc_locked((p->>'stageToken')::uuid,(p->>'leaseToken')::uuid);
 IF s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged' OR jsonb_array_length(s.gc_keep_groups)=0
  OR jsonb_array_length(s.gc_keep_groups)>=jsonb_array_length(s.group_ordinals)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: not a replacement plan';END IF;
 IF sessions.ingest_buffer_gc_kept(s.stage_token) IS DISTINCT FROM s.gc_keep_groups
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: fence changed during copy';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
  AND origin_id=s.origin_id AND batch_id=s.batch_id AND active_stage_token=s.stage_token
  AND row_version=s.gc_outbox_version AND state='buffered' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: active pointer changed';END IF;
 SELECT coalesce(jsonb_agg(s.group_ordinals->(x::text)::int ORDER BY ordinality),'[]') INTO v_groups
 FROM jsonb_array_elements(s.gc_keep_groups) WITH ORDINALITY q(x,ordinality);
 IF p->'groupOrdinals' IS DISTINCT FROM v_groups THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffer gc: replacement must preserve original ordinals';END IF;
 v_sha:=sessions.ingest_digest(p->'sha256');
 v_bytes:=sessions.ingest_bounded(p->'bytes',1,16777216);
 v_key:=p->>'key';
 IF jsonb_typeof(p->'key') IS DISTINCT FROM 'string' OR octet_length(v_key) NOT BETWEEN 1 AND 2048 OR v_key=s.stage_key
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffer gc: replacement key';END IF;
 v_old:=regexp_match(s.stage_key,'^(.*\/buffered-batches/v1/ws=[^/]+/m=[^/]+/b=)([a-f0-9]{32})-([a-f0-9]{16})-([a-f0-9]{32})\.staged\.ndjson\.zst$');
 v_new:=regexp_match(v_key,'^(.*\/buffered-batches/v1/ws=[^/]+/m=[^/]+/b=)([a-f0-9]{32})-([a-f0-9]{16})-([a-f0-9]{32})\.staged\.ndjson\.zst$');
 IF v_old IS NULL OR v_new IS NULL OR v_old[1]<>v_new[1]
  OR v_new[2]<>left(encode(sha256(convert_to(s.digest,'UTF8')),'hex'),32)
  OR v_new[3]<>left(v_sha,16)
  OR v_new[4]<>left(encode(sha256(convert_to(s.claim_token::text,'UTF8')),'hex'),32)
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffer gc: replacement namespace or claim';END IF;
 IF s.gc_replacement_stage_token IS NOT NULL THEN
  SELECT * INTO c FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token FOR UPDATE;
  IF NOT FOUND OR c.cleanup_state<>'live' OR c.receipt_state<>'preparing' OR
    ROW(c.stage_key,c.stage_sha256,c.stage_bytes,c.group_ordinals) IS DISTINCT FROM ROW(v_key,v_sha,v_bytes,v_groups)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: different registered replacement';END IF;
 ELSE
  v_token:=gen_random_uuid();
  INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,
    stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,expires_at,parent_stage_token)
   VALUES(v_token,s.claim_token,s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest,
     v_key,v_sha,v_bytes,v_groups,s.reset_epoch,s.gc_lease_until,s.stage_token) RETURNING * INTO c;
  UPDATE sessions.ingest_stage_intents SET gc_replacement_stage_token=c.stage_token WHERE stage_token=s.stage_token;
 END IF;
 IF clock_timestamp()>=s.gc_lease_until THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: lease expired';END IF;
 RETURN jsonb_build_object('replacementStageToken',c.stage_token,'key',c.stage_key,'sha256',c.stage_sha256,
  'bytes',c.stage_bytes,'groupOrdinals',c.group_ordinals);
END $prepare$;

CREATE FUNCTION sessions.ingest_buffer_gc_swap(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $swap$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;c sessions.ingest_stage_intents%ROWTYPE;
 v_child uuid;v_groups jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','leaseToken','replacementStageToken']);
 s:=sessions.ingest_buffer_gc_locked((p->>'stageToken')::uuid,(p->>'leaseToken')::uuid);
 IF s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged'
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: not a live acknowledged plan';END IF;
 IF sessions.ingest_buffer_gc_kept(s.stage_token) IS DISTINCT FROM s.gc_keep_groups
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: fence changed during copy';END IF;
 IF p->'replacementStageToken'='null'::jsonb THEN v_child:=NULL;
 ELSE v_child:=(p->>'replacementStageToken')::uuid;END IF;
 IF jsonb_array_length(s.gc_keep_groups)=0 THEN
  IF v_child IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffer gc: erased plan cannot retain child';END IF;
 ELSE
  IF v_child IS NULL OR v_child IS DISTINCT FROM s.gc_replacement_stage_token
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: registered replacement required';END IF;
  SELECT * INTO c FROM sessions.ingest_stage_intents WHERE stage_token=v_child FOR UPDATE;
  SELECT coalesce(jsonb_agg(s.group_ordinals->(x::text)::int ORDER BY ordinality),'[]') INTO v_groups
   FROM jsonb_array_elements(s.gc_keep_groups) WITH ORDINALITY q(x,ordinality);
  IF c.stage_token IS DISTINCT FROM v_child OR c.parent_stage_token IS DISTINCT FROM s.stage_token OR c.cleanup_state<>'live'
   OR c.receipt_state<>'preparing' OR c.claim_token<>s.claim_token OR c.reset_epoch<>s.reset_epoch OR c.group_ordinals<>v_groups
   OR ROW(c.org_id,c.workspace_id,c.user_id,c.origin_id,c.batch_id,c.digest)
     IS DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: replacement authority changed';END IF;
 END IF;
 UPDATE sessions.ingest_buffer_outbox SET active_stage_token=v_child,
   state=CASE WHEN v_child IS NULL THEN 'erased' ELSE 'buffered' END,row_version=row_version+1
 WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND origin_id=s.origin_id AND batch_id=s.batch_id
   AND active_stage_token=s.stage_token AND row_version=s.gc_outbox_version AND state='buffered';
 IF NOT FOUND THEN RETURN jsonb_build_object('swapped',false);END IF;
 IF v_child IS NOT NULL THEN
  UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged',row_version=row_version+1
   WHERE stage_token=v_child;
 END IF;
 UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1,gc_lease_version=row_version+1
  WHERE stage_token=s.stage_token;
 IF clock_timestamp()>=s.gc_lease_until THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: lease expired';END IF;
 RETURN jsonb_build_object('swapped',true);
END $swap$;

CREATE FUNCTION sessions.ingest_buffer_gc_finish(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $finish$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;v_barrier text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','leaseToken','barrierVersionId']);
 v_barrier:=p->>'barrierVersionId';
 IF jsonb_typeof(p->'barrierVersionId') IS DISTINCT FROM 'string'
  OR v_barrier='null' OR octet_length(v_barrier) NOT BETWEEN 1 AND 2048 OR v_barrier~'[[:cntrl:]]'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffer gc: barrier version required';END IF;
 SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 IF s.gc_generation IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: unknown lease generation';END IF;
 PERFORM sessions.ingest_require_generation(s.gc_generation);
 IF s.cleanup_state='redacted' AND s.gc_lease_token=(p->>'leaseToken')::uuid AND s.gc_barrier_version_id=v_barrier
 THEN RETURN jsonb_build_object('finished',true);END IF;
 s:=sessions.ingest_buffer_gc_locked((p->>'stageToken')::uuid,(p->>'leaseToken')::uuid);
 IF s.cleanup_state<>'retired' OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE active_stage_token=s.stage_token)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: current object cannot finish';END IF;
 UPDATE sessions.ingest_stage_intents SET cleanup_state='redacted',gc_barrier_version_id=v_barrier,
   row_version=row_version+1,gc_lease_version=row_version+1
  WHERE stage_token=s.stage_token;
 IF clock_timestamp()>=s.gc_lease_until THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: lease expired';END IF;
 RETURN jsonb_build_object('finished',true);
END $finish$;

REVOKE ALL ON FUNCTION sessions.ingest_buffer_gc_kept(uuid),sessions.ingest_buffer_gc_locked(uuid,uuid),
 sessions.ingest_buffer_gc_claim(jsonb),sessions.ingest_buffer_gc_prepare(jsonb),
 sessions.ingest_buffer_gc_swap(jsonb),sessions.ingest_buffer_gc_finish(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_buffer_gc_claim(jsonb),sessions.ingest_buffer_gc_prepare(jsonb),
 sessions.ingest_buffer_gc_swap(jsonb),sessions.ingest_buffer_gc_finish(jsonb) TO bulls96_session_ingest;
