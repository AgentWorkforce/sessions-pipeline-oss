-- Authoritative first buffered-ACK seats. No timestamp/drain-order repair.
-- Existing staged ACKs cannot be retroactively assigned their lost first order.
DO $old_ack$
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE publication_state='buffered'
  AND state='committed' AND buffer_receipt_kind='staged') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: pre-seat ACK requires reconciliation';
 END IF;
END $old_ack$;

-- Private INVOKER capabilities cannot bridge creators through implicit
-- superuser privilege. Refuse mixed-owner upgrades; preserve every existing owner.
DO $creator$
DECLARE v_me oid;v_count integer;v_same boolean;
BEGIN
 SELECT oid INTO STRICT v_me FROM pg_roles WHERE rolname=current_user;
 SELECT count(*)::integer,bool_and(proowner=v_me) INTO v_count,v_same FROM pg_proc
 WHERE oid IN('sessions.ingest_claim_buffered_batch(jsonb)'::regprocedure,
  'sessions.ingest_commit_buffered_batch(jsonb)'::regprocedure,
  'sessions.ingest_claim_batch_mode(jsonb,text)'::regprocedure);
 IF v_count<>3 OR v_same IS DISTINCT FROM true THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat migration requires ingest creator';
 END IF;
 SELECT count(*)::integer,bool_and(relowner=v_me) INTO v_count,v_same FROM pg_class
 WHERE oid IN('sessions.ingest_batches'::regclass,'sessions.ingest_claim_attempts'::regclass,
  'sessions.ingest_stage_intents'::regclass,'sessions.ingest_buffer_outbox'::regclass,
  'sessions.ingest_buffer_admission'::regclass);
 IF v_count<>5 OR v_same IS DISTINCT FROM true THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat tables require original creator';
 END IF;
END $creator$;

CREATE TABLE sessions.ingest_receipt_seat_clock (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 allocated_high_water bigint NOT NULL CHECK(allocated_high_water BETWEEN 0 AND 9223372036854775806),
 last_seat bigint NOT NULL CHECK(last_seat>=allocated_high_water),
 baseline jsonb NOT NULL CHECK(jsonb_typeof(baseline)='object'),
 initial_generation text NOT NULL CHECK(initial_generation~'^[0-9a-f]{64}$'),
 initialized_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions.ingest_receipt_seats (
 key_sha256 text PRIMARY KEY CHECK(key_sha256~'^[0-9a-f]{64}$'),
 key_material jsonb NOT NULL CHECK(jsonb_typeof(key_material)='array'),
 seat bigint NOT NULL UNIQUE CHECK(seat>0),
 first_digest text NOT NULL CHECK(first_digest~'^[0-9a-f]{64}$'),
 source text NOT NULL,session_id text,fence_epoch bigint NOT NULL CHECK(fence_epoch>=0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- One ACK-time current winner per record/reset across session/source aliases.
-- A later erased winner can establish a new seat; old ordinal decisions never change.
CREATE TABLE sessions.ingest_receipt_record_heads (
 key_sha256 text PRIMARY KEY CHECK(key_sha256~'^[0-9a-f]{64}$'),
 key_material jsonb NOT NULL CHECK(jsonb_typeof(key_material)='array'),
 winner_key_sha256 text NOT NULL REFERENCES sessions.ingest_receipt_seats(key_sha256) ON DELETE RESTRICT
);
CREATE TABLE sessions.ingest_receipt_ord_seats (
 claim_token uuid NOT NULL REFERENCES sessions.ingest_claim_attempts(claim_token) ON DELETE RESTRICT,
 ord integer NOT NULL CHECK(ord BETWEEN 0 AND 499),
 key_sha256 text NOT NULL REFERENCES sessions.ingest_receipt_seats(key_sha256) ON DELETE RESTRICT,
 candidate_key_sha256 text NOT NULL CHECK(candidate_key_sha256~'^[0-9a-f]{64}$'),
 body_accepted boolean NOT NULL,
 candidate_digest text NOT NULL CHECK(candidate_digest~'^[0-9a-f]{64}$'),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),
 fence_epoch bigint NOT NULL CHECK(fence_epoch>=0),
 PRIMARY KEY(claim_token,ord)
);
ALTER TABLE sessions.ingest_receipt_seat_clock ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_receipt_seats ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_receipt_ord_seats ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_receipt_record_heads ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_receipt_seat_clock,sessions.ingest_receipt_seats,sessions.ingest_receipt_ord_seats,sessions.ingest_receipt_record_heads
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Owner ceremony only. Source identity/proof/cutoff remain independently
-- verified operator evidence, not claims an app request can manufacture.
CREATE FUNCTION sessions.ingest_receipt_seat_initialize(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $initialize$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;v_epoch text;v_identity text;
 v_generation text;v_floor bigint;v_max bigint:=0;s jsonb;v_n integer:=0;v_value bigint;
 v_baseline jsonb;v_expected text;v_capture timestamptz;v_oid bigint;v_seen_oids bigint[]:='{}';
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(proowner) FROM pg_proc
  WHERE oid='sessions.ingest_receipt_seat_initialize(jsonb)'::regprocedure) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: seat owner required';
 END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['protocol','controlEpoch','capturedAt','sourceSequences',
  'allocatedHighWater','generation','sourceIdentitySha256','sourceProofSha256']);
 IF p->>'protocol' IS DISTINCT FROM 'delivery-acceptance-floor-v1' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: seat protocol';
 END IF;
 v_epoch:=sessions.ingest_digest(p->'controlEpoch');v_identity:=sessions.ingest_digest(p->'sourceIdentitySha256');
 PERFORM sessions.ingest_digest(p->'sourceProofSha256');
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 PERFORM 1 FROM sessions.ingest_buffer_admission
  WHERE control_epoch=v_epoch AND schema_generation=v_generation FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat epoch not owner bound';END IF;
 IF jsonb_typeof(p->'capturedAt') IS DISTINCT FROM 'string'
  OR p->>'capturedAt'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$'
  OR jsonb_typeof(p->'allocatedHighWater') IS DISTINCT FROM 'string'
  OR p->>'allocatedHighWater'!~'^(0|[1-9][0-9]{0,18})$'
  OR jsonb_typeof(p->'sourceSequences') IS DISTINCT FROM 'array'
  OR jsonb_array_length(p->'sourceSequences')<>2 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid seat baseline';
 END IF;
 BEGIN v_capture:=(p->>'capturedAt')::timestamptz;v_floor:=(p->>'allocatedHighWater')::bigint;
 EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid seat cutoff';END;
 IF v_floor>9223372036854775806 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: seat floor exhausted';END IF;
 FOR s IN SELECT value FROM jsonb_array_elements(p->'sourceSequences') LOOP
  PERFORM sessions.ingest_exact_keys(s,ARRAY['sequenceRegclass','sequenceOid','allocatedHighWater','isCalled','incrementBy','cycle']);
  v_expected:=CASE v_n WHEN 0 THEN 'sessions.delivery_catalog_acceptance_seq' ELSE 'sessions.delivery_projection_acceptance_seq' END;
  IF s->>'sequenceRegclass' IS DISTINCT FROM v_expected OR s->>'incrementBy' IS DISTINCT FROM '1'
   OR jsonb_typeof(s->'incrementBy') IS DISTINCT FROM 'string'
   OR s->'cycle' IS DISTINCT FROM 'false'::jsonb OR jsonb_typeof(s->'isCalled') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(s->'sequenceOid') IS DISTINCT FROM 'string' OR s->>'sequenceOid'!~'^[1-9][0-9]{0,9}$'
   OR jsonb_typeof(s->'allocatedHighWater') IS DISTINCT FROM 'string'
   OR s->>'allocatedHighWater'!~'^(0|[1-9][0-9]{0,18})$' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid source allocator';
  END IF;
  BEGIN v_oid:=(s->>'sequenceOid')::bigint;v_value:=(s->>'allocatedHighWater')::bigint;
  EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: allocator overflow';END;
  IF v_oid>4294967295 OR v_oid=ANY(v_seen_oids) OR v_value>9223372036854775806 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: allocator overflow';
  END IF;
  v_seen_oids:=array_append(v_seen_oids,v_oid);v_max:=greatest(v_max,v_value);v_n:=v_n+1;
 END LOOP;
 IF v_floor<>v_max THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: floor must cover both allocators';END IF;
 v_baseline:=p-'generation';
 -- Serialized first initialization. Identical baseline remains idempotent
 -- across schema upgrades; no replay resets the transactional clock.
 INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,
  allocated_high_water,last_seat,baseline,initial_generation)
 VALUES(true,v_epoch,v_identity,v_floor,v_floor,v_baseline,v_generation) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 IF c.baseline IS DISTINCT FROM v_baseline THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat baseline adoption refused';
 END IF;
 PERFORM sessions.ingest_require_generation(v_generation);
 RETURN jsonb_build_object('initialized',true,'controlEpoch',c.control_epoch,
  'sourceIdentitySha256',c.source_identity_sha256,'allocatedHighWater',c.allocated_high_water::text,'lastSeat',c.last_seat::text);
END $initialize$;

-- Normalize original metadata once for ACK or current GC-child reads. A missing
-- non-null session snapshot never becomes an invented epoch0.
CREATE FUNCTION sessions.ingest_receipt_seat_records(p_claim uuid,p_groups jsonb)
RETURNS TABLE(ord integer,key_sha256 text,key_material jsonb,candidate_digest text,reset_epoch bigint,fence_epoch bigint)
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $records$
DECLARE a sessions.ingest_claim_attempts%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;
 g jsonb;e jsonb;v_ords integer[]:='{}';v_ord integer;v_count integer;
BEGIN
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=p_claim;
 IF a.records IS NULL OR jsonb_typeof(a.records)<>'array' OR jsonb_array_length(a.records)>500
  OR a.reset_epoch IS NULL OR a.reset_epoch<0 OR jsonb_typeof(p_groups) IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat original metadata missing';
 END IF;
 FOR g IN SELECT value FROM jsonb_array_elements(p_groups) LOOP
  IF jsonb_typeof(g)<>'array' OR jsonb_array_length(g)=0 THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat ordinals missing';END IF;
  FOR e IN SELECT value FROM jsonb_array_elements(g) LOOP
   IF jsonb_typeof(e)<>'number' OR e::text!~'^(0|[1-9][0-9]{0,2})$' THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat ordinal invalid';END IF;
   v_ord:=(e::text)::integer;
   IF v_ord>499 OR v_ord=ANY(v_ords) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: duplicate seat ordinal';END IF;
   v_ords:=array_append(v_ords,v_ord);
  END LOOP;
 END LOOP;
 IF cardinality(v_ords) NOT BETWEEN 1 AND 500 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat ordinal bound';END IF;
 RETURN QUERY
 WITH rec AS MATERIALIZED (
  SELECT * FROM jsonb_to_recordset(a.records) r(ord integer,source text,session_id text,record_id text,revision bigint,digest text)
 ), epochs AS MATERIALIZED (
  SELECT x.source,x.session_id,min(x.epoch) AS epoch,min(x.reset_epoch) AS reset_epoch,count(*) AS n
  FROM jsonb_to_recordset(a.epochs) x(source text,session_id text,epoch bigint,reset_epoch bigint)
  GROUP BY x.source,x.session_id
 ), selected AS (
  SELECT r.*,CASE WHEN r.session_id IS NULL THEN 0::bigint ELSE x.epoch END AS scope_epoch
  FROM rec r LEFT JOIN epochs x ON x.source=r.source AND x.session_id=r.session_id
  WHERE r.ord=ANY(v_ords) AND r.record_id IS NOT NULL AND r.source IS NOT NULL
   AND r.revision>0 AND r.digest~'^[0-9a-f]{64}$'
   AND (r.session_id IS NULL OR (x.n=1 AND x.epoch>=0 AND x.reset_epoch=a.reset_epoch))
 ), material AS (
  SELECT r.ord,r.digest,r.scope_epoch,jsonb_build_array('buffer-first-ack-seat-v1',c.control_epoch,
   c.source_identity_sha256,a.org_id,a.workspace_id,a.user_id,a.origin_id,r.record_id,r.revision::text,
   a.reset_epoch::text,r.source,r.session_id,r.scope_epoch::text) AS m
  FROM selected r
 )
 SELECT r.ord,encode(sha256(convert_to(r.m::text,'UTF8')),'hex'),r.m,r.digest,a.reset_epoch,r.scope_epoch
 FROM material r ORDER BY r.ord;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count<>cardinality(v_ords) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: incomplete original seat metadata';
 END IF;
END $records$;

-- Runs inside the same short transaction as receipt/outbox/intent. No object IO.
CREATE FUNCTION sessions.ingest_receipt_seat_ack() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ack$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;
 a sessions.ingest_claim_attempts%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 m sessions.ingest_receipt_ord_seats%ROWTYPE;h sessions.ingest_receipt_record_heads%ROWTYPE;
 r record;v_epoch text;v_generation text;v_last bigint;v_alias jsonb;v_alias_hash text;v_accepted boolean;
BEGIN
 IF NEW.publication_state<>'buffered' OR NEW.state<>'committed' THEN RETURN NEW;END IF;
 IF TG_OP='UPDATE' AND OLD.state='committed' THEN RETURN NEW;END IF;
 PERFORM sessions.ingest_require_buffer_admission(NEW.org_id);
 SELECT control_epoch,schema_generation INTO STRICT v_epoch,v_generation FROM sessions.ingest_buffer_admission
  WHERE org_id=NEW.org_id FOR SHARE;
 PERFORM sessions.ingest_require_generation(v_generation);
 SELECT * INTO c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 IF NOT FOUND OR c.control_epoch<>v_epoch THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: first ACK seat baseline not ready';END IF;
 v_last:=c.last_seat;
 IF NEW.buffer_receipt_kind='suppressed' THEN
  PERFORM sessions.ingest_require_buffer_admission(NEW.org_id);RETURN NEW;
 END IF;
 IF NEW.buffer_receipt_kind IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unknown buffered receipt kind';END IF;
 SELECT i.* INTO t FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token
  WHERE o.org_id=NEW.org_id AND o.workspace_id=NEW.workspace_id AND o.origin_id=NEW.origin_id AND o.batch_id=NEW.batch_id
   AND o.user_id=NEW.user_id AND o.digest=NEW.digest AND o.state='buffered' FOR UPDATE OF i;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live'
  OR t.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: seat original stage authority changed';END IF;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>NEW.org_id OR a.workspace_id<>NEW.workspace_id OR a.user_id<>NEW.user_id OR a.origin_id<>NEW.origin_id
  OR a.batch_id<>NEW.batch_id OR a.digest<>NEW.digest OR a.reset_epoch<>t.reset_epoch
  OR a.schema_generation IS DISTINCT FROM v_generation THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: seat attempt authority changed';END IF;
 FOR r IN SELECT * FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) ORDER BY ord LOOP
  -- Source/session aliases share a first-body decision at ACK time. The
  -- current winner's epoch is consulted ONLY here, never when draining old ACKs.
  v_alias:=jsonb_build_array('buffer-record-first-body-v1',c.control_epoch,c.source_identity_sha256,
   a.org_id,a.workspace_id,a.user_id,a.origin_id,r.key_material->7,a.reset_epoch::text);
  v_alias_hash:=encode(sha256(convert_to(v_alias::text,'UTF8')),'hex');
  SELECT * INTO h FROM sessions.ingest_receipt_record_heads WHERE key_sha256=v_alias_hash FOR UPDATE;
  s:=NULL;
  IF FOUND THEN
   IF h.key_material IS DISTINCT FROM v_alias THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: record head collision';END IF;
   SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=h.winner_key_sha256;
   IF s.session_id IS NOT NULL AND (
    coalesce((SELECT f.epoch FROM sessions.ingest_fence_versions f
     WHERE f.org_id=a.org_id AND f.workspace_id=a.workspace_id AND f.user_id=a.user_id
      AND f.origin_id=a.origin_id AND f.source=s.source AND f.session_id=s.session_id),0)<>s.fence_epoch
    OR EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=a.org_id AND d.workspace_id=a.workspace_id
      AND d.user_id=a.user_id AND d.origin_id=a.origin_id AND d.source=s.source AND d.session_id=s.session_id)) THEN
    s:=NULL;
   END IF;
  END IF;
  -- A higher revision replaces custody even when the previous session remains
  -- enabled. Once that current custody is erased, a fresh lower revision may
  -- establish a new lifecycle; historical alias heads must never resurrect it.
  IF s.key_sha256 IS NOT NULL AND (r.key_material->>8)::bigint>(s.key_material->>8)::bigint THEN s:=NULL;END IF;
  IF s.key_sha256 IS NULL THEN
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
    VALUES(encode(sha256(convert_to(jsonb_build_array('buffer-record-lifecycle-seat-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
     r.key_material,v_last,r.candidate_digest,r.key_material->>10,r.key_material->>11,r.fence_epoch)
    RETURNING * INTO s;
   INSERT INTO sessions.ingest_receipt_record_heads(key_sha256,key_material,winner_key_sha256)
    VALUES(v_alias_hash,v_alias,s.key_sha256) ON CONFLICT(key_sha256) DO UPDATE SET winner_key_sha256=EXCLUDED.winner_key_sha256;
  END IF;
  v_accepted:=s.key_material=r.key_material AND s.first_digest=r.candidate_digest;
  INSERT INTO sessions.ingest_receipt_ord_seats(claim_token,ord,key_sha256,candidate_key_sha256,body_accepted,candidate_digest,reset_epoch,fence_epoch)
   VALUES(t.claim_token,r.ord,s.key_sha256,r.key_sha256,v_accepted,r.candidate_digest,r.reset_epoch,r.fence_epoch)
   ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT m FROM sessions.ingest_receipt_ord_seats WHERE claim_token=t.claim_token AND ord=r.ord;
  IF m.key_sha256<>s.key_sha256 OR m.candidate_key_sha256<>r.key_sha256 OR m.body_accepted<>v_accepted
   OR m.candidate_digest<>r.candidate_digest OR m.reset_epoch<>r.reset_epoch OR m.fence_epoch<>r.fence_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat ordinal adoption refused';END IF;
 END LOOP;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF clock_timestamp()>=t.expires_at THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: ACK seat lease expired';END IF;
 PERFORM sessions.ingest_require_buffer_admission(NEW.org_id);
 RETURN NEW;
END $ack$;
CREATE TRIGGER ingest_receipt_seat_ack AFTER INSERT OR UPDATE OF state ON sessions.ingest_batches
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_receipt_seat_ack();

-- Owner-only capability for040600. Reads current exact original-ordinal maps;
-- GC children keep their immutable claim/ord identity and never acquire seats.
CREATE FUNCTION sessions.ingest_receipt_seats_for_stage(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $stage$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;
 a sessions.ingest_claim_attempts%ROWTYPE;v_rows jsonb;v_count integer;v_expected integer;v_last bigint;
BEGIN
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 SELECT * INTO t FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 IF NOT FOUND OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live' OR NOT EXISTS(
  SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.active_stage_token=t.stage_token AND o.state='buffered'
   AND o.org_id=t.org_id AND o.workspace_id=t.workspace_id AND o.user_id=t.user_id
   AND o.origin_id=t.origin_id AND o.batch_id=t.batch_id AND o.digest=t.digest) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current seat stage required';
 END IF;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id
  OR a.batch_id<>t.batch_id OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat claim identity changed';END IF;
 WITH wanted AS MATERIALIZED (SELECT * FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals))
 SELECT jsonb_agg(jsonb_build_object('ord',r.ord,'seat',s.seat::text,'candidateDigest',r.candidate_digest,
  'firstDigest',s.first_digest,'bodyAccepted',m.body_accepted,'resetEpoch',r.reset_epoch::text,'fenceEpoch',r.fence_epoch::text) ORDER BY r.ord),
  count(*)::integer,(SELECT count(*)::integer FROM wanted)
 INTO v_rows,v_count,v_expected
 FROM wanted r JOIN sessions.ingest_receipt_ord_seats m ON m.claim_token=t.claim_token AND m.ord=r.ord
  AND m.candidate_key_sha256=r.key_sha256 AND m.candidate_digest=r.candidate_digest
  AND m.reset_epoch=r.reset_epoch AND m.fence_epoch=r.fence_epoch
 JOIN sessions.ingest_receipt_seats s ON s.key_sha256=m.key_sha256
  AND m.body_accepted=(s.key_material=r.key_material AND s.first_digest=r.candidate_digest)
  AND s.seat>c.allocated_high_water;
 IF v_count<>v_expected OR v_count=0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: missing first ACK seats';
 END IF;
 -- Read the committed upper bound after maps, never an earlier statement's
 -- counter snapshot. This moving bound is not immutable baseline identity.
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_rows) r WHERE (r->>'seat')::bigint>v_last) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat clock snapshot incomplete';END IF;
 RETURN jsonb_build_object('controlEpoch',c.control_epoch,'sourceIdentitySha256',c.source_identity_sha256,
  'allocatedHighWater',c.allocated_high_water::text,'lastAssignedSeat',v_last::text,'records',v_rows);
END $stage$;

REVOKE ALL ON FUNCTION sessions.ingest_receipt_seat_initialize(jsonb),
 sessions.ingest_receipt_seat_records(uuid,jsonb),sessions.ingest_receipt_seat_ack(),
 sessions.ingest_receipt_seats_for_stage(uuid)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
