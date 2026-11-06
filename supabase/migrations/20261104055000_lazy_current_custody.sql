DO $lazy_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid IN(
  'sessions.ingest_claim_buffered_batch(jsonb)'::regprocedure,'sessions.ingest_prepare_buffer_stage(jsonb)'::regprocedure,
  'sessions.ingest_commit_buffered_batch(jsonb)'::regprocedure,'sessions.ingest_set_session_uploads(jsonb)'::regprocedure,
  'sessions.ingest_reset_uploaded_sessions(jsonb)'::regprocedure,'sessions.ingest_apply_ordered_control(jsonb,text)'::regprocedure,
  'sessions.ingest_claim_batch_mode(jsonb,text)'::regprocedure,'sessions.ingest_receipt_seat_ack()'::regprocedure,
  'sessions.ingest_receipt_seats_for_stage(uuid)'::regprocedure,'sessions.ingest_receipt_seat_initialize(jsonb)'::regprocedure,
  'sessions.ingest_require_buffer_admission(text)'::regprocedure,'sessions.ingest_require_custody(text)'::regprocedure,
  'sessions.ingest_buffer_gc_claim(jsonb)'::regprocedure,'sessions.ingest_buffer_gc_prepare(jsonb)'::regprocedure,
  'sessions.ingest_buffer_gc_swap(jsonb)'::regprocedure)
  AND proowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: lazy custody requires original creator';END IF;
END $lazy_creator$;

-- Initial upload authority: bounded CURRENT metadata witnesses, not a full
--61M-row prewarm. No transcript payload, canonical revision seed, or new role.
CREATE TABLE sessions.ingest_lazy_policy(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 baseline_mode text NOT NULL CHECK(baseline_mode='lazy-current'),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256~'^[0-9a-f]{64}$'),
 retention_contract_sha256 text NOT NULL CHECK(retention_contract_sha256~'^[0-9a-f]{64}$'),
 source_proof_sha256 text NOT NULL CHECK(source_proof_sha256~'^[0-9a-f]{64}$'),
 issuer text NOT NULL CHECK(issuer='relayhistory-cloud'),valid_until timestamptz NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions.ingest_lazy_user_seals(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 privacy_fingerprint text NOT NULL CHECK(privacy_fingerprint~'^[0-9a-f]{64}$'),
 initial_privacy_version bigint NOT NULL CHECK(initial_privacy_version>=0),
 initialized_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id),
 FOREIGN KEY(org_id,workspace_id,user_id) REFERENCES sessions.ingest_fence_owners ON DELETE RESTRICT
);
-- GLOBAL native-key initialization persists across deletion/reset. Only current
-- custody/owner is cleared; a stale original Neon row can never reseed the slot.
CREATE TABLE sessions.ingest_lazy_native_slots(
 key_sha256 text PRIMARY KEY CHECK(key_sha256~'^[0-9a-f]{64}$'),key_material jsonb NOT NULL,
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 initialized_witness_sha256 text NOT NULL CHECK(initialized_witness_sha256~'^[0-9a-f]{64}$'),
 lineage bigint NOT NULL DEFAULT 0 CHECK(lineage>=0),
 current_owner text,current_source text,current_session text,current_revision bigint,current_digest text,
 current_reset_epoch bigint,current_fence_epoch bigint,current_seat_key text REFERENCES sessions.ingest_receipt_seats(key_sha256),
 current_legacy_key text REFERENCES sessions.ingest_custody_records(key_sha256),
 initialized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(current_owner IS NULL OR (current_revision>0 AND current_digest~'^[0-9a-f]{64}$'
  AND current_source IS NOT NULL AND current_reset_epoch>=0 AND current_fence_epoch>=0))
);
CREATE INDEX ingest_lazy_native_current_owner ON sessions.ingest_lazy_native_slots(org_id,workspace_id,current_owner)
 WHERE current_owner IS NOT NULL;
CREATE TABLE sessions.ingest_lazy_ack_decisions(
 claim_token uuid NOT NULL REFERENCES sessions.ingest_claim_attempts(claim_token),ord integer NOT NULL CHECK(ord BETWEEN 0 AND 499),
 native_key_sha256 text NOT NULL REFERENCES sessions.ingest_lazy_native_slots(key_sha256),native_lineage bigint NOT NULL CHECK(native_lineage>=0),
 candidate_key_sha256 text NOT NULL CHECK(candidate_key_sha256~'^[0-9a-f]{64}$'),
 candidate_digest text NOT NULL CHECK(candidate_digest~'^[0-9a-f]{64}$'),
 first_digest text NOT NULL CHECK(first_digest~'^[0-9a-f]{64}$'),body_accepted boolean NOT NULL,
 seat_key_sha256 text NOT NULL REFERENCES sessions.ingest_receipt_seats(key_sha256),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),fence_epoch bigint NOT NULL CHECK(fence_epoch>=0),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 source_control_version bigint NOT NULL CHECK(source_control_version>=0),
 source_witness_sha256 text NOT NULL CHECK(source_witness_sha256~'^[0-9a-f]{64}$'),
 PRIMARY KEY(claim_token,ord)
);
ALTER TABLE sessions.ingest_claim_attempts ADD COLUMN source_control_version bigint CHECK(source_control_version>=0),
 ADD COLUMN source_identity_sha256 text CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 ADD COLUMN source_witness_sha256 text CHECK(source_witness_sha256~'^[0-9a-f]{64}$'),
 ADD COLUMN source_processing_ranks jsonb CHECK(jsonb_typeof(source_processing_ranks)='array' AND jsonb_array_length(source_processing_ranks)<=500);
ALTER TABLE sessions.ingest_custody_records ALTER COLUMN import_token DROP NOT NULL,ALTER COLUMN page DROP NOT NULL,
 ADD COLUMN live_source_identity_sha256 text CHECK(live_source_identity_sha256~'^[0-9a-f]{64}$'),
 ADD COLUMN live_control_epoch text CHECK(live_control_epoch~'^[0-9a-f]{64}$'),
 ADD COLUMN live_control_version bigint CHECK(live_control_version>=0),
 ADD COLUMN live_witness_sha256 text CHECK(live_witness_sha256~'^[0-9a-f]{64}$');
ALTER TABLE sessions.ingest_custody_records ADD CONSTRAINT ingest_custody_authority_kind CHECK(
 (import_token IS NOT NULL AND page IS NOT NULL AND live_source_identity_sha256 IS NULL AND live_control_epoch IS NULL
  AND live_control_version IS NULL AND live_witness_sha256 IS NULL)
 OR(import_token IS NULL AND page IS NULL AND live_source_identity_sha256 IS NOT NULL AND live_control_epoch IS NOT NULL
  AND live_control_version IS NOT NULL AND live_witness_sha256 IS NOT NULL));
ALTER TABLE sessions.ingest_lazy_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_lazy_user_seals ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_lazy_ack_decisions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_lazy_policy,sessions.ingest_lazy_user_seals,sessions.ingest_lazy_native_slots,sessions.ingest_lazy_ack_decisions
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_policy_set(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_policy$
DECLARE old sessions.ingest_lazy_policy%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;
 v_gen text;v_source text;v_epoch text;v_privacy text;v_retention text;v_proof text;v_until timestamptz;
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(proowner) FROM pg_proc
  WHERE oid='sessions.ingest_lazy_policy_set(jsonb)'::regprocedure) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: custody policy owner required';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['baselineMode','generation','sourceIdentitySha256','controlEpoch',
  'privacyBaselineSha256','retentionContractSha256','sourceProofSha256','validUntil','issuer']);
 IF p->>'baselineMode' IS DISTINCT FROM 'lazy-current' OR p->>'issuer' IS DISTINCT FROM 'relayhistory-cloud'
  OR jsonb_typeof(p->'validUntil') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid current custody policy';END IF;
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 v_source:=sessions.ingest_digest(p->'sourceIdentitySha256');v_epoch:=sessions.ingest_digest(p->'controlEpoch');
 v_privacy:=sessions.ingest_digest(p->'privacyBaselineSha256');v_retention:=sessions.ingest_digest(p->'retentionContractSha256');
 v_proof:=sessions.ingest_digest(p->'sourceProofSha256');
 BEGIN v_until:=(p->>'validUntil')::timestamptz;EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid policy expiry';END;
 IF NOT isfinite(v_until) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: finite policy expiry required';END IF;
 SELECT * INTO old FROM sessions.ingest_lazy_policy WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_batches) OR EXISTS(SELECT 1 FROM sessions.ingest_fence_owners)
   OR EXISTS(SELECT 1 FROM sessions.ingest_record_revisions) OR EXISTS(SELECT 1 FROM sessions.ingest_custody_imports)
   OR EXISTS(SELECT 1 FROM sessions.ingest_control_receipts) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: lazy policy cannot adopt existing authority';END IF;
 ELSE
  IF old.source_identity_sha256<>v_source OR old.control_epoch<>v_epoch OR old.privacy_baseline_sha256<>v_privacy
   OR old.retention_contract_sha256<>v_retention OR old.source_proof_sha256<>v_proof THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: lazy policy identity is immutable';END IF;
 END IF;
 SELECT * INTO c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE;
 IF FOUND AND(c.source_identity_sha256<>v_source OR c.control_epoch<>v_epoch
  OR c.baseline->>'sourceProofSha256' IS DISTINCT FROM v_proof) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: policy clock identity mismatch';END IF;
 INSERT INTO sessions.ingest_lazy_policy(singleton,baseline_mode,generation,source_identity_sha256,control_epoch,
  privacy_baseline_sha256,retention_contract_sha256,source_proof_sha256,issuer,valid_until)
 VALUES(true,'lazy-current',v_gen,v_source,v_epoch,v_privacy,v_retention,v_proof,'relayhistory-cloud',v_until)
 ON CONFLICT(singleton) DO UPDATE SET generation=EXCLUDED.generation,valid_until=EXCLUDED.valid_until,updated_at=clock_timestamp();
 RETURN jsonb_build_object('accepted',true,'baselineMode','lazy-current','generation',v_gen,'sourceIdentitySha256',v_source,'controlEpoch',v_epoch);
END $lazy_policy$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_policy_set(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


CREATE FUNCTION sessions.ingest_lazy_user(p_w jsonb,p_kind text,p_control jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_user$
DECLARE v_org text;v_ws text;v_user text;v_epoch text;v_source text;v_first boolean;v_last bigint;v_version bigint;v_privacy_version bigint;
 s sessions.ingest_lazy_user_seals%ROWTYPE;b sessions.ingest_seed_fence_baselines%ROWTYPE;v_privacy jsonb;v_d jsonb;v_r jsonb;v_fp text;k record;v_exists boolean;
BEGIN
 v_org:=sessions.ingest_identifier(p_w#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p_w#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p_w#>'{tenant,userId}',false);v_epoch:=sessions.ingest_digest(p_w->'controlEpoch');
 v_source:=sessions.ingest_digest(p_w->'sourceIdentitySha256');
 IF jsonb_typeof(p_w->'sourceControlVersion') IS DISTINCT FROM 'string' OR p_w->>'sourceControlVersion'!~'^(0|[1-9][0-9]{0,18})$'
  OR jsonb_typeof(p_w->'privacyVersion') IS DISTINCT FROM 'string' OR p_w->>'privacyVersion'!~'^(0|[1-9][0-9]{0,18})$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: witness version shape';END IF;
 v_version:=(p_w->>'sourceControlVersion')::bigint;v_privacy_version:=(p_w->>'privacyVersion')::bigint;
 INSERT INTO sessions.ingest_fence_owners VALUES(v_org,v_ws,v_user) ON CONFLICT DO NOTHING RETURNING true INTO v_first;
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT coalesce(max(version),0) INTO v_last FROM sessions.ingest_control_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND epoch=v_epoch;
 IF p_kind='batch' THEN
  IF p_w->'pending' IS DISTINCT FROM 'false'::jsonb OR p_w->'pendingControl' IS DISTINCT FROM 'null'::jsonb
   OR v_version<>v_last OR v_privacy_version<>v_version THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control version changed';END IF;
 ELSE
  IF p_w->'pending' IS DISTINCT FROM 'true'::jsonb OR p_w->'pendingControl' IS DISTINCT FROM p_control
   OR p_control->>'epoch' IS DISTINCT FROM v_epoch OR p_control->>'kind' IS DISTINCT FROM p_kind
   OR p_control->>'version' IS DISTINCT FROM v_version::text OR v_version=0 OR v_privacy_version<>v_version-1 THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: pending source control mismatch';END IF;
  IF v_last<v_version THEN
   IF v_last=9223372036854775807 OR v_last+1<>v_version THEN
    RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control gap';END IF;
  ELSE
   PERFORM 1 FROM sessions.ingest_control_receipts WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
    AND epoch=v_epoch AND version=v_version AND kind=p_kind AND digest=p_control->>'digest';
   IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control replay mismatch';END IF;
  END IF;
 END IF;
 SELECT * INTO s FROM sessions.ingest_lazy_user_seals WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 IF FOUND THEN
  SELECT * INTO b FROM sessions.ingest_seed_fence_baselines WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
  IF NOT FOUND OR s.source_identity_sha256<>v_source OR s.control_epoch<>v_epoch OR s.privacy_fingerprint<>b.fingerprint THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: existing user privacy authority changed';END IF;
  RETURN jsonb_build_object('privacySealed',true,'created',false,'targetVersion',v_last::text);
 END IF;
 -- An already-completed source control implies a previously sealed target.
 -- Lost authority cannot silently become a fresh user merely because rows vanished.
 IF v_first IS DISTINCT FROM true OR v_last<>0 OR v_privacy_version<>0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unsealed user authority cannot be adopted';END IF;
 FOR k IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='sessions' AND c.relkind='r' AND (c.relname LIKE 'ingest_%' OR c.relname LIKE 'upload_%')
   AND c.relname<>'ingest_fence_owners'
   AND NOT EXISTS(SELECT 1 FROM unnest(ARRAY['org_id','workspace_id','user_id']) col
    WHERE NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname=col AND a.attnum>0 AND NOT a.attisdropped))
 LOOP
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM sessions.%I WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3)',k.relname)
   INTO v_exists USING v_org,v_ws,v_user;
  IF v_exists THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: prior user authority requires reviewed recovery';END IF;
 END LOOP;
 v_privacy:=p_w->'privacy';
 PERFORM sessions.ingest_exact_keys(v_privacy,ARRAY['complete','deletions','resets']);
 IF v_privacy->'complete' IS DISTINCT FROM 'true'::jsonb OR octet_length(v_privacy::text)>524288
  OR jsonb_typeof(v_privacy->'deletions') IS DISTINCT FROM 'array' OR jsonb_typeof(v_privacy->'resets') IS DISTINCT FROM 'array'
  OR jsonb_array_length(v_privacy->'deletions')+jsonb_array_length(v_privacy->'resets')>5000 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: complete bounded cold privacy required';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_privacy->'deletions') q(e) WHERE CASE WHEN jsonb_typeof(e)<>'object' THEN true ELSE
  ARRAY(SELECT x FROM jsonb_object_keys(e)x ORDER BY x COLLATE "C") IS DISTINCT FROM ARRAY['origin_id','session_id','source']::text[] END)
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_privacy->'resets')q(e) WHERE CASE WHEN jsonb_typeof(e)<>'object' THEN true ELSE
  ARRAY(SELECT x FROM jsonb_object_keys(e)x ORDER BY x COLLATE "C") IS DISTINCT FROM ARRAY['deleted','request_id']::text[]
  OR jsonb_typeof(e->'deleted') IS DISTINCT FROM 'string' OR e->>'deleted'!~'^(0|[1-9][0-9]{0,18})$' END)
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid cold privacy metadata';END IF;
 SELECT coalesce(jsonb_agg(x ORDER BY x::text COLLATE "C"),'[]') INTO v_d FROM(
  SELECT DISTINCT jsonb_build_object('origin_id',sessions.ingest_identifier(e->'origin_id',false),
   'source',sessions.ingest_identifier(e->'source',false),'session_id',sessions.ingest_identifier(e->'session_id',false)) x
  FROM jsonb_array_elements(v_privacy->'deletions')q(e))q;
 SELECT coalesce(jsonb_agg(x ORDER BY x::text COLLATE "C"),'[]') INTO v_r FROM(
  SELECT DISTINCT jsonb_build_object('request_id',sessions.ingest_identifier(e->'request_id',false),'deleted',(e->>'deleted')::bigint) x
  FROM jsonb_array_elements(v_privacy->'resets')q(e))q;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_r)x GROUP BY x->>'request_id' HAVING count(*)>1) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: conflicting reset metadata';END IF;
 v_fp:=encode(sha256(convert_to(jsonb_build_array('neon-fence-baseline-v1',v_org,v_ws,v_user,v_d,v_r)::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_fence_versions(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
  SELECT v_org,v_ws,v_user,x.origin_id,x.source,x.session_id,1 FROM jsonb_to_recordset(v_d)x(origin_id text,source text,session_id text);
 INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT v_org,v_ws,v_user,x.origin_id,x.source,x.session_id FROM jsonb_to_recordset(v_d)x(origin_id text,source text,session_id text);
 INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id,deleted)
  SELECT v_org,v_ws,v_user,x.request_id,x.deleted FROM jsonb_to_recordset(v_r)x(request_id text,deleted bigint);
 INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint,imported_at)
  VALUES(v_org,v_ws,v_user,v_fp,clock_timestamp());
 INSERT INTO sessions.ingest_lazy_user_seals(org_id,workspace_id,user_id,source_identity_sha256,control_epoch,privacy_fingerprint,initial_privacy_version)
  VALUES(v_org,v_ws,v_user,v_source,v_epoch,v_fp,v_privacy_version);
 RETURN jsonb_build_object('privacySealed',true,'created',true,'targetVersion',v_last::text);
END $lazy_user$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_user(jsonb,text,jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


CREATE FUNCTION sessions.ingest_lazy_begin(p jsonb,p_kind text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;
 v_gen text;v_packet jsonb;w jsonb;v_org text;v_ws text;v_user text;v_new boolean;v_exists boolean;k record;
 v_records jsonb;v_count integer;v_seen integer;v_ack jsonb;
BEGIN
 IF p_kind NOT IN('batch','selection','reset') OR p_kind IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid witness operation';END IF;
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF pol.generation<>v_gen OR pol.valid_until<=clock_timestamp() OR pol.source_identity_sha256<>c.source_identity_sha256
  OR pol.control_epoch<>c.control_epoch OR pol.source_proof_sha256 IS DISTINCT FROM c.baseline->>'sourceProofSha256' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody policy unavailable';END IF;
 v_packet:=p->'custody';PERFORM sessions.ingest_exact_keys(v_packet,ARRAY['witness','sourceDigest']);w:=v_packet->'witness';
 PERFORM sessions.ingest_exact_keys(w,ARRAY['version','issuer','sourceIdentitySha256','controlEpoch','privacyBaselineSha256',
  'tenant','originId','batchId','batchDigest','sourceControlVersion','pending','pendingControl','privacyVersion',
  'originalRecordCount','candidateCount','records','privacy']);
 IF encode(sha256(convert_to(w::text,'UTF8')),'hex')<>sessions.ingest_digest(v_packet->'sourceDigest')
  OR w->'version' IS DISTINCT FROM '1'::jsonb OR w->>'issuer' IS DISTINCT FROM pol.issuer
  OR w->>'sourceIdentitySha256' IS DISTINCT FROM pol.source_identity_sha256 OR w->>'controlEpoch' IS DISTINCT FROM pol.control_epoch
  OR w->>'privacyBaselineSha256' IS DISTINCT FROM pol.privacy_baseline_sha256 OR w->'tenant' IS DISTINCT FROM p->'tenant' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source witness identity mismatch';END IF;
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 IF jsonb_typeof(w->'records') IS DISTINCT FROM 'array' OR octet_length((w->'records')::text)>4194304
  OR jsonb_typeof(w->'candidateCount') IS DISTINCT FROM 'number' OR w->>'candidateCount'!~'^(0|[1-9][0-9]{0,2})$'
  OR jsonb_typeof(w->'originalRecordCount') IS DISTINCT FROM 'number' OR w->>'originalRecordCount'!~'^(0|[1-9][0-9]{0,2})$'
  OR (w->>'candidateCount')::integer>500 OR (w->>'originalRecordCount')::integer>500
  OR (w->>'candidateCount')::integer>(w->>'originalRecordCount')::integer
  OR jsonb_array_length(w->'records')<>(w->>'candidateCount')::integer THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: source witness record bound';END IF;
 IF p_kind='batch' THEN
  v_records:=sessions.ingest_records(p->'records');v_count:=jsonb_array_length(v_records);
  IF w->>'originId' IS DISTINCT FROM p->>'originId' OR w->>'batchId' IS DISTINCT FROM p->>'batchId'
   OR w->>'batchDigest' IS DISTINCT FROM p->>'batchDigest' OR v_count<>(w->>'candidateCount')::integer THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source witness batch mismatch';END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(w->'records')q(e) WHERE CASE WHEN jsonb_typeof(e)<>'object' THEN true ELSE
   ARRAY(SELECT x FROM jsonb_object_keys(e)x ORDER BY x COLLATE "C") IS DISTINCT FROM
    ARRAY['candidateDigest','candidateFenced','current','currentFenced','preparedOrd','processingRank','recordId','revision','revisionId']::text[]
   OR jsonb_typeof(e->'preparedOrd') IS DISTINCT FROM 'number' OR e->>'preparedOrd'!~'^(0|[1-9][0-9]{0,2})$'
   OR jsonb_typeof(e->'processingRank') IS DISTINCT FROM 'number' OR e->>'processingRank'!~'^(0|[1-9][0-9]{0,2})$'
   OR jsonb_typeof(e->'candidateFenced') IS DISTINCT FROM 'boolean' OR jsonb_typeof(e->'currentFenced') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(e->'current') NOT IN('object','null') END)
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: source witness record shape';END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(w->'records')q(e)
   WHERE (e->>'preparedOrd')::integer>=v_count OR (e->>'processingRank')::integer>=v_count)
   OR (SELECT count(DISTINCT e->>'preparedOrd') FROM jsonb_array_elements(w->'records')q(e))<>v_count
   OR (SELECT count(DISTINCT e->>'processingRank') FROM jsonb_array_elements(w->'records')q(e))<>v_count THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source witness rank coverage';END IF;
  SELECT count(*)::integer INTO v_seen FROM jsonb_array_elements(w->'records')q(e)
   JOIN jsonb_to_recordset(v_records)r(ord integer,record_id text,revision bigint,revision_id text,digest text,session_id text)
   ON r.ord=(e->>'preparedOrd')::integer AND r.record_id=e->>'recordId' AND r.revision::text=e->>'revision'
    AND r.revision_id=e->>'revisionId' AND r.digest=e->>'candidateDigest'
   WHERE NOT((e->>'candidateFenced')::boolean AND r.session_id IS NULL);
  IF v_seen<>v_count THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source candidate metadata mismatch';END IF;
 ELSE
  IF w->'batchId' IS DISTINCT FROM 'null'::jsonb OR w->'batchDigest' IS DISTINCT FROM 'null'::jsonb
   OR w->'originalRecordCount' IS DISTINCT FROM '0'::jsonb OR w->'candidateCount' IS DISTINCT FROM '0'::jsonb
   OR w->'records' IS DISTINCT FROM '[]'::jsonb
   OR (p_kind='reset' AND w->'originId' IS DISTINCT FROM 'null'::jsonb)
   OR (p_kind='selection' AND w->>'originId' IS DISTINCT FROM p->>'originId') THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control witness shape';END IF;
 END IF;
 -- The trusted source context permits provisioning, not a claim of empty org
 -- history. Retained closed admissions are never reopened by a runtime request.
 INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)
 VALUES(v_org,pol.privacy_baseline_sha256,pol.retention_contract_sha256,v_gen,pol.valid_until,pol.control_epoch)
 ON CONFLICT DO NOTHING RETURNING true INTO v_new;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org AND privacy_baseline_sha256=pol.privacy_baseline_sha256
  AND retention_contract_sha256=pol.retention_contract_sha256 AND schema_generation=v_gen AND control_epoch=pol.control_epoch
  AND valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: retained org admission closed or conflicting';END IF;
 IF v_new IS TRUE THEN
  FOR k IN SELECT t.relname FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
   WHERE n.nspname='sessions' AND t.relkind='r' AND(t.relname LIKE 'ingest_%' OR t.relname LIKE 'upload_%')
    AND t.relname<>'ingest_buffer_admission'
    AND EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=t.oid AND a.attname='org_id' AND a.attnum>0 AND NOT a.attisdropped)
  LOOP
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM sessions.%I WHERE org_id=$1)',k.relname) INTO v_exists USING v_org;
   IF v_exists THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unbound prior org authority requires recovery';END IF;
  END LOOP;
 END IF;
 v_ack:=sessions.ingest_lazy_user(w,p_kind,p->'control');
 PERFORM sessions.ingest_require_generation(v_gen);
 IF pol.valid_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: policy expired during operation';END IF;
 RETURN v_ack;
END $lazy_begin$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_begin(jsonb,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


CREATE FUNCTION sessions.ingest_lazy_records(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_records$
DECLARE w jsonb;v_org text;v_ws text;v_user text;v_origin text;v_items jsonb;v_reset bigint;
 c sessions.ingest_receipt_seat_clock%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;
 z record;v_key text;v_material jsonb;cur jsonb;v_legacy text;v_meta jsonb;v_fence bigint;seq jsonb;
BEGIN
 w:=p#>'{custody,witness}';v_org:=p#>>'{tenant,orgId}';v_ws:=p#>>'{tenant,workspaceId}';v_user:=p#>>'{tenant,userId}';
 v_origin:=p->>'originId';v_items:=sessions.ingest_records(p->'records');
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 SELECT count(*) INTO v_reset FROM sessions.upload_resets WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 -- Source true is an additional authoritative suppression. False never clears
 -- a target fence; only the ordered control may re-enable a scope.
 WITH pairs AS MATERIALIZED(
  SELECT DISTINCT r.source,r.session_id FROM jsonb_to_recordset(v_items)r(ord integer,source text,session_id text)
   JOIN jsonb_array_elements(w->'records')q(e) ON r.ord=(e->>'preparedOrd')::integer
  WHERE (e->>'candidateFenced')::boolean AND r.session_id IS NOT NULL
 ), fresh AS MATERIALIZED(
  SELECT p.* FROM pairs p WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=v_org
   AND d.workspace_id=v_ws AND d.user_id=v_user AND d.origin_id=v_origin AND d.source=p.source AND d.session_id=p.session_id)
 ), versioned AS(
  INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
  SELECT v_org,v_ws,v_user,v_origin,x.source,x.session_id,1 FROM fresh x
  ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1
 )
 INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
 SELECT v_org,v_ws,v_user,v_origin,x.source,x.session_id FROM fresh x;
 FOR z IN SELECT r.*,e AS witness_record FROM jsonb_to_recordset(v_items)
  r(ord integer,record_id text,revision bigint,revision_id text,digest text,source text,session_id text)
  JOIN jsonb_array_elements(w->'records')q(e) ON r.ord=(e->>'preparedOrd')::integer
  WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=v_org AND d.workspace_id=v_ws
   AND d.user_id=v_user AND d.origin_id=v_origin AND d.source=r.source AND d.session_id=r.session_id)
  ORDER BY (e->>'processingRank')::integer
 LOOP
  v_material:=jsonb_build_array('global-native-custody-v1',v_org,v_ws,v_origin,z.record_id);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF FOUND THEN
   IF slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256 OR slot.control_epoch<>c.control_epoch THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native custody identity changed';END IF;
   IF slot.current_owner IS NOT NULL AND slot.current_owner<>v_user THEN
    RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
   -- An initialized slot is authoritative, even when empty. The original
   -- source copy may be obsolete after target updates or authorized erasure.
   CONTINUE;
  END IF;
  cur:=z.witness_record->'current';v_legacy:=NULL;v_fence:=0;
  IF cur<>'null'::jsonb THEN
   PERFORM sessions.ingest_exact_keys(cur,ARRAY['userId','revision','digest','revisionId','kind','source','sessionId','operation','catalogAcceptedSeq','projectionAcceptedSeq']);
   IF EXISTS(SELECT 1 FROM unnest(ARRAY['userId','revisionId','kind','source'])k WHERE jsonb_typeof(cur->k) IS DISTINCT FROM 'string')
    OR jsonb_typeof(cur->'revision') IS DISTINCT FROM 'string' OR cur->>'revision'!~'^[1-9][0-9]{0,18}$'
    OR jsonb_typeof(cur->'sessionId') NOT IN('string','null') OR cur->>'operation' NOT IN('upsert','delete') THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid current custody metadata';END IF;
   IF cur->>'userId' IS DISTINCT FROM v_user THEN
    RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: source native key belongs to another user';END IF;
   PERFORM sessions.ingest_digest(cur->'digest');
   PERFORM (cur->>'revision')::bigint;
   FOREACH seq IN ARRAY ARRAY[cur->'catalogAcceptedSeq',cur->'projectionAcceptedSeq'] LOOP
    IF seq<>'null'::jsonb THEN
     IF jsonb_typeof(seq)<>'string' OR (seq#>>'{}')!~'^(0|[1-9][0-9]{0,18})$' THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid current custody sequence';END IF;
     PERFORM (seq#>>'{}')::bigint;
    END IF;
   END LOOP;
   IF (z.witness_record->>'currentFenced')::boolean THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source current row is already privacy fenced';END IF;
   IF nullif(cur->>'sessionId','') IS NOT NULL THEN
    SELECT coalesce((SELECT epoch FROM sessions.ingest_fence_versions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
     AND origin_id=v_origin AND source=cur->>'source' AND session_id=cur->>'sessionId'),0) INTO v_fence;
    IF EXISTS(SELECT 1 FROM sessions.upload_deletions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
     AND origin_id=v_origin AND source=cur->>'source' AND session_id=cur->>'sessionId') THEN
     RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: inconsistent current custody privacy';END IF;
   END IF;
   v_meta:=cur||jsonb_build_object('workspaceId',v_ws,'originId',v_origin,'recordId',z.record_id);
   v_legacy:=encode(sha256(convert_to(jsonb_build_array('live-current-custody-v1',c.source_identity_sha256,c.control_epoch,
    v_org,v_ws,v_user,v_origin,z.record_id,v_reset::text,p#>>'{custody,sourceDigest}',v_meta)::text,'UTF8')),'hex');
   INSERT INTO sessions.ingest_custody_records(key_sha256,import_token,page,source_key_sha256,org_id,workspace_id,user_id,origin_id,record_id,
    revision,digest,source,session_id,metadata,reset_epoch,fence_epoch,baseline_fingerprint,
    live_source_identity_sha256,live_control_epoch,live_control_version,live_witness_sha256)
   SELECT v_legacy,NULL,NULL,encode(sha256(convert_to(jsonb_build_array(v_ws,v_origin,z.record_id)::text,'UTF8')),'hex'),
    v_org,v_ws,v_user,v_origin,z.record_id,(cur->>'revision')::bigint,cur->>'digest',cur->>'source',nullif(cur->>'sessionId',''),v_meta,
    v_reset,v_fence,s.privacy_fingerprint,c.source_identity_sha256,c.control_epoch,(w->>'sourceControlVersion')::bigint,p#>>'{custody,sourceDigest}'
   FROM sessions.ingest_lazy_user_seals s WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.user_id=v_user;
   IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: user privacy seal missing';END IF;
  ELSIF (z.witness_record->>'currentFenced')::boolean THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: absent current custody cannot have a scope fence';
  END IF;
  INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,
   source_identity_sha256,control_epoch,initialized_witness_sha256,current_owner,current_source,current_session,current_revision,current_digest,
   current_reset_epoch,current_fence_epoch,current_legacy_key)
  VALUES(v_key,v_material,v_org,v_ws,v_origin,z.record_id,c.source_identity_sha256,c.control_epoch,p#>>'{custody,sourceDigest}',
   CASE WHEN v_legacy IS NULL THEN NULL ELSE v_user END,cur->>'source',nullif(cur->>'sessionId',''),
   (cur->>'revision')::bigint,cur->>'digest',CASE WHEN v_legacy IS NULL THEN NULL ELSE v_reset END,
   CASE WHEN v_legacy IS NULL THEN NULL ELSE v_fence END,v_legacy);
 END LOOP;
END $lazy_records$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_records(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_guard(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_guard$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;v_gen text;
BEGIN
 SELECT generation INTO v_gen FROM sessions.__96bulls_role_validation
  WHERE name='session_directory_search_worker' AND status='pass' FOR SHARE;
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF v_gen IS NULL OR pol.generation<>v_gen OR pol.valid_until<=clock_timestamp()
  OR c.source_identity_sha256<>pol.source_identity_sha256 OR c.control_epoch<>pol.control_epoch
  OR c.baseline->>'sourceProofSha256' IS DISTINCT FROM pol.source_proof_sha256 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody policy unavailable';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=p_org AND schema_generation=v_gen
  AND control_epoch=pol.control_epoch AND privacy_baseline_sha256=pol.privacy_baseline_sha256
  AND retention_contract_sha256=pol.retention_contract_sha256 AND valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered admission closed';END IF;
END $lazy_guard$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_guard(text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.ingest_receipt_seat_initialize(p jsonb) RETURNS jsonb
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
 IF NOT FOUND THEN
  PERFORM 1 FROM sessions.ingest_lazy_policy WHERE singleton AND generation=v_generation AND control_epoch=v_epoch
   AND source_identity_sha256=v_identity AND source_proof_sha256=p->>'sourceProofSha256' FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat epoch not owner bound';END IF;
 END IF;
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

CREATE OR REPLACE FUNCTION sessions.ingest_require_buffer_admission(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $admission$
DECLARE v_generation text;
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
  PERFORM sessions.ingest_lazy_guard(p_org);RETURN;
 END IF;
 SELECT generation INTO v_generation FROM sessions.__96bulls_role_validation
  WHERE name='session_directory_search_worker' AND status='pass' FOR SHARE;
 IF v_generation IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: generation not ready';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_admission
  WHERE org_id=p_org AND valid_until>clock_timestamp() AND schema_generation=v_generation FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered privacy or retention baseline not ready';END IF;
 PERFORM sessions.ingest_require_custody(p_org);
END $admission$;

CREATE OR REPLACE FUNCTION sessions.ingest_require_custody(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $require_custody$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
  PERFORM sessions.ingest_lazy_guard(p_org);RETURN;
 END IF;
 SELECT * INTO c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody clock missing';END IF;
 PERFORM 1 FROM sessions.ingest_custody_imports WHERE org_id=p_org AND status='complete'
  AND control_epoch=c.control_epoch AND source_identity_sha256=c.source_identity_sha256
  AND source_proof_sha256=c.baseline->>'sourceProofSha256' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody baseline incomplete';END IF;
 PERFORM 1 FROM sessions.ingest_custody_global g WHERE singleton
  AND g.generation=(SELECT generation FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass')
  AND g.control_epoch=c.control_epoch AND g.source_identity_sha256=c.source_identity_sha256
  AND g.source_proof_sha256=c.baseline->>'sourceProofSha256' AND g.org_inventory ? p_org FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: global custody baseline incomplete';END IF;
END $require_custody$;


CREATE FUNCTION sessions.ingest_lazy_ack(p jsonb,p_kind text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack$
DECLARE w jsonb;pol sessions.ingest_lazy_policy%ROWTYPE;v_org text;v_ws text;v_user text;v_version bigint;v_last bigint;
BEGIN
 w:=p#>'{custody,witness}';v_org:=p#>>'{tenant,orgId}';v_ws:=p#>>'{tenant,workspaceId}';v_user:=p#>>'{tenant,userId}';
 PERFORM sessions.ingest_lazy_guard(v_org);
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton;
 PERFORM 1 FROM sessions.ingest_lazy_user_seals s JOIN sessions.ingest_seed_fence_baselines b
  USING(org_id,workspace_id,user_id) WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.user_id=v_user
   AND s.source_identity_sha256=pol.source_identity_sha256 AND s.control_epoch=pol.control_epoch AND s.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: actual privacy seal required for ACK';END IF;
 v_version:=(w->>'sourceControlVersion')::bigint;
 IF p_kind='batch' THEN
  SELECT coalesce(max(version),0) INTO v_last FROM sessions.ingest_control_receipts
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND epoch=pol.control_epoch;
  IF v_last<>v_version THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source version changed before ACK';END IF;
 ELSE
  PERFORM 1 FROM sessions.ingest_control_receipts WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
   AND epoch=pol.control_epoch AND version=v_version AND kind=p_kind AND digest=p#>>'{control,digest}';
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: actual control receipt required for ACK';END IF;
 END IF;
 RETURN jsonb_build_object('version',1,'sourceDigest',p#>>'{custody,sourceDigest}','sourceControlVersion',v_version::text,'privacySealed',true);
END $lazy_ack$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_ack(jsonb,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_attempt(p_token uuid) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_attempt$
DECLARE a sessions.ingest_claim_attempts%ROWTYPE;pol sessions.ingest_lazy_policy%ROWTYPE;v_last bigint;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN RETURN;END IF;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=p_token;
 PERFORM sessions.ingest_lazy_guard(a.org_id);
 PERFORM sessions.ingest_lock_fences(a.org_id,a.workspace_id,a.user_id);
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton;
 IF a.source_control_version IS NULL OR a.source_identity_sha256 IS DISTINCT FROM pol.source_identity_sha256
  OR a.source_witness_sha256 IS NULL OR a.source_processing_ranks IS NULL OR a.schema_generation IS DISTINCT FROM pol.generation THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original witnessed attempt required';END IF;
 PERFORM 1 FROM sessions.ingest_lazy_user_seals s JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE s.org_id=a.org_id AND s.workspace_id=a.workspace_id AND s.user_id=a.user_id AND s.source_identity_sha256=pol.source_identity_sha256
   AND s.control_epoch=pol.control_epoch AND s.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: original privacy seal lost';END IF;
 SELECT coalesce(max(version),0) INTO v_last FROM sessions.ingest_control_receipts
  WHERE org_id=a.org_id AND workspace_id=a.workspace_id AND user_id=a.user_id AND epoch=pol.control_epoch;
 IF v_last<>a.source_control_version THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original source version changed';END IF;
END $lazy_attempt$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_attempt(uuid)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_attempt_ack(p_token uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_attempt_ack$
DECLARE a sessions.ingest_claim_attempts%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_lazy_attempt(p_token);
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=p_token;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id
  AND b.batch_id=a.batch_id AND b.user_id=a.user_id AND b.digest=a.digest AND b.state='committed';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: durable batch receipt required for ACK';END IF;
 RETURN jsonb_build_object('version',1,'sourceDigest',a.source_witness_sha256,'sourceControlVersion',a.source_control_version::text,'privacySealed',true);
END $lazy_attempt_ack$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_attempt_ack(uuid)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


CREATE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_batch$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 t sessions.ingest_stage_intents%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 d sessions.ingest_lazy_ack_decisions%ROWTYPE;r record;v_last bigint;v_key text;v_material jsonb;v_accept boolean;v_first text;v_new_lineage bigint;
BEGIN
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_lazy_user_seals u JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE u.org_id=p->>'org_id' AND u.workspace_id=p->>'workspace_id' AND u.user_id=p->>'user_id' AND u.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: real user seal required before any receipt';END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;v_last:=c.last_seat;
 IF p->>'buffer_receipt_kind'='suppressed' THEN RETURN;END IF;
 IF p->>'buffer_receipt_kind' IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unexpected receipt authority';END IF;
 SELECT i.* INTO t FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token
  WHERE o.org_id=p->>'org_id' AND o.workspace_id=p->>'workspace_id' AND o.user_id=p->>'user_id' AND o.origin_id=p->>'origin_id'
   AND o.batch_id=p->>'batch_id' AND o.digest=p->>'digest' AND o.state='buffered' FOR UPDATE OF i;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live'
  OR t.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current original stage required';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id
  OR a.batch_id<>t.batch_id OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch
  OR jsonb_array_length(a.source_processing_ranks)<>jsonb_array_length(a.records) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original candidate order changed';END IF;
 FOR r IN SELECT q.* FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q
  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP
  IF a.source_processing_ranks->>r.ord IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source processing rank missing';END IF;
  v_material:=jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,r.key_material->>7);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF NOT FOUND OR slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256
   OR slot.control_epoch<>c.control_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: initialized native custody slot required';END IF;
  IF slot.current_owner IS NOT NULL AND slot.current_owner<>a.user_id THEN
   RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
  v_accept:=slot.current_owner IS NULL OR slot.current_revision<(r.key_material->>8)::bigint;
  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;
  -- Every ignored old-source candidate receives a real target order reference;
  -- it never fabricates the source row's publication seat.
  IF NOT v_accept AND slot.current_seat_key IS NOT NULL THEN
   SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=slot.current_seat_key;
   IF s.first_digest<>v_first THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
  ELSE
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
   VALUES(encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
    r.key_material,v_last,v_first,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;
  END IF;
  v_new_lineage:=slot.lineage;
  IF v_accept THEN
   IF slot.lineage=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native lineage exhausted';END IF;
   v_new_lineage:=slot.lineage+1;
   UPDATE sessions.ingest_lazy_native_slots SET lineage=v_new_lineage,current_owner=a.user_id,
    current_source=r.key_material->>10,current_session=r.key_material->>11,current_revision=(r.key_material->>8)::bigint,
    current_digest=r.candidate_digest,current_reset_epoch=r.reset_epoch,current_fence_epoch=r.fence_epoch,
    current_seat_key=s.key_sha256,current_legacy_key=NULL WHERE key_sha256=v_key;
  END IF;
  INSERT INTO sessions.ingest_lazy_ack_decisions(claim_token,ord,native_key_sha256,native_lineage,candidate_key_sha256,candidate_digest,
   first_digest,body_accepted,seat_key_sha256,reset_epoch,fence_epoch,generation,source_control_version,source_witness_sha256)
  VALUES(t.claim_token,r.ord,v_key,v_new_lineage,r.key_sha256,r.candidate_digest,v_first,v_accept,s.key_sha256,r.reset_epoch,r.fence_epoch,
   a.schema_generation,a.source_control_version,a.source_witness_sha256) ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT d FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=t.claim_token AND ord=r.ord;
  IF d.native_key_sha256<>v_key OR d.native_lineage<>v_new_lineage OR d.candidate_key_sha256<>r.key_sha256 OR d.candidate_digest<>r.candidate_digest
   OR d.first_digest<>v_first OR d.body_accepted<>v_accept OR d.seat_key_sha256<>s.key_sha256 OR d.reset_epoch<>r.reset_epoch OR d.fence_epoch<>r.fence_epoch
   OR d.generation<>a.schema_generation OR d.source_control_version<>a.source_control_version OR d.source_witness_sha256<>a.source_witness_sha256 THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: immutable ACK decision adoption refused';END IF;
 END LOOP;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF t.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: stage expired while assigning seats';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
END $lazy_ack_batch$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_ack_batch(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_stage_seats(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_stage$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;
 a sessions.ingest_claim_attempts%ROWTYPE;v_rows jsonb;v_n integer;v_expected integer;v_last bigint;
BEGIN
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 SELECT * INTO STRICT t FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 PERFORM sessions.ingest_lazy_guard(t.org_id);
 IF t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live' OR NOT EXISTS(
  SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.active_stage_token=t.stage_token AND o.state='buffered'
   AND o.org_id=t.org_id AND o.workspace_id=t.workspace_id AND o.user_id=t.user_id AND o.origin_id=t.origin_id
   AND o.batch_id=t.batch_id AND o.digest=t.digest) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current acknowledged stage required';END IF;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id OR a.batch_id<>t.batch_id
  OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch OR a.source_identity_sha256 IS DISTINCT FROM c.source_identity_sha256 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current original candidate identity changed';END IF;
 WITH wanted AS MATERIALIZED(SELECT * FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals))
 SELECT jsonb_agg(jsonb_build_object('ord',r.ord,'seat',s.seat::text,'candidateDigest',r.candidate_digest,'firstDigest',d.first_digest,
  'bodyAccepted',d.body_accepted,'resetEpoch',r.reset_epoch::text,'fenceEpoch',r.fence_epoch::text) ORDER BY r.ord),
  count(*)::integer,(SELECT count(*)::integer FROM wanted) INTO v_rows,v_n,v_expected
 FROM wanted r JOIN sessions.ingest_lazy_ack_decisions d ON d.claim_token=t.claim_token AND d.ord=r.ord
  AND d.candidate_key_sha256=r.key_sha256 AND d.candidate_digest=r.candidate_digest AND d.reset_epoch=r.reset_epoch AND d.fence_epoch=r.fence_epoch
  AND d.generation=a.schema_generation AND d.source_control_version=a.source_control_version AND d.source_witness_sha256=a.source_witness_sha256
 JOIN sessions.ingest_receipt_seats s ON s.key_sha256=d.seat_key_sha256 AND s.first_digest=d.first_digest AND s.seat>c.allocated_high_water
 JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=d.native_key_sha256 AND n.org_id=a.org_id AND n.workspace_id=a.workspace_id
  AND n.origin_id=a.origin_id AND n.record_id=r.key_material->>7 AND n.source_identity_sha256=c.source_identity_sha256 AND n.control_epoch=c.control_epoch
 WHERE NOT d.body_accepted OR(s.key_material=r.key_material AND d.first_digest=r.candidate_digest);
 IF v_n=0 OR v_n<>v_expected THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: original ACK decisions missing';END IF;
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_rows)r WHERE (r->>'seat')::bigint>v_last) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK decision clock snapshot incomplete';END IF;
 RETURN jsonb_build_object('controlEpoch',c.control_epoch,'sourceIdentitySha256',c.source_identity_sha256,
  'allocatedHighWater',c.allocated_high_water::text,'lastAssignedSeat',v_last::text,'records',v_rows);
END $lazy_stage$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_stage_seats(uuid)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_clear(p jsonb,p_kind text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_clear$
BEGIN
 IF p_kind='selection' AND (p->>'enabled')::boolean THEN RETURN;END IF;
 PERFORM sessions.ingest_lock_fences(p#>>'{tenant,orgId}',p#>>'{tenant,workspaceId}',p#>>'{tenant,userId}');
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 UPDATE sessions.ingest_lazy_native_slots n SET lineage=n.lineage+1,current_owner=NULL,current_source=NULL,current_session=NULL,
  current_revision=NULL,current_digest=NULL,current_reset_epoch=NULL,current_fence_epoch=NULL,current_seat_key=NULL,current_legacy_key=NULL
 WHERE n.org_id=p#>>'{tenant,orgId}' AND n.workspace_id=p#>>'{tenant,workspaceId}' AND n.current_owner=p#>>'{tenant,userId}'
  AND n.current_session IS NOT NULL AND(p_kind='reset' OR(n.origin_id=p->>'originId' AND EXISTS(
   SELECT 1 FROM sessions.ingest_session_pairs(p->'sessions')s WHERE s.source=n.current_source AND s.session_id=n.current_session)));
END $lazy_clear$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_clear(jsonb,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.ingest_claim_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $buffered$
DECLARE v_generation text;v_result jsonb;v_lazy boolean;v_packet jsonb;v_known sessions.ingest_batches%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_lazy:=EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton);
 IF v_lazy THEN
  PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','receiptId','sessions','records','generation','custody']);
  PERFORM sessions.ingest_lazy_begin(p,'batch');v_packet:=p->'custody';
  SELECT * INTO v_known FROM sessions.ingest_batches b WHERE b.org_id=p#>>'{tenant,orgId}' AND b.workspace_id=p#>>'{tenant,workspaceId}'
   AND b.origin_id=p->>'originId' AND b.batch_id=p->>'batchId';
  IF NOT FOUND THEN PERFORM sessions.ingest_lazy_records(p);
  ELSIF v_known.user_id=p#>>'{tenant,userId}' AND v_known.digest=p->>'batchDigest' AND v_known.state<>'committed' THEN
   PERFORM sessions.ingest_lazy_records(p);
  END IF;
  p:=p-'custody';
 ELSE
  PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','receiptId','sessions','records','generation']);
 END IF;
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 PERFORM sessions.ingest_require_buffer_admission(p#>>'{tenant,orgId}');
 v_result:=sessions.ingest_claim_batch_mode(p-'generation','buffered');
 IF v_result->>'kind' IN('claimed','resume') THEN
  UPDATE sessions.ingest_claim_attempts SET schema_generation=v_generation,
   source_control_version=CASE WHEN v_lazy THEN (v_packet#>>'{witness,sourceControlVersion}')::bigint END,
   source_identity_sha256=CASE WHEN v_lazy THEN v_packet#>>'{witness,sourceIdentitySha256}' END,
   source_witness_sha256=CASE WHEN v_lazy THEN v_packet->>'sourceDigest' END,
   source_processing_ranks=CASE WHEN v_lazy THEN(SELECT coalesce(jsonb_agg(e->'processingRank' ORDER BY(e->>'preparedOrd')::integer),'[]')
    FROM jsonb_array_elements(v_packet#>'{witness,records}')q(e)) END
   WHERE claim_token=(v_result->>'claimToken')::uuid AND (schema_generation IS NULL OR schema_generation=v_generation);
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation collision';END IF;
 END IF;
 PERFORM sessions.ingest_require_buffer_admission(p#>>'{tenant,orgId}');
 IF v_lazy THEN v_result:=v_result||jsonb_build_object('custodyAck',
  sessions.ingest_lazy_ack(p||jsonb_build_object('custody',v_packet),'batch'));END IF;
 RETURN v_result;
END
$buffered$;

CREATE OR REPLACE FUNCTION sessions.ingest_prepare_buffer_stage(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $prepare$
DECLARE k record;a sessions.ingest_claim_attempts%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;v_generation text;
 v_key text;v_sha text;v_bytes bigint;v_groups jsonb;v_count integer;v_distinct integer;v_token uuid;v_pairs jsonb;v_fenced jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','key','sha256','bytes','groupOrdinals','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 k:=sessions.ingest_batch_key(p);
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);
 PERFORM sessions.ingest_lazy_attempt((p->>'claimToken')::uuid);
 PERFORM sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=(p->>'claimToken')::uuid;
 IF a.schema_generation IS DISTINCT FROM v_generation
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation changed';END IF;
 IF a.reset_epoch IS NULL OR a.records IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: attempt lacks buffered proof'; END IF;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id
  AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id AND b.user_id=k.user_id AND b.digest=k.digest
  AND b.publication_state='buffered' AND b.state='pending' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: buffered batch not pending'; END IF;
 v_key:=p->>'key';v_sha:=sessions.ingest_digest(p->'sha256');
 IF jsonb_typeof(p->'key') IS DISTINCT FROM 'string' OR octet_length(v_key) NOT BETWEEN 1 AND 2048
   OR position('/'||k.org_id||'/buffered-batches/v1/' IN v_key)=0
   OR v_key !~ ('-'||substr(encode(sha256(convert_to(p->>'claimToken','UTF8')),'hex'),1,32)||'\.staged\.ndjson\.zst$')
   OR v_key ~ '[\x01-\x1f\x7f]' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered key'; END IF;
 IF jsonb_typeof(p->'bytes') IS DISTINCT FROM 'number' OR (p->>'bytes') !~ '^[1-9][0-9]{0,7}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered bytes';
 END IF;
 v_bytes:=(p->>'bytes')::bigint;
 IF v_bytes>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: buffered bytes exceed limit'; END IF;
 v_groups:=p->'groupOrdinals';
 IF jsonb_typeof(v_groups) IS DISTINCT FROM 'array' OR jsonb_array_length(v_groups) NOT BETWEEN 1 AND 500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered groups';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_groups) g
    WHERE jsonb_typeof(g)<>'array' OR jsonb_array_length(g) NOT BETWEEN 1 AND 500) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered ordinal group';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_groups) g CROSS JOIN LATERAL jsonb_array_elements(g) o
   WHERE jsonb_typeof(o)<>'number' OR o::text !~ '^(0|[1-9][0-9]{0,2})$'
      OR NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(a.records) r(ord integer) WHERE r.ord=(o::text)::int)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: ordinal outside original claim';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_groups) g
  CROSS JOIN LATERAL (
   SELECT count(DISTINCT jsonb_build_array(r.source,r.session_id)) FILTER(WHERE r.session_id IS NOT NULL) cohorts,
          bool_or(r.session_id IS NULL) has_null,bool_or(r.session_id IS NOT NULL) has_session
    FROM jsonb_array_elements(g) o JOIN jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
      ON r.ord=(o::text)::int
  ) q WHERE q.cohorts>1 OR (q.has_null AND q.has_session)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: buffered group mixes session owners';
 END IF;
 SELECT count(*)::int,count(DISTINCT o)::int INTO v_count,v_distinct
  FROM jsonb_array_elements(v_groups) g CROSS JOIN LATERAL jsonb_array_elements(g) o;
 IF v_count<>v_distinct OR v_count>500 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: repeated buffered ordinal'; END IF;
 IF a.reset_epoch<>(SELECT count(*) FROM sessions.upload_resets r
    WHERE r.org_id=k.org_id AND r.workspace_id=k.workspace_id AND r.user_id=k.user_id) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: reset before buffered intent';
 END IF;
 -- Cover every currently live original ordinal exactly once; original metadata
 -- remains in SQL, so scrubbed record/session ids cannot affect this decision.
 SELECT coalesce(jsonb_agg(jsonb_build_array(r.source,r.session_id)),'[]'::jsonb) INTO v_pairs
 FROM (SELECT DISTINCT source,session_id FROM jsonb_to_recordset(a.records) r(source text,session_id text)
       WHERE session_id IS NOT NULL) r;
 v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,a.epochs,v_pairs);
 IF EXISTS(SELECT 1 FROM jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
   WHERE (r.ord IN(SELECT (o::text)::int FROM jsonb_array_elements(v_groups) g CROSS JOIN LATERAL jsonb_array_elements(g) o))
      IS DISTINCT FROM (r.session_id IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) f
          WHERE f->>0=r.source AND f->>1=r.session_id))) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: buffered ordinal coverage changed';
 END IF;
 SELECT * INTO t FROM sessions.ingest_stage_intents
  WHERE claim_token=a.claim_token AND stage_key=v_key FOR UPDATE;
 IF FOUND THEN
  IF t.stage_sha256<>v_sha OR t.stage_bytes<>v_bytes OR t.group_ordinals<>v_groups
    OR t.cleanup_state<>'live' OR t.receipt_state<>'preparing' OR t.expires_at<=clock_timestamp()
    OR t.parent_stage_token IS NOT NULL THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: buffered intent refused';
  END IF;
  PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('stageToken',t.stage_token,'expiresAt',t.expires_at);
 END IF;
 v_token:=gen_random_uuid();
 INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,
   stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,expires_at)
 VALUES(v_token,a.claim_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,
   v_key,v_sha,v_bytes,v_groups,a.reset_epoch,clock_timestamp()+interval '120 seconds')
 RETURNING * INTO t;
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('stageToken',t.stage_token,'expiresAt',t.expires_at);
END
$prepare$;

CREATE OR REPLACE FUNCTION sessions.ingest_commit_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $bcommit$
DECLARE k record;a sessions.ingest_claim_attempts%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;v_generation text;
 v_pairs jsonb;v_fenced jsonb;v_invalid boolean;v_stage uuid;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','stageToken','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 k:=sessions.ingest_batch_key(p);
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);
 PERFORM sessions.ingest_lazy_attempt((p->>'claimToken')::uuid);
 PERFORM sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
 v_stage:=(p->>'stageToken')::uuid;
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=(p->>'claimToken')::uuid;
 IF a.schema_generation IS DISTINCT FROM v_generation
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation changed';END IF;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id
  AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id AND b.user_id=k.user_id AND b.digest=k.digest
  AND b.publication_state='buffered' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: no buffered batch'; END IF;
 SELECT * INTO t FROM sessions.ingest_stage_intents WHERE stage_token=v_stage AND claim_token=a.claim_token FOR UPDATE;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.cleanup_state<>'live'
   OR (t.receipt_state='preparing' AND t.expires_at<=clock_timestamp()) THEN
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_array(r.source,r.session_id)),'[]'::jsonb) INTO v_pairs
 FROM (SELECT DISTINCT r.source,r.session_id FROM jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
  WHERE r.session_id IS NOT NULL AND r.ord IN(
   SELECT (o::text)::int FROM jsonb_array_elements(t.group_ordinals) g CROSS JOIN LATERAL jsonb_array_elements(g) o)) r;
 v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,a.epochs,v_pairs);
 v_invalid:=jsonb_array_length(v_fenced)>0 OR a.reset_epoch IS NULL OR a.reset_epoch<>(SELECT count(*) FROM sessions.upload_resets r
  WHERE r.org_id=k.org_id AND r.workspace_id=k.workspace_id AND r.user_id=k.user_id);
 IF v_invalid THEN
  UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1 WHERE stage_token=v_stage;
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 IF t.receipt_state='acknowledged' THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id
    AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id AND o.digest=k.digest AND o.active_stage_token=v_stage) THEN
   PERFORM sessions.ingest_require_buffer_admission(k.org_id);
   RETURN jsonb_build_object('kind','committed')||CASE WHEN EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
  THEN jsonb_build_object('custodyAck',sessions.ingest_lazy_attempt_ack((p->>'claimToken')::uuid)) ELSE '{}'::jsonb END;
  END IF;
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id
   AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id) THEN RETURN jsonb_build_object('kind','intent_invalid'); END IF;
 -- Receipt and durable recovery pointer are one transaction; no queue proof.
 INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,user_id,origin_id,batch_id,digest,active_stage_token)
 VALUES(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_stage);
 UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged',row_version=row_version+1 WHERE stage_token=v_stage;
 UPDATE sessions.ingest_batches b SET state='committed',committed_at=coalesce(b.committed_at,clock_timestamp()),buffer_receipt_kind='staged'
 WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id
  AND b.user_id=k.user_id AND b.digest=k.digest AND b.publication_state='buffered';
 IF clock_timestamp()>=t.expires_at THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered intent expired';END IF;
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('kind','committed')||CASE WHEN EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
  THEN jsonb_build_object('custodyAck',sessions.ingest_lazy_attempt_ack((p->>'claimToken')::uuid)) ELSE '{}'::jsonb END;
END
$bcommit$;

CREATE OR REPLACE FUNCTION sessions.ingest_apply_ordered_control(p jsonb,p_kind text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ordered$
DECLARE
 t jsonb;c jsonb;v_org text;v_ws text;v_user text;v_epoch text;v_version bigint;
 v_digest text;v_generation text;v_payload jsonb;v_origin text;v_request text;
 v_packet jsonb;v_lazy boolean;
 v_last bigint;v_old sessions.ingest_control_receipts%ROWTYPE;v_result jsonb;v_outcome text;
BEGIN
 v_lazy:=EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton);
 IF v_lazy THEN PERFORM sessions.ingest_lazy_begin(p,p_kind);v_packet:=p->'custody';p:=p-'custody';END IF;
 IF p_kind NOT IN('selection','reset') OR p_kind IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid control kind';
 END IF;
 IF p_kind='selection' THEN
  PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','enabled','generation','control']);
 ELSE
  PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','requestId','generation','control']);
 END IF;
 t:=p->'tenant';PERFORM sessions.ingest_exact_keys(t,ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(t->'orgId',false);
 v_ws:=sessions.ingest_identifier(t->'workspaceId',true);
 v_user:=sessions.ingest_identifier(t->'userId',false);
 c:=p->'control';PERFORM sessions.ingest_exact_keys(c,ARRAY['epoch','version','digest','kind']);
 v_epoch:=sessions.ingest_digest(c->'epoch');v_digest:=sessions.ingest_digest(c->'digest');
 IF c->>'kind' IS DISTINCT FROM p_kind OR jsonb_typeof(c->'version') IS DISTINCT FROM 'string'
   OR c->>'version'!~'^[1-9][0-9]{0,18}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid control tuple';
 END IF;
 BEGIN v_version:=(c->>'version')::bigint;
 EXCEPTION WHEN numeric_value_out_of_range THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: control version overflow';
 END;
 v_generation:=sessions.ingest_digest(p->'generation');
 -- Authority row locks precede USER/BATCH/INTENT and remain short SQL-only.
 PERFORM sessions.ingest_require_generation(v_generation);
 PERFORM sessions.ingest_require_buffer_admission(v_org);
 PERFORM 1 FROM sessions.ingest_buffer_admission
  WHERE org_id=v_org AND control_epoch=v_epoch AND schema_generation=v_generation FOR SHARE;
 IF NOT FOUND THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: foreign control epoch';
 END IF;
 IF p_kind='selection' THEN
  v_origin:=sessions.ingest_identifier(p->'originId',false);
  IF jsonb_typeof(p->'enabled') IS DISTINCT FROM 'boolean' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid selection';
  END IF;
  -- Validate every pair even for a stale request. Preserve source array order
  -- and public object field names in the PG-jsonb digest shared with Neon.
  PERFORM 1 FROM sessions.ingest_session_pairs(p->'sessions');
  v_payload:=jsonb_build_object('origin_id',v_origin,'sessions',
   (SELECT coalesce(jsonb_agg(jsonb_build_object('source',e->>0,'session_id',e->>1) ORDER BY n),'[]'::jsonb)
    FROM jsonb_array_elements(p->'sessions') WITH ORDINALITY AS x(e,n)),
   'enabled',p->'enabled');
 ELSE
  v_request:=sessions.ingest_identifier(p->'requestId',false);
  v_payload:=jsonb_build_object('request_id',v_request);
 END IF;
 IF encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',v_epoch,v_org,v_ws,v_user,p_kind,v_payload)::text,'UTF8')),'hex')<>v_digest THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: control digest mismatch';
 END IF;
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT version INTO v_last FROM sessions.ingest_control_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND epoch=v_epoch
  ORDER BY version DESC LIMIT 1 FOR UPDATE;
 v_last:=coalesce(v_last,0);
 IF v_version<=v_last THEN
  SELECT * INTO v_old FROM sessions.ingest_control_receipts
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND epoch=v_epoch AND version=v_version;
  IF NOT FOUND OR v_old.digest<>v_digest OR v_old.kind<>p_kind THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: conflicting control replay';
  END IF;
  v_result:=v_old.result;
  v_outcome:=CASE WHEN v_version=v_last THEN 'replay' ELSE 'stale' END;
 ELSE
  IF v_last=9223372036854775807 OR v_version<>v_last+1 THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: control version gap';
  END IF;
  IF p_kind='selection' THEN
   v_result:=jsonb_build_object('updated',sessions.ingest_apply_selection(v_org,v_ws,v_user,v_origin,p->'sessions',(p->>'enabled')::boolean));
  ELSE
   v_result:=jsonb_build_object('deleted',sessions.ingest_apply_reset(v_org,v_ws,v_user,v_request));
  END IF;
  INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation)
   VALUES(v_org,v_ws,v_user,v_epoch,v_version,v_digest,p_kind,v_result,v_generation);
  IF v_lazy THEN PERFORM sessions.ingest_lazy_clear(p,p_kind);END IF;
  v_outcome:='applied';
 END IF;
 PERFORM sessions.ingest_require_buffer_admission(v_org);
 v_result:=v_result||jsonb_build_object('control',jsonb_build_object('epoch',v_epoch,
  'version',v_version::text,'digest',v_digest,'kind',p_kind,'outcome',v_outcome));
 IF v_lazy THEN v_result:=v_result||jsonb_build_object('custodyAck',sessions.ingest_lazy_ack(p||jsonb_build_object('custody',v_packet),p_kind));END IF;
 RETURN v_result;
END
$ordered$;

CREATE OR REPLACE FUNCTION sessions.ingest_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selection$
DECLARE t jsonb;v_org text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_org:=p#>>'{tenant,orgId}';
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) OR p?'control' OR p?'generation'
  OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org)
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND publication_state='buffered') THEN
  RETURN sessions.ingest_apply_ordered_control(p,'selection');
 END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','enabled']);
 t:=p->'tenant';PERFORM sessions.ingest_exact_keys(t,ARRAY['orgId','workspaceId','userId']);
 IF jsonb_typeof(p->'enabled') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: enabled is malformed';
 END IF;
 RETURN jsonb_build_object('updated',sessions.ingest_apply_selection(
  sessions.ingest_identifier(t->'orgId',false),sessions.ingest_identifier(t->'workspaceId',true),
  sessions.ingest_identifier(t->'userId',false),sessions.ingest_identifier(p->'originId',false),
  p->'sessions',(p->>'enabled')::boolean));
END $selection$;

CREATE OR REPLACE FUNCTION sessions.ingest_reset_uploaded_sessions(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $reset$
DECLARE t jsonb;v_org text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_org:=p#>>'{tenant,orgId}';
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) OR p?'control' OR p?'generation'
  OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org)
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND publication_state='buffered') THEN
  RETURN sessions.ingest_apply_ordered_control(p,'reset');
 END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','requestId']);
 t:=p->'tenant';PERFORM sessions.ingest_exact_keys(t,ARRAY['orgId','workspaceId','userId']);
 RETURN jsonb_build_object('deleted',sessions.ingest_apply_reset(
  sessions.ingest_identifier(t->'orgId',false),sessions.ingest_identifier(t->'workspaceId',true),
  sessions.ingest_identifier(t->'userId',false),sessions.ingest_identifier(p->'requestId',false)));
END $reset$;

CREATE OR REPLACE FUNCTION sessions.ingest_claim_batch_mode(p jsonb,p_mode text)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $claim$
DECLARE
  k record;
  v_receipt text;
  v_batch sessions.ingest_batches%ROWTYPE;
  v_state text;
  v_pairs jsonb;
  v_items jsonb;
  v_live jsonb;
  v_deleted jsonb;
  v_conflicts jsonb;
  v_count integer;
  v_stale jsonb;
  v_resets integer;
  v_fast boolean;
  v_inserted boolean;
  v_token uuid;
  v_epochs jsonb;
BEGIN
 IF p_mode='canonical' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit buffered witness required';END IF;
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF p_mode NOT IN ('buffered','canonical') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid publication mode'; END IF;
  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['tenant', 'originId', 'batchId', 'batchDigest', 'receiptId', 'sessions', 'records']);
  k := sessions.ingest_batch_key(p);
  IF jsonb_typeof(p -> 'receiptId') IS DISTINCT FROM 'string'
     OR (p ->> 'receiptId') !~ '^rhr_[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: receiptId is malformed';
  END IF;
  v_receipt := p ->> 'receiptId';
  -- Validated and normalized once; every later step reads these values only. No
  -- temporary table: a definer function must not read objects the caller can create.
  v_pairs := (SELECT coalesce(jsonb_agg(jsonb_build_array(x.source, x.session_id)), '[]'::jsonb)
                FROM sessions.ingest_session_pairs(p -> 'sessions') AS x);
  v_items := sessions.ingest_records(p -> 'records');

  IF p_mode='canonical' AND (
    EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=k.org_id) OR
    EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=k.org_id AND publication_state='buffered')) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit buffered publication authority required';
  END IF;
  PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);

  -- (a) A known batch id: its receipt, a resume, or a batch-id 409. The primary key
  -- decides a race between two first claims (the insert below), not a lock.
  SELECT * INTO v_batch FROM sessions.ingest_batches AS b
   WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
     AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
  IF FOUND THEN
    IF v_batch.user_id<>k.user_id THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: batch owner mismatch'; END IF;
    IF v_batch.digest <> k.digest THEN
      RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
    END IF;
    IF v_batch.state = 'committed' THEN
      IF v_batch.publication_state='buffered' AND v_batch.buffer_receipt_kind IS DISTINCT FROM 'suppressed'
        AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox AS o
          LEFT JOIN sessions.ingest_stage_intents AS t ON t.stage_token=o.active_stage_token
          WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id
            AND o.user_id=k.user_id AND o.digest=k.digest AND
            (o.state='erased' OR (t.receipt_state='acknowledged' AND t.cleanup_state='live'))) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered receipt proof missing';
      END IF;
      RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
    END IF;
    IF v_batch.publication_state<>p_mode THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit publication transition required'; END IF;
    v_state := 'resume';
  ELSE
    v_state := 'claimed';
  END IF;

  IF p_mode='buffered' THEN PERFORM sessions.ingest_require_buffer_admission(k.org_id); END IF;

  -- (b) The batch's sessions this user deleted from this origin. As Neon 0038, their
  -- records are dropped before the revision checks (the receipt still acknowledges
  -- them), so only the rest is judged below.
  SELECT coalesce(jsonb_agg(jsonb_build_array(d.source, d.session_id)
           ORDER BY d.source COLLATE "C", d.session_id COLLATE "C"), '[]'::jsonb)
    INTO v_deleted
    FROM sessions.upload_deletions AS d
    JOIN (SELECT x.pair ->> 0 AS source, x.pair ->> 1 AS session_id FROM jsonb_array_elements(v_pairs) AS x(pair)
          UNION
          SELECT i.item ->> 'source', i.item ->> 'session_id' FROM jsonb_array_elements(v_items) AS i(item)
           WHERE i.item ->> 'session_id' IS NOT NULL) AS s
      ON s.source = d.source AND s.session_id = d.session_id
   WHERE d.org_id = k.org_id AND d.workspace_id = k.workspace_id
     AND d.user_id = k.user_id AND d.origin_id = k.origin_id;
  v_live := (SELECT coalesce(jsonb_agg(i.item ORDER BY (i.item ->> 'ord')::integer), '[]'::jsonb)
               FROM jsonb_array_elements(v_items) AS i(item)
              WHERE i.item ->> 'session_id' IS NULL
                 OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_deleted) AS d(pair)
                                 WHERE d.pair ->> 0 = i.item ->> 'source' AND d.pair ->> 1 = i.item ->> 'session_id'));

  -- (c) #250's revision rules over the committed revisions, read without a lock
  -- (the primary key serves record_id = ANY of the batch's ids). In identity order
  -- (record_id, revision, revision_id): a record whose stored revision is at or
  -- above its own is stale (a replay, whatever its digest); a NEW revision (none
  -- stored, or the stored one lower) must not be reused in the batch, by its first
  -- peer in identity order, with both another revision id and another digest.
  WITH sorted AS (
    SELECT i.*, row_number() OVER (
             ORDER BY i.record_id COLLATE "C", i.revision, i.revision_id COLLATE "C", i.ord) AS rn
      FROM jsonb_to_recordset(v_live) AS i(ord integer, source text, session_id text,
                                            record_id text, revision_id text, revision bigint, digest text)
  ), stored AS MATERIALIZED (
    SELECT c.record_id, c.revision
      FROM sessions.ingest_record_revisions AS c
     WHERE c.org_id = k.org_id AND c.workspace_id = k.workspace_id AND c.origin_id = k.origin_id
       AND c.record_id = ANY (ARRAY(SELECT DISTINCT s.record_id FROM sorted AS s))
       AND NOT(p_mode='buffered' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton))
    UNION ALL
    SELECT n.record_id,n.current_revision AS revision FROM sessions.ingest_lazy_native_slots n
     WHERE p_mode='buffered' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
      AND n.org_id=k.org_id AND n.workspace_id=k.workspace_id AND n.origin_id=k.origin_id
      AND n.current_owner=k.user_id AND n.current_revision IS NOT NULL
      -- Probe the existing global-native PK for this bounded batch. An owner
      -- index alone would scan every current record the user has ever uploaded.
      AND n.key_sha256=ANY(ARRAY(SELECT encode(sha256(convert_to(
        jsonb_build_array('global-native-custody-v1',k.org_id,k.workspace_id,k.origin_id,s.record_id)::text,'UTF8')),'hex')
        FROM sorted s))
      AND n.record_id=ANY(ARRAY(SELECT DISTINCT s.record_id FROM sorted s))
  ), peers AS (
    SELECT DISTINCT ON (s.record_id, s.revision) s.record_id, s.revision,
           s.revision_id AS peer_revision_id, s.digest AS peer_digest
      FROM sorted AS s ORDER BY s.record_id, s.revision, s.rn
  ), judged AS (
    SELECT s.rn, s.ord, s.record_id, s.revision_id, s.revision, s.digest,
           c.revision IS NOT NULL AND c.revision >= s.revision AS stale,
           (c.revision IS NULL OR c.revision < s.revision OR
             (c.revision=s.revision AND p_mode='buffered' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)))
             AND p.peer_revision_id <> s.revision_id AND p.peer_digest <> s.digest AS twin,
           p.peer_revision_id, p.peer_digest
      FROM sorted AS s
      JOIN peers AS p ON p.record_id = s.record_id AND p.revision = s.revision
      LEFT JOIN stored AS c ON c.record_id = s.record_id
  ), numbered AS (
    SELECT j.*, row_number() OVER (PARTITION BY j.twin ORDER BY j.rn) AS twin_rn FROM judged AS j
  )
  SELECT count(*) FILTER (WHERE n.twin)::integer,
         coalesce(jsonb_agg(jsonb_build_object(
           'recordId', n.record_id,
           'submittedRevisionId', n.revision_id,
           'submittedRevision', n.revision,
           'submittedDigest', n.digest,
           'currentRevisionId', n.peer_revision_id,
           'currentRevision', n.revision,
           'currentDigest', n.peer_digest) ORDER BY n.rn) FILTER (WHERE n.twin AND n.twin_rn <= 100), '[]'::jsonb),
         coalesce(jsonb_agg(n.ord ORDER BY n.ord) FILTER (WHERE n.stale), '[]'::jsonb),
         bool_and(n.stale) IS NOT FALSE
    INTO v_count, v_conflicts, v_stale, v_fast
    FROM numbered AS n;
  IF v_count > 0 THEN
    IF p_mode='buffered' AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
      RETURN jsonb_build_object('kind','baseline_pending'); END IF;
    RETURN jsonb_build_object('kind', 'revision_conflict', 'conflicts', v_conflicts, 'count', v_count);
  END IF;

  IF p_mode='buffered' THEN
    -- Unknown historical maxima cannot suppress accepted bytes. Potential twins
    -- above were deferred rather than incorrectly409ing a possibly stale batch.
    v_stale:='[]'::jsonb;
    v_fast:=jsonb_array_length(v_live)=0;
  END IF;

  SELECT count(*)::integer INTO v_resets FROM sessions.upload_resets AS r
   WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id AND r.user_id = k.user_id;

  -- (d) The claim. When nothing needs writing (every record stale or deleted), the
  -- batch is committed here and the caller writes nothing (replay_fast).
  IF v_state = 'claimed' THEN
    INSERT INTO sessions.ingest_batches
           (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at,publication_state,buffer_receipt_kind)
    VALUES (k.org_id, k.workspace_id, k.origin_id, k.batch_id, k.user_id, k.digest, v_receipt,
            CASE WHEN v_fast THEN 'committed' ELSE 'pending' END, CASE WHEN v_fast THEN now() END,p_mode,CASE WHEN p_mode='buffered' AND v_fast THEN 'suppressed' END)
    ON CONFLICT (org_id, workspace_id, origin_id, batch_id) DO NOTHING
    RETURNING true INTO v_inserted;
    IF v_inserted IS NULL THEN
      -- A concurrent first claim of this batch id won: answer as a retry would.
      SELECT * INTO v_batch FROM sessions.ingest_batches AS b
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
      IF v_batch.user_id<>k.user_id THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: batch owner mismatch'; END IF;
      IF v_batch.digest <> k.digest THEN
        RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
      END IF;
      IF v_batch.state = 'committed' THEN
        RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
      END IF;
      v_state := 'resume';
    END IF;
  END IF;
  IF v_fast THEN
    IF v_state = 'resume' THEN
      UPDATE sessions.ingest_batches AS b
         SET state = 'committed', committed_at = coalesce(b.committed_at, now()),
             buffer_receipt_kind=CASE WHEN p_mode='buffered' THEN 'suppressed' ELSE b.buffer_receipt_kind END
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id AND b.digest = k.digest;
    END IF;
    v_state := 'replay_fast';
  END IF;

  INSERT INTO sessions.ingest_session_ownership
    (org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT k.org_id,k.workspace_id,k.user_id,k.origin_id,s.source,s.session_id
  FROM (SELECT x.source,x.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS x
        UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_live) AS i(source text,session_id text)
          WHERE i.session_id IS NOT NULL) AS s
  WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions AS d
    WHERE d.org_id=k.org_id AND d.workspace_id=k.workspace_id AND d.user_id=k.user_id
      AND d.origin_id=k.origin_id AND d.source=s.source AND d.session_id=s.session_id)
  ON CONFLICT DO NOTHING;
  IF v_state IN ('claimed','resume') THEN
    v_token:=gen_random_uuid();
    SELECT coalesce(jsonb_agg(jsonb_build_object('source',x.source,'session_id',x.session_id,'epoch',coalesce(f.epoch,0),'reset_epoch',v_resets)
      ORDER BY x.source COLLATE "C",x.session_id COLLATE "C"),'[]'::jsonb) INTO v_epochs
    FROM (SELECT q.source,q.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS q
          UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
           WHERE i.session_id IS NOT NULL) AS x
    LEFT JOIN sessions.ingest_fence_versions AS f ON f.org_id=k.org_id AND f.workspace_id=k.workspace_id
      AND f.user_id=k.user_id AND f.origin_id=k.origin_id AND f.source=x.source AND f.session_id=x.session_id;
    INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch)
    VALUES(v_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_epochs,v_items,v_resets);
  END IF;
  RETURN jsonb_build_object('kind', v_state, 'claimToken',v_token, 'deleted', v_deleted, 'stale', v_stale, 'resets', v_resets);
END
$claim$;

CREATE OR REPLACE FUNCTION sessions.ingest_receipt_seat_ack() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ack$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;
 a sessions.ingest_claim_attempts%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 m sessions.ingest_receipt_ord_seats%ROWTYPE;h sessions.ingest_receipt_record_heads%ROWTYPE;
 l sessions.ingest_custody_records%ROWTYPE;v_legacy text;
 r record;v_epoch text;v_generation text;v_last bigint;v_alias jsonb;v_alias_hash text;v_accepted boolean;
BEGIN
 IF NEW.publication_state<>'buffered' OR NEW.state<>'committed' THEN RETURN NEW;END IF;
 IF TG_OP='UPDATE' AND OLD.state='committed' THEN RETURN NEW;END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN PERFORM sessions.ingest_lazy_ack_batch(to_jsonb(NEW));RETURN NEW;END IF;
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
 -- Full-import source identity attests native C.UTF-8 ordering. bytea keys
 -- reproduce that source order without the target database's ambient collation.
 FOR r IN SELECT w.* FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) w
  JOIN jsonb_to_recordset(a.records) j(ord integer,record_id text,revision bigint,revision_id text) ON j.ord=w.ord
  ORDER BY convert_to(j.record_id,'UTF8'),j.revision,convert_to(j.revision_id,'UTF8'),w.ord LOOP
  -- Source/session aliases share a first-body decision at ACK time. The
  -- current winner's epoch is consulted ONLY here, never when draining old ACKs.
  v_alias:=jsonb_build_array('buffer-record-first-body-v1',c.control_epoch,c.source_identity_sha256,
   a.org_id,a.workspace_id,a.user_id,a.origin_id,r.key_material->7,a.reset_epoch::text);
  v_alias_hash:=encode(sha256(convert_to(v_alias::text,'UTF8')),'hex');
  SELECT * INTO h FROM sessions.ingest_receipt_record_heads WHERE key_sha256=v_alias_hash FOR UPDATE;
  s:=NULL;l:=NULL;v_legacy:=NULL;
  IF FOUND THEN
   IF h.key_material IS DISTINCT FROM v_alias THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: record head collision';END IF;
   IF h.legacy_key_sha256 IS NOT NULL THEN
    SELECT * INTO STRICT l FROM sessions.ingest_custody_records WHERE key_sha256=h.legacy_key_sha256;
    IF l.org_id<>a.org_id OR l.workspace_id<>a.workspace_id OR l.user_id<>a.user_id OR l.origin_id<>a.origin_id
     OR l.record_id IS DISTINCT FROM r.key_material->>7 OR l.reset_epoch<>a.reset_epoch THEN
     RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: legacy custody identity changed';END IF;
    IF (l.session_id IS NOT NULL AND (
     coalesce((SELECT f.epoch FROM sessions.ingest_fence_versions f WHERE f.org_id=a.org_id AND f.workspace_id=a.workspace_id
      AND f.user_id=a.user_id AND f.origin_id=a.origin_id AND f.source=l.source AND f.session_id=l.session_id),0)<>l.fence_epoch
     OR EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=a.org_id AND d.workspace_id=a.workspace_id
      AND d.user_id=a.user_id AND d.origin_id=a.origin_id AND d.source=l.source AND d.session_id=l.session_id)))
     OR (r.key_material->>8)::bigint>l.revision THEN l:=NULL;END IF;
   ELSE
    SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=h.winner_key_sha256;
    IF s.session_id IS NOT NULL AND (
     coalesce((SELECT f.epoch FROM sessions.ingest_fence_versions f
      WHERE f.org_id=a.org_id AND f.workspace_id=a.workspace_id AND f.user_id=a.user_id
       AND f.origin_id=a.origin_id AND f.source=s.source AND f.session_id=s.session_id),0)<>s.fence_epoch
     OR EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=a.org_id AND d.workspace_id=a.workspace_id
       AND d.user_id=a.user_id AND d.origin_id=a.origin_id AND d.source=s.source AND d.session_id=s.session_id)) THEN s:=NULL;END IF;
   END IF;
  END IF;
  -- A higher revision replaces custody even when the previous session remains
  -- enabled. Once that current custody is erased, a fresh lower revision may
  -- establish a new lifecycle; historical alias heads must never resurrect it.
  IF s.key_sha256 IS NOT NULL AND (r.key_material->>8)::bigint>(s.key_material->>8)::bigint THEN s:=NULL;END IF;
  IF l.key_sha256 IS NOT NULL THEN
   -- This is a real target ACK-order reference for an ignored candidate, never
   -- a fabricated publication seat for the old source row. Keep the immutable
   -- source-row FK so even an equal digest cannot turn this false decision true.
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;v_legacy:=l.key_sha256;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
    VALUES(encode(sha256(convert_to(jsonb_build_array('buffer-legacy-replay-reference-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
     r.key_material,v_last,l.digest,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;
  ELSIF s.key_sha256 IS NULL THEN
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
    VALUES(encode(sha256(convert_to(jsonb_build_array('buffer-record-lifecycle-seat-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
     r.key_material,v_last,r.candidate_digest,r.key_material->>10,r.key_material->>11,r.fence_epoch)
    RETURNING * INTO s;
   INSERT INTO sessions.ingest_receipt_record_heads(key_sha256,key_material,winner_key_sha256,legacy_key_sha256)
    VALUES(v_alias_hash,v_alias,s.key_sha256,NULL) ON CONFLICT(key_sha256) DO UPDATE
     SET winner_key_sha256=EXCLUDED.winner_key_sha256,legacy_key_sha256=NULL;
  END IF;
  v_accepted:=v_legacy IS NULL AND s.key_material=r.key_material AND s.first_digest=r.candidate_digest;
  INSERT INTO sessions.ingest_receipt_ord_seats(claim_token,ord,key_sha256,candidate_key_sha256,body_accepted,candidate_digest,reset_epoch,fence_epoch,legacy_custody_key_sha256)
   VALUES(t.claim_token,r.ord,s.key_sha256,r.key_sha256,v_accepted,r.candidate_digest,r.reset_epoch,r.fence_epoch,v_legacy)
   ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT m FROM sessions.ingest_receipt_ord_seats WHERE claim_token=t.claim_token AND ord=r.ord;
  IF m.legacy_custody_key_sha256 IS DISTINCT FROM v_legacy OR m.key_sha256<>s.key_sha256 OR m.candidate_key_sha256<>r.key_sha256 OR m.body_accepted<>v_accepted
   OR m.candidate_digest<>r.candidate_digest OR m.reset_epoch<>r.reset_epoch OR m.fence_epoch<>r.fence_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: seat ordinal adoption refused';END IF;
 END LOOP;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF clock_timestamp()>=t.expires_at THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: ACK seat lease expired';END IF;
 PERFORM sessions.ingest_require_buffer_admission(NEW.org_id);
 RETURN NEW;
END $ack$;

CREATE OR REPLACE FUNCTION sessions.ingest_receipt_seats_for_stage(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $stage$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;
 a sessions.ingest_claim_attempts%ROWTYPE;v_rows jsonb;v_count integer;v_expected integer;v_last bigint;
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN RETURN sessions.ingest_lazy_stage_seats(p_stage);END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 SELECT * INTO t FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 IF NOT FOUND OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live' OR NOT EXISTS(
  SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.active_stage_token=t.stage_token AND o.state='buffered'
   AND o.org_id=t.org_id AND o.workspace_id=t.workspace_id AND o.user_id=t.user_id
   AND o.origin_id=t.origin_id AND o.batch_id=t.batch_id AND o.digest=t.digest) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current seat stage required';
 END IF;
 PERFORM sessions.ingest_require_custody(t.org_id);
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
  AND s.seat>c.allocated_high_water
 LEFT JOIN sessions.ingest_custody_records l ON l.key_sha256=m.legacy_custody_key_sha256
 LEFT JOIN sessions.ingest_custody_imports ci ON ci.import_token=l.import_token
 WHERE (m.legacy_custody_key_sha256 IS NULL
   AND m.body_accepted=(s.key_material=r.key_material AND s.first_digest=r.candidate_digest))
 OR (m.legacy_custody_key_sha256 IS NOT NULL AND NOT m.body_accepted
   AND l.org_id=a.org_id AND l.workspace_id=a.workspace_id AND l.user_id=a.user_id AND l.origin_id=a.origin_id
   AND l.record_id=r.key_material->>7 AND l.revision>=(r.key_material->>8)::bigint
   AND l.reset_epoch=r.reset_epoch AND l.digest=s.first_digest AND s.key_material=r.key_material
   AND ci.org_id=a.org_id AND ci.status='complete' AND ci.control_epoch=c.control_epoch
   AND ci.source_identity_sha256=c.source_identity_sha256 AND ci.source_proof_sha256=c.baseline->>'sourceProofSha256');
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

-- Per-record retention authority. ACKed bytes follow the CURRENT native winner,
-- not the original group's epochs; preparing bytes remain bound to their claim.
ALTER TABLE sessions.ingest_stage_intents ADD COLUMN gc_keep_ordinals jsonb
 CHECK(gc_keep_ordinals IS NULL OR jsonb_typeof(gc_keep_ordinals)='array');
CREATE FUNCTION sessions.ingest_buffer_gc_ordinals(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $gc_ordinals$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 v_groups jsonb;v_result jsonb;v_count int;v_last bigint;
BEGIN
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) OR s.receipt_state='preparing' THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
   SELECT coalesce(max(version),0) INTO v_last FROM sessions.ingest_control_receipts r
    WHERE r.org_id=a.org_id AND r.workspace_id=a.workspace_id AND r.user_id=a.user_id
     AND r.epoch=(SELECT control_epoch FROM sessions.ingest_lazy_policy WHERE singleton);
   IF a.source_control_version IS DISTINCT FROM v_last THEN RETURN '[]';END IF;
  END IF;
  v_groups:=sessions.ingest_buffer_gc_kept(p_stage);
  SELECT coalesce(jsonb_agg(o ORDER BY o::text::int),'[]') INTO v_result
   FROM jsonb_array_elements(v_groups)g
   CROSS JOIN LATERAL jsonb_array_elements(s.group_ordinals->(g::text)::int)o;
  RETURN v_result;
 END IF;
 -- The shared clock prevents a concurrent different-USER ACK from changing a
 -- native slot between this decision and the short prepare/swap transaction.
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE;
 IF ROW(a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest,a.reset_epoch)
  IS DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest,s.reset_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: original identity changed';END IF;
 WITH wanted AS MATERIALIZED(SELECT * FROM sessions.ingest_receipt_seat_records(s.claim_token,s.group_ordinals))
 SELECT count(*) INTO v_count FROM wanted r JOIN sessions.ingest_lazy_ack_decisions d ON d.claim_token=s.claim_token AND d.ord=r.ord
  AND d.candidate_key_sha256=r.key_sha256 AND d.candidate_digest=r.candidate_digest
  AND d.reset_epoch=r.reset_epoch AND d.fence_epoch=r.fence_epoch;
 IF v_count<>(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: immutable ACK decisions required';END IF;
 WITH wanted AS MATERIALIZED(SELECT * FROM sessions.ingest_receipt_seat_records(s.claim_token,s.group_ordinals))
 SELECT coalesce(jsonb_agg(r.ord ORDER BY r.ord),'[]') INTO v_result
 FROM wanted r JOIN sessions.ingest_lazy_ack_decisions d ON d.claim_token=s.claim_token AND d.ord=r.ord
 JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=d.native_key_sha256
  AND n.org_id=s.org_id AND n.workspace_id=s.workspace_id AND n.origin_id=s.origin_id AND n.record_id=r.key_material->>7
 WHERE d.body_accepted AND n.current_owner=s.user_id AND n.current_seat_key=d.seat_key_sha256
  AND n.current_digest=d.candidate_digest AND n.current_revision=(r.key_material->>8)::bigint
  AND n.current_source IS NOT DISTINCT FROM r.key_material->>10
  AND n.current_session IS NOT DISTINCT FROM r.key_material->>11
  AND NOT EXISTS(SELECT 1 FROM sessions.upload_deletions f WHERE f.org_id=s.org_id AND f.workspace_id=s.workspace_id
   AND f.user_id=s.user_id AND f.origin_id=s.origin_id AND f.source=n.current_source AND f.session_id=n.current_session);
 RETURN v_result;
END $gc_ordinals$;
REVOKE ALL ON FUNCTION sessions.ingest_buffer_gc_ordinals(uuid)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_buffer_gc_groups(p_groups jsonb,p_keep jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $gc_groups$
 SELECT coalesce(jsonb_agg(filtered ORDER BY idx),'[]') FROM(
  SELECT g.idx,jsonb_agg(o.value ORDER BY o.idx) FILTER(WHERE p_keep @> jsonb_build_array(o.value)) AS filtered
  FROM jsonb_array_elements(p_groups) WITH ORDINALITY g(value,idx)
  CROSS JOIN LATERAL jsonb_array_elements(g.value) WITH ORDINALITY o(value,idx)
  GROUP BY g.idx
 )q WHERE filtered IS NOT NULL
$gc_groups$;
REVOKE ALL ON FUNCTION sessions.ingest_buffer_gc_groups(jsonb,jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE v_limit int;v_generation text;candidate record;s sessions.ingest_stage_intents%ROWTYPE;
 child sessions.ingest_stage_intents%ROWTYPE;parent sessions.ingest_stage_intents%ROWTYPE;
 v_keep jsonb;v_ordinals jsonb;v_phase text;v_ticket jsonb;v_tickets jsonb:='[]';v_has_outbox boolean;v_abort boolean;v_token uuid;v_outbox_version bigint;
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
   v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
  ELSE
   v_ordinals:=sessions.ingest_buffer_gc_ordinals(s.stage_token);
   SELECT coalesce(jsonb_agg(idx-1 ORDER BY idx),'[]') INTO v_keep
    FROM jsonb_array_elements(s.group_ordinals) WITH ORDINALITY g(value,idx)
    WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(value)o WHERE v_ordinals @> jsonb_build_array(o));
   IF s.receipt_state='preparing' THEN
    IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: preparing current object';END IF;
    v_abort:=s.expires_at<=clock_timestamp() OR jsonb_array_length(v_ordinals)<>(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o);
    IF NOT v_abort THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     CONTINUE;
    END IF;
    v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
   ELSE
    IF NOT v_has_outbox THEN
     -- Acknowledged but unreferenced intent is a known retired object.
     v_abort:=true;v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
    ELSIF jsonb_array_length(v_ordinals)=(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o) THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     CONTINUE;
    ELSE v_phase:='plan';END IF;
   END IF;
  END IF;
  -- A stale prospective child is never adopted into another keep set.
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token FOR UPDATE;
   IF NOT FOUND OR child.cleanup_state<>'live' OR child.group_ordinals IS DISTINCT FROM
     sessions.ingest_buffer_gc_groups(s.group_ordinals,v_ordinals)
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
   row_version=row_version+1,gc_lease_version=row_version+1,gc_checked_at=clock_timestamp(),gc_keep_groups=v_keep,gc_keep_ordinals=v_ordinals,
   gc_replacement_stage_token=s.gc_replacement_stage_token,gc_outbox_version=v_outbox_version,
   cleanup_state=CASE WHEN v_abort THEN 'retired' ELSE cleanup_state END
   WHERE stage_token=s.stage_token RETURNING * INTO s;
  v_ticket:=jsonb_build_object('stageToken',s.stage_token,'claimToken',s.claim_token,'leaseToken',v_token,
   'rowVersion',s.row_version::text,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'keepGroupIndexes',v_keep,'keepOrdinals',v_ordinals,'receiptState',s.receipt_state,'phase',v_phase,
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

CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_prepare(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $prepare$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;c sessions.ingest_stage_intents%ROWTYPE;
 v_groups jsonb;v_key text;v_sha text;v_bytes bigint;v_old text[];v_new text[];v_token uuid;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','leaseToken','key','sha256','bytes','groupOrdinals']);
 s:=sessions.ingest_buffer_gc_locked((p->>'stageToken')::uuid,(p->>'leaseToken')::uuid);
 IF s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged' OR jsonb_array_length(s.gc_keep_groups)=0
  OR s.gc_keep_ordinals IS NULL OR jsonb_array_length(s.gc_keep_ordinals)>=(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: not a replacement plan';END IF;
 IF s.gc_keep_ordinals IS NULL OR sessions.ingest_buffer_gc_ordinals(s.stage_token) IS DISTINCT FROM s.gc_keep_ordinals
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: fence changed during copy';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
  AND origin_id=s.origin_id AND batch_id=s.batch_id AND active_stage_token=s.stage_token
  AND row_version=s.gc_outbox_version AND state='buffered' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: active pointer changed';END IF;
 v_groups:=sessions.ingest_buffer_gc_groups(s.group_ordinals,s.gc_keep_ordinals);
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

CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_swap(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $swap$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;c sessions.ingest_stage_intents%ROWTYPE;
 v_child uuid;v_groups jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','leaseToken','replacementStageToken']);
 s:=sessions.ingest_buffer_gc_locked((p->>'stageToken')::uuid,(p->>'leaseToken')::uuid);
 IF s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged'
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: not a live acknowledged plan';END IF;
 IF s.gc_keep_ordinals IS NULL OR sessions.ingest_buffer_gc_ordinals(s.stage_token) IS DISTINCT FROM s.gc_keep_ordinals
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: fence changed during copy';END IF;
 IF p->'replacementStageToken'='null'::jsonb THEN v_child:=NULL;
 ELSE v_child:=(p->>'replacementStageToken')::uuid;END IF;
 IF jsonb_array_length(s.gc_keep_groups)=0 THEN
  IF v_child IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffer gc: erased plan cannot retain child';END IF;
 ELSE
  IF v_child IS NULL OR v_child IS DISTINCT FROM s.gc_replacement_stage_token
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: registered replacement required';END IF;
  SELECT * INTO c FROM sessions.ingest_stage_intents WHERE stage_token=v_child FOR UPDATE;
  v_groups:=sessions.ingest_buffer_gc_groups(s.group_ordinals,s.gc_keep_ordinals);
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
