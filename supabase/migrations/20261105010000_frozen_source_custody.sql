-- P1: frozen-source custody. Takes Neon off the upload path.
--
-- After the 2026-11-05 05:51Z cutover (relayhistory 0078 phase=active), Neon's legacy delivery
-- tables are frozen: every legacy writer is refused by the 0077/0078 guards. This migration lets the
-- ingest Worker build the SAME current-custody witness packet that relayhistory builds against Neon
-- (CURRENT_CUSTODY_WITNESS_SQL), from a sealed metadata-only copy of the frozen Neon state. Pending
-- privacy controls come from a Supabase-first mirror of Neon's control heads. The packet is
-- validated by the unchanged lazy-current target, so issuer/sourceIdentity/epoch/baseline are those
-- of the live policy. The seal binds the copy to exactly that source identity, epoch and baseline.
--
-- Dormant: nothing calls these functions until the ingest Worker runs with
-- INGEST_CUSTODY_SOURCE=frozen-supabase (or shadow), and every function refuses until an owner
-- writes the completeness seal after the verified copy.
DO $frozen_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relname IN('ingest_lazy_policy','upload_deletions') AND c.relowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: frozen custody requires original creator';END IF;
END $frozen_creator$;

-- Frozen Neon delivery_records metadata (no payload) joined with both acceptance sequences.
-- Primary key = the native key the witness probes.
CREATE TABLE sessions.ingest_frozen_source_records(
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
 revision_id text,revision bigint,digest text,kind text,source text,session_id text,operation text,user_id text,
 projection_accepted_seq bigint,catalog_accepted_seq bigint,
 PRIMARY KEY(org_id,workspace_id,origin_id,record_id)
);
-- Frozen Neon privacy state as of the freeze (deleted_upload_sessions, session_resets).
CREATE TABLE sessions.ingest_frozen_source_deletions(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
 source text NOT NULL,session_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id)
);
CREATE TABLE sessions.ingest_frozen_source_resets(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,
 PRIMARY KEY(org_id,workspace_id,user_id,request_id)
);
-- Per-org verification evidence (Neon-side count/sum vs Supabase-side, computed by the loader).
CREATE TABLE sessions.ingest_frozen_source_org_seals(
 org_id text PRIMARY KEY,record_rows bigint NOT NULL CHECK(record_rows>=0),record_sum numeric NOT NULL,
 privacy_rows bigint NOT NULL CHECK(privacy_rows>=0),privacy_sum numeric NOT NULL,sealed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Written by the owner only after EVERY org verified. An org with no seal row then provably had no
-- frozen Neon state (the copy is complete and Neon refuses new legacy delivery writes).
CREATE TABLE sessions.ingest_frozen_source_completeness(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256~'^[0-9a-f]{64}$'),
 org_count bigint NOT NULL CHECK(org_count>=0),record_rows bigint NOT NULL CHECK(record_rows>=0),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256~'^[0-9a-f]{64}$'),
 neon_snapshot jsonb NOT NULL,sealed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Supabase-first mirror of Neon sessions.delivery_cutover_control_heads. relayhistory begins here
-- BEFORE Neon, requires an identical {version,digest} from Neon, and completes here after Neon.
CREATE TABLE sessions.ingest_source_control_heads(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 epoch text NOT NULL CHECK(epoch~'^[0-9a-f]{64}$'),version bigint NOT NULL CHECK(version>=1),
 digest text NOT NULL CHECK(digest~'^[0-9a-f]{64}$'),kind text NOT NULL CHECK(kind IN('selection','reset')),
 payload jsonb NOT NULL,pending boolean NOT NULL,before_privacy jsonb,privacy_version bigint,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,user_id)
);
ALTER TABLE sessions.ingest_frozen_source_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_frozen_source_deletions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_frozen_source_resets ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_frozen_source_org_seals ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_frozen_source_completeness ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_source_control_heads ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_frozen_source_records,sessions.ingest_frozen_source_deletions,sessions.ingest_frozen_source_resets,
 sessions.ingest_frozen_source_org_seals,sessions.ingest_frozen_source_completeness,sessions.ingest_source_control_heads
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Same snapshot as Neon sessions.snapshot_delivery_cutover_privacy_v1, over the frozen copy.
-- Neon's database collation is C.UTF-8 (codepoint order) and Supabase's is ICU en-US, so every
-- order that shapes a packet byte is pinned to COLLATE "C".
CREATE FUNCTION sessions.ingest_frozen_source_privacy(p_org text,p_workspace text,p_user text) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,sessions,pg_temp AS $frozen_privacy$
DECLARE ds jsonb;rs jsonb;v jsonb;entry jsonb;label text;
BEGIN
 SELECT coalesce(jsonb_agg(jsonb_build_object('origin_id',origin_id,'source',source,'session_id',session_id)
  ORDER BY origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"),'[]'::jsonb) INTO ds
 FROM(SELECT origin_id,source,session_id FROM sessions.ingest_frozen_source_deletions
  WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user ORDER BY origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C" LIMIT 5001)q;
 SELECT coalesce(jsonb_agg(jsonb_build_object('request_id',request_id,'deleted',deleted::text)
  ORDER BY request_id COLLATE "C"),'[]'::jsonb) INTO rs
 FROM(SELECT request_id,deleted FROM sessions.ingest_frozen_source_resets
  WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user ORDER BY request_id COLLATE "C" LIMIT 5001)q;
 IF jsonb_array_length(ds)+jsonb_array_length(rs)>5000 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy oversize';END IF;
 FOR entry IN SELECT x FROM jsonb_array_elements(ds||rs)x LOOP
  FOR label IN SELECT value FROM jsonb_each_text(entry) WHERE key<>'deleted' LOOP
   IF label IS NULL OR octet_length(label) NOT BETWEEN 1 AND 512 OR label~('['||chr(1)||'-'||chr(31)||chr(127)||']') THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy invalid';END IF;
  END LOOP;
 END LOOP;
 v:=jsonb_build_object('complete',true,'deletions',ds,'resets',rs);
 IF octet_length(v::text)>524288 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy oversize';END IF;
 RETURN v;
END $frozen_privacy$;
REVOKE ALL ON FUNCTION sessions.ingest_frozen_source_privacy(text,text,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- The sealed policy context, or a refusal. Never reads Neon.
CREATE FUNCTION sessions.ingest_frozen_source_policy() RETURNS sessions.ingest_lazy_policy
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,sessions,pg_temp AS $frozen_policy$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;s sessions.ingest_frozen_source_completeness%ROWTYPE;
BEGIN
 SELECT * INTO pol FROM sessions.ingest_lazy_policy WHERE singleton;
 IF NOT FOUND OR pol.valid_until<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: custody policy unavailable';END IF;
 SELECT * INTO s FROM sessions.ingest_frozen_source_completeness WHERE singleton;
 IF NOT FOUND OR s.source_identity_sha256<>pol.source_identity_sha256 OR s.control_epoch<>pol.control_epoch
  OR s.privacy_baseline_sha256<>pol.privacy_baseline_sha256 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: copy not sealed';END IF;
 RETURN pol;
END $frozen_policy$;
REVOKE ALL ON FUNCTION sessions.ingest_frozen_source_policy()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Byte-identical replacement for relayhistory's CURRENT_CUSTODY_WITNESS_SQL packet.
-- p: {kind, tenant{orgId,workspaceId,userId}, originId, batchId, batchDigest, originalRecordCount, candidates[]}
-- candidates[]: {preparedOrd, recordId, revision(text), revisionId, candidateDigest, source, sessionId}
-- Returns {witness, sourceDigest} or refuses (55000/22023); a refusal is never an empty witness.
CREATE FUNCTION sessions.ingest_frozen_source_witness(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $frozen_witness$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_kind text;v_org text;v_ws text;v_user text;v_origin text;v_count integer;v_original integer;
 v_records jsonb;v_valid boolean;v_n integer;v_privacy jsonb;w jsonb;
BEGIN
 IF jsonb_typeof(p) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid request';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['kind','tenant','originId','batchId','batchDigest','originalRecordCount','candidates']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_kind:=p->>'kind';
 IF v_kind IS NULL OR v_kind NOT IN('batch','selection','reset') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid witness operation';END IF;
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_origin:=CASE WHEN p->'originId'='null'::jsonb THEN NULL ELSE sessions.ingest_identifier(p->'originId',false) END;
 IF jsonb_typeof(p->'candidates') IS DISTINCT FROM 'array' OR jsonb_typeof(p->'originalRecordCount') IS DISTINCT FROM 'number'
  OR p->>'originalRecordCount'!~'^(0|[1-9][0-9]{0,2})$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid candidate bound';END IF;
 v_count:=jsonb_array_length(p->'candidates');v_original:=(p->>'originalRecordCount')::integer;
 IF v_count>500 OR v_original>500 OR v_count>v_original
  OR (v_kind<>'batch' AND (v_count<>0 OR v_original<>0 OR p->'batchId'<>'null'::jsonb OR p->'batchDigest'<>'null'::jsonb))
  OR (v_kind='reset' AND v_origin IS NOT NULL) OR (v_kind<>'reset' AND v_origin IS NULL) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid witness shape';END IF;
 pol:=sessions.ingest_frozen_source_policy();
 SELECT * INTO h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 IF FOUND AND h.epoch<>pol.control_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control epoch mismatch';END IF;
 IF v_kind='batch' AND coalesce(h.pending,false) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy control pending';END IF;
 v_privacy:=CASE WHEN h.version IS NULL THEN sessions.ingest_frozen_source_privacy(v_org,v_ws,v_user)
  WHEN h.pending AND h.version=1 THEN h.before_privacy ELSE NULL END;
 WITH candidates AS MATERIALIZED(SELECT * FROM jsonb_to_recordset(p->'candidates') AS c("preparedOrd" int,"recordId" text,"revision" text,
   "revisionId" text,"candidateDigest" text,"source" text,"sessionId" text)),
 ranked AS(SELECT *,((row_number() OVER(ORDER BY "recordId" COLLATE "C","revision"::bigint,"revisionId" COLLATE "C"))-1)::int AS rank FROM candidates),
 looked AS(SELECT c.*,r.user_id,r.revision AS current_revision,r.digest,r.revision_id,r.kind,r.source AS current_source,r.session_id,r.operation,
   r.record_id IS NOT NULL AS is_found,r.projection_accepted_seq AS projection_seq,r.catalog_accepted_seq AS catalog_seq,
   (EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_deletions d WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.user_id=v_user
     AND d.origin_id=v_origin AND d.source=c.source AND d.session_id=c."sessionId")
    OR EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.user_id=v_user
     AND d.origin_id=v_origin AND d.source=c.source AND d.session_id=c."sessionId")) AS candidate_fenced,
   (EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_deletions d WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.user_id=r.user_id
     AND d.origin_id=v_origin AND d.source=r.source AND d.session_id=r.session_id)
    OR EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.user_id=r.user_id
     AND d.origin_id=v_origin AND d.source=r.source AND d.session_id=r.session_id)) AS current_fenced
  FROM ranked c LEFT JOIN LATERAL(SELECT x.* FROM sessions.ingest_frozen_source_records x WHERE x.org_id=v_org AND x.workspace_id=v_ws
   AND x.origin_id=v_origin AND x.record_id=c."recordId" LIMIT 1) r ON true),
 checked AS(SELECT *,NOT is_found OR(
   user_id IS NOT NULL AND octet_length(user_id) BETWEEN 1 AND 512 AND user_id!~('['||chr(1)||'-'||chr(31)||chr(127)||']')
   AND revision_id IS NOT NULL AND octet_length(revision_id) BETWEEN 1 AND 512 AND revision_id!~('['||chr(1)||'-'||chr(31)||chr(127)||']')
   AND kind IS NOT NULL AND octet_length(kind) BETWEEN 1 AND 512 AND kind!~('['||chr(1)||'-'||chr(31)||chr(127)||']')
   AND current_source IS NOT NULL AND octet_length(current_source) BETWEEN 1 AND 512 AND current_source!~('['||chr(1)||'-'||chr(31)||chr(127)||']')
   AND (session_id IS NULL OR (octet_length(session_id) BETWEEN 1 AND 512 AND session_id!~('['||chr(1)||'-'||chr(31)||chr(127)||']')))
   AND current_revision>0 AND digest~'^[a-f0-9]{64}$' AND operation IN('upsert','delete')
   AND (projection_seq IS NULL OR projection_seq>=0) AND (catalog_seq IS NULL OR catalog_seq>=0)) AS valid FROM looked)
 SELECT coalesce(bool_and(valid IS TRUE),true),count(*)::int,coalesce(jsonb_agg(
   jsonb_build_object('preparedOrd',"preparedOrd",'processingRank',rank,'candidateDigest',"candidateDigest",'recordId',"recordId",'revision',"revision",'revisionId',"revisionId",
    'candidateFenced',candidate_fenced,'currentFenced',current_fenced,
    'current',CASE WHEN is_found AND valid THEN jsonb_build_object('userId',user_id,'revision',current_revision::text,'digest',digest,'revisionId',revision_id,
     'kind',kind,'source',current_source,'sessionId',session_id,'operation',operation,'catalogAcceptedSeq',catalog_seq::text,'projectionAcceptedSeq',projection_seq::text) ELSE NULL END)
   ORDER BY "preparedOrd"),'[]'::jsonb)
 INTO v_valid,v_n,v_records FROM checked;
 IF NOT v_valid OR octet_length(v_records::text)>4194304 OR octet_length(coalesce(v_privacy,'null'::jsonb)::text)>524288 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: witness unavailable';END IF;
 w:=jsonb_build_object('version',1,'issuer','relayhistory-cloud','sourceIdentitySha256',pol.source_identity_sha256,
  'controlEpoch',pol.control_epoch,'privacyBaselineSha256',pol.privacy_baseline_sha256,
  'tenant',jsonb_build_object('orgId',v_org,'workspaceId',v_ws,'userId',v_user),'originId',v_origin,
  'batchId',p->>'batchId','batchDigest',p->>'batchDigest',
  'sourceControlVersion',coalesce(h.version,0)::text,
  'privacyVersion',(CASE WHEN h.pending THEN h.privacy_version ELSE coalesce(h.version,0) END)::text,
  'pending',coalesce(h.pending,false),
  'pendingControl',CASE WHEN h.pending THEN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind) ELSE NULL END,
  'originalRecordCount',v_original,'candidateCount',v_n,'records',v_records,'privacy',v_privacy);
 RETURN jsonb_build_object('witness',w,'sourceDigest',encode(sha256(convert_to(w::text,'UTF8')),'hex'));
END $frozen_witness$;

-- Mirror of Neon sessions.begin_delivery_cutover_control_v1 (same digest, version and PRE rules),
-- called by relayhistory BEFORE Neon.
CREATE FUNCTION sessions.ingest_source_control_begin(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $control_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_kind text;v_epoch text;d text;fresh boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','kind','payload']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_kind:=p->>'kind';v_epoch:=p->>'epoch';
 IF v_kind IS NULL OR v_kind NOT IN('selection','reset') OR p->'payload' IS NULL OR jsonb_typeof(p->'payload')='null'
  OR octet_length((p->'payload')::text)>2113536 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 pol:=sessions.ingest_frozen_source_policy();
 IF v_epoch IS DISTINCT FROM pol.control_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',v_epoch,v_org,v_ws,v_user,v_kind,p->'payload')::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
  VALUES(v_org,v_ws,v_user,v_epoch,1,d,v_kind,p->'payload',true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF coalesce(fresh,false) THEN
  UPDATE sessions.ingest_source_control_heads SET before_privacy=sessions.ingest_frozen_source_privacy(v_org,v_ws,v_user),privacy_version=0
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 IF h.epoch IS DISTINCT FROM v_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 IF NOT coalesce(fresh,false) THEN
  IF h.pending THEN
   IF h.digest IS DISTINCT FROM d OR h.kind IS DISTINCT FROM v_kind THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control pending';END IF;
   RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',false);
  END IF;
  IF h.version=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control version exhausted';END IF;
  UPDATE sessions.ingest_source_control_heads SET version=h.version+1,digest=d,kind=v_kind,payload=p->'payload',pending=true,
   before_privacy=NULL,privacy_version=h.version,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',true);
END $control_begin$;

-- Mirror of Neon sessions.complete_delivery_cutover_control_v1, called by relayhistory AFTER Neon completes.
CREATE FUNCTION sessions.ingest_source_control_complete(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $control_complete$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_version bigint;v_outcome text;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','version','digest','kind','outcome']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_outcome:=p->>'outcome';
 IF p->>'version'!~'^[1-9][0-9]{0,18}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 v_version:=(p->>'version')::bigint;
 pol:=sessions.ingest_frozen_source_policy();
 IF p->>'epoch' IS DISTINCT FROM pol.control_epoch OR v_outcome IS NULL OR v_outcome NOT IN('applied','replay','stale') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control held';END IF;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF h.epoch IS DISTINCT FROM p->>'epoch' OR v_version>h.version THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control';END IF;
 IF v_version<h.version THEN RETURN jsonb_build_object('completed',true,'superseded',true);END IF;
 IF v_outcome NOT IN('applied','replay') OR h.digest IS DISTINCT FROM p->>'digest' OR h.kind IS DISTINCT FROM p->>'kind' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control';END IF;
 UPDATE sessions.ingest_source_control_heads SET pending=false,updated_at=clock_timestamp()
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 RETURN jsonb_build_object('completed',true,'superseded',false);
END $control_complete$;

REVOKE ALL ON FUNCTION sessions.ingest_frozen_source_witness(jsonb),sessions.ingest_source_control_begin(jsonb),
 sessions.ingest_source_control_complete(jsonb) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_frozen_source_witness(jsonb),sessions.ingest_source_control_begin(jsonb),
 sessions.ingest_source_control_complete(jsonb) TO bulls96_session_ingest;

-- Frozen Neon sessions.delivery_receipts: the historical durable receipts a retried pre-cutover batch
-- replays (relayhistory legacyDeliveryReceipt). Owner-only; read through the definer below.
CREATE TABLE sessions.ingest_frozen_source_receipts(
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,
 digest text NOT NULL CHECK(digest~'^[0-9a-f]{64}$'),receipt jsonb NOT NULL,
 PRIMARY KEY(org_id,workspace_id,origin_id,batch_id)
);
ALTER TABLE sessions.ingest_frozen_source_receipts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_frozen_source_receipts
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Same answer as relayhistory's Neon legacyDeliveryReceipt: refuses while the user's privacy control is
-- pending, otherwise {stored: {digest, receipt}} or {stored: null}.
CREATE FUNCTION sessions.ingest_frozen_source_receipt(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $frozen_receipt$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;v_org text;v_ws text;v_user text;v_origin text;v_batch text;r record;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_origin:=sessions.ingest_identifier(p->'originId',false);
 v_batch:=sessions.ingest_identifier(p->'batchId',false);
 pol:=sessions.ingest_frozen_source_policy();
 IF EXISTS(SELECT 1 FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND pending) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy control pending';END IF;
 SELECT digest,receipt INTO r FROM sessions.ingest_frozen_source_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND origin_id=v_origin AND batch_id=v_batch;
 IF NOT FOUND THEN RETURN jsonb_build_object('stored',NULL);END IF;
 RETURN jsonb_build_object('stored',jsonb_build_object('digest',r.digest,'receipt',r.receipt));
END $frozen_receipt$;
REVOKE ALL ON FUNCTION sessions.ingest_frozen_source_receipt(jsonb) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_frozen_source_receipt(jsonb) TO bulls96_session_ingest;
