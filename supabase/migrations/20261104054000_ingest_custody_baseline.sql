-- Metadata-only current custody, captured under the same frozen source epoch.
-- This authority does not seed canonical revisions or claim transcript S3 proof.
DO $custody_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid IN(
  'sessions.ingest_receipt_seat_ack()'::regprocedure,
  'sessions.ingest_receipt_seats_for_stage(uuid)'::regprocedure,
  'sessions.ingest_require_buffer_admission(text)'::regprocedure) AND proowner<>me)
 OR EXISTS(SELECT 1 FROM pg_class WHERE oid IN(
  'sessions.ingest_receipt_seat_clock'::regclass,'sessions.ingest_receipt_record_heads'::regclass,
  'sessions.ingest_receipt_ord_seats'::regclass,'sessions.ingest_buffer_admission'::regclass) AND relowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody requires original ingest creator';
 END IF;
END $custody_creator$;

CREATE TABLE sessions.ingest_custody_imports(
 org_id text PRIMARY KEY,import_token uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 source_proof_sha256 text NOT NULL CHECK(source_proof_sha256~'^[0-9a-f]{64}$'),
 expected_records bigint NOT NULL CHECK(expected_records>=0),
 expected_pages bigint NOT NULL CHECK(expected_pages>=0),
 expected_chain text NOT NULL CHECK(expected_chain~'^[0-9a-f]{64}$'),
 received_records bigint NOT NULL DEFAULT 0 CHECK(received_records>=0),
 received_pages bigint NOT NULL DEFAULT 0 CHECK(received_pages>=0),
 chain_sha256 text NOT NULL DEFAULT repeat('0',64) CHECK(chain_sha256~'^[0-9a-f]{64}$'),
 status text NOT NULL DEFAULT 'importing' CHECK(status IN('importing','complete')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz
);
CREATE TABLE sessions.ingest_custody_pages(
 import_token uuid NOT NULL REFERENCES sessions.ingest_custody_imports(import_token),
 page bigint NOT NULL CHECK(page>=0),sha256 text NOT NULL CHECK(sha256~'^[0-9a-f]{64}$'),
 records integer NOT NULL CHECK(records BETWEEN 1 AND 10000),PRIMARY KEY(import_token,page)
);
CREATE TABLE sessions.ingest_custody_records(
 key_sha256 text PRIMARY KEY CHECK(key_sha256~'^[0-9a-f]{64}$'),
 import_token uuid NOT NULL REFERENCES sessions.ingest_custody_imports(import_token),
 page bigint NOT NULL CHECK(page>=0),source_key_sha256 text NOT NULL CHECK(source_key_sha256~'^[0-9a-f]{64}$'),
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),digest text NOT NULL CHECK(digest~'^[0-9a-f]{64}$'),
 source text NOT NULL,session_id text,metadata jsonb NOT NULL CHECK(jsonb_typeof(metadata)='object'),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),fence_epoch bigint NOT NULL CHECK(fence_epoch>=0),
 baseline_fingerprint text NOT NULL CHECK(baseline_fingerprint~'^[0-9a-f]{64}$'),
 UNIQUE(import_token,source_key_sha256)
);
CREATE INDEX ingest_custody_records_import ON sessions.ingest_custody_records(import_token,page);
ALTER TABLE sessions.ingest_custody_imports ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_custody_pages ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_custody_records ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_custody_imports,sessions.ingest_custody_pages,sessions.ingest_custody_records
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

ALTER TABLE sessions.ingest_receipt_record_heads ALTER COLUMN winner_key_sha256 DROP NOT NULL;
ALTER TABLE sessions.ingest_receipt_record_heads ADD COLUMN legacy_key_sha256 text
 REFERENCES sessions.ingest_custody_records(key_sha256);
ALTER TABLE sessions.ingest_receipt_record_heads ADD CONSTRAINT ingest_custody_head_authority
 CHECK((winner_key_sha256 IS NULL)<>(legacy_key_sha256 IS NULL));
ALTER TABLE sessions.ingest_receipt_ord_seats ADD COLUMN legacy_custody_key_sha256 text
 REFERENCES sessions.ingest_custody_records(key_sha256);

CREATE TABLE sessions.ingest_custody_global(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 source_proof_sha256 text NOT NULL CHECK(source_proof_sha256~'^[0-9a-f]{64}$'),
 org_inventory jsonb NOT NULL CHECK(jsonb_typeof(org_inventory)='array'),
 org_inventory_sha256 text NOT NULL CHECK(org_inventory_sha256~'^[0-9a-f]{64}$'),
 org_count bigint NOT NULL CHECK(org_count BETWEEN 0 AND 512),released_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE sessions.ingest_custody_global ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_custody_global
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Private owner ceremony. The exact descriptor bytes, not a caller's asserted
-- footer hash, must be the descriptor already pinned by the immutable530 clock.
CREATE FUNCTION sessions.ingest_custody_begin(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $custody_begin$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;i sessions.ingest_custody_imports%ROWTYPE;
 v_org text;v_gen text;v_text text;d jsonb;f jsonb;v_n integer;v_records bigint;v_pages bigint;v_chain text;v_sha text;
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(proowner) FROM pg_proc
  WHERE oid='sessions.ingest_custody_begin(jsonb)'::regprocedure) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: custody owner required';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','orgId','descriptorText']);
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 v_org:=sessions.ingest_identifier(p->'orgId',false);
 IF jsonb_typeof(p->'descriptorText') IS DISTINCT FROM 'string' OR octet_length(p->>'descriptorText')>1048576 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody descriptor bound';END IF;
 v_text:=p->>'descriptorText';v_sha:=encode(sha256(convert_to(v_text,'UTF8')),'hex');
 BEGIN d:=v_text::jsonb;EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid custody descriptor';END;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE;
 IF c.baseline->>'sourceProofSha256' IS DISTINCT FROM v_sha OR d->>'protocol' IS DISTINCT FROM 'delivery-privacy-baseline-v1'
  OR d->'complete' IS DISTINCT FROM 'true'::jsonb OR d->>'epoch' IS DISTINCT FROM c.control_epoch
  OR d->'baselineAcceptanceFloor' IS DISTINCT FROM (c.baseline-'sourceIdentitySha256'-'sourceProofSha256')
  OR d#>>'{custody,protocol}' IS DISTINCT FROM 'delivery-custody-baseline-v1'
  OR d#>>'{custody,sourceIdentitySha256}' IS DISTINCT FROM c.source_identity_sha256
  OR jsonb_typeof(d#>'{custody,orgs}') IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody source proof mismatch';END IF;
 SELECT count(*)::integer INTO v_n FROM jsonb_array_elements(d#>'{custody,orgs}') x WHERE x->>'orgId'=v_org;
 IF v_n<>1 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit unique custody org required';END IF;
 SELECT x INTO STRICT f FROM jsonb_array_elements(d#>'{custody,orgs}') x WHERE x->>'orgId'=v_org;
 PERFORM sessions.ingest_exact_keys(f,ARRAY['orgId','records','pages','chainSha256']);
 IF jsonb_typeof(f->'records') IS DISTINCT FROM 'string' OR f->>'records'!~'^(0|[1-9][0-9]{0,18})$'
  OR jsonb_typeof(f->'pages') IS DISTINCT FROM 'string' OR f->>'pages'!~'^(0|[1-9][0-9]{0,18})$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody footer counters';END IF;
 BEGIN v_records:=(f->>'records')::bigint;v_pages:=(f->>'pages')::bigint;
 EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody footer overflow';END;
 v_chain:=sessions.ingest_digest(f->'chainSha256');
 IF (v_pages=0 AND (v_records<>0 OR v_chain<>repeat('0',64))) OR
  (v_pages>0 AND (v_records<v_pages OR v_records::numeric>v_pages::numeric*10000)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody footer coverage';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org AND control_epoch=c.control_epoch
  AND schema_generation=v_gen FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody org not owner bound';END IF;
 SELECT * INTO i FROM sessions.ingest_custody_imports WHERE org_id=v_org FOR UPDATE;
 IF NOT FOUND THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org)
   OR EXISTS(SELECT 1 FROM sessions.ingest_control_receipts WHERE org_id=v_org)
   OR EXISTS(SELECT 1 FROM sessions.ingest_receipt_record_heads h
    WHERE h.key_material->>3=v_org) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody must precede ingest activity';END IF;
  INSERT INTO sessions.ingest_custody_imports(org_id,generation,control_epoch,source_identity_sha256,source_proof_sha256,
   expected_records,expected_pages,expected_chain)
  VALUES(v_org,v_gen,c.control_epoch,c.source_identity_sha256,v_sha,v_records,v_pages,v_chain)
  ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT i FROM sessions.ingest_custody_imports WHERE org_id=v_org FOR UPDATE;
 END IF;
 IF i.control_epoch<>c.control_epoch OR i.source_identity_sha256<>c.source_identity_sha256 OR i.source_proof_sha256<>v_sha
  OR i.expected_records<>v_records OR i.expected_pages<>v_pages OR i.expected_chain<>v_chain
  OR (i.status='importing' AND i.generation<>v_gen) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody baseline adoption refused';END IF;
 RETURN jsonb_build_object('accepted',true,'importToken',i.import_token,'status',i.status,
  'nextPage',i.received_pages::text,'receivedRecords',i.received_records::text,'chainSha256',i.chain_sha256);
END $custody_begin$;

CREATE FUNCTION sessions.ingest_custody_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $custody_page$
DECLARE i sessions.ingest_custody_imports%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;
 v_gen text;v_token uuid;v_payload jsonb;v_page bigint;v_sha text;v_seen text;v_n integer;
 v_scope record;v_inserted integer;
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(proowner) FROM pg_proc
  WHERE oid='sessions.ingest_custody_page(jsonb)'::regprocedure) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: custody owner required';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','importToken','payload','pageSha256']);
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 BEGIN v_token:=(p->>'importToken')::uuid;EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody token';END;
 SELECT * INTO STRICT i FROM sessions.ingest_custody_imports WHERE import_token=v_token;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=i.org_id FOR SHARE;
 SELECT * INTO STRICT i FROM sessions.ingest_custody_imports WHERE import_token=v_token FOR UPDATE;
 IF i.generation<>v_gen THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody import generation changed';END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=i.org_id AND control_epoch=i.control_epoch
  AND schema_generation=v_gen FOR SHARE;
 IF NOT FOUND OR i.control_epoch<>c.control_epoch OR i.source_identity_sha256<>c.source_identity_sha256
  OR i.source_proof_sha256 IS DISTINCT FROM c.baseline->>'sourceProofSha256' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody authority changed';END IF;
 v_payload:=p->'payload';PERFORM sessions.ingest_exact_keys(v_payload,ARRAY['protocol','controlEpoch','orgId','page','records']);
 IF octet_length(v_payload::text)>16777216 OR v_payload->>'protocol' IS DISTINCT FROM 'delivery-custody-page-v1'
  OR v_payload->>'orgId' IS DISTINCT FROM i.org_id OR v_payload->>'controlEpoch' IS DISTINCT FROM i.control_epoch
  OR jsonb_typeof(v_payload->'page') IS DISTINCT FROM 'string' OR v_payload->>'page'!~'^(0|[1-9][0-9]{0,18})$'
  OR jsonb_typeof(v_payload->'records') IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody page envelope';END IF;
 BEGIN v_page:=(v_payload->>'page')::bigint;EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody page overflow';END;
 v_n:=jsonb_array_length(v_payload->'records');
 IF v_n NOT BETWEEN 1 AND 10000 OR v_page>=i.expected_pages THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody page bound';END IF;
 v_sha:=encode(sha256(convert_to(v_payload::text,'UTF8')),'hex');
 IF v_sha<>sessions.ingest_digest(p->'pageSha256') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody page digest';END IF;
 IF v_page<i.received_pages THEN
  SELECT sha256 INTO STRICT v_seen FROM sessions.ingest_custody_pages WHERE import_token=v_token AND page=v_page;
  IF v_seen<>v_sha THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: changed custody page replay';END IF;
  RETURN jsonb_build_object('accepted',true,'replay',true,'importToken',v_token,'page',v_page::text,'pageSha256',v_sha,
   'nextPage',i.received_pages::text,'receivedRecords',i.received_records::text,'chainSha256',i.chain_sha256);
 END IF;
 IF i.status<>'importing' OR v_page<>i.received_pages OR i.received_records::numeric+v_n>i.expected_records THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody page gap or closed import';END IF;
 -- Set-based validation and insertion keep the metadata-only background path
 -- independent of the500-row upload ACK limit. The16MiB packet fence is fixed.
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_payload->'records') q(e) WHERE CASE
  WHEN jsonb_typeof(e) IS DISTINCT FROM 'object' THEN true ELSE
   ARRAY(SELECT k FROM jsonb_object_keys(e) k ORDER BY k COLLATE "C") IS DISTINCT FROM
    ARRAY['catalogAcceptedSeq','digest','kind','operation','originId','projectionAcceptedSeq','recordId','revision','revisionId','sessionId','source','userId','workspaceId']::text[]
   OR EXISTS(SELECT 1 FROM unnest(ARRAY['workspaceId','userId','originId','recordId','revisionId','kind','source']) k WHERE jsonb_typeof(e->k) IS DISTINCT FROM 'string')
   OR coalesce(e->>'userId','')='' OR coalesce(e->>'originId','')='' OR coalesce(e->>'recordId','')='' OR coalesce(e->>'source','')=''
   OR jsonb_typeof(e->'revision') IS DISTINCT FROM 'string' OR e->>'revision'!~'^[1-9][0-9]{0,18}$'
   OR jsonb_typeof(e->'digest') IS DISTINCT FROM 'string' OR e->>'digest'!~'^[0-9a-f]{64}$'
   OR jsonb_typeof(e->'operation') IS DISTINCT FROM 'string' OR e->>'operation' NOT IN('upsert','delete')
   OR jsonb_typeof(e->'sessionId') NOT IN('string','null')
   OR EXISTS(SELECT 1 FROM unnest(ARRAY[e->'catalogAcceptedSeq',e->'projectionAcceptedSeq']) s
    WHERE CASE WHEN s='null'::jsonb THEN false WHEN jsonb_typeof(s)='string' AND (s#>>'{}')~'^(0|[1-9][0-9]{0,18})$'
     THEN (s#>>'{}')::numeric>9223372036854775807 ELSE true END)
 END) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody metadata shape or bound';END IF;
 FOR v_scope IN SELECT * FROM (
  SELECT DISTINCT e->>'workspaceId' AS workspace_id,e->>'userId' AS user_id FROM jsonb_array_elements(v_payload->'records') q(e)
 ) q ORDER BY workspace_id COLLATE "C",user_id COLLATE "C" LOOP
  PERFORM sessions.ingest_lock_fences(i.org_id,v_scope.workspace_id,v_scope.user_id);
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_payload->'records') q(e)
  LEFT JOIN sessions.ingest_seed_fence_baselines b ON b.org_id=i.org_id AND b.workspace_id=e->>'workspaceId' AND b.user_id=e->>'userId'
  WHERE b.fingerprint IS NULL OR b.closed_at IS NOT NULL)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody requires original privacy baseline';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_payload->'records') q(e)
  JOIN sessions.upload_deletions d ON d.org_id=i.org_id AND d.workspace_id=e->>'workspaceId' AND d.user_id=e->>'userId'
   AND d.origin_id=e->>'originId' AND d.source=e->>'source' AND d.session_id=e->>'sessionId')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody source row already privacy fenced';END IF;
 WITH rec AS MATERIALIZED(SELECT e AS metadata,e->>'workspaceId' AS ws,e->>'userId' AS usr,e->>'originId' AS origin,
  e->>'recordId' AS record,e->>'source' AS source,e->>'sessionId' AS sid FROM jsonb_array_elements(v_payload->'records') q(e)),
 resets AS(SELECT workspace_id,user_id,count(*) AS n FROM sessions.upload_resets WHERE org_id=i.org_id GROUP BY workspace_id,user_id)
 INSERT INTO sessions.ingest_custody_records(key_sha256,import_token,page,source_key_sha256,org_id,workspace_id,user_id,origin_id,record_id,
  revision,digest,source,session_id,metadata,reset_epoch,fence_epoch,baseline_fingerprint)
 SELECT encode(sha256(convert_to(jsonb_build_array('legacy-custody-row-v1',v_token::text,r.metadata)::text,'UTF8')),'hex'),
  v_token,v_page,encode(sha256(convert_to(jsonb_build_array(r.ws,r.origin,r.record)::text,'UTF8')),'hex'),
  i.org_id,r.ws,r.usr,r.origin,r.record,(r.metadata->>'revision')::bigint,r.metadata->>'digest',r.source,r.sid,r.metadata,
  coalesce(x.n,0),CASE WHEN r.sid IS NULL THEN 0 ELSE coalesce(f.epoch,0) END,b.fingerprint
 FROM rec r JOIN sessions.ingest_seed_fence_baselines b ON b.org_id=i.org_id AND b.workspace_id=r.ws AND b.user_id=r.usr
 LEFT JOIN resets x ON x.workspace_id=r.ws AND x.user_id=r.usr
 LEFT JOIN sessions.ingest_fence_versions f ON f.org_id=i.org_id AND f.workspace_id=r.ws AND f.user_id=r.usr
  AND f.origin_id=r.origin AND f.source=r.source AND f.session_id=r.sid;
 GET DIAGNOSTICS v_inserted=ROW_COUNT;
 IF v_inserted<>v_n THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody row coverage';END IF;
 INSERT INTO sessions.ingest_receipt_record_heads(key_sha256,key_material,winner_key_sha256,legacy_key_sha256)
 SELECT encode(sha256(convert_to(a.material::text,'UTF8')),'hex'),a.material,NULL,r.key_sha256
 FROM sessions.ingest_custody_records r CROSS JOIN LATERAL(
  SELECT jsonb_build_array('buffer-record-first-body-v1',c.control_epoch,c.source_identity_sha256,
   r.org_id,r.workspace_id,r.user_id,r.origin_id,r.record_id,r.reset_epoch::text) AS material
 ) a WHERE r.import_token=v_token AND r.page=v_page;
 INSERT INTO sessions.ingest_custody_pages VALUES(v_token,v_page,v_sha,v_n);
 UPDATE sessions.ingest_custody_imports SET received_pages=received_pages+1,received_records=received_records+v_n,
  chain_sha256=encode(sha256(convert_to(chain_sha256||E'\n'||v_sha,'UTF8')),'hex')
 WHERE import_token=v_token RETURNING * INTO i;
 RETURN jsonb_build_object('accepted',true,'replay',false,'importToken',v_token,'page',v_page::text,'pageSha256',v_sha,
  'nextPage',i.received_pages::text,'receivedRecords',i.received_records::text,'chainSha256',i.chain_sha256);
END $custody_page$;

CREATE FUNCTION sessions.ingest_custody_complete(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $custody_complete$
DECLARE i sessions.ingest_custody_imports%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;v_gen text;v_token uuid;k record;
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(proowner) FROM pg_proc
  WHERE oid='sessions.ingest_custody_complete(jsonb)'::regprocedure) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: custody owner required';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','importToken']);
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 BEGIN v_token:=(p->>'importToken')::uuid;EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody token';END;
 SELECT * INTO STRICT i FROM sessions.ingest_custody_imports WHERE import_token=v_token;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=i.org_id FOR SHARE;
 SELECT * INTO STRICT i FROM sessions.ingest_custody_imports WHERE import_token=v_token FOR UPDATE;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=i.org_id AND control_epoch=i.control_epoch AND schema_generation=v_gen FOR SHARE;
 IF NOT FOUND OR i.control_epoch<>c.control_epoch OR i.source_identity_sha256<>c.source_identity_sha256
  OR i.source_proof_sha256 IS DISTINCT FROM c.baseline->>'sourceProofSha256'
  OR (i.status='importing' AND i.generation<>v_gen) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody completion authority changed';END IF;
 IF i.status='complete' THEN
  RETURN jsonb_build_object('accepted',true,'complete',true,'replay',true,'importToken',v_token,'orgId',i.org_id,
   'records',i.received_records::text,'pages',i.received_pages::text,'chainSha256',i.chain_sha256);END IF;
 IF i.received_records<>i.expected_records OR i.received_pages<>i.expected_pages OR i.chain_sha256<>i.expected_chain
  OR (SELECT count(*) FROM sessions.ingest_custody_records WHERE import_token=v_token)<>i.expected_records
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=i.org_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_control_receipts WHERE org_id=i.org_id) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody completion coverage';END IF;
 FOR k IN SELECT * FROM (SELECT DISTINCT workspace_id,user_id FROM sessions.ingest_custody_records WHERE import_token=v_token) q
  ORDER BY workspace_id COLLATE "C",user_id COLLATE "C" LOOP
  PERFORM sessions.ingest_lock_fences(i.org_id,k.workspace_id,k.user_id);
 END LOOP;
 IF EXISTS(SELECT 1 FROM sessions.ingest_custody_records r
  LEFT JOIN sessions.ingest_seed_fence_baselines b ON b.org_id=r.org_id AND b.workspace_id=r.workspace_id AND b.user_id=r.user_id
  LEFT JOIN sessions.ingest_fence_versions f ON f.org_id=r.org_id AND f.workspace_id=r.workspace_id AND f.user_id=r.user_id
   AND f.origin_id=r.origin_id AND f.source=r.source AND f.session_id=r.session_id
  WHERE r.import_token=v_token AND (b.fingerprint IS DISTINCT FROM r.baseline_fingerprint OR b.closed_at IS NOT NULL
   OR (r.session_id IS NOT NULL AND coalesce(f.epoch,0)<>r.fence_epoch)))
 OR EXISTS(SELECT 1 FROM sessions.ingest_custody_records r JOIN sessions.upload_deletions d
  ON d.org_id=r.org_id AND d.workspace_id=r.workspace_id AND d.user_id=r.user_id AND d.origin_id=r.origin_id
   AND d.source=r.source AND d.session_id=r.session_id WHERE r.import_token=v_token)
 OR EXISTS(SELECT 1 FROM (SELECT DISTINCT org_id,workspace_id,user_id,reset_epoch FROM sessions.ingest_custody_records WHERE import_token=v_token) r
  WHERE (SELECT count(*) FROM sessions.upload_resets x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.user_id=r.user_id)<>r.reset_epoch)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody privacy snapshot changed';END IF;
 PERFORM sessions.ingest_require_generation(v_gen);
 UPDATE sessions.ingest_custody_imports SET status='complete',completed_at=clock_timestamp() WHERE import_token=v_token;
 RETURN jsonb_build_object('accepted',true,'complete',true,'replay',false,'importToken',v_token,'orgId',i.org_id,
  'records',i.received_records::text,'pages',i.received_pages::text,'chainSha256',i.chain_sha256);
END $custody_complete$;

CREATE FUNCTION sessions.ingest_require_custody(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $require_custody$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;
BEGIN
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

CREATE OR REPLACE FUNCTION sessions.ingest_require_buffer_admission(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $admission$
DECLARE v_generation text;
BEGIN
 SELECT generation INTO v_generation FROM sessions.__96bulls_role_validation
  WHERE name='session_directory_search_worker' AND status='pass' FOR SHARE;
 IF v_generation IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: generation not ready';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_admission
  WHERE org_id=p_org AND valid_until>clock_timestamp() AND schema_generation=v_generation FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered privacy or retention baseline not ready';END IF;
 PERFORM sessions.ingest_require_custody(p_org);
END $admission$;

REVOKE ALL ON FUNCTION sessions.ingest_custody_begin(jsonb),sessions.ingest_custody_page(jsonb),
 sessions.ingest_custody_complete(jsonb),sessions.ingest_require_custody(text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

--040530 ACK/read capabilities now use proven old AND new current custody.
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

CREATE FUNCTION sessions.ingest_custody_release(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $custody_release$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;i sessions.ingest_custody_imports%ROWTYPE;
 g sessions.ingest_custody_global%ROWTYPE;v_gen text;v_text text;d jsonb;f jsonb;v_inventory jsonb;v_count bigint;v_sha text;
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(proowner) FROM pg_proc
  WHERE oid='sessions.ingest_custody_release(jsonb)'::regprocedure) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: custody owner required';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','descriptorText']);
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 IF jsonb_typeof(p->'descriptorText') IS DISTINCT FROM 'string' OR octet_length(p->>'descriptorText')>1048576 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: custody descriptor bound';END IF;
 v_text:=p->>'descriptorText';
 BEGIN d:=v_text::jsonb;EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid custody descriptor';END;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE;
 IF c.baseline->>'sourceProofSha256' IS DISTINCT FROM encode(sha256(convert_to(v_text,'UTF8')),'hex')
  OR d->>'protocol' IS DISTINCT FROM 'delivery-privacy-baseline-v1' OR d->'complete' IS DISTINCT FROM 'true'::jsonb
  OR d->>'epoch' IS DISTINCT FROM c.control_epoch
  OR d->'baselineAcceptanceFloor' IS DISTINCT FROM (c.baseline-'sourceIdentitySha256'-'sourceProofSha256')
  OR d#>>'{custody,protocol}' IS DISTINCT FROM 'delivery-custody-baseline-v1'
  OR d#>>'{custody,scope}' IS DISTINCT FROM 'all-existing-orgs'
  OR d#>>'{custody,sourceIdentitySha256}' IS DISTINCT FROM c.source_identity_sha256
  OR jsonb_typeof(d#>'{custody,orgs}') IS DISTINCT FROM 'array'
  OR jsonb_typeof(d#>'{custody,orgCount}') IS DISTINCT FROM 'string'
  OR d#>>'{custody,orgCount}'!~'^(0|[1-9][0-9]{0,2})$' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: full global custody proof required';END IF;
 v_count:=(d#>>'{custody,orgCount}')::bigint;
 IF v_count>512 OR v_count<>jsonb_array_length(d#>'{custody,orgs}') OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(d#>'{custody,orgs}') x GROUP BY x->>'orgId' HAVING count(*)<>1) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody org inventory count';END IF;
 SELECT coalesce(jsonb_agg(x->'orgId' ORDER BY n),'[]') INTO v_inventory
  FROM jsonb_array_elements(d#>'{custody,orgs}') WITH ORDINALITY AS r(x,n);
 v_sha:=encode(sha256(convert_to(v_inventory::text,'UTF8')),'hex');
 IF v_sha IS DISTINCT FROM d#>>'{custody,orgInventorySha256}' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: custody org inventory digest';END IF;
 -- Exact descriptor order is source-attested. Both release callers use that
 -- same order; do not impose an unproved target collation or partial inventory.
 FOR f IN SELECT value FROM jsonb_array_elements(d#>'{custody,orgs}') LOOP
  PERFORM sessions.ingest_exact_keys(f,ARRAY['orgId','records','pages','chainSha256']);
  PERFORM sessions.ingest_identifier(f->'orgId',false);
  PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=f->>'orgId' AND control_epoch=c.control_epoch
   AND schema_generation=v_gen AND valid_until>clock_timestamp() FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: all existing org admission required';END IF;
  SELECT * INTO i FROM sessions.ingest_custody_imports WHERE org_id=f->>'orgId' FOR SHARE;
  IF NOT FOUND OR i.status<>'complete' OR i.control_epoch<>c.control_epoch OR i.source_identity_sha256<>c.source_identity_sha256
   OR i.source_proof_sha256 IS DISTINCT FROM c.baseline->>'sourceProofSha256'
   OR i.expected_records::text IS DISTINCT FROM f->>'records' OR i.expected_pages::text IS DISTINCT FROM f->>'pages'
   OR i.expected_chain IS DISTINCT FROM f->>'chainSha256' OR i.received_records<>i.expected_records
   OR i.received_pages<>i.expected_pages OR i.chain_sha256<>i.expected_chain THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: all existing org custody completion required';END IF;
 END LOOP;
 SELECT * INTO g FROM sessions.ingest_custody_global WHERE singleton FOR UPDATE;
 IF FOUND AND (g.control_epoch<>c.control_epoch OR g.source_identity_sha256<>c.source_identity_sha256
  OR g.source_proof_sha256 IS DISTINCT FROM c.baseline->>'sourceProofSha256' OR g.org_inventory IS DISTINCT FROM v_inventory
  OR g.org_inventory_sha256<>v_sha OR g.org_count<>v_count) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: global custody adoption refused';END IF;
 PERFORM sessions.ingest_require_generation(v_gen);
 INSERT INTO sessions.ingest_custody_global(singleton,generation,control_epoch,source_identity_sha256,source_proof_sha256,
  org_inventory,org_inventory_sha256,org_count)
 VALUES(true,v_gen,c.control_epoch,c.source_identity_sha256,c.baseline->>'sourceProofSha256',v_inventory,v_sha,v_count)
 ON CONFLICT(singleton) DO UPDATE SET generation=EXCLUDED.generation,released_at=clock_timestamp();
 RETURN jsonb_build_object('accepted',true,'globalComplete',true,'orgCount',v_count::text,
  'orgInventorySha256',v_sha,'sourceProofSha256',c.baseline->>'sourceProofSha256','generation',v_gen);
END $custody_release$;
REVOKE ALL ON FUNCTION sessions.ingest_custody_release(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
