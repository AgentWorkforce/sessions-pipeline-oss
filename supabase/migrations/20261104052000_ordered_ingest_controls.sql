-- Ordered dual-control target. Epoch is owner-attested from frozen Neon0077.
-- No source payload, signed identity, or first caller can adopt/rotate authority.
-- This unlanded append requires previously empty admission or explicit reviewed
-- owner preparation; never invent an epoch for an existing enrollment.
ALTER TABLE sessions.ingest_buffer_admission
 ADD COLUMN control_epoch text NOT NULL CHECK(control_epoch ~ '^[0-9a-f]{64}$');

CREATE TABLE sessions.ingest_control_receipts (
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 epoch text NOT NULL CHECK(epoch ~ '^[0-9a-f]{64}$'),
 version bigint NOT NULL CHECK(version>0),
 digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'),
 kind text NOT NULL CHECK(kind IN('selection','reset')),
 result jsonb NOT NULL CHECK(jsonb_typeof(result)='object'),
 schema_generation text NOT NULL CHECK(schema_generation ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,user_id,epoch,version)
);
REVOKE ALL ON sessions.ingest_control_receipts
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Invoker/owner-only: called only within the existing ingest-role SD entrypoints.
CREATE FUNCTION sessions.ingest_apply_ordered_control(p jsonb,p_kind text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ordered$
DECLARE
 t jsonb;c jsonb;v_org text;v_ws text;v_user text;v_epoch text;v_version bigint;
 v_digest text;v_generation text;v_payload jsonb;v_origin text;v_request text;
 v_last bigint;v_old sessions.ingest_control_receipts%ROWTYPE;v_result jsonb;v_outcome text;
BEGIN
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
  v_outcome:='applied';
 END IF;
 PERFORM sessions.ingest_require_buffer_admission(v_org);
 RETURN v_result||jsonb_build_object('control',jsonb_build_object('epoch',v_epoch,
  'version',v_version::text,'digest',v_digest,'kind',p_kind,'outcome',v_outcome));
END
$ordered$;
REVOKE ALL ON FUNCTION sessions.ingest_apply_ordered_control(jsonb,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Existing OIDs/ACLs retained. Only genuinely unenrolled legacy callers retain
-- the old unversioned behavior. Buffered selection/reset cannot fall back.
CREATE OR REPLACE FUNCTION sessions.ingest_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selection$
DECLARE t jsonb;v_org text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_org:=p#>>'{tenant,orgId}';
 IF p?'control' OR p?'generation'
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
 IF p?'control' OR p?'generation'
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
