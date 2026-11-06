-- Provenance: renamed from PR #188's
-- 20261103090000_session_brief_documents.sql; re-slotted to 20261105024000 by
-- the window #2 registry after 20261105010000 while preserving relative order.
-- Intentional composition delta: an empty workspace identity is valid, while a
-- missing/JSON-null workspace remains invalid (see queue_session_brief_document).
-- Dormant derived CH metadata publication. Directory text remains authoritative.
-- No external writes, text search RPC, or direct reader/backend table grants.
CREATE SEQUENCE sessions.session_brief_document_epoch_seq AS bigint NO CYCLE;
CREATE TABLE sessions.session_brief_document_control(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 live boolean NOT NULL DEFAULT false
);
INSERT INTO sessions.session_brief_document_control(singleton,live) VALUES(true,false);
CREATE TABLE sessions.session_brief_document_scopes(
 org_id text NOT NULL,workspace_id text NOT NULL,watermark bigint NOT NULL DEFAULT 0,
 initialized boolean NOT NULL DEFAULT false,cursor_phase text NOT NULL DEFAULT 'live',cursor_source text,cursor_session text,
 PRIMARY KEY(org_id,workspace_id),CHECK(cursor_phase IN('live','deleted')),CHECK((cursor_source IS NULL)=(cursor_session IS NULL))
);
CREATE TABLE sessions.session_brief_document_outbox(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 epoch bigint NOT NULL,document jsonb NOT NULL,document_hash text NOT NULL CHECK(document_hash~'^[0-9a-f]{64}$'),
 published_epoch bigint NOT NULL DEFAULT 0,lease_nonce uuid,lease_expires_at timestamptz,
 attempts integer NOT NULL DEFAULT 0,available_at timestamptz NOT NULL DEFAULT clock_timestamp(),last_error text,
 PRIMARY KEY(org_id,workspace_id,harness,session_id),CHECK(epoch>0 AND published_epoch>=0 AND published_epoch<=epoch),
 CHECK((lease_nonce IS NULL)=(lease_expires_at IS NULL)),CHECK(attempts>=0)
);
CREATE INDEX session_brief_document_dirty ON sessions.session_brief_document_outbox(available_at,org_id,workspace_id,harness,session_id) WHERE epoch<>published_epoch;
ALTER TABLE sessions.session_brief_document_scopes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_brief_document_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_brief_document_control ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_brief_document_control,sessions.session_brief_document_scopes,sessions.session_brief_document_outbox FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON SEQUENCE sessions.session_brief_document_epoch_seq FROM PUBLIC,anon,authenticated,service_role;

CREATE FUNCTION sessions.set_session_brief_documents_live(p_live boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp SET lock_timeout='5s' AS $set_live$
DECLARE v_live boolean;
BEGIN
 IF p_live IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief documents: live must be boolean'; END IF;
 SELECT live INTO v_live FROM sessions.session_brief_document_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='brief documents: control row missing'; END IF;
 IF v_live=p_live THEN RETURN v_live; END IF;
 -- Wait out directory writers before changing the trigger-visible state. On enable,
 -- this also makes the scope-key snapshot and the live flip one atomic boundary.
 LOCK TABLE sessions.session_directory IN SHARE MODE;
 IF p_live THEN
  INSERT INTO sessions.session_brief_document_scopes(org_id,workspace_id)
  SELECT DISTINCT org_id,workspace_id FROM sessions.session_directory ON CONFLICT DO NOTHING;
 END IF;
 -- A disabled interval deliberately takes no per-row publication locks. Reset
 -- every scope on either edge so authority is incomplete until a fresh scan has
 -- reconciled updates, insertions and deletions that happened while disabled.
 UPDATE sessions.session_brief_document_scopes
 SET initialized=false,cursor_phase='live',cursor_source=NULL,cursor_session=NULL;
 UPDATE sessions.session_brief_document_control SET live=p_live WHERE singleton;
 RETURN p_live;
END $set_live$;

CREATE FUNCTION sessions.session_brief_document_json(p_row sessions.session_directory,p_erased boolean DEFAULT false)
RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $document$
 SELECT jsonb_build_object('version',1,'orgId',(p_row).org_id,'workspaceId',(p_row).workspace_id,
 'source',(p_row).harness,'sessionId',(p_row).session_id,'visible',(p_row).event_projection_ready AND (p_row).event_projection_version=1 AND (p_row).event_count>0 AND (p_row).event_first_ts IS NOT NULL AND (p_row).event_last_ts IS NOT NULL,'projectionEpoch',(p_row).event_projection_epoch::text,'projectRequiresHydration',(p_row).project_requires_hydration,'deleted',p_erased OR (p_row).deleted_at IS NOT NULL,
 'title',CASE WHEN p_erased OR (p_row).deleted_at IS NOT NULL THEN '' ELSE coalesce((p_row).title,'') END,
 'summary',CASE WHEN p_erased OR (p_row).deleted_at IS NOT NULL THEN '' ELSE coalesce((p_row).short_summary,'') END,
 'projectId',CASE WHEN p_erased OR (p_row).deleted_at IS NOT NULL THEN '' ELSE coalesce((p_row).event_project_id,'') END,
 'lastActivityAt',CASE WHEN p_erased OR (p_row).deleted_at IS NOT NULL THEN '1970-01-01T00:00:00.000000Z'
 ELSE to_char(coalesce((p_row).event_last_ts,(p_row).last_activity_at) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END);
$document$;
CREATE FUNCTION sessions.queue_session_brief_document(p_document jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $queue$
DECLARE e bigint;o text:=p_document->>'orgId';w text:=p_document->>'workspaceId';h text:=p_document->>'source';s text:=p_document->>'sessionId';
BEGIN
 -- Empty workspace is a supported canonical identity; a missing workspace key
 -- is not. The other identity components remain non-empty.
 IF coalesce(o,'')='' OR w IS NULL OR coalesce(h,'')='' OR coalesce(s,'')='' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid identity'; END IF;
 INSERT INTO sessions.session_brief_document_scopes(org_id,workspace_id) VALUES(o,w) ON CONFLICT DO NOTHING;
 -- Allocate after taking the tenant lock: a late older transaction cannot
 -- change documents without advancing the committed tenant watermark.
 PERFORM 1 FROM sessions.session_brief_document_scopes WHERE org_id=o AND workspace_id=w FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sessions.session_brief_document_outbox b WHERE b.org_id=o AND b.workspace_id=w AND b.harness=h AND b.session_id=s AND b.document=p_document) THEN RETURN; END IF;
 e:=nextval('sessions.session_brief_document_epoch_seq'::regclass);
 INSERT INTO sessions.session_brief_document_outbox AS b(org_id,workspace_id,harness,session_id,epoch,document,document_hash)
 VALUES(o,w,h,s,e,p_document,encode(sha256(convert_to(p_document::text,'UTF8')),'hex'))
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET epoch=excluded.epoch,document=excluded.document,document_hash=excluded.document_hash,
 lease_nonce=NULL,lease_expires_at=NULL,attempts=0,available_at=clock_timestamp(),last_error=NULL;
 UPDATE sessions.session_brief_document_scopes SET watermark=e WHERE org_id=o AND workspace_id=w;
END $queue$;
CREATE FUNCTION sessions.session_brief_document_changed()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE before_doc jsonb;after_doc jsonb;v jsonb;docs jsonb:='[]'::jsonb;
BEGIN
 -- Applying this migration is deliberately dormant. This check must remain the
 -- first operation: default-off directory writes take no shared scope-row lock,
 -- allocate no epoch and create no derived publication state.
 IF NOT coalesce((SELECT live FROM sessions.session_brief_document_control WHERE singleton),false) THEN RETURN NULL; END IF;
 IF TG_OP<>'INSERT' THEN before_doc:=sessions.session_brief_document_json(OLD,false); END IF;
 IF TG_OP<>'DELETE' THEN after_doc:=sessions.session_brief_document_json(NEW,false); END IF;
 IF TG_OP='DELETE' THEN docs:=jsonb_build_array(sessions.session_brief_document_json(OLD,true));
 ELSIF TG_OP='INSERT' THEN docs:=jsonb_build_array(after_doc);
 ELSIF before_doc IS DISTINCT FROM after_doc THEN
  IF (OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id) IS DISTINCT FROM (NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id) THEN
   docs:=jsonb_build_array(sessions.session_brief_document_json(OLD,true),after_doc);
  ELSE docs:=jsonb_build_array(after_doc); END IF;
 END IF;
 FOR v IN SELECT value FROM jsonb_array_elements(docs) ORDER BY value->>'orgId' COLLATE "C",value->>'workspaceId' COLLATE "C",value->>'source' COLLATE "C",value->>'sessionId' COLLATE "C" LOOP PERFORM sessions.queue_session_brief_document(v); END LOOP;
 RETURN NULL;
END $changed$;
CREATE TRIGGER session_brief_document_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_directory FOR EACH ROW EXECUTE FUNCTION sessions.session_brief_document_changed();

CREATE FUNCTION sessions.backfill_session_brief_documents(p_limit integer DEFAULT 200)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $backfill$
DECLARE k sessions.session_brief_document_scopes%ROWTYPE;d sessions.session_directory%ROWTYPE;b sessions.session_brief_document_outbox%ROWTYPE;
 n integer:=0;more boolean:=false;last_source text;last_session text;document jsonb;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 200 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid batch'; END IF;
 PERFORM 1 FROM sessions.session_brief_document_control WHERE singleton AND live FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='brief document: publication is disabled'; END IF;
 SELECT * INTO k FROM sessions.session_brief_document_scopes WHERE NOT initialized ORDER BY org_id COLLATE "C",workspace_id COLLATE "C" LIMIT 1 FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('complete',true,'processed',0); END IF;
 IF k.cursor_phase='live' THEN
  FOR d IN SELECT * FROM sessions.session_directory x WHERE x.org_id=k.org_id AND x.workspace_id=k.workspace_id
  AND (k.cursor_source IS NULL OR (x.harness COLLATE "C",x.session_id COLLATE "C")>(k.cursor_source COLLATE "C",k.cursor_session COLLATE "C"))
  ORDER BY x.harness COLLATE "C",x.session_id COLLATE "C" LIMIT p_limit+1 LOOP
   IF n=p_limit THEN more:=true;EXIT; END IF;
   PERFORM sessions.queue_session_brief_document(sessions.session_brief_document_json(d,false));n:=n+1;last_source:=d.harness;last_session:=d.session_id;
  END LOOP;
  IF more THEN
   UPDATE sessions.session_brief_document_scopes SET cursor_source=last_source,cursor_session=last_session
   WHERE org_id=k.org_id AND workspace_id=k.workspace_id;
   RETURN jsonb_build_object('orgId',k.org_id,'workspaceId',k.workspace_id,'complete',false,'processed',n);
  END IF;
  UPDATE sessions.session_brief_document_scopes SET cursor_phase='deleted',cursor_source=NULL,cursor_session=NULL
  WHERE org_id=k.org_id AND workspace_id=k.workspace_id;
  k.cursor_phase:='deleted';k.cursor_source:=NULL;k.cursor_session:=NULL;last_source:=NULL;last_session:=NULL;more:=false;
 END IF;
 -- Reconcile keys that existed in the prior published image but disappeared
 -- while the trigger was disabled. Without this bounded second phase, a delete
 -- during a kill-switch interval could leave an old CH document authoritative.
 FOR b IN SELECT * FROM sessions.session_brief_document_outbox q
 WHERE q.org_id=k.org_id AND q.workspace_id=k.workspace_id
 AND (k.cursor_source IS NULL OR (q.harness COLLATE "C",q.session_id COLLATE "C")>(k.cursor_source COLLATE "C",k.cursor_session COLLATE "C"))
 AND NOT EXISTS(SELECT 1 FROM sessions.session_directory x WHERE (x.org_id,x.workspace_id,x.harness,x.session_id)=(q.org_id,q.workspace_id,q.harness,q.session_id))
 ORDER BY q.harness COLLATE "C",q.session_id COLLATE "C" LIMIT (p_limit-n)+1 LOOP
  IF n=p_limit THEN more:=true;EXIT; END IF;
  document:=b.document||jsonb_build_object('deleted',true,'title','','summary','','projectId','','lastActivityAt','1970-01-01T00:00:00.000000Z');
  PERFORM sessions.queue_session_brief_document(document);n:=n+1;last_source:=b.harness;last_session:=b.session_id;
 END LOOP;
 IF more THEN
  UPDATE sessions.session_brief_document_scopes SET cursor_phase='deleted',cursor_source=last_source,cursor_session=last_session
  WHERE org_id=k.org_id AND workspace_id=k.workspace_id;
 ELSE
  UPDATE sessions.session_brief_document_scopes SET initialized=true,cursor_phase='live',cursor_source=NULL,cursor_session=NULL
  WHERE org_id=k.org_id AND workspace_id=k.workspace_id;
 END IF;
 RETURN jsonb_build_object('orgId',k.org_id,'workspaceId',k.workspace_id,'complete',NOT more,'processed',n);
END $backfill$;
CREATE FUNCTION sessions.claim_session_brief_documents(p_nonce uuid,p_limit integer DEFAULT 100)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE result jsonb;
BEGIN
 IF p_nonce IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 200 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid claim'; END IF;
 WITH wanted AS MATERIALIZED(SELECT b.org_id,b.workspace_id,b.harness,b.session_id FROM sessions.session_brief_document_outbox b
 WHERE b.epoch<>b.published_epoch AND b.attempts<5 AND b.available_at<=clock_timestamp() AND (b.lease_nonce IS NULL OR b.lease_expires_at<=clock_timestamp())
 ORDER BY b.available_at,b.org_id COLLATE "C",b.workspace_id COLLATE "C",b.harness COLLATE "C",b.session_id COLLATE "C" LIMIT p_limit FOR UPDATE SKIP LOCKED),
 updated AS(UPDATE sessions.session_brief_document_outbox b SET lease_nonce=p_nonce,lease_expires_at=clock_timestamp()+interval '120 seconds',attempts=attempts+1
 FROM wanted q WHERE (b.org_id,b.workspace_id,b.harness,b.session_id)=(q.org_id,q.workspace_id,q.harness,q.session_id)
 RETURNING b.document,b.epoch,b.document_hash)
 SELECT coalesce(jsonb_agg(jsonb_build_object('document',u.document,'epoch',u.epoch::text,'hash',u.document_hash,'leaseNonce',p_nonce::text)),'[]'::jsonb) INTO result FROM updated u;
 RETURN result;
END $claim$;
CREATE FUNCTION sessions.ack_session_brief_documents(p_nonce uuid,p_items jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $ack$
DECLARE n integer;
BEGIN
 IF p_nonce IS NULL OR jsonb_typeof(p_items) IS DISTINCT FROM 'array' OR jsonb_array_length(p_items)>200 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid acknowledgement'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_items) e WHERE jsonb_typeof(e) IS DISTINCT FROM 'object' OR coalesce(e->>'epoch','') !~ '^[1-9][0-9]*$' OR coalesce(e->>'hash','') !~ '^[0-9a-f]{64}$') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid acknowledgement'; END IF;
 UPDATE sessions.session_brief_document_outbox b SET published_epoch=b.epoch,lease_nonce=NULL,lease_expires_at=NULL,last_error=NULL
 FROM jsonb_to_recordset(p_items) AS e("orgId" text,"workspaceId" text,source text,"sessionId" text,epoch text,hash text)
 WHERE (b.org_id,b.workspace_id,b.harness,b.session_id)=(e."orgId",e."workspaceId",e.source,e."sessionId") AND b.epoch::text=e.epoch AND b.document_hash=e.hash
 AND b.lease_nonce=p_nonce AND b.lease_expires_at>clock_timestamp();
 GET DIAGNOSTICS n=ROW_COUNT;RETURN n;
END $ack$;
CREATE FUNCTION sessions.retry_session_brief_documents(p_nonce uuid,p_error text)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $retry$
DECLARE n integer;
BEGIN
 IF p_nonce IS NULL OR p_error IS NULL OR p_error !~ '^[a-z][a-z0-9_]{0,63}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid retry'; END IF;
 UPDATE sessions.session_brief_document_outbox SET lease_nonce=NULL,lease_expires_at=NULL,available_at=clock_timestamp()+interval '30 seconds',last_error=p_error
 WHERE lease_nonce=p_nonce AND lease_expires_at>clock_timestamp();GET DIAGNOSTICS n=ROW_COUNT;RETURN n;
END $retry$;

CREATE FUNCTION sessions.session_brief_document_authority(p_workspace_id text,p_session_harnesses text[] DEFAULT NULL,p_session_ids text[] DEFAULT NULL)
RETURNS TABLE(org_id text,workspace_id text,watermark text,initialized boolean,complete boolean,documents jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $authority$
DECLARE o text;k sessions.session_brief_document_scopes%ROWTYPE;dirty boolean;docs jsonb:='[]'::jsonb;
BEGIN
 SELECT s.org_id INTO o FROM (SELECT sessions.session_directory_events_require_ready(p_workspace_id) AS org_id) s;
 IF (p_session_harnesses IS NULL) IS DISTINCT FROM (p_session_ids IS NULL) OR coalesce(cardinality(p_session_ids),0)>200 OR cardinality(p_session_harnesses) IS DISTINCT FROM cardinality(p_session_ids)
 OR EXISTS(SELECT 1 FROM unnest(p_session_harnesses,p_session_ids) a(h,s) WHERE coalesce(a.h,'')='' OR coalesce(a.s,'')='') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='brief document: invalid exact tuples'; END IF;
 SELECT * INTO k FROM sessions.session_brief_document_scopes s WHERE s.org_id=o AND s.workspace_id=p_workspace_id;
 dirty:=EXISTS(SELECT 1 FROM sessions.session_brief_document_outbox b WHERE b.org_id=o AND b.workspace_id=p_workspace_id AND b.epoch<>b.published_epoch);
 IF coalesce(k.initialized,false) AND NOT dirty AND cardinality(p_session_ids)>0 THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('epoch',b.epoch::text,'hash',b.document_hash,'directory',to_jsonb(d))),'[]'::jsonb) INTO docs
  FROM sessions.list_session_directory_events(p_workspace_id=>p_workspace_id,p_session_harnesses=>p_session_harnesses,p_session_ids=>p_session_ids,p_limit=>200) d
  JOIN sessions.session_brief_document_outbox b ON (b.org_id,b.workspace_id,b.harness,b.session_id)=(d.org_id,d.workspace_id,d.harness,d.session_id)
  WHERE b.published_epoch=b.epoch AND b.document->>'deleted'='false';
 END IF;
 RETURN QUERY SELECT o,p_workspace_id,coalesce(k.watermark,0)::text,coalesce(k.initialized,false),coalesce(k.initialized,false) AND NOT dirty,docs;
END $authority$;

REVOKE ALL ON FUNCTION sessions.set_session_brief_documents_live(boolean),sessions.session_brief_document_json(sessions.session_directory,boolean),sessions.queue_session_brief_document(jsonb),sessions.session_brief_document_changed(),sessions.backfill_session_brief_documents(integer),sessions.claim_session_brief_documents(uuid,integer),sessions.ack_session_brief_documents(uuid,jsonb),sessions.retry_session_brief_documents(uuid,text),sessions.session_brief_document_authority(text,text[],text[]) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.backfill_session_brief_documents(integer),sessions.claim_session_brief_documents(uuid,integer),sessions.ack_session_brief_documents(uuid,jsonb),sessions.retry_session_brief_documents(uuid,text) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.session_brief_document_authority(text,text[],text[]) TO session_directory_search_worker;
