-- S3-proven ingest replay bounds. Depends on 040300's fence lock/epochs.
-- No transcript content, original revision IDs or original digests are invented.
DO $dependency$
BEGIN
 IF to_regprocedure('sessions.ingest_lock_fences(text,text,text)') IS NULL OR
    to_regclass('sessions.ingest_fence_versions') IS NULL OR
    to_regclass('sessions.ingest_session_ownership') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: generation fence prerequisite absent';
 END IF;
END $dependency$;

-- One-time per-user import. A stale Neon packet can never re-disable a user
-- after any modern destination operation or after an earlier baseline was sealed.
REVOKE ALL ON FUNCTION sessions.ingest_seed_records(jsonb),sessions.ingest_seed_fences(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE TABLE sessions.ingest_seed_fence_baselines (
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 fingerprint text NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
 imported_at timestamptz NOT NULL DEFAULT clock_timestamp(),closed_at timestamptz,
 PRIMARY KEY(org_id,workspace_id,user_id)
);
ALTER TABLE sessions.ingest_seed_fence_baselines ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_seed_fence_baselines
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_seed_fence_baseline(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $baseline$
DECLARE v_org text;v_ws text;v_user text;v_fp text;v_seen text;v_first boolean;
 v_d jsonb;v_r jsonb;e jsonb;v_deleted int:=0;v_resets int:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest_seeder');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['org_id','workspace_id','user_id','deletions','resets']);
 v_org:=sessions.ingest_identifier(p->'org_id',false);v_ws:=sessions.ingest_identifier(p->'workspace_id',true);
 v_user:=sessions.ingest_identifier(p->'user_id',false);
 IF jsonb_typeof(p->'deletions') IS DISTINCT FROM 'array' OR jsonb_typeof(p->'resets') IS DISTINCT FROM 'array'
  OR jsonb_array_length(p->'deletions')+jsonb_array_length(p->'resets')>5000 OR octet_length(p::text)>16777216
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: baseline bound';END IF;
 v_d:='[]';v_r:='[]';
 FOR e IN SELECT x FROM jsonb_array_elements(p->'deletions')x LOOP
  PERFORM sessions.ingest_exact_keys(e,ARRAY['origin_id','source','session_id']);
  v_d:=v_d||jsonb_build_array(jsonb_build_object(
   'origin_id',sessions.ingest_identifier(e->'origin_id',false),
   'source',sessions.ingest_identifier(e->'source',false),'session_id',sessions.ingest_identifier(e->'session_id',false)));
 END LOOP;
 FOR e IN SELECT x FROM jsonb_array_elements(p->'resets')x LOOP
  PERFORM sessions.ingest_exact_keys(e,ARRAY['request_id','deleted']);
  IF jsonb_typeof(e->'deleted') IS DISTINCT FROM 'string' OR (e->>'deleted')!~'^(0|[1-9][0-9]{0,18})$'
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: reset count';END IF;
  v_r:=v_r||jsonb_build_array(jsonb_build_object('request_id',sessions.ingest_identifier(e->'request_id',false),'deleted',(e->>'deleted')::bigint));
 END LOOP;
 SELECT coalesce(jsonb_agg(x ORDER BY x::text COLLATE "C"),'[]') INTO v_d FROM (SELECT DISTINCT x FROM jsonb_array_elements(v_d)x)q;
 SELECT coalesce(jsonb_agg(x ORDER BY x::text COLLATE "C"),'[]') INTO v_r FROM (SELECT DISTINCT x FROM jsonb_array_elements(v_r)x)q;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_r)x GROUP BY x->>'request_id' HAVING count(*)>1)
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: conflicting reset identity';END IF;
 v_fp:=encode(sha256(convert_to(jsonb_build_array('neon-fence-baseline-v1',v_org,v_ws,v_user,v_d,v_r)::text,'UTF8')),'hex');
 -- INSERT's return value is the linearization fence against a first concurrent
 -- claim/enable/reset. Checking EXISTS before the lock would race.
 INSERT INTO sessions.ingest_fence_owners VALUES(v_org,v_ws,v_user) ON CONFLICT DO NOTHING RETURNING true INTO v_first;
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT fingerprint INTO v_seen FROM sessions.ingest_seed_fence_baselines
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 IF FOUND THEN
  IF v_seen=v_fp THEN RETURN jsonb_build_object('accepted',true,'replay',true,'deletions',0,'resets',0);END IF;
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: baseline already sealed';
 END IF;
 IF v_first IS NOT TRUE OR
   EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user) OR
   EXISTS(SELECT 1 FROM sessions.ingest_record_revisions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user) OR
   EXISTS(SELECT 1 FROM sessions.ingest_fence_versions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user) OR
   EXISTS(SELECT 1 FROM sessions.upload_resets WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user) OR
   EXISTS(SELECT 1 FROM sessions.upload_deletions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: modern destination already initialized';END IF;
 FOR e IN SELECT x FROM jsonb_array_elements(v_d)x LOOP
  PERFORM sessions.ingest_apply_selection(v_org,v_ws,v_user,e->>'origin_id',
   jsonb_build_array(jsonb_build_array(e->>'source',e->>'session_id')),false);
  v_deleted:=v_deleted+1;
 END LOOP;
 FOR e IN SELECT x FROM jsonb_array_elements(v_r)x LOOP
  INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id,deleted)
   VALUES(v_org,v_ws,v_user,e->>'request_id',(e->>'deleted')::bigint) ON CONFLICT DO NOTHING;
  v_resets:=v_resets+1;
 END LOOP;
 INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint,imported_at) VALUES(v_org,v_ws,v_user,v_fp,clock_timestamp());
 RETURN jsonb_build_object('accepted',true,'replay',false,'deletions',v_deleted,'resets',v_resets);
END $baseline$;
-- The former bulk import had no first-initialization or immutable-baseline fence.
CREATE OR REPLACE FUNCTION sessions.ingest_seed_fences(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $old_fences$
BEGIN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='ingest seed: per-user baseline required';END $old_fences$;
REVOKE ALL ON FUNCTION sessions.ingest_seed_fences(jsonb) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


CREATE FUNCTION sessions.ingest_seed_close_baseline()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $close$
DECLARE v_org text;v_ws text;v_user text;
BEGIN
 --040500's trusted buffer claim persists bytes only; historical proof remains
 --open until explicit canonical admission. Before that column exists every row
 --is conservatively canonical. No client payload can choose this exemption.
 IF TG_TABLE_NAME='ingest_batches' AND
   coalesce(to_jsonb(NEW)->>'publication_state','canonical')='buffered'
 THEN RETURN NEW;END IF;
 IF TG_OP='DELETE' THEN v_org:=OLD.org_id;v_ws:=OLD.workspace_id;v_user:=OLD.user_id;
 ELSE v_org:=NEW.org_id;v_ws:=NEW.workspace_id;v_user:=NEW.user_id;END IF;
 UPDATE sessions.ingest_seed_fence_baselines SET closed_at=clock_timestamp()
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND closed_at IS NULL;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
END $close$;
CREATE TRIGGER ingest_seed_close_batch AFTER INSERT OR UPDATE ON sessions.ingest_batches
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_seed_close_baseline();
CREATE TRIGGER ingest_seed_close_versions AFTER INSERT OR UPDATE OR DELETE ON sessions.ingest_fence_versions
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_seed_close_baseline();
CREATE TRIGGER ingest_seed_close_resets AFTER INSERT ON sessions.upload_resets
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_seed_close_baseline();
CREATE TRIGGER ingest_seed_close_enable AFTER DELETE ON sessions.upload_deletions
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_seed_close_baseline();

CREATE TABLE sessions.ingest_seed_source_epochs (
 org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
 epoch bigint NOT NULL CHECK(epoch >= 0),
 PRIMARY KEY(org_id,workspace_id,source,session_id)
);
CREATE TABLE sessions.ingest_seed_captures (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
 generation text NOT NULL CHECK(generation ~ '^[0-9a-f]{64}$'),
 source_epoch bigint NOT NULL, fingerprint text NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
 state jsonb NOT NULL, captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '15 minutes'
);
ALTER TABLE sessions.ingest_seed_source_epochs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_seed_captures ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_seed_source_epochs,sessions.ingest_seed_captures
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
INSERT INTO sessions.ingest_seed_source_epochs
 SELECT DISTINCT org_id,workspace_id,harness,session_id,0 FROM sessions.session_index;

CREATE FUNCTION sessions.ingest_seed_source_changed()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE v_keys jsonb := '[]'; v_key jsonb;
BEGIN
 IF TG_OP='UPDATE' AND
   ROW(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id,OLD.machine_id,OLD.user_id,
       OLD.storage_provider,OLD.bucket,OLD.manifest_key,OLD.revision,OLD.content_sha256,OLD.raw_format,OLD.deleted_at)
   IS NOT DISTINCT FROM
   ROW(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id,NEW.machine_id,NEW.user_id,
       NEW.storage_provider,NEW.bucket,NEW.manifest_key,NEW.revision,NEW.content_sha256,NEW.raw_format,NEW.deleted_at)
 THEN RETURN NEW; END IF;
 IF TG_OP<>'INSERT' THEN v_keys:=v_keys||jsonb_build_array(jsonb_build_array(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id)); END IF;
 IF TG_OP<>'DELETE' THEN v_keys:=v_keys||jsonb_build_array(jsonb_build_array(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id)); END IF;
 FOR v_key IN SELECT DISTINCT x FROM jsonb_array_elements(v_keys)x ORDER BY x LOOP
  INSERT INTO sessions.ingest_seed_source_epochs AS e VALUES(v_key->>0,v_key->>1,v_key->>2,v_key->>3,1)
   ON CONFLICT(org_id,workspace_id,source,session_id) DO UPDATE SET epoch=e.epoch+1;
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $changed$;
CREATE TRIGGER ingest_seed_source_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_seed_source_changed();

-- One authoritative scope image. All machines/owners participate; no LIMIT64.
-- The guard refuses the sentinel and a metadata envelope beyond16MiB.
CREATE FUNCTION sessions.ingest_seed_scope_state(p_org text,p_ws text,p_source text,p_session text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $state$
DECLARE v_rows jsonb; v_state jsonb; v_epoch bigint; v_n integer;
BEGIN
 SELECT epoch INTO v_epoch FROM sessions.ingest_seed_source_epochs
 WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.machine_id COLLATE "C",x.manifest_key COLLATE "C"),'[]'),count(*)::int
 INTO v_rows,v_n FROM (
  SELECT s.org_id,s.workspace_id,s.user_id,s.harness,s.session_id,s.machine_id,
    s.storage_provider,s.bucket,s.manifest_key,s.revision::text,encode(s.content_sha256,'hex') AS content_sha256,s.raw_format::text,
    (SELECT count(*)::text FROM sessions.upload_resets r WHERE r.org_id=s.org_id AND r.workspace_id=s.workspace_id AND r.user_id=s.user_id) AS reset_epoch,
    coalesce((SELECT jsonb_object_agg(v.origin_id,v.epoch::text ORDER BY v.origin_id COLLATE "C")
       FROM sessions.ingest_fence_versions v WHERE v.org_id=s.org_id AND v.workspace_id=s.workspace_id AND v.user_id=s.user_id
        AND v.source=p_source AND v.session_id=p_session),'{}') AS fence_versions,
    coalesce((SELECT jsonb_agg(d.origin_id ORDER BY d.origin_id COLLATE "C")
       FROM sessions.upload_deletions d WHERE d.org_id=s.org_id AND d.workspace_id=s.workspace_id AND d.user_id=s.user_id
        AND d.source=p_source AND d.session_id=p_session),'[]') AS deleted_origins
  FROM sessions.session_index s WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source AND s.session_id=p_session
    AND s.deleted_at IS NULL ORDER BY s.machine_id COLLATE "C",s.manifest_key COLLATE "C" LIMIT 1001
 )x;
 IF v_n>1000 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='ingest seed: pointer bound'; END IF;
 v_state:=jsonb_build_object('source_epoch',coalesce(v_epoch,0)::text,'pointers',v_rows);
 IF octet_length(v_state::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='ingest seed: metadata bound'; END IF;
 RETURN v_state;
END $state$;

-- Keyset discovery of live S3 namespaces. Exporter SQLite is never authority.
CREATE FUNCTION sessions.ingest_seed_scope_page(p jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE v_org text; v_generation text; v_after jsonb; v_limit int; v_rows jsonb; v_next jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest_seeder');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['org_id','generation','after','limit']);
 v_org:=sessions.ingest_identifier(p->'org_id',false); v_generation:=sessions.ingest_digest(p->'generation');
 IF NOT EXISTS(SELECT 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass' AND generation=v_generation)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: generation not ready'; END IF;
 v_limit:=(p->>'limit')::int;
 IF v_limit IS NULL OR v_limit<1 OR v_limit>1000 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: page bound'; END IF;
 v_after:=p->'after';
 IF v_after IS DISTINCT FROM 'null'::jsonb AND
    (jsonb_typeof(v_after)<>'array' OR jsonb_array_length(v_after)<>3)
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: cursor'; END IF;
 SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.workspace_id COLLATE "C",x.source COLLATE "C",x.session_id COLLATE "C"),'[]')
 INTO v_rows FROM (
  SELECT * FROM (
   SELECT workspace_id,harness AS source,session_id,
    array_agg(DISTINCT user_id ORDER BY user_id) AS users,count(*)::int AS pointers,
    sum(greatest(raw_bytes,0))::text AS raw_bytes,sum(greatest(chunk_count,0))::text AS chunks
   FROM sessions.session_index WHERE org_id=v_org AND deleted_at IS NULL AND
    (v_after='null'::jsonb OR ROW(workspace_id COLLATE "C",harness COLLATE "C",session_id COLLATE "C")>
      ROW((v_after->>0) COLLATE "C",(v_after->>1) COLLATE "C",(v_after->>2) COLLATE "C"))
   GROUP BY workspace_id,harness,session_id
  ) names ORDER BY workspace_id COLLATE "C",source COLLATE "C",session_id COLLATE "C" LIMIT v_limit+1
 )x;
 IF jsonb_array_length(v_rows)>v_limit THEN
  v_rows:=v_rows-(jsonb_array_length(v_rows)-1);
  v_next:=jsonb_build_array(v_rows->(v_limit-1)->>'workspace_id',v_rows->(v_limit-1)->>'source',v_rows->(v_limit-1)->>'session_id');
 END IF;
 RETURN jsonb_build_object('rows',v_rows,'next',v_next);
END $page$;

CREATE FUNCTION sessions.ingest_seed_scope_capture(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capture$
DECLARE v_org text;v_ws text;v_source text;v_session text;v_generation text;v_mode text;
 v_state jsonb;v_fingerprint text;v_id uuid;v_expiry timestamptz;v_user text;v_before bigint;v_locked bigint;v_rows jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest_seeder');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['org_id','workspace_id','source','session_id','generation','mode']);
 v_org:=sessions.ingest_identifier(p->'org_id',false);v_ws:=sessions.ingest_identifier(p->'workspace_id',true);
 v_source:=sessions.ingest_identifier(p->'source',false);v_session:=sessions.ingest_identifier(p->'session_id',false);
 v_generation:=sessions.ingest_digest(p->'generation');v_mode:=p->>'mode';
 IF v_mode NOT IN('preview','lease') OR v_mode IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: mode'; END IF;
 IF NOT EXISTS(SELECT 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass' AND generation=v_generation)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: generation not ready'; END IF;
 IF v_mode='lease' THEN
  IF EXISTS(SELECT 1 FROM sessions.session_index s WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_source AND s.session_id=v_session AND s.deleted_at IS NULL
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_seed_fence_baselines b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id AND b.user_id=s.user_id AND b.closed_at IS NULL))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: fence baseline required';END IF;
  SELECT epoch INTO v_before FROM sessions.ingest_seed_source_epochs WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
  FOR v_user IN SELECT DISTINCT user_id FROM sessions.session_index WHERE org_id=v_org AND workspace_id=v_ws AND harness=v_source AND session_id=v_session AND deleted_at IS NULL ORDER BY user_id LOOP
   PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
  END LOOP;
  SELECT epoch INTO v_locked FROM sessions.ingest_seed_source_epochs WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session FOR UPDATE;
  IF v_before IS DISTINCT FROM v_locked THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='ingest seed: source changed'; END IF;
 END IF;
 v_state:=sessions.ingest_seed_scope_state(v_org,v_ws,v_source,v_session);
 v_fingerprint:=encode(sha256(convert_to(v_state::text,'UTF8')),'hex');
 IF v_mode='lease' THEN
  INSERT INTO sessions.ingest_seed_captures(org_id,workspace_id,source,session_id,generation,source_epoch,fingerprint,state)
  VALUES(v_org,v_ws,v_source,v_session,v_generation,(v_state->>'source_epoch')::bigint,v_fingerprint,v_state)
  RETURNING id,expires_at INTO v_id,v_expiry;
 END IF;
 SELECT coalesce(jsonb_agg(
  (x-'reset_epoch'-'fence_versions'-'deleted_origins')||jsonb_build_object('capture',jsonb_build_object(
   'pointer_id',encode(sha256(convert_to(jsonb_build_array(v_fingerprint,x)::text,'UTF8')),'hex'),
   'scope_sha256',v_fingerprint,'reset_epoch',x->'reset_epoch','fence_versions',x->'fence_versions','deleted_origins',x->'deleted_origins'))
  ORDER BY x->>'machine_id',x->>'manifest_key'),'[]') INTO v_rows FROM jsonb_array_elements(v_state->'pointers')x;
 RETURN jsonb_build_object('protocol',1,'capture_id',v_id,'expires_at',v_expiry,'fingerprint',v_fingerprint,
   'source_epoch',v_state->'source_epoch','pointers',v_rows);
END $capture$;

-- Old session/export-state booleans can no longer write the replay index.
CREATE OR REPLACE FUNCTION sessions.ingest_seed_records(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $unsafe$
BEGIN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='ingest seed: S3 proof required'; END $unsafe$;
REVOKE ALL ON FUNCTION sessions.ingest_seed_records(jsonb) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

ALTER TABLE sessions.ingest_record_revisions ALTER COLUMN revision_id DROP NOT NULL;
ALTER TABLE sessions.ingest_record_revisions ALTER COLUMN digest DROP NOT NULL;
ALTER TABLE sessions.ingest_record_revisions ADD COLUMN seed_proof_sha256 text,
 ADD COLUMN seed_snapshot_sha256 text, ADD COLUMN seed_proof_revision bigint;
ALTER TABLE sessions.ingest_record_revisions ADD CONSTRAINT ingest_revision_source_proof CHECK(
 (revision_id IS NOT NULL AND digest IS NOT NULL) OR
 (revision_id IS NULL AND digest IS NULL AND seed_proof_revision IS NOT NULL
  AND seed_proof_sha256 IS NOT NULL AND seed_snapshot_sha256 IS NOT NULL AND seed_proof_revision=revision
  AND seed_proof_sha256 ~ '^[0-9a-f]{64}$' AND seed_snapshot_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE FUNCTION sessions.ingest_seed_proven_records(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $proven$
DECLARE c sessions.ingest_seed_captures%ROWTYPE;v_epoch bigint;v_state jsonb;v_row jsonb;v_pointer jsonb;
 v_pointer_id text;v_record text;v_origin text;v_user text;v_revision bigint;v_operation text;v_source text;v_session text;
 v_proof text;v_count int:=0;v_fenced int:=0;v_user_lock text;v_stored sessions.ingest_record_revisions%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest_seeder');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['capture_id','fingerprint','source_head','records']);
 IF jsonb_typeof(p->'source_head') IS DISTINCT FROM 'string' OR (p->>'source_head')!~'^[0-9a-f]{40}$'
   OR jsonb_typeof(p->'records') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'records')>1000
   OR octet_length(p::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: proof input'; END IF;
 SELECT * INTO c FROM sessions.ingest_seed_captures WHERE id=(p->>'capture_id')::uuid FOR UPDATE;
 IF NOT FOUND OR c.fingerprint IS DISTINCT FROM p->>'fingerprint' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: unknown capture'; END IF;
 IF clock_timestamp()>=c.expires_at THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: capture expired'; END IF;
 PERFORM 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass' AND generation=c.generation FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: generation not ready'; END IF;
 FOR v_user_lock IN SELECT DISTINCT x->>'user_id' FROM jsonb_array_elements(c.state->'pointers')x ORDER BY 1 LOOP
  PERFORM sessions.ingest_lock_fences(c.org_id,c.workspace_id,v_user_lock);
 END LOOP;
 SELECT epoch INTO v_epoch FROM sessions.ingest_seed_source_epochs
 WHERE org_id=c.org_id AND workspace_id=c.workspace_id AND source=c.source AND session_id=c.session_id FOR UPDATE;
 IF v_epoch IS DISTINCT FROM c.source_epoch THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='ingest seed: source changed'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(c.state->'pointers') x
   WHERE NOT EXISTS(SELECT 1 FROM sessions.ingest_seed_fence_baselines b
     WHERE b.org_id=c.org_id AND b.workspace_id=c.workspace_id AND b.user_id=x->>'user_id' AND b.closed_at IS NULL))
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='ingest seed: baseline no longer idle';END IF;
 v_state:=sessions.ingest_seed_scope_state(c.org_id,c.workspace_id,c.source,c.session_id);
 IF encode(sha256(convert_to(v_state::text,'UTF8')),'hex')<>c.fingerprint THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='ingest seed: source or fence changed'; END IF;
 FOR v_row IN SELECT x FROM jsonb_array_elements(p->'records')x LOOP
  PERFORM sessions.ingest_exact_keys(v_row,ARRAY['org_id','workspace_id','user_id','source','session_id','origin_id','record_id','kind','revision','operation','received_at','semantic_sha256','pointer_id']||CASE WHEN v_row?'accepted_seq' THEN ARRAY['accepted_seq']::text[] ELSE '{}'::text[] END);
  v_pointer_id:=sessions.ingest_digest(v_row->'pointer_id');v_pointer:=NULL;
  SELECT x INTO v_pointer FROM jsonb_array_elements(c.state->'pointers')x
   WHERE encode(sha256(convert_to(jsonb_build_array(c.fingerprint,x)::text,'UTF8')),'hex')=v_pointer_id;
  IF v_pointer IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: unknown pointer witness'; END IF;
  v_user:=sessions.ingest_identifier(v_row->'user_id',false);
  v_origin:=sessions.ingest_identifier(v_row->'origin_id',false);v_record:=sessions.ingest_identifier(v_row->'record_id',false);
  v_source:=sessions.ingest_identifier(v_row->'source',false);
  v_session:=CASE WHEN v_row->'session_id'='null'::jsonb THEN NULL ELSE sessions.ingest_identifier(v_row->'session_id',false) END;
  IF v_row->>'org_id' IS DISTINCT FROM c.org_id OR v_row->>'workspace_id' IS DISTINCT FROM c.workspace_id OR
     v_user IS DISTINCT FROM v_pointer->>'user_id' OR v_source IS DISTINCT FROM c.source OR
     (v_session IS DISTINCT FROM c.session_id AND NOT(v_session IS NULL AND c.source='_none' AND c.session_id='_origin'))
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: namespace or owner'; END IF;
  IF jsonb_typeof(v_row->'revision') IS DISTINCT FROM 'string' OR (v_row->>'revision')!~'^[1-9][0-9]{0,18}$'
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: revision'; END IF;
  v_revision:=(v_row->>'revision')::bigint;
  v_operation:=v_row->>'operation';
  IF v_operation IS NULL OR v_operation NOT IN('upsert','delete') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: operation'; END IF;
  PERFORM sessions.ingest_identifier(v_row->'kind',false);
  IF jsonb_typeof(v_row->'received_at') IS DISTINCT FROM 'string' OR (v_row->>'received_at')::timestamptz IS NULL
   OR (v_row?'accepted_seq' AND (jsonb_typeof(v_row->'accepted_seq') IS DISTINCT FROM 'string'
      OR (v_row->>'accepted_seq')!~'^[1-9][0-9]{0,18}$' OR (v_row->>'accepted_seq')::bigint<1))
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: acceptance evidence';END IF;
  PERFORM sessions.ingest_digest(v_row->'semantic_sha256');
  IF v_session IS NOT NULL AND EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=c.org_id AND d.workspace_id=c.workspace_id AND d.user_id=v_user AND d.origin_id=v_origin AND d.source=v_source AND d.session_id=v_session)
  THEN v_fenced:=v_fenced+1;CONTINUE;END IF;
  SELECT * INTO v_stored FROM sessions.ingest_record_revisions r WHERE r.org_id=c.org_id AND r.workspace_id=c.workspace_id AND r.origin_id=v_origin AND r.record_id=v_record FOR UPDATE;
  IF FOUND AND ROW(v_stored.user_id,v_stored.source,v_stored.session_id) IS DISTINCT FROM ROW(v_user,v_source,v_session)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: record namespace collision'; END IF;
  v_proof:=encode(sha256(convert_to(jsonb_build_array('s3-record-proof-v1',c.id,c.fingerprint,p->>'source_head',v_row)::text,'UTF8')),'hex');
  INSERT INTO sessions.ingest_record_revisions AS r
   (org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id,seed_proof_sha256,seed_snapshot_sha256,seed_proof_revision)
   VALUES(c.org_id,c.workspace_id,v_origin,v_record,v_revision,NULL,NULL,v_user,v_source,v_session,v_proof,c.fingerprint,v_revision)
   ON CONFLICT(org_id,workspace_id,origin_id,record_id) DO UPDATE SET
    revision=excluded.revision,revision_id=NULL,digest=NULL,user_id=excluded.user_id,source=excluded.source,session_id=excluded.session_id,
    seed_proof_sha256=excluded.seed_proof_sha256,seed_snapshot_sha256=excluded.seed_snapshot_sha256,
    seed_proof_revision=excluded.seed_proof_revision,updated_at=clock_timestamp()
   WHERE excluded.revision>r.revision;
  IF FOUND THEN v_count:=v_count+1; END IF;
  IF v_session IS NOT NULL THEN INSERT INTO sessions.ingest_session_ownership VALUES(c.org_id,c.workspace_id,v_user,v_origin,v_source,v_session) ON CONFLICT DO NOTHING; END IF;
 END LOOP;
 -- A blocked row write may outlive the lease. This exception rolls EVERY write back.
 IF clock_timestamp()>=c.expires_at THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: capture expired'; END IF;
 RETURN jsonb_build_object('accepted',true,'upserted',v_count,'fenced',v_fenced);
END $proven$;

REVOKE ALL ON FUNCTION sessions.ingest_seed_close_baseline(),sessions.ingest_seed_source_changed(),
 sessions.ingest_seed_scope_state(text,text,text,text),sessions.ingest_seed_scope_page(jsonb),
 sessions.ingest_seed_scope_capture(jsonb),sessions.ingest_seed_proven_records(jsonb),sessions.ingest_seed_fence_baseline(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_seed_scope_page(jsonb),sessions.ingest_seed_scope_capture(jsonb),
 sessions.ingest_seed_proven_records(jsonb),sessions.ingest_seed_fence_baseline(jsonb) TO bulls96_session_ingest_seeder;
