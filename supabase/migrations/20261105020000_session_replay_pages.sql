-- Append-only release rename (registry v2); upstream provenance: 20261103040000_session_replay_pages.sql. Never applied under the upstream filename in this release.
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
-- Dormant materialized canonical reads. Objects are written/verified by the Node
-- producer first; publication is an exact source+epoch CAS. No reader table grant.
CREATE INDEX session_index_replay_canonical_live ON sessions.session_index
 (org_id,workspace_id,harness,(coalesce(canonical_session_id,session_id)),manifest_key) WHERE deleted_at IS NULL;
CREATE SEQUENCE sessions.session_replay_epoch_seq AS bigint NO CYCLE;
CREATE TABLE sessions.session_replay_publications (
 org_id text NOT NULL, workspace_id text NOT NULL, harness text NOT NULL, session_id text NOT NULL,
 epoch bigint NOT NULL DEFAULT nextval('sessions.session_replay_epoch_seq') CHECK(epoch>0),
 published_epoch bigint, source_fingerprint text, source_snapshot jsonb,
 generation text, source_generation text, artifact_prefix text, index_key text, index_sha256 text, index_bytes integer,
 published_at timestamptz,
 PRIMARY KEY(org_id,workspace_id,harness,session_id),
 CHECK(index_key IS NULL OR (published_epoch>0 AND source_fingerprint~'^[0-9a-f]{64}$' AND generation~'^[0-9a-f]{64}$'
  AND source_generation~'^[0-9a-f]{64}$' AND index_sha256~'^[0-9a-f]{64}$' AND index_bytes BETWEEN 1 AND 262144
  AND source_snapshot IS NOT NULL AND artifact_prefix IS NOT NULL AND published_at IS NOT NULL))
);
ALTER TABLE sessions.session_replay_publications ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_replay_publications FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON SEQUENCE sessions.session_replay_epoch_seq FROM PUBLIC,anon,authenticated,service_role;
INSERT INTO sessions.session_replay_publications(org_id,workspace_id,harness,session_id)
 SELECT DISTINCT org_id,workspace_id,harness,coalesce(canonical_session_id,session_id) FROM sessions.session_index WHERE deleted_at IS NULL;

CREATE FUNCTION sessions.session_replay_sources_changed() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
BEGIN
 -- Bind source fields; unrelated classifier/summary writes must not strand a
 -- current artifact waiting for an S3 event that those writes never emit.
 IF TG_OP='UPDATE' AND ROW(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id,OLD.machine_id,OLD.canonical_session_id,OLD.canonical_machine_id,
   OLD.user_id,OLD.storage_provider,OLD.bucket,OLD.manifest_key,OLD.content_sha256,OLD.raw_format,OLD.format_version,OLD.manifest_etag,OLD.raw_bytes,OLD.chunk_count,OLD.revision,OLD.deleted_at)
  IS NOT DISTINCT FROM ROW(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id,NEW.machine_id,NEW.canonical_session_id,NEW.canonical_machine_id,
   NEW.user_id,NEW.storage_provider,NEW.bucket,NEW.manifest_key,NEW.content_sha256,NEW.raw_format,NEW.format_version,NEW.manifest_etag,NEW.raw_bytes,NEW.chunk_count,NEW.revision,NEW.deleted_at) THEN RETURN NEW; END IF;
 IF TG_OP<>'INSERT' THEN
  INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id)
   VALUES(OLD.org_id,OLD.workspace_id,OLD.harness,coalesce(OLD.canonical_session_id,OLD.session_id))
   ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET epoch=nextval('sessions.session_replay_epoch_seq');
 END IF;
 IF TG_OP='INSERT' OR (TG_OP='UPDATE' AND
  ROW(OLD.org_id,OLD.workspace_id,OLD.harness,coalesce(OLD.canonical_session_id,OLD.session_id)) IS DISTINCT FROM
  ROW(NEW.org_id,NEW.workspace_id,NEW.harness,coalesce(NEW.canonical_session_id,NEW.session_id))) THEN
  INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id)
   VALUES(NEW.org_id,NEW.workspace_id,NEW.harness,coalesce(NEW.canonical_session_id,NEW.session_id))
   ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET epoch=nextval('sessions.session_replay_epoch_seq');
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $changed$;
CREATE TRIGGER session_replay_sources_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.session_replay_sources_changed();

-- A directory tombstone is authoritative even before pointer cleanup arrives.
CREATE FUNCTION sessions.session_replay_directory_changed() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $directory_changed$
BEGIN
 IF TG_OP='UPDATE' AND ROW(OLD.deleted_at,OLD.event_projection_epoch) IS NOT DISTINCT FROM ROW(NEW.deleted_at,NEW.event_projection_epoch) THEN RETURN NEW; END IF;
 IF TG_OP='DELETE' THEN
  INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id)
   VALUES(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id)
   ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET epoch=nextval('sessions.session_replay_epoch_seq');
  RETURN OLD;
 END IF;
 INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id)
  VALUES(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id)
  ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET epoch=nextval('sessions.session_replay_epoch_seq');
 RETURN NEW;
END $directory_changed$;
CREATE TRIGGER session_replay_directory_changed AFTER INSERT OR UPDATE OF deleted_at,event_projection_epoch OR DELETE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_replay_directory_changed();

-- Internal, exact JSONB catalog snapshot. Epoch survives erasure/recreation and
-- comes from a non-cycling sequence; returning to identical pointers is not ABA.
CREATE FUNCTION sessions.session_replay_source_snapshot(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $snapshot$
 SELECT jsonb_build_object('epoch',a.epoch::text,
 'directory', (SELECT jsonb_build_object('epoch',d.event_projection_epoch::text,'deleted_at',d.deleted_at) FROM sessions.session_directory d
  WHERE d.org_id=p_org AND d.workspace_id=p_workspace AND d.harness=p_harness AND d.session_id=p_session),
 'pending', coalesce((SELECT jsonb_agg(jsonb_build_object('source_session_id',m.source_session_id,'epoch',m.epoch::text) ORDER BY m.source_session_id COLLATE "C")
  FROM sessions.session_directory_event_pending m WHERE m.org_id=p_org AND m.workspace_id=p_workspace AND m.harness=p_harness
   AND m.source_session_id IN(SELECT p.session_id FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness AND coalesce(p.canonical_session_id,p.session_id)=p_session AND p.deleted_at IS NULL)),'[]'::jsonb),
 'pointers',coalesce((
  SELECT jsonb_agg(jsonb_build_object(
   'org_id',p.org_id,'workspace_id',p.workspace_id,'harness',p.harness,
   'session_id',coalesce(p.canonical_session_id,p.session_id),'machine_id',coalesce(p.canonical_machine_id,p.machine_id),
   'source_session_id',p.session_id,'source_machine_id',p.machine_id,
   'canonical_session_id',p.canonical_session_id,'canonical_machine_id',p.canonical_machine_id,
   'user_id',p.user_id,'storage_provider',p.storage_provider,'bucket',p.bucket,
   'manifest_key',p.manifest_key,'content_sha256',encode(p.content_sha256,'hex'),
   'raw_format',p.raw_format,'format_version',p.format_version,'manifest_etag',p.manifest_etag,'raw_bytes',p.raw_bytes::text,'chunk_count',p.chunk_count,
   'revision',p.revision::text,'deleted_at',p.deleted_at)
   ORDER BY p.manifest_key COLLATE "C",p.session_id COLLATE "C",p.machine_id COLLATE "C")
  FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
   AND coalesce(p.canonical_session_id,p.session_id)=p_session AND p.deleted_at IS NULL
 ),'[]'::jsonb)) FROM sessions.session_replay_publications a
 WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session;
$snapshot$;

CREATE FUNCTION sessions.capture_session_replay_pages(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capture$
DECLARE ids text[]; canonical_id text; captured jsonb; fp text;
BEGIN
 IF p_org IS NULL OR p_org='' OR p_workspace IS NULL OR p_workspace='' OR p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
  OR p_session IS NULL OR char_length(p_session) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='replay capture: invalid scope';
 END IF;
 SELECT array_agg(DISTINCT coalesce(p.canonical_session_id,p.session_id)) INTO ids
  FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness AND p.session_id=p_session AND p.deleted_at IS NULL;
 IF cardinality(ids)>1 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='replay capture: ambiguous canonical source'; END IF;
 canonical_id:=coalesce(ids[1],p_session);
 IF (SELECT count(*) FROM(SELECT 1 FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
  AND coalesce(p.canonical_session_id,p.session_id)=canonical_id AND p.deleted_at IS NULL LIMIT 10001) bounded)>10000 THEN
  RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='replay capture: pointer capacity exceeded';
 END IF;
 captured:=sessions.session_replay_source_snapshot(p_org,p_workspace,p_harness,canonical_id);
 IF captured IS NULL THEN
  IF EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
    AND coalesce(p.canonical_session_id,p.session_id)=canonical_id AND p.deleted_at IS NULL) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='replay capture: source epoch missing';
  END IF;
  captured:=jsonb_build_object('epoch',NULL,'pointers','[]'::jsonb);
 END IF;
 IF octet_length(captured::text)>8388608 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='replay capture: metadata capacity exceeded'; END IF;
 fp:=encode(sha256(convert_to(captured::text,'UTF8')),'hex');
 RETURN jsonb_build_object('exists',jsonb_array_length(captured->'pointers')>0,
  'identity',jsonb_build_object('orgId',p_org,'workspaceId',p_workspace,'source',p_harness,'sessionId',canonical_id),
  'blocked',jsonb_typeof(captured->'directory') IS DISTINCT FROM 'object' OR coalesce(captured->'directory'->>'deleted_at','')<>'',
  'source',jsonb_build_object('epoch',captured->>'epoch','fingerprint',fp),'snapshot',captured);
END $capture$;

CREATE FUNCTION sessions.publish_session_replay_pages(
 p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_snapshot jsonb,
 p_generation text,p_source_generation text,p_prefix text,p_index_key text,p_index_sha256 text,p_index_bytes integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE live jsonb; fp text; anchor jsonb; expected_prefix text;
BEGIN
 -- Row lock is also acquired by every pointer mutation trigger. A pointer txn
 -- already in progress invalidates this publication before that txn can commit.
 PERFORM 1 FROM sessions.session_replay_publications a WHERE a.org_id=p_org AND a.workspace_id=p_workspace
  AND a.harness=p_harness AND a.session_id=p_session AND a.epoch=p_epoch FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 live:=sessions.session_replay_source_snapshot(p_org,p_workspace,p_harness,p_session);
 fp:=encode(sha256(convert_to(live::text,'UTF8')),'hex');
 IF jsonb_typeof(live->'directory') IS DISTINCT FROM 'object' OR coalesce(live->'directory'->>'deleted_at','')<>'' OR live IS DISTINCT FROM p_snapshot OR fp IS DISTINCT FROM p_fingerprint OR jsonb_array_length(live->'pointers')=0
  OR p_generation IS NULL OR p_source_generation IS NULL OR p_index_sha256 IS NULL OR p_index_bytes IS NULL OR p_prefix IS NULL OR p_index_key IS NULL
  OR p_generation !~ '^[0-9a-f]{64}$' OR p_source_generation !~ '^[0-9a-f]{64}$'
  OR p_index_sha256 !~ '^[0-9a-f]{64}$' OR p_index_bytes NOT BETWEEN 1 AND 262144 THEN RETURN false; END IF;
 anchor:=live->'pointers'->0;
 IF (anchor->>'manifest_key') !~ '/manifest\.json$' OR (anchor->>'content_sha256') !~ '^[0-9a-f]{64}$' THEN RETURN false; END IF;
 expected_prefix:=regexp_replace(anchor->>'manifest_key','manifest\.json$','')||'normalized/read-projection-v1/'||(anchor->>'content_sha256')||'/'||p_generation||'/';
 IF p_prefix IS DISTINCT FROM expected_prefix OR p_index_key IS DISTINCT FROM p_prefix||p_index_sha256||'.json' THEN RETURN false; END IF;
 UPDATE sessions.session_replay_publications SET published_epoch=p_epoch,source_fingerprint=fp,source_snapshot=live,
  generation=p_generation,source_generation=p_source_generation,artifact_prefix=p_prefix,index_key=p_index_key,index_sha256=p_index_sha256,index_bytes=p_index_bytes,published_at=now()
  WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session AND epoch=p_epoch;
 RETURN FOUND;
END $publish$;

-- Same signed search-worker policy; org is never accepted as a caller argument.
-- Existing sources without a current publication are distinct from absence.
CREATE FUNCTION sessions.session_directory_replay_pages(p_workspace_id text,p_harness text,p_session_id text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE org text; captured jsonb; a sessions.session_replay_publications%ROWTYPE; identity jsonb; current_ok boolean;
BEGIN
 IF p_workspace_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='replay pages: workspace required'; END IF;
 SELECT s.org_id INTO org FROM sessions.session_directory_worker_scope(p_workspace_id) s;
 IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' OR p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='replay pages: invalid scope';
 END IF;
 -- Public requests name canonical source IDs only. A physical layout alias is
 -- authority for a backend event, never permission to return another session.
 IF NOT EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=org AND p.workspace_id=p_workspace_id AND p.harness=p_harness
   AND coalesce(p.canonical_session_id,p.session_id)=p_session_id AND p.deleted_at IS NULL) THEN
  captured:=coalesce(sessions.session_replay_source_snapshot(org,p_workspace_id,p_harness,p_session_id),jsonb_build_object('epoch',NULL,'pointers','[]'::jsonb));
  RETURN jsonb_build_object('version',1,'exists',false,'current',false,
   'identity',jsonb_build_object('orgId',org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id),
   'source',jsonb_build_object('epoch',captured->>'epoch','fingerprint',encode(sha256(convert_to(captured::text,'UTF8')),'hex')));
 END IF;
 captured:=sessions.capture_session_replay_pages(org,p_workspace_id,p_harness,p_session_id);
 IF captured->'identity'->>'sessionId' IS DISTINCT FROM p_session_id THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='replay pages: ambiguous canonical identity';
 END IF;
 identity:=captured->'identity';
 IF NOT (captured->>'exists')::boolean THEN RETURN jsonb_build_object('version',1,'exists',false,'current',false,'identity',identity,'source',captured->'source'); END IF;
 SELECT * INTO a FROM sessions.session_replay_publications r WHERE r.org_id=org AND r.workspace_id=p_workspace_id AND r.harness=p_harness AND r.session_id=identity->>'sessionId';
 current_ok:=coalesce(NOT (captured->>'blocked')::boolean AND a.index_key IS NOT NULL AND a.epoch=a.published_epoch AND a.epoch::text=captured->'source'->>'epoch'
  AND a.source_fingerprint=captured->'source'->>'fingerprint' AND a.source_snapshot=captured->'snapshot',false);
 RETURN jsonb_build_object('version',1,'exists',true,'current',current_ok,'identity',identity,'source',captured->'source')||
  CASE WHEN current_ok THEN jsonb_build_object('generation',a.generation,'sourceGeneration',a.source_generation,'prefix',a.artifact_prefix,
   'index',jsonb_build_object('key',a.index_key,'sha256',a.index_sha256,'bytes',a.index_bytes)) ELSE '{}'::jsonb END;
END $claim$;
REVOKE ALL ON FUNCTION sessions.session_replay_directory_changed() FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_replay_sources_changed() FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_replay_source_snapshot(text,text,text,text) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.capture_session_replay_pages(text,text,text,text) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_replay_pages(text,text,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.capture_session_replay_pages(text,text,text,text) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_replay_pages(text,text,text) TO session_directory_search_worker;
