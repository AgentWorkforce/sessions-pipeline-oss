-- Append-only release rename (registry v2); upstream provenance: 20261103060000_session_analysis_publications.sql. Never applied under the upstream filename in this release.
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
-- Immutable full v1 payloads live in object storage, not this pointer catalog.
CREATE TABLE sessions.session_analysis_publications (
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 source_epoch bigint NOT NULL CHECK(source_epoch>0),source_fingerprint text NOT NULL CHECK(source_fingerprint~'^[0-9a-f]{64}$'),
 snapshot_at timestamptz NOT NULL,snapshot_id text NOT NULL CHECK(snapshot_id~'^[0-9a-f]{64}$'),
 snapshot_sha256 text NOT NULL CHECK(snapshot_sha256~'^[0-9a-f]{64}$'),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),artifact_prefix text NOT NULL,
 index_key text NOT NULL,index_sha256 text NOT NULL CHECK(index_sha256~'^[0-9a-f]{64}$'),
 index_bytes integer NOT NULL CHECK(index_bytes BETWEEN 1 AND 262144),refresh_nonce uuid NOT NULL,published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
ALTER TABLE sessions.session_analysis_publications ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_publications FROM PUBLIC,anon,authenticated,service_role;

-- Collapsed demand metadata only. The read role may request this exact live
-- canonical scope through one JWT-bound RPC; it never receives table privileges.
CREATE TABLE sessions.session_analysis_wakeups (
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 source_epoch bigint NOT NULL,source_fingerprint text NOT NULL,
 demand_until timestamptz NOT NULL,last_demand_at timestamptz NOT NULL,next_refresh_at timestamptz NOT NULL,
 generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),lease_generation bigint,must_publish boolean NOT NULL DEFAULT true,
 lease_nonce uuid,lease_expires_at timestamptz,attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),last_error_code text,
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
CREATE INDEX session_analysis_wakeups_due ON sessions.session_analysis_wakeups(next_refresh_at);
ALTER TABLE sessions.session_analysis_wakeups ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_wakeups FROM PUBLIC,anon,authenticated,service_role;


CREATE FUNCTION sessions.publish_session_analysis_cache(
 p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,
 p_snapshot_at timestamptz,p_snapshot_id text,p_snapshot_sha256 text,p_generation text,
 p_prefix text,p_index_key text,p_index_sha256 text,p_index_bytes integer,p_lease_nonce uuid
) RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE c jsonb;anchor jsonb;expected_prefix text;prior sessions.session_analysis_publications%ROWTYPE;
BEGIN
 -- Same row lock as every live pointer/directory epoch mutation. Missing live
 -- directory authority cannot be recreated by this publication path.
 PERFORM 1 FROM sessions.session_replay_publications a WHERE a.org_id=p_org AND a.workspace_id=p_workspace
  AND a.harness=p_harness AND a.session_id=p_session AND a.epoch=p_epoch FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 PERFORM 1 FROM sessions.session_analysis_wakeups w WHERE w.org_id=p_org AND w.workspace_id=p_workspace AND w.harness=p_harness AND w.session_id=p_session
  AND w.source_epoch=p_epoch AND w.source_fingerprint=p_fingerprint AND w.lease_nonce=p_lease_nonce AND w.generation=w.lease_generation AND w.lease_expires_at>clock_timestamp() FOR UPDATE;
 IF NOT FOUND THEN RETURN false;END IF;
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT coalesce((c->>'exists')::boolean,false) OR coalesce((c->>'blocked')::boolean,true)
  OR c->'identity'->>'sessionId' IS DISTINCT FROM p_session OR c->'source'->>'epoch' IS DISTINCT FROM p_epoch::text
  OR c->'source'->>'fingerprint' IS DISTINCT FROM p_fingerprint THEN RETURN false; END IF;
 IF p_snapshot_at IS NULL OR p_snapshot_at>clock_timestamp()+interval '30 seconds' OR p_snapshot_at<clock_timestamp()-interval '120 seconds'
  OR p_snapshot_id IS NULL OR p_snapshot_id!~'^[0-9a-f]{64}$' OR p_snapshot_sha256 IS NULL OR p_snapshot_sha256!~'^[0-9a-f]{64}$'
  OR p_generation IS NULL OR p_generation!~'^[0-9a-f]{64}$' OR p_index_sha256 IS NULL OR p_index_sha256!~'^[0-9a-f]{64}$'
  OR p_index_bytes IS NULL OR p_index_bytes NOT BETWEEN 1 AND 262144 OR p_prefix IS NULL OR p_index_key IS NULL THEN RETURN false; END IF;
 anchor:=c->'snapshot'->'pointers'->0;
 expected_prefix:=left(anchor->>'manifest_key',length(anchor->>'manifest_key')-length('manifest.json'))||'normalized/read-projection-v1/'||(anchor->>'content_sha256')||'/'||p_generation||'/';
 IF p_prefix IS DISTINCT FROM expected_prefix OR p_index_key IS DISTINCT FROM p_prefix||p_index_sha256||'.json' THEN RETURN false; END IF;
 SELECT * INTO prior FROM sessions.session_analysis_publications a WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session;
 IF FOUND THEN
  IF p_snapshot_at<prior.snapshot_at THEN RETURN false; END IF;
  IF p_snapshot_at=prior.snapshot_at THEN
   -- Equal source timestamps may retry exact bytes, never divergent content or
   -- a different source epoch. No clock granularity assumption hides a race.
   IF ROW(prior.source_epoch,prior.source_fingerprint,prior.snapshot_id,prior.snapshot_sha256,prior.generation,prior.artifact_prefix,prior.index_key,prior.index_sha256,prior.index_bytes)
    IS DISTINCT FROM ROW(p_epoch,p_fingerprint,p_snapshot_id,p_snapshot_sha256,p_generation,p_prefix,p_index_key,p_index_sha256,p_index_bytes) THEN RETURN false;END IF;
  END IF;
 END IF;
 INSERT INTO sessions.session_analysis_publications(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint,snapshot_at,snapshot_id,snapshot_sha256,generation,artifact_prefix,index_key,index_sha256,index_bytes,refresh_nonce)
 VALUES(p_org,p_workspace,p_harness,p_session,p_epoch,p_fingerprint,p_snapshot_at,p_snapshot_id,p_snapshot_sha256,p_generation,p_prefix,p_index_key,p_index_sha256,p_index_bytes,p_lease_nonce)
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,
 snapshot_at=excluded.snapshot_at,snapshot_id=excluded.snapshot_id,snapshot_sha256=excluded.snapshot_sha256,generation=excluded.generation,
 artifact_prefix=excluded.artifact_prefix,index_key=excluded.index_key,index_sha256=excluded.index_sha256,index_bytes=excluded.index_bytes,refresh_nonce=excluded.refresh_nonce,published_at=clock_timestamp();
 -- A pointer upsert can wait on a row lock or trigger past lease/TTL expiry.
 -- Raising inside this exception block rolls back the provisional write.
 PERFORM 1 FROM sessions.session_analysis_wakeups w WHERE w.org_id=p_org AND w.workspace_id=p_workspace AND w.harness=p_harness AND w.session_id=p_session
  AND w.source_epoch=p_epoch AND w.source_fingerprint=p_fingerprint AND w.lease_nonce=p_lease_nonce AND w.generation=w.lease_generation AND w.lease_expires_at>clock_timestamp();
 IF NOT FOUND OR p_snapshot_at<clock_timestamp()-interval '120 seconds' OR p_snapshot_at>clock_timestamp()+interval '30 seconds' THEN
  RAISE EXCEPTION USING ERRCODE='RH002',MESSAGE='analysis publication: authority expired during write';
 END IF;
 RETURN true;
EXCEPTION WHEN SQLSTATE 'RH002' THEN RETURN false;
END $publish$;

CREATE FUNCTION sessions.session_directory_analysis_cache(p_workspace_id text,p_harness text,p_session_id text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE base jsonb;c jsonb;a sessions.session_analysis_publications%ROWTYPE;ok boolean;body jsonb;org text;
BEGIN
 -- Canonical-only public identity and exact same signed search-worker policy.
 base:=sessions.session_directory_replay_pages(p_workspace_id,p_harness,p_session_id);
 body:=jsonb_build_object('version',1,'exists',base->'exists','current',false,'identity',base->'identity','source',base->'source');
 IF NOT (base->>'exists')::boolean THEN RETURN body; END IF;
 org:=base->'identity'->>'orgId';
 c:=sessions.capture_session_replay_pages(org,p_workspace_id,p_harness,p_session_id);
 SELECT * INTO a FROM sessions.session_analysis_publications p WHERE p.org_id=org AND p.workspace_id=p_workspace_id AND p.harness=p_harness AND p.session_id=p_session_id;
 ok:=coalesce(NOT(c->>'blocked')::boolean AND a.source_epoch::text=c->'source'->>'epoch'
  AND a.source_fingerprint=c->'source'->>'fingerprint' AND a.snapshot_at>=clock_timestamp()-interval '120 seconds' AND a.snapshot_at<=clock_timestamp()+interval '30 seconds',false);
 IF NOT ok THEN RETURN body; END IF;
 RETURN body||jsonb_build_object('current',true,'source',c->'source','snapshotAt',to_char(a.snapshot_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'snapshotId',a.snapshot_id,'snapshotSha256',a.snapshot_sha256,'generation',a.generation,'prefix',a.artifact_prefix,
  'validUntil',to_char((a.snapshot_at+interval '120 seconds') AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'index',jsonb_build_object('key',a.index_key,'sha256',a.index_sha256,'bytes',a.index_bytes));
END $claim$;
REVOKE ALL ON FUNCTION sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_analysis_cache(text,text,text) FROM PUBLIC,anon,authenticated,service_role;
-- Remove any default/future-grantee drift before granting exact role execution.
DO $acl$ DECLARE f text;grantee text; BEGIN
 FOREACH f IN ARRAY ARRAY['sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid)','sessions.session_directory_analysis_cache(text,text,text)'] LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated,service_role',f);
  FOR grantee IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=to_regprocedure(f) AND a.grantee<>p.proowner LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,grantee);
  END LOOP;
 END LOOP;
END $acl$;
GRANT EXECUTE ON FUNCTION sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_analysis_cache(text,text,text) TO session_directory_search_worker;

CREATE FUNCTION sessions.session_analysis_cache_demand(p_org text,p_workspace text,p_harness text,p_session text,p_interactive boolean)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue$
DECLARE c jsonb;found_row sessions.session_analysis_wakeups%ROWTYPE;now_at timestamptz:=clock_timestamp();body jsonb;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF c->'identity'->>'sessionId' IS DISTINCT FROM p_session THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='analysis demand: canonical identity required';END IF;
 body:=jsonb_build_object('version',1,'accepted',false,'exists',c->'exists','identity',c->'identity','source',c->'source','nextRefreshAt',NULL);
 IF NOT(c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN body;END IF;
 INSERT INTO sessions.session_analysis_wakeups AS w(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint,demand_until,last_demand_at,next_refresh_at)
 VALUES(p_org,p_workspace,p_harness,p_session,(c->'source'->>'epoch')::bigint,c->'source'->>'fingerprint',CASE WHEN p_interactive THEN now_at+interval '5 minutes' ELSE now_at END,CASE WHEN p_interactive THEN now_at ELSE '-infinity'::timestamptz END,now_at)
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET
  source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,generation=w.generation+1,must_publish=true,
  demand_until=CASE WHEN p_interactive THEN excluded.demand_until ELSE w.demand_until END,last_demand_at=CASE WHEN p_interactive THEN excluded.last_demand_at ELSE w.last_demand_at END,
  next_refresh_at=CASE WHEN NOT p_interactive OR w.source_epoch IS DISTINCT FROM excluded.source_epoch THEN now_at ELSE w.next_refresh_at END,
  lease_nonce=CASE WHEN w.source_epoch IS DISTINCT FROM excluded.source_epoch THEN NULL ELSE w.lease_nonce END,
  lease_expires_at=CASE WHEN w.source_epoch IS DISTINCT FROM excluded.source_epoch THEN NULL ELSE w.lease_expires_at END
 WHERE NOT p_interactive OR w.last_demand_at<now_at-interval '30 seconds' OR w.source_epoch IS DISTINCT FROM excluded.source_epoch;
 SELECT * INTO found_row FROM sessions.session_analysis_wakeups q WHERE q.org_id=p_org AND q.workspace_id=p_workspace AND q.harness=p_harness AND q.session_id=p_session;
 RETURN body||jsonb_build_object('accepted',true,'nextRefreshAt',to_char(found_row.next_refresh_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
END $enqueue$;
CREATE FUNCTION sessions.enqueue_session_analysis_cache(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue_backend$
 SELECT sessions.session_analysis_cache_demand(p_org,p_workspace,p_harness,p_session,false);
$enqueue_backend$;
CREATE FUNCTION sessions.request_session_analysis_cache(p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $request$
DECLARE base jsonb;
BEGIN
 base:=sessions.session_directory_replay_pages(p_workspace,p_harness,p_session);
 IF NOT(base->>'exists')::boolean THEN RETURN jsonb_build_object('version',1,'accepted',false,'exists',false,'identity',base->'identity','source',base->'source','nextRefreshAt',NULL);END IF;
 RETURN sessions.session_analysis_cache_demand(base->'identity'->>'orgId',p_workspace,p_harness,p_session,true);
END $request$;
CREATE FUNCTION sessions.claim_session_analysis_cache()
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE w sessions.session_analysis_wakeups%ROWTYPE;c jsonb;i integer;
BEGIN
 FOR i IN 1..10 LOOP
  SELECT * INTO w FROM sessions.session_analysis_wakeups q WHERE (q.must_publish OR q.demand_until>clock_timestamp()) AND q.next_refresh_at<=clock_timestamp()
   AND(q.lease_expires_at IS NULL OR q.lease_expires_at<=clock_timestamp())
   ORDER BY (q.demand_until>clock_timestamp()) DESC,q.next_refresh_at,q.org_id,q.workspace_id,q.harness,q.session_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN NULL;END IF;
  c:=sessions.capture_session_replay_pages(w.org_id,w.workspace_id,w.harness,w.session_id);
  IF NOT(c->>'exists')::boolean OR(c->>'blocked')::boolean THEN
   DELETE FROM sessions.session_analysis_wakeups q WHERE q.org_id=w.org_id AND q.workspace_id=w.workspace_id AND q.harness=w.harness AND q.session_id=w.session_id;CONTINUE;
  END IF;
  UPDATE sessions.session_analysis_wakeups q SET source_epoch=(c->'source'->>'epoch')::bigint,source_fingerprint=c->'source'->>'fingerprint',
   lease_nonce=gen_random_uuid(),lease_generation=q.generation,lease_expires_at=clock_timestamp()+interval '90 seconds'
   WHERE q.org_id=w.org_id AND q.workspace_id=w.workspace_id AND q.harness=w.harness AND q.session_id=w.session_id RETURNING * INTO w;
  RETURN jsonb_build_object('version',1,'identity',c->'identity','source',c->'source','leaseNonce',w.lease_nonce,'demandGeneration',w.generation::text,
   'leaseUntil',to_char(w.lease_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'demandUntil',to_char(w.demand_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
 END LOOP;
 RETURN NULL;
END $claim$;
CREATE FUNCTION sessions.heartbeat_session_analysis_cache(p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_nonce uuid)
RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE c jsonb;changed integer;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT(c->>'exists')::boolean OR(c->>'blocked')::boolean OR c->'source'->>'epoch' IS DISTINCT FROM p_epoch::text OR c->'source'->>'fingerprint' IS DISTINCT FROM p_fingerprint THEN RETURN false;END IF;
 UPDATE sessions.session_analysis_wakeups w SET lease_expires_at=clock_timestamp()+interval '90 seconds'
  WHERE w.org_id=p_org AND w.workspace_id=p_workspace AND w.harness=p_harness AND w.session_id=p_session
   AND w.source_epoch=p_epoch AND w.source_fingerprint=p_fingerprint AND w.lease_nonce=p_nonce AND w.lease_expires_at>clock_timestamp();
 GET DIAGNOSTICS changed=ROW_COUNT;RETURN changed=1;
END $heartbeat$;
CREATE FUNCTION sessions.finish_session_analysis_cache(p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_nonce uuid,p_error_code text)
RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $finish$
DECLARE c jsonb;changed integer;
BEGIN
 IF p_error_code IS NOT NULL AND p_error_code!~'^[a-z][a-z0-9_]{0,79}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='analysis cache: invalid bounded error code';END IF;
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT(c->>'exists')::boolean OR(c->>'blocked')::boolean OR c->'source'->>'epoch' IS DISTINCT FROM p_epoch::text OR c->'source'->>'fingerprint' IS DISTINCT FROM p_fingerprint THEN
  DELETE FROM sessions.session_analysis_wakeups w WHERE w.org_id=p_org AND w.workspace_id=p_workspace AND w.harness=p_harness AND w.session_id=p_session AND w.lease_nonce=p_nonce;
  RETURN false;
 END IF;
 -- A successful worker must have actually published a fresh exact source ack.
 IF p_error_code IS NULL AND NOT EXISTS(SELECT 1 FROM sessions.session_analysis_publications a WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session
  AND a.source_epoch=p_epoch AND a.source_fingerprint=p_fingerprint AND a.refresh_nonce=p_nonce AND a.snapshot_at>=clock_timestamp()-interval '120 seconds') THEN RETURN false;END IF;
 UPDATE sessions.session_analysis_wakeups w SET lease_nonce=NULL,lease_expires_at=NULL,must_publish=p_error_code IS NOT NULL OR w.generation IS DISTINCT FROM w.lease_generation,
  next_refresh_at=clock_timestamp()+make_interval(secs=>CASE WHEN w.generation IS DISTINCT FROM w.lease_generation THEN 0 WHEN p_error_code IS NULL THEN 30 ELSE least(300,30*(w.attempts+1)) END),
  attempts=CASE WHEN p_error_code IS NULL THEN 0 ELSE w.attempts+1 END,last_error_code=p_error_code
 WHERE w.org_id=p_org AND w.workspace_id=p_workspace AND w.harness=p_harness AND w.session_id=p_session AND w.source_epoch=p_epoch AND w.source_fingerprint=p_fingerprint
  AND w.lease_nonce=p_nonce AND w.lease_expires_at>clock_timestamp();GET DIAGNOSTICS changed=ROW_COUNT;RETURN changed=1;
END $finish$;
DO $acl$ DECLARE f text;grantee text; BEGIN
 FOREACH f IN ARRAY ARRAY[
 'sessions.session_analysis_cache_demand(text,text,text,text,boolean)',
 'sessions.enqueue_session_analysis_cache(text,text,text,text)',
 'sessions.request_session_analysis_cache(text,text,text)',
 'sessions.claim_session_analysis_cache()',
 'sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid)',
 'sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text)'] LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated,service_role',f);
  FOR grantee IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=to_regprocedure(f) AND a.grantee<>p.proowner LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,grantee);
  END LOOP;
 END LOOP;
END $acl$;
GRANT EXECUTE ON FUNCTION sessions.request_session_analysis_cache(text,text,text) TO session_directory_search_worker;
GRANT EXECUTE ON FUNCTION sessions.enqueue_session_analysis_cache(text,text,text,text),sessions.claim_session_analysis_cache(),sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid),sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text) TO service_role;
