-- Append-only release rename (registry v2); upstream provenance: 20261103080000_session_large_read_jobs.sql. Never applied under the upstream filename in this release.
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
-- Default-off durable handoff for trusted high-memory Node processing.
-- No transcript bytes, credentials, or direct reader table grants.
-- Lease authority uses wall time: now() is frozen at transaction start and
-- must not permit an expired lease to renew or publish in a held transaction.
CREATE TABLE sessions.session_large_read_jobs(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 source_epoch bigint NOT NULL,source_fingerprint text NOT NULL CHECK(source_fingerprint~'^[0-9a-f]{64}$'),
 state text NOT NULL DEFAULT 'queued' CHECK(state IN('queued','running','complete','superseded','failed')),
 available_at timestamptz NOT NULL DEFAULT now(),lease_nonce uuid,lease_expires_at timestamptz,
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),last_error text,
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id),
 CHECK((state='running')=(lease_nonce IS NOT NULL AND lease_expires_at IS NOT NULL))
);
CREATE INDEX session_large_read_jobs_due ON sessions.session_large_read_jobs(available_at,updated_at)
 WHERE state IN('queued','running');
ALTER TABLE sessions.session_large_read_jobs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_large_read_jobs FROM PUBLIC,anon,authenticated,service_role;

CREATE FUNCTION sessions.enqueue_session_large_read(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue$
DECLARE c jsonb; e bigint; f text; canonical_id text;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('enqueued',false,'reason','source_unavailable'); END IF;
 canonical_id:=c->'identity'->>'sessionId';e:=(c->'source'->>'epoch')::bigint;f:=c->'source'->>'fingerprint';
 INSERT INTO sessions.session_large_read_jobs AS j(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint)
 VALUES(p_org,p_workspace,p_harness,canonical_id,e,f)
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET
 source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,state='queued',
 available_at=clock_timestamp(),lease_nonce=NULL,lease_expires_at=NULL,attempts=0,last_error=NULL,updated_at=now()
 WHERE j.source_epoch IS DISTINCT FROM excluded.source_epoch OR j.source_fingerprint IS DISTINCT FROM excluded.source_fingerprint
    OR j.state='superseded';
 RETURN jsonb_build_object('enqueued',true,'changed',FOUND,'identity',c->'identity','source',c->'source');
END $enqueue$;

CREATE FUNCTION sessions.claim_session_large_read(p_nonce uuid,p_lease_seconds integer DEFAULT 120)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE j sessions.session_large_read_jobs%ROWTYPE;c jsonb;
BEGIN
 IF p_nonce IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 30 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='large read: invalid lease'; END IF;
 FOR j IN SELECT * FROM sessions.session_large_read_jobs WHERE
 (state='queued' AND available_at<=clock_timestamp()) OR (state='running' AND lease_expires_at<=clock_timestamp())
 ORDER BY available_at,updated_at LIMIT 100 FOR UPDATE SKIP LOCKED LOOP
  IF j.attempts>=5 THEN
   UPDATE sessions.session_large_read_jobs SET state='failed',lease_nonce=NULL,lease_expires_at=NULL,last_error='attempts_exhausted',updated_at=now() WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END IF;
  c:=sessions.capture_session_replay_pages(j.org_id,j.workspace_id,j.harness,j.session_id);
  IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean OR c->'source'->>'epoch' IS DISTINCT FROM j.source_epoch::text
   OR c->'source'->>'fingerprint' IS DISTINCT FROM j.source_fingerprint THEN
   UPDATE sessions.session_large_read_jobs SET state='superseded',lease_nonce=NULL,lease_expires_at=NULL,updated_at=now()
    WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END IF;
  UPDATE sessions.session_large_read_jobs SET state='running',lease_nonce=p_nonce,lease_expires_at=clock_timestamp()+make_interval(secs=>p_lease_seconds),attempts=attempts+1,last_error=NULL,updated_at=now()
   WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
  RETURN jsonb_build_object('identity',c->'identity','source',c->'source','leaseNonce',p_nonce::text);
 END LOOP;
 RETURN NULL;
END $claim$;

CREATE FUNCTION sessions.heartbeat_session_large_read(p_org text,p_workspace text,p_harness text,p_session text,p_nonce uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE c jsonb;j sessions.session_large_read_jobs%ROWTYPE;
BEGIN
 SELECT * INTO j FROM sessions.session_large_read_jobs WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session FOR UPDATE;
 IF NOT FOUND OR j.state<>'running' OR j.lease_nonce IS DISTINCT FROM p_nonce OR j.lease_expires_at<=clock_timestamp() THEN RETURN false; END IF;
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean OR c->'source'->>'epoch' IS DISTINCT FROM j.source_epoch::text
  OR c->'source'->>'fingerprint' IS DISTINCT FROM j.source_fingerprint THEN RETURN false; END IF;
 UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '120 seconds',updated_at=now()
 WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session
  AND lease_expires_at>clock_timestamp();
 RETURN FOUND;
END $heartbeat$;

-- Publication and lease completion share the row lock and transaction. A stale
-- worker cannot publish even if its old source still happens to exist.
CREATE FUNCTION sessions.publish_session_large_read(
 p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_snapshot jsonb,
 p_generation text,p_source_generation text,p_prefix text,p_index_key text,p_index_sha256 text,p_index_bytes integer,p_nonce uuid
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE j sessions.session_large_read_jobs%ROWTYPE;
BEGIN
 SELECT * INTO j FROM sessions.session_large_read_jobs WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session FOR UPDATE;
 IF NOT FOUND OR j.state<>'running' OR j.lease_nonce IS DISTINCT FROM p_nonce OR j.lease_expires_at<=clock_timestamp()
  OR j.source_epoch IS DISTINCT FROM p_epoch OR j.source_fingerprint IS DISTINCT FROM p_fingerprint THEN RETURN false; END IF;
 -- The nested publisher can wait on the source-publication row lock until
 -- after expiry. Its provisional write must roll back if authority lapses.
 BEGIN
  IF NOT sessions.publish_session_replay_pages(p_org,p_workspace,p_harness,p_session,p_epoch,p_fingerprint,p_snapshot,
   p_generation,p_source_generation,p_prefix,p_index_key,p_index_sha256,p_index_bytes) THEN RETURN false; END IF;
  UPDATE sessions.session_large_read_jobs SET state='complete',lease_nonce=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=now()
  WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session
   AND lease_expires_at>clock_timestamp();
  IF NOT FOUND THEN
   RAISE EXCEPTION USING ERRCODE='RH001',MESSAGE='large read: lease expired during publication';
  END IF;
 EXCEPTION WHEN SQLSTATE 'RH001' THEN RETURN false;
 END;
 RETURN true;
END $publish$;

CREATE FUNCTION sessions.retry_session_large_read(p_org text,p_workspace text,p_harness text,p_session text,p_nonce uuid,p_error text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $retry$
BEGIN
 IF p_error IS NULL OR p_error !~ '^[a-z][a-z0-9_]{0,63}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='large read: invalid error code'; END IF;
 UPDATE sessions.session_large_read_jobs SET state=CASE WHEN attempts>=5 THEN 'failed' ELSE 'queued' END,available_at=clock_timestamp()+interval '30 seconds',lease_nonce=NULL,lease_expires_at=NULL,last_error=p_error,updated_at=now()
 WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session
  AND state='running' AND lease_nonce=p_nonce AND lease_expires_at>clock_timestamp();
 RETURN FOUND;
END $retry$;

CREATE FUNCTION sessions.route_session_large_read(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $route$
DECLARE c jsonb;b numeric;n numeric;unknown_count bigint;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('delegated',false); END IF;
 SELECT coalesce(sum(p.raw_bytes),0),coalesce(sum(p.event_count),0),count(*) FILTER(WHERE p.raw_bytes IS NULL OR p.event_count IS NULL OR p.raw_bytes=0 OR p.event_count=0) INTO b,n,unknown_count FROM sessions.session_index p
 WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
 AND coalesce(p.canonical_session_id,p.session_id)=c->'identity'->>'sessionId' AND p.deleted_at IS NULL;
 -- A conservative selector, not a source validation waiver. The large worker
 -- still validates all actual manifests/chunks/records using its explicit budget.
 IF unknown_count=0 AND b<=67108864 AND n<=100000 THEN RETURN jsonb_build_object('delegated',false); END IF;
 c:=sessions.enqueue_session_large_read(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'enqueued')::boolean THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='large read: durable handoff unavailable'; END IF;
 RETURN jsonb_build_object('delegated',true,'identity',c->'identity','source',c->'source');
END $route$;

REVOKE ALL ON FUNCTION sessions.enqueue_session_large_read(text,text,text,text) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.claim_session_large_read(uuid,integer) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.heartbeat_session_large_read(text,text,text,text,uuid) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.retry_session_large_read(text,text,text,text,uuid,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.enqueue_session_large_read(text,text,text,text),sessions.claim_session_large_read(uuid,integer),
 sessions.heartbeat_session_large_read(text,text,text,text,uuid),sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid),
 sessions.retry_session_large_read(text,text,text,text,uuid,text) TO service_role;

REVOKE ALL ON FUNCTION sessions.route_session_large_read(text,text,text,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.route_session_large_read(text,text,text,text) TO service_role;


-- A durable keyset cursor rotates through every large scope, including after a
-- missed object event. Bounded pages do not silently strand a later cohort.
CREATE TABLE sessions.session_large_read_scan(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),after_identity text[],
 updated_at timestamptz NOT NULL DEFAULT now(),CHECK(after_identity IS NULL OR cardinality(after_identity)=4)
);
INSERT INTO sessions.session_large_read_scan(singleton) VALUES(true);
ALTER TABLE sessions.session_large_read_scan ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_large_read_scan FROM PUBLIC,anon,authenticated,service_role;
CREATE FUNCTION sessions.discover_session_large_reads()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $discover$
DECLARE cursor_id text[];last_id text[];s record;c jsonb;scanned integer:=0;changed integer:=0;
BEGIN
 SELECT after_identity INTO cursor_id FROM sessions.session_large_read_scan WHERE singleton FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('busy',true,'scanned',0,'changed',0,'cycleComplete',false); END IF;
 FOR s IN SELECT p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id) canonical_id
 FROM sessions.session_index p WHERE p.deleted_at IS NULL AND (cursor_id IS NULL OR
 ROW(p.org_id COLLATE "C",p.workspace_id COLLATE "C",p.harness COLLATE "C",coalesce(p.canonical_session_id,p.session_id) COLLATE "C")>
 ROW(cursor_id[1] COLLATE "C",cursor_id[2] COLLATE "C",cursor_id[3] COLLATE "C",cursor_id[4] COLLATE "C"))
 GROUP BY p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id)
 HAVING bool_or(p.raw_bytes IS NULL OR p.event_count IS NULL OR p.raw_bytes=0 OR p.event_count=0) OR sum(p.raw_bytes)>67108864 OR sum(p.event_count)>100000
 ORDER BY p.org_id COLLATE "C",p.workspace_id COLLATE "C",p.harness COLLATE "C",coalesce(p.canonical_session_id,p.session_id) COLLATE "C" LIMIT 128 LOOP
  scanned:=scanned+1;last_id:=ARRAY[s.org_id,s.workspace_id,s.harness,s.canonical_id];
  c:=sessions.enqueue_session_large_read(s.org_id,s.workspace_id,s.harness,s.canonical_id);
  IF coalesce((c->>'changed')::boolean,false) THEN changed:=changed+1;END IF;
 END LOOP;
 UPDATE sessions.session_large_read_scan SET after_identity=CASE WHEN scanned<128 THEN NULL ELSE last_id END,updated_at=now() WHERE singleton;
 RETURN jsonb_build_object('busy',false,'scanned',scanned,'changed',changed,'cycleComplete',scanned<128);
END $discover$;
REVOKE ALL ON FUNCTION sessions.discover_session_large_reads() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.discover_session_large_reads() TO service_role;

DO $acl$ DECLARE f text;g text;BEGIN
 FOREACH f IN ARRAY ARRAY['sessions.enqueue_session_large_read(text,text,text,text)','sessions.claim_session_large_read(uuid,integer)','sessions.heartbeat_session_large_read(text,text,text,text,uuid)','sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)','sessions.retry_session_large_read(text,text,text,text,uuid,text)','sessions.route_session_large_read(text,text,text,text)','sessions.discover_session_large_reads()'] LOOP
 FOR g IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=f::regprocedure AND a.grantee<>p.proowner AND r.rolname<>'service_role'
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,g);END LOOP;
 END LOOP;
END $acl$;
