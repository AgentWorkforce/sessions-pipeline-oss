-- Registry v2 name; upstream provenance: 20261104062000_canonical_history_lineage.sql (#230 draft, never applied under that name).
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
-- Canonical History lineage. This source remains dormant until the full source
-- frontier, logical consumers and permanent retirement transport are proven.
--040600 remains provenance authority for generation object IO, not History moves.
DO $creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid IN('sessions.ingest_require_generation(text)'::regprocedure,
  'sessions.ingest_lazy_stage_seats(uuid)'::regprocedure,'sessions.ingest_publication_claim(jsonb)'::regprocedure,
  'sessions.session_replay_source_snapshot(text,text,text,text)'::regprocedure,
  'sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)'::regprocedure,
  'sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)'::regprocedure,
  'sessions.session_publication_is_managed(text,text,text,text,text)'::regprocedure,
  'sessions.claim_session_large_read(uuid,integer)'::regprocedure,
  'sessions.heartbeat_session_large_read(text,text,text,text,uuid)'::regprocedure,
  'sessions.retry_session_large_read(text,text,text,text,uuid,text)'::regprocedure,
  'sessions.enqueue_session_large_read(text,text,text,text)'::regprocedure,
  'sessions.capture_session_replay_pages(text,text,text,text)'::regprocedure,
  'sessions.session_brief(text,text,text)'::regprocedure,
  'sessions.session_directory_pointers(text,text,text)'::regprocedure,
  'sessions.session_directory_replay_pages(text,text,text)'::regprocedure,
  'sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid)'::regprocedure) AND proowner<>me)
 OR EXISTS(SELECT 1 FROM pg_class WHERE oid IN('sessions.session_large_read_jobs'::regclass,'sessions.session_index'::regclass,
  'sessions.session_directory'::regclass,'sessions.session_replay_publications'::regclass)AND relowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: original coherent creator required';END IF;
END $creator$;

-- OPERATIONAL KILL SWITCH (owner only; see runbooks/history-620-operations.md).
-- live=false disables EVERY live 620 hook (ACK/control/directory/session_index triggers) and every 620
-- read gate, so the database behaves exactly as main, while all ledgers (versions, derived objects,
-- feature journal, outboxes, builds) are retained. pointer_tracking separately arms the session_index
-- pointer-reference trigger; refs_complete is set only by a full ingest_history_pointer_ref_backfill
-- pass made while tracking is armed, and the inventory refuses until then.
CREATE TABLE sessions.ingest_history_switch(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 live boolean NOT NULL DEFAULT true,pointer_tracking boolean NOT NULL DEFAULT false,refs_complete boolean NOT NULL DEFAULT false,
 changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(pointer_tracking OR NOT refs_complete)
);
INSERT INTO sessions.ingest_history_switch DEFAULT VALUES;
ALTER TABLE sessions.ingest_history_switch ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_switch FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_history_live()RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $live$
 SELECT coalesce((SELECT w.live FROM sessions.ingest_history_switch w WHERE w.singleton),false)
$live$;
CREATE FUNCTION sessions.ingest_history_tracking()RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $tracking$
 SELECT coalesce((SELECT w.live AND w.pointer_tracking FROM sessions.ingest_history_switch w WHERE w.singleton),false)
$tracking$;
REVOKE ALL ON FUNCTION sessions.ingest_history_live(),sessions.ingest_history_tracking()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE SEQUENCE sessions.ingest_history_inventory_epoch AS bigint NO CYCLE;
CREATE TABLE sessions.ingest_history_carriers(
 carrier_sha256 text PRIMARY KEY CHECK(carrier_sha256~'^[a-f0-9]{64}$'),
 storage_provider text NOT NULL,bucket text NOT NULL,physical_root text NOT NULL,layout_known boolean NOT NULL,
 epoch bigint NOT NULL DEFAULT nextval('sessions.ingest_history_inventory_epoch'),scanned_epoch bigint,
 scan_token uuid,scan_lease_token uuid,scan_lease_until timestamptz,scan_generation text,
 next_page bigint NOT NULL DEFAULT 0,scanned_records bigint NOT NULL DEFAULT 0 CHECK(scanned_records BETWEEN 0 AND 2000000),
 scan_complete boolean NOT NULL DEFAULT false,scan_descriptor_sha256 text,
 UNIQUE(storage_provider,bucket,physical_root),
 CHECK((scan_token IS NULL)=(scan_lease_token IS NULL)),
 CHECK((scan_token IS NULL)=(scan_lease_until IS NULL))
);
CREATE INDEX ingest_history_unscanned ON sessions.ingest_history_carriers(carrier_sha256)
 WHERE scanned_epoch IS DISTINCT FROM epoch OR NOT scan_complete;
CREATE TABLE sessions.ingest_history_pointer_refs(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,machine_id text NOT NULL,
 carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers ON DELETE RESTRICT,
 pointer jsonb NOT NULL CHECK(jsonb_typeof(pointer)='object'),pointer_sha256 text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id,machine_id)
);
CREATE INDEX ingest_history_pointer_root ON sessions.ingest_history_pointer_refs(carrier_sha256);
CREATE TABLE sessions.ingest_history_scan_refs(
 scan_token uuid NOT NULL,ref_ordinal bigint NOT NULL,carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers,
 pointer jsonb NOT NULL,pointer_sha256 text NOT NULL,PRIMARY KEY(scan_token,ref_ordinal)
);
CREATE TABLE sessions.ingest_history_scan_pages(
 scan_token uuid NOT NULL,page bigint NOT NULL,page_sha256 text NOT NULL CHECK(page_sha256~'^[a-f0-9]{64}$'),
 records bigint NOT NULL CHECK(records>=0),PRIMARY KEY(scan_token,page)
);
CREATE TABLE sessions.ingest_history_native_locations(
 carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers,
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
 max_revision bigint NOT NULL CHECK(max_revision>0),records bigint NOT NULL CHECK(records>0),
 PRIMARY KEY(carrier_sha256,org_id,workspace_id,origin_id,record_id)
);
CREATE INDEX ingest_history_native_lookup ON sessions.ingest_history_native_locations(org_id,workspace_id,origin_id,record_id,carrier_sha256);
ALTER TABLE sessions.ingest_history_carriers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_pointer_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scan_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scan_pages ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_native_locations ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_carriers,sessions.ingest_history_pointer_refs,sessions.ingest_history_scan_refs,
 sessions.ingest_history_scan_pages,sessions.ingest_history_native_locations FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
REVOKE ALL ON SEQUENCE sessions.ingest_history_inventory_epoch FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Physical root, NOT logical source/session, identifies shared raw chunk keys.
CREATE FUNCTION sessions.ingest_history_root(p_key text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $root$
 SELECT (regexp_match(p_key,'^(.*/v1/ws=[A-Za-z0-9._-]+/m=[A-Za-z0-9._-]+/h=[A-Za-z0-9._-]+/d=[0-9]{4}/[0-9]{2}/[0-9]{2}/s=[A-Za-z0-9._-]+/)(?:manifest[.]json|publications/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}/manifest[.]json)$'))[1]
$root$;
CREATE FUNCTION sessions.ingest_history_pointer(p sessions.session_index) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $pointer$
 -- indexed_at/fanout operational clocks do not change admitted raw authority.
 SELECT to_jsonb(p)-ARRAY['indexed_at','state','state_classified_at']
$pointer$;
CREATE FUNCTION sessions.ingest_history_sync_pointer() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $sync$
DECLARE r sessions.session_index%ROWTYPE;v_root text;v_key text;v_pointer jsonb;v_sha text;v_known boolean;cur sessions.ingest_history_pointer_refs%ROWTYPE;
BEGIN
 -- Dormant until an operator arms pointer tracking (and never while the kill switch is off): the
 -- legacy indexer pays nothing. Disarming clears refs_complete, so a fresh backfill pass is required.
 IF NOT sessions.ingest_history_tracking()THEN IF TG_OP='DELETE'THEN RETURN OLD;END IF;RETURN NEW;END IF;
 -- A ref may be STALE here: left from a disarmed/OFF period (the row was deleted, reinserted or changed
 -- untracked). Every path REPAIRS it, never errors: whatever ref exists at the OLD key and at the NEW key
 -- is replaced, and every carrier it named is rescanned. The writer holds the session_index row lock and
 -- the ref row lock it deletes, the same locks the backfill/prune take, so they serialize.
 IF TG_OP<>'DELETE' AND NEW.manifest_key IS NOT NULL AND NEW.storage_provider IS NOT NULL AND NEW.bucket IS NOT NULL THEN
  r:=NEW;
  v_root:=sessions.ingest_history_root(r.manifest_key);
  v_known:=v_root IS NOT NULL;v_root:=coalesce(v_root,r.manifest_key);
  v_key:=encode(sha256(convert_to(jsonb_build_array(r.storage_provider,r.bucket,v_root)::text,'UTF8')),'hex');
  v_pointer:=sessions.ingest_history_pointer(r);v_sha:=encode(sha256(convert_to(v_pointer::text,'UTF8')),'hex');
 END IF;
 -- Unchanged pointer AND an exact current ref: nothing to do (the common indexer clock-only update).
 IF TG_OP='UPDATE' AND sessions.ingest_history_pointer(OLD)=sessions.ingest_history_pointer(NEW)AND v_key IS NOT NULL THEN
  SELECT *INTO cur FROM sessions.ingest_history_pointer_refs WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND harness=NEW.harness
   AND session_id=NEW.session_id AND machine_id=NEW.machine_id;
  IF FOUND AND cur.carrier_sha256=v_key AND cur.pointer_sha256=v_sha THEN RETURN NEW;END IF;
 END IF;
 IF TG_OP<>'INSERT' THEN
  WITH gone AS(DELETE FROM sessions.ingest_history_pointer_refs WHERE org_id=OLD.org_id AND workspace_id=OLD.workspace_id
    AND harness=OLD.harness AND session_id=OLD.session_id AND machine_id=OLD.machine_id RETURNING carrier_sha256)
  UPDATE sessions.ingest_history_carriers SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false
   WHERE carrier_sha256 IN(SELECT carrier_sha256 FROM gone);
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;
 -- A stale ref already at the NEW key (an untracked delete then this reinsert, or a key change onto it).
 WITH gone AS(DELETE FROM sessions.ingest_history_pointer_refs WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id
   AND harness=NEW.harness AND session_id=NEW.session_id AND machine_id=NEW.machine_id RETURNING carrier_sha256)
 UPDATE sessions.ingest_history_carriers SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false
  WHERE carrier_sha256 IN(SELECT carrier_sha256 FROM gone);
 -- Keep tombstoned/nonpublic refs too; they can still carry physical authority.
 -- Unknown layouts are recorded as unscannable, not omitted or imposed as a new
 -- failure on the dormant legacy indexer. They block full inventory admission.
 IF v_key IS NULL THEN RETURN NEW;END IF;
 INSERT INTO sessions.ingest_history_carriers(carrier_sha256,storage_provider,bucket,physical_root,layout_known)
 VALUES(v_key,r.storage_provider,r.bucket,v_root,v_known)
 ON CONFLICT(carrier_sha256)DO UPDATE SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false
 WHERE sessions.ingest_history_carriers.storage_provider=excluded.storage_provider AND sessions.ingest_history_carriers.bucket=excluded.bucket
  AND sessions.ingest_history_carriers.physical_root=excluded.physical_root;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: carrier hash collision';END IF;
 -- Upsert: a ref a concurrent backfill committed after the delete above is re-fenced, never a 23505.
 INSERT INTO sessions.ingest_history_pointer_refs VALUES(r.org_id,r.workspace_id,r.harness,r.session_id,r.machine_id,v_key,v_pointer,v_sha)
  ON CONFLICT(org_id,workspace_id,harness,session_id,machine_id)DO UPDATE SET carrier_sha256=excluded.carrier_sha256,pointer=excluded.pointer,pointer_sha256=excluded.pointer_sha256;
 RETURN NEW;
END $sync$;
CREATE TRIGGER ingest_history_pointer_sync AFTER INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_history_sync_pointer();
-- Seed the reference index without changing stored pointers or applied epochs. NOT run by the
-- migration (a row-by-row loop under the sync trigger would hold session_index writers): an operator
-- pages it, resumably, in bounded batches after deploy. Returns the next cursor, NULL when done.
CREATE FUNCTION sessions.ingest_history_pointer_ref_backfill(p_after jsonb,p_limit integer)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $initial_refs$
DECLARE r sessions.session_index%ROWTYPE;v_root text;v_key text;v_pointer jsonb;v_sha text;v_known boolean;v_last jsonb;old_ref sessions.ingest_history_pointer_refs%ROWTYPE;v_done boolean;f record;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 5000 OR(p_after IS NOT NULL AND jsonb_typeof(p_after)<>'array')THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: pointer ref backfill bound';END IF;
 -- Only meaningful while the trigger maintains every later write; FOR SHARE keeps the switch armed
 -- for this page (a concurrent disarm waits for it).
 PERFORM 1 FROM sessions.ingest_history_switch WHERE singleton AND live AND pointer_tracking FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: arm pointer tracking before the backfill';END IF;
 -- Each row is LOCKED (FOR UPDATE) before its ref is written: a concurrent indexer UPDATE waits for this
 -- page and its trigger then sees the committed ref (no 23505); a row the indexer already holds is waited
 -- for, and its trigger has written the current ref by then.
 FOR r IN SELECT * FROM sessions.session_index i WHERE i.manifest_key IS NOT NULL AND i.storage_provider IS NOT NULL AND i.bucket IS NOT NULL
   AND(p_after IS NULL OR ROW(i.org_id,i.workspace_id,i.harness,i.session_id,i.machine_id)>ROW(p_after->>0,p_after->>1,p_after->>2,p_after->>3,p_after->>4))
   ORDER BY i.org_id,i.workspace_id,i.harness,i.session_id,i.machine_id LIMIT p_limit FOR UPDATE LOOP
  v_last:=jsonb_build_array(r.org_id,r.workspace_id,r.harness,r.session_id,r.machine_id);
  v_root:=sessions.ingest_history_root(r.manifest_key);
  v_known:=v_root IS NOT NULL;v_root:=coalesce(v_root,r.manifest_key);
  v_key:=encode(sha256(convert_to(jsonb_build_array(r.storage_provider,r.bucket,v_root)::text,'UTF8')),'hex');
  v_pointer:=sessions.ingest_history_pointer(r);v_sha:=encode(sha256(convert_to(v_pointer::text,'UTF8')),'hex');
  SELECT *INTO old_ref FROM sessions.ingest_history_pointer_refs WHERE org_id=r.org_id AND workspace_id=r.workspace_id AND harness=r.harness AND session_id=r.session_id AND machine_id=r.machine_id;
  CONTINUE WHEN FOUND AND old_ref.carrier_sha256=v_key AND old_ref.pointer_sha256=v_sha;
  -- A ref left stale by a disarmed period is rewritten exactly as the trigger would (both carriers rescan).
  IF FOUND THEN
   DELETE FROM sessions.ingest_history_pointer_refs WHERE org_id=r.org_id AND workspace_id=r.workspace_id AND harness=r.harness AND session_id=r.session_id AND machine_id=r.machine_id;
   UPDATE sessions.ingest_history_carriers SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false WHERE carrier_sha256=old_ref.carrier_sha256;
  END IF;
  INSERT INTO sessions.ingest_history_carriers(carrier_sha256,storage_provider,bucket,physical_root,layout_known)VALUES(v_key,r.storage_provider,r.bucket,v_root,v_known)
   ON CONFLICT(carrier_sha256)DO UPDATE SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false
   WHERE sessions.ingest_history_carriers.storage_provider=excluded.storage_provider AND sessions.ingest_history_carriers.bucket=excluded.bucket
    AND sessions.ingest_history_carriers.physical_root=excluded.physical_root;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: carrier hash collision';END IF;
  INSERT INTO sessions.ingest_history_pointer_refs VALUES(r.org_id,r.workspace_id,r.harness,r.session_id,r.machine_id,v_key,v_pointer,v_sha);
 END LOOP;
 v_done:=v_last IS NULL OR NOT EXISTS(SELECT 1 FROM sessions.session_index i WHERE i.manifest_key IS NOT NULL AND i.storage_provider IS NOT NULL AND i.bucket IS NOT NULL
   AND ROW(i.org_id,i.workspace_id,i.harness,i.session_id,i.machine_id)>ROW(v_last->>0,v_last->>1,v_last->>2,v_last->>3,v_last->>4));
 -- Orphans in this page's key range: a ref whose session_index row was deleted (or lost its pointer)
 -- while tracking was off has no row to visit above. Each candidate is LOCKED (waits out a writer whose
 -- trigger is repairing it) and RE-VERIFIED in a fresh statement before it is deleted, so a ref a
 -- concurrent reinsert just wrote is never pruned from an old snapshot. Its carrier is rescanned.
 FOR f IN SELECT g.org_id,g.workspace_id,g.harness,g.session_id,g.machine_id FROM sessions.ingest_history_pointer_refs g
   WHERE(p_after IS NULL OR ROW(g.org_id,g.workspace_id,g.harness,g.session_id,g.machine_id)>ROW(p_after->>0,p_after->>1,p_after->>2,p_after->>3,p_after->>4))
    AND(v_done OR ROW(g.org_id,g.workspace_id,g.harness,g.session_id,g.machine_id)<=ROW(v_last->>0,v_last->>1,v_last->>2,v_last->>3,v_last->>4))
    AND NOT EXISTS(SELECT 1 FROM sessions.session_index i WHERE i.org_id=g.org_id AND i.workspace_id=g.workspace_id AND i.harness=g.harness
     AND i.session_id=g.session_id AND i.machine_id=g.machine_id AND i.manifest_key IS NOT NULL AND i.storage_provider IS NOT NULL AND i.bucket IS NOT NULL)
   ORDER BY g.org_id,g.workspace_id,g.harness,g.session_id,g.machine_id LOOP
  SELECT *INTO old_ref FROM sessions.ingest_history_pointer_refs WHERE org_id=f.org_id AND workspace_id=f.workspace_id AND harness=f.harness
   AND session_id=f.session_id AND machine_id=f.machine_id FOR UPDATE;
  CONTINUE WHEN NOT FOUND;
  CONTINUE WHEN EXISTS(SELECT 1 FROM sessions.session_index i WHERE i.org_id=f.org_id AND i.workspace_id=f.workspace_id AND i.harness=f.harness
   AND i.session_id=f.session_id AND i.machine_id=f.machine_id AND i.manifest_key IS NOT NULL AND i.storage_provider IS NOT NULL AND i.bucket IS NOT NULL);
  DELETE FROM sessions.ingest_history_pointer_refs WHERE org_id=f.org_id AND workspace_id=f.workspace_id AND harness=f.harness
   AND session_id=f.session_id AND machine_id=f.machine_id;
  UPDATE sessions.ingest_history_carriers SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false WHERE carrier_sha256=old_ref.carrier_sha256;
 END LOOP;
 -- The last page is the end of the key space: every row older than the arming is now covered.
 IF v_done THEN
  UPDATE sessions.ingest_history_switch SET refs_complete=true,changed_at=clock_timestamp()WHERE singleton AND NOT refs_complete;
  RETURN NULL;
 END IF;
 RETURN v_last;
END $initial_refs$;
REVOKE ALL ON FUNCTION sessions.ingest_history_pointer_ref_backfill(jsonb,integer)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_inventory_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_token uuid:=gen_random_uuid();v_lease uuid:=gen_random_uuid();v_count bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_switch WHERE singleton AND live AND pointer_tracking AND refs_complete)THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: pointer references not tracked and backfilled';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','bucket']);PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'bucket')IS DISTINCT FROM'string' OR octet_length(p->>'bucket')NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid inventory target';END IF;
 SELECT q.* INTO c FROM sessions.ingest_history_carriers q
  WHERE q.storage_provider='s3' AND q.layout_known AND q.bucket=p->>'bucket' AND(q.scanned_epoch IS DISTINCT FROM q.epoch OR NOT q.scan_complete)
   AND(q.scan_lease_until IS NULL OR q.scan_lease_until<=clock_timestamp())
   AND EXISTS(SELECT 1 FROM sessions.ingest_history_pointer_refs r WHERE r.carrier_sha256=q.carrier_sha256)
  ORDER BY q.carrier_sha256 FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN jsonb_build_object('ticket',NULL);END IF;
 -- Never admit a scan over a dangling reference (its session_index row is gone or lost its pointer):
 -- the backfill prunes those; until it has, this carrier is refused (bounded: this carrier's refs only).
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_pointer_refs f WHERE f.carrier_sha256=c.carrier_sha256 AND NOT EXISTS(
   SELECT 1 FROM sessions.session_index i WHERE i.org_id=f.org_id AND i.workspace_id=f.workspace_id AND i.harness=f.harness AND i.session_id=f.session_id
    AND i.machine_id=f.machine_id AND i.manifest_key IS NOT NULL AND i.storage_provider IS NOT NULL AND i.bucket IS NOT NULL))
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: dangling pointer references; rerun the pointer backfill';END IF;
 INSERT INTO sessions.ingest_history_scan_refs(scan_token,ref_ordinal,carrier_sha256,pointer,pointer_sha256)
 SELECT v_token,row_number()OVER(ORDER BY r.org_id COLLATE"C",r.workspace_id COLLATE"C",r.harness COLLATE"C",r.session_id COLLATE"C",r.machine_id COLLATE"C")-1,
  r.carrier_sha256,r.pointer,r.pointer_sha256 FROM sessions.ingest_history_pointer_refs r WHERE r.carrier_sha256=c.carrier_sha256;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count=0 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: carrier lost references';END IF;
 DELETE FROM sessions.ingest_history_native_locations WHERE carrier_sha256=c.carrier_sha256;
 UPDATE sessions.ingest_history_carriers SET scan_token=v_token,scan_lease_token=v_lease,scan_lease_until=clock_timestamp()+interval'5 minutes',
  scan_generation=p->>'generation',next_page=0,scanned_records=0,scanned_epoch=c.epoch,scan_complete=false,scan_descriptor_sha256=NULL
 WHERE carrier_sha256=c.carrier_sha256;
 RETURN jsonb_build_object('ticket',jsonb_build_object('carrierSha256',c.carrier_sha256,'physicalRoot',c.physical_root,'bucket',c.bucket,
  'scanToken',v_token,'leaseToken',v_lease,'epoch',c.epoch::text,'referenceCount',v_count::text,'leaseUntil',clock_timestamp()+interval'5 minutes'));
END $claim$;
CREATE FUNCTION sessions.ingest_history_inventory_locked(p jsonb) RETURNS sessions.ingest_history_carriers
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $locked$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 SELECT * INTO c FROM sessions.ingest_history_carriers WHERE carrier_sha256=p->>'carrierSha256' FOR UPDATE;
 IF NOT FOUND OR c.scan_token IS DISTINCT FROM(p->>'scanToken')::uuid OR c.scan_lease_token IS DISTINCT FROM(p->>'leaseToken')::uuid
  OR c.scan_generation IS DISTINCT FROM p->>'generation' OR c.epoch IS DISTINCT FROM(p->>'epoch')::bigint
  OR c.scanned_epoch IS DISTINCT FROM c.epoch OR c.scan_lease_until<=clock_timestamp() OR c.scan_complete THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory lease/source changed';END IF;
 RETURN c;
END $locked$;
CREATE FUNCTION sessions.ingest_history_inventory_refs(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $refs$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_after bigint;v_rows jsonb;v_total bigint;v_next bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch','after']);
 c:=sessions.ingest_history_inventory_locked(p);v_after:=(p->>'after')::bigint;
 IF v_after<0 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid reference cursor';END IF;
 SELECT count(*)INTO v_total FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token;
 SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',r.ref_ordinal::text,'pointer',r.pointer,'pointerSha256',r.pointer_sha256)ORDER BY r.ref_ordinal),'[]'),max(r.ref_ordinal)+1
 INTO v_rows,v_next FROM(SELECT *FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token AND ref_ordinal>=v_after ORDER BY ref_ordinal LIMIT 100)r;
 IF octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history: reference packet limit';END IF;
 IF c.scan_lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory lease expired';END IF;
 RETURN jsonb_build_object('references',v_rows,'total',v_total::text,'next',CASE WHEN v_next<v_total THEN v_next::text ELSE NULL END);
END $refs$;
CREATE FUNCTION sessions.ingest_history_inventory_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;r jsonb;v_page bigint;v_sha text;prior sessions.ingest_history_scan_pages%ROWTYPE;v_records bigint:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch','page','records']);
 c:=sessions.ingest_history_inventory_locked(p);v_page:=(p->>'page')::bigint;
 IF v_page<0 OR jsonb_typeof(p->'records')IS DISTINCT FROM'array' OR jsonb_array_length(p->'records')NOT BETWEEN 1 AND 500
  OR octet_length(p::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: inventory page bound';END IF;
 v_sha:=encode(sha256(convert_to((p->'records')::text,'UTF8')),'hex');
 SELECT *INTO prior FROM sessions.ingest_history_scan_pages WHERE scan_token=c.scan_token AND page=v_page;
 IF FOUND THEN
  IF prior.page_sha256<>v_sha THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: replayed page changed';END IF;
  RETURN jsonb_build_object('stored',true,'replay',true);END IF;
 IF v_page<>c.next_page THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: missing inventory page';END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(p->'records') LOOP
  PERFORM sessions.ingest_exact_keys(r,ARRAY['orgId','workspaceId','originId','recordId','revision','count']);
  IF EXISTS(SELECT 1 FROM unnest(ARRAY['orgId','workspaceId','originId','recordId'])k
   WHERE jsonb_typeof(r->k)IS DISTINCT FROM'string' OR char_length(r->>k)NOT BETWEEN 1 AND 1024 OR octet_length(r->>k)>4096)
   OR NOT EXISTS(SELECT 1 FROM sessions.ingest_history_scan_refs f WHERE f.scan_token=c.scan_token AND f.pointer->>'org_id'=r->>'orgId'
    AND f.pointer->>'workspace_id'=r->>'workspaceId') OR jsonb_typeof(r->'revision')IS DISTINCT FROM'string'
   OR r->>'revision'!~'^[1-9][0-9]{0,18}$' OR(r->>'revision')::numeric>9223372036854775807
   OR jsonb_typeof(r->'count')IS DISTINCT FROM'number' OR(r->>'count')::numeric<>trunc((r->>'count')::numeric)
   OR(r->>'count')::numeric NOT BETWEEN 1 AND 2000000 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid carrier native record';END IF;
  INSERT INTO sessions.ingest_history_native_locations AS n VALUES(c.carrier_sha256,r->>'orgId',r->>'workspaceId',r->>'originId',r->>'recordId',(r->>'revision')::bigint,(r->>'count')::bigint)
  ON CONFLICT(carrier_sha256,org_id,workspace_id,origin_id,record_id)DO UPDATE SET
   max_revision=greatest(n.max_revision,excluded.max_revision),records=n.records+excluded.records;
  v_records:=v_records+(r->>'count')::bigint;
 END LOOP;
 INSERT INTO sessions.ingest_history_scan_pages VALUES(c.scan_token,v_page,v_sha,v_records);
 UPDATE sessions.ingest_history_carriers SET next_page=next_page+1,scanned_records=scanned_records+v_records
 WHERE carrier_sha256=c.carrier_sha256;
 IF c.scan_lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory lease expired after page';END IF;
 RETURN jsonb_build_object('stored',true,'replay',false);
END $page$;
CREATE FUNCTION sessions.ingest_history_inventory_finish(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $finish$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_refs bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch','pages','records','descriptorSha256']);
 c:=sessions.ingest_history_inventory_locked(p);
 IF jsonb_typeof(p->'pages')IS DISTINCT FROM'string' OR jsonb_typeof(p->'records')IS DISTINCT FROM'string'
  OR p->>'pages'!~'^(0|[1-9][0-9]*)$' OR p->>'records'!~'^(0|[1-9][0-9]*)$'
  OR (p->>'pages')::bigint<>c.next_page OR(p->>'records')::bigint<>c.scanned_records
  OR jsonb_typeof(p->'descriptorSha256')IS DISTINCT FROM'string' OR p->>'descriptorSha256'!~'^[a-f0-9]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory incomplete';END IF;
 -- Exact GLOBAL reference inventory, including users other than the claimant.
 IF EXISTS((SELECT pointer_sha256,pointer FROM sessions.ingest_history_pointer_refs WHERE carrier_sha256=c.carrier_sha256
   EXCEPT ALL SELECT pointer_sha256,pointer FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token)
  UNION ALL(SELECT pointer_sha256,pointer FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token
   EXCEPT ALL SELECT pointer_sha256,pointer FROM sessions.ingest_history_pointer_refs WHERE carrier_sha256=c.carrier_sha256)) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: global carrier references changed';END IF;
 UPDATE sessions.ingest_history_carriers SET scan_complete=true,scan_descriptor_sha256=p->>'descriptorSha256',scan_lease_token=NULL,scan_token=NULL,scan_lease_until=NULL
 WHERE carrier_sha256=c.carrier_sha256;
 IF c.scan_lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: final inventory expired';END IF;
 RETURN jsonb_build_object('complete',true,'records',c.scanned_records::text);
END $finish$;
CREATE FUNCTION sessions.ingest_history_inventory_heartbeat(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_until timestamptz;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch']);
 c:=sessions.ingest_history_inventory_locked(p);v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_history_carriers SET scan_lease_until=v_until WHERE carrier_sha256=c.carrier_sha256 AND scan_lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: expired inventory cannot renew';END IF;
 RETURN jsonb_build_object('leaseUntil',v_until);
END $heartbeat$;
REVOKE ALL ON FUNCTION sessions.ingest_history_root(text),sessions.ingest_history_pointer(sessions.session_index),sessions.ingest_history_sync_pointer(),
 sessions.ingest_history_inventory_claim(jsonb),sessions.ingest_history_inventory_locked(jsonb),sessions.ingest_history_inventory_refs(jsonb),
 sessions.ingest_history_inventory_page(jsonb),sessions.ingest_history_inventory_finish(jsonb),sessions.ingest_history_inventory_heartbeat(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_inventory_claim(jsonb),sessions.ingest_history_inventory_refs(jsonb),
 sessions.ingest_history_inventory_page(jsonb),sessions.ingest_history_inventory_finish(jsonb),sessions.ingest_history_inventory_heartbeat(jsonb)TO bulls96_session_ingest;


-- Plans are immutable capture records. S3 IO never runs in these transactions.
CREATE TABLE sessions.ingest_history_plans(
 plan_token uuid PRIMARY KEY,lease_token uuid NOT NULL,schema_generation text NOT NULL,lease_until timestamptz NOT NULL,
 stage_token uuid NOT NULL REFERENCES sessions.ingest_stage_intents,stage_version bigint NOT NULL,outbox_version bigint NOT NULL,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,
 target_bucket text NOT NULL,target_region text NOT NULL,key_root text NOT NULL,
 accepted_ordinals jsonb NOT NULL,receipt_seats jsonb NOT NULL,native_snapshot jsonb NOT NULL,
 frontier jsonb NOT NULL,state text NOT NULL CHECK(state IN('admitted','prepared','published','rejected')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),published_at timestamptz,completion_sha256 text
);
CREATE TABLE sessions.ingest_history_plan_carriers(
 plan_token uuid NOT NULL REFERENCES sessions.ingest_history_plans,carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers,
 source_epoch bigint NOT NULL,reference_count bigint NOT NULL CHECK(reference_count>0),
 destination_generation uuid NOT NULL,destination_prefix text NOT NULL,
 PRIMARY KEY(plan_token,carrier_sha256),UNIQUE(destination_generation)
);
CREATE TABLE sessions.ingest_history_plan_refs(
 plan_token uuid NOT NULL,carrier_sha256 text NOT NULL,ordinal bigint NOT NULL,
 pointer jsonb NOT NULL,pointer_sha256 text NOT NULL,
 PRIMARY KEY(plan_token,carrier_sha256,ordinal),FOREIGN KEY(plan_token,carrier_sha256)REFERENCES sessions.ingest_history_plan_carriers
);
ALTER TABLE sessions.ingest_history_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_plan_carriers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_plan_refs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_plans,sessions.ingest_history_plan_carriers,sessions.ingest_history_plan_refs
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_native_snapshot(p_stage uuid)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $native$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;v jsonb;v_expected bigint;v_wanted bigint;
BEGIN
 SELECT *INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT *INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 WITH keep AS MATERIALIZED(SELECT DISTINCT(o::text)::integer ord FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o),wanted AS MATERIALIZED(
  SELECT DISTINCT x->>'record_id' record_id,encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,x->>'record_id')::text,'UTF8')),'hex') key
  FROM jsonb_array_elements(a.records)x JOIN keep k ON k.ord=(x->>'ord')::integer
 )SELECT coalesce(jsonb_agg(jsonb_build_object('recordId',w.record_id,'slot',to_jsonb(n))ORDER BY w.record_id COLLATE"C"),'[]'),count(*),(SELECT count(*)FROM wanted)
 INTO v,v_expected,v_wanted FROM wanted w JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=w.key AND n.org_id=a.org_id
  AND n.workspace_id=a.workspace_id AND n.origin_id=a.origin_id AND n.record_id=w.record_id AND n.current_owner=a.user_id;
 IF v_expected=0 OR v_expected<>v_wanted THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: no current qualified native custody';END IF;
 RETURN v;
END $native$;
CREATE FUNCTION sessions.ingest_history_plan_claim(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;o sessions.ingest_buffer_outbox%ROWTYPE;
 f sessions.ingest_publication_frontiers%ROWTYPE;c sessions.ingest_history_carriers%ROWTYPE;
 v_plan uuid:=gen_random_uuid();v_lease uuid:=gen_random_uuid();v_until timestamptz;v_org text;v_ws text;v_user text;
 v_keep jsonb;v_all jsonb;v_native jsonb;v_seats jsonb;v_carriers bigint:=0;v_refs bigint;v_gen uuid;v_root text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','stageToken','bucket','region','keyRoot']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'keyRoot')IS DISTINCT FROM'string' OR p->>'keyRoot'!~'^[a-z0-9][a-z0-9-]{0,62}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid key root';END IF;
 SELECT org_id,workspace_id,user_id INTO v_org,v_ws,v_user FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: stage absent';END IF;
 PERFORM sessions.ingest_require_buffer_admission(v_org);PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT *INTO s FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id AND b.user_id=s.user_id
  AND b.origin_id=s.origin_id AND b.batch_id=s.batch_id AND b.digest=s.digest AND b.state='committed' AND b.buffer_receipt_kind='staged' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: durable staged receipt absent';END IF;
 SELECT *INTO s FROM sessions.ingest_stage_intents WHERE stage_token=s.stage_token FOR UPDATE;
 SELECT *INTO o FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
 IF NOT FOUND OR o.active_stage_token IS DISTINCT FROM s.stage_token OR o.state<>'buffered' OR o.user_id<>s.user_id OR o.digest<>s.digest
  OR s.receipt_state<>'acknowledged' OR s.cleanup_state<>'live' OR s.gc_lease_until>clock_timestamp() OR s.gc_replacement_stage_token IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: stage busy or changed';END IF;
 SELECT *INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 -- Current target custody/lifecycle dominates unqualified historical S3 MAX.
 -- All chosen records have immutable true ACK decisions and current exact seats.
 v_keep:=sessions.ingest_buffer_gc_ordinals(s.stage_token);
 SELECT coalesce(jsonb_agg(x ORDER BY x::text::integer),'[]')INTO v_all FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)x;
 IF v_keep='[]'OR v_keep IS DISTINCT FROM v_all THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: stage requires original-ordinal retention rewrite';END IF;
 v_seats:=sessions.ingest_receipt_seats_for_stage(s.stage_token);v_native:=sessions.ingest_history_native_snapshot(s.stage_token);
 SELECT *INTO f FROM sessions.ingest_publication_frontiers WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND user_id=s.user_id AND origin_id=s.origin_id
  AND schema_generation=p->>'generation' AND target_bucket=p->>'bucket' AND target_region=p->>'region' AND key_root=p->>'keyRoot' AND valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: qualified complete source frontier required';END IF;
 -- Complete coverage is a database predicate, never a limited candidate page.
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_pointer_refs r JOIN sessions.ingest_history_carriers q USING(carrier_sha256)
   WHERE r.org_id=s.org_id AND r.workspace_id=s.workspace_id AND(q.storage_provider<>'s3' OR q.bucket<>f.target_bucket OR NOT q.layout_known OR NOT q.scan_complete OR q.scanned_epoch IS DISTINCT FROM q.epoch)) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: complete verified carrier inventory required';END IF;
 v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_stage_intents SET row_version=row_version+1,gc_lease_version=row_version+1,gc_lease_token=v_lease,
  gc_generation=p->>'generation',gc_lease_until=v_until,gc_keep_groups=NULL,gc_keep_ordinals=NULL,gc_outbox_version=o.row_version WHERE stage_token=s.stage_token RETURNING*INTO s;
 INSERT INTO sessions.ingest_history_plans(plan_token,lease_token,schema_generation,lease_until,stage_token,stage_version,outbox_version,
  org_id,workspace_id,user_id,origin_id,batch_id,digest,target_bucket,target_region,key_root,accepted_ordinals,receipt_seats,native_snapshot,frontier,state)
 VALUES(v_plan,v_lease,p->>'generation',v_until,s.stage_token,s.row_version,o.row_version,s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest,
  f.target_bucket,f.target_region,f.key_root,v_keep,v_seats,v_native,to_jsonb(f),'admitted');
 -- All carriers containing any selected native key, plus destination scope
 -- carriers whose unrelated content must be preserved. The indexed native
 -- lookup includes older locations, not only the latest numeric revision.
 FOR c IN SELECT q.*FROM sessions.ingest_history_carriers q WHERE q.carrier_sha256 IN(
  SELECT n.carrier_sha256 FROM sessions.ingest_history_native_locations n JOIN jsonb_array_elements(a.records)x ON n.record_id=x->>'record_id'
   WHERE n.org_id=s.org_id AND n.workspace_id=s.workspace_id AND n.origin_id=s.origin_id AND v_keep@>to_jsonb((x->>'ord')::integer)
  UNION SELECT r.carrier_sha256 FROM sessions.ingest_history_pointer_refs r JOIN jsonb_array_elements(a.records)x
   ON r.harness=CASE WHEN x->>'session_id'IS NULL THEN '_none'ELSE x->>'source'END
    AND coalesce(r.pointer->>'canonical_session_id',r.session_id)=coalesce(x->>'session_id','_origin')
   WHERE r.org_id=s.org_id AND r.workspace_id=s.workspace_id AND v_keep@>to_jsonb((x->>'ord')::integer))
  ORDER BY q.carrier_sha256 LOOP
  IF NOT c.scan_complete OR c.scanned_epoch IS DISTINCT FROM c.epoch OR c.storage_provider<>'s3' OR c.bucket<>f.target_bucket OR NOT c.layout_known THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: selected carrier inventory changed';END IF;
  SELECT count(*)INTO v_refs FROM sessions.ingest_history_pointer_refs WHERE carrier_sha256=c.carrier_sha256;
  IF v_refs=0 THEN CONTINUE;END IF;
  v_gen:=gen_random_uuid();v_root:=f.key_root||'/'||s.org_id||'/upload-generations/g='||v_gen;
  INSERT INTO sessions.ingest_history_plan_carriers VALUES(v_plan,c.carrier_sha256,c.epoch,v_refs,v_gen,v_root);
  INSERT INTO sessions.ingest_history_plan_refs SELECT v_plan,c.carrier_sha256,
   row_number()OVER(ORDER BY r.org_id COLLATE"C",r.workspace_id COLLATE"C",r.harness COLLATE"C",r.session_id COLLATE"C",r.machine_id COLLATE"C")-1,r.pointer,r.pointer_sha256
   FROM sessions.ingest_history_pointer_refs r WHERE r.carrier_sha256=c.carrier_sha256;
  v_carriers:=v_carriers+1;
 END LOOP;
 -- Baseline closure is irrevocable BEFORE the first generation PUT.
 UPDATE sessions.ingest_batches SET publication_state='canonical' WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND origin_id=s.origin_id AND batch_id=s.batch_id;
 PERFORM sessions.ingest_require_buffer_admission(s.org_id);
 IF v_until<=clock_timestamp()OR f.valid_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: plan expired after capture';END IF;
 RETURN jsonb_build_object('schemaGeneration',p->>'generation','planToken',v_plan,'leaseToken',v_lease,'leaseUntil',v_until,
  'stageToken',s.stage_token,'stageVersion',s.row_version::text,'outboxVersion',o.row_version::text,'carrierCount',v_carriers::text,
  'tenant',jsonb_build_object('orgId',s.org_id,'workspaceId',s.workspace_id,'userId',s.user_id),'originId',s.origin_id,
  'batchId',s.batch_id,'batchDigest',s.digest,'claimToken',s.claim_token,'records',a.records,'acceptedOrdinals',v_keep,'receiptSeats',v_seats,
  'stage',jsonb_build_object('key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,'groupOrdinals',s.group_ordinals));
END $claim$;
CREATE FUNCTION sessions.ingest_history_plan_locked(p_plan uuid,p_lease uuid,p_generation text)RETURNS sessions.ingest_history_plans
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $locked$
DECLARE a sessions.ingest_history_plans%ROWTYPE;s sessions.ingest_stage_intents%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_generation(p_generation);
 SELECT *INTO a FROM sessions.ingest_history_plans WHERE plan_token=p_plan;
 IF NOT FOUND OR a.lease_token IS DISTINCT FROM p_lease OR a.schema_generation IS DISTINCT FROM p_generation THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: unknown plan lease';END IF;
 PERFORM sessions.ingest_require_buffer_admission(a.org_id);s:=sessions.ingest_buffer_gc_locked(a.stage_token,p_lease);
 SELECT *INTO a FROM sessions.ingest_history_plans WHERE plan_token=p_plan FOR UPDATE;
 IF a.state NOT IN('admitted','prepared')OR a.stage_version<>s.row_version OR s.gc_keep_groups IS NOT NULL OR s.gc_keep_ordinals IS NOT NULL
  OR s.cleanup_state<>'live'OR s.receipt_state<>'acknowledged'OR a.lease_until<=clock_timestamp()
  OR NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=a.org_id AND o.workspace_id=a.workspace_id AND o.user_id=a.user_id
   AND o.origin_id=a.origin_id AND o.batch_id=a.batch_id AND o.active_stage_token=a.stage_token AND o.state='buffered'AND o.row_version=a.outbox_version)
  OR sessions.ingest_buffer_gc_ordinals(a.stage_token)IS DISTINCT FROM a.accepted_ordinals
  OR sessions.ingest_history_native_snapshot(a.stage_token)IS DISTINCT FROM a.native_snapshot THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: original receipt/custody changed';END IF;
 PERFORM 1 FROM sessions.ingest_publication_frontiers f WHERE to_jsonb(f)=a.frontier AND f.valid_until>clock_timestamp()FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: frontier changed';END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_plan_carriers pc JOIN sessions.ingest_history_carriers c USING(carrier_sha256)
  WHERE pc.plan_token=a.plan_token AND(c.epoch<>pc.source_epoch OR c.scanned_epoch IS DISTINCT FROM c.epoch OR NOT c.scan_complete))THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: captured carrier changed';END IF;
 IF a.lease_until<=clock_timestamp()OR s.gc_lease_until<=clock_timestamp()THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: final plan deadline expired';END IF;
 RETURN a;
END $locked$;
CREATE FUNCTION sessions.ingest_history_plan_heartbeat(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE a sessions.ingest_history_plans%ROWTYPE;v_until timestamptz;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','planToken','leaseToken']);
 a:=sessions.ingest_history_plan_locked((p->>'planToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_stage_intents SET gc_lease_until=v_until WHERE stage_token=a.stage_token AND gc_lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: expired stage cannot renew';END IF;
 UPDATE sessions.ingest_history_plans SET lease_until=v_until WHERE plan_token=a.plan_token AND lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: expired plan cannot renew';END IF;
 RETURN jsonb_build_object('leaseUntil',v_until);
END $heartbeat$;
REVOKE ALL ON FUNCTION sessions.ingest_history_native_snapshot(uuid),sessions.ingest_history_plan_claim(jsonb),sessions.ingest_history_plan_locked(uuid,uuid,text),sessions.ingest_history_plan_heartbeat(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_plan_claim(jsonb),sessions.ingest_history_plan_heartbeat(jsonb)TO bulls96_session_ingest;

CREATE FUNCTION sessions.ingest_history_plan_carriers_page(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $carriers$
DECLARE a sessions.ingest_history_plans%ROWTYPE;v_after text;v_rows jsonb;v_last text;v_total bigint;v_more boolean;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','planToken','leaseToken','after']);
 a:=sessions.ingest_history_plan_locked((p->>'planToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');v_after:=p->>'after';
 IF jsonb_typeof(p->'after')IS DISTINCT FROM'string'OR(v_after<>''AND v_after!~'^[a-f0-9]{64}$')THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid carrier cursor';END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('carrierSha256',pc.carrier_sha256,'epoch',pc.source_epoch::text,'referenceCount',pc.reference_count::text,
  'generation',pc.destination_generation,'prefix',pc.destination_prefix,'physicalRoot',c.physical_root,'bucket',c.bucket)ORDER BY pc.carrier_sha256),'[]'),max(pc.carrier_sha256)
 INTO v_rows,v_last FROM(SELECT *FROM sessions.ingest_history_plan_carriers WHERE plan_token=a.plan_token AND carrier_sha256>v_after COLLATE"C" ORDER BY carrier_sha256 COLLATE"C" LIMIT 20)pc
 JOIN sessions.ingest_history_carriers c USING(carrier_sha256);
 SELECT count(*),coalesce(bool_or(carrier_sha256>v_last COLLATE"C"),false)INTO v_total,v_more FROM sessions.ingest_history_plan_carriers WHERE plan_token=a.plan_token;
 IF octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history: carrier packet bound';END IF;
 IF a.lease_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: carrier page expired';END IF;
 RETURN jsonb_build_object('carriers',v_rows,'total',v_total::text,'next',CASE WHEN v_more THEN v_last ELSE NULL END);
END $carriers$;
CREATE FUNCTION sessions.ingest_history_plan_refs_page(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $refs$
DECLARE a sessions.ingest_history_plans%ROWTYPE;c sessions.ingest_history_plan_carriers%ROWTYPE;v_after bigint;v_rows jsonb;v_next bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','planToken','leaseToken','carrierSha256','after']);
 a:=sessions.ingest_history_plan_locked((p->>'planToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');
 SELECT *INTO c FROM sessions.ingest_history_plan_carriers WHERE plan_token=a.plan_token AND carrier_sha256=p->>'carrierSha256';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: carrier outside immutable plan';END IF;
 v_after:=(p->>'after')::bigint;IF v_after<0 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid plan reference cursor';END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',r.ordinal::text,'pointer',r.pointer,'pointerSha256',r.pointer_sha256)ORDER BY r.ordinal),'[]'),max(r.ordinal)+1
 INTO v_rows,v_next FROM(SELECT *FROM sessions.ingest_history_plan_refs WHERE plan_token=a.plan_token AND carrier_sha256=c.carrier_sha256 AND ordinal>=v_after ORDER BY ordinal LIMIT 100)r;
 IF octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history: plan reference packet bound';END IF;
 IF a.lease_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: plan reference page expired';END IF;
 RETURN jsonb_build_object('references',v_rows,'total',c.reference_count::text,'next',CASE WHEN v_next<c.reference_count THEN v_next::text ELSE NULL END);
END $refs$;
REVOKE ALL ON FUNCTION sessions.ingest_history_plan_carriers_page(jsonb),sessions.ingest_history_plan_refs_page(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_plan_carriers_page(jsonb),sessions.ingest_history_plan_refs_page(jsonb)TO bulls96_session_ingest;


-- Privacy/move work survives source-stage GC. A known-empty slot is still a
-- native-key erasure obligation; no surviving new body is required to remove
-- every older carrier copy. Original ACK/claim metadata remains immutable.
CREATE TABLE sessions.ingest_history_reconcile_outbox(
 native_key_sha256 text PRIMARY KEY REFERENCES sessions.ingest_lazy_native_slots(key_sha256),
 requested_lineage bigint NOT NULL CHECK(requested_lineage>=0),request_version bigint NOT NULL DEFAULT 1,
 source_identity_sha256 text NOT NULL,control_epoch text NOT NULL,
 prior_owner text,current_owner text,state text NOT NULL DEFAULT'pending'CHECK(state IN('pending','leased','complete')),
 lease_token uuid,lease_until timestamptz,lease_generation text,
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
 CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
CREATE INDEX ingest_history_reconcile_pending ON sessions.ingest_history_reconcile_outbox(requested_at,native_key_sha256)WHERE state<>'complete';
ALTER TABLE sessions.ingest_history_reconcile_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_reconcile_outbox FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- 620 triggers run only while the kill switch is live AND a scope is installed or a build holds a LIVE
-- lease. Otherwise (dormant, killed, or only crashed/expired/abandoned builds) every 620 trigger on the
-- live ACK/control/directory paths returns immediately.
CREATE FUNCTION sessions.ingest_history_active()RETURNS boolean
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $active$
BEGIN
 RETURN sessions.ingest_history_live()AND(EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_builds WHERE state IN('keys','captured')AND lease_until>clock_timestamp()));
END $active$;
-- A MANAGED scope has an INSTALLED current authority (and the kill switch is live); nothing else is
-- managed. This is the ONE gate for every 620 effect on legacy state: invalidation, required_scopes,
-- directory clears, replay-epoch advances, managed classification and every read gate. A build in
-- flight, or a leftover/abandoned build, is NOT managed: a live change can only fence it (below), and
-- every other session keeps the main (#224/legacy) behavior.
CREATE FUNCTION sessions.ingest_history_scope_managed(p_org text,p_ws text,p_harness text,p_session text)RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $managed_scope$
BEGIN
 RETURN sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current c
  WHERE c.org_id=p_org AND c.workspace_id=p_ws AND c.harness=p_harness AND c.session_id=p_session);
END $managed_scope$;
-- Fence: abandon in-flight builds (keys/captured) so their install refuses ("lease lost") and the
-- builder restarts from a fresh begin; the build's state is its fence version (keys -> captured ->
-- installed | abandoned, never back). Rows are locked in build_id order (two concurrent ACKs cannot
-- deadlock) and BEFORE any scope advisory lock or publication row is taken, so a fencing ACK never
-- holds a lock a capture/install of the same build waits on. Touches only ingest_history_scope_builds:
-- never the directory, required scopes, publications or any legacy table.
CREATE FUNCTION sessions.ingest_history_fence_builds(p_builds uuid[])RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $fence$
BEGIN
 IF p_builds IS NULL OR cardinality(p_builds)=0 THEN RETURN;END IF;
 PERFORM 1 FROM sessions.ingest_history_scope_builds WHERE build_id=ANY(p_builds)AND state IN('keys','captured')ORDER BY build_id FOR UPDATE;
 UPDATE sessions.ingest_history_scope_builds SET state='abandoned',updated_at=clock_timestamp()
  WHERE build_id=ANY(p_builds)AND state IN('keys','captured');
END $fence$;
REVOKE ALL ON FUNCTION sessions.ingest_history_active(),sessions.ingest_history_scope_managed(text,text,text,text),sessions.ingest_history_fence_builds(uuid[])
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_history_native_changed()RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE v_scope record;v_builds uuid[];v_old uuid[];
BEGIN
 -- Dormant: the production ACK (ingest_lazy_ack_batch) is untouched.
 IF NOT sessions.ingest_history_active()THEN RETURN NULL;END IF;
 -- FIRST fence every in-flight build this statement touches: a native it registered as a key, or a
 -- native seated (now, or before this UPDATE) in its scope. Fencing is the ONLY effect a build in
 -- flight ever has on a live ACK: no invalidation, required scope, directory clear or epoch advance.
 SELECT array_agg(DISTINCT b.build_id)INTO v_builds FROM sessions.ingest_history_scope_builds b WHERE b.state IN('keys','captured')AND b.lease_until>clock_timestamp()AND(
  EXISTS(SELECT 1 FROM history_new n JOIN sessions.ingest_history_scope_build_records br ON br.record_id=n.record_id
   WHERE br.build_id=b.build_id AND n.org_id=b.org_id AND n.workspace_id=b.workspace_id)
  OR EXISTS(SELECT 1 FROM history_new n WHERE n.current_session IS NOT NULL AND n.org_id=b.org_id AND n.workspace_id=b.workspace_id
   AND n.current_source=b.harness AND n.current_session=b.session_id));
 IF TG_OP='UPDATE'THEN
  SELECT array_agg(DISTINCT b.build_id)INTO v_old FROM sessions.ingest_history_scope_builds b WHERE b.state IN('keys','captured')AND b.lease_until>clock_timestamp()
   AND EXISTS(SELECT 1 FROM history_old o WHERE o.current_session IS NOT NULL AND o.org_id=b.org_id AND o.workspace_id=b.workspace_id
    AND o.current_source=b.harness AND o.current_session=b.session_id);
  v_builds:=coalesce(v_builds,'{}')||coalesce(v_old,'{}');
 END IF;
 PERFORM sessions.ingest_history_fence_builds(v_builds);
 -- Reconcile requests only for natives that touch a managed scope (its key set or its current seat).
 -- A changed source identity re-requests the native instead of aborting the ACK.
 INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,current_owner)
 SELECT n.key_sha256,n.lineage,n.source_identity_sha256,n.control_epoch,n.current_owner FROM history_new n
  WHERE(n.current_session IS NOT NULL AND sessions.ingest_history_scope_managed(n.org_id,n.workspace_id,n.current_source,n.current_session))
   OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_keys k WHERE k.org_id=n.org_id AND k.workspace_id=n.workspace_id AND k.record_id=n.record_id
    AND sessions.ingest_history_scope_managed(k.org_id,k.workspace_id,k.harness,k.session_id))
 ON CONFLICT(native_key_sha256)DO UPDATE SET requested_lineage=excluded.requested_lineage,source_identity_sha256=excluded.source_identity_sha256,
  control_epoch=excluded.control_epoch,request_version=sessions.ingest_history_reconcile_outbox.request_version+1,
  prior_owner=CASE WHEN sessions.ingest_history_reconcile_outbox.current_owner IS DISTINCT FROM excluded.current_owner
   THEN sessions.ingest_history_reconcile_outbox.current_owner ELSE sessions.ingest_history_reconcile_outbox.prior_owner END,
  current_owner=excluded.current_owner,state='pending',lease_token=NULL,lease_until=NULL,lease_generation=NULL,requested_at=clock_timestamp(),completed_at=NULL;
 -- A native winner can change without any physical pointer changing: invalidate every MANAGED
 -- consumer in this same ACK/control transaction, in sorted order (waits, never 40001).
 FOR v_scope IN
  SELECT z.*FROM(
   SELECT k.org_id,k.workspace_id,k.harness,k.session_id FROM history_new n JOIN sessions.ingest_history_scope_keys k
    ON k.org_id=n.org_id AND k.workspace_id=n.workspace_id AND k.record_id=n.record_id
   UNION SELECT n.org_id,n.workspace_id,n.current_source,n.current_session FROM history_new n
    WHERE n.current_owner IS NOT NULL AND n.current_session IS NOT NULL
  )z WHERE sessions.ingest_history_scope_managed(z.org_id,z.workspace_id,z.harness,z.session_id)
  ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
 LOOP
  PERFORM sessions.ingest_history_invalidate_scope(v_scope.org_id,v_scope.workspace_id,v_scope.harness,v_scope.session_id);
 END LOOP;
 IF TG_OP='UPDATE'THEN
  FOR v_scope IN SELECT z.*FROM(
   SELECT DISTINCT o.org_id,o.workspace_id,o.current_source AS harness,o.current_session AS session_id
    FROM history_old o JOIN history_new n USING(key_sha256)WHERE o.current_owner IS NOT NULL AND o.current_session IS NOT NULL
     AND ROW(o.current_owner,o.current_source,o.current_session,o.current_revision,o.current_digest,o.lineage)
      IS DISTINCT FROM ROW(n.current_owner,n.current_source,n.current_session,n.current_revision,n.current_digest,n.lineage)
  )z WHERE sessions.ingest_history_scope_managed(z.org_id,z.workspace_id,z.harness,z.session_id)
  ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
  LOOP PERFORM sessions.ingest_history_invalidate_scope(v_scope.org_id,v_scope.workspace_id,v_scope.harness,v_scope.session_id);END LOOP;
 END IF;
 RETURN NULL;
END $changed$;
-- Transition tables batch resets/large ACKs; never one nested SPI mutation per
-- native row while the existing short USER/ACK critical section is held.
CREATE TRIGGER ingest_history_native_insert AFTER INSERT ON sessions.ingest_lazy_native_slots
 REFERENCING NEW TABLE AS history_new FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_history_native_changed();
CREATE TRIGGER ingest_history_native_update AFTER UPDATE ON sessions.ingest_lazy_native_slots
 REFERENCING OLD TABLE AS history_old NEW TABLE AS history_new FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_history_native_changed();
-- Reconcile requests for slots written while 620 was dormant (cleared slots included) are NOT
-- prefilled by the migration: an operator enqueues them, resumably and in bounded pages, when a
-- legacy reconcile is scheduled. Returns the next cursor, NULL when done.
CREATE FUNCTION sessions.ingest_history_reconcile_backfill(p_after text,p_limit integer)RETURNS text
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $reconcile_backfill$
DECLARE v_last text;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: reconcile backfill bound';END IF;
 WITH page AS(SELECT key_sha256,lineage,source_identity_sha256,control_epoch,current_owner FROM sessions.ingest_lazy_native_slots
   WHERE p_after IS NULL OR key_sha256>p_after ORDER BY key_sha256 LIMIT p_limit),
  ins AS(INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,current_owner)
   SELECT * FROM page ON CONFLICT DO NOTHING)
 SELECT max(key_sha256)INTO v_last FROM page;
 RETURN v_last;
END $reconcile_backfill$;
REVOKE ALL ON FUNCTION sessions.ingest_history_reconcile_backfill(text,integer)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
REVOKE ALL ON FUNCTION sessions.ingest_history_native_changed()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


-- Immutable copy-on-write mask pages make one-record moves cheap even when a
-- logical scope contains a million retained records. Tree fanout16/depth64;
-- counts support ordinal seeks without OFFSET scans or full-mask JSONB copies.
CREATE TABLE sessions.ingest_history_mask_pages(
 sha256 text PRIMARY KEY CHECK(sha256~'^[a-f0-9]{64}$'),org_id text NOT NULL,workspace_id text NOT NULL,
 prefix text NOT NULL CHECK(prefix~'^[a-f0-9]{0,64}$'),kind text NOT NULL CHECK(kind IN('leaf','node')),
 record_count bigint NOT NULL CHECK(record_count BETWEEN 0 AND 2000000),document jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions.ingest_history_required_scopes(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),request_version bigint NOT NULL DEFAULT 1,
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
ALTER TABLE sessions.ingest_history_required_scopes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_required_scopes FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE TABLE sessions.ingest_history_scope_versions(
 authority_id uuid PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 source_epoch bigint NOT NULL CHECK(source_epoch>0),source_fingerprint text CHECK(source_fingerprint~'^[a-f0-9]{64}$'),
 mask_sha256 text NOT NULL REFERENCES sessions.ingest_history_mask_pages,record_count bigint NOT NULL CHECK(record_count BETWEEN 0 AND 2000000),
 derived_anchor_key text NOT NULL,bucket text NOT NULL,region text NOT NULL,key_root text NOT NULL,
 state text NOT NULL CHECK(state IN('building','ready','retired')),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),retired_at timestamptz
);
CREATE INDEX ingest_history_scope_versions_lookup ON sessions.ingest_history_scope_versions(org_id,workspace_id,harness,session_id,authority_id);
CREATE TABLE sessions.ingest_history_scope_current(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,ready boolean NOT NULL DEFAULT false,
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
CREATE TABLE sessions.ingest_history_scope_keys(
 org_id text NOT NULL,workspace_id text NOT NULL,record_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,record_id,harness,session_id)
);
CREATE TABLE sessions.ingest_history_derived_objects(
 object_key text PRIMARY KEY,authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,
 sha256 text NOT NULL CHECK(sha256~'^[a-f0-9]{64}$'),bytes bigint NOT NULL CHECK(bytes BETWEEN 1 AND 16777216),
 cleanup_state text NOT NULL DEFAULT'live'CHECK(cleanup_state IN('live','retired','redacted')),
 row_version bigint NOT NULL DEFAULT 1,lease_token uuid,lease_until timestamptz,barrier_version_id text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ingest_history_derived_authority_cleanup ON sessions.ingest_history_derived_objects(authority_id,cleanup_state);
CREATE INDEX ingest_history_derived_pending_cleanup ON sessions.ingest_history_derived_objects(created_at,object_key)WHERE cleanup_state='retired';
CREATE TABLE sessions.ingest_history_summaries(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,kind text NOT NULL CHECK(kind IN('short','long')),
 authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,source_epoch bigint NOT NULL,source_fingerprint text NOT NULL,
 mask_sha256 text NOT NULL,status text NOT NULL CHECK(status IN('ready','disabled','quarantined','empty','unsupported')),
 object_key text,object_sha256 text,object_bytes bigint,key_id text,model text,built_at timestamptz NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id,kind),
 CHECK(status<>'ready'OR(object_key IS NOT NULL AND object_sha256~'^[a-f0-9]{64}$'AND object_bytes BETWEEN 1 AND 16777216 AND key_id IS NOT NULL))
);
CREATE TABLE sessions.ingest_history_states(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,source_epoch bigint NOT NULL,source_fingerprint text NOT NULL,mask_sha256 text NOT NULL,
 state text NOT NULL CHECK(state IN('active','done','abandoned','unknown')),classified_at timestamptz NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
ALTER TABLE sessions.ingest_history_mask_pages ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scope_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scope_current ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scope_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_derived_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_summaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_states ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_mask_pages,sessions.ingest_history_scope_versions,sessions.ingest_history_scope_current,sessions.ingest_history_scope_keys,
 sessions.ingest_history_derived_objects,sessions.ingest_history_summaries,sessions.ingest_history_states FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_scope_required(p_org text,p_ws text,p_harness text,p_session text)RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $required$
 -- Installed authority only (same gate as ingest_history_scope_managed); a required_scopes row alone
 -- (a leftover of any earlier state) never gates a read.
 SELECT sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current c
  WHERE c.org_id=p_org AND c.workspace_id=p_ws AND c.harness=p_harness AND c.session_id=p_session);
$required$;
REVOKE ALL ON FUNCTION sessions.ingest_history_scope_required(text,text,text,text)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_mask_register(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $mask$
DECLARE d jsonb;r jsonb;w jsonb;child sessions.ingest_history_mask_pages%ROWTYPE;v_count bigint;v_sum bigint:=0;v_prefix text;v_kind text;v_sha text;last_key text:='';v_key text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','document']);PERFORM sessions.ingest_require_generation(p->>'generation');
 d:=p->'document';PERFORM sessions.ingest_exact_keys(d,ARRAY['protocol','orgId','workspaceId','prefix','kind','count','items']);
 PERFORM sessions.ingest_identifier(d->'orgId',false);PERFORM sessions.ingest_identifier(d->'workspaceId',true);
 IF d->>'protocol'IS DISTINCT FROM'history-mask-v1'OR octet_length(d::text)>16777216
  OR jsonb_typeof(d->'prefix')IS DISTINCT FROM'string'OR d->>'prefix'!~'^[a-f0-9]{0,64}$'
  OR jsonb_typeof(d->'count')IS DISTINCT FROM'string'OR d->>'count'!~'^(0|[1-9][0-9]{0,6})$'OR(d->>'count')::bigint>2000000
  OR jsonb_typeof(d->'items')IS DISTINCT FROM'array'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid mask page';END IF;
 v_prefix:=d->>'prefix';v_kind:=d->>'kind';v_count:=(d->>'count')::bigint;
 IF v_kind='leaf'THEN
  IF jsonb_array_length(d->'items')>500 OR jsonb_array_length(d->'items')<>v_count THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask leaf count';END IF;
  FOR r IN SELECT value FROM jsonb_array_elements(d->'items')LOOP
   PERFORM sessions.ingest_exact_keys(r,ARRAY['recordId','winner']);
   IF jsonb_typeof(r->'recordId')IS DISTINCT FROM'string'OR char_length(r->>'recordId')NOT BETWEEN 1 AND 1024 OR octet_length(r->>'recordId')>4096 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask record identity';END IF;
   v_key:=encode(sha256(convert_to(r->>'recordId','UTF8')),'hex');
   IF left(v_key,length(v_prefix))<>v_prefix OR v_key<=last_key COLLATE"C"THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask hash order/alias';END IF;last_key:=v_key;
   w:=r->'winner';
   IF w IS DISTINCT FROM'null'::jsonb THEN
    PERFORM sessions.ingest_exact_keys(w,ARRAY['originId','recordId','kind','revision','acceptedSeq','operation','userId','source','sessionId','contentSha256','lineage']);
    IF w->>'recordId'IS DISTINCT FROM r->>'recordId'OR EXISTS(SELECT 1 FROM unnest(ARRAY['originId','recordId','kind','userId','source'])k WHERE jsonb_typeof(w->k)IS DISTINCT FROM'string'OR char_length(w->>k)NOT BETWEEN 1 AND 1024 OR octet_length(w->>k)>4096)
     OR EXISTS(SELECT 1 FROM unnest(ARRAY['revision','acceptedSeq'])k WHERE jsonb_typeof(w->k)IS DISTINCT FROM'string'OR w->>k!~'^[1-9][0-9]{0,18}$'OR(w->>k)::numeric>9223372036854775807)
     OR jsonb_typeof(w->'lineage')IS DISTINCT FROM'string'OR w->>'lineage'!~'^(0|[1-9][0-9]{0,18})$'OR(w->>'lineage')::numeric>9223372036854775807
     OR jsonb_typeof(w->'operation')IS DISTINCT FROM'string'OR w->>'operation'NOT IN('upsert','delete')OR jsonb_typeof(w->'contentSha256')IS DISTINCT FROM'string'OR w->>'contentSha256'!~'^[a-f0-9]{64}$'
     OR(w->'sessionId'IS DISTINCT FROM'null'::jsonb AND(jsonb_typeof(w->'sessionId')IS DISTINCT FROM'string'OR char_length(w->>'sessionId')NOT BETWEEN 1 AND 1024))THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask current custody';END IF;
   END IF;
  END LOOP;
 ELSIF v_kind='node'THEN
  IF length(v_prefix)>=64 OR jsonb_array_length(d->'items')NOT BETWEEN 1 AND 16 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask node bound';END IF;
  FOR r IN SELECT value FROM jsonb_array_elements(d->'items')LOOP
   PERFORM sessions.ingest_exact_keys(r,ARRAY['digit','sha256']);
   IF jsonb_typeof(r->'digit')IS DISTINCT FROM'string'OR r->>'digit'!~'^[a-f0-9]$'OR r->>'digit'<=last_key COLLATE"C"OR jsonb_typeof(r->'sha256')IS DISTINCT FROM'string'OR r->>'sha256'!~'^[a-f0-9]{64}$'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask child order';END IF;
   SELECT *INTO child FROM sessions.ingest_history_mask_pages WHERE sha256=r->>'sha256';
   IF NOT FOUND OR child.org_id<>d->>'orgId'OR child.workspace_id<>d->>'workspaceId'OR child.prefix<>v_prefix||(r->>'digit')OR child.record_count=0 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: mask child proof';END IF;
   last_key:=r->>'digit';v_sum:=v_sum+child.record_count;
  END LOOP;
  IF v_sum<>v_count THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask node count';END IF;
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: unknown mask page kind';END IF;
 v_sha:=encode(sha256(convert_to(d::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_history_mask_pages VALUES(v_sha,d->>'orgId',d->>'workspaceId',v_prefix,v_kind,v_count,d,clock_timestamp())ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_mask_pages WHERE sha256=v_sha AND document=d)THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: mask hash collision';END IF;
 RETURN jsonb_build_object('registered',true,'sha256',v_sha,'count',v_count::text);
END $mask$;

CREATE FUNCTION sessions.ingest_history_mask_rows(p_root text,p_after bigint,p_limit integer)RETURNS TABLE(ordinal bigint,record_id text,winner jsonb)
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $rows$
 WITH RECURSIVE tree AS(
  SELECT p.sha256,p.document,p.kind,0::bigint AS base,0 AS depth FROM sessions.ingest_history_mask_pages p WHERE p.sha256=p_root
  UNION ALL
  SELECT c.sha256,c.document,c.kind,t.base+x.before_count,t.depth+1 FROM tree t
  CROSS JOIN LATERAL(
   SELECT c0.*,coalesce(sum(c0.record_count)OVER(ORDER BY c0.digit COLLATE"C"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)::bigint before_count
   FROM(SELECT c1.*,i->>'digit' digit FROM jsonb_array_elements(CASE WHEN t.kind='node'THEN t.document->'items'ELSE'[]'::jsonb END)i JOIN sessions.ingest_history_mask_pages c1 ON c1.sha256=i->>'sha256')c0
  )x JOIN sessions.ingest_history_mask_pages c ON c.sha256=x.sha256
  WHERE t.kind='node'AND t.depth<64 AND t.base+x.before_count<p_after+p_limit AND t.base+x.before_count+x.record_count>p_after
 )SELECT t.base+r.n-1,r.v->>'recordId',r.v->'winner'FROM tree t CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN t.kind='leaf'THEN t.document->'items'ELSE'[]'::jsonb END)WITH ORDINALITY r(v,n)
 WHERE t.base+r.n-1>=p_after AND t.base+r.n-1<p_after+p_limit ORDER BY 1
$rows$;

ALTER TABLE sessions.ingest_history_scope_versions ADD COLUMN pointer_snapshot_sha256 text NOT NULL CHECK(pointer_snapshot_sha256~'^[a-f0-9]{64}$');
-- Exact pointer inventory this version was installed over: the 610 retire entry carries it so the
-- producer can enumerate and delete exactly the PRIOR keys after a replacement.
ALTER TABLE sessions.ingest_history_scope_versions ADD COLUMN pointer_snapshot jsonb NOT NULL CHECK(jsonb_typeof(pointer_snapshot)='array'),
 ADD CONSTRAINT ingest_history_scope_versions_pointer_hash CHECK(pointer_snapshot_sha256=encode(sha256(convert_to(pointer_snapshot::text,'UTF8')),'hex')),
 ADD COLUMN pointer_count integer NOT NULL CHECK(pointer_count BETWEEN 0 AND 100000),
 ADD CONSTRAINT ingest_history_scope_versions_pointer_count CHECK(pointer_count=jsonb_array_length(pointer_snapshot)),
 ADD COLUMN feature_revision bigint CHECK(feature_revision>0),
 -- AUTHORITY-INDEPENDENT content identity: sha256 of the source snapshot WITHOUT its history
 -- (authorityId/ready/anchor) member, plus the mask root and record count. source_fingerprint names
 -- the authority itself and so differs on every install; this one is equal exactly when a
 -- replacement would republish the same content, which the install refuses.
 ADD COLUMN content_fingerprint text NOT NULL CHECK(content_fingerprint~'^[a-f0-9]{64}$');
CREATE UNIQUE INDEX ingest_history_scope_versions_feature_revision ON sessions.ingest_history_scope_versions(feature_revision)WHERE feature_revision IS NOT NULL;
-- A version's identity and inventory are frozen once written; only lifecycle columns move, and
-- the fingerprint/feature revision are bound exactly once (inside the installing transaction).
CREATE FUNCTION sessions.ingest_history_scope_version_frozen()RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $frozen$
BEGIN
 IF NEW.authority_id<>OLD.authority_id OR NEW.org_id<>OLD.org_id OR NEW.workspace_id<>OLD.workspace_id OR NEW.harness<>OLD.harness
  OR NEW.session_id<>OLD.session_id OR NEW.source_epoch<>OLD.source_epoch OR NEW.mask_sha256<>OLD.mask_sha256 OR NEW.record_count<>OLD.record_count
  OR NEW.pointer_snapshot IS DISTINCT FROM OLD.pointer_snapshot OR NEW.pointer_snapshot_sha256<>OLD.pointer_snapshot_sha256 OR NEW.pointer_count<>OLD.pointer_count
  OR NEW.content_fingerprint<>OLD.content_fingerprint
  OR(OLD.source_fingerprint IS NOT NULL AND NEW.source_fingerprint IS DISTINCT FROM OLD.source_fingerprint)
  OR(OLD.feature_revision IS NOT NULL AND NEW.feature_revision IS DISTINCT FROM OLD.feature_revision)
  OR(OLD.state='retired'AND NEW.state<>'retired')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: scope version is frozen';END IF;
 RETURN NEW;
END $frozen$;
CREATE TRIGGER ingest_history_scope_version_frozen BEFORE UPDATE ON sessions.ingest_history_scope_versions
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_history_scope_version_frozen();
REVOKE ALL ON FUNCTION sessions.ingest_history_scope_version_frozen()FROM PUBLIC;
-- Producer-side exact-key enumeration: pages the frozen pointer inventory of one authority
-- (retired or current). Owner-only until the producer's least-privilege read role is approved.
CREATE FUNCTION sessions.ingest_history_scope_pointer_page(p_authority uuid,p_after integer,p_limit integer)
RETURNS TABLE(ordinal integer,pointer jsonb)LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
 SELECT(e.n-1)::integer,e.v FROM sessions.ingest_history_scope_versions v CROSS JOIN LATERAL jsonb_array_elements(v.pointer_snapshot)WITH ORDINALITY e(v,n)
 WHERE v.authority_id=p_authority AND p_after>=0 AND p_limit BETWEEN 1 AND 1000 AND e.n-1>=p_after ORDER BY e.n LIMIT p_limit
$page$;
REVOKE ALL ON FUNCTION sessions.ingest_history_scope_pointer_page(uuid,integer,integer)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_scope_pointer_page(uuid,integer,integer)TO bulls96_canonical_feature_producer;

-- A source snapshot names the immutable mask root, not all million rows. Existing
-- replay epoch/fingerprint remains the sole logical read/cache authority.
CREATE OR REPLACE FUNCTION sessions.session_replay_source_snapshot(p_org text,p_workspace text,p_harness text,p_session text)
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
 ),'[]'::jsonb)) || coalesce((SELECT jsonb_build_object('history',jsonb_build_object('version',1,'required',true,'ready',c.ready AND v.source_epoch=a.epoch,
  'authorityId',v.authority_id,'maskSha256',v.mask_sha256,'recordCount',v.record_count::text,'derivedAnchorKey',v.derived_anchor_key))
  FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v USING(authority_id)
  WHERE c.org_id=p_org AND c.workspace_id=p_workspace AND c.harness=p_harness AND c.session_id=p_session AND sessions.ingest_history_live()),
  '{}'::jsonb) FROM sessions.session_replay_publications a
 WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session;
$snapshot$;

CREATE FUNCTION sessions.ingest_history_source_current(p jsonb,p_lock boolean)RETURNS sessions.ingest_history_scope_versions
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $current$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;live jsonb;o text;w text;h text;sid text;
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM sessions.ingest_exact_keys(p->'identity',ARRAY['orgId','workspaceId','source','sessionId']);
 PERFORM sessions.ingest_exact_keys(p->'source',ARRAY['epoch','fingerprint']);
 o:=sessions.ingest_identifier(p#>'{identity,orgId}',false);w:=sessions.ingest_identifier(p#>'{identity,workspaceId}',true);
 h:=sessions.ingest_identifier(p#>'{identity,source}',false);sid:=sessions.ingest_identifier(p#>'{identity,sessionId}',false);
 IF jsonb_typeof(p->'authorityId')IS DISTINCT FROM'string'OR p->>'authorityId'!~'^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
  OR jsonb_typeof(p#>'{source,epoch}')IS DISTINCT FROM'string'OR p#>>'{source,epoch}'!~'^[1-9][0-9]{0,18}$'OR(p#>>'{source,epoch}')::numeric>9223372036854775807
  OR jsonb_typeof(p#>'{source,fingerprint}')IS DISTINCT FROM'string'OR p#>>'{source,fingerprint}'!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p->'maskSha256')IS DISTINCT FROM'string'OR p->>'maskSha256'!~'^[a-f0-9]{64}$'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid logical source evidence';END IF;
 IF p_lock THEN
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',o,w,h,sid),0))THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: logical scope busy';END IF;
  SELECT v0.*INTO v FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v0 USING(authority_id)
   WHERE c.org_id=o AND c.workspace_id=w AND c.harness=h AND c.session_id=sid AND c.ready AND v0.state='ready'
    AND v0.authority_id=(p->>'authorityId')::uuid FOR SHARE OF c,v0;
 ELSE
  SELECT v0.*INTO v FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v0 USING(authority_id)
   WHERE c.org_id=o AND c.workspace_id=w AND c.harness=h AND c.session_id=sid AND c.ready AND v0.state='ready'AND v0.authority_id=(p->>'authorityId')::uuid;
 END IF;
 IF NOT FOUND OR v.mask_sha256 IS DISTINCT FROM p->>'maskSha256'OR v.source_epoch::text IS DISTINCT FROM p#>>'{source,epoch}'
  OR v.source_fingerprint IS DISTINCT FROM p#>>'{source,fingerprint}'THEN RETURN NULL;END IF;
 -- Re-read AFTER any blocking row lock. Identical pointers cannot defeat the
 -- monotonic replay epoch or current mask/owner lifecycle.
 live:=sessions.session_replay_source_snapshot(o,w,h,sid);
 IF live->>'epoch'IS DISTINCT FROM v.source_epoch::text OR encode(sha256(convert_to(live::text,'UTF8')),'hex')IS DISTINCT FROM v.source_fingerprint
  OR live#>>'{history,authorityId}'IS DISTINCT FROM v.authority_id::text OR live#>>'{history,ready}'IS DISTINCT FROM'true'
  OR live#>>'{directory,deleted_at}'IS NOT NULL THEN RETURN NULL;END IF;
 RETURN v;
END $current$;
CREATE FUNCTION sessions.session_history_source_lock(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $lock$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256']);v:=sessions.ingest_history_source_current(p,true);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('current',false);END IF;
 PERFORM set_config('relay_history.source_permit',p::text,true);
 RETURN jsonb_build_object('current',true);
END $lock$;
CREATE FUNCTION sessions.session_history_mask_page(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;v_after bigint;v_rows jsonb;v_n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','after']);v:=sessions.ingest_history_source_current(p,false);
 IF v.authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: mask source changed';END IF;
 IF jsonb_typeof(p->'after')IS DISTINCT FROM'string'OR p->>'after'!~'^(0|[1-9][0-9]{0,18})$'OR(p->>'after')::numeric>v.record_count THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask ordinal bound';END IF;
 v_after:=(p->>'after')::bigint;
 SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',r.ordinal::text,'recordId',r.record_id,'winner',r.winner)ORDER BY r.ordinal),'[]'),count(*)INTO v_rows,v_n FROM sessions.ingest_history_mask_rows(v.mask_sha256,v_after,500)r;
 IF v_n<>least(500,v.record_count-v_after)OR octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: incomplete mask tree';END IF;
 IF(sessions.ingest_history_source_current(p,false)).authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: mask source changed after read';END IF;
 RETURN jsonb_build_object('authorityId',v.authority_id,'records',v_rows,'total',v.record_count::text,'next',CASE WHEN v_after+v_n<v.record_count THEN(v_after+v_n)::text ELSE NULL END);
END $page$;
CREATE FUNCTION sessions.session_history_register_object(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $register$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;v_prefix text;v_tail text;v_key text;v_sha text;v_bytes bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','key','sha256','bytes']);v:=sessions.ingest_history_source_current(p,true);
 IF v.authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: derived source changed';END IF;
 v_prefix:=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/';
 v_key:=p->>'key';v_tail:=substr(v_key,length(v_prefix)+1);v_sha:=p->>'sha256';
 IF jsonb_typeof(p->'key')IS DISTINCT FROM'string'OR left(v_key,length(v_prefix))<>v_prefix OR v_tail!~'^[a-z][a-z0-9-]{0,63}/[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}[.]bin$'
  OR jsonb_typeof(p->'sha256')IS DISTINCT FROM'string'OR v_sha!~'^[a-f0-9]{64}$'OR jsonb_typeof(p->'bytes')IS DISTINCT FROM'number'
  OR(p->>'bytes')::numeric<>trunc((p->>'bytes')::numeric)OR(p->>'bytes')::numeric NOT BETWEEN 1 AND 16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: derived object proof';END IF;
 v_bytes:=(p->>'bytes')::bigint;
 INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes)VALUES(v_key,v.authority_id,v_sha,v_bytes)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects WHERE object_key=v_key AND authority_id=v.authority_id AND sha256=v_sha AND bytes=v_bytes AND cleanup_state='live')THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: immutable object intent changed';END IF;
 RETURN jsonb_build_object('registered',true);
END $register$;
CREATE FUNCTION sessions.ingest_history_require_model_permit(p jsonb)RETURNS sessions.ingest_history_scope_versions
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $permit$
DECLARE e jsonb;v sessions.ingest_history_scope_versions%ROWTYPE;
BEGIN
 e:=jsonb_build_object('generation',p->'generation','identity',p->'identity','source',p->'source','authorityId',p->'authorityId','maskSha256',p->'maskSha256');
 IF nullif(current_setting('relay_history.source_permit',true),'')IS NULL OR current_setting('relay_history.source_permit',true)::jsonb IS DISTINCT FROM e THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: final source transaction permit required';END IF;
 v:=sessions.ingest_history_source_current(e,true);RETURN v;
END $permit$;
CREATE FUNCTION sessions.session_history_summary_publish(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $summary$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;x jsonb;v_at timestamptz;v_n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','pointer']);v:=sessions.ingest_history_require_model_permit(p);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('applied',false);END IF;
 x:=p->'pointer';PERFORM sessions.ingest_exact_keys(x,ARRAY['kind','status','objectKey','objectSha256','objectBytes','keyId','model','builtAt']);
 IF x->>'kind'NOT IN('short','long')OR x->>'status'NOT IN('ready','disabled','quarantined','empty','unsupported')OR jsonb_typeof(x->'builtAt')IS DISTINCT FROM'string'
  OR NOT(x->'model'='null'::jsonb OR(jsonb_typeof(x->'model')='string'AND octet_length(x->>'model')BETWEEN 1 AND 1024))THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: summary metadata';END IF;
 v_at:=(x->>'builtAt')::timestamptz;
 IF x->>'status'='ready'THEN
  IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects o WHERE o.object_key=x->>'objectKey'AND o.authority_id=v.authority_id
   AND o.sha256=x->>'objectSha256'AND o.bytes=(x->>'objectBytes')::bigint AND o.cleanup_state='live'
   AND left(o.object_key,length(v.derived_anchor_key)-length('manifest.json')+length('derived/summary-'||(x->>'kind')||'/'))=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/summary-'||(x->>'kind')||'/')OR jsonb_typeof(x->'keyId')IS DISTINCT FROM'string'OR octet_length(x->>'keyId')NOT BETWEEN 1 AND 1024 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: registered encrypted summary required';END IF;
 ELSE
  IF x->'objectKey'IS DISTINCT FROM'null'::jsonb OR x->'objectSha256'IS DISTINCT FROM'null'::jsonb OR x->'objectBytes'IS DISTINCT FROM'null'::jsonb OR x->'keyId'IS DISTINCT FROM'null'::jsonb THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: nonready summary pointer';END IF;
 END IF;
 INSERT INTO sessions.ingest_history_summaries AS q VALUES(v.org_id,v.workspace_id,v.harness,v.session_id,x->>'kind',v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256,x->>'status',x->>'objectKey',x->>'objectSha256',(x->>'objectBytes')::bigint,x->>'keyId',x->>'model',v_at)
 ON CONFLICT(org_id,workspace_id,harness,session_id,kind)DO UPDATE SET authority_id=excluded.authority_id,source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,
  mask_sha256=excluded.mask_sha256,status=excluded.status,object_key=excluded.object_key,object_sha256=excluded.object_sha256,object_bytes=excluded.object_bytes,key_id=excluded.key_id,model=excluded.model,built_at=excluded.built_at
 WHERE q.authority_id<>excluded.authority_id OR q.built_at<=excluded.built_at;
 GET DIAGNOSTICS v_n=ROW_COUNT;RETURN jsonb_build_object('applied',v_n=1);
END $summary$;
CREATE FUNCTION sessions.session_history_state_publish(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $state$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;v_at timestamptz;v_n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','state','classifiedAt']);v:=sessions.ingest_history_require_model_permit(p);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('applied',false);END IF;
 IF p->>'state'NOT IN('active','done','abandoned','unknown')OR jsonb_typeof(p->'classifiedAt')IS DISTINCT FROM'string'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: state metadata';END IF;
 v_at:=(p->>'classifiedAt')::timestamptz;
 INSERT INTO sessions.ingest_history_states AS q VALUES(v.org_id,v.workspace_id,v.harness,v.session_id,v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256,p->>'state',v_at)
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET authority_id=excluded.authority_id,source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,
  mask_sha256=excluded.mask_sha256,state=excluded.state,classified_at=excluded.classified_at WHERE q.authority_id<>excluded.authority_id OR q.classified_at<=excluded.classified_at;
 GET DIAGNOSTICS v_n=ROW_COUNT;
 IF v_n=1 THEN UPDATE sessions.session_directory SET state=p->>'state'WHERE org_id=v.org_id AND workspace_id=v.workspace_id AND harness=v.harness AND session_id=v.session_id AND deleted_at IS NULL;END IF;
 RETURN jsonb_build_object('applied',v_n=1);
END $state$;
REVOKE ALL ON FUNCTION sessions.ingest_history_mask_register(jsonb),sessions.ingest_history_mask_rows(text,bigint,integer),sessions.ingest_history_source_current(jsonb,boolean),sessions.ingest_history_require_model_permit(jsonb),
 sessions.session_history_source_lock(jsonb),sessions.session_history_mask_page(jsonb),sessions.session_history_register_object(jsonb),sessions.session_history_summary_publish(jsonb),sessions.session_history_state_publish(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_mask_register(jsonb)TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.session_history_source_lock(jsonb),sessions.session_history_mask_page(jsonb),sessions.session_history_register_object(jsonb),sessions.session_history_summary_publish(jsonb),sessions.session_history_state_publish(jsonb)TO bulls96_session_ingest;


-- Existing authenticated replay/analysis claims also inherit the logical
-- pending fence. Empty workspace is a supported source identity, not absence.
CREATE OR REPLACE FUNCTION sessions.capture_session_replay_pages(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capture$
DECLARE ids text[]; canonical_id text; captured jsonb; fp text;
BEGIN
 IF p_org IS NULL OR p_org='' OR p_workspace IS NULL OR p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
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
  'blocked',jsonb_typeof(captured->'directory') IS DISTINCT FROM 'object' OR coalesce(captured->'directory'->>'deleted_at','')<>''
   OR captured#>>'{history,required}'IS NOT DISTINCT FROM'true',
  'source',jsonb_build_object('epoch',captured->>'epoch','fingerprint',fp),'snapshot',captured);
END $capture$;
CREATE FUNCTION sessions.session_history_capture(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capture$
DECLARE c jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity']);PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM sessions.ingest_exact_keys(p->'identity',ARRAY['orgId','workspaceId','source','sessionId']);
 c:=sessions.capture_session_replay_pages(p#>>'{identity,orgId}',p#>>'{identity,workspaceId}',p#>>'{identity,source}',p#>>'{identity,sessionId}');
 -- Only the explicit generation-bound capability can proceed to mask load.
 IF c#>>'{snapshot,history,required}'='true'AND c#>>'{snapshot,history,ready}'='true'
  AND jsonb_typeof(c#>'{snapshot,directory}')='object'AND c#>>'{snapshot,directory,deleted_at}'IS NULL THEN
  c:=jsonb_set(c,'{blocked}','false'::jsonb);END IF;
 RETURN c;
END $capture$;
CREATE FUNCTION sessions.session_history_summary_get(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $get$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;q sessions.ingest_history_summaries%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','kind']);v:=sessions.ingest_history_source_current(p,false);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('current',false,'pointer',NULL);END IF;
 IF p->>'kind'NOT IN('short','long')THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: summary kind';END IF;
 SELECT *INTO q FROM sessions.ingest_history_summaries WHERE org_id=v.org_id AND workspace_id=v.workspace_id AND harness=v.harness AND session_id=v.session_id
  AND kind=p->>'kind'AND authority_id=v.authority_id AND source_epoch=v.source_epoch AND source_fingerprint=v.source_fingerprint AND mask_sha256=v.mask_sha256;
 RETURN jsonb_build_object('current',true,'pointer',CASE WHEN FOUND THEN jsonb_build_object('kind',q.kind,'status',q.status,'objectKey',q.object_key,'objectSha256',q.object_sha256,'objectBytes',q.object_bytes,'keyId',q.key_id,'model',q.model,'builtAt',q.built_at)ELSE NULL END);
END $get$;
REVOKE ALL ON FUNCTION sessions.session_history_capture(jsonb),sessions.session_history_summary_get(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_capture(jsonb),sessions.session_history_summary_get(jsonb)TO bulls96_session_ingest;

CREATE OR REPLACE FUNCTION sessions.publish_session_replay_pages(
 p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_snapshot jsonb,
 p_generation text,p_source_generation text,p_prefix text,p_index_key text,p_index_sha256 text,p_index_bytes integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE live jsonb; fp text; anchor jsonb; expected_prefix text; history_anchor text;
BEGIN
 IF sessions.ingest_history_scope_required(p_org,p_workspace,p_harness,p_session)THEN
  IF nullif(current_setting('relay_history.source_permit',true),'')IS NULL THEN RETURN false;END IF;
  DECLARE e jsonb;v sessions.ingest_history_scope_versions%ROWTYPE;
  BEGIN
   e:=current_setting('relay_history.source_permit',true)::jsonb;
   IF e#>>'{identity,orgId}'IS DISTINCT FROM p_org OR e#>>'{identity,workspaceId}'IS DISTINCT FROM p_workspace OR e#>>'{identity,source}'IS DISTINCT FROM p_harness OR e#>>'{identity,sessionId}'IS DISTINCT FROM p_session
    OR e#>>'{source,epoch}'IS DISTINCT FROM p_epoch::text OR e#>>'{source,fingerprint}'IS DISTINCT FROM p_fingerprint
    OR e->>'authorityId'IS DISTINCT FROM p_snapshot#>>'{history,authorityId}'OR e->>'maskSha256'IS DISTINCT FROM p_snapshot#>>'{history,maskSha256}'THEN RETURN false;END IF;
   v:=sessions.ingest_history_source_current(e,true);IF v.authority_id IS NULL THEN RETURN false;END IF;
   IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects o WHERE o.authority_id=v.authority_id
    AND o.object_key=p_index_key AND o.sha256=p_index_sha256 AND o.bytes=p_index_bytes AND o.cleanup_state='live')THEN RETURN false;END IF;
   history_anchor:=v.derived_anchor_key;
  END;
 END IF;
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
 expected_prefix:=CASE WHEN history_anchor IS NOT NULL THEN left(history_anchor,length(history_anchor)-length('manifest.json'))||'derived/replay/'||p_generation||'/'
 ELSE regexp_replace(anchor->>'manifest_key','manifest\.json$','')||'normalized/read-projection-v1/'||(anchor->>'content_sha256')||'/'||p_generation||'/'END;
 IF p_prefix IS DISTINCT FROM expected_prefix OR p_index_key IS DISTINCT FROM p_prefix||p_index_sha256||'.json' THEN RETURN false; END IF;
 UPDATE sessions.session_replay_publications SET published_epoch=p_epoch,source_fingerprint=fp,source_snapshot=live,
  generation=p_generation,source_generation=p_source_generation,artifact_prefix=p_prefix,index_key=p_index_key,index_sha256=p_index_sha256,index_bytes=p_index_bytes,published_at=now()
  WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session AND epoch=p_epoch;
 RETURN FOUND;
END $publish$;

CREATE FUNCTION sessions.session_history_replay_publish(p jsonb)RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $replay$
DECLARE e jsonb;v sessions.ingest_history_scope_versions%ROWTYPE;a jsonb;snapshot jsonb;ok boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','parameters','largeLease']);
 e:=p-ARRAY['parameters','largeLease'];v:=sessions.ingest_history_source_current(e,true);IF v.authority_id IS NULL THEN RETURN false;END IF;
 a:=p->'parameters';IF jsonb_typeof(a)IS DISTINCT FROM'array'OR jsonb_array_length(a)<>13 OR octet_length(a::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: replay publication parameters';END IF;
 snapshot:=CASE WHEN jsonb_typeof(a->6)='string'THEN(a->>6)::jsonb ELSE a->6 END;
 IF a->>0 IS DISTINCT FROM v.org_id OR a->>1 IS DISTINCT FROM v.workspace_id OR a->>2 IS DISTINCT FROM v.harness OR a->>3 IS DISTINCT FROM v.session_id
  OR a->>4 IS DISTINCT FROM v.source_epoch::text OR a->>5 IS DISTINCT FROM v.source_fingerprint OR snapshot#>>'{history,authorityId}'IS DISTINCT FROM v.authority_id::text THEN RETURN false;END IF;
 PERFORM set_config('relay_history.source_permit',e::text,true);
 IF p->'largeLease'='null'::jsonb THEN
  ok:=sessions.publish_session_replay_pages(a->>0,a->>1,a->>2,a->>3,(a->>4)::bigint,a->>5,snapshot,a->>7,a->>8,a->>9,a->>10,a->>11,(a->>12)::integer);
 ELSE
  IF NOT EXISTS(SELECT 1 FROM sessions.session_large_read_jobs j WHERE j.org_id=v.org_id AND j.workspace_id=v.workspace_id AND j.harness=v.harness AND j.session_id=v.session_id
   AND j.history_generation=p->>'generation'AND j.lease_nonce=(p->>'largeLease')::uuid AND j.state='running'AND j.lease_expires_at>clock_timestamp())THEN RETURN false;END IF;
  ok:=sessions.publish_session_large_read(a->>0,a->>1,a->>2,a->>3,(a->>4)::bigint,a->>5,snapshot,a->>7,a->>8,a->>9,a->>10,a->>11,(a->>12)::integer,(p->>'largeLease')::uuid);
 END IF;
 RETURN ok;
END $replay$;
REVOKE ALL ON FUNCTION sessions.session_history_replay_publish(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_replay_publish(jsonb)TO bulls96_session_ingest;


-- Model workers consume exactly the current masked graph through backend-only
-- metadata, without manufacturing JWT claims or reading publication tables.
CREATE FUNCTION sessions.session_history_replay_descriptor(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $descriptor$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;a sessions.session_replay_publications%ROWTYPE;live jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256']);
 v:=sessions.ingest_history_source_current(p,false);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('current',false,'publication',NULL);END IF;
 SELECT *INTO a FROM sessions.session_replay_publications WHERE org_id=v.org_id AND workspace_id=v.workspace_id AND harness=v.harness AND session_id=v.session_id;
 IF NOT FOUND OR a.index_key IS NULL OR a.epoch IS DISTINCT FROM v.source_epoch OR a.published_epoch IS DISTINCT FROM v.source_epoch
  OR a.source_fingerprint IS DISTINCT FROM v.source_fingerprint THEN RETURN jsonb_build_object('current',true,'publication',NULL);END IF;
 live:=sessions.session_replay_source_snapshot(v.org_id,v.workspace_id,v.harness,v.session_id);
 IF a.source_snapshot IS DISTINCT FROM live OR (sessions.ingest_history_source_current(p,false)).authority_id IS NULL THEN RETURN jsonb_build_object('current',false,'publication',NULL);END IF;
 RETURN jsonb_build_object('current',true,'publication',jsonb_build_object('version',1,'identity',p->'identity','generation',a.generation,
  'source',p->'source','prefix',a.artifact_prefix,'index',jsonb_build_object('key',a.index_key,'sha256',a.index_sha256,'bytes',a.index_bytes)));
END $descriptor$;
REVOKE ALL ON FUNCTION sessions.session_history_replay_descriptor(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_replay_descriptor(jsonb)TO bulls96_session_ingest;

-- Internal COW traversal exposes only one bounded immutable metadata page from
-- the explicit workspace. No installed scope/root visibility is implied.
CREATE FUNCTION sessions.ingest_history_mask_get(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $mask_get$
DECLARE d jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','orgId','workspaceId','sha256']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'sha256')IS DISTINCT FROM'string'OR p->>'sha256'!~'^[a-f0-9]{64}$'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask digest';END IF;
 SELECT document INTO d FROM sessions.ingest_history_mask_pages WHERE sha256=p->>'sha256'AND org_id=p->>'orgId'AND workspace_id=p->>'workspaceId';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: unknown mask page';END IF;
 RETURN d;
END $mask_get$;
REVOKE ALL ON FUNCTION sessions.ingest_history_mask_get(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_mask_get(jsonb)TO bulls96_session_ingest;


CREATE FUNCTION sessions.session_history_register_artifact(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $artifact$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;prefix text;tail text;k text;digest text;n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','key','sha256','bytes']);
 v:=sessions.ingest_history_source_current(p,true);
 IF v.authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: artifact source changed';END IF;
 prefix:=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/replay/';
 k:=p->>'key';tail:=substr(k,length(prefix)+1);digest:=p->>'sha256';
 IF jsonb_typeof(p->'key')IS DISTINCT FROM'string'OR left(k,length(prefix))<>prefix
  OR tail!~'^[a-f0-9]{64}/[a-f0-9]{64}[.](json|bin)$'OR split_part(split_part(tail,'/',2),'.',1)IS DISTINCT FROM digest
  OR jsonb_typeof(p->'sha256')IS DISTINCT FROM'string'OR digest!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p->'bytes')IS DISTINCT FROM'number'OR(p->>'bytes')::numeric<>trunc((p->>'bytes')::numeric)
  OR(p->>'bytes')::numeric NOT BETWEEN 1 AND 16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: artifact object proof';END IF;
 n:=(p->>'bytes')::bigint;
 INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes)VALUES(k,v.authority_id,digest,n)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects WHERE object_key=k AND authority_id=v.authority_id AND sha256=digest AND bytes=n AND cleanup_state='live')THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: artifact intent changed';END IF;
 RETURN jsonb_build_object('registered',true);
END $artifact$;
REVOKE ALL ON FUNCTION sessions.session_history_register_artifact(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_register_artifact(jsonb)TO bulls96_session_ingest;


-- Captured source owners are explicit, including prior owners whose last row
-- disappeared. Controls invalidate legacy-only scopes even when no lazy native
-- slot or buffered payload has ever existed for the affected record.
CREATE TABLE sessions.ingest_history_scope_owners(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 control_epoch text NOT NULL CHECK(control_epoch~'^[a-f0-9]{64}$'),control_version bigint NOT NULL CHECK(control_version>=0),
 PRIMARY KEY(org_id,workspace_id,user_id,harness,session_id)
);
CREATE TABLE sessions.ingest_history_privacy_outbox(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,control_epoch text NOT NULL,
 control_version bigint NOT NULL,request_version bigint NOT NULL DEFAULT 1,state text NOT NULL DEFAULT'pending'CHECK(state IN('pending','leased','complete')),
 lease_token uuid,lease_until timestamptz,lease_generation text,requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
 PRIMARY KEY(org_id,workspace_id,user_id),CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
ALTER TABLE sessions.ingest_history_scope_owners ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_privacy_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_scope_owners,sessions.ingest_history_privacy_outbox FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_invalidate_scope(p_org text,p_ws text,p_harness text,p_session text)RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $invalidate$
BEGIN
 -- Only an INSTALLED authority is ever invalidated: nothing legacy moves for any other session.
 IF NOT sessions.ingest_history_scope_managed(p_org,p_ws,p_harness,p_session)THEN RETURN;END IF;
 -- Waits (bounded by the install's short critical section): every other holder of this key only
 -- try-locks it, and the ACK path takes scope locks in sorted order, so this cannot deadlock.
 PERFORM pg_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',p_org,p_ws,p_harness,p_session),0));
 INSERT INTO sessions.ingest_history_required_scopes AS r(org_id,workspace_id,harness,session_id)VALUES(p_org,p_ws,p_harness,p_session)
  ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET request_version=r.request_version+1,requested_at=clock_timestamp();
 UPDATE sessions.ingest_history_scope_current SET ready=false WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_harness AND session_id=p_session;
 UPDATE sessions.session_directory SET state='unknown',short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
  short_summary_source=NULL,short_summary_generated_at=NULL,event_projection_ready=false,work_status=NULL,work_status_at=NULL WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_harness AND session_id=p_session AND(state<>'unknown'OR short_summary IS NOT NULL OR event_projection_ready OR work_status IS NOT NULL);
 INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id)VALUES(p_org,p_ws,p_harness,p_session)
  ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET epoch=nextval('sessions.session_replay_epoch_seq');
 UPDATE sessions.ingest_history_derived_objects o SET cleanup_state='retired',row_version=row_version+1 FROM sessions.ingest_history_scope_versions v
  WHERE o.authority_id=v.authority_id AND v.org_id=p_org AND v.workspace_id=p_ws AND v.harness=p_harness AND v.session_id=p_session AND o.cleanup_state='live';
END $invalidate$;
CREATE FUNCTION sessions.ingest_history_control_changed()RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $privacy$
DECLARE r record;v_builds uuid[];
BEGIN
 IF NOT sessions.ingest_history_active()THEN RETURN NULL;END IF;
 -- FIRST fence in-flight builds whose captured owner evidence this control change supersedes; a
 -- build in flight is never invalidated (it is not managed).
 SELECT array_agg(DISTINCT b.build_id)INTO v_builds FROM sessions.ingest_history_scope_builds b WHERE b.state IN('keys','captured')AND b.lease_until>clock_timestamp()
  AND EXISTS(SELECT 1 FROM history_controls n JOIN sessions.ingest_history_scope_owners o
   ON o.org_id=n.org_id AND o.workspace_id=n.workspace_id AND o.user_id=n.user_id
   WHERE o.org_id=b.org_id AND o.workspace_id=b.workspace_id AND o.harness=b.harness AND o.session_id=b.session_id
    AND(n.epoch<>o.control_epoch OR n.version>o.control_version));
 -- Lock order is the ACK's everywhere: user fences (held by the control caller), THEN the receipt seat
 -- clock, THEN build rows, THEN per-scope locks. The ACK takes the seat clock before its slot UPDATEs
 -- reach scope locks, and ingest_lazy_clear takes it after this receipt INSERT; taking it here first,
 -- unconditionally while 620 is active (no window between a work check and the loop below), makes an
 -- ACK and a control of a scope owner wait, never 40P01.
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 PERFORM sessions.ingest_history_fence_builds(v_builds);
 FOR r IN SELECT z.*FROM(
  SELECT DISTINCT o.org_id,o.workspace_id,o.harness,o.session_id FROM history_controls n JOIN sessions.ingest_history_scope_owners o
   ON o.org_id=n.org_id AND o.workspace_id=n.workspace_id AND o.user_id=n.user_id
   WHERE(n.epoch<>o.control_epoch OR n.version>o.control_version)AND sessions.ingest_history_scope_managed(o.org_id,o.workspace_id,o.harness,o.session_id)
 )z ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
 LOOP PERFORM sessions.ingest_history_invalidate_scope(r.org_id,r.workspace_id,r.harness,r.session_id);END LOOP;
 INSERT INTO sessions.ingest_history_privacy_outbox(org_id,workspace_id,user_id,control_epoch,control_version)
  SELECT DISTINCT ON(c.org_id,c.workspace_id,c.user_id)c.org_id,c.workspace_id,c.user_id,c.epoch,c.version FROM history_controls c
   WHERE EXISTS(SELECT 1 FROM sessions.ingest_history_scope_owners o WHERE o.org_id=c.org_id AND o.workspace_id=c.workspace_id AND o.user_id=c.user_id)
   ORDER BY c.org_id,c.workspace_id,c.user_id,c.version DESC
 ON CONFLICT(org_id,workspace_id,user_id)DO UPDATE SET control_epoch=excluded.control_epoch,control_version=excluded.control_version,
  request_version=sessions.ingest_history_privacy_outbox.request_version+1,state='pending',lease_token=NULL,lease_until=NULL,lease_generation=NULL,requested_at=clock_timestamp(),completed_at=NULL;
 RETURN NULL;
END $privacy$;
CREATE TRIGGER ingest_history_control_changed AFTER INSERT ON sessions.ingest_control_receipts
 REFERENCING NEW TABLE AS history_controls FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_history_control_changed();
REVOKE ALL ON FUNCTION sessions.ingest_history_invalidate_scope(text,text,text,text),sessions.ingest_history_control_changed()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


-- Old physical classifiers cannot overwrite a managed logical verdict merely
-- because their late invocation has a newer wall clock.
CREATE OR REPLACE FUNCTION sessions.session_publication_is_managed(p_org text,p_ws text,p_harness text,p_session text,p_machine text)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $managed$
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=p_org AND workspace_id=p_ws
  AND physical_source=p_harness AND physical_session=p_session AND machine_id=p_machine)
 OR sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current c WHERE c.org_id=p_org AND c.workspace_id=p_ws AND c.harness=p_harness
  AND(c.session_id=p_session OR c.session_id IN(SELECT coalesce(s.canonical_session_id,s.session_id)FROM sessions.session_index s
    WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_harness AND s.session_id=p_session AND s.machine_id=p_machine)));
$managed$;
CREATE FUNCTION sessions.ingest_history_directory_state()RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $directory_state$
DECLARE assigned text;
BEGIN
 -- Installed authority only: every other row (including one with a leftover build or required-scope
 -- row) keeps the legacy summarizer/classifier behavior unchanged.
 IF NOT sessions.ingest_history_scope_managed(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id)THEN RETURN NEW;END IF;
 IF NEW.deleted_at IS NULL AND(TG_OP='INSERT'OR NEW.event_projection_epoch IS NOT DISTINCT FROM OLD.event_projection_epoch)THEN
  SELECT q.state INTO assigned FROM sessions.ingest_history_states q JOIN sessions.ingest_history_scope_current c
   ON c.org_id=q.org_id AND c.workspace_id=q.workspace_id AND c.harness=q.harness AND c.session_id=q.session_id AND c.authority_id=q.authority_id AND c.ready
   JOIN sessions.ingest_history_scope_versions v ON v.authority_id=q.authority_id AND v.state='ready'
   JOIN sessions.session_replay_publications a ON a.org_id=q.org_id AND a.workspace_id=q.workspace_id AND a.harness=q.harness AND a.session_id=q.session_id AND a.epoch=q.source_epoch
   WHERE q.org_id=NEW.org_id AND q.workspace_id=NEW.workspace_id AND q.harness=NEW.harness AND q.session_id=NEW.session_id
    AND q.source_fingerprint=v.source_fingerprint AND q.source_epoch=v.source_epoch AND q.mask_sha256=v.mask_sha256;
 END IF;
 -- Managed summaries are encrypted registered objects; legacy unqualified
 -- plaintext writes must not become an apparent current short summary.
 --171 callers cannot attest a logical mask. Until610 publishes an exact
 -- source-bound feature receipt, even a late physical repair stays unready.
 NEW.event_projection_ready:=false;NEW.work_status:=NULL;NEW.work_status_at:=NULL;
 NEW.short_summary:=NULL;NEW.short_summary_scrub_ruleset:=NULL;NEW.short_summary_scrub_version:=NULL;
 NEW.short_summary_source:=NULL;NEW.short_summary_generated_at:=NULL;
 NEW.state:=coalesce(assigned,'unknown');RETURN NEW;
END $directory_state$;
CREATE TRIGGER ingest_history_directory_state BEFORE INSERT OR UPDATE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_history_directory_state();
REVOKE ALL ON FUNCTION sessions.ingest_history_directory_state()FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


-- Generation-bound managed graph jobs share the proven570 lease/completion
-- transaction; old physical workers must not consume logical-mask work.
ALTER TABLE sessions.session_large_read_jobs ADD COLUMN history_generation text
 CHECK(history_generation IS NULL OR history_generation~'^[a-f0-9]{64}$');
CREATE OR REPLACE FUNCTION sessions.claim_session_large_read(p_nonce uuid,p_lease_seconds integer DEFAULT 120)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE j sessions.session_large_read_jobs%ROWTYPE;c jsonb;
BEGIN
 IF p_nonce IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 30 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='large read: invalid lease'; END IF;
 FOR j IN SELECT * FROM sessions.session_large_read_jobs WHERE
 ((state='queued' AND available_at<=clock_timestamp()) OR (state='running' AND lease_expires_at<=clock_timestamp()))
 AND NOT(sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current h WHERE h.org_id=session_large_read_jobs.org_id AND h.workspace_id=session_large_read_jobs.workspace_id AND h.harness=session_large_read_jobs.harness AND h.session_id=session_large_read_jobs.session_id))
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

CREATE TABLE sessions.ingest_history_graph_scan(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),after_identity text[],
 CHECK(after_identity IS NULL OR cardinality(after_identity)=4)
);
INSERT INTO sessions.ingest_history_graph_scan VALUES(true,NULL);
ALTER TABLE sessions.ingest_history_graph_scan ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_graph_scan FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_history_enqueue_graph(p_org text,p_ws text,p_harness text,p_session text,p_generation text)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $enqueue_graph$
DECLARE c jsonb;
BEGIN
 c:=sessions.session_history_capture(jsonb_build_object('generation',p_generation,'identity',jsonb_build_object('orgId',p_org,'workspaceId',p_ws,'source',p_harness,'sessionId',p_session)));
 IF c->>'exists'IS DISTINCT FROM'true'OR c->>'blocked'IS DISTINCT FROM'false'OR c#>>'{snapshot,history,ready}'IS DISTINCT FROM'true'THEN RETURN jsonb_build_object('enqueued',false,'changed',false);END IF;
 INSERT INTO sessions.session_large_read_jobs AS j(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint)
 VALUES(p_org,p_ws,p_harness,c#>>'{identity,sessionId}',(c#>>'{source,epoch}')::bigint,c#>>'{source,fingerprint}')
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,state='queued',
 available_at=clock_timestamp(),lease_nonce=NULL,lease_expires_at=NULL,attempts=0,last_error=NULL,updated_at=clock_timestamp()
 WHERE j.source_epoch IS DISTINCT FROM excluded.source_epoch OR j.source_fingerprint IS DISTINCT FROM excluded.source_fingerprint OR j.state='superseded';
 RETURN jsonb_build_object('enqueued',true,'changed',FOUND);
END $enqueue_graph$;
REVOKE ALL ON FUNCTION sessions.ingest_history_enqueue_graph(text,text,text,text,text)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.session_history_large_job(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $large_job$
DECLARE action text;nonce uuid;j sessions.session_large_read_jobs%ROWTYPE;c jsonb;ok boolean;o text;w text;h text;sid text;
 cursor_id text[];last_id text[];r record;scanned integer:=0;changed integer:=0;
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 action:=p->>'action';
 IF action='discover'THEN
  PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','action']);
  SELECT after_identity INTO cursor_id FROM sessions.ingest_history_graph_scan WHERE singleton FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN RETURN jsonb_build_object('busy',true,'scanned',0,'changed',0,'cycleComplete',false);END IF;
  FOR r IN SELECT sc.org_id,sc.workspace_id,sc.harness,sc.session_id FROM sessions.ingest_history_scope_current sc
   JOIN sessions.ingest_history_scope_versions sv USING(authority_id)WHERE sc.ready AND sv.state='ready'AND(cursor_id IS NULL OR
    ROW(sc.org_id COLLATE "C",sc.workspace_id COLLATE "C",sc.harness COLLATE "C",sc.session_id COLLATE "C")>
    ROW(cursor_id[1]COLLATE "C",cursor_id[2]COLLATE "C",cursor_id[3]COLLATE "C",cursor_id[4]COLLATE "C"))
   ORDER BY sc.org_id COLLATE "C",sc.workspace_id COLLATE "C",sc.harness COLLATE "C",sc.session_id COLLATE "C" LIMIT 128 LOOP
   scanned:=scanned+1;last_id:=ARRAY[r.org_id,r.workspace_id,r.harness,r.session_id];
   c:=sessions.session_history_capture(jsonb_build_object('generation',p->>'generation','identity',jsonb_build_object('orgId',r.org_id,'workspaceId',r.workspace_id,'source',r.harness,'sessionId',r.session_id)));
   IF c->>'exists'='true'AND c->>'blocked'='false'AND c#>>'{snapshot,history,ready}'='true'THEN
    c:=sessions.ingest_history_enqueue_graph(r.org_id,r.workspace_id,r.harness,r.session_id,p->>'generation');
    IF c->>'changed'='true'THEN changed:=changed+1;END IF;
   END IF;
  END LOOP;
  UPDATE sessions.ingest_history_graph_scan SET after_identity=CASE WHEN scanned<128 THEN NULL ELSE last_id END WHERE singleton;
  RETURN jsonb_build_object('busy',false,'scanned',scanned,'changed',changed,'cycleComplete',scanned<128);
 END IF;
 IF action='claim'THEN PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','action','leaseNonce']);
 ELSIF action IN('heartbeat','retry')THEN PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','action','leaseNonce','identity']);
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid graph job action';END IF;
 IF jsonb_typeof(p->'leaseNonce')IS DISTINCT FROM'string'OR p->>'leaseNonce'!~'^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid graph lease nonce';END IF;
 nonce:=(p->>'leaseNonce')::uuid;
 IF action='claim'THEN
  FOR j IN SELECT q.*FROM sessions.session_large_read_jobs q
   JOIN sessions.ingest_history_scope_current sc ON sc.org_id=q.org_id AND sc.workspace_id=q.workspace_id AND sc.harness=q.harness AND sc.session_id=q.session_id
   JOIN sessions.ingest_history_scope_versions sv USING(authority_id)
   WHERE sc.ready AND sv.state='ready'AND
    ((q.state='queued'AND q.available_at<=clock_timestamp())OR(q.state='running'AND q.lease_expires_at<=clock_timestamp()))
   ORDER BY q.available_at,q.updated_at LIMIT 100 FOR UPDATE OF q SKIP LOCKED LOOP
   IF j.attempts>=5 THEN
    UPDATE sessions.session_large_read_jobs SET state='failed',lease_nonce=NULL,lease_expires_at=NULL,last_error='attempts_exhausted',updated_at=clock_timestamp()
     WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;CONTINUE;
   END IF;
   c:=sessions.session_history_capture(jsonb_build_object('generation',p->>'generation','identity',jsonb_build_object('orgId',j.org_id,'workspaceId',j.workspace_id,'source',j.harness,'sessionId',j.session_id)));
   IF c->>'exists'IS DISTINCT FROM'true'OR c->>'blocked'IS DISTINCT FROM'false'OR c#>>'{snapshot,history,ready}'IS DISTINCT FROM'true'
    OR c#>>'{source,epoch}'IS DISTINCT FROM j.source_epoch::text OR c#>>'{source,fingerprint}'IS DISTINCT FROM j.source_fingerprint THEN
    UPDATE sessions.session_large_read_jobs SET state='superseded',lease_nonce=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
     WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;CONTINUE;
   END IF;
   UPDATE sessions.session_large_read_jobs SET state='running',lease_nonce=nonce,lease_expires_at=clock_timestamp()+interval'120 seconds',history_generation=p->>'generation',
    attempts=attempts+1,last_error=NULL,updated_at=clock_timestamp()
    WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   RETURN jsonb_build_object('identity',c->'identity','source',c->'source','leaseNonce',nonce::text,'schemaGeneration',p->>'generation');
  END LOOP;
  RETURN NULL;
 END IF;
 PERFORM sessions.ingest_exact_keys(p->'identity',ARRAY['orgId','workspaceId','source','sessionId']);
 o:=sessions.ingest_identifier(p#>'{identity,orgId}',false);w:=sessions.ingest_identifier(p#>'{identity,workspaceId}',true);
 h:=sessions.ingest_identifier(p#>'{identity,source}',false);sid:=sessions.ingest_identifier(p#>'{identity,sessionId}',false);
 SELECT *INTO j FROM sessions.session_large_read_jobs WHERE org_id=o AND workspace_id=w AND harness=h AND session_id=sid FOR UPDATE;
 IF NOT FOUND OR j.state<>'running'OR j.lease_nonce IS DISTINCT FROM nonce OR j.history_generation IS DISTINCT FROM p->>'generation'
  OR j.lease_expires_at<=clock_timestamp()THEN RETURN jsonb_build_object('ok',false);END IF;
 IF action='heartbeat'THEN
  c:=sessions.session_history_capture(jsonb_build_object('generation',p->>'generation','identity',p->'identity'));
  IF c->>'exists'IS DISTINCT FROM'true'OR c->>'blocked'IS DISTINCT FROM'false'OR c#>>'{source,epoch}'IS DISTINCT FROM j.source_epoch::text
   OR c#>>'{source,fingerprint}'IS DISTINCT FROM j.source_fingerprint THEN RETURN jsonb_build_object('ok',false);END IF;
  UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval'120 seconds',updated_at=clock_timestamp()
   WHERE org_id=o AND workspace_id=w AND harness=h AND session_id=sid AND state='running'AND lease_nonce=nonce AND history_generation=p->>'generation'AND lease_expires_at>clock_timestamp();
  ok:=FOUND;
 ELSE ok:=sessions.retry_session_large_read(o,w,h,sid,nonce,'large_processing_failed');END IF;
 RETURN jsonb_build_object('ok',ok);
END $large_job$;
REVOKE ALL ON FUNCTION sessions.session_history_large_job(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_large_job(jsonb)TO bulls96_session_ingest;

CREATE OR REPLACE FUNCTION sessions.session_brief(
  p_workspace_id text, p_harness text, p_session_id text
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  status text, stale boolean, title text, description text, state text,
  revision bigint, current_revision bigint, prompt_version text, engine_version text,
  model text, message_count integer, shortened_messages integer,
  input_tokens bigint, output_tokens bigint, error text,
  generated_at timestamptz, attempted_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $brief$
DECLARE
  v_org text;
  v_live jsonb;
  v_current bigint;
  v_queued boolean;
  b sessions.session_briefs%ROWTYPE;
  v_fresh boolean;
  v_status text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session brief: workspace is required';
  END IF;
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session brief: harness is invalid';
  END IF;
  IF p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session brief: session id is invalid';
  END IF;
  SELECT jsonb_object_agg(p.machine_id, p.revision), max(p.revision)
    INTO v_live, v_current
    FROM sessions.session_index AS p
   WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
     AND p.harness = p_harness AND p.session_id = p_session_id
     AND p.deleted_at IS NULL;
  IF v_live IS NULL THEN
    RETURN;
  END IF;
  -- Raw revision equality cannot attest a globally moved record's brief.
  -- Preserve absent/unmanaged behavior; managed text requires qualified input.
  IF sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id AND r.harness=p_harness
    AND(r.session_id=p_session_id OR r.session_id IN(SELECT coalesce(i.canonical_session_id,i.session_id)FROM sessions.session_index i
      WHERE i.org_id=v_org AND i.workspace_id=p_workspace_id AND i.harness=p_harness AND i.session_id=p_session_id)))THEN
   RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='history: qualified brief metadata not ready';END IF;
  SELECT * INTO b FROM sessions.session_briefs AS x
   WHERE x.org_id = v_org AND x.workspace_id = p_workspace_id
     AND x.harness = p_harness AND x.session_id = p_session_id;
  v_queued := EXISTS (SELECT 1 FROM sessions.session_brief_wakeups AS w
                       WHERE w.org_id = v_org AND w.workspace_id = p_workspace_id
                         AND w.harness = p_harness AND w.session_id = p_session_id);
  v_fresh := b.title IS NOT NULL AND b.brief_source_revisions = v_live;
  v_status := CASE
    WHEN v_fresh THEN 'ready'
    -- A rate-limited wakeup waits for the UTC reset: it is not in flight.
    WHEN b.status = 'rate_limited' THEN 'rate_limited'
    WHEN v_queued THEN CASE WHEN b.title IS NULL THEN 'pending' ELSE 'refreshing' END
    WHEN b.status IN ('failed', 'ineligible', 'quarantined', 'empty') THEN 'failed'
    WHEN b.title IS NOT NULL THEN 'stale'
    ELSE 'missing' END;
  org_id := v_org;
  workspace_id := p_workspace_id;
  harness := p_harness;
  session_id := p_session_id;
  status := v_status;
  stale := b.title IS NOT NULL AND NOT v_fresh;
  title := b.title;
  description := b.description;
  state := b.state;
  revision := b.brief_revision;
  current_revision := v_current;
  prompt_version := b.prompt_version;
  engine_version := b.engine_version;
  model := b.model;
  message_count := CASE WHEN b.title IS NOT NULL THEN b.message_count END;
  shortened_messages := CASE WHEN b.title IS NOT NULL THEN b.shortened_messages END;
  input_tokens := CASE WHEN b.title IS NOT NULL THEN b.input_tokens END;
  output_tokens := CASE WHEN b.title IS NOT NULL THEN b.output_tokens END;
  error := CASE WHEN v_fresh THEN NULL
                WHEN b.status = 'ineligible' THEN coalesce(b.error, 'session_analysis_ineligible')
                WHEN b.status = 'quarantined' THEN coalesce(b.error, 'session_quarantined')
                WHEN b.status = 'empty' THEN coalesce(b.error, 'session_empty')
                ELSE b.error END;
  generated_at := b.generated_at;
  attempted_at := b.attempted_at;
  RETURN NEXT;
END
$brief$;




-- Same JWT-only org/workspace authority as the existing directory endpoints.
-- One STABLE statement snapshot; no caller org/key/bucket or schema generation.
CREATE FUNCTION sessions.session_directory_history_summary(p_workspace_id text,p_harness text,p_session_id text,p_kind text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $public_summary$
DECLARE org text;live jsonb;fp text;required boolean;ready boolean:=false;present boolean;
 v sessions.ingest_history_scope_versions%ROWTYPE;generation text;pointer jsonb;logical_state jsonb;authority jsonb;storage jsonb;
BEGIN
 IF p_workspace_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history summary: workspace required';END IF;
 SELECT x.org_id INTO org FROM sessions.session_directory_worker_scope(p_workspace_id)x;
 IF p_harness IS NULL OR p_harness!~'^[a-z0-9][a-z0-9._-]{0,63}$'OR p_session_id IS NULL OR char_length(p_session_id)NOT BETWEEN 1 AND 255
  OR p_kind IS NULL OR p_kind NOT IN('short','long')THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history summary: invalid scope or kind';END IF;
 -- Canonical identity only, never adopt a physical alias of another session.
 IF(SELECT count(*)FROM(SELECT 1 FROM sessions.session_index i WHERE i.org_id=org AND i.workspace_id=p_workspace_id AND i.harness=p_harness
  AND coalesce(i.canonical_session_id,i.session_id)=p_session_id AND i.deleted_at IS NULL LIMIT 10001)b)>10000 THEN
  RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history summary: pointer capacity exceeded';END IF;
 live:=coalesce(sessions.session_replay_source_snapshot(org,p_workspace_id,p_harness,p_session_id),jsonb_build_object('epoch',NULL,'pointers','[]'::jsonb));
 IF octet_length(live::text)>8388608 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history summary: source metadata capacity exceeded';END IF;
 fp:=encode(sha256(convert_to(live::text,'UTF8')),'hex');
 required:=live#>>'{history,required}'IS NOT DISTINCT FROM'true'
  OR sessions.ingest_history_scope_required(org,p_workspace_id,p_harness,p_session_id);
 present:=jsonb_array_length(live->'pointers')>0 AND jsonb_typeof(live->'directory')IS NOT DISTINCT FROM'object'AND live#>>'{directory,deleted_at}'IS NULL;
 IF required AND present THEN
  SELECT m.generation INTO generation FROM sessions.__96bulls_role_validation m WHERE m.name='session_directory_search_worker'AND m.status='pass'AND m.generation~'^[a-f0-9]{64}$';
  SELECT sv.*INTO v FROM sessions.ingest_history_scope_current sc JOIN sessions.ingest_history_scope_versions sv USING(authority_id)
   JOIN sessions.ingest_history_mask_pages mp ON mp.sha256=sv.mask_sha256 AND mp.org_id=sv.org_id AND mp.workspace_id=sv.workspace_id AND mp.prefix=''AND mp.record_count=sv.record_count
   WHERE sc.org_id=org AND sc.workspace_id=p_workspace_id AND sc.harness=p_harness AND sc.session_id=p_session_id AND sc.ready AND sv.state='ready';
  ready:=FOUND AND generation IS NOT NULL AND v.source_epoch::text IS NOT DISTINCT FROM live->>'epoch'AND v.source_fingerprint IS NOT DISTINCT FROM fp
   AND live#>>'{history,ready}'IS NOT DISTINCT FROM'true'AND live#>>'{history,authorityId}'IS NOT DISTINCT FROM v.authority_id::text
   AND live#>>'{history,maskSha256}'IS NOT DISTINCT FROM v.mask_sha256;
  IF ready THEN
   authority:=jsonb_build_object('authorityId',v.authority_id,'maskSha256',v.mask_sha256,'recordCount',v.record_count::text,'derivedAnchorKey',v.derived_anchor_key,'schemaGeneration',generation);
   storage:=jsonb_build_object('provider','s3','bucket',v.bucket,'region',v.region);
   SELECT jsonb_build_object('kind',q.kind,'status',q.status,'objectKey',q.object_key,'objectSha256',q.object_sha256,'objectBytes',q.object_bytes,'keyId',q.key_id,'model',q.model,'builtAt',to_char(q.built_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))INTO pointer
    FROM sessions.ingest_history_summaries q WHERE q.org_id=org AND q.workspace_id=p_workspace_id AND q.harness=p_harness AND q.session_id=p_session_id AND q.kind=p_kind
     AND q.authority_id=v.authority_id AND q.source_epoch=v.source_epoch AND q.source_fingerprint=v.source_fingerprint AND q.mask_sha256=v.mask_sha256
     AND(q.status<>'ready'OR EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects d WHERE d.authority_id=v.authority_id AND d.object_key=q.object_key AND d.sha256=q.object_sha256 AND d.bytes=q.object_bytes AND d.cleanup_state='live'
      AND left(d.object_key,length(v.derived_anchor_key)-length('manifest.json')+length('derived/summary-'||p_kind||'/'))=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/summary-'||p_kind||'/'));
   SELECT jsonb_build_object('state',q.state,'classifiedAt',to_char(q.classified_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))INTO logical_state FROM sessions.ingest_history_states q
    WHERE q.org_id=org AND q.workspace_id=p_workspace_id AND q.harness=p_harness AND q.session_id=p_session_id
     AND q.authority_id=v.authority_id AND q.source_epoch=v.source_epoch AND q.source_fingerprint=v.source_fingerprint AND q.mask_sha256=v.mask_sha256;
  END IF;
 END IF;
 RETURN jsonb_build_object('version',1,'kind',p_kind,'exists',present,'required',required,'ready',ready,
  'identity',jsonb_build_object('orgId',org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id),
  'source',jsonb_build_object('epoch',live->>'epoch','fingerprint',fp),'authority',authority,'storage',storage,'pointer',pointer,'state',logical_state);
END $public_summary$;
REVOKE ALL ON FUNCTION sessions.session_directory_history_summary(text,text,text,text)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_directory_history_summary(text,text,text,text)TO session_directory_search_worker;

CREATE OR REPLACE FUNCTION sessions.session_directory_pointers(
  p_workspace_id text, p_harness text, p_session_id text
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text, machine_id text, user_id text,
  storage_provider text, bucket text, manifest_key text, content_sha256 bytea,
  raw_format text, revision bigint, deleted_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $pointers$
DECLARE
  v_org text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: workspace is required';
  END IF;
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: harness is invalid';
  END IF;
  IF p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: session id is invalid';
  END IF;
  -- A raw-mode request must not bypass the logical mask, including via a
  -- physical alias of the canonical session. Managed reads use the registered
  -- ordinal graph and its pre/post source claim, never this unmasked pointer API.
  IF sessions.ingest_history_scope_required(v_org,p_workspace_id,p_harness,p_session_id)
   OR EXISTS(SELECT 1 FROM sessions.session_index i WHERE i.org_id=v_org AND i.workspace_id=p_workspace_id AND i.harness=p_harness
    AND(i.session_id=p_session_id OR coalesce(i.canonical_session_id,i.session_id)=p_session_id)
    AND sessions.ingest_history_scope_required(v_org,p_workspace_id,p_harness,coalesce(i.canonical_session_id,i.session_id)))THEN
   RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='history: managed raw pointers require masked graph';END IF;
  RETURN QUERY
    SELECT p.org_id, p.workspace_id, p.harness,
           coalesce(p.canonical_session_id, p.session_id),
           coalesce(p.canonical_machine_id, p.machine_id), p.user_id,
           p.storage_provider, p.bucket, p.manifest_key, p.content_sha256,
           p.raw_format, p.revision, p.deleted_at
      FROM sessions.session_index AS p
     WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
       AND p.harness = p_harness
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id
            OR p.session_id = p_session_id)
       AND p.deleted_at IS NULL
     ORDER BY coalesce(p.canonical_machine_id, p.machine_id)
     LIMIT 64;
END
$pointers$;

CREATE OR REPLACE FUNCTION sessions.session_directory_replay_pages(p_workspace_id text,p_harness text,p_session_id text)
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
 -- This JWT-scoped artifact claim never exposes raw physical pointers.
 IF captured#>>'{snapshot,history,required}'='true'AND captured#>>'{snapshot,history,ready}'='true'
  AND jsonb_typeof(captured#>'{snapshot,directory}')='object'AND captured#>>'{snapshot,directory,deleted_at}'IS NULL THEN
  captured:=jsonb_set(captured,'{blocked}','false'::jsonb);END IF;
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

-- ===========================================================================
-- Production scope-version creator and final install CAS (replaces fixture-only
-- INSERTs into ingest_history_scope_versions/scope_current).
--
-- Protocol, in this order (no network/S3/model IO while any lock is held):
--  1. ingest_history_scope_begin      : leased build row for one logical scope,
--                                       binding the authority it expects to replace.
--  2. ingest_history_scope_build_keys : register EVERY destination record key (and
--                                       captured owners) BEFORE any source capture, so
--                                       a later write to any of them invalidates the
--                                       scope through the existing native/control triggers.
--  3. ingest_history_scope_capture    : after the keys are complete, capture the replay
--                                       epoch, required-scope request version and the
--                                       pointer-set hash. The caller then builds and
--                                       registers the mask (ingest_history_mask_register)
--                                       and derived objects from that captured source.
--  4. ingest_history_scope_install    : final CAS under the scope lock. Refuses (40001)
--                                       unless epoch, request version, pointer set and the
--                                       expected current authority are all unchanged, the
--                                       lease is live, and EVERY mask record is a registered
--                                       scope key. Then atomically installs the new ready
--                                       version, retires the prior version and its derived
--                                       objects, and binds the source fingerprint.
-- Retention-ready: builds carry org_id and created_at.
-- ===========================================================================
CREATE TABLE sessions.ingest_history_scope_builds(
 build_id uuid PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 expected_authority_id uuid,generation text NOT NULL CHECK(generation~'^[a-f0-9]{64}$'),
 lease_token uuid NOT NULL,lease_until timestamptz NOT NULL,
 state text NOT NULL DEFAULT'keys'CHECK(state IN('keys','captured','installed','abandoned')),
 key_count bigint NOT NULL DEFAULT 0 CHECK(key_count BETWEEN 0 AND 2000000),
 base_epoch bigint,base_request_version bigint,pointer_snapshot_sha256 text CHECK(pointer_snapshot_sha256~'^[a-f0-9]{64}$'),
 installed_authority_id uuid,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(state IN('keys','abandoned')OR base_epoch IS NOT NULL)
);
CREATE INDEX ingest_history_scope_builds_scope ON sessions.ingest_history_scope_builds(org_id,workspace_id,harness,session_id,created_at);
CREATE INDEX ingest_history_scope_builds_retention ON sessions.ingest_history_scope_builds(org_id,created_at);
-- The live ACK/control fence scans only in-flight builds (a handful), never the retained history.
CREATE INDEX ingest_history_scope_builds_in_flight ON sessions.ingest_history_scope_builds(org_id,workspace_id,harness,session_id)WHERE state IN('keys','captured');
ALTER TABLE sessions.ingest_history_scope_builds ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_scope_builds FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_scope_build_locked(p jsonb,p_state text)RETURNS sessions.ingest_history_scope_builds
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $buildlocked$
DECLARE b sessions.ingest_history_scope_builds%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'buildId')IS DISTINCT FROM'string'OR p->>'buildId'!~'^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
  OR jsonb_typeof(p->'leaseToken')IS DISTINCT FROM'string'OR p->>'leaseToken'!~'^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid scope build identity';END IF;
 SELECT *INTO b FROM sessions.ingest_history_scope_builds WHERE build_id=(p->>'buildId')::uuid FOR UPDATE;
 IF NOT FOUND OR b.lease_token<>(p->>'leaseToken')::uuid OR b.generation<>p->>'generation'OR b.state<>p_state
  OR b.lease_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: scope build lease lost';END IF;
 RETURN b;
END $buildlocked$;
REVOKE ALL ON FUNCTION sessions.ingest_history_scope_build_locked(jsonb,text)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_scope_begin(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $scopebegin$
DECLARE o text;w text;h text;sid text;cur uuid;v_build uuid:=gen_random_uuid();v_lease uuid:=gen_random_uuid();
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM 1 FROM sessions.ingest_history_switch WHERE singleton AND live FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: 620 is switched off';END IF;
 PERFORM sessions.ingest_exact_keys(p->'identity',ARRAY['orgId','workspaceId','source','sessionId']);
 o:=sessions.ingest_identifier(p#>'{identity,orgId}',false);w:=sessions.ingest_identifier(p#>'{identity,workspaceId}',true);
 h:=sessions.ingest_identifier(p#>'{identity,source}',false);sid:=sessions.ingest_identifier(p#>'{identity,sessionId}',false);
 SELECT authority_id INTO cur FROM sessions.ingest_history_scope_current WHERE org_id=o AND workspace_id=w AND harness=h AND session_id=sid;
 -- A newer build supersedes older unfinished ones for the same scope; they can never install.
 UPDATE sessions.ingest_history_scope_builds SET state='abandoned',updated_at=clock_timestamp()
  WHERE org_id=o AND workspace_id=w AND harness=h AND session_id=sid AND state IN('keys','captured');
 INSERT INTO sessions.ingest_history_scope_builds(build_id,org_id,workspace_id,harness,session_id,expected_authority_id,generation,lease_token,lease_until)
  VALUES(v_build,o,w,h,sid,cur,p->>'generation',v_lease,clock_timestamp()+interval'15 minutes');
 RETURN jsonb_build_object('buildId',v_build,'leaseToken',v_lease,'expectedAuthorityId',cur);
END $scopebegin$;

-- The EXACT key set one build registered (its captured source/scope keys). The install refuses
-- unless the mask record set equals it; rows go with the build (retention by the build's org/time).
CREATE TABLE sessions.ingest_history_scope_build_records(
 build_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_builds ON DELETE CASCADE,record_id text NOT NULL,
 PRIMARY KEY(build_id,record_id)
);
ALTER TABLE sessions.ingest_history_scope_build_records ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_scope_build_records FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_scope_build_keys(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $scopekeys$
DECLARE b sessions.ingest_history_scope_builds%ROWTYPE;r jsonb;n bigint:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','buildId','leaseToken','recordIds','owners']);
 b:=sessions.ingest_history_scope_build_locked(p,'keys');
 IF jsonb_typeof(p->'recordIds')IS DISTINCT FROM'array'OR jsonb_array_length(p->'recordIds')>500
  OR jsonb_typeof(p->'owners')IS DISTINCT FROM'array'OR jsonb_array_length(p->'owners')>500 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: scope key page bound';END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(p->'recordIds')LOOP
  IF jsonb_typeof(r)IS DISTINCT FROM'string'OR char_length(r#>>'{}')NOT BETWEEN 1 AND 1024 OR octet_length(r#>>'{}')>4096 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: scope key identity';END IF;
  INSERT INTO sessions.ingest_history_scope_keys VALUES(b.org_id,b.workspace_id,r#>>'{}',b.harness,b.session_id)ON CONFLICT DO NOTHING;
  INSERT INTO sessions.ingest_history_scope_build_records VALUES(b.build_id,r#>>'{}')ON CONFLICT DO NOTHING;
  n:=n+1;
 END LOOP;
 FOR r IN SELECT value FROM jsonb_array_elements(p->'owners')LOOP
  PERFORM sessions.ingest_exact_keys(r,ARRAY['userId','controlEpoch','controlVersion']);
  IF jsonb_typeof(r->'controlEpoch')IS DISTINCT FROM'string'OR r->>'controlEpoch'!~'^[a-f0-9]{64}$'
   OR jsonb_typeof(r->'controlVersion')IS DISTINCT FROM'string'OR r->>'controlVersion'!~'^(0|[1-9][0-9]{0,18})$'OR(r->>'controlVersion')::numeric>9223372036854775807
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: scope owner evidence';END IF;
  INSERT INTO sessions.ingest_history_scope_owners AS s VALUES(b.org_id,b.workspace_id,sessions.ingest_identifier(r->'userId',false),b.harness,b.session_id,r->>'controlEpoch',(r->>'controlVersion')::bigint)
   ON CONFLICT(org_id,workspace_id,user_id,harness,session_id)DO UPDATE SET control_epoch=EXCLUDED.control_epoch,control_version=EXCLUDED.control_version
   WHERE ROW(s.control_epoch,s.control_version)IS DISTINCT FROM ROW(EXCLUDED.control_epoch,EXCLUDED.control_version);
 END LOOP;
 UPDATE sessions.ingest_history_scope_builds SET key_count=key_count+n,updated_at=clock_timestamp()WHERE build_id=b.build_id;
 RETURN jsonb_build_object('registered',n::text);
END $scopekeys$;

CREATE FUNCTION sessions.ingest_history_scope_capture(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $scopecapture$
DECLARE b sessions.ingest_history_scope_builds%ROWTYPE;v_epoch bigint;v_req bigint;v_snapshot jsonb;v_ptr text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','buildId','leaseToken']);
 b:=sessions.ingest_history_scope_build_locked(p,'keys');
 -- The epoch is captured only AFTER every destination key is registered.
 INSERT INTO sessions.session_replay_publications(org_id,workspace_id,harness,session_id)VALUES(b.org_id,b.workspace_id,b.harness,b.session_id)ON CONFLICT DO NOTHING;
 SELECT epoch INTO STRICT v_epoch FROM sessions.session_replay_publications WHERE org_id=b.org_id AND workspace_id=b.workspace_id AND harness=b.harness AND session_id=b.session_id;
 SELECT request_version INTO v_req FROM sessions.ingest_history_required_scopes WHERE org_id=b.org_id AND workspace_id=b.workspace_id AND harness=b.harness AND session_id=b.session_id;
 v_snapshot:=sessions.session_replay_source_snapshot(b.org_id,b.workspace_id,b.harness,b.session_id);
 v_ptr:=encode(sha256(convert_to(coalesce(v_snapshot->'pointers','[]'::jsonb)::text,'UTF8')),'hex');
 UPDATE sessions.ingest_history_scope_builds SET state='captured',base_epoch=v_epoch,base_request_version=v_req,pointer_snapshot_sha256=v_ptr,
  lease_until=clock_timestamp()+interval'15 minutes',updated_at=clock_timestamp()WHERE build_id=b.build_id;
 RETURN jsonb_build_object('epoch',v_epoch::text,'requestVersion',v_req::text,'pointerSnapshotSha256',v_ptr,'snapshot',v_snapshot);
END $scopecapture$;

CREATE FUNCTION sessions.ingest_history_scope_install(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $scopeinstall$
DECLARE b sessions.ingest_history_scope_builds%ROWTYPE;m sessions.ingest_history_mask_pages%ROWTYPE;cur uuid;v_epoch bigint;v_req bigint;
 v_snapshot jsonb;v_ptr text;v_deleted timestamptz;v_id uuid:=gen_random_uuid();v_fp text;v_anchor text;v_root text;v_n bigint;
 prior sessions.ingest_history_scope_versions%ROWTYPE;v_pointers jsonb;v_rev bigint;v_content text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','buildId','leaseToken','maskSha256','recordCount','derivedAnchorKey','bucket','region','keyRoot']);
 IF jsonb_typeof(p->'maskSha256')IS DISTINCT FROM'string'OR p->>'maskSha256'!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p->'recordCount')IS DISTINCT FROM'string'OR p->>'recordCount'!~'^(0|[1-9][0-9]{0,6})$'OR(p->>'recordCount')::bigint>2000000
  OR EXISTS(SELECT 1 FROM unnest(ARRAY['derivedAnchorKey','bucket','region','keyRoot'])k WHERE jsonb_typeof(p->k)IS DISTINCT FROM'string'OR char_length(p->>k)NOT BETWEEN 1 AND 1024)
  OR p->>'keyRoot'!~'^[a-z0-9][a-z0-9-]{0,62}$'OR p->>'region'!~'^[a-z]{2}(-[a-z]+)+-[0-9]$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid scope install evidence';END IF;
 -- Switch row (FOR SHARE) BEFORE the build row: the same order as ingest_history_set_live, so a
 -- concurrent switch-off waits for this install (or this install refuses), never a deadlock.
 PERFORM 1 FROM sessions.ingest_history_switch WHERE singleton AND live FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: 620 is switched off';END IF;
 b:=sessions.ingest_history_scope_build_locked(p,'captured');
 v_anchor:=p->>'derivedAnchorKey';v_root:=(p->>'keyRoot')||'/'||b.org_id||'/';
 IF left(v_anchor,length(v_root))<>v_root OR right(v_anchor,length('/manifest.json'))<>'/manifest.json'OR v_anchor~'(^|/)[.]{1,2}(/|$)'OR v_anchor~'//'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: derived anchor outside tenant';END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',b.org_id,b.workspace_id,b.harness,b.session_id),0))THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: logical scope busy';END IF;
 -- Re-read everything AFTER the scope lock: nothing captured earlier may have moved.
 SELECT authority_id INTO cur FROM sessions.ingest_history_scope_current WHERE org_id=b.org_id AND workspace_id=b.workspace_id AND harness=b.harness AND session_id=b.session_id FOR UPDATE;
 SELECT epoch INTO v_epoch FROM sessions.session_replay_publications WHERE org_id=b.org_id AND workspace_id=b.workspace_id AND harness=b.harness AND session_id=b.session_id FOR SHARE;
 SELECT request_version INTO v_req FROM sessions.ingest_history_required_scopes WHERE org_id=b.org_id AND workspace_id=b.workspace_id AND harness=b.harness AND session_id=b.session_id;
 SELECT deleted_at INTO v_deleted FROM sessions.session_directory WHERE org_id=b.org_id AND workspace_id=b.workspace_id AND harness=b.harness AND session_id=b.session_id;
 v_snapshot:=sessions.session_replay_source_snapshot(b.org_id,b.workspace_id,b.harness,b.session_id);
 v_pointers:=coalesce(v_snapshot->'pointers','[]'::jsonb);
 v_ptr:=encode(sha256(convert_to(v_pointers::text,'UTF8')),'hex');
 IF cur IS DISTINCT FROM b.expected_authority_id OR v_epoch IS DISTINCT FROM b.base_epoch OR v_req IS DISTINCT FROM b.base_request_version
  OR v_ptr IS DISTINCT FROM b.pointer_snapshot_sha256 OR v_deleted IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: scope source changed since capture';END IF;
 SELECT *INTO m FROM sessions.ingest_history_mask_pages WHERE sha256=p->>'maskSha256';
 IF NOT FOUND OR m.org_id<>b.org_id OR m.workspace_id<>b.workspace_id OR m.prefix<>''OR m.record_count<>(p->>'recordCount')::bigint
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask root does not match scope';END IF;
 -- Coverage: the complete mask tree is reachable and every record is a registered key of THIS scope.
 SELECT count(*)INTO v_n FROM sessions.ingest_history_mask_rows(m.sha256,0,m.record_count::integer)r;
 IF v_n<>m.record_count THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: incomplete mask tree';END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_mask_rows(m.sha256,0,m.record_count::integer)r WHERE NOT EXISTS(
   SELECT 1 FROM sessions.ingest_history_scope_keys k WHERE k.org_id=b.org_id AND k.workspace_id=b.workspace_id AND k.record_id=r.record_id
    AND k.harness=b.harness AND k.session_id=b.session_id))
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: mask record not registered as a scope key';END IF;
 -- Exact set equality: the mask's distinct records are exactly the keys THIS build registered.
 IF(SELECT count(DISTINCT r.record_id)FROM sessions.ingest_history_mask_rows(m.sha256,0,m.record_count::integer)r)<>m.record_count
  OR(SELECT count(*)FROM sessions.ingest_history_scope_build_records br WHERE br.build_id=b.build_id)<>m.record_count
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_mask_rows(m.sha256,0,m.record_count::integer)r WHERE NOT EXISTS(
   SELECT 1 FROM sessions.ingest_history_scope_build_records br WHERE br.build_id=b.build_id AND br.record_id=r.record_id))
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: mask record set does not equal the build key set';END IF;
 -- Authority-independent content identity, from the snapshot re-read under the scope lock BEFORE any
 -- swap: the 'history' member (prior authorityId/ready/anchor) is dropped, so two installs over the
 -- same epoch, directory, pending set, frozen pointer inventory and mask are equal.
 v_content:=encode(sha256(convert_to(((v_snapshot-'history')||jsonb_build_object('mask',jsonb_build_object(
  'sha256',m.sha256,'recordCount',m.record_count::text)))::text,'UTF8')),'hex');
 -- Replacement: pin the PRIOR authority and its exact pointer inventory BEFORE the pointer CAS.
 IF cur IS NOT NULL THEN
  SELECT *INTO prior FROM sessions.ingest_history_scope_versions WHERE authority_id=cur FOR UPDATE;
  IF NOT FOUND OR prior.state<>'ready'OR prior.source_fingerprint IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: prior authority not retirable';END IF;
  -- No identical-content replacement: refuse BEFORE anything moves (current row, versions,
  -- derived objects and journal stay exactly as they are).
  IF v_content=prior.content_fingerprint THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: replacement does not change the source content';END IF;
 END IF;
 INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint,mask_sha256,record_count,
  derived_anchor_key,bucket,region,key_root,state,pointer_snapshot_sha256,pointer_snapshot,pointer_count,content_fingerprint)
  VALUES(v_id,b.org_id,b.workspace_id,b.harness,b.session_id,v_epoch,NULL,m.sha256,m.record_count,v_anchor,p->>'bucket',p->>'region',p->>'keyRoot','ready',v_ptr,v_pointers,jsonb_array_length(v_pointers),v_content);
 INSERT INTO sessions.ingest_history_scope_current AS c VALUES(b.org_id,b.workspace_id,b.harness,b.session_id,v_id,true)
  ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET authority_id=EXCLUDED.authority_id,ready=true;
 IF cur IS NOT NULL THEN
  UPDATE sessions.ingest_history_scope_versions SET state='retired',retired_at=clock_timestamp()WHERE authority_id=cur AND state<>'retired';
  UPDATE sessions.ingest_history_derived_objects SET cleanup_state='retired',row_version=row_version+1 WHERE authority_id=cur AND cleanup_state='live';
 END IF;
 -- The fingerprint binds the snapshot that now names this authority as current and ready.
 v_fp:=encode(sha256(convert_to(sessions.session_replay_source_snapshot(b.org_id,b.workspace_id,b.harness,b.session_id)::text,'UTF8')),'hex');
 UPDATE sessions.ingest_history_scope_versions SET source_fingerprint=v_fp WHERE authority_id=v_id;
 -- 610 compose point: the feature journal append is atomic with the pointer CAS above;
 -- any later failure or rollback of this transaction emits nothing.
 -- Replacement emits retire(prior authority, prior inventory) then publish(new authority, new
 -- inventory): two consecutive revisions, both or neither.
 SELECT sessions.canonical_feature_changed(
  jsonb_build_object('orgId',b.org_id,'workspaceId',b.workspace_id,'harness',b.harness,'sessionId',b.session_id,'generation',b.generation),
  jsonb_build_object('authorityId',v_id::text,'epoch',v_epoch::text,'fingerprint',v_fp),
  CASE WHEN cur IS NULL THEN NULL ELSE jsonb_build_object('authorityId',prior.authority_id::text,'epoch',prior.source_epoch::text,'fingerprint',prior.source_fingerprint)END,
  jsonb_build_object('authorityId',v_id::text,'hash',v_ptr,'count',jsonb_array_length(v_pointers)::text),
  CASE WHEN cur IS NULL THEN NULL ELSE jsonb_build_object('authorityId',prior.authority_id::text,'hash',prior.pointer_snapshot_sha256,'count',prior.pointer_count::text)END)
  INTO v_rev;
 -- The publish revision is the installed current revision.
 UPDATE sessions.ingest_history_scope_versions SET feature_revision=v_rev WHERE authority_id=v_id;
 UPDATE sessions.ingest_history_scope_builds SET state='installed',installed_authority_id=v_id,updated_at=clock_timestamp()WHERE build_id=b.build_id;
 RETURN jsonb_build_object('installed',true,'authorityId',v_id,'retiredAuthorityId',cur,
  'source',jsonb_build_object('epoch',v_epoch::text,'fingerprint',v_fp),'maskSha256',m.sha256,'recordCount',m.record_count::text,
  'featureRevision',v_rev::text);
END $scopeinstall$;
-- Owner-only operational switch. OFF: abandon every in-flight build, then every live hook and read gate
-- is inert (main behavior) with all ledgers kept. ON (from off): every installed scope is invalidated
-- (set-based), because ACKs/controls while off were not tracked; its next build re-derives it. Lock
-- order: seat clock, switch row, (ON only) session_index SHARE NOWAIT, build rows (sorted); begin/install
-- take the switch row before any build row.
-- Set-based invalidation of EVERY installed scope (the resume path): the same effects as
-- ingest_history_invalidate_scope, in five statements instead of one loop iteration per scope. Called only
-- by ingest_history_set_live(true), which holds the seat clock (no ACK/control runs) and the switch row FOR
-- UPDATE (no begin/install runs), so the per-scope advisory lock adds nothing here.
CREATE FUNCTION sessions.ingest_history_invalidate_installed()RETURNS bigint
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $invalidate_all$
DECLARE n bigint;
BEGIN
 INSERT INTO sessions.ingest_history_required_scopes AS r(org_id,workspace_id,harness,session_id)
  SELECT org_id,workspace_id,harness,session_id FROM sessions.ingest_history_scope_current
  ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET request_version=r.request_version+1,requested_at=clock_timestamp();
 UPDATE sessions.ingest_history_scope_current SET ready=false WHERE ready;
 UPDATE sessions.session_directory d SET state='unknown',short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
  short_summary_source=NULL,short_summary_generated_at=NULL,event_projection_ready=false,work_status=NULL,work_status_at=NULL
  FROM sessions.ingest_history_scope_current c WHERE d.org_id=c.org_id AND d.workspace_id=c.workspace_id AND d.harness=c.harness AND d.session_id=c.session_id
   AND(d.state<>'unknown'OR d.short_summary IS NOT NULL OR d.event_projection_ready OR d.work_status IS NOT NULL);
 INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id)
  SELECT org_id,workspace_id,harness,session_id FROM sessions.ingest_history_scope_current
  ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET epoch=nextval('sessions.session_replay_epoch_seq');
 UPDATE sessions.ingest_history_derived_objects o SET cleanup_state='retired',row_version=row_version+1
  FROM sessions.ingest_history_scope_versions v JOIN sessions.ingest_history_scope_current c
   ON c.org_id=v.org_id AND c.workspace_id=v.workspace_id AND c.harness=v.harness AND c.session_id=v.session_id
  WHERE o.authority_id=v.authority_id AND o.cleanup_state='live';
 SELECT count(*)INTO n FROM sessions.ingest_history_scope_current;
 RETURN n;
END $invalidate_all$;
REVOKE ALL ON FUNCTION sessions.ingest_history_invalidate_installed()FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_history_set_live(p_live boolean)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='5s' AS $set_live$
DECLARE w sessions.ingest_history_switch%ROWTYPE;n bigint:=0;v_builds uuid[];
BEGIN
 IF p_live IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: switch value required';END IF;
 -- A no-op (already in the requested state) takes no lock at all.
 IF(SELECT x.live FROM sessions.ingest_history_switch x WHERE x.singleton)IS NOT DISTINCT FROM p_live THEN
  RETURN jsonb_build_object('live',p_live,'changed',false);END IF;
 -- Seat clock FIRST, as every ACK and the lazy batch claim take it. Main takes the seat clock and the
 -- session_index write lock in BOTH orders (the lazy batch claim: seat clock, then a session_index UPDATE
 -- via the retire-fence trigger; a lazy selection-disable/reset control: session_index, then the seat
 -- clock), so no blocking position for the SHARE lock below is cycle-free. Resume therefore never WAITS
 -- for session_index: it takes SHARE with NOWAIT and, if any writer holds it, fails with a retryable
 -- 55P03 (nothing changed). SHARE then guarantees no writer that began while OFF is still uncommitted;
 -- later writers see live=true and are tracked. The function's lock_timeout (5s) bounds the seat clock wait.
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 SELECT *INTO STRICT w FROM sessions.ingest_history_switch WHERE singleton FOR UPDATE;
 IF w.live=p_live THEN RETURN jsonb_build_object('live',p_live,'changed',false);END IF;
 IF p_live THEN
  BEGIN
   LOCK TABLE sessions.session_index IN SHARE MODE NOWAIT;
  EXCEPTION WHEN lock_not_available THEN
   RAISE EXCEPTION USING ERRCODE='55P03',MESSAGE='history: session_index has an in-flight writer; nothing changed, retry ingest_history_set_live(true)';
  END;
 END IF;
 -- Both transitions abandon every in-flight build: one begun before OFF, or one that raced the switch
 -- and captured while OFF, saw changes no trigger fenced. Its builder starts again.
 SELECT array_agg(build_id)INTO v_builds FROM sessions.ingest_history_scope_builds WHERE state IN('keys','captured');
 PERFORM sessions.ingest_history_fence_builds(v_builds);
 IF NOT p_live THEN
  -- OFF stops pointer tracking too (ingest_history_tracking() is false): the reference inventory is no
  -- longer attested. refs_complete is cleared here and only a fresh full backfill pass sets it again.
  UPDATE sessions.ingest_history_switch SET live=false,refs_complete=false,changed_at=clock_timestamp()WHERE singleton;
  RETURN jsonb_build_object('live',false,'changed',true,'abandonedBuilds',coalesce(cardinality(v_builds),0)::text);
 END IF;
 -- Resume never restores refs_complete: inventory work stays refused until a fresh full backfill pass.
 UPDATE sessions.ingest_history_switch SET live=true,refs_complete=false,changed_at=clock_timestamp()WHERE singleton;
 n:=sessions.ingest_history_invalidate_installed();
 RETURN jsonb_build_object('live',true,'changed',true,'invalidatedScopes',n::text,'abandonedBuilds',coalesce(cardinality(v_builds),0)::text);
END $set_live$;
-- Owner-only: arm/disarm the session_index pointer-reference trigger. Disarming drops refs_complete
-- (later writes are untracked); re-arming requires a fresh full ingest_history_pointer_ref_backfill pass.
CREATE FUNCTION sessions.ingest_history_set_pointer_tracking(p_on boolean)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET lock_timeout='5s' AS $set_tracking$
BEGIN
 IF p_on IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: switch value required';END IF;
 -- Arming waits for every in-flight session_index writer (SHARE blocks new writers only until this
 -- short transaction commits): a writer that began before the arm cannot commit an untracked row
 -- after the backfill has passed its key. Run with a lock_timeout.
 IF p_on THEN LOCK TABLE sessions.session_index IN SHARE MODE;END IF;
 UPDATE sessions.ingest_history_switch SET pointer_tracking=p_on,refs_complete=CASE WHEN p_on THEN refs_complete ELSE false END,changed_at=clock_timestamp()
  WHERE singleton AND pointer_tracking IS DISTINCT FROM p_on;
 RETURN(SELECT jsonb_build_object('pointerTracking',pointer_tracking,'refsComplete',refs_complete)FROM sessions.ingest_history_switch WHERE singleton);
END $set_tracking$;
REVOKE ALL ON FUNCTION sessions.ingest_history_set_live(boolean),sessions.ingest_history_set_pointer_tracking(boolean)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- CREATE FUNCTION grants EXECUTE to PUBLIC by default: close it before granting the ingest role.
REVOKE ALL ON FUNCTION sessions.ingest_history_scope_build_locked(jsonb,text),sessions.ingest_history_scope_begin(jsonb),sessions.ingest_history_scope_build_keys(jsonb),
 sessions.ingest_history_scope_capture(jsonb),sessions.ingest_history_scope_install(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_scope_begin(jsonb),sessions.ingest_history_scope_build_keys(jsonb),sessions.ingest_history_scope_capture(jsonb),sessions.ingest_history_scope_install(jsonb)TO bulls96_session_ingest;
