-- Generated from620 owned definitions + authoritative560 originals. Stop all620/630 workers first.
-- Operational rollback retains installed authority/object ledgers; never forget late PUT obligations.
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
-- Named operational rollback: allowed while no authority was ever installed (no plans, versions,
-- derived objects or model work). Re-derivable queues (reconcile/privacy outbox, required scopes,
-- in-flight builds) are discarded with the schema; installed authority always blocks.
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_plans)OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_versions)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects)OR to_regprocedure('sessions.session_history_model_claim(jsonb)')IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: retain authority/object ledgers; use sessions.ingest_history_set_live(false)';END IF;
-- The down removes this file's ledger row so the next migrate reapplies it (no function_acl_drift): only
-- possible while it is the LAST applied upstream file, else sequencePreflight would see a ledger_gap.
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105026000_canonical_history_lineage.sql'')'INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: a later migration is applied; use sessions.ingest_history_set_live(false)';END IF;
 END IF;
END $rollback_guard$;
DROP TRIGGER IF EXISTS ingest_history_pointer_sync ON sessions.session_index;
DROP TRIGGER IF EXISTS ingest_history_native_insert ON sessions.ingest_lazy_native_slots;
DROP TRIGGER IF EXISTS ingest_history_native_update ON sessions.ingest_lazy_native_slots;
DROP TRIGGER IF EXISTS ingest_history_scope_version_frozen ON sessions.ingest_history_scope_versions;
DROP TRIGGER IF EXISTS ingest_history_control_changed ON sessions.ingest_control_receipts;
DROP TRIGGER IF EXISTS ingest_history_directory_state ON sessions.session_directory;
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
 ),'[]'::jsonb)) FROM sessions.session_replay_publications a
 WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session;
$snapshot$;
CREATE OR REPLACE FUNCTION sessions.publish_session_replay_pages(
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
CREATE OR REPLACE FUNCTION sessions.session_publication_is_managed(p_org text,p_ws text,p_harness text,p_session text,p_machine text)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $managed$
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=p_org AND workspace_id=p_ws
  AND physical_source=p_harness AND physical_session=p_session AND machine_id=p_machine);
$managed$;
CREATE OR REPLACE FUNCTION sessions.claim_session_large_read(p_nonce uuid,p_lease_seconds integer DEFAULT 120)
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
CREATE OR REPLACE FUNCTION sessions.capture_session_replay_pages(p_org text,p_workspace text,p_harness text,p_session text)
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
DROP FUNCTION IF EXISTS
 sessions.ingest_history_set_pointer_tracking(boolean),
 sessions.ingest_history_set_live(boolean),
 sessions.ingest_history_invalidate_installed(),
 sessions.ingest_history_scope_install(jsonb),
 sessions.ingest_history_scope_capture(jsonb),
 sessions.ingest_history_scope_build_keys(jsonb),
 sessions.ingest_history_scope_begin(jsonb),
 sessions.ingest_history_scope_build_locked(jsonb,text),
 sessions.session_directory_history_summary(text,text,text,text),
 sessions.session_history_large_job(jsonb),
 sessions.ingest_history_enqueue_graph(text,text,text,text,text),
 sessions.ingest_history_directory_state(),
 sessions.ingest_history_control_changed(),
 sessions.ingest_history_invalidate_scope(text,text,text,text),
 sessions.session_history_register_artifact(jsonb),
 sessions.ingest_history_mask_get(jsonb),
 sessions.session_history_replay_descriptor(jsonb),
 sessions.session_history_replay_publish(jsonb),
 sessions.session_history_summary_get(jsonb),
 sessions.session_history_capture(jsonb),
 sessions.session_history_state_publish(jsonb),
 sessions.session_history_summary_publish(jsonb),
 sessions.ingest_history_require_model_permit(jsonb),
 sessions.session_history_register_object(jsonb),
 sessions.session_history_mask_page(jsonb),
 sessions.session_history_source_lock(jsonb),
 sessions.ingest_history_source_current(jsonb,boolean),
 sessions.ingest_history_scope_pointer_page(uuid,integer,integer),
 sessions.ingest_history_scope_version_frozen(),
 sessions.ingest_history_mask_rows(text,bigint,integer),
 sessions.ingest_history_mask_register(jsonb),
 sessions.ingest_history_scope_required(text,text,text,text),
 sessions.ingest_history_reconcile_backfill(text,integer),
 sessions.ingest_history_native_changed(),
 sessions.ingest_history_fence_builds(uuid[]),
 sessions.ingest_history_scope_managed(text,text,text,text),
 sessions.ingest_history_active(),
 sessions.ingest_history_plan_refs_page(jsonb),
 sessions.ingest_history_plan_carriers_page(jsonb),
 sessions.ingest_history_plan_heartbeat(jsonb),
 sessions.ingest_history_plan_locked(uuid,uuid,text),
 sessions.ingest_history_plan_claim(jsonb),
 sessions.ingest_history_native_snapshot(uuid),
 sessions.ingest_history_inventory_heartbeat(jsonb),
 sessions.ingest_history_inventory_finish(jsonb),
 sessions.ingest_history_inventory_page(jsonb),
 sessions.ingest_history_inventory_refs(jsonb),
 sessions.ingest_history_inventory_locked(jsonb),
 sessions.ingest_history_inventory_claim(jsonb),
 sessions.ingest_history_pointer_ref_backfill(jsonb,integer),
 sessions.ingest_history_sync_pointer(),
 sessions.ingest_history_pointer(sessions.session_index),
 sessions.ingest_history_root(text),
 sessions.ingest_history_tracking(),
 sessions.ingest_history_live();
DROP TABLE IF EXISTS
 sessions.ingest_history_scope_build_records,
 sessions.ingest_history_scope_builds,
 sessions.ingest_history_graph_scan,
 sessions.ingest_history_privacy_outbox,
 sessions.ingest_history_scope_owners,
 sessions.ingest_history_states,
 sessions.ingest_history_summaries,
 sessions.ingest_history_derived_objects,
 sessions.ingest_history_scope_keys,
 sessions.ingest_history_scope_current,
 sessions.ingest_history_scope_versions,
 sessions.ingest_history_required_scopes,
 sessions.ingest_history_mask_pages,
 sessions.ingest_history_reconcile_outbox,
 sessions.ingest_history_plan_refs,
 sessions.ingest_history_plan_carriers,
 sessions.ingest_history_plans,
 sessions.ingest_history_native_locations,
 sessions.ingest_history_scan_pages,
 sessions.ingest_history_scan_refs,
 sessions.ingest_history_pointer_refs,
 sessions.ingest_history_carriers,
 sessions.ingest_history_switch;
DROP SEQUENCE IF EXISTS sessions.ingest_history_inventory_epoch;
ALTER TABLE sessions.session_large_read_jobs DROP COLUMN history_generation;
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105026000_canonical_history_lineage.sql';
 END IF;
END $ledger$;
