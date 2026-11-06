-- Background-only immutable canonical publication.040500/040510 own buffered
-- receipts and stage GC; this append never enables the old direct/fanout writer.
-- Upload generations are unrelated to raw.epoch (scrub epochs).
CREATE TABLE sessions.ingest_publication_frontiers (
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
 source_frontier_sha256 text NOT NULL CHECK(source_frontier_sha256~'^[a-f0-9]{64}$'),
 record_maxima_sha256 text NOT NULL CHECK(record_maxima_sha256~'^[a-f0-9]{64}$'),
 valid_until timestamptz NOT NULL,
 target_bucket text NOT NULL CHECK(octet_length(target_bucket) BETWEEN 1 AND 255),
 target_region text NOT NULL CHECK(target_region~'^[a-z0-9-]{1,63}$'),
 key_root text NOT NULL CHECK(key_root~'^[a-z0-9][a-z0-9-]{0,62}$'),
 schema_generation text NOT NULL CHECK(schema_generation~'^[a-f0-9]{64}$'),
 PRIMARY KEY(org_id,workspace_id,user_id,origin_id)
);
CREATE TABLE sessions.ingest_publication_heads (
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
 source text NOT NULL,session_id text NOT NULL,
 physical_workspace text NOT NULL,physical_source text NOT NULL,physical_session text NOT NULL,machine_id text NOT NULL,
 generation uuid NOT NULL,fence_epoch bigint NOT NULL,reset_epoch bigint NOT NULL,
 pointer_revision bigint NOT NULL CHECK(pointer_revision>=0),row_version bigint NOT NULL DEFAULT 1,
 current_key text,current_raw_sha256 text,base_pointer jsonb,build_attempt_token uuid,build_lease_until timestamptz,
 state text NOT NULL CHECK(state IN('building','live','retired')),
 PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id),
 UNIQUE(org_id,workspace_id,physical_source,physical_session,machine_id)
);
CREATE TABLE sessions.ingest_publication_generations (
 generation uuid PRIMARY KEY,prefix text NOT NULL UNIQUE,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
 source text NOT NULL,session_id text NOT NULL,fence_epoch bigint NOT NULL,reset_epoch bigint NOT NULL,
 state text NOT NULL CHECK(state IN('building','live','retired')),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),retired_at timestamptz,
 target_bucket text NOT NULL,target_region text NOT NULL,key_root text NOT NULL,
 FOREIGN KEY(org_id,workspace_id,user_id,origin_id,source,session_id)
  REFERENCES sessions.ingest_publication_heads ON DELETE RESTRICT
);
CREATE TABLE sessions.ingest_publication_deferrals (
 stage_token uuid PRIMARY KEY REFERENCES sessions.ingest_stage_intents ON DELETE RESTRICT,
 retry_after timestamptz NOT NULL,reason text NOT NULL CHECK(reason IN('40001','55P03','55000','57014','40P01')),
 attempts bigint NOT NULL DEFAULT 1 CHECK(attempts>0)
);
ALTER TABLE sessions.ingest_publication_deferrals ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_publication_deferrals FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE TABLE sessions.ingest_publication_attempts (
 attempt_token uuid PRIMARY KEY,schema_generation text NOT NULL CHECK(schema_generation~'^[a-f0-9]{64}$'),stage_token uuid NOT NULL REFERENCES sessions.ingest_stage_intents ON DELETE RESTRICT,
 lease_token uuid NOT NULL,lease_until timestamptz NOT NULL,stage_version bigint NOT NULL,outbox_version bigint NOT NULL,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,
 accepted_ordinals jsonb NOT NULL CHECK(jsonb_typeof(accepted_ordinals)='array'),
 revision_snapshot jsonb NOT NULL CHECK(jsonb_typeof(revision_snapshot)='array'),
 head_snapshots jsonb NOT NULL CHECK(jsonb_typeof(head_snapshots)='array'),
 frontier jsonb NOT NULL,
 state text NOT NULL DEFAULT'admitted' CHECK(state IN('admitted','prepared','published','rejected')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),published_at timestamptz,completion_sha256 text CHECK(completion_sha256~'^[a-f0-9]{64}$')
);
CREATE INDEX ingest_publication_attempt_stage_idx ON sessions.ingest_publication_attempts(stage_token,lease_until);
CREATE TABLE sessions.ingest_publication_images (
 attempt_token uuid NOT NULL REFERENCES sessions.ingest_publication_attempts ON DELETE RESTRICT,
 group_index integer NOT NULL CHECK(group_index>=0),
 generation uuid NOT NULL REFERENCES sessions.ingest_publication_generations ON DELETE RESTRICT,
 manifest_key text NOT NULL UNIQUE,manifest_sha256 text NOT NULL CHECK(manifest_sha256~'^[a-f0-9]{64}$'),
 manifest_bytes bigint NOT NULL CHECK(manifest_bytes BETWEEN 1 AND 16777216),
 raw_sha256 text NOT NULL CHECK(raw_sha256~'^[a-f0-9]{64}$'),pointer_revision bigint NOT NULL,
 pointer_row jsonb NOT NULL CHECK(jsonb_typeof(pointer_row)='object'),
 PRIMARY KEY(attempt_token,group_index)
);
CREATE TABLE sessions.ingest_publication_objects (
 object_key text PRIMARY KEY,generation uuid NOT NULL REFERENCES sessions.ingest_publication_generations ON DELETE RESTRICT,
 sha256 text NOT NULL CHECK(sha256~'^[a-f0-9]{64}$'),bytes bigint NOT NULL CHECK(bytes BETWEEN 1 AND 16777216),
 kind text NOT NULL CHECK(kind IN('raw','manifest','derived')),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 cleanup_state text NOT NULL DEFAULT'live' CHECK(cleanup_state IN('live','retired','redacted')),
 row_version bigint NOT NULL DEFAULT 1,lease_token uuid,lease_until timestamptz,barrier_version_id text,gc_schema_generation text CHECK(gc_schema_generation~'^[a-f0-9]{64}$'),
 CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
CREATE INDEX ingest_publication_objects_cleanup_idx ON sessions.ingest_publication_objects(generation,cleanup_state,object_key);
ALTER TABLE sessions.ingest_publication_frontiers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_publication_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_publication_generations ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_publication_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_publication_images ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_publication_objects ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_publication_frontiers,sessions.ingest_publication_heads,sessions.ingest_publication_generations,
 sessions.ingest_publication_attempts,sessions.ingest_publication_images,sessions.ingest_publication_objects
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Exactly layoutComponent's non-Unicode JS regexp, including two replacement
-- units for an astral scalar and the existing long-id UTF-8 hash suffix.
CREATE FUNCTION sessions.ingest_publication_component(p text) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $component$
DECLARE safe text:='';ch text;
BEGIN
 IF p IS NULL OR p='' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: empty layout identity';END IF;
 FOREACH ch IN ARRAY regexp_split_to_array(p,'') LOOP
  safe:=safe||CASE WHEN ch COLLATE"C"~'^[A-Za-z0-9._-]$' THEN ch WHEN ascii(ch)>65535 THEN '__' ELSE '_' END;
 END LOOP;
 safe:=regexp_replace(safe,'^\.','_');
 IF length(safe)>128 THEN RETURN left(safe,119)||'-'||left(encode(sha256(convert_to(p,'UTF8')),'hex'),8);END IF;
 RETURN safe;
END $component$;

CREATE FUNCTION sessions.ingest_publication_revision_snapshot(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $snapshot$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;v jsonb;
BEGIN
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 SELECT coalesce(jsonb_agg(jsonb_build_object('recordId',q.record_id,'stored',to_jsonb(r)-'updated_at') ORDER BY q.record_id COLLATE"C"),'[]') INTO v
 FROM (SELECT DISTINCT x->>'record_id' record_id FROM jsonb_array_elements(a.records)x
  WHERE (x->>'ord')::int IN(SELECT (o::text)::int FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o))q
 LEFT JOIN sessions.ingest_record_revisions r ON r.org_id=s.org_id AND r.workspace_id=s.workspace_id AND r.origin_id=s.origin_id AND r.record_id=q.record_id;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v)x WHERE x->'stored' IS DISTINCT FROM 'null'::jsonb AND x#>>'{stored,user_id}' IS DISTINCT FROM s.user_id)
 THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='publication: record belongs to another owner';END IF;
 RETURN v;
END $snapshot$;

-- The caller has obtained USER -> BATCH -> INTENT through040510's one helper.
CREATE FUNCTION sessions.ingest_publication_locked(p_attempt uuid,p_lease uuid,p_generation text)
RETURNS sessions.ingest_publication_attempts
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $locked$
DECLARE a sessions.ingest_publication_attempts%ROWTYPE;s sessions.ingest_stage_intents%ROWTYPE;v jsonb;h sessions.ingest_publication_heads%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_generation(p_generation);
 SELECT * INTO a FROM sessions.ingest_publication_attempts WHERE attempt_token=p_attempt;
 IF NOT FOUND OR a.lease_token IS DISTINCT FROM p_lease OR a.schema_generation IS DISTINCT FROM p_generation THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: unknown attempt';END IF;
 PERFORM sessions.ingest_require_buffer_admission(a.org_id);
 s:=sessions.ingest_buffer_gc_locked(a.stage_token,p_lease);
 SELECT * INTO a FROM sessions.ingest_publication_attempts WHERE attempt_token=p_attempt FOR UPDATE;
 IF a.state NOT IN('admitted','prepared') OR a.stage_version<>s.row_version OR s.gc_keep_groups IS NOT NULL
  OR s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged' OR a.lease_until<=clock_timestamp()
  OR NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=a.org_id AND o.workspace_id=a.workspace_id
    AND o.origin_id=a.origin_id AND o.batch_id=a.batch_id AND o.active_stage_token=a.stage_token
    AND o.row_version=a.outbox_version AND o.state='buffered')
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: stale stage lease';END IF;
 PERFORM 1 FROM sessions.ingest_publication_frontiers f
    WHERE f.org_id=a.org_id AND f.workspace_id=a.workspace_id AND f.user_id=a.user_id AND f.origin_id=a.origin_id
      AND(to_jsonb(f)-'valid_until'||jsonb_build_object('valid_until_epoch',extract(epoch FROM f.valid_until)))=a.frontier AND f.valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: source frontier unavailable';END IF;
 IF jsonb_array_length(sessions.ingest_buffer_gc_kept(s.stage_token))<>jsonb_array_length(s.group_ordinals)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: original privacy fence changed';END IF;
 FOR v IN SELECT value FROM jsonb_array_elements(a.head_snapshots)
   ORDER BY value->>'physicalSource' COLLATE"C",value->>'physicalSession' COLLATE"C" LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:publication-pointer',a.org_id,a.workspace_id,v->>'physicalSource',v->>'physicalSession',v->>'machineId'),0))
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer scope busy';END IF;
  SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=a.org_id AND workspace_id=a.workspace_id
   AND user_id=a.user_id AND origin_id=a.origin_id AND source=v->>'source' AND session_id=v->>'sessionId' FOR UPDATE;
  IF NOT FOUND OR h.generation::text IS DISTINCT FROM v->>'generation' OR h.row_version::text IS DISTINCT FROM v->>'headVersion'
   OR h.state='retired' OR h.build_attempt_token IS DISTINCT FROM a.attempt_token OR h.build_lease_until IS NULL OR h.build_lease_until<=clock_timestamp()
   OR h.current_key IS DISTINCT FROM v->>'previousKey' OR h.current_raw_sha256 IS DISTINCT FROM v->>'previousRawSha256'
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: head changed';END IF;
  a.lease_until:=least(a.lease_until,h.build_lease_until);
 END LOOP;
 IF s.gc_lease_until<=clock_timestamp() OR a.lease_until<=clock_timestamp()
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: lease expired after head locks';END IF;
 a.lease_until:=least(a.lease_until,s.gc_lease_until);
 RETURN a;
END $locked$;


CREATE FUNCTION sessions.ingest_publication_candidates(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $candidates$
DECLARE v_limit int;v jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit','generation','keyRoot','bucket','region']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_limit:=sessions.ingest_bounded(p->'limit',1,20);
 SELECT coalesce(jsonb_agg(to_jsonb(q.stage_token)),'[]') INTO v FROM(
  SELECT s.stage_token FROM sessions.ingest_buffer_outbox o
  JOIN sessions.ingest_stage_intents s ON s.stage_token=o.active_stage_token
  LEFT JOIN sessions.ingest_publication_deferrals d ON d.stage_token=s.stage_token
  JOIN sessions.ingest_publication_frontiers f ON f.org_id=o.org_id AND f.workspace_id=o.workspace_id AND f.user_id=o.user_id AND f.origin_id=o.origin_id
  WHERE o.state='buffered' AND s.receipt_state='acknowledged' AND s.cleanup_state='live'
   AND (d.retry_after IS NULL OR d.retry_after<=clock_timestamp()) AND s.gc_replacement_stage_token IS NULL
   AND jsonb_array_length(sessions.ingest_buffer_gc_kept(s.stage_token))=jsonb_array_length(s.group_ordinals)
   AND (s.gc_lease_until IS NULL OR s.gc_lease_until<=clock_timestamp()) AND f.valid_until>clock_timestamp()
   AND f.schema_generation=p->>'generation' AND f.key_root=p->>'keyRoot'
   AND f.target_bucket=p->>'bucket' AND f.target_region=p->>'region'
  ORDER BY o.created_at,o.org_id COLLATE"C",o.workspace_id COLLATE"C",o.origin_id COLLATE"C",o.batch_id COLLATE"C" LIMIT v_limit)q;
 RETURN jsonb_build_object('stageTokens',v);
END $candidates$;

CREATE FUNCTION sessions.ingest_publication_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;o sessions.ingest_buffer_outbox%ROWTYPE;
 f sessions.ingest_publication_frontiers%ROWTYPE;h sessions.ingest_publication_heads%ROWTYPE;g record;r record;
 v_org text;v_ws text;v_user text;v_keyroot text;v_snapshot jsonb;v_accepted jsonb;v_seats jsonb;v_heads jsonb:='[]';v_ordinals jsonb;
 v_attempt uuid:=gen_random_uuid();v_lease uuid:=gen_random_uuid();v_gen uuid;v_source text;v_session text;v_fence bigint;
 v_physws text;v_physsource text;v_physsession text;v_machine text;v_base jsonb;v_revision bigint;v_prefix text;v_pairs jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','keyRoot','bucket','region','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_keyroot:=p->>'keyRoot';
 IF jsonb_typeof(p->'keyRoot') IS DISTINCT FROM'string' OR v_keyroot!~'^[a-z0-9][a-z0-9-]{0,62}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: invalid key root';END IF;
 SELECT org_id,workspace_id,user_id INTO v_org,v_ws,v_user FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: stage absent';END IF;
 PERFORM sessions.ingest_require_buffer_admission(v_org);
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id
  AND b.origin_id=s.origin_id AND b.batch_id=s.batch_id AND b.user_id=s.user_id AND b.digest=s.digest
  AND b.state='committed' AND b.buffer_receipt_kind='staged' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: no durable buffered receipt';END IF;
 SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=s.stage_token FOR UPDATE;
 SELECT * INTO o FROM sessions.ingest_buffer_outbox b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id
  AND b.origin_id=s.origin_id AND b.batch_id=s.batch_id FOR UPDATE;
 IF NOT FOUND OR o.active_stage_token IS DISTINCT FROM s.stage_token OR o.state<>'buffered'
  OR s.receipt_state<>'acknowledged' OR s.cleanup_state<>'live'
  OR s.gc_lease_until>clock_timestamp() OR s.gc_replacement_stage_token IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: stage busy or changed';END IF;
 PERFORM sessions.ingest_require_buffer_admission(s.org_id);
 SELECT * INTO f FROM sessions.ingest_publication_frontiers WHERE org_id=s.org_id AND workspace_id=s.workspace_id
  AND user_id=s.user_id AND origin_id=s.origin_id AND valid_until>clock_timestamp()
  AND key_root=v_keyroot AND target_bucket=p->>'bucket' AND target_region=p->>'region' AND schema_generation=p->>'generation' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: complete source frontier required';END IF;
 IF jsonb_array_length(sessions.ingest_buffer_gc_kept(s.stage_token))<>jsonb_array_length(s.group_ordinals)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: stage needs privacy GC';END IF;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 v_snapshot:=sessions.ingest_publication_revision_snapshot(s.stage_token);
 -- First durable ACK owns both the canonical order and the body decision.
 -- Losing equal-revision aliases remain auditable in the seat snapshot but
 -- can never enter an image, create a head, or poison a publication lease.
 v_seats:=sessions.ingest_receipt_seats_for_stage(s.stage_token);
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(a.records)x JOIN jsonb_array_elements(v_snapshot)q ON q->>'recordId'=x->>'record_id'
  WHERE (x->>'revision')::bigint>coalesce((q#>>'{stored,revision}')::bigint,0) AND q->'stored'<>'null'::jsonb
   AND (x->>'ord')::int IN(SELECT(y::text)::int FROM jsonb_array_elements(s.group_ordinals)gg CROSS JOIN LATERAL jsonb_array_elements(gg)y)
   AND (x->>'ord')::int IN(SELECT(z->>'ord')::int FROM jsonb_array_elements(v_seats->'records')z WHERE (z->>'bodyAccepted')::boolean)
   AND NOT(x->>'session_id' IS NULL AND q#>>'{stored,session_id}' IS NULL)
   AND ROW(x->>'source',x->>'session_id') IS DISTINCT FROM ROW(q#>>'{stored,source}',q#>>'{stored,session_id}'))
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: cross-scope record move requires reconciliation';END IF;
 SELECT coalesce(jsonb_agg((x->>'ord')::int ORDER BY (x->>'ord')::int),'[]') INTO v_accepted
 FROM jsonb_array_elements(a.records)x JOIN jsonb_array_elements(v_snapshot)q ON q->>'recordId'=x->>'record_id'
 WHERE (x->>'ord')::int IN(SELECT (y::text)::int FROM jsonb_array_elements(s.group_ordinals)gg CROSS JOIN LATERAL jsonb_array_elements(gg)y)
  AND (x->>'ord')::int IN(SELECT(z->>'ord')::int FROM jsonb_array_elements(v_seats->'records')z WHERE (z->>'bodyAccepted')::boolean)
  AND (x->>'revision')::bigint>coalesce((q#>>'{stored,revision}')::bigint,0);
 -- Repeat the legacy in-batch first-peer conflict predicate on the complete
 -- newly admitted set; no publication can make an invalid subset canonical.
 IF EXISTS(WITH items AS(
  SELECT x AS item,row_number()OVER(PARTITION BY x->>'record_id',x->>'revision'
    ORDER BY (x->>'revision_id') COLLATE"C",(x->>'ord')::int) rn
  FROM jsonb_array_elements(a.records)x WHERE v_accepted@>to_jsonb((x->>'ord')::int)
 ), peers AS(SELECT * FROM items WHERE rn=1)
 SELECT 1 FROM items i JOIN peers q ON i.item->>'record_id'=q.item->>'record_id' AND i.item->>'revision'=q.item->>'revision'
 WHERE i.item->>'revision_id'<>q.item->>'revision_id' AND i.item->>'digest'<>q.item->>'digest')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: new revision conflict';END IF;
 v_physws:=sessions.ingest_publication_component(s.workspace_id);
 v_machine:='srv_'||left(encode(sha256(convert_to(s.workspace_id||chr(31)||s.origin_id||chr(31)||s.user_id,'UTF8')),'hex'),32);
 FOR g IN SELECT value,ordinality-1 idx FROM jsonb_array_elements(s.group_ordinals)WITH ORDINALITY LOOP
  SELECT coalesce(jsonb_agg(y ORDER BY ordinality),'[]') INTO v_ordinals
   FROM jsonb_array_elements(g.value)WITH ORDINALITY q(y,ordinality) WHERE v_accepted@>y;
  IF jsonb_array_length(v_ordinals)=0 THEN CONTINUE;END IF;
  SELECT x->>'source' source,x->>'session_id' session_id INTO r FROM jsonb_array_elements(a.records)x
   WHERE (x->>'ord')::int=(v_ordinals->>0)::int;
  v_source:=CASE WHEN r.session_id IS NULL THEN '_none' ELSE r.source END;
  v_session:=coalesce(r.session_id,'_origin');
  v_physsource:=sessions.ingest_publication_component(v_source);v_physsession:=sessions.ingest_publication_component(v_session);
  v_fence:=0;
  IF r.session_id IS NOT NULL THEN SELECT coalesce(max(epoch),0) INTO v_fence FROM sessions.ingest_fence_versions
   WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND user_id=s.user_id AND origin_id=s.origin_id
    AND source=r.source AND session_id=r.session_id;END IF;
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:publication-pointer',s.org_id,s.workspace_id,v_physsource,v_physsession,v_machine),0))
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer scope busy';END IF;
  SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=s.org_id AND workspace_id=s.workspace_id
   AND user_id=s.user_id AND origin_id=s.origin_id AND source=v_source AND session_id=v_session FOR UPDATE;
  IF NOT FOUND THEN
   SELECT to_jsonb(q),coalesce(q.revision,0) INTO v_base,v_revision FROM sessions.session_index q
    WHERE q.org_id=s.org_id AND q.workspace_id=s.workspace_id AND q.harness=v_physsource
     AND q.session_id=v_physsession AND q.machine_id=v_machine FOR UPDATE;
   v_revision:=coalesce(v_revision,0);
   IF v_base IS NOT NULL AND v_base->>'deleted_at' IS NULL AND v_base->>'user_id' IS DISTINCT FROM s.user_id
   THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='publication: legacy pointer owner mismatch';END IF;
   IF v_base->>'deleted_at' IS NOT NULL THEN v_base:=NULL;END IF;
   v_gen:=gen_random_uuid();v_prefix:=v_keyroot||'/'||s.org_id||'/upload-generations/g='||v_gen;
   INSERT INTO sessions.ingest_publication_heads(org_id,workspace_id,user_id,origin_id,source,session_id,
    physical_workspace,physical_source,physical_session,machine_id,generation,fence_epoch,reset_epoch,pointer_revision,base_pointer,state)
   VALUES(s.org_id,s.workspace_id,s.user_id,s.origin_id,v_source,v_session,
    v_physws,v_physsource,v_physsession,v_machine,v_gen,v_fence,s.reset_epoch,v_revision,v_base,'building') RETURNING*INTO h;
   INSERT INTO sessions.ingest_publication_generations VALUES(v_gen,v_prefix,s.org_id,s.workspace_id,s.user_id,s.origin_id,
    v_source,v_session,v_fence,s.reset_epoch,'building',clock_timestamp(),NULL,f.target_bucket,f.target_region,f.key_root);
  ELSIF h.fence_epoch<>v_fence OR h.reset_epoch<>s.reset_epoch OR h.state='retired'
   OR NOT EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE generation=h.generation AND state IN('building','live')) THEN
   UPDATE sessions.ingest_publication_generations SET state='retired',retired_at=coalesce(retired_at,clock_timestamp()) WHERE generation=h.generation;
   v_gen:=gen_random_uuid();v_prefix:=v_keyroot||'/'||s.org_id||'/upload-generations/g='||v_gen;
   UPDATE sessions.ingest_publication_heads SET generation=v_gen,fence_epoch=v_fence,reset_epoch=s.reset_epoch,
    state='building',current_key=NULL,current_raw_sha256=NULL,
    base_pointer=CASE WHEN h.state='building' AND h.current_key IS NULL AND h.fence_epoch=v_fence AND h.reset_epoch=s.reset_epoch
     THEN h.base_pointer ELSE NULL END,row_version=row_version+1
    WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id AND origin_id=h.origin_id
     AND source=h.source AND session_id=h.session_id RETURNING*INTO h;
   INSERT INTO sessions.ingest_publication_generations VALUES(v_gen,v_prefix,s.org_id,s.workspace_id,s.user_id,s.origin_id,
    v_source,v_session,v_fence,s.reset_epoch,'building',clock_timestamp(),NULL,f.target_bucket,f.target_region,f.key_root);
  END IF;
  SELECT prefix INTO v_prefix FROM sessions.ingest_publication_generations WHERE generation=h.generation AND state IN('building','live')
   AND target_bucket=f.target_bucket AND target_region=f.target_region AND key_root=f.key_root;
  IF NOT FOUND OR v_prefix IS DISTINCT FROM v_keyroot||'/'||s.org_id||'/upload-generations/g='||h.generation
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: generation changed';END IF;
  IF h.build_attempt_token IS NOT NULL AND h.build_lease_until>clock_timestamp() THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: another batch is building this head';
  END IF;
  UPDATE sessions.ingest_publication_heads SET build_attempt_token=v_attempt,build_lease_until=clock_timestamp()+interval'5 minutes'
   WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id AND origin_id=h.origin_id
    AND source=h.source AND session_id=h.session_id RETURNING*INTO h;
  v_heads:=v_heads||jsonb_build_array(jsonb_build_object('groupIndex',g.idx,'ordinals',v_ordinals,'generation',h.generation,'prefix',v_prefix,
   'headVersion',h.row_version::text,'revision',(h.pointer_revision+1)::text,'previousKey',h.current_key,
   'previousRawSha256',h.current_raw_sha256,'basePointer',h.base_pointer,'source',h.source,'sessionId',h.session_id,
   'physicalWorkspace',h.physical_workspace,'physicalSource',h.physical_source,'physicalSession',h.physical_session,'machineId',h.machine_id,
   'fenceEpoch',h.fence_epoch::text,'resetEpoch',h.reset_epoch::text));
 END LOOP;
 -- This closes the strict seed baseline BEFORE any canonical image is written.
 UPDATE sessions.ingest_batches SET publication_state='canonical' WHERE org_id=s.org_id AND workspace_id=s.workspace_id
  AND origin_id=s.origin_id AND batch_id=s.batch_id;
 UPDATE sessions.ingest_stage_intents SET row_version=row_version+1,gc_lease_version=row_version+1,
  gc_lease_token=v_lease,gc_generation=p->>'generation',gc_lease_until=clock_timestamp()+interval'5 minutes',gc_keep_groups=NULL,gc_outbox_version=o.row_version
  WHERE stage_token=s.stage_token RETURNING*INTO s;
 INSERT INTO sessions.ingest_publication_attempts(attempt_token,schema_generation,stage_token,lease_token,lease_until,stage_version,outbox_version,
  org_id,workspace_id,user_id,origin_id,batch_id,digest,accepted_ordinals,revision_snapshot,head_snapshots,frontier)
 VALUES(v_attempt,p->>'generation',s.stage_token,v_lease,s.gc_lease_until,s.row_version,o.row_version,s.org_id,s.workspace_id,s.user_id,s.origin_id,
  s.batch_id,s.digest,v_accepted,v_snapshot,v_heads,to_jsonb(f)-'valid_until'||jsonb_build_object('valid_until_epoch',extract(epoch FROM f.valid_until)));
 PERFORM sessions.ingest_require_buffer_admission(s.org_id);
 IF s.gc_lease_until<=clock_timestamp() OR f.valid_until<=clock_timestamp()
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: admission expired';END IF;
 RETURN jsonb_build_object('schemaGeneration',p->>'generation','attemptToken',v_attempt,'leaseToken',v_lease,'leaseUntil',s.gc_lease_until,
  'stageToken',s.stage_token,'stageVersion',s.row_version::text,'outboxVersion',o.row_version::text,
  'tenant',jsonb_build_object('orgId',s.org_id,'workspaceId',s.workspace_id,'userId',s.user_id),
  'originId',s.origin_id,'batchId',s.batch_id,'batchDigest',s.digest,'claimToken',s.claim_token,
  'stage',jsonb_build_object('key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,'groupOrdinals',s.group_ordinals),
  'records',a.records,'acceptedOrdinals',v_accepted,'receiptSeats',v_seats,'heads',v_heads);
END $claim$;

CREATE FUNCTION sessions.ingest_publication_heartbeat(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE a sessions.ingest_publication_attempts%ROWTYPE;v_until timestamptz;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['attemptToken','leaseToken','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 a:=sessions.ingest_publication_locked((p->>'attemptToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');
 v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_stage_intents SET gc_lease_until=v_until WHERE stage_token=a.stage_token;
 UPDATE sessions.ingest_publication_attempts SET lease_until=v_until WHERE attempt_token=a.attempt_token;
 UPDATE sessions.ingest_publication_heads SET build_lease_until=v_until WHERE build_attempt_token=a.attempt_token;
 PERFORM sessions.ingest_require_buffer_admission(a.org_id);
 IF a.lease_until<=clock_timestamp() OR (a.frontier->>'valid_until_epoch')::numeric<=extract(epoch FROM clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: old heartbeat lease expired';END IF;
 RETURN jsonb_build_object('leaseUntil',v_until);
END $heartbeat$;


REVOKE ALL ON FUNCTION sessions.ingest_publication_component(text),sessions.ingest_publication_revision_snapshot(uuid),
 sessions.ingest_publication_locked(uuid,uuid,text),sessions.ingest_publication_candidates(jsonb),
 sessions.ingest_publication_claim(jsonb),sessions.ingest_publication_heartbeat(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_candidates(jsonb),sessions.ingest_publication_claim(jsonb),
 sessions.ingest_publication_heartbeat(jsonb) TO bulls96_session_ingest;


CREATE FUNCTION sessions.ingest_publication_scope(p jsonb,p_lock boolean) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $scope$
DECLARE v_org text;v_ws text;v_harness text;v_session text;v_machine text;v_key text;v_revision bigint;
 h sessions.ingest_publication_heads%ROWTYPE;v_reset bigint;v_fence bigint;v_current boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['orgId','workspaceId','harness','sessionId','machineId','manifestKey','revision','schemaGeneration']);
 PERFORM sessions.ingest_require_generation(p->>'schemaGeneration');
 v_org:=sessions.ingest_identifier(p->'orgId',false);v_ws:=sessions.ingest_identifier(p->'workspaceId',true);
 v_harness:=sessions.ingest_identifier(p->'harness',false);v_session:=sessions.ingest_identifier(p->'sessionId',false);
 v_machine:=sessions.ingest_identifier(p->'machineId',false);v_key:=p->>'manifestKey';
 IF jsonb_typeof(p->'manifestKey') IS DISTINCT FROM'string' OR octet_length(v_key) NOT BETWEEN 1 AND 2048
  OR jsonb_typeof(p->'revision') IS DISTINCT FROM'string' OR (p->>'revision')!~'^[1-9][0-9]{0,18}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: invalid pointer evidence';END IF;
 v_revision:=(p->>'revision')::bigint;
 IF p_lock THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: final writes require fresh snapshots';END IF;
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:publication-pointer',v_org,v_ws,v_harness,v_session,v_machine),0))
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer scope busy';END IF;
 END IF;
 SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=v_org AND workspace_id=v_ws
  AND physical_source=v_harness AND physical_session=v_session AND machine_id=v_machine;
 IF NOT FOUND THEN
  RETURN jsonb_build_object('managed',position('/upload-generations/'IN v_key)>0,
   'current',position('/upload-generations/'IN v_key)=0,'generation',NULL,'revision',v_revision::text);
 END IF;
 IF p_lock THEN
  PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id FOR UPDATE NOWAIT;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: user fence missing';END IF;
  SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=v_org AND workspace_id=v_ws
   AND physical_source=v_harness AND physical_session=v_session AND machine_id=v_machine FOR UPDATE NOWAIT;
 END IF;
 SELECT count(*) INTO v_reset FROM sessions.upload_resets WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id;
 SELECT coalesce(max(epoch),0) INTO v_fence FROM sessions.ingest_fence_versions WHERE org_id=h.org_id AND workspace_id=h.workspace_id
  AND user_id=h.user_id AND origin_id=h.origin_id AND source=h.source AND session_id=h.session_id;
 v_current:=h.state='live' AND h.current_key=v_key AND h.pointer_revision=v_revision
  AND h.reset_epoch=v_reset AND h.fence_epoch=v_fence
  AND EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE generation=h.generation AND state='live');
 RETURN jsonb_build_object('managed',true,'current',coalesce(v_current,false),'generation',h.generation,'revision',h.pointer_revision::text);
END $scope$;
CREATE FUNCTION sessions.session_publication_probe(p jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $probe$
 SELECT sessions.ingest_publication_scope(p,false);
$probe$;
CREATE FUNCTION sessions.session_publication_lock(p jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $lock$
 SELECT sessions.ingest_publication_scope(p,true);
$lock$;

-- A stale event cannot bypass the runtime gate through direct pointer SQL. The
-- per-pointer range lock covers first managed-head creation as well as existing
-- heads. All waits here are nonblocking; no user lock spans object/model I/O.
CREATE FUNCTION sessions.ingest_publication_guard_pointer() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $guard$
DECLARE r sessions.session_index%ROWTYPE;h sessions.ingest_publication_heads%ROWTYPE;v_reset bigint;v_fence bigint;
BEGIN
 IF TG_OP='DELETE' THEN r:=OLD;ELSE r:=NEW;END IF;
 IF r.machine_id NOT LIKE'srv\_%' ESCAPE'\' THEN
  IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer mutation requires fresh snapshots';END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:publication-pointer',r.org_id,r.workspace_id,r.harness,r.session_id,r.machine_id),0))
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer mutation busy';END IF;
 SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=r.org_id AND workspace_id=r.workspace_id
  AND physical_source=r.harness AND physical_session=r.session_id AND machine_id=r.machine_id;
 IF NOT FOUND THEN
  IF position('/upload-generations/'IN r.manifest_key)>0 THEN RETURN NULL;END IF;
  IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
 END IF;
 PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id FOR UPDATE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: user fence missing';END IF;
 SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=r.org_id AND workspace_id=r.workspace_id
  AND physical_source=r.harness AND physical_session=r.session_id AND machine_id=r.machine_id FOR UPDATE NOWAIT;
 IF h.state='retired' THEN
  IF TG_OP IN('UPDATE','DELETE') AND OLD.manifest_key=coalesce(h.current_key,h.base_pointer->>'manifest_key')
   AND (TG_OP='DELETE' OR NEW.deleted_at IS NOT NULL) THEN
   IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
  END IF;
  RETURN NULL;
 END IF;
 IF TG_OP='DELETE' OR h.state<>'live' OR r.deleted_at IS NOT NULL THEN RETURN NULL;END IF;
 SELECT count(*) INTO v_reset FROM sessions.upload_resets WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id;
 SELECT coalesce(max(epoch),0) INTO v_fence FROM sessions.ingest_fence_versions WHERE org_id=h.org_id AND workspace_id=h.workspace_id
  AND user_id=h.user_id AND origin_id=h.origin_id AND source=h.source AND session_id=h.session_id;
 IF r.user_id IS DISTINCT FROM h.user_id OR r.manifest_key IS DISTINCT FROM h.current_key
  OR r.revision<>h.pointer_revision OR encode(r.content_sha256,'hex') IS DISTINCT FROM h.current_raw_sha256
  OR h.reset_epoch<>v_reset OR h.fence_epoch<>v_fence
  OR NOT EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE generation=h.generation AND state='live')
 THEN RETURN NULL;END IF;
 RETURN NEW;
END $guard$;
CREATE TRIGGER ingest_publication_pointer_guard BEFORE INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_publication_guard_pointer();

CREATE FUNCTION sessions.ingest_publication_retire_fence() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $retire$
DECLARE h record;v_reset bigint;v_version boolean:=TG_TABLE_NAME='ingest_fence_versions';
BEGIN
 -- The existing selection/reset operation already owns this exact user lock.
 PERFORM sessions.ingest_lock_fences(NEW.org_id,NEW.workspace_id,NEW.user_id);
 SELECT count(*) INTO v_reset FROM sessions.upload_resets WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND user_id=NEW.user_id;
 FOR h IN SELECT*FROM sessions.ingest_publication_heads q WHERE q.org_id=NEW.org_id AND q.workspace_id=NEW.workspace_id AND q.user_id=NEW.user_id
  AND q.state<>'retired' AND
   ((v_version AND q.origin_id=to_jsonb(NEW)->>'origin_id' AND q.source=to_jsonb(NEW)->>'source' AND q.session_id=to_jsonb(NEW)->>'session_id'
      AND q.fence_epoch<>(to_jsonb(NEW)->>'epoch')::bigint) OR (NOT v_version AND q.reset_epoch<>v_reset))
  ORDER BY q.physical_source COLLATE"C",q.physical_session COLLATE"C",q.machine_id COLLATE"C" FOR UPDATE
 LOOP
  UPDATE sessions.ingest_publication_generations SET state='retired',retired_at=coalesce(retired_at,clock_timestamp()) WHERE generation=h.generation;
  UPDATE sessions.ingest_publication_heads SET state='retired',row_version=row_version+1,pointer_revision=pointer_revision+1,
   build_attempt_token=NULL,build_lease_until=NULL
   WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id AND origin_id=h.origin_id AND source=h.source AND session_id=h.session_id;
  UPDATE sessions.session_index SET deleted_at=coalesce(deleted_at,clock_timestamp()),indexed_at=clock_timestamp()
   WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND harness=h.physical_source AND session_id=h.physical_session AND machine_id=h.machine_id
    AND manifest_key=coalesce(h.current_key,h.base_pointer->>'manifest_key') AND deleted_at IS NULL;
  -- Clear derived text immediately. Surviving carriers may rebuild it through
  -- the normal complete-source projector; an erased generation may not.
  UPDATE sessions.session_directory d SET short_summary=NULL,
   deleted_at=CASE WHEN EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=d.org_id AND p.workspace_id=d.workspace_id
     AND p.harness=d.harness AND coalesce(p.canonical_session_id,p.session_id)=d.session_id AND p.deleted_at IS NULL) THEN d.deleted_at ELSE coalesce(d.deleted_at,clock_timestamp()) END
   WHERE d.org_id=h.org_id AND d.workspace_id=h.workspace_id AND d.harness=h.physical_source AND d.session_id IN(h.session_id,h.physical_session);
 END LOOP;
 RETURN NEW;
END $retire$;
CREATE TRIGGER ingest_publication_retire_version AFTER INSERT OR UPDATE ON sessions.ingest_fence_versions
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_publication_retire_fence();
CREATE TRIGGER ingest_publication_retire_reset AFTER INSERT ON sessions.upload_resets
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_publication_retire_fence();

CREATE FUNCTION sessions.session_publication_is_managed(p_org text,p_ws text,p_harness text,p_session text,p_machine text)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $managed$
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=p_org AND workspace_id=p_ws
  AND physical_source=p_harness AND physical_session=p_session AND machine_id=p_machine);
$managed$;
REVOKE ALL ON FUNCTION sessions.session_publication_is_managed(text,text,text,text,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_publication_is_managed(text,text,text,text,text) TO service_role;

-- Unqualified delayed classifiers have no authority over a managed image.
CREATE OR REPLACE FUNCTION sessions.set_session_state(
 p_org_id text,p_workspace_id text,p_harness text,p_session_id text,p_machine_id text,p_state text,p_classified_at timestamptz)
RETURNS boolean LANGUAGE sql SET search_path=pg_catalog,sessions,pg_temp AS $legacy_state$
 WITH updated AS(
  UPDATE sessions.session_index s SET state=p_state,state_classified_at=p_classified_at
  WHERE s.org_id=p_org_id AND s.workspace_id=p_workspace_id AND s.harness=p_harness AND s.session_id=p_session_id AND s.machine_id=p_machine_id
   AND(s.state_classified_at IS NULL OR s.state_classified_at<p_classified_at)
   AND NOT sessions.session_publication_is_managed(s.org_id,s.workspace_id,s.harness,s.session_id,s.machine_id)
  RETURNING 1)SELECT EXISTS(SELECT 1 FROM updated);
$legacy_state$;
CREATE FUNCTION sessions.set_session_state_for_publication(
 p_org text,p_ws text,p_harness text,p_session text,p_machine text,p_key text,p_revision bigint,p_state text,p_at timestamptz,p_generation text)
RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $qualified_state$
DECLARE v jsonb;n int;
BEGIN
 v:=sessions.ingest_publication_scope(jsonb_build_object('orgId',p_org,'workspaceId',p_ws,'harness',p_harness,'sessionId',p_session,
   'machineId',p_machine,'manifestKey',p_key,'revision',p_revision::text,'schemaGeneration',p_generation),true);
 IF (v->>'current')::boolean IS NOT TRUE THEN RETURN false;END IF;
 UPDATE sessions.session_index SET state=p_state,state_classified_at=p_at WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_harness
  AND session_id=p_session AND machine_id=p_machine AND manifest_key=p_key AND revision=p_revision
  AND(state_classified_at IS NULL OR state_classified_at<p_at);
 GET DIAGNOSTICS n=ROW_COUNT;RETURN n=1;
END $qualified_state$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scope(jsonb,boolean),sessions.ingest_publication_guard_pointer(),
 sessions.ingest_publication_retire_fence(),sessions.session_publication_probe(jsonb),sessions.session_publication_lock(jsonb),
 sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamptz,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_publication_probe(jsonb),sessions.session_publication_lock(jsonb),
 sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamptz,text) TO service_role;


CREATE FUNCTION sessions.ingest_publication_prepare(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $prepare$
DECLARE a sessions.ingest_publication_attempts%ROWTYPE;i jsonb;h jsonb;v_parts text[];v_key text;v_sha text;v_raw text;v_row jsonb;v_index int;
 existing sessions.ingest_publication_images%ROWTYPE;v_bytes int;v_started timestamptz;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['attemptToken','leaseToken','images','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 a:=sessions.ingest_publication_locked((p->>'attemptToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');
 IF jsonb_typeof(p->'images') IS DISTINCT FROM'array' OR jsonb_array_length(p->'images') NOT BETWEEN 1 AND 500 OR octet_length(p::text)>16777216
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: image plan bounds';END IF;
 FOR i IN SELECT value FROM jsonb_array_elements(p->'images') LOOP
  PERFORM sessions.ingest_exact_keys(i,ARRAY['groupIndex','manifestKey','manifestSha256','manifestBytes','rawSha256','startedAt','pointerRow']);
  v_index:=sessions.ingest_bounded(i->'groupIndex',0,499);
  SELECT value INTO h FROM jsonb_array_elements(a.head_snapshots) WHERE(value->>'groupIndex')::int=v_index;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: image outside admitted groups';END IF;
  v_key:=i->>'manifestKey';v_sha:=sessions.ingest_digest(i->'manifestSha256');v_raw:=sessions.ingest_digest(i->'rawSha256');
  v_bytes:=sessions.ingest_bounded(i->'manifestBytes',1,16777216);v_started:=(i->>'startedAt')::timestamptz;
  v_parts:=regexp_match(v_key,'^(.*)/v1/ws=([^/]+)/m=([^/]+)/h=([^/]+)/d=(\d{4}/\d{2}/\d{2})/s=([^/]+)/publications/([a-f0-9-]{36})/manifest\.json$');
  IF v_parts IS NULL OR v_parts[1] IS DISTINCT FROM h->>'prefix' OR v_parts[2] IS DISTINCT FROM h->>'physicalWorkspace'
   OR v_parts[3] IS DISTINCT FROM h->>'machineId' OR v_parts[4] IS DISTINCT FROM h->>'physicalSource'
   OR v_parts[5] IS DISTINCT FROM to_char(v_started AT TIME ZONE'UTC','YYYY/MM/DD')
   OR v_parts[6] IS DISTINCT FROM h->>'physicalSession' OR v_parts[7] IS DISTINCT FROM a.attempt_token::text
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: image namespace or attempt mismatch';END IF;
  v_row:=i->'pointerRow';
  PERFORM sessions.ingest_exact_keys(v_row,ARRAY['org_id','workspace_id','harness','session_id','machine_id','user_id','created_at','last_activity_at','event_count',
    'storage_provider','bucket','manifest_key','content_sha256','raw_bytes','chunk_count','raw_format','format_version','normalized_version','scrubbed',
    'manifest_etag','revision','trace_id','deleted_at']);
  IF v_row->>'org_id' IS DISTINCT FROM a.org_id OR v_row->>'workspace_id' IS DISTINCT FROM a.workspace_id
   OR v_row->>'user_id' IS DISTINCT FROM a.user_id OR v_row->>'harness' IS DISTINCT FROM h->>'physicalSource'
   OR v_row->>'session_id' IS DISTINCT FROM h->>'physicalSession' OR v_row->>'machine_id' IS DISTINCT FROM h->>'machineId'
   OR v_row->>'manifest_key' IS DISTINCT FROM v_key OR v_row->>'revision' IS DISTINCT FROM h->>'revision'
   OR v_row->>'content_sha256' IS DISTINCT FROM chr(92)||'x'||v_raw
   OR v_row->>'storage_provider' IS DISTINCT FROM's3' OR v_row->>'bucket' IS DISTINCT FROM a.frontier->>'target_bucket'
   OR v_row->>'raw_format' IS DISTINCT FROM'ndjson' OR v_row->>'scrubbed' IS DISTINCT FROM'true'
   OR v_row->>'deleted_at' IS NOT NULL OR v_row->>'manifest_etag' IS NOT NULL OR octet_length(v_row::text)>32768
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: pointer metadata differs from admitted image';END IF;
  -- Type/constraint casts are performed before any canonical pointer changes.
  PERFORM 1 FROM jsonb_populate_record(NULL::sessions.session_index,v_row)q;
  SELECT*INTO existing FROM sessions.ingest_publication_images WHERE attempt_token=a.attempt_token AND group_index=v_index FOR UPDATE;
  IF FOUND THEN
   IF ROW(existing.generation,existing.manifest_key,existing.manifest_sha256,existing.manifest_bytes,existing.raw_sha256,existing.pointer_revision,existing.pointer_row)
      IS DISTINCT FROM ROW((h->>'generation')::uuid,v_key,v_sha,v_bytes::bigint,v_raw,(h->>'revision')::bigint,v_row)
   THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: immutable image plan changed';END IF;
  ELSE
   INSERT INTO sessions.ingest_publication_images VALUES(a.attempt_token,v_index,(h->>'generation')::uuid,v_key,v_sha,v_bytes,v_raw,(h->>'revision')::bigint,v_row);
  END IF;
 END LOOP;
 UPDATE sessions.ingest_publication_attempts SET state='prepared' WHERE attempt_token=a.attempt_token;
 IF a.lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: prepare expired';END IF;
 RETURN jsonb_build_object('prepared',true);
END $prepare$;

CREATE FUNCTION sessions.ingest_publication_register_objects(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $objects$
DECLARE a sessions.ingest_publication_attempts%ROWTYPE;i jsonb;h jsonb;v_parts text[];v_key text;v_sha text;v_bytes int;v_index int;v_kind text;
 existing sessions.ingest_publication_objects%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['attemptToken','leaseToken','objects','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 a:=sessions.ingest_publication_locked((p->>'attemptToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');
 IF jsonb_typeof(p->'objects') IS DISTINCT FROM'array' OR jsonb_array_length(p->'objects') NOT BETWEEN 1 AND 128 OR octet_length(p::text)>1048576
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: object intent bounds';END IF;
 FOR i IN SELECT value FROM jsonb_array_elements(p->'objects') LOOP
  PERFORM sessions.ingest_exact_keys(i,ARRAY['groupIndex','key','sha256','bytes','kind']);
  v_index:=sessions.ingest_bounded(i->'groupIndex',0,499);v_key:=i->>'key';v_kind:=i->>'kind';
  v_sha:=sessions.ingest_digest(i->'sha256');v_bytes:=sessions.ingest_bounded(i->'bytes',1,16777216);
  IF jsonb_typeof(i->'key') IS DISTINCT FROM'string' OR octet_length(v_key) NOT BETWEEN 1 AND 2048
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: invalid object key';END IF;
  SELECT value INTO h FROM jsonb_array_elements(a.head_snapshots) WHERE(value->>'groupIndex')::int=v_index;
  IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE generation=(h->>'generation')::uuid AND state IN('building','live'))
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: object generation unavailable';END IF;
  IF v_kind='raw' THEN
   v_parts:=regexp_match(v_key,'^(.*)/v1/ws=([^/]+)/m=([^/]+)/h=([^/]+)/d=(\d{4}/\d{2}/\d{2})/s=([^/]+)/raw/\d{10}-\d+-[a-f0-9]{8}\.ndjson\.zst$');
   IF v_parts IS NULL OR v_parts[1] IS DISTINCT FROM h->>'prefix' OR v_parts[2] IS DISTINCT FROM h->>'physicalWorkspace'
    OR v_parts[3] IS DISTINCT FROM h->>'machineId' OR v_parts[4] IS DISTINCT FROM h->>'physicalSource' OR v_parts[6] IS DISTINCT FROM h->>'physicalSession'
   THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: raw intent namespace mismatch';END IF;
  ELSIF v_kind='manifest' THEN
   IF NOT EXISTS(SELECT 1 FROM sessions.ingest_publication_images x WHERE x.attempt_token=a.attempt_token AND x.group_index=v_index
       AND x.manifest_key=v_key AND x.manifest_sha256=v_sha AND x.manifest_bytes=v_bytes)
   THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: manifest intent differs from image plan';END IF;
  ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: object kind refused';END IF;
  SELECT*INTO existing FROM sessions.ingest_publication_objects WHERE object_key=v_key FOR UPDATE;
  IF FOUND THEN
   IF ROW(existing.generation,existing.sha256,existing.bytes,existing.kind,existing.cleanup_state)
    IS DISTINCT FROM ROW((h->>'generation')::uuid,v_sha,v_bytes::bigint,v_kind,'live'::text)
   THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: immutable object intent changed';END IF;
  ELSE
   INSERT INTO sessions.ingest_publication_objects(object_key,generation,sha256,bytes,kind)
    VALUES(v_key,(h->>'generation')::uuid,v_sha,v_bytes,v_kind);
  END IF;
 END LOOP;
 IF a.lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: object registration expired';END IF;
 RETURN jsonb_build_object('registered',true);
END $objects$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_prepare(jsonb),sessions.ingest_publication_register_objects(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_prepare(jsonb),sessions.ingest_publication_register_objects(jsonb) TO bulls96_session_ingest;


CREATE FUNCTION sessions.ingest_publication_publish(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE a sessions.ingest_publication_attempts%ROWTYPE;v jsonb;i sessions.ingest_publication_images%ROWTYPE;message jsonb;
 v_messages jsonb;v_message_sha text;v_expected int;v_written int;v_actual int;v_notice_count int;v_row jsonb;v_applied boolean;v_result record;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['attemptToken','leaseToken','messages','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_messages:=p->'messages';
 IF jsonb_typeof(v_messages) IS DISTINCT FROM'array' OR jsonb_array_length(v_messages)>500 OR octet_length(p::text)>16777216
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: notification bounds';END IF;
 v_message_sha:=encode(sha256(convert_to(v_messages::text,'UTF8')),'hex');
 SELECT*INTO a FROM sessions.ingest_publication_attempts WHERE attempt_token=(p->>'attemptToken')::uuid;
 IF NOT FOUND OR a.lease_token IS DISTINCT FROM(p->>'leaseToken')::uuid OR a.schema_generation IS DISTINCT FROM p->>'generation'
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: unknown attempt';END IF;
 IF a.state='published' THEN
  IF a.completion_sha256 IS DISTINCT FROM v_message_sha THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: completed request differs';END IF;
  RETURN jsonb_build_object('published',true,'replay',true,'images',jsonb_array_length(a.head_snapshots),
    'records',(SELECT count(DISTINCT x->>'record_id') FROM sessions.ingest_stage_intents s JOIN sessions.ingest_claim_attempts c ON c.claim_token=s.claim_token
      CROSS JOIN LATERAL jsonb_array_elements(c.records)x WHERE s.stage_token=a.stage_token AND a.accepted_ordinals@>to_jsonb((x->>'ord')::int)),
    'eligibleSessions',(SELECT count(*) FROM jsonb_array_elements(a.head_snapshots)h WHERE NOT(h->>'source'='_none' AND h->>'sessionId'='_origin')));
 END IF;
 -- Same lock as the existing complete canonical CH projector. Never wait
 -- for a remote read while holding a user fence or publication head lock.
 FOR v IN SELECT value FROM jsonb_array_elements(a.head_snapshots)
  WHERE NOT(value->>'source'='_none' AND value->>'sessionId'='_origin') ORDER BY value->>'physicalSource' COLLATE"C",value->>'sessionId' COLLATE"C"
 LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',a.org_id,a.workspace_id,v->>'physicalSource',v->>'sessionId'),0))
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: canonical projector busy';END IF;
 END LOOP;
 a:=sessions.ingest_publication_locked(a.attempt_token,a.lease_token,p->>'generation');
 IF sessions.ingest_publication_revision_snapshot(a.stage_token) IS DISTINCT FROM a.revision_snapshot
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: complete record maxima changed';END IF;
 SELECT count(*) INTO v_actual FROM sessions.ingest_publication_images WHERE attempt_token=a.attempt_token;
 IF v_actual<>jsonb_array_length(a.head_snapshots)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: complete image set not prepared';END IF;
 SELECT count(*) INTO v_notice_count FROM jsonb_array_elements(a.head_snapshots)h WHERE NOT(h->>'source'='_none' AND h->>'sessionId'='_origin');
 IF jsonb_array_length(v_messages)<>v_notice_count OR
  (SELECT count(DISTINCT m#>>'{payload,key}')FROM jsonb_array_elements(v_messages)m)<>v_notice_count
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: notification set differs';END IF;
 -- All registered manifests must match the immutable prepared byte proof. A
 -- trusted bounded caller invokes this only after exact immutable PUT ACKs.
 IF EXISTS(SELECT 1 FROM sessions.ingest_publication_images x
  LEFT JOIN sessions.ingest_publication_objects o ON o.object_key=x.manifest_key
  WHERE x.attempt_token=a.attempt_token AND(o.object_key IS NULL OR o.generation<>x.generation OR o.sha256<>x.manifest_sha256
    OR o.bytes<>x.manifest_bytes OR o.kind<>'manifest' OR o.cleanup_state<>'live'))
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: manifest intent proof missing';END IF;
 -- A competing record owner/newer revision can win a unique-key wait after the
 -- preceding snapshot. Require every admitted highest record to actually apply;
 -- failure rolls back pointers/jobs/authority together.
 WITH admitted AS(
  SELECT x FROM sessions.ingest_stage_intents s JOIN sessions.ingest_claim_attempts c ON c.claim_token=s.claim_token
   CROSS JOIN LATERAL jsonb_array_elements(c.records)x
  WHERE s.stage_token=a.stage_token AND a.accepted_ordinals@>to_jsonb((x->>'ord')::int)
 ), highest AS(
  SELECT DISTINCT ON((x->>'record_id') COLLATE"C")x FROM admitted ORDER BY (x->>'record_id') COLLATE"C",(x->>'revision')::bigint DESC,(x->>'ord')::int
 )
 INSERT INTO sessions.ingest_record_revisions AS t(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)
 SELECT a.org_id,a.workspace_id,a.origin_id,x->>'record_id',(x->>'revision')::bigint,x->>'revision_id',x->>'digest',a.user_id,x->>'source',x->>'session_id' FROM highest
 ON CONFLICT(org_id,workspace_id,origin_id,record_id) DO UPDATE SET revision=excluded.revision,revision_id=excluded.revision_id,digest=excluded.digest,
  user_id=excluded.user_id,source=excluded.source,session_id=excluded.session_id,updated_at=clock_timestamp()
 WHERE t.user_id=excluded.user_id AND excluded.revision>t.revision;
 GET DIAGNOSTICS v_written=ROW_COUNT;
 SELECT count(DISTINCT x->>'record_id') INTO v_expected
 FROM sessions.ingest_stage_intents s JOIN sessions.ingest_claim_attempts c ON c.claim_token=s.claim_token
 CROSS JOIN LATERAL jsonb_array_elements(c.records)x WHERE s.stage_token=a.stage_token AND a.accepted_ordinals@>to_jsonb((x->>'ord')::int);
 IF v_written<>v_expected THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: admitted record lost its revision race';END IF;
 FOR v IN SELECT value FROM jsonb_array_elements(a.head_snapshots)ORDER BY value->>'physicalSource' COLLATE"C",value->>'physicalSession' COLLATE"C" LOOP
  SELECT*INTO STRICT i FROM sessions.ingest_publication_images WHERE attempt_token=a.attempt_token AND group_index=(v->>'groupIndex')::int;
  IF NOT(v->>'source'='_none' AND v->>'sessionId'='_origin') THEN
   SELECT value INTO message FROM jsonb_array_elements(v_messages) WHERE value#>>'{payload,key}'=i.manifest_key;
   IF NOT FOUND OR message->>'v' IS DISTINCT FROM'1' OR message->>'stage' IS DISTINCT FROM'object-landed'
    OR message#>>'{ctx,orgId}' IS DISTINCT FROM a.org_id OR message#>>'{ctx,workspaceId}' IS DISTINCT FROM v->>'physicalWorkspace'
    OR message#>>'{ctx,harness}' IS DISTINCT FROM v->>'physicalSource' OR message#>>'{ctx,sessionId}' IS DISTINCT FROM v->>'physicalSession'
    OR message#>>'{ctx,machineId}' IS DISTINCT FROM v->>'machineId' OR message#>>'{ctx,traceId}' IS DISTINCT FROM lower(i.pointer_row->>'trace_id')
    OR message->>'revision' IS DISTINCT FROM i.pointer_revision::text OR message#>>'{payload,revision}' IS DISTINCT FROM i.pointer_revision::text
    OR message#>>'{payload,kind}' IS DISTINCT FROM'manifest' OR message#>>'{payload,deleted}' IS NOT NULL
    OR message#>>'{payload,provider}' IS DISTINCT FROM's3' OR message#>>'{payload,bucket}' IS DISTINCT FROM a.frontier->>'target_bucket'
    OR message#>>'{payload,scrubbed}' IS DISTINCT FROM'true'
    OR coalesce(message#>>'{payload,version}',message#>>'{payload,etag}','')=''
    OR octet_length(coalesce(message#>>'{payload,version}',message#>>'{payload,etag}'))>4096
    OR message->>'workKey' IS DISTINCT FROM i.manifest_key||'@'||coalesce(message#>>'{payload,version}',message#>>'{payload,etag}')
   THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: notification differs from current image';END IF;
  ELSE message:=NULL;END IF;
  UPDATE sessions.ingest_publication_generations SET state='live'WHERE generation=i.generation AND state IN('building','live');
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: generation retired';END IF;
  UPDATE sessions.ingest_publication_heads SET current_key=i.manifest_key,current_raw_sha256=i.raw_sha256,pointer_revision=i.pointer_revision,
   state='live',base_pointer=NULL,row_version=row_version+1,build_attempt_token=NULL,build_lease_until=NULL
   WHERE org_id=a.org_id AND workspace_id=a.workspace_id AND user_id=a.user_id AND origin_id=a.origin_id
    AND source=v->>'source' AND session_id=v->>'sessionId' AND generation=i.generation AND row_version=(v->>'headVersion')::bigint;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: head CAS lost';END IF;
  IF message IS NOT NULL THEN
   v_row:=jsonb_set(i.pointer_row,'{manifest_etag}',to_jsonb(coalesce(message#>>'{payload,etag}',message#>>'{payload,version}')));
   SELECT*INTO v_result FROM sessions.upsert_session_index(jsonb_build_array(v_row));
   IF v_result.applied IS NOT TRUE THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer CAS did not apply';END IF;
   UPDATE sessions.session_index SET canonical_session_id=v->>'sessionId',canonical_machine_id=v->>'machineId'
    WHERE org_id=a.org_id AND workspace_id=a.workspace_id AND harness=v->>'physicalSource' AND session_id=v->>'physicalSession'
     AND machine_id=v->>'machineId' AND manifest_key=i.manifest_key AND revision=i.pointer_revision;
   INSERT INTO sessions.pipeline_jobs(stage,work_key,trace_id,org_id,workspace_id,session_id,machine_id,message)
   VALUES('object-landed',message->>'workKey',(message#>>'{ctx,traceId}')::uuid,a.org_id,message#>>'{ctx,workspaceId}',
    message#>>'{ctx,sessionId}',message#>>'{ctx,machineId}',message);
  END IF;
 END LOOP;
 -- "erased" here is the existing buffer-outbox state: no active buffered copy.
 -- Canonical images already hold all admitted bytes;040510 cleans this retired
 -- source object via its persistent barrier, including ambiguous old writes.
 UPDATE sessions.ingest_buffer_outbox SET active_stage_token=NULL,state='erased',row_version=row_version+1
  WHERE org_id=a.org_id AND workspace_id=a.workspace_id AND origin_id=a.origin_id AND batch_id=a.batch_id
   AND active_stage_token=a.stage_token AND row_version=a.outbox_version;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: source outbox CAS lost';END IF;
 UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1,
  gc_lease_token=NULL,gc_lease_until=NULL,gc_lease_version=NULL,gc_keep_groups=NULL WHERE stage_token=a.stage_token;
 UPDATE sessions.ingest_publication_attempts SET state='published',published_at=clock_timestamp(),completion_sha256=v_message_sha WHERE attempt_token=a.attempt_token;
 PERFORM sessions.ingest_require_buffer_admission(a.org_id);
 IF a.lease_until<=clock_timestamp() OR (a.frontier->>'valid_until_epoch')::numeric<=extract(epoch FROM clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: final acknowledgement expired';END IF;
 RETURN jsonb_build_object('published',true,'replay',false,'images',jsonb_array_length(a.head_snapshots),'records',v_written,'eligibleSessions',v_notice_count);
END $publish$;

CREATE FUNCTION sessions.ingest_publication_abandon(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $abandon$
DECLARE a sessions.ingest_publication_attempts%ROWTYPE;s sessions.ingest_stage_intents%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['attemptToken','leaseToken','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 SELECT*INTO a FROM sessions.ingest_publication_attempts WHERE attempt_token=(p->>'attemptToken')::uuid;
 IF NOT FOUND OR a.lease_token IS DISTINCT FROM(p->>'leaseToken')::uuid OR a.schema_generation IS DISTINCT FROM p->>'generation' THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: unknown attempt';END IF;
 IF a.state='published' THEN RETURN jsonb_build_object('abandoned',false);END IF;
 s:=sessions.ingest_buffer_gc_locked(a.stage_token,a.lease_token);
 UPDATE sessions.ingest_publication_attempts SET state='rejected' WHERE attempt_token=a.attempt_token;
 UPDATE sessions.ingest_publication_generations g SET state='retired',retired_at=coalesce(retired_at,clock_timestamp())
  FROM sessions.ingest_publication_heads h WHERE h.generation=g.generation AND h.build_attempt_token=a.attempt_token
   AND h.state='building' AND h.current_key IS NULL AND g.state='building';
 UPDATE sessions.ingest_publication_heads SET build_attempt_token=NULL,build_lease_until=NULL WHERE build_attempt_token=a.attempt_token;
 UPDATE sessions.ingest_stage_intents SET gc_lease_token=NULL,gc_lease_until=NULL,gc_lease_version=NULL,gc_keep_groups=NULL
  WHERE stage_token=a.stage_token AND gc_lease_token=a.lease_token;
 RETURN jsonb_build_object('abandoned',true);
END $abandon$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_publish(jsonb),sessions.ingest_publication_abandon(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_publish(jsonb),sessions.ingest_publication_abandon(jsonb) TO bulls96_session_ingest;


CREATE FUNCTION sessions.session_publication_register_object(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $derived$
DECLARE authority jsonb;h sessions.ingest_publication_heads%ROWTYPE;v_key text;v_sha text;v_bytes int;v_prefix text;v_tail text;
 existing sessions.ingest_publication_objects%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['pointer','key','sha256','bytes']);
 authority:=sessions.ingest_publication_scope(p->'pointer',true);
 IF authority->>'managed' IS DISTINCT FROM'true' OR authority->>'current' IS DISTINCT FROM'true'
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: derived source no longer current';END IF;
 v_key:=p->>'key';v_sha:=sessions.ingest_digest(p->'sha256');v_bytes:=sessions.ingest_bounded(p->'bytes',1,16777216);
 v_prefix:=left(p#>>'{pointer,manifestKey}',length(p#>>'{pointer,manifestKey}')-length('manifest.json'))||'derived/';
 v_tail:=substr(v_key,length(v_prefix)+1);
 IF jsonb_typeof(p->'key') IS DISTINCT FROM'string' OR octet_length(v_key)>2048
  OR left(v_key,length(v_prefix))<>v_prefix OR v_tail!~'^[a-z][a-z0-9-]{0,63}/[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}\.bin$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: derived object namespace or hash differs';END IF;
 SELECT*INTO STRICT h FROM sessions.ingest_publication_heads WHERE generation=(authority->>'generation')::uuid;
 SELECT*INTO existing FROM sessions.ingest_publication_objects WHERE object_key=v_key FOR UPDATE;
 IF FOUND THEN
  IF ROW(existing.generation,existing.sha256,existing.bytes,existing.kind,existing.cleanup_state)
   IS DISTINCT FROM ROW(h.generation,v_sha,v_bytes::bigint,'derived'::text,'live'::text)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: derived intent differs';END IF;
 ELSE
  INSERT INTO sessions.ingest_publication_objects(object_key,generation,sha256,bytes,kind)VALUES(v_key,h.generation,v_sha,v_bytes,'derived');
 END IF;
 RETURN jsonb_build_object('registered',true,'generation',h.generation);
END $derived$;
REVOKE ALL ON FUNCTION sessions.session_publication_register_object(jsonb) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_publication_register_object(jsonb) TO service_role;


-- Whole retired generations only: live snapshots may share immutable raw keys.
-- Expired unpublished builders are retired before their first object can be GC'd.
CREATE FUNCTION sessions.ingest_publication_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $gc_claim$
DECLARE r record;h sessions.ingest_publication_heads%ROWTYPE;o sessions.ingest_publication_objects%ROWTYPE;
 v_limit int;v_token uuid;v_tickets jsonb:='[]';v_count int:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','bucket','region','keyRoot','limit']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_limit:=sessions.ingest_bounded(p->'limit',1,100);
 -- Bounded abandoned-builder maintenance. USER -> HEAD, same as admission.
 FOR r IN SELECT q.* FROM sessions.ingest_publication_heads q
  JOIN sessions.ingest_publication_generations g ON g.generation=q.generation
  WHERE q.state='building' AND q.current_key IS NULL AND g.state='building'
   AND (q.build_lease_until IS NULL OR q.build_lease_until<=clock_timestamp())
   AND g.target_bucket=p->>'bucket' AND g.target_region=p->>'region' AND g.key_root=p->>'keyRoot'
  ORDER BY g.created_at,g.generation LIMIT v_limit LOOP
  PERFORM sessions.ingest_lock_fences(r.org_id,r.workspace_id,r.user_id);
  SELECT*INTO h FROM sessions.ingest_publication_heads q WHERE q.org_id=r.org_id AND q.workspace_id=r.workspace_id
   AND q.user_id=r.user_id AND q.origin_id=r.origin_id AND q.source=r.source AND q.session_id=r.session_id FOR UPDATE SKIP LOCKED;
  IF NOT FOUND OR h.generation<>r.generation OR h.state<>'building' OR h.current_key IS NOT NULL
   OR h.build_lease_until>clock_timestamp() THEN CONTINUE;END IF;
  UPDATE sessions.ingest_publication_generations SET state='retired',retired_at=coalesce(retired_at,clock_timestamp())
   WHERE generation=h.generation AND state='building';
  UPDATE sessions.ingest_publication_heads SET row_version=row_version+1,build_attempt_token=NULL,build_lease_until=NULL
   WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id AND origin_id=h.origin_id
    AND source=h.source AND session_id=h.session_id;
 END LOOP;
 FOR r IN SELECT q.object_key,g.org_id,g.workspace_id,g.user_id FROM sessions.ingest_publication_objects q
  JOIN sessions.ingest_publication_generations g ON g.generation=q.generation
  WHERE g.state='retired' AND q.cleanup_state<>'redacted' AND (q.lease_until IS NULL OR q.lease_until<=clock_timestamp())
   AND g.target_bucket=p->>'bucket' AND g.target_region=p->>'region' AND g.key_root=p->>'keyRoot'
  ORDER BY g.retired_at,q.object_key COLLATE"C" LIMIT v_limit LOOP
  PERFORM sessions.ingest_lock_fences(r.org_id,r.workspace_id,r.user_id);
  SELECT*INTO o FROM sessions.ingest_publication_objects WHERE object_key=r.object_key FOR UPDATE SKIP LOCKED;
  IF NOT FOUND OR o.cleanup_state='redacted' OR o.lease_until>clock_timestamp()
   OR NOT EXISTS(SELECT 1 FROM sessions.ingest_publication_generations g WHERE g.generation=o.generation AND g.state='retired'
    AND g.target_bucket=p->>'bucket' AND g.target_region=p->>'region' AND g.key_root=p->>'keyRoot')
   OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE generation=o.generation AND state='live') THEN CONTINUE;END IF;
  v_token:=gen_random_uuid();
  UPDATE sessions.ingest_publication_objects SET cleanup_state='retired',row_version=row_version+1,lease_token=v_token,
   lease_until=clock_timestamp()+interval'5 minutes',gc_schema_generation=p->>'generation' WHERE object_key=o.object_key RETURNING*INTO o;
  v_tickets:=v_tickets||jsonb_build_array(jsonb_build_object('key',o.object_key,'sha256',o.sha256,'bytes',o.bytes,'kind',o.kind,
   'uploadGeneration',o.generation,'leaseToken',o.lease_token,'rowVersion',o.row_version::text,'schemaGeneration',o.gc_schema_generation,
   'bucket',p->>'bucket','region',p->>'region','keyRoot',p->>'keyRoot'));
  v_count:=v_count+1;
 END LOOP;
 RETURN jsonb_build_object('tickets',v_tickets);
END $gc_claim$;

CREATE FUNCTION sessions.ingest_publication_gc_finish(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $gc_finish$
DECLARE o sessions.ingest_publication_objects%ROWTYPE;g sessions.ingest_publication_generations%ROWTYPE;v_version text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','key','leaseToken','rowVersion','barrierVersionId']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_version:=p->>'barrierVersionId';
 IF jsonb_typeof(p->'barrierVersionId') IS DISTINCT FROM'string' OR octet_length(v_version) NOT BETWEEN 1 AND 1024 OR v_version='null'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: invalid barrier version';END IF;
 SELECT gg.* INTO g FROM sessions.ingest_publication_generations gg JOIN sessions.ingest_publication_objects q ON q.generation=gg.generation WHERE q.object_key=p->>'key';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: GC object absent';END IF;
 PERFORM sessions.ingest_lock_fences(g.org_id,g.workspace_id,g.user_id);
 SELECT*INTO STRICT o FROM sessions.ingest_publication_objects WHERE object_key=p->>'key' FOR UPDATE;
 IF o.lease_token IS DISTINCT FROM(p->>'leaseToken')::uuid OR o.row_version IS DISTINCT FROM(p->>'rowVersion')::bigint
  OR o.gc_schema_generation IS DISTINCT FROM p->>'generation'
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: GC lease changed';END IF;
 IF o.cleanup_state='redacted' AND o.barrier_version_id=v_version THEN RETURN jsonb_build_object('finished',true);END IF;
 IF o.cleanup_state<>'retired' OR o.lease_until<=clock_timestamp() OR o.lease_until IS NULL
  OR NOT EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE generation=o.generation AND state='retired')
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE generation=o.generation AND state='live')
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: GC authority changed';END IF;
 UPDATE sessions.ingest_publication_objects SET cleanup_state='redacted',barrier_version_id=v_version WHERE object_key=o.object_key;
 IF o.lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: GC lease expired';END IF;
 RETURN jsonb_build_object('finished',true);
END $gc_finish$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_gc_claim(jsonb),sessions.ingest_publication_gc_finish(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_gc_claim(jsonb),sessions.ingest_publication_gc_finish(jsonb) TO bulls96_session_ingest;

-- Staged receipts never authorize the retired direct/fanout APIs.
CREATE OR REPLACE FUNCTION sessions.ingest_fanout_fence(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $ffence$
DECLARE v_org text;v_ws text;v_user text;v_origin text;v_epochs jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
    USING(org_id,workspace_id,origin_id,batch_id)
    WHERE a.claim_token::text=p->>'claimToken' AND (b.publication_state='buffered' OR b.buffer_receipt_kind='staged')) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered publication refused';
  END IF;

 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','batchDigest','claimToken']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_origin:=sessions.ingest_identifier(p->'originId',false);
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 v_epochs:=sessions.ingest_attempt_epochs(v_org,v_ws,v_user,v_origin,
   NULL,sessions.ingest_digest(p->'batchDigest'),p->>'claimToken');
 RETURN jsonb_build_object('deleted',sessions.ingest_attempt_fenced(v_org,v_ws,v_user,v_origin,v_epochs,p->'sessions'));
END
$ffence$;

-- Staged receipts never authorize the retired direct/fanout APIs.
CREATE OR REPLACE FUNCTION sessions.ingest_commit_revisions(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $crev$
DECLARE
  t jsonb;
  v_org text; v_ws text; v_user text; v_origin text; v_machine text;
  v_items jsonb;
  v_committed integer;
  v_rearmed integer;
  v_epochs jsonb;
  v_fenced jsonb;
  v_pairs jsonb;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
    USING(org_id,workspace_id,origin_id,batch_id)
    WHERE a.claim_token::text=p->>'claimToken' AND (b.publication_state='buffered' OR b.buffer_receipt_kind='staged')) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered publication refused';
  END IF;

  PERFORM sessions.ingest_exact_keys(p, ARRAY['tenant', 'originId', 'machineId', 'records','batchDigest','claimToken']);
  t := p -> 'tenant';
  PERFORM sessions.ingest_exact_keys(t, ARRAY['orgId', 'workspaceId', 'userId']);
  v_org := sessions.ingest_identifier(t -> 'orgId', false);
  v_ws := sessions.ingest_identifier(t -> 'workspaceId', true);
  v_user := sessions.ingest_identifier(t -> 'userId', false);
  v_origin := sessions.ingest_identifier(p -> 'originId', false);
  IF jsonb_typeof(p -> 'machineId') IS DISTINCT FROM 'string' OR (p ->> 'machineId') !~ '^srv_[0-9a-f]{32}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: machineId is malformed';
  END IF;
  v_machine := p ->> 'machineId';
  v_items := sessions.ingest_records(p -> 'records');
  PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
  v_epochs:=sessions.ingest_attempt_epochs(v_org,v_ws,v_user,v_origin,
    NULL,sessions.ingest_digest(p->'batchDigest'),p->>'claimToken');
  v_pairs:=(SELECT coalesce(jsonb_agg(jsonb_build_array(x.source,x.session_id)),'[]'::jsonb)
    FROM (SELECT DISTINCT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
      WHERE i.session_id IS NOT NULL) AS x);
  v_fenced:=sessions.ingest_attempt_fenced(v_org,v_ws,v_user,v_origin,v_epochs,v_pairs);
  WITH items AS (
    SELECT * FROM jsonb_to_recordset(v_items) AS i(ord integer, source text, session_id text, record_id text,
                                                    revision_id text, revision bigint, digest text)
  ), fenced AS (
    SELECT x.pair->>0 AS source,x.pair->>1 AS session_id FROM jsonb_array_elements(v_fenced) AS x(pair)
  ), rearm AS (
    INSERT INTO sessions.upload_erasure_rearm (org_id, workspace_id, user_id, origin_id, machine_id, source, session_id)
    SELECT v_org, v_ws, v_user, v_origin, v_machine, f.source, f.session_id FROM fenced AS f
    ON CONFLICT DO NOTHING
  ), upserted AS (
    INSERT INTO sessions.ingest_record_revisions AS r
           (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id)
    SELECT DISTINCT ON (i.record_id) v_org, v_ws, v_origin, i.record_id, i.revision, i.revision_id, i.digest,
           v_user, i.source, i.session_id
      FROM items AS i
     WHERE NOT EXISTS (SELECT 1 FROM fenced AS f WHERE f.source = i.source AND f.session_id = i.session_id)
     ORDER BY i.record_id, i.revision DESC, i.revision_id, i.ord
    ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE
       SET revision = EXCLUDED.revision, revision_id = EXCLUDED.revision_id, digest = EXCLUDED.digest,
           user_id = EXCLUDED.user_id, source = EXCLUDED.source, session_id = EXCLUDED.session_id,
           updated_at = now()
     WHERE EXCLUDED.revision > r.revision
    RETURNING 1
  )
  SELECT (SELECT count(*)::integer FROM upserted), (SELECT count(*)::integer FROM fenced)
    INTO v_committed, v_rearmed;
  RETURN jsonb_build_object('committed', v_committed, 'rearmed', v_rearmed);
END
$crev$;

-- Staged receipts never authorize the retired direct/fanout APIs.
CREATE OR REPLACE FUNCTION sessions.ingest_commit_batch(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $commit$
DECLARE
  k record;
  v_machine text;
  v_items jsonb;
  v_claimed_at timestamptz;
  v_rearmed integer;
  v_epochs jsonb;
  v_fenced jsonb;
  v_pairs jsonb;
  v_invalidated boolean;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
    USING(org_id,workspace_id,origin_id,batch_id)
    WHERE a.claim_token::text=p->>'claimToken' AND (b.publication_state='buffered' OR b.buffer_receipt_kind='staged')) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered publication refused';
  END IF;

  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['tenant', 'originId', 'batchId', 'batchDigest', 'sessions', 'records', 'machineId', 'claimToken']);
  k := sessions.ingest_batch_key(p);
  IF jsonb_typeof(p -> 'machineId') IS DISTINCT FROM 'string'
     OR (p ->> 'machineId') !~ '^srv_[0-9a-f]{32}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: machineId is malformed';
  END IF;
  v_machine := p ->> 'machineId';
  v_items := sessions.ingest_records(p -> 'records');
  PERFORM sessions.ingest_session_pairs(p -> 'sessions');

  PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);

  v_epochs:=sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
  v_pairs:=(SELECT coalesce(jsonb_agg(jsonb_build_array(x.source,x.session_id)),'[]'::jsonb)
    FROM (SELECT q.source,q.session_id FROM sessions.ingest_session_pairs(p->'sessions') AS q
          UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
            WHERE i.session_id IS NOT NULL) AS x);
  v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,v_epochs,v_pairs);
  SELECT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) AS x(pair)
    WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions AS d
      WHERE d.org_id=k.org_id AND d.workspace_id=k.workspace_id AND d.user_id=k.user_id
        AND d.origin_id=k.origin_id AND d.source=x.pair->>0 AND d.session_id=x.pair->>1))
    INTO v_invalidated;
  SELECT b.claimed_at INTO v_claimed_at FROM sessions.ingest_batches AS b
   WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id
     AND b.batch_id=k.batch_id AND b.digest=k.digest AND b.user_id=k.user_id FOR UPDATE;
  IF NOT v_invalidated THEN
    UPDATE sessions.ingest_batches AS b SET state='committed',committed_at=coalesce(b.committed_at,now())
     WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id
       AND b.batch_id=k.batch_id AND b.digest=k.digest AND b.user_id=k.user_id;
  END IF;

  -- The written revisions become stored: per record its highest (first in identity
  -- order), inserted, or replacing a lower one only (#250: an equal revision keeps
  -- the stored row). Rows are touched in record_id order, so two commits sharing
  -- records lock them in the same order. Idempotent.
  INSERT INTO sessions.ingest_record_revisions AS t
         (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id)
  SELECT DISTINCT ON (i.record_id) k.org_id, k.workspace_id, k.origin_id,
         i.record_id, i.revision, i.revision_id, i.digest, k.user_id, i.source, i.session_id
    FROM jsonb_to_recordset(v_items) AS i(ord integer, source text, session_id text, record_id text,
                                          revision_id text, revision bigint, digest text)
   WHERE i.session_id IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) AS f(pair)
     WHERE f.pair->>0=i.source AND f.pair->>1=i.session_id)
   ORDER BY i.record_id, i.revision DESC, i.revision_id, i.ord
  ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE
     SET revision = EXCLUDED.revision, revision_id = EXCLUDED.revision_id,
         digest = EXCLUDED.digest, user_id = EXCLUDED.user_id, source = EXCLUDED.source,
         session_id = EXCLUDED.session_id, updated_at = now()
   WHERE EXCLUDED.revision > t.revision;

  -- The fence again: a tombstone, or a reset since the claim, that this batch's
  -- objects may have outrun. Those sessions are swept again.
  WITH reached AS (
    SELECT x.pair->>0 AS source,x.pair->>1 AS session_id FROM jsonb_array_elements(v_fenced) AS x(pair)
  ), queued AS (
    INSERT INTO sessions.upload_erasure_rearm
           (org_id, workspace_id, user_id, origin_id, machine_id, source, session_id)
    SELECT k.org_id, k.workspace_id, k.user_id, k.origin_id, v_machine, r.source, r.session_id
      FROM reached AS r
    ON CONFLICT DO NOTHING
  )
  SELECT count(*)::integer INTO v_rearmed FROM reached;
  RETURN jsonb_build_object('rearmed', v_rearmed,'invalidated',v_invalidated);
END
$commit$;

-- Bounded fairness only: neither a receipt nor any record's stored authority is
-- changed by refusal. Unknown policy cases remain durable and operator-visible.
CREATE FUNCTION sessions.ingest_publication_defer(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $defer$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['stageToken','code','generation']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF p->>'code' NOT IN('40001','55P03','55000','57014','40P01') OR jsonb_typeof(p->'code') IS DISTINCT FROM'string'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: invalid refusal category';END IF;
 SELECT*INTO s FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 IF NOT FOUND THEN RETURN jsonb_build_object('deferred',false);END IF;
 PERFORM sessions.ingest_lock_fences(s.org_id,s.workspace_id,s.user_id);
 PERFORM 1 FROM sessions.ingest_batches WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
 SELECT*INTO s FROM sessions.ingest_stage_intents WHERE stage_token=s.stage_token FOR UPDATE;
 IF s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged' OR NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.active_stage_token=s.stage_token AND o.state='buffered')
 THEN RETURN jsonb_build_object('deferred',false);END IF;
 INSERT INTO sessions.ingest_publication_deferrals(stage_token,retry_after,reason) VALUES(s.stage_token,clock_timestamp()+interval'30 seconds',p->>'code')
 ON CONFLICT(stage_token)DO UPDATE SET retry_after=excluded.retry_after,reason=excluded.reason,attempts=sessions.ingest_publication_deferrals.attempts+1;
 RETURN jsonb_build_object('deferred',true);
END $defer$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_defer(jsonb) FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_defer(jsonb) TO bulls96_session_ingest;
