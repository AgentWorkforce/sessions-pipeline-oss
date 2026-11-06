-- Registry v2 name; upstream provenance: 20261104061000_canonical_feature_journal.sql (#230 draft, never applied under that name).
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
-- 610 canonical feature journal + owner-only invoker, composed with the 620 final install CAS
-- (supporter-cloud's proposal on feat/canonical-feature-producer @ 4021f10: same 5-argument
-- retire-then-publish contract). Metadata only: no payload, no transcript bytes.
-- Contract (migration-completeness's review of #230):
--  * the ONLY live writer is sessions.ingest_history_scope_install (same owner, SECURITY DEFINER),
--    inside its transaction after every lease/source/pointer/current-pointer/retirement fence; a
--    rollback or refusal emits zero rows;
--  * a new scope appends publish(new image, new authority, new pointer-set descriptor);
--  * a replacement appends retire(PRIOR image, prior authority, PRIOR descriptor) THEN
--    publish(NEW image, new authority, NEW descriptor) as consecutive revisions in one transaction;
--  * a pointer-set descriptor is bounded: {authorityId, hash, count}, never the raw pointers. It
--    must equal the frozen inventory of that authority in ingest_history_scope_versions; the
--    producer pages the exact keys from that frozen authority at produce time;
--  * one global allocator under one transaction advisory lock, strictly monotonic, starting above
--    the drained legacy feature clock: nothing is appended until the operator seed row exists;
--  * every image key is immutable and distinguishable: scope digest + generation + authority id.
-- Changes from the draft: advisory-lock allocator (MAX ... FOR UPDATE is not valid with an
-- aggregate), no org:session image key (neither parenthesized-safe nor immutable), the ingest
-- generation as schema_generation, descriptors verified against the frozen authority inventory.
-- Retention-ready: org_id + recorded_at.
CREATE TABLE sessions.canonical_feature_journal(
 revision bigint PRIMARY KEY CHECK(revision>=0),
 kind text NOT NULL CHECK(kind IN('seed','publish','retire')),
 org_id text NOT NULL,
 workspace_id text NOT NULL,
 scope jsonb NOT NULL,
 schema_generation text NOT NULL CHECK(schema_generation~'^[a-f0-9]{64}$'),
 image_key text NOT NULL,
 source_hash text NOT NULL CHECK(source_hash~'^[a-f0-9]{64}$'),
 prior_keyset_hash text CHECK(prior_keyset_hash~'^[a-f0-9]{64}$'),
 source_authority jsonb NOT NULL,
 prior_source_authority jsonb,
 pointer_set jsonb NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(kind='seed'OR image_key~'^hs1:[a-f0-9]{64}:[a-f0-9]{64}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
 CHECK(kind<>'retire'OR(prior_keyset_hash IS NOT NULL AND prior_source_authority IS NULL)),
 CHECK(kind<>'publish'OR(prior_source_authority IS NULL)=(prior_keyset_hash IS NULL))
);
CREATE UNIQUE INDEX canonical_feature_journal_one_seed ON sessions.canonical_feature_journal((true))WHERE kind='seed';
CREATE UNIQUE INDEX canonical_feature_journal_image_kind ON sessions.canonical_feature_journal(image_key,kind)WHERE kind<>'seed';
CREATE INDEX canonical_feature_journal_kind_rev ON sessions.canonical_feature_journal(kind,revision);
CREATE INDEX canonical_feature_journal_org_time ON sessions.canonical_feature_journal(org_id,recorded_at);
ALTER TABLE sessions.canonical_feature_journal ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.canonical_feature_journal FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Least-privilege producer read (bootstrap session_ingest_roles.sql creates the role NOLOGIN):
-- SELECT only, through an explicit RLS policy; it validates every row it drains.
GRANT USAGE ON SCHEMA sessions TO bulls96_canonical_feature_producer;
GRANT SELECT ON sessions.canonical_feature_journal TO bulls96_canonical_feature_producer;
CREATE POLICY canonical_feature_journal_producer_read ON sessions.canonical_feature_journal FOR SELECT TO bulls96_canonical_feature_producer USING(true);

-- Bounded descriptor shape; its binding to the frozen authority is checked by the invoker.
CREATE FUNCTION sessions.canonical_feature_pointer_set_valid(ps jsonb)RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path='' AS $ps$
 SELECT coalesce(ps IS NOT NULL AND jsonb_typeof(ps)='object'
  AND(SELECT array_agg(k ORDER BY k)FROM jsonb_object_keys(ps)k)=ARRAY['authorityId','count','hash']
  AND jsonb_typeof(ps->'authorityId')='string'AND ps->>'authorityId'~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND jsonb_typeof(ps->'hash')='string'AND ps->>'hash'~'^[a-f0-9]{64}$'
  AND jsonb_typeof(ps->'count')='string'AND ps->>'count'~'^(0|[1-9][0-9]{0,5})$',false)
$ps$;
CREATE FUNCTION sessions.canonical_feature_source_valid(src jsonb)RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path='' AS $src$
 SELECT coalesce(src IS NOT NULL AND jsonb_typeof(src)='object'
  AND jsonb_typeof(src->'authorityId')='string'AND src->>'authorityId'~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND jsonb_typeof(src->'epoch')='string'AND src->>'epoch'~'^[1-9][0-9]{0,18}$'
  AND jsonb_typeof(src->'fingerprint')='string'AND src->>'fingerprint'~'^[a-f0-9]{64}$',false)
$src$;
-- Immutable image identity: digest(tenant/scope) : generation : authority id.
CREATE FUNCTION sessions.canonical_feature_image_key(p_scope jsonb,p_authority text)RETURNS text
LANGUAGE sql IMMUTABLE SET search_path='' AS $img$
 SELECT 'hs1:'||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.concat_ws(pg_catalog.chr(31),'history-scope-v1',
  p_scope->>'orgId',p_scope->>'workspaceId',p_scope->>'harness',p_scope->>'sessionId'),'UTF8')),'hex')||':'||(p_scope->>'generation')||':'||p_authority
$img$;

CREATE FUNCTION sessions.canonical_feature_changed(p_scope jsonb,p_source jsonb,p_prior_source jsonb,p_pointer_set jsonb,p_prior_pointer_set jsonb)
RETURNS bigint LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path='' AS $feature$
DECLARE v_revision bigint;v_seeded boolean;v_prior_image text;
BEGIN
 IF p_scope IS NULL OR jsonb_typeof(p_scope)IS DISTINCT FROM'object'
  OR jsonb_typeof(p_scope->'orgId')IS DISTINCT FROM'string'OR jsonb_typeof(p_scope->'workspaceId')IS DISTINCT FROM'string'
  OR jsonb_typeof(p_scope->'harness')IS DISTINCT FROM'string'OR jsonb_typeof(p_scope->'sessionId')IS DISTINCT FROM'string'
  OR jsonb_typeof(p_scope->'generation')IS DISTINCT FROM'string'OR p_scope->>'generation'!~'^[a-f0-9]{64}$'
  OR NOT sessions.canonical_feature_source_valid(p_source)OR NOT sessions.canonical_feature_pointer_set_valid(p_pointer_set)
  OR(p_prior_source IS NULL)<>(p_prior_pointer_set IS NULL)
  OR(p_prior_source IS NOT NULL AND(NOT sessions.canonical_feature_source_valid(p_prior_source)
   OR NOT sessions.canonical_feature_pointer_set_valid(p_prior_pointer_set)OR p_prior_source->>'authorityId'=p_source->>'authorityId'
   OR p_prior_pointer_set->>'authorityId'IS DISTINCT FROM p_prior_source->>'authorityId'
   OR p_prior_source->>'fingerprint'=p_source->>'fingerprint'))
  OR p_pointer_set->>'authorityId'IS DISTINCT FROM p_source->>'authorityId'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='canonical_feature_changed: invalid input';END IF;
 -- Each descriptor must be exactly the frozen inventory of its authority, in this scope.
 IF EXISTS(SELECT 1 FROM(VALUES(p_source,p_pointer_set),(p_prior_source,p_prior_pointer_set))d(src,ps)WHERE src IS NOT NULL AND NOT EXISTS(
   SELECT 1 FROM sessions.ingest_history_scope_versions v WHERE v.authority_id=(ps->>'authorityId')::uuid
    AND v.org_id=p_scope->>'orgId'AND v.workspace_id=p_scope->>'workspaceId'AND v.harness=p_scope->>'harness'AND v.session_id=p_scope->>'sessionId'
    AND v.source_epoch::text=src->>'epoch'AND v.source_fingerprint=src->>'fingerprint'
    AND v.pointer_snapshot_sha256=ps->>'hash'AND v.pointer_count::text=ps->>'count'))
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='canonical_feature_changed: descriptor does not match the frozen authority';END IF;
 -- One global allocator: strictly increasing, never reused; a pair is consecutive.
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('96bulls:canonical-feature-journal',0));
 SELECT max(revision),bool_or(kind='seed')INTO v_revision,v_seeded FROM sessions.canonical_feature_journal;
 IF v_seeded IS NOT TRUE THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical_feature_changed: journal not seeded above the legacy feature clock';END IF;
 IF p_prior_source IS NOT NULL THEN
  v_prior_image:=sessions.canonical_feature_image_key(p_scope,p_prior_source->>'authorityId');
  v_revision:=v_revision+1;
  INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,
   prior_keyset_hash,source_authority,prior_source_authority,pointer_set)
  VALUES(v_revision,'retire',p_scope->>'orgId',p_scope->>'workspaceId',p_scope,p_scope->>'generation',v_prior_image,
   p_prior_source->>'fingerprint',p_prior_pointer_set->>'hash',p_prior_source,NULL,p_prior_pointer_set);
 END IF;
 v_revision:=v_revision+1;
 INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,
  prior_keyset_hash,source_authority,prior_source_authority,pointer_set)
 VALUES(v_revision,'publish',p_scope->>'orgId',p_scope->>'workspaceId',p_scope,p_scope->>'generation',
  sessions.canonical_feature_image_key(p_scope,p_source->>'authorityId'),p_source->>'fingerprint',
  p_prior_pointer_set->>'hash',p_source,p_prior_source,p_pointer_set);
 RETURN v_revision;
END $feature$;

-- Operator seed (owner only, once, before activation): records the drained legacy feature clock H
-- so the managed counter starts at H+1 and never overlaps a legacy-issued revision.
CREATE FUNCTION sessions.canonical_feature_journal_seed(p_legacy_high bigint,p_generation text)RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path='' AS $seed$
BEGIN
 IF p_legacy_high IS NULL OR p_legacy_high<0 OR p_generation IS NULL OR p_generation!~'^[a-f0-9]{64}$'THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='canonical_feature_journal_seed: invalid input';END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('96bulls:canonical-feature-journal',0));
 IF EXISTS(SELECT 1 FROM sessions.canonical_feature_journal)THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical_feature_journal_seed: journal already seeded or in use';END IF;
 INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,
  source_authority,pointer_set)
 VALUES(p_legacy_high,'seed','','','{}'::jsonb,p_generation,'seed',pg_catalog.repeat('0',64),
  pg_catalog.jsonb_build_object('legacyHigh',p_legacy_high::text),'{}'::jsonb);
 RETURN p_legacy_high;
END $seed$;

-- OWNER-ONLY: no role receives EXECUTE; the 620 install (same owner, SECURITY DEFINER) calls the
-- invoker, and the operator runs the seed as the owner.
REVOKE ALL ON FUNCTION sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb),sessions.canonical_feature_journal_seed(bigint,text),
 sessions.canonical_feature_pointer_set_valid(jsonb),sessions.canonical_feature_source_valid(jsonb),sessions.canonical_feature_image_key(jsonb,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
