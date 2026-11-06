-- Buffer-only acceptance. No canonical publication or queue TTL is ACK proof.
-- Original record metadata stays in SQL; transcript payload stays scrubbed in S3.
ALTER TABLE sessions.ingest_batches
 ADD COLUMN publication_state text NOT NULL DEFAULT 'canonical' CHECK(publication_state IN('buffered','canonical')),
 ADD COLUMN buffer_receipt_kind text CHECK(buffer_receipt_kind IN('staged','suppressed'));
ALTER TABLE sessions.ingest_claim_attempts
 ADD COLUMN records jsonb CHECK(records IS NULL OR jsonb_typeof(records)='array'),
 ADD COLUMN reset_epoch bigint CHECK(reset_epoch IS NULL OR reset_epoch>=0),
 ADD COLUMN schema_generation text CHECK(schema_generation IS NULL OR schema_generation ~ '^[0-9a-f]{64}$');

CREATE TABLE sessions.ingest_buffer_admission (
 org_id text PRIMARY KEY,
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256 ~ '^[0-9a-f]{64}$'),
 retention_contract_sha256 text NOT NULL CHECK(retention_contract_sha256 ~ '^[0-9a-f]{64}$'),
 schema_generation text NOT NULL CHECK(schema_generation ~ '^[0-9a-f]{64}$'),
 valid_until timestamptz NOT NULL
);
-- Each intent is registered beforePUT. Retired keys remain durable cleanup
-- tickets, including unacknowledged/ambiguous old writes.
CREATE TABLE sessions.ingest_stage_intents (
 stage_token uuid PRIMARY KEY,
 claim_token uuid NOT NULL REFERENCES sessions.ingest_claim_attempts(claim_token) ON DELETE RESTRICT,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,
 stage_key text NOT NULL UNIQUE CHECK(octet_length(stage_key) BETWEEN 1 AND 2048),
 stage_sha256 text NOT NULL CHECK(stage_sha256 ~ '^[0-9a-f]{64}$'),
 stage_bytes bigint NOT NULL CHECK(stage_bytes BETWEEN 1 AND 16777216),
 group_ordinals jsonb NOT NULL CHECK(jsonb_typeof(group_ordinals)='array'),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),
 receipt_state text NOT NULL DEFAULT 'preparing' CHECK(receipt_state IN('preparing','acknowledged')),
 cleanup_state text NOT NULL DEFAULT 'live' CHECK(cleanup_state IN('live','retired','redacted')),
 parent_stage_token uuid REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
 row_version bigint NOT NULL DEFAULT 1 CHECK(row_version>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 UNIQUE(claim_token,stage_key),
 FOREIGN KEY(org_id,workspace_id,origin_id,batch_id) REFERENCES sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id) ON DELETE RESTRICT
);
CREATE INDEX ingest_stage_intents_cleanup_idx ON sessions.ingest_stage_intents(cleanup_state,created_at,stage_token);
CREATE TABLE sessions.ingest_buffer_outbox (
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,
 user_id text NOT NULL,digest text NOT NULL,
 active_stage_token uuid REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
 state text NOT NULL DEFAULT 'buffered' CHECK(state IN('buffered','erased')),
 row_version bigint NOT NULL DEFAULT 1 CHECK(row_version>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,origin_id,batch_id),
 FOREIGN KEY(org_id,workspace_id,origin_id,batch_id) REFERENCES sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id) ON DELETE RESTRICT,
 CHECK((state='buffered')=(active_stage_token IS NOT NULL))
);
ALTER TABLE sessions.ingest_buffer_admission ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_stage_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_buffer_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_buffer_admission,sessions.ingest_stage_intents,sessions.ingest_buffer_outbox
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Shared current-PASS fence. Take this before USER/BATCH/INTENT locks.
-- Owner-only invoker: callers are the reviewed security-definer RPCs.
CREATE FUNCTION sessions.ingest_require_generation(p_expected text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $generation$
BEGIN
 IF p_expected IS NULL OR p_expected!~'^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: generation required';END IF;
 PERFORM 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'
  AND generation=p_expected AND status='pass' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: generation not ready';END IF;
END $generation$;
REVOKE ALL ON FUNCTION sessions.ingest_require_generation(text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_require_buffer_admission(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $admission$
DECLARE v_generation text;
BEGIN
 SELECT generation INTO v_generation FROM sessions.__96bulls_role_validation
  WHERE name='session_directory_search_worker' AND status='pass' FOR SHARE;
 IF v_generation IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: generation not ready';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_admission
  WHERE org_id=p_org AND valid_until>clock_timestamp() AND schema_generation=v_generation FOR SHARE;
 IF NOT FOUND THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered privacy or retention baseline not ready';
 END IF;
END
$admission$;

CREATE FUNCTION sessions.ingest_claim_batch_mode(p jsonb,p_mode text)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $claim$
DECLARE
  k record;
  v_receipt text;
  v_batch sessions.ingest_batches%ROWTYPE;
  v_state text;
  v_pairs jsonb;
  v_items jsonb;
  v_live jsonb;
  v_deleted jsonb;
  v_conflicts jsonb;
  v_count integer;
  v_stale jsonb;
  v_resets integer;
  v_fast boolean;
  v_inserted boolean;
  v_token uuid;
  v_epochs jsonb;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF p_mode NOT IN ('buffered','canonical') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid publication mode'; END IF;
  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['tenant', 'originId', 'batchId', 'batchDigest', 'receiptId', 'sessions', 'records']);
  k := sessions.ingest_batch_key(p);
  IF jsonb_typeof(p -> 'receiptId') IS DISTINCT FROM 'string'
     OR (p ->> 'receiptId') !~ '^rhr_[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: receiptId is malformed';
  END IF;
  v_receipt := p ->> 'receiptId';
  -- Validated and normalized once; every later step reads these values only. No
  -- temporary table: a definer function must not read objects the caller can create.
  v_pairs := (SELECT coalesce(jsonb_agg(jsonb_build_array(x.source, x.session_id)), '[]'::jsonb)
                FROM sessions.ingest_session_pairs(p -> 'sessions') AS x);
  v_items := sessions.ingest_records(p -> 'records');

  IF p_mode='canonical' AND (
    EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=k.org_id) OR
    EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=k.org_id AND publication_state='buffered')) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit buffered publication authority required';
  END IF;
  PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);

  -- (a) A known batch id: its receipt, a resume, or a batch-id 409. The primary key
  -- decides a race between two first claims (the insert below), not a lock.
  SELECT * INTO v_batch FROM sessions.ingest_batches AS b
   WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
     AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
  IF FOUND THEN
    IF v_batch.user_id<>k.user_id THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: batch owner mismatch'; END IF;
    IF v_batch.digest <> k.digest THEN
      RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
    END IF;
    IF v_batch.state = 'committed' THEN
      IF v_batch.publication_state='buffered' AND v_batch.buffer_receipt_kind IS DISTINCT FROM 'suppressed'
        AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox AS o
          LEFT JOIN sessions.ingest_stage_intents AS t ON t.stage_token=o.active_stage_token
          WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id
            AND o.user_id=k.user_id AND o.digest=k.digest AND
            (o.state='erased' OR (t.receipt_state='acknowledged' AND t.cleanup_state='live'))) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered receipt proof missing';
      END IF;
      RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
    END IF;
    IF v_batch.publication_state<>p_mode THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit publication transition required'; END IF;
    v_state := 'resume';
  ELSE
    v_state := 'claimed';
  END IF;

  IF p_mode='buffered' THEN PERFORM sessions.ingest_require_buffer_admission(k.org_id); END IF;

  -- (b) The batch's sessions this user deleted from this origin. As Neon 0038, their
  -- records are dropped before the revision checks (the receipt still acknowledges
  -- them), so only the rest is judged below.
  SELECT coalesce(jsonb_agg(jsonb_build_array(d.source, d.session_id)
           ORDER BY d.source COLLATE "C", d.session_id COLLATE "C"), '[]'::jsonb)
    INTO v_deleted
    FROM sessions.upload_deletions AS d
    JOIN (SELECT x.pair ->> 0 AS source, x.pair ->> 1 AS session_id FROM jsonb_array_elements(v_pairs) AS x(pair)
          UNION
          SELECT i.item ->> 'source', i.item ->> 'session_id' FROM jsonb_array_elements(v_items) AS i(item)
           WHERE i.item ->> 'session_id' IS NOT NULL) AS s
      ON s.source = d.source AND s.session_id = d.session_id
   WHERE d.org_id = k.org_id AND d.workspace_id = k.workspace_id
     AND d.user_id = k.user_id AND d.origin_id = k.origin_id;
  v_live := (SELECT coalesce(jsonb_agg(i.item ORDER BY (i.item ->> 'ord')::integer), '[]'::jsonb)
               FROM jsonb_array_elements(v_items) AS i(item)
              WHERE i.item ->> 'session_id' IS NULL
                 OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_deleted) AS d(pair)
                                 WHERE d.pair ->> 0 = i.item ->> 'source' AND d.pair ->> 1 = i.item ->> 'session_id'));

  -- (c) #250's revision rules over the committed revisions, read without a lock
  -- (the primary key serves record_id = ANY of the batch's ids). In identity order
  -- (record_id, revision, revision_id): a record whose stored revision is at or
  -- above its own is stale (a replay, whatever its digest); a NEW revision (none
  -- stored, or the stored one lower) must not be reused in the batch, by its first
  -- peer in identity order, with both another revision id and another digest.
  WITH sorted AS (
    SELECT i.*, row_number() OVER (
             ORDER BY i.record_id COLLATE "C", i.revision, i.revision_id COLLATE "C", i.ord) AS rn
      FROM jsonb_to_recordset(v_live) AS i(ord integer, source text, session_id text,
                                            record_id text, revision_id text, revision bigint, digest text)
  ), stored AS (
    SELECT c.record_id, c.revision
      FROM sessions.ingest_record_revisions AS c
     WHERE c.org_id = k.org_id AND c.workspace_id = k.workspace_id AND c.origin_id = k.origin_id
       AND c.record_id = ANY (ARRAY(SELECT DISTINCT s.record_id FROM sorted AS s))
  ), peers AS (
    SELECT DISTINCT ON (s.record_id, s.revision) s.record_id, s.revision,
           s.revision_id AS peer_revision_id, s.digest AS peer_digest
      FROM sorted AS s ORDER BY s.record_id, s.revision, s.rn
  ), judged AS (
    SELECT s.rn, s.ord, s.record_id, s.revision_id, s.revision, s.digest,
           c.revision IS NOT NULL AND c.revision >= s.revision AS stale,
           (c.revision IS NULL OR c.revision < s.revision)
             AND p.peer_revision_id <> s.revision_id AND p.peer_digest <> s.digest AS twin,
           p.peer_revision_id, p.peer_digest
      FROM sorted AS s
      JOIN peers AS p ON p.record_id = s.record_id AND p.revision = s.revision
      LEFT JOIN stored AS c ON c.record_id = s.record_id
  ), numbered AS (
    SELECT j.*, row_number() OVER (PARTITION BY j.twin ORDER BY j.rn) AS twin_rn FROM judged AS j
  )
  SELECT count(*) FILTER (WHERE n.twin)::integer,
         coalesce(jsonb_agg(jsonb_build_object(
           'recordId', n.record_id,
           'submittedRevisionId', n.revision_id,
           'submittedRevision', n.revision,
           'submittedDigest', n.digest,
           'currentRevisionId', n.peer_revision_id,
           'currentRevision', n.revision,
           'currentDigest', n.peer_digest) ORDER BY n.rn) FILTER (WHERE n.twin AND n.twin_rn <= 100), '[]'::jsonb),
         coalesce(jsonb_agg(n.ord ORDER BY n.ord) FILTER (WHERE n.stale), '[]'::jsonb),
         bool_and(n.stale) IS NOT FALSE
    INTO v_count, v_conflicts, v_stale, v_fast
    FROM numbered AS n;
  IF v_count > 0 THEN
    IF p_mode='buffered' THEN RETURN jsonb_build_object('kind','baseline_pending'); END IF;
    RETURN jsonb_build_object('kind', 'revision_conflict', 'conflicts', v_conflicts, 'count', v_count);
  END IF;

  IF p_mode='buffered' THEN
    -- Unknown historical maxima cannot suppress accepted bytes. Potential twins
    -- above were deferred rather than incorrectly409ing a possibly stale batch.
    v_stale:='[]'::jsonb;
    v_fast:=jsonb_array_length(v_live)=0;
  END IF;

  SELECT count(*)::integer INTO v_resets FROM sessions.upload_resets AS r
   WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id AND r.user_id = k.user_id;

  -- (d) The claim. When nothing needs writing (every record stale or deleted), the
  -- batch is committed here and the caller writes nothing (replay_fast).
  IF v_state = 'claimed' THEN
    INSERT INTO sessions.ingest_batches
           (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at,publication_state,buffer_receipt_kind)
    VALUES (k.org_id, k.workspace_id, k.origin_id, k.batch_id, k.user_id, k.digest, v_receipt,
            CASE WHEN v_fast THEN 'committed' ELSE 'pending' END, CASE WHEN v_fast THEN now() END,p_mode,CASE WHEN p_mode='buffered' AND v_fast THEN 'suppressed' END)
    ON CONFLICT (org_id, workspace_id, origin_id, batch_id) DO NOTHING
    RETURNING true INTO v_inserted;
    IF v_inserted IS NULL THEN
      -- A concurrent first claim of this batch id won: answer as a retry would.
      SELECT * INTO v_batch FROM sessions.ingest_batches AS b
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
      IF v_batch.user_id<>k.user_id THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: batch owner mismatch'; END IF;
      IF v_batch.digest <> k.digest THEN
        RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
      END IF;
      IF v_batch.state = 'committed' THEN
        RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
      END IF;
      v_state := 'resume';
    END IF;
  END IF;
  IF v_fast THEN
    IF v_state = 'resume' THEN
      UPDATE sessions.ingest_batches AS b
         SET state = 'committed', committed_at = coalesce(b.committed_at, now()),
             buffer_receipt_kind=CASE WHEN p_mode='buffered' THEN 'suppressed' ELSE b.buffer_receipt_kind END
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id AND b.digest = k.digest;
    END IF;
    v_state := 'replay_fast';
  END IF;

  INSERT INTO sessions.ingest_session_ownership
    (org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT k.org_id,k.workspace_id,k.user_id,k.origin_id,s.source,s.session_id
  FROM (SELECT x.source,x.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS x
        UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_live) AS i(source text,session_id text)
          WHERE i.session_id IS NOT NULL) AS s
  WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions AS d
    WHERE d.org_id=k.org_id AND d.workspace_id=k.workspace_id AND d.user_id=k.user_id
      AND d.origin_id=k.origin_id AND d.source=s.source AND d.session_id=s.session_id)
  ON CONFLICT DO NOTHING;
  IF v_state IN ('claimed','resume') THEN
    v_token:=gen_random_uuid();
    SELECT coalesce(jsonb_agg(jsonb_build_object('source',x.source,'session_id',x.session_id,'epoch',coalesce(f.epoch,0),'reset_epoch',v_resets)
      ORDER BY x.source COLLATE "C",x.session_id COLLATE "C"),'[]'::jsonb) INTO v_epochs
    FROM (SELECT q.source,q.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS q
          UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
           WHERE i.session_id IS NOT NULL) AS x
    LEFT JOIN sessions.ingest_fence_versions AS f ON f.org_id=k.org_id AND f.workspace_id=k.workspace_id
      AND f.user_id=k.user_id AND f.origin_id=k.origin_id AND f.source=x.source AND f.session_id=x.session_id;
    INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch)
    VALUES(v_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_epochs,v_items,v_resets);
  END IF;
  RETURN jsonb_build_object('kind', v_state, 'claimToken',v_token, 'deleted', v_deleted, 'stale', v_stale, 'resets', v_resets);
END
$claim$;


REVOKE ALL ON FUNCTION sessions.ingest_claim_batch_mode(jsonb,text),sessions.ingest_require_buffer_admission(text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE OR REPLACE FUNCTION sessions.ingest_claim_batch(p jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $canonical$
 SELECT sessions.ingest_claim_batch_mode(p,'canonical');
$canonical$;
CREATE FUNCTION sessions.ingest_claim_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $buffered$
DECLARE v_generation text;v_result jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','receiptId','sessions','records','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 PERFORM sessions.ingest_require_buffer_admission(p#>>'{tenant,orgId}');
 v_result:=sessions.ingest_claim_batch_mode(p-'generation','buffered');
 IF v_result->>'kind' IN('claimed','resume') THEN
  UPDATE sessions.ingest_claim_attempts SET schema_generation=v_generation
   WHERE claim_token=(v_result->>'claimToken')::uuid AND (schema_generation IS NULL OR schema_generation=v_generation);
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation collision';END IF;
 END IF;
 PERFORM sessions.ingest_require_buffer_admission(p#>>'{tenant,orgId}');
 RETURN v_result;
END
$buffered$;

CREATE FUNCTION sessions.ingest_prepare_buffer_stage(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $prepare$
DECLARE k record;a sessions.ingest_claim_attempts%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;v_generation text;
 v_key text;v_sha text;v_bytes bigint;v_groups jsonb;v_count integer;v_distinct integer;v_token uuid;v_pairs jsonb;v_fenced jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','key','sha256','bytes','groupOrdinals','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 k:=sessions.ingest_batch_key(p);
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);
 PERFORM sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=(p->>'claimToken')::uuid;
 IF a.schema_generation IS DISTINCT FROM v_generation
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation changed';END IF;
 IF a.reset_epoch IS NULL OR a.records IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: attempt lacks buffered proof'; END IF;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id
  AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id AND b.user_id=k.user_id AND b.digest=k.digest
  AND b.publication_state='buffered' AND b.state='pending' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: buffered batch not pending'; END IF;
 v_key:=p->>'key';v_sha:=sessions.ingest_digest(p->'sha256');
 IF jsonb_typeof(p->'key') IS DISTINCT FROM 'string' OR octet_length(v_key) NOT BETWEEN 1 AND 2048
   OR position('/'||k.org_id||'/buffered-batches/v1/' IN v_key)=0
   OR v_key !~ ('-'||substr(encode(sha256(convert_to(p->>'claimToken','UTF8')),'hex'),1,32)||'\.staged\.ndjson\.zst$')
   OR v_key ~ '[\x01-\x1f\x7f]' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered key'; END IF;
 IF jsonb_typeof(p->'bytes') IS DISTINCT FROM 'number' OR (p->>'bytes') !~ '^[1-9][0-9]{0,7}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered bytes';
 END IF;
 v_bytes:=(p->>'bytes')::bigint;
 IF v_bytes>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: buffered bytes exceed limit'; END IF;
 v_groups:=p->'groupOrdinals';
 IF jsonb_typeof(v_groups) IS DISTINCT FROM 'array' OR jsonb_array_length(v_groups) NOT BETWEEN 1 AND 500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered groups';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_groups) g
    WHERE jsonb_typeof(g)<>'array' OR jsonb_array_length(g) NOT BETWEEN 1 AND 500) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid buffered ordinal group';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_groups) g CROSS JOIN LATERAL jsonb_array_elements(g) o
   WHERE jsonb_typeof(o)<>'number' OR o::text !~ '^(0|[1-9][0-9]{0,2})$'
      OR NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(a.records) r(ord integer) WHERE r.ord=(o::text)::int)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: ordinal outside original claim';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_groups) g
  CROSS JOIN LATERAL (
   SELECT count(DISTINCT jsonb_build_array(r.source,r.session_id)) FILTER(WHERE r.session_id IS NOT NULL) cohorts,
          bool_or(r.session_id IS NULL) has_null,bool_or(r.session_id IS NOT NULL) has_session
    FROM jsonb_array_elements(g) o JOIN jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
      ON r.ord=(o::text)::int
  ) q WHERE q.cohorts>1 OR (q.has_null AND q.has_session)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: buffered group mixes session owners';
 END IF;
 SELECT count(*)::int,count(DISTINCT o)::int INTO v_count,v_distinct
  FROM jsonb_array_elements(v_groups) g CROSS JOIN LATERAL jsonb_array_elements(g) o;
 IF v_count<>v_distinct OR v_count>500 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: repeated buffered ordinal'; END IF;
 IF a.reset_epoch<>(SELECT count(*) FROM sessions.upload_resets r
    WHERE r.org_id=k.org_id AND r.workspace_id=k.workspace_id AND r.user_id=k.user_id) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: reset before buffered intent';
 END IF;
 -- Cover every currently live original ordinal exactly once; original metadata
 -- remains in SQL, so scrubbed record/session ids cannot affect this decision.
 SELECT coalesce(jsonb_agg(jsonb_build_array(r.source,r.session_id)),'[]'::jsonb) INTO v_pairs
 FROM (SELECT DISTINCT source,session_id FROM jsonb_to_recordset(a.records) r(source text,session_id text)
       WHERE session_id IS NOT NULL) r;
 v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,a.epochs,v_pairs);
 IF EXISTS(SELECT 1 FROM jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
   WHERE (r.ord IN(SELECT (o::text)::int FROM jsonb_array_elements(v_groups) g CROSS JOIN LATERAL jsonb_array_elements(g) o))
      IS DISTINCT FROM (r.session_id IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) f
          WHERE f->>0=r.source AND f->>1=r.session_id))) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: buffered ordinal coverage changed';
 END IF;
 SELECT * INTO t FROM sessions.ingest_stage_intents
  WHERE claim_token=a.claim_token AND stage_key=v_key FOR UPDATE;
 IF FOUND THEN
  IF t.stage_sha256<>v_sha OR t.stage_bytes<>v_bytes OR t.group_ordinals<>v_groups
    OR t.cleanup_state<>'live' OR t.receipt_state<>'preparing' OR t.expires_at<=clock_timestamp()
    OR t.parent_stage_token IS NOT NULL THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: buffered intent refused';
  END IF;
  PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('stageToken',t.stage_token,'expiresAt',t.expires_at);
 END IF;
 v_token:=gen_random_uuid();
 INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,
   stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,expires_at)
 VALUES(v_token,a.claim_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,
   v_key,v_sha,v_bytes,v_groups,a.reset_epoch,clock_timestamp()+interval '120 seconds')
 RETURNING * INTO t;
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('stageToken',t.stage_token,'expiresAt',t.expires_at);
END
$prepare$;

CREATE FUNCTION sessions.ingest_commit_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $bcommit$
DECLARE k record;a sessions.ingest_claim_attempts%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;v_generation text;
 v_pairs jsonb;v_fenced jsonb;v_invalid boolean;v_stage uuid;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','stageToken','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 k:=sessions.ingest_batch_key(p);
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);
 PERFORM sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
 v_stage:=(p->>'stageToken')::uuid;
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=(p->>'claimToken')::uuid;
 IF a.schema_generation IS DISTINCT FROM v_generation
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation changed';END IF;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id
  AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id AND b.user_id=k.user_id AND b.digest=k.digest
  AND b.publication_state='buffered' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: no buffered batch'; END IF;
 SELECT * INTO t FROM sessions.ingest_stage_intents WHERE stage_token=v_stage AND claim_token=a.claim_token FOR UPDATE;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.cleanup_state<>'live'
   OR (t.receipt_state='preparing' AND t.expires_at<=clock_timestamp()) THEN
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_array(r.source,r.session_id)),'[]'::jsonb) INTO v_pairs
 FROM (SELECT DISTINCT r.source,r.session_id FROM jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
  WHERE r.session_id IS NOT NULL AND r.ord IN(
   SELECT (o::text)::int FROM jsonb_array_elements(t.group_ordinals) g CROSS JOIN LATERAL jsonb_array_elements(g) o)) r;
 v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,a.epochs,v_pairs);
 v_invalid:=jsonb_array_length(v_fenced)>0 OR a.reset_epoch IS NULL OR a.reset_epoch<>(SELECT count(*) FROM sessions.upload_resets r
  WHERE r.org_id=k.org_id AND r.workspace_id=k.workspace_id AND r.user_id=k.user_id);
 IF v_invalid THEN
  UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1 WHERE stage_token=v_stage;
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 IF t.receipt_state='acknowledged' THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id
    AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id AND o.digest=k.digest AND o.active_stage_token=v_stage) THEN
   PERFORM sessions.ingest_require_buffer_admission(k.org_id);
   RETURN jsonb_build_object('kind','committed');
  END IF;
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id
   AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id) THEN RETURN jsonb_build_object('kind','intent_invalid'); END IF;
 -- Receipt and durable recovery pointer are one transaction; no queue proof.
 INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,user_id,origin_id,batch_id,digest,active_stage_token)
 VALUES(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_stage);
 UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged',row_version=row_version+1 WHERE stage_token=v_stage;
 UPDATE sessions.ingest_batches b SET state='committed',committed_at=coalesce(b.committed_at,clock_timestamp()),buffer_receipt_kind='staged'
 WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id
  AND b.user_id=k.user_id AND b.digest=k.digest AND b.publication_state='buffered';
 IF clock_timestamp()>=t.expires_at THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered intent expired';END IF;
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('kind','committed');
END
$bcommit$;
REVOKE ALL ON FUNCTION sessions.ingest_claim_buffered_batch(jsonb),
 sessions.ingest_prepare_buffer_stage(jsonb),sessions.ingest_commit_buffered_batch(jsonb)
 FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.ingest_claim_buffered_batch(jsonb),
 sessions.ingest_prepare_buffer_stage(jsonb),sessions.ingest_commit_buffered_batch(jsonb)
 TO bulls96_session_ingest;


-- Buffered admission is not canonical publication authority.
CREATE OR REPLACE FUNCTION sessions.ingest_fanout_fence(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $ffence$
DECLARE v_org text;v_ws text;v_user text;v_origin text;v_epochs jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
    USING(org_id,workspace_id,origin_id,batch_id)
    WHERE a.claim_token::text=p->>'claimToken' AND b.publication_state='buffered') THEN
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

-- Buffered admission is not canonical publication authority.
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
    WHERE a.claim_token::text=p->>'claimToken' AND b.publication_state='buffered') THEN
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

-- Buffered admission is not canonical publication authority.
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
    WHERE a.claim_token::text=p->>'claimToken' AND b.publication_state='buffered') THEN
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

-- Guard existing controls without broadening grants or scope.
CREATE OR REPLACE FUNCTION sessions.ingest_set_session_uploads(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $select$
DECLARE
  t jsonb;
  v_org text;v_generation text;v_result jsonb;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  -- Retained enrollment/history cannot fall back after admission closes.
  v_org:=p#>>'{tenant,orgId}';
  IF p?'generation' OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org)
    OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND publication_state='buffered') THEN
   v_generation:=sessions.ingest_digest(p->'generation');
   PERFORM sessions.ingest_require_generation(v_generation);
   PERFORM sessions.ingest_require_buffer_admission(v_org);
   p:=p-'generation';
  END IF;
  PERFORM sessions.ingest_exact_keys(p, ARRAY['tenant', 'originId', 'sessions', 'enabled']);
  t := p -> 'tenant';
  PERFORM sessions.ingest_exact_keys(t, ARRAY['orgId', 'workspaceId', 'userId']);
  IF jsonb_typeof(p -> 'enabled') IS DISTINCT FROM 'boolean' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: enabled is malformed';
  END IF;
  v_result:=jsonb_build_object('updated', sessions.ingest_apply_selection(
    sessions.ingest_identifier(t -> 'orgId', false), sessions.ingest_identifier(t -> 'workspaceId', true),
    sessions.ingest_identifier(t -> 'userId', false), sessions.ingest_identifier(p -> 'originId', false),
    p -> 'sessions', (p ->> 'enabled')::boolean));
  IF v_generation IS NOT NULL THEN PERFORM sessions.ingest_require_buffer_admission(v_org);END IF;
  RETURN v_result;
END
$select$;

-- Guard existing controls without broadening grants or scope.
CREATE OR REPLACE FUNCTION sessions.ingest_reset_uploaded_sessions(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $resetfn$
DECLARE
  t jsonb;
  v_org text;v_generation text;v_result jsonb;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  -- Retained enrollment/history cannot fall back after admission closes.
  v_org:=p#>>'{tenant,orgId}';
  IF p?'generation' OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org)
    OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND publication_state='buffered') THEN
   v_generation:=sessions.ingest_digest(p->'generation');
   PERFORM sessions.ingest_require_generation(v_generation);
   PERFORM sessions.ingest_require_buffer_admission(v_org);
   p:=p-'generation';
  END IF;
  PERFORM sessions.ingest_exact_keys(p, ARRAY['tenant', 'requestId']);
  t := p -> 'tenant';
  PERFORM sessions.ingest_exact_keys(t, ARRAY['orgId', 'workspaceId', 'userId']);
  v_result:=jsonb_build_object('deleted', sessions.ingest_apply_reset(
    sessions.ingest_identifier(t -> 'orgId', false), sessions.ingest_identifier(t -> 'workspaceId', true),
    sessions.ingest_identifier(t -> 'userId', false), sessions.ingest_identifier(p -> 'requestId', false)));
  IF v_generation IS NOT NULL THEN PERFORM sessions.ingest_require_buffer_admission(v_org);END IF;
  RETURN v_result;
END
$resetfn$;
