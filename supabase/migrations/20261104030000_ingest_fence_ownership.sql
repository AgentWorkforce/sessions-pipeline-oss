-- Short per-user ledger decisions only; S3 IO stays outside these transactions.
-- Claims durably register ownership before staged acknowledgement, so reset reaches
-- pending sessions. Drain/reconcile pre-upgrade fanout before activating this writer.
CREATE TABLE sessions.ingest_fence_owners (
 org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,user_id)
);
CREATE TABLE sessions.ingest_session_ownership (
 org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
 origin_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id)
);
ALTER TABLE sessions.ingest_fence_owners ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_session_ownership ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_fence_owners,sessions.ingest_session_ownership
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- A prior late commit may already have restored a fenced revision. Forget that
-- untrustworthy replay index entry before enabling the repaired writer.
DELETE FROM sessions.ingest_record_revisions AS r
 USING sessions.upload_deletions AS d
 WHERE r.org_id=d.org_id AND r.workspace_id=d.workspace_id AND r.user_id=d.user_id
   AND r.origin_id=d.origin_id AND r.source=d.source AND r.session_id=d.session_id;
INSERT INTO sessions.ingest_session_ownership
 SELECT DISTINCT org_id,workspace_id,user_id,origin_id,source,session_id
 FROM sessions.ingest_record_revisions WHERE session_id IS NOT NULL;
CREATE FUNCTION sessions.ingest_lock_fences(p_org text,p_ws text,p_user text)
RETURNS void LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lock$
BEGIN
 INSERT INTO sessions.ingest_fence_owners VALUES(p_org,p_ws,p_user) ON CONFLICT DO NOTHING;
 PERFORM 1 FROM sessions.ingest_fence_owners AS f
 WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.user_id=p_user FOR UPDATE;
END
$lock$;
REVOKE ALL ON FUNCTION sessions.ingest_lock_fences(text,text,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Versions survive enable and ownership removal: the disappearance of a
-- tombstone must not make a pre-disable write trusted again.
CREATE TABLE sessions.ingest_fence_versions (
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 epoch bigint NOT NULL CHECK(epoch>0),
 PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id)
);
-- Every resume receives a new immutable snapshot. An old writer cannot borrow
-- the newer resume's epochs by presenting the same batch id/digest.
CREATE TABLE sessions.ingest_claim_attempts (
 claim_token uuid PRIMARY KEY,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,
 epochs jsonb NOT NULL CHECK(jsonb_typeof(epochs)='array'),
 FOREIGN KEY(org_id,workspace_id,origin_id,batch_id)
  REFERENCES sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id) ON DELETE CASCADE
);
CREATE INDEX ingest_claim_attempts_batch_idx ON sessions.ingest_claim_attempts(org_id,workspace_id,origin_id,batch_id);
ALTER TABLE sessions.ingest_fence_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_claim_attempts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_fence_versions,sessions.ingest_claim_attempts
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_attempt_epochs(
 p_org text,p_ws text,p_user text,p_origin text,p_batch text,p_digest text,p_token text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $attempt$
DECLARE v jsonb;
BEGIN
 IF p_batch IS NOT NULL AND NOT EXISTS(SELECT 1 FROM sessions.ingest_batches AS b
   WHERE b.org_id=p_org AND b.workspace_id=p_ws AND b.user_id=p_user AND b.origin_id=p_origin
     AND b.batch_id=p_batch AND b.digest=p_digest) THEN
  RAISE EXCEPTION USING ERRCODE='P0002',MESSAGE='session ingest: no claim for this batch';
 END IF;
 IF p_token IS NULL OR p_token !~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: unknown claim attempt';
 END IF;
 SELECT a.epochs INTO v FROM sessions.ingest_claim_attempts AS a
 JOIN sessions.ingest_batches AS b USING(org_id,workspace_id,origin_id,batch_id)
 WHERE a.org_id=p_org AND a.workspace_id=p_ws AND a.user_id=p_user
   AND a.origin_id=p_origin AND (p_batch IS NULL OR a.batch_id=p_batch) AND a.digest=p_digest
   AND a.claim_token=p_token::uuid AND b.user_id=p_user AND b.digest=p_digest;
 IF NOT FOUND THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: unknown claim attempt';
 END IF;
 RETURN v;
END
$attempt$;

CREATE FUNCTION sessions.ingest_attempt_fenced(
 p_org text,p_ws text,p_user text,p_origin text,p_epochs jsonb,p_pairs jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $afence$
DECLARE v jsonb;v_reset bigint;
BEGIN
 SELECT count(*) INTO v_reset FROM sessions.upload_resets AS r
 WHERE r.org_id=p_org AND r.workspace_id=p_ws AND r.user_id=p_user;
 IF EXISTS(SELECT 1 FROM sessions.ingest_session_pairs(p_pairs) AS s
   WHERE NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(p_epochs) AS e(source text,session_id text,epoch bigint,reset_epoch bigint)
     WHERE e.source=s.source AND e.session_id=s.session_id)) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: session outside claim attempt';
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_array(s.source,s.session_id)
   ORDER BY s.source COLLATE "C",s.session_id COLLATE "C"),'[]'::jsonb) INTO v
 FROM sessions.ingest_session_pairs(p_pairs) AS s
 JOIN jsonb_to_recordset(p_epochs) AS e(source text,session_id text,epoch bigint,reset_epoch bigint)
   ON e.source=s.source AND e.session_id=s.session_id
 LEFT JOIN sessions.ingest_fence_versions AS f
   ON f.org_id=p_org AND f.workspace_id=p_ws AND f.user_id=p_user
    AND f.origin_id=p_origin AND f.source=s.source AND f.session_id=s.session_id
 WHERE coalesce(f.epoch,0)<>e.epoch OR v_reset<>e.reset_epoch OR EXISTS(
   SELECT 1 FROM sessions.upload_deletions AS d
   WHERE d.org_id=p_org AND d.workspace_id=p_ws AND d.user_id=p_user
     AND d.origin_id=p_origin AND d.source=s.source AND d.session_id=s.session_id);
 RETURN v;
END
$afence$;
REVOKE ALL ON FUNCTION sessions.ingest_attempt_epochs(text,text,text,text,text,text,text),
 sessions.ingest_attempt_fenced(text,text,text,text,jsonb,jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.ingest_claim_batch(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
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
      RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
    END IF;
    v_state := 'resume';
  ELSE
    v_state := 'claimed';
  END IF;

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
    RETURN jsonb_build_object('kind', 'revision_conflict', 'conflicts', v_conflicts, 'count', v_count);
  END IF;

  SELECT count(*)::integer INTO v_resets FROM sessions.upload_resets AS r
   WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id AND r.user_id = k.user_id;

  -- (d) The claim. When nothing needs writing (every record stale or deleted), the
  -- batch is committed here and the caller writes nothing (replay_fast).
  IF v_state = 'claimed' THEN
    INSERT INTO sessions.ingest_batches
           (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at)
    VALUES (k.org_id, k.workspace_id, k.origin_id, k.batch_id, k.user_id, k.digest, v_receipt,
            CASE WHEN v_fast THEN 'committed' ELSE 'pending' END, CASE WHEN v_fast THEN now() END)
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
         SET state = 'committed', committed_at = coalesce(b.committed_at, now())
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
    INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs)
    VALUES(v_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_epochs);
  END IF;
  RETURN jsonb_build_object('kind', v_state, 'claimToken',v_token, 'deleted', v_deleted, 'stale', v_stale, 'resets', v_resets);
END
$claim$;

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

CREATE OR REPLACE FUNCTION sessions.ingest_apply_selection(p_org text, p_ws text, p_user text, p_origin text,
                                                p_sessions jsonb, p_enabled boolean)
RETURNS integer
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $apply$
DECLARE
  v_n integer;
BEGIN
  PERFORM sessions.ingest_lock_fences(p_org,p_ws,p_user);
  IF p_enabled THEN
    DELETE FROM sessions.upload_deletions AS d
     USING sessions.ingest_fence_pairs(p_sessions) AS s
     WHERE d.org_id = p_org AND d.workspace_id = p_ws AND d.user_id = p_user
       AND d.origin_id = p_origin AND d.source = s.source AND d.session_id = s.session_id;
  ELSE
    INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
    SELECT DISTINCT p_org,p_ws,p_user,p_origin,x.source,x.session_id,1 FROM sessions.ingest_fence_pairs(p_sessions) AS x
    ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1;
    INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id)
    SELECT DISTINCT p_org, p_ws, p_user, p_origin, s.source, s.session_id
      FROM sessions.ingest_fence_pairs(p_sessions) AS s
    ON CONFLICT DO NOTHING;
    DELETE FROM sessions.ingest_record_revisions AS t
     USING sessions.ingest_fence_pairs(p_sessions) AS s
     WHERE t.org_id = p_org AND t.workspace_id = p_ws AND t.user_id = p_user
       AND t.origin_id = p_origin AND t.source = s.source AND t.session_id = s.session_id;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    DELETE FROM sessions.ingest_session_ownership AS o
     USING sessions.ingest_fence_pairs(p_sessions) AS s
     WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=p_user
       AND o.origin_id=p_origin AND o.source=s.source AND o.session_id=s.session_id;
    RETURN v_n;
  END IF;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END
$apply$;

CREATE OR REPLACE FUNCTION sessions.ingest_apply_reset(p_org text, p_ws text, p_user text, p_request text)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $reset$
DECLARE
  v_previous bigint;
  v_total bigint;
BEGIN
  PERFORM sessions.ingest_lock_fences(p_org,p_ws,p_user);
  SELECT r.deleted INTO v_previous FROM sessions.upload_resets AS r
   WHERE r.org_id = p_org AND r.workspace_id = p_ws AND r.user_id = p_user AND r.request_id = p_request;
  IF FOUND THEN RETURN v_previous; END IF;
  WITH owned AS (
    SELECT DISTINCT t.origin_id, t.source, t.session_id FROM sessions.ingest_record_revisions AS t
     WHERE t.org_id = p_org AND t.workspace_id = p_ws AND t.user_id = p_user AND t.session_id IS NOT NULL
    UNION
    SELECT o.origin_id,o.source,o.session_id FROM sessions.ingest_session_ownership AS o
     WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=p_user
  ), versioned AS (
    INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
    SELECT p_org,p_ws,p_user,o.origin_id,o.source,o.session_id,1 FROM owned AS o
    ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1
  ), fenced AS (
    INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id)
    SELECT p_org, p_ws, p_user, o.origin_id, o.source, o.session_id FROM owned AS o
    ON CONFLICT DO NOTHING
  )
  SELECT count(*) INTO v_total FROM (SELECT DISTINCT source, session_id FROM owned) AS x;
  DELETE FROM sessions.ingest_record_revisions AS t
   WHERE t.org_id = p_org AND t.workspace_id = p_ws AND t.user_id = p_user AND t.session_id IS NOT NULL;
  DELETE FROM sessions.ingest_session_ownership AS o
   WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=p_user;
  INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, deleted)
  VALUES (p_org, p_ws, p_user, p_request, v_total);
  RETURN v_total;
END
$reset$;

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

CREATE OR REPLACE FUNCTION sessions.ingest_fanout_fence(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $ffence$
DECLARE v_org text;v_ws text;v_user text;v_origin text;v_epochs jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
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
