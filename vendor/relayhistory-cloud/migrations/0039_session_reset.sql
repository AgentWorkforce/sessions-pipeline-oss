-- A reset is idempotent and serialized with this user's delivery on every origin.
CREATE TABLE sessions.session_resets (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
  request_id text NOT NULL, deleted bigint NOT NULL,
  PRIMARY KEY (org_id, workspace_id, user_id, request_id)
);
ALTER FUNCTION sessions.accept_delivery_batch(text,text,text,text,text,text,jsonb,jsonb)
  RENAME TO accept_delivery_batch_before_reset;
CREATE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(p_org,p_workspace,p_user)::text, 0));
  RETURN sessions.accept_delivery_batch_before_reset(p_org,p_workspace,p_user,p_origin,p_batch,p_digest,p_records,p_receipt);
END;
$delivery$;
CREATE FUNCTION sessions.reset_uploaded_sessions(p_org text, p_workspace text, p_user text, p_request text)
RETURNS bigint LANGUAGE plpgsql AS $reset$
DECLARE previous bigint; total bigint; item record;
BEGIN
  PERFORM sessions.require_upload_cleanup_ready();
  PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(p_org,p_workspace,p_user)::text, 0));
  SELECT deleted INTO previous FROM sessions.session_resets
    WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user AND request_id=p_request;
  IF FOUND THEN RETURN previous; END IF;
  -- Selection takes admission before origin/workspace; acquire it before the
  -- origin prelock here as well, avoiding an admission/origin lock inversion.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org,0));
  PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  -- Delivery takes origin before workspace. Acquire every selected origin in
  -- canonical order before the first deletion takes the projection lock;
  -- otherwise a later origin can be held by another user's writer waiting on
  -- the workspace this reset already holds. Own-user delivery is excluded by
  -- the user advisory lock above while this origin set is being established.
  INSERT INTO sessions.delivery_origins(org_id,workspace_id,origin_id)
    SELECT DISTINCT p_org,p_workspace,origin_id FROM sessions.owned_uploaded_session_identities(p_org,p_workspace,p_user)
    ORDER BY origin_id ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins o
    WHERE o.org_id=p_org AND o.workspace_id=p_workspace AND EXISTS (
      SELECT 1 FROM sessions.owned_uploaded_session_identities(p_org,p_workspace,p_user) r WHERE r.origin_id=o.origin_id)
    ORDER BY o.origin_id FOR UPDATE;
  SELECT count(*) INTO total FROM (SELECT DISTINCT source,session_id FROM sessions.owned_uploaded_session_identities(p_org,p_workspace,p_user)) s;
  FOR item IN SELECT origin_id, jsonb_agg(DISTINCT jsonb_build_object('source',source,'session_id',session_id)) AS ids
    FROM sessions.owned_uploaded_session_identities(p_org,p_workspace,p_user)
    GROUP BY origin_id ORDER BY origin_id LOOP
    PERFORM sessions.set_session_uploads(p_org,p_workspace,p_user,item.origin_id,item.ids,false);
  END LOOP;
  INSERT INTO sessions.session_resets VALUES(p_org,p_workspace,p_user,p_request,total);
  RETURN total;
END;
$reset$;
