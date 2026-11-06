-- The brief's state: where the session's work stands, as the model read the conversation
-- (session-summary-v3: in-progress, completed, blocked, abandoned, answered), returned by
-- sessions.session_brief beside the title and the now two-sentence description.
--
-- Briefs written by the v2 prompt keep a NULL state until they are regenerated: the worker
-- treats a brief from another prompt version as not reusable, so the next wakeup for that
-- session calls the model. Every brief from any other prompt must carry a state.
--
-- session_brief keeps its signature, so its ACL manifest entry and its place on the frozen
-- SECURITY DEFINER list are unchanged. Adding a column to RETURNS TABLE needs DROP and
-- CREATE, which also drops its grants, so they are restated exactly as 20261102000000 left
-- them. Code only; nothing here is applied by this repository. Down path:
-- supabase/rollbacks/20261102020000_session_brief_state.down.sql.

ALTER TABLE sessions.session_briefs
  ADD COLUMN state text
    CHECK (state IN ('in-progress', 'completed', 'blocked', 'abandoned', 'answered')),
  -- A state describes a brief, never a missing one; and only v2 briefs may lack one.
  ADD CONSTRAINT session_briefs_state_text CHECK (
    (title IS NOT NULL OR state IS NULL) AND
    (title IS NULL OR state IS NOT NULL OR prompt_version = 'session-summary-v2'));

DROP FUNCTION sessions.session_brief(text, text, text);

-- As 20261102000000, plus `state`.
CREATE FUNCTION sessions.session_brief(
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

REVOKE ALL ON FUNCTION sessions.session_brief(text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_brief(text, text, text)
  TO session_directory_search_worker;

-- As in 20261102000000: strip EXECUTE from every other grantee present at creation.
DO $revoke$
DECLARE
  v_grantee text;
BEGIN
  FOR v_grantee IN
    SELECT r.rolname
      FROM pg_proc AS p
      CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
      JOIN pg_roles AS r ON r.oid = a.grantee
     WHERE p.oid = 'sessions.session_brief(text, text, text)'::regprocedure
       AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
       AND r.rolname <> 'session_directory_search_worker'
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION sessions.session_brief(text, text, text) FROM %I', v_grantee);
  END LOOP;
END
$revoke$;
