-- Model-written session briefs (title + one-sentence description) on the 96Bulls pipeline,
-- replacing relayhistory-cloud's Neon brief job (sessions.session_brief_v3 and
-- sessions.session_analysis_jobs kind = 'brief'). Khaliq's ruling (neon-retire, 2026-11-01):
-- keep MODEL-written briefs, port the job onto this pipeline and write it to Supabase.
--
-- Three tables, all service-only (RLS on, no authenticated policy):
--   session_briefs          one row per logical session (the session_directory key): the
--                           latest brief and the latest attempt's status. The text is
--                           re-scrubbed and bounded by the writer (src/session-brief/store.ts)
--                           and the bounds are repeated here.
--   session_brief_wakeups   the debounced work list, armed by the HoopDb indexer after every
--                           session-indexed upsert when SESSION_BRIEF_ENABLED=true. Same rule
--                           as relayhistory's session_event_outbox (0048): due ten minutes
--                           after the newest evidence, but never more than sixty minutes
--                           after the first unbriefed evidence, so an active session is
--                           briefed at least hourly and an idle one ten minutes after it stops.
--   session_brief_attempts  per-session, per-UTC-day admission counts, for relayhistory's
--                           budgets: 30 attempts per session per day and a daily pool of
--                           distinct sessions per organization.
--
-- Reads: sessions.session_brief(workspace, harness, session), the one Worker RPC, under the
-- same role guard and signed-claims scope as sessions.session_directory_pointers
-- (20261031001000). The title is also projected into session_directory.title (projector
-- precedence: a brief title wins over the pointer title) and the description into
-- short_summary through writeShortSummary, so search_session_directory returns both.
--
-- Deletion: a tombstoned or deleted pointer, or a deleted directory row (erasure), removes
-- the session's brief and wakeup at once (triggers below), as propagatePointerTombstone
-- clears the short summary: the brief may describe the deleted machine's content.
--
-- Code only; nothing here is applied by this repository. Down path:
-- supabase/rollbacks/20261102000000_session_briefs.down.sql.

CREATE TABLE sessions.session_briefs (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL CHECK (harness ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255),
  -- The latest attempt's outcome. A failed or refused attempt keeps the previous brief.
  status text NOT NULL
    CHECK (status IN ('ready', 'failed', 'rate_limited', 'ineligible', 'quarantined', 'empty')),
  -- The highest live pointer revision the latest attempt read, and every machine's revision.
  revision bigint NOT NULL CHECK (revision >= 0),
  source_revisions jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(source_revisions) = 'object'),
  -- SHA-256 of the model input (relayhistory's brief fingerprint): an equal input reuses
  -- the stored brief without a model call.
  source_fingerprint text CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  -- The brief itself: SESSION_SUMMARY_JSON_SCHEMA bounds, one line each.
  title text CHECK (char_length(title) BETWEEN 1 AND 90 AND title !~ '[\r\n]'),
  description text CHECK (char_length(description) BETWEEN 1 AND 320 AND description !~ '[\r\n]'),
  -- The revisions and fingerprint the title and description were generated from.
  brief_revision bigint CHECK (brief_revision >= 0),
  brief_source_revisions jsonb CHECK (jsonb_typeof(brief_source_revisions) = 'object'),
  brief_fingerprint text CHECK (brief_fingerprint ~ '^[0-9a-f]{64}$'),
  scrub_ruleset text CHECK (scrub_ruleset = 'relayhistory-scrub'),
  scrub_version smallint CHECK (scrub_version >= 1),
  schema_version smallint NOT NULL DEFAULT 1 CHECK (schema_version = 1),
  prompt_version text CHECK (prompt_version ~ '^[a-z0-9][a-z0-9._:-]{0,63}$'),
  engine_version text CHECK (engine_version ~ '^[a-z0-9][a-z0-9._:-]{0,127}$'),
  model text CHECK (model ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$'),
  message_count integer CHECK (message_count >= 0),
  shortened_messages integer CHECK (shortened_messages >= 0),
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  duration_ms integer CHECK (duration_ms >= 0),
  -- A controlled code only (model_rate_limited, session_analysis_ineligible, ...), never text.
  error text CHECK (error ~ '^[a-z][a-z0-9_]{0,63}$'),
  generated_at timestamptz,
  attempted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT session_briefs_pkey PRIMARY KEY (org_id, workspace_id, harness, session_id),
  -- A brief is all or nothing: text, provenance and generation metadata together.
  CONSTRAINT session_briefs_text_provenance CHECK (
    (title IS NULL) = (description IS NULL) AND
    (title IS NULL) = (brief_revision IS NULL) AND
    (title IS NULL) = (brief_source_revisions IS NULL) AND
    (title IS NULL) = (brief_fingerprint IS NULL) AND
    (title IS NULL) = (scrub_ruleset IS NULL) AND
    (title IS NULL) = (scrub_version IS NULL) AND
    (title IS NULL) = (prompt_version IS NULL) AND
    (title IS NULL) = (engine_version IS NULL) AND
    (title IS NULL) = (model IS NULL) AND
    (title IS NULL) = (generated_at IS NULL)
  ),
  CONSTRAINT session_briefs_ready_has_text CHECK (status <> 'ready' OR title IS NOT NULL),
  CONSTRAINT session_briefs_ready_has_no_error CHECK (status <> 'ready' OR error IS NULL)
);
COMMENT ON TABLE sessions.session_briefs IS
  'Model-written session brief (title, description) per logical session. Derived and rebuildable from the scrubbed raw in object storage. Read through sessions.session_brief only.';

CREATE TABLE sessions.session_brief_wakeups (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  -- 'background': armed by session-indexed. 'sweep': queued for an indexed session with no brief.
  admission text NOT NULL CHECK (admission IN ('background', 'sweep')),
  -- First unbriefed evidence (the 60-minute cap counts from here) and the newest.
  pending_since timestamptz NOT NULL DEFAULT now(),
  evidence_at timestamptz NOT NULL DEFAULT now(),
  due_at timestamptz NOT NULL,
  -- Bumped by every arm; completion deletes the wakeup only for the generation it ran.
  generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  lease_id uuid,
  lease_expires_at timestamptz,
  last_error text CHECK (last_error ~ '^[a-z][a-z0-9_]{0,63}$'),
  PRIMARY KEY (org_id, workspace_id, harness, session_id),
  CHECK ((lease_id IS NULL) = (lease_expires_at IS NULL))
);
CREATE INDEX session_brief_wakeups_due_idx ON sessions.session_brief_wakeups (due_at);
CREATE INDEX session_brief_wakeups_evidence_idx ON sessions.session_brief_wakeups (evidence_at);

CREATE TABLE sessions.session_brief_attempts (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  day date NOT NULL,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  PRIMARY KEY (org_id, day, workspace_id, harness, session_id)
);

ALTER TABLE sessions.session_briefs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_brief_wakeups ENABLE ROW LEVEL SECURITY;
-- The global daily spend ceiling sums one day's attempts across every org.
CREATE INDEX session_brief_attempts_day_idx ON sessions.session_brief_attempts (day);
ALTER TABLE sessions.session_brief_attempts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_briefs, sessions.session_brief_wakeups,
  sessions.session_brief_attempts FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_briefs, sessions.session_brief_wakeups,
  sessions.session_brief_attempts TO service_role;

-- Deletion propagates to the brief: any machine's pointer tombstoned or deleted, or the
-- logical directory row deleted (erasure). The brief and its wakeup go; the next upload
-- arms a fresh wakeup.
CREATE FUNCTION sessions.session_briefs_forget()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
BEGIN
  DELETE FROM sessions.session_briefs AS b
   WHERE b.org_id = OLD.org_id AND b.workspace_id = OLD.workspace_id
     AND b.harness = OLD.harness AND b.session_id = OLD.session_id;
  DELETE FROM sessions.session_brief_wakeups AS w
   WHERE w.org_id = OLD.org_id AND w.workspace_id = OLD.workspace_id
     AND w.harness = OLD.harness AND w.session_id = OLD.session_id;
  RETURN NULL;
END
$forget$;

CREATE TRIGGER session_briefs_forget_tombstoned_pointer
  AFTER UPDATE OF deleted_at ON sessions.session_index
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_briefs_forget();
CREATE TRIGGER session_briefs_forget_deleted_pointer
  AFTER DELETE ON sessions.session_index
  FOR EACH ROW EXECUTE FUNCTION sessions.session_briefs_forget();
CREATE TRIGGER session_briefs_forget_tombstoned_directory
  AFTER UPDATE OF deleted_at ON sessions.session_directory
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_briefs_forget();
CREATE TRIGGER session_briefs_forget_deleted_directory
  AFTER DELETE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_briefs_forget();

-- The brief for one logical session in a claimed workspace, for relayhistory-cloud's
-- GET /v1/sessions/:id/brief. Zero rows: no live pointer (not found). `status` uses
-- relayhistory's SessionBriefStatus words plus 'missing' (no brief and nothing queued):
--   ready        the brief covers every live pointer at its current revision
--   pending      no brief yet; a wakeup is queued
--   refreshing   a wakeup is queued; the previous brief is returned
--   stale        an older brief is returned and nothing is queued
--   failed       the latest attempt failed (error has the code); a stale brief may be returned
--   rate_limited a daily budget refused the latest attempt (its wakeup waits for the next UTC
--                day); a stale brief may be returned
--   missing      no brief and nothing queued (briefs off, or never indexed with them on)
-- Ineligible, quarantined and empty attempts report 'failed' with their own error code.
CREATE FUNCTION sessions.session_brief(
  p_workspace_id text, p_harness text, p_session_id text
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  status text, stale boolean, title text, description text,
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

REVOKE ALL ON FUNCTION sessions.session_briefs_forget()
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_brief(text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_brief(text, text, text)
  TO session_directory_search_worker;

-- As in 20261031001000: strip EXECUTE from every grantee present at creation (default
-- privileges, a pre-existing runtime role), whatever the order. A grant added later is
-- still refused at call time by the role guard in sessions.session_directory_worker_scope.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_briefs_forget()'::regprocedure,
    'sessions.session_brief(text, text, text)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'session_directory_search_worker'
                  AND v_fn = 'sessions.session_brief(text, text, text)'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
