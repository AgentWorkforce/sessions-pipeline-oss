-- Every session gets a brief once it goes idle, and session search ranks on it.
--
-- 1. Idle wakeups. A session's outbox wakeup (0034) came due 5 seconds after
--    its first new evidence, capped at 30 seconds, so an active session was
--    re-briefed while it was still being written (up to the 30/day per-session
--    cap). The wakeup is now a trailing debounce: each revision pushes it to
--    10 minutes after the latest evidence, capped at 60 minutes after the
--    wakeup was first recorded, so a long-running session still gets a brief
--    roughly hourly. Request and job wakeups are unchanged. An already-sent
--    message that arrives before its pushed-back due time is superseded by the
--    newer generation (see consumeSessionEvent), never retried into the DLQ.
--
-- 2. Background budget. The background pool (0043) admitted 20 sessions per org
--    per UTC day, so almost no session of a busy workspace got a brief. The
--    budget now defaults to 5,000 and a deployment may override it per call with
--    the transaction-local setting `relayhistory.background_brief_budget`
--    (Worker var SESSION_BRIEF_BACKGROUND_DAILY_BUDGET). The demand pool (200),
--    the per-session limits and the analysis cap (20 jobs/org/day,
--    session_analysis_admissions) are unchanged, so model analysis spend does
--    not change.
--
-- 3. Brief search index. `session_brief_search` holds the latest completed
--    brief per session (title + description, at most 410 characters) and its
--    text-embedding-3-small vector. A brief job's completion upserts the row
--    with no embedding; the Worker's minute tick embeds pending rows in bounded
--    batches. An unchanged brief text keeps its embedding. The row cascades
--    with the brief job, so upload deletion and reset remove it with the brief.
--    `evidence_workspace_ids` copies the brief job's frozen input provenance
--    (source.evidenceWorkspaceIds). A workspace-scoped search uses a brief only
--    when that provenance is exactly the requested workspace and the session
--    has no evidence in another workspace, the same rules as the brief route
--    (attestSessionWorkspace, scopedJobSafe).
--    Existing briefs are indexed by scripts/backfill-session-briefs.mjs, not
--    here, so this migration stays a quick DDL transaction.

CREATE OR REPLACE FUNCTION sessions.record_session_wakeup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $wake$
DECLARE k text; t text; due timestamptz;
BEGIN
  IF TG_TABLE_NAME = 'session_revisions' THEN
    k := 'session'; t := NEW.session_id; due := now() + interval '10 minutes';
  ELSE
    IF NEW.status NOT IN ('queued', 'running') THEN RETURN NEW; END IF;
    IF TG_TABLE_NAME = 'session_analysis_requests' THEN k := 'request'; ELSE k := NEW.kind; END IF;
    t := NEW.id::text;
    due := CASE WHEN NEW.status = 'running' THEN NEW.lease_expires_at ELSE NEW.available_at END;
  END IF;
  INSERT INTO sessions.session_event_outbox AS o (kind,target,org_id,session_id,available_at)
    VALUES (k,t,NEW.org_id,NEW.session_id,due)
    ON CONFLICT (kind,org_id,target) DO UPDATE SET generation=o.generation+1,
      available_at=CASE WHEN k='session' THEN LEAST(EXCLUDED.available_at,o.created_at+interval '60 minutes') ELSE EXCLUDED.available_at END, sent_at=NULL, quarantined_at=NULL;
  RETURN NEW;
END;
$wake$;

-- STABLE, not IMMUTABLE: the background budget reads a transaction-local setting.
-- A missing, non-numeric or out-of-range setting falls back to 5000.
CREATE OR REPLACE FUNCTION sessions.session_admission_budget(p_admission text)
RETURNS integer LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $admission_budget$
  SELECT CASE p_admission
    WHEN 'demand' THEN 200
    ELSE COALESCE((
      SELECT v::integer FROM (SELECT current_setting('relayhistory.background_brief_budget', true) AS v) s
       WHERE v ~ '^[0-9]{1,7}$' AND v::integer BETWEEN 0 AND 1000000), 5000)
  END;
$admission_budget$;

CREATE TABLE sessions.session_brief_search (
  org_id text NOT NULL,
  session_id text NOT NULL,
  workspace_id text,
  source text NOT NULL,
  brief_job_id uuid NOT NULL REFERENCES sessions.session_analysis_jobs (id) ON DELETE CASCADE,
  evidence_workspace_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  title text NOT NULL,
  description text NOT NULL,
  content_hash text NOT NULL,
  brief_completed_at timestamptz NOT NULL,
  embedding public.vector(1536),
  embedding_model text,
  embedded_at timestamptz,
  embedding_attempts integer NOT NULL DEFAULT 0,
  embedding_available_at timestamptz NOT NULL DEFAULT now(),
  embedding_error text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, session_id)
);
CREATE INDEX session_brief_search_job_idx ON sessions.session_brief_search (brief_job_id);
CREATE INDEX session_brief_search_recent_idx ON sessions.session_brief_search (org_id, brief_completed_at DESC);
CREATE INDEX session_brief_search_pending_idx ON sessions.session_brief_search (embedding_available_at)
  WHERE embedding IS NULL AND embedding_attempts < 5;
CREATE INDEX IF NOT EXISTS session_brief_search_embedding_hnsw_idx
  ON sessions.session_brief_search USING hnsw (embedding public.vector_cosine_ops)
  WHERE embedding IS NOT NULL;

-- Index one completed brief job. Idempotent: an older completion never replaces
-- a newer one, and an unchanged text keeps its embedding. Returns whether the
-- row was written.
CREATE FUNCTION sessions.index_session_brief_job(p_job_id uuid)
RETURNS boolean LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $index_brief$
DECLARE j sessions.session_analysis_jobs%ROWTYPE; t text; d text; h text; written boolean;
BEGIN
  SELECT * INTO j FROM sessions.session_analysis_jobs WHERE id = p_job_id;
  IF NOT FOUND OR j.kind <> 'brief' OR j.status <> 'completed'
     OR jsonb_typeof(j.result) IS DISTINCT FROM 'object' THEN
    RETURN false;
  END IF;
  t := left(btrim(COALESCE(j.result->>'title', '')), 90);
  d := left(btrim(COALESCE(j.result->>'description', '')), 320);
  IF t = '' THEN RETURN false; END IF;
  h := md5(t || E'\n' || d);
  INSERT INTO sessions.session_brief_search AS s
    (org_id, session_id, workspace_id, source, brief_job_id, evidence_workspace_ids,
     title, description, content_hash, brief_completed_at)
    VALUES (j.org_id, j.session_id, NULLIF(j.workspace_id, ''), j.source_provider, j.id,
            CASE WHEN jsonb_typeof(j.source->'evidenceWorkspaceIds') = 'array'
                 THEN j.source->'evidenceWorkspaceIds' ELSE '[]'::jsonb END,
            t, d, h, COALESCE(j.completed_at, now()))
    ON CONFLICT (org_id, session_id) DO UPDATE SET
      workspace_id = EXCLUDED.workspace_id,
      source = EXCLUDED.source,
      brief_job_id = EXCLUDED.brief_job_id,
      evidence_workspace_ids = EXCLUDED.evidence_workspace_ids,
      title = EXCLUDED.title,
      description = EXCLUDED.description,
      content_hash = EXCLUDED.content_hash,
      brief_completed_at = EXCLUDED.brief_completed_at,
      embedding = CASE WHEN s.content_hash = EXCLUDED.content_hash THEN s.embedding END,
      embedding_model = CASE WHEN s.content_hash = EXCLUDED.content_hash THEN s.embedding_model END,
      embedded_at = CASE WHEN s.content_hash = EXCLUDED.content_hash THEN s.embedded_at END,
      embedding_attempts = CASE WHEN s.content_hash = EXCLUDED.content_hash THEN s.embedding_attempts ELSE 0 END,
      embedding_available_at = CASE WHEN s.content_hash = EXCLUDED.content_hash THEN s.embedding_available_at ELSE now() END,
      embedding_error = CASE WHEN s.content_hash = EXCLUDED.content_hash THEN s.embedding_error END,
      updated_at = now()
    WHERE s.brief_completed_at <= EXCLUDED.brief_completed_at
    RETURNING true INTO written;
  RETURN COALESCE(written, false);
END;
$index_brief$;

CREATE FUNCTION sessions.record_brief_search() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $record_brief_search$
BEGIN
  IF NEW.kind = 'brief' AND NEW.status = 'completed' AND OLD.status IS DISTINCT FROM 'completed' THEN
    PERFORM sessions.index_session_brief_job(NEW.id);
  END IF;
  RETURN NEW;
END;
$record_brief_search$;
CREATE TRIGGER session_brief_search_index AFTER UPDATE OF status ON sessions.session_analysis_jobs
FOR EACH ROW EXECUTE FUNCTION sessions.record_brief_search();

REVOKE ALL ON FUNCTION sessions.session_admission_budget(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.index_session_brief_job(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.record_brief_search() FROM PUBLIC;
