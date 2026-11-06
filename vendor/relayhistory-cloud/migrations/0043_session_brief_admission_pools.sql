-- Session brief admission is split into two daily pools per organization.
--
-- Until now one budget (20 distinct sessions per UTC day) covered every
-- admission. The event outbox (0034) admits every session that receives new
-- evidence, so ingest spent the whole budget minutes after 00:00Z. On
-- 2026-10-13 one workspace had at least 3,000 active sessions; sessions someone
-- actually asked about (dashboard detail, PR-linked merge-train sessions) were
-- rate_limited with no request for days.
--
-- * background: outbox/ingest wakeups. Unchanged budget of 20 sessions/day.
-- * demand: an explicit read (dashboard, PR-linked lookups). 200 sessions/day.
--
-- Model analysis spend is unchanged: enqueue_session_job_v2 still caps analysis
-- jobs at 20 per org per day via session_analysis_admissions. Only briefs gain
-- headroom. Both pools reset at the next UTC midnight.

-- Existing rows were admitted by ingest. ADD COLUMN with a constant default is
-- metadata-only; the default then flips so new explicit requests are demand.
ALTER TABLE sessions.session_analysis_requests
  ADD COLUMN admission text NOT NULL DEFAULT 'background';
ALTER TABLE sessions.session_analysis_requests
  ALTER COLUMN admission SET DEFAULT 'demand';

CREATE FUNCTION sessions.session_admission_budget(p_admission text)
RETURNS integer LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $admission_budget$
  SELECT CASE p_admission WHEN 'demand' THEN 200 ELSE 20 END;
$admission_budget$;

-- Distinct sessions (other than this one) admitted today from one pool.
CREATE FUNCTION sessions.sessions_admitted_today(p_org_id text, p_session_id text, p_admission text)
RETURNS integer LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $admitted_today_pool$
  SELECT count(DISTINCT session_id)::integer FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id <> p_session_id AND admission = p_admission
     AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
$admitted_today_pool$;

-- session_brief_v2 (0038) with an admission pool. Every other rule is unchanged.
CREATE FUNCTION sessions.session_brief_v3(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text,
  p_brief_engine text, p_analysis_engine text, p_admit boolean, p_admission text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_brief_v3$
DECLARE
  current_revision text;
  engine text;
  brief record;
  analysis record;
  brief_fresh boolean;
  analysis_fresh boolean;
  latest sessions.session_analysis_requests%ROWTYPE;
  brief_job sessions.session_analysis_jobs%ROWTYPE;
  analysis_job sessions.session_analysis_jobs%ROWTYPE;
  brief_busy boolean;
  analysis_busy boolean;
  brief_due boolean;
  analysis_due boolean;
  request_limit text;
  recent_failure text;
  exhausted boolean;
  retryable boolean;
  admitted_id uuid;
  limited boolean := false;
  request_status text;
  brief_error text;
  analysis_error text;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = ''
    OR p_brief_engine IS NULL OR btrim(p_brief_engine) = ''
    OR p_analysis_engine IS NULL OR btrim(p_analysis_engine) = ''
    OR p_admission IS NULL OR p_admission NOT IN ('demand', 'background') THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  -- Check evidence only after deletion/admission serialization has settled.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  engine := p_brief_engine || '#' || p_analysis_engine;
  -- Shared with request_session_analysis_v1 so both admission paths serialize.
  current_revision := sessions.current_session_revision(p_org_id, p_session_id);

  SELECT * INTO brief FROM sessions.cached_session_job(p_org_id, p_session_id, 'brief');
  SELECT * INTO analysis FROM sessions.cached_session_job(p_org_id, p_session_id, 'analysis');
  brief_fresh := brief.job_id IS NOT NULL AND brief.revision IS NOT DISTINCT FROM current_revision
    AND brief.engine = p_brief_engine;
  analysis_fresh := analysis.job_id IS NOT NULL AND analysis.revision IS NOT DISTINCT FROM current_revision
    AND analysis.engine = p_analysis_engine;

  SELECT * INTO latest FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;
  SELECT * INTO brief_job FROM sessions.session_analysis_jobs
   WHERE id = latest.brief_job_id AND org_id = p_org_id AND session_id = p_session_id;
  SELECT * INTO analysis_job FROM sessions.session_analysis_jobs
   WHERE id = latest.analysis_job_id AND org_id = p_org_id AND session_id = p_session_id;
  -- Busy means any job of that kind is in flight for the session, not only the
  -- latest request's, so a deferred analysis is not re-admitted on every read.
  brief_busy := COALESCE(latest.status IN ('queued', 'running'), false)
    OR EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                WHERE org_id = p_org_id AND session_id = p_session_id
                  AND kind = 'brief' AND status IN ('queued', 'running'));
  analysis_busy := COALESCE(latest.status IN ('queued', 'running'), false)
    OR EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                WHERE org_id = p_org_id AND session_id = p_session_id
                  AND kind = 'analysis' AND status IN ('queued', 'running'));

  exhausted := EXISTS (SELECT 1 FROM sessions.session_analysis_admissions
                        WHERE org_id = p_org_id AND day = (now() AT TIME ZONE 'UTC')::date
                          AND requests >= 20);

  -- Another attempt for the same revision and engine is made only when it can
  -- change the result: a transient failure after a cooldown, a failed job that
  -- still has its single requeue, a deferred analysis, or a refused analysis once
  -- model admission has room again.
  retryable := COALESCE(latest.id IS NULL
    OR latest.source_revision IS DISTINCT FROM current_revision
    -- The claim records the handling worker's engine, so every finished request
    -- has one; a request from before 0024 retries once under the current engine.
    OR latest.engine IS DISTINCT FROM engine
    OR (latest.status = 'failed'
        AND latest.last_error IN ('session_collection_failed', 'request_lease_expired')
        AND latest.updated_at <= now() - interval '1 hour')
    OR (latest.status = 'resolved' AND (
          (brief_job.status = 'failed' AND brief_job.requeues < 1)
       OR (analysis_job.status = 'failed' AND analysis_job.requeues < 1)
       OR (latest.analysis_job_id IS NULL AND latest.last_error IS NULL)
       OR (latest.analysis_job_id IS NULL AND NOT exhausted
           AND latest.updated_at <= now() - interval '1 hour'))), false);

  -- Each kind is judged on its own: a slow analysis never holds back a brief. An
  -- analysis starts at most once per 30 minutes per session and never while the
  -- org's model admission is exhausted.
  brief_due := NOT brief_fresh AND NOT brief_busy;
  analysis_due := NOT analysis_fresh AND NOT analysis_busy AND NOT exhausted
    AND NOT sessions.session_analysis_cooling(p_org_id, p_session_id);
  -- A throttled read returns the stale result with a non-polling status.
  request_limit := sessions.session_request_limit(p_org_id, p_session_id);

  IF p_admit AND (brief_due OR analysis_due) AND retryable AND request_limit IS DISTINCT FROM 'throttled' THEN
    IF request_limit = 'capped'
       OR sessions.sessions_admitted_today(p_org_id, p_session_id, p_admission)
          >= sessions.session_admission_budget(p_admission) THEN
      limited := true;
    ELSE
      INSERT INTO sessions.session_analysis_requests
        (org_id, workspace_id, session_id, requested_by_user_id, source_revision, engine, admission)
        VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, current_revision, engine, p_admission)
        RETURNING * INTO latest;
      admitted_id := latest.id;
      brief_job := NULL;
      analysis_job := NULL;
      -- Only the kinds that were due are now in flight; the other keeps its state.
      brief_busy := brief_busy OR brief_due;
      analysis_busy := analysis_busy OR analysis_due;
    END IF;
  END IF;

  request_status := CASE
    WHEN latest.status <> 'resolved' THEN latest.status
    WHEN brief_job.status = 'failed' OR analysis_job.status = 'failed' THEN 'failed'
    WHEN brief_job.status = 'running' OR analysis_job.status = 'running' THEN 'running'
    WHEN brief_job.status = 'queued' OR analysis_job.status = 'queued' THEN 'queued'
    ELSE 'completed' END;
  brief_error := CASE WHEN latest.status = 'failed' THEN latest.last_error
                      WHEN brief_job.status = 'failed' THEN brief_job.last_error END;
  -- An analysis deferred by a failed run's cooldown reports that failure.
  SELECT last_error INTO recent_failure FROM sessions.session_analysis_jobs
   WHERE org_id = p_org_id AND session_id = p_session_id AND kind = 'analysis'
     AND last_error IS DISTINCT FROM 'superseded' AND status IN ('completed', 'failed')
   ORDER BY COALESCE(completed_at, updated_at) DESC, id DESC LIMIT 1;
  analysis_error := CASE WHEN latest.status = 'failed' THEN latest.last_error
                         WHEN analysis_job.status = 'failed' THEN analysis_job.last_error
                         WHEN latest.status = 'resolved' AND latest.analysis_job_id IS NULL
                           THEN COALESCE(latest.last_error, recent_failure) END;

  RETURN jsonb_build_object(
    'outcome', 'ok',
    'sessionId', p_session_id,
    'admittedRequestId', admitted_id,
    'status', CASE
      WHEN brief_fresh THEN 'ready'
      WHEN brief_busy THEN CASE WHEN brief.job_id IS NULL THEN 'pending' ELSE 'refreshing' END
      WHEN limited THEN 'rate_limited'
      WHEN brief_error IS NOT NULL OR brief.job_id IS NULL THEN 'failed'
      ELSE 'stale' END,
    'stale', brief.job_id IS NOT NULL AND NOT brief_fresh,
    'briefId', brief.job_id,
    'brief', brief.result,
    'generatedAt', brief.completed_at,
    'error', CASE WHEN brief_fresh THEN NULL ELSE brief_error END,
    'analysis', jsonb_build_object(
      'status', CASE
        WHEN analysis_fresh THEN 'ready'
        WHEN analysis_busy THEN CASE WHEN analysis.job_id IS NULL THEN 'pending' ELSE 'refreshing' END
        WHEN limited OR exhausted OR analysis_error = 'session_analysis_rate_limited' THEN 'rate_limited'
        WHEN analysis_error IS NOT NULL THEN 'failed'
        WHEN analysis.job_id IS NULL THEN 'pending'
        ELSE 'stale' END,
      'stale', analysis.job_id IS NOT NULL AND NOT analysis_fresh,
      'id', analysis.job_id,
      'result', analysis.result,
      'generatedAt', analysis.completed_at,
      'error', CASE WHEN analysis_fresh THEN NULL ELSE analysis_error END),
    'request', CASE WHEN latest.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', latest.id, 'sessionId', latest.session_id, 'status', request_status,
      'analysisId', latest.analysis_job_id,
      'error', COALESCE(brief_error, analysis_error),
      'createdAt', latest.created_at, 'updatedAt', latest.updated_at) END);
END;
$session_brief_v3$;

-- The v2 entry points stay for workers mid-rollout. They admit from the
-- background pool so an old worker can never spend the demand reserve.
CREATE OR REPLACE FUNCTION sessions.session_brief_v2(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text,
  p_brief_engine text, p_analysis_engine text, p_admit boolean
) RETURNS jsonb LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $brief_v2_pool$
  SELECT sessions.session_brief_v3(p_org_id, p_workspace_id, p_user_id, p_session_id,
    p_brief_engine, p_analysis_engine, p_admit, 'background');
$brief_v2_pool$;

REVOKE ALL ON FUNCTION sessions.session_admission_budget(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.sessions_admitted_today(text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_brief_v3(text, text, text, text, text, text, boolean, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_brief_v2(text, text, text, text, text, text, boolean) FROM PUBLIC;
