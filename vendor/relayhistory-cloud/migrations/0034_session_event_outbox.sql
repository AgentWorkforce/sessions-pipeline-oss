-- Database truth and wakeups commit together, including legacy Cloud SQL admission.
-- Wakeups are coalesced per target; generation fencing prevents a concurrent ingest
-- from being acknowledged by an older delivery. No transcript enters either queue.
CREATE TABLE sessions.session_event_outbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kind text NOT NULL CHECK (kind IN ('session', 'request', 'brief', 'analysis')),
  target text NOT NULL,
  org_id text NOT NULL,
  session_id text NOT NULL,
  generation bigint NOT NULL DEFAULT 1,
  available_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz,
  quarantined_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (kind, org_id, target)
);
CREATE INDEX session_event_outbox_pending ON sessions.session_event_outbox (available_at, sent_at);
CREATE FUNCTION sessions.record_session_wakeup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $wake$
DECLARE k text; t text; due timestamptz;
BEGIN
  IF TG_TABLE_NAME = 'session_revisions' THEN
    k := 'session'; t := NEW.session_id; due := now() + interval '5 seconds';
  ELSE
    IF NEW.status NOT IN ('queued', 'running') THEN RETURN NEW; END IF;
    IF TG_TABLE_NAME = 'session_analysis_requests' THEN k := 'request'; ELSE k := NEW.kind; END IF;
    t := NEW.id::text;
    due := CASE WHEN NEW.status = 'running' THEN NEW.lease_expires_at ELSE NEW.available_at END;
  END IF;
  INSERT INTO sessions.session_event_outbox AS o (kind,target,org_id,session_id,available_at)
    VALUES (k,t,NEW.org_id,NEW.session_id,due)
    ON CONFLICT (kind,org_id,target) DO UPDATE SET generation=o.generation+1,
      available_at=CASE WHEN k='session' THEN LEAST(EXCLUDED.available_at,o.created_at+interval '30 seconds') ELSE EXCLUDED.available_at END, sent_at=NULL, quarantined_at=NULL;
  RETURN NEW;
END;
$wake$;
CREATE TRIGGER session_revision_wakeup AFTER INSERT OR UPDATE ON sessions.session_revisions
FOR EACH ROW EXECUTE FUNCTION sessions.record_session_wakeup();
CREATE TRIGGER session_request_wakeup AFTER INSERT OR UPDATE OF status, available_at, lease_expires_at ON sessions.session_analysis_requests
FOR EACH ROW EXECUTE FUNCTION sessions.record_session_wakeup();
CREATE TRIGGER session_job_wakeup AFTER INSERT OR UPDATE OF status, available_at, lease_expires_at ON sessions.session_analysis_jobs
FOR EACH ROW EXECUTE FUNCTION sessions.record_session_wakeup();
-- Existing admitted work is recovered without waiting for a dashboard read.
INSERT INTO sessions.session_event_outbox (kind,target,org_id,session_id,available_at)
SELECT 'request',id::text,org_id,session_id,COALESCE(lease_expires_at,available_at)
FROM sessions.session_analysis_requests WHERE status IN ('queued','running');
INSERT INTO sessions.session_event_outbox (kind,target,org_id,session_id,available_at)
SELECT kind,id::text,org_id,session_id,COALESCE(lease_expires_at,available_at)
FROM sessions.session_analysis_jobs WHERE status IN ('queued','running');

CREATE TABLE sessions.brief_completion_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL UNIQUE,
  org_id text NOT NULL,
  workspace_id text,
  user_id text NOT NULL,
  session_id text NOT NULL,
  source text NOT NULL,
  source_fingerprint text NOT NULL,
  source_revision text,
  completed_at timestamptz NOT NULL,
  sent_at timestamptz,
  acknowledged_at timestamptz
);
CREATE INDEX brief_completion_pending ON sessions.brief_completion_events (sent_at) WHERE acknowledged_at IS NULL;
CREATE FUNCTION sessions.record_brief_completion() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $completion$
BEGIN
  IF NEW.kind='brief' AND NEW.status='completed' AND OLD.status IS DISTINCT FROM 'completed' THEN
    INSERT INTO sessions.brief_completion_events
      (job_id,org_id,workspace_id,user_id,session_id,source,source_fingerprint,source_revision,completed_at)
    SELECT NEW.id,NEW.org_id,NEW.workspace_id,NEW.requested_by_user_id,NEW.session_id,
      NEW.source_provider,NEW.source_fingerprint,
      (SELECT r.source_revision FROM sessions.session_analysis_requests r WHERE r.brief_job_id=NEW.id
       ORDER BY r.created_at DESC,r.id DESC LIMIT 1),NEW.completed_at
    ON CONFLICT (job_id) DO NOTHING;
  END IF;
  RETURN NEW;
END;
$completion$;
CREATE TRIGGER brief_completion_event AFTER UPDATE OF status ON sessions.session_analysis_jobs
FOR EACH ROW EXECUTE FUNCTION sessions.record_brief_completion();
CREATE FUNCTION sessions.read_brief_completion_event_v1(p_id uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog,sessions,pg_temp AS $read$
 SELECT jsonb_build_object('eventId',id,'jobId',job_id,'orgId',org_id,'workspaceId',workspace_id,
   'userId',user_id,'sessionId',session_id,'source',source,'sourceFingerprint',source_fingerprint,
   'sourceRevision',source_revision,'completedAt',completed_at)
 FROM sessions.brief_completion_events WHERE id=p_id;
$read$;
CREATE FUNCTION sessions.pending_brief_completion_events_v1(p_limit integer) RETURNS TABLE(event_id uuid)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog,sessions,pg_temp AS $pending$
 SELECT id FROM sessions.brief_completion_events WHERE acknowledged_at IS NULL
 ORDER BY completed_at,id LIMIT LEAST(100,GREATEST(1,p_limit));
$pending$;
CREATE FUNCTION sessions.ack_brief_completion_events_v1(p_ids uuid[]) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog,sessions,pg_temp AS $ack$
 UPDATE sessions.brief_completion_events SET acknowledged_at=now() WHERE id=ANY(p_ids);
$ack$;
REVOKE ALL ON FUNCTION sessions.read_brief_completion_event_v1(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.pending_brief_completion_events_v1(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.ack_brief_completion_events_v1(uuid[]) FROM PUBLIC;

-- Preserve the six-argument admission API; read-only project workers use the
-- explicit seventh argument and cannot fan out additional model jobs.
CREATE FUNCTION sessions.session_brief_v2(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text,
  p_brief_engine text, p_analysis_engine text, p_admit boolean
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_brief$
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
    OR p_analysis_engine IS NULL OR btrim(p_analysis_engine) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  engine := p_brief_engine || '#' || p_analysis_engine;
  -- Shared with request_session_analysis_v1 so both admission paths serialize.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
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
    IF request_limit = 'capped' OR sessions.sessions_admitted_today(p_org_id, p_session_id) >= 20 THEN
      limited := true;
    ELSE
      INSERT INTO sessions.session_analysis_requests
        (org_id, workspace_id, session_id, requested_by_user_id, source_revision, engine)
        VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, current_revision, engine)
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
$session_brief$;
CREATE OR REPLACE FUNCTION sessions.session_brief_v2(
 p_org_id text,p_workspace_id text,p_user_id text,p_session_id text,p_brief_engine text,p_analysis_engine text
) RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $brief_compat$
 SELECT sessions.session_brief_v2(p_org_id,p_workspace_id,p_user_id,p_session_id,p_brief_engine,p_analysis_engine,true);
$brief_compat$;
REVOKE ALL ON FUNCTION sessions.session_brief_v2(text,text,text,text,text,text,boolean) FROM PUBLIC;
