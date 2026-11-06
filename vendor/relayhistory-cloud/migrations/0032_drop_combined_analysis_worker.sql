-- The Worker that collected one combined analysis job per request is retired.
-- Its entry points go, and combined jobs it left unfinished are collected again as
-- separate brief and analysis jobs.

-- Admission runs while this migration applies. Take the per-organization lock that
-- session_brief_v2 and request_session_analysis_v1 hold while admitting, in a fixed
-- order, for every organization with an unfinished combined job (an analysis job
-- carrying summary versions).
SELECT pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || org_id, 0))
  FROM (SELECT DISTINCT org_id FROM sessions.session_analysis_jobs
         WHERE kind = 'analysis' AND summary_version <> '' AND status IN ('queued', 'running')
         ORDER BY org_id) AS affected;

-- Unfinished combined jobs complete without a brief under the current Worker, so
-- they are superseded. Each was charged one model admission when it was created or
-- requeued; charges from today are refunded so the replacement analysis can spend
-- them again. Admissions carry no per-job record, so a requeued job last updated
-- today counts as charged today; at worst this refunds one slot per superseded job. A session's latest
-- request returns to the collection queue when it was linked to one of these jobs
-- and the session has no request in flight; an older request stays as it is, since
-- the latest request or the next read covers the session. At most one request per
-- session is requeued.
WITH unfinished AS (
  SELECT id, org_id,
         created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
           OR (requeues > 0
               AND updated_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')
           AS charged_today
    FROM sessions.session_analysis_jobs
   WHERE kind = 'analysis' AND summary_version <> '' AND status IN ('queued', 'running')
     FOR UPDATE
), combined AS (
  UPDATE sessions.session_analysis_jobs AS j
     SET status = 'failed', last_error = 'superseded', lease_token = NULL,
         lease_expires_at = NULL, updated_at = now()
    FROM unfinished
   WHERE j.id = unfinished.id
  RETURNING j.id, j.org_id, unfinished.charged_today
), refunded AS (
  UPDATE sessions.session_analysis_admissions AS a
     SET requests = greatest(0, a.requests - refunds.refund)
    FROM (SELECT org_id, count(*)::integer AS refund FROM combined
           WHERE charged_today GROUP BY org_id) AS refunds
   WHERE a.org_id = refunds.org_id AND a.day = (now() AT TIME ZONE 'UTC')::date
  RETURNING a.org_id
)
UPDATE sessions.session_analysis_requests AS r
   SET status = 'queued', analysis_job_id = NULL, attempts = 0, available_at = now(),
       last_error = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = now()
  FROM combined
 WHERE r.analysis_job_id = combined.id AND r.status = 'resolved'
   AND r.id = (SELECT latest.id FROM sessions.session_analysis_requests AS latest
                WHERE latest.org_id = r.org_id AND latest.session_id = r.session_id
                ORDER BY latest.created_at DESC, latest.id DESC LIMIT 1)
   AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_requests AS active
                    WHERE active.org_id = r.org_id AND active.session_id = r.session_id
                      AND active.status IN ('queued', 'running'));

DROP FUNCTION sessions.session_brief_v1(text, text, text, text, text);
DROP FUNCTION sessions.session_source_revision(text, text);
DROP FUNCTION sessions.collect_session_analysis_request(uuid, uuid, text, text, text, integer, text, text, text, jsonb);
DROP FUNCTION sessions.enqueue_session_analysis_job(text, text, text, text, text, text, text, integer, text, text, text, jsonb);
