-- Test oracle extracted exactly from RHC 9ff3a285cf2d7ac617ad4a2a59ffc6931a60fecb
-- 0031_v1_brief_projection.sql and 0024_session_brief_jobs.sql read function.
-- Cloud's v1 session-summary reader only needs the quick brief. The 0024
-- compatibility projection accidentally withheld that result until the full
-- analysis also finished, defeating the separate brief lane. Preserve the v1
-- status/error lifecycle, but publish a completed brief in the result immediately.
CREATE OR REPLACE FUNCTION sessions.session_requests_v1_projection(
  p_org_id text, p_session_id text
) RETURNS TABLE (
  request_id uuid, session_id text, status text, error text, job_id uuid,
  request_created_at timestamptz, request_updated_at timestamptz,
  source_provider text, source_fingerprint text, attempts integer,
  analysis_version text, schema_version integer, summary_version text,
  model_version text, summary_model_version text, result jsonb,
  job_created_at timestamptz, job_updated_at timestamptz, job_completed_at timestamptz
) LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $v1_projection$
  SELECT r.id, r.session_id,
    CASE WHEN r.status <> 'resolved' THEN r.status
         WHEN b.id IS NULL THEN COALESCE(a.status, 'failed')
         WHEN b.status = 'failed' OR a.status = 'failed' THEN 'failed'
         WHEN b.status = 'running' OR a.status = 'running' THEN 'running'
         WHEN b.status = 'queued' OR a.status = 'queued' THEN 'queued'
         WHEN a.id IS NULL AND r.last_error IS NOT NULL THEN 'failed'
         ELSE 'completed' END,
    CASE WHEN r.status = 'failed' THEN r.last_error
         WHEN r.status = 'resolved' THEN COALESCE(
           CASE WHEN b.status = 'failed' THEN b.last_error END,
           CASE WHEN a.status = 'failed' THEN a.last_error END,
           CASE WHEN a.id IS NULL THEN r.last_error END) END,
    -- Each request is its own v1 job, so no two requests share an ID with
    -- different states. Requests from before 0024 keep their analysis job's ID.
    CASE WHEN b.id IS NULL THEN a.id ELSE md5('v1-job:' || r.id::text)::uuid END,
    r.created_at, r.updated_at,
    COALESCE(a.source_provider, b.source_provider),
    COALESCE(a.source_fingerprint, b.source_fingerprint),
    COALESCE(a.attempts, b.attempts),
    COALESCE(a.analysis_version, ''), COALESCE(a.schema_version, 0),
    COALESCE(NULLIF(b.summary_version, ''), a.summary_version, ''),
    COALESCE(a.model_version, ''),
    COALESCE(NULLIF(b.summary_model_version, ''), a.summary_model_version, ''),
    CASE WHEN b.id IS NULL THEN a.result
         WHEN b.status = 'completed' THEN
           CASE WHEN a.status = 'completed' THEN COALESCE(a.result, '{}'::jsonb)
                ELSE '{}'::jsonb END || jsonb_build_object('brief', b.result) END,
    COALESCE(a.created_at, b.created_at),
    GREATEST(a.updated_at, b.updated_at),
    CASE WHEN b.id IS NULL THEN a.completed_at
         WHEN b.status = 'completed' AND (a.id IS NULL OR a.status = 'completed')
           THEN GREATEST(a.completed_at, b.completed_at) END
    FROM sessions.session_analysis_requests AS r
    -- A superseded analysis moved to a newer request; this request keeps its brief.
    LEFT JOIN sessions.session_analysis_jobs AS a ON a.id = r.analysis_job_id
      AND a.org_id = r.org_id AND a.session_id = r.session_id
      AND NOT (a.status = 'failed' AND a.last_error = 'superseded' AND r.brief_job_id IS NOT NULL)
    LEFT JOIN sessions.session_analysis_jobs AS b ON b.id = r.brief_job_id
      AND b.org_id = r.org_id AND b.session_id = r.session_id
   WHERE r.org_id = p_org_id AND r.session_id = p_session_id;
$v1_projection$;

CREATE OR REPLACE FUNCTION sessions.read_session_analysis_v1(
  p_org_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $read_analysis$
DECLARE
  public_requests jsonb;
  public_analyses jsonb;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_requests WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_jobs WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', v.request_id, 'sessionId', v.session_id, 'status', v.status,
    'analysisId', v.job_id, 'error', v.error,
    'createdAt', v.request_created_at, 'updatedAt', v.request_updated_at
  ) ORDER BY v.request_created_at DESC, v.request_id DESC), '[]'::jsonb) INTO public_requests
  FROM (SELECT * FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
         ORDER BY request_created_at DESC, request_id DESC LIMIT 50) AS v;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', v.job_id, 'sessionId', v.session_id, 'sourceProvider', v.source_provider,
    'sourceFingerprint', v.source_fingerprint, 'status', v.status, 'attempts', v.attempts,
    'analysisVersion', v.analysis_version, 'schemaVersion', v.schema_version,
    'summaryVersion', v.summary_version, 'modelVersion', v.model_version,
    'summaryModelVersion', v.summary_model_version, 'result', v.result,
    'error', CASE WHEN v.status = 'failed' THEN v.error END,
    'createdAt', v.job_created_at, 'updatedAt', v.job_updated_at, 'completedAt', v.job_completed_at
  ) ORDER BY v.job_created_at DESC, v.job_id DESC), '[]'::jsonb) INTO public_analyses
  FROM (SELECT DISTINCT ON (job_id) *
          FROM (SELECT * FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
                 ORDER BY request_created_at DESC, request_id DESC LIMIT 50) AS recent
         WHERE job_id IS NOT NULL
         ORDER BY job_id, request_created_at DESC, request_id DESC) AS v;

  RETURN jsonb_build_object('outcome', 'ok', 'sessionId', p_session_id,
    'requests', public_requests, 'analyses', public_analyses);
END;
$read_analysis$;
