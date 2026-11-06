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
