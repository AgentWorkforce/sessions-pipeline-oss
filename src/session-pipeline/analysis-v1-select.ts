/** Pure SELECT equivalent of RHC0031 v1 projection +0024 public envelope.
 * Source: RHC9ff3a285cf2d7ac617ad4a2a59ffc6931a60fecb. Runtime needs SELECT
 * on the four approved tables, no EXECUTE on source application functions.
 * Actual upstream SQL functions remain independent test oracles. */
import {ANALYSIS_NAMESPACE_SOURCES_SQL} from "./analysis-scope.ts";
export const ANALYSIS_V1_SELECT_SQL=String.raw`WITH namespace_sources AS MATERIALIZED (${ANALYSIS_NAMESPACE_SOURCES_SQL}), projection(request_id,session_id,status,error,job_id,request_created_at,request_updated_at,source_provider,source_fingerprint,attempts,analysis_version,schema_version,summary_version,model_version,summary_model_version,result,job_created_at,job_updated_at,job_completed_at) AS MATERIALIZED (
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
   WHERE r.org_id = $1 AND r.session_id = $2 AND r.workspace_id=$3
      AND (COALESCE(a.source_provider,b.source_provider)=$4 OR (
       COALESCE(a.source_provider,b.source_provider) IS NULL
       AND (SELECT count(*) FROM namespace_sources)=1 AND EXISTS(SELECT 1 FROM namespace_sources WHERE source=$4)))
)
SELECT CASE WHEN $1 IS NOT NULL AND length($1) BETWEEN 1 AND 512 AND btrim($1)<>'' AND $2 IS NOT NULL AND length($2) BETWEEN 1 AND 512 AND btrim($2)<>'' AND (
 EXISTS(SELECT 1 FROM namespace_sources WHERE source=$4) OR EXISTS(SELECT 1 FROM projection)
) THEN jsonb_build_object('outcome','ok','sessionId',$2,'requests',(SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', v.request_id, 'sessionId', v.session_id, 'status', v.status,
    'analysisId', v.job_id, 'error', v.error,
    'createdAt', v.request_created_at, 'updatedAt', v.request_updated_at
  ) ORDER BY v.request_created_at DESC, v.request_id DESC), '[]'::jsonb)
  FROM (SELECT * FROM projection
         ORDER BY request_created_at DESC, request_id DESC LIMIT 50) AS v),'analyses',(SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', v.job_id, 'sessionId', v.session_id, 'sourceProvider', v.source_provider,
    'sourceFingerprint', v.source_fingerprint, 'status', v.status, 'attempts', v.attempts,
    'analysisVersion', v.analysis_version, 'schemaVersion', v.schema_version,
    'summaryVersion', v.summary_version, 'modelVersion', v.model_version,
    'summaryModelVersion', v.summary_model_version, 'result', v.result,
    'error', CASE WHEN v.status = 'failed' THEN v.error END,
    'createdAt', v.job_created_at, 'updatedAt', v.job_updated_at, 'completedAt', v.job_completed_at
  ) ORDER BY v.job_created_at DESC, v.job_id DESC), '[]'::jsonb)
  FROM (SELECT DISTINCT ON (job_id) *
          FROM (SELECT * FROM projection
                 ORDER BY request_created_at DESC, request_id DESC LIMIT 50) AS recent
         WHERE job_id IS NOT NULL
         ORDER BY job_id, request_created_at DESC, request_id DESC) AS v)) ELSE jsonb_build_object('outcome','not_found') END AS payload,
 EXISTS(SELECT 1 FROM sessions.session_analysis_requests r
  LEFT JOIN sessions.session_analysis_jobs a ON a.id=r.analysis_job_id AND a.org_id=r.org_id AND a.session_id=r.session_id
   AND NOT(a.status='failed' AND a.last_error='superseded' AND r.brief_job_id IS NOT NULL)
  LEFT JOIN sessions.session_analysis_jobs b ON b.id=r.brief_job_id AND b.org_id=r.org_id AND b.session_id=r.session_id
  WHERE r.org_id=$1 AND r.session_id=$2 AND r.workspace_id=$3
   AND COALESCE(a.source_provider,b.source_provider) IS NULL AND (SELECT count(*) FROM namespace_sources)<>1
 ) AS unbound_requests`;
