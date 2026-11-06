/** Proven provider evidence is scoped by the full legacy namespace. */
import {resolvedPublicTurnSourceSql} from "../shared/legacy-turn-source.ts";
export const ANALYSIS_NAMESPACE_SOURCES_SQL=`SELECT source FROM (
 SELECT e.source FROM sessions.convergence_events e WHERE e.org_id=$1 AND e.session_id=$2 AND e.workspace_id=$3
 UNION SELECT ${resolvedPublicTurnSourceSql("t")} AS source FROM sessions.conversation_turns t WHERE t.org_id=$1 AND t.session_id=$2 AND t.workspace_id=$3
 UNION SELECT j.source_provider FROM sessions.session_analysis_jobs j WHERE j.org_id=$1 AND j.session_id=$2
  AND j.source->'evidenceWorkspaceIds'=to_jsonb(ARRAY[$3::text])
 ) proven WHERE source IS NOT NULL AND source<>''`;
