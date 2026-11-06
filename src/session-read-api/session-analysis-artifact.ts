/** Shared immutable analysis-cache envelope. Public payload rows stay owned by
 * the RHC v1 SQL projection; private identity/ordinal fields are out of band. */
import type {ArtifactIdentity,ArtifactRef,ArtifactSource} from "./canonical-artifact.ts";
export const ANALYSIS_ARTIFACT_VERSION=1 as const;
export const ANALYSIS_ARTIFACT_TABLES=["session_analysis_requests","session_analyses"] as const;
export const ANALYSIS_MAX_HISTORY=50;
/** Matches the selected public Cloud/Worker aggregate response ceiling. */
export const ANALYSIS_MAX_ENVELOPE_BYTES=16*1024*1024;
export const ANALYSIS_TRANSPORT_RESERVE_BYTES=64*1024;
export const ANALYSIS_MAX_SNAPSHOT_BYTES=ANALYSIS_MAX_ENVELOPE_BYTES-ANALYSIS_TRANSPORT_RESERVE_BYTES;
export const ANALYSIS_FRESH_SECONDS=120;
export interface CachedBriefTitle {version:1;complete:true;title:string|null}
export interface SessionAnalysisIndex {
 version:1;kind:"session-analysis-index";codec:"canonical-analysis-graph-v1";
 projection:"read_session_analysis_v1/0031";outcome:"ok"|"not_found";
 identity:ArtifactIdentity;source:ArtifactSource;generation:string;
 snapshotAt:string;snapshotId:string;snapshotSha256:string;
 cachedBriefTitle?:CachedBriefTitle;sourceSnapshotSha256?:string;
 graph:ArtifactRef;counts:{requests:number;analyses:number};
}
export interface SessionAnalysisArtifactRow {
 org_id:string;workspace_id:string;source:string;session_id:string;
 ordinal:number;payload:Record<string,unknown>;
}
export interface SessionAnalysisSnapshot {
 snapshotAt:string;snapshotId:string;cachedBriefTitle?:CachedBriefTitle;
 payload:{outcome:"not_found"}|{outcome:"ok";sessionId:string;requests:Record<string,unknown>[];analyses:Record<string,unknown>[]};
}

/** Fixed envelope key order on both sides; public row keys/values are preserved. */
export function analysisSnapshotJson(payload:SessionAnalysisSnapshot["payload"]):string {
 return payload.outcome==="not_found"?JSON.stringify({outcome:"not_found"}):JSON.stringify({outcome:"ok",sessionId:payload.sessionId,requests:payload.requests,analyses:payload.analyses});
}

/** Generation tuple is independent of JSONB/RPC property insertion order. */
/** Bind compact title metadata without changing the public v1 envelope digest. */
export function analysisSourceSnapshotJson(snapshotSha256:string,title:CachedBriefTitle):string {
 if(!/^[0-9a-f]{64}$/u.test(snapshotSha256)||title?.version!==1||title.complete!==true||!(title.title===null||typeof title.title==="string"&&title.title.length>0&&title.title.trim()===title.title))throw new Error("analysis_title_attestation_invalid");
 return JSON.stringify(["analysis-source-snapshot-v2",snapshotSha256,{version:1,complete:true,title:title.title}]);
}

/** Optional source digest selects v2; five-argument persisted v1 stays byte-exact. */
export function analysisGenerationJson(identity:ArtifactIdentity,source:ArtifactSource,snapshotAt:string,snapshotId:string,snapshotSha256:string,sourceSnapshotSha256?:string):string {
 const fields=[{orgId:identity.orgId,workspaceId:identity.workspaceId,source:identity.source,sessionId:identity.sessionId},{epoch:source.epoch,fingerprint:source.fingerprint},snapshotAt,snapshotId,snapshotSha256];
 if(sourceSnapshotSha256===undefined)return JSON.stringify(["session-analysis-cache-v1",...fields]);
 if(!/^[0-9a-f]{64}$/u.test(sourceSnapshotSha256))throw new Error("analysis_source_digest_invalid");
 return JSON.stringify(["session-analysis-cache-v2",...fields,sourceSnapshotSha256]);
}
