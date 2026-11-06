/** Dormant readonly legacy-analysis snapshot -> immutable object graph -> exact
 * source-epoch/monotonic-snapshot CAS. Runtime refresh wiring is separate. */
import {objectKey,readAll,type ObjectStore} from "../../packages/object-store/src/index.ts";
import type {SqlExecutor} from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import type {ArtifactIdentity,ArtifactSource} from "../session-read-api/canonical-artifact.ts";
import {ARTIFACT_MAX_OBJECT_BYTES} from "../session-read-api/canonical-artifact.ts";
import {ANALYSIS_ARTIFACT_TABLES,ANALYSIS_MAX_SNAPSHOT_BYTES,analysisSnapshotJson,analysisGenerationJson,analysisSourceSnapshotJson,type SessionAnalysisIndex,type SessionAnalysisSnapshot} from "../session-read-api/session-analysis-artifact.ts";
import {materializeCanonicalArtifacts,artifactDigest,ArtifactBuildError} from "./replay-pages.ts";
import {readLegacyAnalysisSnapshot,type ReadonlyAnalysisSource} from "./analysis-snapshot.ts";
import type {ReplayCapture} from "./replay-publication.ts";
export interface AnalysisPublisherDeps {db:SqlExecutor;store:ObjectStore;source:ReadonlyAnalysisSource;lease:{nonce:string;source:ArtifactSource}}
function refuse(code:string):never{throw new ArtifactBuildError(code)}
export function analysisArtifactGeneration(identity:ArtifactIdentity,source:ArtifactSource,snapshot:SessionAnalysisSnapshot,snapshotSha256:string){return artifactDigest(analysisGenerationJson(identity,source,snapshot.snapshotAt,snapshot.snapshotId,snapshotSha256,snapshot.cachedBriefTitle===undefined?undefined:artifactDigest(analysisSourceSnapshotJson(snapshotSha256,snapshot.cachedBriefTitle))))}
async function capture(db:SqlExecutor,identity:ArtifactIdentity){const c=(await db.query<{c:ReplayCapture}>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0]?.c;
 if(!c||c.identity?.orgId!==identity.orgId||c.identity.workspaceId!==identity.workspaceId||c.identity.source!==identity.source||c.identity.sessionId!==identity.sessionId)refuse("analysis_capture_invalid");
 if(typeof c.exists!=="boolean"||typeof c.blocked!=="boolean")refuse("analysis_capture_invalid");
 if(c.exists&&(!/^[1-9][0-9]*$/u.test(c.source?.epoch??"")||!/^[0-9a-f]{64}$/u.test(c.source?.fingerprint??"")||!c.snapshot?.pointers?.length))refuse("analysis_capture_invalid");return c;
}
export async function buildSessionAnalysisArtifacts(store:ObjectStore,identity:ArtifactIdentity,source:ArtifactSource,prefix:string,snapshot:SessionAnalysisSnapshot){
 const payloadBytes=new TextEncoder().encode(analysisSnapshotJson(snapshot.payload));
 // Explicit assembly refusal; proving a given legal payload fits workerd remains
 // a consumer capacity gate. Never silently truncate analysis/history fields.
 if(payloadBytes.length>ANALYSIS_MAX_SNAPSHOT_BYTES)refuse("analysis_response_bound");
 if(snapshot.cachedBriefTitle!==undefined&&new TextEncoder().encode(JSON.stringify(snapshot.cachedBriefTitle)).length>ARTIFACT_MAX_OBJECT_BYTES)refuse("analysis_index_bound");
 const snapshotSha256=artifactDigest(payloadBytes),generation=analysisArtifactGeneration(identity,source,snapshot,snapshotSha256);
 const items=snapshot.payload.outcome==="ok"?[snapshot.payload.requests,snapshot.payload.analyses]:[[],[]];
 const rows=items.flatMap((items,n)=>items.map((payload,ordinal)=>({table:ANALYSIS_ARTIFACT_TABLES[n]!,row:{org_id:identity.orgId,workspace_id:identity.workspaceId,source:identity.source,session_id:identity.sessionId,ordinal,payload}})));
 const graph=await materializeCanonicalArtifacts(store,{identity,generation,source,prefix,rows,additionalTables:ANALYSIS_ARTIFACT_TABLES,legacyCoverage:null});
 const document:SessionAnalysisIndex={version:1,kind:"session-analysis-index",codec:"canonical-analysis-graph-v1",projection:"read_session_analysis_v1/0031",outcome:snapshot.payload.outcome,identity,source,generation,snapshotAt:snapshot.snapshotAt,snapshotId:snapshot.snapshotId,snapshotSha256,...(snapshot.cachedBriefTitle===undefined?{}:{cachedBriefTitle:snapshot.cachedBriefTitle,sourceSnapshotSha256:artifactDigest(analysisSourceSnapshotJson(snapshotSha256,snapshot.cachedBriefTitle))}),graph:graph.index,counts:{requests:items[0]!.length,analyses:items[1]!.length}};
 const bytes=new TextEncoder().encode(JSON.stringify(document)),sha256=artifactDigest(bytes),key=prefix+sha256+".json";
 if(bytes.length>ARTIFACT_MAX_OBJECT_BYTES)refuse("analysis_index_bound");
 const ack=await store.put(objectKey(key),bytes,{ifNoneMatch:true,contentSha256:sha256});
 if(ack.size!==bytes.length||ack.contentSha256!==sha256)refuse("analysis_write_ack_invalid");
 const read=await readAll(await store.get(objectKey(key)));if(read.length!==bytes.length||artifactDigest(read)!==sha256)refuse("analysis_write_verification_failed");
 return {document,index:{key,sha256,bytes:bytes.length},objects:graph.objects+1,bytes:graph.bytes+bytes.length};
}
export async function publishSessionAnalysis(identity:ArtifactIdentity,deps:AnalysisPublisherDeps){
 const first=await capture(deps.db,identity);if(!first.exists||first.blocked)return {published:false,reason:!first.exists?"absent":"source_blocked"} as const;
 if(first.source.epoch!==deps.lease.source.epoch||first.source.fingerprint!==deps.lease.source.fingerprint)refuse("analysis_lease_source_changed");
 const snapshot=await readLegacyAnalysisSnapshot(deps.source,identity),snapshotSha256=artifactDigest(analysisSnapshotJson(snapshot.payload)),generation=analysisArtifactGeneration(identity,first.source,snapshot,snapshotSha256);
 const current=await capture(deps.db,identity);if(!current.exists||current.blocked||current.source.epoch!==first.source.epoch||current.source.fingerprint!==first.source.fingerprint)refuse("analysis_source_changed");
 const anchor=first.snapshot.pointers[0]!;if(!anchor.manifest_key.endsWith("/manifest.json"))refuse("analysis_anchor_invalid");
 const rawSha=anchor.content_sha256.replace(/^\\x/u,"").toLowerCase();if(!/^[0-9a-f]{64}$/u.test(rawSha))refuse("analysis_anchor_invalid");
 const prefix=anchor.manifest_key.slice(0,-"manifest.json".length)+"normalized/read-projection-v1/"+rawSha+"/"+generation+"/";
 const made=await buildSessionAnalysisArtifacts(deps.store,identity,first.source,prefix,snapshot);
 const parameters=[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,first.source.epoch,first.source.fingerprint,snapshot.snapshotAt,snapshot.snapshotId,snapshotSha256,generation,prefix,made.index.key,made.index.sha256,made.index.bytes,deps.lease.nonce];
 const result=(await deps.db.query<{published:boolean}>("SELECT sessions.publish_session_analysis_cache($1,$2,$3,$4,$5::bigint,$6,$7::timestamptz,$8,$9,$10,$11,$12,$13,$14::integer,$15::uuid) published",parameters)).rows[0]?.published;
 if(result!==true)refuse("analysis_publish_cas_failed");return {published:true,snapshotAt:snapshot.snapshotAt,generation,index:made.index,objects:made.objects,bytes:made.bytes} as const;
}
