/** Durable demand refresh worker. No global source scan and no request-path Neon.
 * Backfill/hints enqueue scope IDs through the same backend RPC and leased path. */
import type {ArtifactIdentity,ArtifactSource} from "../session-read-api/canonical-artifact.ts";
import {publishSessionAnalysis,type AnalysisPublisherDeps} from "./analysis-publication.ts";
import {ArtifactBuildError} from "./replay-pages.ts";
export type AnalysisLease={version:1;identity:ArtifactIdentity;source:ArtifactSource;leaseNonce:string;leaseUntil:string;demandUntil:string};
export type AnalysisRefreshDeps=Omit<AnalysisPublisherDeps,"lease">;
const params=(l:AnalysisLease)=>[l.identity.orgId,l.identity.workspaceId,l.identity.source,l.identity.sessionId,l.source.epoch,l.source.fingerprint,l.leaseNonce];
function checked(value:unknown):AnalysisLease|null{if(value===null)return null;const l=value as AnalysisLease;if(l?.version!==1||![l.identity?.orgId,l.identity?.workspaceId,l.identity?.source,l.identity?.sessionId].every(v=>typeof v==="string"&&v.length>0)||!/^[1-9][0-9]*$/u.test(l.source?.epoch??"")||!/^[0-9a-f]{64}$/u.test(l.source?.fingerprint??"")||!/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/u.test(l.leaseNonce??""))throw new ArtifactBuildError("analysis_lease_invalid");return l}
export async function enqueueAnalysisScope(deps:Pick<AnalysisRefreshDeps,"db">,identity:ArtifactIdentity){return (await deps.db.query<{ack:unknown}>("SELECT sessions.enqueue_session_analysis_cache($1,$2,$3,$4) ack",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0]?.ack}
export async function analysisRefreshTick(deps:AnalysisRefreshDeps,options:{maxJobs?:number;maxMs?:number;now?:()=>number}={}){
 const maxJobs=options.maxJobs??10,maxMs=options.maxMs??240000,now=options.now??Date.now;
 if(!Number.isInteger(maxJobs)||maxJobs<1||maxJobs>100||!Number.isFinite(maxMs)||maxMs<1||maxMs>240000)throw new Error("analysis refresh: invalid bounded tick");
 const stop=now()+maxMs,result={claimed:0,published:0,failed:0,lost:0};
 while(result.claimed<maxJobs&&now()<stop){const lease=checked((await deps.db.query<{lease:unknown}>("SELECT sessions.claim_session_analysis_cache() lease")).rows[0]?.lease??null);if(!lease)break;result.claimed++;
  let lost=false,pending:Promise<void>|undefined;
  const heartbeat=()=>{if(pending)return;pending=(async()=>{try{const ok=(await deps.db.query<{ok:boolean}>("SELECT sessions.heartbeat_session_analysis_cache($1,$2,$3,$4,$5::bigint,$6,$7::uuid) ok",params(lease))).rows[0]?.ok;if(ok!==true)lost=true}catch{lost=true}})().finally(()=>{pending=undefined})};
  const timer=setInterval(heartbeat,20000);let errorCode:string|null=null;
  try{const publication=await publishSessionAnalysis(lease.identity,{...deps,lease:{nonce:lease.leaseNonce,source:lease.source}});await pending;if(!publication.published)errorCode="analysis_source_unavailable";else if(lost)errorCode="analysis_lease_lost";else result.published++;}
  catch(error){const code=(error as {code?:unknown})?.code;errorCode=typeof code==="string"&&/^[a-z][a-z0-9_]{0,79}$/u.test(code)?code:"analysis_refresh_failed";result.failed++;}
  finally{clearInterval(timer);await pending;}
  const finished=(await deps.db.query<{ok:boolean}>("SELECT sessions.finish_session_analysis_cache($1,$2,$3,$4,$5::bigint,$6,$7::uuid,$8) ok",[...params(lease),errorCode])).rows[0]?.ok;
  if(finished!==true)result.lost++;
 }
 return result;
}
