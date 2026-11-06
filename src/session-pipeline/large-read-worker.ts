import { layoutComponent } from "../shared/layout-component.ts";
/** Portable trusted Node worker; dependencies supply approved runtime identities.
 * No credential values or automatic production startup are defined here. */
import { randomUUID } from "node:crypto";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { nodeReadBudget } from "../session-read-api/node-read-profile.ts";
import { indexSearchScope, type SearchIndexDeps } from "./search-index.ts";
import { produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { SessionScope } from "./pointer.ts";
export interface LargeReadJob { identity:{orgId:string;workspaceId:string;source:string;sessionId:string};source:{epoch:string;fingerprint:string};leaseNonce:string }
export interface LargeReadDeps { db:SqlExecutor;search:SearchIndexDeps;replay:ReplayProducerDeps;signal?:AbortSignal;heartbeatMs?:number }
function scopeOf(j:LargeReadJob):SessionScope{return {orgId:j.identity.orgId,workspaceId:j.identity.workspaceId,harness:j.identity.source,sessionId:j.identity.sessionId}}
const args=(s:SessionScope)=>[s.orgId,s.workspaceId,s.harness,s.sessionId];
/** Route result; `identity` is the canonical job scope (jobs are keyed by canonical session id). */
export interface LargeRoute { delegated:boolean;identity?:SessionScope }
export async function routeLargeRead(db:SqlExecutor,s:SessionScope):Promise<LargeRoute>{
 const r=(await db.query<{r:{delegated:boolean;identity?:Partial<LargeReadJob["identity"]>}}>("SELECT sessions.route_session_large_read($1,$2,$3,$4) r",args(s))).rows[0]?.r;
 if(!r||typeof r.delegated!=="boolean")throw new Error("large_handoff_invalid");
 const i=r.identity;
 if(!r.delegated||typeof i?.sessionId!=="string"||!i.sessionId||i.orgId!==s.orgId||i.workspaceId!==s.workspaceId||i.source!==s.harness)return {delegated:r.delegated};
 return {delegated:true,identity:{...s,sessionId:i.sessionId}};
}
export interface LargeJobState { state:string;attempts:number }
/** One read of the job row behind a delegated route (flag-on path only). The SQL contract has
 * no state in the route result; a `failed` job is never claimed again until the source changes. */
export async function largeReadJobState(db:SqlExecutor,s:SessionScope):Promise<LargeJobState|null>{
 const row=(await db.query<{state:string;attempts:number}>("SELECT state,attempts FROM sessions.session_large_read_jobs WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",args(s))).rows[0];
 return row?{state:row.state,attempts:Number(row.attempts)}:null;
}
export async function runLargeReadOnce(deps:LargeReadDeps):Promise<"idle"|"published"|"retry"|"lease_lost">{
 if(deps.signal?.aborted)return "idle";
 const interval=deps.heartbeatMs??30000;if(interval<100||interval>30000)throw new Error("large_heartbeat_invalid");
 const nonce=randomUUID();
 const j=(await deps.db.query<{job:LargeReadJob|null}>("SELECT sessions.claim_session_large_read($1::uuid,120) job",[nonce])).rows[0]?.job;
 if(j===null)return "idle";
 if(!j||j.leaseNonce!==nonce||!j.identity||!/^[1-9][0-9]*$/.test(j.source?.epoch??"")||!/^[0-9a-f]{64}$/.test(j.source?.fingerprint??""))throw new Error("large_claim_invalid");
 const scope=scopeOf(j),params=[...args(scope),nonce];let lost=false,beating=false;
 const heartbeat=async()=>{if(beating||lost)return;beating=true;try{const r=await deps.db.query<{ok:boolean}>("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",params);if(r.rows[0]?.ok!==true)lost=true}catch{lost=true}finally{beating=false}};
 const timer=setInterval(()=>void heartbeat(),interval);
 const active=()=>{if(lost||deps.signal?.aborted)throw new Error("large_lease_lost")};
 try{
  active();await heartbeat();active();
  const captured=(await deps.db.query<{c:any}>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",args(scope))).rows[0]?.c;
  if(!captured?.exists||captured.blocked||captured.source?.epoch!==j.source.epoch||captured.source?.fingerprint!==j.source.fingerprint)throw new Error("large_source_changed");
  const physical=layoutComponent(j.identity.sessionId);
  if(!captured.snapshot?.pointers?.some((p:any)=>p.source_session_id===physical))throw new Error("large_physical_source_missing");
  const indexScope={...scope,sessionId:physical},budget=nodeReadBudget("large");
  const indexed=await indexSearchScope(indexScope,{...deps.search,limits:{...deps.search.limits,maxScopeBytes:budget.sourceBytes,maxScopeEvents:budget.sourceRecords,maxRows:budget.projectedRows}});
  if(indexed.busy||!indexed.complete)throw new Error("large_index_incomplete");
  active();
  const result=await produceCanonicalReplay(scope,{...deps.replay,profile:"large",publish:async publishParams=>{
   active();if(String(publishParams[4])!==j.source.epoch||publishParams[5]!==j.source.fingerprint)return false;
   const r=await deps.db.query<{ok:boolean}>("SELECT sessions.publish_session_large_read($1,$2,$3,$4,$5::bigint,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::integer,$14::uuid) ok",[...publishParams,nonce]);
   return r.rows[0]?.ok===true;
  }});
  if(!result.published)throw new Error("large_publication_unavailable");
  return "published";
 }catch{
  if(lost||deps.signal?.aborted)return "lease_lost";
  const r=await deps.db.query<{ok:boolean}>("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...params,"large_processing_failed"]);
  return r.rows[0]?.ok===true?"retry":"lease_lost";
 }finally{clearInterval(timer)}
}
