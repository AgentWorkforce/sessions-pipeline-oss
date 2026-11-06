/** Bounded metadata-only source discovery. Durable continuation is the Lambda
 * async event cursor, acknowledged only after this page's enqueue RPCs succeed. */
import type {ArtifactIdentity} from "../session-read-api/canonical-artifact.ts";
import type {ReadonlyAnalysisSource} from "./analysis-snapshot.ts";
import {ANALYSIS_NAMESPACE_SOURCES_SQL} from "./analysis-scope.ts";
import {log} from "../shared/log.ts";
/** Stable reasons for a per-key refusal that is logged and skipped, never retried by the scan. */
export type AnalysisReconcileRefusal="namespace_fanout_bound"|"enqueue_invalid_scope"|"enqueue_ambiguous_source"|"enqueue_capacity_exceeded";
/** Deterministic per-key refusals from the demand function and the
 * capture_session_replay_pages call inside it (migration 20261105026000):
 * 22023 (non-canonical physical id, out-of-range scope), 55000 "ambiguous canonical
 * source", 54000 "pointer capacity exceeded" / "metadata capacity exceeded".
 * Classification is code plus exact message prefix; anything else, including
 * 55000 "source epoch missing" (may be transient), aborts and replays the cursor. */
const REFUSED_ENQUEUE_MESSAGES:readonly [string,string,Exclude<AnalysisReconcileRefusal,"namespace_fanout_bound"|"enqueue_invalid_scope">][]=[
 ["55000","replay capture: ambiguous canonical source","enqueue_ambiguous_source"],
 ["54000","replay capture: pointer capacity exceeded","enqueue_capacity_exceeded"],
 ["54000","replay capture: metadata capacity exceeded","enqueue_capacity_exceeded"],
];
export function refusedEnqueueReason(error:unknown):AnalysisReconcileRefusal|undefined{
 const e=error as {code?:unknown;message?:unknown}|null;if(typeof e?.code!=="string")return undefined;
 if(e.code==="22023")return "enqueue_invalid_scope";
 if(typeof e.message!=="string")return undefined;
 for(const [code,prefix,reason] of REFUSED_ENQUEUE_MESSAGES)if(e.code===code&&e.message.startsWith(prefix))return reason;
 return undefined;
}
export type AnalysisReconcileCursor=[string,string,string];
export function checkedAnalysisCursor(value:unknown):AnalysisReconcileCursor|undefined{if(value===undefined)return undefined;if(!Array.isArray(value)||value.length!==3||value.some(v=>typeof v!=="string"||!v||v.length>512))throw new Error("analysis reconcile: invalid cursor");return value as AnalysisReconcileCursor}
type Key={org_id:string;workspace_id:string;session_id:string};
export async function reconcileAnalysisPage(deps:{source:ReadonlyAnalysisSource;enqueue:(identity:ArtifactIdentity)=>Promise<unknown>;onRefused?:(reason:AnalysisReconcileRefusal,key:{orgId:string;workspaceId:string;sessionId:string;source?:string})=>void},options:{cursor?:AnalysisReconcileCursor;pageSize?:number;maxMs?:number;now?:()=>number}={}){
 const pageSize=options.pageSize??50;if(!Number.isInteger(pageSize)||pageSize<1||pageSize>100)throw new Error("analysis reconcile: invalid page size");const cursor=checkedAnalysisCursor(options.cursor);
 const now=options.now??Date.now,maxMs=options.maxMs??240000;if(!Number.isFinite(maxMs)||maxMs<1||maxMs>240000)throw new Error("analysis reconcile: invalid time budget");const stop=now()+maxMs;
 return deps.source.snapshot(async db=>{const {rows}=await db.query<Key>(`WITH candidates AS(
 SELECT org_id,workspace_id,session_id FROM sessions.session_analysis_requests
 UNION SELECT org_id,coalesce(workspace_id,CASE WHEN jsonb_typeof(source->'evidenceWorkspaceIds')='array' THEN CASE WHEN jsonb_array_length(source->'evidenceWorkspaceIds')=1 THEN source->'evidenceWorkspaceIds'->>0 END END),session_id FROM sessions.session_analysis_jobs
 ) SELECT org_id,workspace_id,session_id FROM candidates WHERE org_id IS NOT NULL AND workspace_id IS NOT NULL AND session_id IS NOT NULL
 AND length(org_id) BETWEEN 1 AND 512 AND btrim(org_id)<>'' AND length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id)<>'' AND length(session_id) BETWEEN 1 AND 512 AND btrim(session_id)<>''
 AND($1::text IS NULL OR(org_id COLLATE "C",workspace_id COLLATE "C",session_id COLLATE "C")>($1::text COLLATE "C",$2::text COLLATE "C",$3::text COLLATE "C"))
 ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",session_id COLLATE "C" LIMIT $4`,[...(cursor??[null,null,null]),pageSize+1]);
 let scanned=0,accepted=0,unavailable=0,invalidScope=0,refused=0;
 const refuse=deps.onRefused??((reason,key)=>log.warn("analysis reconcile key refused",{stage:"analysis-cache",reason,org_id:key.orgId,workspace_id:key.workspaceId,session_id:key.sessionId,harness:key.source}));
 let last:Key|undefined;
 for(const k of rows.slice(0,pageSize)){
  if(now()>=stop)break;
  const providers=(await db.query<{source:string}>(`SELECT source FROM (${ANALYSIS_NAMESPACE_SOURCES_SQL}) scoped ORDER BY source COLLATE "C" LIMIT 9`,[k.org_id,k.session_id,k.workspace_id])).rows.map(r=>r.source);
  // Refuse the whole key rather than acknowledge a truncated fanout; log and advance
  // so one oversized key cannot block discovery of every key after it.
  if(providers.length>8){refused++;refuse("namespace_fanout_bound",{orgId:k.org_id,workspaceId:k.workspace_id,sessionId:k.session_id});scanned++;last=k;continue}
  if(!providers.length)invalidScope++;
  for(const source of providers){
   if(!/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(source)){invalidScope++;continue}
   const scope:ArtifactIdentity={orgId:k.org_id,workspaceId:k.workspace_id,source,sessionId:k.session_id};
   let ack:{accepted?:unknown};
   try{ack=await deps.enqueue(scope) as {accepted?:unknown}}catch(error){const reason=refusedEnqueueReason(error);if(!reason)throw error;refused++;refuse(reason,{orgId:k.org_id,workspaceId:k.workspace_id,sessionId:k.session_id,source});continue}
   if(typeof ack?.accepted!=="boolean")throw new Error("analysis reconcile: invalid durable enqueue acknowledgement");
   if(ack.accepted)accepted++;else unavailable++;
  }
  // Advance only after this exact key is durably acknowledged or explicitly
  // classified unavailable/invalid/refused. Other failures replay the same input cursor.
  scanned++;last=k;
 }
 const hasMore=scanned<rows.length;
 if(hasMore&&!last)throw new Error("analysis reconcile: budget exhausted before progress");
 return {scanned,accepted,unavailable,invalidScope,refused,nextCursor:hasMore&&last?[last.org_id,last.workspace_id,last.session_id] as AnalysisReconcileCursor:undefined,complete:!hasMore};
 });
}
