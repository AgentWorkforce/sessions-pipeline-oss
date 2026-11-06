/** Disabled-by-default background drain. Ingress always keeps its durable
 * buffer receipt; this consumer alone may admit/publish canonical images.
 */
import {zstdDecompressSync} from "node:zlib";
import type {ObjectStore} from "../../packages/object-store/src/index.ts";
import {boundedPublicationStore} from "./publication-store.ts";
import {readBufferedStage,BUFFER_STAGE_MAX_BYTES} from "./staging.ts";
import {preparePublicationImage,writePublicationImage} from "./publication-image.ts";
import type {PublicationRpc,PublicationTarget,PublicationTicket} from "./publication-protocol.ts";
export interface PublicationJobDeps {
 rpc:PublicationRpc;target:PublicationTarget;
 /** Must inject this signal into every underlying request, including body IO. */
 store(signal:AbortSignal):ObjectStore;
}
export interface PublicationJobOptions {
 enabled?:boolean;signal?:AbortSignal;maxJobMs?:number;heartbeatMs?:number;candidateLimit?:number;
}
const unavailable=(code:string)=>Object.assign(Error(code),{code});
export async function publishBufferedStage(deps:PublicationJobDeps,stageToken:string,options:PublicationJobOptions={}){
 if(options.enabled!==true)return {enabled:false as const};
 const budget=options.maxJobMs??20*60*1000,interval=options.heartbeatMs??30000;
 if(!Number.isSafeInteger(budget)||budget<1000||budget>60*60*1000||!Number.isSafeInteger(interval)||interval<10||interval>60000)
  throw unavailable("publication_job_bounds");
 const controller=new AbortController(),signal=AbortSignal.any([controller.signal,AbortSignal.timeout(budget),...(options.signal?[options.signal]:[])]);
 const store=boundedPublicationStore(deps.store(signal),signal);let ticket:PublicationTicket|undefined;
 let heartbeat:Promise<void>|undefined,heartbeatError:unknown,timer:ReturnType<typeof setInterval>|undefined,finishing=false;
 const stop=async()=>{if(timer)clearInterval(timer);timer=undefined;await heartbeat;if(heartbeatError)throw heartbeatError;signal.throwIfAborted();};
 try{
  ticket=await deps.rpc.claim(stageToken,signal);
  const active=ticket;
  timer=setInterval(()=>{
   if(heartbeat||finishing||signal.aborted)return;
   heartbeat=deps.rpc.heartbeat(active,signal).catch(error=>{heartbeatError=error;controller.abort(error);}).finally(()=>{heartbeat=undefined;});
  },interval);
  const decoded=active.heads.length?await readBufferedStage(store,active.stage,bytes=>new Uint8Array(zstdDecompressSync(bytes,{maxOutputLength:BUFFER_STAGE_MAX_BYTES}))):undefined;
  signal.throwIfAborted();
  const messages:unknown[]=[];
  // Retain one image/new raw body at a time, even when historical input is GiB.
  for(const head of active.heads){
   signal.throwIfAborted();
   const image=await preparePublicationImage(active,head,decoded!,deps.target,store,signal);
   const message=await writePublicationImage(active,image,deps.rpc,store,signal,deps.target);
   if(message!==null)messages.push(message);
  }
  finishing=true;await stop();
  // No heartbeat races the commit that retires this very stage lease.
  await deps.rpc.heartbeat(active,signal);
  let lastError:unknown;
  for(let attempt=0;attempt<3;attempt++){
   signal.throwIfAborted();
   try{return {enabled:true as const,...await deps.rpc.publish(active,messages,signal)};}
   catch(error){
    lastError=error;
    const code=(error as {code?:unknown})?.code;
    if(!["40001","55P03","57014","08006","ECONNRESET"].includes(String(code)))throw error;
    // Retry the same immutable request first: an ambiguous COMMIT may already
    // have completed. No new generation/nonce or source rebase is invented.
    if(attempt<2)await new Promise<void>((resolve,reject)=>{
     const abort=()=>{clearTimeout(timeout);reject(signal.reason)};
     const timeout=setTimeout(()=>{signal.removeEventListener("abort",abort);resolve();},50*(attempt+1));
     signal.addEventListener("abort",abort,{once:true});if(signal.aborted)abort();
    });
   }
  }
  throw lastError;
 }catch(error){
  if(ticket){
   // Metadata-only best effort. Expired/replaced leases are never borrowed.
   const cleanup=AbortSignal.timeout(5000);
   await deps.rpc.abandon(ticket,cleanup).catch(()=>{});
  }
  const raw=(error as {code?:unknown})?.code;
  const code=typeof raw==="string"&&/^[A-Za-z0-9_]{1,64}$/.test(raw)?raw:signal.aborted?"publication_aborted":"publication_job_failed";
  throw unavailable(code);
 }finally{
  finishing=true;if(timer)clearInterval(timer);
  controller.abort(unavailable("publication_job_complete"));
  await heartbeat;
 }
}
export async function canonicalPublicationTick(deps:PublicationJobDeps,options:PublicationJobOptions={}){
 if(options.enabled!==true)return {enabled:false as const,examined:0,published:0,deferred:0,eligibleSessions:0};
 const signal=options.signal??AbortSignal.timeout(20*60*1000),limit=options.candidateLimit??1;
 if(!Number.isInteger(limit)||limit<1||limit>20)throw unavailable("publication_candidate_bound");
 const candidates=await deps.rpc.candidates(limit,signal);
 let published=0,deferred=0,eligibleSessions=0;
 for(const stageToken of candidates){
  signal.throwIfAborted();
  try{
   const result=await publishBufferedStage(deps,stageToken,{...options,signal});
   if(result.enabled){published++;eligibleSessions+=result.eligibleSessions;}
  }catch(error){
   const code=String((error as {code?:unknown})?.code??"");
   if(["40001","55P03","55000","57014","40P01"].includes(code)){await deps.rpc.defer(stageToken,code,signal);deferred++;continue;}
   throw error;
  }
 }
 return {enabled:true as const,examined:candidates.length,published,deferred,eligibleSessions};
}
