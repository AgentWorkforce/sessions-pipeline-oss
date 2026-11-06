/** The background publisher calls only reviewed ingest RPCs, as the ingest role.
 * Its pool/transport owns cancellation and SET LOCAL deadlines, never role settings.
 * Reuses the exact bounded PostgreSQL executor from191 recovery73c50a7.
 */
import {parseReceiptSeatSnapshot,acceptedReceiptSeat}from "./publication-seats.ts";
import {assertBufferedSpec}from "./buffered-gc.ts";
import type {SemanticBoundedSql} from "../search-features/semantic-copy-postgres.ts";
import type {PublicationRpc,PublicationTarget,PublicationTicket} from "./publication-protocol.ts";
const UUID=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const HEX=/^[a-f0-9]{64}$/;
const bad=()=>Object.assign(Error("publication_rpc_invalid"),{code:"publication_rpc_invalid"});
function ticket(v:any,target:PublicationTarget):PublicationTicket{
 if(!v||v.schemaGeneration!==target.generation||!UUID.test(v.attemptToken)||!UUID.test(v.leaseToken)||!UUID.test(v.claimToken)||!UUID.test(v.stageToken)||
  !Number.isFinite(Date.parse(v.leaseUntil))||!v.tenant||typeof v.tenant.orgId!=="string"||typeof v.tenant.workspaceId!=="string"||typeof v.tenant.userId!=="string"||
  !v.stage||!HEX.test(v.stage.sha256)||!Number.isSafeInteger(v.stage.bytes)||v.stage.bytes<1||v.stage.bytes>16*1024*1024||
  typeof v.stage.key!=="string"||!Array.isArray(v.stage.groupOrdinals)||v.stage.groupOrdinals.length>500||
  !Array.isArray(v.records)||v.records.length>500||!Array.isArray(v.acceptedOrdinals)||!Array.isArray(v.heads)||v.heads.length>500)throw bad();
 assertBufferedSpec(v.stage);
 if(!v.stage.key.startsWith(target.keyRoot+"/"+v.tenant.orgId+"/buffered-batches/"))throw bad();
 const ords=new Set(v.records.map((r:any)=>r.ord));
 if(ords.size!==v.records.length||v.acceptedOrdinals.some((i:any)=>!Number.isInteger(i)||!ords.has(i)))throw bad();
 let previous=-1;
 for(const i of v.acceptedOrdinals){if(i<=previous)throw bad();previous=i;}
 for(const h of v.heads){
  if(!UUID.test(h.generation)||typeof h.prefix!=="string"||h.prefix!==target.keyRoot+"/"+v.tenant.orgId+"/upload-generations/g="+h.generation||
   !Number.isInteger(h.groupIndex)||h.groupIndex<0||h.groupIndex>=v.stage.groupOrdinals.length||!Array.isArray(h.ordinals)||!h.ordinals.length||
   h.ordinals.some((i:any)=>!v.acceptedOrdinals.includes(i))||typeof h.revision!=="string"||!/^[1-9][0-9]*$/.test(h.revision)||
   !Number.isSafeInteger(Number(h.revision)))throw bad();
 }
 let receiptSeats;
 try{
  receiptSeats=parseReceiptSeatSnapshot(v.receiptSeats,v.records,v.stage);
  for(const h of v.heads)for(const ord of h.ordinals)acceptedReceiptSeat(receiptSeats,ord,{resetEpoch:h.resetEpoch,fenceEpoch:h.fenceEpoch});
 }catch{throw bad();}
 const covered=v.heads.flatMap((h:any)=>h.ordinals).sort((a:number,b:number)=>a-b);
 if(JSON.stringify(covered)!==JSON.stringify(v.acceptedOrdinals))throw bad();
 return {...v,receiptSeats};
}
export function publicationRpc(db:SemanticBoundedSql,target:PublicationTarget,options:{now?:()=>number;queryTimeoutMs?:number}={}):PublicationRpc{
 if(!HEX.test(target.generation)||!target.bucket||!target.region||!target.keyRoot)throw bad();
 const now=options.now??Date.now,timeout=options.queryTimeoutMs??15000;
 if(!Number.isSafeInteger(timeout)||timeout<1||timeout>60000)throw bad();
 const call=async(fn:string,p:Record<string,unknown>,signal:AbortSignal)=>{
  signal.throwIfAborted();
  const r=await db.queryBounded<{result:unknown}>("SELECT sessions.ingest_publication_"+fn+"($1::jsonb) AS result",
   [JSON.stringify({...p,generation:target.generation})],{signal,deadline:now()+timeout,now});
  if(r.rows.length!==1||!r.rows[0]?.result||typeof r.rows[0].result!=="object")throw bad();
  return r.rows[0].result as any;
 };
 const attempt=(t:PublicationTicket)=>{if(t.schemaGeneration!==target.generation)throw bad();return {attemptToken:t.attemptToken,leaseToken:t.leaseToken};};
 return {
  async candidates(limit,signal){const r=await call("candidates",{limit,keyRoot:target.keyRoot,bucket:target.bucket,region:target.region},signal);if(!Array.isArray(r.stageTokens)||r.stageTokens.some((x:any)=>typeof x!=="string"||!UUID.test(x)))throw bad();return r.stageTokens;},
  async defer(stageToken,code,signal){const r=await call("defer",{stageToken,code},signal);if(typeof r.deferred!=="boolean")throw bad();},
  async claim(stageToken,signal){return ticket(await call("claim",{stageToken,keyRoot:target.keyRoot,bucket:target.bucket,region:target.region},signal),target);},
  async heartbeat(t,signal){const r=await call("heartbeat",attempt(t),signal);if(typeof r.leaseUntil!=="string"||!Number.isFinite(Date.parse(r.leaseUntil)))throw bad();t.leaseUntil=r.leaseUntil;},
  async prepare(t,images,signal){if((await call("prepare",{...attempt(t),images},signal)).prepared!==true)throw bad();},
  async register(t,objects,signal){if((await call("register_objects",{...attempt(t),objects},signal)).registered!==true)throw bad();},
  async publish(t,messages,signal){const r=await call("publish",{...attempt(t),messages},signal);if(r.published!==true||typeof r.replay!=="boolean"||!Number.isInteger(r.images))throw bad();return r;},
  async abandon(t,signal){if(typeof(await call("abandon",attempt(t),signal)).abandoned!=="boolean")throw bad();}
 };
}
