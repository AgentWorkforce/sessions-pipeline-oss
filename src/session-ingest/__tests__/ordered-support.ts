/** Test-only stand-in for the RHC PostgreSQL journal, never production fallback.
 * Actual caller integration must use the source journal's stored version/digest.
 */
import {createHash} from "node:crypto";
import type {Ledger,LedgerTenant} from "../ledger.ts";
import type {OrderedControl} from "../control-protocol.ts";
export function orderedFixtureLedger(base:Ledger,db:{query(s:string,p?:any[]):Promise<{rows:any[]}>},epoch="3".repeat(64)):Ledger{
 async function next(tenant:LedgerTenant,kind:OrderedControl["kind"],payload:unknown):Promise<OrderedControl>{
  const r=await db.query("SELECT coalesce((SELECT max(version) FROM sessions.ingest_control_receipts WHERE org_id=$2 AND workspace_id=$3 AND user_id=$4 AND epoch=$1),0)::text AS v,encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",[epoch,tenant.orgId,tenant.workspaceId,tenant.userId,kind,JSON.stringify(payload)]);
  return{epoch,version:(BigInt(r.rows[0].v)+1n).toString(),digest:r.rows[0].d,kind};
 }
 return{...base,async setSessionUploads(input){
  const c=await next(input.tenant,"selection",{origin_id:input.originId,sessions:input.sessions.map(([source,session_id])=>({source,session_id})),enabled:input.enabled});
  await base.applyOrderedControl!({...input,control:{...c,kind:"selection"}});
 },async resetUploadedSessions(tenant,requestId){
  const c=await next(tenant,"reset",{request_id:requestId});
  return(await base.applyOrderedControl!({tenant,requestId,control:{...c,kind:"reset"}})).deleted!;
 }};
}

/** Synthetic owner attestation only; production uses the frozen-source exporter. */
export function fixtureCustodyDescriptor(epoch="3".repeat(64),floor="9007199254740993",orgs=["org-a"]){
 const baselineAcceptanceFloor={protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",
  sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:floor,isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:floor};
 return{protocol:"delivery-privacy-baseline-v1",epoch,complete:true,scopes:0,deletions:0,resets:0,emptyScopes:0,sha256:"e".repeat(64),
  baselineAcceptanceFloor,custody:{protocol:"delivery-custody-baseline-v1",scope:"all-existing-orgs",orgCount:String(orgs.length),
   orgInventorySha256:createHash("sha256").update("["+orgs.map(x=>JSON.stringify(x)).join(", ")+"]").digest("hex"),sourceIdentitySha256:"c".repeat(64),
   orgs:orgs.map(orgId=>({orgId,records:"0",pages:"0",chainSha256:"0".repeat(64)}))}};
}
export function fixtureSeatBaseline(generation:string,epoch="3".repeat(64),floor="9007199254740993",orgs=["org-a"]){
 const d=fixtureCustodyDescriptor(epoch,floor,orgs);
 return{...d.baselineAcceptanceFloor,generation,sourceIdentitySha256:d.custody.sourceIdentitySha256,
  sourceProofSha256:createHash("sha256").update(JSON.stringify(d)).digest("hex")};
}
export async function initializeFixtureSeatClock(db:{query(s:string,p?:any[]):Promise<any>},generation:string,epoch="3".repeat(64)){
 const orgs=(await db.query("SELECT org_id FROM sessions.ingest_buffer_admission ORDER BY org_id")).rows.map((r:any)=>r.org_id);
 const descriptor=fixtureCustodyDescriptor(epoch,"9007199254740993",orgs);
 await db.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify(fixtureSeatBaseline(generation,epoch,"9007199254740993",orgs))]);
 for(const orgId of orgs){
  const r=await db.query("SELECT sessions.ingest_custody_begin($1::jsonb) AS r",[JSON.stringify({generation,orgId,descriptorText:JSON.stringify(descriptor)})]);
  await db.query("SELECT sessions.ingest_custody_complete($1::jsonb)",[JSON.stringify({generation,importToken:r.rows[0].r.importToken})]);
 }
 await db.query("SELECT sessions.ingest_custody_release($1::jsonb)",[JSON.stringify({generation,descriptorText:JSON.stringify(descriptor)})]);
}
