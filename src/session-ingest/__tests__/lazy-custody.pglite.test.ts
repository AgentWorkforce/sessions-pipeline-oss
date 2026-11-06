import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {createIngestApp} from "../app.ts";
import {CURRENT_CUSTODY_CONTENT_TYPE,CURRENT_CUSTODY_ACK_HEADER} from "../custody-protocol.ts";
import {RECOVERY_CONTENT_TYPE,RECOVERY_PROTOCOL} from "../recovery-protocol.ts";
import type {CurrentCustodyPacket,SourceCurrentRecord} from "../custody-protocol.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),generation="a".repeat(64),epoch="3".repeat(64),open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});
async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:"c".repeat(64),controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:"c".repeat(64),sourceProofSha256:"d".repeat(64)});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();open.push(fs.done);const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 async function packet(raw:any,current:SourceCurrentRecord|null=null,version="0",tenant=TENANT):Promise<CurrentCustodyPacket>{
  const p=await prepareDelivery(tenant,raw);
  const witness:any={version:1,issuer:"relayhistory-cloud",sourceIdentitySha256:"c".repeat(64),controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),tenant,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,sourceControlVersion:version,pending:false,pendingControl:null,privacyVersion:version,originalRecordCount:raw.records.length,candidateCount:p.records.length,records:p.records.map((r,i)=>({preparedOrd:i,processingRank:i,candidateDigest:r.digest,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateFenced:false,currentFenced:false,current})),privacy:version==="0"?{complete:true,deletions:[],resets:[]}:null};
  return sign(witness);
 }
 async function sign(witness:any):Promise<CurrentCustodyPacket>{const r=await db.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d",[JSON.stringify(witness)]);return{witness,sourceDigest:r.rows[0]!.d as string}}
 const send=async(id:string,r:any,current:SourceCurrentRecord|null=null,version="0")=>{const raw=await batch([r],{batch_id:id});const custody=await packet(raw,current,version);return deliverBatch({...deps,custody},TENANT,raw)};
 const seats=async(id:string)=>(await db.query("SELECT sessions.ingest_receipt_seats_for_stage(o.active_stage_token) AS r FROM sessions.ingest_buffer_outbox o WHERE batch_id=$1",[id])).rows[0]!.r as any;

 async function control(kind:"selection"|"reset",payload:any,tenant=TENANT){
  const v=String(BigInt((await db.query("SELECT coalesce(max(version),0)::text AS n FROM sessions.ingest_control_receipts WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3",[tenant.orgId,tenant.workspaceId,tenant.userId])).rows[0]!.n as string)+1n);
  const digest=(await db.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",[epoch,tenant.orgId,tenant.workspaceId,tenant.userId,kind,JSON.stringify(payload)])).rows[0]!.d as string;
  const control={epoch,version:v,digest,kind};
  const custody=await sign({version:1,issuer:"relayhistory-cloud",sourceIdentitySha256:"c".repeat(64),controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),tenant,originId:kind==="selection"?payload.origin_id:null,batchId:null,batchDigest:null,sourceControlVersion:v,pending:true,pendingControl:control,privacyVersion:String(BigInt(v)-1n),originalRecordCount:0,candidateCount:0,records:[],privacy:v==="1"?{complete:true,deletions:[],resets:[]}:null});
  const input=kind==="selection"?{tenant,originId:payload.origin_id,sessions:payload.sessions.map((x:any)=>[x.source,x.session_id]),enabled:payload.enabled,control:{...control,kind:"selection" as const},custody}:{tenant,requestId:payload.request_id,control:{...control,kind:"reset" as const},custody};
  return{result:await ledger.applyOrderedControl!(input),control,custody,input};
 }
 const gc=async(name:string,p:any)=>(await run("SELECT sessions.ingest_buffer_gc_"+name+"($1::jsonb) AS result",[JSON.stringify(name==="claim"?{generation,...p}:p)]) as any).rows[0].result;
 return{db,owner,ledger,deps,packet,sign,send,seats,control,gc};

}
async function metadata(raw:any){const p=await prepareDelivery(TENANT,await batch([raw])),r=p.records[0]!;return{userId:TENANT.userId,revision:String(r.revision),digest:r.digest,revisionId:r.revision_id,kind:r.kind,source:r.source,sessionId:r.session_id,operation:r.operation,catalogAcceptedSeq:null,projectionAcceptedSeq:"17"} as SourceCurrentRecord}
test("lazy old custody exact/changed/lower first-body decisions and actual receiver sticky seal",async()=>{
 const f=await fixture(),old=record({record_id:"old",revision:8,session_id:"B",payload:{text:"Σİ😀",large:1e100}}),current=await metadata(old);
 await f.send("exact8",old,current);expect((await f.seats("exact8")).records[0].bodyAccepted).toBe(false);
 await f.send("changed8",{...old,payload:{text:"different"}},current);expect((await f.seats("changed8")).records[0].firstDigest).toBe(current.digest);
 await f.send("lower1",record({record_id:"old",revision:1,session_id:"A"}),current);expect((await f.seats("lower1")).records[0].bodyAccepted).toBe(false);
 const raw=await batch([record({record_id:"fresh"})],{batch_id:"http-new"}),packet=await f.packet(raw);
 const envelope={protocol:"relay-ingest-custody-v1",original:{bodyBase64:Buffer.from(JSON.stringify({protocolVersion:1,batch:raw})).toString("base64"),contentEncoding:"identity"},...packet};
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{}});
 const result=await app.request("/v1/delivery/batches",{method:"POST",headers:{"content-type":CURRENT_CUSTODY_CONTENT_TYPE},body:JSON.stringify(envelope)});
 expect(result.status).toBe(200);expect(JSON.parse(result.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toEqual({version:1,sourceDigest:packet.sourceDigest,sourceControlVersion:"0",privacySealed:true});
 expect((await f.seats("http-new")).records[0].bodyAccepted).toBe(true);
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]!.n).toBe(0);
});

test("signed archive recovery preserves the exact prepared receipt and enters only the buffered path",async()=>{
 const f=await fixture(),raw=await batch([record({record_id:"archived",payload:{text:"alice@example.com"}})],{batch_id:"archived-batch"});
 const prepared=await prepareDelivery(TENANT,raw),custody=await f.packet(raw);
 const envelope={protocol:RECOVERY_PROTOCOL,prepared,custody},body=JSON.stringify(envelope);
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{}});
 const response=await app.request("/v1/delivery/recovery",{method:"POST",headers:{"content-type":RECOVERY_CONTENT_TYPE},body});
 expect(response.status).toBe(200);
 expect(await response.json()).toMatchObject(prepared.receipt);
 expect(JSON.parse(response.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toEqual({version:1,sourceDigest:custody.sourceDigest,sourceControlVersion:"0",privacySealed:true});
 expect((await f.seats("archived-batch")).records[0].bodyAccepted).toBe(true);
 expect((await f.db.query("SELECT publication_state FROM sessions.ingest_batches WHERE batch_id='archived-batch'")).rows[0]!.publication_state).toBe("buffered");
});

test("archive recovery fails closed on receipt, tenant, custody or mode changes before publication",async()=>{
 const f=await fixture(),raw=await batch([record({record_id:"archive-refuse"})],{batch_id:"archive-refuse"});
 const prepared=await prepareDelivery(TENANT,raw),custody=await f.packet(raw);
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{}});
 const send=(value:unknown,type=RECOVERY_CONTENT_TYPE)=>app.request("/v1/delivery/recovery",{method:"POST",headers:{"content-type":type},body:JSON.stringify(value)});
 const base={protocol:RECOVERY_PROTOCOL,prepared,custody};
 expect((await send({...base,prepared:{...prepared,receipt:{...prepared.receipt,receiptId:"rhr_"+"f".repeat(64)}}})).status).toBe(400);
 expect((await send({...base,custody:{...custody,witness:{...custody.witness,batchDigest:"e".repeat(64)}}})).status).toBe(400);
 expect((await send(base,"application/json")).status).toBe(503);
 const canonical=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:{...f.deps,acceptanceMode:undefined},log:()=>{}});
 expect((await canonical.request("/v1/delivery/recovery",{method:"POST",headers:{"content-type":RECOVERY_CONTENT_TYPE},body:JSON.stringify(base)})).status).toBe(503);
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_batches WHERE batch_id='archive-refuse'")).rows[0]!.n).toBe(0);
});

test("fully fenced archive recovery preserves the original receipt without writing a buffered object",async()=>{
 const f=await fixture(),raw=await batch([record({record_id:"archive-fenced"})],{batch_id:"archive-fenced"});
 await f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"sess-1"}],enabled:false});
 const original=await prepareDelivery(TENANT,raw),packet=await f.packet(raw,null,"1");
 const prepared={...original,records:[]},witness={...packet.witness,candidateCount:0,records:[]};
 const custody=await f.sign(witness),body=JSON.stringify({protocol:RECOVERY_PROTOCOL,prepared,custody});
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{}});
 const response=await app.request("/v1/delivery/recovery",{method:"POST",headers:{"content-type":RECOVERY_CONTENT_TYPE},body});
 expect(response.status).toBe(200);expect(await response.json()).toMatchObject(original.receipt);
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_stage_intents WHERE batch_id='archive-fenced'")).rows[0]!.n).toBe(0);
});

test("old8/B ignored1/A then erase B permits fresh1/C without reloading stale original source custody",async()=>{
 const f=await fixture(),old=record({record_id:"legacy-move",revision:8,session_id:"B"}),cur=await metadata(old);
 await f.send("ignored",record({record_id:"legacy-move",session_id:"A"}),cur);
 const c=await f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"B"}],enabled:false});expect(c.result.custodyAck?.privacySealed).toBe(true);
 await f.send("fresh",record({record_id:"legacy-move",session_id:"C"}),cur,"1");
 expect((await f.seats("fresh")).records[0].bodyAccepted).toBe(true);
 expect((await f.seats("ignored")).records[0].bodyAccepted).toBe(false);
 expect((await f.db.query("SELECT current_session,current_revision::text FROM sessions.ingest_lazy_native_slots")).rows[0]).toEqual({current_session:"C",current_revision:"1"});
});
test("committed NULL-session custody survives reset; no-op enable preserves current-seat bytes; per-ordinal GC retires only superseded records",async()=>{
 const f=await fixture();
 const records=[record({record_id:"moving",session_id:"A"}),record({record_id:"unrelated",session_id:"A"}),record({record_id:"null",session_id:null,revision:8})];
 const raw=await batch(records,{batch_id:"mixed-A"});await deliverBatch({...f.deps,custody:await f.packet(raw)},TENANT,raw);
 await f.send("move-B",record({record_id:"moving",session_id:"B",revision:2}));
 await f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"B"}],enabled:false});
 const tickets=(await f.gc("claim",{limit:10})).tickets;
 expect(tickets.find((t:any)=>t.groupOrdinals.flat().length===3).keepOrdinals).toEqual([1,2]);
 expect(tickets.find((t:any)=>t.groupOrdinals.flat().length===3).keepGroupIndexes.length).toBeGreaterThan(0);
 expect(tickets.find((t:any)=>t.groupOrdinals.flat().length===1).keepOrdinals).toEqual([]);
 await f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"A"}],enabled:true});
 await f.db.exec("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second'");
 const enabled=(await f.gc("claim",{limit:10})).tickets.find((t:any)=>t.groupOrdinals.flat().length===3);
 expect(enabled.keepOrdinals).toEqual([1,2]);
 await f.control("reset",{request_id:"reset-real"});
 await f.db.exec("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second'");
 const reset=(await f.gc("claim",{limit:10})).tickets.find((t:any)=>t.groupOrdinals.flat().length===3);
 expect(reset.keepOrdinals).toEqual([2]);
 await f.send("lower-null",record({record_id:"null",session_id:null,revision:1}),null,"3");
 expect((await f.seats("lower-null")).records[0].bodyAccepted).toBe(false);
});
test("cold zero-row reset seals USER; old correct-generation unqualified control cannot bypass lazy policy",async()=>{
 const f=await fixture();
 await expect(f.ledger.resetUploadedSessions(TENANT,"old")).rejects.toThrow();
 const r=await f.control("reset",{request_id:"empty"});expect(r.result.deleted).toBe(0);expect(r.result.custodyAck?.privacySealed).toBe(true);
 const old=record({record_id:"new-after-reset"});await f.send("new-after-reset",old,null,"1");
 expect((await f.seats("new-after-reset")).records[0].bodyAccepted).toBe(true);
 await f.db.exec("DELETE FROM sessions.ingest_lazy_user_seals");
 await expect(f.send("lost-seal",record({record_id:"lost"}),null,"1")).rejects.toThrow();
});

test("in-batch native-key twins use actual CURRENT revision: new/equal conflicting peers409, below-current peers ACK ignored",async()=>{
 const f=await fixture(),old=record({record_id:"twins",revision:8,session_id:"B"}),current=await metadata(old);
 async function send(id:string,revision:number,cur:SourceCurrentRecord|null){
  const records=[{...record({record_id:"twins",revision,session_id:"A"}),revision_id:"z-peer",payload:{text:"Z"}},{...record({record_id:"twins",revision,session_id:"A"}),revision_id:"a-peer",payload:{text:"A"}}];
  const raw=await batch(records,{batch_id:id});const custody=await f.packet(raw,cur);return deliverBatch({...f.deps,custody},TENANT,raw);
 }
 await send("below",1,current);expect((await f.seats("below")).records.every((r:any)=>!r.bodyAccepted)).toBe(true);
 await expect(send("equal",8,current)).rejects.toMatchObject({status:409,code:"delivery_conflict"});
 await expect(send("above",9,current)).rejects.toMatchObject({status:409,code:"delivery_conflict"});
 const g=await fixture(),raw=await batch([{...record({record_id:"new-twin"}),revision_id:"one",payload:{text:"one"}},{...record({record_id:"new-twin"}),revision_id:"two",payload:{text:"two"}}],{batch_id:"new-conflict"});
 await expect(deliverBatch({...g.deps,custody:await g.packet(raw)},TENANT,raw)).rejects.toMatchObject({status:409});
});

test("source-native processing rank assigns ACK seats while preserving original ordinal maps",async()=>{
 const f=await fixture(),raw=await batch([{...record({record_id:"z-native",session_id:"catalog"}),kind:"session",payload:{title:"Z wins"}},{...record({record_id:"a-native",session_id:"catalog"}),kind:"session",payload:{title:"A first"}}],{batch_id:"rank"});
 const packet=await f.packet(raw);packet.witness.records[0]!.processingRank=1;packet.witness.records[1]!.processingRank=0;
 await deliverBatch({...f.deps,custody:await f.sign(packet.witness)},TENANT,raw);
 const rows=(await f.seats("rank")).records;expect(rows.map((r:any)=>r.ord)).toEqual([0,1]);expect(rows.every((r:any)=>r.bodyAccepted)).toBe(true);expect(BigInt(rows[0].seat)).toBeGreaterThan(BigInt(rows[1].seat));
});
