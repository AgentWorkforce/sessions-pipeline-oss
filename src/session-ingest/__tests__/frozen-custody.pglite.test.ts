import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import type {CurrentCustodyPacket} from "../custody-protocol.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),generation="a".repeat(64),epoch="3".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();open.push(fs.done);const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 const seal=()=>db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const rpc=async(fn:string,p:unknown)=>((await run("SELECT sessions."+fn+"($1::jsonb) AS r",[JSON.stringify(p)])) as any).rows[0].r;
 async function witnessFor(raw:any,kind:"batch"|"selection"|"reset"="batch",tenant=TENANT){
  const p=await prepareDelivery(tenant,raw);
  return rpc("ingest_frozen_source_witness",{kind,tenant,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:raw.records.length,
   candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))}) as Promise<CurrentCustodyPacket>;
 }
 return{db,owner,run,rpc,ledger,deps,seal,witnessFor};
}
const err=async(p:Promise<unknown>)=>{try{await p;return null}catch(e){return String((e as Error).message)}};

test("refuses until the owner seals the complete copy",async()=>{
 const f=await fixture(),raw=await batch([record({record_id:"r1"})],{batch_id:"b1"});
 expect(await err(f.witnessFor(raw))).toContain("frozen source: copy not sealed");
 await f.seal();
 const w=await f.witnessFor(raw);expect(w.witness.sourceIdentitySha256).toBe(identity);
});

test("a sealed frozen witness is accepted end to end by the unchanged lazy-current target",async()=>{
 const f=await fixture();await f.seal();
 const raw=await batch([record({record_id:"fresh-1"}),record({record_id:"fresh-2"})],{batch_id:"frozen-ok"});
 const custody=await f.witnessFor(raw);
 expect(custody.witness).toMatchObject({version:1,issuer:"relayhistory-cloud",controlEpoch:epoch,privacyBaselineSha256:baseline,sourceControlVersion:"0",pending:false,pendingControl:null,privacyVersion:"0",candidateCount:2,privacy:{complete:true,deletions:[],resets:[]}});
 expect(custody.witness.records.every((r:any)=>r.current===null&&r.candidateFenced===false)).toBe(true);
 const d=(await f.db.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d",[JSON.stringify(custody.witness)])).rows[0] as any;
 expect(custody.sourceDigest).toBe(d.d);
 const result=await deliverBatch({...f.deps,custody},TENANT,raw);
 expect(result.custodyAck?.sourceDigest).toBe(custody.sourceDigest);
});

test("frozen current state comes from the copy, and ranks use C byte order like Neon's C.UTF-8",async()=>{
 const f=await fixture();await f.seal();
 const old=record({record_id:"a",revision:8,session_id:"S"});
 const prepared=(await prepareDelivery(TENANT,await batch([old]))).records[0]!;
 await f.db.query("INSERT INTO sessions.ingest_frozen_source_records VALUES($1,$2,$3,'a',$4,8,$5,$6,$7,$8,'upsert',$9,17,NULL)",
  [TENANT.orgId,TENANT.workspaceId,(await prepareDelivery(TENANT,await batch([old]))).originId,prepared.revision_id,prepared.digest,prepared.kind,prepared.source,prepared.session_id,TENANT.userId]);
 const raw=await batch([record({record_id:"b"}),record({record_id:"B"}),record({record_id:"a",revision:9,session_id:"S"})],{batch_id:"rank"});
 const w=(await f.witnessFor(raw)).witness;
 const byId=Object.fromEntries(w.records.map((r:any)=>[r.recordId,r]));
 expect([byId.B.processingRank,byId.a.processingRank,byId.b.processingRank]).toEqual([0,1,2]);
 expect(byId.a.current).toMatchObject({revision:"8",digest:prepared.digest,projectionAcceptedSeq:"17",catalogAcceptedSeq:null,operation:"upsert"});
 expect(byId.b.current).toBeNull();
});

test("a Supabase-first pending control blocks batches; begin/complete mirror Neon's version, digest and PRE",async()=>{
 const f=await fixture();await f.seal();
 const raw=await batch([record({record_id:"p1"})],{batch_id:"pend"}),origin=(await prepareDelivery(TENANT,raw)).originId;
 const payload={origin_id:origin,sessions:[{source:"claude",session_id:"X"}],enabled:false};
 const b=await f.rpc("ingest_source_control_begin",{epoch,tenant:TENANT,kind:"selection",payload});
 const neon=(await f.db.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,'selection',$5::jsonb)::text,'UTF8')),'hex') AS d",[epoch,TENANT.orgId,TENANT.workspaceId,TENANT.userId,JSON.stringify(payload)])).rows[0] as any;
 expect(b).toEqual({epoch,version:"1",digest:neon.d,kind:"selection",pending:true,apply:true});
 expect(await f.rpc("ingest_source_control_begin",{epoch,tenant:TENANT,kind:"selection",payload})).toMatchObject({version:"1",apply:false});
 expect(await err(f.rpc("ingest_source_control_begin",{epoch,tenant:TENANT,kind:"reset",payload:{request_id:"r"}}))).toContain("control pending");
 expect(await err(f.witnessFor(raw))).toContain("privacy control pending");
 const cw=(await f.rpc("ingest_frozen_source_witness",{kind:"selection",tenant:TENANT,originId:origin,batchId:null,batchDigest:null,originalRecordCount:0,candidates:[]})).witness;
 expect(cw).toMatchObject({pending:true,sourceControlVersion:"1",privacyVersion:"0",pendingControl:{epoch,version:"1",digest:neon.d,kind:"selection"},privacy:{complete:true,deletions:[],resets:[]}});
 expect(await f.rpc("ingest_source_control_complete",{epoch,tenant:TENANT,version:"1",digest:neon.d,kind:"selection",outcome:"applied"})).toEqual({completed:true,superseded:false});
 const w=(await f.witnessFor(raw)).witness;
 expect(w).toMatchObject({pending:false,sourceControlVersion:"1",privacyVersion:"1",pendingControl:null,privacy:null});
 const b2=await f.rpc("ingest_source_control_begin",{epoch,tenant:TENANT,kind:"reset",payload:{request_id:"r"}});
 expect(b2).toMatchObject({version:"2",apply:true});
 const cw2=(await f.rpc("ingest_frozen_source_witness",{kind:"reset",tenant:TENANT,originId:null,batchId:null,batchDigest:null,originalRecordCount:0,candidates:[]})).witness;
 expect(cw2).toMatchObject({pending:true,sourceControlVersion:"2",privacyVersion:"1",privacy:null});
});

test("fences union the frozen Neon deletions with live Supabase upload_deletions",async()=>{
 const f=await fixture();await f.seal();
 const raw=await batch([record({record_id:"f1",session_id:"gone"}),record({record_id:"f2",session_id:"live"})],{batch_id:"fence"});
 const p=await prepareDelivery(TENANT,raw),r1=p.records.find(r=>r.record_id==="f1")!;
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,$5,'gone')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,p.originId,r1.source]);
 const w=(await f.witnessFor(raw)).witness;
 const byId=Object.fromEntries(w.records.map((r:any)=>[r.recordId,r]));
 expect(byId.f1.candidateFenced).toBe(true);expect(byId.f2.candidateFenced).toBe(false);
});

test("the ingest role cannot read the frozen tables directly; only the definer functions",async()=>{
 const f=await fixture();await f.seal();
 expect(await err(f.run("SELECT count(*) FROM sessions.ingest_frozen_source_records"))).toContain("permission denied");
 expect(await err(f.run("SELECT count(*) FROM sessions.ingest_source_control_heads"))).toContain("permission denied");
 expect(await err(f.run("SELECT sessions.ingest_frozen_source_privacy('o','w','u')"))).toContain("permission denied");
});

test("rehearsed generation window: readiness flip closes uploads until the atomic rebind reopens them, idempotently",async()=>{
 const {rebindIngestGeneration}=await import("../../hoopdb/ingest-generation-rebind.ts");
 const f=await fixture();await f.seal();
 const next="f".repeat(64),client={query:async(text:string,params?:unknown[])=>(params?.length?f.db.query(text,params as never):f.db.query(text)) as never};
 const raw1=await batch([record({record_id:"g1"})],{batch_id:"gen-a"});
 await deliverBatch({...f.deps,custody:await f.witnessFor(raw1)},TENANT,raw1);
 expect((await f.db.query("SELECT schema_generation FROM sessions.ingest_buffer_admission WHERE org_id=$1",[TENANT.orgId])).rows[0]).toEqual({schema_generation:generation});
 // Owner-apply of a new migration: readiness moves to the new generation.
 await f.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status='pass'",[next]);
 const run=async(s:string,p:unknown[]=[])=>{await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await f.db.query(s,p as never);await f.db.exec("COMMIT");return r as never}catch(e){await f.db.exec("ROLLBACK");throw e}};
 const nextDeps={...f.deps,ledger:sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:next})};
 const raw2=await batch([record({record_id:"g2"})],{batch_id:"gen-b"});
 const witness=async(raw:any)=>{const p=await prepareDelivery(TENANT,raw);return f.rpc("ingest_frozen_source_witness",{kind:"batch",tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:raw.records.length,candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))})};
 expect(await err(deliverBatch({...nextDeps,custody:await witness(raw2)},TENANT,raw2))).not.toBeNull();
 // Wrong direction / not-ready target refuses and changes nothing.
 expect(await err(rebindIngestGeneration(client,{from:next,to:generation}))).toContain("readiness_not_pass_for_target");
 const dry=await rebindIngestGeneration(client,{from:generation,to:next,dryRun:true});
 expect(dry).toMatchObject({policy:"rebound",admissionsRebound:1,dryRun:true});
 expect((await f.db.query("SELECT generation FROM sessions.ingest_lazy_policy")).rows[0]).toEqual({generation});
 const real=await rebindIngestGeneration(client,{from:generation,to:next});
 expect(real).toMatchObject({policy:"rebound",admissionsRebound:1,admissionsTotal:1,dryRun:false});
 const ok=await deliverBatch({...nextDeps,custody:await witness(raw2)},TENANT,raw2);
 expect(ok.custodyAck?.sourceDigest).toMatch(/^[a-f0-9]{64}$/);
 expect(await rebindIngestGeneration(client,{from:generation,to:next})).toMatchObject({policy:"already",admissionsRebound:0});
});

test("HTTP: frozen-supabase builds the witness when relayhistory sends none; shadow logs match without changing the answer",async()=>{
 const {createIngestApp}=await import("../app.ts");
 const {CURRENT_CUSTODY_CONTENT_TYPE,CURRENT_CUSTODY_ACK_HEADER}=await import("../custody-protocol.ts");
 const f=await fixture();await f.seal();
 const logs:Record<string,unknown>[]=[];
 const mk=(custodySource:"relayhistory"|"shadow"|"frozen-supabase")=>createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:(x)=>logs.push(x),custodySource});
 const raw1=await batch([record({record_id:"h1"})],{batch_id:"http-frozen"});
 const r1=await mk("frozen-supabase").request("/v1/delivery/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:raw1})});
 expect(r1.status).toBe(200);
 expect(JSON.parse(r1.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toMatchObject({version:1,sourceControlVersion:"0"});
 const raw2=await batch([record({record_id:"h2"})],{batch_id:"http-shadow"}),packet=await f.witnessFor(raw2);
 const envelope=(p:any)=>JSON.stringify({protocol:"relay-ingest-custody-v1",original:{bodyBase64:Buffer.from(JSON.stringify({protocolVersion:1,batch:raw2})).toString("base64"),contentEncoding:"identity"},...p});
 const r2=await mk("shadow").request("/v1/delivery/batches",{method:"POST",headers:{"content-type":CURRENT_CUSTODY_CONTENT_TYPE},body:envelope(packet)});
 expect(r2.status).toBe(200);
 expect(logs.filter((l)=>l.route==="custody.shadow").map((l)=>l.outcome)).toEqual(["match"]);
 logs.length=0;
 const r3=await mk("relayhistory").request("/v1/delivery/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:await batch([record({record_id:"h3"})],{batch_id:"http-default"})})});
 // Default mode never builds a witness: with none from relayhistory, the lazy target fails closed.
 expect(r3.status).toBe(503);
 expect(logs.some((l)=>l.route==="custody.shadow")).toBe(false);
 expect(r3.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
});

test("HTTP: control mirror routes begin and complete with the tenant from the signed identity",async()=>{
 const {createIngestApp}=await import("../app.ts");
 const f=await fixture();await f.seal();
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{},custodySource:"frozen-supabase"});
 const payload={request_id:"req-1"};
 const b=await app.request("/v1/custody/control/begin",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({epoch,kind:"reset",payload})});
 expect(b.status).toBe(200);const head=await b.json() as any;expect(head).toMatchObject({version:"1",kind:"reset",pending:true,apply:true});
 const c=await app.request("/v1/custody/control/complete",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({epoch,version:head.version,digest:head.digest,kind:"reset",outcome:"applied"})});
 expect(c.status).toBe(200);expect(await c.json()).toEqual({completed:true,superseded:false});
 const bad=await app.request("/v1/custody/control/begin",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({epoch,kind:"reset",payload,tenant:{orgId:"other"}})});
 expect(bad.status).toBe(400);
});

test("HTTP: frozen legacy receipt and pending gate (the two remaining Neon reads on uploads)",async()=>{
 const {createIngestApp}=await import("../app.ts");
 const f=await fixture();await f.seal();
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{},custodySource:"frozen-supabase"});
 const post=(path:string,body:unknown)=>app.request(path,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)});
 expect(await (await post("/v1/custody/legacy-receipt",{originId:"o1",batchId:"b1"})).json()).toEqual({stored:null});
 const receipt={protocolVersion:1,batchId:"b1",acceptanceLevel:"durable",acceptedRevisionIds:["r1"],unsupportedRevisionIds:[]};
 await f.db.query("INSERT INTO sessions.ingest_frozen_source_receipts VALUES($1,$2,'o1','b1',$3,$4)",[TENANT.orgId,TENANT.workspaceId,"9".repeat(64),JSON.stringify(receipt)]);
 expect(await (await post("/v1/custody/legacy-receipt",{originId:"o1",batchId:"b1"})).json()).toEqual({stored:{digest:"9".repeat(64),receipt}});
 expect((await post("/v1/custody/pending",{originId:"o1"})).status).toBe(200);
 await f.rpc("ingest_source_control_begin",{epoch,tenant:TENANT,kind:"reset",payload:{request_id:"q"}});
 expect((await post("/v1/custody/pending",{originId:"o1"})).status).toBe(503);
 expect((await post("/v1/custody/legacy-receipt",{originId:"o1",batchId:"b1"})).status).toBe(503);
});
