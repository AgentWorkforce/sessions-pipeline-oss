import {orderedFixtureLedger,initializeFixtureSeatClock} from "./ordered-support.ts";
import {afterEach,expect,setDefaultTimeout,test} from "bun:test";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {createHash,randomUUID} from "node:crypto";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {sqlLedger} from "../ledger-sql.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),open:PGlite[]=[];
afterEach(async()=>{for(const db of open.splice(0))await db.close()});
const hash=(s:string)=>createHash("sha256").update(s).digest("hex");
const generation="a".repeat(64);
const tenant={orgId:"org-a",workspaceId:"ws-a",userId:"user-a"},origin="origin-a";
async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(db);
 const read=(path:string)=>readFileSync(join(root,path),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const name of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])
  await db.exec(read("supabase/bootstrap/"+name+".sql"));
 for(const name of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())
  await db.exec(read("supabase/migrations/"+name));
 await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now());INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','"+generation+"','pass') ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status=excluded.status");
 const as=async(role:string,text:string,params:unknown[]=[])=>{
  await db.exec("BEGIN;SET LOCAL ROLE "+role);
  try{const r=await db.query(text,params as never);await db.exec("COMMIT");return r}
  catch(e){await db.exec("ROLLBACK");throw e}
 };
 const rpc=async(name:string,p:unknown)=>(await as("bulls96_session_ingest","SELECT sessions.ingest_buffer_gc_"+name+"($1::jsonb) AS r",[JSON.stringify(name==="claim"?{generation,...p as any}:p)])).rows[0]!.r as any;
 await db.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[tenant.orgId,generation]);
 await initializeFixtureSeatClock(db as never,generation);
 const ledger=orderedFixtureLedger(sqlLedger(((s:string,p:unknown[])=>as("bulls96_session_ingest",s,p)) as never,{acceptanceMode:"buffered",expectedGeneration:generation}),db as never);
 let serial=0;
 async function stage(opts:{ack?:boolean;groups?:number[][];expired?:boolean}={}){
  const batch="batch-"+(++serial),claim=randomUUID(),token=randomUUID(),digest=hash(batch),sha=hash(token);
  const keyFor=(bodySha:string)=>"96bulls/org-a/buffered-batches/v1/ws=ws-a/m=fixture/b="+hash(digest).slice(0,32)+"-"+bodySha.slice(0,16)+"-"+hash(claim).slice(0,32)+".staged.ndjson.zst";
  const groups=opts.groups??[[0],[1],[2]];
  const duplicate=new Set(groups.flat()).size!==groups.flat().length,ackGroups=duplicate?[[0],[1],[2]]:groups;
  await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state) VALUES($1,$2,$3,$4,$5,$6,'rhr_'||repeat('a',64),$7,CASE WHEN $8 THEN clock_timestamp() END,'buffered')",[tenant.orgId,tenant.workspaceId,origin,batch,tenant.userId,digest,"pending",false]);
  const records=[{ord:0,source:"claude",session_id:"gone",record_id:"g",revision_id:"g:1",revision:1,digest:hash("g")},
   {ord:1,source:"claude",session_id:"live",record_id:"l",revision_id:"l:1",revision:1,digest:hash("l")},
   {ord:2,source:"claude",session_id:null,record_id:"n",revision_id:"n:1",revision:1,digest:hash("n")}];
  await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch,schema_generation) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10)",[claim,tenant.orgId,tenant.workspaceId,tenant.userId,origin,batch,digest,JSON.stringify([{source:"claude",session_id:"gone",epoch:0,reset_epoch:0},{source:"claude",session_id:"live",epoch:0,reset_epoch:0}]),JSON.stringify(records),generation]);
  await db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,receipt_state,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,10,$11,0,$12,clock_timestamp()+$13::interval)",[token,claim,tenant.orgId,tenant.workspaceId,tenant.userId,origin,batch,digest,keyFor(sha),sha,JSON.stringify(ackGroups),"preparing","5 minutes"]);
  if(opts.ack!==false){
   await db.query("INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token) VALUES($1,$2,$3,$4,$5,$6,$7)",[tenant.orgId,tenant.workspaceId,origin,batch,tenant.userId,digest,token]);
   await db.query("UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged' WHERE stage_token=$1",[token]);
   await db.query("UPDATE sessions.ingest_batches SET state='committed',committed_at=clock_timestamp(),buffer_receipt_kind='staged' WHERE batch_id=$1",[batch]);
  }
  if(duplicate)await db.query("UPDATE sessions.ingest_stage_intents SET group_ordinals=$1 WHERE stage_token=$2",[JSON.stringify(groups),token]);
  if(opts.expired)await db.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[token]);
  return {batch,claim,token,digest,keyFor,sha};
 }
 const disable=()=>ledger.setSessionUploads({tenant,originId:origin,sessions:[["claude","gone"]],enabled:false});
 return {db,rpc,as,ledger,stage,disable};
}
const lease=(t:any)=>({stageToken:t.stageToken,leaseToken:t.leaseToken});

test("mixed acknowledged erasure preserves kept groups, registers before PUT, swaps atomically and resumes cleanup",async()=>{
 const f=await fixture(),s=await f.stage();await f.disable();
 const t=(await f.rpc("claim",{limit:10})).tickets[0];
 expect(t).toMatchObject({stageToken:s.token,claimToken:s.claim,receiptState:"acknowledged",phase:"plan",keepGroupIndexes:[1,2],groupOrdinals:[[0],[1],[2]],rowVersion:"2"});
 const sha=hash("replacement"),spec={...lease(t),key:s.keyFor(sha),sha256:sha,bytes:8,groupOrdinals:[[1],[2]]};
 await expect(f.rpc("prepare",{...spec,groupOrdinals:[[0],[1]]})).rejects.toMatchObject({code:"22023"});
 await expect(f.rpc("prepare",{...spec,key:spec.key.replace("org-a","org-foreign")})).rejects.toMatchObject({code:"22023"});
 const child=await f.rpc("prepare",spec);
 expect((await f.db.query("SELECT receipt_state,cleanup_state,parent_stage_token,reset_epoch::text FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child.replacementStageToken])).rows[0]).toEqual({receipt_state:"preparing",cleanup_state:"live",parent_stage_token:s.token,reset_epoch:"0"});
 await f.db.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[child.replacementStageToken]);
 expect((await f.rpc("claim",{limit:10})).tickets).toEqual([]);
 await expect(f.rpc("finish",{...lease(t),barrierVersionId:"barrier-v1"})).rejects.toMatchObject({code:"40001"});
 await expect(f.rpc("swap",{...lease(t),replacementStageToken:randomUUID()})).rejects.toMatchObject({code:"40001"});
 expect(await f.rpc("swap",{...lease(t),replacementStageToken:child.replacementStageToken})).toEqual({swapped:true});
 expect((await f.db.query("SELECT active_stage_token,state FROM sessions.ingest_buffer_outbox")).rows[0]).toEqual({active_stage_token:child.replacementStageToken,state:"buffered"});
 expect((await f.db.query("SELECT receipt_state,cleanup_state,claim_token,group_ordinals FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child.replacementStageToken])).rows[0]).toEqual({receipt_state:"acknowledged",cleanup_state:"live",claim_token:s.claim,group_ordinals:[[1],[2]]});
 // Crash after swap: old raw is no longer required for the cleanup ticket.
 await f.db.exec("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE cleanup_state='retired'");
 const cleanup=(await f.rpc("claim",{limit:10})).tickets.find((x:any)=>x.stageToken===s.token);
 expect(cleanup.phase).toBe("cleanup");
 expect(await f.rpc("finish",{...lease(cleanup),barrierVersionId:"persistent-S3-barrier-version"})).toEqual({finished:true});
 expect(await f.rpc("finish",{...lease(cleanup),barrierVersionId:"persistent-S3-barrier-version"})).toEqual({finished:true});
 expect((await f.db.query("SELECT cleanup_state,gc_barrier_version_id FROM sessions.ingest_stage_intents WHERE stage_token=$1",[s.token])).rows[0]).toEqual({cleanup_state:"redacted",gc_barrier_version_id:"persistent-S3-barrier-version"});
});
test("reset invalidates no-session groups and all-erased swap keeps an erased durable receipt",async()=>{
 const f=await fixture(),s=await f.stage();
 await f.ledger.resetUploadedSessions(tenant,"reset-all");
 const t=(await f.rpc("claim",{limit:10})).tickets[0];expect(t.keepGroupIndexes).toEqual([]);
 expect(await f.rpc("swap",{...lease(t),replacementStageToken:null})).toEqual({swapped:true});
 expect((await f.db.query("SELECT active_stage_token,state FROM sessions.ingest_buffer_outbox")).rows[0]).toEqual({active_stage_token:null,state:"erased"});
 await expect(f.rpc("finish",{...lease(t),barrierVersionId:"null"})).rejects.toMatchObject({code:"22023"});
 expect(await f.rpc("finish",{...lease(t),barrierVersionId:"barrier-reset"})).toEqual({finished:true});
});
test("expired or fenced unacknowledged intent is durably retired without ACK",async()=>{
 const f=await fixture(),s=await f.stage({ack:false,expired:true});
 const t=(await f.rpc("claim",{limit:10})).tickets[0];
 expect(t).toMatchObject({phase:"cleanup",receiptState:"preparing",keepGroupIndexes:[]});
 expect((await f.db.query("SELECT cleanup_state,receipt_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[s.token])).rows[0]).toEqual({cleanup_state:"retired",receipt_state:"preparing"});
 expect(await f.rpc("finish",{...lease(t),barrierVersionId:"barrier-before-late-put"})).toEqual({finished:true});
});
test("healthy first candidates do not starve later erasure and child reset epochs never refresh",async()=>{
 const f=await fixture();
 for(let i=0;i<5;i++)await f.stage({groups:[[1]]});
 const s=await f.stage();await f.disable();
 expect((await f.rpc("claim",{limit:1})).tickets).toEqual([]);
 const t=(await f.rpc("claim",{limit:1})).tickets[0];expect(t.stageToken).toBe(s.token);
 const sha=hash("child");
 const child=await f.rpc("prepare",{...lease(t),key:s.keyFor(sha),sha256:sha,bytes:8,groupOrdinals:[[1],[2]]});
 await f.ledger.resetUploadedSessions(tenant,"reset-during-copy");
 await expect(f.rpc("swap",{...lease(t),replacementStageToken:child.replacementStageToken})).rejects.toMatchObject({code:"40001"});
 expect((await f.db.query("SELECT active_stage_token FROM sessions.ingest_buffer_outbox WHERE batch_id=$1",[s.batch])).rows[0]).toEqual({active_stage_token:s.token});
 expect((await f.db.query("SELECT reset_epoch::text,receipt_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child.replacementStageToken])).rows[0]).toEqual({reset_epoch:"0",receipt_state:"preparing"});
});
test("expired lease and changed outbox version refuse swap; role and tables stay closed",async()=>{
 const f=await fixture(),s=await f.stage();await f.disable();
 const t=(await f.rpc("claim",{limit:10})).tickets[0],sha=hash("child");
 const child=await f.rpc("prepare",{...lease(t),key:s.keyFor(sha),sha256:sha,bytes:8,groupOrdinals:[[1],[2]]});
 await f.db.exec("UPDATE sessions.ingest_buffer_outbox SET row_version=row_version+1");
 expect(await f.rpc("swap",{...lease(t),replacementStageToken:child.replacementStageToken})).toEqual({swapped:false});
 await f.db.exec("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token='"+s.token+"'");
 await expect(f.rpc("swap",{...lease(t),replacementStageToken:child.replacementStageToken})).rejects.toMatchObject({code:"40001"});
 await expect(f.as("bulls96_session_ingest_seeder","SELECT sessions.ingest_buffer_gc_claim('{\"limit\":1}')")).rejects.toMatchObject({code:"42501"});
 await expect(f.as("bulls96_session_ingest","SELECT * FROM sessions.ingest_stage_intents")).rejects.toMatchObject({code:"42501"});
});
test("malformed cross-session and duplicate ordinals never permit dropping an acknowledged group",async()=>{
 for(const groups of [[[0,1]],[[0],[0]]] as number[][][]){
  const f=await fixture();await f.stage({groups});await f.disable();
  await expect(f.rpc("claim",{limit:10})).rejects.toMatchObject({code:"55000"});
 }
});

test("pure ingest phase2 accepts bare/profiled roles by OID and rejects effective PUBLIC/MAINTAIN/column drift",async()=>{
 const f=await fixture(),verify=readFileSync(join(root,"supabase/bootstrap/session_ingest_verify.sql"),"utf8");
 await f.db.exec("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC;SET search_path=sessions,public");
 await f.db.exec(verify);
 await f.db.exec("GRANT SELECT(stage_key) ON sessions.ingest_stage_intents TO PUBLIC");
 await expect(f.db.exec(verify)).rejects.toMatchObject({code:"55000"});
 await f.db.exec("REVOKE SELECT(stage_key) ON sessions.ingest_stage_intents FROM PUBLIC");
 if(Number((await f.db.query<{v:string}>("SELECT current_setting('server_version_num') AS v")).rows[0]!.v)>=170000){
  await f.db.exec("GRANT MAINTAIN ON sessions.ingest_stage_intents TO PUBLIC");
  await expect(f.db.exec(verify)).rejects.toMatchObject({code:"55000"});
  await f.db.exec("REVOKE MAINTAIN ON sessions.ingest_stage_intents FROM PUBLIC");
 }
 await f.db.exec("ALTER ROLE bulls96_session_ingest LOGIN");
 await expect(f.db.exec(verify)).rejects.toMatchObject({code:"55000"});
 await f.db.exec(readFileSync(join(root,"supabase/bootstrap/session_ingest_role.sql"),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n"));
 await f.db.exec(verify);
 await f.db.exec("ALTER ROLE bulls96_session_ingest SET statement_timeout='60s'");
 await expect(f.db.exec(verify)).rejects.toMatchObject({code:"55000"});
});

test("publisher's shared exclusion lease blocks GC but cannot masquerade as a GC plan",async()=>{
 const f=await fixture(),s=await f.stage(),token=randomUUID();
 await f.db.query("UPDATE sessions.ingest_stage_intents SET row_version=row_version+1,gc_lease_version=row_version+1,gc_lease_token=$1,gc_lease_until=clock_timestamp()+interval '5 minutes',gc_keep_groups=NULL WHERE stage_token=$2",[token,s.token]);
 await f.disable();
 expect((await f.rpc("claim",{limit:10})).tickets).toEqual([]);
 const sha=hash("publisher-copy");
 await expect(f.rpc("prepare",{stageToken:s.token,leaseToken:token,key:s.keyFor(sha),sha256:sha,bytes:8,groupOrdinals:[[1],[2]]})).rejects.toMatchObject({code:"40001"});
 await expect(f.rpc("swap",{stageToken:s.token,leaseToken:token,replacementStageToken:null})).rejects.toMatchObject({code:"40001"});
 await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[s.token]);
 const t=(await f.rpc("claim",{limit:10})).tickets[0];
 expect(t.stageToken).toBe(s.token);expect(t.leaseToken).not.toBe(token);
});

test("ticket matches the shared consumer fields including original reset epoch and cleanup state",async()=>{
 const f=await fixture(),s=await f.stage();await f.disable();
 const t=(await f.rpc("claim",{limit:1})).tickets[0];
 expect(t).toEqual({stageToken:s.token,claimToken:s.claim,leaseToken:expect.any(String),rowVersion:"2",
  key:s.keyFor(s.sha),sha256:s.sha,bytes:10,groupOrdinals:[[0],[1],[2]],keepGroupIndexes:[1,2],keepOrdinals:[1,2],
  receiptState:"acknowledged",phase:"plan",resetEpoch:"0",cleanupState:"live"});
});
test("a legal mixed-source no-session group remains readable and reset removes every original ordinal",async()=>{
 const {prepareDelivery,deliveryAccount}=await import("../vendor/delivery/prepare.ts");
 const {groupRecords}=await import("../writer.ts");
 const {batch,record}=await import("./support.ts");
 const f=await fixture();
 const input=await batch([record({record_id:"n0",origin_id:origin,source:"claude",session_id:null}),
  record({record_id:"n1",origin_id:origin,source:"codex",session_id:null})],{origin_id:origin});
 input.account_id=await deliveryAccount(tenant);
 const p=await prepareDelivery(tenant,input),grouped=groupRecords(p.records,new Date("2026-11-04T00:00:00Z"));
 expect(grouped.index).toEqual([[0,1]]);
 const s=await f.stage({groups:grouped.index});
 const records=p.records.map((r,ord)=>({ord,source:r.source,session_id:r.session_id,record_id:r.record_id,
  revision_id:r.revision_id,revision:r.revision,digest:r.digest}));
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$1,epochs='[]' WHERE claim_token=$2",[JSON.stringify(records),s.claim]);
 expect((await f.rpc("claim",{limit:1})).tickets).toEqual([]);
 await f.ledger.resetUploadedSessions(tenant,"mixed-no-session-reset");
 const t=(await f.rpc("claim",{limit:1})).tickets[0];expect(t.keepGroupIndexes).toEqual([]);
 expect(await f.rpc("swap",{...lease(t),replacementStageToken:null})).toEqual({swapped:true});
 expect(await f.rpc("finish",{...lease(t),barrierVersionId:"barrier-mixed-no-session"})).toEqual({finished:true});
});
test("two expired parent copies retain the same deterministic prospective child until swap",async()=>{
 const f=await fixture(),s=await f.stage();await f.disable();
 const t0=(await f.rpc("claim",{limit:1})).tickets[0],sha=hash("same-kept-body");
 const spec={key:s.keyFor(sha),sha256:sha,bytes:8,groupOrdinals:[[1],[2]]};
 const child=await f.rpc("prepare",{...lease(t0),...spec});
 await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[s.token]);
 await f.db.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[child.replacementStageToken]);
 const t1=(await f.rpc("claim",{limit:1})).tickets[0];expect(t1.stageToken).toBe(s.token);
 expect((await f.rpc("prepare",{...lease(t1),...spec})).replacementStageToken).toBe(child.replacementStageToken);
 await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[s.token]);
 const tickets=(await f.rpc("claim",{limit:1})).tickets;
 expect(tickets.map((t:any)=>t.stageToken)).toEqual([s.token]);
 const t2=tickets[0];
 expect((await f.rpc("prepare",{...lease(t2),...spec})).replacementStageToken).toBe(child.replacementStageToken);
 expect((await f.db.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child.replacementStageToken])).rows[0]).toEqual({cleanup_state:"live"});
 expect(await f.rpc("swap",{...lease(t2),replacementStageToken:child.replacementStageToken})).toEqual({swapped:true});
 expect(await f.rpc("finish",{...lease(t2),barrierVersionId:"barrier-parent-after-retry"})).toEqual({finished:true});
});

test("prospective ownership ends when original kept groups shrink and orphan cleanup remains resumable",async()=>{
 const f=await fixture(),s=await f.stage();await f.disable();
 const t0=(await f.rpc("claim",{limit:1})).tickets[0],sha=hash("future-invalid-child");
 const child=await f.rpc("prepare",{...lease(t0),key:s.keyFor(sha),sha256:sha,bytes:8,groupOrdinals:[[1],[2]]});
 await f.ledger.resetUploadedSessions(tenant,"shrink-after-copy");
 await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[s.token]);
 const t=(await f.rpc("claim",{limit:1})).tickets[0];
 expect(t.stageToken).toBe(s.token);expect(t.keepGroupIndexes).toEqual([]);
 expect((await f.db.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child.replacementStageToken])).rows[0]).toEqual({cleanup_state:"retired"});
 expect(await f.rpc("swap",{...lease(t),replacementStageToken:null})).toEqual({swapped:true});
 const orphan=(await f.rpc("claim",{limit:10})).tickets.find((x:any)=>x.stageToken===child.replacementStageToken);
 expect(orphan).toMatchObject({phase:"cleanup",cleanupState:"retired",receiptState:"preparing"});
 expect(await f.rpc("finish",{...lease(orphan),barrierVersionId:"barrier-orphan"})).toEqual({finished:true});
 expect(await f.rpc("finish",{...lease(t),barrierVersionId:"barrier-retired-parent"})).toEqual({finished:true});
});
