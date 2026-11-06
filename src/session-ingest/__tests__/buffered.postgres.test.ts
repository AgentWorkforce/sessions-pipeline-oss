import {orderedFixtureLedger,initializeFixtureSeatClock} from "./ordered-support.ts";
/**
 * Opt-in, owned PostgreSQL + filesystem object proof. No deployed credentials.
 */
import {afterEach,describe,test,expect} from "bun:test";
import pg from "pg";
import {sessionsFunctionAcl} from "../../hoopdb/migrate-guards.ts";
import {type MigrationClient} from "../../hoopdb/migrate.ts";
import {readFileSync,readdirSync,mkdtempSync,rmSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {zstdDecompressSync} from "node:zlib";
import {createFsStore} from "../../../packages/object-store/src/fs.ts";
import {objectKey,type ObjectStore} from "../../shared/vendor.ts";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {fanout,readBufferedStage,prepareBufferedReplacement,putBufferedStage} from "../staging.ts";
import {createIngestApp,SYNC_SCOPE} from "../app.ts";
import {batch,record,TENANT,SIGNING_KEY,signedHeaders} from "./support.ts";
import type {Ledger} from "../ledger.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
const root=join(import.meta.dir,"../../..");
const text=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const cleanup:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const f of cleanup.splice(0).reverse())await f();},30000);
async function fixture(admit=true,boundary=false){
 const admin=new pg.Client({connectionString:url});await admin.connect();
 const name="buffer_accept_"+crypto.randomUUID().replaceAll("-","");await admin.query("CREATE DATABASE "+name);
 const target=new URL(url!);target.pathname="/"+name;
 const pool=new pg.Pool({connectionString:target.href});
 const dir=mkdtempSync(join(tmpdir(),"buffer-accept-"));
 const store=createFsStore({root:dir}) as unknown as ObjectStore;
 cleanup.push(async()=>{await pool.end();await admin.query("DROP DATABASE "+name);await admin.end();rmSync(dir,{recursive:true,force:true});});
 await pool.query(text("supabase/tests/supabase_shim.sql"));
 await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
 for(const f of ["session_directory_search_worker.sql","session_token_verifier.sql","session_ingest_roles.sql"])await pool.query(text("supabase/bootstrap/"+f));
 if(boundary)await pool.query(text("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql").replaceAll(":'runtime_role'","'service_role'"));
 for(const f of readdirSync(join(root,"supabase/migrations")).sort()){
  let s=text("supabase/migrations/"+f);
  if(f.includes("040500")&&process.env.BUFFER_MUTATION==="no_receipt_fence"){
   const old=s;s=s.replace(" IF v_invalid THEN"," IF false THEN");if(s===old)throw Error("mutation missed");
  }
  if(f.includes("040500")&&process.env.BUFFER_MUTATION==="transaction_time")s=s.replaceAll("clock_timestamp()","now()");
  await pool.query(s);
 }
 if(boundary)for(const f of readdirSync(join(root,"src/hoopdb/migrations")).sort())await pool.query(text("src/hoopdb/migrations/"+f));
 await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
 await pool.query("UPDATE sessions.__96bulls_role_validation SET generation=repeat('a',64),status='pass'");
 if(!(await pool.query("SELECT count(*)::int AS n FROM sessions.__96bulls_role_validation")).rows[0].n)
  await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker',repeat('a',64),'pass')");
 await pool.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
 try{await pool.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,deletions:[],resets:[]})]);await pool.query("COMMIT")}catch(e){await pool.query("ROLLBACK");throw e}
 if(admit)await pool.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,$2,$2,$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[TENANT.orgId,"a".repeat(64)]);
 if(admit)await initializeFixtureSeatClock(pool,"a".repeat(64));
 const run=async(t:string,p:unknown[])=>{const c=await pool.connect();try{await c.query("SET ROLE bulls96_session_ingest");return await c.query(t,p);}finally{await c.query("RESET ROLE");c.release();}};
 const ledger=orderedFixtureLedger(sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:"a".repeat(64)}),pool);
 const deps={ledger,target:{store,keyRoot:"96bulls"},acceptanceMode:"buffered" as const,inlineMaxSessions:0,fanout:async()=>{throw Error("buffer_queue_called");}};
 const count=async(table:string)=>(await pool.query("SELECT count(*)::int n FROM sessions."+table)).rows[0].n as number;
 const rows=async()=>(await pool.query("SELECT stage_key AS key,stage_sha256 AS sha256,stage_bytes::int AS bytes,group_ordinals AS \"groupOrdinals\",stage_token,claim_token,receipt_state,cleanup_state FROM sessions.ingest_stage_intents ORDER BY created_at")).rows;
 return {pool,store,ledger,deps,count,rows,run};
}
const unzip=(b:Uint8Array)=>new Uint8Array(zstdDecompressSync(b,{maxOutputLength:16*1024*1024}));
describe.skipIf(!url)("buffer-only actual PG receipt/outbox",()=>{
 test("fresh event-trigger apply has exact final function ACL and owner-only internal helpers",async()=>{
  const d=await fixture(true,true);
  const got=await sessionsFunctionAcl({query:async(t,p)=>{const out=await d.pool.query(t,p);return {rows:out.rows};}} as MigrationClient);
  expect(got).toEqual(JSON.parse(text("src/hoopdb/sessions-function-acl.json")).functions);
  await expect(d.run("SELECT sessions.ingest_claim_batch_mode('{}'::jsonb,'canonical')",[])).rejects.toMatchObject({code:"42501"});
  const grants=await d.pool.query("SELECT has_table_privilege('bulls96_session_ingest','sessions.ingest_stage_intents','SELECT') ok");
  expect(grants.rows[0].ok).toBe(false);
 },15000);
 test("signed actual HTTP handler keeps durable receipt wire and original record revisions",async()=>{
  const d=await fixture();
  const app=createIngestApp({signingKey:SIGNING_KEY,deliver:d.deps,log:()=>{}});
  const body=JSON.stringify({protocolVersion:1,batch:await batch([record({record_id:"wire-event"})])});
  const response=await app.fetch(new Request("https://fixture/v1/delivery/batches",{method:"POST",body,headers:signedHeaders({...TENANT,scopes:[SYNC_SCOPE]},"POST","/v1/delivery/batches",new TextEncoder().encode(body))}));
  expect(response.status).toBe(200);
  const receipt=await response.json() as {acceptanceLevel:string,acceptedRevisionIds:string[]};
  expect(receipt.acceptanceLevel).toBe("durable");expect(receipt.acceptedRevisionIds).toEqual(["wire-event:r1"]);
  expect(await d.count("ingest_buffer_outbox")).toBe(1);expect(await d.count("ingest_record_revisions")).toBe(0);
 },15000);
 test("intent expiry is checked after a blocked row lock, not at transaction start",async()=>{
  const d=await fixture();const lock=await d.pool.connect();
  const ledger:Ledger={...d.ledger,async commitBuffered(p){
   await lock.query("BEGIN");
   await lock.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()+interval '0.2 seconds' WHERE stage_token=$1",[p.stageToken]);
   const committing=d.ledger.commitBuffered!(p);
   await new Promise(r=>setTimeout(r,350));
   await lock.query("COMMIT");lock.release();
   await committing;
  }};
  await expect(deliverBatch({...d.deps,ledger},TENANT,await batch([record({record_id:"clock-block"})]))).rejects.toMatchObject({status:503,cause_code:"ledger_stage_invalidated"});
  expect(await d.count("ingest_buffer_outbox")).toBe(0);
 },15000);
 test("ACK requires intent before PUT, atomic outbox, no canonical/queue/replica writes; replay and batch conflict",async()=>{
  const d=await fixture();let puts=0;
  const wrapped: ObjectStore={...d.store,async put(...args){puts++;expect(await d.count("ingest_stage_intents")).toBe(1);expect(await d.count("ingest_buffer_outbox")).toBe(0);return d.store.put(...args);}};
  const b=await batch([record({record_id:"normal"})]);
  const first=await deliverBatch({...d.deps,target:{...d.deps.target,store:wrapped}},TENANT,b);
  expect(first.outcome).toBe("accepted_buffered");expect(first.stats.manifests_written).toBe(0);expect(puts).toBe(1);
  expect(await d.count("ingest_record_revisions")).toBe(0);expect(await d.count("ingest_buffer_outbox")).toBe(1);
  const [r]=await d.rows();expect(r.receipt_state).toBe("acknowledged");
  expect((await readBufferedStage(d.store,r,unzip)).groups[0]?.lines[0]).toContain("normal");
  const keys=(await d.store.list(objectKey("96bulls"))).items.map(x=>String(x.key));
  expect(keys).toHaveLength(1);expect(keys[0]).toContain("/buffered-batches/v1/");
  expect((await deliverBatch(d.deps,TENANT,b)).outcome).toBe("replay");
  await expect(deliverBatch(d.deps,TENANT,await batch([record({record_id:"different"})]))).rejects.toMatchObject({status:409});
 },15000);
 test("missing and expired privacy/retention baseline refuse before batch or object",async()=>{
  const d=await fixture(false);const b=await batch([record({record_id:"missing"})]);
  await expect(deliverBatch(d.deps,TENANT,b)).rejects.toMatchObject({status:503});
  expect(await d.count("ingest_batches")).toBe(0);expect(await d.count("ingest_stage_intents")).toBe(0);
  await d.pool.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,$2,$2,$2,clock_timestamp()-interval '1 second',repeat('3',64))",[TENANT.orgId,"a".repeat(64)]);
  await expect(deliverBatch(d.deps,TENANT,b)).rejects.toMatchObject({status:503});
  expect((await d.store.list(objectKey("96bulls"))).items).toHaveLength(0);
 },15000);
 test("unknown historical in-batch twins defer503, never premature409 or receipt",async()=>{
  const d=await fixture();
  await expect(deliverBatch(d.deps,TENANT,await batch([record({record_id:"twins",payload:{text:"one"}}),record({record_id:"twins",revision_id:"twins:other",payload:{text:"two"}})]))).rejects.toMatchObject({status:503,cause_code:"ledger_baseline_pending"});
  expect(await d.count("ingest_batches")).toBe(0);expect(await d.count("ingest_stage_intents")).toBe(0);
 },15000);
 test("PUT failure keeps durable preparing intent and no ACK; retry can commit",async()=>{
  const d=await fixture();const b=await batch([record({record_id:"failed"})]);
  const bad={...d.store,async put(){throw Error("own synthetic put failure");}} as ObjectStore;
  await expect(deliverBatch({...d.deps,target:{...d.deps.target,store:bad}},TENANT,b)).rejects.toMatchObject({status:503});
  expect(await d.count("ingest_stage_intents")).toBe(1);expect(await d.count("ingest_buffer_outbox")).toBe(0);
  expect((await d.rows())[0].receipt_state).toBe("preparing");
  expect((await deliverBatch(d.deps,TENANT,b)).outcome).toBe("accepted_buffered");
  expect(await d.count("ingest_buffer_outbox")).toBe(1);
 },15000);
 for(const which of ["disable-enable","reset-no-session","retired","expired"]){
 test("during PUT "+which+" cannot later acknowledge invalid attempt",async()=>{
  const d=await fixture();
  const store={...d.store,async put(...args:Parameters<ObjectStore["put"]>){
   const result=await d.store.put(...args);
   if(which==="disable-enable"){
    await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false});
    await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});
   }else if(which==="reset-no-session")await d.ledger.resetUploadedSessions(TENANT,"reset-during-put");
   else if(which==="retired")await d.pool.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='retired'");
   else await d.pool.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second'");
   return result;
  }} as ObjectStore;
  const r=record({record_id:"fenced",...(which==="reset-no-session"?{session_id:null,kind:"presence" as const}:{})});
  await expect(deliverBatch({...d.deps,target:{...d.deps.target,store}},TENANT,await batch([r]))).rejects.toMatchObject({status:503,cause_code:"ledger_stage_invalidated"});
  expect(await d.count("ingest_buffer_outbox")).toBe(0);
  expect((await d.pool.query("SELECT state FROM sessions.ingest_batches")).rows[0].state).toBe("pending");
 },15000);}
 test("ambiguous receipt COMMIT replays same durable ACK and never duplicates outbox",async()=>{
  const d=await fixture();let lost=true;
  const ledger:Ledger={...d.ledger,async commitBuffered(p){await d.ledger.commitBuffered!(p);if(lost){lost=false;throw Error("response lost");}}};
  const b=await batch([record({record_id:"lost-receipt"})]);
  await expect(deliverBatch({...d.deps,ledger},TENANT,b)).rejects.toMatchObject({status:503});
  expect((await deliverBatch(d.deps,TENANT,b)).outcome).toBe("replay");expect(await d.count("ingest_buffer_outbox")).toBe(1);
 },15000);
 test("buffered bytes never fan out even under canonical mode and SQL rejects canonical fanout fence",async()=>{
  const d=await fixture();await deliverBatch(d.deps,TENANT,await batch([record({record_id:"no-publish"})]));
  const [r]=await d.rows();
  await expect(fanout(d.deps.target,{v:1,kind:"ingest-fanout",key:r.key,from:0,to:1},unzip,d.ledger)).rejects.toThrow("buffered_publication_refused");
  await expect(d.ledger.fanoutFence(TENANT,"origin-1",[["claude","sess-1"]],{batchDigest:(await d.pool.query("SELECT digest FROM sessions.ingest_batches")).rows[0].digest,claimToken:r.claim_token})).rejects.toMatchObject({code:"ledger_sqlstate_55000"});
  expect(await d.count("ingest_record_revisions")).toBe(0);
 },15000);
 test("mixed stage rewrite preserves exact kept ordinal mapping and scrubbed content; redaction barrier defeats late create",async()=>{
  const d=await fixture();await deliverBatch(d.deps,TENANT,await batch([record({record_id:"keep",session_id:"keep-session"}),record({record_id:"erase",session_id:"erase-session"})]));
  const [r]=await d.rows();const decoded=await readBufferedStage(d.store,r,unzip);
  const replacement=prepareBufferedReplacement("96bulls",{...r,stageToken:r.stage_token,claimToken:r.claim_token,leaseToken:crypto.randomUUID(),receiptState:"acknowledged",rowVersion:"2",resetEpoch:"0",phase:"plan",cleanupState:"live",keepOrdinals:[0],keepGroupIndexes:[0]},decoded);
  expect(replacement.spec.groupOrdinals).toEqual([[0]]);expect(replacement.spec.key).not.toBe(r.key);
  await putBufferedStage(d.store,replacement);
  expect((await readBufferedStage(d.store,replacement.spec,unzip)).groups).toHaveLength(1);
  await d.store.put(objectKey(replacement.spec.key),new TextEncoder().encode('{"redacted":true}'),{contentType:"application/json"});
  await expect(putBufferedStage(d.store,replacement)).rejects.toThrow();
 },15000);
});

