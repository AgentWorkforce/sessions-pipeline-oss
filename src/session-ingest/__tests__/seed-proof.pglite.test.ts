import {orderedFixtureLedger,initializeFixtureSeatClock} from "./ordered-support.ts";
import { afterEach, expect, test, setDefaultTimeout } from "bun:test";
import { readFileSync,readdirSync,mkdtempSync,rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectStoreManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { readManifestRecords } from "../../session-read-api/canonical-projection.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { writeBatchObjects } from "../writer.ts";
import { seedOrgFromS3 } from "../seed-s3.ts";
import { SeedProofIndex } from "../seed-proof.ts";
import { seedOrg,exportedFromState,type SeedSql } from "../seed.ts";
import { deliverBatch } from "../deliver.ts";
import { normalizeRelayhistoryRecords } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { TENANT,batch,record,scratchStore } from "./support.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),gen="a".repeat(64),head="b".repeat(40);
const read=(p:string)=>readFileSync(join(root,p),"utf8");
const cleanups:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of cleanups.splice(0))await close()});
const args={org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,source:"claude",session_id:"sess-1",generation:gen};
async function fixture(baseline=true,oldOperation:"upsert"|"delete"="upsert"){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});cleanups.push(()=>db.close());
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const role of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])
  await db.exec(read("supabase/bootstrap/"+role+".sql"));
 for(const file of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())
  await db.exec(read("supabase/migrations/"+file));
 await db.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now());
 INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','${gen}','pass') ON CONFLICT(name)DO UPDATE SET generation=excluded.generation,status=excluded.status;`);
 const sql=(role:string):SeedSql=>({async query(text,params){
  await db.exec("BEGIN;SET LOCAL ROLE "+role);
  try{const out=await db.query(text,params as never);await db.exec("COMMIT");return out as never}
  catch(e){await db.exec("ROLLBACK");throw e}
 }});
 const seeder=sql("bulls96_session_ingest_seeder"),ingest=sql("bulls96_session_ingest");
 const rpc=async(name:string,p:unknown)=>{return(await seeder.query<{r:any}>(`SELECT sessions.${name}($1::jsonb) AS r`,[JSON.stringify(p)])).rows[0]!.r};
 if(baseline)await rpc("ingest_seed_fence_baseline",{org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,deletions:[],resets:[]});
 const fs=scratchStore();cleanups.push(fs.done);
 const dir=mkdtempSync(join(tmpdir(),"seed-proof-sql-"));cleanups.push(()=>rmSync(dir,{recursive:true,force:true}));
 const p=await prepareDelivery(TENANT,await batch([record({record_id:"r1",revision:7,operation:oldOperation,payload:oldOperation==="delete"?null:{text:"original durable",ts_ms:1761955200000}})]));
 const written=await writeBatchObjects({store:fs.store,keyRoot:"96bulls",sleep:async()=>{}},{
  orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:TENANT.userId,originId:p.originId,batchDigest:p.batchDigest,records:p.records});
 const m=written.manifests[0]!;
 const insert=async()=>db.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
 storage_provider,bucket,manifest_key,content_sha256,raw_format,revision,trace_id)
 VALUES($1,$2,'claude','sess-1',$3,$4,now(),now(),'s3','fixture',$5,decode($6,'hex'),'ndjson',$7,$8)`,
 [TENANT.orgId,TENANT.workspaceId,m.manifest.session.machine_id,TENANT.userId,m.key,m.manifest.raw.content_sha256,m.manifest.revision,m.manifest.trace_id]);
 await insert();
 const deps={store:fs.store,bucket:"fixture",provider:"s3",reader:objectStoreManifestReader(fs.store,{provider:"s3",bucket:"fixture"}),
  prefixFor:(org:string)=>"96bulls/"+org,decompress:(raw:Uint8Array,max:number)=>new Uint8Array(zstdDecompressSync(raw,{maxOutputLength:max}))};
 const prepared=async(mode="lease")=>{
  const capture=await rpc("ingest_seed_scope_capture",{...args,mode});
  const index=new SeedProofIndex(join(dir,capture.capture_id??"preview"+".sqlite"),TENANT.orgId,head);
  cleanups.push(()=>index.close());
  for(const pointer of capture.pointers)await index.addPointer(pointer,deps);
  index.seal(capture.pointers);
  const records=[...index.batches()].flatMap(b=>b.records);
  return {capture,records,payload:{capture_id:capture.capture_id,fingerprint:capture.fingerprint,source_head:head,records}};
 };
 return {db,seeder,ingest,rpc,deps,dir,insert,written,prepared};
}
test("actual SQL + verified S3 proof seeds only the durable record; stale exporter/session state is rejected",async()=>{
 const f=await fixture();
 expect(()=>exportedFromState([{session_id:"s",group_revision:5,exported_revision:5,error:null,failed_revision:null}])).toThrow();
 await expect(seedOrg(null,null,"org-a",()=>true)).rejects.toMatchObject({code:"seed_proof_required"});
 await expect(f.rpc("ingest_seed_records",[])).rejects.toMatchObject({code:"42501"});
 const seeded=await seedOrgFromS3(f.seeder,f.deps,TENANT.orgId,gen,{sourceHead:head,cacheDir:f.dir,execute:true});
 expect(seeded).toMatchObject({records:1,upserted:1,enumerated_complete:true});
 expect((await f.db.query("SELECT record_id,revision::text,revision_id,digest,seed_proof_sha256 FROM sessions.ingest_record_revisions")).rows[0])
  .toMatchObject({record_id:"r1",revision:"7",revision_id:null,digest:null});
 const ledger=sqlLedger(f.ingest.query as never);
 const claim=await ledger.claim({tenant:TENANT,originId:"origin-1",batchId:"new-after-stale-export",batchDigest:"c".repeat(64),receiptId:"rhr_"+"d".repeat(64),
  sessions:[["claude","sess-1"]],records:[{source:"claude",sessionId:"sess-1",recordId:"r2",revisionId:"r2:original",revision:2,digest:"e".repeat(64)}]});
 expect(claim.kind).toBe("claimed");
 const replay=await ledger.claim({tenant:TENANT,originId:"origin-1",batchId:"regenerated-old",batchDigest:"d".repeat(64),receiptId:"rhr_"+"e".repeat(64),
  sessions:[["claude","sess-1"]],records:[{source:"claude",sessionId:"sess-1",recordId:"r1",revisionId:"new-regenerated-original-id",revision:7,digest:"f".repeat(64)}]});
 expect(replay.kind).toBe("replay_fast");
 const m=f.written.manifests[0]!.manifest;
 expect((await readManifestRecords(m,f.deps.store,"96bulls/"+TENANT.orgId,f.deps.decompress))[0]!.payload?.text).toBe("original durable");
});
test("preview performs no replay-index/capture write and old seed constraint cannot accept invented IDs",async()=>{
 const f=await fixture();
 expect((await seedOrgFromS3(f.seeder,f.deps,TENANT.orgId,gen,{sourceHead:head,cacheDir:f.dir})).executed).toBe(false);
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]).toEqual({n:0});
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_seed_captures")).rows[0]).toEqual({n:0});
 await expect(f.db.exec("INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id) VALUES('o','w','a','r',1,NULL,NULL,'u','s','id')")).rejects.toThrow();
});
test("source delete/recreate and raw digest/revision mutation reject old captured proof",async()=>{
 const f=await fixture();const p=await f.prepared();
 await f.db.exec("DELETE FROM sessions.session_index");
 await f.insert();
 await expect(f.rpc("ingest_seed_proven_records",p.payload)).rejects.toMatchObject({code:"40001"});
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]).toEqual({n:0});
 const q=await f.prepared();
 await f.db.exec("UPDATE sessions.session_index SET revision=revision+1,content_sha256=decode(repeat('0',64),'hex')");
 await expect(f.rpc("ingest_seed_proven_records",q.payload)).rejects.toMatchObject({code:"40001"});
});
test("disable then enable never resets seed authority; an unknown-session reset count also fences capture",async()=>{
 const f=await fixture();const p=await f.prepared();
 const ledger=sqlLedger(f.ingest.query as never);
 await ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false});
 await ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});
 await expect(f.rpc("ingest_seed_proven_records",p.payload)).rejects.toMatchObject({code:"40001"});
 await expect(f.prepared()).rejects.toMatchObject({code:"55000"});
 const g=await fixture(),q=await g.prepared();
 await sqlLedger(g.ingest.query as never).resetUploadedSessions(TENANT,"reset-before-seed-ownership");
 await expect(g.rpc("ingest_seed_proven_records",q.payload)).rejects.toMatchObject({code:"40001"});
 await expect(g.prepared()).rejects.toMatchObject({code:"55000"});
});
test("foreign owner/namespace/pointer and expired or wrong generation capture refuse before any seed",async()=>{
 const f=await fixture();const p=await f.prepared();
 for(const patch of [{user_id:"foreign"},{org_id:"foreign"},{workspace_id:"foreign"},{source:"codex"},{session_id:"foreign"},{pointer_id:"0".repeat(64)}]){
  await expect(f.rpc("ingest_seed_proven_records",{...p.payload,records:[{...p.records[0],...patch}]})).rejects.toThrow();
 }
 await f.db.query("UPDATE sessions.ingest_seed_captures SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",[p.capture.capture_id]);
 await expect(f.rpc("ingest_seed_proven_records",p.payload)).rejects.toMatchObject({code:"55000"});
 const q=await f.prepared();
 await f.db.exec("UPDATE sessions.__96bulls_role_validation SET status='pending'");
 await expect(f.rpc("ingest_seed_proven_records",q.payload)).rejects.toMatchObject({code:"55000"});
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]).toEqual({n:0});
});
test("repeat seed is idempotent, real higher revision metadata remains exact, all private tables stay inaccessible",async()=>{
 const f=await fixture();const p=await f.prepared();
 expect(await f.rpc("ingest_seed_proven_records",p.payload)).toMatchObject({accepted:true,upserted:1});
 expect(await f.rpc("ingest_seed_proven_records",p.payload)).toMatchObject({accepted:true,upserted:0});
 await f.db.exec("UPDATE sessions.ingest_record_revisions SET revision=9,revision_id='actual-client-id',digest=repeat('a',64)");
 expect(await f.rpc("ingest_seed_proven_records",p.payload)).toMatchObject({accepted:true,upserted:0});
 expect((await f.db.query("SELECT revision::text,revision_id,digest FROM sessions.ingest_record_revisions")).rows[0])
  .toEqual({revision:"9",revision_id:"actual-client-id",digest:"a".repeat(64)});
 await expect(f.seeder.query("SELECT * FROM sessions.ingest_seed_captures")).rejects.toMatchObject({code:"42501"});
 await expect(f.ingest.query("SELECT sessions.ingest_seed_scope_capture($1::jsonb)",[JSON.stringify({...args,mode:"preview"})])).rejects.toMatchObject({code:"42501"});
});


test("one-time fence baseline cannot be reapplied after enable and never follows a prior modern operation",async()=>{
 const f=await fixture(false);
 const baseline={org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,
  deletions:[{origin_id:"origin-1",source:"claude",session_id:"sess-1"}],resets:[]};
 expect(await f.rpc("ingest_seed_fence_baseline",baseline)).toMatchObject({accepted:true,replay:false,deletions:1});
 const ledger=sqlLedger(f.ingest.query as never);
 await ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});
 expect(await f.rpc("ingest_seed_fence_baseline",baseline)).toMatchObject({accepted:true,replay:true,deletions:0});
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.upload_deletions")).rows[0]).toEqual({n:0});
 await expect(f.rpc("ingest_seed_fence_baseline",{...baseline,deletions:[]})).rejects.toMatchObject({code:"55000"});
 await expect(f.rpc("ingest_seed_fences",{deletions:[],resets:[]})).rejects.toMatchObject({code:"42501"});
});
test("a first no-op destination enable makes even an empty stale baseline inadmissible",async()=>{
 const f=await fixture(false),ledger=sqlLedger(f.ingest.query as never);
 await ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});
 await expect(f.rpc("ingest_seed_fence_baseline",{org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,deletions:[],resets:[]})).rejects.toMatchObject({code:"55000"});
 await expect(f.rpc("ingest_seed_scope_capture",{...args,mode:"lease"})).rejects.toMatchObject({code:"55000"});
});

test("a canonical claim permanently closes a previously captured seed baseline",async()=>{
 const f=await fixture(),p=await f.prepared(),ledger=sqlLedger(f.ingest.query as never);
 await ledger.claim({tenant:TENANT,originId:"origin-1",batchId:"canonical-admission",batchDigest:"c".repeat(64),receiptId:"rhr_"+"d".repeat(64),sessions:[["claude","sess-1"]],records:[]});
 await expect(f.rpc("ingest_seed_proven_records",p.payload)).rejects.toMatchObject({code:"40001"});
 await expect(f.prepared()).rejects.toMatchObject({code:"55000"});
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]).toEqual({n:0});
});

for(const control of [
 {name:"regenerated equal revision",revision:7,oldOperation:"upsert",outcome:"replay_fast",text:"original durable",count:1},
 {name:"older revision",revision:6,oldOperation:"upsert",outcome:"replay_fast",text:"original durable",count:1},
 {name:"newer revision",revision:8,oldOperation:"upsert",outcome:"accepted",text:"new content",count:1},
 {name:"old tombstone",revision:7,oldOperation:"delete",outcome:"replay_fast",text:undefined,count:0},
] as const)test("actual S3 proof + SQL + deliver preserves "+control.name,async()=>{
 const f=await fixture(true,control.oldOperation),p=await f.prepared();
 await f.rpc("ingest_seed_proven_records",p.payload);
 const input=await batch([record({record_id:"r1",revision:control.revision,revision_id:"original-client-exact",payload:{text:"new content",ts_ms:1761955200000}})],{batch_id:"cold-proof-"+control.name});
 const result=await deliverBatch({ledger:sqlLedger(f.ingest.query as never),target:{store:f.deps.store,keyRoot:"96bulls",sleep:async()=>{}}},TENANT,input);
 expect(result.outcome).toBe(control.outcome);
 expect(result.receipt.acceptedRevisionIds).toEqual(["original-client-exact"]);
 const latest=await f.deps.reader.read(f.written.manifests[0]!.key);
 const folded=normalizeRelayhistoryRecords(await readManifestRecords(latest!.manifest,f.deps.store,"96bulls/"+TENANT.orgId,f.deps.decompress));
 expect(folded).toHaveLength(control.count);
 expect(folded[0]?.payload?.text).toBe(control.text);
});

async function bufferReceipt(f:Awaited<ReturnType<typeof fixture>>){
 await f.db.exec("ALTER TABLE sessions.ingest_batches ADD COLUMN IF NOT EXISTS publication_state text NOT NULL DEFAULT 'canonical' CHECK(publication_state IN('buffered','canonical'))");
 await f.db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,publication_state) VALUES($1,$2,'origin-1','buffered-receipt',$3,repeat('c',64),'rhr_'||repeat('d',64),'pending','buffered')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
}
test("trusted buffered receipt leaves historical proof open, but explicit canonical admission closes it",async()=>{
 const f=await fixture();
 await f.db.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[TENANT.orgId,gen]);
 await initializeFixtureSeatClock(f.db as never,gen);
 const p=await f.prepared();await bufferReceipt(f);
 expect(await f.rpc("ingest_seed_proven_records",p.payload)).toMatchObject({accepted:true,upserted:1});
 await f.db.exec("UPDATE sessions.ingest_batches SET state='committed',committed_at=clock_timestamp(),buffer_receipt_kind='suppressed' WHERE batch_id='buffered-receipt'");
 expect(await f.rpc("ingest_seed_proven_records",p.payload)).toMatchObject({accepted:true,upserted:0});
 await f.prepared();
 await f.db.exec("UPDATE sessions.ingest_batches SET publication_state='canonical' WHERE batch_id='buffered-receipt'");
 await expect(f.rpc("ingest_seed_proven_records",p.payload)).rejects.toMatchObject({code:"40001"});
 await expect(f.prepared()).rejects.toMatchObject({code:"55000"});
});
for(const action of ["disable","reset"] as const)test("buffered receipt cannot exempt "+action+" from baseline closure",async()=>{
 const f=await fixture();
 await f.db.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[TENANT.orgId,gen]);
 await initializeFixtureSeatClock(f.db as never,gen);
 const p=await f.prepared();await bufferReceipt(f);
 const ledger=orderedFixtureLedger(sqlLedger(f.ingest.query as never,{acceptanceMode:"buffered",expectedGeneration:gen}),f.db as never);
 if(action==="disable")await ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false});
 else await ledger.resetUploadedSessions(TENANT,"buffer-reset");
 await expect(f.rpc("ingest_seed_proven_records",p.payload)).rejects.toMatchObject({code:"40001"});
 await expect(f.prepared()).rejects.toMatchObject({code:"55000"});
});

test("equal seeded NULL-original row remains valid through the actual staged commit RPC",async()=>{
 const f=await fixture(),p=await f.prepared();await f.rpc("ingest_seed_proven_records",p.payload);
 const ledger=sqlLedger(f.ingest.query as never);
 const records=[{source:"claude",sessionId:"sess-1",recordId:"r1",revisionId:"client-equal",revision:7,digest:"e".repeat(64)},
  {source:"claude",sessionId:"sess-1",recordId:"r2",revisionId:"client-new",revision:8,digest:"f".repeat(64)}];
 const claim=await ledger.claim({tenant:TENANT,originId:"origin-1",batchId:"mixed-commit",batchDigest:"c".repeat(64),receiptId:"rhr_"+"d".repeat(64),sessions:[["claude","sess-1"]],records});
 expect(claim.kind).toBe("claimed");
 if(claim.kind!=="claimed")throw new Error("claim_required");
 expect(await ledger.commitRevisions(TENANT,"origin-1",f.written.manifests[0]!.manifest.session.machine_id,records,{batchId:"mixed-commit",batchDigest:"c".repeat(64),claimToken:claim.claimToken})).toEqual({committed:1,rearmed:0});
 expect((await f.db.query("SELECT record_id,revision::text,revision_id,digest FROM sessions.ingest_record_revisions ORDER BY record_id")).rows).toEqual([
  {record_id:"r1",revision:"7",revision_id:null,digest:null},
  {record_id:"r2",revision:"8",revision_id:"client-new",digest:"f".repeat(64)}]);
});
