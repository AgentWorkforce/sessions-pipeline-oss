import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {createHash} from "node:crypto";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {orderedFixtureLedger,fixtureCustodyDescriptor} from "./ordered-support.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),generation="a".repeat(64),epoch="3".repeat(64),open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});
const hash=(s:string)=>createHash("sha256").update(s).digest("hex");
async function fixture(rows:any[]=[],orgs=[TENANT.orgId],scope="all-existing-orgs"){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());
 const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
 await db.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,deletions:[],resets:[]})]);await db.exec("COMMIT");
 for(const org of orgs)await db.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)",[org,generation,epoch]);
 const payload={protocol:"delivery-custody-page-v1",controlEpoch:epoch,orgId:TENANT.orgId,page:"0",records:rows};
 const pageSha256=(await db.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS s",[JSON.stringify(payload)])).rows[0]!.s as string;
 const d=fixtureCustodyDescriptor(epoch,"9007199254740993",orgs);d.custody.scope=scope;
 if(rows.length)d.custody.orgs[0]={orgId:TENANT.orgId,records:String(rows.length),pages:"1",chainSha256:hash("0".repeat(64)+"\n"+pageSha256)};
 const descriptorText=JSON.stringify(d);
 await db.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...d.baselineAcceptanceFloor,generation,sourceIdentitySha256:d.custody.sourceIdentitySha256,sourceProofSha256:hash(descriptorText)})]);
 async function owner(name:string,p:unknown){return(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any}
 const tokens=new Map<string,string>();
 const begin=async(orgId=TENANT.orgId)=>{const r=await owner("ingest_custody_begin",{generation,orgId,descriptorText});tokens.set(orgId,r.importToken);return r};
 const page=async(p=payload,sha=pageSha256)=>owner("ingest_custody_page",{generation,importToken:tokens.get(p.orgId),payload:p,pageSha256:sha});
 const complete=async(orgId=TENANT.orgId)=>owner("ingest_custody_complete",{generation,importToken:tokens.get(orgId)});
 const release=()=>owner("ingest_custody_release",{generation,descriptorText});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=orderedFixtureLedger(sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation}),db as never);
 const fs=scratchStore();open.push(fs.done);
 const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 const send=async(id:string,r:any)=>deliverBatch(deps,TENANT,await batch([r],{batch_id:id}));
 const seats=async(id:string)=>(await db.query("SELECT sessions.ingest_receipt_seats_for_stage(o.active_stage_token) AS r FROM sessions.ingest_buffer_outbox o WHERE batch_id=$1",[id])).rows[0]!.r as any;
 return{db,d,descriptorText,payload,pageSha256,owner,begin,page,complete,release,run,ledger,send,seats,tokens};
}
async function metadata(raw:ReturnType<typeof record>){
 const prepared=await prepareDelivery(TENANT,await batch([raw]));
 const r=prepared.records[0]!;
 return{workspaceId:TENANT.workspaceId,userId:TENANT.userId,originId:r.origin_id,recordId:r.record_id,revision:String(r.revision),
  digest:r.digest,revisionId:r.revision_id,kind:r.kind,source:r.source,sessionId:r.session_id,operation:r.operation,catalogAcceptedSeq:"17",projectionAcceptedSeq:null};
}
test("old8/B source custody makes exact/changed8 and lower1/A ignored; erase B permits fresh1/C; canonical ledger stays empty",async()=>{
 const raw=record({record_id:"legacy-custody",revision:8,session_id:"B",payload:{text:"Σİ😀é",large:1e100,unsafe_integer:9007199254740992}});
 const meta=await metadata(raw),f=await fixture([meta]);
 await f.begin();await expect(f.complete()).rejects.toMatchObject({code:"55000"});
 await expect(f.send("incomplete",raw)).rejects.toThrow();
 await f.page();await f.complete();await expect(f.send("not-global",raw)).rejects.toThrow();
 await f.release();
 await f.send("exact8",raw);const exact=(await f.seats("exact8")).records[0];
 expect(exact.bodyAccepted).toBe(false);expect(exact.candidateDigest).toBe(meta.digest);expect(exact.firstDigest).toBe(meta.digest);
 await f.send("changed8",{...raw,payload:{text:"changed"}});const changed=(await f.seats("changed8")).records[0];
 expect(changed.bodyAccepted).toBe(false);expect(changed.firstDigest).toBe(meta.digest);expect(changed.candidateDigest).not.toBe(meta.digest);
 const a=record({record_id:raw.record_id,revision:1,session_id:"A",payload:{text:"ignored A"}});
 await f.send("lower-A",a);const ignored=(await f.seats("lower-A")).records[0];
 expect(ignored.bodyAccepted).toBe(false);expect(ignored.firstDigest).toBe(meta.digest);
 await f.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","B"]],enabled:false});
 const c={...a,session_id:"C",payload:{text:"fresh C"}};
 await f.send("fresh-C",c);const fresh=(await f.seats("fresh-C")).records[0];
 expect(fresh.bodyAccepted).toBe(true);expect(fresh.firstDigest).toBe(fresh.candidateDigest);
 expect((await f.seats("lower-A")).records[0]).toEqual(ignored);
 expect((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0]!.n).toBe(0);
 expect((await f.db.query("SELECT metadata FROM sessions.ingest_custody_records")).rows[0]!.metadata).toEqual(meta);
});
test("page hashes, replay identity, source/org boundaries and completion coverage fail closed",async()=>{
 const f=await fixture([await metadata(record({record_id:"one"}))]);
 await f.begin();
 await expect(f.page({...f.payload,page:"1"})).rejects.toThrow();
 await expect(f.page(f.payload,"f".repeat(64))).rejects.toMatchObject({code:"55000"});
 await expect(f.page({...f.payload,orgId:"foreign"})).rejects.toThrow();
 await expect(f.owner("ingest_custody_begin",{generation,orgId:TENANT.orgId,descriptorText:f.descriptorText+" "})).rejects.toMatchObject({code:"55000"});
 const one=await f.page();expect(one.nextPage).toBe("1");
 expect((await f.page()).replay).toBe(true);
 const changed={...f.payload,records:[{...f.payload.records[0],source:"codex"}]};
 const sha=(await f.db.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS s",[JSON.stringify(changed)])).rows[0]!.s as string;
 await expect(f.page(changed,sha)).rejects.toMatchObject({code:"55000"});
 await f.complete();await f.release();
 expect((await f.page()).replay).toBe(true);expect((await f.complete()).replay).toBe(true);
});
test("global release requires every explicit org including empty ones and all current admission rows; omitted org is unavailable",async()=>{
 const f=await fixture([],["org-a","org-empty"]);
 await f.begin();await f.complete();await expect(f.release()).rejects.toMatchObject({code:"55000"});
 await expect(f.send("partial",record({record_id:"partial"}))).rejects.toThrow();
 await f.begin("org-empty");await f.complete("org-empty");
 await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=clock_timestamp()-interval '1 second' WHERE org_id='org-empty'");
 await expect(f.release()).rejects.toMatchObject({code:"55000"});
 await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=clock_timestamp()+interval '1 hour'");
 expect((await f.release()).orgCount).toBe("2");
 await f.send("global",record({record_id:"global"}));
 expect((await f.seats("global")).records[0].bodyAccepted).toBe(true);
 await f.db.query("INSERT INTO sessions.ingest_buffer_admission VALUES('future',repeat('a',64),repeat('b',64),$1,clock_timestamp()+interval '1 hour',$2)",[generation,epoch]);
 await expect(f.begin("future")).rejects.toMatchObject({code:"55000"});
 expect((await f.db.query("SELECT org_inventory FROM sessions.ingest_custody_global")).rows[0]!.org_inventory).toEqual(["org-a","org-empty"]);
});
test("partial source inventory can never produce global release even when every selected org is imported",async()=>{
 const f=await fixture([],["org-a"],"selected-orgs");await f.begin();await f.complete();
 await expect(f.release()).rejects.toMatchObject({code:"55000"});await expect(f.send("partial",record({record_id:"no"}))).rejects.toThrow();
});
test("changed target privacy/reset or compiled generation before completion refuses without admission",async()=>{
 const f=await fixture([await metadata(record({record_id:"privacy"}))]);await f.begin();await f.page();
 await f.db.query("INSERT INTO sessions.ingest_fence_versions VALUES($1,$2,$3,'origin-1','claude','sess-1',1)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 await expect(f.complete()).rejects.toMatchObject({code:"55000"});
 expect((await f.db.query("SELECT status FROM sessions.ingest_custody_imports")).rows[0]!.status).toBe("importing");
 const g=await fixture();await g.begin();
 await g.db.exec("UPDATE sessions.__96bulls_role_validation SET generation=repeat('b',64);UPDATE sessions.ingest_buffer_admission SET schema_generation=repeat('b',64)");
 await expect(g.owner("ingest_custody_complete",{generation:"b".repeat(64),importToken:g.tokens.get(TENANT.orgId)})).rejects.toMatchObject({code:"55000"});
});
test("ingest/seeder have no custody mutation or table permissions",async()=>{
 const f=await fixture();await f.begin();await f.complete();await f.release();
 for(const role of ["bulls96_session_ingest","bulls96_session_ingest_seeder"]){
  await f.db.exec("BEGIN;SET LOCAL ROLE "+role);
  await expect(f.db.query("SELECT sessions.ingest_custody_release($1::jsonb)",[JSON.stringify({generation,descriptorText:f.descriptorText})])).rejects.toMatchObject({code:"42501"});await f.db.exec("ROLLBACK");
  await f.db.exec("BEGIN;SET LOCAL ROLE "+role);
  await expect(f.db.query("SELECT * FROM sessions.ingest_custody_records")).rejects.toMatchObject({code:"42501"});await f.db.exec("ROLLBACK");
 }
});
