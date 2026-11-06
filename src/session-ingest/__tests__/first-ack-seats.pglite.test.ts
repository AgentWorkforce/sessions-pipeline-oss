import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {orderedFixtureLedger,fixtureSeatBaseline,initializeFixtureSeatClock} from "./ordered-support.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),gen="a".repeat(64),epoch="3".repeat(64),open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});
async function fixture(initialized=true){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());
 const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+gen+"','pass')");
 await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
 await db.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,deletions:[],resets:[]})]);await db.exec("COMMIT");
 await db.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)",[TENANT.orgId,gen,epoch]);
 if(initialized)await initializeFixtureSeatClock(db as never,gen);
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=orderedFixtureLedger(sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:gen}),db as never);
 const fs=scratchStore();open.push(fs.done);
 const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 const stage=async(id:string)=>(await db.query("SELECT i.stage_token FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token WHERE o.batch_id=$1",[id])).rows[0]?.stage_token as string;
 const seats=async(id:string)=>(await db.query("SELECT sessions.ingest_receipt_seats_for_stage($1::uuid) AS r",[await stage(id)])).rows[0]!.r as any;
 const last=async()=>(await db.query("SELECT last_seat::text AS s FROM sessions.ingest_receipt_seat_clock")).rows[0]!.s as string;
 return{db,run,ledger,deps,stage,seats,last};
}
test("real producer ACK atomically maps original ordinals above allocated floor; replay never reseats",async()=>{
 const f=await fixture(),b=await batch([record({record_id:"seat-a"}),record({record_id:"seat-b"})],{batch_id:"first"});
 expect((await deliverBatch(f.deps,TENANT,b)).outcome).toBe("accepted_buffered");
 const s=await f.seats("first");expect(s.controlEpoch).toBe(epoch);expect(s.records.map((r:any)=>r.seat)).toEqual(["9007199254740994","9007199254740995"]);
 expect(s.records.map((r:any)=>r.ord)).toEqual([0,1]);expect(s.records.every((r:any)=>r.firstDigest===r.candidateDigest)).toBe(true);
 expect((await deliverBatch(f.deps,TENANT,b)).outcome).toBe("replay");expect(await f.last()).toBe("9007199254740995");
});
test("later same-origin equal revision keeps first ACK seat/body; higher revision gets another seat",async()=>{
 const f=await fixture();
 for(const[id,text,revision]of [["one","first",7],["two","later different",7],["three","new revision",8]]as const)
  await deliverBatch(f.deps,TENANT,await batch([record({record_id:"shared",revision,payload:{text,ts_ms:1761955200000}})],{batch_id:id}));
 const one=(await f.seats("one")).records[0],two=(await f.seats("two")).records[0],three=(await f.seats("three")).records[0];
 expect(two.seat).toBe(one.seat);expect(two.firstDigest).toBe(one.firstDigest);expect(two.candidateDigest).not.toBe(two.firstDigest);
 expect(BigInt(three.seat)>BigInt(one.seat)).toBe(true);expect(await f.last()).toBe(three.seat);
});
test("uninitialized baseline blocks actual staged and suppressed ACKs; runtime cannot initialize/read clock",async()=>{
 const f=await fixture(false);
 await expect(deliverBatch(f.deps,TENANT,await batch([record({record_id:"cold"})]))).rejects.toThrow();
 await expect(f.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false})).rejects.toThrow();
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,\'origin-1\',\'claude\',\'sess-1\')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 await expect(deliverBatch(f.deps,TENANT,await batch([record({record_id:"suppressed"})],{batch_id:"suppressed"}))).rejects.toThrow();
 await expect(f.run("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify(fixtureSeatBaseline(gen))])).rejects.toMatchObject({code:"42501"});
 await expect(f.run("SELECT * FROM sessions.ingest_receipt_seat_clock",[])).rejects.toMatchObject({code:"42501"});
});
test("initializer covers exactly both allocated sequences and never re-adopts a changed source/floor",async()=>{
 const f=await fixture(false),p=fixtureSeatBaseline(gen);
 await expect(f.db.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...p,sourceSequences:[p.sourceSequences[0]]})])).rejects.toThrow();
 await initializeFixtureSeatClock(f.db as never,gen);
 await expect(f.db.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...p,sourceIdentitySha256:"e".repeat(64)})])).rejects.toMatchObject({code:"55000"});
 await expect(f.db.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...p,sourceSequences:[...p.sourceSequences].reverse()})])).rejects.toThrow();
 expect(await f.last()).toBe(p.allocatedHighWater);
});

test("ACK-time alias decision survives later winner erasure; only a NEW ACK may establish a new lifecycle",async()=>{
 const f=await fixture();
 const a=record({record_id:"alias",session_id:"session-A",source:"claude",payload:{text:"first A"}});
 const b=record({record_id:"alias",session_id:"session-B",source:"codex",payload:{text:"later B"}});
 await deliverBatch(f.deps,TENANT,await batch([a],{batch_id:"alias-A"}));
 await deliverBatch(f.deps,TENANT,await batch([b],{batch_id:"alias-B"}));
 const first=(await f.seats("alias-A")).records[0],loser=(await f.seats("alias-B")).records[0];
 expect(first.bodyAccepted).toBe(true);expect(loser.bodyAccepted).toBe(false);
 expect(loser.seat).toBe(first.seat);expect(loser.firstDigest).toBe(first.firstDigest);
 expect(loser.candidateDigest).not.toBe(loser.firstDigest);
 await f.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","session-A"]],enabled:false});
 expect((await f.seats("alias-B")).records[0]).toEqual(loser);
 await deliverBatch(f.deps,TENANT,await batch([b],{batch_id:"alias-B-new-ACK"}));
 const newer=(await f.seats("alias-B-new-ACK")).records[0];
 expect(newer.bodyAccepted).toBe(true);expect(newer.firstDigest).toBe(newer.candidateDigest);
 expect(BigInt(newer.seat)>BigInt(first.seat)).toBe(true);
 expect((await f.seats("alias-B")).records[0]).toEqual(loser);
 expect((await deliverBatch(f.deps,TENANT,await batch([b],{batch_id:"alias-B"}))).outcome).toBe("replay");
 expect((await f.seats("alias-B")).records[0].bodyAccepted).toBe(false);
});

test("current record custody supersedes old revision aliases; erased higher revision permits fresh lower revision",async()=>{
 const f=await fixture(),send=async(id:string,revision:number,session:string)=>deliverBatch(f.deps,TENANT,await batch([
  record({record_id:"move",revision,session_id:session,payload:{text:id}})],{batch_id:id}));
 await send("move-A",1,"A");await send("move-B",2,"B");
 const a=(await f.seats("move-A")).records[0],b=(await f.seats("move-B")).records[0];
 await send("old-live",1,"C");const stale=(await f.seats("old-live")).records[0];
 expect(stale.bodyAccepted).toBe(false);expect(stale.seat).toBe(b.seat);
 await f.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","B"]],enabled:false});
 await send("fresh-lower",1,"A");const fresh=(await f.seats("fresh-lower")).records[0];
 expect(fresh.bodyAccepted).toBe(true);expect(BigInt(fresh.seat)>BigInt(b.seat)).toBe(true);
 expect(fresh.seat).not.toBe(a.seat);expect((await f.seats("old-live")).records[0]).toEqual(stale);
});
