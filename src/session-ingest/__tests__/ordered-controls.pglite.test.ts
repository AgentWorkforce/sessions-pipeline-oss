import {initializeFixtureSeatClock} from "./ordered-support.ts";
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {orderedControl,orderedControlResult} from "../control-protocol.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),epoch="3".repeat(64),generation="a".repeat(64),open:PGlite[]=[];
afterEach(async()=>{for(const db of open.splice(0))await db.close()});
async function fixture(admit=true){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(db);
 const read=(f:string)=>readFileSync(join(root,f),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const tenant={orgId:"org",workspaceId:"workspace",userId:"user"};
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
 await db.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:tenant.orgId,workspace_id:tenant.workspaceId,user_id:tenant.userId,deletions:[],resets:[]})]);await db.exec("COMMIT");
 if(admit)await db.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)",[tenant.orgId,generation,epoch]);
 if(admit)await initializeFixtureSeatClock(db as never,generation);
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation}),legacy=sqlLedger(run);
 const input=(enabled:boolean)=>({tenant,originId:"origin",sessions:[["claude","session"]] as [string,string][],enabled});
 async function selection(version:string,enabled:boolean,e=epoch){
  const payload={origin_id:"origin",sessions:[{source:"claude",session_id:"session"}],enabled};
  const digest=(await db.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,'org','workspace','user','selection',$2::jsonb)::text,'UTF8')),'hex') AS h",[e,JSON.stringify(payload)])).rows[0]!.h as string;
  return{...input(enabled),control:{epoch:e,version,digest,kind:"selection" as const}};
 }
 const state=async()=>JSON.stringify((await db.query("SELECT (SELECT jsonb_agg(to_jsonb(d)) FROM sessions.upload_deletions d) AS d,(SELECT jsonb_agg(to_jsonb(v)) FROM sessions.ingest_fence_versions v) AS v,(SELECT jsonb_agg(to_jsonb(r)) FROM sessions.ingest_control_receipts r) AS r")).rows);
 return{db,run,tenant,ledger,legacy,input,selection,state};
}
for(const [old,next]of [[false,true],[true,false]])test("ordered selection preserves newer "+next+" against delayed "+old,async()=>{
 const f=await fixture(),one=await f.selection("1",old),two=await f.selection("2",next);
 expect((await f.ledger.applyOrderedControl!(one)).control.outcome).toBe("applied");
 expect((await f.ledger.applyOrderedControl!(two)).control.outcome).toBe("applied");
 const before=await f.state(),ack=await f.ledger.applyOrderedControl!(one);
 expect(ack.control).toEqual({...one.control,outcome:"stale"});expect(await f.state()).toBe(before);
});
test("owner epoch, contiguous versions and historical same-version digest are mandatory",async()=>{
 const f=await fixture(),before=await f.state();
 await expect(f.ledger.applyOrderedControl!(await f.selection("1",false,"4".repeat(64)))).rejects.toThrow();
 await expect(f.ledger.applyOrderedControl!(await f.selection("2",false))).rejects.toThrow();expect(await f.state()).toBe(before);
 await f.ledger.applyOrderedControl!(await f.selection("1",false));await f.ledger.applyOrderedControl!(await f.selection("2",true));
 const current=await f.state();await expect(f.ledger.applyOrderedControl!(await f.selection("1",true))).rejects.toThrow();expect(await f.state()).toBe(current);
});
test("unversioned buffered controls and direct metadata/helper access fail closed; unenrolled legacy remains",async()=>{
 const f=await fixture();await expect(f.ledger.setSessionUploads(f.input(false))).rejects.toThrow();
 await expect(f.run("SELECT * FROM sessions.ingest_control_receipts",[])).rejects.toMatchObject({code:"42501"});
 await expect(f.run("SELECT sessions.ingest_apply_ordered_control('{}'::jsonb,'selection')",[])).rejects.toMatchObject({code:"42501"});
 const g=await fixture(false);await g.legacy.setSessionUploads(g.input(false));expect((await g.db.query("SELECT count(*)::int AS n FROM sessions.upload_deletions")).rows[0]!.n).toBe(1);
});
test("private protocol keeps BIGINT strings exact and refuses incomplete/mixed acknowledgements",()=>{
 const c={epoch,version:"9007199254740993",digest:"d".repeat(64),kind:"selection" as const};
 expect(orderedControl(c).version).toBe(c.version);expect(()=>orderedControl({...c,kind:["selection"]})).toThrow();
 for(const version of ["0","01","9223372036854775808",9007199254740992])expect(()=>orderedControl({...c,version})).toThrow();
 expect(orderedControlResult({updated:1,control:{...c,outcome:"stale"}},c).control.version).toBe(c.version);
 expect(()=>orderedControlResult({updated:1},c)).toThrow();expect(()=>orderedControlResult({updated:1,control:{...c,outcome:["stale"]}},c)).toThrow();
 expect(()=>orderedControlResult({updated:1,control:{...c,version:"9007199254740992",outcome:"stale"}},c)).toThrow();
});
