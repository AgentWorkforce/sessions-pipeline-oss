// deps.large routing in the event consumer (SESSION_LARGE_READ_HANDOFF_ENABLED): real pointer
// stage, real search index (PGlite + recorded ClickHouse transport), stubbed or real route.
import {afterEach,expect,test} from "bun:test";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {objectKey} from "../../../packages/object-store/src/index.ts";
import {objectStoreManifestReader,prefixAllowed,prefixOrgResolver} from "../../shared/vendor.ts";
import {processBatch,type ConsumerDeps} from "../consumer.ts";
import {droppedFanout} from "../pointer.ts";
import {largeReadJobState,routeLargeRead} from "../large-read-worker.ts";
import {replayDatabase,exportReplayFixture,createFsStore} from "./replay-fixture.ts";
import {recordingReplayClickHouse} from "./replay-clickhouse-fixture.ts";
const cleanup:Array<()=>Promise<unknown>>=[];
afterEach(async()=>{for(const c of cleanup.splice(0).reverse())await c()});
const bindings={PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"};
async function fixture(){
 const x=await replayDatabase();cleanup.push(()=>x.pg.close());
 const root=await mkdtemp(join(tmpdir(),"consumer-large-"));cleanup.push(()=>rm(root,{recursive:true,force:true}));
 const store=createFsStore({root}),source=await exportReplayFixture(store,x.db,3);
 const reader=objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"});
 let searches=0;
 const deps:ConsumerDeps={
  pointer:{db:{...x.db,pool:undefined,end:async()=>{}} as any,lock:async(_s:any,work:any)=>work(),reader,exists:async(k:any)=>(await store.head(objectKey(k)))!==undefined,orgs:prefixOrgResolver,allowPrefix:(p:any,o:any)=>prefixAllowed(bindings,p,o),fanout:droppedFanout},
  search:{transact:<T>(work:any)=>{searches++;return x.pg.transaction((tx:any)=>work({query:(sql:string,params?:unknown[])=>tx.query(sql,params)})) as Promise<T>},reader,store,ch:recordingReplayClickHouse().ch,prefixFor:(org:string)=>`96bulls/${org}`},
 };
 const messages=[{messageId:"m1",body:JSON.stringify({reconcile:{key:source.manifests[0]!.manifestKey}})}];
 const run=async()=>{
  const lines:any[]=[],out=process.stdout.write,err=process.stderr.write;
  const tap=(w:any,stream:NodeJS.WriteStream)=>(chunk:any,...rest:any[])=>{for(const l of String(chunk).split("\n"))if(l.startsWith("{"))try{const j=JSON.parse(l);if(j.stage==="large-read")lines.push(j)}catch{}return w.call(stream,chunk,...rest)};
  process.stdout.write=tap(out,process.stdout) as any;process.stderr.write=tap(err,process.stderr) as any;
  const before=searches;
  try{const r=await processBatch(messages,deps);return {failures:r.batchItemFailures.map(f=>f.itemIdentifier),searched:searches>before,lines}}
  finally{process.stdout.write=out;process.stderr.write=err}
 };
 return {x,deps,run,source};
}
const pgError=(code:string,message:string)=>Object.assign(new Error(message),{code});

test("flag off: no deps.large, no routing call, search runs as before",async()=>{
 const f=await fixture();
 expect(f.deps.large).toBeUndefined();
 const r=await f.run();
 expect(r).toMatchObject({failures:[],searched:true,lines:[]});
 // The real route would delegate this fixture (0/NULL counts): it must not have been consulted.
 expect((await f.x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_large_read_jobs")).rows[0]!.n).toBe(0);
},60000);

test("flag on + delegated: search skipped, job state read once and logged",async()=>{
 const f=await fixture();let routes=0,states=0;
 f.deps.large={route:async s=>{routes++;return {delegated:true,identity:{...s,sessionId:"canonical"}}},jobState:async s=>{states++;expect(s.sessionId).toBe("canonical");return {state:"queued",attempts:0}}};
 const r=await f.run();
 expect(r.failures).toEqual([]);expect(r.searched).toBe(false);expect({routes,states}).toEqual({routes:1,states:1});
 expect(r.lines).toEqual([expect.objectContaining({level:"info",message:"large scope delegated",state:"queued",attempt:0})]);
},60000);

test("flag on + not delegated: search runs and no job state is read",async()=>{
 const f=await fixture();let states=0;
 f.deps.large={route:async()=>({delegated:false}),jobState:async()=>{states++;return null}};
 expect(await f.run()).toMatchObject({failures:[],searched:true,lines:[]});expect(states).toBe(0);
},60000);

test.each([
 ["54000 pointer capacity",pgError("54000","replay capture: pointer capacity exceeded")],
 ["54000 metadata capacity",pgError("54000","replay capture: metadata capacity exceeded")],
])("flag on + capacity refusal (%s): logged, not failed, search runs",async(_name,error)=>{
 const f=await fixture();
 f.deps.large={route:async()=>{throw error},jobState:async()=>{throw new Error("must not be read")}};
 const r=await f.run();
 expect(r.failures).toEqual([]);expect(r.searched).toBe(true);
 expect(r.lines).toEqual([expect.objectContaining({level:"warn",message:"large scope handoff refused",reason:"replay_capacity_refused",error_code:"54000"})]);
},60000);

test("flag on + 55000 ambiguous canonical source: logged refusal, not failed, search runs",async()=>{
 const f=await fixture();
 f.deps.large={route:async()=>{throw pgError("55000","replay capture: ambiguous canonical source")},jobState:async()=>{throw new Error("must not be read")}};
 const r=await f.run();
 expect(r.failures).toEqual([]);expect(r.searched).toBe(true);
 expect(r.lines).toEqual([expect.objectContaining({level:"warn",message:"large scope handoff refused",reason:"large_handoff_ambiguous_source",error_code:"55000",org_id:expect.any(String),workspace_id:expect.any(String),harness:expect.any(String),session_id:expect.any(String)})]);
 for(const l of r.lines)expect(Object.keys(l).every(k=>!/text|content|payload|message_text/.test(k))).toBe(true);
},60000);

test("real route + ambiguous canonical source in PGlite: refusal falls through to search",async()=>{
 const f=await fixture();
 f.deps.large={route:s=>routeLargeRead(f.x.db,s),jobState:s=>largeReadJobState(f.x.db,s)};
 // A second live physical pointer of the same session id (another machine) resolves to a
 // different canonical id: capture_session_replay_pages raises 55000 ambiguous canonical source.
 await f.x.pg.exec("ALTER TABLE sessions.session_index DISABLE TRIGGER USER");
 await f.x.pg.exec("CREATE TEMP TABLE twin AS SELECT * FROM sessions.session_index LIMIT 1");
 await f.x.pg.exec("UPDATE twin SET machine_id='machine:twin',canonical_session_id='other-canonical',canonical_machine_id=NULL,manifest_key=manifest_key||'.twin'");
 await f.x.pg.exec("INSERT INTO sessions.session_index SELECT * FROM twin");
 await f.x.pg.exec("ALTER TABLE sessions.session_index ENABLE TRIGGER USER");
 await expect(f.x.db.query("SELECT sessions.capture_session_replay_pages('org_replay','ws_replay','claude','large_replay')")).rejects.toMatchObject({code:"55000",message:"replay capture: ambiguous canonical source"});
 const r=await f.run();
 expect(r.failures).toEqual([]);expect(r.searched).toBe(true);
 expect(r.lines).toEqual([expect.objectContaining({level:"warn",message:"large scope handoff refused",reason:"large_handoff_ambiguous_source",error_code:"55000"})]);
 expect((await f.x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_large_read_jobs")).rows[0]!.n).toBe(0);
},60000);

test.each([
 ["unexpected error",new Error("boom")],
 ["55000 durable handoff unavailable (enqueue infrastructure, retried)",pgError("55000","large read: durable handoff unavailable")],
 ["55000 source epoch missing (may be transient, retried)",pgError("55000","replay capture: source epoch missing")],
 ["55000 with an unrecognised message",pgError("55000","object not in prerequisite state")],
 ["55000 ambiguous message without a SQLSTATE",new Error("replay capture: ambiguous canonical source")],
 ["22023 invalid scope",pgError("22023","replay capture: invalid scope")],
 ["connection error",pgError("ECONNRESET","read ECONNRESET")],
 ["invalid route result",new Error("large_handoff_invalid")],
])("flag on + %s: batch failure, search skipped",async(_name,error)=>{
 const f=await fixture();
 f.deps.large={route:async()=>{throw error}};
 const r=await f.run();
 expect(r.failures).toEqual(["m1"]);expect(r.searched).toBe(false);
 expect(r.lines).toEqual([expect.objectContaining({level:"error",message:"large scope handoff failed"})]);
},60000);

test("flag on + job state read failure: still delegated, warned, batch succeeds",async()=>{
 const f=await fixture();
 f.deps.large={route:async()=>({delegated:true}),jobState:async()=>{throw pgError("42501","permission denied")}};
 const r=await f.run();
 expect(r).toMatchObject({failures:[],searched:false});
 expect(r.lines).toEqual([expect.objectContaining({level:"warn",message:"large scope job state unavailable",error_code:"42501"})]);
},60000);

test("real route + real job state: a failed job is a logged gap, not silent",async()=>{
 const f=await fixture();
 f.deps.large={route:s=>routeLargeRead(f.x.db,s),jobState:s=>largeReadJobState(f.x.db,s)};
 const first=await f.run();
 expect(first).toMatchObject({failures:[],searched:false});
 expect(first.lines).toEqual([expect.objectContaining({level:"info",message:"large scope delegated",state:"queued",attempt:0})]);
 await f.x.pg.exec("UPDATE sessions.session_large_read_jobs SET state='failed',attempts=5,last_error='large_processing_failed'");
 const again=await f.run();
 // Same source: enqueue leaves the failed job as is, so the scope stays delegated and unindexed.
 expect(again).toMatchObject({failures:[],searched:false});
 expect(again.lines).toEqual([expect.objectContaining({level:"warn",message:"large scope job will not be processed",state:"failed",attempt:5,reason:"large_job_failed"})]);
 for(const l of again.lines)expect(Object.keys(l).every(k=>!/text|content|payload/.test(k))).toBe(true);
},60000);
