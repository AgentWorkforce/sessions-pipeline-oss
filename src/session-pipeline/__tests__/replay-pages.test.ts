import {afterAll,afterEach,beforeAll,expect,test} from "bun:test";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {objectKey,readAll,type ObjectStore} from "../../../packages/object-store/src/index.ts";
import {materializeCanonicalArtifacts,artifactDigest,compareArtifactRows} from "../replay-pages.ts";
import {produceCanonicalReplay} from "../replay-publication.ts";
import type {ArtifactRangeRef,ArtifactEntry,ArtifactIndex} from "../../session-read-api/canonical-artifact.ts";
import {clonedReplayDatabase,warmReplayTemplate,closeReplayTemplate,PGLITE_TEST_TIMEOUT,replayDatabase,exportReplayFixture,createFsStore,REPLAY_FIXTURE_IDENTITY as identity} from "./replay-fixture.ts";
const roots:string[]=[],dbs:Awaited<ReturnType<typeof replayDatabase>>[]=[];
afterEach(async()=>{for(const x of dbs.splice(0))await x.pg.close();for(const r of roots.splice(0))await rm(r,{recursive:true,force:true})});
beforeAll(()=>warmReplayTemplate(),120_000);afterAll(()=>closeReplayTemplate());
async function storage(){const root=await mkdtemp(join(tmpdir(),"replay-artifact-"));roots.push(root);return createFsStore({root})}
const input=(rows:any[])=>({identity,generation:"a".repeat(64),source:{epoch:"1",fingerprint:"b".repeat(64)},prefix:"96bulls/org_replay/normalized/test/",rows,legacyCoverage:null});
async function read(store:ObjectStore,ref:{key:string,sha256:string,bytes:number}){const b=await readAll(await store.get(objectKey(ref.key)));expect(b.length).toBe(ref.bytes);expect(artifactDigest(b)).toBe(ref.sha256);return b}
async function leaves(store:ObjectStore,ref:ArtifactRangeRef):Promise<ArtifactRangeRef[]>{if(ref.kind!=="node")return[ref];const node=JSON.parse(new TextDecoder().decode(await read(store,ref)));expect(node.children.length).toBeLessThanOrEqual(128);const result=[];for(const c of node.children)result.push(...await leaves(store,c));return result}
async function entries(store:ObjectStore,index:ArtifactIndex,table:string,view="raw"){const root=index.tables[table]![view as "raw"]!.root;if(!root)return[];const result:ArtifactEntry[]=[];for(const p of await leaves(store,root)){const page=JSON.parse(new TextDecoder().decode(await read(store,p)));expect(page.rows.length).toBe(p.count);result.push(...page.rows)}return result}
async function row(store:ObjectStore,e:ArtifactEntry){let r=e.row;if(e.rowJson){const parts=[];for(const s of await leaves(store,e.rowJson.root))parts.push(await read(store,s));const all=Buffer.concat(parts);expect(artifactDigest(all)).toBe(e.rowJson.sha256);r=JSON.parse(all.toString("utf8"))}if(e.content){let content="";for(const s of await leaves(store,e.content.root))content+=JSON.parse(new TextDecoder().decode(await read(store,s)));expect(content.length).toBe(e.content.chars);expect(artifactDigest(JSON.stringify(content))).toBe(e.content.jsonSha256);r={...r,content}}return r!}
test("arbitrary nested arrays and replay content segment losslessly including lone surrogates",async()=>{const store=await storage(),content=("text\ud800😀\udfff".repeat(9000)),tools=Array.from({length:12000},(_,n)=>({name:"Read",input:{n,path:"src/test.ts"}})),original={ts:"2026-11-01T10:00:00.123456Z",event_id:"native",machine_id:"m",kind:"session_event",content,tool_calls:tools,record:{payload:{message_id:"native-msg",text:content}}};const made=await materializeCanonicalArtifacts(store,input([{table:"convergence_events",row:original}]));const e=(await entries(store,made.document,"convergence_events","replay"))[0]!;expect(e.rowJson).toBeDefined();expect(e.content).toBeDefined();expect(await row(store,e)).toEqual({...original,record:{payload:{message_id:"native-msg"}}});expect(await row(store,(await entries(store,made.document,"convergence_events"))[0]!)).toEqual(original)});
test("hierarchical pages retain exact microsecond and numeric BIGSERIAL ordering",async()=>{const store=await storage();const rows=Array.from({length:3000},(_,n)=>({table:"session_links",row:{id:String(9007199254740993n+BigInt(2999-n)),link_ts:"2026-11-01T10:00:00.123456Z",metadata:{kept:true}}}));const made=await materializeCanonicalArtifacts(store,input(rows));const all=await entries(store,made.document,"session_links");expect(all.length).toBe(3000);expect((await row(store,all[0]!)).id).toBe("9007199254740993");expect((await row(store,all.at(-1)!)).id).toBe("9007199254743992");expect(made.document.tables.convergence_events!.bounds).toEqual({count:0,root:null});expect(compareArtifactRows({table:"convergence_events",row:{ts:"2026-11-01T10:00:00.123455Z",event_id:"z"}},{table:"convergence_events",row:{ts:"2026-11-01T10:00:00.123456Z",event_id:"a"}})).toBeLessThan(0)});
test("actual producer hydrates full union, read-verifies objects and publishes idempotently",async()=>{const x=await clonedReplayDatabase();dbs.push(x);const store=await storage();await exportReplayFixture(store,x.db,36,2);const deps={db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}};const scope={orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId};const first=await produceCanonicalReplay(scope,deps),second=await produceCanonicalReplay(scope,deps);expect(first.published).toBe(true);expect(first.rows).toBe(36);expect(second.publication).toEqual(first.publication);const index=JSON.parse(new TextDecoder().decode(await read(store,first.publication!.index)));expect((await entries(store,index,"convergence_events","replay")).length).toBe(36);expect((await entries(store,index,"session_directory_event_values")).length).toBe(1);expect(first.publication!.prefix).toContain("/normalized/read-projection-v1/")},PGLITE_TEST_TIMEOUT);
test.each(["write","ack","readback","source_change"])("failed %s publishes no current generation",async mode=>{const x=await clonedReplayDatabase();dbs.push(x);const store=await storage();await exportReplayFixture(store,x.db,3);let changed=false;const guarded=new Proxy(store,{get(target,key){if(key==="put")return async(k:any,b:any,o:any)=>{if(mode==="write")throw Error("fixture write failed");const ack=await target.put(k,b,o);return mode==="ack"?{...ack,size:1}:ack};if(key==="get")return async(k:any)=>{if(String(k).includes("normalized/read-projection-v1/")){if(mode==="readback")return new ReadableStream({start(c){c.enqueue(new Uint8Array([1]));c.close()}});if(mode==="source_change"&&!changed){changed=true;await x.pg.exec("UPDATE sessions.session_index SET user_id='raced'")}}return target.get(k)};const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value}});await expect(produceCanonicalReplay({orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId},{db:x.db,store:guarded,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}})).rejects.toThrow();expect((await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n).toBe(0)},PGLITE_TEST_TIMEOUT);
test("existing event consumer leaves pages dormant by default and publishes when explicitly wired",async()=>{
 const x=await clonedReplayDatabase();dbs.push(x);const store=await storage();const source=await exportReplayFixture(store,x.db,3);
 const {processBatch}=await import("../consumer.ts");const {droppedFanout}=await import("../pointer.ts");
 const {objectStoreManifestReader,prefixOrgResolver,prefixAllowed}=await import("../../shared/vendor.ts");
 const key=source.manifests[0]!.manifestKey,bindings={PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"};
 const deps:any={pointer:{db:{...x.db,pool:undefined,end:async()=>{}},lock:async(_scope:any,work:any)=>work(),reader:objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"}),exists:async(k:any)=>(await store.head(objectKey(k)))!==undefined,orgs:prefixOrgResolver,allowPrefix:(p:any,o:any)=>prefixAllowed(bindings,p,o),fanout:droppedFanout}};
 const messages=[{messageId:"fixture",body:JSON.stringify({reconcile:{key}})}];
 expect(await processBatch(messages,deps)).toEqual({batchItemFailures:[]});
 expect((await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n).toBe(0);
 deps.replay={db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:bindings};
 expect(await processBatch(messages,deps)).toEqual({batchItemFailures:[]});
 expect((await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n).toBe(1);
},PGLITE_TEST_TIMEOUT);

test("legacy turn ordinals follow public turn_index and exact ID order, not timestamp",()=>{const rows=[{table:"conversation_turns",row:{turn_index:1,id:"9007199254740993",ts:"2025-01-01T00:00:00Z"}},{table:"conversation_turns",row:{turn_index:0,id:"9007199254740994",ts:"2030-01-01T00:00:00Z"}},{table:"conversation_turns",row:{turn_index:0,id:"9007199254740993",ts:"2040-01-01T00:00:00Z"}}];rows.sort(compareArtifactRows);expect(rows.map(r=>r.row.id)).toEqual(["9007199254740993","9007199254740994","9007199254740993"]);expect(rows.map(r=>r.row.turn_index)).toEqual([0,0,1])});
test("actual multi-machine union honors per-origin revision, acceptance order and cross-origin tombstones",async()=>{
 const x=await clonedReplayDatabase();dbs.push(x);const store=await storage();
 const base={kind:"session_event",record_id:"event-000000000001",revision:"2",operation:"upsert" as const,received_at:"2020-01-01T00:00:00Z",payload:{ts_ms:Date.parse("2026-11-01T10:00:00Z"),text:"new per-origin revision",role:"assistant",message_id:"revision-native"}};
 await exportReplayFixture(store,x.db,6,2,[{...base,origin_id:"capacity:1",accepted_seq:"900001"},{...base,origin_id:"capacity:other",revision:"1",accepted_seq:"900002",payload:{...base.payload,text:"last accepted independent source",message_id:"accepted-native"}},{...base,origin_id:"capacity:other",record_id:"event-000000000002",revision:"1",operation:"delete",payload:null,accepted_seq:"900003"}]);
 const result=await produceCanonicalReplay({orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId},{db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}});
 const index=JSON.parse(new TextDecoder().decode(await read(store,result.publication!.index))),rows=[];
 for(const e of await entries(store,index,"convergence_events"))rows.push(await row(store,e));
 expect(rows.length).toBe(5);expect(rows.find(r=>r.delivery_record_id==="event-000000000001")?.content).toBe("last accepted independent source");expect(rows.some(r=>r.delivery_record_id==="event-000000000002")).toBe(false);
},PGLITE_TEST_TIMEOUT);

test('publisher refuses a row beyond the shared hydrated consumer bound before any artifact write',async()=>{
 const {ARTIFACT_MAX_ROW_BYTES}=await import('../../session-read-api/canonical-artifact.ts');
 let writes=0;const forbidden={put:async()=>{writes++;throw Error('unexpected write')}} as unknown as ObjectStore;
 await expect(materializeCanonicalArtifacts(forbidden,input([{table:'convergence_events',row:{content:'x'.repeat(ARTIFACT_MAX_ROW_BYTES)}}]))).rejects.toMatchObject({code:'artifact_row_bound'});
 expect(writes).toBe(0);
});

test("actual materializer preserves public persisted-source order after canonical turn attribution",async()=>{const s=await storage(),small="00000000-0000-4000-8000-000000000001",large="ffffffff-ffff-4fff-8fff-ffffffffffff",turnRow=(id:string,stored:string)=>({table:"conversation_turns",row:{org_id:identity.orgId,workspace_id:identity.workspaceId,source:identity.source,session_id:identity.sessionId,id,legacy_stored_source:stored,turn_index:0,session_owner:"owner",content:"kept",role:"assistant",actor_name:"actor",actor_role:"assistant",metadata:{nativeCli:"claude"},ts:"2026-11-01T00:00:00.000001Z"}}),made=await materializeCanonicalArtifacts(s,{identity,source:{epoch:"1",fingerprint:"b".repeat(64)},generation:"d".repeat(64),prefix:"fixture/",rows:[turnRow(small,"z-label"),turnRow(large,"a-label")],legacyCoverage:null});const all=await entries(s,made.document,"conversation_turns");expect((await Promise.all(all.map(e=>row(s,e)))).map(r=>r.id)).toEqual([large,small]);expect((await Promise.all(all.map(e=>row(s,e)))).map(r=>r.legacy_stored_source)).toEqual(["a-label","z-label"])},PGLITE_TEST_TIMEOUT);
async function consumerFixture(){
 const x=await clonedReplayDatabase();dbs.push(x);const store=await storage();const source=await exportReplayFixture(store,x.db,3);
 const {processBatch}=await import("../consumer.ts");const {droppedFanout}=await import("../pointer.ts");
 const {objectStoreManifestReader,prefixOrgResolver,prefixAllowed}=await import("../../shared/vendor.ts");
 const key=source.manifests[0]!.manifestKey,bindings={PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"};
 const deps:any={pointer:{db:{...x.db,pool:undefined,end:async()=>{}},lock:async(_scope:any,work:any)=>work(),reader:objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"}),exists:async(k:any)=>(await store.head(objectKey(k)))!==undefined,orgs:prefixOrgResolver,allowPrefix:(p:any,o:any)=>prefixAllowed(bindings,p,o),fanout:droppedFanout}};
 const wire=(over:{store?:ObjectStore;db?:any}={})=>{deps.replay={db:over.db??x.db,store:over.store??store,provider:"s3",bucket:"replay-fixture",prefix:bindings}};
 const lines:any[]=[];
 const run=async()=>{const out=process.stdout.write,err=process.stderr.write;const tap=(w:any,stream:NodeJS.WriteStream)=>(chunk:any,...rest:any[])=>{for(const l of String(chunk).split("\n"))if(l.startsWith("{"))try{const j=JSON.parse(l);if(j.stage==="replay-pages")lines.push(j)}catch{}return w.call(stream,chunk,...rest)};
  process.stdout.write=tap(out,process.stdout) as any;process.stderr.write=tap(err,process.stderr) as any;
  try{return await processBatch([{messageId:"fixture",body:JSON.stringify({reconcile:{key}})}],deps)}finally{process.stdout.write=out;process.stderr.write=err}};
 const published=async()=>(await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n;
 return {x,store,wire,run,lines,published};
}
const isCapture=(sql:unknown)=>String(sql).includes("capture_session_replay_pages");
test("replay lost source race is logged at info and does not fail the S3 message",async()=>{
 const f=await consumerFixture();let bumped=false;
 f.wire({store:new Proxy(f.store,{get(t:any,k){if(k==="put")return async(key:any,b:any,o:any)=>{if(!bumped&&String(key).includes("/normalized/")){bumped=true;await f.x.pg.exec("UPDATE sessions.session_index SET user_id='raced'")}return t.put(key,b,o)};const v=Reflect.get(t,k);return typeof v==="function"?v.bind(t):v}})});
 expect(await f.run()).toEqual({batchItemFailures:[]});
 expect(bumped).toBe(true);expect(await f.published()).toBe(0);
 expect(f.lines).toEqual([expect.objectContaining({level:"info",reason:"replay_source_superseded",error_code:"artifact_source_changed",ok:false})]);
},PGLITE_TEST_TIMEOUT);
test("replay capacity refusals (SQLSTATE 54000, producer pointer bound) warn with a stable reason and are not retried",async()=>{
 const f=await consumerFixture();
 // The real capture raises 54000 above 10,000 pointers; raise the same SQLSTATE through PGlite without inserting them.
 f.wire({db:{...f.x.db,query:(sql:any,params:any)=>isCapture(sql)?f.x.db.query("DO $$BEGIN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='replay capture: pointer capacity exceeded';END$$"):f.x.db.query(sql,params)}});
 expect(await f.run()).toEqual({batchItemFailures:[]});
 // The producer's own guard: a capture reporting more than ARTIFACT_MAX_SOURCE_POINTERS pointers.
 f.wire({db:{...f.x.db,query:async(sql:any,params:any)=>{const r=await f.x.db.query<any>(sql,params);if(!isCapture(sql))return r;const c=r.rows[0].captured,p=c.snapshot.pointers[0];return {...r,rows:[{captured:{...c,snapshot:{...c.snapshot,pointers:Array.from({length:10001},()=>p)}}}]}}}});
 expect(await f.run()).toEqual({batchItemFailures:[]});
 expect(await f.published()).toBe(0);
 expect(f.lines).toEqual([
  expect.objectContaining({level:"warn",reason:"replay_capacity_refused",error_code:"54000"}),
  expect.objectContaining({level:"warn",reason:"replay_capacity_refused",error_code:"artifact_capture_bound"}),
 ]);
},PGLITE_TEST_TIMEOUT);
test("replay S3 AccessDenied stays a batch failure",async()=>{
 const f=await consumerFixture();
 f.wire({store:new Proxy(f.store,{get(t:any,k){if(k==="put")return async()=>{const e:any=new Error("AccessDenied");e.name="AccessDenied";e.code="AccessDenied";throw e};const v=Reflect.get(t,k);return typeof v==="function"?v.bind(t):v}})});
 expect(await f.run()).toEqual({batchItemFailures:[{itemIdentifier:"fixture"}]});
 expect(f.lines).toEqual([expect.objectContaining({level:"error",error_code:"AccessDenied"})]);
},PGLITE_TEST_TIMEOUT);
test("replay failure classification uses typed codes only",async()=>{
 const {classifyReplayFailure}=await import("../replay-publication.ts");const {ArtifactBuildError}=await import("../replay-pages.ts");
 const {CanonicalReadError}=await import("../../session-read-api/canonical.ts");const {CanonicalTimingError}=await import("../../session-read-api/canonical-timing.ts");
 const pg=(code:string)=>Object.assign(new Error("x"),{code});
 expect(classifyReplayFailure(new ArtifactBuildError("artifact_publish_cas_failed"))?.kind).toBe("superseded");
 expect(classifyReplayFailure(new CanonicalReadError("source_generation_changed"))?.kind).toBe("superseded");
 expect(classifyReplayFailure(new CanonicalReadError("canonical_projection_incomplete",503,["scope_too_large"]))).toMatchObject({kind:"capacity",code:"scope_too_large"});
 expect(classifyReplayFailure(new CanonicalReadError("pointer_bound"))?.kind).toBe("capacity");
 expect(classifyReplayFailure(new ArtifactBuildError("artifact_row_bound"))?.kind).toBe("capacity");
 expect(classifyReplayFailure(new CanonicalTimingError("canonical timing timestamp invalid"))).toMatchObject({kind:"capacity",code:"canonical_timing_invalid"});
 expect(classifyReplayFailure(pg("54000"))?.kind).toBe("capacity");
 for(const e of [new CanonicalReadError("canonical_projection_incomplete",503,["manifest_missing"]),new CanonicalReadError("object_missing"),new ArtifactBuildError("artifact_write_verification_failed"),pg("40001"),pg("55000"),new Error("artifact_source_changed"),new Error("scope_too_large"),Object.assign(new Error("AccessDenied"),{name:"AccessDenied"})])
  expect(classifyReplayFailure(e)).toBeNull();
});
