import {afterEach,expect,test} from "bun:test";
import {legacyAnalysisOracle} from "./analysis-oracle-fixture.ts";
import {reconcileAnalysisPage,type AnalysisReconcileCursor} from "../analysis-reconcile.ts";
import {enqueueAnalysisScope} from "../analysis-refresh.ts";
import {replayDatabase,addReplayPointer,REPLAY_FIXTURE_IDENTITY} from "./replay-fixture.ts";
import {projectSessionDirectory} from "../../hoopdb/session-directory-projector.ts";
const open:Awaited<ReturnType<typeof legacyAnalysisOracle>>[]=[];afterEach(async()=>{for(const f of open.splice(0))await f.pg.close()});
async function fixture(){const f=await legacyAnalysisOracle();open.push(f);return f}
async function key(f:Awaited<ReturnType<typeof fixture>>,id:string,n:number){await f.pg.query("INSERT INTO sessions.convergence_events VALUES('org',$1,'ws','claude')",[id]);await f.pg.query("INSERT INTO sessions.session_analysis_requests VALUES($1,'org','ws',$2,'queued',NULL,NULL,NULL,now(),now())",["00000000-0000-4000-8000-"+String(100+n).padStart(12,"0"),id])}
test("keyset discovery traverses every distinct request/job scope beyond page limits in native byte order",async()=>{const f=await fixture(),ids=["session",...Array.from({length:55},(_,i)=>`s-${String(i).padStart(2,"0")}`),"s-😀","s-\ue000"];for(let i=1;i<ids.length;i++)await key(f,ids[i]!,i);const observed:string[]=[];let cursor:AnalysisReconcileCursor|undefined,pages=0;do{const page=await reconcileAnalysisPage({source:f.source,enqueue:async identity=>{observed.push(identity.sessionId);return {accepted:true}}},{pageSize:7,cursor});expect(page.scanned).toBeLessThanOrEqual(7);cursor=page.nextCursor;pages++}while(cursor);expect(pages).toBeGreaterThan(8);expect(observed).toEqual(ids.sort((a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b))));expect(new Set(observed).size).toBe(ids.length)});
test("enqueue failure never acknowledges a continuation cursor; retry revisits the same source page",async()=>{const f=await fixture();await key(f,"before-session",1);let calls=0;await expect(reconcileAnalysisPage({source:f.source,enqueue:async()=>{calls++;throw Error("synthetic write failure")}},{pageSize:1})).rejects.toThrow("synthetic write failure");const keys:string[]=[];const page=await reconcileAnalysisPage({source:f.source,enqueue:async i=>{keys.push(i.sessionId);return {accepted:true}}},{pageSize:1});expect(calls).toBe(1);expect(keys).toEqual(["before-session"]);expect(page.nextCursor).toEqual(["org","ws","before-session"])});
test("scoped discovery fans out all proven sources and ignores another workspace",async()=>{const f=await fixture();await f.pg.exec("INSERT INTO sessions.convergence_events VALUES('org','session','foreign','gemini'),('org','session','ws','codex')");const pairs:string[]=[];expect(await reconcileAnalysisPage({source:f.source,enqueue:async i=>{pairs.push(i.workspaceId+":"+i.source);return {accepted:true}}})).toMatchObject({scanned:1,accepted:2,invalidScope:0,complete:true});expect(pairs).toEqual(["ws:claude","ws:codex"])});
test("namespace overflow refuses the whole key without a truncated fanout, logs a stable reason and advances",async()=>{const f=await fixture();for(let i=0;i<8;i++)await f.pg.query("INSERT INTO sessions.convergence_events VALUES('org','session','ws',$1)",[`source-${i}`]);await key(f,"later-session",1);const calls:string[]=[],refusals:string[]=[];const page=await reconcileAnalysisPage({source:f.source,enqueue:async i=>{calls.push(i.sessionId);return {accepted:true}},onRefused:(reason,k)=>refusals.push(reason+":"+k.sessionId)});expect(calls).toEqual(["later-session"]);expect(refusals).toEqual(["namespace_fanout_bound:session"]);expect(page).toMatchObject({scanned:2,accepted:1,refused:1,complete:true,nextCursor:undefined})});
test("deadline checkpoints only the acknowledged prefix instead of stranding a complete source page",async()=>{const f=await fixture();for(let i=0;i<4;i++)await key(f,`before-${i}`,i+1);let clock=0;const keys:string[]=[];const page=await reconcileAnalysisPage({source:f.source,enqueue:async i=>{keys.push(i.sessionId);clock+=60;return {accepted:true}}},{maxMs:100,now:()=>clock});expect(keys).toEqual(["before-0","before-1"]);expect(page.nextCursor).toEqual(["org","ws","before-1"]);expect(page.complete).toBe(false);const next:string[]=[];await reconcileAnalysisPage({source:f.source,enqueue:async i=>{next.push(i.sessionId);return {accepted:true}}},{cursor:page.nextCursor});expect(next).toEqual(["before-2","before-3","session"])});
test("a per-key 22023 enqueue refusal (physical legacy id, overlong id) is logged and skipped; later keys are still enqueued",async()=>{const f=await fixture(),target=await replayDatabase();try{const id=REPLAY_FIXTURE_IDENTITY;
 // Reviewer probe P4: the legacy row is keyed by the physical session id, which maps to a different canonical id.
 await addReplayPointer(target.db,{key:"96bulls/org_replay/v1/ws=ws_replay/m=m/h=claude/d=2026-11-01/s=physical/manifest.json",digest:"a".repeat(64),physicalSession:"physical",physicalMachine:"m",canonicalMachine:"m"});
 await projectSessionDirectory(target.db,{orgId:id.orgId,workspaceId:id.workspaceId,harness:id.source,sessionId:"physical"});
 const overlong="x".repeat(300);for(const sid of ["physical",overlong,"zzz-later"]){await f.pg.query("INSERT INTO sessions.convergence_events VALUES($1,$2,$3,'claude')",[id.orgId,sid,id.workspaceId]);await f.pg.query("INSERT INTO sessions.session_analysis_requests VALUES(gen_random_uuid(),$1,$2,$3,'queued',NULL,NULL,NULL,now(),now())",[id.orgId,id.workspaceId,sid])}
 const seen:string[]=[],acks:unknown[]=[],refusals:string[]=[];
 const run=()=>reconcileAnalysisPage({source:f.source,enqueue:async i=>{seen.push(i.sessionId);const ack=await enqueueAnalysisScope(target,i);acks.push(ack);return ack},onRefused:(reason,k)=>refusals.push(reason+":"+k.sessionId.slice(0,8))});
 const page=await run();
 expect(refusals).toEqual(["enqueue_invalid_scope:physical","enqueue_invalid_scope:xxxxxxxx"]);
 expect(seen.filter(s=>s!=="session")).toEqual(["physical",overlong,"zzz-later"]);
 expect(acks.at(-1)).toMatchObject({version:1,accepted:false});
 expect(page).toMatchObject({refused:2,complete:true,nextCursor:undefined});
 // A transient failure on the later key still throws and acknowledges nothing.
 await expect(reconcileAnalysisPage({source:f.source,enqueue:async i=>{if(i.sessionId==="zzz-later")throw Object.assign(Error("connection reset"),{code:"08006"});return enqueueAnalysisScope(target,i)},onRefused:()=>{}})).rejects.toThrow("connection reset");
}finally{await target.pg.close()}});
const pgError=(code:string,message:string)=>Object.assign(new Error(message),{code,severity:"ERROR"});
test.each([
 ["55000","replay capture: ambiguous canonical source","enqueue_ambiguous_source"],
 ["54000","replay capture: pointer capacity exceeded","enqueue_capacity_exceeded"],
 ["54000","replay capture: metadata capacity exceeded","enqueue_capacity_exceeded"],
])("deterministic capture refusal %s %s is logged as %s, advances the cursor and later keys are still enqueued",async(code,message,reason)=>{const f=await fixture();await key(f,"a-refused",1);await key(f,"b-later",2);const seen:string[]=[],refusals:string[]=[];
 const page=await reconcileAnalysisPage({source:f.source,enqueue:async i=>{seen.push(i.sessionId);if(i.sessionId==="a-refused")throw pgError(code,message);return {accepted:true}},onRefused:(r,k)=>refusals.push(`${r}:${k.orgId}:${k.workspaceId}:${k.source}:${k.sessionId}`)},{pageSize:2});
 expect(refusals).toEqual([`${reason}:org:ws:claude:a-refused`]);expect(seen).toEqual(["a-refused","b-later"]);
 expect(page).toMatchObject({scanned:2,accepted:1,refused:1,nextCursor:["org","ws","b-later"],complete:false});
});
test.each([
 ["55000","replay capture: source epoch missing"],
 ["55000","replay pages: ambiguous canonical identity"],
 ["54000","history summary: pointer capacity exceeded"],
 ["08006","replay capture: ambiguous canonical source"],
 ["08006","connection reset"],
])("non-deterministic or unqualified enqueue error %s %s aborts the scan without advancing the cursor",async(code,message)=>{const f=await fixture();await key(f,"a-transient",1);await key(f,"b-later",2);const seen:string[]=[],refusals:string[]=[];
 await expect(reconcileAnalysisPage({source:f.source,enqueue:async i=>{seen.push(i.sessionId);if(i.sessionId==="a-transient")throw pgError(code,message);return {accepted:true}},onRefused:r=>refusals.push(r)},{pageSize:2})).rejects.toMatchObject({code,message});
 expect(seen).toEqual(["a-transient"]);expect(refusals).toEqual([]);
 // Retry revisits the same key: nothing was acknowledged past the failure.
 const retry:string[]=[];const page=await reconcileAnalysisPage({source:f.source,enqueue:async i=>{retry.push(i.sessionId);return {accepted:true}}},{pageSize:1});expect(retry).toEqual(["a-transient"]);expect(page.nextCursor).toEqual(["org","ws","a-transient"]);
});
test("a real ambiguous canonical source from capture_session_replay_pages is refused as enqueue_ambiguous_source and the scan advances",async()=>{const f=await fixture(),target=await replayDatabase();try{const id=REPLAY_FIXTURE_IDENTITY;
 // One physical session id recorded on two machines under two different canonical ids.
 await addReplayPointer(target.db,{key:"96bulls/org_replay/v1/ws=ws_replay/m=m1/h=claude/d=2026-11-01/s=dup/manifest.json",digest:"a".repeat(64),physicalSession:"dup",physicalMachine:"m1",canonicalSession:"canon-one"});
 await addReplayPointer(target.db,{key:"96bulls/org_replay/v1/ws=ws_replay/m=m2/h=claude/d=2026-11-01/s=dup/manifest.json",digest:"b".repeat(64),physicalSession:"dup",physicalMachine:"m2",canonicalSession:"canon-two"});
 for(const sid of ["dup","zzz-later"]){await f.pg.query("INSERT INTO sessions.convergence_events VALUES($1,$2,$3,'claude')",[id.orgId,sid,id.workspaceId]);await f.pg.query("INSERT INTO sessions.session_analysis_requests VALUES(gen_random_uuid(),$1,$2,$3,'queued',NULL,NULL,NULL,now(),now())",[id.orgId,id.workspaceId,sid])}
 await expect(enqueueAnalysisScope(target,{...id,sessionId:"dup"})).rejects.toMatchObject({code:"55000",message:"replay capture: ambiguous canonical source"});
 const refusals:string[]=[],seen:string[]=[];
 const page=await reconcileAnalysisPage({source:f.source,enqueue:async i=>{seen.push(i.sessionId);return enqueueAnalysisScope(target,i)},onRefused:(r,k)=>refusals.push(r+":"+k.sessionId)});
 expect(refusals).toEqual(["enqueue_ambiguous_source:dup"]);expect(seen.filter(s=>s!=="session")).toEqual(["dup","zzz-later"]);expect(page).toMatchObject({complete:true,nextCursor:undefined});
}finally{await target.pg.close()}});
