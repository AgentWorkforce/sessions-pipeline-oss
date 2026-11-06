/** Synthetic, secret-free replay fixtures; never reads a service or environment credential. */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { runMigrations } from "../../hoopdb/migrate.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey, parseManifest, readAll, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import type { SessionUnit } from "../../migration/neon-s3-sync/neon-reader.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
export const REPLAY_FIXTURE_IDENTITY = { orgId:"org_replay",workspaceId:"ws_replay",source:"claude",sessionId:"large:replay" };
export const REPLAY_FIXTURE_AT = "2026-11-01T10:00:00.000Z";
const executor = (pg: PGlite) => ({ async query<T>(sql:string,params?:unknown[]) { if(params?.length)return pg.query<T>(sql,params);const r=await pg.exec(sql);return {rows:(r.at(-1)?.rows??[]) as T[]}; } });
export async function replayDatabase() {
 const pg = new PGlite({extensions:{btree_gin,pg_trgm}});
 const db = executor(pg);
 await runMigrations(db,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 return {pg,db};
}
/** Tests: one migrated template per test file; each test gets an isolated clone (~1 s instead of a ~5 s migration). */
let template: Promise<PGlite> | undefined;
export const PGLITE_TEST_TIMEOUT = 30_000;
export function warmReplayTemplate(): Promise<unknown> { return template ??= replayDatabase().then(x => x.pg); }
export async function closeReplayTemplate() { const t = template; template = undefined; if (t) await (await t).close(); }
export async function clonedReplayDatabase(): Promise<Awaited<ReturnType<typeof replayDatabase>>> {
 await warmReplayTemplate();
 const pg = await (await template!).clone() as PGlite;
 return {pg,db:executor(pg)};
}
export async function addReplayPointer(db: ReturnType<typeof replayDatabase> extends Promise<infer R> ? R extends {db:infer D}?D:never:never, pointer: { identity?:{orgId:string;workspaceId:string;source:string;sessionId:string}; key:string; digest:string; physicalSession:string; physicalMachine:string; canonicalSession?:string; canonicalMachine?:string; user?:string; revision?:number; trace?:string }) {
 const i=pointer.identity??REPLAY_FIXTURE_IDENTITY;
 await db.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,canonical_session_id,canonical_machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,content_sha256,raw_format,revision,trace_id)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,'s3','replay-fixture',$10,decode($11,'hex'),'ndjson',$12,$13)`,[i.orgId,i.workspaceId,i.source,pointer.physicalSession,pointer.physicalMachine,pointer.canonicalSession??i.sessionId,pointer.canonicalMachine??pointer.physicalMachine,pointer.user??"user_replay",REPLAY_FIXTURE_AT,pointer.key,pointer.digest,pointer.revision??1,pointer.trace??"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"]);
}
export async function exportReplayFixture(store:ObjectStore, db:Parameters<typeof addReplayPointer>[0], count:number, lanes=1, extraRecords:RelayhistoryStreamRecord[]=[]){
 const i=REPLAY_FIXTURE_IDENTITY;
 const unit:SessionUnit={orgId:i.orgId,workspaceId:i.workspaceId,source:i.source,sessionId:i.sessionId,firstMs:Date.parse(REPLAY_FIXTURE_AT),lastMs:Date.parse(REPLAY_FIXTURE_AT)+count,machineIds:[],totals:{}};
 const encoder=new TextEncoder();
 const result=await exportUnit({store,keyRoot:"96bulls",grantId:"replay_fixture",maxChunkBytes:4*1024*1024},unit,{
  fenced:async()=>false,
  async *lines(){
   for(let n=0;n<count;n++){
    const lane=n%lanes,at=new Date(unit.firstMs+(count>=300000&&n<2000?0:Math.floor(n/3))).toISOString();
    const record:RelayhistoryStreamRecord={kind:"session_event",origin_id:`capacity:${lane}`,record_id:`event-${String(n).padStart(12,"0")}`,revision:"1",accepted_seq:String(n+1),operation:"upsert",received_at:at,payload:{ts_ms:Date.parse(at),text:"Synthetic replay capacity content preserves real projected fields and native message identity. "+String(n),role:n%2?"assistant":"user",message_id:`native-${n}`,model:"fixture-model",input_tokens:12,output_tokens:24,tags:["capacity"],files_touched:["src/fixture.ts"],tool_calls:[{name:"Read",input:{path:"src/fixture.ts"}}]}};
    if(n%10===0){
      const content=n===0&&count>=300000?"Synthetic capacity content preserves complete public text. ".repeat(40000):String(record.payload!.text);
      record.kind="convergence_event";record.origin_id=`machine:machine:${lane}`;record.record_id=`direct-${String(n).padStart(12,"0")}`;
      record.payload={source:i.source,session_id:i.sessionId,machine_id:`machine:${lane}`,user_id:"user_replay",event_id:record.record_id,kind:"session_event",type:"assistant",ts:n===0&&count>=300000?at:at.slice(0,-1)+"456Z",ingested_at:at,actor_name:"CapacityAgent",actor_role:"owner",content,model:"fixture-model",provider:"fixture",input_tokens:12,output_tokens:24,reasoning_tokens:5,cache_read_tokens:7,cache_create_tokens:3,task_ref:{},tags:["capacity","native"],files_touched:["src/fixture.ts"],tool_calls:n===0&&count>=300000?Array.from({length:5000},(_,call)=>({id:`call-${call}`,name:"Read",input:{path:"src/fixture.ts",call}})):[{id:`call-${n}`,name:"Read",input:{path:"src/fixture.ts"}}],record:{payload:{message_id:`native-${n}`,text:content}}};
    }
    yield {machine:`machine:${lane}`,userId:"user_replay",bytes:encoder.encode(JSON.stringify(record)+"\n")};
   }
   for(const record of extraRecords)yield {machine:"machine:0",userId:"user_replay",bytes:encoder.encode(JSON.stringify(record)+"\n")};
  },
 });
 for(const m of result.manifests){const manifest=await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(m.manifestKey)))),m.manifestKey);const ext=manifest.extensions!["96bulls"] as {canonical_machine_id:string};await addReplayPointer(db,{key:m.manifestKey,digest:manifest.raw.content_sha256,physicalSession:manifest.session.session_id,physicalMachine:manifest.session.machine_id,canonicalMachine:ext.canonical_machine_id,revision:manifest.revision,trace:manifest.trace_id});}
 const physicalSessions=new Set<string>();
 for(const m of result.manifests){const manifest=await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(m.manifestKey)))),m.manifestKey);physicalSessions.add(manifest.session.session_id);}
 for(const sessionId of physicalSessions)await projectSessionDirectory(db,{orgId:i.orgId,workspaceId:i.workspaceId,harness:i.source,sessionId});
 return result;
}
export {createFsStore};
