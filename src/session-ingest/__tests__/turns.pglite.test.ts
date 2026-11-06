import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {zstdDecompressSync} from "node:zlib";
import {sqlLedger} from "../ledger-sql.ts";
import {prepareDelivery,deliveryAccount} from "../vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION} from "../vendor/delivery/delivery-contracts.ts";
import {readBufferedStage} from "../staging.ts";
import type {CurrentCustodyPacket} from "../custody-protocol.ts";
import {TURN_KIND,TURN_ORIGIN_ID,parseTurnBatch,turnRecordId} from "../turns.ts";
import {normalizeRelayhistoryRecords,type RelayhistoryStreamRecord} from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import {projectRelayhistorySession} from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
setDefaultTimeout(30000);

const root=join(import.meta.dir,"../../.."),generation="a".repeat(64),epoch="3".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

/** Same harness as frozen-custody.pglite.test.ts: every migration, the ingest role, a sealed empty frozen copy. */
async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();open.push(fs.done);const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const rpc=async(fn:string,p:unknown)=>((await run("SELECT sessions."+fn+"($1::jsonb) AS r",[JSON.stringify(p)])) as any).rows[0].r;
 async function witnessFor(raw:any){
  const p=await prepareDelivery(TENANT,raw);
  return rpc("ingest_frozen_source_witness",{kind:"batch",tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:raw.records.length,
   candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))}) as Promise<CurrentCustodyPacket>;
 }
 const stages=async()=>(await db.query("SELECT stage_key AS key,stage_sha256 AS sha256,stage_bytes::int AS bytes,group_ordinals AS \"groupOrdinals\" FROM sessions.ingest_stage_intents ORDER BY created_at")).rows as any[];
 return{db,deps,witnessFor,stages,store:fs.store};
}

const SESSION="7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a5b";
function turn(index:number,over:Record<string,unknown>={}){
 const source=(over.source as string)??"claude";
 const payload={id:`${String(index).padStart(8,"0")}-aaaa-4bbb-8ccc-dddddddddddd`,org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,source,session_id:SESSION,turn_index:index,
  session_owner:"user-1",role:"user",content:`turn ${index}`,actor_name:"Ada",actor_role:"owner",metadata_json:JSON.stringify({nativeCli:"claude",relaySession:{sessionId:SESSION}}),ts:"2026-11-05T12:00:00.123Z",tagged:true,...over};
 const record_id=turnRecordId(payload.workspace_id as string,source,payload.session_id as string,payload.turn_index as number);
 const revision=(over.revision as number|undefined)??1_793_000_000_000_000+index;
 delete (payload as any).revision;
 return {schema_version:1 as const,origin_id:TURN_ORIGIN_ID,record_id,revision_id:`turnrev:${record_id.slice(5)}:${revision}`,revision,kind:TURN_KIND as any,source,session_id:SESSION,operation:"upsert" as const,payload};
}
async function turnBatch(records:any[],batchId="turns-1"){
 return {schema_version:1 as const,origin_id:TURN_ORIGIN_ID,batch_id:batchId,job_id:"relayhistory-turns",generation:1,destination_id:"relayhistory",instance_id:"relayhistory-api",
  account_id:await deliveryAccount(TENANT),mapping_version:DELIVERY_MAPPING_VERSION,records};
}
const body=(b:unknown)=>JSON.stringify({protocolVersion:1,batch:b});

test("record id is the Neon row key; pinned vector shared with relayhistory-cloud",()=>{
 // The same literal is pinned in relayhistory-cloud lib/turns-forward.test.ts.
 expect(turnRecordId("default","claude","s-1",0)).toBe("turn:561b1d693ee08cfc15f125ffef9be7d76f0c117b7e9ff2672386c8ed37e13ebb");
});

test("parse: accepts only conversation_turn upserts of one session, bound to the signed tenant",async()=>{
 const ok=await turnBatch([turn(0),turn(1)]);
 expect(parseTurnBatch({protocolVersion:1,batch:ok},TENANT).records).toHaveLength(2);
 const refuse=async(mutate:(b:any)=>void,ident:{orgId:string;workspaceId:string}=TENANT)=>{const b=structuredClone(await turnBatch([turn(0),turn(1)]));mutate(b);
  try{parseTurnBatch({protocolVersion:1,batch:b},ident);return null}catch(e:any){return `${e.status}:${e.code}`}};
 expect(await refuse((b)=>{b.records[0].kind="session_event"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.origin_id="origin-1";for(const r of b.records)r.origin_id="origin-1"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].operation="delete";b.records[0].payload=null})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[1].session_id="other";b.records[1].payload.session_id="other"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].payload.org_id="org-b"})).toBe("403:delivery_account_mismatch");
 expect(await refuse((b)=>{b.records[0].payload.workspace_id="ws-b"})).toBe("403:delivery_account_mismatch");
 expect(await refuse(()=>{}, {orgId:"org-b",workspaceId:TENANT.workspaceId})).toBe("403:delivery_account_mismatch");
 expect(await refuse((b)=>{b.records[0].record_id=b.records[1].record_id})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[1]=structuredClone(b.records[0]);b.records[1].revision_id="other"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].payload.extra=1})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].payload.ts="2026-11-05T12:00:00Z"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].payload.metadata_json="[]"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].payload.turn_index=-1})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records[0].payload.role="tool"})).toBe("400:invalid_delivery");
 expect(await refuse((b)=>{b.records=[]})).toBe("400:invalid_delivery");
 // A token without a workspace keys turns under `default`, exactly as Neon does.
 const bare={orgId:TENANT.orgId,workspaceId:""};
 const r=turn(0,{workspace_id:"default"});r.record_id=turnRecordId("default","claude",SESSION,0);
 expect(parseTurnBatch({protocolVersion:1,batch:{...(await turnBatch([r])),account_id:await deliveryAccount(bare)}},bare).records).toHaveLength(1);
});

test("HTTP: turns land in the buffered stage through the normal custody + claim path, idempotently",async()=>{
 const {createIngestApp}=await import("../app.ts");
 const {CURRENT_CUSTODY_CONTENT_TYPE,CURRENT_CUSTODY_ACK_HEADER}=await import("../custody-protocol.ts");
 const f=await fixture();const logs:Record<string,unknown>[]=[];
 const mk=(custodySource:"relayhistory"|"frozen-supabase")=>createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:(x)=>logs.push(x),custodySource});
 const b1=await turnBatch([turn(0),turn(1,{role:"assistant",content:"card 4111 1111 1111 1111 here"})]);
 const r1=await mk("frozen-supabase").request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:body(b1)});
 expect(r1.status).toBe(200);
 const receipt=await r1.json() as any;
 expect(receipt).toMatchObject({protocolVersion:1,batchId:"turns-1",acceptanceLevel:"durable",acceptedRevisionIds:b1.records.map((r)=>r.revision_id)});
 expect(JSON.parse(r1.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toMatchObject({version:1});
 expect(logs.find((l)=>l.route==="turns.batches")).toMatchObject({outcome:"accepted_buffered",count:2});
 // The stored records are the scrubbed turn rows, under the session's own (source, session) group.
 const [stage]=await f.stages();
 const decoded=await readBufferedStage(f.store,stage,(x)=>new Uint8Array(zstdDecompressSync(x)));
 expect(decoded.groups.map((g:any)=>[g.source,g.sessionId])).toEqual([["claude",SESSION]]);
 const lines=decoded.groups.flatMap((g:any)=>g.lines.map((l:string)=>({...JSON.parse(l),source:g.source,session_id:g.sessionId})));
 expect(lines.map((l:any)=>[l.kind,l.source,l.session_id,l.payload.turn_index])).toEqual([[TURN_KIND,"claude",SESSION,0],[TURN_KIND,"claude",SESSION,1]]);
 expect(lines[1].payload.content).not.toContain("4111 1111 1111 1111");
 expect(JSON.parse(lines[0].payload.metadata_json)).toEqual({nativeCli:"claude",relaySession:{sessionId:SESSION}});
 // Same batch again: the content-addressed receipt replays; nothing new is staged.
 const again=await mk("frozen-supabase").request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:body(b1)});
 expect(again.status).toBe(200);expect(await again.json()).toEqual(receipt);
 expect(await f.stages()).toHaveLength(1);
 // relayhistory custody mode: the witness comes in relayhistory's private envelope, as for a batch.
 const b2=await turnBatch([turn(0,{content:"replaced",revision:1_793_000_000_000_900})],"turns-2");
 const packet=await f.witnessFor(b2);
 const envelope=JSON.stringify({protocol:"relay-ingest-custody-v1",original:{bodyBase64:Buffer.from(body(b2)).toString("base64"),contentEncoding:"identity"},...packet});
 const r2=await mk("relayhistory").request("/v1/turns/batches",{method:"POST",headers:{"content-type":CURRENT_CUSTODY_CONTENT_TYPE},body:envelope});
 expect(r2.status).toBe(200);
 expect(await f.stages()).toHaveLength(2);
 // Default custody mode with no witness fails closed (503), exactly like a batch.
 const r3=await mk("relayhistory").request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:body(await turnBatch([turn(5)],"turns-3"))});
 expect(r3.status).toBe(503);
});

test("HTTP: the private kind is never accepted on the public delivery route, and turns route refuses delivery records",async()=>{
 const {createIngestApp}=await import("../app.ts");
 const f=await fixture();
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:f.deps,log:()=>{},custodySource:"frozen-supabase"});
 const pub=await app.request("/v1/delivery/batches",{method:"POST",headers:{"content-type":"application/json"},body:body(await turnBatch([turn(0)]))});
 expect(pub.status).toBe(422);expect((await pub.json() as any).error.code).toBe("unsupported_evidence");
 const priv=await app.request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:body(await batch([record({record_id:"r1"})]))});
 expect(priv.status).toBe(400);
 const noScope=createIngestApp({identify:()=>({...TENANT,scopes:["rth:read"]}),deliver:f.deps,log:()=>{},custodySource:"frozen-supabase"});
 expect((await noScope.request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:body(await turnBatch([turn(0)]))})).status).toBe(403);
 expect(await f.stages()).toHaveLength(0);
});

test("projection: turn records produce no transcript events or catalog rows; a later revision wins per key",()=>{
 const line=(r:any,at:string):RelayhistoryStreamRecord=>({kind:r.kind,origin_id:r.origin_id,record_id:r.record_id,revision:String(r.revision),operation:"upsert",received_at:at,payload:r.payload} as any);
 const a=turn(0),b=turn(0,{content:"replaced",revision:a.revision+5});
 const latest=normalizeRelayhistoryRecords([line(b,"2026-11-05T12:00:01.000Z"),line(a,"2026-11-05T12:00:00.000Z")]);
 expect(latest).toHaveLength(1);expect(latest[0]!.payload!.content).toBe("replaced");
 const projected=projectRelayhistorySession(latest,{orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,source:"claude",sessionId:SESSION,machineId:"srv_x",userId:"user-1"} as any) as any;
 expect(projected.rows).toEqual([]);expect(projected.unprojected).toBe(1);
});

test("cross-repo: the batch relayhistory-cloud builds (lib/turns-forward.ts) parses and prepares unchanged",async()=>{
 // Regenerate: WRITE_TURN_FIXTURE=<this path> npx vitest run test/turns-forward.test.ts -t cross-repo (relayhistory-cloud).
 const wire=JSON.parse(readFileSync(join(import.meta.dir,"fixtures/relayhistory-turn-batch.json"),"utf8"));
 const rh={orgId:"org-a",workspaceId:"ws-a",userId:"user-a"};
 const parsed=parseTurnBatch(wire,rh);
 expect(parsed.records.map((r:any)=>[r.payload.turn_index,r.source])).toEqual([[0,"claude"],[1,"claude"]]);
 const prepared=await prepareDelivery(rh,parsed);
 expect(prepared.records.map((r)=>r.payload!.content)).toEqual(["hello","hi"]);
 expect(()=>parseTurnBatch(wire,{...rh,orgId:"org-b"})).toThrow();
});
