/** Offline, per-cohort S3 proof bootstrap. No exporter-state/session predicate is accepted. */
import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { SeedProofIndex, SeedProofError, type ProofDeps, type SeedPointer } from "./seed-proof.ts";
import type { SeedSql } from "./seed.ts";
interface Scope {workspace_id:string;source:string;session_id:string;users:string[];raw_bytes:string;chunks:string;pointers:number}
interface Capture {protocol:1;capture_id:string|null;expires_at:string|null;fingerprint:string;source_epoch:string;pointers:SeedPointer[]}
const fail=(code:string):never=>{throw new SeedProofError(code)};
export async function seedOrgFromS3(
  sql:SeedSql,deps:ProofDeps,orgId:string,generation:string,
  opts:{sourceHead:string;cacheDir:string;execute?:boolean;maxScopes?:number;concurrency?:number;log?:(counts:Record<string,unknown>)=>void},
) {
  if(!/^[a-zA-Z0-9._-]{1,128}$/.test(orgId)||orgId.startsWith(".")||
    !/^[a-f0-9]{64}$/.test(generation)||!/^[a-f0-9]{40}$/.test(opts.sourceHead))fail("seed_identity");
  await mkdir(opts.cacheDir,{recursive:true,mode:0o700});
  const rpc=async<T>(name:"ingest_seed_scope_page"|"ingest_seed_scope_capture"|"ingest_seed_proven_records",p:unknown):Promise<T>=>{
    const out=await sql.query<{r:T}>(`SELECT sessions.${name}($1::jsonb) AS r`,[JSON.stringify(p)]);
    if(!out.rows[0]?.r)fail("seed_rpc_shape");return out.rows[0]!.r;
  };
  const result={scopes:0,records:0,upserted:0,fenced:0,non_delivery_pointers:0,non_delivery_records:0,enumerated_complete:false,executed:opts.execute===true};
  const processScope=async(scope:Scope)=>{
      const input={org_id:orgId,workspace_id:scope.workspace_id,source:scope.source,session_id:scope.session_id,generation};
      const capture=await rpc<Capture>("ingest_seed_scope_capture",{...input,mode:opts.execute?"lease":"preview"});
      if(capture.protocol!==1 || !/^[a-f0-9]{64}$/.test(capture.fingerprint) || !Array.isArray(capture.pointers))fail("seed_capture_shape");
      if(opts.execute && (!capture.capture_id||!capture.expires_at||Date.parse(capture.expires_at)<=Date.now()))fail("seed_capture_expired");
      if(capture.pointers.some(p=>p.org_id!==orgId||p.workspace_id!==scope.workspace_id||p.harness!==scope.source||p.session_id!==scope.session_id))
        fail("seed_capture_namespace");
      const key=createHash("sha256").update(JSON.stringify([orgId,scope,capture.fingerprint,opts.sourceHead])).digest("hex");
      const index=new SeedProofIndex(join(opts.cacheDir,key+".sqlite"),orgId,opts.sourceHead);
      try {
        index.restartVerification();
        for(const pointer of capture.pointers) await index.addPointer(pointer,deps);
        const current=await rpc<Capture>("ingest_seed_scope_capture",{...input,mode:"preview"});
        if(current.fingerprint!==capture.fingerprint)fail("seed_source_changed");
        const sealed=index.seal(current.pointers);
        result.records+=sealed.records;
        result.non_delivery_pointers+=sealed.non_delivery_pointers;
        result.non_delivery_records+=sealed.unseeded_records;
        if(opts.execute)for(const batch of index.batches()) {
          const ack=await rpc<{accepted:boolean;upserted:number;fenced:number}>("ingest_seed_proven_records",{
            capture_id:capture.capture_id,fingerprint:capture.fingerprint,source_head:opts.sourceHead,records:batch.records,
          });
          if(ack.accepted!==true || !Number.isSafeInteger(ack.upserted)||ack.upserted<0||
             !Number.isSafeInteger(ack.fenced)||ack.fenced<0||ack.upserted+ack.fenced>batch.records.length)fail("seed_ack");
          index.acknowledged(batch.batch_sha256,batch.records.length);
          result.upserted+=ack.upserted;result.fenced+=ack.fenced;
        }
      }finally{index.close();}
      result.scopes++;
      opts.log?.({outcome:"cohort",scopes:result.scopes,records:result.records,upserted:result.upserted,fenced:result.fenced});
    }

  let after:string[]|null=null;
  for(;;) {
    const page:{rows:Scope[];next:string[]|null}=await rpc<{rows:Scope[];next:string[]|null}>("ingest_seed_scope_page",{org_id:orgId,generation,after,limit:200});
    if(!Array.isArray(page.rows)||page.rows.length>200)fail("seed_page_shape");
    if(page.next && (!page.rows.length||JSON.stringify(page.next)===JSON.stringify(after)))fail("seed_page_stalled");
    const concurrency=opts.concurrency??1;
    if(!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>4)fail("seed_concurrency");
    for(let i=0;i<page.rows.length;i+=concurrency){
      const remaining=opts.maxScopes===undefined?concurrency:Math.min(concurrency,opts.maxScopes-result.scopes);
      if(remaining<=0)return result;
      const outcomes=await Promise.allSettled(page.rows.slice(i,i+remaining).map(processScope));
      const failed=outcomes.find((r):r is PromiseRejectedResult=>r.status==="rejected");
      if(failed)throw failed.reason;
    }
    if(!page.next){result.enumerated_complete=true;return result;}
    after=page.next;
  }
}


/** Discovery is metadata-only and mints no capture ticket. */
export async function seedUsers(sql:SeedSql,orgId:string,generation:string) {
  const users=new Map<string,{workspace_id:string;user_id:string}>();
  let after:string[]|null=null,scopes=0,pointers=0,raw=0n,chunks=0n;
  for(;;){
    const page:{rows:Scope[];next:string[]|null}=(await sql.query<{r:{rows:Scope[];next:string[]|null}}>(
      "SELECT sessions.ingest_seed_scope_page($1::jsonb) AS r",[JSON.stringify({org_id:orgId,generation,after,limit:200})])).rows[0]!.r;
    if(!Array.isArray(page.rows)||page.rows.length>200)fail("seed_page_shape");
    for(const scope of page.rows){
      if(!Array.isArray(scope.users))fail("seed_page_shape");
      for(const user of scope.users)users.set(JSON.stringify([scope.workspace_id,user]),{workspace_id:scope.workspace_id,user_id:user});
      if(users.size>100000)fail("seed_user_bound");
      scopes++;pointers+=scope.pointers;raw+=BigInt(scope.raw_bytes);chunks+=BigInt(scope.chunks);
    }
    if(!page.next)return {users:[...users.values()],scopes,pointers,raw_bytes:raw.toString(),chunks:chunks.toString()};
    if(!page.rows.length||JSON.stringify(page.next)===JSON.stringify(after))fail("seed_page_stalled");
    after=page.next;
  }
}
