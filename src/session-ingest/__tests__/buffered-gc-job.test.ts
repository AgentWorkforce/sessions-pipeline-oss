import {test,expect} from "bun:test";
import {gcJobConfig,gcPoolConfig,bufferedKeyAllowed,scopedGcStore,runBufferedGcJob} from "../buffered-gc-job.ts";
import {gcEnabled,handler} from "../buffered-gc-lambda.ts";
const env={AWS_REGION:"us-east-1",HOOPDB_DB_MODE:"supabase",INGEST_GC_BUCKET:"synthetic-bucket",
 INGEST_GC_KMS_KEY_ARN:"arn:aws:kms:us-east-1:111122223333:key/11111111-1111-4111-8111-111111111111",
 INGEST_GC_SECRET_ARN:"arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls/prod/buffered-gc-abcdef",
 INGEST_GC_EXPECT_GENERATION:"a".repeat(64)};
const url="postgresql://bulls96_session_ingest.abcdefghijklmnopqrst:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require";
test("default-off entry exits before credentials/database/objects and rejects ambiguous switches",async()=>{
 const old=process.env.INGEST_GC_ENABLED;delete process.env.INGEST_GC_ENABLED;
 try{expect(await handler()).toEqual({enabled:false});expect(gcEnabled({})).toBe(false);expect(()=>gcEnabled({INGEST_GC_ENABLED:"1"})).toThrow()}
 finally{if(old!==undefined)process.env.INGEST_GC_ENABLED=old}
});
test("job requires pinned target, generation and bounded budgets",()=>{
 expect(gcJobConfig(env).budgetMs).toBe(45000);
 for(const patch of [{AWS_REGION:"us-west-2"},{HOOPDB_DB_MODE:"local"},{INGEST_GC_EXPECT_GENERATION:""},{INGEST_GC_BUDGET_MS:"60000"},{INGEST_GC_BATCH_LIMIT:"100"},{INGEST_GC_KMS_KEY_ARN:env.INGEST_GC_KMS_KEY_ARN.replace("111122223333","111111111111")}])expect(()=>gcJobConfig({...env,...patch})).toThrow();
});
test("dedicated ingest login uses pinned TLS; URL options cannot disable it",()=>{
 const c=gcPoolConfig(JSON.stringify({hoopdb_url:url}));expect(c.user).toBe("bulls96_session_ingest.abcdefghijklmnopqrst");expect((c.ssl as any).rejectUnauthorized).toBe(true);expect(c.connectionTimeoutMillis).toBe(1000);
 for(const x of [url.replace("bulls96_session_ingest","relayhistory_owner"),url.replace("abcdefghijklmnopqrst","other"),url.replace(":5432",":6543"),url+"&sslmode=disable",url+"&application_name=leak",url.replace("aws-0-us-east-1.pooler.supabase.com","localhost")])expect(()=>gcPoolConfig(JSON.stringify({hoopdb_url:x}))).toThrow();
 expect(()=>gcPoolConfig(JSON.stringify({hoopdb_url:url,other:"unused"}))).toThrow();
});
test("compiled generation mismatch refuses before secret retrieval",async()=>{
 let secrets=0;
 await expect(runBufferedGcJob(env,{assets:async()=>({expected:{names:["x"],generation:"b".repeat(64)},verifiers:{searchWorker:"x",tokenVerifier:"x",ingestSeeder:"x",ingest:"x"}}),secret:async()=>{secrets++;throw Error("unused")}})).rejects.toMatchObject({code:"compiled_generation_mismatch"});
 expect(secrets).toBe(0);
});
test("buffer-only store refuses canonical/object enumeration and invalid paths before transport",async()=>{
 let calls=0;const raw:any={getObject:async()=>{calls++;return {}},delete:async()=>{calls++},list:async()=>{calls++}};
 const s=scopedGcStore(raw);
 await expect(s.getObject("96bulls/org/claude/session/manifest.json" as any)).rejects.toMatchObject({code:"store_scope"});
 await expect(s.delete("anything" as any)).rejects.toMatchObject({code:"store_operation_refused"});
 await expect(s.list("anything" as any)).rejects.toMatchObject({code:"store_operation_refused"});
 expect(calls).toBe(0);expect(bufferedKeyAllowed("96bulls/../buffered-batches/v1/ws=x/m=srv_"+"a".repeat(32)+"/b="+"b".repeat(32)+"-"+"c".repeat(16)+"-"+"d".repeat(32)+".staged.ndjson.zst")).toBe(false);
});
