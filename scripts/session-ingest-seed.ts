/**
 * Offline S3-proven replay bootstrap. Default: read-only preview, no ledger writes.
 * Secrets only through inherited /dev/fd or /proc/self/fd descriptors; no values in argv/logs.
 * Requires reviewed raw S3 GetObject/KMS decrypt, seeder SET ROLE, and (execute only)
 * readonly Neon SELECT on deletion/reset fences. No transcript rows are read from Neon.
 */
import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { zstdDecompressSync } from "node:zlib";
import pg from "pg";
import { generation } from "../src/hoopdb/migrate.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { objectStoreManifestReader } from "../packages/pipeline/src/lib/pipeline/adapter.ts";
import { seedFences, type SeedSql } from "../src/session-ingest/seed.ts";
import { seedOrgFromS3, seedUsers } from "../src/session-ingest/seed-s3.ts";

function fail(code:string):never{throw Object.assign(new Error(code),{code})}
const fd=(name:string)=>{
 const path=process.env[name];
 if(!path||!/^\/(?:dev\/fd|proc\/self\/fd)\/[0-9]+$/.test(path))fail("seed_secret_fd_required");
 return readFileSync(path,"utf8").trim();
};
const parseUrl=(text:string)=>{
 try{return new URL(text)}catch{return fail("seed_connection")}
};
function supaClient(){
 const u=parseUrl(fd("SUPABASE_SEED_URL_FILE"));
 if(!["postgresql:","postgres:"].includes(u.protocol)||u.hostname!=="aws-0-us-east-1.pooler.supabase.com"||
   u.port!=="5432"||u.pathname!=="/postgres"||decodeURIComponent(u.username)!=="relayhistory_owner.abcdefghijklmnopqrst"||!u.password||u.hash)
  fail("seed_connection");
 for(const[k,v]of u.searchParams)if(!((k==="sslmode"&&["require","verify-full"].includes(v))||(k==="channel_binding"&&v==="require")))fail("seed_connection_options");
 return new pg.Pool({max:4,host:u.hostname,port:5432,database:"postgres",user:decodeURIComponent(u.username),password:decodeURIComponent(u.password),
   ssl:{ca:SUPABASE_PROD_CA_2021,rejectUnauthorized:true,servername:u.hostname},connectionTimeoutMillis:10000,
   application_name:"96bulls-s3-proof-seed",options:"-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000"});
}
function neonClient(){
 const u=parseUrl(fd("NEON_URL_FILE"));
 if(!["postgresql:","postgres:"].includes(u.protocol)||!u.hostname.endsWith(".neon.tech")||!u.password||u.hash||
  !process.env.SEED_EXPECT_NEON_HOST||u.hostname!==process.env.SEED_EXPECT_NEON_HOST||
  !process.env.SEED_EXPECT_NEON_DATABASE||decodeURIComponent(u.pathname.slice(1))!==process.env.SEED_EXPECT_NEON_DATABASE)fail("seed_neon_connection");
 for(const[k,v]of u.searchParams)if(!((k==="sslmode"&&["require","verify-full"].includes(v))||(k==="channel_binding"&&v==="require")))fail("seed_connection_options");
 return new pg.Client({host:u.hostname,port:Number(u.port||5432),database:decodeURIComponent(u.pathname.slice(1)),user:decodeURIComponent(u.username),password:decodeURIComponent(u.password),
  ssl:{rejectUnauthorized:true,servername:u.hostname},connectionTimeoutMillis:10000,
  application_name:"96bulls-s3-proof-fences",options:"-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000"});
}
function readStore():{store:ObjectStore;bucket:string}{
 const bucket=process.env.SEED_BUCKET;
 if(!bucket||bucket!==process.env.SEED_EXPECT_BUCKET)fail("seed_bucket_pin");
 const creds=JSON.parse(fd("SEED_S3_CREDS_FILE"));
 if(typeof creds.accessKeyId!=="string"||!creds.accessKeyId||typeof creds.secretAccessKey!=="string"||!creds.secretAccessKey)fail("seed_s3_credentials");
 const endpoint=process.env.SEED_S3_ENDPOINT;
 if(endpoint&&!endpoint.startsWith("https://"))fail("seed_s3_tls");
 const original=createS3Store({bucket,region:process.env.SEED_S3_REGION||"us-east-1",credentials:creds,...(endpoint?{endpoint}:{})});
 const denied=async():Promise<never>=>fail("seed_store_read_only");
 const store:ObjectStore={backend:original.backend,
  get:original.get.bind(original),getObject:original.getObject.bind(original),head:original.head.bind(original),list:original.list.bind(original),
  put:denied,delete:denied,presignGet:denied};
 return {store,bucket};
}
export async function main(){
 process.umask(0o077);
 const org=process.argv[2],args=process.argv.slice(3),execute=args.includes("--execute");
 if(!org||!/^[A-Za-z0-9._-]{1,128}$/.test(org)||org.startsWith(".")||args.some(a=>a!=="--execute"&&!/^--max-scopes=[1-9][0-9]*$/.test(a)))fail("seed_usage");
 if(process.env.EXPORT_STATE_FILE)fail("seed_export_state_forbidden");
 const root=fileURLToPath(new URL("..",import.meta.url));
 const git=(args:string[])=>execFileSync("git",["-C",root,...args],{encoding:"utf8",env:{PATH:process.env.PATH}}).trim();
 if(git(["status","--porcelain"]))fail("seed_source_dirty");
 const head=git(["rev-parse","HEAD"]),expected=await generation();
 if(process.env.SEED_EXPECT_GENERATION!==expected)fail("seed_generation_pin");
 const cacheDir=process.env.SEED_PROOF_DIR;
 if(!cacheDir)fail("seed_proof_dir_required");
 // Validate EVERY local option and credential/store shape before a baseline can
 // be sealed. Network/source changes remain fail-closed and require source freeze.
 const max=args.find(a=>a.startsWith("--max-scopes="))?.split("=")[1];
 const maxScopes=max?Number(max):undefined;
 if(maxScopes!==undefined&&(!Number.isSafeInteger(maxScopes)||maxScopes<1||maxScopes>1000000))fail("seed_scope_bound");
 const concurrency=Number(process.env.SEED_CONCURRENCY||"4");
 if(!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>4)fail("seed_concurrency");
 const {store,bucket}=readStore();
 const supa=supaClient(),neon=execute?neonClient():null;
 supa.on("error",()=>{});neon?.on("error",()=>{});
 const attested=new WeakSet<pg.PoolClient>();
 const sql:SeedSql={async query(text,params){
   const client=await supa.connect();
   try{
    if(!attested.has(client)){
     const me=(await client.query("SELECT current_user AS me,session_user AS session,rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_catalog.pg_roles WHERE rolname=current_user")).rows[0];
     if(!me||me.me!=="relayhistory_owner"||me.session!=="relayhistory_owner"||me.rolsuper||me.rolcreaterole||me.rolcreatedb||me.rolreplication||me.rolbypassrls)fail("seed_destination_role");
     attested.add(client);
    }
    await client.query("BEGIN");
    try{await client.query("SET LOCAL ROLE bulls96_session_ingest_seeder");const result=await client.query(text,params);await client.query("COMMIT");return result as never}
    catch(error){await client.query("ROLLBACK").catch(()=>{});throw error}
   }finally{client.release()}
 }};

 try{
  const discovered=await seedUsers(sql,org,expected);
  console.log(JSON.stringify({outcome:"discovered",scopes:discovered.scopes,pointers:discovered.pointers,raw_bytes:discovered.raw_bytes,chunks:discovered.chunks,users:discovered.users.length}));
  let fences:{deletions:number;resets:number}|null=null;
  if(neon){
   await neon.connect();
   const source={async readOnly<T>(work:(sql:SeedSql)=>Promise<T>){
    await neon.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    try{
     const me=(await neon.query("SELECT current_user AS role,rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=current_user")).rows[0];
     if(!me||me.role!=="relayhistory_readonly"||me.rolsuper||me.rolcreaterole||me.rolcreatedb||me.rolreplication||me.rolbypassrls)fail("seed_neon_role");
     const result=await work({query:(text,params)=>neon.query(text,params) as never});
     await neon.query("COMMIT");return result;
    }catch(error){await neon.query("ROLLBACK").catch(()=>{});throw error}
   }};
   // Import tombstones/resets BEFORE capturing S3/fence generations.
   fences=await seedFences(source,sql,org,discovered.users);
  }
  const result=await seedOrgFromS3(sql,{
    store,bucket,provider:"s3",reader:objectStoreManifestReader(store,{provider:"s3",bucket,maxBytes:64*1024*1024}),
    prefixFor:org=>"96bulls/"+org,
    decompress:(stored,maxBytes)=>new Uint8Array(zstdDecompressSync(stored,{maxOutputLength:maxBytes})),
  },org,expected,{sourceHead:head,cacheDir,execute,concurrency,...(maxScopes===undefined?{}:{maxScopes}),
    log:counts=>console.log(JSON.stringify(counts))});
  console.log(JSON.stringify({outcome:execute?"proof_seeded":"proof_preview",...result,fences}));
 }finally{await Promise.allSettled([supa.end(),...(neon?[neon.end()]:[])])}
}
if(import.meta.main)main().catch(error=>{
 const code=typeof error?.code==="string"&&/^[A-Za-z0-9_]{1,64}$/.test(error.code)?error.code:"seed_failed";
 console.error(JSON.stringify({outcome:"refused",code}));process.exitCode=1;
});
