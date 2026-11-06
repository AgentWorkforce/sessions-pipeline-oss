/** Permanent buffered GC job. Default-off Lambda entry imports this only when armed.
 * Credentials are execution-role sessions; transcript bodies/keys and secret text
 * never reach logs. SQL operations reuse the independently reviewed owned canceler.
 */
import pg from "pg";
import {createS3Store,type ObjectStore} from "../../packages/object-store/src/index.ts";
import {createS3Eraser,type PreservedVersionEraser} from "../migration/neon-s3-erasure/eraser.ts";
import {SemanticPostgresExecutor} from "../search-features/semantic-copy-postgres.ts";
import {derivedSchemaAssets,type DerivedSchemaAssets} from "../session-pipeline/derived-schema.ts";
import {awsJsonCall,lambdaCredentials} from "../session-pipeline/aws-json.ts";
import {SUPABASE_PROD_CA_2021} from "../session-pipeline/supabase-ca.ts";
import {bufferedGcTick,sqlBufferedGcLedger,BufferedGcError} from "./buffered-gc.ts";
export interface GcJobConfig {region:"us-east-1";bucket:string;kmsKeyArn:string;secretArn:string;expectedGeneration:string;limit:number;budgetMs:number}
function no(code:string):never{throw new BufferedGcError(code)}
export function gcJobConfig(env:NodeJS.ProcessEnv):GcJobConfig{
 const need=(name:string)=>{const v=env[name];if(!v)no("config_required");return v};
 if(env.AWS_REGION!=="us-east-1"||env.HOOPDB_DB_MODE!=="supabase")no("target_required");
 const bucket=need("INGEST_GC_BUCKET"),kmsKeyArn=need("INGEST_GC_KMS_KEY_ARN"),secretArn=need("INGEST_GC_SECRET_ARN");
 if(!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)||!/^arn:aws:kms:us-east-1:111122223333:key\/[0-9a-f-]{36}$/.test(kmsKeyArn)||
  !/^arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls\/[A-Za-z0-9_./-]+\/buffered-gc-[A-Za-z0-9]{6}$/.test(secretArn))no("target_invalid");
 const expectedGeneration=need("INGEST_GC_EXPECT_GENERATION");
 if(!/^[a-f0-9]{64}$/.test(expectedGeneration))no("generation_required");
 const limit=Number(env.INGEST_GC_BATCH_LIMIT??"5"),budgetMs=Number(env.INGEST_GC_BUDGET_MS??"45000");
 if(!Number.isInteger(limit)||limit<1||limit>20||!Number.isInteger(budgetMs)||budgetMs<5000||budgetMs>50000)no("budget_invalid");
 return{region:"us-east-1",bucket,kmsKeyArn,secretArn,expectedGeneration,limit,budgetMs};
}
/** Dedicated login only. URL SSL options cannot weaken pinned CA/hostname checks. */
export function gcPoolConfig(text:string):pg.PoolConfig{
 let v:unknown;try{v=JSON.parse(text)}catch{no("secret_invalid")}
 if(!v||typeof v!=="object"||Array.isArray(v)||Object.keys(v).join(",")!=="hoopdb_url"||typeof(v as any).hoopdb_url!=="string")no("secret_invalid");
 let u:URL;try{u=new URL((v as any).hoopdb_url)}catch{no("database_target")}
 if(!["postgres:","postgresql:"].includes(u.protocol)||u.hostname!=="aws-0-us-east-1.pooler.supabase.com"||
  u.port!=="5432"||u.pathname!=="/postgres"||decodeURIComponent(u.username)!=="bulls96_session_ingest.abcdefghijklmnopqrst"||!u.password||u.hash)no("database_target");
 for(const[k,x]of u.searchParams)if(!((k==="sslmode"&&["require","verify-full"].includes(x))||(k==="channel_binding"&&x==="require")))no("database_options");
 return{host:u.hostname,port:5432,database:"postgres",user:decodeURIComponent(u.username),password:decodeURIComponent(u.password),
  ssl:{ca:SUPABASE_PROD_CA_2021,rejectUnauthorized:true,servername:u.hostname},max:2,connectionTimeoutMillis:1000,
  idleTimeoutMillis:1000,application_name:"96bulls-buffered-gc"};
}
export function bufferedKeyAllowed(key:string):boolean{
 return /^96bulls\/[A-Za-z0-9._-]{1,128}\/buffered-batches\/v1\/ws=[^/]+\/m=srv_[0-9a-f]{32}\/b=[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{32}\.staged\.ndjson\.zst$/.test(key)&&!key.split("/").some(x=>x==="."||x==="..");
}
export function scopedGcStore(store:ObjectStore):ObjectStore{
 return new Proxy(store,{get(target,key){
  if(key==="delete"||key==="list"||key==="presignGet")return async()=>no("store_operation_refused");
  const value=(target as any)[key];
  if(["get","getObject","head","put"].includes(String(key)))return async(first:string,...rest:any[])=>{
   if(!bufferedKeyAllowed(String(first)))no("store_scope");return value.call(target,first,...rest);
  };
  return typeof value==="function"?value.bind(target):value;
 }});
}
export interface GcJobOverrides{
 assets?:()=>Promise<DerivedSchemaAssets>;
 secret?:(config:GcJobConfig,fetcher:typeof fetch)=>Promise<string>;
 pool?:(config:pg.PoolConfig)=>pg.Pool;
 store?:(config:Parameters<typeof createS3Store>[0])=>ObjectStore;
 eraser?:(config:Parameters<typeof createS3Eraser>[0])=>PreservedVersionEraser;
 fetcher?:typeof fetch;
}
/** Test factories can point at owned PG/MinIO; production entry supplies none. */
export async function runBufferedGcJob(env:NodeJS.ProcessEnv,overrides:GcJobOverrides={}){
 const config=gcJobConfig(env),assets=await(overrides.assets??derivedSchemaAssets)();
 if(assets.expected.generation!==config.expectedGeneration||!assets.expected.names.length||
  Object.values(assets.verifiers).length!==4||Object.values(assets.verifiers).some(s=>typeof s!=="string"||!s.trim()))no("compiled_generation_mismatch");
 const deadline=Date.now()+config.budgetMs,controller=new AbortController(),timer=setTimeout(()=>controller.abort(),config.budgetMs);
 const actualFetch=overrides.fetcher??fetch;
 const boundedFetch=(async(input:Parameters<typeof fetch>[0],init?:RequestInit)=>{
  controller.signal.throwIfAborted();
  return actualFetch(input,{...init,signal:AbortSignal.any([controller.signal,...(init?.signal?[init.signal]:[])])});
 }) as typeof fetch;
 let pool:pg.Pool|undefined;
 try{
  const credentials=()=>{const c=lambdaCredentials();if(!/^ASIA[A-Z0-9]{16}$/.test(c.accessKeyId)||!c.sessionToken)no("managed_credentials_required");return c};
  const secret=overrides.secret?await overrides.secret(config,boundedFetch):await(async()=>{
   const r=await awsJsonCall<{SecretString?:string}>({service:"secretsmanager",target:"secretsmanager.GetSecretValue",region:config.region,
    body:{SecretId:config.secretArn},credentials:credentials(),fetcher:boundedFetch,timeoutMs:Math.min(5000,config.budgetMs)});
   if(!r.SecretString)no("secret_empty");return r.SecretString;
  })();
  controller.signal.throwIfAborted();
  pool=(overrides.pool??(c=>new pg.Pool(c)))(gcPoolConfig(secret));pool.on("error",()=>{});
  const sql=new SemanticPostgresExecutor(pool),read=new SemanticPostgresExecutor(pool,true);
  const context=()=>({signal:controller.signal,deadline:Math.min(deadline,Date.now()+1800)});
  const who=(await read.queryBounded<{me:string;session:string}>("SELECT current_user AS me,session_user AS session",[],context())).rows[0];
  if(who?.me!=="bulls96_session_ingest"||who.session!=="bulls96_session_ingest")no("database_role");
  await read.queryBounded("SET LOCAL hoopdb.db_mode='supabase';"+Object.values(assets.verifiers).join("\n"),[],context());
  const rawStore=(overrides.store??createS3Store)({bucket:config.bucket,region:config.region,credentials:async()=>credentials(),
   serverSideEncryption:"aws:kms",sseKmsKeyId:config.kmsKeyArn,fetch:boundedFetch});
  const eraser=(overrides.eraser??createS3Eraser)({bucket:config.bucket,region:config.region,credentials:credentials(),fetch:boundedFetch});
  const result=await bufferedGcTick({keyRoot:"96bulls",store:scopedGcStore(rawStore),
   eraser:{eraseObjectExcept:async(key,version)=>{if(!bufferedKeyAllowed(key))no("eraser_scope");controller.signal.throwIfAborted();return eraser.eraseObjectExcept(key,version)}},
   ledger:sqlBufferedGcLedger({query:(s,p)=>sql.queryBounded(s,p,context())},config.expectedGeneration)},config.limit);
  controller.signal.throwIfAborted();return result;
 }finally{
  controller.abort();clearTimeout(timer);
  if(pool)await pool.end();
 }
}
