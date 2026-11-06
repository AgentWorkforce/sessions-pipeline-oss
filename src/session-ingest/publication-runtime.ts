/** Default-off host Node runtime. Managed credentials are pinned IMDSv2 only;
 * source SQL/CH/legacy compute are never run on the ingress request path.
 */
import pg from'pg';
import{createS3Store,parseSessionKey,parseUploadGenerationPrefix,type ObjectStore}from'../../packages/object-store/src/index.ts';
import{imdsCredentials}from'../shared/imds-credentials.ts';
import{derivedSchemaAssets,type DerivedSchemaAssets}from'../session-pipeline/derived-schema.ts';
import{awsJsonCall}from'../session-pipeline/aws-json.ts';
import{createS3Eraser,type PreservedVersionEraser}from'../migration/neon-s3-erasure/eraser.ts';
import{SemanticPostgresExecutor}from'../search-features/semantic-copy-postgres.ts';
import{gcPoolConfig,bufferedKeyAllowed}from'./buffered-gc-job.ts';
import{publicationRpc}from'./publication-rpc.ts';
import{canonicalPublicationTick}from'./publication-job.ts';
import{publicationGcLedger,collectPublicationObject}from'./publication-gc.ts';
import type{PublicationTarget}from'./publication-protocol.ts';
import type{AwsCredentials}from'../../packages/object-store/src/sigv4.ts';
const no=(code:string):never=>{throw Object.assign(Error('publication_'+code),{code:'publication_'+code});};
export interface PublicationRuntimeConfig{bucket:string;region:'us-east-1';keyRoot:'96bulls';generation:string;secretArn:string;kmsKeyArn:string;instanceProfileArn:string;roleName:string;budgetMs:number;candidateLimit:number;gcLimit:number}
export function publicationRuntimeConfig(env:NodeJS.ProcessEnv):PublicationRuntimeConfig{
 const need=(k:string)=>{if(!env[k])no('config_required');return env[k]!};
 if(env.AWS_REGION!=='us-east-1'||env.HOOPDB_DB_MODE!=='supabase')no('target_required');
 const bucket=need('INGEST_PUBLISH_BUCKET'),generation=need('INGEST_PUBLISH_EXPECT_GENERATION'),secretArn=need('INGEST_PUBLISH_SECRET_ARN'),kmsKeyArn=need('INGEST_PUBLISH_KMS_KEY_ARN'),instanceProfileArn=need('INGEST_PUBLISH_INSTANCE_PROFILE_ARN'),roleName=need('INGEST_PUBLISH_ROLE_NAME');
 if(!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)||!/^[a-f0-9]{64}$/.test(generation)||
  !/^arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls\/[A-Za-z0-9_./-]+\/(?:canonical-publisher|buffered-gc)-[A-Za-z0-9]{6}$/.test(secretArn)||
  !/^arn:aws:kms:us-east-1:111122223333:key\/[a-f0-9-]{36}$/.test(kmsKeyArn)||
  !/^arn:aws:iam::111122223333:instance-profile\/[A-Za-z0-9_+=,.@/-]{1,512}$/.test(instanceProfileArn)||!/^[A-Za-z0-9_+=,.@-]{1,64}$/.test(roleName))no('target_invalid');
 const budgetMs=Number(env.INGEST_PUBLISH_BUDGET_MS??'1200000'),candidateLimit=Number(env.INGEST_PUBLISH_BATCH_LIMIT??'1'),gcLimit=Number(env.INGEST_PUBLICATION_GC_LIMIT??'20');
 if(!Number.isSafeInteger(budgetMs)||budgetMs<5000||budgetMs>3600000||!Number.isInteger(candidateLimit)||candidateLimit<1||candidateLimit>20||!Number.isInteger(gcLimit)||gcLimit<1||gcLimit>100)no('budget_invalid');
 return{bucket,region:'us-east-1',keyRoot:'96bulls',generation,secretArn,kmsKeyArn,instanceProfileArn,roleName,budgetMs,candidateLimit,gcLimit};
}
export function publicationEnabled(env:NodeJS.ProcessEnv,mode:'publish'|'gc'):boolean{
 const v=env[mode==='publish'?'INGEST_PUBLISH_ENABLED':'INGEST_PUBLICATION_GC_ENABLED']??'false';if(v!=='true'&&v!=='false')no('enabled_invalid');return v==='true';
}
function generationKey(key:string):boolean{const loc=parseSessionKey(key),g=loc&&parseUploadGenerationPrefix(loc.prefix??'');return!!g&&/^96bulls\/[A-Za-z0-9._-]{1,128}$/.test(g.base);}
function readKey(key:string):boolean{if(bufferedKeyAllowed(key))return true;const loc=parseSessionKey(key);return!!loc&&!!loc.prefix&&(/^96bulls\/[A-Za-z0-9._-]{1,128}$/.test(loc.prefix)||generationKey(key));}
export function scopedPublicationStore(raw:ObjectStore):ObjectStore{
 return new Proxy(raw,{get(t,k){
  if(['delete','list','presignGet'].includes(String(k)))return async()=>no('store_operation_refused');
  const v=Reflect.get(t,k);if(['get','getObject','head','put'].includes(String(k)))return async(key:string,...args:any[])=>{
   if(!(k==='put'?generationKey(String(key)):readKey(String(key))))no('store_scope');return v.call(t,key,...args);
  };return typeof v==='function'?v.bind(t):v;
 }});
}
export interface PublicationRuntimeOverrides{
 assets?:()=>Promise<DerivedSchemaAssets>;credentials?:(signal:AbortSignal)=>Promise<AwsCredentials>;
 secret?:(config:PublicationRuntimeConfig,fetcher:typeof fetch)=>Promise<string>;
 pool?:(config:pg.PoolConfig)=>pg.Pool;store?:(config:Parameters<typeof createS3Store>[0])=>ObjectStore;
 eraser?:(config:Parameters<typeof createS3Eraser>[0])=>PreservedVersionEraser;fetcher?:typeof fetch;
}
/** Factories are test-only ports; process entry never reads overrides from input. */
export async function runCanonicalPublicationRuntime(env:NodeJS.ProcessEnv,mode:'publish'|'gc',overrides:PublicationRuntimeOverrides={},external?:AbortSignal){
 if(!publicationEnabled(env,mode))return{enabled:false as const};
 const config=publicationRuntimeConfig(env),assets=await(overrides.assets??derivedSchemaAssets)();
 if(assets.expected.generation!==config.generation||!assets.expected.names.length||Object.values(assets.verifiers).length!==4||Object.values(assets.verifiers).some(s=>!s?.trim()))no('compiled_generation_mismatch');
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),config.budgetMs),signal=AbortSignal.any([controller.signal,...(external?[external]:[])]),deadline=Date.now()+config.budgetMs;
 let pool:pg.Pool|undefined;
 try{
  const fetcher=(async(input:Parameters<typeof fetch>[0],init?:RequestInit)=>{signal.throwIfAborted();return(overrides.fetcher??fetch)(input,{...init,signal:AbortSignal.any([signal,AbortSignal.timeout(30000),...(init?.signal?[init.signal]:[])])});})as typeof fetch;
  const managed=imdsCredentials({instanceId:'i-0123456789abcdef0',accountId:'111122223333',region:'us-east-1',instanceProfileArn:config.instanceProfileArn,roleName:config.roleName},overrides.fetcher?{fetcher:overrides.fetcher}:{});
  const credentials=()=>overrides.credentials?overrides.credentials(signal):managed(signal);
  const secret=overrides.secret?await overrides.secret(config,fetcher):await(async()=>{
   const r=await awsJsonCall<{SecretString?:string}>({service:'secretsmanager',target:'secretsmanager.GetSecretValue',region:config.region,body:{SecretId:config.secretArn},credentials:await credentials(),fetcher,timeoutMs:5000});if(!r.SecretString)no('secret_empty');return r.SecretString!;
  })();signal.throwIfAborted();
  pool=(overrides.pool??(c=>new pg.Pool(c)))({...gcPoolConfig(secret),max:4,application_name:'96bulls-canonical-publication'});pool.on('error',()=>{});
  const sql=new SemanticPostgresExecutor(pool),read=new SemanticPostgresExecutor(pool,true),context=()=>({signal,deadline:Math.min(deadline,Date.now()+1800)});
  const who=(await read.queryBounded<{me:string;session:string}>('SELECT current_user AS me,session_user AS session',[],context())).rows[0];
  if(who?.me!=='bulls96_session_ingest'||who.session!=='bulls96_session_ingest')no('database_role');
  await read.queryBounded("SET LOCAL hoopdb.db_mode='supabase';"+Object.values(assets.verifiers).join('\n'),[],context());
  const target:PublicationTarget=config;
  const raw=scopedPublicationStore((overrides.store??createS3Store)({bucket:config.bucket,region:config.region,credentials,serverSideEncryption:'aws:kms',sseKmsKeyId:config.kmsKeyArn,fetch:fetcher}));
  if(mode==='publish')return await canonicalPublicationTick({target,rpc:publicationRpc(sql,target),store:()=>raw},{enabled:true,signal,maxJobMs:config.budgetMs,candidateLimit:config.candidateLimit});
  const eraser=(overrides.eraser??createS3Eraser)({bucket:config.bucket,region:config.region,credentials:await credentials(),fetch:fetcher});
  const ledger=publicationGcLedger(sql,target),tickets=await ledger.claim(config.gcLimit,signal);let finished=0;
  for(const ticket of tickets){signal.throwIfAborted();if(!generationKey(ticket.key))no('eraser_scope');
   if(await collectPublicationObject(ticket,{ledger,target,store:raw,eraser:{eraseObjectExcept:async(key,version)=>{if(!generationKey(key))no('eraser_scope');signal.throwIfAborted();return eraser.eraseObjectExcept(key,version);}}},signal))finished++;
  }
  signal.throwIfAborted();return{enabled:true as const,examined:tickets.length,finished};
 }finally{controller.abort();clearTimeout(timer);if(pool)await pool.end();}
}
