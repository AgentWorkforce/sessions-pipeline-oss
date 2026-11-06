/** Explicit Node job entry point. The disabled path reads no credentials, opens no connections and
 * only writes its status line. Bundled by scripts/build-large-read-worker.ts (embedded schema assets). */
import {createS3Store,objectStoreManifestReader} from "../shared/vendor.ts";
import {supabasePool,parseSecret,transaction} from "./lambda.ts";
import {lambdaCredentials} from "./aws-json.ts";
import {createClickHouseClient} from "./vendor/search-index/clickhouse.ts";
import {runLargeReadOnce,type LargeReadDeps} from "./large-read-worker.ts";
import {errorFields,log} from "../shared/log.ts";
import {classifyReplayFailure} from "./replay-publication.ts";
export interface LargeReadRuntime { deps:LargeReadDeps;close:()=>Promise<void> }
export function largeReadRuntime(env:NodeJS.ProcessEnv):LargeReadRuntime{
 const need=(key:string)=>{const value=env[key];if(!value)throw new Error(`large worker missing ${key}`);return value};
 const secret=parseSecret(need("SESSION_PIPELINE_SECRET_JSON"));if(!secret.clickhouse)throw new Error("large worker needs ClickHouse");
 const region=need("AWS_REGION"),bucket=need("PIPELINE_RAW_S3_BUCKET"),prefix=need("PIPELINE_KEY_PREFIX").replace(/^\/+|\/+$/g,"");
 if(!prefix||env.PIPELINE_ORG_FROM_PREFIX!=="true")throw new Error("large worker requires org-prefix layout");
 lambdaCredentials(env); // Validate before opening a pool. Values are never logged or persisted.
 const encryption=env.PIPELINE_RAW_S3_SSE;if(encryption!==undefined&&encryption!=="AES256"&&encryption!=="aws:kms")throw new Error("large worker invalid encryption");
 if(env.PIPELINE_RAW_S3_KMS_KEY_ID&&encryption!=="aws:kms")throw new Error("large worker invalid encryption key mode");
 const db=supabasePool(secret.hoopdb_url,4),store=createS3Store({bucket,region,credentials:async()=>lambdaCredentials(env),serverSideEncryption:encryption,sseKmsKeyId:env.PIPELINE_RAW_S3_KMS_KEY_ID});
 const deps:LargeReadDeps={db,search:{transact:work=>transaction(db,work),reader:objectStoreManifestReader(store,{provider:"s3",bucket}),store,ch:createClickHouseClient(secret.clickhouse),prefixFor:org=>`${prefix}/${org}`},replay:{db,store,provider:"s3",bucket,prefix:{PIPELINE_KEY_PREFIX:prefix,PIPELINE_ORG_FROM_PREFIX:"true"}}};
 return {deps,close:()=>db.end()};
}
export async function largeReadMain(env:NodeJS.ProcessEnv=process.env,factory:(env:NodeJS.ProcessEnv)=>LargeReadRuntime=largeReadRuntime){
 if(env.SESSION_LARGE_READ_WORKER_ENABLED!=="1")return {enabled:false,processed:0,published:0,failed:0,scanned:0,cycleComplete:false};
 const maxJobs=Number(env.SESSION_LARGE_READ_MAX_JOBS??"1");if(!Number.isInteger(maxJobs)||maxJobs<1||maxJobs>10)throw new Error("large worker invalid job bound");
 const runtime=factory(env),stop=new AbortController(),terminate=()=>stop.abort(),timer=setTimeout(terminate,30*60*1000);
 process.once("SIGTERM",terminate);process.once("SIGINT",terminate);
 let processed=0,published=0,failed=0,scanned=0,cycleComplete=false,scanPages=0,discoveryFailures=0;
 type Page={busy:boolean;scanned:number;changed:number;cycleComplete:boolean};
 const discover=async():Promise<Page|null>=>{
   let page:Page|undefined;
   // One scope discovery cannot enqueue (54000 capacity, ambiguous source, ...) rolls back the
   // whole page, cursor included. Log the bounded code and keep claiming already-queued jobs.
   // Follow-up migration 20261105032000 isolates per-scope discovery failures.
   try{page=(await runtime.deps.db.query<{r:Page}>("SELECT sessions.discover_session_large_reads() r")).rows[0]?.r}
   catch(error){log.error("large worker discovery failed",{stage:"large-read",...errorFields(error),reason:classifyReplayFailure(error)?.reason??"discovery_failed"});discoveryFailures++;return null}
   if(!page||typeof page.busy!=="boolean"||!Number.isSafeInteger(page.scanned)||page.scanned<0||page.scanned>128||!Number.isSafeInteger(page.changed)||page.changed<0||page.changed>page.scanned||typeof page.cycleComplete!=="boolean")throw new Error("large worker invalid discovery");
   scanned+=page.scanned;cycleComplete=page.cycleComplete;return page;
 };
 try{
  // Always advance the durable reconciliation cursor, even under a busy queue.
  await discover();scanPages++;
  while(!stop.signal.aborted&&processed<maxJobs){
   const result=await runLargeReadOnce({...runtime.deps,signal:stop.signal});
   if(result!=="idle"){processed++;if(result==="published")published++;else failed++;continue;}
   if(scanPages++>=16)break;
   const page=await discover();if(!page||page.busy)break; // a failing page would fail again: stop, do not spin
   if(page.cycleComplete&&page.changed===0)break;
  }
  return {enabled:true,processed,published,failed,scanned,cycleComplete,...(discoveryFailures?{discoveryFailures}:{})};
 }finally{clearTimeout(timer);process.removeListener("SIGTERM",terminate);process.removeListener("SIGINT",terminate);await runtime.close()}
}
if(import.meta.main)largeReadMain().then(result=>{console.info(JSON.stringify({message:"large canonical worker",...result}));if(result.failed||("discoveryFailures" in result&&result.discoveryFailures))process.exitCode=1}).catch(error=>{console.error(JSON.stringify({message:"large canonical worker failed",code:typeof error?.code==="string"?error.code:"worker_error"}));process.exitCode=1});
