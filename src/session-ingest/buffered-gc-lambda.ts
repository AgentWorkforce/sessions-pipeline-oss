/** EventBridge/portable Node entry. A single reviewed switch arms job+schedule. */
export function gcEnabled(env:NodeJS.ProcessEnv):boolean{
 const v=env.INGEST_GC_ENABLED??"false";if(v!=="true"&&v!=="false")throw Error("gc_enabled_invalid");return v==="true";
}
export async function handler(_event:unknown={},_context?:unknown){
 if(!gcEnabled(process.env))return{enabled:false};
 const name=process.env.AWS_LAMBDA_FUNCTION_NAME??"buffered-gc";
 const metric=(healthy:number,counts:Record<string,number>)=>console.log(JSON.stringify({
  _aws:{Timestamp:Date.now(),CloudWatchMetrics:[{Namespace:"96Bulls/BufferedGc",Dimensions:[["FunctionName"]],Metrics:[{Name:"Healthy",Unit:"Count"}]}]},
  FunctionName:/^[A-Za-z0-9_-]{1,64}$/.test(name)?name:"buffered-gc",Healthy:healthy,...counts}));
 try{
  const {runBufferedGcJob}=await import("./buffered-gc-job.ts");
  const result=await runBufferedGcJob(process.env);metric(1,result);return{enabled:true,...result};
 }catch(e){
  const code=typeof(e as any)?.code==="string"&&/^[A-Za-z0-9_]{1,64}$/.test((e as any).code)?(e as any).code:"gc_failed";
  metric(0,{failed:1});throw Error("buffered_gc_"+code);
 }
}

// Portable job export used by offline container/bundle proofs; the Lambda handler
// never accepts factories or credentials from its invocation payload.
export {runBufferedGcJob} from "./buffered-gc-job.ts";
