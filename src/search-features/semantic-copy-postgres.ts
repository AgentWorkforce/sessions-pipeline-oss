/** Job-owned PostgreSQL operations. A canceled RPC never reaches COMMIT.
 * Per-operation transactions pin SET LOCAL deadlines even behind transaction
 * poolers. Only this client's active query receives a protocol cancel packet.
 * No role defaults or backend termination are used. */
import pg,{type Pool,type PoolClient} from "pg";
import type {SqlExecutor} from "../../packages/pipeline/src/lib/pipeline/queue.ts";
export interface SemanticQueryContext {signal:AbortSignal;deadline:number;now?:()=>number}
export interface SemanticBoundedSql extends SqlExecutor {
 queryBounded<T>(sql:string,args:unknown[]|undefined,context:SemanticQueryContext):Promise<{rows:T[]}>;
}
export interface SemanticScopedSql extends SqlExecutor {
 queryWithSignal<T>(sql:string,args:unknown[]|undefined,signal:AbortSignal):Promise<{rows:T[]}>;
}
export function scopeSemanticSql(db:SemanticBoundedSql,context:SemanticQueryContext):SemanticScopedSql{
 if(typeof db.queryBounded!=="function")throw new Error("semantic_bounded_transport_required");
 return {query:<T>(sql:string,args?:unknown[])=>db.queryBounded<T>(sql,args,context),
 queryWithSignal:<T>(sql:string,args:unknown[]|undefined,signal:AbortSignal)=>db.queryBounded<T>(sql,args,{...context,signal:AbortSignal.any([context.signal,signal])})};
}
const abortError=()=>Object.assign(new Error("semantic_query_aborted"),{name:"AbortError"});
export class SemanticPostgresExecutor implements SemanticBoundedSql {
 constructor(readonly pool:Pool,readonly readonlySource=false){
  const timeout=(pool as any).options?.connectionTimeoutMillis;
  if(!Number.isInteger(timeout)||timeout<1||timeout>1000)throw new Error("semantic_pool_connection_bound_required");
 }
 query<T>(sql:string,args?:unknown[]){return this.queryBounded<T>(sql,args,{signal:AbortSignal.timeout(15000),deadline:Date.now()+15000})}
 async queryBounded<T>(sql:string,args:unknown[]|undefined,context:SemanticQueryContext):Promise<{rows:T[]}>{
  if(!Number.isFinite(context.deadline))throw new Error("semantic_query_deadline_invalid");
  const now=context.now??Date.now,remaining=()=>Math.ceil(context.deadline-now());
  const active=()=>{context.signal.throwIfAborted();if(remaining()<=0)throw abortError()};
  active();let client:PoolClient|undefined,broken=false,current:pg.Query|undefined;
  const cancelers=new Set<pg.Client>(),timers=new Set<ReturnType<typeof setTimeout>>();
  // The acquire callback destroys a late connection and never dispatches SQL.
  client=await new Promise<PoolClient>((resolve,reject)=>{
   let finished=false;
   const timer=setTimeout(()=>{finished=true;reject(abortError())},Math.min(1000,Math.max(1,remaining()))+500);
   this.pool.connect((error,c)=>{
    clearTimeout(timer);if(error){reject(error);return}
    if(!c){reject(new Error("semantic_connection_missing"));return}
    if(finished||context.signal.aborted||remaining()<=0){c.release(true);reject(abortError());return}
    resolve(c);
   });
  });
  const onClientError=()=>{};client.on("error",onClientError);
  const cancel=()=>{
   broken=true;
   if(current){
    // pg's cancel protocol uses this exact connection's PID+secret, including
    // transaction-pooler routing. It cannot cancel a sibling job's connection.
    const c=new pg.Client({...((this.pool as any).options??{}),connectionTimeoutMillis:1000});
    c.on("error",()=>{});cancelers.add(c);
    try{(c as any).cancel(client,current)}catch{}
   }
   const timer=setTimeout(()=>{(client as any)?.connection?.stream?.destroy(abortError())},500);
   timers.add(timer);
  };
  context.signal.addEventListener("abort",cancel,{once:true});
  const exec=<R>(text:string,values?:unknown[]):Promise<{rows:R[]}>=>{
   active();
   return new Promise((resolve,reject)=>{
    const rows:R[]=[],q=new pg.Query({text,values});
    current=q;
    const timer=setTimeout(()=>{broken=true;cancel();(client as any)?.connection?.stream?.destroy(abortError())},Math.max(1,remaining())+500);
    timers.add(timer);
    const done=()=>{clearTimeout(timer);timers.delete(timer);if(current===q)current=undefined};
    q.on("row",row=>rows.push(row as R));
    q.on("error",error=>{done();reject(error)});
    q.on("end",()=>{done();resolve({rows})});
    try{client!.query(q)}catch(error){done();reject(error)}
   });
  };
  try{
   active();const ms=Math.max(1,Math.min(15000,remaining())),lock=Math.min(5000,ms);
   // Only validated integers are interpolated; all application values remain
   // PostgreSQL protocol parameters. Settings disappear on commit/rollback.
   await exec("BEGIN"+(this.readonlySource?" READ ONLY":"")+";SET LOCAL search_path=pg_catalog,pg_temp;SET LOCAL statement_timeout="+ms+";SET LOCAL lock_timeout="+lock);
   active();const result=await exec<T>(sql,args);active();
   await exec("COMMIT");active();return result;
  }catch(error){broken=true;throw error}
  finally{
   context.signal.removeEventListener("abort",cancel);
   for(const timer of timers)clearTimeout(timer);
   for(const c of cancelers)(c as any).connection?.stream?.destroy();
   // Failed/aborted transactions close this owned connection; no COMMIT is
   // queued after failure. Successful transactions are ready for pool reuse.
   client.release(broken||context.signal.aborted);
   client.removeListener("error",onClientError);
  }
 }
}
