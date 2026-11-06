/** Actual ClickHouse gzip/JSONEachRow codec with a local intercepted transport. */
import {gunzipSync} from "node:zlib";
import {createClickHouseClient} from "../vendor/search-index/clickhouse.ts";
import {EVENTS_TABLE,SESSION_STATE_TABLE} from "../vendor/search-index/schema.ts";
export function recordingReplayClickHouse(){
 let rows=0,compressedBytes=0,jsonBytes=0,maxBatch=0;
 const ids=new Set<string>(),states=new Map<string,Record<string,unknown>>();
 const ch=createClickHouseClient({url:"http://127.0.0.1:1",user:"fixture",password:"fixture",database:"replay_proof"},(async(url,init)=>{
  const params=new URL(String(url)).searchParams,query=params.get("query");
  if(query?.startsWith("INSERT INTO ")){
   const compressed=new Uint8Array(await new Response(init!.body).arrayBuffer()),text=gunzipSync(compressed).toString("utf8");
   compressedBytes+=compressed.length;jsonBytes+=Buffer.byteLength(text);let batch=0;
   for(const line of text.split("\n")){if(!line)continue;const row=JSON.parse(line);batch++;
    if(query.startsWith(`INSERT INTO ${EVENTS_TABLE} `)){rows++;ids.add(JSON.stringify([row.org_id,row.workspace_id,row.source,row.session_id,row.machine_id,row.kind,row.event_id]));}
    else if(query.startsWith(`INSERT INTO ${SESSION_STATE_TABLE} `))states.set(row.session_id,row);
    else throw Error("unexpected proof insert");
   }
   maxBatch=Math.max(maxBatch,batch);return new Response("");
  }
  if(params.get("default_format")==="JSONEachRow"){
   const sql=String(init!.body);
   if(!sql.includes("search_ingest_state")&&!sql.includes(`FROM ${EVENTS_TABLE} FINAL`))throw Error("unexpected proof select");
   return new Response("");
  }
  throw Error("unexpected proof command");
 }) as typeof fetch);
 return {ch,stats:()=>({rows,uniqueRows:ids.size,compressedBytes,jsonBytes,maxBatch,states:Object.fromEntries(states)})};
}
