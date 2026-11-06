/** Persistent exact redaction marker. Typed callers validate their namespace and
 * retirement authority BEFORE calling. No delete-key window permits late create.
 */
import {createHash} from 'node:crypto';
import {objectKey,isObjectStoreError,type ObjectStore} from './vendor.ts';
const sha=(b:Uint8Array)=>createHash('sha256').update(b).digest('hex');
const no=(code:string):never=>{throw Object.assign(Error('redaction_barrier_'+code),{code:'redaction_barrier_'+code});};
async function bounded(body:ReadableStream<Uint8Array>,max:number){
 const reader=body.getReader(),parts:Uint8Array[]=[];let size=0;
 try{for(;;){const n=await reader.read();if(n.done)break;size+=n.value.byteLength;if(size>max)no('body_bound');parts.push(n.value);}}
 finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
 const out=new Uint8Array(size);let offset=0;for(const p of parts){out.set(p,offset);offset+=p.byteLength;}return out;
}
export interface RedactionProof{key:string;sha256:string;bytes:number;marker:Uint8Array}
export async function ensureExactRedactionBarrier(store:ObjectStore,p:RedactionProof):Promise<string>{
 if(!/^[a-f0-9]{64}$/.test(p.sha256)||!Number.isSafeInteger(p.bytes)||p.bytes<1||p.bytes>16777216||
  !(p.marker instanceof Uint8Array)||!p.marker.byteLength||p.marker.byteLength>65536)no('proof');
 const key=objectKey(p.key),expected=sha(p.marker);
 for(let attempt=0;attempt<8;attempt++){
  let current,denied:unknown;
  try{current=await store.getObject(key);}
  catch(e){if(isObjectStoreError(e,'unauthorized')&&(e.status===403||e.status===undefined))denied=e;
   else if(!isObjectStoreError(e,'not_found'))throw e;}
  if(current){
   const body=await bounded(current.body,Math.max(p.bytes,p.marker.byteLength));const digest=sha(body);
   if(body.byteLength===p.marker.byteLength&&digest===expected){
    const version=current.meta.version.version;
    if(typeof version!=='string'||!version||version==='null')no('version_unavailable');return version as string;
   }
   if(body.byteLength!==p.bytes||digest!==p.sha256)no('unexpected_content');
   if(!current.meta.version.eTag&&!current.meta.version.version)no('cas_unavailable');
  }
  try{await store.put(key,p.marker,current?{ifMatch:current.meta.version,contentType:'application/json'}:{ifNoneMatch:true,contentType:'application/json'});}
  catch(e){if(denied)throw denied;if(!isObjectStoreError(e,'already_exists')&&!isObjectStoreError(e,'precondition_failed'))throw e;}
 }
 return no('contended');
}
