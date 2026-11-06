/** Erase WHOLE retired upload generations, never objects shared by a live image. */
import type{SemanticBoundedSql}from'../search-features/semantic-copy-postgres.ts';
import type{ObjectStore}from'../../packages/object-store/src/index.ts';
import type{PreservedVersionEraser}from'../migration/neon-s3-erasure/eraser.ts';
import{ensureExactRedactionBarrier}from'../shared/redaction-barrier.ts';
import{parseSessionKey}from'../shared/vendor.ts';
import{parseUploadGenerationPrefix}from'../../packages/object-store/src/layout.ts';
import{boundedPublicationStore}from'./publication-store.ts';
import type{PublicationTarget}from'./publication-protocol.ts';
export interface PublicationGcTicket{key:string;sha256:string;bytes:number;kind:'raw'|'manifest'|'derived';uploadGeneration:string;
 leaseToken:string;rowVersion:string;schemaGeneration:string;bucket:string;region:string;keyRoot:string}
const uuid=(s:unknown)=>typeof s==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(s);
const no=():never=>{throw Object.assign(Error('publication_gc_invalid'),{code:'publication_gc_invalid'});};
function assertTicket(t:PublicationGcTicket,target:PublicationTarget){
 if(!t||typeof t.key!=='string'||!uuid(t.uploadGeneration)||!uuid(t.leaseToken)||!/^[1-9][0-9]*$/.test(t.rowVersion)||
  !/^[a-f0-9]{64}$/.test(t.sha256)||!Number.isSafeInteger(t.bytes)||t.bytes<1||t.bytes>16777216||
  !['raw','manifest','derived'].includes(t.kind)||t.schemaGeneration!==target.generation||t.bucket!==target.bucket||t.region!==target.region||t.keyRoot!==target.keyRoot)no();
 const loc=parseSessionKey(t.key),prefix=loc&&parseUploadGenerationPrefix(loc.prefix??'');
 if(!prefix||prefix.generation!==t.uploadGeneration||!prefix.base.startsWith(target.keyRoot+'/')||
  prefix.base.slice(target.keyRoot.length+1).includes('/')||!prefix.base.slice(target.keyRoot.length+1))no();
}
export interface PublicationGcLedger{claim(limit:number,signal:AbortSignal):Promise<PublicationGcTicket[]>;finish(t:PublicationGcTicket,version:string,signal:AbortSignal):Promise<boolean>}
export function publicationGcLedger(db:SemanticBoundedSql,target:PublicationTarget):PublicationGcLedger{
 if(!/^[a-f0-9]{64}$/.test(target.generation))no();
 const call=async(fn:string,p:unknown,signal:AbortSignal)=>{
  const r=await db.queryBounded<{result:any}>(`SELECT sessions.ingest_publication_gc_${fn}($1::jsonb) result`,[JSON.stringify(p)],{signal,deadline:Date.now()+15000});
  if(r.rows.length!==1||!r.rows[0]?.result||typeof r.rows[0].result!=='object')no();return r.rows[0].result;
 };
 return{async claim(limit,signal){
  if(!Number.isInteger(limit)||limit<1||limit>100)no();
  const r=await call('claim',{limit,generation:target.generation,bucket:target.bucket,region:target.region,keyRoot:target.keyRoot},signal);
  if(!Array.isArray(r.tickets)||r.tickets.length>limit)no();r.tickets.forEach((t:PublicationGcTicket)=>assertTicket(t,target));return r.tickets;
 },async finish(t,barrierVersionId,signal){assertTicket(t,target);
  const r=await call('finish',{generation:target.generation,key:t.key,leaseToken:t.leaseToken,rowVersion:t.rowVersion,barrierVersionId},signal);
  if(typeof r.finished!=='boolean')no();return r.finished;
 }};
}
export async function collectPublicationObject(t:PublicationGcTicket,deps:{ledger:PublicationGcLedger;target:PublicationTarget;store:ObjectStore;eraser:PreservedVersionEraser},signal:AbortSignal):Promise<boolean>{
 assertTicket(t,deps.target);signal.throwIfAborted();
 const marker=new TextEncoder().encode(JSON.stringify({v:1,kind:'upload-generation-redaction',generation:t.uploadGeneration,original_sha256:t.sha256}));
 const store=boundedPublicationStore(deps.store,signal);
 const version=await ensureExactRedactionBarrier(store,{key:t.key,sha256:t.sha256,bytes:t.bytes,marker});
 signal.throwIfAborted();
 // The caller's eraser MUST apply the same request deadline; the persistent
 // marker keeps late create-only PUTs blocked across a crash at this boundary.
 await deps.eraser.eraseObjectExcept(t.key,version);signal.throwIfAborted();
 return deps.ledger.finish(t,version,signal);
}
