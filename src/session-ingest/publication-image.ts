/** Bounded immutable image construction. Reuses the writer's manifest/chunk codec
 * and the reader's admitted source identity; it does not implement another fold.
 */
import {createHash} from "node:crypto";
import {
 objectKey,parseManifest,parseSessionKey,rawChunkKey,computeContentSha256,publicationManifestKey,isObjectStoreError,
 META_CONTENT_SHA256,type ObjectStore,type PutOpts,type PutAck,type ManifestV1,type ObjectVersion,
} from "../../packages/object-store/src/index.ts";
import {sealRawChunk} from "../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import {readCapped,buildObjectLandedMessage} from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import {canonicalManifestIdentity,manifestUserId} from "../shared/manifest-identity.ts";
import {nextManifest,lineTimeMs,admittedIdentity,LIVE_OWNER,derivedUuidV7} from "./writer.ts";
import {acceptedReceiptSeat,injectReceiptSeat}from "./publication-seats.ts";
import {fitLine} from "./oversize.ts";
import type {PublicationHead,PublicationTicket,PublicationRpc,PublicationImageSpec,PublicationObjectSpec,PublicationTarget} from "./publication-protocol.ts";
import type {DecodedBufferedStage} from "./staging.ts";
const MAX_CHUNK=16*1024*1024,MAX_MANIFEST=16*1024*1024;
const bytesSha=(bytes:Uint8Array)=>createHash("sha256").update(bytes).digest("hex");
const enc=new TextEncoder(),dec=new TextDecoder();
const refused=(code:string)=>Object.assign(Error(code),{code});
const rawDigest=(value:unknown)=>typeof value==="string"?value.replace(/^\\x/,""):"";
export interface PreparedPublicationImage {
 spec:PublicationImageSpec;manifest:ManifestV1;body:Uint8Array;head:PublicationHead;
 newChunk:{spec:PublicationObjectSpec;body:Uint8Array;options?:PutOpts};
 baseCopies:{spec:PublicationObjectSpec;sourceKey:string}[];
}
async function bounded(store:ObjectStore,key:string,bytes:number,signal:AbortSignal):Promise<Uint8Array>{
 signal.throwIfAborted();
 const result=await readCapped((await store.getObject(objectKey(key))).body,bytes);
 signal.throwIfAborted();
 if(result===null||result.byteLength!==bytes)throw refused("publication_object_bytes_mismatch");
 return result;
}
/** All registered generation objects, including retries, use create-only writes.
 * A persistent redaction barrier is never accepted as the original object.
 */
export async function putPublicationObject(store:ObjectStore,spec:Pick<PublicationObjectSpec,"key"|"sha256"|"bytes">,body:Uint8Array,signal:AbortSignal,options?:PutOpts):Promise<PutAck>{
 signal.throwIfAborted();
 if(body.byteLength!==spec.bytes||bytesSha(body)!==spec.sha256)throw refused("publication_object_proof_mismatch");
 try{
  const ack=await store.put(objectKey(spec.key),body,{...options,ifMatch:undefined,ifNoneMatch:true,contentSha256:spec.sha256});
  signal.throwIfAborted();
  if(ack.size!==spec.bytes||ack.contentSha256!==spec.sha256)throw refused("publication_put_ack_mismatch");
  return ack;
 }catch(error){
  signal.throwIfAborted();
  if(!isObjectStoreError(error,"already_exists")&&!isObjectStoreError(error,"precondition_failed"))throw error;
  const object=await store.getObject(objectKey(spec.key));
  const existing=await readCapped(object.body,spec.bytes);
  signal.throwIfAborted();
  if(!existing||existing.byteLength!==spec.bytes||bytesSha(existing)!==spec.sha256)throw refused("publication_existing_object_mismatch");
  return {key:objectKey(spec.key),version:object.meta.version,contentSha256:spec.sha256,size:spec.bytes,created:false};
 }
}
async function readPrevious(ticket:PublicationTicket,head:PublicationHead,target:PublicationTarget,store:ObjectStore,signal:AbortSignal){
 const key=head.previousKey??(typeof head.basePointer?.manifest_key==="string"?head.basePointer.manifest_key:null);
 if(!key)return null;
 signal.throwIfAborted();
 let object;
 try{object=await store.getObject(objectKey(key));}catch(error){
  if(isObjectStoreError(error,"not_found"))throw refused("publication_previous_manifest_missing");
  throw error;
 }
 const bytes=await readCapped(object.body,MAX_MANIFEST);
 signal.throwIfAborted();
 if(!bytes)throw refused("publication_previous_manifest_limit");
 const manifest=await parseManifest(dec.decode(bytes),key),identity=canonicalManifestIdentity(manifest),at=parseSessionKey(key);
 if(!at||manifest.raw.owner!==LIVE_OWNER||manifest.raw.format!=="ndjson"||!manifest.raw.scrub||
  manifest.session.workspace_id!==head.physicalWorkspace||manifest.session.harness!==head.physicalSource||
  manifest.session.session_id!==head.physicalSession||manifest.session.machine_id!==head.machineId||
  identity.sessionId!==head.sessionId||manifestUserId(manifest)!==ticket.tenant.userId||
  manifest.extensions?.["client.relayhistory_export"]!==undefined)
  throw refused("publication_previous_identity_mismatch");
 if(head.previousKey){
  if(at.prefix!==head.prefix||manifest.raw.content_sha256!==head.previousRawSha256||String(manifest.revision)!==String(BigInt(head.revision)-1n))
   throw refused("publication_previous_snapshot_mismatch");
 }else{
  const base=head.basePointer!;
  if(at.prefix!==target.keyRoot+"/"+ticket.tenant.orgId||base.storage_provider!=="s3"||base.bucket!==target.bucket||
   manifest.raw.content_sha256!==rawDigest(base.content_sha256)||String(manifest.revision)!==String(base.revision)||base.deleted_at!==null)
   throw refused("publication_legacy_base_mismatch");
 }
 return {key,manifest,etag:object.meta.version};
}
export async function preparePublicationImage(
 ticket:PublicationTicket,head:PublicationHead,decoded:DecodedBufferedStage,target:PublicationTarget,store:ObjectStore,signal:AbortSignal,
 now:()=>Date=()=>new Date(),
):Promise<PreparedPublicationImage>{
 signal.throwIfAborted();
 const header=decoded.header,group=decoded.groups[head.groupIndex],ordinals=decoded.spec.groupOrdinals[head.groupIndex];
 if(header.org_id!==ticket.tenant.orgId||header.workspace_id!==ticket.tenant.workspaceId||header.user_id!==ticket.tenant.userId||
  header.origin_id!==ticket.originId||header.claim_token!==ticket.claimToken||header.batch_digest!==ticket.batchDigest||
  !group||group.source!==head.source||group.sessionId!==head.sessionId||!ordinals)throw refused("publication_stage_identity_mismatch");
 const selected=new Set(head.ordinals),lines=group.lines.flatMap((line,i)=>{
  const ord=ordinals[i]!;if(!selected.has(ord))return [];
  const slot=acceptedReceiptSeat(ticket.receiptSeats,ord,{resetEpoch:head.resetEpoch,fenceEpoch:head.fenceEpoch});
  return [injectReceiptSeat(fitLine(line),slot,ticket.originId)];
 });
 if(lines.length!==head.ordinals.length)throw refused("publication_stage_ordinals_mismatch");
 const previous=await readPrevious(ticket,head,target,store,signal),prev=previous?.manifest??null;
 const received=new Date(header.received_at);
 if(!Number.isFinite(received.getTime()))throw refused("publication_stage_clock_invalid");
 const startedAt=prev?.session.started_at??new Date(Math.min(...lines.map(l=>lineTimeMs(l,received.getTime())),received.getTime())).toISOString();
 const location={prefix:head.prefix,workspaceId:head.physicalWorkspace,machineId:head.machineId,harness:head.physicalSource,sessionId:head.physicalSession,startDate:startedAt.slice(0,10)};
 const traceId=prev?.trace_id??derivedUuidV7(LIVE_OWNER+"\u001f"+head.generation+"\u001f"+head.source+"\u001f"+head.sessionId,Date.parse(startedAt));
 let captured:{key:string;body:Uint8Array;options?:PutOpts}|undefined;
 const capture={...store,backend:store.backend,async put(key,body,options){
  if(captured||body.byteLength>MAX_CHUNK)throw refused("publication_new_chunk_limit");
  captured={key:String(key),body,options};
  return {key,version:{eTag:"captured-not-published"} as ObjectVersion,size:body.byteLength,contentSha256:bytesSha(body),created:true};
 }} satisfies ObjectStore;
 const source=enc.encode(lines.join("\n")+"\n");
 if(source.byteLength>MAX_CHUNK)throw refused("publication_new_chunk_limit");
 const sealed=await sealRawChunk(capture,{location,format:"ndjson",seq:prev?.raw.chunks.length??0,epoch:prev?.raw.epoch??0,startOffset:prev?.raw.next_offset??0,source,traceId});
 if(!captured)throw refused("publication_chunk_codec_missing");
 const packed=captured as {key:string;body:Uint8Array;options?:PutOpts};
 const chunks=[...(prev?.raw.chunks??[]),sealed.entry];
 const manifest=nextManifest({batches:[[ticket.batchDigest,sealed.entry.seq]],prev,location,startedAt,
  sessionFields:{workspace_id:head.physicalWorkspace,machine_id:head.machineId,harness:head.physicalSource,session_id:head.physicalSession,started_at:startedAt},
  entry:sealed.entry,scrubVersion:sealed.scrub.version,traceId,contentSha256:await computeContentSha256(chunks),chunks,
  identity:admittedIdentity(prev,ticket.tenant.userId,head.sessionId),now:new Date(Math.max(now().getTime(),Date.parse(prev?.updated_at??"1970-01-01"))).toISOString()});
 // Derived objects belong to the exact old raw snapshot and namespace. They
 // are regenerated through the registered publication ports, never inherited.
 delete manifest.server;
 manifest.revision=Number(head.revision);
 manifest.writer.plugin_version="96bulls-background-publisher/1";
 manifest.extensions={...manifest.extensions,"96bulls.upload_publication":{v:1,generation:head.generation,publication:ticket.attemptToken,claim_token:ticket.claimToken}};
 const key=String(publicationManifestKey(location,ticket.attemptToken));
 await parseManifest(manifest,key);
 const body=enc.encode(JSON.stringify(manifest));
 if(body.byteLength>MAX_MANIFEST)throw refused("publication_manifest_limit");
 const row={org_id:ticket.tenant.orgId,workspace_id:ticket.tenant.workspaceId,harness:head.physicalSource,session_id:head.physicalSession,machine_id:head.machineId,
  user_id:ticket.tenant.userId,created_at:startedAt,last_activity_at:manifest.session.ended_at??manifest.raw.updated_at,event_count:chunks.reduce((n,c)=>n+(c.records??0),0),
  storage_provider:"s3",bucket:target.bucket,manifest_key:key,content_sha256:"\\x"+manifest.raw.content_sha256,raw_bytes:manifest.raw.raw_bytes,chunk_count:chunks.length,
  raw_format:"ndjson",format_version:1,normalized_version:null,scrubbed:true,manifest_etag:null,revision:manifest.revision,trace_id:traceId,deleted_at:null};
 const baseCopies:PreparedPublicationImage["baseCopies"]=[];
 if(previous&&!head.previousKey){
  const oldLoc=parseSessionKey(previous.key)!;
  for(const entry of prev!.raw.chunks){
   if(entry.stored_bytes>MAX_CHUNK||!entry.stored_sha256)throw refused("publication_legacy_chunk_limit");
   baseCopies.push({sourceKey:String(rawChunkKey(oldLoc,entry.seq,entry.start_offset,entry.sha256,"ndjson")),
    spec:{groupIndex:head.groupIndex,key:String(rawChunkKey(location,entry.seq,entry.start_offset,entry.sha256,"ndjson")),sha256:entry.stored_sha256,bytes:entry.stored_bytes,kind:"raw"}});
  }
 }
 return {head,manifest,body,spec:{groupIndex:head.groupIndex,manifestKey:key,manifestSha256:bytesSha(body),manifestBytes:body.byteLength,rawSha256:manifest.raw.content_sha256,startedAt,pointerRow:row},
  newChunk:{spec:{groupIndex:head.groupIndex,key:packed.key,sha256:bytesSha(packed.body),bytes:packed.body.byteLength,kind:"raw"},body:packed.body,options:packed.options},baseCopies};
}
export async function writePublicationImage(
 ticket:PublicationTicket,image:PreparedPublicationImage,rpc:PublicationRpc,store:ObjectStore,signal:AbortSignal,target:PublicationTarget,
):Promise<unknown|null>{
 await rpc.prepare(ticket,[image.spec],signal);
 const specs=[...image.baseCopies.map(c=>c.spec),image.newChunk.spec,
  {groupIndex:image.head.groupIndex,key:image.spec.manifestKey,sha256:image.spec.manifestSha256,bytes:image.body.byteLength,kind:"manifest" as const}];
 for(let i=0;i<specs.length;i+=128)await rpc.register(ticket,specs.slice(i,i+128),signal);
 // One bounded source object at a time. Exact immutable prior copies can be reused.
 for(const copy of image.baseCopies){
  signal.throwIfAborted();
  const existing=await store.head(objectKey(copy.spec.key));
  if(existing&&existing.size===copy.spec.bytes&&existing.metadata[META_CONTENT_SHA256]===copy.spec.sha256)continue;
  const source=await store.getObject(objectKey(copy.sourceKey));
  const bytes=await readCapped(source.body,copy.spec.bytes);signal.throwIfAborted();
  if(!bytes||bytes.byteLength!==copy.spec.bytes||bytesSha(bytes)!==copy.spec.sha256)throw refused("publication_legacy_chunk_digest");
  // The destination adapter writes its reserved stored-body hash from the
  // independently verified contentSha256 option; never replay it as user metadata.
  const{[META_CONTENT_SHA256]:_reserved,...metadata}=source.meta.metadata;
  await putPublicationObject(store,copy.spec,bytes,signal,{metadata});
 }
 await putPublicationObject(store,image.newChunk.spec,image.newChunk.body,signal,image.newChunk.options);
 const ack=await putPublicationObject(store,{key:image.spec.manifestKey,sha256:image.spec.manifestSha256,bytes:image.body.byteLength},image.body,signal,{contentType:"application/json"});
 if(image.head.source==="_none"&&image.head.sessionId==="_origin")return null;
 const built=buildObjectLandedMessage({landed:{key:image.spec.manifestKey,provider:"s3",bucket:target.bucket,size:image.body.byteLength,...(ack.version.eTag?{etag:ack.version.eTag}:{}),...(ack.version.version?{version:ack.version.version}:{})},
  orgId:ticket.tenant.orgId,manifest:image.manifest});
 if(built.status!=="enqueue")throw refused("publication_notification_invalid");
 return built.message;
}
