/** Pure shared CH publication protocol. No runtime/client/environment dependencies.
 * RHC consumes this file through a checked generator; do not hand-copy contracts. */
export const SEMANTIC_COPY_VERSION=1 as const;
export const SEMANTIC_COPY_AUTHORITY_TABLE="session_feature_copy_authority";
export const SEMANTIC_COPY_PROFILE_TABLE="session_feature_copy_profiles";
export const SEMANTIC_COPY_PROFILE_BYTES=262144;
export const SEMANTIC_COPY_PROFILE_ENTRIES=8192;
export type SemanticFeatureKind="event"|"brief";
export interface SemanticFeatureCheckpoint {
 phase:"baseline"|"journal";baselineH:string|null;baselineCursor:string|null;
 checkpoint:string;observedRevision:string;polled:boolean;
}
export interface SemanticFeatureAuthority extends SemanticFeatureCheckpoint {
 version:1;sourceId:string;kind:SemanticFeatureKind;sourceAuthorityHash:string;targetHash:string;profileHash:string;
 generation:string;statusVersion:string;dirty:boolean;complete:boolean;
 stateJson:string;stateHash:string;dataHash:string;authorityHash:string;freshUntil:string;
}
export interface SemanticFeatureProfile {sourceHash:string;source:Record<string,string>;entries:[string,string][]}
export const semanticDecimal=(v:unknown):v is string=>typeof v==="string"&&/^(0|[1-9][0-9]{0,18})$/u.test(v)&&BigInt(v)<=9223372036854775807n;
export const semanticHash=(v:unknown):v is string=>typeof v==="string"&&/^[0-9a-f]{64}$/u.test(v);
export function semanticCheckpoint(value:unknown):SemanticFeatureCheckpoint{
 const v=value as SemanticFeatureCheckpoint;
 if(!v||typeof v!=="object"||Object.keys(v).sort().join(",")!=="baselineCursor,baselineH,checkpoint,observedRevision,phase,polled"||
 !["baseline","journal"].includes(v.phase)||typeof v.polled!=="boolean"||!(v.baselineCursor===null||typeof v.baselineCursor==="string"&&v.baselineCursor.length<=16384)||
 !semanticDecimal(v.checkpoint)||!semanticDecimal(v.observedRevision)||!(v.baselineH===null||semanticDecimal(v.baselineH))||
 BigInt(v.checkpoint)>BigInt(v.observedRevision))throw new Error("feature_checkpoint_invalid");
 if(v.phase==="baseline"&&(v.polled||(v.baselineH===null?(v.checkpoint!=="0"||v.observedRevision!=="0"||v.baselineCursor!==null):(v.checkpoint!==v.baselineH||v.observedRevision!==v.baselineH))))throw new Error("feature_checkpoint_invalid");
 if(v.phase==="journal"&&(v.baselineH===null||v.baselineCursor!==null||BigInt(v.checkpoint)<BigInt(v.baselineH)))throw new Error("feature_checkpoint_invalid");
 return v;
}
export function semanticDataHashInput(v:Pick<SemanticFeatureAuthority,"sourceId"|"kind"|"sourceAuthorityHash"|"targetHash"|"profileHash"|"stateHash">){
 return ["semantic-copy-data-v1",v.sourceId,v.kind,v.sourceAuthorityHash,v.targetHash,v.profileHash,v.stateHash].join("\u001f");
}
export function semanticAuthorityHashInput(dataHash:string,generation:string,complete:boolean){return ["semantic-copy-authority-v1",dataHash,generation,String(complete)].join("\u001f")}
export async function semanticSha256(value:string):Promise<string>{const b=await crypto.subtle.digest("SHA-256",new TextEncoder().encode(value));return Array.from(new Uint8Array(b),x=>x.toString(16).padStart(2,"0")).join("")}
export function semanticStatusVersion(generation:string,dirty:boolean){if(!semanticDecimal(generation)||generation==="0")throw new Error("feature_generation_invalid");return (2n*BigInt(generation)+(dirty?0n:1n)).toString()}
export function parseSemanticProfile(json:string):SemanticFeatureProfile{
 if(new TextEncoder().encode(json).length>SEMANTIC_COPY_PROFILE_BYTES)throw new Error("feature_profile_bound");
 const v=JSON.parse(json);if(!v||Object.keys(v).sort().join(",")!=="entries,source"||!v.source||typeof v.source!=="object"||Array.isArray(v.source)||!["datcollate","datctype","datlocprovider"].every(k=>typeof v.source[k]==="string"&&v.source[k])||!((v.source.datlocprovider==="c"&&(v.source.datlocale===undefined||v.source.datlocale===""))||(v.source.datlocprovider==="b"&&v.source.datcollate==="C.UTF-8"&&v.source.datctype==="C.UTF-8"&&v.source.datlocale==="C.UTF-8"))||!Array.isArray(v.entries)||v.entries.length>SEMANTIC_COPY_PROFILE_ENTRIES)throw new Error("feature_profile_invalid");
 const keys=new Set<string>();for(const p of v.entries){if(!Array.isArray(p)||p.length!==2||p.some((s:unknown)=>typeof s!=="string"||Array.from(s).length!==1)||keys.has(p[0]))throw new Error("feature_profile_invalid");keys.add(p[0])}
 return {sourceHash:"",source:v.source,entries:v.entries};
}
export function foldSemanticQuery(profile:SemanticFeatureProfile,text:string){const map=new Map(profile.entries);return Array.from(text,c=>map.get(c)??c).join("")}

export function semanticTargetHashInput(endpoint:string,database:string,eventSchemaProtocolHash:string,briefSchemaProtocolHash:string){const u=new URL(endpoint);if(!["http:","https:"].includes(u.protocol)||!database||!semanticHash(eventSchemaProtocolHash)||!semanticHash(briefSchemaProtocolHash))throw new Error("feature_target_invalid");return JSON.stringify(["semantic-copy-target-v1",u.origin+u.pathname.replace(/\/+$/u,""),database,eventSchemaProtocolHash,briefSchemaProtocolHash])}

/** Legacy frozen-rank cursors bind deployment/source identity, not moving journal checkpoints. */
export function semanticCursorBindingInput(v:Pick<SemanticFeatureAuthority,"sourceId"|"sourceAuthorityHash"|"targetHash"|"profileHash">){return JSON.stringify(["semantic-copy-cursor-v1",v.sourceId,v.sourceAuthorityHash,v.targetHash,v.profileHash])}
