import {DeliveryError,MAX_DELIVERY_BYTES} from "./vendor/delivery/delivery-contracts.ts";
import {COLD_PRIVACY_BYTES,CUSTODY_RECORD_BYTES,CURRENT_CUSTODY_PROTOCOL,type CurrentCustodyEnvelope} from "./custody-protocol.ts";
const object=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==="object"&&!Array.isArray(v);
const keys=(v:Record<string,unknown>,names:string[])=>Object.keys(v).sort().join("\n")===names.sort().join("\n");
const fail=():never=>{throw new DeliveryError("delivery_unavailable",503,"Current custody evidence is unavailable")};
/** Full witness and its source-PG JSONB digest are checked by SQL. This bounds
 * private allocations and recovers only the original authenticated wire. */
export function decodeCustodyEnvelope(bytes:Uint8Array):{packet:Pick<CurrentCustodyEnvelope,"witness"|"sourceDigest">;original:Uint8Array;encoding:string}{
 let v:unknown;try{v=JSON.parse(new TextDecoder("utf-8",{fatal:true,ignoreBOM:false}).decode(bytes))}catch{return fail()}
 if(!object(v)||!keys(v,["protocol","original","witness","sourceDigest"])||v.protocol!==CURRENT_CUSTODY_PROTOCOL||!object(v.original)||!keys(v.original,["bodyBase64","contentEncoding"])||!object(v.witness)||typeof v.sourceDigest!=="string"||!/^[0-9a-f]{64}$/.test(v.sourceDigest))return fail();
 const o=v.original,w=v.witness;
 if(typeof o.bodyBase64!=="string"||o.bodyBase64.length>4*Math.ceil(MAX_DELIVERY_BYTES/3)||! /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(o.bodyBase64)||!["identity","gzip","zstd"].includes(String(o.contentEncoding)))return fail();
 const original=Buffer.from(o.bodyBase64,"base64");if(original.byteLength>MAX_DELIVERY_BYTES||original.toString("base64")!==o.bodyBase64)return fail();
 if(!Array.isArray(w.records)||w.records.length>500||Buffer.byteLength(JSON.stringify(w.records),"utf8")>CUSTODY_RECORD_BYTES||w.privacy!==null&&Buffer.byteLength(JSON.stringify(w.privacy),"utf8")>COLD_PRIVACY_BYTES)return fail();
 return {packet:{witness:w as unknown as CurrentCustodyEnvelope["witness"],sourceDigest:v.sourceDigest},original,encoding:String(o.contentEncoding)};
}
