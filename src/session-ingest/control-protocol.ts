import {validatedCustodyAck} from "./custody-ack.ts";
import type {CurrentCustodyAck} from "./custody-protocol.ts";
/** Private signed dual-control contract. No decimal version is coerced to Number. */
export interface OrderedControl {epoch:string;version:string;digest:string;kind:"selection"|"reset"}
export interface OrderedControlAck extends OrderedControl {outcome:"applied"|"replay"|"stale"}
export interface OrderedControlResult {custodyAck?:CurrentCustodyAck;control:OrderedControlAck;updated?:number;deleted?:number}
export function orderedControl(v:unknown):OrderedControl{
 if(!v||typeof v!=="object"||Array.isArray(v))throw Error("control_invalid");
 const c=v as Record<string,unknown>;
 if(Object.keys(c).sort().join(",")!=="digest,epoch,kind,version"||
  typeof c.epoch!=="string"||!/^[a-f0-9]{64}$/.test(c.epoch)||typeof c.digest!=="string"||!/^[a-f0-9]{64}$/.test(c.digest)||
  typeof c.version!=="string"||!/^[1-9][0-9]{0,18}$/.test(c.version)||BigInt(c.version)>9223372036854775807n||
  (typeof c.kind!=="string"||!["selection","reset"].includes(c.kind)))throw Error("control_invalid");
 return{epoch:c.epoch,version:c.version,digest:c.digest,kind:c.kind as OrderedControl["kind"]};
}
export function orderedControlResult(v:unknown,expected:OrderedControl):OrderedControlResult{
 if(!v||typeof v!=="object"||Array.isArray(v))throw Error("control_ack_invalid");
 const r=v as Record<string,unknown>,c=r.control;
 if(!c||typeof c!=="object"||Array.isArray(c))throw Error("control_ack_invalid");
 const a=c as Record<string,unknown>;
 if(Object.keys(a).sort().join(",")!=="digest,epoch,kind,outcome,version"||
  a.epoch!==expected.epoch||a.version!==expected.version||a.digest!==expected.digest||a.kind!==expected.kind||
  (typeof a.outcome!=="string"||!["applied","replay","stale"].includes(a.outcome)))throw Error("control_ack_invalid");
 const key=expected.kind==="selection"?"updated":"deleted";
 if(Object.keys(r).sort().join(",")!==["control",key,...("custodyAck" in r?["custodyAck"]:[])].sort().join(",")||
  typeof r[key]!=="number"||!Number.isSafeInteger(r[key])||(r[key] as number)<0)throw Error("control_ack_invalid");
 if("custodyAck" in r)validatedCustodyAck(r.custodyAck);
 return r as unknown as OrderedControlResult;
}
