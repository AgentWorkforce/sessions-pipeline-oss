/** Trusted040530 ACK seats. Payload strings are never reserialized to inject a
 * seat: that would round large numeric literals or change preserved raw text.
 */
import{parseRelayhistoryRecord}from'../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts';
import type{BufferedClaimRecord,BufferedStageSpec}from'./buffered-protocol.ts';
const MAX=9223372036854775807n,HEX=/^[a-f0-9]{64}$/;
function no(code:string):never{throw Object.assign(Error('publication_seats_'+code),{code:'publication_seats_'+code});}
const object=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
function exact(v:unknown,keys:string[]):asserts v is Record<string,unknown>{
 if(!object(v)||Object.keys(v).length!==keys.length||keys.some(k=>!Object.hasOwn(v,k)))no('shape');
}
function decimal(v:unknown):string{
 if(typeof v!=='string'||!/^(0|[1-9][0-9]{0,18})$/.test(v)||BigInt(v)>MAX)no('decimal');return v as string;
}
export interface ReceiptSeatBaseline{controlEpoch:string;sourceIdentitySha256:string;allocatedHighWater:string}
export interface ReceiptSeat{
 readonly ord:number;readonly seat:string;readonly bodyAccepted:boolean;readonly candidateDigest:string;readonly firstDigest:string;readonly resetEpoch:string;readonly fenceEpoch:string;
}
export interface ReceiptSeatSlot{readonly record:Readonly<BufferedClaimRecord>;readonly seat:Readonly<ReceiptSeat>}
export interface ReceiptSeatSnapshot{
 readonly baseline:Readonly<ReceiptSeatBaseline>;readonly claimClockUpperBound:string;readonly slots:ReadonlyMap<number,ReceiptSeatSlot>;
}
export function parseReceiptSeatSnapshot(value:unknown,records:BufferedClaimRecord[],stage:BufferedStageSpec):ReceiptSeatSnapshot{
 exact(value,['controlEpoch','sourceIdentitySha256','allocatedHighWater','lastAssignedSeat','records']);
 if(typeof value.controlEpoch!=='string'||!HEX.test(value.controlEpoch)||typeof value.sourceIdentitySha256!=='string'||!HEX.test(value.sourceIdentitySha256))no('baseline');
 const floor=decimal(value.allocatedHighWater),upper=decimal(value.lastAssignedSeat);
 if(BigInt(upper)<BigInt(floor)||!Array.isArray(value.records)||value.records.length>500||!Array.isArray(records)||records.length>500||!Array.isArray(stage.groupOrdinals)||stage.groupOrdinals.length>500)no('bounds');
 const metadata=new Map<number,BufferedClaimRecord>();
 for(const r of records){if(!r||!Number.isInteger(r.ord)||r.ord<0||r.ord>499||metadata.has(r.ord)||!Number.isSafeInteger(r.revision)||r.revision<1||!HEX.test(r.digest))no('metadata');metadata.set(r.ord,r);}
 const wanted=new Set<number>();
 for(const group of stage.groupOrdinals){if(!Array.isArray(group)||!group.length)no('ordinals');for(const ord of group){if(!Number.isInteger(ord)||!metadata.has(ord)||wanted.has(ord))no('ordinals');wanted.add(ord);}}
 if(!wanted.size||value.records.length!==wanted.size)no('coverage');
 const slots=new Map<number,ReceiptSeatSlot>(),bySeat=new Map<string,string>(),byIdentity=new Map<string,string>(),byAcceptedSeat=new Map<string,string>();let previous=-1,originalReset:string|undefined;
 for(const v of value.records){
  exact(v,['ord','seat','bodyAccepted','candidateDigest','firstDigest','resetEpoch','fenceEpoch']);
  if(typeof v.ord!=='number'||!Number.isInteger(v.ord)||v.ord<=previous||!wanted.has(v.ord))no('ordinals');previous=v.ord;
  const record=metadata.get(v.ord)!;
  if(typeof v.bodyAccepted!=='boolean')no('decision');
  if(typeof v.candidateDigest!=='string'||v.candidateDigest!==record.digest||typeof v.firstDigest!=='string'||!HEX.test(v.firstDigest))no('digest');
  const seat=decimal(v.seat),resetEpoch=decimal(v.resetEpoch),fenceEpoch=decimal(v.fenceEpoch);
  if(BigInt(seat)<=BigInt(floor)||BigInt(seat)>BigInt(upper)||record.session_id===null&&fenceEpoch!=='0')no('seat');
  if(originalReset!==undefined&&originalReset!==resetEpoch)no('reset_alias');originalReset=resetEpoch;
  const key=JSON.stringify([record.source,record.session_id,record.record_id,record.revision,resetEpoch,fenceEpoch]);
  if(v.bodyAccepted&&v.firstDigest!==v.candidateDigest)no('decision');
  const proof=JSON.stringify([seat,v.firstDigest,v.bodyAccepted]);
  if(byIdentity.has(key)&&byIdentity.get(key)!==proof)no('seat_alias');byIdentity.set(key,proof);
  // One accepted winner may also be the immutable decision for a losing
  // equal-revision scope alias. Its native record/revision/reset must agree.
  const seatIdentity=JSON.stringify([record.record_id,record.revision,resetEpoch,v.firstDigest]);
  if(bySeat.has(seat)&&bySeat.get(seat)!==seatIdentity)no('seat_alias');bySeat.set(seat,seatIdentity);
  if(v.bodyAccepted){if(byAcceptedSeat.has(seat)&&byAcceptedSeat.get(seat)!==key)no('seat_alias');byAcceptedSeat.set(seat,key);}
  slots.set(v.ord,Object.freeze({record:Object.freeze({...record}),seat:Object.freeze({ord:v.ord,seat,bodyAccepted:v.bodyAccepted,candidateDigest:v.candidateDigest,firstDigest:v.firstDigest,resetEpoch,fenceEpoch})}));
 }
 return Object.freeze({baseline:Object.freeze({controlEpoch:value.controlEpoch,sourceIdentitySha256:value.sourceIdentitySha256,allocatedHighWater:floor}),claimClockUpperBound:upper,slots});
}
/** SQL may select only the first durable body; never silently change its exact
 * selected ordinal set here. A losing later body must already be stale in600.
 */
export function acceptedReceiptSeat(snapshot:ReceiptSeatSnapshot,ord:number,epochs?:{resetEpoch:string;fenceEpoch:string}):ReceiptSeatSlot{
 const slot=snapshot.slots.get(ord);if(!slot||!slot.seat.bodyAccepted||slot.seat.firstDigest!==slot.seat.candidateDigest)no('losing_body');
 if(epochs&&(slot.seat.resetEpoch!==epochs.resetEpoch||slot.seat.fenceEpoch!==epochs.fenceEpoch))no('epoch');return slot;
}
export function injectReceiptSeat(line:string,slot:ReceiptSeatSlot,originId:string):string{
 const parsed=parseRelayhistoryRecord(line);
 if(parsed.accepted_seq!==undefined||parsed.origin_id!==originId||parsed.record_id!==slot.record.record_id||parsed.revision!==String(slot.record.revision))no('line_identity');
 if(!slot.seat.bodyAccepted||slot.seat.firstDigest!==slot.record.digest||slot.seat.candidateDigest!==slot.record.digest)no('losing_body');
 const trimmed=line.trimEnd();if(!trimmed.endsWith('}'))no('line_object');
 return trimmed.slice(0,-1)+',"accepted_seq":'+JSON.stringify(slot.seat.seat)+'}'+line.slice(trimmed.length);
}
