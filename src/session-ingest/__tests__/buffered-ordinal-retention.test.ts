import {afterEach, expect, test} from "bun:test";
import {zstdDecompressSync} from "node:zlib";
import {prepareBufferedStage, prepareBufferedReplacement, putBufferedStage, readBufferedStage} from "../staging.ts";
import {assertBufferedTicket} from "../buffered-gc.ts";
import type {BufferedGcTicket} from "../buffered-protocol.ts";
import {groupRecords} from "../writer.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {batch, record, TENANT, scratchStore} from "./support.ts";
const cleanup: (()=>void)[]=[];
afterEach(()=>{for(const done of cleanup.splice(0))done()});
const unzip=(b:Uint8Array)=>new Uint8Array(zstdDecompressSync(b));
async function fixture(){
 const {store,done}=scratchStore();cleanup.push(done);
 const p=await prepareDelivery(TENANT,await batch([
  record({record_id:"obsolete",session_id:"same",payload:{text:"OBSOLETE-PRIVATE-BYTES"}}),
  record({record_id:"keep",session_id:"same",payload:{text:"KEEP-EXACT-VALUE"}}),
  record({record_id:"drop-group",session_id:"other"}),
  record({record_id:"keep-null",session_id:null,source:"codex"}),
 ]));
 const grouped=groupRecords(p.records,new Date("2026-11-04T00:00:00Z"));
 const prepared=prepareBufferedStage("96bulls",{claim_token:"11111111-1111-4111-8111-111111111111",batch_digest:"a".repeat(64),received_at:"2026-11-04T00:00:00Z",org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,origin_id:"origin-1",user_id:TENANT.userId},grouped.groups,grouped.index);
 await putBufferedStage(store,prepared);
 const decoded=await readBufferedStage(store,prepared.spec,unzip);
 const indexes=prepared.spec.groupOrdinals.flatMap((ords,i)=>ords.some(ord=>[1,3].includes(ord))?[i]:[]);
 const ticket:BufferedGcTicket={...prepared.spec,stageToken:"22222222-2222-4222-8222-222222222222",claimToken:decoded.header.claim_token,leaseToken:"33333333-3333-4333-8333-333333333333",receiptState:"acknowledged",rowVersion:"2",resetEpoch:"0",phase:"plan",cleanupState:"live",keepOrdinals:[1,3],keepGroupIndexes:indexes};
 return {store,prepared,decoded,ticket};
}
test("partial same-group retention removes obsolete bytes, preserves original sparse ordinals and exact retained values",async()=>{
 const {store,decoded,ticket}=await fixture();
 assertBufferedTicket(ticket);
 const replacement=prepareBufferedReplacement("96bulls",ticket,decoded);
 expect(replacement.spec.groupOrdinals).toEqual([[1],[3]]);
 await putBufferedStage(store,replacement);
 const child=await readBufferedStage(store,replacement.spec,unzip);
 expect(child.groups[0]!.lines).toEqual([decoded.groups[0]!.lines[1]!]);
 expect(child.groups[0]!.meta).toEqual([decoded.groups[0]!.meta![1]!]);
 expect(child.groups[1]!.lines).toEqual(decoded.groups[2]!.lines);
 expect(JSON.stringify(child.groups)).not.toContain("OBSOLETE-PRIVATE-BYTES");
 const retry=prepareBufferedReplacement("96bulls",ticket,decoded);
 expect(retry.spec).toEqual(replacement.spec);expect(retry.body).toEqual(replacement.body);
 // A later rewrite retains original ord=3, never reindexes it to 0 or 1.
 const next:BufferedGcTicket={...ticket,...replacement.spec,keepOrdinals:[3],keepGroupIndexes:[1]};
 assertBufferedTicket(next);
 const grandchild=prepareBufferedReplacement("96bulls",next,child);
 expect(grandchild.spec.groupOrdinals).toEqual([[3]]);
});
test("record shrink is a valid plan even when every group remains",async()=>{
 const {store,decoded,ticket}=await fixture();
 const allGroups={...ticket,keepOrdinals:[1,2,3],keepGroupIndexes:[0,1,2]};
 assertBufferedTicket(allGroups);
 const replacement=prepareBufferedReplacement("96bulls",allGroups,decoded);
 expect(replacement.spec.groupOrdinals).toEqual([[1],[2],[3]]);
 await putBufferedStage(store,replacement);
 expect((await readBufferedStage(store,replacement.spec,unzip)).groups).toHaveLength(3);
});
test("missing, foreign, duplicate, unordered or inconsistent ordinal masks refuse before replacement",async()=>{
 const {decoded,ticket}=await fixture();
 for(const patch of [{keepOrdinals:undefined},{keepOrdinals:[1,1,3]},{keepOrdinals:[3,1]},{keepOrdinals:[1,99]},{keepGroupIndexes:[0]},{keepGroupIndexes:[2,0]}]){
  const invalid={...ticket,...patch} as BufferedGcTicket;
  expect(()=>assertBufferedTicket(invalid)).toThrow();
  expect(()=>prepareBufferedReplacement("96bulls",invalid,decoded)).toThrow();
 }
 const full={...ticket,keepOrdinals:[0,1,2,3],keepGroupIndexes:[0,1,2]};
 expect(()=>assertBufferedTicket(full)).toThrow();
 expect(()=>prepareBufferedReplacement("96bulls",full,decoded)).toThrow();
 const empty={...ticket,keepOrdinals:[],keepGroupIndexes:[]};
 assertBufferedTicket(empty);expect(()=>prepareBufferedReplacement("96bulls",empty,decoded)).toThrow();
 const cleanupTicket={...empty,phase:"cleanup" as const,cleanupState:"retired" as const};
 assertBufferedTicket(cleanupTicket);
 expect(()=>assertBufferedTicket({...cleanupTicket,keepOrdinals:[1],keepGroupIndexes:[0]})).toThrow();
});

test("GC preserves verified staged values rather than reapplying a non-idempotent scrubber",async()=>{
 const {store,decoded,ticket}=await fixture();
 // A codec vector, supplied directly at the initial-stage boundary: two scrub
 // passes produce different values. GC must copy the verified first-pass value.
 const lines=decoded.groups[0]!.lines.map((line,i)=>i===1?JSON.stringify({...JSON.parse(line),payload:{text:"?state=)?state=)"}}):line);
 const groups=[{...decoded.groups[0]!,lines},...decoded.groups.slice(1)];
 const initial=prepareBufferedStage("96bulls",decoded.header,groups,decoded.spec.groupOrdinals);
 await putBufferedStage(store,initial);
 const staged=await readBufferedStage(store,initial.spec,unzip);
 expect(JSON.parse(staged.groups[0]!.lines[1]!).payload.text).toBe("?state=[REDACTED]");
 const replacement=prepareBufferedReplacement("96bulls",{...ticket,...initial.spec},staged);
 await putBufferedStage(store,replacement);
 const child=await readBufferedStage(store,replacement.spec,unzip);
 expect(JSON.parse(child.groups[0]!.lines[0]!).payload.text).toBe("?state=[REDACTED]");
});
