/** Internal publication protocol: metadata only; payload bytes remain in storage. */
import type {BufferedStageSpec,BufferedClaimRecord} from "./buffered-protocol.ts";
import type {ReceiptSeatSnapshot} from "./publication-seats.ts";
import type {LedgerTenant} from "./ledger.ts";
export interface PublicationHead {
 groupIndex:number;ordinals:number[];generation:string;prefix:string;headVersion:string;revision:string;
 previousKey:string|null;previousRawSha256:string|null;basePointer:Record<string,unknown>|null;
 source:string;sessionId:string;physicalWorkspace:string;physicalSource:string;physicalSession:string;machineId:string;
 fenceEpoch:string;resetEpoch:string;
}
export interface PublicationTicket {
 schemaGeneration:string;attemptToken:string;leaseToken:string;leaseUntil:string;stageToken:string;stageVersion:string;outboxVersion:string;
 tenant:LedgerTenant;originId:string;batchId:string;batchDigest:string;claimToken:string;stage:BufferedStageSpec;
 records:BufferedClaimRecord[];receiptSeats:ReceiptSeatSnapshot;acceptedOrdinals:number[];heads:PublicationHead[];
}
export interface PublicationTarget {bucket:string;region:string;keyRoot:string;generation:string}
export interface PublicationObjectSpec {groupIndex:number;key:string;sha256:string;bytes:number;kind:"raw"|"manifest"}
export interface PublicationImageSpec {
 groupIndex:number;manifestKey:string;manifestSha256:string;manifestBytes:number;rawSha256:string;startedAt:string;pointerRow:Record<string,unknown>;
}
export interface PublicationRpc {
 candidates(limit:number,signal:AbortSignal):Promise<string[]>;
 defer(stageToken:string,code:string,signal:AbortSignal):Promise<void>;
 claim(stageToken:string,signal:AbortSignal):Promise<PublicationTicket>;
 heartbeat(ticket:PublicationTicket,signal:AbortSignal):Promise<void>;
 prepare(ticket:PublicationTicket,images:PublicationImageSpec[],signal:AbortSignal):Promise<void>;
 register(ticket:PublicationTicket,objects:PublicationObjectSpec[],signal:AbortSignal):Promise<void>;
 publish(ticket:PublicationTicket,messages:unknown[],signal:AbortSignal):Promise<{published:true;replay:boolean;images:number;records:number;eligibleSessions:number}>;
 abandon(ticket:PublicationTicket,signal:AbortSignal):Promise<void>;
}
