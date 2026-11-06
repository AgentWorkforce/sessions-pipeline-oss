/** Versioned PRIVATE RHC -> ingest wire. No public request fields or body caps change.
 * The original wire bytes are retained verbatim and authenticated with the complete
 * envelope. SQL recomputes sourceDigest over PostgreSQL JSONB text, not JS JSON.
 */
import type {OrderedControl} from "./control-protocol.ts";
import type {LedgerTenant} from "./ledger.ts";
export const CURRENT_CUSTODY_PROTOCOL="relay-ingest-custody-v1";
export const CURRENT_CUSTODY_CONTENT_TYPE="application/vnd.relay-ingest-custody-v1+json";
export const CURRENT_CUSTODY_ISSUER="relayhistory-cloud";
export const PRIVATE_CUSTODY_BYTES=8*1024*1024;
export const COLD_PRIVACY_BYTES=512*1024;
export const CUSTODY_RECORD_BYTES=4*1024*1024;
export interface SourcePrivacyPacket {
 complete:true;
 deletions:Array<{origin_id:string;source:string;session_id:string}>;
 resets:Array<{request_id:string;deleted:string}>;
}
export interface SourceCurrentRecord {
 userId:string;revision:string;digest:string;revisionId:string;kind:string;
 source:string;sessionId:string|null;operation:"upsert"|"delete";
 catalogAcceptedSeq:string|null;projectionAcceptedSeq:string|null;
}
export interface SourceCustodyRecord {
 /** Existing retained/prepared index. It is not a public original index across retired kinds. */
 preparedOrd:number;
 /** Actual native-source record_id/revision/revision_id processing rank. */
 processingRank:number;
 candidateDigest:string;recordId:string;revision:string;revisionId:string;
 candidateFenced:boolean;currentFenced:boolean;current:SourceCurrentRecord|null;
}
export interface SourceCustodyWitness {
 version:1;issuer:typeof CURRENT_CUSTODY_ISSUER;
 sourceIdentitySha256:string;controlEpoch:string;privacyBaselineSha256:string;
 tenant:LedgerTenant;originId:string|null;batchId:string|null;batchDigest:string|null;
 sourceControlVersion:string;pending:boolean;pendingControl:OrderedControl|null;
 /** Batch current version; control's durable PRE version(n-1). */
 privacyVersion:string;
 originalRecordCount:number;candidateCount:number;records:SourceCustodyRecord[];
 /** Omission is accepted only when the target already has its permanent USER seal. */
 privacy:SourcePrivacyPacket|null;
}
export interface CurrentCustodyPacket {witness:SourceCustodyWitness;sourceDigest:string}
export interface CurrentCustodyEnvelope extends CurrentCustodyPacket {
 protocol:typeof CURRENT_CUSTODY_PROTOCOL;
 original:{bodyBase64:string;contentEncoding:"identity"|"gzip"|"zstd"};
}
export interface LazyCustodyPolicy {
 baselineMode:"lazy-current";generation:string;sourceIdentitySha256:string;controlEpoch:string;
 privacyBaselineSha256:string;retentionContractSha256:string;sourceProofSha256:string;
 validUntil:string;issuer:typeof CURRENT_CUSTODY_ISSUER;
}

export const CURRENT_CUSTODY_ACK_HEADER="x-relay-custody-ack";
export interface CurrentCustodyAck {version:1;sourceDigest:string;sourceControlVersion:string;privacySealed:true}
