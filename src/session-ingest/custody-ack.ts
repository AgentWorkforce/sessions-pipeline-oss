import type {CurrentCustodyAck,CurrentCustodyPacket} from "./custody-protocol.ts";
export function validatedCustodyAck(v:unknown,packet?:CurrentCustodyPacket):CurrentCustodyAck{
 if(!v||typeof v!=="object"||Array.isArray(v))throw Error("custody_ack_invalid");
 const a=v as Record<string,unknown>;
 if(Object.keys(a).sort().join(",")!=="privacySealed,sourceControlVersion,sourceDigest,version"
  ||a.version!==1||a.privacySealed!==true||typeof a.sourceDigest!=="string"||!/^[0-9a-f]{64}$/.test(a.sourceDigest)
  ||typeof a.sourceControlVersion!=="string"||!/^(0|[1-9][0-9]{0,18})$/.test(a.sourceControlVersion)||BigInt(a.sourceControlVersion)>9223372036854775807n
  ||packet&&(a.sourceDigest!==packet.sourceDigest||a.sourceControlVersion!==packet.witness.sourceControlVersion))throw Error("custody_ack_invalid");
 return a as unknown as CurrentCustodyAck;
}
