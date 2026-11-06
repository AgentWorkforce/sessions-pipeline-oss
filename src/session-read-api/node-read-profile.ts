/** Trusted Node execution budgets. No request parameter selects these profiles. */
import { ARTIFACT_MAX_SOURCE_BYTES, ARTIFACT_MAX_SOURCE_RECORDS } from "./canonical-artifact.ts";
export type NodeReadProfile = "standard" | "large";
export interface NodeReadBudget { sourceBytes:number; sourceRecords:number; projectedRows:number; artifactRows:number; artifactBytes:number }
const standard:Readonly<NodeReadBudget>=Object.freeze({sourceBytes:ARTIFACT_MAX_SOURCE_BYTES,sourceRecords:ARTIFACT_MAX_SOURCE_RECORDS,projectedRows:500_000,artifactRows:2_000_000,artifactBytes:2*1024**3});
const large:Readonly<NodeReadBudget>=Object.freeze({sourceBytes:2*1024**3,sourceRecords:2_000_000,projectedRows:4_000_000,artifactRows:4_000_000,artifactBytes:16*1024**3});
export function nodeReadBudget(profile:NodeReadProfile="standard"):Readonly<NodeReadBudget>{
 if(profile==="standard")return standard;
 if(profile==="large")return large;
 throw new Error("node_read_profile_invalid");
}
