import {SEMANTIC_COPY_AUTHORITY_TABLE,SEMANTIC_COPY_PROFILE_TABLE} from "./semantic-copy-protocol.ts";
/** Operator-applied rebuildable metadata only; runtime never executes DDL. */
export function semanticCopySchemaStatements():string[]{return [
"CREATE TABLE IF NOT EXISTS "+SEMANTIC_COPY_AUTHORITY_TABLE+" (source_id String,kind LowCardinality(String),source_authority_hash FixedString(64),target_hash FixedString(64),profile_hash FixedString(64),generation UInt64,status_version UInt64,dirty UInt8,complete UInt8,state_json String,state_hash FixedString(64),data_hash FixedString(64),authority_hash FixedString(64),fresh_until DateTime64(6,'UTC')) ENGINE=ReplacingMergeTree(status_version) ORDER BY(source_id,kind)",
"CREATE TABLE IF NOT EXISTS "+SEMANTIC_COPY_PROFILE_TABLE+" (source_id String,source_authority_hash FixedString(64),target_hash FixedString(64),profile_hash FixedString(64),profile_json String) ENGINE=ReplacingMergeTree ORDER BY(source_id,source_authority_hash,target_hash,profile_hash)"
]}
