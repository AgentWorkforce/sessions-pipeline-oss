/** Dense all-event features, no retention/window; exact source nullability and microseconds. */
export const EVENT_FEATURE_PROJECTION_VERSION = 1 as const;
export const EVENT_FEATURES_TABLE = "session_event_features";
export function eventFeatureSchemaStatements(): string[] {
  return [
    "CREATE TABLE IF NOT EXISTS session_event_features(\n org_id String,workspace_id String,source LowCardinality(String),session_id String,machine_id String,kind LowCardinality(String),event_id String,ts DateTime64(6,'UTC'),\n version UInt64,source_revision UInt64,source_hash FixedString(64),deleted UInt8,\n ingested_at DateTime64(6,'UTC'),user_id String,actor_name Nullable(String),actor_role Nullable(String),\n project_id Nullable(String),project_folded Nullable(String),task_title Nullable(String),task_title_folded Nullable(String),\n content Nullable(String) CODEC(ZSTD(3)),content_folded Nullable(String) CODEC(ZSTD(3)),task_ref_text String,task_ref_text_folded String,tags_json String,\n embedding Array(Float32),embedding_model Nullable(String),embedding_dim UInt32,content_hash Nullable(String),\n retention_days UInt32 DEFAULT 0\n) ENGINE=ReplacingMergeTree(version)\nPARTITION BY toYYYYMM(ts)\nORDER BY(org_id,workspace_id,source,session_id,machine_id,kind,event_id,ts)\nTTL toDateTime(ts) + toIntervalDay(retention_days) DELETE WHERE retention_days > 0\nSETTINGS index_granularity=1024",
  ];
}
