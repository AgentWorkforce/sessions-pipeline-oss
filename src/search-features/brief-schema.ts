/** Exact legacy derived brief features, separate from directory title documents. */
export const BRIEF_FEATURES_TABLE = "session_brief_features";
export function briefFeatureSchemaStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${BRIEF_FEATURES_TABLE}(
 org_id String,workspace_id String,source LowCardinality(String),session_id String,
 version UInt64,source_revision UInt64,source_hash FixedString(64),deleted UInt8,
 title String CODEC(ZSTD(3)),description String CODEC(ZSTD(3)),title_folded String CODEC(ZSTD(3)),description_folded String CODEC(ZSTD(3)),
 brief_completed_at DateTime64(6,'UTC'),embedding Array(Float32),embedding_model Nullable(String),embedding_dim UInt32,content_hash String,
 INDEX title_text title_folded TYPE text(tokenizer=ngrams(3)),
 INDEX description_text description_folded TYPE text(tokenizer=ngrams(3))
,
 retention_days UInt32 DEFAULT 0
) ENGINE=ReplacingMergeTree(version)
PARTITION BY toYYYYMM(brief_completed_at)
ORDER BY(org_id,workspace_id,source,session_id)
TTL toDateTime(brief_completed_at) + toIntervalDay(retention_days) DELETE WHERE retention_days > 0
SETTINGS index_granularity=1024`,
  ];
}
