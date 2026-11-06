/** Rebuildable metadata search index; transcript reconciliation never owns it. */
export const BRIEF_DOCUMENTS_TABLE = "session_brief_documents";
export function briefDocumentSchemaStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${BRIEF_DOCUMENTS_TABLE}
(
 org_id String,workspace_id String,source LowCardinality(String),session_id String,
 epoch UInt64,document_hash String,deleted UInt8,visible UInt8,
 title String CODEC(ZSTD(3)),summary String CODEC(ZSTD(3)),title_folded String CODEC(ZSTD(3)),summary_folded String CODEC(ZSTD(3)),project_id String,
 last_activity_at DateTime64(6,'UTC'),
 INDEX title_text title_folded TYPE text(tokenizer=ngrams(3)),
 INDEX summary_text summary_folded TYPE text(tokenizer=ngrams(3))
)
ENGINE=ReplacingMergeTree(epoch)
ORDER BY(org_id,workspace_id,source,session_id)
SETTINGS index_granularity=1024`,
  ];
}
