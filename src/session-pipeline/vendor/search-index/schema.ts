/**
 * ClickHouse schema for the derived transcript search index.
 *
 * Neon (`sessions.convergence_events`) stays the source of truth. These tables
 * are a rebuildable index: dropping them and re-running the backfill restores
 * them. Adapted from the relayhistory-cloud #236 prototype to the columns the
 * legacy keyword search filters on and returns.
 *
 * Identity. Neon's primary key is (org_id, machine_id, source, session_id,
 * kind, event_id). The sort key adds workspace_id and ts (tenant columns
 * first, then time for pruning), so a row whose ts or workspace moves in Neon
 * becomes a new ClickHouse row; ingest deletes the old (key, ts, workspace)
 * row explicitly when it reconciles the session (see reconcile.ts).
 *
 * Idempotency. ReplacingMergeTree(version): re-inserting an event collapses to
 * the newest read. `version` is the epoch millisecond at which the row was read
 * from Neon, so a slow backfill batch can never overwrite a newer tail read.
 * Readers must collapse duplicates that merges have not yet folded: `FINAL`,
 * or `LIMIT 1 BY` the identity ordered by version.
 *
 * Erasure. Lightweight `DELETE FROM ... WHERE` (masks rows immediately; merges
 * purge them later).
 */

export const EVENTS_TABLE = "transcript_events";
export const SESSION_STATE_TABLE = "search_ingest_session_state";
export const INGEST_STATE_TABLE = "search_ingest_state";

export const SCHEMA_VERSION = 1;

/** DDL in apply order. Every statement is idempotent (IF NOT EXISTS). */
export function schemaStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${EVENTS_TABLE}
(
    org_id             LowCardinality(String),
    workspace_id       LowCardinality(String),
    ts                 DateTime64(3, 'UTC'),
    session_id         String,
    source             LowCardinality(String),
    machine_id         String,
    kind               LowCardinality(String),
    event_id           String,
    type               LowCardinality(String),
    user_id            String,
    actor_name         String,
    actor_role         LowCardinality(String),
    subagent_id        String,
    project_id         String,
    task_title         String,
    task_ref           String,
    task_ref_id        String,
    tags               Array(String),
    files_touched      Array(String),
    tool_name          LowCardinality(String),
    tool_status        LowCardinality(String),
    delivery_record_id String,
    content            String CODEC(ZSTD(3)),
    content_bytes      UInt32,
    ingested_at        DateTime64(3, 'UTC'),
    fingerprint        String,
    version            UInt64,
    INDEX content_text content TYPE text(tokenizer = ngrams(3), preprocessor = lowerUTF8(content)),
    INDEX title_text task_title TYPE text(tokenizer = ngrams(3), preprocessor = lowerUTF8(task_title)),
    INDEX session_bf session_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX project_bf project_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX task_ref_bf task_ref_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX tags_bf tags TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(ts)
ORDER BY (org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id)
SETTINGS index_granularity = 1024`,
    // Per (org, session) Neon revision the tail last reconciled.
    `CREATE TABLE IF NOT EXISTS ${SESSION_STATE_TABLE}
(
    org_id          String,
    session_id      String,
    revision        UInt64,
    synced_at       DateTime64(3, 'UTC'),
    full_synced_at  DateTime64(3, 'UTC'),
    scanned_rows    UInt32,
    -- 1 after a recent-only or row-capped pass: the sweep owes a full pass.
    needs_full      UInt8
)
ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (org_id, session_id)`,
    // Named watermarks and window metadata (tail_watermark, window_start, ...).
    `CREATE TABLE IF NOT EXISTS ${INGEST_STATE_TABLE}
(
    name        String,
    value       String,
    updated_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY name`,
  ];
}

/** Settings some ClickHouse versions require for the text index. */
export const SCHEMA_SETTINGS: Record<string, string | number> = {
  allow_experimental_full_text_index: 1,
  enable_full_text_index: 1,
};
