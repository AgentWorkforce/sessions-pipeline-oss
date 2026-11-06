/**
 * One immutable catalog sentinel per schema step: a read-only predicate that is false
 * before the step and true from the step on (it stays true after every later step).
 * The migrator refuses (unledgered_schema) when an unrecorded step's sentinel is
 * already true, which catches steps the CREATE inventory can't see: ALTER-only steps
 * and CREATE OR REPLACE replacements.
 *
 * Tests prove, on PGlite and on real PostgreSQL 15 and 17:
 * - the table names every schema step;
 * - each sentinel is false after the steps before it and true after it, and all of
 *   them are true after the full set;
 * - for each ALTER or replacement step, an honestly applied and ledgered state is
 *   accepted, and an applied-but-unledgered state is refused before any write.
 *
 * Adding a migration means adding its sentinel here; the coverage test fails until you
 * do.
 */
const relation = (name: string) =>
  `SELECT to_regclass('${name}') IS NOT NULL AS applied`;
const column = (table: string, col: string) =>
  `SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('${table}') AND attname = '${col}' AND NOT attisdropped) AS applied`;
const fn = (schema: string, name: string) =>
  `SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = '${schema}' AND p.proname = '${name}') AS applied`;

export interface Sentinel {
  /** What the step leaves behind that no earlier step does. */
  evidence: string;
  /** A read-only query returning one row with a boolean `applied`. */
  sql: string;
}

export const MIGRATION_SENTINELS: Record<string, Sentinel> = {
  "upstream:20261105024000_session_brief_documents.sql": { evidence: "table sessions.session_brief_document_outbox", sql: relation("sessions.session_brief_document_outbox") },
  "upstream:20261105025000_canonical_feature_journal.sql": { evidence: "table sessions.canonical_feature_journal", sql: relation("sessions.canonical_feature_journal") },
  "upstream:20261105026000_canonical_history_lineage.sql": { evidence: "table sessions.ingest_history_carriers", sql: relation("sessions.ingest_history_carriers") },
  "upstream:20261105030000_canonical_feature_consumer_acks.sql": { evidence: "table sessions.canonical_feature_copy_state", sql: relation("sessions.canonical_feature_copy_state") },
  "upstream:20261105010000_frozen_source_custody.sql": {evidence:"table sessions.ingest_frozen_source_completeness",sql:relation("sessions.ingest_frozen_source_completeness")},
  "upstream:20261104055000_lazy_current_custody.sql": {evidence:"table sessions.ingest_lazy_policy",sql:relation("sessions.ingest_lazy_policy")},
  "upstream:20261104054000_ingest_custody_baseline.sql": {evidence:"table sessions.ingest_custody_imports",sql:relation("sessions.ingest_custody_imports")},
  "upstream:20261104053000_ingest_first_ack_seats.sql": {evidence:"table sessions.ingest_receipt_seat_clock",sql:relation("sessions.ingest_receipt_seat_clock")},
  "upstream:20261104052000_ordered_ingest_controls.sql": {evidence:"column sessions.ingest_buffer_admission.control_epoch",sql:column("sessions.ingest_buffer_admission","control_epoch")},
  "upstream:20261104060000_ingest_canonical_publication.sql": {evidence:"table sessions.ingest_publication_heads",sql:relation("sessions.ingest_publication_heads")},
  "upstream:20261104051000_ingest_buffer_gc.sql": {evidence:"column sessions.ingest_stage_intents.gc_lease_token",sql:column("sessions.ingest_stage_intents","gc_lease_token")},
  "upstream:20261105021000_session_large_read_jobs.sql": { evidence: "table sessions.session_large_read_jobs", sql: relation("sessions.session_large_read_jobs") },
  "upstream:20261105023000_session_analysis_publications.sql": { evidence: "table sessions.session_analysis_publications", sql: relation("sessions.session_analysis_publications") },
  "upstream:20261105022000_session_desktop_catalog.sql": { evidence: "column sessions.session_directory.catalog_read_version", sql: column("sessions.session_directory", "catalog_read_version") },
  "upstream:20261105020000_session_replay_pages.sql": { evidence: "table sessions.session_replay_publications", sql: relation("sessions.session_replay_publications") },
  "upstream:20261025000000_sessions_index.sql": {
    evidence: "table sessions.session_index",
    sql: relation("sessions.session_index"),
  },
  "upstream:20261025000100_repo_identity.sql": {
    evidence: "table sessions.repos",
    sql: relation("sessions.repos"),
  },
  "upstream:20261025000200_scrub_quarantine.sql": {
    evidence: "function sessions.session_index_scrub_sticky",
    sql: fn("sessions", "session_index_scrub_sticky"),
  },
  "upstream:20261025000300_upsert_keep_on_null.sql": {
    evidence:
      "replacement: sessions.upsert_session_index keeps stored values on NULL (its body has coalesce(excluded.title, s.title))",
    sql: `SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'sessions' AND p.proname = 'upsert_session_index'
              AND p.prosrc LIKE '%coalesce(excluded.title, s.title)%') AS applied`,
  },
  "upstream:20261025000600_pipeline_queue.sql": {
    evidence: "table sessions.pipeline_jobs",
    sql: relation("sessions.pipeline_jobs"),
  },
  "upstream:20261030000000_relayhistory_machines_compat.sql": {
    evidence: "ALTER-only: column sessions.machines.cursors_json",
    sql: column("sessions.machines", "cursors_json"),
  },
  "upstream:20261030010000_session_directory.sql": {
    evidence: "table sessions.session_directory",
    sql: relation("sessions.session_directory"),
  },
  "upstream:20261030020000_session_directory_short_summary.sql": {
    evidence: "column sessions.session_directory.short_summary",
    sql: column("sessions.session_directory", "short_summary"),
  },
  "upstream:20261031000000_migration_assertion_ledger.sql": {
    evidence: "table sessions.migration_assertion_uses",
    sql: relation("sessions.migration_assertion_uses"),
  },
  "upstream:20261031000100_manifest_readiness.sql": {
    evidence: "table sessions.manifest_readiness",
    sql: relation("sessions.manifest_readiness"),
  },
  "upstream:20261031001000_session_directory_reads_expand.sql": {
    evidence: "table sessions.session_directory_resurrection_fences",
    sql: relation("sessions.session_directory_resurrection_fences"),
  },
  "upstream:20261101050000_session_directory_list.sql": {
    evidence: "table sessions.session_directory_agents",
    sql: relation("sessions.session_directory_agents"),
  },
  "upstream:20261104020000_auth_sessions.sql": {
    evidence: "table sessions.auth_sessions",
    sql: relation("sessions.auth_sessions"),
  },
  "upstream:20261101000000_migration_ingest_audit_base_read.sql": {
    evidence: "table sessions.migration_base_read_uses",
    sql: relation("sessions.migration_base_read_uses"),
  },
  "upstream:20261104020100_auth_session_verify.sql": {
    evidence: "table sessions.auth_token_verifier_policy",
    sql: relation("sessions.auth_token_verifier_policy"),
  },
  "upstream:20261101000200_claimless_window_activations.sql": {
    evidence: "table sessions.claimless_window_activations",
    sql: relation("sessions.claimless_window_activations"),
  },
  "upstream:20261102000000_session_briefs.sql": {
    evidence: "table sessions.session_briefs",
    sql: relation("sessions.session_briefs"),
  },
  "upstream:20261102010000_session_directory_canonical_ids.sql": {
    evidence: "ALTER-only: column sessions.session_index.canonical_session_id",
    sql: column("sessions.session_index", "canonical_session_id"),
  },
  "upstream:20261104022000_session_ingest_ledger.sql": {
    evidence: "table sessions.ingest_batches",
    sql: relation("sessions.ingest_batches"),
  },
  "upstream:20261102020000_session_brief_state.sql": {
    evidence: "ALTER-only: column sessions.session_briefs.state (session_brief replaced to return it)",
    sql: column("sessions.session_briefs", "state"),
  },
  "upstream:20261104021000_session_directory_event_metadata.sql": {
    evidence: "table sessions.session_directory_event_times",
    sql: relation("sessions.session_directory_event_times"),
  },
  "upstream:20261104030000_ingest_fence_ownership.sql": {
    evidence: "table sessions.ingest_session_ownership",
    sql: relation("sessions.ingest_session_ownership"),
  },
  "upstream:20261104040000_ingest_s3_proof_seed.sql": {evidence:"table sessions.ingest_seed_captures",sql:relation("sessions.ingest_seed_captures")},
  "upstream:20261104050000_buffered_upload_receipts.sql": {
    evidence: "table sessions.ingest_stage_intents",
    sql: relation("sessions.ingest_stage_intents"),
  },
  "96bulls:0001_96bulls_observability.sql": {
    evidence: "column sessions.pipeline_jobs.traceparent",
    sql: column("sessions.pipeline_jobs", "traceparent"),
  },
  "96bulls:0002_session_ext_and_summaries.sql": {
    evidence: "table sessions.session_index_ext",
    sql: relation("sessions.session_index_ext"),
  },
  "96bulls:0003_fanout_ledger.sql": {
    evidence: "ALTER-only: column sessions.session_index_ext.fanout_revision",
    sql: column("sessions.session_index_ext", "fanout_revision"),
  },
  "96bulls:0004_summary_pointers.sql": {
    evidence:
      "DELETE + ALTER: column sessions.session_summaries.object_key (summary dropped)",
    sql: column("sessions.session_summaries", "object_key"),
  },
};

/** The ALTER-only and replacement steps the CREATE inventory alone can't see. */
export const NON_CREATE_STEPS = [
  "upstream:20261025000300_upsert_keep_on_null.sql",
  "upstream:20261030000000_relayhistory_machines_compat.sql",
  "upstream:20261102010000_session_directory_canonical_ids.sql",
  "upstream:20261102020000_session_brief_state.sql",
  "96bulls:0003_fanout_ledger.sql",
  "96bulls:0004_summary_pointers.sql",
] as const;
