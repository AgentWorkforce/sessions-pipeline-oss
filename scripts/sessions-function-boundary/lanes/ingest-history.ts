/**
 * Lane: ingest-history (620 canonical History lineage, PR #230).
 * SQL authority: supabase/migrations/20261105026000_canonical_history_lineage.sql.
 * Also generates that migration's rollback: it drops every owned function/table/trigger and restores
 * each replaced function from the migration named in `replaces[...].original` (in this order).
 */
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105026000_canonical_history_lineage.sql";
const ROLLBACK = "supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql";

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const restore: string[] = [];
  for (const sig of Object.keys(ctx.lane.replaces)) {
    const name = sig.slice("sessions.".length, sig.indexOf("("));
    const src = await ctx.read(ctx.lane.replaces[sig]!.original);
    const match = src.match(new RegExp("CREATE(?: OR REPLACE)? FUNCTION sessions\\." + name + "\\([\\s\\S]*?AS\\s+\\$([a-z_]+)\\$[\\s\\S]*?\\$\\1\\$;", "i"));
    if (!match) throw Error("History rollback original missing " + name);
    restore.push(match[0].replace(/^CREATE FUNCTION/, "CREATE OR REPLACE FUNCTION"));
  }
  const ownSig = ctx.defined.filter((k) => k in ctx.lane.functions).reverse();
  const ownTables = [...ctx.sql.matchAll(/CREATE TABLE\s+(sessions\.ingest_history_\w+)\(/g)].map((m) => m[1]!).reverse();
  const ownTriggers = [...ctx.sql.matchAll(/CREATE TRIGGER\s+(\w+)[\s\S]*?\bON\s+(sessions\.\w+)/g)].map((m) => "DROP TRIGGER IF EXISTS " + m[1] + " ON " + m[2] + ";");
  const down = [
    "-- Generated from620 owned definitions + authoritative560 originals. Stop all620/630 workers first.",
    "-- Operational rollback retains installed authority/object ledgers; never forget late PUT obligations.",
    "DO $rollback_guard$ DECLARE v_later boolean; BEGIN",
    "-- Named operational rollback: allowed while no authority was ever installed (no plans, versions,",
    "-- derived objects or model work). Re-derivable queues (reconcile/privacy outbox, required scopes,",
    "-- in-flight builds) are discarded with the schema; installed authority always blocks.",
    " IF EXISTS(SELECT 1 FROM sessions.ingest_history_plans)OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_versions)",
    "  OR EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects)OR to_regprocedure('sessions.session_history_model_claim(jsonb)')IS NOT NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: retain authority/object ledgers; use sessions.ingest_history_set_live(false)';END IF;",
    "-- The down removes this file's ledger row so the next migrate reapplies it (no function_acl_drift): only",
    "-- possible while it is the LAST applied upstream file, else sequencePreflight would see a ledger_gap.",
    " IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN",
    "  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE \"C\">''upstream:20261105026000_canonical_history_lineage.sql'')'INTO v_later;",
    "  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: a later migration is applied; use sessions.ingest_history_set_live(false)';END IF;",
    " END IF;",
    "END $rollback_guard$;", ...ownTriggers, ...restore,
    "DROP FUNCTION IF EXISTS\n " + ownSig.join(",\n ") + ";",
    "DROP TABLE IF EXISTS\n " + ownTables.join(",\n ") + ";",
    "DROP SEQUENCE IF EXISTS sessions.ingest_history_inventory_epoch;",
    "ALTER TABLE sessions.session_large_read_jobs DROP COLUMN history_generation;",
    "DO $ledger$ BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN",
    "  DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105026000_canonical_history_lineage.sql';",
    " END IF;",
    "END $ledger$;",
  ].join("\n") + "\n";
  return { [ROLLBACK]: down };
}

export const ingestHistory: LaneManifest = {
  lane: "ingest-history",
  owner: "#230 (codex/history620-creator-20261105)",
  migration: MIGRATION,
  prefixes: ["sessions.ingest_history_", "sessions.session_history_", "sessions.session_directory_history_"],
  grantSources: [
    { path: MIGRATION, roles: ["bulls96_session_ingest", "bulls96_canonical_feature_producer", "service_role", "session_directory_search_worker"] },
    // 630 model work (not on this branch yet): its bulls96_session_model grants on 620 functions.
    { path: "supabase/migrations/20261104063000_history_model_work.sql", roles: ["bulls96_session_model"], optional: true },
  ],
  functions: {
    "sessions.ingest_history_live()": { secdef: false, grants: [] },
    "sessions.ingest_history_tracking()": { secdef: false, grants: [] },
    "sessions.ingest_history_root(text)": { secdef: false, grants: [] },
    "sessions.ingest_history_pointer(sessions.session_index)": { secdef: false, grants: [] },
    "sessions.ingest_history_sync_pointer()": { secdef: true, grants: [] },
    "sessions.ingest_history_pointer_ref_backfill(jsonb,integer)": { secdef: false, grants: [] },
    "sessions.ingest_history_inventory_claim(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_inventory_locked(jsonb)": { secdef: false, grants: [] },
    "sessions.ingest_history_inventory_refs(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_inventory_page(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_inventory_finish(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_inventory_heartbeat(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_native_snapshot(uuid)": { secdef: false, grants: [] },
    "sessions.ingest_history_plan_claim(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_plan_locked(uuid,uuid,text)": { secdef: false, grants: [] },
    "sessions.ingest_history_plan_heartbeat(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_plan_carriers_page(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_plan_refs_page(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_active()": { secdef: false, grants: [] },
    "sessions.ingest_history_scope_managed(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.ingest_history_fence_builds(uuid[])": { secdef: false, grants: [] },
    "sessions.ingest_history_native_changed()": { secdef: true, grants: [] },
    "sessions.ingest_history_reconcile_backfill(text,integer)": { secdef: false, grants: [] },
    "sessions.ingest_history_scope_required(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.ingest_history_mask_register(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_mask_rows(text,bigint,integer)": { secdef: false, grants: [] },
    "sessions.ingest_history_scope_version_frozen()": { secdef: false, grants: [] },
    "sessions.ingest_history_scope_pointer_page(uuid,integer,integer)": { secdef: true, grants: ["bulls96_canonical_feature_producer"] },
    "sessions.ingest_history_source_current(jsonb,boolean)": { secdef: false, grants: [] },
    "sessions.session_history_source_lock(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_mask_page(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_register_object(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_require_model_permit(jsonb)": { secdef: false, grants: [] },
    "sessions.session_history_summary_publish(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_state_publish(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_capture(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_summary_get(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_replay_publish(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_replay_descriptor(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_mask_get(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_history_register_artifact(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_invalidate_scope(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.ingest_history_control_changed()": { secdef: true, grants: [] },
    "sessions.ingest_history_directory_state()": { secdef: true, grants: [] },
    "sessions.ingest_history_enqueue_graph(text,text,text,text,text)": { secdef: false, grants: [] },
    "sessions.session_history_large_job(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.session_directory_history_summary(text,text,text,text)": { secdef: true, grants: ["session_directory_search_worker"] },
    "sessions.ingest_history_scope_build_locked(jsonb,text)": { secdef: false, grants: [] },
    "sessions.ingest_history_scope_begin(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_scope_build_keys(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_scope_capture(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_scope_install(jsonb)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.ingest_history_invalidate_installed()": { secdef: false, grants: [] },
    "sessions.ingest_history_set_live(boolean)": { secdef: false, grants: [] },
    "sessions.ingest_history_set_pointer_tracking(boolean)": { secdef: false, grants: [] },
  },
  replaces: {
    "sessions.session_replay_source_snapshot(text,text,text,text)": { secdef: true, original: "supabase/migrations/20261105020000_session_replay_pages.sql" },
    "sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)": { secdef: true, original: "supabase/migrations/20261105020000_session_replay_pages.sql" },
    "sessions.session_publication_is_managed(text,text,text,text,text)": { secdef: true, original: "supabase/migrations/20261104060000_ingest_canonical_publication.sql" },
    "sessions.claim_session_large_read(uuid,integer)": { secdef: true, original: "supabase/migrations/20261105021000_session_large_read_jobs.sql" },
    "sessions.capture_session_replay_pages(text,text,text,text)": { secdef: true, original: "supabase/migrations/20261105020000_session_replay_pages.sql" },
    "sessions.session_brief(text,text,text)": { secdef: true, original: "supabase/migrations/20261102020000_session_brief_state.sql" },
    "sessions.session_directory_pointers(text,text,text)": { secdef: true, original: "supabase/migrations/20261102010000_session_directory_canonical_ids.sql" },
    "sessions.session_directory_replay_pages(text,text,text)": { secdef: true, original: "supabase/migrations/20261105020000_session_replay_pages.sql" },
  },
  extraOutputs: rollback,
};
