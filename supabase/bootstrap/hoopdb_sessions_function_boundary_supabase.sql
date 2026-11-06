-- Supabase-only, transactional, mode-bound bootstrap: the reviewed `sessions` function
-- boundary event trigger (HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger).
--
-- Why: on Supabase the migration owner is `postgres`, and its GLOBAL function default
-- (ALTER DEFAULT PRIVILEGES FOR ROLE postgres ...) would reach every schema of the
-- project (#98). This trigger is scoped to schema `sessions` by NAME at fire time
-- (`sessions` may not exist yet when it is installed) and changes no default privilege.
--
-- What the trigger does, on ddl_command_end for the tags below:
--   - every routine CREATEd in `sessions` by the command: REVOKE ALL ... FROM PUBLIC. The
--     range/multirange constructors of a CREATE TYPE ... AS RANGE in `sessions` are
--     checked too; PostgreSQL makes the bootstrap superuser their owner, so the owner
--     check aborts that command (otherwise their PUBLIC EXECUTE would make every later
--     function GRANT in the database fail);
--   - then, for every `sessions` routine the command touched (and for every `sessions`
--     routine after a function GRANT/REVOKE, an extension or schema change), it ABORTS
--     the command (42501) when:
--       * the owner is not the trigger function's owner (the migration owner);
--       * an ACL entry is outside the frozen manifest for that signature (embedded
--         below, equal to src/hoopdb/sessions-function-acl.json), or outside owner-only
--         EXECUTE for a signature the manifest does not list; PUBLIC is never allowed.
--         The runtime role written into the function body may also hold EXECUTE
--         on the frozen runtime application functions (RUNTIME_APPLICATION_FUNCTIONS,
--         granted by the migrator, #116);
--       * it is SECURITY DEFINER but not in the frozen SECURITY DEFINER list;
--   - after ALTER DEFAULT PRIVILEGES: aborts when a function default scoped to
--     `sessions` grants anyone but its own role.
--   Entries are checked as a subset of the manifest while migrations run; the migrator
--   checks exact equality with the manifest after every non-local migrate, inside its
--   validation transaction (function_acl_drift), and the live gate keeps refusing any
--   PUBLIC-executable `sessions` function.
-- The function is SECURITY INVOKER with a pinned search_path: it runs as the user who
-- issued the DDL, so it can only revoke what that user owns, and it raises otherwise.
--
-- Supabase (supautils, https://github.com/supabase/supautils, "Non-Superuser Event
-- Triggers" and "Privileged Role"; https://supabase.com/docs/guides/database/postgres/event-triggers):
--   - `postgres` (the privileged role) can CREATE EVENT TRIGGER; supautils creates it as
--     the superuser and then changes its ownership to the privileged role, so the gate
--     expects evtowner = the migration owner (postgres) and fails closed otherwise;
--   - triggers created by the privileged role are SKIPPED for superusers and for
--     reserved roles: DDL run as supabase_admin is not covered. That matches the platform
--     trust root (SUPABASE_PLATFORM_SUPERUSERS); any other role able to CREATE in
--     `sessions` still needs its own exact function default (the migrator gate);
--   - privileged-role triggers do not fire for CREATE EXTENSION (supautils issue 123):
--     the migrator's post-migrate ACL manifest check and the live gate remain.
--   In those skipped cases (superusers, reserved roles, CREATE EXTENSION) only the live
--   gate (PUBLIC or the runtime role) and the next migrate's checks catch a bad ACL.
--   Whether supautils permits CREATE EVENT TRIGGER on the project is NOT OBSERVED.
-- The trigger is ENABLE ALWAYS ('A'): an 'O' trigger is skipped in replica mode
-- (session_replication_role = replica). An owner (or superuser) could still ALTER
-- EVENT TRIGGER ... DISABLE; the next migrate's gate refuses, and the live gate keeps
-- refusing PUBLIC-executable functions.
-- On plain PostgreSQL a non-superuser cannot CREATE EVENT TRIGGER: this script then
-- fails closed (55000, nothing created, the transaction rolls back).
--
-- Idempotent: an existing trigger of this name must already match exactly (event, tags,
-- function, enabled ALWAYS); otherwise it refuses and an operator decides. Never run it outside
-- Supabase: selfhosted, cloudsql and local use global-default-revoke or nothing.
--
-- Run as postgres, after hoopdb_runtime_role_supabase.sql, before migrate:
--   psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -d <the HoopDb database> \
--     -f supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql
\set ON_ERROR_STOP 1
BEGIN;
SELECT set_config('hoopdb.db_mode', 'supabase', true),
       set_config('hoopdb.boundary_runtime_role', :'runtime_role', true);
DO $preflight$
BEGIN
  IF current_setting('hoopdb.db_mode', true) IS DISTINCT FROM 'supabase' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'sessions function boundary bootstrap: supabase mode only';
  END IF;
  IF to_regnamespace('hoopdb_boundary') IS NOT NULL
     AND (SELECT nspowner FROM pg_namespace WHERE nspname = 'hoopdb_boundary') <> (SELECT oid FROM pg_roles WHERE rolname = current_user) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'sessions function boundary bootstrap: schema hoopdb_boundary exists and is not owned by the current user';
  END IF;
END
$preflight$;
CREATE SCHEMA IF NOT EXISTS hoopdb_boundary;
REVOKE ALL ON SCHEMA hoopdb_boundary FROM PUBLIC;
DO $settings$
DECLARE
  v_role text := current_setting('hoopdb.boundary_runtime_role');
  v_have text;
BEGIN
  IF v_role !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'sessions function boundary bootstrap: runtime_role must be a role name';
  END IF;
  SELECT substring(p.prosrc FROM 'v_runtime text := ''([a-z0-9_]+)'';') INTO v_have
    FROM pg_proc p
   WHERE p.oid = to_regprocedure('hoopdb_boundary.sessions_function_boundary()');
  IF v_have IS NOT NULL AND v_have <> v_role THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('sessions function boundary bootstrap: the trigger already names runtime role %s; an operator must decide', v_have);
  END IF;
END
$settings$;
-- The function is created from the reviewed body below with the runtime role written
-- into it as a literal (the only variable part; the gate normalizes it back and checks
-- the frozen sha256). A function-level SET of a custom parameter would need superuser.
DO $install_fn$
DECLARE
  v_role text := current_setting('hoopdb.boundary_runtime_role');
  v_body text := $boundary$
DECLARE
  v_ns oid := to_regnamespace('sessions');
  v_owner oid;
  v_manifest jsonb := $manifest${"sessions.ack_session_brief_documents(uuid,jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.applied_repo_evidence(jsonb,jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.auth_session_resolve(text,timestamp with time zone,timestamp with time zone)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.auth_session_verify(text,timestamp with time zone)":["<owner>:EXECUTE","session_token_verifier:EXECUTE"],"sessions.auth_sessions_mirror(jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.backfill_session_brief_documents(integer)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb)":["<owner>:EXECUTE"],"sessions.canonical_feature_image_key(jsonb,text)":["<owner>:EXECUTE"],"sessions.canonical_feature_journal_seed(bigint,text)":["<owner>:EXECUTE"],"sessions.canonical_feature_page_ack_fence()":["<owner>:EXECUTE"],"sessions.canonical_feature_pointer_set_valid(jsonb)":["<owner>:EXECUTE"],"sessions.canonical_feature_source_valid(jsonb)":["<owner>:EXECUTE"],"sessions.canonical_remote(text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.capture_session_replay_pages(text,text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.claim_session_analysis_cache()":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.claim_session_brief_documents(uuid,integer)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.claim_session_large_read(uuid,integer)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.claimless_window_activations_append_only()":["<owner>:EXECUTE"],"sessions.clean_root_commit(text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.discover_session_large_reads()":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.enqueue_session_analysis_cache(text,text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.enqueue_session_large_read(text,text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text)":["<owner>:EXECUTE","bulls96_canonical_feature_producer:EXECUTE"],"sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.heartbeat_session_large_read(text,text,text,text,uuid)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.ingest_acquire_slot(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_apply_ordered_control(jsonb,text)":["<owner>:EXECUTE"],"sessions.ingest_apply_reset(text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_apply_selection(text,text,text,text,jsonb,boolean)":["<owner>:EXECUTE"],"sessions.ingest_attempt_epochs(text,text,text,text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_attempt_fenced(text,text,text,text,jsonb,jsonb)":["<owner>:EXECUTE"],"sessions.ingest_batch_key(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_bounded(jsonb,integer,integer)":["<owner>:EXECUTE"],"sessions.ingest_buffer_gc_claim(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_buffer_gc_finish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_buffer_gc_groups(jsonb,jsonb)":["<owner>:EXECUTE"],"sessions.ingest_buffer_gc_kept(uuid)":["<owner>:EXECUTE"],"sessions.ingest_buffer_gc_locked(uuid,uuid)":["<owner>:EXECUTE"],"sessions.ingest_buffer_gc_ordinals(uuid)":["<owner>:EXECUTE"],"sessions.ingest_buffer_gc_prepare(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_buffer_gc_swap(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_claim_batch(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_claim_batch_mode(jsonb,text)":["<owner>:EXECUTE"],"sessions.ingest_claim_buffered_batch(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_commit_batch(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_commit_buffered_batch(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_commit_revisions(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_custody_begin(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_custody_complete(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_custody_page(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_custody_release(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_digest(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_exact_keys(jsonb,text[])":["<owner>:EXECUTE"],"sessions.ingest_fanout_fence(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_fence_pairs(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_frozen_source_policy()":["<owner>:EXECUTE"],"sessions.ingest_frozen_source_privacy(text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_frozen_source_receipt(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_frozen_source_witness(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_active()":["<owner>:EXECUTE"],"sessions.ingest_history_control_changed()":["<owner>:EXECUTE"],"sessions.ingest_history_directory_state()":["<owner>:EXECUTE"],"sessions.ingest_history_enqueue_graph(text,text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_history_fence_builds(uuid[])":["<owner>:EXECUTE"],"sessions.ingest_history_invalidate_installed()":["<owner>:EXECUTE"],"sessions.ingest_history_invalidate_scope(text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_history_inventory_claim(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_inventory_finish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_inventory_heartbeat(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_inventory_locked(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_history_inventory_page(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_inventory_refs(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_live()":["<owner>:EXECUTE"],"sessions.ingest_history_mask_get(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_mask_register(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_mask_rows(text,bigint,integer)":["<owner>:EXECUTE"],"sessions.ingest_history_native_changed()":["<owner>:EXECUTE"],"sessions.ingest_history_native_snapshot(uuid)":["<owner>:EXECUTE"],"sessions.ingest_history_plan_carriers_page(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_plan_claim(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_plan_heartbeat(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_plan_locked(uuid,uuid,text)":["<owner>:EXECUTE"],"sessions.ingest_history_plan_refs_page(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_pointer(sessions.session_index)":["<owner>:EXECUTE"],"sessions.ingest_history_pointer_ref_backfill(jsonb,integer)":["<owner>:EXECUTE"],"sessions.ingest_history_reconcile_backfill(text,integer)":["<owner>:EXECUTE"],"sessions.ingest_history_require_model_permit(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_history_root(text)":["<owner>:EXECUTE"],"sessions.ingest_history_scope_begin(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_scope_build_keys(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_scope_build_locked(jsonb,text)":["<owner>:EXECUTE"],"sessions.ingest_history_scope_capture(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_scope_install(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_history_scope_managed(text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_history_scope_pointer_page(uuid,integer,integer)":["<owner>:EXECUTE","bulls96_canonical_feature_producer:EXECUTE"],"sessions.ingest_history_scope_required(text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_history_scope_version_frozen()":["<owner>:EXECUTE"],"sessions.ingest_history_set_live(boolean)":["<owner>:EXECUTE"],"sessions.ingest_history_set_pointer_tracking(boolean)":["<owner>:EXECUTE"],"sessions.ingest_history_source_current(jsonb,boolean)":["<owner>:EXECUTE"],"sessions.ingest_history_sync_pointer()":["<owner>:EXECUTE"],"sessions.ingest_history_tracking()":["<owner>:EXECUTE"],"sessions.ingest_identifier(jsonb,boolean)":["<owner>:EXECUTE"],"sessions.ingest_lazy_ack(jsonb,text)":["<owner>:EXECUTE"],"sessions.ingest_lazy_ack_batch(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_lazy_attempt(uuid)":["<owner>:EXECUTE"],"sessions.ingest_lazy_attempt_ack(uuid)":["<owner>:EXECUTE"],"sessions.ingest_lazy_begin(jsonb,text)":["<owner>:EXECUTE"],"sessions.ingest_lazy_clear(jsonb,text)":["<owner>:EXECUTE"],"sessions.ingest_lazy_guard(text)":["<owner>:EXECUTE"],"sessions.ingest_lazy_policy_set(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_lazy_records(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_lazy_stage_seats(uuid)":["<owner>:EXECUTE"],"sessions.ingest_lazy_user(jsonb,text,jsonb)":["<owner>:EXECUTE"],"sessions.ingest_lock_fences(text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_prepare_buffer_stage(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_abandon(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_candidates(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_claim(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_component(text)":["<owner>:EXECUTE"],"sessions.ingest_publication_defer(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_gc_claim(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_gc_finish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_guard_pointer()":["<owner>:EXECUTE"],"sessions.ingest_publication_heartbeat(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_locked(uuid,uuid,text)":["<owner>:EXECUTE"],"sessions.ingest_publication_prepare(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_publish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_register_objects(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_publication_retire_fence()":["<owner>:EXECUTE"],"sessions.ingest_publication_revision_snapshot(uuid)":["<owner>:EXECUTE"],"sessions.ingest_publication_scope(jsonb,boolean)":["<owner>:EXECUTE"],"sessions.ingest_receipt_seat_ack()":["<owner>:EXECUTE"],"sessions.ingest_receipt_seat_initialize(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_receipt_seat_records(uuid,jsonb)":["<owner>:EXECUTE"],"sessions.ingest_receipt_seats_for_stage(uuid)":["<owner>:EXECUTE"],"sessions.ingest_records(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_release_slot(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_require_buffer_admission(text)":["<owner>:EXECUTE"],"sessions.ingest_require_custody(text)":["<owner>:EXECUTE"],"sessions.ingest_require_generation(text)":["<owner>:EXECUTE"],"sessions.ingest_require_role(text)":["<owner>:EXECUTE"],"sessions.ingest_reset_uploaded_sessions(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_revision(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_seed_close_baseline()":["<owner>:EXECUTE"],"sessions.ingest_seed_fence_baseline(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest_seeder:EXECUTE"],"sessions.ingest_seed_fences(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_seed_proven_records(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest_seeder:EXECUTE"],"sessions.ingest_seed_records(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_seed_scope_capture(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest_seeder:EXECUTE"],"sessions.ingest_seed_scope_page(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest_seeder:EXECUTE"],"sessions.ingest_seed_scope_state(text,text,text,text)":["<owner>:EXECUTE"],"sessions.ingest_seed_source_changed()":["<owner>:EXECUTE"],"sessions.ingest_session_pairs(jsonb)":["<owner>:EXECUTE"],"sessions.ingest_set_session_uploads(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_source_control_begin(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.ingest_source_control_complete(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.jwt_org_id()":["<owner>:EXECUTE","service_role:EXECUTE","authenticated:EXECUTE"],"sessions.jwt_workspace_ids()":["<owner>:EXECUTE","service_role:EXECUTE","authenticated:EXECUTE"],"sessions.legacy_remote(text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.list_session_directory(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.lock_repo_catalog(text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.member_of(text,text[])":["<owner>:EXECUTE","service_role:EXECUTE","authenticated:EXECUTE"],"sessions.migration_ingest_audit_append_only()":["<owner>:EXECUTE"],"sessions.pipeline_dead_letters_observability()":["<owner>:EXECUTE"],"sessions.pipeline_events_maintain(interval,integer)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.pipeline_jobs_observability()":["<owner>:EXECUTE"],"sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.queue_session_brief_document(jsonb)":["<owner>:EXECUTE"],"sessions.repo_backfill_report()":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.repo_backfill_step(integer,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.repo_group(text,uuid)":["<owner>:EXECUTE","service_role:EXECUTE","authenticated:EXECUTE"],"sessions.repo_live(text,uuid)":["<owner>:EXECUTE","service_role:EXECUTE","authenticated:EXECUTE"],"sessions.repo_visibility_ids(text,uuid)":["<owner>:EXECUTE","service_role:EXECUTE","authenticated:EXECUTE"],"sessions.repos_maintain(integer)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.request_session_analysis_cache(text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.resolve_repo(text,text,text,timestamp with time zone)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.retry_session_brief_documents(uuid,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.retry_session_large_read(text,text,text,text,uuid,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.route_session_large_read(text,text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.search_session_directory(text,text,text,text,text,boolean,text,text,text,text,text,timestamp with time zone,timestamp with time zone,text,timestamp with time zone,text,text,text,integer)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_analysis_cache_demand(text,text,text,text,boolean)":["<owner>:EXECUTE"],"sessions.session_brief(text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_brief_document_authority(text,text[],text[])":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_brief_document_changed()":["<owner>:EXECUTE"],"sessions.session_brief_document_json(sessions.session_directory,boolean)":["<owner>:EXECUTE"],"sessions.session_briefs_forget()":["<owner>:EXECUTE"],"sessions.session_directory_activity(text,timestamp with time zone,timestamp with time zone)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_analysis_cache(text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_companions_follow()":["<owner>:EXECUTE"],"sessions.session_directory_desktop_catalog(text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_event_activity(text,timestamp with time zone,timestamp with time zone)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_event_times_forget()":["<owner>:EXECUTE"],"sessions.session_directory_events_changed()":["<owner>:EXECUTE"],"sessions.session_directory_events_list_sql(integer,boolean,integer,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean)":["<owner>:EXECUTE"],"sessions.session_directory_events_require_ready(text)":["<owner>:EXECUTE"],"sessions.session_directory_events_tombstone()":["<owner>:EXECUTE"],"sessions.session_directory_forget_catalog()":["<owner>:EXECUTE"],"sessions.session_directory_history_summary(text,text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_list_sql(integer,boolean,integer,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean)":["<owner>:EXECUTE"],"sessions.session_directory_pointers(text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_project_name()":["<owner>:EXECUTE"],"sessions.session_directory_project_text(text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_directory_readiness()":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_record_fences()":["<owner>:EXECUTE"],"sessions.session_directory_replay_pages(text,text,text)":["<owner>:EXECUTE","session_directory_search_worker:EXECUTE"],"sessions.session_directory_search_query(text)":["<owner>:EXECUTE"],"sessions.session_directory_search_text(text,text,text,text,text,text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_directory_tombstone_clears_catalog()":["<owner>:EXECUTE"],"sessions.session_directory_tombstone_clears_summary()":["<owner>:EXECUTE"],"sessions.session_directory_worker_scope(text)":["<owner>:EXECUTE"],"sessions.session_history_capture(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_large_job(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_mask_page(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_register_artifact(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_register_object(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_replay_descriptor(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_replay_publish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_source_lock(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_state_publish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_summary_get(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_history_summary_publish(jsonb)":["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"],"sessions.session_index_batch(jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_index_scrub_sticky()":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_publication_is_managed(text,text,text,text,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_publication_lock(jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_publication_probe(jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_publication_register_object(jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.session_replay_directory_changed()":["<owner>:EXECUTE"],"sessions.session_replay_source_snapshot(text,text,text,text)":["<owner>:EXECUTE"],"sessions.session_replay_sources_changed()":["<owner>:EXECUTE"],"sessions.set_session_brief_documents_live(boolean)":["<owner>:EXECUTE"],"sessions.set_session_state(text,text,text,text,text,text,timestamp with time zone)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamp with time zone,text)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.upsert_machine(text,text,text,text,text,text,timestamp with time zone)":["<owner>:EXECUTE","service_role:EXECUTE"],"sessions.upsert_session_index(jsonb)":["<owner>:EXECUTE","service_role:EXECUTE"]}$manifest$::jsonb;
  v_secdef text[] := ARRAY[
    'sessions.ack_session_brief_documents(uuid,jsonb)',
    'sessions.auth_session_verify(text,timestamp with time zone)',
    'sessions.backfill_session_brief_documents(integer)',
    'sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb)',
    'sessions.canonical_feature_journal_seed(bigint,text)',
    'sessions.canonical_feature_page_ack_fence()',
    'sessions.capture_session_replay_pages(text,text,text,text)',
    'sessions.claim_session_analysis_cache()',
    'sessions.claim_session_brief_documents(uuid,integer)',
    'sessions.claim_session_large_read(uuid,integer)',
    'sessions.discover_session_large_reads()',
    'sessions.enqueue_session_analysis_cache(text,text,text,text)',
    'sessions.enqueue_session_large_read(text,text,text,text)',
    'sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text)',
    'sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text)',
    'sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid)',
    'sessions.heartbeat_session_large_read(text,text,text,text,uuid)',
    'sessions.ingest_acquire_slot(jsonb)',
    'sessions.ingest_buffer_gc_claim(jsonb)',
    'sessions.ingest_buffer_gc_finish(jsonb)',
    'sessions.ingest_buffer_gc_prepare(jsonb)',
    'sessions.ingest_buffer_gc_swap(jsonb)',
    'sessions.ingest_claim_batch(jsonb)',
    'sessions.ingest_claim_buffered_batch(jsonb)',
    'sessions.ingest_commit_batch(jsonb)',
    'sessions.ingest_commit_buffered_batch(jsonb)',
    'sessions.ingest_commit_revisions(jsonb)',
    'sessions.ingest_fanout_fence(jsonb)',
    'sessions.ingest_frozen_source_receipt(jsonb)',
    'sessions.ingest_frozen_source_witness(jsonb)',
    'sessions.ingest_history_control_changed()',
    'sessions.ingest_history_directory_state()',
    'sessions.ingest_history_inventory_claim(jsonb)',
    'sessions.ingest_history_inventory_finish(jsonb)',
    'sessions.ingest_history_inventory_heartbeat(jsonb)',
    'sessions.ingest_history_inventory_page(jsonb)',
    'sessions.ingest_history_inventory_refs(jsonb)',
    'sessions.ingest_history_mask_get(jsonb)',
    'sessions.ingest_history_mask_register(jsonb)',
    'sessions.ingest_history_native_changed()',
    'sessions.ingest_history_plan_carriers_page(jsonb)',
    'sessions.ingest_history_plan_claim(jsonb)',
    'sessions.ingest_history_plan_heartbeat(jsonb)',
    'sessions.ingest_history_plan_refs_page(jsonb)',
    'sessions.ingest_history_scope_begin(jsonb)',
    'sessions.ingest_history_scope_build_keys(jsonb)',
    'sessions.ingest_history_scope_capture(jsonb)',
    'sessions.ingest_history_scope_install(jsonb)',
    'sessions.ingest_history_scope_pointer_page(uuid,integer,integer)',
    'sessions.ingest_history_sync_pointer()',
    'sessions.ingest_prepare_buffer_stage(jsonb)',
    'sessions.ingest_publication_abandon(jsonb)',
    'sessions.ingest_publication_candidates(jsonb)',
    'sessions.ingest_publication_claim(jsonb)',
    'sessions.ingest_publication_defer(jsonb)',
    'sessions.ingest_publication_gc_claim(jsonb)',
    'sessions.ingest_publication_gc_finish(jsonb)',
    'sessions.ingest_publication_guard_pointer()',
    'sessions.ingest_publication_heartbeat(jsonb)',
    'sessions.ingest_publication_prepare(jsonb)',
    'sessions.ingest_publication_publish(jsonb)',
    'sessions.ingest_publication_register_objects(jsonb)',
    'sessions.ingest_publication_retire_fence()',
    'sessions.ingest_release_slot(jsonb)',
    'sessions.ingest_reset_uploaded_sessions(jsonb)',
    'sessions.ingest_seed_close_baseline()',
    'sessions.ingest_seed_fence_baseline(jsonb)',
    'sessions.ingest_seed_fences(jsonb)',
    'sessions.ingest_seed_proven_records(jsonb)',
    'sessions.ingest_seed_records(jsonb)',
    'sessions.ingest_seed_scope_capture(jsonb)',
    'sessions.ingest_seed_scope_page(jsonb)',
    'sessions.ingest_seed_source_changed()',
    'sessions.ingest_set_session_uploads(jsonb)',
    'sessions.ingest_source_control_begin(jsonb)',
    'sessions.ingest_source_control_complete(jsonb)',
    'sessions.list_session_directory(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer)',
    'sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer,text)',
    'sessions.pipeline_events_maintain(interval,integer)',
    'sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid)',
    'sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)',
    'sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)',
    'sessions.queue_session_brief_document(jsonb)',
    'sessions.repo_visibility_ids(text,uuid)',
    'sessions.request_session_analysis_cache(text,text,text)',
    'sessions.retry_session_brief_documents(uuid,text)',
    'sessions.retry_session_large_read(text,text,text,text,uuid,text)',
    'sessions.route_session_large_read(text,text,text,text)',
    'sessions.search_session_directory(text,text,text,text,text,boolean,text,text,text,text,text,timestamp with time zone,timestamp with time zone,text,timestamp with time zone,text,text,text,integer)',
    'sessions.session_analysis_cache_demand(text,text,text,text,boolean)',
    'sessions.session_brief(text,text,text)',
    'sessions.session_brief_document_authority(text,text[],text[])',
    'sessions.session_brief_document_changed()',
    'sessions.session_briefs_forget()',
    'sessions.session_directory_activity(text,timestamp with time zone,timestamp with time zone)',
    'sessions.session_directory_analysis_cache(text,text,text)',
    'sessions.session_directory_companions_follow()',
    'sessions.session_directory_desktop_catalog(text,text,text)',
    'sessions.session_directory_event_activity(text,timestamp with time zone,timestamp with time zone)',
    'sessions.session_directory_forget_catalog()',
    'sessions.session_directory_history_summary(text,text,text,text)',
    'sessions.session_directory_pointers(text,text,text)',
    'sessions.session_directory_readiness()',
    'sessions.session_directory_record_fences()',
    'sessions.session_directory_replay_pages(text,text,text)',
    'sessions.session_history_capture(jsonb)',
    'sessions.session_history_large_job(jsonb)',
    'sessions.session_history_mask_page(jsonb)',
    'sessions.session_history_register_artifact(jsonb)',
    'sessions.session_history_register_object(jsonb)',
    'sessions.session_history_replay_descriptor(jsonb)',
    'sessions.session_history_replay_publish(jsonb)',
    'sessions.session_history_source_lock(jsonb)',
    'sessions.session_history_state_publish(jsonb)',
    'sessions.session_history_summary_get(jsonb)',
    'sessions.session_history_summary_publish(jsonb)',
    'sessions.session_publication_is_managed(text,text,text,text,text)',
    'sessions.session_publication_lock(jsonb)',
    'sessions.session_publication_probe(jsonb)',
    'sessions.session_publication_register_object(jsonb)',
    'sessions.session_replay_directory_changed()',
    'sessions.session_replay_source_snapshot(text,text,text,text)',
    'sessions.session_replay_sources_changed()',
    'sessions.set_session_brief_documents_live(boolean)',
    'sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamp with time zone,text)'
  ]::text[];
  v_runtime_fns text[] := ARRAY[
    'sessions.ack_session_brief_documents(uuid,jsonb)',
    'sessions.applied_repo_evidence(jsonb,jsonb)',
    'sessions.backfill_session_brief_documents(integer)',
    'sessions.canonical_remote(text)',
    'sessions.capture_session_replay_pages(text,text,text,text)',
    'sessions.claim_session_analysis_cache()',
    'sessions.claim_session_brief_documents(uuid,integer)',
    'sessions.claim_session_large_read(uuid,integer)',
    'sessions.clean_root_commit(text)',
    'sessions.discover_session_large_reads()',
    'sessions.enqueue_session_analysis_cache(text,text,text,text)',
    'sessions.enqueue_session_large_read(text,text,text,text)',
    'sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text)',
    'sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid)',
    'sessions.heartbeat_session_large_read(text,text,text,text,uuid)',
    'sessions.jwt_org_id()',
    'sessions.jwt_workspace_ids()',
    'sessions.legacy_remote(text)',
    'sessions.lock_repo_catalog(text)',
    'sessions.member_of(text,text[])',
    'sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid)',
    'sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)',
    'sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)',
    'sessions.repo_backfill_report()',
    'sessions.repo_backfill_step(integer,text)',
    'sessions.repo_group(text,uuid)',
    'sessions.repo_live(text,uuid)',
    'sessions.repo_visibility_ids(text,uuid)',
    'sessions.repos_maintain(integer)',
    'sessions.resolve_repo(text,text,text,timestamp with time zone)',
    'sessions.retry_session_brief_documents(uuid,text)',
    'sessions.retry_session_large_read(text,text,text,text,uuid,text)',
    'sessions.route_session_large_read(text,text,text,text)',
    'sessions.session_directory_project_text(text,text,text)',
    'sessions.session_directory_search_text(text,text,text,text,text,text,text,text)',
    'sessions.session_index_batch(jsonb)',
    'sessions.session_index_scrub_sticky()',
    'sessions.session_publication_is_managed(text,text,text,text,text)',
    'sessions.session_publication_lock(jsonb)',
    'sessions.session_publication_probe(jsonb)',
    'sessions.session_publication_register_object(jsonb)',
    'sessions.set_session_state(text,text,text,text,text,text,timestamp with time zone)',
    'sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamp with time zone,text)',
    'sessions.upsert_machine(text,text,text,text,text,text,timestamp with time zone)',
    'sessions.upsert_session_index(jsonb)'
  ]::text[];
  v_runtime text := '__RUNTIME_ROLE__';
  v_cmd record;
  v_fn record;
  v_targets oid[] := '{}';
  v_ctors oid[];
  v_all boolean := false;
  v_defaults boolean := false;
  v_bad text[] := '{}';
  v_sig text;
  v_allowed text[];
  v_entries text[];
BEGIN
  IF v_ns IS NULL THEN
    RETURN;
  END IF;
  -- By catalog oid, not by name: the issuer needs no privilege on schema hoopdb_boundary.
  SELECT p.proowner INTO v_owner FROM pg_catalog.pg_event_trigger e
    JOIN pg_catalog.pg_proc p ON p.oid = e.evtfoid
   WHERE e.evtname = 'hoopdb_sessions_function_boundary';
  IF v_owner IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'sessions function boundary: trigger definition not found';
  END IF;
  FOR v_cmd IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands() LOOP
    IF v_cmd.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass THEN
      IF (SELECT p.pronamespace FROM pg_catalog.pg_proc p WHERE p.oid = v_cmd.objid) = v_ns THEN
        v_targets := v_targets || v_cmd.objid;
        IF v_cmd.command_tag IN ('CREATE FUNCTION', 'CREATE PROCEDURE', 'CREATE AGGREGATE') THEN
          EXECUTE pg_catalog.format('REVOKE ALL ON ROUTINE %s FROM PUBLIC', v_cmd.objid::pg_catalog.regprocedure);
        END IF;
      END IF;
    ELSIF v_cmd.classid = 'pg_catalog.pg_type'::pg_catalog.regclass THEN
      -- CREATE TYPE ... AS RANGE creates constructor functions (range and multirange)
      -- with the built-in PUBLIC EXECUTE, owned by the bootstrap superuser: check them
      -- (the owner check then aborts the command unless the issuer could revoke).
      IF (SELECT t.typnamespace FROM pg_catalog.pg_type t WHERE t.oid = v_cmd.objid) = v_ns THEN
        -- One REVOKE for all of them: it fires this trigger again, which must not see a
        -- constructor still PUBLIC-executable.
        SELECT pg_catalog.array_agg(p.oid) INTO v_ctors FROM pg_catalog.pg_proc p
         WHERE p.pronamespace = v_ns AND p.proacl IS NULL
           AND (p.prorettype = v_cmd.objid
                OR p.prorettype IN (SELECT r.rngmultitypid FROM pg_catalog.pg_range r WHERE r.rngtypid = v_cmd.objid));
        IF v_ctors IS NOT NULL THEN
          v_targets := v_targets || v_ctors;
          EXECUTE pg_catalog.format('REVOKE ALL ON ROUTINE %s FROM PUBLIC',
            (SELECT pg_catalog.string_agg(o::pg_catalog.regprocedure::text, ', ') FROM pg_catalog.unnest(v_ctors) o));
        END IF;
      END IF;
    ELSIF v_cmd.command_tag IN ('GRANT', 'REVOKE') THEN
      IF v_cmd.object_type IN ('FUNCTION', 'PROCEDURE', 'ROUTINE') THEN
        v_all := true;
      END IF;
    ELSIF v_cmd.command_tag = 'ALTER DEFAULT PRIVILEGES' THEN
      v_defaults := true;
    ELSIF v_cmd.command_tag IN ('CREATE EXTENSION', 'ALTER EXTENSION', 'ALTER SCHEMA') THEN
      v_all := true;
    END IF;
  END LOOP;
  FOR v_fn IN
    SELECT p.oid, p.proowner, p.prosecdef, p.proacl FROM pg_catalog.pg_proc p
     WHERE p.pronamespace = v_ns AND (v_all OR p.oid = ANY (v_targets))
     ORDER BY p.oid
  LOOP
    v_sig := v_fn.oid::pg_catalog.regprocedure::text;
    IF v_fn.proowner <> v_owner THEN
      v_bad := v_bad || pg_catalog.format('%s is owned by %s, not the migration owner', v_sig, v_fn.proowner::pg_catalog.regrole);
    END IF;
    v_allowed := coalesce(
      (SELECT pg_catalog.array_agg(pg_catalog.replace(e, '<owner>', v_fn.proowner::pg_catalog.regrole::text))
         FROM pg_catalog.jsonb_array_elements_text(v_manifest -> v_sig) e),
      ARRAY[v_fn.proowner::pg_catalog.regrole::text || ':EXECUTE']);
    IF v_fn.proacl IS NULL THEN
      v_entries := ARRAY['PUBLIC:EXECUTE'];
    ELSE
      SELECT pg_catalog.array_agg(
               CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::pg_catalog.regrole::text END
               || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END)
        INTO v_entries FROM pg_catalog.aclexplode(v_fn.proacl) a;
    END IF;
    IF NOT (coalesce(v_entries, '{}') <@ v_allowed) AND v_sig = ANY (v_runtime_fns) THEN
      v_allowed := v_allowed || (pg_catalog.quote_ident(v_runtime) || ':EXECUTE');
    END IF;
    IF NOT (coalesce(v_entries, '{}') <@ v_allowed) THEN
      v_bad := v_bad || pg_catalog.format('%s has EXECUTE outside the manifest: %s', v_sig, pg_catalog.array_to_string(v_entries, ','));
    END IF;
    IF v_fn.prosecdef AND NOT (v_sig = ANY (v_secdef)) THEN
      v_bad := v_bad || pg_catalog.format('%s is SECURITY DEFINER outside the reviewed list', v_sig);
    END IF;
  END LOOP;
  IF v_defaults OR v_all THEN
    FOR v_fn IN
      SELECT d.defaclrole, a.grantee FROM pg_catalog.pg_default_acl d, pg_catalog.aclexplode(d.defaclacl) a
       WHERE d.defaclnamespace = v_ns AND d.defaclobjtype = 'f' AND a.grantee <> d.defaclrole
    LOOP
      v_bad := v_bad || pg_catalog.format('a sessions-scoped function default of %s grants %s',
        v_fn.defaclrole::pg_catalog.regrole, CASE WHEN v_fn.grantee = 0 THEN 'PUBLIC' ELSE v_fn.grantee::pg_catalog.regrole::text END);
    END LOOP;
  END IF;
  IF pg_catalog.cardinality(v_bad) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'sessions function boundary: ' || pg_catalog.array_to_string(v_bad, '; ');
  END IF;
END
$boundary$;
BEGIN
  EXECUTE pg_catalog.format(
    'CREATE OR REPLACE FUNCTION hoopdb_boundary.sessions_function_boundary() RETURNS event_trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS %L',
    pg_catalog.replace(v_body, '__RUNTIME_ROLE__', v_role));
END
$install_fn$;
REVOKE ALL ON FUNCTION hoopdb_boundary.sessions_function_boundary() FROM PUBLIC;
DO $install$
DECLARE
  v_tags text[] := ARRAY[
    'ALTER AGGREGATE',
    'ALTER DEFAULT PRIVILEGES',
    'ALTER EXTENSION',
    'ALTER FUNCTION',
    'ALTER PROCEDURE',
    'ALTER ROUTINE',
    'ALTER SCHEMA',
    'CREATE AGGREGATE',
    'CREATE EXTENSION',
    'CREATE FUNCTION',
    'CREATE PROCEDURE',
    'CREATE TYPE',
    'GRANT',
    'REVOKE'
  ]::text[];
  v_row record;
BEGIN
  SELECT e.evtevent, e.evttags, e.evtenabled, e.evtfoid INTO v_row
    FROM pg_event_trigger e WHERE e.evtname = 'hoopdb_sessions_function_boundary';
  IF FOUND THEN
    IF v_row.evtevent <> 'ddl_command_end'
       OR (SELECT array_agg(t ORDER BY t) FROM unnest(v_row.evttags) t) IS DISTINCT FROM (SELECT array_agg(t ORDER BY t) FROM unnest(v_tags) t)
       OR v_row.evtenabled <> 'A'
       OR v_row.evtfoid <> 'hoopdb_boundary.sessions_function_boundary()'::regprocedure THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'sessions function boundary bootstrap: an event trigger named hoopdb_sessions_function_boundary exists with another definition; an operator must decide';
    END IF;
    RETURN;
  END IF;
  BEGIN
    EXECUTE pg_catalog.format(
      'CREATE EVENT TRIGGER hoopdb_sessions_function_boundary ON ddl_command_end WHEN TAG IN (%s) EXECUTE FUNCTION hoopdb_boundary.sessions_function_boundary()',
      (SELECT string_agg(pg_catalog.quote_literal(t), ', ') FROM unnest(v_tags) t));
  EXCEPTION WHEN insufficient_privilege THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'sessions function boundary bootstrap: CREATE EVENT TRIGGER was refused for ' || current_user
        || ' (needs Supabase supautils privileged role, or a superuser); nothing was created';
  END;
  -- ENABLE ALWAYS: an 'O' (origin) trigger is skipped in sessions with
  -- session_replication_role = replica (as dump/restore tooling sets); 'A' fires there too.
  EXECUTE 'ALTER EVENT TRIGGER hoopdb_sessions_function_boundary ENABLE ALWAYS';
END
$install$;
COMMIT;
