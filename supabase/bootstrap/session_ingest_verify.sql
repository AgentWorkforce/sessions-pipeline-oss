-- Read-only phase2: bare NOLOGIN or approved LOGIN timeout profile.
-- Exact ingest RPC OIDs derive from the combined ACL manifest; no effective
-- table/column/sequence rights, including PUBLIC and PG17 MAINTAIN.
DO $verify$
DECLARE r pg_roles%ROWTYPE; m record;v record;v_mode text:=coalesce(current_setting('hoopdb.db_mode',true),'');
 v_allowed text[] := ARRAY[
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
 'sessions.ingest_history_inventory_claim(jsonb)',
 'sessions.ingest_history_inventory_finish(jsonb)',
 'sessions.ingest_history_inventory_heartbeat(jsonb)',
 'sessions.ingest_history_inventory_page(jsonb)',
 'sessions.ingest_history_inventory_refs(jsonb)',
 'sessions.ingest_history_mask_get(jsonb)',
 'sessions.ingest_history_mask_register(jsonb)',
 'sessions.ingest_history_plan_carriers_page(jsonb)',
 'sessions.ingest_history_plan_claim(jsonb)',
 'sessions.ingest_history_plan_heartbeat(jsonb)',
 'sessions.ingest_history_plan_refs_page(jsonb)',
 'sessions.ingest_history_scope_begin(jsonb)',
 'sessions.ingest_history_scope_build_keys(jsonb)',
 'sessions.ingest_history_scope_capture(jsonb)',
 'sessions.ingest_history_scope_install(jsonb)',
 'sessions.ingest_prepare_buffer_stage(jsonb)',
 'sessions.ingest_publication_abandon(jsonb)',
 'sessions.ingest_publication_candidates(jsonb)',
 'sessions.ingest_publication_claim(jsonb)',
 'sessions.ingest_publication_defer(jsonb)',
 'sessions.ingest_publication_gc_claim(jsonb)',
 'sessions.ingest_publication_gc_finish(jsonb)',
 'sessions.ingest_publication_heartbeat(jsonb)',
 'sessions.ingest_publication_prepare(jsonb)',
 'sessions.ingest_publication_publish(jsonb)',
 'sessions.ingest_publication_register_objects(jsonb)',
 'sessions.ingest_release_slot(jsonb)',
 'sessions.ingest_reset_uploaded_sessions(jsonb)',
 'sessions.ingest_set_session_uploads(jsonb)',
 'sessions.ingest_source_control_begin(jsonb)',
 'sessions.ingest_source_control_complete(jsonb)',
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
 'sessions.session_history_summary_publish(jsonb)'
 ]::text[];
 v_oids oid[];
 v_table text:='SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'||
  CASE WHEN current_setting('server_version_num')::int>=170000 THEN ',MAINTAIN' ELSE '' END;
BEGIN
 SELECT array_agg(to_regprocedure(fn)::oid) INTO v_oids FROM unnest(v_allowed) fn;
 IF cardinality(v_oids)<>cardinality(v_allowed) OR array_position(v_oids,NULL) IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest RPC missing';END IF;
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_session_ingest';
 IF NOT FOUND OR r.rolinherit OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest role attributes differ';END IF;
 IF EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) OR
    EXISTS(SELECT 1 FROM pg_db_role_setting WHERE setrole=r.oid AND setdatabase<>0)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest role membership or settings differ';END IF;
 IF r.rolcanlogin THEN
  IF coalesce((SELECT array_agg(x ORDER BY x) FROM unnest(r.rolconfig)x),'{}') IS DISTINCT FROM
    ARRAY['idle_in_transaction_session_timeout=10s','lock_timeout=2s','statement_timeout=5s']::text[]
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest LOGIN timeout profile differs';END IF;
 ELSIF coalesce(cardinality(r.rolconfig),0)<>0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest bare-role settings differ';
 END IF;
 IF current_setting('server_version_num')::int>=160000 THEN
  FOR m IN EXECUTE 'SELECT u.rolname,u.rolinherit,u.rolcanlogin,u.rolsuper,u.rolcreaterole,u.rolcreatedb,u.rolreplication,u.rolbypassrls,g.rolname AS grantor,g.rolsuper AS grantor_super,a.admin_option,a.inherit_option,a.set_option FROM pg_auth_members a JOIN pg_roles u ON u.oid=a.member JOIN pg_roles g ON g.oid=a.grantor WHERE a.roleid=$1' USING r.oid LOOP
   IF v_mode='supabase' AND m.rolname='postgres' AND m.grantor='supabase_admin' AND m.grantor_super AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option THEN NULL;
   ELSE RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest has an unexpected member';END IF;
  END LOOP;
 ELSE
  IF EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid=r.oid)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest has an unexpected member';END IF;
 END IF;
 IF NOT has_schema_privilege(r.oid,'sessions','USAGE') OR has_schema_privilege(r.oid,'sessions','CREATE')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest schema privileges differ';END IF;
 FOR v IN SELECT c.oid,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' LOOP
  IF v.relkind IN('r','p','v','m','f') AND
   (has_table_privilege(r.oid,v.oid,v_table) OR has_any_column_privilege(r.oid,v.oid,'SELECT,INSERT,UPDATE,REFERENCES'))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('session ingest: privilege on %s',v.oid::regclass);END IF;
  IF v.relkind='S' AND has_sequence_privilege(r.oid,v.oid,'USAGE,SELECT,UPDATE')
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest has effective sequence privilege';END IF;
 END LOOP;
 FOR v IN SELECT p.oid,p.oid::regprocedure::text AS fn,p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' LOOP
  IF has_function_privilege(r.oid,v.oid,'EXECUTE') IS DISTINCT FROM (v.oid=ANY(v_oids))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('session ingest: unexpected EXECUTE on %s',v.oid::regprocedure);END IF;
  IF v.oid=ANY(v_oids) AND NOT v.prosecdef
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest RPC is not SECURITY DEFINER';END IF;
 END LOOP;
 IF (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.oid=ANY(v_oids))<>cardinality(v_allowed)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest RPC missing';END IF;
END $verify$;
