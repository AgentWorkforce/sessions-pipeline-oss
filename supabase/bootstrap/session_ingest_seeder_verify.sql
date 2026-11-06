-- Read-only phase2: the offline S3 seeder is NOLOGIN and can invoke only the
-- generated exact seed RPC list. No table/column/sequence rights, including PUBLIC.
DO $verify$
DECLARE r pg_roles%ROWTYPE; m record;v record;v_mode text:=coalesce(current_setting('hoopdb.db_mode',true),'');
 v_allowed text[] := ARRAY[
 'sessions.ingest_seed_fence_baseline(jsonb)',
 'sessions.ingest_seed_proven_records(jsonb)',
 'sessions.ingest_seed_scope_capture(jsonb)',
 'sessions.ingest_seed_scope_page(jsonb)'
 ]::text[];
 v_oids oid[];
 v_table text:='SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'||
  CASE WHEN current_setting('server_version_num')::int>=170000 THEN ',MAINTAIN' ELSE '' END;
BEGIN
 SELECT array_agg(to_regprocedure(fn)::oid) INTO v_oids FROM unnest(v_allowed) fn;
 IF cardinality(v_oids)<>cardinality(v_allowed) OR array_position(v_oids,NULL) IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder RPC missing';END IF;
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_session_ingest_seeder';
 IF NOT FOUND OR r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder role attributes differ';END IF;
 IF EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) OR
    EXISTS(SELECT 1 FROM pg_db_role_setting WHERE setrole=r.oid)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder role membership or settings differ';END IF;
 IF current_setting('server_version_num')::int>=160000 THEN
  FOR m IN EXECUTE 'SELECT u.rolname,u.rolinherit,u.rolcanlogin,u.rolsuper,u.rolcreaterole,u.rolcreatedb,u.rolreplication,u.rolbypassrls,g.rolname AS grantor,g.rolsuper AS grantor_super,a.admin_option,a.inherit_option,a.set_option FROM pg_auth_members a JOIN pg_roles u ON u.oid=a.member JOIN pg_roles g ON g.oid=a.grantor WHERE a.roleid=$1' USING r.oid LOOP
   IF v_mode='supabase' AND m.rolname='postgres' AND m.grantor='supabase_admin' AND m.grantor_super AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option THEN NULL;
   ELSIF m.rolname='relayhistory_owner' AND NOT(m.rolsuper OR m.rolcreaterole OR m.rolcreatedb OR m.rolreplication OR m.rolbypassrls) AND m.rolcanlogin AND NOT m.rolinherit AND NOT m.admin_option AND NOT m.inherit_option AND m.set_option THEN NULL;
   ELSE RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder has an unexpected member';END IF;
  END LOOP;
 ELSE
  FOR m IN SELECT u.rolname,u.rolinherit,u.rolcanlogin,u.rolsuper,u.rolcreaterole,u.rolcreatedb,u.rolreplication,u.rolbypassrls,a.admin_option FROM pg_auth_members a JOIN pg_roles u ON u.oid=a.member WHERE a.roleid=r.oid LOOP
   IF m.rolname<>'relayhistory_owner' OR m.rolsuper OR m.rolcreaterole OR m.rolcreatedb OR m.rolreplication OR m.rolbypassrls OR NOT m.rolcanlogin OR m.rolinherit OR m.admin_option
   THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder has an unexpected member';END IF;
  END LOOP;
 END IF;
 IF NOT has_schema_privilege(r.oid,'sessions','USAGE') OR has_schema_privilege(r.oid,'sessions','CREATE')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder schema privileges differ';END IF;
 FOR v IN SELECT c.oid,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' LOOP
  IF v.relkind IN('r','p','v','m','f') AND
   (has_table_privilege(r.oid,v.oid,v_table) OR has_any_column_privilege(r.oid,v.oid,'SELECT,INSERT,UPDATE,REFERENCES'))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder has effective table/column privilege';END IF;
  IF v.relkind='S' AND has_sequence_privilege(r.oid,v.oid,'USAGE,SELECT,UPDATE')
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder has effective sequence privilege';END IF;
 END LOOP;
 FOR v IN SELECT p.oid,p.oid::regprocedure::text AS fn,p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' LOOP
  IF has_function_privilege(r.oid,v.oid,'EXECUTE') IS DISTINCT FROM (v.oid=ANY(v_oids))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder function privileges differ';END IF;
  IF v.oid=ANY(v_oids) AND NOT v.prosecdef
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder RPC is not SECURITY DEFINER';END IF;
 END LOOP;
 IF (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.oid=ANY(v_oids))<>cardinality(v_allowed)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='S3 seeder RPC missing';END IF;
END $verify$;
