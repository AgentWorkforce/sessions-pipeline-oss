-- Phase 2: post-migration deploy validator for the session directory search Worker role.
--
-- Phase 1 (session_directory_search_worker.sql) provisions and checks the bare role
-- before migrate. This file runs AFTER ALL migrations, on every deploy, and checks that
-- the role's direct grants are exactly what this repository's migration set approves.
-- 20261030020000's role guard runs only once (the ledger then skips the file), so this
-- is what catches later drift.
--
-- Interface (stable; the deploy-time provisioning in self-hosted-lead's #75 runs this):
--   run as    any role that can read the catalogs: the migration owner (`hoop`, or
--             Supabase `postgres`) is enough. It changes nothing.
--   when      after every migrate, once the full migration set is applied.
--   exit      0 when everything holds; otherwise SQLSTATE 55000 naming every violation.
--   live      it is also the readiness gate: schemaVerdict (src/shared/db.ts) runs this file
--             read-only on every probe and before every worker claim.
--   marker    run alone, it records nothing. Readers become ready only on a PASS record
--             (sessions.__96bulls_role_validation) written in the same transaction as
--             a successful run: by src/hoopdb/migrate.ts, or by
--             session_directory_search_worker_mark_pass.sql for an external verifier.
--   role      the phase 1 spec: NOLOGIN NOINHERIT NOBYPASSRLS, no SUPERUSER,
--             CREATEROLE, CREATEDB or REPLICATION; exactly one setting, role-wide
--             statement_timeout=2s.
--   members   a member of no role. Allowed members are the separately approved
--             authenticator binding and, only with mode `supabase` on PG16+, the exact
--             postgres creator edge granted by superuser supabase_admin with ADMIN true
--             and INHERIT/SET false.
--   grants    direct aclitems naming this role (PUBLIC grants are not the role's own
--             and are ignored) must be exactly:
--               USAGE on schema sessions;
--               EXECUTE on each approved RPC below.
--             None may carry GRANT OPTION. Every approved RPC must exist with exactly
--             this signature. A missing RPC, or a function of the same name with any
--             other signature (redefined), fails; the allowlist never shrinks to match.
--             No direct reference to the role may exist in any database of the cluster
--             (pg_shdepend): no owned objects; no table, column, sequence, function,
--             schema, database, type, language, large object, parameter, foreign server
--             or tablespace privileges; no default privileges; no RLS policies.
--
-- Approved RPCs for this migration set. A migration that adds, removes or re-signs
-- one updates this list in the same change.
--   20261030020000  sessions.search_session_directory(text, text, text, text, text,
--                     boolean, text, text, text, text, text, timestamptz, timestamptz,
--                     text, timestamptz, text, text, text, integer)
--   20261031001000  sessions.session_directory_pointers(text, text, text)
--   20261031001000  sessions.session_directory_readiness()
--   20261101050000  sessions.list_session_directory(text, text[], text[], text, text[],
--                     text, timestamptz, timestamptz, timestamptz, timestamptz, text[],
--                     text[], timestamptz, text, text, integer)
--   20261101050000  sessions.session_directory_activity(text, timestamptz, timestamptz)
--   20261102000000  sessions.session_brief(text, text, text)
--
-- Run it with
--   psql -v ON_ERROR_STOP=1 -X -f supabase/bootstrap/session_directory_search_worker_verify.sql
-- in the one database that holds the migrations. Roles are cluster-wide, and the
-- catalogs of other databases are not visible from here, so any reference to the role
-- from another database of the same cluster fails closed: keep one migrated database
-- per cluster (as the CNPG HoopDb cluster and a Supabase project are). Runbook:
-- runbooks/session-directory-search-worker-bootstrap.md.
DO $verify$
DECLARE
  v_oid oid;
  v_problems text[] := '{}';
  v_row record;
  v_settings text;
  v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
  v_sessions oid := to_regnamespace('sessions')::oid;
  v_approved text[] := ARRAY[
 'sessions.list_session_directory(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer)',
 'sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer,text)',
 'sessions.request_session_analysis_cache(text,text,text)',
 'sessions.search_session_directory(text,text,text,text,text,boolean,text,text,text,text,text,timestamp with time zone,timestamp with time zone,text,timestamp with time zone,text,text,text,integer)',
 'sessions.session_brief(text,text,text)',
 'sessions.session_brief_document_authority(text,text[],text[])',
 'sessions.session_directory_activity(text,timestamp with time zone,timestamp with time zone)',
 'sessions.session_directory_analysis_cache(text,text,text)',
 'sessions.session_directory_desktop_catalog(text,text,text)',
 'sessions.session_directory_event_activity(text,timestamp with time zone,timestamp with time zone)',
 'sessions.session_directory_history_summary(text,text,text,text)',
 'sessions.session_directory_pointers(text,text,text)',
 'sessions.session_directory_readiness()',
 'sessions.session_directory_replay_pages(text,text,text)'
 ];
  v_fns oid[] := '{}';
  v_fn oid;
  v_sig text;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT oid INTO v_oid FROM pg_roles WHERE rolname = 'session_directory_search_worker';
  IF v_oid IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'verify session_directory_search_worker: the role does not exist',
      HINT = 'Run phase 1 (supabase/bootstrap/session_directory_search_worker.sql) and the migrations first.';
  END IF;

  -- The role spec (as phase 1).
  SELECT * INTO v_row FROM pg_roles WHERE oid = v_oid;
  IF v_row.rolcanlogin THEN v_problems := array_append(v_problems, 'LOGIN'::text); END IF;
  IF v_row.rolinherit THEN v_problems := array_append(v_problems, 'INHERIT'::text); END IF;
  IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF v_row.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF v_row.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  IF (SELECT count(*) FROM pg_db_role_setting WHERE setrole = v_oid) <> 1
     OR NOT EXISTS (SELECT 1 FROM pg_db_role_setting
                     WHERE setrole = v_oid AND setdatabase = 0
                       AND setconfig = ARRAY['statement_timeout=2s']::text[]) THEN
    SELECT coalesce(string_agg(
             CASE WHEN s.setdatabase = 0 THEN '' ELSE format('database %s: ', s.setdatabase) END
               || array_to_string(s.setconfig, ', '), '; '), 'none')
      INTO v_settings
      FROM pg_db_role_setting AS s WHERE s.setrole = v_oid;
    v_problems := array_append(v_problems,
      format('role settings must be exactly statement_timeout=2s (found: %s)', v_settings)::text);
  END IF;

  -- Membership: only the approved authenticator binding.
  FOR v_row IN SELECT r.rolname FROM pg_auth_members AS m JOIN pg_roles AS r ON r.oid = m.roleid
                WHERE m.member = v_oid LOOP
    v_problems := array_append(v_problems, format('member of %I', v_row.rolname)::text);
  END LOOP;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT r.rolname, r.rolinherit, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS r ON r.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1 ORDER BY r.rolname, g.rolname' USING v_oid LOOP
      IF v_mode = 'supabase'
         AND v_row.rolname = 'postgres'
         AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL;
      ELSIF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
    FOR v_row IN EXECUTE
      'SELECT r.rolname FROM pg_auth_members AS m JOIN pg_roles AS r ON r.oid = m.member
        WHERE m.roleid = $1 AND m.inherit_option' USING v_oid LOOP
      v_problems := array_append(v_problems, format('granted WITH INHERIT to %I', v_row.rolname)::text);
    END LOOP;
  ELSE
    FOR v_row IN SELECT r.rolname, r.rolinherit, m.admin_option FROM pg_auth_members AS m
                   JOIN pg_roles AS r ON r.oid = m.member
                  WHERE m.roleid = v_oid LOOP
      IF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  END IF;

  -- Approved RPCs: each must exist with exactly its signature, and nothing else may
  -- share their names.
  IF v_sessions IS NULL THEN
    v_problems := array_append(v_problems, 'schema sessions is missing'::text);
  END IF;
  FOREACH v_sig IN ARRAY v_approved LOOP
    v_fn := to_regprocedure(v_sig)::oid;
    IF v_fn IS NULL THEN
      v_problems := array_append(v_problems, format('approved RPC missing: %s', v_sig)::text);
    ELSE
      v_fns := array_append(v_fns, v_fn);
    END IF;
  END LOOP;
  FOR v_row IN SELECT p.oid::regprocedure AS fn FROM pg_proc AS p
                WHERE p.pronamespace = v_sessions
                  AND p.proname IN ('session_brief_document_authority', 'search_session_directory', 'session_directory_pointers',
                                    'session_directory_readiness', 'list_session_directory',
                                    'session_directory_activity', 'session_directory_event_activity', 'list_session_directory_events', 'session_brief')
                  AND NOT (p.oid = ANY (v_fns)) LOOP
    v_problems := array_append(v_problems, format('approved RPC redefined: %s', v_row.fn)::text);
  END LOOP;

  -- Required direct grants.
  IF NOT EXISTS (SELECT 1 FROM pg_namespace AS n, aclexplode(n.nspacl) AS a
                  WHERE n.oid = v_sessions AND a.grantee = v_oid AND a.privilege_type = 'USAGE') THEN
    v_problems := array_append(v_problems, 'missing USAGE on schema sessions'::text);
  END IF;
  FOREACH v_fn IN ARRAY v_fns LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_proc AS p, aclexplode(p.proacl) AS a
                    WHERE p.oid = v_fn AND a.grantee = v_oid AND a.privilege_type = 'EXECUTE') THEN
      v_problems := array_append(v_problems, format('missing EXECUTE on %s', v_fn::regprocedure)::text);
    END IF;
  END LOOP;
  -- No extra privilege or GRANT OPTION on the approved objects.
  IF EXISTS (SELECT 1 FROM pg_namespace AS n, aclexplode(n.nspacl) AS a
              WHERE n.oid = v_sessions AND a.grantee = v_oid
                AND (a.privilege_type <> 'USAGE' OR a.is_grantable)) THEN
    v_problems := array_append(v_problems, 'schema sessions: privileges other than USAGE'::text);
  END IF;
  FOR v_row IN SELECT p.oid::regprocedure AS fn FROM pg_proc AS p, aclexplode(p.proacl) AS a
                WHERE p.oid = ANY (v_fns) AND a.grantee = v_oid
                  AND (a.privilege_type <> 'EXECUTE' OR a.is_grantable) LOOP
    v_problems := array_append(v_problems, format('%s: privileges other than EXECUTE', v_row.fn)::text);
  END LOOP;

  -- Everything else that references the role, in any database: pg_shdepend records every
  -- owner, ACL, policy and initial-privilege reference naming it (never PUBLIC). Only
  -- the ACL entries on schema sessions and the approved RPCs, in this database, may stay.
  FOR v_row IN
    SELECT d.dbid, d.classid, d.objid, d.objsubid, d.deptype, db.datname
      FROM pg_shdepend AS d LEFT JOIN pg_database AS db ON db.oid = d.dbid
     WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = v_oid
       AND NOT (d.deptype = 'a' AND d.dbid = v_db AND d.objsubid = 0
                AND ((d.classid = 'pg_namespace'::regclass AND d.objid = v_sessions)
                     OR (d.classid = 'pg_proc'::regclass AND d.objid = ANY (v_fns))))
  LOOP
    v_problems := array_append(v_problems, format('%s %s',
      CASE v_row.deptype WHEN 'o' THEN 'owns' WHEN 'a' THEN 'has privileges on'
                         WHEN 'r' THEN 'is named in policy' WHEN 'i' THEN 'has initial privileges on'
                         ELSE 'is referenced by' END,
      CASE WHEN v_row.dbid IN (0, v_db) THEN pg_describe_object(v_row.classid, v_row.objid, v_row.objsubid)
           ELSE format('an object in database %I', v_row.datname) END)::text);
  END LOOP;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'verify session_directory_search_worker: the role does not match the approved grants: '
                || array_to_string(v_problems, ', '),
      HINT = 'Nothing was changed. Revoke or restore by hand (an operator decision), then rerun.';
  END IF;
END
$verify$;
