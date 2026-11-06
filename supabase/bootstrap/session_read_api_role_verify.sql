-- Read-only verification of the session read API role (session_read_api_role.sql).
-- Raises, listing every problem, unless the role has exactly the approved shape:
-- attributes, no memberships, exactly the approved column SELECT on session_index and
-- schema USAGE as its effective privileges in schema sessions, no EXECUTE on any
-- sessions function, and the tenant-bound RLS policy on session_index.
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_qual text;
BEGIN
  SELECT * INTO v_role FROM pg_roles WHERE rolname = 'bulls96_read_api';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'session read api role: bulls96_read_api does not exist';
  END IF;
  IF NOT v_role.rolcanlogin THEN v_problems := array_append(v_problems, ('role cannot log in')::text); END IF;
  IF v_role.rolinherit THEN v_problems := array_append(v_problems, ('role has INHERIT')::text); END IF;
  IF v_role.rolbypassrls THEN v_problems := array_append(v_problems, ('role has BYPASSRLS')::text); END IF;
  IF v_role.rolsuper OR v_role.rolcreatedb OR v_role.rolcreaterole OR v_role.rolreplication THEN
    v_problems := array_append(v_problems, ('role has SUPERUSER, CREATEDB, CREATEROLE or REPLICATION')::text);
  END IF;
  IF NOT coalesce(v_role.rolconfig @> ARRAY['statement_timeout=2s'], false) THEN
    v_problems := array_append(v_problems, ('statement_timeout is not 2s')::text);
  END IF;
  IF NOT coalesce(v_role.rolconfig @> ARRAY['default_transaction_read_only=on'], false) THEN
    v_problems := array_append(v_problems, ('default_transaction_read_only is not on')::text);
  END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = v_role.oid LOOP
    v_problems := array_append(v_problems, (format('member of %I', v_row.rolname))::text);
  END LOOP;

  -- Effective table privileges in sessions, however obtained.
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel,
           has_table_privilege(v_role.oid, c.oid, 'SELECT') AS sel,
           has_table_privilege(v_role.oid, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') AS write,
           has_any_column_privilege(v_role.oid, c.oid, 'SELECT') AS col_sel,
           has_any_column_privilege(v_role.oid, c.oid, 'INSERT, UPDATE, REFERENCES') AS col_write
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  LOOP
    IF v_row.write OR v_row.col_write THEN
      v_problems := array_append(v_problems, (format('write privilege on %s', v_row.rel))::text);
    ELSIF v_row.rel = 'sessions.session_index' THEN
      IF v_row.sel THEN v_problems := array_append(v_problems, ('whole-table SELECT on sessions.session_index')::text); END IF;
      IF NOT v_row.col_sel THEN v_problems := array_append(v_problems, ('no column SELECT on sessions.session_index')::text); END IF;
    ELSIF v_row.sel OR v_row.col_sel THEN
      v_problems := array_append(v_problems, (format('SELECT on %s', v_row.rel))::text);
    END IF;
  END LOOP;
  FOR v_row IN
    SELECT a.attname,
           has_column_privilege(v_role.oid, 'sessions.session_index'::regclass, a.attnum, 'SELECT') AS sel
      FROM pg_attribute a
     WHERE a.attrelid = 'sessions.session_index'::regclass AND a.attnum > 0 AND NOT a.attisdropped
  LOOP
    IF v_row.sel <> (v_row.attname IN ('org_id', 'workspace_id', 'harness', 'session_id', 'created_at', 'deleted_at',
                                       'canonical_session_id', 'machine_id', 'canonical_machine_id', 'user_id',
                                       'storage_provider', 'bucket', 'manifest_key', 'content_sha256', 'raw_format', 'revision')) THEN
      v_problems := array_append(v_problems, (format('column %s: SELECT is %s', v_row.attname, CASE WHEN v_row.sel THEN 'granted' ELSE 'missing' END))::text);
    END IF;
  END LOOP;
  -- No callable sessions function: the claim-bound RPCs refuse other roles anyway,
  -- but a definer function without such a check must not be reachable.
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef AND has_function_privilege(v_role.oid, p.oid, 'EXECUTE')
  LOOP
    v_problems := array_append(v_problems, (format('EXECUTE on security definer %s', v_row.fn))::text);
  END LOOP;

  IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'sessions.session_index'::regclass) THEN
    v_problems := array_append(v_problems, ('RLS is off on sessions.session_index')::text);
  END IF;
  SELECT pg_get_expr(p.polqual, p.polrelid) INTO v_qual
    FROM pg_policy p
   WHERE p.polrelid = 'sessions.session_index'::regclass AND p.polname = 'session_index_read_api'
     AND p.polpermissive AND p.polcmd = 'r' AND p.polroles = ARRAY[v_role.oid];
  IF v_qual IS NULL THEN
    v_problems := array_append(v_problems, ('policy session_index_read_api is missing or not FOR SELECT TO bulls96_read_api only')::text);
  ELSIF v_qual NOT LIKE '%current_setting(''bulls96_read.org_id''::text, true)%'
     OR v_qual NOT LIKE '%current_setting(''bulls96_read.workspace_id''::text, true)%' THEN
    v_problems := array_append(v_problems, ('policy session_index_read_api is not bound to the tenant settings')::text);
  END IF;
  FOR v_row IN
    SELECT p.polrelid::regclass::text AS rel, p.polname FROM pg_policy p
     WHERE v_role.oid = ANY(p.polroles) AND p.polname <> 'session_index_read_api'
  LOOP
    v_problems := array_append(v_problems, (format('unapproved policy %s on %s', v_row.polname, v_row.rel))::text);
  END LOOP;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION 'session read api role is not in the approved shape: %', array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'session read api role: PASS';
END
$verify$;
