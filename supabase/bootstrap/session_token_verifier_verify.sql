-- Read-only verification of the auth token verifier role
-- (supabase/bootstrap/session_token_verifier.sql; the 20261104020100 migration).
-- Raises, listing every problem, unless the role has exactly the approved shape:
-- NOLOGIN and no elevated attributes, member of no role, members limited to the
-- separately approved `authenticator` edge (never ADMIN or inherited privileges) and the Supabase PG16+
-- `postgres` creator edge, USAGE on schema sessions, no CREATE, no table or
-- column privilege anywhere in schema sessions, and EXECUTE on exactly
-- sessions.auth_session_verify(text, timestamptz) among sessions functions.
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_fn oid := 'sessions.auth_session_verify(text,timestamptz)'::regprocedure;
  v_version int := current_setting('server_version_num')::int;
  -- PG17 adds MAINTAIN (VACUUM/ANALYZE/REINDEX/REFRESH/CLUSTER/LOCK); it does
  -- not exist before 17, so the effective privilege list is version-guarded.
  v_table_privs text := 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
                        || CASE WHEN current_setting('server_version_num')::int >= 170000
                                THEN ', MAINTAIN' ELSE '' END;
BEGIN
  SELECT * INTO v_role FROM pg_roles WHERE rolname = 'session_token_verifier';
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'session token verifier: session_token_verifier does not exist';
  END IF;
  IF v_role.rolcanlogin THEN v_problems := array_append(v_problems, ('role can log in')::text); END IF;
  IF v_role.rolinherit THEN v_problems := array_append(v_problems, ('role has INHERIT')::text); END IF;
  IF v_role.rolbypassrls THEN v_problems := array_append(v_problems, ('role has BYPASSRLS')::text); END IF;
  IF v_role.rolsuper OR v_role.rolcreatedb OR v_role.rolcreaterole OR v_role.rolreplication THEN
    v_problems := array_append(v_problems, ('role has SUPERUSER, CREATEDB, CREATEROLE or REPLICATION')::text);
  END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = v_role.oid LOOP
    v_problems := array_append(v_problems, (format('member of %I', v_row.rolname))::text);
  END LOOP;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super, m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members m JOIN pg_roles u ON u.oid = m.member JOIN pg_roles g ON g.oid = m.grantor
        WHERE m.roleid = $1' USING v_role.oid LOOP
      IF v_row.rolname = 'postgres' AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- Supabase PG16+ creator edge
      ELSIF v_row.rolname = 'authenticator' AND NOT v_row.admin_option
         AND NOT v_row.inherit_option AND v_row.set_option THEN
        NULL; -- separately approved PostgREST switch-to edge
      ELSE
        v_problems := array_append(v_problems, (format('unexpected member %I', v_row.rolname))::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT u.rolname, u.rolinherit, m.admin_option FROM pg_auth_members m
                   JOIN pg_roles u ON u.oid = m.member WHERE m.roleid = v_role.oid LOOP
      IF NOT (v_row.rolname = 'authenticator' AND NOT v_row.admin_option AND NOT v_row.rolinherit) THEN
        v_problems := array_append(v_problems, (format('unexpected member %I', v_row.rolname))::text);
      END IF;
    END LOOP;
  END IF;

  -- No table, view or column privilege of any kind in sessions, however
  -- obtained: effective rights, PUBLIC-derived included. USAGE on the schema
  -- makes any PUBLIC grant reachable, so effective is the only safe check.
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
       AND (has_table_privilege(v_role.oid, c.oid, v_table_privs)
            OR has_any_column_privilege(v_role.oid, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))
  LOOP
    v_problems := array_append(v_problems, (format('privilege on %s', v_row.rel))::text);
  END LOOP;
  IF NOT has_schema_privilege(v_role.oid, 'sessions', 'USAGE') THEN
    v_problems := array_append(v_problems, ('no USAGE on schema sessions')::text);
  END IF;
  IF has_schema_privilege(v_role.oid, 'sessions', 'CREATE') THEN
    v_problems := array_append(v_problems, ('CREATE on schema sessions')::text);
  END IF;

  -- EXECUTE on exactly the one approved RPC: direct grants via proacl only.
  -- A merely PUBLIC-executable function is not a grant to this role; the
  -- sessions function boundary and live gate own that check. Function
  -- identity is by OID.
  IF NOT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(p.proacl) a
                  WHERE p.oid = v_fn AND a.grantee = v_role.oid
                    AND a.privilege_type = 'EXECUTE' AND NOT a.is_grantable) THEN
    v_problems := array_append(v_problems, (format('missing EXECUTE on %s', v_fn::regprocedure))::text);
  END IF;
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn
      FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace,
           aclexplode(p.proacl) a
     WHERE n.nspname = 'sessions' AND a.grantee = v_role.oid
       AND (p.oid <> v_fn OR a.privilege_type <> 'EXECUTE' OR a.is_grantable)
  LOOP
    v_problems := array_append(v_problems, (format('%s: unapproved direct privilege', v_row.fn))::text);
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE oid = v_fn AND prosecdef) THEN
    v_problems := array_append(v_problems, ('sessions.auth_session_verify(text,timestamptz) is missing or not SECURITY DEFINER')::text);
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'session token verifier is not in the approved shape: ' || array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'session token verifier: PASS';
END
$verify$;
