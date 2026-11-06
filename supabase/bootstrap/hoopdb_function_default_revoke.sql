-- Privileged selfhosted bootstrap: no future function in the HoopDb database becomes
-- PUBLIC- (or anyone-else-) executable by default.
--
-- PostgreSQL gives every new function PUBLIC EXECUTE by built-in default, and a
-- per-schema ALTER DEFAULT PRIVILEGES cannot revoke that. This sets, in THIS database, the
-- GLOBAL function default of the migration owner and of every superuser to exactly
-- {<self>:EXECUTE} (no other grantee, no grant option):
--   ALTER DEFAULT PRIVILEGES FOR ROLE <role> REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC
-- Global for those roles: every function they create later in this database, in any
-- schema, starts executable by its owner only. That is safe only because a selfhosted
-- HoopDb database is dedicated to HoopDb. Explicit grants made in migrations (the
-- Worker's RPCs, authenticated's helpers, service_role's grants) are untouched (the ACL
-- manifest test checks it). Never run this on Supabase: there the owner is postgres and
-- the change would reach every schema of the project (an operator decision, #98).
--
-- Fails closed, before any write, when:
--   - a target role's existing global function default already grants anything other
--     than the built-in <self>/PUBLIC EXECUTE (a poisoned default, for example EXECUTE to
--     authenticated, or a grant option): an operator must look at it;
--   - another role (not the owner, not a superuser) can CREATE in sessions without an
--     exact-safe default of its own: revoke its CREATE first;
--   - a schema-scoped function default IN sessions (any role) grants anyone but its own
--     role: it adds to the global default, so remove it first.
-- And after the change it verifies every sessions creator is exactly {<self>:EXECUTE}.
-- The migrator (selfhosted and supabase mode) refuses unless the catalog shows the same
-- (HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke; src/hoopdb/migrate-guards.ts).
--
-- Idempotent. Run as a superuser (CNPG postgres), before migrate:
--   psql -v ON_ERROR_STOP=1 -X -v migration_owner=hoop -d <the HoopDb database> \
--     -f supabase/bootstrap/hoopdb_function_default_revoke.sql
\set ON_ERROR_STOP 1
SELECT set_config('hoopdb.migration_owner', :'migration_owner', false) AS migration_owner;
DO $fn_default$
DECLARE
  v_owner text := current_setting('hoopdb.migration_owner');
  v_owner_oid oid;
  v_row record;
  v_bad text[] := '{}';
  v_sessions boolean := to_regnamespace('sessions') IS NOT NULL;
BEGIN
  SELECT oid INTO v_owner_oid FROM pg_roles WHERE rolname = v_owner;
  IF v_owner_oid IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('function default bootstrap: role %I does not exist', v_owner);
  END IF;
  IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'function default bootstrap: run as a superuser (it sets every superuser''s default too)';
  END IF;

  -- Precheck, before any write.
  -- (a) Targets (owner + superusers): an existing global function default may hold only
  --     the built-in pieces (<self> EXECUTE, PUBLIC EXECUTE), without grant options.
  FOR v_row IN
    SELECT r.rolname, a.grantee, a.privilege_type, a.is_grantable
      FROM pg_roles r
      JOIN pg_default_acl d ON d.defaclrole = r.oid AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
      CROSS JOIN LATERAL aclexplode(d.defaclacl) a
     WHERE (r.oid = v_owner_oid OR r.rolsuper)
       AND NOT (a.privilege_type = 'EXECUTE' AND NOT a.is_grantable AND a.grantee IN (0, r.oid))
  LOOP
    v_bad := array_append(v_bad, format('%I has a poisoned function default (%s %s%s)',
      v_row.rolname, CASE WHEN v_row.grantee = 0 THEN 'PUBLIC' ELSE (SELECT rolname FROM pg_roles WHERE oid = v_row.grantee) END,
      v_row.privilege_type, CASE WHEN v_row.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END)::text);
  END LOOP;
  -- (b) Any other role that can CREATE in sessions must already be exact-safe.
  IF v_sessions THEN
    FOR v_row IN
      SELECT r.rolname FROM pg_roles r
       WHERE has_schema_privilege(r.oid, 'sessions', 'CREATE') AND NOT r.rolsuper AND r.oid <> v_owner_oid
         AND coalesce((SELECT array_agg(a.grantee::text || ':' || a.privilege_type || ':' || a.is_grantable::text ORDER BY 1)
                         FROM pg_default_acl d, aclexplode(d.defaclacl) a
                        WHERE d.defaclrole = r.oid AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'), '{}')
             <> ARRAY[r.oid::text || ':EXECUTE:false']
    LOOP
      v_bad := array_append(v_bad, format('%I can CREATE in sessions without an exact-safe function default', v_row.rolname)::text);
    END LOOP;
  END IF;
  -- (c) No schema-scoped function default in sessions may grant anyone but its own role.
  IF v_sessions THEN
    FOR v_row IN
      SELECT r.rolname, a.grantee, a.privilege_type
        FROM pg_default_acl d JOIN pg_roles r ON r.oid = d.defaclrole, aclexplode(d.defaclacl) a
       WHERE d.defaclnamespace = to_regnamespace('sessions')::oid AND d.defaclobjtype = 'f' AND a.grantee <> d.defaclrole
    LOOP
      v_bad := array_append(v_bad, format('%I has a sessions-scoped function default granting %s %s',
        v_row.rolname, CASE WHEN v_row.grantee = 0 THEN 'PUBLIC' ELSE (SELECT rolname FROM pg_roles WHERE oid = v_row.grantee) END,
        v_row.privilege_type)::text);
    END LOOP;
  END IF;
  IF cardinality(v_bad) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'function default bootstrap refused (nothing changed): ' || array_to_string(v_bad, '; ');
  END IF;

  -- The change: owner and every superuser.
  FOR v_row IN SELECT rolname FROM pg_roles WHERE oid = v_owner_oid OR rolsuper ORDER BY 1 LOOP
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC', v_row.rolname);
  END LOOP;

  -- Postcondition: owner, superusers and every sessions creator are exactly {<self>:EXECUTE}.
  FOR v_row IN
    SELECT r.oid, r.rolname FROM pg_roles r
     WHERE r.oid = v_owner_oid OR r.rolsuper OR (v_sessions AND has_schema_privilege(r.oid, 'sessions', 'CREATE'))
  LOOP
    IF coalesce((SELECT array_agg(a.grantee::text || ':' || a.privilege_type || ':' || a.is_grantable::text ORDER BY 1)
                   FROM pg_default_acl d, aclexplode(d.defaclacl) a
                  WHERE d.defaclrole = v_row.oid AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'), '{}')
       <> ARRAY[v_row.oid::text || ':EXECUTE:false'] THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('function default bootstrap: %I is not exactly {<self>:EXECUTE} after the change', v_row.rolname);
    END IF;
  END LOOP;
END
$fn_default$;
