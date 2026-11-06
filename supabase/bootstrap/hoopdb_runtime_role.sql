-- Privileged bootstrap for the dedicated HoopDb runtime role.
--
-- The services connect as this role (HOOPDB_RUNTIME_ROLE). In selfhosted and supabase
-- mode it must be a dedicated, non-owner role; the migrator validates its exact
-- least-privilege allowlist before any write and grants it only USAGE on schema
-- sessions and SELECT on the ledger and readiness marker (src/hoopdb/migrate-guards.ts).
--
--   selfhosted (CNPG, Cloud SQL)  this file, run by the deploy as a superuser
--                                 (CNPG `postgres`), before migrate, every deploy.
--   supabase                      the transactional, mode-bound wrapper
--                                 hoopdb_runtime_role_supabase.sql, run as postgres.
--   local                         not needed: the Docker stack runs owner-as-runtime.
--
-- It creates the role, if absent, with NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
-- NOBYPASSRLS and LOGIN or NOLOGIN as configured, and grants it nothing. It never sets a
-- password: the credential is provisioned separately (secret manager), never on disk. If
-- the role exists it only verifies those attributes, a member of no role, no owned
-- objects, and no role granted to another member except the exact optional workload
-- edge and the unavoidable PG16+ Supabase creator edge (55000 otherwise, nothing
-- changed). A workload edge must have no ADMIN, must preserve INHERIT/SET, and must be
-- granted by the frozen Supabase creator or a catalog-verified superuser. The migrator
-- checks the full object-ACL graph before every write.
--
-- Database ACL (run it connected to the HoopDb database itself):
--   REVOKE TEMPORARY ON DATABASE <this database> FROM PUBLIC, and
--   GRANT CONNECT ON DATABASE <this database> TO <runtime role>.
-- The migrator refuses a runtime role with effective TEMPORARY, and PUBLIC holds
-- TEMPORARY by default, so revoking it from the role alone is not enough.
-- DATABASE-WIDE SIDE EFFECT: every role in this database that relied on PUBLIC for
-- TEMPORARY loses it (the database owner and superusers keep it). No HoopDb service
-- uses temporary objects (docs/evidence/runtime-temp-usage.md). Grant TEMPORARY
-- explicitly to any other role that needs it. The Supabase wrapper does not revoke
-- PUBLIC TEMPORARY: the migrator accepts only its exact, PUBLIC-derived hosted baseline.
--
-- Usage:
--   psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -v runtime_login=true \
--     [-v workload_role=the_login_role] \
--     -d <the HoopDb database> -f supabase/bootstrap/hoopdb_runtime_role.sql
\set ON_ERROR_STOP 1
SELECT set_config('hoopdb.runtime_role', :'runtime_role', false) AS runtime_role,
       set_config('hoopdb.runtime_login', :'runtime_login', false) AS runtime_login;
\if :{?workload_role}
SELECT set_config('hoopdb.runtime_workload_role', :'workload_role', false) AS runtime_workload_role;
\else
SELECT set_config('hoopdb.runtime_workload_role', '', false) AS runtime_workload_role;
\endif
DO $runtime$
DECLARE
  v_role text := current_setting('hoopdb.runtime_role');
  v_workload text := current_setting('hoopdb.runtime_workload_role');
  v_login_text text := lower(current_setting('hoopdb.runtime_login'));
  v_login boolean;
  v_oid oid;
  v_me record;
  v_row record;
  v_problems text[] := '{}';
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
  v_has_creator_edge boolean := false;
BEGIN
  IF v_role !~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$' OR v_role = 'session_directory_search_worker' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('runtime role bootstrap: invalid role name %L', v_role);
  END IF;
  IF v_workload <> '' AND
     (v_workload !~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$'
      OR v_workload IN (v_role, 'session_directory_search_worker')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('runtime role bootstrap: invalid workload role name %L', v_workload);
  END IF;
  IF v_login_text NOT IN ('true', 'false') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'runtime role bootstrap: runtime_login must be true or false';
  END IF;
  v_login := v_login_text = 'true';
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
    SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
    IF NOT v_me.rolsuper
       AND NOT (v_mode = 'supabase' AND current_user = 'postgres'
                AND v_me.rolcreaterole
                AND (v_version < 160000 OR EXISTS (
                  SELECT 1 FROM pg_roles
                   WHERE rolname = 'supabase_admin' AND rolsuper))) THEN
      RAISE EXCEPTION USING ERRCODE = '42501',
        MESSAGE = format('runtime role bootstrap: %I may not create %I', current_user, v_role),
        HINT = 'Run as a superuser, or use the reviewed Supabase wrapper as postgres. The PG16+ path is catalog-bound to superuser supabase_admin.';
    END IF;
    EXECUTE format('CREATE ROLE %I %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
                   v_role, CASE WHEN v_login THEN 'LOGIN' ELSE 'NOLOGIN' END);
  END IF;
  SELECT * INTO v_row FROM pg_roles WHERE rolname = v_role;
  v_oid := v_row.oid;
  IF v_row.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF v_row.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF v_row.rolcanlogin <> v_login THEN
    v_problems := array_append(v_problems, format('LOGIN is %s, configured %s', v_row.rolcanlogin, v_login)::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = v_oid) THEN
    v_problems := array_append(v_problems, 'a member of another role'::text);
  END IF;
  IF v_version >= 160000 THEN
    EXECUTE
      'SELECT EXISTS (
         SELECT 1 FROM pg_auth_members m
         JOIN pg_roles member ON member.oid = m.member
         JOIN pg_roles grantor ON grantor.oid = m.grantor
        WHERE m.roleid = $1
          AND member.rolname = ''postgres''
          AND grantor.rolname = ''supabase_admin'' AND grantor.rolsuper
          AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option)'
      INTO v_has_creator_edge USING v_oid;
    FOR v_row IN EXECUTE
      'SELECT member.rolname AS member_name, grantor.rolname AS grantor_name,
              grantor.rolsuper AS grantor_super, m.admin_option,
              m.inherit_option, m.set_option
         FROM pg_auth_members m
         JOIN pg_roles member ON member.oid = m.member
         JOIN pg_roles grantor ON grantor.oid = m.grantor
        WHERE m.roleid = $1 ORDER BY member.rolname, grantor.rolname'
      USING v_oid LOOP
      IF v_mode = 'supabase'
         AND current_user = 'postgres'
         AND v_row.member_name = 'postgres'
         AND v_row.grantor_name = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL;
      ELSIF v_workload <> ''
         AND v_row.member_name = v_workload
         AND NOT v_row.admin_option AND v_row.inherit_option AND v_row.set_option
         AND (v_row.grantor_super OR
              (v_mode = 'supabase' AND current_user = 'postgres'
               AND v_has_creator_edge AND v_row.grantor_name = 'postgres')) THEN
        NULL;
      ELSE
        v_problems := array_append(v_problems,
          format('granted to %I by %I with ADMIN=%s INHERIT=%s SET=%s',
                 v_row.member_name, v_row.grantor_name, v_row.admin_option,
                 v_row.inherit_option, v_row.set_option)::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN
      SELECT member.rolname AS member_name, grantor.rolname AS grantor_name,
             grantor.rolsuper AS grantor_super, m.admin_option,
             member.rolinherit AS inherit_option, true AS set_option
        FROM pg_auth_members m JOIN pg_roles member ON member.oid = m.member
        JOIN pg_roles grantor ON grantor.oid = m.grantor
       WHERE m.roleid = v_oid
    LOOP
      IF v_workload <> ''
         AND v_row.member_name = v_workload
         AND NOT v_row.admin_option AND v_row.inherit_option
         AND v_row.grantor_super THEN
        NULL;
      ELSE
        v_problems := array_append(v_problems,
          format('granted to %I by %I with ADMIN=%s INHERIT=%s',
                 v_row.member_name, v_row.grantor_name, v_row.admin_option,
                 v_row.inherit_option)::text);
      END IF;
    END LOOP;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_shdepend WHERE refclassid = 'pg_authid'::regclass AND refobjid = v_oid AND deptype = 'o') THEN
    v_problems := array_append(v_problems, 'owns objects'::text);
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('runtime role bootstrap: %I does not match the spec: %s', v_role, array_to_string(v_problems, ', ')),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
END
$runtime$;
DO $db$
BEGIN
  IF coalesce(current_setting('hoopdb.db_mode', true), '') <> 'supabase' THEN
    EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
  END IF;
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), current_setting('hoopdb.runtime_role'));
END
$db$;
