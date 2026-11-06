-- Cloud SQL function boundary for the HoopDb migration owner.
--
-- Google documents cloudsqladmin as an immutable Cloud SQL system user with database
-- superuser privileges. Customers cannot alter its global defaults. In cloudsql mode
-- the catalog gate therefore treats that exact SUPERUSER as the provider trust root,
-- while every ordinary role able to CREATE in sessions remains covered.
-- https://cloud.google.com/sql/docs/postgres/users#system-users
--
-- Run this as the migration owner, before every migrate. It changes only that owner's
-- global function default in the current database. It refuses before the change unless:
--   * cloudsqladmin exists, is SUPERUSER, and is not the migration owner;
--   * every existing sessions function belongs to the migration owner;
--   * no ordinary role other than the owner can CREATE in sessions; and
--   * no sessions-scoped function default grants another role.
--
-- Idempotent. The migrator independently re-derives the complete catalog evidence.
DO $cloudsql_function_boundary$
DECLARE
  v_me oid := (SELECT oid FROM pg_roles WHERE rolname = current_user);
  v_bad text[] := '{}';
  v_row record;
  v_default text[];
BEGIN
  IF (SELECT rolsuper FROM pg_roles WHERE oid = v_me) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL function boundary: the migration owner must not be SUPERUSER';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles
     WHERE rolname = 'cloudsqladmin' AND rolsuper AND oid <> v_me
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL function boundary: immutable cloudsqladmin SUPERUSER was not proven';
  END IF;

  IF to_regnamespace('sessions') IS NOT NULL THEN
    FOR v_row IN
      SELECT r.rolname
        FROM pg_roles r
       WHERE has_schema_privilege(r.oid, 'sessions', 'CREATE')
         AND r.oid <> v_me
         AND NOT (r.rolname = 'cloudsqladmin' AND r.rolsuper)
       ORDER BY 1
    LOOP
      v_bad := array_append(v_bad, format('%I can CREATE in sessions', v_row.rolname)::text);
    END LOOP;
    IF EXISTS (
      SELECT 1 FROM pg_proc p
       WHERE p.pronamespace = to_regnamespace('sessions')::oid AND p.proowner <> v_me
    ) THEN
      v_bad := array_append(v_bad, 'a sessions function is not owned by the migration owner'::text);
    END IF;
    FOR v_row IN
      SELECT r.rolname, a.grantee, a.privilege_type
        FROM pg_default_acl d
        JOIN pg_roles r ON r.oid = d.defaclrole,
             aclexplode(d.defaclacl) a
       WHERE d.defaclnamespace = to_regnamespace('sessions')::oid
         AND d.defaclobjtype = 'f'
         AND a.grantee <> d.defaclrole
    LOOP
      v_bad := array_append(v_bad, format('%I has a sessions-scoped function default granting %s',
        v_row.rolname, v_row.privilege_type)::text);
    END LOOP;
  END IF;

  -- Before changing it, the owner's global function default may contain only PostgreSQL's
  -- built-in owner/PUBLIC EXECUTE pieces, without grant options.
  IF EXISTS (
    SELECT 1
      FROM pg_default_acl d, aclexplode(d.defaclacl) a
     WHERE d.defaclrole = v_me AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
       AND NOT (a.privilege_type = 'EXECUTE' AND NOT a.is_grantable AND a.grantee IN (0, v_me))
  ) THEN
    v_bad := array_append(v_bad, 'the migration owner has a poisoned global function default'::text);
  END IF;
  IF cardinality(v_bad) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL function boundary refused (nothing changed): ' || array_to_string(v_bad, '; ');
  END IF;

  EXECUTE format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC',
    current_user
  );

  SELECT coalesce(array_agg(entry ORDER BY entry), '{}') INTO v_default
    FROM (
      SELECT CASE WHEN a.grantee = v_me THEN '<self>' ELSE pg_get_userbyid(a.grantee) END
             || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END AS entry
        FROM pg_default_acl d, aclexplode(d.defaclacl) a
       WHERE d.defaclrole = v_me AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
    ) normalized;
  IF v_default <> ARRAY['<self>:EXECUTE']::text[] THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL function boundary: owner default is not exactly {<self>:EXECUTE}';
  END IF;
END
$cloudsql_function_boundary$;
