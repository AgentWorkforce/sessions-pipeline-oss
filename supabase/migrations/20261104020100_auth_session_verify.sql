-- Appended as 20261104020100 to preserve existing upstream ledger prefixes.
-- A second verifier for relayhistory bearer tokens (`rth_at_*` / `rth_st_*`):
-- lets another service (the 96Bulls session read API) check a token against
-- sessions.auth_sessions over PostgREST without Neon and without the
-- service-role key. Code only: applying it to a hosted project needs Khaliq's go.
--
-- Vendored from relayhistory-cloud #245 (supabase/migrations/20261101000100_auth_session_verify.sql
-- at fd9d9027d3794af2493241d3e817fb3ba9977b32). One upstream block is adapted to this
-- repo's contract: the upstream file created session_token_verifier inline; a CNPG /
-- Cloud SQL migration owner has no CREATEROLE, so here the role is provisioned by
-- supabase/bootstrap/session_token_verifier.sql before migrate and only verified
-- (42704 absent, 55000 elevated or wrongly membered) — the pattern of
-- 20261102010000_session_ingest_ledger.sql.
--
-- What it adds.
--   session_token_verifier: a NOLOGIN, NOINHERIT role reached only through a
--     PostgREST JWT whose `role` claim names it. USAGE on schema sessions and
--     EXECUTE on sessions.auth_session_verify, nothing else: no table, no other
--     function, no BYPASSRLS. It is a member of no role. Its members may only be
--     the separately approved `authenticator` edge (PostgREST's switch-to, a
--     deployment step without ADMIN OPTION or inherited privileges) and, on Supabase PG16+, the
--     unavoidable `postgres` creator edge (granted by the superuser
--     supabase_admin, ADMIN without INHERIT or SET).
--   sessions.auth_token_verifier_policy: the one JWT issuer and audience the
--     verify RPC accepts, plus a short maximum token lifetime. PostgREST checks
--     the signature, but a project can trust several keys and need not check
--     `aud`, so the RPC pins both (the pattern this repo uses for
--     session_directory_search_worker). No row is seeded: verify refuses every
--     call until an environment adds its own, as a deployment step.
--   sessions.auth_session_verify(hash, now): read-only. Returns the one valid
--     session for an access-token hash, or nothing, with non-secret columns
--     only (no token hashes, no refresh expiry, no analytics identity).
--
-- Validity mirrors resolveAccessToken in relayhistory-cloud
-- packages/relayhistory/src/auth/tokens.ts:
-- the row's revoked_at is NULL and access_token_expires_at is strictly after
-- now. "Now" is the later of p_now and the database clock, so a caller can make
-- the check stricter but can never pass an old timestamp to revive an expired
-- token. The token-prefix check (`rth_at_` or `rth_st_`) happens before hashing,
-- so it stays the caller's job; a refresh token's hash never matches
-- access_token_hash anyway.
--
-- last_used_at stays relayhistory's: this function never writes.
--
-- Not done here, deliberately (deployment steps, relayhistory-cloud docs/auth-backend.md):
--   PG16+: GRANT session_token_verifier TO authenticator
--            WITH INHERIT FALSE, SET TRUE, ADMIN FALSE
--   PG15: GRANT session_token_verifier TO authenticator (member must be NOINHERIT)
--   INSERT INTO sessions.auth_token_verifier_policy (issuer, audience) VALUES (...)

DO $role$
DECLARE
  r pg_roles%ROWTYPE;
  v_row record;
  v_problems text[];
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = 'session_token_verifier';
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = 'role session_token_verifier does not exist; this migration does not create it',
      HINT = 'Run supabase/bootstrap/session_token_verifier.sql as a privileged role first, then rerun.';
  END IF;
  v_problems := '{}';
  IF r.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF r.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF r.rolinherit THEN v_problems := array_append(v_problems, 'INHERIT'::text); END IF;
  IF r.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF r.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF r.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  IF r.rolcanlogin THEN v_problems := array_append(v_problems, 'LOGIN'::text); END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members AS m JOIN pg_roles AS g ON g.oid = m.roleid
                WHERE m.member = r.oid LOOP
    v_problems := array_append(v_problems, format('member of %I', v_row.rolname)::text);
  END LOOP;
  -- Members OF the verifier. Only the separately approved authenticator edge
  -- (never ADMIN or inherited privileges) and, on Supabase PG16+, the unavoidable postgres creator edge.
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS u ON u.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1' USING r.oid LOOP
      IF v_row.rolname = 'postgres' AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- Supabase PG16+ creator edge
      ELSIF v_row.rolname = 'authenticator' AND NOT v_row.admin_option
         AND NOT v_row.inherit_option AND v_row.set_option THEN
        NULL; -- separately approved PostgREST switch-to edge
      ELSE
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT u.rolname, u.rolinherit, m.admin_option FROM pg_auth_members AS m
                   JOIN pg_roles AS u ON u.oid = m.member WHERE m.roleid = r.oid LOOP
      IF NOT (v_row.rolname = 'authenticator' AND NOT v_row.admin_option AND NOT v_row.rolinherit) THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('role session_token_verifier does not match the bootstrap spec: %s', array_to_string(v_problems, ', ')),
      HINT = 'Fix or drop the role and rerun supabase/bootstrap/session_token_verifier.sql. The migration never alters a role.';
  END IF;
END
$role$;

CREATE TABLE IF NOT EXISTS sessions.auth_token_verifier_policy (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  issuer text NOT NULL CHECK (issuer ~ '^https?://'),
  audience text NOT NULL CHECK (audience <> ''),
  max_lifetime_seconds integer NOT NULL DEFAULT 60
    CHECK (max_lifetime_seconds BETWEEN 1 AND 60)
);
ALTER TABLE sessions.auth_token_verifier_policy ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.auth_token_verifier_policy
  FROM PUBLIC, anon, authenticated, service_role, session_token_verifier;

CREATE FUNCTION sessions.auth_session_verify(
  p_access_token_hash text,
  p_now timestamptz
)
RETURNS TABLE (
  id text,
  token_family_id text,
  subject_type text,
  user_id text,
  org_id text,
  workspace_id text,
  scopes jsonb,
  access_token_expires_at timestamptz,
  label text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $verify$
DECLARE
  v_claims jsonb;
  v_policy sessions.auth_token_verifier_policy%ROWTYPE;
  v_now timestamptz := greatest(p_now, clock_timestamp());
BEGIN
  -- The caller itself, whatever EXECUTE grants a later migration adds (several
  -- here run GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO service_role).
  -- Inside SECURITY DEFINER the `role` setting still names the caller's SET ROLE.
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'session_token_verifier' THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'auth verify: caller is not the token verifier role';
  END IF;
  BEGIN
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  EXCEPTION WHEN others THEN
    v_claims := NULL;
  END;
  SELECT * INTO v_policy FROM sessions.auth_token_verifier_policy WHERE singleton;
  IF NOT FOUND
     OR v_claims IS NULL OR jsonb_typeof(v_claims) <> 'object'
     OR (v_claims ->> 'role') IS DISTINCT FROM 'session_token_verifier'
     OR (v_claims ->> 'iss') IS DISTINCT FROM v_policy.issuer
     OR jsonb_typeof(v_claims -> 'aud') IS DISTINCT FROM 'string'
     OR (v_claims ->> 'aud') IS DISTINCT FROM v_policy.audience
     OR jsonb_typeof(v_claims -> 'exp') IS DISTINCT FROM 'number'
     OR jsonb_typeof(v_claims -> 'iat') IS DISTINCT FROM 'number'
     OR (v_claims ->> 'exp')::numeric - (v_claims ->> 'iat')::numeric
          NOT BETWEEN 1 AND v_policy.max_lifetime_seconds
     OR (v_claims ->> 'exp')::numeric <= extract(epoch FROM clock_timestamp())
     -- A far-future iat must not stretch the window; 30s allows for clock skew.
     OR (v_claims ->> 'exp')::numeric - extract(epoch FROM clock_timestamp())
          > v_policy.max_lifetime_seconds + 30 THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'auth verify: verifier claims missing or invalid';
  END IF;

  -- hashToken output: 64 lowercase hex characters. Anything else is unknown.
  IF p_access_token_hash IS NULL OR p_access_token_hash !~ '^[0-9a-f]{64}$' THEN
    RETURN;
  END IF;

  RETURN QUERY
    SELECT a.id, a.token_family_id, a.subject_type, a.user_id, a.org_id,
           a.workspace_id, a.scopes, a.access_token_expires_at, a.label
      FROM sessions.auth_sessions AS a
     WHERE a.access_token_hash = p_access_token_hash
       AND a.revoked_at IS NULL
       AND a.access_token_expires_at > v_now
     LIMIT 1;
END
$verify$;

REVOKE ALL ON FUNCTION sessions.auth_session_verify(text, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.auth_session_verify(text, timestamptz)
  TO session_token_verifier;
-- Remove every other grantee present at creation (ALTER DEFAULT PRIVILEGES or a
-- pre-existing runtime role). A grant issued later is refused by the role guard.
DO $revoke$
DECLARE
  v_grantee text;
BEGIN
  FOR v_grantee IN
    SELECT r.rolname
      FROM pg_proc AS p
      CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
      JOIN pg_roles AS r ON r.oid = a.grantee
     WHERE p.oid = 'sessions.auth_session_verify(text, timestamptz)'::regprocedure
       AND a.privilege_type = 'EXECUTE'
       AND a.grantee <> p.proowner
       AND r.rolname <> 'session_token_verifier'
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION sessions.auth_session_verify(text, timestamptz) FROM %I', v_grantee);
  END LOOP;
END
$revoke$;

-- USAGE on sessions makes every PUBLIC-executable function there callable by
-- the verifier. The earlier migrations' convention is REVOKE ... FROM PUBLIC on
-- all sessions functions; 20261031000000 added a trigger function after the last
-- such sweep (sessions.migration_ingest_audit_append_only, still PUBLIC). Sweep
-- again before granting USAGE. Triggers fire without EXECUTE, so this changes
-- no behaviour; service_role's explicit grants are untouched.
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC;
GRANT USAGE ON SCHEMA sessions TO session_token_verifier;
