-- G6 expand phase: claim-bound replacements for the last direct reads RelayHistory #226
-- makes (pointers on session_index, and the directory readiness marker), and the
-- resurrection fences of decision 0002. ADDITIVE ONLY: it revokes nothing, adds no search
-- index (the only new index is the fences table's PRIMARY KEY) and does not change
-- sessions.search_session_directory.
--
-- Expand/contract (docs/session-search-index-contract.md, "Direct surface: expand then
-- contract"): this migration only adds the RPC capabilities. Direct grants are revoked in a
-- later, separate contract migration, after #226's consumer cutover and direct-use proof;
-- it is not part of this change.
--
-- Both functions follow the search RPC's rules: SECURITY DEFINER with a pinned
-- search_path, fixed SQL with bound values, the caller must be the
-- session_directory_search_worker role, the tenant comes only from the signed JWT claims
-- (with the same issuer, audience, lifetime and workspace-bound pins), and EXECUTE goes
-- to that role alone.
--
-- Code only; nothing here is applied by this repository. Down path:
-- supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql.

-- Resurrection fences (decision 0002: a deleted upload or pointer never comes back).
-- When a logical row becomes a tombstone, record every pointer's highest revision at
-- that moment. The projector may resurrect the row only for a live pointer whose revision
-- is strictly above its machine's fence, or a machine with no pointer at tombstone time.
-- Re-sent, stale or un-deleted pointer revisions stay fenced permanently. Fences are
-- history: a later tombstone only raises them. Erasure deletes them with the row.
CREATE TABLE sessions.session_directory_resurrection_fences (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  machine_id text NOT NULL,
  revision bigint NOT NULL CHECK (revision >= 1),
  fenced_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, harness, session_id, machine_id),
  FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.session_directory (org_id, workspace_id, harness, session_id)
    ON DELETE CASCADE
);
ALTER TABLE sessions.session_directory_resurrection_fences ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_resurrection_fences FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_directory_resurrection_fences TO service_role;

CREATE FUNCTION sessions.session_directory_record_fences()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fences$
BEGIN
  INSERT INTO sessions.session_directory_resurrection_fences AS f
    (org_id, workspace_id, harness, session_id, machine_id, revision)
  SELECT p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.revision
    FROM sessions.session_index AS p
   WHERE p.org_id = NEW.org_id AND p.workspace_id = NEW.workspace_id
     AND p.harness = NEW.harness AND p.session_id = NEW.session_id
  ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id)
  DO UPDATE SET revision = greatest(f.revision, excluded.revision), fenced_at = now();
  RETURN NULL;
END
$fences$;

CREATE TRIGGER session_directory_record_fences
  AFTER INSERT OR UPDATE OF deleted_at ON sessions.session_directory
  FOR EACH ROW WHEN (NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_directory_record_fences();
REVOKE ALL ON FUNCTION sessions.session_directory_record_fences() FROM PUBLIC, anon, authenticated, service_role;

-- The Worker's tenant from the signed claims, refusing everything else (42501). With a
-- workspace, it must be one the claims list; without one, the claims must list at least
-- one. Not callable directly: only the SECURITY DEFINER functions below use it.
CREATE FUNCTION sessions.session_directory_worker_scope(p_workspace_id text)
RETURNS TABLE (org_id text, workspace_ids text[])
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_claims jsonb;
  v_policy sessions.session_directory_search_token_policy%ROWTYPE;
  v_org text;
  v_workspace_claim jsonb;
  v_workspaces text[];
BEGIN
  -- Inside SECURITY DEFINER the `role` setting still names the caller's SET ROLE.
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'session_directory_search_worker' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: caller is not the search worker role';
  END IF;
  BEGIN
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  EXCEPTION WHEN others THEN
    v_claims := NULL;
  END;
  IF v_claims IS NULL OR jsonb_typeof(v_claims) <> 'object'
     OR (v_claims ->> 'role') IS DISTINCT FROM 'session_directory_search_worker'
     OR jsonb_typeof(v_claims -> 'app_metadata') IS DISTINCT FROM 'object'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'org_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'workspace_ids') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant claims missing or invalid';
  END IF;
  -- The same token pins as sessions.search_session_directory (20261030020000): one
  -- issuer and audience from sessions.session_directory_search_token_policy, a
  -- lifetime of 1 to max_lifetime_seconds, and exp at most that plus 30s away.
  SELECT * INTO v_policy
    FROM sessions.session_directory_search_token_policy WHERE singleton;
  IF NOT FOUND
     OR (v_claims ->> 'iss') IS DISTINCT FROM v_policy.issuer
     OR jsonb_typeof(v_claims -> 'aud') IS DISTINCT FROM 'string'
     OR (v_claims ->> 'aud') IS DISTINCT FROM v_policy.audience
     OR jsonb_typeof(v_claims -> 'exp') IS DISTINCT FROM 'number'
     OR jsonb_typeof(v_claims -> 'iat') IS DISTINCT FROM 'number'
     OR (v_claims ->> 'exp')::numeric - (v_claims ->> 'iat')::numeric
          NOT BETWEEN 1 AND v_policy.max_lifetime_seconds
     OR (v_claims ->> 'exp')::numeric - extract(epoch FROM clock_timestamp())
          > v_policy.max_lifetime_seconds + 30 THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant claims missing or invalid';
  END IF;
  v_org := v_claims -> 'app_metadata' ->> 'org_id';
  -- The same workspace bound: 1 to 64 unique ids of 1 to 128 non-space characters,
  -- and one bad entry refuses the whole claim.
  v_workspace_claim := v_claims -> 'app_metadata' -> 'workspace_ids';
  IF jsonb_array_length(v_workspace_claim) NOT BETWEEN 1 AND 64 THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant claims missing or invalid';
  END IF;
  SELECT coalesce(array_agg(DISTINCT w.value #>> '{}'), '{}') INTO v_workspaces
    FROM jsonb_array_elements(v_workspace_claim) AS w
   WHERE jsonb_typeof(w.value) = 'string'
     AND (w.value #>> '{}') ~ '^[^[:space:][:cntrl:]]{1,128}$';
  IF v_org = '' OR cardinality(v_workspaces) <> jsonb_array_length(v_workspace_claim)
     OR (p_workspace_id IS NOT NULL AND NOT (p_workspace_id = ANY (v_workspaces))) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant claims missing or invalid';
  END IF;
  org_id := v_org;
  workspace_ids := v_workspaces;
  RETURN NEXT;
END
$scope$;

-- Pointers for one logical session in a claimed workspace: exactly RelayHistory #226's
-- POINTER_COLUMNS, live pointers only, ordered by machine_id, at most 64 rows.
CREATE FUNCTION sessions.session_directory_pointers(
  p_workspace_id text, p_harness text, p_session_id text
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text, machine_id text, user_id text,
  storage_provider text, bucket text, manifest_key text, content_sha256 bytea,
  raw_format text, revision bigint, deleted_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $pointers$
DECLARE
  v_org text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: workspace is required';
  END IF;
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: harness is invalid';
  END IF;
  IF p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: session id is invalid';
  END IF;
  RETURN QUERY
    SELECT p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.user_id,
           p.storage_provider, p.bucket, p.manifest_key, p.content_sha256,
           p.raw_format, p.revision, p.deleted_at
      FROM sessions.session_index AS p
     WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
       AND p.harness = p_harness AND p.session_id = p_session_id
       AND p.deleted_at IS NULL
     ORDER BY p.machine_id
     LIMIT 64;
END
$pointers$;

-- The directory readiness marker (completed_at only), for a valid Worker caller.
CREATE FUNCTION sessions.session_directory_readiness()
RETURNS TABLE (completed_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $readiness$
BEGIN
  PERFORM 1 FROM sessions.session_directory_worker_scope(NULL);
  RETURN QUERY SELECT r.completed_at FROM sessions.session_directory_rollout AS r WHERE r.singleton;
END
$readiness$;

REVOKE ALL ON FUNCTION sessions.session_directory_worker_scope(text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_pointers(text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_readiness()
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_pointers(text, text, text)
  TO session_directory_search_worker;
GRANT EXECUTE ON FUNCTION sessions.session_directory_readiness()
  TO session_directory_search_worker;

-- As in 20261030020000: strip EXECUTE from every grantee present at creation (default
-- privileges, a pre-existing runtime role such as relayhistory_app), whatever the order.
-- A grant added later is still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_directory_worker_scope(text)'::regprocedure,
    'sessions.session_directory_pointers(text, text, text)'::regprocedure,
    'sessions.session_directory_readiness()'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'session_directory_search_worker'
                  AND v_fn <> 'sessions.session_directory_worker_scope(text)'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
