-- Preserve source RelayHistory identifiers alongside the object-layout-safe pointer key.
-- The physical session_id/machine_id remain the session_index primary key and S3 lookup
-- scope. Directory rows, memberships, fences and the claimed pointer read surface expose
-- the source identifiers. Code only; applying this migration requires a separate operator
-- approval. Down path:
-- supabase/rollbacks/20261102010000_session_directory_canonical_ids.down.sql.

ALTER TABLE sessions.session_index
  ADD COLUMN canonical_session_id text,
  ADD COLUMN canonical_machine_id text,
  ADD CONSTRAINT session_index_canonical_session_id_valid CHECK (
    canonical_session_id IS NULL OR
    (char_length(canonical_session_id) BETWEEN 1 AND 255
     AND canonical_session_id !~ '[[:cntrl:]]')),
  ADD CONSTRAINT session_index_canonical_machine_id_valid CHECK (
    canonical_machine_id IS NULL OR
    (char_length(canonical_machine_id) BETWEEN 1 AND 255
     AND canonical_machine_id !~ '[[:cntrl:]]'));

CREATE INDEX session_index_canonical_live
  ON sessions.session_index
    (org_id, workspace_id, harness, canonical_session_id, canonical_machine_id)
  WHERE deleted_at IS NULL AND canonical_session_id IS NOT NULL;

COMMENT ON COLUMN sessions.session_index.canonical_session_id IS
  'Source RelayHistory session id before object-layout encoding; NULL means session_id.';
COMMENT ON COLUMN sessions.session_index.canonical_machine_id IS
  'Source RelayHistory machine id before object-layout encoding; NULL means machine_id.';

CREATE OR REPLACE FUNCTION sessions.session_directory_record_fences()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fences$
BEGIN
  INSERT INTO sessions.session_directory_resurrection_fences AS f
    (org_id, workspace_id, harness, session_id, machine_id, revision)
  SELECT p.org_id, p.workspace_id, p.harness, NEW.session_id,
         coalesce(p.canonical_machine_id, p.machine_id), max(p.revision)
    FROM sessions.session_index AS p
   WHERE p.org_id = NEW.org_id AND p.workspace_id = NEW.workspace_id
     AND p.harness = NEW.harness
     AND (coalesce(p.canonical_session_id, p.session_id) = NEW.session_id
          OR p.session_id = NEW.session_id)
   GROUP BY p.org_id, p.workspace_id, p.harness,
            coalesce(p.canonical_machine_id, p.machine_id)
  ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id)
  DO UPDATE SET revision = greatest(f.revision, excluded.revision), fenced_at = now();
  RETURN NULL;
END
$fences$;

CREATE OR REPLACE FUNCTION sessions.session_directory_pointers(
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
    SELECT p.org_id, p.workspace_id, p.harness,
           coalesce(p.canonical_session_id, p.session_id),
           coalesce(p.canonical_machine_id, p.machine_id), p.user_id,
           p.storage_provider, p.bucket, p.manifest_key, p.content_sha256,
           p.raw_format, p.revision, p.deleted_at
      FROM sessions.session_index AS p
     WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
       AND p.harness = p_harness
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id
            OR p.session_id = p_session_id)
       AND p.deleted_at IS NULL
     ORDER BY coalesce(p.canonical_machine_id, p.machine_id)
     LIMIT 64;
END
$pointers$;

REVOKE ALL ON FUNCTION sessions.session_directory_record_fences()
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_pointers(text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_pointers(text, text, text)
  TO session_directory_search_worker;
