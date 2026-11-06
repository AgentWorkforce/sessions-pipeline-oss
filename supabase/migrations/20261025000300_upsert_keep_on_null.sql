-- Keep-on-null for upsert_session_index (stacked on 20261025000200).
--
-- The ingest worker (WP4, packages/session-ingest) builds a pointer row from the
-- manifest alone. Fields only the transform knows (title, model, git branch and
-- commit, the totals and cost) and fields the tenant resolver may not know
-- (project_id), or a manifest may lack (manifest_etag), arrive NULL. Overwriting
-- would erase what an earlier revision or the transform recorded, so on an applied
-- revision these keep the stored value when the incoming one is NULL:
--
--   title, model, git_repo, git_branch, git_commit, project_id, manifest_etag,
--   event_count, input_tokens, output_tokens, cost_usd_micros
--
-- An explicit value, including 0 for a total, still replaces it. The cost is that a
-- later revision cannot clear these fields back to NULL.
--
-- Everything else keeps overwrite semantics, because the manifest is authoritative
-- for it each revision: pointers and sizes, revision, trace_id, user_id,
-- last_activity_at, normalized_version, scrubbed, and deleted_at (a tombstone
-- revision must be able to undelete). created_at keeps least(); state stays
-- insert-only; git_remote_url/git_root_commit keep the both-NULL rule.
--
-- Revision fencing, the repo resolution phase and the result shape are unchanged.
-- Code only: applying it to a hosted project needs Khaliq's go.

CREATE OR REPLACE FUNCTION sessions.upsert_session_index(p_rows jsonb)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text, machine_id text,
  revision bigint, input_revision bigint, applied boolean
) LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $upsert$
#variable_conflict use_column
DECLARE
  v_written jsonb;
  v_applied jsonb;
  v_repos jsonb := '{}';
  v_ev record;
BEGIN
  WITH input AS (SELECT * FROM sessions.session_index_batch(p_rows)),
  written AS (
    INSERT INTO sessions.session_index AS s (
      org_id, workspace_id, harness, session_id, machine_id, user_id, project_id,
      title, state, state_classified_at, created_at, last_activity_at,
      git_repo, git_branch, git_commit, model,
      git_remote_url, git_root_commit,
      event_count, input_tokens, output_tokens, cost_usd_micros,
      storage_provider, bucket, manifest_key, content_sha256, raw_bytes, chunk_count,
      raw_format, format_version, normalized_version, manifest_etag,
      revision, trace_id, deleted_at, indexed_at, scrubbed)
    SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id, i.user_id,
           i.project_id, left(i.title, 200), coalesce(i.state, 'unknown'),
           i.state_classified_at, i.created_at, i.last_activity_at,
           i.git_repo, i.git_branch, i.git_commit, i.model,
           sessions.canonical_remote(i.git_remote_url), sessions.clean_root_commit(i.git_root_commit),
           coalesce(i.event_count, 0), coalesce(i.input_tokens, 0),
           coalesce(i.output_tokens, 0), i.cost_usd_micros,
           i.storage_provider, i.bucket, i.manifest_key, i.content_sha256,
           coalesce(i.raw_bytes, 0), coalesce(i.chunk_count, 0),
           i.raw_format, coalesce(i.format_version, 1), i.normalized_version,
           i.manifest_etag, coalesce(i.revision, 1), i.trace_id, i.deleted_at, now(),
           coalesce(i.scrubbed, false)
      FROM input AS i
    ON CONFLICT ON CONSTRAINT session_index_pkey DO UPDATE SET
      user_id = excluded.user_id,
      project_id = coalesce(excluded.project_id, s.project_id),
      title = coalesce(excluded.title, s.title),
      -- First activity never moves later; it fixed the object's d= segment.
      created_at = least(s.created_at, excluded.created_at),
      last_activity_at = excluded.last_activity_at,
      git_repo = coalesce(excluded.git_repo, s.git_repo),
      git_branch = coalesce(excluded.git_branch, s.git_branch),
      git_commit = coalesce(excluded.git_commit, s.git_commit),
      model = coalesce(excluded.model, s.model),
      -- A manifest without a repo block keeps the stored evidence (and repo_id);
      -- one with a block replaces both fields.
      git_remote_url = CASE WHEN excluded.git_remote_url IS NULL AND excluded.git_root_commit IS NULL
                            THEN s.git_remote_url ELSE excluded.git_remote_url END,
      git_root_commit = CASE WHEN excluded.git_remote_url IS NULL AND excluded.git_root_commit IS NULL
                             THEN s.git_root_commit ELSE excluded.git_root_commit END,
      -- NOT NULL totals were coalesced to 0 for the insert, so excluded cannot tell
      -- an omitted total from a real 0: read the batch row itself (under this
      -- row's lock, so against the current stored values).
      event_count = coalesce((SELECT b.event_count FROM input AS b WHERE b.org_id = excluded.org_id
                                AND b.workspace_id = excluded.workspace_id AND b.harness = excluded.harness
                                AND b.session_id = excluded.session_id AND b.machine_id = excluded.machine_id),
                             s.event_count),
      input_tokens = coalesce((SELECT b.input_tokens FROM input AS b WHERE b.org_id = excluded.org_id
                                 AND b.workspace_id = excluded.workspace_id AND b.harness = excluded.harness
                                 AND b.session_id = excluded.session_id AND b.machine_id = excluded.machine_id),
                              s.input_tokens),
      output_tokens = coalesce((SELECT b.output_tokens FROM input AS b WHERE b.org_id = excluded.org_id
                                  AND b.workspace_id = excluded.workspace_id AND b.harness = excluded.harness
                                  AND b.session_id = excluded.session_id AND b.machine_id = excluded.machine_id),
                               s.output_tokens),
      cost_usd_micros = coalesce(excluded.cost_usd_micros, s.cost_usd_micros),
      storage_provider = excluded.storage_provider,
      bucket = excluded.bucket,
      manifest_key = excluded.manifest_key,
      content_sha256 = excluded.content_sha256,
      raw_bytes = excluded.raw_bytes,
      chunk_count = excluded.chunk_count,
      raw_format = excluded.raw_format,
      format_version = excluded.format_version,
      normalized_version = excluded.normalized_version,
      manifest_etag = coalesce(excluded.manifest_etag, s.manifest_etag),
      revision = excluded.revision,
      trace_id = excluded.trace_id,
      deleted_at = excluded.deleted_at,
      indexed_at = excluded.indexed_at,
      -- Sticky quarantine is enforced by session_index_scrub_sticky (any writer).
      scrubbed = excluded.scrubbed
    WHERE excluded.revision > s.revision
    RETURNING s.org_id, s.workspace_id, s.harness, s.session_id, s.machine_id, s.revision)
  -- Keys as named objects: array containment would ignore element order and match
  -- (…, 'claude', 'codex') against (…, 'codex', 'claude').
  SELECT coalesce(jsonb_agg(jsonb_build_object(
           'o', w.org_id, 'w', w.workspace_id, 'h', w.harness, 's', w.session_id,
           'm', w.machine_id)), '[]')
    INTO v_written
    FROM written AS w;

  -- Phase 2: repo identity for applied rows with evidence. The catalog locks come
  -- after this statement's row locks (see the migration header) in org order.
  SELECT jsonb_agg(to_jsonb(e)) INTO v_applied
    FROM sessions.applied_repo_evidence(p_rows, v_written) AS e;
  IF v_applied IS NOT NULL THEN
    PERFORM sessions.lock_repo_catalog(o.org_id)
       FROM (SELECT DISTINCT e.org_id FROM jsonb_to_recordset(v_applied) AS e(org_id text)) AS o
      ORDER BY o.org_id;
    FOR v_ev IN
      SELECT e.org_id, e.remote, e.root, max(e.last_activity_at) AS seen_at
        FROM jsonb_to_recordset(v_applied) AS e(org_id text, remote text, root text, last_activity_at timestamptz)
       GROUP BY e.org_id, e.remote, e.root
       ORDER BY e.org_id, e.remote, e.root
    LOOP
      v_repos := v_repos || jsonb_build_object(
        concat_ws(E'\n', v_ev.org_id, coalesce(v_ev.remote, ''), coalesce(v_ev.root, '')),
        sessions.resolve_repo(v_ev.org_id, v_ev.remote, v_ev.root, v_ev.seen_at));
    END LOOP;
    UPDATE sessions.session_index AS s
       SET repo_id = (v_repos ->> concat_ws(E'\n', e.org_id, coalesce(e.remote, ''), coalesce(e.root, '')))::uuid
      FROM jsonb_to_recordset(v_applied) AS e(org_id text, workspace_id text, harness text, session_id text,
                                              machine_id text, remote text, root text)
     WHERE s.org_id = e.org_id AND s.workspace_id = e.workspace_id AND s.harness = e.harness
       AND s.session_id = e.session_id AND s.machine_id = e.machine_id
       AND s.repo_id IS DISTINCT FROM
           (v_repos ->> concat_ws(E'\n', e.org_id, coalesce(e.remote, ''), coalesce(e.root, '')))::uuid;
  END IF;

  -- A second statement, so under READ COMMITTED it takes a fresh snapshot: a write
  -- rejected after waiting on a concurrent writer reports the revision that writer
  -- committed, not the one in the first statement's snapshot.
  RETURN QUERY
  SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id,
         coalesce(cur.revision, i.revision), coalesce(i.revision, 1::bigint),
         v_written @> jsonb_build_array(jsonb_build_object(
           'o', i.org_id, 'w', i.workspace_id, 'h', i.harness, 's', i.session_id,
           'm', i.machine_id))
    FROM sessions.session_index_batch(p_rows) AS i
    LEFT JOIN sessions.session_index AS cur
      ON cur.org_id = i.org_id AND cur.workspace_id = i.workspace_id
     AND cur.harness = i.harness AND cur.session_id = i.session_id
     AND cur.machine_id = i.machine_id;
END
$upsert$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION sessions.jwt_org_id(), sessions.jwt_workspace_ids(),
  sessions.member_of(text, text[]), sessions.repo_group(text, uuid),
  sessions.repo_live(text, uuid), sessions.repo_visibility_ids(text, uuid) TO authenticated;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO service_role;
