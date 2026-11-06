-- Stable repository identity for the sessions index (stacked on 20261025000000).
--
-- session_index.git_repo is whatever string the client saw, so a repo rename, a
-- transfer, an owner rename or an ssh-vs-https remote splits "all sessions in this
-- repo". This migration gives each session a repo_id that survives all of those.
--
-- Decisions (Khaliq, 2026-10-25): no GitHub App or OAuth connection is required;
-- the cleaned remote is stored on the session row; the repo catalog is per org
-- (each org has its own repos rows, nothing global); repo_id is resolved inline in
-- upsert_session_index, with no extra queue hop.
--
-- Evidence comes from the manifest's optional repo block, derived offline by the
-- client: the remote (canonicalized here exactly as the client's
-- project_identity::canonicalize_remote_url does: host/path, lowercase host,
-- credentials and port dropped, .git stripped) and the repository's root commit
-- (omitted for shallow clones).
--
-- Identity. A repo with a root commit is identified by (org, root commit): renames,
-- transfers, owner renames and host moves keep it. A fork shares its upstream's
-- root commit, so without a provider lookup it groups with the upstream as one
-- codebase; every remote it was seen under is kept in repo_names. A remote seen
-- without a root commit (shallow clone, or a session indexed before clients sent
-- one) gets a remote-only repo, which is merged into the root-commit repo the first
-- time any session reports both. Merges leave a merged_into tombstone; resolution
-- always returns the live repo, and repos_maintain() repoints older session rows.
-- provider_repo_id / repo_owner_provider_id are reserved for an optional future
-- provider lookup and stay NULL here.
--
-- Write order. upsert_session_index writes session rows first (revision-fenced,
-- unchanged), then resolves repos only for the rows that applied, so a stale or
-- replayed manifest can never change the catalog. Repo resolution is serialized
-- per org by one transaction-level advisory lock taken after the session row
-- locks; the backfill takes its row locks (SKIP LOCKED) before the same lock.
-- Once a transaction holds an org's lock it touches only repo tables and session
-- rows it already locked, so ingestion, backfill and repoint cannot deadlock.

ALTER TABLE sessions.session_index
  -- Canonical remote as host/path (credentials stripped), from the manifest.
  ADD COLUMN git_remote_url text,
  ADD COLUMN git_root_commit text CHECK (git_root_commit ~ '^[0-9a-f]{40}([0-9a-f]{24})?$'),
  -- -> sessions.repos; assigned by upsert_session_index, never by the client.
  ADD COLUMN repo_id uuid;

-- Sessions that reported a remote: the repo_names read policy's lookup.
CREATE INDEX si_remote ON sessions.session_index (org_id, lower(git_remote_url))
  WHERE git_remote_url IS NOT NULL AND deleted_at IS NULL;

-- A repo's sessions, newest activity first (keyset like the other lists).
CREATE INDEX si_repo ON sessions.session_index
  (org_id, repo_id, last_activity_at DESC, session_id DESC, harness DESC, workspace_id DESC, machine_id DESC)
  WHERE repo_id IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE sessions.repos (
  org_id text NOT NULL,
  repo_id uuid NOT NULL DEFAULT gen_random_uuid(),
  -- The identity evidence; NULL until some session reports it.
  root_commit text CHECK (root_commit ~ '^[0-9a-f]{40}([0-9a-f]{24})?$'),
  -- Current display identity: the most recently seen remote, split. NULL for a
  -- repo only ever seen without a remote (local-only, identified by root commit).
  remote text,
  host text,
  repo_owner text,
  name text,
  -- Reserved for an optional provider lookup (GitHub/GitLab ids); NULL here.
  provider_repo_id text,
  repo_owner_provider_id text,
  resolution text NOT NULL CHECK (resolution IN ('root_commit', 'remote')),
  merged_into uuid,
  first_seen_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  -- For a repo created from a remote alone: the activity time of the evidence
  -- that created it. Never moves, unlike first_seen_at (older evidence lowers that),
  -- so rooted evidence older than it can never claim the repo (see resolve_repo).
  remote_only_since timestamptz,
  CONSTRAINT repos_pkey PRIMARY KEY (org_id, repo_id),
  CHECK (merged_into IS DISTINCT FROM repo_id),
  CHECK (root_commit IS NOT NULL OR remote IS NOT NULL)
);
-- One live repo per root commit per org.
CREATE UNIQUE INDEX repos_root_uq ON sessions.repos (org_id, root_commit)
  WHERE root_commit IS NOT NULL AND merged_into IS NULL;
-- Everything under one owner (org or user namespace) on one host.
CREATE INDEX repos_owner_idx ON sessions.repos (org_id, host, lower(repo_owner))
  WHERE merged_into IS NULL;
-- Merge tombstones still to repoint.
CREATE INDEX repos_merged_idx ON sessions.repos (org_id, merged_into)
  WHERE merged_into IS NOT NULL;

-- Every remote a repo has been seen under. remote_key is lower(host/path):
-- GitHub and GitLab names are case-insensitive. A name later reused by a different
-- repo (its root commit differs) is repointed to that repo.
CREATE TABLE sessions.repo_names (
  org_id text NOT NULL,
  remote_key text NOT NULL,
  remote text NOT NULL,
  repo_id uuid NOT NULL,
  first_seen_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  CONSTRAINT repo_names_pkey PRIMARY KEY (org_id, remote_key)
);
CREATE INDEX repo_names_repo_idx ON sessions.repo_names (org_id, repo_id);

-- host/path for a git remote, or NULL if it is not recognizably one. Mirrors
-- relayhistory's project_identity::canonicalize_remote_url so client and server
-- agree on the key: scp-like user@host:path, or scheme://[auth@]host[:port]/path.
-- It also accepts its own output, a bare host/path (what the manifest's repo block
-- carries), so it is idempotent, including single-label and underscore hosts such as
-- intranet/team/repo or _corp/acme/api; that form keeps its path (a repo can be
-- named x.git) and is refused if it has a query or fragment.
-- A bare value cannot be told apart from a relative path, so legacy strings go
-- through legacy_remote() instead.
CREATE FUNCTION sessions.canonical_remote(p_url text)
RETURNS text LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $canon$
DECLARE
  v text := btrim(p_url);
  m text[];
  v_host text;
  v_path text;
  v_rest text;
  v_raw boolean := true;
  i int;
BEGIN
  IF v IS NULL OR v = '' OR v ~ '[\n\r]' THEN
    RETURN NULL;
  END IF;
  m := regexp_match(v, '^[A-Za-z0-9_-]+@(\[[^]]*\]|[^:\s\[]+):(.+)$');
  IF m IS NOT NULL THEN
    v_host := lower(m[1]);
    v_path := regexp_replace(regexp_replace(m[2], '^/+', ''), '/+$', '');
  ELSIF v ~ '^(\[[^]/\s@]*\]|[A-Za-z0-9_][^/\s:@\[]*)/[^\s@]' THEN
    -- Already canonical: keep the path as is (a repo may be named x.git), but a
    -- canonical value never has a query or fragment, which can carry tokens.
    IF v ~ '[?#]' THEN
      RETURN NULL;
    END IF;
    v_raw := false;
    v_host := lower(split_part(v, '/', 1));
    v_path := regexp_replace(substr(v, length(split_part(v, '/', 1)) + 2), '/+$', '');
  ELSIF v ~ '^[A-Za-z][A-Za-z0-9+.-]*://.' THEN
    v_rest := substr(v, position('://' in v) + 3);
    i := position('@' in v_rest);
    IF i > 0 THEN
      v_rest := substr(v_rest, i + 1);
    END IF;
    i := position('/' in v_rest);
    IF i = 0 THEN
      RETURN NULL;
    END IF;
    v_host := substr(v_rest, 1, i - 1);
    IF v_host LIKE '[%' THEN
      v_host := coalesce(substring(v_host from '^(\[[^]]*\])'), v_host);
    ELSE
      v_host := split_part(v_host, ':', 1);
    END IF;
    v_host := lower(v_host);
    v_path := regexp_replace(substr(v_rest, i + 1), '/+$', '');
  ELSE
    RETURN NULL;
  END IF;
  IF v_raw THEN
    -- A raw remote: drop any query or fragment (never stored: they can carry
    -- tokens), then exactly one .git, as the client does.
    v_path := regexp_replace(split_part(split_part(v_path, '?', 1), '#', 1), '/+$', '');
    v_path := regexp_replace(v_path, '\.git$', '');
  END IF;
  IF v_host = '' OR v_path = '' THEN
    RETURN NULL;
  END IF;
  RETURN v_host || '/' || v_path;
END
$canon$;

-- A root commit in canonical form (lowercase SHA-1 or SHA-256 hex), else NULL.
CREATE FUNCTION sessions.clean_root_commit(p_root text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $clean_root$
  SELECT CASE WHEN lower(btrim(p_root)) ~ '^[0-9a-f]{40}([0-9a-f]{24})?$' THEN lower(btrim(p_root)) END
$clean_root$;

-- canonical_remote for a legacy free-form string (session_index.git_repo): only the
-- scheme and scp-like forms, or a bare host/path whose host is dotted labels
-- (github.com), so a relative directory such as src/app, ./src or ../src is never
-- taken for a remote.
CREATE FUNCTION sessions.legacy_remote(p_value text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, sessions, pg_temp AS $legacy$
  SELECT CASE
    WHEN btrim(p_value) ~ '^[A-Za-z][A-Za-z0-9+.-]*://'
      OR btrim(p_value) ~ '^[A-Za-z0-9_-]+@'
    THEN sessions.canonical_remote(p_value)
    -- A bare legacy value is raw, not canonical: strip one .git like the client
    -- does, so github.com/acme/api.git and git@github.com:acme/api.git agree.
    WHEN split_part(btrim(p_value), '/', 1) ~ '^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)+$'
    THEN sessions.canonical_remote(regexp_replace(btrim(p_value), '\.git/*$', ''))
  END
$legacy$;

-- The live repo for a possibly merged one.
CREATE FUNCTION sessions.repo_live(p_org_id text, p_repo_id uuid)
RETURNS uuid LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $live$
DECLARE
  v uuid := p_repo_id;
  v_next uuid;
BEGIN
  FOR i IN 1..32 LOOP
    v_next := NULL;
    SELECT r.merged_into INTO v_next FROM sessions.repos AS r
     WHERE r.org_id = p_org_id AND r.repo_id = v;
    EXIT WHEN v_next IS NULL;
    v := v_next;
  END LOOP;
  RETURN v;
END
$live$;

-- A live repo and every repo merged into it: what "sessions in this repo" matches
-- until repos_maintain() has repointed the merged rows.
CREATE FUNCTION sessions.repo_group(p_org_id text, p_repo_id uuid)
RETURNS uuid[] LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $group$
  WITH RECURSIVE g(repo_id) AS (
    SELECT sessions.repo_live(p_org_id, p_repo_id)
    UNION
    SELECT r.repo_id FROM sessions.repos AS r JOIN g ON r.org_id = p_org_id AND r.merged_into = g.repo_id
  )
  SELECT array_agg(repo_id) FROM g
$group$;

-- Serialize repo resolution for an org. Transaction-scoped and re-entrant.
CREATE FUNCTION sessions.lock_repo_catalog(p_org_id text)
RETURNS void LANGUAGE sql
SET search_path = pg_catalog, pg_temp AS $lock$
  SELECT pg_advisory_xact_lock(hashtextextended('repo-catalog:' || p_org_id, 0))
$lock$;

-- Resolve (and record) the repo for one remote and/or root commit in an org.
-- Takes the org's catalog lock (a no-op when the caller already holds it). Writes
-- only when something changed or the evidence is newer than what is stored.
CREATE FUNCTION sessions.resolve_repo(
  p_org_id text, p_remote text, p_root_commit text, p_seen_at timestamptz
) RETURNS uuid LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $resolve$
DECLARE
  v_remote text := sessions.canonical_remote(p_remote);
  v_root text := sessions.clean_root_commit(p_root_commit);
  v_seen timestamptz := coalesce(p_seen_at, now());
  v_key text := lower(sessions.canonical_remote(p_remote));
  v_host text;
  v_path text;
  v_named uuid;
  v_named_root text;
  v_named_since timestamptz;
  v_repo uuid;
BEGIN
  IF v_remote IS NULL AND v_root IS NULL THEN
    RETURN NULL;
  END IF;
  PERFORM sessions.lock_repo_catalog(p_org_id);

  IF v_remote IS NOT NULL THEN
    v_host := split_part(v_remote, '/', 1);
    v_path := substr(v_remote, length(v_host) + 2);
    SELECT sessions.repo_live(p_org_id, n.repo_id) INTO v_named
      FROM sessions.repo_names AS n
     WHERE n.org_id = p_org_id AND n.remote_key = v_key;
    IF v_named IS NOT NULL THEN
      SELECT r.root_commit, coalesce(r.remote_only_since, r.first_seen_at)
        INTO v_named_root, v_named_since
        FROM sessions.repos AS r WHERE r.org_id = p_org_id AND r.repo_id = v_named;
    END IF;
  END IF;

  IF v_root IS NOT NULL THEN
    SELECT r.repo_id INTO v_repo
      FROM sessions.repos AS r
     WHERE r.org_id = p_org_id AND r.root_commit = v_root AND r.merged_into IS NULL;
    -- A remote-only repo that holds this name is only identified with this root
    -- commit by evidence no older than the evidence that created it
    -- (remote_only_since, which never moves). Older evidence may be from before
    -- the name was reused, so it resolves on its own; fresher evidence for the
    -- same name and root merges them later.
    IF v_repo IS NULL AND v_named IS NOT NULL AND v_named_root IS NULL
       AND v_seen >= v_named_since THEN
      -- The remote-only repo for this name now has its root commit.
      UPDATE sessions.repos AS r SET root_commit = v_root, resolution = 'root_commit'
       WHERE r.org_id = p_org_id AND r.repo_id = v_named;
      v_repo := v_named;
    ELSIF v_repo IS NULL THEN
      INSERT INTO sessions.repos AS r (org_id, root_commit, resolution, first_seen_at, last_seen_at)
      VALUES (p_org_id, v_root, 'root_commit', v_seen, v_seen)
      RETURNING r.repo_id INTO v_repo;
    ELSIF v_named IS NOT NULL AND v_named <> v_repo AND v_named_root IS NULL
          AND v_seen >= v_named_since THEN
      -- A remote-only repo turns out to be this one: merge it, and its names.
      UPDATE sessions.repos AS r SET merged_into = v_repo
       WHERE r.org_id = p_org_id AND r.repo_id = v_named;
      UPDATE sessions.repo_names AS n SET repo_id = v_repo
       WHERE n.org_id = p_org_id AND n.repo_id = v_named;
      UPDATE sessions.repos AS r
         SET first_seen_at = least(r.first_seen_at, o.first_seen_at)
        FROM sessions.repos AS o
       WHERE r.org_id = p_org_id AND r.repo_id = v_repo
         AND o.org_id = p_org_id AND o.repo_id = v_named;
    END IF;
    -- A name already held by a repo with a different root commit has been reused
    -- (or a fork took the name); it moves to this repo below if this is newer.
  ELSE
    v_repo := v_named;
    IF v_repo IS NULL THEN
      INSERT INTO sessions.repos AS r (org_id, resolution, remote, first_seen_at, last_seen_at,
                                       remote_only_since)
      VALUES (p_org_id, 'remote', v_remote, v_seen, v_seen, v_seen)
      RETURNING r.repo_id INTO v_repo;
    END IF;
  END IF;

  IF v_remote IS NOT NULL THEN
    INSERT INTO sessions.repo_names AS n (org_id, remote_key, remote, repo_id, first_seen_at, last_seen_at)
    VALUES (p_org_id, v_key, v_remote, v_repo, v_seen, v_seen)
    ON CONFLICT ON CONSTRAINT repo_names_pkey DO UPDATE SET
      repo_id = CASE WHEN excluded.last_seen_at >= n.last_seen_at THEN excluded.repo_id ELSE n.repo_id END,
      remote = CASE WHEN excluded.last_seen_at >= n.last_seen_at THEN excluded.remote ELSE n.remote END,
      first_seen_at = least(n.first_seen_at, excluded.first_seen_at),
      last_seen_at = greatest(n.last_seen_at, excluded.last_seen_at)
    -- Exact times: ownership of a reused name and the display both compare them.
    WHERE (excluded.last_seen_at >= n.last_seen_at
           AND (n.repo_id <> excluded.repo_id OR n.remote <> excluded.remote))
       OR excluded.first_seen_at < n.first_seen_at
       OR excluded.last_seen_at > n.last_seen_at;
    -- The repo's display identity follows the most recently seen remote. A
    -- remote-only insert records remote but not its parts, so fill them too.
    UPDATE sessions.repos AS r
       SET remote = v_remote,
           host = v_host,
           repo_owner = CASE WHEN position('/' in v_path) > 0
                             THEN regexp_replace(v_path, '/[^/]*$', '') ELSE '' END,
           name = regexp_replace(v_path, '^.*/', '')
     WHERE r.org_id = p_org_id AND r.repo_id = v_repo
       AND (r.host IS NULL OR r.last_seen_at <= v_seen)
       AND (r.remote IS DISTINCT FROM v_remote OR r.host IS NULL);
  END IF;
  UPDATE sessions.repos AS r
     SET first_seen_at = least(r.first_seen_at, v_seen),
         last_seen_at = greatest(r.last_seen_at, v_seen)
   WHERE r.org_id = p_org_id AND r.repo_id = v_repo
     AND (v_seen < r.first_seen_at OR v_seen > r.last_seen_at);
  RETURN v_repo;
END
$resolve$;

-- The batch rows that phase 1 applied and that carry repo evidence, canonicalized.
CREATE FUNCTION sessions.applied_repo_evidence(p_rows jsonb, p_written jsonb)
RETURNS TABLE (org_id text, workspace_id text, harness text, session_id text, machine_id text,
               remote text, root text, last_activity_at timestamptz)
LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $applied$
  SELECT e.org_id, e.workspace_id, e.harness, e.session_id, e.machine_id, e.remote, e.root,
         e.last_activity_at
    FROM (SELECT b.org_id, b.workspace_id, b.harness, b.session_id, b.machine_id,
                 sessions.canonical_remote(b.git_remote_url) AS remote,
                 sessions.clean_root_commit(b.git_root_commit) AS root, b.last_activity_at
            FROM sessions.session_index_batch(p_rows) AS b) AS e
    JOIN jsonb_to_recordset(p_written) AS w(o text, w text, h text, s text, m text)
      ON w.o = e.org_id AND w.w = e.workspace_id AND w.h = e.harness
     AND w.s = e.session_id AND w.m = e.machine_id
   WHERE e.remote IS NOT NULL OR e.root IS NOT NULL
$applied$;

-- upsert_session_index, now also resolving repo identity (same contract). Phase 1
-- is the revision-fenced row write, as before. Phase 2 resolves repos only for rows
-- that applied and carried evidence, under the org's catalog lock, and sets repo_id
-- only where it changed.
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
      revision, trace_id, deleted_at, indexed_at)
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
           i.manifest_etag, coalesce(i.revision, 1), i.trace_id, i.deleted_at, now()
      FROM input AS i
    ON CONFLICT ON CONSTRAINT session_index_pkey DO UPDATE SET
      user_id = excluded.user_id,
      project_id = excluded.project_id,
      title = excluded.title,
      -- First activity never moves later; it fixed the object's d= segment.
      created_at = least(s.created_at, excluded.created_at),
      last_activity_at = excluded.last_activity_at,
      git_repo = excluded.git_repo,
      git_branch = excluded.git_branch,
      git_commit = excluded.git_commit,
      model = excluded.model,
      -- A manifest without a repo block keeps the stored evidence (and repo_id);
      -- one with a block replaces both fields.
      git_remote_url = CASE WHEN excluded.git_remote_url IS NULL AND excluded.git_root_commit IS NULL
                            THEN s.git_remote_url ELSE excluded.git_remote_url END,
      git_root_commit = CASE WHEN excluded.git_remote_url IS NULL AND excluded.git_root_commit IS NULL
                             THEN s.git_root_commit ELSE excluded.git_root_commit END,
      event_count = excluded.event_count,
      input_tokens = excluded.input_tokens,
      output_tokens = excluded.output_tokens,
      cost_usd_micros = excluded.cost_usd_micros,
      storage_provider = excluded.storage_provider,
      bucket = excluded.bucket,
      manifest_key = excluded.manifest_key,
      content_sha256 = excluded.content_sha256,
      raw_bytes = excluded.raw_bytes,
      chunk_count = excluded.chunk_count,
      raw_format = excluded.raw_format,
      format_version = excluded.format_version,
      normalized_version = excluded.normalized_version,
      manifest_etag = excluded.manifest_etag,
      revision = excluded.revision,
      trace_id = excluded.trace_id,
      deleted_at = excluded.deleted_at,
      indexed_at = excluded.indexed_at
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

-- Repoint up to p_limit session rows that still carry a merged repo's id to its
-- live repo; returns rows changed. Not scheduled in v1 (merges are rare); lists
-- stay correct without it through repo_group(). Safe to run by hand.
CREATE FUNCTION sessions.repos_maintain(p_limit integer DEFAULT 5000)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $repos_maintain$
DECLARE
  v_count integer;
BEGIN
  IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100000 THEN
    RAISE EXCEPTION 'p_limit must be between 1 and 100000, got %', p_limit;
  END IF;
  WITH merged AS (
    SELECT r.org_id, r.repo_id, sessions.repo_live(r.org_id, r.repo_id) AS live
      FROM sessions.repos AS r WHERE r.merged_into IS NOT NULL
  ),
  target AS (
    SELECT s.org_id, s.workspace_id, s.harness, s.session_id, s.machine_id, m.live
      FROM sessions.session_index AS s
      JOIN merged AS m ON s.org_id = m.org_id AND s.repo_id = m.repo_id
     WHERE s.deleted_at IS NULL
     LIMIT p_limit
       FOR UPDATE OF s SKIP LOCKED
  )
  UPDATE sessions.session_index AS s SET repo_id = t.live
    FROM target AS t
   WHERE s.org_id = t.org_id AND s.workspace_id = t.workspace_id AND s.harness = t.harness
     AND s.session_id = t.session_id AND s.machine_id = t.machine_id;
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END
$repos_maintain$;

-- Backfill, part 1: a read-only dry run over rows that have no repo yet. Writes
-- nothing. Its counts are the review gate before any live backfill.
CREATE FUNCTION sessions.repo_backfill_report()
RETURNS TABLE (metric text, value bigint) LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $report$
  WITH pending AS (
    SELECT s.org_id,
           coalesce(sessions.canonical_remote(s.git_remote_url),
                    sessions.legacy_remote(s.git_repo)) AS remote,
           s.git_root_commit AS root
      FROM sessions.session_index AS s
     WHERE s.repo_id IS NULL AND s.deleted_at IS NULL
  ),
  remotes AS (SELECT DISTINCT org_id, lower(remote) AS remote_key FROM pending WHERE remote IS NOT NULL),
  roots AS (SELECT DISTINCT org_id, root FROM pending WHERE root IS NOT NULL)
  SELECT 'rows_without_repo', count(*) FROM pending
  UNION ALL SELECT 'rows_resolvable', count(*) FROM pending WHERE remote IS NOT NULL OR root IS NOT NULL
  UNION ALL SELECT 'rows_unresolvable', count(*) FROM pending WHERE remote IS NULL AND root IS NULL
  UNION ALL SELECT 'rows_with_root_commit', count(*) FROM pending WHERE root IS NOT NULL
  UNION ALL SELECT 'distinct_remotes', count(*) FROM remotes
  UNION ALL SELECT 'distinct_root_commits', count(*) FROM roots
  UNION ALL SELECT 'remotes_already_known', count(*) FROM remotes AS x
    WHERE EXISTS (SELECT 1 FROM sessions.repo_names AS n
                   WHERE n.org_id = x.org_id AND n.remote_key = x.remote_key)
  UNION ALL SELECT 'root_commits_already_known', count(*) FROM roots AS x
    WHERE EXISTS (SELECT 1 FROM sessions.repos AS r
                   WHERE r.org_id = x.org_id AND r.root_commit = x.root AND r.merged_into IS NULL)
  UNION ALL SELECT 'root_commits_under_several_remotes', count(*) FROM (
    SELECT org_id, root FROM pending WHERE root IS NOT NULL AND remote IS NOT NULL
     GROUP BY org_id, root HAVING count(DISTINCT lower(remote)) > 1) AS shared
$report$;

-- Backfill, part 2: assign repo_id to up to p_limit resolvable rows. Never bumps
-- revision or indexed_at, so it is safe beside live ingestion. Refuses to write
-- unless p_confirm = 'apply': the dry run above is reviewed first, and a live run
-- needs Khaliq's go. Lock order matches upsert_session_index: the batch's session
-- rows first (SKIP LOCKED, so rows ingestion holds are left for a later call),
-- then the orgs' catalog locks. Every selected row resolves, so repeated calls
-- finish; the count returned is rows assigned.
CREATE FUNCTION sessions.repo_backfill_step(p_limit integer DEFAULT 1000, p_confirm text DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $backfill$
DECLARE
  v_batch jsonb;
  v_ev record;
  v_repo uuid;
  v_count integer := 0;
  v_n integer;
BEGIN
  IF p_confirm IS DISTINCT FROM 'apply' THEN
    RAISE EXCEPTION 'repo_backfill_step writes; review repo_backfill_report() and pass p_confirm => ''apply''';
  END IF;
  IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION 'p_limit must be between 1 and 10000, got %', p_limit;
  END IF;
  -- Row locks first, then catalog locks (the upsert's order).
  WITH picked AS (
    SELECT s.org_id, s.workspace_id, s.harness, s.session_id, s.machine_id,
           coalesce(sessions.canonical_remote(s.git_remote_url),
                    sessions.legacy_remote(s.git_repo)) AS remote,
           s.git_root_commit AS root, s.last_activity_at
      FROM sessions.session_index AS s
     WHERE s.repo_id IS NULL AND s.deleted_at IS NULL
       AND (s.git_root_commit IS NOT NULL
            OR coalesce(sessions.canonical_remote(s.git_remote_url),
                        sessions.legacy_remote(s.git_repo)) IS NOT NULL)
     ORDER BY s.org_id, s.workspace_id, s.harness, s.session_id, s.machine_id
     LIMIT p_limit
       FOR UPDATE OF s SKIP LOCKED
  )
  SELECT coalesce(jsonb_agg(to_jsonb(picked)), '[]') INTO v_batch FROM picked;

  PERFORM sessions.lock_repo_catalog(o.org_id)
     FROM (SELECT DISTINCT b.org_id
             FROM jsonb_to_recordset(v_batch) AS b(org_id text)) AS o
    ORDER BY o.org_id;
  FOR v_ev IN
    SELECT b.org_id, b.remote, b.root, max(b.last_activity_at) AS seen_at
      FROM jsonb_to_recordset(v_batch) AS b(org_id text, remote text, root text, last_activity_at timestamptz)
     GROUP BY b.org_id, b.remote, b.root
     ORDER BY b.org_id, b.remote, b.root
  LOOP
    v_repo := sessions.resolve_repo(v_ev.org_id, v_ev.remote, v_ev.root, v_ev.seen_at);
    UPDATE sessions.session_index AS s
       SET repo_id = v_repo, git_remote_url = coalesce(s.git_remote_url, b.remote)
      FROM jsonb_to_recordset(v_batch) AS b(org_id text, workspace_id text, harness text,
                                             session_id text, machine_id text, remote text, root text)
     WHERE b.org_id = v_ev.org_id
       AND b.remote IS NOT DISTINCT FROM v_ev.remote AND b.root IS NOT DISTINCT FROM v_ev.root
       AND s.org_id = b.org_id AND s.workspace_id = b.workspace_id AND s.harness = b.harness
       AND s.session_id = b.session_id AND s.machine_id = b.machine_id
       AND s.repo_id IS NULL;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_count := v_count + v_n;
  END LOOP;
  RETURN v_count;
END
$backfill$;

ALTER TABLE sessions.repos ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.repo_names ENABLE ROW LEVEL SECURITY;

-- The ids in a repo's merge group (the repo and every repo merged into it) that the
-- caller can see a session on: the caller's org claim must match and the session
-- must be in one of the caller's claimed workspaces. It reads past RLS so the read
-- policies below can follow merges without querying repos from inside a repos
-- policy, so it checks access itself; called directly, it reveals only ids the
-- caller's own sessions already point at.
CREATE FUNCTION sessions.repo_visibility_ids(p_org_id text, p_repo_id uuid)
RETURNS uuid[] LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $visibility$
  WITH RECURSIVE g(repo_id) AS (
    SELECT p_repo_id
    UNION
    SELECT r.repo_id FROM sessions.repos AS r JOIN g ON r.org_id = p_org_id AND r.merged_into = g.repo_id
  )
  SELECT array_agg(g.repo_id) FROM g
   WHERE p_org_id = sessions.jwt_org_id()
     AND EXISTS (SELECT 1 FROM sessions.session_index AS s
                  WHERE s.org_id = p_org_id AND s.repo_id = g.repo_id AND s.deleted_at IS NULL
                    AND sessions.member_of(s.workspace_id, sessions.jwt_workspace_ids()))
$visibility$;

-- A member sees a repo, and its names, only if a session they can see (RLS on
-- session_index: their org and workspaces) points at it or at a repo merged into
-- it (sessions keep a merged id until repos_maintain repoints them). The repo row's
-- display (remote/host/repo_owner/name) is the repo's current name in the org.
CREATE POLICY repos_member_read ON sessions.repos
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id())
         AND EXISTS (SELECT 1 FROM sessions.session_index AS s
                      WHERE s.org_id = repos.org_id
                        AND s.repo_id = ANY (sessions.repo_visibility_ids(repos.org_id, repos.repo_id))
                        AND s.deleted_at IS NULL));
-- A repo's alias history is stricter: a name is visible only if one of the
-- member's visible sessions reported that remote itself, so a fork or old name
-- seen only in another workspace never shows (merges do not widen it). The session
-- must also belong to the name's current repo (or one merged into it), so a name
-- another workspace's repo has since reused does not reveal that repo.
CREATE POLICY repo_names_member_read ON sessions.repo_names
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id())
         AND EXISTS (SELECT 1 FROM sessions.session_index AS s
                      WHERE s.org_id = repo_names.org_id
                        AND lower(s.git_remote_url) = repo_names.remote_key
                        AND s.repo_id = ANY (sessions.repo_visibility_ids(repo_names.org_id, repo_names.repo_id))
                        AND s.deleted_at IS NULL));

REVOKE ALL ON sessions.repos, sessions.repo_names FROM PUBLIC, anon, authenticated;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC, anon, authenticated;
GRANT SELECT ON sessions.repos, sessions.repo_names TO authenticated;
GRANT EXECUTE ON FUNCTION sessions.jwt_org_id(), sessions.jwt_workspace_ids(),
  sessions.member_of(text, text[]), sessions.repo_group(text, uuid),
  sessions.repo_live(text, uuid), sessions.repo_visibility_ids(text, uuid) TO authenticated;
GRANT ALL ON sessions.repos, sessions.repo_names TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO service_role;
