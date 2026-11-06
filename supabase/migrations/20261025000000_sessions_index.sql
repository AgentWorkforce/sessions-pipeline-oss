-- Supabase `sessions` pointer index for the object-storage sessions pipeline.
--
-- Raw session logs live in object storage under
--   {prefix}/v1/ws={workspace_id}/m={machine_id}/h={harness}/d={yyyy}/{mm}/{dd}/s={session_id}/
-- and are canonical. Every row here is derived from a session's manifest.json and
-- can be rebuilt by replaying the manifests: no bodies, no embeddings, only what a
-- session list, filter or "open this session" lookup needs, plus the pointer to the
-- manifest. The index ships inactive: nothing reads it until the ingest worker's
-- flag is on, and Neon (packages/relayhistory/db) stays authoritative until cutover.
--
-- Identity. A row is one manifest: (org_id, workspace_id, harness, session_id,
-- machine_id), the ws=/m=/h=/s= segments of the object path. Session ids are only
-- unique per harness (Neon's session_catalog key, source = harness). machine_id is
-- in the key because each manifest has its own revision counter: a session resumed
-- or copied onto a second machine writes a second manifest, and sharing one row
-- would fence one machine's revisions against the other's unrelated counter
-- (Khaliq, 2026-10-25). Such a session has one row per machine; a session-level
-- list keeps only each session's newest manifest with NOT EXISTS (see
-- supabase/README.md). Grouping a page would not stop a later page repeating it.
-- machine_id is last so lookups that do not
-- know the machine still use the key's prefix.
--
-- Writes. Only the ingest worker writes, as service_role, through
-- upsert_session_index(), which is fenced by the manifest revision so a late or
-- replayed object-landed event can never roll a row back. The state classifier
-- writes through set_session_state(), fenced by its own classification time, so a
-- manifest upsert and a classification never overwrite each other.
--
-- Reads. Signed-in users read through RLS: a row is visible when its org and
-- workspace are in the caller's JWT claims (jwt_org_id, jwt_workspace_ids). Every list query is
-- a keyset page over a partial index ending in the sort key plus the key columns
-- that make it unique; see supabase/README.md for each index and the query it serves.
--
-- Sizing. ~1.3 KB/row including indexes; no partitioning of session_index until
-- ~100M rows. pipeline_events is range-partitioned by month and keeps 90 days.

CREATE SCHEMA IF NOT EXISTS sessions;

CREATE TABLE sessions.session_index (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  -- Neon "source": claude, codex, opencode, ...
  harness text NOT NULL CHECK (harness ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255),
  machine_id text NOT NULL,
  -- The teammate whose machine produced the session.
  user_id text NOT NULL,
  project_id text,
  title text CHECK (char_length(title) <= 200),
  -- text + CHECK rather than an enum: enum_eq is not LEAKPROOF, so under RLS
  -- `state = 'active'` could never be an index condition (texteq is leakproof).
  state text NOT NULL DEFAULT 'unknown'
    CHECK (state IN ('active', 'done', 'abandoned', 'unknown')),
  state_classified_at timestamptz,
  -- First activity. Immutable after the first manifest: it fixes the d= path
  -- segment (session start date, UTC).
  created_at timestamptz NOT NULL,
  -- Most recent activity: the sort key of every session list.
  last_activity_at timestamptz NOT NULL,
  git_repo text,
  git_branch text,
  git_commit text,
  -- The session's most recent model.
  model text,
  event_count integer NOT NULL DEFAULT 0 CHECK (event_count >= 0),
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  -- NULL when no event reported a cost (as in Neon's session_rollups).
  cost_usd_micros bigint CHECK (cost_usd_micros >= 0),
  -- Pointer to the canonical raw object set.
  storage_provider text NOT NULL CHECK (storage_provider IN ('s3', 'gcs', 'azure', 'r2', 'fs')),
  bucket text NOT NULL,
  manifest_key text NOT NULL,
  -- SHA-256 of the manifest body this row was derived from.
  content_sha256 bytea CHECK (octet_length(content_sha256) = 32),
  raw_bytes bigint NOT NULL DEFAULT 0 CHECK (raw_bytes >= 0),
  chunk_count integer NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
  raw_format text CHECK (raw_format IN ('jsonl', 'ndjson', 'sqlite-rows')),
  format_version smallint NOT NULL DEFAULT 1 CHECK (format_version >= 1),
  -- Highest normalized/v{N}/ available, if a transform has run.
  normalized_version smallint CHECK (normalized_version >= 1),
  manifest_etag text,
  -- The manifest's revision. Strictly increasing per session; fences upserts.
  revision bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
  -- One UUIDv7 per session, shared by every pipeline span for it.
  trace_id uuid NOT NULL,
  -- Tombstone. Deletion is a manifest revision like any other.
  deleted_at timestamptz,
  indexed_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT session_index_pkey PRIMARY KEY (org_id, workspace_id, harness, session_id, machine_id)
);

-- Keyset pages. Every list sorts by most recent activity, newest first (Khaliq,
-- 2026-10-25; the same order as Neon's session_rollups). Each index ends in
-- last_activity_at plus whichever key columns the list does not pin with an
-- equality, so the order is total over the primary key:
--   workspace lists:       (last_activity_at, session_id, harness, machine_id)
--                            < ($ts, $session, $harness, $machine)
--   cross-workspace lists: (last_activity_at, session_id, harness, workspace_id, machine_id)
--                            < ($ts, $session, $harness, $workspace, $machine)
--   (si_harness pins harness and si_machine pins machine_id, so those drop out.)
-- Without workspace_id, the same session id under one harness in two workspaces with
-- equal timestamps would tie, and a page ending between them would skip one.
-- The sort key moves: a session that gets new activity while a client pages jumps
-- to the front, so that pass does not show it again (no duplicates); it appears on
-- the next first page. Already-seen rows never repeat.
-- Partial on deleted_at IS NULL: lists never show tombstones.

-- Workspace session list.
CREATE INDEX si_ws_recent ON sessions.session_index
  (org_id, workspace_id, last_activity_at DESC, session_id DESC, harness DESC, machine_id DESC)
  WHERE deleted_at IS NULL;
-- All workspaces of an org (org-wide/admin view; Neon session_rollups_org_recent_idx).
CREATE INDEX si_org_recent ON sessions.session_index
  (org_id, last_activity_at DESC, session_id DESC, harness DESC, workspace_id DESC, machine_id DESC)
  WHERE deleted_at IS NULL;
-- One teammate's sessions in a workspace.
CREATE INDEX si_user_recent ON sessions.session_index
  (org_id, workspace_id, user_id, last_activity_at DESC, session_id DESC, harness DESC, machine_id DESC)
  WHERE deleted_at IS NULL;
-- One harness's sessions in a workspace; harness is an equality, so it drops out
-- of the tie-breakers.
CREATE INDEX si_harness ON sessions.session_index
  (org_id, workspace_id, harness, last_activity_at DESC, session_id DESC, machine_id DESC)
  WHERE deleted_at IS NULL;
-- One machine's sessions, across the org's workspaces.
CREATE INDEX si_machine ON sessions.session_index
  (org_id, machine_id, last_activity_at DESC, session_id DESC, harness DESC, workspace_id DESC)
  WHERE deleted_at IS NULL;
-- Sessions not yet done (active / abandoned / unclassified).
CREATE INDEX si_state ON sessions.session_index
  (org_id, workspace_id, state, last_activity_at DESC, session_id DESC, harness DESC, machine_id DESC)
  WHERE state <> 'done' AND deleted_at IS NULL;
-- A project's sessions. Org-scoped like Neon's project index, since a project can
-- span workspaces; a workspace filter is applied to the few rows it returns.
CREATE INDEX si_project ON sessions.session_index
  (org_id, project_id, last_activity_at DESC, session_id DESC, harness DESC, workspace_id DESC, machine_id DESC)
  WHERE project_id IS NOT NULL AND deleted_at IS NULL;
-- Open a session by id when the client knows neither workspace nor harness
-- (Neon session_catalog_org_session_idx); returns one row per machine. Includes
-- tombstones on purpose.
CREATE INDEX si_session ON sessions.session_index (org_id, session_id);
-- Pipeline tracing: trace_id -> session.
CREATE INDEX si_trace ON sessions.session_index (trace_id);

-- RelayHistory owns a sessions.machines with the same key (org_id, machine_id)
-- and complementary columns, and its runner may have created it first on the
-- shared Supabase project. Create it if absent, then add this product's
-- columns either way (20261030000000 adds RelayHistory's).
CREATE TABLE IF NOT EXISTS sessions.machines (
  org_id text NOT NULL,
  machine_id text NOT NULL,
  machine_name text,
  hostname_aliases text[] NOT NULL DEFAULT '{}',
  os text,
  client_version text,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT machines_pkey PRIMARY KEY (org_id, machine_id)
);
ALTER TABLE sessions.machines
  ADD COLUMN IF NOT EXISTS machine_name text,
  ADD COLUMN IF NOT EXISTS hostname_aliases text[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS client_version text;

-- One row per span event of the pipeline, keyed to a session by trace_id. This is
-- the single event table for both the index and observability/DLQ (W9) work.
-- Diagnostics only: service_role reads and writes it; tenants have no access.
-- Writers do a plain INSERT. Redelivery can repeat a row, and a unique key on a
-- partitioned table would have to include `at`, which differs per delivery, so
-- readers dedupe on idempotency_key instead.
CREATE TABLE sessions.pipeline_events (
  trace_id uuid NOT NULL,
  stage text NOT NULL CHECK (stage IN (
    'ingest', 'deliver.put', 'storage.event', 'transform', 'index.upsert', 'fetch')),
  status text NOT NULL CHECK (status IN (
    'started', 'ok', 'error', 'retrying', 'dead_lettered', 'replayed', 'skipped')),
  -- {code, name, message?, retryable}: the pipeline's own codes, never foreign
  -- error text (which can carry session content or secrets).
  error jsonb CHECK (error IS NULL OR (jsonb_typeof(error) = 'object' AND error ? 'code')),
  attrs jsonb NOT NULL DEFAULT '{}',
  at timestamptz NOT NULL DEFAULT now(),
  duration_ms integer CHECK (duration_ms >= 0),
  attempt integer NOT NULL DEFAULT 1 CHECK (attempt >= 1),
  -- trace:stage:status:attempt:discriminator
  idempotency_key text,
  -- Denormalized so an operator can find a tenant's failures without a join.
  org_id text,
  workspace_id text,
  machine_id text,
  harness text,
  session_id text
) PARTITION BY RANGE (at);

-- Rows outside every monthly partition land here rather than fail the writer;
-- pipeline_events_maintain() moves them into their month when it creates it.
CREATE TABLE sessions.pipeline_events_default
  PARTITION OF sessions.pipeline_events DEFAULT;
ALTER TABLE sessions.pipeline_events_default ENABLE ROW LEVEL SECURITY;

-- Everything that happened to one session, in order (trace summary:
-- WHERE workspace_id = $1 AND trace_id = $2 ORDER BY at).
CREATE INDEX pipeline_events_trace_idx ON sessions.pipeline_events (trace_id, at);
-- Recent failures by stage. started/ok/skipped/replayed rows stay out.
CREATE INDEX pipeline_events_failures_idx ON sessions.pipeline_events (stage, at DESC)
  WHERE status IN ('error', 'retrying', 'dead_lettered');

-- One row per unit of work a stage gave up on, for inspection and replay.
-- Service-role only, like pipeline_events.
CREATE TABLE sessions.pipeline_dead_letters (
  -- '<stage>:<workKey>': a unit of work dead-letters into one row however often it fails.
  id text PRIMARY KEY,
  stage text NOT NULL CHECK (stage IN (
    'ingest', 'deliver.put', 'storage.event', 'transform', 'index.upsert', 'fetch')),
  trace_id uuid,
  org_id text,
  workspace_id text,
  -- The queue message to replay.
  message jsonb NOT NULL,
  state text NOT NULL DEFAULT 'dead' CHECK (state IN ('dead', 'replaying', 'resolved')),
  first_dead_at timestamptz NOT NULL DEFAULT now(),
  last_dead_at timestamptz NOT NULL DEFAULT now(),
  dead_count integer NOT NULL DEFAULT 1 CHECK (dead_count >= 1),
  replay_count integer NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
  last_replay_id uuid,
  last_replay_at timestamptz,
  resolved_at timestamptz
);
-- Operator queue: what is dead in each stage, oldest first.
CREATE INDEX pipeline_dead_letters_state_idx
  ON sessions.pipeline_dead_letters (state, stage, first_dead_at);
-- From a trace to its dead letters.
CREATE INDEX pipeline_dead_letters_trace_idx ON sessions.pipeline_dead_letters (trace_id);

-- Create the monthly partitions from this month through p_months_ahead months out,
-- drop each partition whose whole range is older than p_retain, delete expired rows
-- that landed in the default partition, and delete dead letters resolved more than
-- 30 days ago (unresolved ones stay until someone clears them). Idempotent; run
-- daily from the Worker's cron over REST as service_role. p_retain can lengthen
-- retention but never shorten it below the 90-day policy. SECURITY DEFINER:
-- service_role has USAGE but not CREATE on the schema, and should not; partitions
-- it creates are owned by the migration owner, like the ones created at apply time.
-- EXECUTE is revoked from PUBLIC and granted to service_role only, below.
CREATE FUNCTION sessions.pipeline_events_maintain(
  p_retain interval DEFAULT interval '90 days',
  p_months_ahead integer DEFAULT 2
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $maintain$
DECLARE
  v_month date;
  v_name text;
  v_part record;
  v_cutoff timestamptz := now() - p_retain;
BEGIN
  -- The caller is service_role over REST; bound what it can ask for. Check the
  -- computed cutoff, not the interval: intervals compare months as 30 days, but
  -- now() - '3 months' subtracts calendar months and can land under 90 days ago.
  IF v_cutoff IS NULL OR v_cutoff > now() - interval '90 days' THEN
    RAISE EXCEPTION 'pipeline_events retention must reach back at least 90 days, got %', p_retain;
  END IF;
  IF p_months_ahead IS NULL OR p_months_ahead NOT BETWEEN 0 AND 12 THEN
    RAISE EXCEPTION 'p_months_ahead must be between 0 and 12, got %', p_months_ahead;
  END IF;

  FOR i IN 0..p_months_ahead LOOP
    v_month := (date_trunc('month', now() AT TIME ZONE 'UTC') + make_interval(months => i))::date;
    v_name := 'pipeline_events_' || to_char(v_month, 'YYYY_MM');
    CONTINUE WHEN to_regclass('sessions.' || v_name) IS NOT NULL;
    -- Attaching a range the default partition has rows for would fail, so build the
    -- partition standalone, move those rows in, then attach. The lock keeps writers
    -- from adding rows for that range to the default partition between the move and
    -- the attach; it is held to commit, which for a daily run is milliseconds.
    LOCK TABLE sessions.pipeline_events_default IN SHARE ROW EXCLUSIVE MODE;
    EXECUTE format('CREATE TABLE sessions.%I (LIKE sessions.pipeline_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_name);
    EXECUTE format(
      'WITH moved AS (DELETE FROM sessions.pipeline_events_default
                        WHERE at >= %L::timestamptz AND at < %L::timestamptz RETURNING *)
       INSERT INTO sessions.%I SELECT * FROM moved',
      v_month::timestamp AT TIME ZONE 'UTC',
      (v_month + interval '1 month')::timestamp AT TIME ZONE 'UTC',
      v_name);
    EXECUTE format(
      'ALTER TABLE sessions.pipeline_events ATTACH PARTITION sessions.%I FOR VALUES FROM (%L) TO (%L)',
      v_name,
      v_month::timestamp AT TIME ZONE 'UTC',
      (v_month + interval '1 month')::timestamp AT TIME ZONE 'UTC');
    -- Reads go through the parent's policies; a partition named directly has none.
    EXECUTE format('ALTER TABLE sessions.%I ENABLE ROW LEVEL SECURITY', v_name);
    EXECUTE format('REVOKE ALL ON sessions.%I FROM PUBLIC, anon, authenticated', v_name);
  END LOOP;

  FOR v_part IN
    SELECT c.relname
      FROM pg_inherits AS i
      JOIN pg_class AS c ON c.oid = i.inhrelid
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE i.inhparent = 'sessions.pipeline_events'::regclass
       AND n.nspname = 'sessions'
       AND c.relname ~ '^pipeline_events_[0-9]{4}_[0-9]{2}$'
       AND (to_date(substr(c.relname, 17), 'YYYY_MM') + interval '1 month')::timestamp AT TIME ZONE 'UTC'
           <= v_cutoff
  LOOP
    EXECUTE format('DROP TABLE sessions.%I', v_part.relname);
  END LOOP;

  -- Rows dated before every monthly partition (late or replayed events) live in the
  -- default partition; expire them on the same clock.
  DELETE FROM sessions.pipeline_events_default WHERE at < v_cutoff;

  DELETE FROM sessions.pipeline_dead_letters
   WHERE state = 'resolved' AND resolved_at < now() - interval '30 days';
END
$maintain$;

DO $$ BEGIN PERFORM sessions.pipeline_events_maintain(); END $$;

-- Revision-fenced batch upsert from manifests. p_rows is a JSON array of objects
-- whose keys are session_index column names. Within the batch the highest revision
-- per manifest (session and machine) wins; against the table a row applies only when its revision is
-- greater than the stored one, so replays and out-of-order events are no-ops.
-- state/state_classified_at are owned by set_session_state(): an upsert sets them
-- on insert only. indexed_at is always the time of the write.
-- Returns one row per manifest in the batch: revision is the revision stored after
-- the call, input_revision the one offered; applied = false means it was stale.
-- The batch as session_index rows, highest revision per manifest.
CREATE FUNCTION sessions.session_index_batch(p_rows jsonb)
RETURNS SETOF sessions.session_index LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $batch$
  SELECT DISTINCT ON (r.org_id, r.workspace_id, r.harness, r.session_id, r.machine_id) r.*
    FROM jsonb_populate_recordset(NULL::sessions.session_index, p_rows) AS r
   ORDER BY r.org_id, r.workspace_id, r.harness, r.session_id, r.machine_id,
            r.revision DESC NULLS LAST
$batch$;

CREATE FUNCTION sessions.upsert_session_index(p_rows jsonb)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text, machine_id text,
  revision bigint, input_revision bigint, applied boolean
) LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $upsert$
#variable_conflict use_column
DECLARE
  v_written jsonb;
BEGIN
  WITH input AS (SELECT * FROM sessions.session_index_batch(p_rows)),
  written AS (
    INSERT INTO sessions.session_index AS s (
      org_id, workspace_id, harness, session_id, machine_id, user_id, project_id,
      title, state, state_classified_at, created_at, last_activity_at,
      git_repo, git_branch, git_commit, model,
      event_count, input_tokens, output_tokens, cost_usd_micros,
      storage_provider, bucket, manifest_key, content_sha256, raw_bytes, chunk_count,
      raw_format, format_version, normalized_version, manifest_etag,
      revision, trace_id, deleted_at, indexed_at)
    SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id, i.user_id,
           i.project_id, left(i.title, 200), coalesce(i.state, 'unknown'),
           i.state_classified_at, i.created_at, i.last_activity_at,
           i.git_repo, i.git_branch, i.git_commit, i.model,
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

-- Record a classification. Applies only when newer than the stored one, so
-- classifier retries and reorderings converge. Returns whether it applied.
CREATE FUNCTION sessions.set_session_state(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text,
  p_machine_id text, p_state text, p_classified_at timestamptz
) RETURNS boolean LANGUAGE sql
SET search_path = pg_catalog, sessions, pg_temp AS $set_state$
  WITH updated AS (
    UPDATE sessions.session_index AS s
       SET state = p_state, state_classified_at = p_classified_at
     WHERE s.org_id = p_org_id AND s.workspace_id = p_workspace_id
       AND s.harness = p_harness AND s.session_id = p_session_id
       AND s.machine_id = p_machine_id
       AND (s.state_classified_at IS NULL OR s.state_classified_at < p_classified_at)
    RETURNING 1
  )
  SELECT EXISTS (SELECT 1 FROM updated)
$set_state$;

-- Record that a machine was seen. Aliases accumulate; times only widen.
CREATE FUNCTION sessions.upsert_machine(
  p_org_id text, p_machine_id text, p_machine_name text, p_hostname text,
  p_os text, p_client_version text, p_seen_at timestamptz
) RETURNS void LANGUAGE sql
SET search_path = pg_catalog, sessions, pg_temp AS $upsert_machine$
  INSERT INTO sessions.machines AS m (
    org_id, machine_id, machine_name, hostname_aliases, os, client_version,
    first_seen_at, last_seen_at)
  VALUES (p_org_id, p_machine_id, p_machine_name,
          CASE WHEN p_hostname IS NULL THEN '{}' ELSE ARRAY[p_hostname] END,
          p_os, p_client_version, p_seen_at, p_seen_at)
  ON CONFLICT ON CONSTRAINT machines_pkey DO UPDATE SET
    machine_name = CASE WHEN excluded.last_seen_at >= m.last_seen_at
                        THEN coalesce(excluded.machine_name, m.machine_name) ELSE m.machine_name END,
    os = CASE WHEN excluded.last_seen_at >= m.last_seen_at
              THEN coalesce(excluded.os, m.os) ELSE m.os END,
    client_version = CASE WHEN excluded.last_seen_at >= m.last_seen_at
                          THEN coalesce(excluded.client_version, m.client_version) ELSE m.client_version END,
    hostname_aliases = CASE WHEN p_hostname IS NULL OR p_hostname = ANY (m.hostname_aliases)
                            THEN m.hostname_aliases ELSE m.hostname_aliases || p_hostname END,
    first_seen_at = least(m.first_seen_at, excluded.first_seen_at),
    last_seen_at = greatest(m.last_seen_at, excluded.last_seen_at)
$upsert_machine$;

-- Tenancy from the caller's Supabase JWT:
--   app_metadata.org_id         text
--   app_metadata.workspace_ids  text[] (JSON array)
-- Membership lives behind these two functions so it can move to a synced
-- membership table without touching any policy.
CREATE FUNCTION sessions.jwt_org_id() RETURNS text LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp AS $jwt_org$
  SELECT nullif(current_setting('request.jwt.claims', true), '')::jsonb
           -> 'app_metadata' ->> 'org_id'
$jwt_org$;

CREATE FUNCTION sessions.jwt_workspace_ids() RETURNS text[] LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp AS $jwt_ws$
  SELECT coalesce(array_agg(v), '{}')
    FROM jsonb_array_elements_text(
           coalesce(nullif(current_setting('request.jwt.claims', true), '')::jsonb
                      -> 'app_metadata' -> 'workspace_ids', '[]'::jsonb)) AS v
$jwt_ws$;

ALTER TABLE sessions.session_index ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.machines ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.pipeline_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.pipeline_dead_letters ENABLE ROW LEVEL SECURITY;

-- Workspace membership test for the policy. Deliberately plpgsql (never inlined):
-- inlined, `workspace_id = ANY (claims)` becomes an index condition with a guessed
-- ~2% selectivity, and an org-wide page then reads every row of the org through
-- si_ws_recent and sorts them (measured 19.7 ms at 100k rows) instead of walking
-- si_org_recent (0.4 ms). As an opaque function it is a cheap filter with the
-- default 1/3 selectivity, so index choice follows the query's own predicates.
CREATE FUNCTION sessions.member_of(p_workspace_id text, p_workspace_ids text[])
RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE COST 1
SET search_path = pg_catalog, pg_temp AS $member_of$
BEGIN
  RETURN p_workspace_id = ANY (p_workspace_ids);
END
$member_of$;

-- The claim lookups are scalar subqueries so they run once per statement (an
-- InitPlan), not once per row. Note for query authors: under RLS only LEAKPROOF
-- operators become index conditions; keep filterable columns text/timestamptz/uuid.
CREATE POLICY session_index_member_read ON sessions.session_index
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id())
         AND sessions.member_of(workspace_id, (SELECT sessions.jwt_workspace_ids())));

CREATE POLICY machines_member_read ON sessions.machines
  FOR SELECT TO authenticated
  USING (org_id = (SELECT sessions.jwt_org_id()));
-- pipeline_events, pipeline_dead_letters: no policy, so only service_role
-- (BYPASSRLS) can touch them.

REVOKE ALL ON SCHEMA sessions FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA sessions FROM PUBLIC, anon, authenticated;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC, anon, authenticated;

GRANT USAGE ON SCHEMA sessions TO authenticated, service_role;
GRANT SELECT ON sessions.session_index, sessions.machines TO authenticated;
GRANT EXECUTE ON FUNCTION sessions.jwt_org_id(), sessions.jwt_workspace_ids(),
  sessions.member_of(text, text[]) TO authenticated;

GRANT ALL ON ALL TABLES IN SCHEMA sessions TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO service_role;
