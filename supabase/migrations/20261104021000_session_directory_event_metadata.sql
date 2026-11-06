-- Append-only release name; authored originally as 20261103010000_session_directory_event_metadata.sql.
-- Original name never released/applied; original statement bodies preserved.
-- Complete canonical-event metadata only; no prompt, transcript, opening-summary or
-- exact activity projection. Existing rows are NOT ready until controlled replay.
ALTER TABLE sessions.session_directory
 ADD COLUMN project_name text CHECK(project_name IS NULL OR (char_length(project_name) BETWEEN 1 AND 200 AND project_name=lower(btrim(project_name)) AND project_name !~ '[[:cntrl:]/\\]')),
 ADD COLUMN event_first_ts timestamptz,
 ADD COLUMN event_last_ts timestamptz,
 ADD COLUMN kinds text[] NOT NULL DEFAULT '{}' CHECK(cardinality(kinds)<=64),
 ADD COLUMN event_user_ids text[] NOT NULL DEFAULT '{}' CHECK(cardinality(event_user_ids)<=1000),
 ADD COLUMN event_machine_ids text[] NOT NULL DEFAULT '{}' CHECK(cardinality(event_machine_ids)<=1000),
 ADD COLUMN event_models text[] NOT NULL DEFAULT '{}' CHECK(cardinality(event_models)<=1000),
 ADD COLUMN event_project_id text,
 ADD COLUMN event_project_event jsonb CHECK(event_project_event IS NULL OR jsonb_typeof(event_project_event)='object'),
 ADD COLUMN project_requires_hydration boolean NOT NULL DEFAULT false,
 ADD CONSTRAINT session_directory_project_locator CHECK(project_requires_hydration=(event_project_event IS NOT NULL) AND (NOT project_requires_hydration OR event_project_id IS NULL)),
 ADD COLUMN opening_event jsonb CHECK(opening_event IS NULL OR jsonb_typeof(opening_event)='object'),
 ADD COLUMN task_refs jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(task_refs)='array' AND jsonb_array_length(task_refs)<=1000 AND octet_length(task_refs::text)<=131072),
 ADD COLUMN reasoning_tokens bigint NOT NULL DEFAULT 0 CHECK(reasoning_tokens>=0),
 ADD COLUMN cache_read_tokens bigint NOT NULL DEFAULT 0 CHECK(cache_read_tokens>=0),
 ADD COLUMN cache_create_tokens bigint NOT NULL DEFAULT 0 CHECK(cache_create_tokens>=0),
 ADD COLUMN cache_create_5m_tokens bigint CHECK(cache_create_5m_tokens>=0),
 ADD COLUMN cache_create_1h_tokens bigint CHECK(cache_create_1h_tokens>=0),
 ADD COLUMN work_status text CHECK(work_status IN ('active','idle','finished')),
 ADD COLUMN work_status_at timestamptz,
 ADD COLUMN event_projection_epoch bigint NOT NULL DEFAULT 0 CHECK(event_projection_epoch>=0),
 ADD COLUMN event_projection_version smallint NOT NULL DEFAULT 0 CHECK(event_projection_version BETWEEN 0 AND 1),
 ADD COLUMN event_projection_ready boolean NOT NULL DEFAULT false,
 ADD COLUMN event_projected_at timestamptz,
 ADD COLUMN event_projection_sources jsonb NOT NULL DEFAULT '{}',
 ADD CONSTRAINT session_directory_events_ready CHECK(NOT event_projection_ready OR (event_projection_version=1 AND event_projected_at IS NOT NULL AND event_projection_sources<>'{}'::jsonb)),
 ADD CONSTRAINT session_directory_work_signal CHECK((work_status IS NULL)=(work_status_at IS NULL));

-- Dirty scopes include phantom pointers with no directory row. Epochs persist
-- after reconciliation, so deleting/recreating a marker cannot cause ABA.
CREATE TABLE sessions.session_directory_event_pending(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,source_session_id text NOT NULL,
 epoch bigint NOT NULL DEFAULT 1 CHECK(epoch>=1),dirty boolean NOT NULL DEFAULT true,
 PRIMARY KEY(org_id,workspace_id,harness,source_session_id)
);
CREATE INDEX session_directory_event_pending_dirty_idx ON sessions.session_directory_event_pending(org_id,workspace_id) WHERE dirty;
CREATE INDEX session_directory_event_unready_idx ON sessions.session_directory(org_id,workspace_id) WHERE deleted_at IS NULL AND (NOT event_projection_ready OR event_projection_version<>1);
ALTER TABLE sessions.session_directory_event_pending ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_event_pending FROM PUBLIC,anon,authenticated;
GRANT ALL ON sessions.session_directory_event_pending TO service_role;
INSERT INTO sessions.session_directory_event_pending(org_id,workspace_id,harness,source_session_id)
 SELECT DISTINCT org_id,workspace_id,harness,session_id FROM sessions.session_index WHERE deleted_at IS NULL;
-- Existing remote canonicalization is the authority; never basename cwd or
-- unproven relative project_id strings. Retain project_key/git_repo provenance.
CREATE FUNCTION sessions.session_directory_project_name()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,sessions,pg_temp AS $name$
DECLARE remote text;value text;
BEGIN
 NEW.project_name:=NULL;
 IF NEW.deleted_at IS NOT NULL THEN RETURN NEW; END IF;
 remote:=sessions.legacy_remote(NEW.git_repo);
 IF remote IS NULL AND NEW.project_key_method IS NOT NULL AND NEW.project_key_method NOT IN ('path','path_fallback')
  AND NEW.project_key !~ '^(?:[/~\\]|\.{1,2}/|[A-Za-z]:|file:)' THEN
  remote:=NEW.project_key;
 END IF;
 IF remote IS NOT NULL AND position('/' IN remote)>0 THEN
  value:=lower(regexp_replace(remote,'^.*/',''));
  IF value<>'' AND value NOT IN ('.','..') AND char_length(value)<=200 AND value !~ '[[:cntrl:]/\\]' THEN NEW.project_name:=value; END IF;
 END IF;
 RETURN NEW;
END $name$;
CREATE TRIGGER session_directory_project_name BEFORE INSERT OR UPDATE OF git_repo,project_key,project_key_method ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_project_name();
CREATE INDEX session_directory_events_recent_idx ON sessions.session_directory(org_id,workspace_id,event_last_ts DESC,session_id DESC,harness DESC) WHERE deleted_at IS NULL AND event_count>0;
CREATE INDEX session_directory_project_name_recent_idx ON sessions.session_directory(org_id,workspace_id,project_name,event_last_ts DESC,session_id DESC,harness DESC) WHERE deleted_at IS NULL AND project_name IS NOT NULL;
-- Pointer state-only classification does not invalidate event metadata. New source
-- revisions, machines, keys, canonical identities or deletions do, atomically with
-- the pointer write (including a phantom machine inserted during reconciliation).
CREATE FUNCTION sessions.session_directory_events_changed()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,sessions,pg_temp AS $changed$
BEGIN
 IF TG_OP='UPDATE' AND ROW(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id,OLD.machine_id,OLD.canonical_session_id,OLD.revision,OLD.manifest_key,OLD.deleted_at)
  IS NOT DISTINCT FROM ROW(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id,NEW.machine_id,NEW.canonical_session_id,NEW.revision,NEW.manifest_key,NEW.deleted_at) THEN RETURN NEW; END IF;
 IF TG_OP<>'INSERT' THEN
  INSERT INTO sessions.session_directory_event_pending AS m(org_id,workspace_id,harness,source_session_id)
   VALUES(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id)
   ON CONFLICT(org_id,workspace_id,harness,source_session_id) DO UPDATE SET epoch=m.epoch+1,dirty=true;
 END IF;
 IF TG_OP='INSERT' OR (TG_OP='UPDATE' AND ROW(OLD.org_id,OLD.workspace_id,OLD.harness,OLD.session_id) IS DISTINCT FROM ROW(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id)) THEN
  INSERT INTO sessions.session_directory_event_pending AS m(org_id,workspace_id,harness,source_session_id)
   VALUES(NEW.org_id,NEW.workspace_id,NEW.harness,NEW.session_id)
   ON CONFLICT(org_id,workspace_id,harness,source_session_id) DO UPDATE SET epoch=m.epoch+1,dirty=true;
 END IF;
 IF TG_OP='INSERT' THEN
  UPDATE sessions.session_directory SET event_projection_ready=false,event_projection_epoch=event_projection_epoch+1
   WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND harness=NEW.harness
    AND session_id IN (NEW.session_id,coalesce(NEW.canonical_session_id,NEW.session_id));
  RETURN NEW;
 ELSIF TG_OP='DELETE' THEN
  UPDATE sessions.session_directory SET event_projection_ready=false,event_projection_epoch=event_projection_epoch+1
   WHERE org_id=OLD.org_id AND workspace_id=OLD.workspace_id AND harness=OLD.harness
    AND session_id IN (OLD.session_id,coalesce(OLD.canonical_session_id,OLD.session_id));
  RETURN OLD;
 ELSE
  UPDATE sessions.session_directory SET event_projection_ready=false,event_projection_epoch=event_projection_epoch+1
   WHERE (org_id=OLD.org_id AND workspace_id=OLD.workspace_id AND harness=OLD.harness
      AND session_id IN (OLD.session_id,coalesce(OLD.canonical_session_id,OLD.session_id)))
    OR (org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND harness=NEW.harness
      AND session_id IN (NEW.session_id,coalesce(NEW.canonical_session_id,NEW.session_id)));
  RETURN NEW;
 END IF;
END
$changed$;
CREATE TRIGGER session_directory_events_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_events_changed();

CREATE FUNCTION sessions.session_directory_events_tombstone()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $tombstone$
BEGIN
 IF NEW.deleted_at IS NOT NULL THEN
  NEW.event_first_ts:=NULL;NEW.event_last_ts:=NULL;NEW.kinds:='{}';NEW.task_refs:='[]';NEW.task_title:=NULL;
  NEW.event_user_ids:='{}';NEW.event_machine_ids:='{}';NEW.event_models:='{}';NEW.event_project_id:=NULL;NEW.opening_event:=NULL;NEW.project_name:=NULL;NEW.event_project_event:=NULL;NEW.project_requires_hydration:=false;
  NEW.event_count:=0;NEW.input_tokens:=0;NEW.output_tokens:=0;NEW.cost_usd_micros:=NULL;
  NEW.reasoning_tokens:=0;NEW.cache_read_tokens:=0;NEW.cache_create_tokens:=0;
  NEW.cache_create_5m_tokens:=NULL;NEW.cache_create_1h_tokens:=NULL;
  NEW.work_status:=NULL;NEW.work_status_at:=NULL;
  NEW.event_projection_version:=0;NEW.event_projection_ready:=false;
  NEW.event_projected_at:=NULL;NEW.event_projection_sources:='{}';
 END IF;
 RETURN NEW;
END
$tombstone$;
CREATE TRIGGER session_directory_events_tombstone BEFORE INSERT OR UPDATE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_events_tombstone();


-- One content-free timestamp per logical session, preserving microseconds. A
-- session/day count is existence, so equal timestamps need only one index row.
CREATE TABLE sessions.session_directory_event_times(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,event_ts timestamptz NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id,event_ts),
 FOREIGN KEY(org_id,workspace_id,harness,session_id) REFERENCES sessions.session_directory(org_id,workspace_id,harness,session_id) ON DELETE CASCADE
);
CREATE INDEX session_directory_event_times_window_idx ON sessions.session_directory_event_times(org_id,workspace_id,event_ts,harness,session_id);
ALTER TABLE sessions.session_directory_event_times ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_event_times FROM PUBLIC,anon,authenticated;
GRANT ALL ON sessions.session_directory_event_times TO service_role;
CREATE FUNCTION sessions.session_directory_event_times_forget()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,sessions,pg_temp AS $forget$
BEGIN
 IF NEW.deleted_at IS NOT NULL THEN
  DELETE FROM sessions.session_directory_event_times WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND harness=NEW.harness AND session_id=NEW.session_id;
 END IF;
 RETURN NEW;
END $forget$;
CREATE TRIGGER session_directory_event_times_forget AFTER UPDATE OF deleted_at ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_event_times_forget();

-- Workspace-wide attestation comes BEFORE filters or pagination: stale values
-- cannot hide an unready session from a selective page. Current live pointers
-- are grouped once, not re-scanned per directory row. No transcript is read.
CREATE FUNCTION sessions.session_directory_events_require_ready(p_workspace_id text)
RETURNS text LANGUAGE plpgsql STABLE
SET search_path=pg_catalog,sessions,pg_temp AS $ready$
DECLARE v_org text;
BEGIN
 SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) s;
 IF EXISTS(SELECT 1 FROM sessions.session_directory_event_pending WHERE org_id=v_org AND workspace_id=p_workspace_id AND dirty)
 OR EXISTS(SELECT 1 FROM sessions.session_directory WHERE org_id=v_org AND workspace_id=p_workspace_id AND deleted_at IS NULL AND (NOT event_projection_ready OR event_projection_version<>1)) THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='session directory: event_projection_not_ready';
 END IF;
 RETURN v_org;
END $ready$;
CREATE FUNCTION sessions.session_directory_events_list_sql(
  p_source_count integer, p_has_project boolean, p_user_count integer, p_has_agent boolean,
  p_has_last_after boolean, p_has_last_before boolean,
  p_has_first_after boolean, p_has_first_before boolean, p_has_keys boolean, p_has_cursor boolean,
  p_has_not_sources boolean, p_has_project_name boolean DEFAULT false
) RETURNS text LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, pg_temp AS $list_sql$
DECLARE
  v_users integer := coalesce(p_user_count, 0);
  v_where text;
BEGIN
  -- One sort key for the order and the cursor: the logical row's
  -- (last_activity_at, session_id, harness). The companions (agents, users) are
  -- membership filters only, never the order, so a companion row whose copied
  -- last_activity_at lags the directory row can neither repeat nor skip a session
  -- across pages (H1, #124 review). The planner may still drive from a companion's
  -- index to find a selective agent's or user's sessions, then sort them.
  v_where := ' WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL AND d.event_count>0 AND d.event_first_ts IS NOT NULL AND d.event_last_ts IS NOT NULL'
    || CASE WHEN p_has_agent THEN
         ' AND (d.org_id, d.workspace_id, d.harness, d.session_id) IN (SELECT a.org_id, a.workspace_id, a.harness, a.session_id'
         || ' FROM sessions.session_directory_agents AS a'
         || ' WHERE a.org_id = $1 AND a.workspace_id = $2 AND a.agent_name_normalized = $6)'
       ELSE '' END
    || CASE WHEN v_users > 0 THEN ' AND d.event_user_ids && $5::text[]' ELSE '' END
    -- One source is an ordered equality on session_directory_harness_recent_idx.
    || CASE WHEN coalesce(p_source_count, 0) = 1 THEN ' AND d.harness = ($3)[1]'
            WHEN coalesce(p_source_count, 0) > 1 THEN ' AND d.harness = ANY ($3)' ELSE '' END
    || CASE WHEN p_has_not_sources THEN ' AND NOT (d.harness = ANY ($17::text[]))' ELSE '' END
    || CASE WHEN p_has_project THEN
         ' AND (d.event_project_id IS NOT NULL OR d.project_key IS NOT NULL OR d.git_repo IS NOT NULL)'
         || ' AND sessions.session_directory_project_text(d.event_project_id, d.project_key, d.git_repo) ILIKE $4 ESCAPE ''\'''
       ELSE '' END
    || CASE WHEN p_has_last_after THEN ' AND d.event_last_ts >= $7' ELSE '' END
    || CASE WHEN p_has_last_before THEN ' AND d.event_last_ts < $8' ELSE '' END
    || CASE WHEN p_has_first_after THEN ' AND d.event_first_ts >= $9' ELSE '' END
    || CASE WHEN p_has_first_before THEN ' AND d.event_first_ts < $10' ELSE '' END
    || CASE WHEN p_has_keys THEN
         ' AND (d.harness, d.session_id) IN (SELECT k.h, k.s FROM unnest($15::text[], $16::text[]) AS k(h, s))'
       ELSE '' END
    || CASE WHEN p_has_cursor THEN
         ' AND (d.event_last_ts, d.session_id, d.harness) < ($11, $12, $13)'
       ELSE '' END
    || CASE WHEN p_has_project_name THEN ' AND d.project_name=$18 AND d.project_name IS NOT NULL' ELSE '' END;
  RETURN 'SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.event_first_ts AS created_at, d.event_last_ts AS last_activity_at,'
    || ' d.title, d.task_title, d.agent_names, d.event_project_id AS project_id, d.project_key, d.project_key_method,'
    || ' d.git_repo, d.git_branch, d.initial_commit, d.originator, d.agent_version,'
    || ' d.event_models AS models,'
    || ' d.state, d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,'
    || ' d.event_machine_ids AS machine_ids, d.event_user_ids AS user_ids,'
    -- Parents: live sessions whose delivered relationships name this one as their child
    -- (a tombstoned parent's edges are deleted with it).
    || ' ARRAY(SELECT DISTINCT r.parent_session_id FROM sessions.session_directory_relationships AS r'
    ||       ' WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness'
    ||       ' AND r.child_session_id = d.session_id ORDER BY 1 LIMIT 64) AS parent_session_ids,'
    -- Children: relationship rows naming this session as parent, linked or not (an
    -- erased child's edge is deleted with it).
    || ' (SELECT count(*) FROM sessions.session_directory_relationships AS r'
    ||       ' WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness'
    ||       ' AND r.parent_session_id = d.session_id)::integer AS child_session_count,'
    || ' d.short_summary, d.short_summary_generated_at, d.catalog_projected_at, d.projected_at'
    || ' ,d.kinds,d.task_refs,d.reasoning_tokens,d.cache_read_tokens,d.cache_create_tokens,'
    || ' d.cache_create_5m_tokens,d.cache_create_1h_tokens,d.work_status,d.work_status_at,'
    || ' d.event_projection_version,d.event_projection_ready,d.event_projected_at,d.opening_event,d.event_projection_epoch,d.event_project_event,d.project_requires_hydration,d.project_name'
    || ' FROM sessions.session_directory AS d' || v_where
    || ' ORDER BY d.event_last_ts DESC, d.session_id DESC, d.harness DESC LIMIT $14';
END
$list_sql$;
CREATE FUNCTION sessions.list_session_directory_events(
  p_workspace_id text,
  p_sources text[] DEFAULT NULL,
  p_not_sources text[] DEFAULT NULL,
  p_project_contains text DEFAULT NULL,
  p_user_ids text[] DEFAULT NULL,
  p_agent text DEFAULT NULL,
  p_last_ts_after timestamptz DEFAULT NULL,
  p_last_ts_before timestamptz DEFAULT NULL,
  p_first_ts_after timestamptz DEFAULT NULL,
  p_first_ts_before timestamptz DEFAULT NULL,
  p_session_harnesses text[] DEFAULT NULL,
  p_session_ids text[] DEFAULT NULL,
  p_cursor_last_activity_at timestamptz DEFAULT NULL,
  p_cursor_session_id text DEFAULT NULL,
  p_cursor_harness text DEFAULT NULL,
  p_limit integer DEFAULT 50,
  p_project_name text DEFAULT NULL
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  created_at timestamptz, last_activity_at timestamptz,
  title text, task_title text, agent_names text[], project_id text,
  project_key text, project_key_method text, git_repo text, git_branch text,
  initial_commit text, originator text, agent_version text, models text[],
  state text, event_count bigint, input_tokens bigint, output_tokens bigint,
  cost_usd_micros bigint, machine_ids text[], user_ids text[],
  parent_session_ids text[], child_session_count integer,
  short_summary text, short_summary_generated_at timestamptz,
  catalog_projected_at timestamptz, projected_at timestamptz,
  kinds text[], task_refs jsonb, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint, cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, work_status text, work_status_at timestamptz, event_projection_version smallint, event_projection_ready boolean, event_projected_at timestamptz, opening_event jsonb, event_projection_epoch bigint, event_project_event jsonb, project_requires_hydration boolean, project_name text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $list$
DECLARE
  v_org text;
  v_sources text[];
  v_not_sources text[];
  v_users text[];
  v_agent text;
  v_project text;
  v_has_cursor boolean;
  v_project_name text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: workspace is required';
  END IF;
  -- Role guard and signed-claims pins; refuses (42501) before any value is read.
  SELECT sessions.session_directory_events_require_ready(p_workspace_id) INTO v_org;

  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 200 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: limit must be 1 to 200';
  END IF;
  IF (p_session_harnesses IS NULL) <> (p_session_ids IS NULL)
     OR (p_session_ids IS NOT NULL AND (
          cardinality(p_session_ids) NOT BETWEEN 1 AND 200
          OR cardinality(p_session_harnesses) <> cardinality(p_session_ids)
          OR EXISTS (SELECT 1 FROM unnest(p_session_harnesses, p_session_ids) AS k(h, s)
                      WHERE h IS NULL OR h !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
                         OR s IS NULL OR char_length(s) NOT BETWEEN 1 AND 255))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023',
      MESSAGE = 'session list: session keys must be 1 to 200 (harness, session id) pairs';
  END IF;
  IF p_sources IS NOT NULL AND cardinality(p_sources) > 0 THEN
    IF cardinality(p_sources) > 16 OR EXISTS (
         SELECT 1 FROM unnest(p_sources) AS s(v)
          WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$') THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: sources must be 1 to 16 valid harness names';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_sources FROM unnest(p_sources) AS s(v);
  END IF;
  IF p_not_sources IS NOT NULL AND cardinality(p_not_sources) > 0 THEN
    IF cardinality(p_not_sources) > 16 OR EXISTS (
         SELECT 1 FROM unnest(p_not_sources) AS s(v)
          WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$') THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: not_sources must be 1 to 16 valid harness names';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_not_sources FROM unnest(p_not_sources) AS s(v);
  END IF;
  IF p_user_ids IS NOT NULL AND cardinality(p_user_ids) > 0 THEN
    IF cardinality(p_user_ids) > 20 OR EXISTS (
         SELECT 1 FROM unnest(p_user_ids) AS u(v)
          WHERE v IS NULL OR char_length(v) NOT BETWEEN 1 AND 255) THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: users must be 1 to 20 ids of 1 to 255 characters';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_users FROM unnest(p_user_ids) AS u(v);
  END IF;
  IF p_agent IS NOT NULL THEN
    v_agent := lower(btrim(p_agent));
    IF v_agent = '' OR char_length(v_agent) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: agent must be 1 to 200 characters';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM sessions.session_directory_catalog_rollout AS r
                    WHERE r.singleton AND r.completed_at IS NOT NULL) THEN
      RAISE EXCEPTION USING ERRCODE = '0A000',
        MESSAGE = 'session list: agent_filter_not_ready: the directory catalog backfill is not complete';
    END IF;
  END IF;
  IF p_project_name IS NOT NULL THEN
    v_project_name:=lower(btrim(p_project_name));
    IF v_project_name='' OR char_length(v_project_name)>200 OR v_project_name ~ '[[:cntrl:]/\\]' THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session list: project_name must be a nonempty name up to 200 characters';
    END IF;
  END IF;
  IF p_project_contains IS NOT NULL THEN
    v_project := btrim(p_project_contains);
    IF v_project = '' OR char_length(v_project) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: project_contains must be 1 to 200 characters';
    END IF;
    IF EXISTS(SELECT 1 FROM sessions.session_directory d WHERE d.org_id=v_org AND d.workspace_id=p_workspace_id AND d.deleted_at IS NULL AND d.project_requires_hydration) THEN
      RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='session directory: project_filter_requires_hydration';
    END IF;
  END IF;
  IF (p_last_ts_after IS NOT NULL AND p_last_ts_before IS NOT NULL AND p_last_ts_after > p_last_ts_before)
     OR (p_first_ts_after IS NOT NULL AND p_first_ts_before IS NOT NULL AND p_first_ts_after > p_first_ts_before) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: an after bound is later than its before bound';
  END IF;
  v_has_cursor := p_cursor_last_activity_at IS NOT NULL OR p_cursor_session_id IS NOT NULL
    OR p_cursor_harness IS NOT NULL;
  IF v_has_cursor AND (p_cursor_last_activity_at IS NULL OR p_cursor_session_id IS NULL
       OR p_cursor_harness IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: cursor is incomplete';
  END IF;

  RETURN QUERY EXECUTE sessions.session_directory_events_list_sql(
      coalesce(cardinality(v_sources), 0), v_project IS NOT NULL, coalesce(cardinality(v_users), 0),
      v_agent IS NOT NULL, p_last_ts_after IS NOT NULL, p_last_ts_before IS NOT NULL,
      p_first_ts_after IS NOT NULL, p_first_ts_before IS NOT NULL, p_session_ids IS NOT NULL,
      v_has_cursor, v_not_sources IS NOT NULL, v_project_name IS NOT NULL)
  USING v_org, p_workspace_id, v_sources,
        CASE WHEN v_project IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_project, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        v_users, v_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before,
        p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_limit + 1,
        p_session_harnesses, p_session_ids, v_not_sources, v_project_name;
END
$list$;
CREATE FUNCTION sessions.session_directory_event_activity(p_workspace_id text,p_since timestamptz,p_until timestamptz)
RETURNS TABLE(day date,harness text,sessions bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $activity$
DECLARE v_org text;
BEGIN
 IF p_workspace_id IS NULL OR p_since IS NULL OR p_until IS NULL OR p_until<p_since THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session event activity: workspace and ordered window are required';
 END IF;
 IF (p_until AT TIME ZONE 'UTC')::date-(p_since AT TIME ZONE 'UTC')::date+1>366 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session event activity: at most 366 UTC days';
 END IF;
 v_org:=sessions.session_directory_events_require_ready(p_workspace_id);
 RETURN QUERY SELECT (t.event_ts AT TIME ZONE 'UTC')::date,t.harness,count(DISTINCT t.session_id)
  FROM sessions.session_directory_event_times t JOIN sessions.session_directory d
   ON d.org_id=t.org_id AND d.workspace_id=t.workspace_id AND d.harness=t.harness AND d.session_id=t.session_id
  WHERE t.org_id=v_org AND t.workspace_id=p_workspace_id AND d.deleted_at IS NULL
   AND t.event_ts>=p_since AND t.event_ts<=p_until
  GROUP BY 1,2 ORDER BY 1,2;
END $activity$;

REVOKE ALL ON FUNCTION sessions.session_directory_project_name() FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_events_changed() FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_events_tombstone() FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_event_times_forget() FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_events_require_ready(text) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_events_list_sql(integer,boolean,integer,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,text[],text[],timestamptz,text,text,integer,text) FROM PUBLIC,anon,authenticated,service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_event_activity(text,timestamptz,timestamptz) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,text[],text[],timestamptz,text,text,integer,text) TO session_directory_search_worker;
GRANT EXECUTE ON FUNCTION sessions.session_directory_event_activity(text,timestamptz,timestamptz) TO session_directory_search_worker;
DO $acl$ DECLARE f regprocedure;g text; BEGIN
 FOREACH f IN ARRAY ARRAY['sessions.session_directory_project_name()'::regprocedure,'sessions.session_directory_events_changed()'::regprocedure,'sessions.session_directory_events_tombstone()'::regprocedure,'sessions.session_directory_event_times_forget()'::regprocedure,'sessions.session_directory_events_require_ready(text)'::regprocedure,'sessions.session_directory_events_list_sql(integer,boolean,integer,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean)'::regprocedure,'sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,text[],text[],timestamptz,text,text,integer,text)'::regprocedure,'sessions.session_directory_event_activity(text,timestamptz,timestamptz)'::regprocedure] LOOP
  FOR g IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=f AND a.grantee<>p.proowner AND NOT(r.rolname='session_directory_search_worker' AND f IN ('sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,text[],text[],timestamptz,text,text,integer,text)'::regprocedure,'sessions.session_directory_event_activity(text,timestamptz,timestamptz)'::regprocedure)) LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,g);
  END LOOP;
 END LOOP;
END $acl$;
