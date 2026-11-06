-- Fast, CDC-safe lookup of sessions by their delivered root agent name.
--
-- PostgreSQL 17 logical replication does not publish generated-column values.
-- Keep the normalized name and the session's event recency as ordinary columns,
-- maintained on the Neon source. The table remains one row per catalog session
-- key: child-agent labels in session_relationships are deliberately not session
-- identities and do not belong here.
--
-- This migration installs maintenance only. Existing rows are filled in bounded,
-- resumable batches by scripts/rollout-session-agent-lookup.mjs; the migration
-- never scans or rewrites the table in one transaction.
--
-- Apply prerequisite: drain or pause delivery writers for this short DDL
-- transaction. Trigger installation touches all three projection tables. Take
-- their heavyweight locks up front in the same order as normal writers, before
-- ALTER TABLE can lock the agent table out of order. The five-second lock_timeout
-- makes a missed quiet window fail and roll back instead of waiting into a cycle;
-- retry only after writers are quiet.
--
-- This ledger migration is intentionally one-shot, like the rest of this
-- directory: apply it only through apply-neon-migration.mjs. Do not replay the
-- raw SQL after the ledger records it; the ADD COLUMN/CREATE objects are not
-- idempotent. The reviewed incident rollback is
-- scripts/rollback-session-agent-lookup.sql, not a second execution of 0067.

SELECT set_config('relayhistory.migration_saved_lock_timeout', current_setting('lock_timeout'), true),
       set_config('lock_timeout', '5s', true);

-- The DO wrapper also gives schema-only/autocommit validators a transaction
-- block. Production's migration runner already wraps every statement in one
-- transaction, so these locks remain held through all DDL until commit there.
DO $migration_locks$
BEGIN
  EXECUTE 'LOCK TABLE sessions.session_catalog IN SHARE ROW EXCLUSIVE MODE';
  EXECUTE 'LOCK TABLE sessions.session_rollups IN SHARE ROW EXCLUSIVE MODE';
  EXECUTE 'LOCK TABLE sessions.session_catalog_agents IN ACCESS EXCLUSIVE MODE';
END
$migration_locks$;

ALTER TABLE sessions.session_catalog_agents
  ADD COLUMN agent_name_normalized text,
  ADD COLUMN last_ts timestamptz;

-- Every new/re-delivered catalog row has a replication-safe normalized value.
-- On insert (or an identity move), seed recency from the maintained rollup. A
-- concurrent rollup writer either finishes first and is visible here, or updates
-- this row after waiting for this transaction's row lock.
CREATE FUNCTION sessions.lock_session_catalog_agent_lookup()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $lock$
DECLARE
  target record;
BEGIN
  -- Managed agent-row writers already hold the session lock: catalog projection
  -- enters through session_catalog, rollup refresh enters from the rollup
  -- trigger, and the bounded backfill takes it before FOR UPDATE. A direct
  -- UPDATE/DELETE on session_catalog_agents has already locked its target tuple
  -- before this row trigger runs. It must never WAIT for the session lock while
  -- a rollup writer holds that lock and waits for the same agent tuple; try and
  -- fail with 55P03 instead. session_catalog also tries without waiting: an
  -- arbitrary multi-row catalog statement has already locked/projected earlier
  -- rows in statement order, which need not match the sorted order used by
  -- rollup writers. Delivered batches pre-acquire every affected session lock
  -- in sorted order below, so their row triggers succeed re-entrantly.
  IF TG_OP = 'UPDATE'
     AND (OLD.org_id, OLD.session_id) IS DISTINCT FROM (NEW.org_id, NEW.session_id) THEN
    FOR target IN
      SELECT DISTINCT org_id, session_id
        FROM (VALUES (OLD.org_id, OLD.session_id),
                     (NEW.org_id, NEW.session_id)) AS keys(org_id, session_id)
       ORDER BY org_id, session_id
    LOOP
      IF NOT pg_try_advisory_xact_lock(
        sessions.session_rollup_lock_key(target.org_id, target.session_id)) THEN
        RAISE EXCEPTION 'session agent lookup row is busy'
          USING ERRCODE = '55P03';
      END IF;
    END LOOP;
  ELSIF TG_OP = 'DELETE' THEN
    IF NOT pg_try_advisory_xact_lock(
      sessions.session_rollup_lock_key(OLD.org_id, OLD.session_id)) THEN
      RAISE EXCEPTION 'session agent lookup row is busy'
        USING ERRCODE = '55P03';
    END IF;
  ELSE
    IF NOT pg_try_advisory_xact_lock(
      sessions.session_rollup_lock_key(NEW.org_id, NEW.session_id)) THEN
      RAISE EXCEPTION 'session agent lookup row is busy'
        USING ERRCODE = '55P03';
    END IF;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END
$lock$;

-- PostgreSQL runs same-kind triggers by name; lookup_lock precedes lookup_prepare.
CREATE TRIGGER session_catalog_agents_lookup_lock
  BEFORE INSERT OR UPDATE OR DELETE ON sessions.session_catalog_agents
  FOR EACH ROW EXECUTE FUNCTION sessions.lock_session_catalog_agent_lookup();

-- The delivery projection writes session_catalog before it upserts or deletes
-- the matching agent row. Take the same lock at that source boundary as well,
-- before ON CONFLICT or DELETE can acquire an existing agent-row lock.
CREATE TRIGGER session_catalog_agent_source_lock
  BEFORE INSERT OR UPDATE OR DELETE ON sessions.session_catalog
  FOR EACH ROW EXECUTE FUNCTION sessions.lock_session_catalog_agent_lookup();

-- A delivery batch projects activity and catalog records in record-id order,
-- which is not session order. Acquire both every submitted session and every
-- durable session those records may replace before the first row is projected.
-- Catalog records with a payload-only identity use delivery_catalog_key's
-- canonical fallback. The caller has already taken
-- 0059's user -> org -> origin -> workspace locks and passes that migration's
-- shared contention deadline, so this new stage cannot extend the wait budget.
CREATE FUNCTION sessions.delivery_session_agent_locks_within(
  p_org text, p_workspace text, p_origin text, p_records jsonb,
  p_contended_until timestamptz
) RETURNS numeric LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $agent_locks$
DECLARE
  target record;
  waited numeric;
  total_ms numeric := 0;
BEGIN
  FOR target IN
    WITH submitted AS MATERIALIZED (
      SELECT value AS item FROM jsonb_array_elements(p_records)
    ), keys AS (
      SELECT p_org AS org_id,
             coalesce(item->>'session_id', sessions.delivery_catalog_key(
               item->>'kind', item->>'source', item->>'session_id',
               item->>'operation', NULLIF(item->'payload', 'null'::jsonb))->>2) AS session_id
        FROM submitted
      UNION
      SELECT d.org_id,
             coalesce(d.session_id, sessions.delivery_catalog_key(
               d.kind, d.source, d.session_id, d.operation, d.payload)->>2) AS session_id
        FROM sessions.delivery_records AS d
        JOIN submitted AS s ON s.item->>'record_id' = d.record_id
       WHERE d.org_id = p_org AND d.workspace_id = p_workspace
         AND d.origin_id = p_origin
    )
    SELECT DISTINCT org_id, session_id
      FROM keys
     WHERE session_id IS NOT NULL
     ORDER BY org_id, session_id
  LOOP
    waited := sessions.delivery_advisory_lock_within(
      sessions.session_rollup_lock_key(target.org_id, target.session_id),
      extract(epoch FROM p_contended_until - clock_timestamp()) * 1000);
    IF waited IS NULL THEN RETURN NULL; END IF;
    total_ms := total_ms + waited;
  END LOOP;
  RETURN total_ms;
END
$agent_locks$;

-- 0059's wrapper plus the sorted session-agent lock stage. Keep the same
-- contention deadline across org/origin/workspace/session locks and report the
-- new stage independently. A busy session lock is the same retryable 55P03
-- contract as the four pre-existing delivery locks.
--
-- write-hold: sessions.upload_cleanup_ready()
ALTER FUNCTION sessions.accept_delivery_batch(text,text,text,text,text,text,jsonb,jsonb)
  RENAME TO accept_delivery_batch_before_session_agent_locks;

CREATE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE
  started timestamptz := clock_timestamp();
  step timestamptz;
  replay jsonb;
  contended_until timestamptz;
  saved text;
  waited numeric;
  busy text;
  timing jsonb := '{}'::jsonb;
  workspace_key bigint := hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || p_org || chr(31) || p_workspace, 0);
  result jsonb;
BEGIN
  IF sessions.upload_cleanup_ready() THEN
    SELECT receipt INTO replay FROM sessions.delivery_receipts
      WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin
        AND batch_id=p_batch AND digest=p_digest;
    IF FOUND THEN
      RETURN replay || jsonb_build_object('_delivery_timing', jsonb_build_object(
        'fast_path', true, 'total_ms', sessions.delivery_elapsed_ms(started)));
    END IF;
  END IF;
  PERFORM set_config('relayhistory.delivery_workspace_lock_ms', '0', true);
  PERFORM set_config('relayhistory.delivery_workspace_lock_key', '', true);
  <<locks>>
  BEGIN
    waited := sessions.delivery_advisory_lock_within(
      hashtextextended(jsonb_build_array(p_org,p_workspace,p_user)::text, 0),
      sessions.delivery_lock_budget_ms('relayhistory.delivery_user_lock_wait_ms', 10000));
    IF waited IS NULL THEN busy := 'user'; EXIT locks; END IF;
    timing := timing || jsonb_build_object('user_lock_ms', round(waited::numeric, 1));
    contended_until := clock_timestamp()
      + sessions.delivery_lock_budget_ms('relayhistory.delivery_lock_wait_ms', 2000) * interval '1 millisecond';
    waited := sessions.delivery_advisory_lock_within(
      hashtextextended('relayhistory.memory_claims:' || p_org,0),
      extract(epoch FROM contended_until - clock_timestamp()) * 1000);
    IF waited IS NULL THEN busy := 'org'; EXIT locks; END IF;
    timing := timing || jsonb_build_object('org_lock_ms', round(waited::numeric, 1));
    step := clock_timestamp();
    saved := current_setting('lock_timeout');
    PERFORM set_config('lock_timeout',
      GREATEST(1, ceil(extract(epoch FROM contended_until - clock_timestamp()) * 1000))::bigint || 'ms', true);
    BEGIN
      INSERT INTO sessions.delivery_origins VALUES (p_org,p_workspace,p_origin) ON CONFLICT DO NOTHING;
      PERFORM set_config('lock_timeout',
        GREATEST(1, ceil(extract(epoch FROM contended_until - clock_timestamp()) * 1000))::bigint || 'ms', true);
      PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
    EXCEPTION WHEN lock_not_available THEN
      busy := 'origin';
    END;
    PERFORM set_config('lock_timeout', saved, true);
    IF busy IS NOT NULL THEN EXIT locks; END IF;
    timing := timing || jsonb_build_object('origin_lock_ms', sessions.delivery_elapsed_ms(step));
    IF jsonb_array_length(p_records) > 0 THEN
      waited := sessions.delivery_advisory_lock_within(workspace_key,
        extract(epoch FROM contended_until - clock_timestamp()) * 1000);
      IF waited IS NULL THEN busy := 'workspace'; EXIT locks; END IF;
      PERFORM set_config('relayhistory.delivery_workspace_lock_ms', waited::text, true);
      PERFORM set_config('relayhistory.delivery_workspace_lock_key', workspace_key::text, true);
      timing := timing || jsonb_build_object('workspace_lock_ms', round(waited::numeric, 1));

      waited := sessions.delivery_session_agent_locks_within(
        p_org, p_workspace, p_origin, p_records, contended_until);
      IF waited IS NULL THEN busy := 'session'; EXIT locks; END IF;
      timing := timing || jsonb_build_object('session_lock_ms', round(waited::numeric, 1));
    END IF;
  END locks;
  IF busy IS NOT NULL THEN
    RAISE EXCEPTION 'delivery_busy' USING ERRCODE = '55P03',
      DETAIL = (timing || jsonb_build_object('fast_path', false, 'busy_lock', busy,
        'total_ms', sessions.delivery_elapsed_ms(started)))::text;
  END IF;
  result := sessions.accept_delivery_batch_before_reset(
    p_org,p_workspace,p_user,p_origin,p_batch,p_digest,p_records,p_receipt);
  RETURN result || jsonb_build_object('_delivery_timing', timing || jsonb_build_object(
    'fast_path', false,
    'workspace_lock_ms', round(COALESCE(NULLIF(current_setting(
      'relayhistory.delivery_workspace_lock_ms', true), '')::numeric, 0), 1),
    'total_ms', sessions.delivery_elapsed_ms(started)));
END;
$delivery$;

CREATE FUNCTION sessions.prepare_session_catalog_agent_lookup()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $prepare$
BEGIN
  NEW.agent_name_normalized := lower(NEW.agent_name);
  IF TG_OP = 'INSERT'
     OR (NEW.org_id, NEW.workspace_id, NEW.source, NEW.session_id)
        IS DISTINCT FROM (OLD.org_id, OLD.workspace_id, OLD.source, OLD.session_id) THEN
    SELECT max(r.last_ts) INTO NEW.last_ts
      FROM sessions.session_rollups AS r
     WHERE r.org_id = NEW.org_id AND r.workspace_id = NEW.workspace_id
       AND r.source = NEW.source AND r.session_id = NEW.session_id;
  END IF;
  RETURN NEW;
END
$prepare$;

CREATE TRIGGER session_catalog_agents_lookup_prepare
  BEFORE INSERT OR UPDATE ON sessions.session_catalog_agents
  FOR EACH ROW EXECUTE FUNCTION sessions.prepare_session_catalog_agent_lookup();

-- Refresh exactly the session keys changed by session_rollups. Rollup writes
-- already hold session_rollup_lock_key(org, session), so inserts and invalidating
-- updates/deletes serialize with the existing rebuild/backfill machinery.
CREATE FUNCTION sessions.refresh_session_catalog_agent_recency(p_keys jsonb)
RETURNS void LANGUAGE sql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh$
  UPDATE sessions.session_catalog_agents AS a
     SET last_ts = x.last_ts
    FROM (
      SELECT k.org_id, k.workspace_id, k.source, k.session_id, max(r.last_ts) AS last_ts
        FROM jsonb_to_recordset(p_keys) AS k(
          org_id text, workspace_id text, source text, session_id text)
        LEFT JOIN sessions.session_rollups AS r
          ON r.org_id = k.org_id AND r.workspace_id = k.workspace_id
         AND r.source = k.source AND r.session_id = k.session_id
       GROUP BY k.org_id, k.workspace_id, k.source, k.session_id
    ) AS x
   WHERE a.org_id = x.org_id AND a.workspace_id = x.workspace_id
     AND a.source = x.source AND a.session_id = x.session_id
     AND a.last_ts IS DISTINCT FROM x.last_ts
$refresh$;

CREATE FUNCTION sessions.refresh_inserted_session_catalog_agent_recency()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh_inserted$
BEGIN
  PERFORM sessions.refresh_session_catalog_agent_recency((
    SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
      'org_id', org_id, 'workspace_id', workspace_id,
      'source', source, 'session_id', session_id)), '[]'::jsonb)
      FROM new_rows));
  RETURN NULL;
END
$refresh_inserted$;

CREATE FUNCTION sessions.refresh_deleted_session_catalog_agent_recency()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh_deleted$
BEGIN
  PERFORM sessions.refresh_session_catalog_agent_recency((
    SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
      'org_id', org_id, 'workspace_id', workspace_id,
      'source', source, 'session_id', session_id)), '[]'::jsonb)
      FROM old_rows));
  RETURN NULL;
END
$refresh_deleted$;

CREATE FUNCTION sessions.refresh_updated_session_catalog_agent_recency()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh_updated$
BEGIN
  PERFORM sessions.refresh_session_catalog_agent_recency((
    SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
      'org_id', org_id, 'workspace_id', workspace_id,
      'source', source, 'session_id', session_id)), '[]'::jsonb)
      FROM (SELECT org_id, workspace_id, source, session_id FROM old_rows
            UNION
            SELECT org_id, workspace_id, source, session_id FROM new_rows) AS changed));
  RETURN NULL;
END
$refresh_updated$;

CREATE TRIGGER session_rollups_agent_recency_insert
  AFTER INSERT ON sessions.session_rollups REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.refresh_inserted_session_catalog_agent_recency();
CREATE TRIGGER session_rollups_agent_recency_delete
  AFTER DELETE ON sessions.session_rollups REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.refresh_deleted_session_catalog_agent_recency();
CREATE TRIGGER session_rollups_agent_recency_update
  AFTER UPDATE ON sessions.session_rollups REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.refresh_updated_session_catalog_agent_recency();

-- A catalog deletion invalidates the name projection. 0058 deliberately left
-- stale rows and made reads join session_catalog; CDC consumers cannot safely
-- reproduce that implicit join, so remove the source row in the same transaction.
CREATE FUNCTION sessions.delete_removed_session_catalog_agents()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $delete_removed$
BEGIN
  DELETE FROM sessions.session_catalog_agents AS a
   USING old_rows AS o
   WHERE a.org_id = o.org_id AND a.workspace_id = o.workspace_id
     AND a.source = o.source AND a.session_id = o.session_id;
  RETURN NULL;
END
$delete_removed$;

CREATE TRIGGER session_catalog_agent_delete
  AFTER DELETE ON sessions.session_catalog REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.delete_removed_session_catalog_agents();

CREATE TABLE sessions.session_catalog_agent_lookup_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  cursor_org_id text,
  cursor_workspace_id text,
  cursor_source text,
  cursor_session_id text,
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.session_catalog_agent_lookup_rollout (completed_at)
  SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM sessions.session_catalog_agents) THEN now() END;

CREATE FUNCTION sessions.session_catalog_agent_lookup_backfill_step(p_batch integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp
SET plan_cache_mode = force_custom_plan AS $backfill$
DECLARE
  state sessions.session_catalog_agent_lookup_rollout%ROWTYPE;
  orgs text[];
  workspaces text[];
  sources text[];
  ids text[];
  found_count integer;
  target record;
BEGIN
  IF p_batch < 1 OR p_batch > 1000 THEN
    RAISE EXCEPTION 'session agent lookup batch must be between 1 and 1000'
      USING ERRCODE = '22023';
  END IF;
  SELECT * INTO state FROM sessions.session_catalog_agent_lookup_rollout FOR UPDATE;
  IF state.completed_at IS NOT NULL THEN RETURN 0; END IF;

  SELECT array_agg(a.org_id ORDER BY a.org_id, a.workspace_id, a.source, a.session_id),
         array_agg(a.workspace_id ORDER BY a.org_id, a.workspace_id, a.source, a.session_id),
         array_agg(a.source ORDER BY a.org_id, a.workspace_id, a.source, a.session_id),
         array_agg(a.session_id ORDER BY a.org_id, a.workspace_id, a.source, a.session_id)
    INTO orgs, workspaces, sources, ids
    FROM (SELECT org_id, workspace_id, source, session_id
            FROM sessions.session_catalog_agents
           WHERE state.cursor_org_id IS NULL
              OR (org_id, workspace_id, source, session_id) >
                 (state.cursor_org_id, state.cursor_workspace_id, state.cursor_source, state.cursor_session_id)
           ORDER BY org_id, workspace_id, source, session_id
           LIMIT p_batch) AS a;
  found_count := coalesce(array_length(orgs, 1), 0);

  IF found_count > 0 THEN
    -- Take every session lock in the same order as session_rollups before any
    -- FOR UPDATE row lock. The row trigger then takes these locks re-entrantly.
    FOR target IN
      SELECT DISTINCT k.org_id, k.session_id
        FROM unnest(orgs, ids) AS k(org_id, session_id)
       ORDER BY k.org_id, k.session_id
    LOOP
      IF NOT pg_try_advisory_xact_lock(
        sessions.session_rollup_lock_key(target.org_id, target.session_id)) THEN
        RAISE EXCEPTION 'session agent lookup row is busy'
          USING ERRCODE = '55P03';
      END IF;
    END LOOP;

    -- Yield the whole ordered batch rather than skipping a busy row behind the
    -- cursor; the operator retries after its catalog delivery commits.
    PERFORM 1
      FROM sessions.session_catalog_agents AS a
      JOIN unnest(orgs, workspaces, sources, ids) AS k(org_id, workspace_id, source, session_id)
        ON a.org_id = k.org_id AND a.workspace_id = k.workspace_id
       AND a.source = k.source AND a.session_id = k.session_id
       FOR UPDATE OF a NOWAIT;

    -- Remove 0058-era orphans before this table becomes independently safe to publish.
    DELETE FROM sessions.session_catalog_agents AS a
     USING unnest(orgs, workspaces, sources, ids) AS k(org_id, workspace_id, source, session_id)
     WHERE a.org_id = k.org_id AND a.workspace_id = k.workspace_id
       AND a.source = k.source AND a.session_id = k.session_id
       AND NOT EXISTS (SELECT 1 FROM sessions.session_catalog AS c
                        WHERE c.org_id = a.org_id AND c.workspace_id = a.workspace_id
                          AND c.source = a.source AND c.session_id = a.session_id);

    UPDATE sessions.session_catalog_agents AS a
       SET agent_name_normalized = lower(a.agent_name),
           last_ts = (SELECT max(r.last_ts) FROM sessions.session_rollups AS r
                       WHERE r.org_id = a.org_id AND r.workspace_id = a.workspace_id
                         AND r.source = a.source AND r.session_id = a.session_id)
      FROM unnest(orgs, workspaces, sources, ids) AS k(org_id, workspace_id, source, session_id)
     WHERE a.org_id = k.org_id AND a.workspace_id = k.workspace_id
       AND a.source = k.source AND a.session_id = k.session_id;
  END IF;

  UPDATE sessions.session_catalog_agent_lookup_rollout SET
    cursor_org_id = coalesce(orgs[found_count], cursor_org_id),
    cursor_workspace_id = coalesce(workspaces[found_count], cursor_workspace_id),
    cursor_source = coalesce(sources[found_count], cursor_source),
    cursor_session_id = coalesce(ids[found_count], cursor_session_id),
    completed_at = CASE WHEN found_count < p_batch THEN now() END,
    updated_at = now();
  RETURN found_count;
END
$backfill$;
REVOKE ALL ON FUNCTION sessions.session_catalog_agent_lookup_backfill_step(integer) FROM PUBLIC;

SELECT set_config('lock_timeout', COALESCE(NULLIF(
  current_setting('relayhistory.migration_saved_lock_timeout', true), ''), current_setting('lock_timeout')), true);
