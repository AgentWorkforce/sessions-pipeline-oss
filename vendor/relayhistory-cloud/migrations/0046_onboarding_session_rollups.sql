-- Onboarding reads one row per retained source/session for the exact user,
-- rather than revisiting every historical transcript on every status poll.
-- Schema/triggers only: existing history is backfilled in bounded transactions.
CREATE TABLE sessions.onboarding_session_rollups (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  event_count bigint NOT NULL CHECK (event_count > 0),
  last_received_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, user_id, source, session_id)
);
-- Rebuilds and maintenance lock at the same (org, session) grain as the existing
-- session rollups. Sharing their ordered lock avoids a second lock hierarchy.
CREATE INDEX onboarding_session_rollups_session_idx
  ON sessions.onboarding_session_rollups (org_id, session_id);

CREATE TABLE sessions.onboarding_rollup_dirty (
  org_id text NOT NULL,
  session_id text NOT NULL,
  PRIMARY KEY (org_id, session_id)
);

CREATE FUNCTION sessions.rebuild_onboarding_rollups(p_org_ids text[], p_session_ids text[])
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $rebuild$
BEGIN
  DELETE FROM sessions.onboarding_session_rollups AS r
   USING unnest(p_org_ids, p_session_ids) AS k(org_id, session_id)
   WHERE r.org_id = k.org_id AND r.session_id = k.session_id;
  INSERT INTO sessions.onboarding_session_rollups
    (org_id, workspace_id, user_id, source, session_id, event_count, last_received_at)
  SELECT e.org_id, e.workspace_id, e.user_id, e.source, e.session_id,
         count(*), max(e.ingested_at)
    FROM sessions.convergence_events AS e
    JOIN unnest(p_org_ids, p_session_ids) AS k(org_id, session_id)
      ON e.org_id = k.org_id AND e.session_id = k.session_id
   WHERE e.kind IN ('history', 'session_event')
     AND length(trim(coalesce(e.content, ''))) > 0
   GROUP BY e.org_id, e.workspace_id, e.user_id, e.source, e.session_id;
END;
$rebuild$;

CREATE FUNCTION sessions.rebuild_dirty_onboarding_rollup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $dirty$
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(NEW.org_id, NEW.session_id));
  PERFORM sessions.rebuild_onboarding_rollups(ARRAY[NEW.org_id], ARRAY[NEW.session_id]);
  DELETE FROM sessions.onboarding_rollup_dirty
   WHERE org_id = NEW.org_id AND session_id = NEW.session_id;
  RETURN NULL;
END;
$dirty$;
CREATE CONSTRAINT TRIGGER onboarding_rollup_dirty_rebuild
  AFTER INSERT ON sessions.onboarding_rollup_dirty
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION sessions.rebuild_dirty_onboarding_rollup();

CREATE FUNCTION sessions.mark_onboarding_rollups_dirty(p_keys jsonb)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $mark$
DECLARE k record;
BEGIN
  FOR k IN SELECT DISTINCT x.org_id, x.session_id
             FROM jsonb_to_recordset(p_keys) AS x(org_id text, session_id text)
            ORDER BY org_id, session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
    INSERT INTO sessions.onboarding_rollup_dirty VALUES (k.org_id, k.session_id)
      ON CONFLICT DO NOTHING;
  END LOOP;
END;
$mark$;

CREATE FUNCTION sessions.merge_inserted_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $merge$
DECLARE k record;
BEGIN
  IF NOT (SELECT maintenance_enabled FROM sessions.onboarding_rollup_rollout) THEN
    RETURN NULL;
  END IF;
  FOR k IN SELECT DISTINCT org_id, session_id FROM changed_rows ORDER BY org_id, session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  INSERT INTO sessions.onboarding_session_rollups AS r
    (org_id, workspace_id, user_id, source, session_id, event_count, last_received_at)
  SELECT e.org_id, e.workspace_id, e.user_id, e.source, e.session_id,
         count(*), max(e.ingested_at)
    FROM changed_rows AS e
   WHERE e.kind IN ('history', 'session_event')
     AND length(trim(coalesce(e.content, ''))) > 0
     AND NOT EXISTS (SELECT 1 FROM sessions.onboarding_rollup_dirty AS d
                      WHERE d.org_id = e.org_id AND d.session_id = e.session_id)
   GROUP BY e.org_id, e.workspace_id, e.user_id, e.source, e.session_id
   ORDER BY e.org_id, e.workspace_id, e.user_id, e.source, e.session_id
  ON CONFLICT (org_id, workspace_id, user_id, source, session_id) DO UPDATE SET
    event_count = r.event_count + EXCLUDED.event_count,
    last_received_at = greatest(r.last_received_at, EXCLUDED.last_received_at);
  RETURN NULL;
END;
$merge$;

CREATE FUNCTION sessions.mark_deleted_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $deleted$
DECLARE k record;
BEGIN
  IF NOT (SELECT maintenance_enabled FROM sessions.onboarding_rollup_rollout) THEN
    RETURN NULL;
  END IF;
  -- This trigger runs before the existing session-rollup delete trigger. Lock
  -- its complete key set first, including ineligible events, in the same order.
  FOR k IN SELECT DISTINCT org_id, session_id FROM changed_rows ORDER BY org_id, session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  PERFORM sessions.mark_onboarding_rollups_dirty(
    (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', org_id, 'session_id', session_id)), '[]')
       FROM changed_rows WHERE kind IN ('history', 'session_event')
         AND length(trim(coalesce(content, ''))) > 0));
  RETURN NULL;
END;
$deleted$;

CREATE FUNCTION sessions.mark_updated_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $updated$
DECLARE
  k record;
  advances jsonb;
  dirty jsonb;
BEGIN
  IF NOT (SELECT maintenance_enabled FROM sessions.onboarding_rollup_rollout) THEN
    RETURN NULL;
  END IF;
  -- Own the complete key set before the other session-rollup triggers run.
  FOR k IN SELECT org_id, session_id FROM old_rows
           UNION SELECT org_id, session_id FROM new_rows ORDER BY org_id, session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  WITH current_values AS MATERIALIZED (
    SELECT org_id, workspace_id, user_id, source, session_id, machine_id, kind, event_id,
           ingested_at, kind IN ('history', 'session_event') AND length(trim(coalesce(content, ''))) > 0 AS eligible
      FROM new_rows
  ), previous_values AS MATERIALIZED (
    SELECT org_id, workspace_id, user_id, source, session_id, machine_id, kind, event_id,
           ingested_at, kind IN ('history', 'session_event') AND length(trim(coalesce(content, ''))) > 0 AS eligible
      FROM old_rows
  ), changed AS MATERIALIZED (
    (SELECT * FROM current_values EXCEPT SELECT * FROM previous_values)
    UNION ALL
    (SELECT * FROM previous_values EXCEPT SELECT * FROM current_values)
  ), forward_updates AS MATERIALIZED (
    -- A normal ingest replay advances receipt time without changing membership.
    -- Merge that maximum instead of re-reading all history of a large session.
    SELECT n.* FROM current_values AS n JOIN previous_values AS o
      ON n.org_id = o.org_id AND n.workspace_id = o.workspace_id AND n.user_id = o.user_id
     AND n.source = o.source AND n.session_id = o.session_id AND n.machine_id = o.machine_id
     AND n.kind = o.kind AND n.event_id = o.event_id
     AND n.eligible AND o.eligible AND n.ingested_at >= o.ingested_at
  )
  SELECT (SELECT coalesce(jsonb_agg(to_jsonb(a)), '[]') FROM (
            SELECT org_id, workspace_id, user_id, source, session_id, max(ingested_at) AS newest
              FROM forward_updates GROUP BY org_id, workspace_id, user_id, source, session_id
          ) AS a),
         (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', c.org_id, 'session_id', c.session_id)), '[]')
            FROM changed AS c WHERE c.eligible AND NOT EXISTS (
              SELECT 1 FROM forward_updates AS f
               WHERE f.org_id = c.org_id AND f.source = c.source AND f.session_id = c.session_id
                 AND f.machine_id = c.machine_id AND f.kind = c.kind AND f.event_id = c.event_id))
    INTO advances, dirty;
  PERFORM sessions.mark_onboarding_rollups_dirty(dirty);
  UPDATE sessions.onboarding_session_rollups AS r SET
    last_received_at = greatest(r.last_received_at, a.newest)
    FROM jsonb_to_recordset(advances) AS a(
      org_id text, workspace_id text, user_id text, source text, session_id text, newest timestamptz)
   WHERE r.org_id = a.org_id AND r.workspace_id = a.workspace_id AND r.user_id = a.user_id
     AND r.source = a.source AND r.session_id = a.session_id
     AND r.last_received_at < a.newest
     AND NOT EXISTS (SELECT 1 FROM sessions.onboarding_rollup_dirty AS d
                      WHERE d.org_id = r.org_id AND d.session_id = r.session_id);
  RETURN NULL;
END;
$updated$;

CREATE FUNCTION sessions.truncate_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $truncate$
BEGIN
  IF NOT (SELECT maintenance_enabled FROM sessions.onboarding_rollup_rollout) THEN
    RETURN NULL;
  END IF;
  TRUNCATE sessions.onboarding_session_rollups;
  RETURN NULL;
END;
$truncate$;

CREATE TRIGGER convergence_events_rollup_0_onboarding_insert
  AFTER INSERT ON sessions.convergence_events REFERENCING NEW TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.merge_inserted_onboarding_rollups();
CREATE TRIGGER convergence_events_rollup_0_onboarding_delete
  AFTER DELETE ON sessions.convergence_events REFERENCING OLD TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.mark_deleted_onboarding_rollups();
CREATE TRIGGER convergence_events_rollup_0_onboarding_update
  AFTER UPDATE ON sessions.convergence_events REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.mark_updated_onboarding_rollups();
CREATE TRIGGER convergence_events_rollup_0_onboarding_truncate
  AFTER TRUNCATE ON sessions.convergence_events
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.truncate_onboarding_rollups();

CREATE TABLE sessions.onboarding_rollup_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  cursor_org_id text,
  cursor_session_id text,
  maintenance_enabled boolean NOT NULL DEFAULT false,
  reads_enabled boolean NOT NULL DEFAULT false,
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.onboarding_rollup_rollout (completed_at)
  SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM sessions.convergence_events) THEN now() END;

-- REVOKE alone cannot restrict roles inheriting pg_write_all_data. Keep the
-- rollout switch owner-only even when the runtime inherits broad write grants.
CREATE FUNCTION sessions.guard_onboarding_rollout_owner() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $owner$
BEGIN
  IF current_user <> pg_get_userbyid((SELECT relowner FROM pg_class
      WHERE oid = 'sessions.onboarding_rollup_rollout'::regclass)) THEN
    RAISE EXCEPTION 'onboarding rollout requires the table owner' USING ERRCODE = '42501';
  END IF;
  RETURN NULL;
END;
$owner$;
CREATE TRIGGER onboarding_rollout_owner_write
  BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON sessions.onboarding_rollup_rollout
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.guard_onboarding_rollout_owner();

CREATE FUNCTION sessions.onboarding_rollup_backfill_step(p_batch integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $backfill$
DECLARE
  state sessions.onboarding_rollup_rollout%ROWTYPE;
  orgs text[];
  ids text[];
  i integer;
  found_count integer;
BEGIN
  IF p_batch IS NULL OR p_batch < 1 OR p_batch > 500 THEN
    RAISE EXCEPTION 'invalid onboarding backfill batch size' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO STRICT state FROM sessions.onboarding_rollup_rollout FOR UPDATE;
  IF NOT state.maintenance_enabled THEN
    RAISE EXCEPTION 'onboarding maintenance is disabled' USING ERRCODE = '55000';
  END IF;
  IF state.completed_at IS NOT NULL THEN RETURN 0; END IF;
  IF state.cursor_org_id IS NULL THEN
    SELECT array_agg(k.org_id ORDER BY k.org_id, k.session_id),
           array_agg(k.session_id ORDER BY k.org_id, k.session_id)
      INTO orgs, ids
      FROM (SELECT DISTINCT org_id, session_id FROM sessions.convergence_events
             ORDER BY org_id, session_id LIMIT p_batch) AS k;
  ELSE
    SELECT array_agg(k.org_id ORDER BY k.org_id, k.session_id),
           array_agg(k.session_id ORDER BY k.org_id, k.session_id)
      INTO orgs, ids
      FROM (SELECT DISTINCT org_id, session_id FROM sessions.convergence_events
             WHERE (org_id, session_id) > (state.cursor_org_id, state.cursor_session_id)
             ORDER BY org_id, session_id LIMIT p_batch) AS k;
  END IF;
  found_count := coalesce(array_length(orgs, 1), 0);
  FOR i IN 1 .. found_count LOOP
    IF NOT pg_try_advisory_xact_lock(sessions.session_rollup_lock_key(orgs[i], ids[i])) THEN
      RAISE EXCEPTION 'onboarding backfill yielded to a concurrent write' USING ERRCODE = '55P03';
    END IF;
  END LOOP;
  IF found_count > 0 THEN
    PERFORM sessions.rebuild_onboarding_rollups(orgs, ids);
  END IF;
  UPDATE sessions.onboarding_rollup_rollout SET
    cursor_org_id = coalesce(orgs[found_count], cursor_org_id),
    cursor_session_id = coalesce(ids[found_count], cursor_session_id),
    completed_at = CASE WHEN found_count < p_batch THEN now() END,
    updated_at = now();
  RETURN found_count;
END;
$backfill$;
REVOKE ALL ON FUNCTION sessions.onboarding_rollup_backfill_step(integer) FROM PUBLIC;
