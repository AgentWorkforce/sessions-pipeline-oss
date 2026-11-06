-- Apply deletes and moved rows to the session and onboarding rollups as
-- differences at commit, instead of rebuilding the whole session.
--
-- Before this migration every DELETE on convergence_events, and every update
-- that changed a rolled-up column other than usage, marked the session dirty,
-- and a deferred trigger re-aggregated all of its events at commit
-- (rebuild_session_rollups, 0030, and rebuild_onboarding_rollups, 0046, which
-- also detoasts every eligible event's content). Each delivery revision deletes
-- the record's previous projection and inserts the new one
-- (retract_delivery_projection), and the projection moves tool rows onto their
-- parent's time, so every revision batch paid two full passes over the
-- session: about 32,000 buffers each at 174,000 events locally, far more
-- against production's cache, growing with the session.
--
-- Now:
--   * A delete, and the old version of a row an update moves, is noted in a
--     transaction-local setting (set_config(..., true)): per rollup key, the
--     removed rows' count and sums, their earliest and latest ts and newest
--     titled ts, and each set value they carried (user, machine, kind, model,
--     task ref) with the ts of one removed carrier. No rollup row is written at
--     delete time. (A note per statement in a table would add a row version per
--     statement to rows the same transaction keeps rewriting; a 500-record
--     revision batch makes about 1,500 such statements.)
--   * Inserts, and the new version of a moved row, merge exactly as 0030/0046
--     inserts always have.
--   * At commit a deferred trigger, once per session, applies the noted
--     removals under the session lock: counts and sums are subtracted exactly
--     and a key with no events left is deleted. A stored extreme the removed
--     rows could have carried (first_ts, last_ts, the newest title, the newest
--     onboarding receipt) is kept if a point probe finds an event still at it,
--     else that one column is recomputed from the key's events. A removed set
--     value is kept if an event still carries it (probed first at the removed
--     carrier's ts, where a re-inserted revision lands), else removed from the
--     array in place. In the revision pattern (delete, then re-insert the same
--     rows) every probe is a hit and nothing is recomputed.
--
-- A full rebuild of the session (the previous behaviour) remains where an
-- incremental change cannot be exact or cheap:
--   * the rollup backfill has not completed (session_rollup_rollout or
--     onboarding_rollup_rollout.completed_at is NULL): the stored rows may not
--     count the removed events yet;
--   * one statement removes or moves more than session_rollup_diff_limit()
--     (5,000) rows, as a bulk deletion does. The row count is checked before
--     any row is copied into an array, so a bulk statement pays only for
--     finding its sessions;
--   * at commit a key is missing or counts fewer events than were removed, or
--     a recomputation finds no event for a key still counted (stored rows that
--     disagree with their events);
--   * at commit more than 16 removed set values of one session miss the point
--     probe at their removed carrier's time and would each need a probe over
--     the key, which could cost more than one pass (probing stops at the 17th
--     miss), or the session has more than session_rollup_value_note_limit()
--     (128) removed set values, where even the point probes are not worth
--     running. That budget is enforced when values are noted: the statement
--     that takes a session past it marks the session for a rebuild and drops
--     its notes, so a long transaction stops rewriting a note it would only
--     discard at commit;
--   * one transaction would carry notes for more than
--     session_rollup_note_session_limit() (64) sessions: further sessions are
--     marked for a rebuild instead, so note bookkeeping stays bounded. When a
--     statement trips the cap, every session in that statement is marked,
--     including ones that would still have fit: conservative, and exact;
--   * at commit the subtraction empties a key that a probe finds still has
--     events (stored count drifted low).
-- That emptied-key probe is not a point probe when the key really is empty:
-- it walks the session's timeline index, filtering on workspace, source and
-- project (onboarding: user, kind and content, detoasting each candidate).
-- For a whole-session delete the walk is short; moving every row out of one
-- key of a large session (a project or workspace reassignment) costs about one
-- pass over the session, roughly a rebuild. That is rare.
-- A session rebuilt in the transaction never also has the removals noted before
-- its rebuild applied: the rebuild drops them (they are already reflected).
-- Usage-only updates keep 0030's immediate difference path.
--
-- Concurrency: every trigger still takes the per-(org, session) advisory locks
-- (session_rollup_lock_key) in key order before touching a session, so the
-- transaction holds them until commit and the commit-time resolution runs under
-- them, as the rebuild it replaces did. Another writer's uncommitted rows are
-- invisible to the resolution; that writer waits for the lock and then applies
-- its own inserts (unions, least/greatest, sums) and, at its own commit, its
-- own removals. A rolled-back savepoint reverts its notes with its rows,
-- because transaction-local settings follow subtransactions.
--
-- No index, data or lock-order change, and no write hold. 0030's and 0046's
-- insert merges are unchanged.

-- The rolled-up columns of one event, for passing removed/added rows to helpers.
CREATE TYPE sessions.session_rollup_event AS (
  org_id text,
  session_id text,
  workspace_id text,
  source text,
  project_id text,
  ts timestamptz,
  user_id text,
  machine_id text,
  kind text,
  model text,
  task_title text,
  task_ref jsonb,
  cost_usd_micros bigint,
  input_tokens bigint,
  output_tokens bigint,
  reasoning_tokens bigint,
  cache_read_tokens bigint,
  cache_create_tokens bigint,
  cache_create_5m_tokens bigint,
  cache_create_1h_tokens bigint
);

CREATE TYPE sessions.onboarding_rollup_event AS (
  org_id text,
  workspace_id text,
  user_id text,
  source text,
  session_id text,
  ingested_at timestamptz
);

-- Sessions with noted removals to apply at commit, once per session and
-- rollup. Rows only ever belong to the current transaction, like
-- session_rollup_dirty.
--
-- Each kind resolves only its own notes. With constraints IMMEDIATE, a pending
-- row resolves as soon as it is inserted, inside the trigger that noted it, and
-- that trigger notes last, after its own in-place writes. The onboarding
-- update trigger runs before the session update trigger of the same
-- statement; were one pending row to apply both kinds, its resolution could
-- apply (or, on a fallback, rebuild) the session rollups before the session
-- trigger had merged that statement's rows, and the merge would count them
-- again. (One shared row was safe only through an invariant: SET CONSTRAINTS
-- ... IMMEDIATE drains every pending row and each later note resolves at once,
-- so no session notes are outstanding when an onboarding note resolves.
-- Separate kinds make that independent of the invariant.)
CREATE TABLE sessions.session_rollup_pending (
  org_id text NOT NULL,
  session_id text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('session', 'onboarding')),
  PRIMARY KEY (org_id, session_id, kind)
);

-- One statement removing or moving more rows than this rebuilds its sessions
-- instead (bulk deletion), rather than carrying every row through the diff.
-- 5,000 unless relayhistory.session_rollup_diff_limit is set, which the tests
-- use to drive the bulk path with small fixtures. It is read at run time:
-- plpgsql caches the expressions that call it, so a constant body redefined
-- by CREATE OR REPLACE would not reach callers already cached.
CREATE FUNCTION sessions.session_rollup_diff_limit() RETURNS integer
LANGUAGE sql STABLE PARALLEL SAFE AS $diff_limit$
  SELECT coalesce(nullif(pg_catalog.current_setting('relayhistory.session_rollup_diff_limit', true), '')::pg_catalog.int4, 5000)
$diff_limit$;

-- At most this many sessions carry notes in one transaction. Each statement
-- re-aggregates the notes so far and each session's commit-time resolver
-- filters them, so a transaction noting removals across many sessions (a
-- cleanup or reset touching thousands) would pay that per session; beyond the
-- limit its further sessions are marked for a rebuild instead.
CREATE FUNCTION sessions.session_rollup_note_session_limit() RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $note_limit$ SELECT 64 $note_limit$;

-- At most this many removed set values are noted for one session (8 times the
-- 16-miss probe budget: past it the session is a bulk removal, not a
-- revision, and is rebuilt instead). note_removed_session_rollups enforces it
-- as values are noted; apply_removed_session_rollups checks it again.
CREATE FUNCTION sessions.session_rollup_value_note_limit() RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $value_limit$ SELECT 128 $value_limit$;

-- A transaction-local note (a jsonb array), empty when unset. No SET clause,
-- so callers inline it; it names only pg_catalog objects.
CREATE FUNCTION sessions.session_rollup_note(p_name text) RETURNS jsonb
LANGUAGE sql STABLE AS $note$
  SELECT coalesce(nullif(pg_catalog.current_setting(p_name, true), ''), '[]')::pg_catalog.jsonb
$note$;

-- Merges added rows into the stored totals: 0030's insert merge over an array.
CREATE FUNCTION sessions.merge_session_rollup_events(p_added sessions.session_rollup_event[])
RETURNS void LANGUAGE sql
SET search_path = pg_catalog, sessions, pg_temp AS $merge_events$
  INSERT INTO sessions.session_rollups AS r (
    org_id, session_id, workspace_id, source, project_id,
    first_ts, last_ts, event_count, user_ids, machine_ids, kinds, models,
    task_title, task_title_ts, task_refs, cost_usd_micros, cost_events,
    input_tokens, output_tokens, reasoning_tokens, cache_read_tokens,
    cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens,
    cache_split_incomplete_events)
  SELECT e.org_id, e.session_id, e.workspace_id, e.source, e.project_id,
         min(e.ts), max(e.ts), count(*),
         array_agg(DISTINCT e.user_id),
         array_agg(DISTINCT e.machine_id),
         array_agg(DISTINCT e.kind),
         coalesce(array_agg(DISTINCT e.model) FILTER (WHERE e.model IS NOT NULL), '{}'),
         (array_agg(e.task_title ORDER BY e.ts DESC, e.task_title DESC)
            FILTER (WHERE e.task_title IS NOT NULL))[1],
         max(e.ts) FILTER (WHERE e.task_title IS NOT NULL),
         coalesce(jsonb_agg(DISTINCT e.task_ref) FILTER (WHERE e.task_ref <> '{}'::jsonb), '[]'::jsonb),
         sum(e.cost_usd_micros),
         count(e.cost_usd_micros),
         coalesce(sum(e.input_tokens), 0),
         coalesce(sum(e.output_tokens), 0),
         coalesce(sum(e.reasoning_tokens), 0),
         coalesce(sum(e.cache_read_tokens), 0),
         coalesce(sum(e.cache_create_tokens), 0),
         coalesce(sum(e.cache_create_5m_tokens), 0),
         coalesce(sum(e.cache_create_1h_tokens), 0),
         count(*) FILTER (WHERE NOT sessions.session_rollup_split_complete(
           e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens))
    FROM unnest(p_added) AS e
   WHERE NOT EXISTS (
     SELECT 1 FROM sessions.session_rollup_dirty AS d
      WHERE d.org_id = e.org_id AND d.session_id = e.session_id)
   GROUP BY e.org_id, e.session_id, e.workspace_id, e.source, e.project_id
   ORDER BY e.org_id, e.session_id, e.workspace_id, e.source, e.project_id
  ON CONFLICT ON CONSTRAINT session_rollups_key DO UPDATE SET
    first_ts = least(r.first_ts, excluded.first_ts),
    last_ts = greatest(r.last_ts, excluded.last_ts),
    event_count = r.event_count + excluded.event_count,
    user_ids = sessions.session_rollup_text_union(r.user_ids, excluded.user_ids),
    machine_ids = sessions.session_rollup_text_union(r.machine_ids, excluded.machine_ids),
    kinds = sessions.session_rollup_text_union(r.kinds, excluded.kinds),
    models = sessions.session_rollup_text_union(r.models, excluded.models),
    task_title = CASE
      WHEN excluded.task_title_ts IS NULL THEN r.task_title
      WHEN r.task_title_ts IS NULL
        OR (excluded.task_title_ts, excluded.task_title) > (r.task_title_ts, r.task_title)
        THEN excluded.task_title
      ELSE r.task_title END,
    task_title_ts = greatest(r.task_title_ts, excluded.task_title_ts),
    task_refs = sessions.session_rollup_jsonb_union(r.task_refs, excluded.task_refs),
    cost_usd_micros = CASE
      WHEN r.cost_usd_micros IS NULL THEN excluded.cost_usd_micros
      WHEN excluded.cost_usd_micros IS NULL THEN r.cost_usd_micros
      ELSE r.cost_usd_micros + excluded.cost_usd_micros END,
    cost_events = r.cost_events + excluded.cost_events,
    input_tokens = r.input_tokens + excluded.input_tokens,
    output_tokens = r.output_tokens + excluded.output_tokens,
    reasoning_tokens = r.reasoning_tokens + excluded.reasoning_tokens,
    cache_read_tokens = r.cache_read_tokens + excluded.cache_read_tokens,
    cache_create_tokens = r.cache_create_tokens + excluded.cache_create_tokens,
    cache_create_5m_tokens = r.cache_create_5m_tokens + excluded.cache_create_5m_tokens,
    cache_create_1h_tokens = r.cache_create_1h_tokens + excluded.cache_create_1h_tokens,
    cache_split_incomplete_events = r.cache_split_incomplete_events + excluded.cache_split_incomplete_events;
$merge_events$;

-- Notes removed rows for commit. The caller holds their sessions' locks.
CREATE FUNCTION sessions.note_removed_session_rollups(p_removed sessions.session_rollup_event[])
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $note_removed$
DECLARE
  totals jsonb;
  vals jsonb;
  over_budget jsonb;
BEGIN
  IF cardinality(p_removed) = 0 THEN RETURN; END IF;
  -- Before the backfill completes, stored rows may not count these events yet.
  IF (SELECT completed_at IS NULL FROM sessions.session_rollup_rollout) THEN
    PERFORM sessions.mark_session_rollups_dirty((
      SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', e.org_id, 'session_id', e.session_id)), '[]')
        FROM unnest(p_removed) AS e));
    RETURN;
  END IF;
  -- A session already marked for a rebuild needs no note: the rebuild reads its
  -- events at commit (and drops any notes made before it was marked).
  SELECT array_agg(e) INTO p_removed FROM unnest(p_removed) AS e
   WHERE NOT EXISTS (SELECT 1 FROM sessions.session_rollup_dirty AS d
                      WHERE d.org_id = e.org_id AND d.session_id = e.session_id);
  IF p_removed IS NULL THEN RETURN; END IF;
  IF (SELECT count(*) FROM (
        SELECT x.e->>'o', x.e->>'s'
          FROM jsonb_array_elements(sessions.session_rollup_note('relayhistory.session_rollup_removed')) AS x(e)
        UNION
        SELECT e.org_id, e.session_id FROM unnest(p_removed) AS e) AS n)
       > sessions.session_rollup_note_session_limit() THEN
    PERFORM sessions.mark_session_rollups_dirty((
      SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', e.org_id, 'session_id', e.session_id)), '[]')
        FROM unnest(p_removed) AS e));
    RETURN;
  END IF;

  -- Per key: totals to subtract and the extremes the removed rows reached.
  totals := (
    SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.o, t.s, t.w, t.src, t.p), '[]')
      FROM (SELECT u.o, u.s, u.w, u.src, u.p,
                   sum(u.n)::bigint AS n, sum(u.cost)::bigint AS cost, sum(u.cost_n)::bigint AS cost_n,
                   sum(u.input)::bigint AS input, sum(u.output)::bigint AS output,
                   sum(u.reasoning)::bigint AS reasoning, sum(u.cache_read)::bigint AS cache_read,
                   sum(u.cache_create)::bigint AS cache_create, sum(u.c5m)::bigint AS c5m,
                   sum(u.c1h)::bigint AS c1h, sum(u.incomplete)::bigint AS incomplete,
                   min(u.first) AS first, max(u.last) AS last, max(u.title_ts) AS title_ts
              FROM (SELECT * FROM jsonb_to_recordset(sessions.session_rollup_note('relayhistory.session_rollup_removed')) AS x(
                      o text, s text, w text, src text, p text, n bigint, cost bigint, cost_n bigint,
                      input bigint, output bigint, reasoning bigint, cache_read bigint,
                      cache_create bigint, c5m bigint, c1h bigint, incomplete bigint,
                      first timestamptz, last timestamptz, title_ts timestamptz)
                    UNION ALL
                    SELECT e.org_id, e.session_id, e.workspace_id, e.source, e.project_id,
                           count(*), coalesce(sum(e.cost_usd_micros), 0), count(e.cost_usd_micros),
                           sum(e.input_tokens), sum(e.output_tokens), sum(e.reasoning_tokens),
                           sum(e.cache_read_tokens), sum(e.cache_create_tokens),
                           sum(coalesce(e.cache_create_5m_tokens, 0)), sum(coalesce(e.cache_create_1h_tokens, 0)),
                           count(*) FILTER (WHERE NOT sessions.session_rollup_split_complete(
                             e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens)),
                           min(e.ts), max(e.ts), max(e.ts) FILTER (WHERE e.task_title IS NOT NULL)
                      FROM unnest(p_removed) AS e
                     GROUP BY e.org_id, e.session_id, e.workspace_id, e.source, e.project_id) AS u
             GROUP BY u.o, u.s, u.w, u.src, u.p) AS t);

  -- Per key: each set value the removed rows carried, with one carrier's ts.
  vals := (
    SELECT coalesce(jsonb_agg(to_jsonb(v) ORDER BY v.o, v.s, v.w, v.src, v.p, v.m, v.val), '[]')
      FROM (SELECT DISTINCT ON (u.o, u.s, u.w, u.src, u.p, u.m, u.val) u.*
              FROM (SELECT * FROM jsonb_to_recordset(sessions.session_rollup_note('relayhistory.session_rollup_removed_values')) AS x(
                      o text, s text, w text, src text, p text, m text, val text, t timestamptz)
                    UNION ALL
                    SELECT e.org_id, e.session_id, e.workspace_id, e.source, e.project_id, m.member, m.value, e.ts
                      FROM unnest(p_removed) AS e
                      CROSS JOIN LATERAL (VALUES
                        ('user_id', e.user_id), ('machine_id', e.machine_id), ('kind', e.kind),
                        ('model', e.model),
                        ('task_ref', CASE WHEN e.task_ref <> '{}'::jsonb THEN e.task_ref::text END)
                      ) AS m(member, value)
                     WHERE m.value IS NOT NULL) AS u
             ORDER BY u.o, u.s, u.w, u.src, u.p, u.m, u.val, u.t) AS v);

  -- A session past the value budget would only be rebuilt at commit: mark it
  -- now and drop its notes, so later statements neither grow nor re-sort them
  -- (they skip a session marked for a rebuild).
  SELECT jsonb_agg(jsonb_build_object('org_id', c.o, 'session_id', c.s) ORDER BY c.o, c.s)
    INTO over_budget
    FROM (SELECT x.e->>'o' AS o, x.e->>'s' AS s
            FROM jsonb_array_elements(vals) AS x(e)
           GROUP BY 1, 2
          HAVING count(*) > sessions.session_rollup_value_note_limit()) AS c;
  IF over_budget IS NOT NULL THEN
    totals := (SELECT coalesce(jsonb_agg(x.e ORDER BY x.i), '[]')
                 FROM jsonb_array_elements(totals) WITH ORDINALITY AS x(e, i)
                WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(over_budget) AS b(k)
                                   WHERE b.k->>'org_id' = x.e->>'o' AND b.k->>'session_id' = x.e->>'s'));
    vals := (SELECT coalesce(jsonb_agg(x.e ORDER BY x.i), '[]')
               FROM jsonb_array_elements(vals) WITH ORDINALITY AS x(e, i)
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(over_budget) AS b(k)
                                 WHERE b.k->>'org_id' = x.e->>'o' AND b.k->>'session_id' = x.e->>'s'));
  END IF;
  PERFORM set_config('relayhistory.session_rollup_removed', totals::text, true);
  PERFORM set_config('relayhistory.session_rollup_removed_values', vals::text, true);

  INSERT INTO sessions.session_rollup_pending (org_id, session_id, kind)
  SELECT DISTINCT e.org_id, e.session_id, 'session' FROM unnest(p_removed) AS e
   WHERE over_budget IS NULL OR NOT EXISTS (
           SELECT 1 FROM jsonb_array_elements(over_budget) AS b(k)
            WHERE b.k->>'org_id' = e.org_id AND b.k->>'session_id' = e.session_id)
   ORDER BY e.org_id, e.session_id
  ON CONFLICT DO NOTHING;
  -- Last, after the notes: under IMMEDIATE the mark rebuilds at once, and the
  -- rebuild drops whatever notes the session still has.
  IF over_budget IS NOT NULL THEN
    PERFORM sessions.mark_session_rollups_dirty(over_budget);
  END IF;
END;
$note_removed$;

-- Applies one session's noted removals at commit. The caller holds its lock.
CREATE FUNCTION sessions.apply_removed_session_rollups(p_org_id text, p_session_id text)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp
-- Plan each probe for its own session and value (see 0054). Probes spell the
-- project match as = OR both NULL: a custom plan folds that to "project_id IS
-- NULL" for a NULL project, where IS NOT DISTINCT FROM kept a default 0.5%
-- selectivity, estimated one row, and chose a sequential scan and sort over the
-- session timeline index (38,642 buffers at 175,000 events).
SET plan_cache_mode = force_custom_plan AS $apply_removed$
DECLARE
  removed jsonb := sessions.session_rollup_note('relayhistory.session_rollup_removed');
  removed_values jsonb := sessions.session_rollup_note('relayhistory.session_rollup_removed_values');
  d record;
  v record;
  r sessions.session_rollups%ROWTYPE;
  found_ts timestamptz;
  found_title text;
  misses jsonb := '[]';
  probes integer := 0;
BEGIN
  -- Take this session's notes out; other sessions resolve on their own row.
  PERFORM set_config('relayhistory.session_rollup_removed', (
    SELECT coalesce(jsonb_agg(x.e), '[]')::text FROM jsonb_array_elements(removed) AS x(e)
     WHERE NOT (x.e->>'o' = p_org_id AND x.e->>'s' = p_session_id)), true);
  PERFORM set_config('relayhistory.session_rollup_removed_values', (
    SELECT coalesce(jsonb_agg(x.e), '[]')::text FROM jsonb_array_elements(removed_values) AS x(e)
     WHERE NOT (x.e->>'o' = p_org_id AND x.e->>'s' = p_session_id)), true);

  -- A session marked for a rebuild reads every event at commit anyway.
  IF EXISTS (SELECT 1 FROM sessions.session_rollup_dirty AS x
              WHERE x.org_id = p_org_id AND x.session_id = p_session_id) THEN
    RETURN;
  END IF;
  FOR d IN SELECT * FROM jsonb_to_recordset(removed) AS x(
             o text, s text, w text, src text, p text, n bigint, cost bigint, cost_n bigint,
             input bigint, output bigint, reasoning bigint, cache_read bigint,
             cache_create bigint, c5m bigint, c1h bigint, incomplete bigint,
             first timestamptz, last timestamptz, title_ts timestamptz)
            WHERE x.o = p_org_id AND x.s = p_session_id LOOP
    SELECT * INTO r FROM sessions.session_rollups AS x
     WHERE x.org_id = d.o AND x.session_id = d.s AND x.workspace_id = d.w
       AND x.source = d.src AND x.project_id IS NOT DISTINCT FROM d.p;
    IF NOT FOUND OR r.event_count < d.n THEN
      -- The stored rows disagree with the events they summarize.
      PERFORM sessions.rebuild_session_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
      RETURN;
    END IF;
    IF r.event_count = d.n THEN
      -- The arithmetic says no event is left; check before dropping the key.
      IF EXISTS (SELECT 1 FROM sessions.convergence_events AS e
                  WHERE e.org_id = d.o AND e.session_id = d.s
                    AND e.workspace_id = d.w AND e.source = d.src
                    AND (e.project_id = d.p OR (e.project_id IS NULL AND d.p IS NULL))) THEN
        PERFORM sessions.rebuild_session_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
        RETURN;
      END IF;
      DELETE FROM sessions.session_rollups AS x
       WHERE x.org_id = d.o AND x.session_id = d.s AND x.workspace_id = d.w
         AND x.source = d.src AND x.project_id IS NOT DISTINCT FROM d.p;
      CONTINUE;
    END IF;
    UPDATE sessions.session_rollups AS x SET
      event_count = x.event_count - d.n,
      cost_events = x.cost_events - d.cost_n,
      cost_usd_micros = CASE WHEN x.cost_events - d.cost_n = 0 THEN NULL
                             ELSE coalesce(x.cost_usd_micros, 0) - d.cost END,
      input_tokens = x.input_tokens - d.input,
      output_tokens = x.output_tokens - d.output,
      reasoning_tokens = x.reasoning_tokens - d.reasoning,
      cache_read_tokens = x.cache_read_tokens - d.cache_read,
      cache_create_tokens = x.cache_create_tokens - d.cache_create,
      cache_create_5m_tokens = x.cache_create_5m_tokens - d.c5m,
      cache_create_1h_tokens = x.cache_create_1h_tokens - d.c1h,
      cache_split_incomplete_events = x.cache_split_incomplete_events - d.incomplete
     WHERE x.org_id = d.o AND x.session_id = d.s AND x.workspace_id = d.w
       AND x.source = d.src AND x.project_id IS NOT DISTINCT FROM d.p;

    -- An extreme the removed rows reached stays only while an event is at it.
    IF d.first <= r.first_ts AND NOT EXISTS (
        SELECT 1 FROM sessions.convergence_events AS e
         WHERE e.org_id = r.org_id AND e.session_id = r.session_id AND e.ts = r.first_ts
           AND e.workspace_id = r.workspace_id AND e.source = r.source
           AND (e.project_id = r.project_id OR (e.project_id IS NULL AND r.project_id IS NULL))) THEN
      SELECT e.ts INTO found_ts FROM sessions.convergence_events AS e
       WHERE e.org_id = r.org_id AND e.session_id = r.session_id AND e.ts > r.first_ts
         AND e.workspace_id = r.workspace_id AND e.source = r.source
         AND (e.project_id = r.project_id OR (e.project_id IS NULL AND r.project_id IS NULL))
       ORDER BY e.ts LIMIT 1;
      IF found_ts IS NULL THEN
        PERFORM sessions.rebuild_session_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
        RETURN;
      END IF;
      UPDATE sessions.session_rollups AS x SET first_ts = found_ts
       WHERE x.org_id = d.o AND x.session_id = d.s AND x.workspace_id = d.w
         AND x.source = d.src AND x.project_id IS NOT DISTINCT FROM d.p;
    END IF;
    IF d.last >= r.last_ts AND NOT EXISTS (
        SELECT 1 FROM sessions.convergence_events AS e
         WHERE e.org_id = r.org_id AND e.session_id = r.session_id AND e.ts = r.last_ts
           AND e.workspace_id = r.workspace_id AND e.source = r.source
           AND (e.project_id = r.project_id OR (e.project_id IS NULL AND r.project_id IS NULL))) THEN
      SELECT e.ts INTO found_ts FROM sessions.convergence_events AS e
       WHERE e.org_id = r.org_id AND e.session_id = r.session_id AND e.ts < r.last_ts
         AND e.workspace_id = r.workspace_id AND e.source = r.source
         AND (e.project_id = r.project_id OR (e.project_id IS NULL AND r.project_id IS NULL))
       ORDER BY e.ts DESC LIMIT 1;
      IF found_ts IS NULL THEN
        PERFORM sessions.rebuild_session_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
        RETURN;
      END IF;
      UPDATE sessions.session_rollups AS x SET last_ts = found_ts
       WHERE x.org_id = d.o AND x.session_id = d.s AND x.workspace_id = d.w
         AND x.source = d.src AND x.project_id IS NOT DISTINCT FROM d.p;
    END IF;
    IF d.title_ts >= r.task_title_ts AND NOT EXISTS (
        SELECT 1 FROM sessions.convergence_events AS e
         WHERE e.org_id = r.org_id AND e.session_id = r.session_id
           AND e.ts = r.task_title_ts AND e.task_title = r.task_title
           AND e.workspace_id = r.workspace_id AND e.source = r.source
           AND (e.project_id = r.project_id OR (e.project_id IS NULL AND r.project_id IS NULL))) THEN
      found_ts := NULL;
      found_title := NULL;
      SELECT e.ts, e.task_title INTO found_ts, found_title FROM sessions.convergence_events AS e
       WHERE e.org_id = r.org_id AND e.session_id = r.session_id AND e.ts <= r.task_title_ts
         AND e.task_title IS NOT NULL
         AND e.workspace_id = r.workspace_id AND e.source = r.source
         AND (e.project_id = r.project_id OR (e.project_id IS NULL AND r.project_id IS NULL))
       ORDER BY e.ts DESC, e.task_title DESC LIMIT 1;
      UPDATE sessions.session_rollups AS x SET task_title = found_title, task_title_ts = found_ts
       WHERE x.org_id = d.o AND x.session_id = d.s AND x.workspace_id = d.w
         AND x.source = d.src AND x.project_id IS NOT DISTINCT FROM d.p;
    END IF;
  END LOOP;

  -- A removed set value stays only while an event of the key carries it. The
  -- first probe is a point probe at a removed carrier's time, where a
  -- re-inserted revision lands; only its misses need a probe over the key, and
  -- a session with more than 16 misses is rebuilt instead, as soon as the
  -- 17th miss is found. A session with more than
  -- session_rollup_value_note_limit() removed values (a bulk removal, not a
  -- revision) is rebuilt without probing; noting marks such a session for a
  -- rebuild already, so this is a guard.
  -- relayhistory.session_rollup_point_probes reports how many point probes
  -- the last resolution ran, for tests and diagnosis.
  IF (SELECT count(*) FROM jsonb_array_elements(removed_values) AS x(e)
       WHERE x.e->>'o' = p_org_id AND x.e->>'s' = p_session_id) > sessions.session_rollup_value_note_limit() THEN
    PERFORM set_config('relayhistory.session_rollup_point_probes', '0', true);
    PERFORM sessions.rebuild_session_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
    RETURN;
  END IF;
  FOR v IN SELECT * FROM jsonb_to_recordset(removed_values) AS x(
             o text, s text, w text, src text, p text, m text, val text, t timestamptz)
            WHERE x.o = p_org_id AND x.s = p_session_id LOOP
    CONTINUE WHEN NOT EXISTS (
      SELECT 1 FROM sessions.session_rollups AS x
       WHERE x.org_id = v.o AND x.session_id = v.s AND x.workspace_id = v.w
         AND x.source = v.src AND x.project_id IS NOT DISTINCT FROM v.p);
    probes := probes + 1;
    CONTINUE WHEN EXISTS (
      SELECT 1 FROM sessions.convergence_events AS e
       WHERE e.org_id = v.o AND e.session_id = v.s AND e.ts = v.t
         AND e.workspace_id = v.w AND e.source = v.src AND (e.project_id = v.p OR (e.project_id IS NULL AND v.p IS NULL))
         AND CASE v.m WHEN 'user_id' THEN e.user_id = v.val WHEN 'machine_id' THEN e.machine_id = v.val
                      WHEN 'kind' THEN e.kind = v.val WHEN 'model' THEN e.model = v.val
                      WHEN 'task_ref' THEN e.task_ref = v.val::jsonb END);
    misses := misses || jsonb_build_array(to_jsonb(v));
    EXIT WHEN jsonb_array_length(misses) > 16;
  END LOOP;
  PERFORM set_config('relayhistory.session_rollup_point_probes', probes::text, true);
  IF jsonb_array_length(misses) > 16 THEN
    PERFORM sessions.rebuild_session_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
    RETURN;
  END IF;
  FOR v IN SELECT * FROM jsonb_to_recordset(misses) AS x(
             o text, s text, w text, src text, p text, m text, val text, t timestamptz) LOOP
    CONTINUE WHEN EXISTS (
      SELECT 1 FROM sessions.convergence_events AS e
       WHERE e.org_id = v.o AND e.session_id = v.s
         AND e.workspace_id = v.w AND e.source = v.src AND (e.project_id = v.p OR (e.project_id IS NULL AND v.p IS NULL))
         AND CASE v.m WHEN 'user_id' THEN e.user_id = v.val WHEN 'machine_id' THEN e.machine_id = v.val
                      WHEN 'kind' THEN e.kind = v.val WHEN 'model' THEN e.model = v.val
                      WHEN 'task_ref' THEN e.task_ref = v.val::jsonb END);
    -- No event carries it: drop it, keeping the sorted order.
    UPDATE sessions.session_rollups AS x SET
      user_ids = CASE WHEN v.m = 'user_id' THEN array_remove(x.user_ids, v.val) ELSE x.user_ids END,
      machine_ids = CASE WHEN v.m = 'machine_id' THEN array_remove(x.machine_ids, v.val) ELSE x.machine_ids END,
      kinds = CASE WHEN v.m = 'kind' THEN array_remove(x.kinds, v.val) ELSE x.kinds END,
      models = CASE WHEN v.m = 'model' THEN array_remove(x.models, v.val) ELSE x.models END,
      task_refs = CASE WHEN v.m = 'task_ref' THEN
          (SELECT coalesce(jsonb_agg(t.e ORDER BY t.i), '[]'::jsonb)
             FROM jsonb_array_elements(x.task_refs) WITH ORDINALITY AS t(e, i)
            WHERE t.e <> v.val::jsonb)
        ELSE x.task_refs END
     WHERE x.org_id = v.o AND x.session_id = v.s AND x.workspace_id = v.w
       AND x.source = v.src AND x.project_id IS NOT DISTINCT FROM v.p;
  END LOOP;
END;
$apply_removed$;

-- Notes removed eligible rows for commit. The caller holds their sessions' locks.
CREATE FUNCTION sessions.note_removed_onboarding_rollups(p_removed sessions.onboarding_rollup_event[])
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $note_onboarding$
BEGIN
  IF cardinality(p_removed) = 0 THEN RETURN; END IF;
  IF (SELECT completed_at IS NULL FROM sessions.onboarding_rollup_rollout) THEN
    PERFORM sessions.mark_onboarding_rollups_dirty((
      SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', e.org_id, 'session_id', e.session_id)), '[]')
        FROM unnest(p_removed) AS e));
    RETURN;
  END IF;
  SELECT array_agg(e) INTO p_removed FROM unnest(p_removed) AS e
   WHERE NOT EXISTS (SELECT 1 FROM sessions.onboarding_rollup_dirty AS d
                      WHERE d.org_id = e.org_id AND d.session_id = e.session_id);
  IF p_removed IS NULL THEN RETURN; END IF;
  IF (SELECT count(*) FROM (
        SELECT x.e->>'o', x.e->>'s'
          FROM jsonb_array_elements(sessions.session_rollup_note('relayhistory.onboarding_rollup_removed')) AS x(e)
        UNION
        SELECT e.org_id, e.session_id FROM unnest(p_removed) AS e) AS n)
       > sessions.session_rollup_note_session_limit() THEN
    PERFORM sessions.mark_onboarding_rollups_dirty((
      SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', e.org_id, 'session_id', e.session_id)), '[]')
        FROM unnest(p_removed) AS e));
    RETURN;
  END IF;
  PERFORM set_config('relayhistory.onboarding_rollup_removed', (
    SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.o, t.s, t.w, t.u, t.src), '[]')::text
      FROM (SELECT x.o, x.w, x.u, x.src, x.s, sum(x.n)::bigint AS n, max(x.newest) AS newest
              FROM (SELECT * FROM jsonb_to_recordset(sessions.session_rollup_note('relayhistory.onboarding_rollup_removed')) AS y(
                      o text, w text, u text, src text, s text, n bigint, newest timestamptz)
                    UNION ALL
                    SELECT e.org_id, e.workspace_id, e.user_id, e.source, e.session_id, count(*), max(e.ingested_at)
                      FROM unnest(p_removed) AS e
                     GROUP BY e.org_id, e.workspace_id, e.user_id, e.source, e.session_id) AS x
             GROUP BY x.o, x.w, x.u, x.src, x.s) AS t), true);
  INSERT INTO sessions.session_rollup_pending (org_id, session_id, kind)
  SELECT DISTINCT e.org_id, e.session_id, 'onboarding' FROM unnest(p_removed) AS e
   ORDER BY e.org_id, e.session_id
  ON CONFLICT DO NOTHING;
END;
$note_onboarding$;

-- Applies one session's noted onboarding removals at commit. The caller holds its lock.
CREATE FUNCTION sessions.apply_removed_onboarding_rollups(p_org_id text, p_session_id text)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp
SET plan_cache_mode = force_custom_plan AS $apply_onboarding$
DECLARE
  removed jsonb := sessions.session_rollup_note('relayhistory.onboarding_rollup_removed');
  d record;
  r sessions.onboarding_session_rollups%ROWTYPE;
  newest timestamptz;
BEGIN
  PERFORM set_config('relayhistory.onboarding_rollup_removed', (
    SELECT coalesce(jsonb_agg(x.e), '[]')::text FROM jsonb_array_elements(removed) AS x(e)
     WHERE NOT (x.e->>'o' = p_org_id AND x.e->>'s' = p_session_id)), true);
  IF EXISTS (SELECT 1 FROM sessions.onboarding_rollup_dirty AS x
              WHERE x.org_id = p_org_id AND x.session_id = p_session_id) THEN
    RETURN;
  END IF;
  FOR d IN SELECT * FROM jsonb_to_recordset(removed) AS x(
             o text, w text, u text, src text, s text, n bigint, newest timestamptz)
            WHERE x.o = p_org_id AND x.s = p_session_id LOOP
    SELECT * INTO r FROM sessions.onboarding_session_rollups AS x
     WHERE x.org_id = d.o AND x.workspace_id = d.w AND x.user_id = d.u
       AND x.source = d.src AND x.session_id = d.s;
    IF NOT FOUND OR r.event_count < d.n THEN
      PERFORM sessions.rebuild_onboarding_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
      RETURN;
    END IF;
    IF r.event_count = d.n THEN
      IF EXISTS (SELECT 1 FROM sessions.convergence_events AS e
                  WHERE e.org_id = d.o AND e.session_id = d.s
                    AND e.workspace_id = d.w AND e.user_id = d.u AND e.source = d.src
                    AND e.kind IN ('history', 'session_event')
                    AND length(trim(coalesce(e.content, ''))) > 0) THEN
        PERFORM sessions.rebuild_onboarding_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
        RETURN;
      END IF;
      DELETE FROM sessions.onboarding_session_rollups AS x
       WHERE x.org_id = d.o AND x.workspace_id = d.w AND x.user_id = d.u
         AND x.source = d.src AND x.session_id = d.s;
      CONTINUE;
    END IF;
    newest := r.last_received_at;
    IF d.newest >= r.last_received_at AND NOT EXISTS (
        SELECT 1 FROM sessions.convergence_events AS e
         WHERE e.org_id = r.org_id AND e.session_id = r.session_id
           AND e.workspace_id = r.workspace_id AND e.user_id = r.user_id AND e.source = r.source
           AND e.ingested_at = r.last_received_at
           AND e.kind IN ('history', 'session_event')
           AND length(trim(coalesce(e.content, ''))) > 0) THEN
      SELECT max(e.ingested_at) INTO newest FROM sessions.convergence_events AS e
       WHERE e.org_id = r.org_id AND e.session_id = r.session_id
         AND e.workspace_id = r.workspace_id AND e.user_id = r.user_id AND e.source = r.source
         AND e.ingested_at < r.last_received_at
         AND e.kind IN ('history', 'session_event')
         AND length(trim(coalesce(e.content, ''))) > 0;
      IF newest IS NULL THEN
        PERFORM sessions.rebuild_onboarding_rollups(ARRAY[p_org_id], ARRAY[p_session_id]);
        RETURN;
      END IF;
    END IF;
    UPDATE sessions.onboarding_session_rollups AS x
       SET event_count = x.event_count - d.n, last_received_at = newest
     WHERE x.org_id = d.o AND x.workspace_id = d.w AND x.user_id = d.u
       AND x.source = d.src AND x.session_id = d.s;
  END LOOP;
END;
$apply_onboarding$;

CREATE FUNCTION sessions.resolve_pending_session_rollup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $resolve_pending$
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(NEW.org_id, NEW.session_id));
  IF NEW.kind = 'session' THEN
    PERFORM sessions.apply_removed_session_rollups(NEW.org_id, NEW.session_id);
  ELSE
    PERFORM sessions.apply_removed_onboarding_rollups(NEW.org_id, NEW.session_id);
  END IF;
  DELETE FROM sessions.session_rollup_pending
   WHERE org_id = NEW.org_id AND session_id = NEW.session_id AND kind = NEW.kind;
  RETURN NULL;
END;
$resolve_pending$;

CREATE CONSTRAINT TRIGGER session_rollup_pending_resolve
  AFTER INSERT ON sessions.session_rollup_pending
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION sessions.resolve_pending_session_rollup();

-- Drops one session's entries from a transaction-local note.
CREATE FUNCTION sessions.forget_session_rollup_note(p_name text, p_org_id text, p_session_id text)
RETURNS void LANGUAGE sql
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
  SELECT set_config(p_name, (
    SELECT coalesce(jsonb_agg(x.e), '[]')::text
      FROM jsonb_array_elements(sessions.session_rollup_note(p_name)) AS x(e)
     WHERE NOT (x.e->>'o' = p_org_id AND x.e->>'s' = p_session_id)), true);
$forget$;

-- A rebuild reads the session's events as they are when it runs, so every
-- removal noted for the session before it is already reflected. 0030's and
-- 0046's rebuild triggers, unchanged except that they drop those notes: the
-- pending trigger may fire after the rebuild has deleted its dirty marker
-- (deferred triggers fire in the order their events were queued), and must not
-- subtract them a second time. A removal noted after the rebuild (a later
-- statement after SET CONSTRAINTS ... IMMEDIATE) is not reflected in it and is
-- applied as usual. While the marker exists, apply_removed_*_rollups skips the
-- session and drops its notes itself.
CREATE OR REPLACE FUNCTION sessions.rebuild_dirty_session_rollup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $rebuild_dirty$
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(NEW.org_id, NEW.session_id));
  PERFORM sessions.rebuild_session_rollups(ARRAY[NEW.org_id], ARRAY[NEW.session_id]);
  PERFORM sessions.forget_session_rollup_note('relayhistory.session_rollup_removed', NEW.org_id, NEW.session_id);
  PERFORM sessions.forget_session_rollup_note('relayhistory.session_rollup_removed_values', NEW.org_id, NEW.session_id);
  DELETE FROM sessions.session_rollup_dirty
   WHERE org_id = NEW.org_id AND session_id = NEW.session_id;
  RETURN NULL;
END;
$rebuild_dirty$;

CREATE OR REPLACE FUNCTION sessions.rebuild_dirty_onboarding_rollup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $dirty$
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(NEW.org_id, NEW.session_id));
  PERFORM sessions.rebuild_onboarding_rollups(ARRAY[NEW.org_id], ARRAY[NEW.session_id]);
  PERFORM sessions.forget_session_rollup_note('relayhistory.onboarding_rollup_removed', NEW.org_id, NEW.session_id);
  DELETE FROM sessions.onboarding_rollup_dirty
   WHERE org_id = NEW.org_id AND session_id = NEW.session_id;
  RETURN NULL;
END;
$dirty$;

CREATE OR REPLACE FUNCTION sessions.mark_deleted_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $mark_deleted$
DECLARE
  k record;
  removed sessions.session_rollup_event[];
BEGIN
  IF (SELECT count(*) FROM (SELECT 1 FROM changed_rows LIMIT sessions.session_rollup_diff_limit() + 1) AS n)
       > sessions.session_rollup_diff_limit() THEN
    PERFORM sessions.mark_session_rollups_dirty(
      (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', org_id, 'session_id', session_id)), '[]')
         FROM changed_rows));
    RETURN NULL;
  END IF;
  SELECT array_agg(ROW(c.org_id, c.session_id, c.workspace_id, c.source, c.project_id,
                       c.ts, c.user_id, c.machine_id, c.kind, c.model, c.task_title, c.task_ref,
                       c.cost_usd_micros, c.input_tokens, c.output_tokens, c.reasoning_tokens,
                       c.cache_read_tokens, c.cache_create_tokens, c.cache_create_5m_tokens,
                       c.cache_create_1h_tokens)::sessions.session_rollup_event)
    INTO removed FROM changed_rows AS c;
  IF removed IS NULL THEN RETURN NULL; END IF;
  FOR k IN SELECT DISTINCT e.org_id, e.session_id FROM unnest(removed) AS e
            ORDER BY e.org_id, e.session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  PERFORM sessions.note_removed_session_rollups(removed);
  RETURN NULL;
END;
$mark_deleted$;

-- Updates come in two shapes, as in 0030. A row whose old and new versions
-- differ only in usage applies the usage difference now. Any other change to a
-- rolled-up column (time, title, model, task ref, or a move to another session,
-- workspace or project) merges the new version now and notes the old version
-- as removed, for commit.
CREATE OR REPLACE FUNCTION sessions.mark_updated_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $mark_updated$
DECLARE
  usage_pairs jsonb;
  is_bulk boolean;
  bulk_sessions jsonb;
  removed sessions.session_rollup_event[];
  added sessions.session_rollup_event[];
  k record;
BEGIN
  WITH changed AS MATERIALIZED (
    (SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'new' AS side FROM new_rows
     EXCEPT SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'new' FROM old_rows)
    UNION ALL
    (SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'old' AS side FROM old_rows
     EXCEPT SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'old' FROM new_rows)
  ),
  -- One row's old and new versions that differ only in usage.
  pairs AS MATERIALIZED (
    SELECT n.org_id, n.session_id, n.workspace_id, n.source, n.project_id,
           n.machine_id, n.kind, n.event_id,
           coalesce(n.cost_usd_micros, 0) - coalesce(o.cost_usd_micros, 0) AS d_cost,
           (n.cost_usd_micros IS NOT NULL)::int - (o.cost_usd_micros IS NOT NULL)::int AS d_cost_events,
           n.input_tokens - o.input_tokens AS d_input,
           n.output_tokens - o.output_tokens AS d_output,
           n.reasoning_tokens - o.reasoning_tokens AS d_reasoning,
           n.cache_read_tokens - o.cache_read_tokens AS d_cache_read,
           n.cache_create_tokens - o.cache_create_tokens AS d_cache_create,
           coalesce(n.cache_create_5m_tokens, 0) - coalesce(o.cache_create_5m_tokens, 0) AS d_5m,
           coalesce(n.cache_create_1h_tokens, 0) - coalesce(o.cache_create_1h_tokens, 0) AS d_1h,
           (NOT sessions.session_rollup_split_complete(n.cache_create_tokens, n.cache_create_5m_tokens, n.cache_create_1h_tokens))::int
             - (NOT sessions.session_rollup_split_complete(o.cache_create_tokens, o.cache_create_5m_tokens, o.cache_create_1h_tokens))::int
             AS d_incomplete
      FROM changed AS n
      JOIN changed AS o
        ON n.side = 'new' AND o.side = 'old'
       AND n.org_id = o.org_id AND n.session_id = o.session_id AND n.source = o.source
       AND n.machine_id = o.machine_id AND n.kind = o.kind AND n.event_id = o.event_id
       AND n.workspace_id = o.workspace_id
       AND n.project_id IS NOT DISTINCT FROM o.project_id
       AND n.ts = o.ts AND n.user_id = o.user_id
       AND n.model IS NOT DISTINCT FROM o.model
       AND n.task_title IS NOT DISTINCT FROM o.task_title
       AND n.task_ref = o.task_ref
  ),
  moved AS MATERIALIZED (
    SELECT c.* FROM changed AS c
     WHERE NOT EXISTS (
       SELECT 1 FROM pairs AS p
        WHERE p.org_id = c.org_id AND p.session_id = c.session_id AND p.source = c.source
          AND p.machine_id = c.machine_id AND p.kind = c.kind AND p.event_id = c.event_id)
  ),
  -- Whether the moved rows (both versions) exceed the diff limit, counted
  -- before any is copied into an array: past it only their sessions are
  -- collected, for a rebuild. (Each CASE evaluates only the subquery it
  -- returns.)
  bulk AS MATERIALIZED (
    SELECT count(*) > sessions.session_rollup_diff_limit() AS over
      FROM (SELECT 1 FROM moved LIMIT sessions.session_rollup_diff_limit() + 1) AS n
  )
  SELECT (SELECT coalesce(jsonb_agg(to_jsonb(p)), '[]') FROM pairs AS p),
         (SELECT b.over FROM bulk AS b),
         CASE WHEN (SELECT b.over FROM bulk AS b) THEN
           (SELECT jsonb_agg(DISTINCT jsonb_build_object('org_id', m.org_id, 'session_id', m.session_id))
              FROM moved AS m) END,
         CASE WHEN NOT (SELECT b.over FROM bulk AS b) THEN
           (SELECT array_agg(ROW(m.org_id, m.session_id, m.workspace_id, m.source, m.project_id,
                                 m.ts, m.user_id, m.machine_id, m.kind, m.model, m.task_title, m.task_ref,
                                 m.cost_usd_micros, m.input_tokens, m.output_tokens, m.reasoning_tokens,
                                 m.cache_read_tokens, m.cache_create_tokens, m.cache_create_5m_tokens,
                                 m.cache_create_1h_tokens)::sessions.session_rollup_event)
              FROM moved AS m WHERE m.side = 'old') END,
         CASE WHEN NOT (SELECT b.over FROM bulk AS b) THEN
           (SELECT array_agg(ROW(m.org_id, m.session_id, m.workspace_id, m.source, m.project_id,
                                 m.ts, m.user_id, m.machine_id, m.kind, m.model, m.task_title, m.task_ref,
                                 m.cost_usd_micros, m.input_tokens, m.output_tokens, m.reasoning_tokens,
                                 m.cache_read_tokens, m.cache_create_tokens, m.cache_create_5m_tokens,
                                 m.cache_create_1h_tokens)::sessions.session_rollup_event)
              FROM moved AS m WHERE m.side = 'new') END
    INTO usage_pairs, is_bulk, bulk_sessions, removed, added;
  removed := coalesce(removed, '{}');
  added := coalesce(added, '{}');
  IF jsonb_array_length(usage_pairs) = 0 AND NOT is_bulk
     AND cardinality(removed) + cardinality(added) = 0 THEN
    RETURN NULL;
  END IF;

  -- Every touched session's lock, in key order, before any of them is written.
  FOR k IN SELECT DISTINCT x.org_id, x.session_id
             FROM (SELECT p.org_id, p.session_id
                     FROM jsonb_to_recordset(usage_pairs) AS p(org_id text, session_id text)
                   UNION ALL SELECT e.org_id, e.session_id FROM unnest(removed) AS e
                   UNION ALL SELECT e.org_id, e.session_id FROM unnest(added) AS e
                   UNION ALL SELECT b.org_id, b.session_id
                     FROM jsonb_to_recordset(coalesce(bulk_sessions, '[]')) AS b(org_id text, session_id text)) AS x
            ORDER BY x.org_id, x.session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;

  -- Every in-place write of this statement (usage differences, merges) comes
  -- before any rebuild mark or note. With constraints IMMEDIATE, a dirty mark
  -- rebuilds the session as soon as it is inserted, from the events as they
  -- are now, and the pending trigger applies notes as soon as they are noted;
  -- a write after either would count this statement's rows twice.
  IF jsonb_array_length(usage_pairs) > 0 THEN
    UPDATE sessions.session_rollups AS r SET
      cost_events = r.cost_events + d.d_cost_events,
      cost_usd_micros = CASE WHEN r.cost_events + d.d_cost_events = 0 THEN NULL
                             ELSE coalesce(r.cost_usd_micros, 0) + d.d_cost END,
      input_tokens = r.input_tokens + d.d_input,
      output_tokens = r.output_tokens + d.d_output,
      reasoning_tokens = r.reasoning_tokens + d.d_reasoning,
      cache_read_tokens = r.cache_read_tokens + d.d_cache_read,
      cache_create_tokens = r.cache_create_tokens + d.d_cache_create,
      cache_create_5m_tokens = r.cache_create_5m_tokens + d.d_5m,
      cache_create_1h_tokens = r.cache_create_1h_tokens + d.d_1h,
      cache_split_incomplete_events = r.cache_split_incomplete_events + d.d_incomplete
      FROM (SELECT p.org_id, p.session_id, p.workspace_id, p.source, p.project_id,
                   sum(p.d_cost) AS d_cost, sum(p.d_cost_events) AS d_cost_events,
                   sum(p.d_input) AS d_input, sum(p.d_output) AS d_output,
                   sum(p.d_reasoning) AS d_reasoning, sum(p.d_cache_read) AS d_cache_read,
                   sum(p.d_cache_create) AS d_cache_create, sum(p.d_5m) AS d_5m,
                   sum(p.d_1h) AS d_1h, sum(p.d_incomplete) AS d_incomplete
              FROM jsonb_to_recordset(usage_pairs) AS p(
                     org_id text, session_id text, workspace_id text, source text, project_id text,
                     d_cost bigint, d_cost_events bigint, d_input bigint, d_output bigint,
                     d_reasoning bigint, d_cache_read bigint, d_cache_create bigint, d_5m bigint,
                     d_1h bigint, d_incomplete bigint)
             GROUP BY p.org_id, p.session_id, p.workspace_id, p.source, p.project_id) AS d
     WHERE r.org_id = d.org_id AND r.session_id = d.session_id
       AND r.workspace_id = d.workspace_id AND r.source = d.source
       AND r.project_id IS NOT DISTINCT FROM d.project_id
       -- A session rebuilt at commit reads the new values itself.
       AND NOT EXISTS (SELECT 1 FROM sessions.session_rollup_dirty AS x
                        WHERE x.org_id = r.org_id AND x.session_id = r.session_id);
  END IF;

  IF is_bulk THEN
    PERFORM sessions.mark_session_rollups_dirty(bulk_sessions);
  ELSE
    IF cardinality(added) > 0 THEN
      PERFORM sessions.merge_session_rollup_events(added);
    END IF;
    PERFORM sessions.note_removed_session_rollups(removed);
  END IF;
  RETURN NULL;
END;
$mark_updated$;

CREATE OR REPLACE FUNCTION sessions.mark_deleted_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $deleted$
DECLARE
  k record;
  removed sessions.onboarding_rollup_event[];
BEGIN
  IF NOT (SELECT maintenance_enabled FROM sessions.onboarding_rollup_rollout) THEN
    RETURN NULL;
  END IF;
  -- This trigger runs before the session-rollup delete trigger. Lock its
  -- complete key set first, including ineligible events, in the same order.
  FOR k IN SELECT DISTINCT org_id, session_id FROM changed_rows ORDER BY org_id, session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  -- Count eligible rows before copying any into an array; only a statement
  -- with more rows than the limit pays for the eligibility count.
  IF (SELECT count(*) FROM (SELECT 1 FROM changed_rows LIMIT sessions.session_rollup_diff_limit() + 1) AS n)
       > sessions.session_rollup_diff_limit() THEN
    IF (SELECT count(*) FROM (
          SELECT 1 FROM changed_rows AS c
           WHERE c.kind IN ('history', 'session_event') AND length(trim(coalesce(c.content, ''))) > 0
           LIMIT sessions.session_rollup_diff_limit() + 1) AS n)
         > sessions.session_rollup_diff_limit() THEN
      PERFORM sessions.mark_onboarding_rollups_dirty((
        SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', c.org_id, 'session_id', c.session_id)), '[]')
          FROM changed_rows AS c
         WHERE c.kind IN ('history', 'session_event') AND length(trim(coalesce(c.content, ''))) > 0));
      RETURN NULL;
    END IF;
  END IF;
  SELECT array_agg(ROW(c.org_id, c.workspace_id, c.user_id, c.source, c.session_id, c.ingested_at)
                     ::sessions.onboarding_rollup_event)
    INTO removed FROM changed_rows AS c
   WHERE c.kind IN ('history', 'session_event') AND length(trim(coalesce(c.content, ''))) > 0;
  IF removed IS NULL THEN RETURN NULL; END IF;
  PERFORM sessions.note_removed_onboarding_rollups(removed);
  RETURN NULL;
END;
$deleted$;

-- An eligible row whose key or receipt changed, or whose eligibility changed,
-- merges its new version now and notes its old version as removed. A receipt
-- that only moves forward keeps 0046's immediate advance.
CREATE OR REPLACE FUNCTION sessions.mark_updated_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $updated$
DECLARE
  k record;
  advances jsonb;
  is_bulk boolean;
  bulk_sessions jsonb;
  removed sessions.onboarding_rollup_event[];
  added sessions.onboarding_rollup_event[];
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
    (SELECT n.*, 'new' AS side FROM (SELECT * FROM current_values EXCEPT SELECT * FROM previous_values) AS n)
    UNION ALL
    (SELECT o.*, 'old' AS side FROM (SELECT * FROM previous_values EXCEPT SELECT * FROM current_values) AS o)
  ), forward_updates AS MATERIALIZED (
    -- A normal ingest replay advances receipt time without changing membership.
    SELECT n.* FROM current_values AS n JOIN previous_values AS o
      ON n.org_id = o.org_id AND n.workspace_id = o.workspace_id AND n.user_id = o.user_id
     AND n.source = o.source AND n.session_id = o.session_id AND n.machine_id = o.machine_id
     AND n.kind = o.kind AND n.event_id = o.event_id
     AND n.eligible AND o.eligible AND n.ingested_at >= o.ingested_at
  ), moved AS MATERIALIZED (
    SELECT c.* FROM changed AS c
     WHERE c.eligible AND NOT EXISTS (
       SELECT 1 FROM forward_updates AS f
        WHERE f.org_id = c.org_id AND f.source = c.source AND f.session_id = c.session_id
          AND f.machine_id = c.machine_id AND f.kind = c.kind AND f.event_id = c.event_id)
  ),
  -- Over the diff limit, collect only the sessions (see mark_updated_session_rollups).
  bulk AS MATERIALIZED (
    SELECT count(*) > sessions.session_rollup_diff_limit() AS over
      FROM (SELECT 1 FROM moved LIMIT sessions.session_rollup_diff_limit() + 1) AS n
  )
  SELECT (SELECT coalesce(jsonb_agg(to_jsonb(a)), '[]') FROM (
            SELECT org_id, workspace_id, user_id, source, session_id, max(ingested_at) AS newest
              FROM forward_updates GROUP BY org_id, workspace_id, user_id, source, session_id
          ) AS a),
         (SELECT b.over FROM bulk AS b),
         CASE WHEN (SELECT b.over FROM bulk AS b) THEN
           (SELECT jsonb_agg(DISTINCT jsonb_build_object('org_id', m.org_id, 'session_id', m.session_id))
              FROM moved AS m) END,
         CASE WHEN NOT (SELECT b.over FROM bulk AS b) THEN
           (SELECT array_agg(ROW(m.org_id, m.workspace_id, m.user_id, m.source, m.session_id, m.ingested_at)
                               ::sessions.onboarding_rollup_event)
              FROM moved AS m WHERE m.side = 'old') END,
         CASE WHEN NOT (SELECT b.over FROM bulk AS b) THEN
           (SELECT array_agg(ROW(m.org_id, m.workspace_id, m.user_id, m.source, m.session_id, m.ingested_at)
                               ::sessions.onboarding_rollup_event)
              FROM moved AS m WHERE m.side = 'new') END
    INTO advances, is_bulk, bulk_sessions, removed, added;
  removed := coalesce(removed, '{}');
  added := coalesce(added, '{}');

  -- In-place writes first, then marks and notes (see mark_updated_session_rollups).
  UPDATE sessions.onboarding_session_rollups AS r SET
    last_received_at = greatest(r.last_received_at, a.newest)
    FROM jsonb_to_recordset(advances) AS a(
      org_id text, workspace_id text, user_id text, source text, session_id text, newest timestamptz)
   WHERE r.org_id = a.org_id AND r.workspace_id = a.workspace_id AND r.user_id = a.user_id
     AND r.source = a.source AND r.session_id = a.session_id
     AND r.last_received_at < a.newest
     AND NOT EXISTS (SELECT 1 FROM sessions.onboarding_rollup_dirty AS d
                      WHERE d.org_id = r.org_id AND d.session_id = r.session_id);

  IF is_bulk THEN
    PERFORM sessions.mark_onboarding_rollups_dirty(bulk_sessions);
  ELSE
    IF cardinality(added) > 0 THEN
      -- 0046's insert merge over the new versions.
      INSERT INTO sessions.onboarding_session_rollups AS r
        (org_id, workspace_id, user_id, source, session_id, event_count, last_received_at)
      SELECT e.org_id, e.workspace_id, e.user_id, e.source, e.session_id,
             count(*), max(e.ingested_at)
        FROM unnest(added) AS e
       WHERE NOT EXISTS (SELECT 1 FROM sessions.onboarding_rollup_dirty AS d
                          WHERE d.org_id = e.org_id AND d.session_id = e.session_id)
       GROUP BY e.org_id, e.workspace_id, e.user_id, e.source, e.session_id
       ORDER BY e.org_id, e.workspace_id, e.user_id, e.source, e.session_id
      ON CONFLICT (org_id, workspace_id, user_id, source, session_id) DO UPDATE SET
        event_count = r.event_count + EXCLUDED.event_count,
        last_received_at = greatest(r.last_received_at, EXCLUDED.last_received_at);
    END IF;
    PERFORM sessions.note_removed_onboarding_rollups(removed);
  END IF;

  RETURN NULL;
END;
$updated$;

-- TRUNCATE sessions.convergence_events empties both rollups (0030, 0046), but
-- removal notes taken earlier in the same transaction would survive it and be
-- subtracted at commit from rows inserted after the truncate. Each truncate
-- trigger now drops its own rollup's notes with its rows. A pending row may
-- still resolve at commit; with no notes left it applies nothing.
CREATE OR REPLACE FUNCTION sessions.truncate_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $truncate_rollups$
BEGIN
  TRUNCATE sessions.session_rollups;
  PERFORM set_config('relayhistory.session_rollup_removed', '', true);
  PERFORM set_config('relayhistory.session_rollup_removed_values', '', true);
  DELETE FROM sessions.session_rollup_pending WHERE kind = 'session';
  RETURN NULL;
END;
$truncate_rollups$;

CREATE OR REPLACE FUNCTION sessions.truncate_onboarding_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $truncate$
BEGIN
  IF NOT (SELECT maintenance_enabled FROM sessions.onboarding_rollup_rollout) THEN
    RETURN NULL;
  END IF;
  TRUNCATE sessions.onboarding_session_rollups;
  PERFORM set_config('relayhistory.onboarding_rollup_removed', '', true);
  DELETE FROM sessions.session_rollup_pending WHERE kind = 'onboarding';
  RETURN NULL;
END;
$truncate$;
