-- Session wakeups are claimed in bounded, index-ordered batches, re-sent on a
-- growing backoff, and removed as soon as their target finishes.
--
-- Production (2026-10-27): session_event_outbox held ~85k live rows (92k dead,
-- 1.2 GB heap): 66,697 'session' wakeups due and never sent (oldest due
-- 10-14), ~11k 'request' wakeups due and unsent, and 4-6k 'session' rows sent
-- again every five minutes. The (available_at, sent_at) index from 0034 was
-- scanned 2.4M times, reading ~72k tuples per scan.
--
-- Why. publishSessionEvents ran after every successful non-GET request and
-- after every consumed queue message, and each run (a) deleted terminal rows
-- with a full-table anti-join and (b) selected the 50 oldest "pending" rows.
-- That selection ordered by (available_at, created_at, id), which the index
-- does not provide, and filtered `sent_at IS NULL OR sent_at < now() - 5 min`,
-- which it cannot use, so every run walked the recently sent head rows (and
-- dead tuples) before reaching eligible ones. A row sent but not yet resolved
-- became due again after a fixed five minutes with its original, oldest
-- available_at, so it always sorted ahead of every never-sent row. Concurrent
-- publishers (no claim) all picked the same oldest rows and sent duplicates,
-- the consumer spent each message on another full publish, its queue lag
-- passed five minutes, and the same head rows were re-sent each interval
-- while everything behind them starved.
-- A second defect pinned rows to the head: the 0048 trigger caps a session
-- wakeup at created_at + 60 minutes, and a consumer re-arm kept the first
-- evidence's created_at, so each later revision of a long-lived session made
-- its wakeup due days in the past.
--
-- 1. claim_session_events_v1 claims at most 100 rows per call and marks them
--    sent. Four lanes, each an index-ordered scan that locks rows FOR UPDATE
--    SKIP LOCKED while scanning (so a row an upload holds is passed over and
--    the scan continues to the next, never waiting and never ending early):
--      fresh    never sent, due within the last hour, oldest first;
--      backlog  never sent, due longer ago, oldest first;
--      resend   sent and unresolved, next_send_at reached, most overdue first;
--      legacy   sent before this migration (no next_send_at), once.
--    The lanes are merged round-robin (each lane's first row, then each
--    lane's second, ...; ties by eligibility), so an old backlog cannot hold
--    back new wakeups and re-sends cannot hold back either. A lane with
--    fewer rows leaves its share to the others. Within a lane, rate-limited
--    wakeups that come due together at the UTC reset go longest-starved
--    first (created_at).
-- 2. Re-send backoff. next_send_at is set at each send: the time outstanding
--    since the first send, between 15 minutes and 1 hour (15, 15, 30, 60, 60,
--    ...). The first re-send waits 15 minutes so a queue lag of a few minutes
--    does not duplicate every wakeup. The resend lane is a range scan on
--    next_send_at, bounded by its limit however many rows are backing off.
-- 3. Parking. sends counts the sends of the current burst: a row with
--    sent_at NULL (new, re-armed by new evidence or the consumer, released,
--    or un-parked) starts again at 1 when next sent. Only a re-send (a row
--    already sent and unresolved) can be exhausted: claimed after 20 sends of
--    one burst (~17 hours) it is quarantined instead, as the DLQ does. New
--    evidence clears that (0034 trigger) and delivery starts a fresh burst;
--    unpark_session_events_v1 releases parked rows in bulk (operators only).
--    A failed queue send releases its claims to the never-sent lanes, which
--    restarts the burst too, so repeated send failures never park a row.
-- 3a. Locks. Each lane locks up to the claim's limit while scanning, and only
--    that many rows in all are chosen, so up to three times the limit extra
--    rows stay locked until the claim's statement commits (milliseconds; an
--    upload re-arming one of them waits that long in its trigger).
-- 4. Burst and hold. The consumer's re-arm records burst_started_at, which
--    the trigger's 60-minute cap now uses instead of created_at; created_at
--    stays the row's starvation key. A rate-limit re-arm also records
--    hold_until (the UTC reset or refusal window), and new evidence never
--    makes the wakeup due before it: the outcome cannot change earlier.
-- 5. A request or job that reaches a terminal status deletes its own wakeup
--    in the same transaction, so finished work no longer stays in the table
--    until a sweep (or one more send at its lease expiry).
-- 6. prune_session_events_v1 is the bounded sweep for terminal wakeups left
--    from before (5): a limited number of non-session rows per call, skipping
--    locked rows. The Worker runs it from the minute cron only.
-- 7. claim_session_work_events_v1 claims one session's never-sent request and
--    job wakeups through the unique (kind, org_id, target) index, so a brief
--    admitted on demand, and the jobs its collection creates, are sent at once
--    without a table-wide publish.
--
-- Spend controls (model calls from background wakeups):
-- 8. Stale wakeups. evidence_at is the time of the latest evidence a session
--    wakeup carries (set by the trigger; re-arms do not move it).
--    prune_stale_session_wakeups_v1 deletes never-sent session wakeups whose
--    COALESCE(evidence_at, created_at) is older than the Worker's
--    SESSION_WAKE_MAX_AGE_HOURS, in bounded batches from the minute cron, and
--    the consumer drops any it still receives, without admitting anything. New
--    evidence records a fresh wakeup; GET /brief briefs on demand.
-- 9. Background analyses. collect_session_analysis_request_v2 no longer
--    enqueues an analysis job for a request admitted from the 'background'
--    pool (0043) unless the transaction-local setting
--    relayhistory.background_analysis is 'on' (Worker
--    SESSION_BACKGROUND_ANALYSIS_ENABLED). Such a request resolves with its
--    brief only and no error. Demand requests (GET /brief,
--    POST /analysis) are unchanged.
-- 10. POST /analysis (request_session_analysis_v1) used to answer with the
--    session's queued, running or just-created request. That may now be a
--    background request collected without an analysis, so a user's request
--    would be accepted and never analyzed. A background request not yet
--    collected (queued, or claimed and still collecting) is promoted to
--    demand, and one already collected without an analysis is not reused:
--    the user gets a new demand request.
--
-- The two partial indexes are CONCURRENT_INDEXES in apply-neon-migration.mjs,
-- built after this transaction commits. This migration adds columns that are
-- nullable or have a constant default (catalog-only; the runner bounds the
-- lock wait) and functions. It rewrites no data.

ALTER TABLE sessions.session_event_outbox
  ADD COLUMN first_sent_at timestamptz,
  ADD COLUMN next_send_at timestamptz,
  ADD COLUMN sends integer NOT NULL DEFAULT 0,
  ADD COLUMN burst_started_at timestamptz,
  ADD COLUMN hold_until timestamptz,
  ADD COLUMN evidence_at timestamptz;

CREATE OR REPLACE FUNCTION sessions.record_session_wakeup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $wake$
DECLARE k text; t text; due timestamptz;
BEGIN
  IF TG_TABLE_NAME = 'session_revisions' THEN
    k := 'session'; t := NEW.session_id; due := now() + interval '10 minutes';
  ELSE
    IF TG_TABLE_NAME = 'session_analysis_requests' THEN k := 'request'; ELSE k := NEW.kind; END IF;
    t := NEW.id::text;
    IF NEW.status NOT IN ('queued', 'running') THEN
      -- A finished target needs no further wakeups.
      IF TG_OP = 'UPDATE' THEN
        DELETE FROM sessions.session_event_outbox o
         WHERE o.kind = k AND o.org_id = NEW.org_id AND o.target = t;
      END IF;
      RETURN NEW;
    END IF;
    due := CASE WHEN NEW.status = 'running' THEN NEW.lease_expires_at ELSE NEW.available_at END;
  END IF;
  INSERT INTO sessions.session_event_outbox AS o (kind,target,org_id,session_id,available_at,evidence_at)
    VALUES (k,t,NEW.org_id,NEW.session_id,due,CASE WHEN k='session' THEN now() END)
    ON CONFLICT (kind,org_id,target) DO UPDATE SET generation=o.generation+1,
      -- GREATEST ignores a NULL hold.
      available_at=CASE WHEN k='session' THEN GREATEST(
          LEAST(EXCLUDED.available_at, COALESCE(o.burst_started_at, o.created_at) + interval '60 minutes'),
          o.hold_until)
        ELSE EXCLUDED.available_at END,
      evidence_at=CASE WHEN k='session' THEN now() ELSE o.evidence_at END,
      -- sent_at = NULL starts a new send burst: the claim restarts sends,
      -- first_sent_at and next_send_at when it next sends a never-sent row.
      sent_at=NULL, quarantined_at=NULL;
  RETURN NEW;
END;
$wake$;

CREATE FUNCTION sessions.claim_session_events_v1(p_limit integer)
RETURNS TABLE(event_id uuid, generation bigint, available_at timestamptz, send_order bigint, parked boolean)
LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $claim_events$
  WITH fresh AS (
    SELECT o.id, o.available_at AS eligible_at, o.created_at, o.sends
      FROM sessions.session_event_outbox o
     WHERE o.quarantined_at IS NULL AND o.sent_at IS NULL
       AND o.available_at > now() - interval '1 hour'
       AND o.available_at <= now() + interval '60 seconds'
     ORDER BY o.available_at, o.created_at, o.id
     LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE OF o SKIP LOCKED
  ), backlog AS (
    SELECT o.id, o.available_at AS eligible_at, o.created_at, o.sends
      FROM sessions.session_event_outbox o
     WHERE o.quarantined_at IS NULL AND o.sent_at IS NULL
       AND o.available_at <= now() - interval '1 hour'
     ORDER BY o.available_at, o.created_at, o.id
     LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE OF o SKIP LOCKED
  ), resend AS (
    SELECT o.id, o.next_send_at AS eligible_at, o.created_at, o.sends
      FROM sessions.session_event_outbox o
     WHERE o.quarantined_at IS NULL AND o.sent_at IS NOT NULL
       AND o.next_send_at <= now()
     ORDER BY o.next_send_at, o.id
     LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE OF o SKIP LOCKED
  ), legacy AS (
    -- Sent by the pre-0062 publisher, which set no next_send_at: re-sent once
    -- on its old five-minute rule, after which the claim sets next_send_at.
    -- Index order is (next_send_at, id), so these NULL entries come by id.
    SELECT o.id, o.sent_at + interval '5 minutes' AS eligible_at, o.created_at, o.sends
      FROM sessions.session_event_outbox o
     WHERE o.quarantined_at IS NULL AND o.sent_at IS NOT NULL
       AND o.next_send_at IS NULL AND o.sent_at < now() - interval '5 minutes'
     ORDER BY o.next_send_at, o.id
     LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE OF o SKIP LOCKED
  ), lanes AS (
    SELECT l.*, row_number() OVER (PARTITION BY l.lane ORDER BY l.eligible_at, l.created_at, l.id) AS turn
      FROM (SELECT 1 AS lane, * FROM fresh UNION ALL SELECT 2, * FROM backlog
            UNION ALL SELECT 3, * FROM resend UNION ALL SELECT 4, * FROM legacy) l
  ), ranked AS (
    -- Only already-sent rows (re-send and legacy lanes) count their burst's
    -- sends; a never-sent row's counter is stale and restarts at 1 below.
    SELECT c.id, c.sends >= 20 AND c.lane IN (3, 4) AS exhausted,
           row_number() OVER (ORDER BY c.turn, c.eligible_at, c.created_at, c.id) AS send_order
      FROM lanes c
  ), chosen AS (
    -- The constant LIMIT is the planner's row estimate: without it the
    -- updates below hash-join a sequential scan of the outbox.
    SELECT r.* FROM ranked r WHERE r.send_order <= LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     ORDER BY r.send_order LIMIT 100
  ), parked AS (
    UPDATE sessions.session_event_outbox o SET quarantined_at = now()
      FROM chosen r WHERE o.id = r.id AND r.exhausted
    RETURNING o.id, o.generation, o.available_at, r.send_order, true AS parked
  ), sent AS (
    UPDATE sessions.session_event_outbox o SET sent_at = now(),
        sends = CASE WHEN o.sent_at IS NULL THEN 1 ELSE o.sends + 1 END,
        first_sent_at = CASE WHEN o.sent_at IS NULL THEN now() ELSE COALESCE(o.first_sent_at, o.sent_at) END,
        next_send_at = now() + LEAST(interval '1 hour', GREATEST(interval '15 minutes',
          now() - CASE WHEN o.sent_at IS NULL THEN now() ELSE COALESCE(o.first_sent_at, o.sent_at) END))
      FROM chosen r WHERE o.id = r.id AND NOT r.exhausted
    RETURNING o.id, o.generation, o.available_at, r.send_order, false AS parked
  )
  SELECT * FROM sent UNION ALL SELECT * FROM parked;
$claim_events$;

CREATE FUNCTION sessions.claim_session_work_events_v1(p_org_id text, p_session_id text)
RETURNS TABLE(event_id uuid, generation bigint, available_at timestamptz, send_order bigint, parked boolean)
LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $claim_work$
  WITH target AS (
    SELECT 'request'::text AS kind, r.id::text AS target FROM sessions.session_analysis_requests r
     WHERE r.org_id = p_org_id AND r.session_id = p_session_id AND r.status IN ('queued', 'running')
    UNION ALL
    SELECT j.kind, j.id::text FROM sessions.session_analysis_jobs j
     WHERE j.org_id = p_org_id AND j.session_id = p_session_id AND j.status IN ('queued', 'running')
  ), due AS (
    SELECT o.id, o.available_at, o.created_at
      FROM sessions.session_event_outbox o
      JOIN target t ON o.kind = t.kind AND o.org_id = p_org_id AND o.target = t.target
     WHERE o.quarantined_at IS NULL AND o.sent_at IS NULL
       AND o.available_at <= now() + interval '60 seconds'
     ORDER BY o.available_at, o.created_at, o.id
     LIMIT 20
     FOR UPDATE OF o SKIP LOCKED
  ), ranked AS (
    SELECT d.id, row_number() OVER (ORDER BY d.available_at, d.created_at, d.id) AS send_order FROM due d
  )
  UPDATE sessions.session_event_outbox o SET sent_at = now(), sends = 1, first_sent_at = now(),
      next_send_at = now() + interval '15 minutes'
    FROM ranked r WHERE o.id = r.id
  RETURNING o.id, o.generation, o.available_at, r.send_order, false;
$claim_work$;

-- Returns claims whose send failed, so the next publish retries them. Fenced by
-- generation and skips rows another transaction holds (an upload re-arming a
-- session wakeup has already cleared sent_at).
CREATE FUNCTION sessions.release_session_event_claims_v1(p_ids uuid[], p_generations bigint[])
RETURNS void
LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $release_events$
  WITH held AS (
    SELECT o.id FROM sessions.session_event_outbox o
      JOIN unnest(p_ids, p_generations) AS claimed(id, generation)
        ON o.id = claimed.id AND o.generation = claimed.generation
     WHERE o.quarantined_at IS NULL
     FOR UPDATE OF o SKIP LOCKED
  )
  UPDATE sessions.session_event_outbox o SET sent_at = NULL
    FROM held WHERE o.id = held.id;
$release_events$;

-- Deletes up to p_limit request/job wakeups whose target is no longer queued or
-- running. Session wakeups are never touched, so no upload's row is involved.
CREATE FUNCTION sessions.prune_session_events_v1(p_limit integer)
RETURNS integer
LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $prune_events$
  WITH dead AS (
    SELECT o.id FROM sessions.session_event_outbox o
     WHERE o.kind IN ('request', 'brief', 'analysis')
       AND ((o.kind = 'request' AND NOT EXISTS (
              SELECT 1 FROM sessions.session_analysis_requests r
               WHERE r.id = CASE WHEN o.kind = 'request' THEN o.target::uuid END
                 AND r.status IN ('queued', 'running')))
         OR (o.kind IN ('brief', 'analysis') AND NOT EXISTS (
              SELECT 1 FROM sessions.session_analysis_jobs j
               WHERE j.id = CASE WHEN o.kind IN ('brief', 'analysis') THEN o.target::uuid END
                 AND j.status IN ('queued', 'running'))))
     LIMIT LEAST(1000, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE SKIP LOCKED
  ), gone AS (
    DELETE FROM sessions.session_event_outbox o USING dead WHERE o.id = dead.id RETURNING 1
  )
  SELECT count(*)::integer FROM gone;
$prune_events$;

-- Operators only (no runtime grant): returns up to p_limit wakeups the claim
-- parked (quarantined after 20 sends, e.g. while the consumer was down) to the
-- never-sent queue, oldest first, and reports how many. Run it repeatedly to
-- release more; ambiguous-attribution and DLQ quarantines are left alone
-- unless they also reached 20 sends. Not index-supported: it reads the
-- quarantined rows it skips past, so keep p_limit small on a busy table.
CREATE FUNCTION sessions.unpark_session_events_v1(p_limit integer)
RETURNS integer
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $unpark_events$
  WITH parked AS (
    SELECT o.id FROM sessions.session_event_outbox o
     WHERE o.quarantined_at IS NOT NULL AND o.sends >= 20
     ORDER BY o.quarantined_at, o.id
     LIMIT LEAST(10000, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE SKIP LOCKED
  ), released AS (
    UPDATE sessions.session_event_outbox o
       SET quarantined_at = NULL, sent_at = NULL, sends = 0, next_send_at = NULL
      FROM parked WHERE o.id = parked.id
    RETURNING 1
  )
  SELECT count(*)::integer FROM released;
$unpark_events$;
REVOKE ALL ON FUNCTION sessions.unpark_session_events_v1(integer) FROM PUBLIC;

-- Deletes up to p_limit never-sent session wakeups whose latest evidence (or,
-- for a wakeup recorded before evidence_at existed, its creation) is older
-- than p_max_age_hours. Reads the never-sent index from the oldest due time,
-- skipping locked rows; a stale wakeup re-armed to a later due time is left
-- to the consumer, which drops it when it comes due.
CREATE FUNCTION sessions.prune_stale_session_wakeups_v1(p_max_age_hours integer, p_limit integer)
RETURNS integer
LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $prune_stale$
  WITH stale AS (
    SELECT o.id FROM sessions.session_event_outbox o
     WHERE o.quarantined_at IS NULL AND o.sent_at IS NULL
       AND o.available_at <= now() - make_interval(hours => GREATEST(1, p_max_age_hours))
       AND o.kind = 'session'
       AND COALESCE(o.evidence_at, o.created_at) <= now() - make_interval(hours => GREATEST(1, p_max_age_hours))
     ORDER BY o.available_at, o.created_at, o.id
     LIMIT LEAST(5000, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE OF o SKIP LOCKED
  ), gone AS (
    DELETE FROM sessions.session_event_outbox o USING stale WHERE o.id = stale.id RETURNING 1
  )
  SELECT count(*)::integer FROM gone;
$prune_stale$;

-- 0024's collection, unchanged except that a background-pool request does not
-- enqueue an analysis job unless relayhistory.background_analysis is 'on'
-- (set transaction-locally by the Worker). Such a request resolves with its
-- brief only and no error, so it never reads as rate-limited and no wakeup
-- sleeps until the UTC reset waiting for an analysis slot.
CREATE OR REPLACE FUNCTION sessions.collect_session_analysis_request_v2(
  p_request_id uuid, p_lease_token uuid, p_source_provider text,
  p_brief_fingerprint text, p_summary_version text, p_summary_model_version text,
  p_brief_source jsonb,
  p_analysis_fingerprint text, p_analysis_version text, p_schema_version integer,
  p_model_version text, p_analysis_source jsonb
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $collect_v2$
DECLARE
  request_row sessions.session_analysis_requests%ROWTYPE;
  brief_id uuid;
  analysis_id uuid;
  same_analysis uuid;
  same_status text;
  deferred boolean;
  brief_only boolean;
BEGIN
  SELECT * INTO request_row FROM sessions.session_analysis_requests
   WHERE id = p_request_id FOR UPDATE;
  IF NOT FOUND OR request_row.status <> 'running'
     OR p_lease_token IS NULL OR request_row.lease_token IS NULL
     OR request_row.lease_token IS DISTINCT FROM p_lease_token
     OR request_row.lease_expires_at IS NULL
     OR request_row.lease_expires_at <= clock_timestamp() THEN
    RETURN jsonb_build_object('outcome', 'lease-lost');
  END IF;

  brief_id := sessions.enqueue_session_job_v2('brief',
    request_row.org_id, request_row.workspace_id, request_row.session_id,
    request_row.requested_by_user_id, p_source_provider, p_brief_fingerprint,
    '', 0, p_summary_version, '', p_summary_model_version, p_brief_source);
  UPDATE sessions.session_analysis_jobs
     SET status = 'failed', last_error = 'superseded', updated_at = now()
   WHERE org_id = request_row.org_id AND session_id = request_row.session_id
     AND kind = 'brief' AND status = 'queued' AND id <> brief_id;

  brief_only := request_row.admission = 'background'
    AND current_setting('relayhistory.background_analysis', true) IS DISTINCT FROM 'on';

  SELECT id, status INTO same_analysis, same_status FROM sessions.session_analysis_jobs
   WHERE org_id = request_row.org_id AND session_id = request_row.session_id
     AND kind = 'analysis' AND source_provider = p_source_provider
     AND source_fingerprint = p_analysis_fingerprint AND analysis_version = p_analysis_version
     AND schema_version = p_schema_version AND summary_version = ''
     AND model_version = p_model_version AND summary_model_version = '';
  -- Reusing a queued, running or completed job spends nothing; a new job or a
  -- failed job's requeue waits for the session's running analysis and cooldown.
  deferred := (same_analysis IS NULL OR same_status = 'failed') AND (
    EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
             WHERE org_id = request_row.org_id AND session_id = request_row.session_id
               AND kind = 'analysis' AND status = 'running')
    OR sessions.session_analysis_cooling(request_row.org_id, request_row.session_id));
  IF NOT deferred AND NOT brief_only THEN
    UPDATE sessions.session_analysis_jobs
       SET status = 'failed', last_error = 'superseded', updated_at = now()
     WHERE org_id = request_row.org_id AND session_id = request_row.session_id
       AND kind = 'analysis' AND status = 'queued' AND id IS DISTINCT FROM same_analysis;
    analysis_id := sessions.enqueue_session_job_v2('analysis',
      request_row.org_id, request_row.workspace_id, request_row.session_id,
      request_row.requested_by_user_id, p_source_provider, p_analysis_fingerprint,
      p_analysis_version, p_schema_version, '', p_model_version, '', p_analysis_source);
  END IF;

  UPDATE sessions.session_analysis_requests
     SET status = 'resolved', brief_job_id = brief_id, analysis_job_id = analysis_id,
         last_error = CASE WHEN NOT deferred AND NOT brief_only AND analysis_id IS NULL
                           THEN 'session_analysis_rate_limited' END,
         lease_token = NULL, lease_expires_at = NULL, updated_at = now()
   WHERE id = request_row.id;
  RETURN jsonb_build_object('outcome', 'collected', 'briefJobId', brief_id, 'analysisJobId', analysis_id);
END;
$collect_v2$;

-- 0038's request_session_analysis_v1 with item 10 above; every other rule is
-- unchanged.
CREATE OR REPLACE FUNCTION sessions.request_session_analysis_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request_analysis$
DECLARE
  existing sessions.session_analysis_requests%ROWTYPE;
  latest sessions.session_analysis_requests%ROWTYPE;
  active record;
  briefed_only boolean;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  -- Check evidence only after deletion/admission serialization has settled.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  -- A background request not yet collected now collects as a demand one,
  -- analysis included. Collection reads admission under its row lock: if it
  -- is collecting right now, this waits for it, finds the row resolved and
  -- skips it, and the brief-only check below makes a new request instead.
  -- (Collection then takes only the 'session-analysis:' lock, not this
  -- function's, so the wait cannot deadlock.)
  UPDATE sessions.session_analysis_requests SET admission = 'demand', updated_at = now()
   WHERE org_id = p_org_id AND session_id = p_session_id
     AND status IN ('queued', 'running') AND admission = 'background';
  SELECT * INTO latest FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;
  briefed_only := latest.admission = 'background' AND latest.status = 'resolved'
    AND latest.analysis_job_id IS NULL;

  SELECT * INTO active FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
   WHERE status IN ('queued', 'running')
   ORDER BY request_created_at DESC, request_id DESC LIMIT 1;
  IF active.request_id IS NOT NULL
     AND NOT (briefed_only AND active.request_id = latest.id) THEN
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', active.request_id, 'sessionId', active.session_id, 'status', active.status,
      'analysisId', active.job_id, 'error', NULL,
      'createdAt', active.request_created_at, 'updatedAt', active.request_updated_at));
  END IF;

  -- The per-session limits shared with session_brief_v2: a throttled request
  -- returns the session's latest request, unless that is a brief-only one.
  IF NOT briefed_only AND sessions.session_request_limit(p_org_id, p_session_id) = 'throttled' THEN
    SELECT * INTO active FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
     ORDER BY request_created_at DESC, request_id DESC LIMIT 1;
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', active.request_id, 'sessionId', active.session_id, 'status', active.status,
      'analysisId', active.job_id, 'error', active.error,
      'createdAt', active.request_created_at, 'updatedAt', active.request_updated_at));
  END IF;
  IF sessions.session_request_limit(p_org_id, p_session_id) = 'capped'
     OR sessions.sessions_admitted_today(p_org_id, p_session_id) >= 20 THEN
    RETURN jsonb_build_object('outcome', 'rate_limited');
  END IF;

  INSERT INTO sessions.session_analysis_requests (org_id, workspace_id, session_id, requested_by_user_id)
    VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id)
    RETURNING * INTO existing;
  RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
    'id', existing.id, 'sessionId', existing.session_id, 'status', existing.status,
    'analysisId', NULL, 'error', NULL,
    'createdAt', existing.created_at, 'updatedAt', existing.updated_at));
END;
$request_analysis$;
