-- The revision feed's claim enforces an in-flight cap atomically and sends
-- never-sent revisions before retries.
--
-- Production (2026-10-27, after 0057): with about 9,650 unacknowledged rows
-- the publisher sent about 1,850 events per five minutes while Cloud's
-- consumer acknowledged 60-200, so Cloud's queue piled up again with session
-- evidence reads running up to 92 s (the 2026-10-26 failure). A cap counted by
-- the Worker before calling the claim is not enough:
-- - Publishers run after every upload and on the minute tick, so several can
--   read the same count and each claim up to the room, overshooting the cap.
-- - claim_session_revision_events_v1 orders by available_at only. With more
--   overdue retries than the cap allows per window, retries of old rows that
--   Cloud has not acknowledged take every slot and a new revision is never
--   sent.
--
-- claim_session_revision_events_v2(p_limit, p_max_in_flight):
-- - Takes a transaction advisory try-lock, so one claim runs at a time; a
--   concurrent publisher claims nothing and the next publish (an upload or
--   the minute tick) picks up.
-- - Counts sends from the last 15 minutes that are still unacknowledged and
--   claims at most p_max_in_flight minus that (and at most p_limit, 1..100).
--   Older sends stop counting, so a lost message cannot wedge the feed.
-- - Claims never-sent rows first, each by available_at, so new revisions
--   reach Cloud while retries of a backlog wait their turn. A tenth of the
--   room is kept for retries, in expectation: floor(room / 10) slots plus one
--   more with probability (room % 10) / 10. Slots usually free a few at a
--   time as Cloud acknowledges, and plain floor(room / 10) would then reserve
--   none. Retries also take whatever room new rows leave, so retries are never
--   shut out by a steady stream of new revisions, and room kept for retries
--   that none were due to use goes back to new rows. Cloud's per-minute recovery
--   (pending_session_revision_events_v1) remains a backstop for both.
-- - "Never sent" is sent_at IS NULL. A newer generation of a row Cloud has not
--   acknowledged keeps its sent_at (0055), so it is a retry here: Cloud already
--   holds that session, and 0057 re-arms it when its ack is stale.
-- - Each row comes back with `retry`, because UPDATE ... RETURNING has no
--   order: the publisher sends in (retry, available_at) order, so a failed
--   send releases retries, not new revisions.
-- Retry spacing (0055) is unchanged; v1 stays for any caller that has not
-- moved. Functions only here. The never-sent branch, the count and the retry
-- branch each read a partial index on session_revision_events (both in
-- CONCURRENT_INDEXES, built after this commits; until then the 0035 pending
-- index serves them, more slowly).
CREATE FUNCTION sessions.claim_session_revision_events_v2(p_limit integer, p_max_in_flight integer)
RETURNS TABLE(event_id uuid, generation bigint, available_at timestamptz, retry boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $claim_revision$
DECLARE
  room integer;
  retry_share integer;
  claimed integer;
  retried integer;
BEGIN
  IF NOT pg_try_advisory_xact_lock(hashtextextended('relayhistory.revision_feed_claim', 0)) THEN
    RETURN;
  END IF;
  SELECT GREATEST(0, COALESCE(p_max_in_flight, 300) - count(*)::integer) INTO room
    FROM sessions.session_revision_events f
   WHERE f.acknowledged_at IS NULL AND f.sent_at IS NOT NULL
     AND f.sent_at > now() - interval '15 minutes';
  room := LEAST(room, 100, GREATEST(1, COALESCE(p_limit, 1)));
  IF room = 0 THEN
    RETURN;
  END IF;
  retry_share := room / 10 + CASE WHEN random() < (room % 10) / 10.0 THEN 1 ELSE 0 END;

  RETURN QUERY
  WITH due AS (
    SELECT e.id FROM sessions.session_revision_events e
     WHERE e.acknowledged_at IS NULL AND e.sent_at IS NULL
       AND e.available_at <= now() + interval '60 seconds'
     ORDER BY e.available_at, e.id
     LIMIT room - retry_share
     FOR UPDATE SKIP LOCKED
  )
  UPDATE sessions.session_revision_events e SET sent_at = now()
    FROM due WHERE e.id = due.id
  RETURNING e.id, e.generation, e.available_at, false;
  GET DIAGNOSTICS claimed = ROW_COUNT;

  RETURN QUERY
  WITH due AS (
    SELECT e.id FROM sessions.session_revision_events e
     WHERE e.acknowledged_at IS NULL AND e.sent_at IS NOT NULL
       AND e.sent_at < now() - interval '5 minutes'
       AND e.sent_at < now() - LEAST(interval '1 hour',
             GREATEST(interval '5 minutes', e.sent_at - e.created_at))
       AND e.available_at <= now() + interval '60 seconds'
     ORDER BY e.sent_at, e.id
     LIMIT room - claimed
     FOR UPDATE SKIP LOCKED
  )
  UPDATE sessions.session_revision_events e SET sent_at = now()
    FROM due WHERE e.id = due.id
  RETURNING e.id, e.generation, e.available_at, true;
  GET DIAGNOSTICS retried = ROW_COUNT;

  -- Room kept for retries that none were due to use goes back to new rows.
  IF claimed + retried < room THEN
    RETURN QUERY
    WITH due AS (
      SELECT e.id FROM sessions.session_revision_events e
       WHERE e.acknowledged_at IS NULL AND e.sent_at IS NULL
         AND e.available_at <= now() + interval '60 seconds'
       ORDER BY e.available_at, e.id
       LIMIT room - claimed - retried
       FOR UPDATE SKIP LOCKED
    )
    UPDATE sessions.session_revision_events e SET sent_at = now()
      FROM due WHERE e.id = due.id
    RETURNING e.id, e.generation, e.available_at, false;
  END IF;
END;
$claim_revision$;
REVOKE ALL ON FUNCTION sessions.claim_session_revision_events_v2(integer, integer) FROM PUBLIC;
