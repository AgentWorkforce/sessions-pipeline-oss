-- The revision feed publisher claims rows instead of reading them and then
-- marking each one sent.
--
-- publishSessionRevisions read up to 50 due rows, then per row sent the queue
-- message and ran `UPDATE ... SET sent_at=now() WHERE id=$1 AND generation=$2`.
-- It runs after every non-GET request, on the minute tick and after every queue
-- batch, so under upload load dozens run at once and all read the same rows.
-- accept_delivery_batch re-enqueues a session's row through the revision
-- trigger (0035) and holds that row lock until the batch commits; a batch
-- waiting on storage held it for 40-50 seconds. Every publisher queued on the
-- one locked row: production on 2026-10-23 saw up to 119 lock waiters, each
-- pinning a connection, and every waiter had already sent its duplicate
-- session.revised message before blocking.
--
-- claim_session_revision_events_v1 marks due rows sent in one statement and
-- skips rows another transaction holds, so concurrent publishers split the due
-- rows and never wait. A skipped row is either claimed by another publisher or
-- being re-enqueued by an ingest batch whose commit resets sent_at to NULL, so
-- a later publish picks it up with the newer generation. Due-ness is unchanged:
-- unacknowledged, available within 60 seconds, and unsent or sent more than
-- five minutes ago.
--
-- release_session_revision_claims_v1 returns claims whose send failed so they
-- are retried on the next publish rather than after the five-minute resend.
-- It is generation-fenced and skips held rows for the same reasons.
CREATE FUNCTION sessions.claim_session_revision_events_v1(p_limit integer)
RETURNS TABLE(event_id uuid, generation bigint, available_at timestamptz)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $claim_revision$
  WITH due AS (
    SELECT e.id FROM sessions.session_revision_events e
     WHERE e.acknowledged_at IS NULL AND e.available_at <= now() + interval '60 seconds'
       AND (e.sent_at IS NULL OR e.sent_at < now() - interval '5 minutes')
     ORDER BY e.available_at, e.id
     LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE SKIP LOCKED
  )
  UPDATE sessions.session_revision_events e SET sent_at = now()
    FROM due WHERE e.id = due.id
  RETURNING e.id, e.generation, e.available_at;
$claim_revision$;

CREATE FUNCTION sessions.release_session_revision_claims_v1(p_ids uuid[], p_generations bigint[])
RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $release_revision$
  WITH held AS (
    SELECT e.id FROM sessions.session_revision_events e
      JOIN unnest(p_ids, p_generations) AS claimed(id, generation)
        ON e.id = claimed.id AND e.generation = claimed.generation
     WHERE e.acknowledged_at IS NULL
     FOR UPDATE OF e SKIP LOCKED
  )
  UPDATE sessions.session_revision_events e SET sent_at = NULL
    FROM held WHERE e.id = held.id;
$release_revision$;

REVOKE ALL ON FUNCTION sessions.claim_session_revision_events_v1(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.release_session_revision_claims_v1(uuid[], bigint[]) FROM PUBLIC;
