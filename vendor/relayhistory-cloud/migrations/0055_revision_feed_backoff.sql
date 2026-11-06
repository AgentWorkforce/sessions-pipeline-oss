-- Bound the revision feed's re-sends so a slow consumer cannot amplify load.
--
-- Production (2026-10-26): 4,831 unacknowledged session_revision_events were
-- re-sent every five minutes (4,830 sends in ten minutes against 93 new events
-- an hour). Each send costs Cloud a read_session_revision_event_v1 and a
-- session_pull_request_evidence_v1, both of which read the whole session. As
-- those reads slowed, they overlapped and timed out, so almost nothing was
-- acknowledged after 11:00Z (3 acks an hour), every event stayed due, and the
-- reads (70+ in flight, 50-120 s each) starved delivery batches into 503s.
--
-- 1. Re-send backoff. An unacknowledged row is due again once the time since
--    its last send is at least as long as it has been outstanding (created_at
--    is the start of the current burst), between 5 minutes and 1 hour:
--    roughly 5, 5, 10, 20, 40, 60, 60... minutes.
-- 2. A newer revision no longer clears sent_at. It still bumps the generation,
--    so the next send carries it and an older ack stays fenced, but a session
--    that changes on every upload is published at most once per backoff
--    interval instead of after every batch. A never-sent row is unchanged.
-- 3. pending_session_revision_events_v1 (Cloud's per-minute recovery) lists
--    only rows whose next send is more than 10 minutes overdue, that is, rows
--    the publisher has not covered, instead of every due row the publisher
--    just sent.
-- 4. read_session_revision_event_v1 returns NULL for an acknowledged row, so a
--    stale or duplicate queue message does no work, and reads the session's
--    evidence for attribution only when the row carries no workspace.
--
-- Functions only: no table, index, trigger or lock change.

CREATE OR REPLACE FUNCTION sessions.enqueue_session_revision_event(
  p_org_id text, p_session_id text, p_revision bigint,
  p_workspace_id text DEFAULT NULL, p_source text DEFAULT NULL, p_user_id text DEFAULT NULL
) RETURNS void LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $enqueue_revision$
  INSERT INTO sessions.session_revision_events AS e
      (org_id, session_id, workspace_id, source, user_id, revision, available_at)
    VALUES (p_org_id, p_session_id, p_workspace_id, p_source, p_user_id, p_revision,
      now() + interval '5 seconds')
    ON CONFLICT (org_id, session_id, (COALESCE(workspace_id, '')), (COALESCE(source, ''))) DO UPDATE SET
      generation = e.generation + 1,
      revision = EXCLUDED.revision,
      user_id = EXCLUDED.user_id,
      created_at = CASE WHEN e.acknowledged_at IS NULL THEN e.created_at ELSE now() END,
      available_at = LEAST(EXCLUDED.available_at,
        CASE WHEN e.acknowledged_at IS NULL THEN e.created_at ELSE now() END + interval '30 seconds'),
      acknowledged_at = NULL;
$enqueue_revision$;

CREATE OR REPLACE FUNCTION sessions.claim_session_revision_events_v1(p_limit integer)
RETURNS TABLE(event_id uuid, generation bigint, available_at timestamptz)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $claim_revision$
  WITH due AS (
    SELECT e.id FROM sessions.session_revision_events e
     WHERE e.acknowledged_at IS NULL AND e.available_at <= now() + interval '60 seconds'
       AND (e.sent_at IS NULL OR e.sent_at < now() - LEAST(interval '1 hour',
             GREATEST(interval '5 minutes', e.sent_at - e.created_at)))
     ORDER BY e.available_at, e.id
     LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)))
     FOR UPDATE SKIP LOCKED
  )
  UPDATE sessions.session_revision_events e SET sent_at = now()
    FROM due WHERE e.id = due.id
  RETURNING e.id, e.generation, e.available_at;
$claim_revision$;

CREATE OR REPLACE FUNCTION sessions.pending_session_revision_events_v1(p_limit integer)
RETURNS TABLE(event_id uuid, generation bigint)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $pending_revision$
 SELECT e.id, e.generation FROM sessions.session_revision_events e
  WHERE e.acknowledged_at IS NULL AND e.available_at <= now()
    AND COALESCE(e.sent_at, e.available_at) < now() - interval '10 minutes'
      - LEAST(interval '1 hour', GREATEST(interval '5 minutes', COALESCE(e.sent_at - e.created_at, interval '0')))
  ORDER BY e.available_at, e.id LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)));
$pending_revision$;

CREATE OR REPLACE FUNCTION sessions.read_session_revision_event_v1(p_id uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $read_revision$
 SELECT jsonb_build_object('eventId', e.id, 'generation', e.generation, 'orgId', e.org_id,
   'sessionId', e.session_id, 'revision', e.revision,
   'workspaceId', CASE WHEN e.workspace_id IS NOT NULL THEN e.workspace_id
                       WHEN o.ambiguous THEN NULL ELSE o.workspace_id END,
   'userId', CASE WHEN e.workspace_id IS NOT NULL THEN COALESCE(
                    (SELECT sc.user_id FROM sessions.session_catalog sc
                      WHERE sc.org_id = e.org_id AND sc.workspace_id = e.workspace_id
                        AND sc.source = e.source AND sc.session_id = e.session_id),
                    e.user_id)
                  WHEN o.ambiguous THEN NULL ELSE o.user_id END,
   'source', CASE WHEN e.workspace_id IS NOT NULL THEN e.source
                  WHEN o.ambiguous THEN NULL ELSE o.source END,
   'ambiguous', e.workspace_id IS NULL AND o.ambiguous)
 FROM sessions.session_revision_events e
 CROSS JOIN LATERAL (
   -- Only a row without its own workspace is attributed from evidence; for the
   -- others both branches are skipped by the constant filter.
   SELECT count(DISTINCT evidence.workspace_id) > 1 OR count(DISTINCT evidence.source) > 1 AS ambiguous,
     min(evidence.workspace_id) AS workspace_id, min(evidence.user_id) AS user_id,
     min(evidence.source) AS source
   FROM (
     SELECT c.workspace_id, c.user_id, NULLIF(c.source, '') AS source FROM sessions.convergence_events c
      WHERE e.workspace_id IS NULL AND c.org_id = e.org_id AND c.session_id = e.session_id
     UNION ALL
     SELECT t.workspace_id, NULL::text,
       CASE WHEN NULLIF(COALESCE(NULLIF(t.metadata->>'nativeCli', ''), t.metadata->>'source'), '') IS NOT NULL
              OR t.source IS DISTINCT FROM t.session_owner
            THEN NULLIF(t.source, '') END
     FROM sessions.conversation_turns t
      WHERE e.workspace_id IS NULL AND t.org_id = e.org_id AND t.session_id = e.session_id
   ) evidence
 ) o
 WHERE e.id = p_id AND e.acknowledged_at IS NULL;
$read_revision$;
