-- A busy session's revision event is acknowledged, and its read no longer scans
-- the session.
--
-- Production (2026-10-27): about 5,860 session_revision_events stayed
-- unacknowledged, re-sent about 1,000 times per ten minutes. Almost all came
-- from session_revisions (no workspace_id), so each send cost Cloud a
-- read_session_revision_event_v1 that aggregated every event of the session.
-- The oldest, created 10-26 03:09Z, had reached generation 500,440: its
-- session is revised on every upload, faster than Cloud reads and acknowledges
-- it, so an ack at the generation Cloud read always lost the fence and the row
-- was never acknowledged.
--
-- 1. An ack for an older generation now counts as progress. Cloud processed
--    the session as of that generation, and only revisions since then remain,
--    so the row starts a new burst as if just sent (created_at = sent_at =
--    now()). claim_session_revision_events_v1's floor then publishes it again
--    five minutes later. A session revised continuously is processed about
--    once per five minutes plus Cloud's processing time instead of never.
--    The row stays unacknowledged and its generation is unchanged, so the
--    fence still holds: only an ack of the current generation acknowledges it.
--    Without progress (no ack at all), 0055's growing backoff applies as before.
-- 2. read_session_revision_event_v1 attributes a row without a workspace from
--    session_rollups, a few rows per session kept in the event writer's
--    transaction (0030), instead of every convergence_events row. Until the
--    rollups backfill is complete it keeps reading events, as the session list
--    does. Conversation turns are still read directly; they have no rollup.
--    Rollups follow convergence_events exactly, so attribution lags only where
--    events do (e.g. while a delivery-projection rollout is unfinished), the
--    same as the scan it replaces.
-- 3. acked_generation records the generation of the last ack, current or
--    stale, so generation - acked_generation shows how far revisions outran
--    Cloud, and sent_at with no acked_generation shows sends never acked.
--
-- One nullable column without a default (a catalog-only change; the runner
-- bounds the lock wait) and functions: no index, trigger or data rewrite.

ALTER TABLE sessions.session_revision_events ADD COLUMN acked_generation bigint;

CREATE OR REPLACE FUNCTION sessions.ack_session_revision_events_v1(p_ids uuid[], p_generations bigint[])
RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $ack_revision$
 UPDATE sessions.session_revision_events e SET
     acknowledged_at = CASE WHEN e.generation = acked.generation THEN now() END,
     acked_generation = acked.generation,
     created_at = CASE WHEN e.generation = acked.generation THEN e.created_at ELSE now() END,
     sent_at = CASE WHEN e.generation = acked.generation THEN e.sent_at ELSE now() END
   FROM (SELECT id, max(generation) AS generation FROM unnest(p_ids, p_generations) AS a(id, generation)
          GROUP BY id) acked
  WHERE e.id = acked.id AND e.generation >= acked.generation AND e.acknowledged_at IS NULL
    -- Only a newer ack is progress: a duplicate or late ack of a generation
    -- already recorded (say a second worker's, after a new revision) leaves
    -- the clock alone, and acked_generation never moves backward.
    AND (e.acked_generation IS NULL OR acked.generation > e.acked_generation);
$ack_revision$;

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
 CROSS JOIN (SELECT EXISTS (SELECT 1 FROM sessions.session_rollup_rollout
                             WHERE completed_at IS NOT NULL) AS done) rolled
 CROSS JOIN LATERAL (
   -- Only a row without its own workspace is attributed from evidence; for the
   -- others every branch is skipped by its constant filter, and exactly one of
   -- the rollup and event branches runs for the rest.
   SELECT count(DISTINCT evidence.workspace_id) > 1 OR count(DISTINCT evidence.source) > 1 AS ambiguous,
     min(evidence.workspace_id) AS workspace_id, min(evidence.user_id) AS user_id,
     min(evidence.source) AS source
   FROM (
     SELECT r.workspace_id, u.user_id, NULLIF(r.source, '') AS source
       FROM sessions.session_rollups r CROSS JOIN LATERAL unnest(r.user_ids) AS u(user_id)
      WHERE e.workspace_id IS NULL AND rolled.done AND r.org_id = e.org_id AND r.session_id = e.session_id
     UNION ALL
     SELECT c.workspace_id, c.user_id, NULLIF(c.source, '') AS source FROM sessions.convergence_events c
      WHERE e.workspace_id IS NULL AND NOT rolled.done AND c.org_id = e.org_id AND c.session_id = e.session_id
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
