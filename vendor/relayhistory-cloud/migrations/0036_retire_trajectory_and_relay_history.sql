-- Trajectories and Relaycast messages (the `relay` source) are no longer history.
-- The Worker now acknowledges and drops them on both upload paths; this deletes
-- what was stored before. THIS DELETES PRODUCTION DATA and cannot be undone:
--
--   * delivery records of kind `trajectory` or source `relay`/`trajectory`, with
--     their catalog acceptance rows;
--   * every convergence event from those uploads and from the legacy ingest
--     trajectory lens (source `trajectories`: its decisions, findings and
--     reflections included), and Relaycast events;
--   * the turns, outcomes, links, catalog/relationship/marker/commit-link rows,
--     rollups and timing cache rows those sources produced.
--
-- Derived analysis (briefs, patterns, memory claims) is left as it is.
--
-- convergence_events.trajectory_id is no longer mapped by the Worker but is not
-- dropped here: migrations apply before the new Worker is deployed, and the
-- running Worker still names that column in every insert and select. A later
-- migration drops it once this Worker is live.

-- Delivery writes take this per-workspace lock (project_delivery_session_v2,
-- project_delivery_catalog) before touching projections. Take every affected
-- workspace's lock up front, in a fixed order, so a concurrent batch waits for
-- this transaction instead of deadlocking with it.
SELECT pg_advisory_xact_lock(hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || org_id || chr(31) || workspace_id, 0))
  FROM (SELECT DISTINCT org_id, workspace_id FROM sessions.delivery_records
         WHERE kind = 'trajectory' OR source IN ('relay', 'trajectory')
         ORDER BY org_id, workspace_id) AS affected;

WITH retired AS (
  DELETE FROM sessions.delivery_records
   WHERE kind = 'trajectory' OR source IN ('relay', 'trajectory')
  RETURNING org_id, workspace_id, origin_id, record_id
)
DELETE FROM sessions.delivery_catalog_acceptance AS acceptance
 USING retired
 WHERE acceptance.org_id = retired.org_id
   AND acceptance.workspace_id = retired.workspace_id
   AND acceptance.origin_id = retired.origin_id
   AND acceptance.record_id = retired.record_id;

-- Deleting events marks their sessions' rollups dirty and invalidates timing
-- through the existing triggers; the explicit deletes below cover rows those
-- triggers would leave keyed to a source that no longer exists.
DELETE FROM sessions.convergence_events
 WHERE kind = 'trajectory'
    OR lens = 'trajectories'
    OR source IN ('relay', 'trajectory', 'trajectories');

DELETE FROM sessions.conversation_turns
 WHERE source IN ('relay', 'trajectory', 'trajectories');
DELETE FROM sessions.session_outcomes
 WHERE source IN ('relay', 'trajectory', 'trajectories');
DELETE FROM sessions.session_links
 WHERE source IN ('relay', 'trajectory', 'trajectories');
DELETE FROM sessions.session_catalog
 WHERE source IN ('relay', 'trajectory');
DELETE FROM sessions.session_relationships
 WHERE source IN ('relay', 'trajectory');
DELETE FROM sessions.session_markers
 WHERE source IN ('relay', 'trajectory');
DELETE FROM sessions.session_commit_links
 WHERE source IN ('relay', 'trajectory');
DELETE FROM sessions.session_rollups
 WHERE source IN ('relay', 'trajectory', 'trajectories');
DELETE FROM sessions.session_timing_cache
 WHERE source IN ('relay', 'trajectory', 'trajectories');
