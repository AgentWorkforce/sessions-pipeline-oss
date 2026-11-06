-- What the minute tick reads to notice a pinned xmin horizon before it costs
-- delivery.
--
-- On 2026-10-24 an autovacuum ANALYZE of convergence_events held one snapshot
-- for 21 hours. Vacuum ran constantly and removed nothing: the outbox, timing
-- cache, revision feed and rollups grew to millions of dead rows, every poll and
-- delivery batch read through them, and accept_delivery_batch reached its
-- ~125 s ceiling as a 503. Nothing reported it for 16 hours.
--
-- The runtime role sees only its own sessions in pg_stat_activity, so this is
-- SECURITY DEFINER under the migration owner. It returns aggregates only: how
-- far behind the oldest snapshot or transaction id holding removal back is,
-- what kind of backend holds it, and live/dead tuple counts for the tables
-- that churn on every delivery. Query text is never
-- returned, except an autovacuum worker's, which names only its operation and
-- table ("autovacuum: ANALYZE sessions.convergence_events").
--
-- It needs its owner to see other roles' sessions (pg_read_all_stats, which
-- the production migration owner has). Without it, PostgreSQL still shows a
-- holder's backend_xmin but hides its transaction start and backend type, and
-- an autovacuum worker (which has no user) is always hidden that way; the
-- holder is then reported with stats_visible = false rather than looking
-- healthy. A holder with no transaction at all (a walsender's
-- hot_standby_feedback) is judged by how far behind it is alone.
CREATE FUNCTION sessions.database_health_v1()
RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $database_health$
  SELECT jsonb_build_object(
    'oldest_snapshot', (
      SELECT jsonb_build_object(
        'xmin_age', h.horizon_age,
        'xact_age_s', floor(extract(epoch FROM now() - a.xact_start))::bigint,
        'backend_type', a.backend_type,
        'state', a.state,
        -- PostgreSQL replaces a row it will not show this owner with the
        -- literal query '<insufficient privilege>'. A visible walsender holding
        -- hot_standby_feedback has no xact_start at all, so that is no sign.
        'stats_visible', a.query IS DISTINCT FROM '<insufficient privilege>',
        -- A deploy's CREATE INDEX CONCURRENTLY holds a snapshot too; say so,
        -- so it is not mistaken for a stuck client.
        'command', (SELECT c.command FROM pg_stat_progress_create_index AS c WHERE c.pid = a.pid),
        'autovacuum', CASE WHEN a.backend_type = 'autovacuum worker' THEN left(a.query, 200) END
      )
        FROM pg_stat_activity AS a
        -- A backend holds removal back by its snapshot (backend_xmin) or, once
        -- it has written, by its own transaction id (backend_xid), which stays
        -- while it sits idle in transaction with no snapshot at all.
        CROSS JOIN LATERAL (
          SELECT greatest(age(a.backend_xmin), age(a.backend_xid)) AS horizon_age
        ) AS h
       WHERE (a.backend_xmin IS NOT NULL OR a.backend_xid IS NOT NULL)
         AND a.pid <> pg_backend_pid()
         -- Another database's snapshots do not hold back this one's tables;
         -- a backend with no database (a walsender's feedback) can.
         AND (a.datid IS NULL OR a.datname = current_database())
         -- A backend in a vacuum phase does not hold back removal (PostgreSQL
         -- 14+), and vacuuming convergence_events alone runs for hours. An
         -- ANALYZE, autovacuum's included, does hold it and is not listed here.
         AND NOT EXISTS (SELECT 1 FROM pg_stat_progress_vacuum AS v WHERE v.pid = a.pid)
       -- The holder is the backend furthest behind. Fresh queries inherit a
       -- pinned horizon, so ties go to the transaction open longest. A long
       -- transaction whose current snapshot is fresh is not furthest behind,
       -- and its age alone is not reported as a pin (see database-health.ts).
       ORDER BY h.horizon_age DESC NULLS LAST, a.xact_start NULLS LAST
       LIMIT 1
    ),
    -- A slot's xmin holds back every table; its catalog_xmin only the catalogs.
    'slot_xmin_age', (SELECT max(age(s.xmin)) FROM pg_replication_slots AS s),
    'slot_catalog_xmin_age', (SELECT max(age(s.catalog_xmin)) FROM pg_replication_slots AS s),
    'tables', COALESCE((
      SELECT jsonb_object_agg(t.relname, jsonb_build_object('live', t.n_live_tup, 'dead', t.n_dead_tup))
        FROM pg_stat_user_tables AS t
       WHERE t.schemaname = 'sessions'
         AND t.relname IN ('session_event_outbox', 'session_revision_events', 'session_timing_cache', 'session_rollups')
    ), '{}'::jsonb)
  );
$database_health$;

REVOKE ALL ON FUNCTION sessions.database_health_v1() FROM PUBLIC;
