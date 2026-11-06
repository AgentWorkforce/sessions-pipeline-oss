-- 20261025000600_pipeline_queue: renamed stages, pipeline_jobs, and the claim
-- (the same statement as pollPostgresStage in lib/pipeline/queue.ts).

DO $$
BEGIN
  INSERT INTO sessions.pipeline_events (trace_id, stage, status)
  SELECT gen_random_uuid(), s, 'ok'
    FROM unnest(ARRAY['ingest', 'deliver.put', 'object-landed', 'session-indexed', 'summary-short',
                      'summary-long', 'status-classify', 'file-normalize', 'file-maintenance', 'fetch']) AS s;
  BEGIN
    INSERT INTO sessions.pipeline_events (trace_id, stage, status)
      VALUES (gen_random_uuid(), 'storage.event', 'ok');
    RAISE EXCEPTION 'old stage name accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
      VALUES ('fetch', 'k', gen_random_uuid(), 'o', 'w', '{}');
    RAISE EXCEPTION 'span stage queued';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  DELETE FROM sessions.pipeline_events;
END $$;

-- Committed first: the other connections must see these jobs.
INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
SELECT 'session-indexed', 'race-' || g, gen_random_uuid(), 'o', 'w', '{}'
  FROM generate_series(1, 4) AS g;

-- Two consumers claiming at once: the second skips the first's locked rows
-- instead of waiting on them, and no job is claimed twice.
DO $$
DECLARE
  conn text := (SELECT string_agg(k || '=''' || replace(replace(v, '\', '\\'), '''', '\''') || '''', ' ')
                  FROM (VALUES
                    ('host', split_part(current_setting('unix_socket_directories'), ',', 1)),
                    ('port', current_setting('port')),
                    ('user', current_user::text),
                    ('dbname', current_database()::text)) AS c(k, v));
  claim text := $q$
    WITH next AS (
      SELECT id FROM sessions.pipeline_jobs
       WHERE stage = 'session-indexed' AND visible_at <= now()
       ORDER BY visible_at, id LIMIT %s FOR UPDATE SKIP LOCKED)
    UPDATE sessions.pipeline_jobs AS j
       SET visible_at = now() + interval '300 seconds', lease_id = gen_random_uuid()
      FROM next WHERE j.id = next.id
    RETURNING j.id$q$;
  a bigint[]; b bigint[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') THEN
    RAISE NOTICE 'no dblink: skipping the concurrent claim test';
    DELETE FROM sessions.pipeline_jobs;
    RETURN;
  END IF;
  BEGIN
    PERFORM dblink_connect('q_a', conn);
    PERFORM dblink_connect('q_b', conn);
  EXCEPTION WHEN sqlclient_unable_to_establish_sqlconnection THEN
    RAISE NOTICE 'dblink cannot connect to this server: skipping the concurrent claim test';
    PERFORM dblink_disconnect(c) FROM unnest(dblink_get_connections()) AS c;
    DELETE FROM sessions.pipeline_jobs;
    RETURN;
  END;
  PERFORM dblink_exec('q_a', 'BEGIN');
  SELECT array_agg(id) INTO a FROM dblink('q_a', format(claim, 2)) AS t(id bigint);
  -- q_a still holds its two rows; q_b must not block on them.
  PERFORM dblink_exec('q_b', 'SET statement_timeout = 2000');
  SELECT array_agg(id) INTO b FROM dblink('q_b', format(claim, 4)) AS t(id bigint);
  PERFORM dblink_exec('q_a', 'COMMIT');
  ASSERT cardinality(a) = 2 AND cardinality(b) = 2, format('claimed %s and %s', a, b);
  ASSERT NOT (a && b), format('a job was claimed twice: %s / %s', a, b);
  ASSERT (SELECT count(*) FROM sessions.pipeline_jobs WHERE lease_id IS NOT NULL AND attempts = 0) = 4,
    'every job leased once, no attempt counted before it starts';
  PERFORM dblink_disconnect('q_a');
  PERFORM dblink_disconnect('q_b');
  DELETE FROM sessions.pipeline_jobs;
END $$;

\echo pipeline queue tests: ok
