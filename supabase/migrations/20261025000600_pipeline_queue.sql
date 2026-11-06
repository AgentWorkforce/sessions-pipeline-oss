-- Session pipeline stages and the Postgres queue backend (not applied to any
-- hosted project).
--
-- 1. The queued stages are renamed and extended to the seven of the pipeline
--    registry (packages/relayhistory/src/lib/pipeline/registry.ts):
--      storage.event -> object-landed, index.upsert -> session-indexed,
--      transform -> file-normalize, plus summary-short, summary-long,
--      status-classify and file-maintenance. Span stages (ingest, deliver.put,
--      fetch) are unchanged. Rows already written under an old name are renamed,
--      so the tables stay consistent wherever this runs.
-- 2. sessions.pipeline_jobs: the queue for deployments without Cloudflare
--    Queues (PipelineQueue `postgres` backend). Consumers claim with
--    FOR UPDATE SKIP LOCKED under a lease; an exhausted job is flagged `dlq` and
--    quarantined into pipeline_dead_letters by the same consumer code. Messages
--    carry ids and object keys only, never session content.

-- pipeline_events_stage_check / pipeline_dead_letters_stage_check are the names
-- Postgres gave the inline CHECKs in 20261025000000; dropping them on the
-- partitioned parent drops them on every partition.
ALTER TABLE sessions.pipeline_events DROP CONSTRAINT pipeline_events_stage_check;
ALTER TABLE sessions.pipeline_dead_letters DROP CONSTRAINT pipeline_dead_letters_stage_check;

UPDATE sessions.pipeline_events SET stage = CASE stage
    WHEN 'storage.event' THEN 'object-landed'
    WHEN 'index.upsert' THEN 'session-indexed'
    WHEN 'transform' THEN 'file-normalize' END
  WHERE stage IN ('storage.event', 'index.upsert', 'transform');
-- The id starts with '<stage>:', so it is renamed with the stage. (Ids written
-- before tenant-scoped ids keep their old remainder; no hosted project has any.)
UPDATE sessions.pipeline_dead_letters SET
    stage = v.stage_to,
    id = v.stage_to || substr(id, length(v.stage_from) + 1),
    message = jsonb_set(message, '{stage}', to_jsonb(v.stage_to))
  FROM (VALUES ('storage.event', 'object-landed'),
               ('index.upsert', 'session-indexed'),
               ('transform', 'file-normalize')) AS v (stage_from, stage_to)
  WHERE stage = v.stage_from;

ALTER TABLE sessions.pipeline_events ADD CONSTRAINT pipeline_events_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'fetch'));
ALTER TABLE sessions.pipeline_dead_letters ADD CONSTRAINT pipeline_dead_letters_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'fetch'));

-- One session UUID traceable to the customer: dead letters carry the session
-- and machine beside the org/workspace/trace they already had.
ALTER TABLE sessions.pipeline_dead_letters
  ADD COLUMN session_id text,
  ADD COLUMN machine_id text;
CREATE INDEX pipeline_dead_letters_session_idx
  ON sessions.pipeline_dead_letters (org_id, session_id);

-- One row per queued message until it is acked (deleted). Service-role only.
CREATE TABLE sessions.pipeline_jobs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  stage text NOT NULL CHECK (stage IN ('object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance')),
  -- Out of attempts on the main queue: the next delivery quarantines it.
  dlq boolean NOT NULL DEFAULT false,
  work_key text NOT NULL,
  trace_id uuid NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  session_id text,
  machine_id text,
  -- The StageMessage, as sent.
  message jsonb NOT NULL CHECK (jsonb_typeof(message) = 'object'),
  -- Deliveries so far on the current queue (main, then DLQ from zero).
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  -- Not claimable before this: the send delay, a retry's backoff, or a lease.
  visible_at timestamptz NOT NULL DEFAULT now(),
  -- The claim that holds the job; ack and retry apply only under it.
  lease_id uuid,
  enqueued_at timestamptz NOT NULL DEFAULT now()
);
-- The claim: WHERE stage = $1 AND visible_at <= now() ORDER BY visible_at, id.
CREATE INDEX pipeline_jobs_claim_idx ON sessions.pipeline_jobs (stage, visible_at, id);
-- From a trace to its queued work.
CREATE INDEX pipeline_jobs_trace_idx ON sessions.pipeline_jobs (trace_id);
-- From a customer's session to its queued work.
CREATE INDEX pipeline_jobs_session_idx ON sessions.pipeline_jobs (org_id, session_id);

ALTER TABLE sessions.pipeline_jobs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.pipeline_jobs FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.pipeline_jobs TO service_role;
