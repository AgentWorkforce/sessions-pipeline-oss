-- 96bulls 0001: observability columns from plan section 12.
-- Applied AFTER the vendored relayhistory-cloud sessions migrations
-- (last: 20261025000600_pipeline_queue.sql). Lives in 96BullSessions until the
-- local stack proves it; then it is proposed upstream as its own PR.

-- pipeline_jobs carries the ids every consumer span needs, filled from the
-- StageMessage by trigger so the vendored queue writer stays untouched.
ALTER TABLE sessions.pipeline_jobs
  ADD COLUMN IF NOT EXISTS session_uuid uuid,
  ADD COLUMN IF NOT EXISTS tenant_id text,
  ADD COLUMN IF NOT EXISTS traceparent text
    CHECK (traceparent IS NULL OR traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$');

CREATE OR REPLACE FUNCTION sessions.pipeline_jobs_observability()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v_uuid text := NEW.message #>> '{ctx,session_uuid}';
  v_tp   text := NEW.message #>> '{ctx,traceparent}';
BEGIN
  NEW.tenant_id := coalesce(NEW.message #>> '{ctx,tenant_id}', NEW.org_id);
  IF v_uuid ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
    NEW.session_uuid := v_uuid::uuid;
  END IF;
  IF v_tp ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' THEN
    NEW.traceparent := v_tp;
  ELSE
    -- The pipeline trace id is the W3C trace id; synthesize a parent with a
    -- span id derived from the work key so retries of one job share it.
    NEW.traceparent := '00-' || replace(NEW.trace_id::text, '-', '') || '-'
      || left(encode(sha256(convert_to(NEW.stage || ':' || NEW.work_key, 'UTF8')), 'hex'), 16) || '-01';
  END IF;
  RETURN NEW;
END
$fn$;

DROP TRIGGER IF EXISTS pipeline_jobs_observability ON sessions.pipeline_jobs;
CREATE TRIGGER pipeline_jobs_observability
  BEFORE INSERT ON sessions.pipeline_jobs
  FOR EACH ROW EXECUTE FUNCTION sessions.pipeline_jobs_observability();

CREATE INDEX IF NOT EXISTS pipeline_jobs_session_uuid_idx
  ON sessions.pipeline_jobs (session_uuid) WHERE session_uuid IS NOT NULL;

-- Dead letters keep the session uuid and the original traceparent so a replay
-- can link its new trace to the first one (section 12, DLQ/replay).
ALTER TABLE sessions.pipeline_dead_letters
  ADD COLUMN IF NOT EXISTS session_uuid uuid,
  ADD COLUMN IF NOT EXISTS traceparent text;

CREATE OR REPLACE FUNCTION sessions.pipeline_dead_letters_observability()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v_uuid text := NEW.message #>> '{ctx,session_uuid}';
  v_tp   text := NEW.message #>> '{ctx,traceparent}';
BEGIN
  IF v_uuid ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
    NEW.session_uuid := v_uuid::uuid;
  END IF;
  IF v_tp ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' THEN
    NEW.traceparent := coalesce(NEW.traceparent, v_tp);
  END IF;
  RETURN NEW;
END
$fn$;

DROP TRIGGER IF EXISTS pipeline_dead_letters_observability ON sessions.pipeline_dead_letters;
CREATE TRIGGER pipeline_dead_letters_observability
  BEFORE INSERT OR UPDATE ON sessions.pipeline_dead_letters
  FOR EACH ROW EXECUTE FUNCTION sessions.pipeline_dead_letters_observability();
