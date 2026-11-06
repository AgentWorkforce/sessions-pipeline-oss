-- Delivery replay fast path and per-batch lock-wait timing.
--
-- Production (2026-10-15): one workspace's batches queued behind each other for
-- up to ~125s. A desktop retry of a batch that had already committed waited in
-- the same queue (per-user, org memory, origin and workspace projection locks)
-- only to read back its stored receipt. The request log could not say which
-- lock a slow batch waited on.
--
-- 1. Replay fast path. Before any lock, a batch whose (org, workspace, origin,
--    batch) receipt is already committed with the same digest returns that
--    receipt. This is exactly what the locked path returns for it:
--    * receipts and origins are insert-only (no reset, deletion, retention or
--      expiry path deletes or updates them), so a committed receipt stays the
--      locked path's answer, including after a session deletion or reset;
--    * a replay writes nothing on the locked path either (the origin row exists
--      because it was inserted with the receipt; the record loop never runs);
--    * a different digest is a typed conflict and still takes the locked path;
--    * while the upload-cleanup checkpoint is not complete, every call takes
--      the locked path, so the maintenance hold still rejects replays.
--    A batch still in flight is not yet visible (READ COMMITTED), so its retry
--    waits on the locked path exactly as before.
--
-- 2. Lock timing. The wrapper takes the per-user lock (0039), then the org
--    memory mutex and the origin row lock that 0038's body takes next, in the
--    same order, timing each; 0038's body re-acquires both re-entrantly. The
--    workspace projection lock (0022/0023) is timed where the catalog and
--    activity triggers already take it. The returned JSON gains a private
--    `_delivery_timing` object; the stored receipt is unchanged and the API
--    strips the field before responding and logs it on the request line.
--
-- Write hold. This replaces accept_delivery_batch, the function 0038/0044
-- guard with the upload-cleanup hold, and adds a path (the replay fast path)
-- that depends on the hold's readiness function. AGENTS.md requires such a
-- migration to carry the marker below, so Deploy Production's preflight
-- (scripts/check-write-hold.mjs) refuses it on a merge-triggered deploy and it
-- applies only from a dispatched run with allow_write_hold=true. The hold is
-- not re-armed: while upload_cleanup_ready() is true (it cannot regress) the
-- guard is inert, and while it is false every call takes the guarded path.
--
-- write-hold: sessions.upload_cleanup_ready()
--
-- No lock changes key, scope or order: a first-time batch acquires user ->
-- org -> origin -> workspace as before. Every statement here is CREATE [OR
-- REPLACE] FUNCTION: no table is created, altered or scanned, no trigger is
-- created, and no data is backfilled (see the 2026-10-02/03 outage in 0029).

CREATE FUNCTION sessions.delivery_elapsed_ms(p_since timestamptz)
RETURNS numeric LANGUAGE sql VOLATILE AS $elapsed$
  SELECT round((extract(epoch FROM clock_timestamp() - p_since) * 1000)::numeric, 1);
$elapsed$;

-- The workspace projection lock, taken exactly where callers took it before.
-- Only the first acquisition of a key in a transaction can wait: every later
-- call for that key is re-entrant, so it takes the lock untimed and the wait
-- reported is the contention the batch actually met, not per-row call cost.
-- The raw wait (unrounded) accumulates in a transaction-local counter; the
-- wrapper rounds it once when it reports.
CREATE FUNCTION sessions.delivery_projection_lock_timed(p_org text, p_workspace text)
RETURNS void LANGUAGE plpgsql AS $projection_lock$
DECLARE
  lock_key bigint := hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || p_org || chr(31) || p_workspace, 0);
  started timestamptz;
BEGIN
  IF current_setting('relayhistory.delivery_workspace_lock_key', true) = lock_key::text THEN
    PERFORM pg_advisory_xact_lock(lock_key);
    RETURN;
  END IF;
  started := clock_timestamp();
  PERFORM pg_advisory_xact_lock(lock_key);
  PERFORM set_config('relayhistory.delivery_workspace_lock_ms',
    (COALESCE(NULLIF(current_setting('relayhistory.delivery_workspace_lock_ms', true), '')::numeric, 0)
      + extract(epoch FROM clock_timestamp() - started) * 1000)::text,
    true);
  PERFORM set_config('relayhistory.delivery_workspace_lock_key', lock_key::text, true);
END;
$projection_lock$;

-- 0038's activity trigger, with its lock taken through the timed helper at the
-- same point (record_delivery_projection_acceptance then takes it re-entrantly).
CREATE OR REPLACE FUNCTION sessions.project_delivery_session_v2() RETURNS trigger LANGUAGE plpgsql AS $projection$
BEGIN
  -- Preserve the existing evidence-only expiry fast path: it touches no
  -- projection and must not wait on a workspace while holding evidence rows.
  IF NOT (TG_OP='UPDATE' AND NEW.kind IN ('session_lineage','turn_receipt')
      AND OLD.kind IN ('session_lineage','turn_receipt')) THEN
    PERFORM sessions.delivery_projection_lock_timed(NEW.org_id, NEW.workspace_id);
    PERFORM sessions.record_delivery_projection_acceptance(NEW);
  END IF;
  PERFORM sessions.project_delivery_activity_row(NEW, OLD, TG_OP);
  RETURN NEW;
END;
$projection$;

-- 0022's catalog trigger, unchanged except that its single workspace lock goes
-- through the timed helper.
CREATE OR REPLACE FUNCTION sessions.project_delivery_catalog()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_projection$
DECLARE
  old_key jsonb;
  new_key jsonb;
  old_lock text;
  new_lock text;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_key := sessions.delivery_catalog_key(OLD.kind, OLD.source, OLD.session_id, OLD.operation, OLD.payload);
    IF old_key IS NOT NULL THEN
      old_lock := jsonb_build_array(OLD.org_id, OLD.workspace_id, old_key)::text;
    END IF;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_key := sessions.delivery_catalog_key(NEW.kind, NEW.source, NEW.session_id, NEW.operation, NEW.payload);
    IF new_key IS NOT NULL THEN
      new_lock := jsonb_build_array(NEW.org_id, NEW.workspace_id, new_key)::text;
    END IF;
  END IF;
  -- One lock per workspace, not per key: a batch touches many keys in record-id
  -- order, so per-key locks let two origins' batches take the same keys in
  -- opposite orders and deadlock. The lock name is the one the activity projection
  -- (project_delivery_session) takes, so a batch that mixes catalog and activity
  -- rows also needs only this single lock. A row keeps its workspace (it is in the
  -- delivery_records key), so OLD and NEW never need two.
  IF old_lock IS NOT NULL OR new_lock IS NOT NULL THEN
    PERFORM sessions.delivery_projection_lock_timed(
      COALESCE(NEW.org_id, OLD.org_id), COALESCE(NEW.workspace_id, OLD.workspace_id));
  END IF;

  -- A removed or moved row leaves no acceptance behind. A tombstone's entry is
  -- inert: only live contenders are ranked, and a later upsert redraws it.
  IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND (OLD.org_id, OLD.workspace_id, OLD.origin_id, OLD.record_id)
      IS DISTINCT FROM (NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id)) THEN
    DELETE FROM sessions.delivery_catalog_acceptance
     WHERE org_id = OLD.org_id AND workspace_id = OLD.workspace_id
       AND origin_id = OLD.origin_id AND record_id = OLD.record_id;
  END IF;
  IF new_lock IS NOT NULL THEN
    INSERT INTO sessions.delivery_catalog_acceptance (org_id, workspace_id, origin_id, record_id, accepted_seq)
    VALUES (NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id, nextval('sessions.delivery_catalog_acceptance_seq'))
    ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE SET accepted_seq = excluded.accepted_seq;
    PERFORM sessions.project_delivery_catalog_row(NEW);
  END IF;
  -- The row no longer contends for its prior key (tombstone, removal, re-kind or
  -- re-key): recompute that key from the contenders left.
  IF old_lock IS NOT NULL AND old_lock IS DISTINCT FROM new_lock THEN
    PERFORM sessions.project_delivery_catalog_key(OLD.org_id, OLD.workspace_id, old_key);
  END IF;
  RETURN NULL;
END;
$catalog_projection$;

-- 0039/0044's wrapper: replay fast path, then the same locks in the same order.
CREATE OR REPLACE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE
  started timestamptz := clock_timestamp();
  step timestamptz;
  replay jsonb;
  user_ms numeric;
  org_ms numeric;
  origin_ms numeric;
  result jsonb;
BEGIN
  IF sessions.upload_cleanup_ready() THEN
    SELECT receipt INTO replay FROM sessions.delivery_receipts
      WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin
        AND batch_id=p_batch AND digest=p_digest;
    IF FOUND THEN
      RETURN replay || jsonb_build_object('_delivery_timing', jsonb_build_object(
        'fast_path', true, 'total_ms', sessions.delivery_elapsed_ms(started)));
    END IF;
  END IF;
  PERFORM set_config('relayhistory.delivery_workspace_lock_ms', '0', true);
  PERFORM set_config('relayhistory.delivery_workspace_lock_key', '', true);
  step := clock_timestamp();
  PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(p_org,p_workspace,p_user)::text, 0));
  user_ms := sessions.delivery_elapsed_ms(step);
  -- The two locks 0038's accept_delivery_batch_before_reset takes first, in its
  -- order; it re-acquires them below without waiting.
  step := clock_timestamp();
  PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  org_ms := sessions.delivery_elapsed_ms(step);
  step := clock_timestamp();
  INSERT INTO sessions.delivery_origins VALUES (p_org,p_workspace,p_origin) ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
  origin_ms := sessions.delivery_elapsed_ms(step);
  result := sessions.accept_delivery_batch_before_reset(p_org,p_workspace,p_user,p_origin,p_batch,p_digest,p_records,p_receipt);
  RETURN result || jsonb_build_object('_delivery_timing', jsonb_build_object(
    'fast_path', false,
    'user_lock_ms', user_ms,
    'org_lock_ms', org_ms,
    'origin_lock_ms', origin_ms,
    'workspace_lock_ms', round(COALESCE(NULLIF(current_setting('relayhistory.delivery_workspace_lock_ms', true), '')::numeric, 0), 1),
    'total_ms', sessions.delivery_elapsed_ms(started)));
END;
$delivery$;
