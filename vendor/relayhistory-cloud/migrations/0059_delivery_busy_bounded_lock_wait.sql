-- Bounded delivery lock waits: a batch that cannot get its locks soon is
-- refused as busy instead of queueing.
--
-- Production (2026-10-27): one org with ~5 uploading machines in one workspace
-- saw p50 44-78 s per batch and 503s at the Worker's ~125 s ceiling. The batch
-- work itself is cheap (~0.4 s for 500 records, warm). The time was spent
-- queueing, blocking and exclusive, on the org memory mutex and the workspace
-- projection lock, while each queued request held a database connection that
-- other tenants share. A single-uploader org is unaffected (p50 ~3 s).
--
-- What changes. A first-time batch still takes user -> org -> origin ->
-- workspace, in that order, before writing anything. Each lock is first tried
-- without waiting. If it is held, the batch waits for it only within a bound;
-- when the bound runs out it raises `delivery_busy` (SQLSTATE 55P03,
-- lock_not_available). The Worker answers 429 with Retry-After. Raising aborts
-- the whole statement, so a refused batch writes nothing, not even its origin
-- row.
--
-- Bounds:
--   * The user lock, 10 s. It serializes one user's batches. The desktop sends
--     up to four batches of one job at once (relay-desktop
--     probe/src/probe/collector.rs DELIVERY_CONCURRENCY), and they meet here.
--     A 429 for each of them would put a single uploader into the client's
--     exponential backoff: 1 s * 2^attempts, which resets only after a
--     failure-free window. Waiting here is bounded in practice too: the holder
--     now waits at most the contended bound below, then its own work.
--   * Org, origin and workspace together, 2 s from when the user lock was
--     taken. These are the locks other machines contend for. A short wait
--     absorbs one batch ahead (~0.4-2 s); a longer queue is refused.
-- Both can be lowered per session through `relayhistory.delivery_user_lock_wait_ms`
-- and `relayhistory.delivery_lock_wait_ms`. The real-Postgres tests use these.
-- The Worker sets neither.
--
-- Why lock_timeout and not pg_try_advisory_xact_lock alone. A try-lock is used
-- first, so an uncontended lock never pays for anything else. A try-only
-- design would turn every momentary overlap into a 429. lock_timeout applies
-- to advisory locks: in PostgreSQL 17.11 (pgvector/pgvector:pg17),
-- `SET LOCAL lock_timeout='300ms'; SELECT pg_advisory_xact_lock(k)` against a
-- held k fails after 300 ms with 55P03 "canceling statement due to lock
-- timeout". pg_advisory_xact_lock_shared behaves the same. A plpgsql
-- `EXCEPTION WHEN lock_not_available` block catches it. The advisory wait is
-- an ordinary heavyweight-lock wait, which is where lock_timeout fires. The
-- timeout is set transaction-locally only around each wait and restored
-- straight after, so the batch body's own row locks keep the caller's setting.
--
-- Deadlocks. Lock order is unchanged, so no new cycle is possible. A delivery
-- now also leaves any lock queue after the bound. Claim writers
-- (upsert_live_memory_claim, upsert_reflex_pattern), selection and reset still
-- wait without a bound, as before: their callers are not retried by a client
-- backoff.
--
-- The workspace projection lock is now taken here, up front, when the batch
-- has records. Before, the first projection trigger took it, after the
-- conflict preflight. It is the same key, after origin as before. The trigger's
-- delivery_projection_lock_timed (0047) then finds it held and takes it
-- re-entrantly, so the reported workspace wait is this one. A batch whose
-- records were all unchanged revisions used to skip it; it now takes it
-- briefly.
--
-- Replay fast path: unchanged. A committed receipt with the same digest is
-- returned before any lock, so a retry after a 429 that had in fact committed
-- never waits or gets busy.
--
-- Timing: `_delivery_timing` is unchanged on success. On busy, the exception
-- DETAIL carries the same fields measured so far plus `busy_lock` (user, org,
-- origin or workspace). The Worker logs it on the request line.
--
-- Write hold. This replaces accept_delivery_batch, the function 0038/0044
-- guard with the upload-cleanup hold, and keeps 0047's fast path that depends
-- on the hold's readiness function. Like 0047 it carries the marker, so Deploy
-- Production applies it only from a dispatched run with allow_write_hold=true.
-- The hold itself is not changed and is inert while upload_cleanup_ready() is
-- true (it cannot regress).
--
-- write-hold: sessions.upload_cleanup_ready()
--
-- Every statement is CREATE [OR REPLACE] FUNCTION: no table, index or trigger
-- is created, altered or scanned.

-- A wait budget in ms: the session's override when set, else the default.
CREATE FUNCTION sessions.delivery_lock_budget_ms(p_setting text, p_default numeric)
RETURNS numeric LANGUAGE sql STABLE AS $budget$
  SELECT GREATEST(0, COALESCE(NULLIF(current_setting(p_setting, true), '')::numeric, p_default));
$budget$;

-- Take an advisory xact lock, waiting at most p_wait_ms. Returns the time
-- spent in ms (unrounded), or NULL when the lock was still held at the bound.
-- A lock already held by this transaction is re-entrant and returns at once.
CREATE FUNCTION sessions.delivery_advisory_lock_within(p_key bigint, p_wait_ms numeric)
RETURNS numeric LANGUAGE plpgsql AS $lock_within$
DECLARE
  started timestamptz := clock_timestamp();
  saved text;
BEGIN
  IF pg_try_advisory_xact_lock(p_key) THEN RETURN 0; END IF;
  saved := current_setting('lock_timeout');
  -- lock_timeout=0 means no limit, so the smallest bound is 1 ms.
  PERFORM set_config('lock_timeout', GREATEST(1, ceil(p_wait_ms))::bigint || 'ms', true);
  BEGIN
    PERFORM pg_advisory_xact_lock(p_key);
  EXCEPTION WHEN lock_not_available THEN
    PERFORM set_config('lock_timeout', saved, true);
    RETURN NULL;
  END;
  PERFORM set_config('lock_timeout', saved, true);
  RETURN extract(epoch FROM clock_timestamp() - started) * 1000;
END;
$lock_within$;

-- 0047's wrapper with bounded lock waits.
CREATE OR REPLACE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE
  started timestamptz := clock_timestamp();
  step timestamptz;
  replay jsonb;
  contended_until timestamptz;
  saved text;
  waited numeric;
  busy text;
  timing jsonb := '{}'::jsonb;
  workspace_key bigint := hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || p_org || chr(31) || p_workspace, 0);
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
  -- One labelled block so each refusal leaves with what was measured so far.
  <<locks>>
  BEGIN
    waited := sessions.delivery_advisory_lock_within(
      hashtextextended(jsonb_build_array(p_org,p_workspace,p_user)::text, 0),
      sessions.delivery_lock_budget_ms('relayhistory.delivery_user_lock_wait_ms', 10000));
    IF waited IS NULL THEN busy := 'user'; EXIT locks; END IF;
    timing := timing || jsonb_build_object('user_lock_ms', round(waited::numeric, 1));
    contended_until := clock_timestamp()
      + sessions.delivery_lock_budget_ms('relayhistory.delivery_lock_wait_ms', 2000) * interval '1 millisecond';
    -- The two locks 0038's accept_delivery_batch_before_reset takes first, in its
    -- order; it re-acquires them below without waiting.
    waited := sessions.delivery_advisory_lock_within(
      hashtextextended('relayhistory.memory_claims:' || p_org,0),
      extract(epoch FROM contended_until - clock_timestamp()) * 1000);
    IF waited IS NULL THEN busy := 'org'; EXIT locks; END IF;
    timing := timing || jsonb_build_object('org_lock_ms', round(waited::numeric, 1));
    step := clock_timestamp();
    saved := current_setting('lock_timeout');
    PERFORM set_config('lock_timeout',
      GREATEST(1, ceil(extract(epoch FROM contended_until - clock_timestamp()) * 1000))::bigint || 'ms', true);
    BEGIN
      -- A concurrent first insert of this origin also waits here.
      INSERT INTO sessions.delivery_origins VALUES (p_org,p_workspace,p_origin) ON CONFLICT DO NOTHING;
      -- lock_timeout bounds each wait, not their sum: give the row lock only
      -- what the insert left, so the 2 s bound holds across both.
      PERFORM set_config('lock_timeout',
        GREATEST(1, ceil(extract(epoch FROM contended_until - clock_timestamp()) * 1000))::bigint || 'ms', true);
      PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
    EXCEPTION WHEN lock_not_available THEN
      busy := 'origin';
    END;
    PERFORM set_config('lock_timeout', saved, true);
    IF busy IS NOT NULL THEN EXIT locks; END IF;
    timing := timing || jsonb_build_object('origin_lock_ms', sessions.delivery_elapsed_ms(step));
    IF jsonb_array_length(p_records) > 0 THEN
      waited := sessions.delivery_advisory_lock_within(workspace_key,
        extract(epoch FROM contended_until - clock_timestamp()) * 1000);
      IF waited IS NULL THEN busy := 'workspace'; EXIT locks; END IF;
      -- The projection triggers' delivery_projection_lock_timed now finds the
      -- lock held and reports this wait.
      PERFORM set_config('relayhistory.delivery_workspace_lock_ms', waited::text, true);
      PERFORM set_config('relayhistory.delivery_workspace_lock_key', workspace_key::text, true);
    END IF;
  END locks;
  IF busy IS NOT NULL THEN
    -- Aborts the statement: the origin insert above, if any, is undone too.
    RAISE EXCEPTION 'delivery_busy' USING ERRCODE = '55P03',
      DETAIL = (timing || jsonb_build_object('fast_path', false, 'busy_lock', busy,
        'total_ms', sessions.delivery_elapsed_ms(started)))::text;
  END IF;
  result := sessions.accept_delivery_batch_before_reset(p_org,p_workspace,p_user,p_origin,p_batch,p_digest,p_records,p_receipt);
  RETURN result || jsonb_build_object('_delivery_timing', timing || jsonb_build_object(
    'fast_path', false,
    'workspace_lock_ms', round(COALESCE(NULLIF(current_setting('relayhistory.delivery_workspace_lock_ms', true), '')::numeric, 0), 1),
    'total_ms', sessions.delivery_elapsed_ms(started)));
END;
$delivery$;
