-- Restore the 0038/0039 delivery-path guards now that the owner-only
-- upload_cleanup rollout has completed (sessions.upload_cleanup_ready()).
--
-- 0041/0042 were an emergency un-hold applied during the 2026-10-13 hold
-- window so durable delivery could resume while the historical backfill
-- finished. They dropped the guard call in sessions.accept_delivery_batch
-- and the three delivery-path triggers. The backfill only needed the hold
-- for pre-migration rows because remember_uploaded_support_identity ledgers
-- new writes at write time.
--
-- The canonical layering is 0039's thin wrapper (a per-user advisory lock
-- serializing delivery against reset_uploaded_sessions) delegating to
-- accept_delivery_batch_before_reset, which carries the 0038 cleanup guard,
-- deletion filter and memory-claims lock. The emergency un-hold collapsed
-- that into one inlined body and lost the per-user lock — review caught it —
-- so this restores the reviewed wrapper verbatim rather than the inlined
-- shape. The hold is inert while upload_cleanup_ready() stays true (the
-- checkpoint guard forbids phase regression), so restoring it changes no
-- runtime behavior today; it returns the deployed schema to byte-parity
-- with the reviewed 0038/0039 design. Any future hold would arrive as its
-- own marked migration — the checkpoint cannot be rolled back to re-arm.
--
-- write-hold: sessions.upload_cleanup_ready()
--
-- Deploy ordering is enforced by the write-hold preflight, not by an
-- in-file readiness assert: the marker above requires a dispatched deploy
-- with allow_write_hold=true, so this cannot re-arm a live hold through a
-- merge-triggered deploy. The statements are idempotent so the file applies
-- cleanly whether or not the 0041/0042 un-hold migrations have already run.
--
-- CREATE TRIGGER takes SHARE ROW EXCLUSIVE on each table, and delivery
-- writers hold ROW EXCLUSIVE on these tables for their whole transaction.
-- Writers lock in origins→records→events order (insert origin, then
-- records, then projected events), so this file acquires the table locks
-- in the same order. A cycle needs a writer holding a table the migration
-- wants next while the migration holds one it already passed; since any
-- writer holding records or events RE already holds origins RE — which
-- SHARE ROW EXCLUSIVE conflicts with — the migration can never have
-- acquired origins before it, so no cycle exists. The earlier file order
-- (records first) deadlocked twice in deploy on 2026-10-14 (SQLSTATE
-- 40P01 at the origins CREATE TRIGGER).
CREATE OR REPLACE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(p_org,p_workspace,p_user)::text, 0));
  RETURN sessions.accept_delivery_batch_before_reset(p_org,p_workspace,p_user,p_origin,p_batch,p_digest,p_records,p_receipt);
END;
$delivery$;

DROP TRIGGER IF EXISTS guard_delivery_origin_cleanup ON sessions.delivery_origins;
CREATE TRIGGER guard_delivery_origin_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.delivery_origins
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_delivery_cleanup();
DROP TRIGGER IF EXISTS guard_delivery_cleanup ON sessions.delivery_records;
CREATE TRIGGER guard_delivery_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.delivery_records
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_delivery_cleanup();
DROP TRIGGER IF EXISTS guard_upload_projection_cleanup ON sessions.convergence_events;
CREATE TRIGGER guard_upload_projection_cleanup BEFORE INSERT OR UPDATE OR DELETE ON sessions.convergence_events
  FOR EACH ROW EXECUTE FUNCTION sessions.guard_upload_projection_cleanup();
