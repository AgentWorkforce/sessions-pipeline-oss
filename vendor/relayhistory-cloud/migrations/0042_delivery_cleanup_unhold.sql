-- Companion to 0041: relax the delivery-path write triggers while the
-- owner-only upload_cleanup rollout finishes its historical backfill.
--
-- guard_delivery_cleanup (delivery_records, delivery_origins) and
-- guard_upload_projection_cleanup (convergence_events, delivery-shaped rows)
-- enforced the 0038 maintenance hold at the table layer. Delivery may run
-- during cleanup because:
--  * remember_uploaded_support_identity ledgers every delivery_records write at
--    write time, so rows landing behind the phase-1 cursor still record their
--    uploaded identity;
--  * convergence_events delivery rows carry delivery_record_id whose identity
--    the same trigger already recorded, so phase-2 misses are benign;
--  * operation='delete' records cannot arrive until the Desktop deletion client
--    ships, so cursor-only tombstone recovery cannot be missed;
--  * neighborhood_claims/edges/patterns/pattern_hits stay guarded, so the
--    phase-3/4 claim and pattern cleanup remains race-free.
--
-- The follow-up migration recreates these triggers verbatim once
-- sessions.upload_cleanup_ready() reports true.
DROP TRIGGER guard_delivery_cleanup ON sessions.delivery_records;
DROP TRIGGER guard_delivery_origin_cleanup ON sessions.delivery_origins;
DROP TRIGGER guard_upload_projection_cleanup ON sessions.convergence_events;
