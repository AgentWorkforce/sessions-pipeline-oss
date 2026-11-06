-- Temporarily un-hold delivery batch ingestion while the owner-only
-- upload_cleanup rollout finishes its historical backfill.
--
-- The 0038 hold exists so the phase-1/phase-2 recovery cursors cannot miss
-- concurrent writes. It is safe to relax for delivery specifically:
--  * remember_uploaded_support_identity ledgers every delivery_records write at
--    write time, so rows inserted behind the phase-1 cursor still record their
--    uploaded identity and rows ahead are re-ledgered ON CONFLICT DO NOTHING;
--  * operation='delete' records cannot arrive until the Desktop deletion client
--    ships, so no cursor-only tombstone recovery can be missed;
--  * selection, reset, and runtime learning writes remain held by their own
--    guards, keeping the phase-3/4 claim and pattern cleanup race-free.
--
-- The follow-up migration restores sessions.require_upload_cleanup_ready()
-- here once sessions.upload_cleanup_ready() reports true. This file must be
-- applied byte-identically to the emergency manual application recorded in
-- sessions.__migrations during the 2026-10-13 hold window.
CREATE OR REPLACE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE allowed jsonb;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('relayhistory.memory_claims:' || p_org,0));
  INSERT INTO sessions.delivery_origins VALUES (p_org,p_workspace,p_origin) ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
  SELECT COALESCE(jsonb_agg(r), '[]'::jsonb) INTO allowed
    FROM jsonb_array_elements(p_records) r
    WHERE NOT EXISTS (SELECT 1 FROM sessions.deleted_upload_sessions d
      WHERE d.org_id=p_org AND d.workspace_id=p_workspace AND d.user_id=p_user
        AND d.origin_id=p_origin AND d.source=r->>'source' AND d.session_id=r->>'session_id');
  -- Receipt identity remains tied to the original request, including suppressed records.
  RETURN sessions.accept_delivery_batch_before_session_deletion(p_org,p_workspace,p_user,p_origin,p_batch,p_digest,allowed,p_receipt);
END;
$delivery$;
