-- History-feature track (P5b), applied by scripts/history-owner-apply.ts, never by the
-- HoopDb ingest migrator. The runner creates the schema, its ledger and readiness
-- marker itself; this first migration only pins the schema's privileges.
-- Every table added in this track must carry org_id and a timestamptz column
-- (retention-ready; enforced by src/history-db/__tests__/history-track.test.ts).
REVOKE ALL ON SCHEMA history FROM PUBLIC;
COMMENT ON SCHEMA history IS 'P5b history features ported from relayhistory-cloud; separate migration track from sessions.';
