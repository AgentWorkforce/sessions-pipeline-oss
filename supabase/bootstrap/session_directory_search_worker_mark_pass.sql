-- External verifier: run phase 2 and record PASS for one migration-set generation.
--
-- Only needed when the migrator runs with HOOPDB_ROLE_VERIFY=false. It then leaves the
-- readiness marker 'pending', and no reader becomes ready until this script (or a
-- migrator with the verifier on) records PASS for the readers' own generation.
--
-- Usage (psql 12+, from the repository root, against the migrated database):
--   GEN=$(bun src/hoopdb/migrate.ts --generation)    # same image as the readers
--   psql -v ON_ERROR_STOP=1 -X -v generation="$GEN" -v db_mode=selfhosted \
--     -f supabase/bootstrap/session_directory_search_worker_mark_pass.sql
--
-- What it does, in one transaction:
--   - It takes the migrator's advisory lock, so it can't interleave with a migrator.
--   - It runs supabase/bootstrap/session_directory_search_worker_verify.sql (phase 2;
--     55000 on any violation, which rolls back).
--   - Only if phase 2 passed, it sets the row to 'pass', and only when the row is
--     'pending' for exactly :generation. Anything else (no row, another generation,
--     already 'pass') fails with 55000 and changes nothing.
--
-- Record format (sessions.__96bulls_role_validation), the contract with every reader:
--   name        text PRIMARY KEY  'session_directory_search_worker'
--   generation  text NOT NULL     sha256 hex of the migration set
--                                 (`bun src/hoopdb/migrate.ts --generation`)
--   status      text NOT NULL     'pending' | 'pass'
--   updated_at  timestamptz NOT NULL
-- Readers are ready only on status = 'pass' AND generation = their own generation.
-- A migrator writes ('pending', <generation>) under the advisory lock before any
-- migration step. Never write 'pass' any other way.
\set ON_ERROR_STOP 1
BEGIN;
SELECT pg_advisory_xact_lock(961996, 1);
SELECT set_config('hoopdb.role_validation_generation', :'generation', true);
SELECT set_config('hoopdb.db_mode', :'db_mode', true);
\ir session_directory_search_worker_verify.sql
\ir session_token_verifier_verify.sql
\ir session_ingest_seeder_verify.sql
\ir session_ingest_verify.sql
DO $mark$
DECLARE
  v_gen text := current_setting('hoopdb.role_validation_generation');
  v_n int;
BEGIN
  IF v_gen !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'mark_pass: generation must be the 64-hex migration-set digest';
  END IF;
  UPDATE sessions.__96bulls_role_validation
     SET status = 'pass', updated_at = now()
   WHERE name = 'session_directory_search_worker' AND generation = v_gen AND status = 'pending';
  GET DIAGNOSTICS v_n = ROW_COUNT;
  IF v_n <> 1 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('mark_pass: no pending role validation for generation %s', v_gen),
      HINT = 'Run the migrator for this image first; PASS is recorded only for its pending generation.';
  END IF;
END
$mark$;
COMMIT;
