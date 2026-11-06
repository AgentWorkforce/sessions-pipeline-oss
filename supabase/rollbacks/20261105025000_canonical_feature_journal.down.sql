-- Reverse of 20261105025000. Only valid after 20261105026000 is rolled back (its install calls the invoker).
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105025000_canonical_feature_journal.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical feature journal: a later migration is applied; roll it back first';END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.canonical_feature_journal_seed(bigint,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb);
DROP FUNCTION IF EXISTS sessions.canonical_feature_image_key(jsonb,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_source_valid(jsonb);
DROP FUNCTION IF EXISTS sessions.canonical_feature_pointer_set_valid(jsonb);
DROP TABLE IF EXISTS sessions.canonical_feature_journal;
DO $r$BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_canonical_feature_producer')THEN
 REVOKE USAGE ON SCHEMA sessions FROM bulls96_canonical_feature_producer;END IF;END$r$;
DO $ledger$BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105025000_canonical_feature_journal.sql';
 END IF;
END $ledger$;
