-- Stop the canonical feature producer before applying this rollback.
-- Progress is restartable metadata; dropping it forces the affected journal
-- entries to be copied again after the migration is reapplied.
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105030000_canonical_feature_consumer_acks.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='canonical feature consumer: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;
DROP TRIGGER IF EXISTS canonical_feature_page_ack_fence
  ON sessions.canonical_feature_page_ack;
DROP FUNCTION IF EXISTS sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_page_ack_fence();
DROP TABLE IF EXISTS sessions.canonical_feature_page_ack;
DROP TABLE IF EXISTS sessions.canonical_feature_copy_state;
DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105030000_canonical_feature_consumer_acks.sql';
  END IF;
END
$ledger$;
