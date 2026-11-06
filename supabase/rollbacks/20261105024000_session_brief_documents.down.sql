-- Rollback for registry slot 20261105024000 (upstream #188: 20261103090000).
-- Disable CH metadata search and publisher before this operator-only rollback.
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105024000_session_brief_documents.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='brief documents: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;
DROP TRIGGER session_brief_document_changed ON sessions.session_directory;
DROP FUNCTION sessions.session_brief_document_authority(text,text[],text[]);
DROP FUNCTION sessions.retry_session_brief_documents(uuid,text);
DROP FUNCTION sessions.ack_session_brief_documents(uuid,jsonb);
DROP FUNCTION sessions.claim_session_brief_documents(uuid,integer);
DROP FUNCTION sessions.backfill_session_brief_documents(integer);
DROP FUNCTION sessions.session_brief_document_changed();
DROP FUNCTION sessions.queue_session_brief_document(jsonb);
DROP FUNCTION sessions.session_brief_document_json(sessions.session_directory,boolean);
DROP FUNCTION sessions.set_session_brief_documents_live(boolean);
DROP TABLE sessions.session_brief_document_outbox;
DROP TABLE sessions.session_brief_document_scopes;
DROP TABLE sessions.session_brief_document_control;
DROP SEQUENCE sessions.session_brief_document_epoch_seq;
DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105024000_session_brief_documents.sql';
  END IF;
END
$ledger$;
-- Retain the CH derived table until separately authorized physical cleanup.
