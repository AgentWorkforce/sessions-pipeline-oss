-- Operator verification still requires an explicitly provisioned LOGIN.
-- Shared pure phase2 also runs for bare roles in migrator/live readiness.
\set ON_ERROR_STOP 1
DO $login$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_session_ingest' AND rolcanlogin)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest operator: LOGIN prerequisite absent';END IF;
END $login$;
\ir session_ingest_verify.sql
