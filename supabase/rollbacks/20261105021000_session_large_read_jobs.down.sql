-- Disable routing and all large workers before applying this operator-only rollback.
DROP FUNCTION sessions.discover_session_large_reads();
DROP TABLE sessions.session_large_read_scan;
DROP FUNCTION sessions.route_session_large_read(text,text,text,text);
DROP FUNCTION sessions.retry_session_large_read(text,text,text,text,uuid,text);
DROP FUNCTION sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid);
DROP FUNCTION sessions.heartbeat_session_large_read(text,text,text,text,uuid);
DROP FUNCTION sessions.claim_session_large_read(uuid,integer);
DROP FUNCTION sessions.enqueue_session_large_read(text,text,text,text);
DROP TABLE sessions.session_large_read_jobs;
