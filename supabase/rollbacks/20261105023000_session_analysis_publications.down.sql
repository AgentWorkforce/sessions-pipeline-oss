-- Disable publisher/consumer and drain demand leases before schema rollback.
DROP FUNCTION sessions.request_session_analysis_cache(text,text,text);
DROP FUNCTION sessions.enqueue_session_analysis_cache(text,text,text,text);
DROP FUNCTION sessions.session_analysis_cache_demand(text,text,text,text,boolean);
DROP FUNCTION sessions.claim_session_analysis_cache();
DROP FUNCTION sessions.heartbeat_session_analysis_cache(text,text,text,text,bigint,text,uuid);
DROP FUNCTION sessions.finish_session_analysis_cache(text,text,text,text,bigint,text,uuid,text);
DROP TABLE sessions.session_analysis_wakeups;
-- Disable analysis cache consumer/publisher before schema rollback.
DROP FUNCTION sessions.session_directory_analysis_cache(text,text,text);
DROP FUNCTION sessions.publish_session_analysis_cache(text,text,text,text,bigint,text,timestamp with time zone,text,text,text,text,text,text,integer,uuid);
DROP TABLE sessions.session_analysis_publications;
