-- Disable page readers/producers and retire existing artifact acknowledgements
-- before rollback. Reapply allocates a new epoch domain; old cursors/acks must
-- never be retained across this operator-controlled schema rollback.
DROP FUNCTION sessions.session_directory_replay_pages(text,text,text);
DROP FUNCTION sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer);
DROP FUNCTION sessions.capture_session_replay_pages(text,text,text,text);
DROP FUNCTION sessions.session_replay_source_snapshot(text,text,text,text);
DROP TRIGGER session_replay_directory_changed ON sessions.session_directory;
DROP TRIGGER session_replay_sources_changed ON sessions.session_index;
DROP FUNCTION sessions.session_replay_directory_changed();
DROP FUNCTION sessions.session_replay_sources_changed();
DROP TABLE sessions.session_replay_publications;
DROP SEQUENCE sessions.session_replay_epoch_seq;
DROP INDEX sessions.session_index_replay_canonical_live;
