-- Roll back strict readers and pipeline code before removing this additive schema.
DROP FUNCTION sessions.list_session_directory_events(text,text[],text[],text,text[],text,timestamptz,timestamptz,timestamptz,timestamptz,text[],text[],timestamptz,text,text,integer,text);
DROP FUNCTION sessions.session_directory_event_activity(text,timestamptz,timestamptz);
DROP TRIGGER session_directory_events_changed ON sessions.session_index;
DROP TRIGGER session_directory_events_tombstone ON sessions.session_directory;
DROP TRIGGER session_directory_event_times_forget ON sessions.session_directory;
DROP FUNCTION sessions.session_directory_events_changed();
DROP FUNCTION sessions.session_directory_events_tombstone();
DROP FUNCTION sessions.session_directory_event_times_forget();
DROP FUNCTION sessions.session_directory_events_require_ready(text);
DROP FUNCTION sessions.session_directory_events_list_sql(integer,boolean,integer,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean);
DROP TRIGGER session_directory_project_name ON sessions.session_directory;
DROP FUNCTION sessions.session_directory_project_name();
DROP TABLE sessions.session_directory_event_times;
DROP TABLE sessions.session_directory_event_pending;
ALTER TABLE sessions.session_directory DROP CONSTRAINT session_directory_events_ready,DROP CONSTRAINT session_directory_work_signal,DROP CONSTRAINT session_directory_project_locator,
 DROP COLUMN project_name,
 DROP COLUMN event_first_ts,
 DROP COLUMN event_last_ts,
 DROP COLUMN kinds,
 DROP COLUMN event_user_ids,
 DROP COLUMN event_machine_ids,
 DROP COLUMN event_models,
 DROP COLUMN event_project_id,
 DROP COLUMN opening_event,
 DROP COLUMN event_project_event,
 DROP COLUMN project_requires_hydration,
 DROP COLUMN task_refs,
 DROP COLUMN reasoning_tokens,
 DROP COLUMN cache_read_tokens,
 DROP COLUMN cache_create_tokens,
 DROP COLUMN cache_create_5m_tokens,
 DROP COLUMN cache_create_1h_tokens,
 DROP COLUMN work_status,
 DROP COLUMN work_status_at,
 DROP COLUMN event_projection_version,
 DROP COLUMN event_projection_ready,
 DROP COLUMN event_projected_at,
 DROP COLUMN event_projection_epoch,
 DROP COLUMN event_projection_sources;
