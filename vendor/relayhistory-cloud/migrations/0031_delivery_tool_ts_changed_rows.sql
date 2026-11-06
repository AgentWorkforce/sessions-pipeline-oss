-- Tool-time settlement writes only the rows whose time changes. 0023 stays as applied.
--
-- 0023's settle_delivery_tool_ts rewrote every untimed tool_call/file_edit row
-- sharing a tool_use_id each time a session event naming that id was written,
-- even when the earliest parent time did not move. A session that reuses one
-- tool_use_id across N tool rows and N events (harnesses with per-turn call ids
-- such as `call_0`) therefore cost N x N row versions inside one delivery
-- transaction, under the workspace projection lock: 250 tools + 250 events took
-- about 5 s on local PostgreSQL 18 (100 + 100: 0.5 s). The result is the same;
-- the earliest parent time is read once and only rows whose ts differs from it
-- are written, so a settle writes only the rows that actually move. No
-- convergence_events trigger observes the skipped updates.
CREATE OR REPLACE FUNCTION sessions.settle_delivery_tool_ts(
  p_org text, p_workspace text, p_source text, p_session text, p_tool_use_id text
) RETURNS void LANGUAGE plpgsql AS $tool_ts$
DECLARE
  parent_ts timestamptz;
BEGIN
  IF p_tool_use_id IS NULL THEN RETURN; END IF;
  SELECT min(parent.ts) INTO parent_ts FROM sessions.convergence_events AS parent
   WHERE parent.org_id = p_org AND parent.workspace_id = p_workspace AND parent.source = p_source
     AND parent.session_id = p_session AND parent.kind = 'session_event'
     AND parent.delivery_record_id IS NOT NULL
     AND parent.record->'payload'->>'tool_use_id' = p_tool_use_id;
  UPDATE sessions.convergence_events AS tool SET ts = COALESCE(parent_ts, tool.ingested_at)
   WHERE tool.org_id = p_org AND tool.workspace_id = p_workspace AND tool.source = p_source
     AND tool.session_id = p_session AND tool.kind IN ('tool_call', 'file_edit')
     AND tool.delivery_record_id IS NOT NULL
     AND tool.record->'payload'->>'tool_use_id' = p_tool_use_id
     AND sessions.delivery_activity_ms(tool.record->'payload') IS NULL
     AND tool.ts IS DISTINCT FROM COALESCE(parent_ts, tool.ingested_at);
END;
$tool_ts$;
