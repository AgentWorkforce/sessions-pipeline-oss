-- Session revision feed for Cloud hooks. Every committed change to a session's
-- evidence (session_revisions) upserts one coalesced row per (org, session) in
-- the same transaction; catalog and commit-link changes upsert one per (org,
-- session, workspace, source). History publishes only {kind,eventId,generation}; Cloud
-- loads trusted tenant/source metadata through the SECURITY DEFINER functions
-- below and acknowledges with the generation it handled, so a revision that
-- lands while Cloud is working survives the ack and is delivered again.
--
-- Coalescing matches 0034's session wakeup (due 5s after the latest change,
-- never later than 30s after the burst began) with one difference: created_at
-- marks the start of the current burst. It restarts when a change arrives after
-- Cloud acknowledged the previous one; without that reset every later change
-- would be due immediately (created_at+30s in the past) and the feed would stop
-- coalescing after a session's first acknowledged burst.
--
-- Existing sessions are not backfilled: the feed starts with the next revision.
-- A row is one feed target. Revision rows (from session_revisions) are keyed by
-- (org, session) and leave workspace_id/source null: their attribution is
-- derived from the session's evidence when read. Rows woken by a table that
-- carries its own tenant scope (session_catalog, session_commit_links) are keyed
-- by (org, session, workspace, source) and carry that scope, so a session id
-- reused in another workspace never lends this change its attribution, and two
-- workspaces sharing a session id get separate events. user_id is the latest
-- change's actor as the trigger determined it (see
-- record_session_evidence_revision_event); the reader prefers the scope's live
-- catalog uploader over it.
CREATE TABLE sessions.session_revision_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL,
  session_id text NOT NULL,
  workspace_id text,
  source text,
  user_id text,
  revision bigint NOT NULL,
  generation bigint NOT NULL DEFAULT 1,
  available_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz,
  acknowledged_at timestamptz,
  CHECK ((workspace_id IS NULL) = (source IS NULL))
);
CREATE UNIQUE INDEX session_revision_events_target ON sessions.session_revision_events
  (org_id, session_id, (COALESCE(workspace_id, '')), (COALESCE(source, '')));
CREATE INDEX session_revision_events_pending ON sessions.session_revision_events (available_at, id)
  WHERE acknowledged_at IS NULL;

-- One coalesced upsert per feed target; every feed writer goes through here.
-- Trigger functions run as the writing role (Cloud's runtime role during ingest)
-- and a nested call checks EXECUTE, so this helper keeps the default EXECUTE
-- grant, exactly like 0034's trigger functions. It runs with invoker rights: a
-- caller still needs INSERT/UPDATE on session_revision_events.
CREATE FUNCTION sessions.enqueue_session_revision_event(
  p_org_id text, p_session_id text, p_revision bigint,
  p_workspace_id text DEFAULT NULL, p_source text DEFAULT NULL, p_user_id text DEFAULT NULL
) RETURNS void LANGUAGE sql SET search_path = pg_catalog, sessions, pg_temp AS $enqueue_revision$
  INSERT INTO sessions.session_revision_events AS e
      (org_id, session_id, workspace_id, source, user_id, revision, available_at)
    VALUES (p_org_id, p_session_id, p_workspace_id, p_source, p_user_id, p_revision,
      now() + interval '5 seconds')
    ON CONFLICT (org_id, session_id, (COALESCE(workspace_id, '')), (COALESCE(source, ''))) DO UPDATE SET
      generation = e.generation + 1,
      revision = EXCLUDED.revision,
      user_id = EXCLUDED.user_id,
      created_at = CASE WHEN e.acknowledged_at IS NULL THEN e.created_at ELSE now() END,
      available_at = LEAST(EXCLUDED.available_at,
        CASE WHEN e.acknowledged_at IS NULL THEN e.created_at ELSE now() END + interval '30 seconds'),
      sent_at = NULL,
      acknowledged_at = NULL;
$enqueue_revision$;

CREATE FUNCTION sessions.record_session_revision_event() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $revision_event$
BEGIN
  PERFORM sessions.enqueue_session_revision_event(NEW.org_id, NEW.session_id, NEW.revision);
  RETURN NEW;
END;
$revision_event$;
CREATE TRIGGER session_revision_event AFTER INSERT OR UPDATE ON sessions.session_revisions
FOR EACH ROW EXECUTE FUNCTION sessions.record_session_revision_event();

-- PR evidence also reads commit links and the catalog's repo_url. Those are not
-- brief inputs and do not bump session_revisions (that would invalidate briefs),
-- so their changes wake the feed directly at the current revision: statement
-- level, one upsert per touched (org, session, workspace, source), covering
-- deletes and links moved between sessions or workspaces. Catalog rows count
-- only when repo_url changes, because every upload rewrites the catalog row.
-- Remaining-evidence user of one scope after a statement: the single distinct
-- authenticated uploader among the scope's catalog row, commit links and events,
-- else null (none left, or several users). Used where a change has no actor of
-- its own (a deletion, or a row moved out of the scope).
CREATE FUNCTION sessions.session_scope_sole_user(
  p_org_id text, p_workspace_id text, p_source text, p_session_id text
) RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog, sessions, pg_temp AS $sole_user$
  SELECT CASE WHEN count(DISTINCT u.user_id) = 1 THEN min(u.user_id) END FROM (
    SELECT sc.user_id FROM sessions.session_catalog sc
     WHERE sc.org_id = p_org_id AND sc.workspace_id = p_workspace_id
       AND sc.source = p_source AND sc.session_id = p_session_id
    UNION ALL
    SELECT l.user_id FROM sessions.session_commit_links l
     WHERE l.org_id = p_org_id AND l.workspace_id = p_workspace_id
       AND l.source = p_source AND l.session_id = p_session_id
    UNION ALL
    SELECT c.user_id FROM sessions.convergence_events c
     WHERE c.org_id = p_org_id AND c.workspace_id = p_workspace_id
       AND c.source = p_source AND c.session_id = p_session_id
  ) u;
$sole_user$;

CREATE FUNCTION sessions.record_session_evidence_revision_event() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $evidence_event$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- The inserted rows are the change's actor: their own uploader, when the
    -- statement wrote that scope as exactly one user.
    PERFORM sessions.enqueue_session_revision_event(t.org_id, t.session_id,
        COALESCE((SELECT r.revision FROM sessions.session_revisions r
          WHERE r.org_id = t.org_id AND r.session_id = t.session_id), 0),
        t.workspace_id, t.source, t.actor)
      FROM (SELECT n.org_id, n.session_id, n.workspace_id, n.source,
                   CASE WHEN count(DISTINCT n.user_id) = 1 THEN min(n.user_id) END AS actor
              FROM new_rows n
             WHERE TG_TABLE_NAME = 'session_commit_links' OR to_jsonb(n)->>'repo_url' IS NOT NULL
             GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4) t;
  ELSIF TG_OP = 'DELETE' THEN
    -- A deletion has no actor here (old_rows holds the previous uploader, not
    -- whoever removed it), so it is attributed to the one user whose evidence
    -- remains in the scope, else null.
    PERFORM sessions.enqueue_session_revision_event(t.org_id, t.session_id,
        COALESCE((SELECT r.revision FROM sessions.session_revisions r
          WHERE r.org_id = t.org_id AND r.session_id = t.session_id), 0),
        t.workspace_id, t.source,
        sessions.session_scope_sole_user(t.org_id, t.workspace_id, t.source, t.session_id))
      FROM (SELECT DISTINCT o.org_id, o.session_id, o.workspace_id, o.source FROM old_rows o
             WHERE TG_TABLE_NAME = 'session_commit_links' OR to_jsonb(o)->>'repo_url' IS NOT NULL
             ORDER BY 1, 2, 3, 4) t;
  ELSE
    -- Changed PR-evidence content only. Uploader (user_id) and delivery
    -- bookkeeping never count; for the catalog only repo_url does. A scope with
    -- changed new rows is attributed to their single uploader; a scope that only
    -- lost rows (moved away) follows the deletion rule.
    PERFORM sessions.enqueue_session_revision_event(t.org_id, t.session_id,
        COALESCE((SELECT r.revision FROM sessions.session_revisions r
          WHERE r.org_id = t.org_id AND r.session_id = t.session_id), 0),
        t.workspace_id, t.source,
        CASE WHEN t.new_users > 0 THEN t.actor
             ELSE sessions.session_scope_sole_user(t.org_id, t.workspace_id, t.source, t.session_id) END)
      FROM (SELECT changed.org_id, changed.session_id, changed.workspace_id, changed.source,
                   count(changed.user_id) FILTER (WHERE changed.side = 'new') AS new_users,
                   CASE WHEN count(DISTINCT changed.user_id) FILTER (WHERE changed.side = 'new') = 1
                     THEN min(changed.user_id) FILTER (WHERE changed.side = 'new') END AS actor
              FROM (
              SELECT 'new' AS side, d.* FROM (
                SELECT n.org_id, n.session_id, n.workspace_id, n.source, n.user_id,
                  CASE WHEN TG_TABLE_NAME = 'session_catalog' THEN to_jsonb(n)->'repo_url'
                  ELSE to_jsonb(n) - 'received_at' - 'record_id' - 'origin_id' - 'user_id' END AS body
                FROM new_rows n) d
               WHERE (d.org_id, d.session_id, d.workspace_id, d.source, d.body) NOT IN (
                SELECT o.org_id, o.session_id, o.workspace_id, o.source,
                  CASE WHEN TG_TABLE_NAME = 'session_catalog' THEN to_jsonb(o)->'repo_url'
                  ELSE to_jsonb(o) - 'received_at' - 'record_id' - 'origin_id' - 'user_id' END
                FROM old_rows o)
              UNION ALL
              SELECT 'old' AS side, d.* FROM (
                SELECT o.org_id, o.session_id, o.workspace_id, o.source, o.user_id,
                  CASE WHEN TG_TABLE_NAME = 'session_catalog' THEN to_jsonb(o)->'repo_url'
                  ELSE to_jsonb(o) - 'received_at' - 'record_id' - 'origin_id' - 'user_id' END AS body
                FROM old_rows o) d
               WHERE (d.org_id, d.session_id, d.workspace_id, d.source, d.body) NOT IN (
                SELECT n.org_id, n.session_id, n.workspace_id, n.source,
                  CASE WHEN TG_TABLE_NAME = 'session_catalog' THEN to_jsonb(n)->'repo_url'
                  ELSE to_jsonb(n) - 'received_at' - 'record_id' - 'origin_id' - 'user_id' END
                FROM new_rows n)
            ) changed GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4) t;
  END IF;
  RETURN NULL;
END;
$evidence_event$;
CREATE TRIGGER session_commit_links_revision_event_insert
  AFTER INSERT ON sessions.session_commit_links REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.record_session_evidence_revision_event();
CREATE TRIGGER session_commit_links_revision_event_update
  AFTER UPDATE ON sessions.session_commit_links REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.record_session_evidence_revision_event();
CREATE TRIGGER session_commit_links_revision_event_delete
  AFTER DELETE ON sessions.session_commit_links REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.record_session_evidence_revision_event();
CREATE TRIGGER session_catalog_revision_event_insert
  AFTER INSERT ON sessions.session_catalog REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.record_session_evidence_revision_event();
CREATE TRIGGER session_catalog_revision_event_update
  AFTER UPDATE ON sessions.session_catalog REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.record_session_evidence_revision_event();
CREATE TRIGGER session_catalog_revision_event_delete
  AFTER DELETE ON sessions.session_catalog REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.record_session_evidence_revision_event();

-- Attribution model (from the write paths): convergence_events, session_catalog
-- and session_commit_links carry the authenticated workspace and uploader
-- (user_id) and the record's harness (source). conversation_turns carry the
-- authenticated workspace but no authenticated user (session_owner is free text,
-- a human name or a harness), and their source column is the harness only when
-- the turn or its request was tagged; otherwise 0020/ingest store session_owner
-- there as a fallback.
--
-- A scoped row (catalog/commit-link change) reports its own workspace/source.
-- Its user is the scope's live catalog uploader, else the change's actor stored
-- by the trigger: the changed row's own uploader on insert/update, or on delete
-- the one user whose evidence remains in the scope (else null). No user is taken
-- from an unrelated row.
--
-- A revision row derives attribution from session evidence: workspace and
-- harness from events and turns (a turn's owner-fallback source is not a
-- harness), user only from events (authenticated). More than one workspace or
-- harness makes it ambiguous and those fields are then null.
--
-- userId is best-effort, informational attribution, NOT an identity assertion.
-- It may be null, and in a session several users contributed to it names only
-- one of them. Consumers must not authorize, scope tenant access, or select
-- credentials with it; authorize on orgId + workspaceId only.
CREATE FUNCTION sessions.read_session_revision_event_v1(p_id uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $read_revision$
 SELECT jsonb_build_object('eventId', e.id, 'generation', e.generation, 'orgId', e.org_id,
   'sessionId', e.session_id, 'revision', e.revision,
   'workspaceId', CASE WHEN e.workspace_id IS NOT NULL THEN e.workspace_id
                       WHEN o.ambiguous THEN NULL ELSE o.workspace_id END,
   'userId', CASE WHEN e.workspace_id IS NOT NULL THEN COALESCE(
                    (SELECT sc.user_id FROM sessions.session_catalog sc
                      WHERE sc.org_id = e.org_id AND sc.workspace_id = e.workspace_id
                        AND sc.source = e.source AND sc.session_id = e.session_id),
                    e.user_id)
                  WHEN o.ambiguous THEN NULL ELSE o.user_id END,
   'source', CASE WHEN e.workspace_id IS NOT NULL THEN e.source
                  WHEN o.ambiguous THEN NULL ELSE o.source END,
   'ambiguous', e.workspace_id IS NULL AND o.ambiguous)
 FROM sessions.session_revision_events e
 CROSS JOIN LATERAL (
   SELECT count(DISTINCT evidence.workspace_id) > 1 OR count(DISTINCT evidence.source) > 1 AS ambiguous,
     min(evidence.workspace_id) AS workspace_id, min(evidence.user_id) AS user_id,
     min(evidence.source) AS source
   FROM (
     SELECT c.workspace_id, c.user_id, NULLIF(c.source, '') AS source FROM sessions.convergence_events c
      WHERE c.org_id = e.org_id AND c.session_id = e.session_id
     UNION ALL
     SELECT t.workspace_id, NULL::text,
       CASE WHEN NULLIF(COALESCE(NULLIF(t.metadata->>'nativeCli', ''), t.metadata->>'source'), '') IS NOT NULL
              OR t.source IS DISTINCT FROM t.session_owner
            THEN NULLIF(t.source, '') END
     FROM sessions.conversation_turns t
      WHERE t.org_id = e.org_id AND t.session_id = e.session_id
   ) evidence
 ) o
 WHERE e.id = p_id;
$read_revision$;

CREATE FUNCTION sessions.pending_session_revision_events_v1(p_limit integer)
RETURNS TABLE(event_id uuid, generation bigint)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $pending_revision$
 SELECT e.id, e.generation FROM sessions.session_revision_events e
  WHERE e.acknowledged_at IS NULL AND e.available_at <= now()
  ORDER BY e.available_at, e.id LIMIT LEAST(100, GREATEST(1, COALESCE(p_limit, 1)));
$pending_revision$;

-- Generation-fenced: an ack for an older generation leaves a newer revision pending.
CREATE FUNCTION sessions.ack_session_revision_events_v1(p_ids uuid[], p_generations bigint[]) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $ack_revision$
 UPDATE sessions.session_revision_events e SET acknowledged_at = now()
   FROM unnest(p_ids, p_generations) AS acked(id, generation)
  WHERE e.id = acked.id AND e.generation = acked.generation AND e.acknowledged_at IS NULL;
$ack_revision$;

-- Pull requests a session created or contributed to, read deterministically from
-- that session's own recorded tool calls and their own successful results (plus
-- explicit commit links). Nothing is inferred from prose or unrelated URLs.
--
-- Calls are delivery-projected tool_call rows that are not errored; each kind
-- below needs the call's own tool_result row (same tool_use_id, workspace, source
-- and session) whose result_status is not errored/cancelled and whose
-- error_signal is none. Shell tools are Claude Bash and Codex exec_command/shell/
-- exec (and equivalents); their command is args.command (string or argv array),
-- else args.cmd, else the projected target.
--
-- creator:
--   gh_pr_create             shell `gh pr create`; PR URL from its output.
--   mcp_create_pull_request  tool name ending in create_pull_request; when the
--                            result has JSON "html_url"/"url" PR keys only those
--                            are used (a PR body may mention other PRs).
--   head_ref: gh --head/-H, MCP args.head, gh's "Creating pull request for X",
--   else the branch this session last pushed to the same repo before creating.
-- contributor:
--   git_push                 shell `git push`; owner/repo from the output's
--                            `To <github remote>` line and head_ref from each
--                            ref update `<src> -> <dst>` under it. Rejected
--                            (`!`), deleted (`-`) and tag refs never match.
--   gh_pr_command            shell `gh pr comment|edit|review|merge|checkout|
--                            ready|close|reopen|update-branch`; the target is
--                            its first positional argument (a PR URL, a number,
--                            or a branch -> head_ref), never a URL inside an
--                            option value. A number/branch resolves its repo
--                            from -R/--repo, the catalog's github.com repo_url,
--                            the command's own output URL for that number, or
--                            the one GitHub repo this session pushed to; with no
--                            positional the output's PR URL is the target.
--   mcp_pull_request_tool    PR-acting GitHub MCP tools; owner/repo and
--                            pull_number/issue_number from args.
--                            add_issue_comment counts only when its result links
--                            a /pull/ URL.
--   commit_link              session_commit_links evidence.github_pr.id "o/r#N".
--
-- Branch-only evidence joins a numbered target found in the same session for the
-- same owner/repo/head_ref by timeline (latest PR at or before it, else the first
-- after it; between two different PRs on a reused branch it stays branch-only). One row per target (owner, repo, number), else
-- (owner, repo, head_ref): role is creator when any creator evidence exists;
-- evidence_kind/tool_use_id/tool_name come from the strongest, then earliest
-- evidence; detected_at is the earliest and last_seen_at the latest evidence.
-- A null workspace or source matches any.
CREATE FUNCTION sessions.session_pull_request_evidence_v1(
  p_org_id text, p_workspace_id text, p_source text, p_session_id text
) RETURNS TABLE(pull_request_url text, owner text, repo text, number integer, head_ref text,
  tool_use_id text, tool_name text, evidence_kind text, role text,
  detected_at timestamptz, last_seen_at timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp AS $pr_evidence$
 WITH calls AS (
   SELECT c.workspace_id, c.source, c.record->'payload'->>'tool_use_id' AS call_id,
     c.tool_name AS call_name, lower(c.tool_name) AS lname,
     COALESCE(c.tool_calls->0->'args',
       sessions.delivery_catalog_json(c.record->'payload'->'args_json'), '{}'::jsonb) AS args,
     c.record->'payload'->>'target' AS target
   FROM sessions.convergence_events c
   WHERE c.org_id = p_org_id AND c.session_id = p_session_id
     AND (p_workspace_id IS NULL OR c.workspace_id = p_workspace_id)
     AND (p_source IS NULL OR c.source = p_source)
     AND c.kind = 'tool_call' AND c.delivery_record_id IS NOT NULL
     AND NULLIF(c.record->'payload'->>'tool_use_id', '') IS NOT NULL
     AND COALESCE(c.record->'payload'->>'is_error', '') NOT IN ('1', 'true')
     AND c.tool_status IS DISTINCT FROM 'error'
 ), shaped AS (
   SELECT calls.*,
     calls.lname IN ('bash', 'shell', 'exec', 'exec_command', 'shell_command', 'local_shell',
       'local_shell_call', 'container.exec', 'unified_exec', 'run_shell_command',
       'run_terminal_cmd') AS is_shell,
     COALESCE(
       CASE jsonb_typeof(calls.args->'command')
         WHEN 'string' THEN calls.args->>'command'
         WHEN 'array' THEN (SELECT string_agg(part, ' ') FROM jsonb_array_elements_text(calls.args->'command') AS part)
       END,
       CASE WHEN jsonb_typeof(calls.args->'cmd') = 'string' THEN calls.args->>'cmd' END,
       calls.target, '') AS cmd
   FROM calls
 ), kinds AS (
   SELECT shaped.*, k.kind
   FROM shaped CROSS JOIN LATERAL (
     SELECT 'mcp_create_pull_request' AS kind
      WHERE shaped.lname ~ '(^|[^a-z0-9])create_pull_request$'
     UNION ALL SELECT 'mcp_pull_request_tool'
      WHERE shaped.lname ~ '(^|[^a-z0-9])(update_pull_request|merge_pull_request|create_pull_request_review|create_and_submit_pull_request_review|submit_pending_pull_request_review|add_pull_request_review_comment[a-z_]*|add_issue_comment|update_pull_request_branch|request_copilot_review)$'
     UNION ALL SELECT 'gh_pr_create'
      WHERE shaped.is_shell AND shaped.cmd ~* '(^|[^[:alnum:]_-])gh\s+pr\s+create($|[^[:alnum:]_-])'
     UNION ALL SELECT 'gh_pr_command'
      WHERE shaped.is_shell AND shaped.cmd ~* '(^|[^[:alnum:]_-])gh\s+pr\s+(comment|edit|review|merge|checkout|ready|close|reopen|update-branch)($|[^[:alnum:]_-])'
     UNION ALL SELECT 'git_push'
      WHERE shaped.is_shell AND shaped.cmd ~* '(^|[^[:alnum:]_-])git(\s+-[cC]\s+\S+)*\s+push($|[^[:alnum:]_-])'
   ) k
 ), res AS (
   SELECT kinds.*, r.event_id AS result_id, r.content, r.ts,
     -- Codex `shell` wraps output as {"output": "...", "metadata": {...}}.
     COALESCE(CASE WHEN r.content ~ '^\s*\{'
       THEN sessions.delivery_catalog_json(to_jsonb(r.content))->>'output' END, r.content) AS out
   FROM kinds
   JOIN sessions.convergence_events r
     ON r.org_id = p_org_id AND r.workspace_id = kinds.workspace_id AND r.source = kinds.source
    AND r.session_id = p_session_id AND r.delivery_record_id IS NOT NULL
    AND r.record->'payload'->>'tool_use_id' = kinds.call_id
   WHERE r.kind = 'session_event'
     AND (r.type = 'tool_result' OR r.actor_role = 'tool_result'
          OR r.record->'payload'->>'kind' = 'tool_result')
     AND COALESCE(r.record->'payload'->>'result_status', '') NOT IN ('errored', 'cancelled')
     AND COALESCE(NULLIF(r.record->'payload'->>'error_signal', ''), 'none') = 'none'
     AND r.content IS NOT NULL
 ), push_lines AS (
   SELECT res.result_id, res.call_id, line.n, line.text
   FROM res CROSS JOIN LATERAL regexp_split_to_table(res.out, '\r?\n') WITH ORDINALITY AS line(text, n)
   WHERE res.kind = 'git_push'
 ), push_remotes AS (
   SELECT l.result_id, l.call_id, l.n, m[1] AS owner, regexp_replace(m[2], '\.git$', '', 'i') AS repo
   FROM push_lines l
   CROSS JOIN LATERAL regexp_match(l.text,
     '^\s*To\s+(?:\S*[/@])?github\.com[:/]([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/?\s*$', 'i') AS m
   WHERE m IS NOT NULL
 ), push_refs AS (
   SELECT l.result_id, l.call_id, l.n, regexp_replace(m[1], '^refs/heads/', '') AS head_ref
   FROM push_lines l
   CROSS JOIN LATERAL regexp_match(l.text,
     '^\s*[*+]?\s*(?:\[new branch\]|[0-9a-f]{4,}\.{2,3}[0-9a-f]{4,})\s+\S+\s+->\s+(\S+)') AS m
   WHERE m IS NOT NULL
 ), pushes AS (
   SELECT DISTINCT remote.owner, remote.repo, ref.head_ref, res.call_id, res.call_name, res.ts
   FROM push_refs ref
   JOIN res ON res.kind = 'git_push' AND res.result_id = ref.result_id AND res.call_id = ref.call_id
   CROSS JOIN LATERAL (
     SELECT pr.owner, pr.repo FROM push_remotes pr
      WHERE pr.result_id = ref.result_id AND pr.call_id = ref.call_id AND pr.n < ref.n
      ORDER BY pr.n DESC LIMIT 1
   ) remote
 ), creators AS (
   -- head_ref only from deterministic sources: --head/-H, MCP args.head, or gh's
   -- own "Creating pull request for <branch>" line. It is never inferred from the
   -- branch the session last pushed: the checked-out branch is unknown, so that
   -- guess could fold another branch's pushes into this PR. Without one the
   -- creator row is keyed by number only.
   SELECT found.pr_owner AS owner, found.pr_repo AS repo, found.pr_number::integer AS number,
     COALESCE(
       CASE WHEN res.kind = 'gh_pr_create'
         THEN substring(res.cmd from '(?:--head|-H)(?:\s+|=)["'']?([^\s"'';&|]+)') END,
       CASE WHEN res.kind = 'mcp_create_pull_request' AND jsonb_typeof(res.args->'head') = 'string'
         THEN res.args->>'head' END,
       substring(res.out from 'Creating pull request for (\S+) into ')) AS head_ref,
     res.call_id, res.call_name, res.kind, res.ts
   FROM res CROSS JOIN LATERAL (
     SELECT sessions.delivery_catalog_json(to_jsonb(res.content)) AS j
   ) parsed CROSS JOIN LATERAL (
     -- A JSON object result (MCP): only its own top-level html_url, else url. A
     -- URL anywhere inside a string value (a PR body) is never read.
     SELECT top[1] AS pr_owner, top[2] AS pr_repo, top[3] AS pr_number
       FROM (SELECT regexp_match(COALESCE(
           CASE WHEN parsed.j->>'html_url' ~* '^https://github\.com/[^/]+/[^/]+/pull/[0-9]'
             THEN parsed.j->>'html_url' END,
           parsed.j->>'url'),
         '^https://github\.com/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]{1,9})(?:[/?#].*)?$', 'i') AS top) t
      WHERE res.kind = 'mcp_create_pull_request' AND jsonb_typeof(parsed.j) = 'object'
        AND t.top IS NOT NULL
     UNION ALL
     -- Text output: gh's own output (Codex's {"output": ...} wrapper removed), or
     -- an MCP result that is not a JSON object.
     SELECT bare[1], bare[2], bare[3]
       FROM regexp_matches(res.out,
         'https://github\.com/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]{1,9})(?![0-9])', 'gi') AS bare
      WHERE res.kind = 'gh_pr_create'
         OR jsonb_typeof(parsed.j) IS DISTINCT FROM 'object'
   ) found
   WHERE res.kind IN ('gh_pr_create', 'mcp_create_pull_request')
 ), gh_args AS (
   SELECT res.*,
     substring(res.cmd from '(?:^|[^[:alnum:]_-])gh\s+pr\s+(?:comment|edit|review|merge|checkout|ready|close|reopen|update-branch)((?:\s[^;&|\n]*)?)(?=$|[;&|\n])') AS seg,
     lower(substring(res.cmd from '(?:^|[^[:alnum:]_-])gh\s+pr\s+(comment|edit|review|merge|checkout|ready|close|reopen|update-branch)(?:\s|$|[;&|\n])')) AS sub,
     (SELECT regexp_match(sc.repo_url,
        '^(?:(?:https?|ssh|git)://)?(?:[^@/\s]+@)?github\.com[:/]([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/?$', 'i')
       FROM sessions.session_catalog sc
      WHERE sc.org_id = p_org_id AND sc.workspace_id = res.workspace_id
        AND sc.source = res.source AND sc.session_id = p_session_id) AS catalog_repo
   FROM res WHERE res.kind = 'gh_pr_command'
 ), gh_parsed AS (
   -- The target is the first positional argument, never a URL inside an option
   -- value such as --body or --title. The segment is split into shell words
   -- (adjacent quoted/unquoted fragments form one word, quotes then removed);
   -- a word starting with `-` is a flag, and the word after a flag that takes a
   -- value is that flag's value. Value flags follow `gh pr <sub> --help` per
   -- subcommand (e.g. -c takes a value for close/reopen but is a switch for
   -- review, -m a value for edit but a switch for merge; -b is --branch for
   -- checkout), plus --attach (comment/edit) and --worktree (checkout) from
   -- other gh releases. `--flag=value` carries its own value; unknown flags are
   -- switches. -R/--repo is read with quoted values blanked for the same reason.
   SELECT gh_args.*,
     regexp_match(regexp_replace(gh_args.seg, '"[^"]*"|''[^'']*''', '""', 'g'),
       '(?:^|\s)(?:-R|--repo)(?:\s+|=)(?:(?:https://)?github\.com/)?([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)', 'i') AS repo_flag,
     (SELECT w.tok FROM (
        SELECT t.n, t.tok, t.is_flag,
          COALESCE(lag(t.takes_value) OVER (ORDER BY t.n), false) AS is_flag_value
        FROM (
          SELECT x.n, x.tok,
            x.tok ~ '^-.' AS is_flag,
            -- `--flag=value` (quoted or not) carries its own value.
            x.tok ~ '^-.' AND x.tok !~ '=' AND x.tok = ANY (
              ARRAY['-R', '--repo'] || CASE gh_args.sub
                WHEN 'comment' THEN ARRAY['-b', '--body', '-F', '--body-file', '--attach']
                WHEN 'edit' THEN ARRAY['-b', '--body', '-F', '--body-file', '-t', '--title',
                  '-B', '--base', '-m', '--milestone', '--add-assignee', '--remove-assignee',
                  '--add-label', '--remove-label', '--add-project', '--remove-project',
                  '--add-reviewer', '--remove-reviewer', '--attach']
                WHEN 'review' THEN ARRAY['-b', '--body', '-F', '--body-file']
                WHEN 'merge' THEN ARRAY['-b', '--body', '-F', '--body-file', '-t', '--subject',
                  '-A', '--author-email', '--match-head-commit']
                WHEN 'checkout' THEN ARRAY['-b', '--branch', '--worktree']
                WHEN 'close' THEN ARRAY['-c', '--comment']
                WHEN 'reopen' THEN ARRAY['-c', '--comment']
                ELSE ARRAY[]::text[] END) AS takes_value
          FROM (
            -- One token per shell word: adjacent quoted and unquoted fragments
            -- join into one word, then the quotes are removed.
            SELECT y.ord AS n,
              regexp_replace(y.word[1], '"([^"]*)"|''([^'']*)''', '\1\2', 'g') AS tok
            FROM regexp_matches(gh_args.seg, '((?:"[^"]*"|''[^'']*''|[^\s"''])+)', 'g')
              WITH ORDINALITY AS y(word, ord)
          ) x
        ) t
      ) w
      WHERE NOT w.is_flag AND NOT w.is_flag_value
      ORDER BY w.n LIMIT 1) AS positional,
     regexp_match(gh_args.out, 'https://github\.com/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]{1,9})(?![0-9])', 'i') AS out_url
   FROM gh_args
 ), gh_targets AS (
   SELECT gh_parsed.*,
     regexp_match(positional, '^https://github\.com/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]{1,9})(?:[/?#][^\s]*)?$', 'i') AS arg_url,
     CASE WHEN positional ~ '^#?[0-9]{1,9}$' THEN ltrim(positional, '#')::integer END AS arg_number,
     CASE WHEN positional !~ '^#?[0-9]+$' AND positional !~* '^https?:'
            AND positional ~ '^[A-Za-z0-9._/:-]+$'
       THEN positional END AS arg_branch,
     -- The one GitHub repository this session pushed to, when there is exactly one.
     (SELECT CASE WHEN count(DISTINCT lower(p.owner) || '/' || lower(p.repo)) = 1
               THEN ARRAY[min(p.owner), min(p.repo)] END FROM pushes p) AS push_repo
   FROM gh_parsed
 ), gh_cmds AS (
   SELECT pr_target[1] AS owner, regexp_replace(pr_target[2], '\.git$', '', 'i') AS repo,
     pr_target[3]::integer AS number, arg_branch AS head_ref, call_id, call_name, kind, ts
   FROM (
     SELECT gh_targets.*, CASE
       WHEN arg_url IS NOT NULL THEN arg_url
       WHEN arg_number IS NOT NULL THEN COALESCE(repo_flag, catalog_repo,
           CASE WHEN out_url[3]::integer = arg_number THEN out_url[1:2] END, push_repo)
         || ARRAY[arg_number::text]
       WHEN arg_branch IS NOT NULL THEN COALESCE(repo_flag, catalog_repo, push_repo) || ARRAY[NULL::text]
       ELSE out_url END AS pr_target
     FROM gh_targets
   ) g
 ), mcp_tools AS (
   SELECT res.args->>'owner' AS owner, res.args->>'repo' AS repo,
     (SELECT v FROM (VALUES (res.args->>'pull_number'), (res.args->>'pullNumber'),
        (res.args->>'issue_number'), (res.args->>'issueNumber')) AS n(v)
       WHERE v ~ '^[0-9]{1,9}$' LIMIT 1)::integer AS number,
     NULL::text AS head_ref, res.call_id, res.call_name, res.kind, res.ts
   FROM res
   WHERE res.kind = 'mcp_pull_request_tool'
     -- An issue number names an issue or a PR. A comment counts only when GitHub's
     -- own comment URL for this very owner/repo/number is a /pull/ URL (the
     -- top-level html_url of a JSON result, else one in the text); a PR merely
     -- quoted in the comment body never qualifies.
     AND (res.lname !~ '(^|[^a-z0-9])add_issue_comment$'
          OR EXISTS (
            SELECT 1 FROM (
              SELECT sessions.delivery_catalog_json(to_jsonb(res.content)) AS j) parsed
            CROSS JOIN LATERAL regexp_matches(
              CASE WHEN jsonb_typeof(parsed.j) = 'object' THEN COALESCE(parsed.j->>'html_url', '')
                   ELSE res.content END,
              'https://github\.com/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]{1,9})#issuecomment-', 'gi') AS m
            WHERE lower(m[1]) = lower(res.args->>'owner') AND lower(m[2]) = lower(res.args->>'repo')
              AND m[3] = COALESCE(res.args->>'issue_number', res.args->>'issueNumber')))
 ), links AS (
   SELECT m[1] AS owner, m[2] AS repo, m[3]::integer AS number, NULL::text AS head_ref,
     NULL::text AS call_id, NULL::text AS call_name, 'commit_link' AS kind,
     COALESCE(l.linked_at, l.received_at) AS ts
   FROM sessions.session_commit_links l
   CROSS JOIN LATERAL regexp_match(l.evidence->'github_pr'->>'id',
     '^([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)#([0-9]{1,9})$') AS m
   WHERE l.org_id = p_org_id AND l.session_id = p_session_id
     AND (p_workspace_id IS NULL OR l.workspace_id = p_workspace_id)
     AND (p_source IS NULL OR l.source = p_source)
     AND jsonb_typeof(l.evidence) = 'object'
     AND COALESCE(l.evidence->'github_pr'->>'system', 'github') = 'github'
     AND m IS NOT NULL
 ), raw_evidence AS (
   SELECT owner, repo, number, head_ref, call_id, call_name, kind, ts FROM creators
   UNION ALL SELECT owner, repo, NULL::integer, head_ref, call_id, call_name, 'git_push', ts FROM pushes
   UNION ALL SELECT owner, repo, number, head_ref, call_id, call_name, kind, ts FROM gh_cmds
   UNION ALL SELECT owner, repo, number, head_ref, call_id, call_name, kind, ts FROM mcp_tools
   UNION ALL SELECT owner, repo, number, head_ref, call_id, call_name, kind, ts FROM links
 ), evidence AS (
   -- One head-ref normalization for every source (gh --head/-H, MCP head, gh's
   -- "Creating pull request for", git push destinations, gh pr <branch>):
   -- `owner:branch` keeps the branch and records owner as the head's owner (a
   -- fork when it differs from the PR repo's owner), and `refs/heads/` is
   -- dropped. A remote-style prefix such as `origin/feat` is kept: it is a legal
   -- branch name, gh --head does not take remote names, and push output names
   -- remote URLs, not remote names, so stripping it would be a guess.
   SELECT owner, repo, number,
     regexp_replace(regexp_replace(head_ref, '^[A-Za-z0-9-]+:', ''), '^refs/heads/', '') AS head_ref,
     COALESCE(substring(head_ref from '^([A-Za-z0-9-]+):'), owner) AS head_owner,
     call_id, call_name, kind, ts
   FROM raw_evidence
 ), valid AS (
   SELECT * FROM evidence
   WHERE owner ~ '^[A-Za-z0-9-]+$' AND repo ~ '^[A-Za-z0-9_.-]+$'
     AND (number IS NOT NULL OR NULLIF(head_ref, '') IS NOT NULL)
 ), ranked AS (
   SELECT v.*, lower(v.owner) AS owner_key, lower(v.repo) AS repo_key,
     -- Branch-only evidence joins a numbered PR on the same branch by timeline:
     -- the latest PR seen at or before it, else the first one after it. When both
     -- exist and differ (a push between two PRs reusing the branch) it is
     -- ambiguous and stays a branch-only row.
     CASE WHEN v.number IS NOT NULL THEN v.number
          WHEN before_pr.number IS NULL THEN after_pr.number
          WHEN after_pr.number IS NULL OR after_pr.number = before_pr.number THEN before_pr.number
     END AS pr_number,
     CASE v.kind WHEN 'gh_pr_create' THEN 1 WHEN 'mcp_create_pull_request' THEN 1
       WHEN 'commit_link' THEN 2 WHEN 'mcp_pull_request_tool' THEN 3
       WHEN 'gh_pr_command' THEN 4 ELSE 5 END AS strength
   FROM valid v
   LEFT JOIN LATERAL (
     SELECT n.number FROM valid n
      WHERE v.number IS NULL AND n.number IS NOT NULL AND n.head_ref = v.head_ref
        AND lower(n.owner) = lower(v.owner) AND lower(n.repo) = lower(v.repo)
        AND lower(n.head_owner) = lower(v.head_owner) AND n.ts <= v.ts
      ORDER BY n.ts DESC, n.call_id DESC LIMIT 1) before_pr ON true
   LEFT JOIN LATERAL (
     SELECT n.number FROM valid n
      WHERE v.number IS NULL AND n.number IS NOT NULL AND n.head_ref = v.head_ref
        AND lower(n.owner) = lower(v.owner) AND lower(n.repo) = lower(v.repo)
        AND lower(n.head_owner) = lower(v.head_owner) AND n.ts > v.ts
      ORDER BY n.ts, n.call_id LIMIT 1) after_pr ON true
 ), grouped AS (
   SELECT ranked.*, COALESCE(pr_number::text, 'ref:' || head_ref) AS target_key
   FROM ranked
 )
 SELECT DISTINCT ON (owner_key, repo_key, target_key)
   CASE WHEN pr_number IS NOT NULL
     THEN 'https://github.com/' || owner || '/' || repo || '/pull/' || pr_number END,
   owner, repo, pr_number,
   COALESCE(head_ref, max(head_ref) OVER w),
   call_id, call_name, kind,
   CASE WHEN bool_or(strength = 1) OVER w THEN 'creator' ELSE 'contributor' END,
   min(ts) OVER w, max(ts) OVER w
 FROM grouped
 WINDOW w AS (PARTITION BY owner_key, repo_key, target_key)
 ORDER BY owner_key, repo_key, target_key, strength, ts, call_id NULLS LAST;
$pr_evidence$;

REVOKE ALL ON FUNCTION sessions.read_session_revision_event_v1(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.pending_session_revision_events_v1(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.ack_session_revision_events_v1(uuid[], bigint[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_pull_request_evidence_v1(text, text, text, text) FROM PUBLIC;
