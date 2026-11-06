-- Plan session_pull_request_evidence_v1 for each call's own session.
--
-- 0035 wrote it in LANGUAGE sql. PostgreSQL 17 plans a non-inlined SQL
-- function's statement once per call with its arguments as unknown
-- parameters, the generic plan, and no plan_cache_mode reaches it. Under that
-- plan the `res` step looks up each PR-relevant call's own tool_result through
-- the (org_id, session_id, workspace_id) btree and filters tool_use_id on
-- every row: the planner multiplies the org, session and workspace
-- selectivities as if independent (org and workspace are 1:1, 129 distinct
-- each; session 14,396), estimates one row per probe, and so prefers that
-- 368 MB index to the 2 GB exact one. Each probe therefore reads the whole
-- session. Production (2026-10-28, PG 17.11, one
-- 326,877-event session), EXPLAIN of the step:
--   generic: inner Index Scan on the (org, session, workspace) btree, rows=1,
--            Filter tool_use_id = c.tool_use_id
--   custom:  inner Index Scan on the 0029 tool_use_id index, tool_use_id in the
--            Index Cond (both sides)
-- Cloud's project-summary-worker calls it per revised session; such reads ran
-- 30-125 s, up to ten at once, waiting on Neon reads and LockManager.
--
-- plpgsql RETURN QUERY with plan_cache_mode = force_custom_plan plans each call
-- with its arguments as constants: the exact (org_id, workspace_id, source,
-- session_id, tool_use_id) probe, and Cloud's non-null workspace and source
-- become index keys of the `calls` step instead of filters. The query is 0035's
-- text unchanged; #variable_conflict use_column keeps every unqualified name
-- that matches an output column (owner, repo, head_ref, ...) the column, as in
-- SQL. Same signature, result columns, volatility, owner, SECURITY DEFINER and
-- search_path; CREATE OR REPLACE keeps the grants. No data, index or lock change.

CREATE OR REPLACE FUNCTION sessions.session_pull_request_evidence_v1(
  p_org_id text, p_workspace_id text, p_source text, p_session_id text
) RETURNS TABLE(pull_request_url text, owner text, repo text, number integer, head_ref text,
  tool_use_id text, tool_name text, evidence_kind text, role text,
  detected_at timestamptz, last_seen_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp
SET plan_cache_mode = force_custom_plan AS $pr_evidence$
#variable_conflict use_column
BEGIN
RETURN QUERY
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
END;
$pr_evidence$;
