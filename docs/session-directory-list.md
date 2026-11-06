# The session list from the session directory

Migration: `supabase/migrations/20261101050000_session_directory_list.sql`.
Rollback: `supabase/rollbacks/20261101050000_session_directory_list.down.sql`.
Tests: `src/hoopdb/__tests__/session-directory-list.test.ts`.

Consumer: relayhistory-cloud `LIST_BACKEND=supabase`
(`docs/list-backend.md` there). It serves `GET /v1/sessions` and
`GET /v1/sessions/activity` without a text query. Text search belongs to
ClickHouse; this list never reads transcript content.

## Dimensions

| dimension | where it lives | written by |
| --- | --- | --- |
| source (harness), latest model, counts, tokens, cost, first/last activity, users, machines | `session_directory` (20261030010000) | projector, from `session_index` |
| project id, repository, branch | `session_directory` | `writeDirectoryCatalog` from the complete raw-record projection; pointer values are a fallback |
| title | `session_directory.title` | projector (pointer title); the brief writer (feat/model-session-brief) projects the model-written title with precedence |
| short summary | `session_directory.short_summary` (20261030020000) | `writeShortSummary` |
| **agent names (several per session)** | **`session_directory.agent_names` + `session_directory_agents`** (new) | **`writeDirectoryCatalog`** |
| **catalog models** | **`session_directory.catalog_models`** (new); the RPC returns them united with the pointer model | `writeDirectoryCatalog` |
| **project key and method, initial commit, originator, agent version** | **`session_directory`** (new columns) | `writeDirectoryCatalog` |
| **parent and child sessions** | **`session_directory_relationships`** (new, parent to child edges); parents and child count are read from the edges | `writeDirectoryCatalog` |

`writeDirectoryCatalog` (`src/hoopdb/session-directory-catalog.ts`) takes a
`DirectoryCatalog`. `directoryCatalogFromProjection` builds one from the rows
that `projectRelayhistorySession` (`packages/pipeline`) already makes out of
the delivered `session` and `relationship` records. That is the same parse that
relayhistory's `project_delivery_catalog_row` performs.

A project key that is a local path is never stored. This covers method `path`,
absolute paths, `~`, drive letters, UNC paths and `file:` paths.

The S3 search-index pass calls `writeDirectoryCatalog` after every complete
logical-session read. A partial, missing, unscrubbed, conflicting, over-limit,
or ClickHouse-truncated pass never clears or updates catalog fields. Historical
sessions still need the controlled re-projection in
`docs/activation/session-directory-parity-reprojection.md`. Until that backfill
runs, old catalog fields and parent/child links may be empty, and the list
**refuses `p_agent`** with `0A000 agent_filter_not_ready` instead of answering
an empty list. relayhistory-cloud falls back to Neon on that error. An operator
unlocks it with
`UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()`
once the backfill is complete.

`writeDirectoryCatalog` is one statement. It locks the live directory row
(`FOR UPDATE`) and joins every write to that lock, so a tombstone either
commits first (nothing is written) or waits and then removes what was written.

## RPCs

Both RPCs resolve the tenant through `sessions.session_directory_worker_scope`.
That means the same role guard and the same signed-claims pins (issuer,
audience, lifetime, workspace bound) as `search_session_directory`. EXECUTE is
granted to `session_directory_search_worker` only.

`sessions.list_session_directory` takes:

- `p_workspace_id`
- `p_sources text[]` (OR), `p_not_sources text[]`
- `p_project_contains`: a literal substring of the project id, project key or
  repository
- `p_user_ids text[]` (OR), `p_agent` (lower, exact)
- `p_last_ts_after` (>=), `p_last_ts_before` (<), `p_first_ts_after` (>=),
  `p_first_ts_before` (<)
- `p_session_harnesses text[]` + `p_session_ids text[]` (a batch lookup by key)
- `p_cursor_last_activity_at`, `p_cursor_session_id`, `p_cursor_harness`
- `p_limit` (1 to 200)

It returns up to `p_limit + 1` rows, newest activity first.

`sessions.session_directory_activity(p_workspace_id, p_since, p_until)` uses
span semantics: a session counts on every UTC day from its first to its last
activity. The window is at most 366 days.

**Ordering.** Every page is ordered, and every cursor compared, on the
directory row's `(last_activity_at, session_id, harness)`. The agent and user
companions are membership filters only. A trigger also keeps every companion's
`last_activity_at` equal to the row's in the row's own transaction, so a stale
companion can neither repeat nor skip a session.

**Erasure.** Triggers on tombstone and on delete remove the session's agent
rows, the edges it delivered as a parent, and the edges live parents hold
naming it as a child.

Plans by filter shape, each pinned by an EXPLAIN test (20,000 tenant rows plus
60,000 other-tenant rows) with a selective value:

| shape | index |
| --- | --- |
| no filter | `session_directory_workspace_recent_idx` |
| one source | `session_directory_harness_recent_idx` (org, workspace, harness, last_activity_at DESC) |
| project substring | `session_directory_tenant_project_text_trgm_idx` (new; trigram over id, key and repo) |
| one or several users | `session_directory_users_recent_idx` (membership probe), then the directory primary key and a sort |
| agent | `session_directory_agents_recent_idx` (new; membership probe), then a sort |
| batch keys | a key index (`session_directory_session_lookup_idx` or the primary key) |

A tenant-prefixed GIN over `user_ids` was tried and dropped. Its
`org_id`/`workspace_id` posting lists cover the whole tenant, and the planner
never picks it.

## Backfill

After the migration, existing rows have empty catalog columns and no agent or
relationship rows. Two paths are possible, and each is an operator step that
needs Khaliq's OK, like any write to a real database:

1. Replay `writeDirectoryCatalog` from a complete projection of each session's
   canonical raw objects in object storage, one call per session.
2. For sessions that only Neon knows, copy from Neon `sessions.session_catalog`,
   `session_catalog_agents` and `session_relationships` (read-only role) into
   `writeDirectoryCatalog` inputs. Neon keeps one agent per session (0058
   primary key).

Until the backfill is done, relayhistory-cloud should stay in `shadow` mode for
the agent and catalog fields.

## Gaps, not in this migration

- **`task_refs` / PR link and `kinds`.** Neither has a projection, so the list
  returns `[]` for both. Cloud's PR badge reads `taskRefs`.
- **Reasoning and cache token totals, cache 5m/1h split.** Not on
  `session_index`.
- **Session catalog detail.** Relationship detail (kind, identity status,
  child agent type/name/model, spawn depth), markers, commit links and the
  first prompt are not carried. relayhistory `GET /v1/sessions/:id/catalog`
  stays on Neon.
- **Exact `project_id` and `no_project`.** Not list RPC parameters yet; the
  search RPC has them.
- **Hybrid search v2 asks (relayhistory-cloud #246).** A title-plus-summary-only
  match mode, separate from the metadata `search_text`, belongs in
  `search_session_directory` and is not here. The brief `generated_at`
  lives in `session_briefs`, owned by feat/model-session-brief. This RPC
  returns `short_summary_generated_at`.
