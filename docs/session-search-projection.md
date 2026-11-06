# V1 session search projection in Supabase

Khaliq, 2026-10-30. Supabase holds the pointers plus filterable, indexed
session metadata. That covers short summaries, agent name, projects and
session state, together with the other fields people filter and sort on.
Full transcript text stays in S3, and V1 transcript search uses a ClickHouse
text index (ClickHouse or ObsessionDB). This doc maps every filter and sort Relay Desktop offers to a
column and index. It builds on `session_index` and the logical
`session_directory` (agent name, draft #38) rather than adding a third table.

## Where each Desktop filter lives

The filters come from `relay-desktop`
`probe/src/linux_app/models/session_query.rs` (`matches`); the sorts from
`probe/src/relay/session_query.rs` (`SORTS`).

| Desktop filter or sort | Server? | Column | Index |
| --- | --- | --- | --- |
| `agent:` / `@name` (exact, prefix, contains) | yes | `session_directory.agent_name` (#38) | #38: `session_directory_agent_recent_idx` (normalized `text_pattern_ops` plus activity cursor) for exact and prefixes of 3+ chars; contains through `session_directory_metadata_trgm_idx` over a bounded `search_text` |
| `title:` (contains) and sort "session" | yes | `session_index.title` | trigram GIN on `lower(title)` per org; recency keyset for the sort |
| short summary (free-text search) | yes | `short_summary` on `session_directory`: **approved by Khaliq on 2026-10-30, pending the coordinator's record** (at most about 1,000 chars, scrubbed); built by the G6 owner (sp-w6-supabase-schema) | trigram GIN and Postgres FTS, each behind a query-plan test, not claimed until that test exists |
| `project:` (contains; exact id) and sort | yes | `project_id` (exact), `project_name` (new) | `si_project` (exact, exists); trigram on `lower(project_name)` |
| `harness:` / `source:` (exact) and sort | yes | `harness` | `si_harness` (exists) |
| session state (`active`, `done`, `abandoned`) and sort "status" | yes | `state` (Referee) | `si_state` (exists, for not-done); add a done-inclusive keyset if done must be listed by state |
| `updated:` / `after:` / `before:` and sort "updated" | yes | `last_activity_at` | every `si_*_recent` keyset (exists) |
| sort "created" | yes | `created_at` | new keyset (org, workspace, created_at DESC, …) |
| sort "model" | yes | `model` | new (org, workspace, model, recency) |
| sort "reasoning" | yes | `reasoning_effort` (new, enum-like text) | none until a server query needs it (contract rule) |
| `id:` (contains) and sort "sessionID" | yes | `session_id` | `si_session` for exact; contains stays client-side |
| free text (title, source, project, directory, id, agent) | partly | OR across `title`, `short_summary`, `project_name`, `agent_name` | the trigram indexes above (BitmapOr) |
| `path:` and sort "directory" | **no** | none. Only `cwd_sha256` is stored, because plaintext paths leak usernames and projects | Desktop-local only |
| `is:active` / `inactive` / `on-relay` / `off-relay`, and sort "relay" | **no** | live relay state on the desktop | Desktop-local only |
| `upload:` and sort "upload" | **no** | the desktop's upload state | Desktop-local only |

## Rules

- **Tenancy:** every index leads with `org_id` (then `workspace_id` where the
  query is workspace-scoped), and RLS applies to every column above. Desktop
  and Cloud never query across orgs.
- **Short summaries:** produced by Bob Costas from the S3 content, scrubbed,
  and length-capped. No embeddings or pgvector in Supabase in V1;
  transcript search is ClickHouse/ObsessionDB's (V1).
- **Erasure:** row deletion removes a workspace's projection rows (tombstone
  driven); backups and PITR age out on Supabase retention. Key-deletion
  shredding covers only S3 (see 0002; decision deferred to
  sessionspipelinegrok).
- **No index is added without a server query.** This follows
  `docs/session-search-index-contract.md`, and it applies to the "reasoning"
  index and the done-inclusive state keyset.
- **Desktop-local dimensions** (path, relay link, upload state) never reach
  Supabase: they stay in the desktop's SQLite projection.

## Work

1. **Migration:** add `short_summary`, its `tsvector`, `project_name` and
   `reasoning_effort` to `session_index`, plus the trigram, FTS, created and
   model indexes. Large-table indexes are built `CONCURRENTLY` per the
   contract.
2. **Index stage:** fill `title`, `model`, `project_name`, `reasoning_effort`
   from the manifest extensions or normalized output, and `short_summary`
   from Bob Costas.
3. **Coordinate with #38:** `agent_name` and its exact, prefix and contains
   indexes live on `session_directory`.
4. **Audit:** add the new shapes to `search-index-contract.ts`, so the
   readiness audit enforces them.
