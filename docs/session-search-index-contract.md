# Logical session directory and search contract

Production uses S3 for canonical session content and Supabase for a rebuildable
metadata index (decision 0002). `sessions.session_index` remains one row per
physical machine manifest and holds the object pointer. The additive
`sessions.session_directory` is one row per logical session:

`(org_id, workspace_id, harness, session_id)`.

That split is required for stable pagination. A session resumed on three machines
has three manifest pointers but must occur once on a session list. The projector
(`src/hoopdb/session-directory-projector.ts`, called by the HoopDb indexer) merges
the physical rows into the directory with the V1 precedence in "Projector precedence"
below. It never sets the rollout marker: the backfill lane does, so the audit keeps
failing closed until then.

Cloud must treat agent lookup as a future RelayHistory-advertised metadata
capability. It must not bind directly to this table or assume an `agent_name` API
field until that capability is implemented. The scalar stored here is the delivered
root agent identity; child-agent relationship labels are not session identities.

User and machine filters use `session_directory_users` and
`session_directory_machines`. The arrays on `session_directory` are response
conveniences, not indexed filter inputs. This avoids GIN-plus-sort plans for
recent-session pages.

## Indexed remote query shapes

| Filter or lookup                     | Index strategy                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| workspace or org recent              | partial ordered btree with a total logical-session cursor                                                                                                                                                                                                                                                                                     |
| harness/source                       | workspace + harness + activity cursor                                                                                                                                                                                                                                                                                                         |
| exact project or no project          | separate partial activity indexes                                                                                                                                                                                                                                                                                                             |
| agent exact or prefix                | exact: normalized name with `text_pattern_ops`, then activity cursor, direct or through the RPC. Prefix: RPC only (the direct surface refuses it, `AgentPrefixRequiresRpcError`), with a candidate budget of 5,000 checked by an ordered walk of `session_directory_agent_recent_idx`; a broader prefix is `agent_prefix_too_broad` (`54000`) |
| created range                        | workspace + `created_at` cursor                                                                                                                                                                                                                                                                                                               |
| state                                | workspace + state + activity cursor                                                                                                                                                                                                                                                                                                           |
| user or machine                      | normalized membership table + activity cursor                                                                                                                                                                                                                                                                                                 |
| session ID without workspace/harness | org + session ID lookup                                                                                                                                                                                                                                                                                                                       |
| project substring                    | `sessions.search_session_directory` RPC only, over tenant-scoped trigram GIN `(org_id, workspace_id, project_id)` (replaces #38's `session_directory_project_trgm_idx`)                                                                                                                                                                       |
| metadata + short summary substring   | `sessions.search_session_directory` RPC only, over tenant-scoped trigram GIN `(org_id, workspace_id, search_text)` (btree_gin)                                                                                                                                                                                                                |

The executable definitions live in
`src/hoopdb/search-index-contract.ts`. `bun run audit:search-indexes` is
read-only: it checks catalog structure and fails until the singleton
`session_directory_rollout.completed_at` is set by the future projector/backfill
lane. It never creates indexes or marks rollout complete.

## Metadata-only search boundary

`search_text` may contain only title, task title, agent name, project, repository,
branch, and model metadata, plus the scrubbed `short_summary` (below), and is searched
only through the tenant-bound RPC. It must never
contain transcript text, event content, tool payloads, prompts, or responses.
Canonical content remains in S3.

### Short summary (G6)

Provenance of scope: the helm-chart-chief coordinator recorded the user's direct
approval to implement bounded short_summary work in its controlling session. Draft
only; applying or merging needs the user's OK on the exact head. Migration:
`20261030020000_session_directory_short_summary.sql`.

- **Bound:** `char_length(short_summary) <= 1000`, a CHECK in the database and the
  same bound (in code points) in the writer. Over-length text is refused, not cut.
- **Scrubbed only:** `src/hoopdb/session-directory-summary.ts` is the only write path.
  It runs the shared `lib/scrub.ts` scrubber, refuses if the scrub throws or is not
  stable on its own output, and records `short_summary_scrub_ruleset` and
  `short_summary_scrub_version` (the raw-chunk `SCRUB_RULESET` pair) plus
  `short_summary_source` and `short_summary_generated_at`. The database requires those
  labels (all or none) but cannot prove the text was scrubbed: only the writer enforces
  that. An older `generated_at` never replaces a newer summary. A refused summary (`ShortSummaryRejected`) clears any older stored summary and its share of `search_text`, under the same fence, so an older summary never stays searchable after a newer one was refused; a refused older summary leaves a newer one in place.
- **Search document:** `sessions.session_directory_search_text(...)` is the one
  definition of `search_text`; the summary is its last field. The writer rebuilds
  `search_text` with it, and the future projector must use the same function.
- **Projector:** `src/hoopdb/session-directory-projector.ts` (`projectSessionDirectory`) is the create/update path. The HoopDb indexer calls it after every pointer upsert and pointer tombstone. Precedence is the V1 contract in "Projector precedence" below. It never sets the rollout marker.
- **Summary producer:** Bob Costas's `summary-short` handler calls `writeShortSummary` after publishing the encrypted object (and `clearShortSummary` for any non-ready status). A refused summary is logged by reason only. If the directory write fails after the pointer committed, every retry that finds the summary current authenticates the published object and republishes it, even when the row already holds a summary (it may be an older revision's text). The `generated_at` fence makes this idempotent. If the pointer has no object, or the object is missing or cannot be authenticated, the directory summary is cleared and the stage fails with the non-retryable `summary_object_integrity`, which the stage queue dead-letters at once. If the store or keyring read throws, the directory summary is cleared first and the (retryable) error is rethrown. Each current-summary replay costs one object-store read plus one decrypt.
- **Deletion:** the HoopDb indexer's pointer-tombstone branch (`src/hoopdb/index.ts`)
  calls `propagatePointerTombstone`, which clears the summary and its share of
  `search_text` at once, then `projectSessionDirectory`. With machines still live the
  projector rebuilds metadata and memberships. With none left it tombstones the row,
  empties `machine_ids`/`user_ids` and deletes the membership rows. A trigger plus CHECK is the backstop for any other writer that
  sets `deleted_at`. Erasure (`eraseSessionDirectory`) deletes rows. PostgreSQL WAL,
  replicas and backups keep their own copies until retention ages them out; that is
  accepted for this bounded, scrubbed field and is why nothing larger goes here.
- **Rollback:** `supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql`
  rebuilds `search_text` without the summary, drops the columns, trigger, functions,
  RPC and tenant index, revokes the Worker role's USAGE on `sessions` (the role itself
  is kept at its bootstrap spec; removing it is a separate operator step in
  `runbooks/session-directory-search-worker-bootstrap.md`), and restores #38's index and
  grants (tested up, down, up).

### The search RPC (pinned contract)

`search_text`, and therefore `short_summary`, is searchable only through this RPC.
#38's non-tenant-scoped `session_directory_metadata_trgm_idx` is dropped and replaced
by `session_directory_tenant_search_trgm_idx` on `(org_id, workspace_id, search_text
gin_trgm_ops)`. `authenticated` keeps RLS-scoped SELECT on the listing columns but not
on `search_text` or `short_summary`, so a PostgREST `search_text=ilike.*q*` table
filter is refused. (Under RLS, ILIKE is not LEAKPROOF and could never use a trigram
index anyway.) The executable copy is `SESSION_DIRECTORY_SEARCH_RPC` and
`SESSION_DIRECTORY_SEARCH_SQL` in `src/hoopdb/search-index-contract.ts`.

| Item           | Contract                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Function       | `sessions.search_session_directory(p_workspace_id, p_query, p_harness, p_project_id, p_project_contains, p_no_project, p_agent, p_agent_match, p_state, p_user_id, p_machine_id, p_since, p_until, p_session_id, p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_cursor_workspace_id, p_limit)`, every argument defaulted (the first two keep their positions). PostgREST `POST /rest/v1/rpc/search_session_directory`, `Content-Profile: sessions`, a JSON body of named `p_*` arguments. `searchRpcRequest` in `src/hoopdb/search-query.ts` builds it under the same rules                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Security       | `SECURITY DEFINER`, `search_path = pg_catalog, sessions, pg_temp`. The statement is assembled only from fixed fragments chosen by which arguments are set; every value is bound with `EXECUTE ... USING` (one custom plan per call). Runs as the migration owner (table owner, RLS not forced); no BYPASSRLS role is created                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Caller         | only `session_directory_search_worker`: `NOLOGIN NOINHERIT NOBYPASSRLS`, exactly one role setting `statement_timeout=2s`, provisioned by the privileged `supabase/bootstrap/session_directory_search_worker.sql` and verified (never created or altered) by the migration, with its exact grants re-checked after every migrate by `supabase/bootstrap/session_directory_search_worker_verify.sql` (runbook `runbooks/session-directory-search-worker-bootstrap.md`), `USAGE` on `sessions`, `EXECUTE` on this function, no table or column privileges. `EXECUTE` is revoked from PUBLIC, `anon`, `authenticated`, `service_role`, `relayhistory_app` and any grantee present at creation (default privileges). A grant added later (a runner's `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions`) is still refused: the function itself requires the caller's role to be the Worker (`42501`). The revoke also strips `EXECUTE` from `postgres` or `supabase_admin` if default privileges granted it to them, so ops tooling must not expect to call the RPC; only the Worker, through PostgREST, calls it. Binding it to `authenticator` and issuing its credential are a separate, reviewed deployment step. That binding (`GRANT session_directory_search_worker TO authenticator`) must keep `authenticator` NOINHERIT; with INHERIT, `has_function_privilege` becomes true for `authenticator` through membership. The Worker must stay NOLOGIN: the function's `session_user` fallback relies on it. **Role guard:** the migration never creates or alters the role. Its first statement requires the role to exist (else `42704`) exactly as the bootstrap makes it: no LOGIN, INHERIT, BYPASSRLS, SUPERUSER, CREATEROLE, CREATEDB or REPLICATION; exactly one role setting, `statement_timeout=2s`; a member of no role; no member except a NOINHERIT `authenticator` without ADMIN OPTION; no owned objects; and no explicit table, column, function, schema, database or default privileges. Otherwise it fails closed with `55000`, naming every violation, before any other DDL runs. The down path does not drop the role (the migration never created it); it revokes what the up granted and leaves the role at its bootstrap spec |
| Tenant         | from the signed JWT claims only (`request.jwt.claims`, as the RLS helpers read them): `role` must equal `session_directory_search_worker`, `app_metadata.org_id` (string) is the org, and `app_metadata.workspace_ids` (array of strings) the workspaces. `p_workspace_id` must be one of them; `NULL` means organization-wide over exactly the claimed workspaces. The caller cannot name an org. `workspace_ids` must hold 1 to 64 unique strings of 1 to 128 non-space, non-control characters; one bad entry refuses the whole claim. `iss` and `aud` must equal the row in `sessions.session_directory_search_token_policy`, which no migration seeds, so search is refused until an environment adds one. `aud` must be a string, `exp - iat` must be 1 to 60 seconds, and `exp` at most 90 seconds away. Signature verification is PostgREST's                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Query          | One rule, implemented identically in `src/hoopdb/search-query.ts` (`normalizeSearchQuery`, `searchRpcRequest`) and `sessions.session_directory_search_query`: NFC; remove every format character (Cf: ZWSP, ZWNJ, ZWJ, BOM, bidi controls, soft hyphen); collapse runs of separators and controls (Z, Cc: NBSP, U+2000-U+200A, U+3000, tabs, newlines) to one space and trim; NFC again; then at least 3 meaningful code points (not a space, not a combining mark M) and at most 200 in total. The character classes are pinned Unicode 17.0 range tables, rendered into the migration from the TypeScript data (a test pins them). Refused input raises `22023` before the search statement runs. `\`, `%`, `_` are escaped so the term is literal; matched as `search_text ILIKE '%term%'`. PostgreSQL `normalize()` uses the server's own Unicode tables, which may be older than 17.0; for code points the server does not know, NFC may differ from the TypeScript result. The category classes are fixed data and do not depend on the server version, so the 3-character floor holds either way                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Order and size | `ORDER BY last_activity_at DESC, session_id DESC, harness DESC` (organization-wide: then `workspace_id DESC`); membership filters order by the membership table's copy of the same key, which the projector keeps equal. `p_limit` 1..50 (default 50); at most `p_limit + 1` rows (51), the extra row meaning "more". Cursor: the last returned row's `(last_activity_at, session_id, harness[, workspace_id])`, all required together; `workspace_id` is required organization-wide and must match `p_workspace_id` otherwise                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Returns        | `org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, task_title, agent_name, project_id, git_repo, git_branch, model, state, event_count, input_tokens, output_tokens, cost_usd_micros, machine_ids, user_ids, short_summary, projected_at`; live rows only                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Errors         | `42501` caller is not the Worker role, or tenant claims missing, malformed, for another role, or not covering the workspace; `22023` an invalid value (text under the meaningful-character rule for `p_query`, `p_project_contains` and a prefix `p_agent`; `p_limit` outside 1..50; unknown state, agent match or harness format; an incomplete or mismatched cursor; `p_since` after `p_until`; `p_no_project` with `p_project_id` or `p_project_contains`); `0A000` an intentionally unsupported combination (below); `57014` the 2 s statement timeout                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

#### Filters and their semantics

Inventory of today's callers (cloud MCP `search_shared_sessions` and `list_session_timings`,
the dashboard list and hybrid search, Skip history and projects, canvas presets, the iPad
summary search, relay's MCP proxy, and RelayHistory #226's `GET /v1/sessions`) is in the
G6 PR. Every list filter they send maps to an argument; text composes with every filter
and with the cursor, and filters AND together.

| Read API filter         | RPC argument                | Semantics                                                                                                                                                                                               |
| ----------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `workspace` / org scope | `p_workspace_id` / `NULL`   | workspace equality, or the claimed workspaces                                                                                                                                                           |
| `q` (`search=metadata`) | `p_query`                   | case-insensitive literal substring of `search_text` (metadata + scrubbed `short_summary`)                                                                                                               |
| `source`                | `p_harness`                 | exact                                                                                                                                                                                                   |
| `project`               | `p_project_id`              | exact                                                                                                                                                                                                   |
| `projectContains`       | `p_project_contains`        | case-insensitive literal substring; ANDed with `project` when both are given                                                                                                                            |
| `noProject`             | `p_no_project`              | `project_id IS NULL`                                                                                                                                                                                    |
| `agent`                 | `p_agent` + `p_agent_match` | `exact` (default, as legacy RelayHistory) or `prefix` (3+ meaningful characters), on the normalized name                                                                                                |
| `state`                 | `p_state`                   | exact                                                                                                                                                                                                   |
| `user`, `machine`       | `p_user_id`, `p_machine_id` | membership; ANDed when both are given                                                                                                                                                                   |
| `since`                 | `p_since`                   | `last_activity_at >= since`                                                                                                                                                                             |
| `until`                 | `p_until`                   | `last_activity_at <= until`, inclusive, compared as `timestamptz` (so any UTC offset in the input is honoured): exactly the legacy direct-list filter (#226's PostgREST `last_activity_at=lte.<until>`) |
| session id              | `p_session_id`              | exact, workspace-scoped or organization-wide                                                                                                                                                            |
| `cursor`, `limit`       | `p_cursor_*`, `p_limit`     | keyset as above                                                                                                                                                                                         |

Intentionally unsupported, returned as `0A000` (never as empty results): an
organization-wide page (`p_workspace_id` `NULL`) with `p_agent`, `p_project_id`,
`p_project_contains`, `p_no_project`, `p_user_id` or `p_machine_id`, because those
indexes lead with the workspace. Not filters here at all: `kind`, `taskRef`,
`noTaskRef`, `tag`, transcript text (`/v1/events?q=`, hybrid ranking) and ordering
other than recency; #226 refuses those with a capability error before the RPC.

Plan evidence (PGlite, 30,000 rows, tenant of 22,500, captured with auto_explain
ANALYZE/BUFFERS/JSON inside the RPC as called by the Worker role): a selective term is
a Bitmap Index Scan on `session_directory_tenant_search_trgm_idx` with no recheck
losses; a common term walks `session_directory_workspace_recent_idx` and stops at 51
rows. Neither plan has a Seq Scan. Buffer and time gates in the test are indicative
for PGlite, not production SLOs; the production benchmark gate above still applies.

### Not in V1 Supabase search

FTS/tsvector, embeddings/pgvector, long summaries, transcripts, and model or reasoning
search belong to the V2 search store (ClickHouse/ObsessionDB), as already decided. G6
adds none of them. Project filters (exact, substring and none) are in V1 because the
current read surface exposes them; the substring is served only through the RPC over
the tenant-scoped project trigram index.

Consequently `search_shared_sessions`, `/v1/events?q=`, and dashboard transcript
search cannot be implemented by silently querying `session_directory`. They remain
unavailable on this data plane until ClickHouse/ObsessionDB is ready. A product may
label a separate metadata search, but it must not claim transcript-search parity.
Do not add a temporary Supabase transcript/tsvector projection based on a claim that
deleting an S3 or per-workspace KMS key crypto-shreds it: PostgreSQL heap pages,
indexes, WAL, replicas, and backups have independent persistence. Transcript storage
requires a recorded, technically viable encryption, deletion, RLS, and backup design
before it can enter this contract.

The legacy relayhistory-cloud `0067_session_catalog_agent_lookup.sql` projection
and its `session_catalog_agents` index are not the production contract. Its
normalization and ordered-prefix lessons remain valid; its event-triggered catalog
projection and legacy rollout marker do not.

## Agent prefix: RPC only, with a candidate budget

Exact lookup accepts a complete normalized name, on the direct list surface or through
the RPC. Direct exact input must be ASCII (`NonAsciiAgentFilterError` otherwise).

Prefix lookup is served only by `sessions.search_session_directory`
(`p_agent_match => 'prefix'`). On the direct surface every prefix is a typed
`AgentPrefixRequiresRpcError` (`directAgentPrefix`), raised before any SQL, and there is
no direct prefix query. Under RLS the direct path could not bound a broad prefix: its
bounds and LIKE recheck are estimated as independent, and PostgreSQL 15 and 17 chose a
Seq Scan of the whole tenant (`docs/evidence/session-directory-plans.md`).

In the RPC, a prefix needs at least three meaningful characters (`22023` otherwise).
Before the page runs, the RPC counts the prefix's candidates, stopping after 5,001:

- the count is an ordered walk of `session_directory_agent_recent_idx` (an Index Only
  Scan; seqscan, bitmapscan and sort are disabled for that one statement and restored
  after);
- more than `AGENT_PREFIX_CANDIDATE_BUDGET` (5,000 = page size 50 x the 100x gate) is
  refused as `agent_prefix_too_broad` (`54000`), asking for a longer prefix or an exact
  agent;
- so no prefix, however broad, reads more than 5,001 index entries or scans the tenant.
  Raising the minimum length alone would not bound it.

The release evidence still records p50/p95 and plans on a production-shaped
population. The budget, not a test host's speed, is the bound.

## Direct surface: expand then contract

`authenticated` can still read `project_id` directly, and PostgREST lets a role filter
on any column it can SELECT. So a direct `project_id=ilike.*v*` is possible, and under
RLS it can never use an index: `texticlike` and `textlike` are not LEAKPROOF. No
cosmetic index is added. The surface closes in two phases.

**Expand (in #38, additive only)**

- Migration `20261031001000_session_directory_reads_expand.sql`.
- Project substring is already served by the RPC (`p_project_contains`, tenant project
  trigram index inside the RPC identity).
- `sessions.session_directory_pointers(p_workspace_id, p_harness, p_session_id)`
  returns exactly RelayHistory #226's 13 pointer columns for live pointers in a claimed
  workspace, ordered by `machine_id`, at most 64 rows.
- `sessions.session_directory_readiness()` returns only the rollout marker's
  `completed_at`.
- Both are SECURITY DEFINER with a pinned `search_path` and fixed SQL. The tenant
  comes from the signed claims (`42501` otherwise), through
  `sessions.session_directory_worker_scope`, which applies the search RPC's token pins
  too: the `session_directory_search_token_policy` issuer and audience, the lifetime
  bound, and 1 to 64 valid workspace ids. EXECUTE goes to
  `session_directory_search_worker` only, and both apply the same call-time role guard.
- `sessions.session_directory_resurrection_fences` and its trigger implement the
  decision 0002 resurrection rule (see "Projector precedence").
- Nothing is revoked. `short_summary` is unchanged. There is no `service_role` fallback.

**Contract (later, separate, not applied here)**

- Prerequisites: #226 cuts over to the RPCs behind a flag with shadow parity, and
  logs prove zero direct reads over an agreed window.
- Then a separate forward migration revokes direct SELECT on `session_directory`,
  `_users`, `_machines`, `session_index` and `session_directory_rollout` from
  `authenticated` and from the static reader's role.
- The plan and the consumer inventory are in #87
  (`docs/session-directory-consumers.md`).

## Projector precedence (V1)

For a logical session whose machine manifests disagree, `projectSessionDirectory`
applies, and `session-directory-precedence.test.ts` pins:

- **created_at:** stable and historical: the earliest `created_at` ever projected. It
  never moves later, not even when the earliest pointer is tombstoned.
- **Scalars and counters:** the latest live pointer wins title, project, repository,
  branch, model, state, event and token counts and cost, without summing across
  machines. `last_activity_at` is that pointer's.
- **Brief title:** a model-written brief title (`sessions.session_briefs`, written by
  the session-brief worker, `src/session-brief/`) wins over the latest pointer's title
  while the brief exists. Any pointer tombstone or delete removes the brief (trigger), so
  the pointer title returns on the next projection. With briefs off no row exists and
  this rule changes nothing.
- **Short summary producers:** `writeShortSummary` never lets a non-brief producer
  (Bob Costas's extractive summary) replace a `session-brief/*` summary; the brief
  replaces any summary not newer than it. Clearing (deletion, quarantine) still clears it.
- **Deterministic ties:** latest `last_activity_at`, then highest `revision`, then
  highest `machine_id`.
- **Memberships:** `machine_ids`, `user_ids` and the membership tables are the union of
  LIVE pointers. Each membership row carries the directory row's `last_activity_at`.
- **Tombstone:** when no live pointer remains. The trigger clears `short_summary` and
  `search_text`, and memberships are deleted.
- **Resurrection:** only for a genuinely new pointer revision (helm-chart-chief's
  ruling on decision 0002, below):
  - When a row becomes a tombstone, the trigger `session_directory_record_fences`
    records every pointer's highest revision in
    `session_directory_resurrection_fences` (expand migration `20261031001000`).
  - The row comes back only for a live pointer whose revision is strictly above its
    machine's fence, or from a machine with no fence. It returns with the historical
    `created_at` and no summary until Bob Costas writes one.
  - Re-sent, stale or un-deleted pointer revisions stay fenced permanently. Fences only
    rise, and erasure deletes them with the row.
- **Unchanged:** `task_title`, `agent_name` and `short_summary` are kept as projected.

Provenance of the approval, recorded verbatim so it can be judged and reverted:

- In helm-chart-chief's session, helm-chart-chief asked: "I recommend stable historical
  created_at, latest-live scalar/counter winner without summing, union of live
  memberships, tombstone when none remain, and resurrection when a later live pointer
  appears. Approve that V1 behavior?"
- Khaliq's next message was: "keep going".
- It was relayed to this lane by the 96-no-bull-migration coordinator. The
  implementing agent did not see that session firsthand.

Decision 0002 and resurrection (helm-chart-chief's ruling):

- A deleted upload or pointer never comes back. Its revision is fenced, so un-deleting it,
  re-sending it, or sending an older or equal revision cannot revive anything.
- A logical session may be resurrected, but only by a genuinely new live pointer with a
  strictly newer admissible revision (a new upload after the deletion).

`session-directory-precedence.test.ts` covers all three: a deleted pointer never
revives, a stale or equal revision cannot, and a new higher revision (or a machine with
no prior pointer) can. The rollout marker is still set only by the backfill lane.

## Dimensions that remain local

Relay Desktop also filters local path/directory, upload state, relay presence,
reasoning, and live active/inactive state. Those are local catalog/runtime
dimensions and do not belong in Supabase. Model is searchable metadata, but there
is no remote ordered model-filter contract yet. Adding an API filter requires a
query-plan test before adding another write-amplifying index.
