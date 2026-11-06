# Complete event metadata and exact project-name lookup

Migration `20261104021000_session_directory_event_metadata.sql` is additive.
Nothing in this change applies it or activates a reader. Old list/activity RPCs
remain available. Deploy the reviewed function-boundary bootstrap before the
migration, then pipeline code, controlled replay, and strict consumers last.
Readiness is false/version zero for old directory rows until complete replay.

`list_session_directory_events` keeps all existing list arguments and appends
optional `p_project_name text`. It returns the existing response columns plus
`kinds`, `task_refs`, reasoning/cache token totals (including nullable 5m/1h
splits), `work_status`, `work_status_at`, `event_projection_version`,
`event_projection_ready`, `event_projected_at`, `event_projection_epoch`,
`opening_event`, `event_project_event`, `project_requires_hydration`, and
`project_name`. Public users, machines, models and project ID derive from
canonical events, rather than uploader pointer memberships. Those pointer
memberships remain unchanged for ownership and authorization. Event first/last
timestamps are stored separately; strict response/cursors use them while the
pointer's historical timestamps keep their original meaning.

Both locators are content-free objects containing `session_id`, `source`,
`machine_id`, `kind`, `event_id`, exact microsecond `ts`, and the S3 IndexRow
`fingerprint`. The actual event session ID may differ from the directory key.
A NULL opening locator attests no eligible event. Raw prompts and summaries are
never copied into PostgreSQL. A path-valued maximum project ID is not stored:
it has a locator and `project_requires_hydration=true`. Consumers hydrate the
exact newest fingerprint from ClickHouse and recheck the strict directory epoch
before returning text. Project substring filtering refuses a workspace with
path-backed projects rather than silently losing matches.

`p_project_name` is an exact recorded-repository name, across all harnesses:
trim/lower, nonempty, at most 200 characters, no controls or slash/backslash.
Percent and underscore are literal characters. It derives from existing remote
identity helpers and a proven non-path project key, never a cwd basename or
transcript text. Host/owner provenance remains in git_repo/project_key. The
partial btree leads with org/workspace/project_name, then event recency and the
full session/source cursor. Tests EXPLAIN the actual generated statement on
80,000 rows (20,000 target-tenant, 60,000 other-tenant) with a selective name.

`session_directory_event_activity(workspace,since,until)` preserves its original
argument shape, counts actual timestamp existence, and uses inclusive bounds.
The companion timestamp set is deduplicated per logical session, at full
microsecond precision. Sparse days are absent; callers fill zero days. Query
text is unsupported here and remains a separate transcript-search operation.

Strict RPCs attest the entire workspace before filtering or paging. Pointer
mutations atomically dirty a scope marker, including phantom pointers with no
directory row. Indexed dirty/unready probes avoid a full source scan per read.
A complete writer locks source markers and the live directory row, rechecks the
entire canonical sibling source set and mutation epoch, then replaces aggregate
metadata and timestamps atomically. The source markers are monotonic and kept
after completion to avoid ABA. Partial/conflicting/oversized reads never publish
ready data. Tombstones clear metadata and timestamp companions; erased scope
processing clears only its fenced dirty marker.

Canonical IDs still require the shared deterministic layout admission check.
Two distinct physical strings cannot both claim one canonical manifest ID when
that check fails. A conflicting DB alias does not bypass it. Complete metadata
uses all admissible canonical siblings under a canonical lock; CH reconciliation
continues to target its addressed physical scope.

Rollout gates remain open: independent exact-head reviews, CI, deployed schema
and role verification, exhaustive complete replay/source membership proof,
production response parity and query latency. Bounded metadata limits fail closed
rather than truncate. New activity/name indexes require real-deployment plan/SLO
verification; offline fixtures do not establish production latency.
