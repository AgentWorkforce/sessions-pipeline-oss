# supabase/migrations

**Status (2026-11-02):** these migrations are applied to **production Supabase**
(`abcdefghijklmnopqrst`) through `src/hoopdb/migrate.ts`, the only migration authority. The
generation there is `eff8b3be…`, with 18/18 ledger rows in `sessions.__96bulls_migrations`, the
readiness marker at `pass`, and `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger`. The record is
`docs/activation/supabase-incremental-upgrade-1d99ccc8-to-eff8b3be.md` §10. Never apply them with
`supabase db push`: the migrator refuses a mixed authority (`mixed_migration_authority`). Any
further apply to a real database needs Khaliq's OK under AGENTS.md.

Tests also run every file here in the in-process PGlite inside `packages/pipeline` vitest, and in
the real-PostgreSQL proof (`.github/workflows/pg-proof.yml`).

## Provenance

| File                                                 | Source                                                                               |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `20261025000600_pipeline_queue.sql`                  | relayhistory-cloud PR #210 (the framework this repo moved in)                        |
| `20261025000000_sessions_index.sql`                  | relayhistory-cloud PR #171, merge commit `51c313fda3c4288cf75f9927c4004fb39b6cd2ee`  |
| `20261025000100_repo_identity.sql`                   | relayhistory-cloud PR #177 (`7f49063`)                                               |
| `20261025000200_scrub_quarantine.sql`                | relayhistory-cloud PR #185 (`fe89fc3`)                                               |
| `20261025000300_upsert_keep_on_null.sql`             | relayhistory-cloud PR #185 (`fe89fc3`)                                               |
| `20261030010000_session_directory.sql`               | 96BullSessions decision 0002 logical-session metadata contract                       |
| `20261030020000_session_directory_short_summary.sql` | 96BullSessions G6: bounded, scrubbed `short_summary` and the tenant-bound search RPC |
| `20261102000000_session_briefs.sql`                 | 96BullSessions: model-written session briefs (neon-retire), the debounced wakeups and the Worker brief RPC |
| `20261102020000_session_brief_state.sql`            | 96BullSessions: the brief's state (session-summary-v3), returned by the Worker brief RPC |
| `../tests/supabase_shim.sql`                         | relayhistory-cloud PR #171, merge commit `51c313fda3c4288cf75f9927c4004fb39b6cd2ee`  |

Repository: https://github.com/AgentWorkforce/relayhistory-cloud. The `000000` to
`000300` files and `supabase_shim.sql` are verbatim copies of the blobs at
`8603c141bf7dfcd67687db1e5f02e48c1eb2ef61` (the `sp/pipeline-framework` branch of
PR #210, which sits on main `248fe67`, after #171, #177 and #185). They are byte
identical at the PR #211 head too. Do not edit them here; change them upstream.

## Why they are here

`20261025000600_pipeline_queue.sql` alters `sessions.pipeline_events` and
`sessions.pipeline_dead_letters` and creates `sessions.pipeline_jobs` in the
`sessions` schema. All of that comes from `20261025000000_sessions_index.sql`. The
`000100` to `000300` files come before `000600` on relayhistory-cloud main, and the
TypeScript tests apply every file here in name order after
`supabase/tests/supabase_shim.sql` (which stubs the Supabase roles and `auth`
functions). So these files are dependencies of `000600`, vendored so the tests run.

`20261030010000_session_directory.sql` is owned by this repository rather than
vendored. The migration itself creates no projector, backfill, deployment, or data write.
(It was applied to production Supabase on 2026-11-01; see the status above.)

`20261030020000_session_directory_short_summary.sql` is also owned here. Provenance
of scope: the helm-chart-chief coordinator recorded the user's direct approval to
implement bounded short_summary work in its controlling session. Applied to production
Supabase on 2026-11-01 (see the status above). Its down
path is `../rollbacks/20261030020000_session_directory_short_summary.down.sql`, kept
out of this directory because the tests apply every file here in name order. The
migration needs the `btree_gin` extension (available on Supabase) for its
tenant-scoped trigram index. It does not create the NOLOGIN role
`session_directory_search_worker`: the privileged, idempotent
`../bootstrap/session_directory_search_worker.sql` provisions it first, and the
migration verifies it exactly or fails closed before any DDL. After all migrations,
`../bootstrap/session_directory_search_worker_verify.sql` checks its exact grants on every deploy
(runbook:
`runbooks/session-directory-search-worker-bootstrap.md`). The tests run the shim, then
the bootstrap, then every migration; PGlite tests load `btree_gin` alongside `pg_trgm`.
