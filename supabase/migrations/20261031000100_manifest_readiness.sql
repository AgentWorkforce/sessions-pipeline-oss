-- The writer gate for backfill and adopted manifest epochs. Applying this to a
-- real database needs Khaliq's explicit OK; see runbooks/manifest-activation.md.
create table if not exists sessions.manifest_readiness (
  reader text not null,
  capability text not null,
  release_sha text not null,
  attested_at timestamptz not null,
  hmac text not null,
  primary key (reader, capability)
);

create table if not exists sessions.backfill_activation_ready (
  manifest_key text not null,
  grant_id text not null,
  plan_digest text not null,
  ready_at timestamptz not null default now(),
  primary key (manifest_key, grant_id)
);

-- Sessions the exporter could not finish. A row with no resolved_at blocks
-- authoritative coverage: its pre-T0 records are still only on the T0 branch.
create table if not exists sessions.export_deferred_sessions (
  org_id text not null,
  workspace_id text not null,
  manifest_key text not null,
  reason text not null,
  deferred_at timestamptz not null default now(),
  resolved_at timestamptz,
  primary key (org_id, manifest_key)
);
create index if not exists export_deferred_sessions_open_idx
  on sessions.export_deferred_sessions (org_id, workspace_id)
  where resolved_at is null;

-- The legacy machine-claim window: who still uses it, and with which client.
create table if not exists sessions.claimless_uses (
  day date not null,
  org_id text not null,
  surface text not null check (surface in ('status', 'write')),
  caller text not null,
  client_version text not null,
  uses bigint not null default 1,
  last_at timestamptz not null default now(),
  primary key (day, org_id, surface, caller, client_version)
);

-- Service-role only, like every other pipeline table: RLS on with no policy.
ALTER TABLE sessions.manifest_readiness ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.manifest_readiness FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.manifest_readiness TO service_role;
ALTER TABLE sessions.export_deferred_sessions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.export_deferred_sessions FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.export_deferred_sessions TO service_role;
ALTER TABLE sessions.claimless_uses ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.claimless_uses FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.claimless_uses TO service_role;
ALTER TABLE sessions.backfill_activation_ready ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.backfill_activation_ready FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.backfill_activation_ready TO service_role;

-- The new queued stage. The span stages stay allowed where they were.
ALTER TABLE sessions.pipeline_events DROP CONSTRAINT pipeline_events_stage_check;
ALTER TABLE sessions.pipeline_events ADD CONSTRAINT pipeline_events_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'fetch'));
ALTER TABLE sessions.pipeline_dead_letters DROP CONSTRAINT pipeline_dead_letters_stage_check;
ALTER TABLE sessions.pipeline_dead_letters ADD CONSTRAINT pipeline_dead_letters_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'fetch'));
ALTER TABLE sessions.pipeline_jobs DROP CONSTRAINT pipeline_jobs_stage_check;
ALTER TABLE sessions.pipeline_jobs ADD CONSTRAINT pipeline_jobs_stage_check
  CHECK (stage IN ('object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation'));
