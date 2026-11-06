-- RelayHistory's Worker writes the shared sessions.machines from T0
-- (docs/relayhistory-supabase-migration.md). Make the table a compatible
-- superset whichever runner created it first. Same key, complementary columns:
-- RelayHistory knows the workspace, hostname, label and upload cursors; the
-- pipeline knows aliases and client_version.
ALTER TABLE sessions.machines
  ADD COLUMN IF NOT EXISTS workspace_id text NOT NULL DEFAULT 'default',
  ADD COLUMN IF NOT EXISTS hostname text,
  ADD COLUMN IF NOT EXISTS label text,
  ADD COLUMN IF NOT EXISTS relayhistory_version text,
  ADD COLUMN IF NOT EXISTS cursors_json jsonb NOT NULL DEFAULT '{}'::jsonb;
-- sessions.upsert_machine does not know the workspace. When RelayHistory created
-- the column (NOT NULL, no default), give it the same default.
ALTER TABLE sessions.machines ALTER COLUMN workspace_id SET DEFAULT 'default';

-- Tenants keep seeing only the pipeline's columns. Upload cursors and
-- RelayHistory's hostname/label stay with the services that own them, and a
-- column added later is private until it is granted here.
REVOKE SELECT ON sessions.machines FROM authenticated;
GRANT SELECT (org_id, machine_id, machine_name, hostname_aliases, os,
              client_version, first_seen_at, last_seen_at)
  ON sessions.machines TO authenticated;
