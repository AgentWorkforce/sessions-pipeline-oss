-- When the legacy machine-claim window first opened. There is one identity per
-- compat class for the whole database, not one per deployment: a new deployment
-- label must not open a new window, so stages sharing a database share it. The
-- row is written once and never changes, so the window's dates cannot be
-- restated. Applying this to a real database needs Khaliq's explicit OK; see
-- runbooks/manifest-activation.md.
create table sessions.claimless_window_activations (
  compat_class text primary key
    check (compat_class = 'manifest_machine_claim_compat_v1'),
  first_activation timestamptz not null,
  recorded_at timestamptz not null default now()
);

create function sessions.claimless_window_activations_append_only()
returns trigger language plpgsql as $$
begin
  raise exception 'claimless_window_activations is append-only';
end;
$$;

create trigger claimless_window_activations_append_only
  before update or delete on sessions.claimless_window_activations
  for each row execute function sessions.claimless_window_activations_append_only();

create trigger claimless_window_activations_no_truncate
  before truncate on sessions.claimless_window_activations
  for each statement execute function sessions.claimless_window_activations_append_only();

alter table sessions.claimless_window_activations enable row level security;
revoke all on sessions.claimless_window_activations from public, anon, authenticated;
grant select, insert on sessions.claimless_window_activations to service_role;
