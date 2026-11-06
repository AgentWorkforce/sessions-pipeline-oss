-- Appended as 20261104020000 to preserve existing upstream ledger prefixes.
-- Upstream provenance: relayhistory-cloud supabase/migrations/20261101000000_auth_sessions.sql.
-- History token auth (`rth_at_*` / `rth_st_*` / `rth_rt_*` hash lookup) on
-- Supabase, so the Worker's hottest Neon read can leave Neon. Code only:
-- applying it to a hosted project needs Khaliq's go.
--
-- The table is a column-for-column copy of Neon's sessions.auth_sessions
-- (packages/relayhistory/db/migrations/0002_neon_convergence.sql plus 0045's
-- analytics_identity), so a row moves between the two unchanged. Tokens are
-- never stored: only their SHA-256 hashes.
--
-- Access. Service-role only: RLS is on with no policy, and anon/authenticated
-- get no grant on the table or the functions. service_role gets SELECT,
-- INSERT and UPDATE (no DELETE, no TRUNCATE: a session ends by revocation,
-- which must leave the row). The Worker reaches it over PostgREST with the
-- service key (src/auth/supabase-store.ts), selected by AUTH_BACKEND.
--
-- Functions.
--   auth_session_resolve: the per-request lookup in ONE round trip. Returns
--     the active (unrevoked) row for an access-token hash and, when the row is
--     unexpired and its last_used_at is at or before p_touch_before, stamps
--     last_used_at. The stamp is telemetry: a failure (for example a read-only
--     transaction) is swallowed and never fails the lookup. NULL
--     p_touch_before never stamps (the dual-mode shadow read).
--   auth_sessions_mirror: upserts whole rows copied from Neon (dual-mode
--     write mirroring and the operator backfill). Newest updated_at wins, and
--     a revocation is sticky: an older or unrevoked copy can never clear
--     revoked_at, so a replayed or out-of-order mirror cannot resurrect a
--     revoked token.

CREATE SCHEMA IF NOT EXISTS sessions;

CREATE TABLE IF NOT EXISTS sessions.auth_sessions (
  id text PRIMARY KEY,
  token_family_id text NOT NULL,
  subject_type text NOT NULL DEFAULT 'cli',
  user_id text NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  scopes jsonb NOT NULL DEFAULT '[]'::jsonb,
  access_token_hash text NOT NULL UNIQUE,
  access_token_expires_at timestamptz NOT NULL,
  refresh_token_hash text NOT NULL UNIQUE,
  refresh_token_expires_at timestamptz NOT NULL,
  label text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz,
  last_refreshed_at timestamptz,
  revoked_at timestamptz,
  revoked_reason text,
  analytics_identity jsonb
);

CREATE INDEX IF NOT EXISTS auth_sessions_family_idx ON sessions.auth_sessions (token_family_id);
CREATE INDEX IF NOT EXISTS auth_sessions_org_idx ON sessions.auth_sessions (org_id);

ALTER TABLE sessions.auth_sessions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.auth_sessions FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON sessions.auth_sessions TO service_role;

CREATE OR REPLACE FUNCTION sessions.auth_session_resolve(
  p_access_token_hash text,
  p_now timestamptz,
  p_touch_before timestamptz DEFAULT NULL
)
RETURNS SETOF sessions.auth_sessions
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  r sessions.auth_sessions;
BEGIN
  SELECT * INTO r FROM sessions.auth_sessions
   WHERE access_token_hash = p_access_token_hash AND revoked_at IS NULL;
  IF NOT FOUND THEN
    RETURN;
  END IF;
  IF p_touch_before IS NOT NULL
     AND r.access_token_expires_at > p_now
     AND (r.last_used_at IS NULL OR r.last_used_at <= p_touch_before) THEN
    BEGIN
      UPDATE sessions.auth_sessions
         SET last_used_at = p_now, updated_at = p_now
       WHERE id = r.id
         AND (last_used_at IS NULL OR last_used_at <= p_touch_before);
    EXCEPTION WHEN OTHERS THEN
      -- Telemetry never decides authorization.
      NULL;
    END;
  END IF;
  RETURN NEXT r;
END
$$;

CREATE OR REPLACE FUNCTION sessions.auth_sessions_mirror(p_rows jsonb)
RETURNS integer
LANGUAGE sql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $$
  WITH src AS (
    SELECT * FROM jsonb_populate_recordset(NULL::sessions.auth_sessions, p_rows)
  ), applied AS (
    INSERT INTO sessions.auth_sessions AS t (
      id, token_family_id, subject_type, user_id, org_id, workspace_id, scopes,
      access_token_hash, access_token_expires_at, refresh_token_hash,
      refresh_token_expires_at, label, created_at, updated_at, last_used_at,
      last_refreshed_at, revoked_at, revoked_reason, analytics_identity)
    SELECT id, token_family_id, subject_type, user_id, org_id, workspace_id,
      scopes, access_token_hash, access_token_expires_at, refresh_token_hash,
      refresh_token_expires_at, label, created_at, updated_at, last_used_at,
      last_refreshed_at, revoked_at, revoked_reason, analytics_identity
      FROM src
    ON CONFLICT (id) DO UPDATE SET
      token_family_id = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.token_family_id ELSE t.token_family_id END,
      subject_type = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.subject_type ELSE t.subject_type END,
      user_id = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.user_id ELSE t.user_id END,
      org_id = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.org_id ELSE t.org_id END,
      workspace_id = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.workspace_id ELSE t.workspace_id END,
      scopes = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.scopes ELSE t.scopes END,
      access_token_hash = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.access_token_hash ELSE t.access_token_hash END,
      access_token_expires_at = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.access_token_expires_at ELSE t.access_token_expires_at END,
      refresh_token_hash = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.refresh_token_hash ELSE t.refresh_token_hash END,
      refresh_token_expires_at = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.refresh_token_expires_at ELSE t.refresh_token_expires_at END,
      label = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.label ELSE t.label END,
      created_at = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.created_at ELSE t.created_at END,
      last_used_at = GREATEST(t.last_used_at, EXCLUDED.last_used_at),
      last_refreshed_at = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.last_refreshed_at ELSE t.last_refreshed_at END,
      analytics_identity = CASE WHEN EXCLUDED.updated_at >= t.updated_at THEN EXCLUDED.analytics_identity ELSE t.analytics_identity END,
      -- Sticky: once revoked, always revoked (first recorded revocation kept).
      revoked_at = COALESCE(t.revoked_at, EXCLUDED.revoked_at),
      revoked_reason = CASE WHEN t.revoked_at IS NOT NULL THEN t.revoked_reason ELSE EXCLUDED.revoked_reason END,
      updated_at = GREATEST(t.updated_at, EXCLUDED.updated_at)
    WHERE EXCLUDED.updated_at >= t.updated_at
       OR (t.revoked_at IS NULL AND EXCLUDED.revoked_at IS NOT NULL)
       OR EXCLUDED.last_used_at > t.last_used_at
       OR (t.last_used_at IS NULL AND EXCLUDED.last_used_at IS NOT NULL)
    RETURNING 1
  )
  SELECT count(*)::integer FROM applied;
$$;

REVOKE ALL ON FUNCTION sessions.auth_session_resolve(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION sessions.auth_sessions_mirror(jsonb)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION sessions.auth_session_resolve(text, timestamptz, timestamptz)
  TO service_role;
GRANT EXECUTE ON FUNCTION sessions.auth_sessions_mirror(jsonb) TO service_role;
