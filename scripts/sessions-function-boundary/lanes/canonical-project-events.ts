/**
 * Lane: canonical-project-events (F10).
 * EMPTY SLOT, filled by PR #237 (codex/f10-canonical-project-events-20261105) when it restacks on this
 * generator: set `migration` to its 20261105027000_canonical_project_events.sql, list its
 * sessions.canonical_project_* functions with their bulls96_project_events / bulls96_session_ingest
 * grants, set prefixes ["sessions.canonical_project_"], and add the bulls96_project_events verifier
 * (supabase/bootstrap/canonical_project_events_verify.sql) to ../verifiers.ts.
 */
import type { LaneManifest } from "../types.ts";

export const canonicalProjectEvents: LaneManifest = {
  lane: "canonical-project-events",
  owner: "#237 (codex/f10-canonical-project-events-20261105)",
  migration: null,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {},
};
