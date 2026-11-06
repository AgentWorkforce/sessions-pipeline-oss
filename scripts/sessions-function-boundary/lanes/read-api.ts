/**
 * Lane: read-api (P3b read API directory functions).
 * EMPTY SLOT, filled by PR #236 (feat/p3b-read-api-directory-for) when it restacks on this generator:
 * set `migration` to its 20261105029000_session_directory_read_api.sql, list its three functions
 * (session_directory_read_api_scope owner-only; list_session_directory_for and
 * session_directory_activity_for SECURITY DEFINER, EXECUTE to bulls96_read_api), add the
 * prefixes they live under, and point scripts/sync-read-api-directory-boundary.ts at
 * scripts/sync-sessions-function-boundary.ts.
 */
import type { LaneManifest } from "../types.ts";

export const readApi: LaneManifest = {
  lane: "read-api",
  owner: "#236 (feat/p3b-read-api-directory-for)",
  migration: null,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {},
};
