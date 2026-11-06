/**
 * Consumer-driven metadata index contract for decision 0002.
 *
 * Canonical transcript/event content lives in object storage. This contract only
 * covers the rebuildable Supabase logical-session directory and its metadata.
 * Transcript search belongs to ClickHouse/ObsessionDB and MUST NOT be added to
 * session_directory.search_text. The one derived-content exception is the bounded,
 * scrubbed short_summary (G6). The helm-chart-chief coordinator recorded the user's
 * direct approval to implement bounded short_summary work in its controlling session.
 */
export type SearchIndexRequirement = {
  id: string;
  indexName: string;
  tableName: string;
  consumers: readonly string[];
  queryShape: string;
  definitionFragments: readonly string[];
  reloptionFragments?: readonly string[];
  expectedPredicate?: string;
  requiresDirectoryProjection?: boolean;
};

/** Prefixes shorter than this are too broad for the ordered btree contract. */
export const MIN_AGENT_PREFIX_CHARS = 3;

/** Deterministic gate for production-shaped prefix benchmarks. */
export const AGENT_PREFIX_BENCHMARK_GATE = {
  pageSize: 50,
  maxIndexRowsExaminedPerResult: 100,
} as const;

export const SESSION_DIRECTORY_SEARCH_BOUNDARY = {
  metadataOnly: true,
  fields: [
    "title",
    "task_title",
    "agent_name",
    "project_id",
    "git_repo",
    "git_branch",
    "model",
    // Scrubbed, <= 1000 characters, written only by session-directory-summary.ts.
    "short_summary",
  ],
  excluded: [
    "transcript content",
    "event content",
    "tool payloads",
    "long summary",
    "embeddings",
  ],
  transcriptBackend: "ClickHouse/ObsessionDB (not yet available)",
} as const;

/**
 * A direct agent filter whose input the JavaScript normalization cannot match to the
 * stored PostgreSQL lower(btrim(agent_name)) exactly; use the RPC instead.
 */
export class NonAsciiAgentFilterError extends RangeError {
  constructor(readonly match: "exact" | "prefix") {
    super(
      `agent ${match === "exact" ? "name" : "prefix"} contains non-ASCII characters: ` +
        "the direct query normalizes it in JavaScript, which can disagree with " +
        "PostgreSQL lower() on the stored name, so it could miss rows. Use " +
        `sessions.search_session_directory with p_agent_match => '${match}'.`,
    );
    this.name = "NonAsciiAgentFilterError";
  }
}

// eslint-disable-next-line no-control-regex
const NON_ASCII = /[^\u0000-\u007f]/;

/**
 * The JavaScript normalization of a direct agent filter (exact or prefix): trim and
 * lowercase. It accepts ASCII only, where it equals PostgreSQL lower(btrim()) on every
 * collation; any other input throws NonAsciiAgentFilterError rather than risk a
 * silent false negative.
 */
export function normalizeAgentName(value: string): string {
  if (NON_ASCII.test(value)) throw new NonAsciiAgentFilterError("exact");
  return value.trim().toLowerCase();
}

/** The `$3` of SESSION_DIRECTORY_QUERIES.agentExact: ASCII only, non-empty. */
export function agentExactValue(value: string): string {
  const normalized = normalizeAgentName(value);
  if (normalized === "" || normalized.length > 200) {
    throw new RangeError("agent name must be 1 to 200 characters");
  }
  return normalized;
}

/**
 * Agent prefix search is RPC-only (helm-chart-chief ruling on #38). The tenant-bound RPC
 * first counts the prefix's candidates, stopping after this budget + 1, and refuses a
 * broader prefix as `agent_prefix_too_broad` (SQLSTATE 54000) instead of reading the
 * whole tenant. It equals the page size times the row-amplification gate.
 */
export const AGENT_PREFIX_CANDIDATE_BUDGET =
  AGENT_PREFIX_BENCHMARK_GATE.pageSize *
  AGENT_PREFIX_BENCHMARK_GATE.maxIndexRowsExaminedPerResult;

/** A direct (authenticated, PostgREST table) agent prefix: never served directly. */
export class AgentPrefixRequiresRpcError extends RangeError {
  readonly code = "agent_prefix_requires_rpc";
  constructor() {
    super(
      "agent prefix search is served only by sessions.search_session_directory " +
        "(p_agent_match => 'prefix'); the direct list surface takes exact agent names only",
    );
    this.name = "AgentPrefixRequiresRpcError";
  }
}

/**
 * The direct list surface's answer to every agent prefix: a typed requires-RPC refusal.
 * It runs no SQL. Exact agent filters stay direct (agentExactValue).
 */
export function directAgentPrefix(value: string): never {
  void value;
  throw new AgentPrefixRequiresRpcError();
}

/**
 * Whether `value` is long enough to send to the RPC as an agent prefix (3 or more
 * characters after trim). A predicate: it never throws. Breadth is decided by the
 * RPC's candidate budget, not here.
 */
export function isUsefulAgentPrefix(value: string): boolean {
  return Array.from(value.trim()).length >= MIN_AGENT_PREFIX_CHARS;
}

/**
 * Columns exposed by the authenticated metadata-list surface. Keep this list in
 * lockstep with the column grant in the short-summary migration: search_text,
 * short_summary and its provenance are deliberately available only through the
 * tenant-bound RPC.
 */
export const SESSION_DIRECTORY_LIST_COLUMNS = [
  "org_id",
  "workspace_id",
  "harness",
  "session_id",
  "created_at",
  "last_activity_at",
  "title",
  "task_title",
  "agent_name",
  "agent_name_normalized",
  "project_id",
  "git_repo",
  "git_branch",
  "model",
  "state",
  "event_count",
  "input_tokens",
  "output_tokens",
  "cost_usd_micros",
  "machine_ids",
  "user_ids",
  "projection_version",
  "projected_at",
  "deleted_at",
] as const;

const listColumns = SESSION_DIRECTORY_LIST_COLUMNS.join(", ");
const joinedListColumns = SESSION_DIRECTORY_LIST_COLUMNS.map(
  (column) => `d.${column}`,
).join(", ");

/** Parameterized definitions shared by plan tests and future API adapters. */
export const SESSION_DIRECTORY_QUERIES = {
  workspaceRecent: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $3`,
  harnessRecent: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC
 LIMIT $4`,
  projectRecent: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND project_id = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  withoutProjectRecent: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND project_id IS NULL AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $3`,
  agentExact: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2
   AND agent_name_normalized = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  createdRange: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND created_at >= $3
   AND created_at < $4 AND deleted_at IS NULL
 ORDER BY created_at DESC, session_id DESC, harness DESC
 LIMIT $5`,
  stateRecent: `
SELECT ${listColumns} FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND state = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  userRecent: `
SELECT ${joinedListColumns} FROM sessions.session_directory_users AS membership
 JOIN sessions.session_directory AS d USING (org_id, workspace_id, harness, session_id)
 WHERE membership.org_id = $1 AND membership.workspace_id = $2
   AND membership.user_id = $3 AND d.deleted_at IS NULL
 ORDER BY membership.last_activity_at DESC, membership.session_id DESC, membership.harness DESC
 LIMIT $4`,
  machineRecent: `
SELECT ${joinedListColumns} FROM sessions.session_directory_machines AS membership
 JOIN sessions.session_directory AS d USING (org_id, workspace_id, harness, session_id)
 WHERE membership.org_id = $1 AND membership.workspace_id = $2
   AND membership.machine_id = $3 AND d.deleted_at IS NULL
 ORDER BY membership.last_activity_at DESC, membership.session_id DESC, membership.harness DESC
 LIMIT $4`,
} as const;

/**
 * The tenant-bound search and list RPC: the only path over search_text (and so
 * short_summary), and the V1 directory path for every filter the RelayHistory read
 * surface exposes. Pinned contract for RelayHistory #226; changing any of it is a
 * contract change. Call through PostgREST as `POST /rest/v1/rpc/search_session_directory`
 * with `Content-Profile: sessions`, named `p_*` arguments, and a JWT whose `role` is the
 * Worker role and whose `app_metadata` carries `org_id` and `workspace_ids`.
 * `searchRpcRequest` (search-query.ts) builds a request under the same rules.
 */
export const SESSION_DIRECTORY_SEARCH_RPC = {
  name: "sessions.search_session_directory",
  args: {
    p_workspace_id:
      "text, NULL = organization-wide over the claimed workspaces",
    p_query:
      "text: substring of search_text (metadata + scrubbed short_summary)",
    p_harness: "text: exact harness (the read API's `source`)",
    p_project_id: "text: exact project",
    p_project_contains: "text: case-insensitive project substring",
    p_no_project: "boolean: sessions without a project",
    p_agent: "text: normalized agent name",
    p_agent_match: "'exact' (default) | 'prefix'",
    p_state: "'active' | 'done' | 'abandoned' | 'unknown'",
    p_user_id: "text: user membership",
    p_machine_id: "text: machine membership",
    p_since: "timestamptz: last_activity_at >= since",
    p_until:
      "timestamptz: last_activity_at <= until (inclusive; the legacy direct-list filter)",
    p_session_id: "text: exact session id",
    p_cursor_last_activity_at: "timestamptz",
    p_cursor_session_id: "text",
    p_cursor_harness: "text",
    p_cursor_workspace_id:
      "text: required for organization-wide pages, else equal to p_workspace_id or NULL",
    p_limit:
      "integer 1..50 (default 50); the RPC returns up to p_limit + 1 rows",
  },
  workerRole: "session_directory_search_worker",
  claims: {
    role: "session_directory_search_worker",
    orgId: "app_metadata.org_id",
    workspaceIds: "app_metadata.workspace_ids",
  },
  /** `iss` comes from sessions.session_directory_search_token_policy, per environment. */
  audience: "session-directory-search",
  maxTokenLifetimeSeconds: 60,
  /** A larger membership is split across tokens by the caller, which merges the pages. */
  maxClaimedWorkspaces: 64,
  maxWorkspaceIdChars: 128,
  /** Meaningful characters after normalizeSearchQuery: not separators, format characters or marks. */
  minQueryChars: 3,
  maxQueryChars: 200,
  minAgentPrefixChars: 3,
  defaultLimit: 50,
  maxLimit: 50,
  /** At most maxLimit + 1 rows: a full page plus one row that signals more. */
  maxRows: 51,
  order:
    "last_activity_at DESC, session_id DESC, harness DESC (organization-wide: then workspace_id DESC)",
  cursor:
    "(last_activity_at, session_id, harness[, workspace_id]) of the last returned row; strictly older rows follow",
  returns: [
    "org_id",
    "workspace_id",
    "harness",
    "session_id",
    "created_at",
    "last_activity_at",
    "title",
    "task_title",
    "agent_name",
    "project_id",
    "git_repo",
    "git_branch",
    "model",
    "state",
    "event_count",
    "input_tokens",
    "output_tokens",
    "cost_usd_micros",
    "machine_ids",
    "user_ids",
    "short_summary",
    "projected_at",
  ],
  errors: {
    "42501":
      "caller is not the Worker role, or tenant claims missing, malformed, for another role, from another issuer or audience, too long-lived, over the workspace bound, or not covering the workspace",
    "22023":
      "an invalid value: text under the meaningful-character rule, limit, state, agent match, harness, cursor, since > until, or a contradictory pair",
    "0A000": "an intentionally unsupported combination (see unsupported)",
    "54000":
      "agent_prefix_too_broad: more than AGENT_PREFIX_CANDIDATE_BUDGET (5000) sessions match the agent prefix; use a longer prefix or an exact agent",
    "57014": "statement_timeout (2s on the Worker role) exceeded",
  },
  /** Returned as 0A000, never as empty results. */
  unsupported: [
    "organization-wide (no p_workspace_id) with p_agent, p_project_id, p_project_contains, p_no_project, p_user_id or p_machine_id",
    "transcript/event text search (V2 search store)",
    "ordering other than recency",
  ],
  /** Returned as 22023. */
  contradictory: [
    "p_no_project with p_project_id or p_project_contains",
    "p_since later than p_until",
  ],
} as const;

/**
 * Expand-phase replacements for RelayHistory #226's remaining direct reads
 * (20261031001000_session_directory_reads_expand.sql). Same rules as the search RPC:
 * SECURITY DEFINER, pinned search_path, fixed SQL, Worker role only, tenant from the
 * signed claims (42501 otherwise). The direct grants they replace are revoked only in a
 * later, separate contract step.
 */
export const SESSION_DIRECTORY_POINTERS_RPC = {
  name: "sessions.session_directory_pointers",
  args: {
    p_workspace_id: "text (must be claimed)",
    p_harness: "text",
    p_session_id: "text",
  },
  /** Exactly RelayHistory #226's POINTER_COLUMNS. */
  returns: [
    "org_id",
    "workspace_id",
    "harness",
    "session_id",
    "machine_id",
    "user_id",
    "storage_provider",
    "bucket",
    "manifest_key",
    "content_sha256",
    "raw_format",
    "revision",
    "deleted_at",
  ],
  order: "machine_id",
  maxRows: 64,
  errors: {
    "42501":
      "not the Worker role, or claims missing/invalid/not covering the workspace",
    "22023": "missing workspace, invalid harness or session id",
  },
} as const;

export const SESSION_DIRECTORY_READINESS_RPC = {
  name: "sessions.session_directory_readiness",
  args: {},
  returns: ["completed_at"],
  errors: { "42501": "not the Worker role, or claims missing/invalid" },
} as const;

/**
 * The model-written brief for one logical session (20261102000000; `state` from
 * 20261102020000), for relayhistory-cloud's
 * GET /v1/sessions/:id/brief. Zero rows when the session has no live pointer.
 */
export const SESSION_BRIEF_RPC = {
  name: "sessions.session_brief",
  args: {
    p_workspace_id: "text (must be claimed)",
    p_harness: "text",
    p_session_id: "text",
  },
  returns: [
    "org_id",
    "workspace_id",
    "harness",
    "session_id",
    "status",
    "stale",
    "title",
    "description",
    "state",
    "revision",
    "current_revision",
    "prompt_version",
    "engine_version",
    "model",
    "message_count",
    "shortened_messages",
    "input_tokens",
    "output_tokens",
    "error",
    "generated_at",
    "attempted_at",
  ],
  statuses: [
    "ready",
    "pending",
    "refreshing",
    "stale",
    "failed",
    "rate_limited",
    "missing",
  ],
  maxRows: 1,
  errors: {
    "42501":
      "not the Worker role, or claims missing/invalid/not covering the workspace",
    "22023": "missing workspace, invalid harness or session id",
  },
} as const;

/**
 * The exact statement the RPC executes for a workspace-scoped text search with no other
 * filter (whitespace aside). Plan tests compare auto_explain's Query Text to it; other
 * shapes add the fixed fragments listed in the migration.
 */
export const SESSION_DIRECTORY_SEARCH_SQL = `
SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,
 d.title, d.task_title, d.agent_name, d.project_id, d.git_repo, d.git_branch, d.model, d.state,
 d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,
 d.machine_ids, d.user_ids, d.short_summary, d.projected_at
 FROM sessions.session_directory AS d
 WHERE d.org_id = $1 AND d.deleted_at IS NULL AND d.workspace_id = $2
 AND d.search_text <> '' AND d.search_text ILIKE $4 ESCAPE '\\'
 ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT $19`;

export const REQUIRED_SEARCH_INDEXES = [
  {
    id: "workspace-recent-sessions",
    indexName: "session_directory_workspace_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions", "Cloud MCP list_session_timings"],
    queryShape: "org + workspace equality; logical-session activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, last_activity_at desc, session_id desc, harness desc)",
    ],
    expectedPredicate: "(deleted_at IS NULL)",
    requiresDirectoryProjection: true,
  },
  {
    id: "org-recent-sessions",
    indexName: "session_directory_org_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud org-wide session list"],
    queryShape: "org equality; cross-workspace logical-session activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, last_activity_at desc, session_id desc, harness desc, workspace_id desc)",
    ],
    expectedPredicate: "(deleted_at IS NULL)",
    requiresDirectoryProjection: true,
  },
  {
    id: "workspace-harness-recent-sessions",
    indexName: "session_directory_harness_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions harness/source filter", "Cloud MCP"],
    queryShape: "org + workspace + harness equality; activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, harness, last_activity_at desc, session_id desc)",
    ],
    expectedPredicate: "(deleted_at IS NULL)",
    requiresDirectoryProjection: true,
  },
  {
    id: "workspace-project-recent-sessions",
    indexName: "session_directory_project_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions project filter", "Cloud MCP"],
    queryShape: "org + workspace + exact project; activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, project_id, last_activity_at desc, session_id desc, harness desc)",
    ],
    expectedPredicate: "((project_id IS NOT NULL) AND (deleted_at IS NULL))",
    requiresDirectoryProjection: true,
  },
  {
    id: "workspace-no-project-recent-sessions",
    indexName: "session_directory_no_project_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions unassigned-project filter"],
    queryShape: "org + workspace + no project; activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, last_activity_at desc, session_id desc, harness desc)",
    ],
    expectedPredicate: "((project_id IS NULL) AND (deleted_at IS NULL))",
    requiresDirectoryProjection: true,
  },
  {
    id: "agent-name-exact-or-prefix",
    indexName: "session_directory_agent_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions agent filter", "Relay Desktop remote parity"],
    queryShape:
      "normalized exact or >=3-character prefix agent name; activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, agent_name_normalized text_pattern_ops, last_activity_at desc, session_id desc, harness desc)",
    ],
    expectedPredicate:
      "((agent_name_normalized IS NOT NULL) AND (deleted_at IS NULL))",
    requiresDirectoryProjection: true,
  },
  {
    id: "workspace-created-range",
    indexName: "session_directory_created_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions created filter"],
    queryShape: "org + workspace + created range; created keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, created_at desc, session_id desc, harness desc)",
    ],
    expectedPredicate: "(deleted_at IS NULL)",
    requiresDirectoryProjection: true,
  },
  {
    id: "workspace-state-recent",
    indexName: "session_directory_state_recent_idx",
    tableName: "session_directory",
    consumers: ["Cloud Sessions state filter"],
    queryShape: "org + workspace + state; activity keyset",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, workspace_id, state, last_activity_at desc, session_id desc, harness desc)",
    ],
    expectedPredicate: "(deleted_at IS NULL)",
    requiresDirectoryProjection: true,
  },
  {
    id: "session-id-lookup",
    indexName: "session_directory_session_lookup_idx",
    tableName: "session_directory",
    consumers: ["Cloud session open", "Cloud MCP"],
    queryShape: "org + session id when workspace and harness are unknown",
    definitionFragments: [
      "on sessions.session_directory",
      "(org_id, session_id)",
    ],
    requiresDirectoryProjection: true,
  },
  {
    id: "tenant-project-substring",
    indexName: "session_directory_tenant_project_trgm_idx",
    tableName: "session_directory",
    consumers: [
      "sessions.search_session_directory p_project_contains (RelayHistory projectContains)",
    ],
    queryShape: "org + workspace scoped case-insensitive project substring",
    definitionFragments: [
      "on sessions.session_directory using gin",
      "(org_id, workspace_id, project_id",
      "gin_trgm_ops)",
    ],
    expectedPredicate: "((project_id IS NOT NULL) AND (deleted_at IS NULL))",
    requiresDirectoryProjection: true,
  },
  {
    id: "tenant-search-substring",
    indexName: "session_directory_tenant_search_trgm_idx",
    tableName: "session_directory",
    consumers: [
      "sessions.search_session_directory (RelayHistory #226 metadata search)",
    ],
    queryShape:
      "org + workspace scoped case-insensitive substring over search_text (metadata plus scrubbed short_summary)",
    definitionFragments: [
      "on sessions.session_directory using gin",
      "(org_id, workspace_id, search_text",
      "gin_trgm_ops)",
    ],
    expectedPredicate: "((search_text <> ''::text) AND (deleted_at IS NULL))",
    requiresDirectoryProjection: true,
  },
  {
    id: "user-membership-recent",
    indexName: "session_directory_users_recent_idx",
    tableName: "session_directory_users",
    consumers: ["Cloud Sessions person filter", "Cloud MCP user filter"],
    queryShape: "org + workspace + user membership; activity keyset",
    definitionFragments: [
      "on sessions.session_directory_users",
      "(org_id, workspace_id, user_id, last_activity_at desc, session_id desc, harness desc)",
    ],
    requiresDirectoryProjection: true,
  },
  {
    id: "machine-membership-recent",
    indexName: "session_directory_machines_recent_idx",
    tableName: "session_directory_machines",
    consumers: ["Cloud machine filter"],
    queryShape: "org + workspace + machine membership; activity keyset",
    definitionFragments: [
      "on sessions.session_directory_machines",
      "(org_id, workspace_id, machine_id, last_activity_at desc, session_id desc, harness desc)",
    ],
    requiresDirectoryProjection: true,
  },
  {
    id: "event-activity-window",
    indexName: "session_directory_event_times_window_idx",
    tableName: "session_directory_event_times",
    consumers: ["History exact no-query session activity"],
    queryShape: "org + workspace + inclusive event timestamp window; distinct sessions per UTC day and harness",
    definitionFragments: ["on sessions.session_directory_event_times", "(org_id, workspace_id, event_ts, harness, session_id)"],
    requiresDirectoryProjection: true,
  },
  {
    id: "project-name-exact-recent",
    indexName: "session_directory_project_name_recent_idx",
    tableName: "session_directory",
    consumers: ["History exact recorded project-name session lookup"],
    queryShape: "org + workspace + exact normalized project_name; event activity recency keyset across all harnesses",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, project_name, event_last_ts desc, session_id desc, harness desc)"],
    expectedPredicate: "((deleted_at IS NULL) AND (project_name IS NOT NULL))",
    requiresDirectoryProjection: true,
  },
] as const satisfies readonly SearchIndexRequirement[];

export type SearchIndexCatalogRow = {
  table_name: string;
  index_name: string;
  index_definition: string;
  predicate: string | null;
  is_valid: boolean;
  is_ready: boolean;
  reloptions: string | null;
};

export const SEARCH_INDEX_CATALOG_SQL = `
SELECT table_rel.relname AS table_name,
       index_rel.relname AS index_name,
       pg_get_indexdef(index_rel.oid) AS index_definition,
       pg_get_expr(catalog.indpred, catalog.indrelid) AS predicate,
       catalog.indisvalid AS is_valid,
       catalog.indisready AS is_ready,
       array_to_string(index_rel.reloptions, ',') AS reloptions
  FROM pg_catalog.pg_index AS catalog
  JOIN pg_catalog.pg_class AS table_rel ON table_rel.oid = catalog.indrelid
  JOIN pg_catalog.pg_class AS index_rel ON index_rel.oid = catalog.indexrelid
  JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = table_rel.relnamespace
 WHERE namespace.nspname = 'sessions'
 ORDER BY table_rel.relname, index_rel.relname`;

function normalized(value: string | null | undefined): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/["']/g, "")
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function normalizedPredicate(value: string | null | undefined): string {
  const literals: string[] = [];
  let masked = "";
  const input = value ?? "";
  for (let index = 0; index < input.length;) {
    if (input[index] !== "'") {
      masked += input[index];
      index += 1;
      continue;
    }
    const start = index;
    index += 1;
    while (index < input.length) {
      if (input[index] !== "'") {
        index += 1;
        continue;
      }
      if (input[index + 1] === "'") {
        index += 2;
        continue;
      }
      index += 1;
      break;
    }
    masked += `\u0001${literals.length}\u0002`;
    literals.push(input.slice(start, index));
  }
  return masked
    .toLowerCase()
    .replace(/"/g, "")
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim()
    .replace(
      /\u0001(\d+)\u0002/g,
      (_marker, offset: string) => literals[Number(offset)] ?? "",
    );
}

export type SearchIndexReadiness = { directoryProjectionComplete: boolean };
export type SearchIndexAuditItem = {
  id: string;
  indexName: string;
  tableName: string;
  ready: boolean;
  reasons: string[];
};

export function auditSearchIndexes(
  rows: readonly SearchIndexCatalogRow[],
  readiness: SearchIndexReadiness,
): { passed: boolean; indexes: SearchIndexAuditItem[] } {
  const byName = new Map(rows.map((row) => [row.index_name, row]));
  const indexes = REQUIRED_SEARCH_INDEXES.map((requirement) => {
    const row = byName.get(requirement.indexName);
    const reasons: string[] = [];
    if (!row) {
      reasons.push("missing");
    } else {
      if (row.table_name !== requirement.tableName) reasons.push("wrong table");
      if (!row.is_valid) reasons.push("invalid");
      if (!row.is_ready) reasons.push("not ready");
      const definition = normalized(row.index_definition);
      for (const fragment of requirement.definitionFragments) {
        if (!definition.includes(normalized(fragment)))
          reasons.push(`definition missing: ${fragment}`);
      }
      const expectedPredicate =
        "expectedPredicate" in requirement
          ? requirement.expectedPredicate
          : null;
      if (
        normalizedPredicate(row.predicate) !==
        normalizedPredicate(expectedPredicate)
      ) {
        reasons.push(
          expectedPredicate ? "predicate mismatch" : "unexpected predicate",
        );
      }
      const options = normalized(row.reloptions);
      const reloptionFragments: readonly string[] =
        "reloptionFragments" in requirement
          ? (requirement.reloptionFragments as readonly string[])
          : [];
      for (const fragment of reloptionFragments) {
        if (!options.includes(normalized(fragment)))
          reasons.push(`option missing: ${fragment}`);
      }
    }
    if (
      "requiresDirectoryProjection" in requirement &&
      requirement.requiresDirectoryProjection &&
      !readiness.directoryProjectionComplete
    ) {
      reasons.push("directory projection incomplete");
    }
    return {
      id: requirement.id,
      indexName: requirement.indexName,
      tableName: requirement.tableName,
      ready: reasons.length === 0,
      reasons,
    };
  });
  return { passed: indexes.every((index) => index.ready), indexes };
}
