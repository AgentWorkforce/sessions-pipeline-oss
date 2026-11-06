/**
 * The only write path for `sessions.session_directory.short_summary` (G6).
 *
 * The text is session data derived from transcript content, so it is stored only
 * after the shared `lib/scrub.ts` scrubber has run over it, and the scrub ruleset
 * and version it went through are recorded beside it. The writer fails closed:
 * an over-length summary, a scrub that throws, a scrub that would truncate, or a
 * scrub that is not stable on its own output is refused and nothing is written.
 * The database repeats the length and provenance checks
 * (supabase/migrations/20261030020000_session_directory_short_summary.sql).
 *
 * Long summaries stay in object storage (Bob Costas's encrypted summary objects).
 * No summary text is ever put in a log line, an error message or telemetry.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  scrubText,
} from "../shared/vendor.ts";
import { meaningfulChars } from "./search-query.ts";

/** Hard bound, in Unicode code points (PostgreSQL `char_length`). */
export const SHORT_SUMMARY_MAX_CHARS = 1000;

/** Same shape as `session_summaries.model_id`, prefixed by the producer. */
const SOURCE_PATTERN = /^[a-z0-9][a-z0-9._/-]{0,63}$/;

export type ShortSummaryRejection =
  | "empty"
  | "over_length"
  | "scrub_failed"
  | "scrub_unstable"
  | "invalid_source";

/** Refusal. The message names the reason only, never the text. */
export class ShortSummaryRejected extends Error {
  constructor(readonly reason: ShortSummaryRejection) {
    super(`short_summary rejected: ${reason}`);
    this.name = "ShortSummaryRejected";
  }
}

export type ScrubbedShortSummary = {
  text: string;
  scrubRuleset: typeof SCRUB_RULESET;
  scrubVersion: number;
};

export type SessionDirectoryKey = {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
};

export type ShortSummaryInput = SessionDirectoryKey & {
  summary: string;
  /** Producer and version, e.g. `bob-costas/extractive/v1`. */
  source: string;
  generatedAt: Date;
};

const codePoints = (value: string) => Array.from(value).length;

/**
 * Scrub and bound one summary. `scrub` is the shared scrubber; it is a parameter
 * only so tests can prove a failing scrub is refused.
 */
export function scrubShortSummary(
  summary: string,
  scrub: (value: string) => string = scrubText,
): ScrubbedShortSummary {
  const trimmed = summary.trim();
  // Blank-looking text (only Unicode spaces, format characters or combining marks)
  // is empty under the same rule the search query uses.
  if (meaningfulChars(trimmed) === 0) throw new ShortSummaryRejected("empty");
  // Refuse rather than truncate: a cut can split a credential below the scrubber's
  // match length, and a silently shortened summary is not what the producer wrote.
  if (codePoints(trimmed) > SHORT_SUMMARY_MAX_CHARS)
    throw new ShortSummaryRejected("over_length");
  let text: string;
  try {
    text = scrub(trimmed);
    if (typeof text !== "string")
      throw new ShortSummaryRejected("scrub_failed");
    // A fixed point proves the stored text is scrubbed under these rules.
    if (scrub(text) !== text) throw new ShortSummaryRejected("scrub_unstable");
  } catch (error) {
    if (error instanceof ShortSummaryRejected) throw error;
    throw new ShortSummaryRejected("scrub_failed");
  }
  text = text.trim();
  if (meaningfulChars(text) === 0) throw new ShortSummaryRejected("empty");
  if (codePoints(text) > SHORT_SUMMARY_MAX_CHARS)
    throw new ShortSummaryRejected("over_length");
  return {
    text,
    scrubRuleset: SCRUB_RULESET,
    scrubVersion: SCRUB_RULESET_VERSION,
  };
}

/** The session-brief worker's producer prefix (src/session-brief/versions.ts). */
export const BRIEF_SUMMARY_SOURCE_PREFIX = "session-brief/";

/**
 * Store a summary on a live (not tombstoned) directory row and rebuild its search
 * document with `sessions.session_directory_search_text`, so the summary is always
 * searchable through `search_text`. An older `generatedAt` never replaces a newer
 * summary, and a non-brief producer (Bob Costas's extractive summary) never replaces a
 * model-written brief (`session-brief/*`). Returns whether a row was written.
 */
export async function writeShortSummary(
  db: SqlExecutor,
  input: ShortSummaryInput,
  scrub: (value: string) => string = scrubText,
): Promise<{ written: boolean; scrubVersion: number }> {
  let scrubbed: ScrubbedShortSummary;
  try {
    if (!SOURCE_PATTERN.test(input.source))
      throw new ShortSummaryRejected("invalid_source");
    scrubbed = scrubShortSummary(input.summary, scrub);
  } catch (error) {
    // A refused newer summary must not leave an older one searchable: the old text
    // describes content the producer has since replaced. Clear it (and its share of
    // search_text) under the same generated_at fence, so a refused *older* summary
    // never clears a newer good one. Then rethrow the refusal.
    if (error instanceof ShortSummaryRejected)
      await clearOlderShortSummary(db, input, input.generatedAt, input.source);
    throw error;
  }
  const { rows } = await db.query<{ written: boolean }>(
    `WITH target AS (
       SELECT d.session_id
         FROM sessions.session_directory AS d
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
          AND (d.session_id = $4 OR EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
               AND p.harness = d.harness AND p.session_id = $4
               AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))
        ORDER BY (d.session_id <> $4) DESC LIMIT 1
     )
     UPDATE sessions.session_directory AS d
        SET short_summary = $5,
            short_summary_scrub_ruleset = $6,
            short_summary_scrub_version = $7,
            short_summary_source = $8,
            short_summary_generated_at = $9::timestamptz,
            search_text = sessions.session_directory_search_text(
              d.title, d.task_title, d.agent_name, d.project_id,
              d.git_repo, d.git_branch, d.model, $5),
            projected_at = now()
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = t.session_id
        AND d.deleted_at IS NULL
        AND (d.short_summary_generated_at IS NULL OR d.short_summary_generated_at <= $9::timestamptz)
        AND (d.short_summary_source IS NULL OR left(d.short_summary_source, $10) <> $11
             OR left($8, $10) = $11)
      RETURNING true AS written`,
    [
      input.orgId,
      input.workspaceId,
      input.harness,
      input.sessionId,
      scrubbed.text,
      scrubbed.scrubRuleset,
      scrubbed.scrubVersion,
      input.source,
      input.generatedAt.toISOString(),
      BRIEF_SUMMARY_SOURCE_PREFIX.length,
      BRIEF_SUMMARY_SOURCE_PREFIX,
    ],
  );
  return { written: rows.length > 0, scrubVersion: scrubbed.scrubVersion };
}

/** Clear the summary only if it is not newer than `notAfter` (the generated_at fence). */
async function clearOlderShortSummary(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  notAfter: Date,
  source: string,
): Promise<boolean> {
  const { rows } = await db.query<{ cleared: boolean }>(
    `WITH target AS (
       SELECT d.session_id
         FROM sessions.session_directory AS d
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
          AND (d.session_id = $4 OR EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
               AND p.harness = d.harness AND p.session_id = $4
               AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))
        ORDER BY (d.session_id <> $4) DESC LIMIT 1
     )
     UPDATE sessions.session_directory AS d
        SET short_summary = NULL,
            short_summary_scrub_ruleset = NULL,
            short_summary_scrub_version = NULL,
            short_summary_source = NULL,
            short_summary_generated_at = NULL,
            search_text = sessions.session_directory_search_text(
              d.title, d.task_title, d.agent_name, d.project_id,
              d.git_repo, d.git_branch, d.model, NULL),
            projected_at = now()
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = t.session_id
        AND d.deleted_at IS NULL AND d.short_summary IS NOT NULL
        AND d.short_summary_generated_at <= $5::timestamptz
        -- The same producer precedence as the write: a refused extractive summary
        -- never clears a model-written brief.
        AND (left(d.short_summary_source, $7) <> $8 OR left($6, $7) = $8)
      RETURNING true AS cleared`,
    [
      key.orgId,
      key.workspaceId,
      key.harness,
      key.sessionId,
      notAfter.toISOString(),
      source,
      BRIEF_SUMMARY_SOURCE_PREFIX.length,
      BRIEF_SUMMARY_SOURCE_PREFIX,
    ],
  );
  return rows.length > 0;
}

/** Remove a row's summary (and its share of the search document) without a tombstone. */
export async function clearShortSummary(
  db: SqlExecutor,
  key: SessionDirectoryKey,
): Promise<boolean> {
  const { rows } = await db.query<{ cleared: boolean }>(
    `WITH target AS (
       SELECT d.session_id
         FROM sessions.session_directory AS d
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
          AND (d.session_id = $4 OR EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
               AND p.harness = d.harness AND p.session_id = $4
               AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))
        ORDER BY (d.session_id <> $4) DESC LIMIT 1
     )
     UPDATE sessions.session_directory AS d
        SET short_summary = NULL,
            short_summary_scrub_ruleset = NULL,
            short_summary_scrub_version = NULL,
            short_summary_source = NULL,
            short_summary_generated_at = NULL,
            search_text = CASE WHEN d.deleted_at IS NULL THEN sessions.session_directory_search_text(
              d.title, d.task_title, d.agent_name, d.project_id,
              d.git_repo, d.git_branch, d.model, NULL) ELSE '' END,
            projected_at = now()
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = t.session_id
        AND d.short_summary IS NOT NULL
      RETURNING true AS cleared`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId],
  );
  return rows.length > 0;
}

/**
 * Projector deletion path: a machine manifest of this logical session was
 * tombstoned in `session_index`. The summary was built from content that may
 * include that machine, so it is cleared at once (with its share of search_text).
 * When no live pointer remains, the directory row itself is tombstoned, which also
 * empties search_text (trigger). Called by the HoopDb indexer's deletion branch.
 */
export async function propagatePointerTombstone(
  db: SqlExecutor,
  key: SessionDirectoryKey,
): Promise<{ found: boolean; tombstoned: boolean }> {
  const { rows } = await db.query<{ tombstoned: boolean }>(
    `WITH identity_stats AS (
       SELECT count(*) AS pointers,
              count(DISTINCT coalesce(canonical_session_id, session_id)) AS identities,
              min(coalesce(canonical_session_id, session_id)) AS canonical_session_id
         FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4
     ), identity AS (
       SELECT CASE WHEN pointers = 0 THEN $4 ELSE canonical_session_id END AS canonical_session_id
         FROM identity_stats WHERE pointers = 0 OR identities = 1
     ), target AS (
       SELECT CASE
         WHEN EXISTS (
           SELECT 1 FROM sessions.session_directory AS existing
            WHERE existing.org_id = $1 AND existing.workspace_id = $2
              AND existing.harness = $3 AND existing.session_id = i.canonical_session_id)
           THEN i.canonical_session_id
         WHEN i.canonical_session_id <> $4 AND EXISTS (
           SELECT 1 FROM sessions.session_directory AS legacy
            WHERE legacy.org_id = $1 AND legacy.workspace_id = $2
              AND legacy.harness = $3 AND legacy.session_id = $4)
           THEN $4
         ELSE i.canonical_session_id END AS session_id
       FROM identity AS i
     )
     UPDATE sessions.session_directory AS d
        SET short_summary = NULL,
            short_summary_scrub_ruleset = NULL,
            short_summary_scrub_version = NULL,
            short_summary_source = NULL,
            short_summary_generated_at = NULL,
            search_text = sessions.session_directory_search_text(
              d.title, d.task_title, d.agent_name, d.project_id,
              d.git_repo, d.git_branch, d.model, NULL),
            deleted_at = CASE
              WHEN EXISTS (
                SELECT 1 FROM sessions.session_index AS p
                 WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
                   AND p.harness = d.harness AND p.session_id = $4
                   AND p.deleted_at IS NULL)
              THEN d.deleted_at
              ELSE coalesce(d.deleted_at, now()) END,
            projected_at = now()
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
        AND d.session_id = t.session_id
      RETURNING d.deleted_at IS NOT NULL AS tombstoned`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId],
  );
  return { found: rows.length > 0, tombstoned: rows[0]?.tombstoned === true };
}

/**
 * Erasure: hard-delete directory rows (memberships cascade) for one session or a
 * whole workspace. Returns the number of rows removed.
 */
export async function eraseSessionDirectory(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId: string } | SessionDirectoryKey,
): Promise<number> {
  const session = "sessionId" in scope ? scope : null;
  const { rows } = await db.query<{ n: number }>(
    session
      ? `WITH gone AS (
           DELETE FROM sessions.session_directory AS d
            WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
              AND (d.session_id = $4 OR EXISTS (
                SELECT 1 FROM sessions.session_index AS p
                 WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
                   AND p.harness = d.harness AND p.session_id = $4
                   AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))
           RETURNING 1)
         SELECT count(*)::int AS n FROM gone`
      : `WITH gone AS (DELETE FROM sessions.session_directory
           WHERE org_id = $1 AND workspace_id = $2 RETURNING 1)
         SELECT count(*)::int AS n FROM gone`,
    session
      ? [session.orgId, session.workspaceId, session.harness, session.sessionId]
      : [scope.orgId, scope.workspaceId],
  );
  return rows[0]?.n ?? 0;
}
