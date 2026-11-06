/**
 * SQL for the session brief: arming and claiming debounced wakeups, relayhistory's
 * admission limits, and the one write path for sessions.session_briefs and the brief's
 * projection into session_directory (title; description through writeShortSummary).
 *
 * Values only ever travel as bound parameters. No brief text is logged here.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { HoopDb } from "../shared/db.ts";
import {
  ShortSummaryRejected,
  scrubShortSummary,
  writeShortSummary,
  type SessionDirectoryKey,
} from "../hoopdb/session-directory-summary.ts";
import { SESSION_SUMMARY_STATES, type SessionSummaryState } from "./engine/summary-contract.ts";
import type { BriefPointer } from "./input.ts";
import { SESSION_BRIEF_PROMPT_VERSION, SESSION_BRIEF_SOURCE_PREFIX } from "./versions.ts";

const KEY = `org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4`;
const keyParams = (k: SessionDirectoryKey) => [k.orgId, k.workspaceId, k.harness, k.sessionId];

export type WakeupAdmission = "background" | "sweep";

/**
 * Arm (or push back) a session's wakeup: due `idleSeconds` after this evidence, but never
 * later than `maxWaitSeconds` after the first unbriefed evidence (relayhistory 0048:
 * LEAST(now() + 10 minutes, created_at + 60 minutes)). Bumps the generation, so a run in
 * flight leaves the wakeup for the new evidence instead of deleting it.
 */
export async function armWakeup(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  opts: { admission?: WakeupAdmission; idleSeconds: number; maxWaitSeconds: number },
): Promise<void> {
  await db.query(
    `INSERT INTO sessions.session_brief_wakeups AS w
       (org_id, workspace_id, harness, session_id, admission, pending_since, evidence_at, due_at)
     VALUES ($1, $2, $3, $4, $5, now(), now(), now() + make_interval(secs => $6::integer))
     ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET
       admission = CASE WHEN excluded.admission = 'background' THEN 'background' ELSE w.admission END,
       evidence_at = now(),
       due_at = LEAST(now() + make_interval(secs => $6::integer),
                      w.pending_since + make_interval(secs => $7::integer)),
       generation = w.generation + 1,
       attempts = 0,
       last_error = NULL`,
    [...keyParams(key), opts.admission ?? "background", opts.idleSeconds, opts.maxWaitSeconds],
  );
}

/** Drop never-claimed wakeups with no evidence for `maxAgeHours` (relayhistory prune_stale_session_wakeups_v1). */
export async function pruneStaleWakeups(db: SqlExecutor, maxAgeHours: number, limit = 5_000): Promise<number> {
  const { rows } = await db.query<{ n: number }>(
    `WITH gone AS (
       DELETE FROM sessions.session_brief_wakeups AS w
        USING (SELECT org_id, workspace_id, harness, session_id FROM sessions.session_brief_wakeups
                WHERE evidence_at < now() - make_interval(hours => $1::integer)
                  AND (lease_expires_at IS NULL OR lease_expires_at < now())
                ORDER BY evidence_at LIMIT $2 FOR UPDATE SKIP LOCKED) AS s
        WHERE w.org_id = s.org_id AND w.workspace_id = s.workspace_id
          AND w.harness = s.harness AND w.session_id = s.session_id
        RETURNING 1)
     SELECT count(*)::int AS n FROM gone`,
    [maxAgeHours, limit],
  );
  return rows[0]?.n ?? 0;
}

export interface ClaimedWakeup extends SessionDirectoryKey {
  admission: WakeupAdmission;
  generation: string;
  attempts: number;
  leaseId: string;
}

/** Claim up to `batch` due wakeups under a lease (SKIP LOCKED, so any number of workers may poll). */
export async function claimWakeups(db: SqlExecutor, batch: number, leaseSeconds: number): Promise<ClaimedWakeup[]> {
  const { rows } = await db.query<{
    org_id: string;
    workspace_id: string;
    harness: string;
    session_id: string;
    admission: WakeupAdmission;
    generation: string;
    attempts: number;
    lease_id: string;
  }>(
    `WITH due AS (
       SELECT org_id, workspace_id, harness, session_id FROM sessions.session_brief_wakeups
        WHERE due_at <= now() AND (lease_expires_at IS NULL OR lease_expires_at < now())
        ORDER BY due_at, org_id, workspace_id, harness, session_id
        LIMIT $1 FOR UPDATE SKIP LOCKED)
     UPDATE sessions.session_brief_wakeups AS w
        SET lease_id = gen_random_uuid(), lease_expires_at = now() + make_interval(secs => $2::integer),
            attempts = w.attempts + 1
       FROM due
      WHERE w.org_id = due.org_id AND w.workspace_id = due.workspace_id
        AND w.harness = due.harness AND w.session_id = due.session_id
      RETURNING w.org_id, w.workspace_id, w.harness, w.session_id, w.admission,
                w.generation::text AS generation, w.attempts, w.lease_id::text AS lease_id`,
    [batch, leaseSeconds],
  );
  return rows.map((r) => ({
    orgId: r.org_id,
    workspaceId: r.workspace_id,
    harness: r.harness,
    sessionId: r.session_id,
    admission: r.admission,
    generation: r.generation,
    attempts: Number(r.attempts),
    leaseId: r.lease_id,
  }));
}

/**
 * Done with a claim. The wakeup goes only if no new evidence armed it meanwhile (same
 * generation); otherwise the lease is released and the new generation stays due, in a
 * fresh debounce window: the evidence that re-armed it is unbriefed from now, so
 * pending_since restarts here and due_at is recomputed against it. Without this, a
 * session active for over an hour kept its first pending_since and every later re-arm
 * was due at once (one model call per revision, bounded only by the throttle).
 */
export async function finishWakeup(
  db: SqlExecutor,
  w: ClaimedWakeup,
  window: { idleSeconds: number; maxWaitSeconds: number },
): Promise<void> {
  const params = [...keyParams(w), w.leaseId, w.generation];
  const { rows } = await db.query(
    `DELETE FROM sessions.session_brief_wakeups
      WHERE ${KEY} AND lease_id = $5::uuid AND generation = $6::bigint RETURNING 1`,
    params,
  );
  if (rows.length) return;
  await db.query(
    `UPDATE sessions.session_brief_wakeups AS w
        SET lease_id = NULL, lease_expires_at = NULL, pending_since = now(),
            due_at = LEAST(w.evidence_at + make_interval(secs => $6::integer),
                           now() + make_interval(secs => $7::integer))
      WHERE ${KEY} AND lease_id = $5::uuid`,
    [...params.slice(0, 5), window.idleSeconds, window.maxWaitSeconds],
  );
}

/** Put a claimed wakeup back, due later (a retry backoff, a throttle or a budget reset). */
export async function rescheduleWakeup(
  db: SqlExecutor,
  w: ClaimedWakeup,
  due: { nextUtcDay: true } | { seconds: number },
  opts: { error?: string; refundAttempt?: boolean } = {},
): Promise<void> {
  // $5 lease, $6 generation, $7 seconds (ignored for nextUtcDay), $8 error, $9 refund.
  const dueSql =
    "nextUtcDay" in due
      ? `(date_trunc('day', now() AT TIME ZONE 'UTC') + interval '1 day') AT TIME ZONE 'UTC'`
      : `now() + make_interval(secs => $7::integer)`;
  await db.query(
    `UPDATE sessions.session_brief_wakeups AS w
        SET lease_id = NULL, lease_expires_at = NULL,
            -- Re-armed meanwhile: the new evidence keeps its own (earlier) due time.
            due_at = CASE WHEN w.generation = $6::bigint THEN ${dueSql}
                          ELSE least(w.due_at, ${dueSql}) END,
            attempts = CASE WHEN $9::boolean THEN greatest(w.attempts - 1, 0) ELSE w.attempts END,
            last_error = coalesce($8::text, w.last_error)
      WHERE ${KEY} AND w.lease_id = $5::uuid AND $7::integer >= 0`,
    [
      ...keyParams(w),
      w.leaseId,
      w.generation,
      "seconds" in due ? Math.max(0, Math.floor(due.seconds)) : 0,
      opts.error ?? null,
      opts.refundAttempt === true,
    ],
  );
}

/** The session's live pointers (at most 64, as session_directory_pointers). */
export async function livePointers(db: SqlExecutor, key: SessionDirectoryKey): Promise<BriefPointer[]> {
  const { rows } = await db.query<{ machine_id: string; manifest_key: string | null; revision: string }>(
    `SELECT machine_id, manifest_key, revision::text AS revision FROM sessions.session_index
      WHERE ${KEY} AND deleted_at IS NULL ORDER BY machine_id LIMIT 64`,
    keyParams(key),
  );
  return rows
    .filter((r) => r.manifest_key)
    .map((r) => ({ machineId: r.machine_id, manifestKey: r.manifest_key!, revision: Number(r.revision) }));
}

export interface StoredBrief {
  status: string;
  revision: number;
  title: string | null;
  description: string | null;
  state: SessionSummaryState | null;
  /** The prompt that wrote the stored text; a brief from another prompt is not reused. */
  promptVersion: string | null;
  briefFingerprint: string | null;
  model: string | null;
  generatedAt: Date | null;
  attemptedAt: Date;
  /** Seconds since attempted_at, by the database clock. */
  attemptedAgoSeconds: number;
}

export async function storedBrief(db: SqlExecutor, key: SessionDirectoryKey): Promise<StoredBrief | null> {
  const { rows } = await db.query<Record<string, unknown>>(
    `SELECT status, revision::text AS revision, title, description, state, prompt_version,
            brief_fingerprint, model,
            generated_at, attempted_at, extract(epoch FROM now() - attempted_at)::float8 AS ago
       FROM sessions.session_briefs WHERE ${KEY}`,
    keyParams(key),
  );
  const r = rows[0];
  if (!r) return null;
  return {
    status: String(r.status),
    revision: Number(r.revision),
    title: (r.title as string | null) ?? null,
    description: (r.description as string | null) ?? null,
    state: (r.state as SessionSummaryState | null) ?? null,
    promptVersion: (r.prompt_version as string | null) ?? null,
    briefFingerprint: (r.brief_fingerprint as string | null) ?? null,
    model: (r.model as string | null) ?? null,
    generatedAt: r.generated_at ? new Date(r.generated_at as string) : null,
    attemptedAt: new Date(r.attempted_at as string),
    attemptedAgoSeconds: Number(r.ago),
  };
}

export type Admission = { admitted: true } | { admitted: false; reason: "capped" | "budget" | "global" };

/**
 * relayhistory's admission limits, by the database's UTC day: at most `sessionDailyCap`
 * attempts for one session, and a refusal once `budget` other sessions of the org were
 * admitted today (sessions_admitted_today >= session_admission_budget), and, over every
 * org, a refusal once today's attempts reach `globalDailyCap` (a spend ceiling; Khaliq,
 * 2026-11-02). An admitted attempt is counted here.
 */
export async function admit(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  limits: { sessionDailyCap: number; budget: number; globalDailyCap: number },
): Promise<Admission> {
  const { rows } = await db.query<{ mine: number; others: number; total: number }>(
    `SELECT coalesce((SELECT attempts FROM sessions.session_brief_attempts
                       WHERE org_id = $1 AND day = (now() AT TIME ZONE 'UTC')::date
                         AND workspace_id = $2 AND harness = $3 AND session_id = $4), 0)::int AS mine,
            (SELECT count(*) FROM sessions.session_brief_attempts
              WHERE org_id = $1 AND day = (now() AT TIME ZONE 'UTC')::date
                AND NOT (workspace_id = $2 AND harness = $3 AND session_id = $4))::int AS others,
            (SELECT coalesce(sum(attempts), 0) FROM sessions.session_brief_attempts
              WHERE day = (now() AT TIME ZONE 'UTC')::date)::int AS total`,
    keyParams(key),
  );
  const { mine, others, total } = rows[0] ?? { mine: 0, others: 0, total: 0 };
  if (Number(total) >= limits.globalDailyCap) return { admitted: false, reason: "global" };
  if (Number(mine) >= limits.sessionDailyCap) return { admitted: false, reason: "capped" };
  if (Number(others) >= limits.budget) return { admitted: false, reason: "budget" };
  await db.query(
    `INSERT INTO sessions.session_brief_attempts (org_id, workspace_id, harness, session_id, day, attempts)
     VALUES ($1, $2, $3, $4, (now() AT TIME ZONE 'UTC')::date, 1)
     ON CONFLICT (org_id, day, workspace_id, harness, session_id)
     DO UPDATE SET attempts = sessions.session_brief_attempts.attempts + 1`,
    keyParams(key),
  );
  return { admitted: true };
}

/** Admission rows are only needed for today; keep yesterday's for the day boundary. */
export async function pruneAttempts(db: SqlExecutor): Promise<void> {
  await db.query(
    `DELETE FROM sessions.session_brief_attempts WHERE day < (now() AT TIME ZONE 'UTC')::date - 1`,
  );
}

/** A brief refused by the scrub or its bounds. The reason only; never the text. */
export class BriefRejected extends Error {
  constructor(readonly reason: string) {
    super(`session brief rejected: ${reason}`);
    this.name = "BriefRejected";
  }
}

/**
 * Scrub and bound a model brief to SESSION_SUMMARY_JSON_SCHEMA (title <= 90, description
 * <= 320, one line each) with the shared scrubber at a fixed point, as writeShortSummary
 * does. Refuses rather than truncates. The state is one of SESSION_SUMMARY_STATES.
 */
export function scrubBrief(
  brief: { title: string; description: string; state: SessionSummaryState },
  scrub?: (value: string) => string,
): {
  title: string;
  description: string;
  state: SessionSummaryState;
  scrubRuleset: string;
  scrubVersion: number;
} {
  const one = (value: string, max: number, field: string) => {
    let s;
    try {
      s = scrubShortSummary(value, scrub);
    } catch (error) {
      if (error instanceof ShortSummaryRejected) throw new BriefRejected(`${field}_${error.reason}`);
      throw error;
    }
    if (Array.from(s.text).length > max) throw new BriefRejected(`${field}_over_length`);
    if (/[\r\n]/.test(s.text)) throw new BriefRejected(`${field}_multiline`);
    return s;
  };
  const title = one(brief.title, 90, "title");
  const description = one(brief.description, 320, "description");
  if (!SESSION_SUMMARY_STATES.includes(brief.state)) throw new BriefRejected("state_invalid");
  return {
    title: title.text,
    description: description.text,
    state: brief.state,
    scrubRuleset: title.scrubRuleset,
    scrubVersion: title.scrubVersion,
  };
}

export interface BriefText {
  title: string;
  description: string;
  state: SessionSummaryState;
  scrubRuleset: string;
  scrubVersion: number;
  fingerprint: string;
  promptVersion: string;
  engineVersion: string;
  model: string;
  messageCount: number;
  shortenedMessages: number;
  inputTokens: number;
  outputTokens: number;
  durationMs: number;
  generatedAt: Date;
}

export type BriefOutcome =
  /** A new model generation. */
  | { status: "ready"; text: BriefText }
  /** The input is unchanged (same fingerprint): the stored brief now covers these revisions. */
  | { status: "reused"; fingerprint: string }
  | { status: "failed" | "rate_limited" | "ineligible" | "empty"; error: string }
  | { status: "quarantined"; error: string };

/** The directory short_summary producer for a model, within writeShortSummary's source pattern. */
export function briefSource(model: string): string {
  const source = `${SESSION_BRIEF_SOURCE_PREFIX}${model.toLowerCase().replace(/[^a-z0-9._/-]/g, "-")}`.slice(0, 64);
  return /^[a-z0-9][a-z0-9._/-]{0,63}$/.test(source) ? source : `${SESSION_BRIEF_SOURCE_PREFIX}model`;
}

/**
 * Record an attempt's outcome for one logical session, in one transaction under a
 * per-session advisory lock. A ready outcome replaces the brief text; any other outcome
 * keeps the previous text (stale), except a quarantine, which clears it. An attempt that
 * read older revisions than the stored row never overwrites it. Then the brief title is
 * projected into session_directory.title and the description into short_summary (through
 * writeShortSummary, the only short_summary write path).
 */
export async function recordBrief(
  db: HoopDb,
  key: SessionDirectoryKey,
  sourceRevisions: Record<string, number>,
  outcome: BriefOutcome,
): Promise<{ written: boolean }> {
  const revision = Math.max(0, ...Object.values(sourceRevisions));
  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
      `96bulls:brief:${JSON.stringify(keyParams(key))}`,
    ]);
    const held = await client.query<{ revision: string }>(
      `SELECT revision::text AS revision FROM sessions.session_briefs WHERE ${KEY} FOR UPDATE`,
      keyParams(key),
    );
    if (held.rows[0] && Number(held.rows[0].revision) > revision) {
      await client.query("ROLLBACK");
      return { written: false };
    }
    // The input must still be exactly the session's live pointer set: the same machines
    // at the same revisions. FOR SHARE holds those rows until COMMIT, so a pointer
    // tombstone or delete (and its brief-forget trigger) waits for this write, or wins
    // before it, and the brief is dropped as stale. This fences a deleted machine's
    // content out of a brief generated before the delete, and an older input (fewer
    // machines at the same maximum revision) out of a newer brief.
    // Exactly livePointers' read (64 by machine_id, then rows with a manifest), so a
    // session past 64 machines still compares equal to the set the input was built from.
    const live = await client.query<{ machine_id: string; manifest_key: string | null; revision: string }>(
      `SELECT machine_id, manifest_key, revision::text AS revision FROM sessions.session_index
        WHERE ${KEY} AND deleted_at IS NULL ORDER BY machine_id LIMIT 64 FOR SHARE`,
      keyParams(key),
    );
    if (!sameSourceSet(live.rows.filter((r) => r.manifest_key), sourceRevisions)) {
      await client.query("ROLLBACK");
      return { written: false };
    }
    const revisions = JSON.stringify(sourceRevisions);
    let reusedText: { title: string; description: string; model: string } | null = null;
    if (outcome.status === "ready") {
      const t = outcome.text;
      await client.query(
        `INSERT INTO sessions.session_briefs AS b
           (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
            source_fingerprint, title, description, state, brief_revision, brief_source_revisions,
            brief_fingerprint, scrub_ruleset, scrub_version, prompt_version, engine_version,
            model, message_count, shortened_messages, input_tokens, output_tokens, duration_ms,
            error, generated_at, attempted_at, updated_at)
         VALUES ($1, $2, $3, $4, 'ready', $5, $6::jsonb, $7, $8, $9, $21, $5, $6::jsonb, $7, $10, $11,
                 $12, $13, $14, $15, $16, $17, $18, $19, NULL, $20::timestamptz, now(), now())
         ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET
           status = 'ready', revision = excluded.revision, source_revisions = excluded.source_revisions,
           source_fingerprint = excluded.source_fingerprint, title = excluded.title,
           description = excluded.description, state = excluded.state,
           brief_revision = excluded.brief_revision,
           brief_source_revisions = excluded.brief_source_revisions,
           brief_fingerprint = excluded.brief_fingerprint, scrub_ruleset = excluded.scrub_ruleset,
           scrub_version = excluded.scrub_version, prompt_version = excluded.prompt_version,
           engine_version = excluded.engine_version, model = excluded.model,
           message_count = excluded.message_count, shortened_messages = excluded.shortened_messages,
           input_tokens = excluded.input_tokens, output_tokens = excluded.output_tokens,
           duration_ms = excluded.duration_ms, generated_at = excluded.generated_at,
           error = NULL, attempted_at = now(), updated_at = now()`,
        [
          ...keyParams(key),
          revision,
          revisions,
          t.fingerprint,
          t.title,
          t.description,
          t.scrubRuleset,
          t.scrubVersion,
          t.promptVersion,
          t.engineVersion,
          t.model,
          t.messageCount,
          t.shortenedMessages,
          t.inputTokens,
          t.outputTokens,
          t.durationMs,
          t.generatedAt.toISOString(),
          t.state,
        ],
      );
    } else if (outcome.status === "reused") {
      const reused = await client.query<{ title: string; description: string; model: string }>(
        `UPDATE sessions.session_briefs
            SET status = 'ready', revision = $5, source_revisions = $6::jsonb,
                source_fingerprint = $7, brief_revision = $5, brief_source_revisions = $6::jsonb,
                error = NULL, attempted_at = now(), updated_at = now()
          WHERE ${KEY} AND brief_fingerprint = $7 AND title IS NOT NULL AND prompt_version = $8
          RETURNING title, description, model`,
        [...keyParams(key), revision, revisions, outcome.fingerprint, SESSION_BRIEF_PROMPT_VERSION],
      );
      const row = reused.rows[0];
      if (!row) {
        // The stored brief changed under us (deleted, replaced): nothing to reuse.
        await client.query("ROLLBACK");
        return { written: false };
      }
      reusedText = row;
    } else {
      const clear = outcome.status === "quarantined";
      await client.query(
        `INSERT INTO sessions.session_briefs AS b
           (org_id, workspace_id, harness, session_id, status, revision, source_revisions, error,
            attempted_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, now(), now())
         ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET
           status = excluded.status, revision = excluded.revision,
           source_revisions = excluded.source_revisions, error = excluded.error,
           attempted_at = now(), updated_at = now()
           ${
             clear
               ? `, source_fingerprint = NULL, title = NULL, description = NULL, state = NULL,
                  brief_revision = NULL,
                  brief_source_revisions = NULL, brief_fingerprint = NULL, scrub_ruleset = NULL,
                  scrub_version = NULL, prompt_version = NULL, engine_version = NULL, model = NULL,
                  message_count = NULL, shortened_messages = NULL, input_tokens = 0,
                  output_tokens = 0, duration_ms = NULL, generated_at = NULL`
               : ""
           }`,
        [...keyParams(key), outcome.status, revision, revisions, outcome.error],
      );
    }
    const projected = outcome.status === "ready" ? outcome.text : reusedText;
    if (projected) {
      await projectBriefTitle(client, key, projected.title);
      await writeShortSummary(client, {
        ...key,
        summary: projected.description,
        source: briefSource(projected.model),
        generatedAt: new Date(),
      });
    } else if (outcome.status === "quarantined") {
      // Fall back to the pointer title, and remove a description this stage wrote, so a
      // quarantined brief leaves nothing searchable. Another producer's summary is kept.
      await projectBriefTitle(client, key, null);
      await clearBriefDescription(client, key);
    }
    await client.query("COMMIT");
    return { written: true };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/** True when `rows` (machine_id, revision) are exactly the brief's source revisions. */
function sameSourceSet(rows: Array<{ machine_id: string; revision: string }>, source: Record<string, number>): boolean {
  const ids = Object.keys(source);
  if (rows.length !== ids.length) return false;
  return rows.every((r) => Object.prototype.hasOwnProperty.call(source, r.machine_id) && source[r.machine_id] === Number(r.revision));
}

/** Clear the directory short_summary only when this stage produced it (its source prefix). */
async function clearBriefDescription(db: SqlExecutor, key: SessionDirectoryKey): Promise<void> {
  await db.query(
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
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
        AND d.session_id = t.session_id
        AND d.short_summary IS NOT NULL
        AND left(d.short_summary_source, char_length($5::text)) = $5::text`,
    [...keyParams(key), SESSION_BRIEF_SOURCE_PREFIX],
  );
}

/**
 * The brief title on the directory row (the projector keeps it: a brief title wins over
 * the pointer title). `null` restores the latest live pointer's title.
 */
async function projectBriefTitle(db: SqlExecutor, key: SessionDirectoryKey, title: string | null): Promise<void> {
  await db.query(
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
     ), title_value AS (
       SELECT coalesce($5::text, (
         SELECT p.title FROM sessions.session_index AS p
          WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.harness = $3 AND p.session_id = $4
            AND p.deleted_at IS NULL
          ORDER BY p.last_activity_at DESC, p.revision DESC, p.machine_id DESC LIMIT 1)) AS title
     )
     UPDATE sessions.session_directory AS d
        SET title = t.title,
            search_text = sessions.session_directory_search_text(
              t.title, d.task_title, d.agent_name, d.project_id, d.git_repo, d.git_branch,
              d.model, d.short_summary),
            projected_at = now()
       FROM title_value AS t CROSS JOIN target AS target
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
        AND d.session_id = target.session_id
        AND d.deleted_at IS NULL AND d.title IS DISTINCT FROM t.title`,
    [...keyParams(key), title],
  );
}

/** Sweep: arm indexed sessions that have neither a brief nor a wakeup (backfill after enabling). */
export async function sweepUnbriefed(db: SqlExecutor, limit: number): Promise<number> {
  const { rows } = await db.query<{ n: number }>(
    `WITH missing AS (
       SELECT d.org_id, d.workspace_id, d.harness, physical.session_id
         FROM sessions.session_directory AS d
         CROSS JOIN LATERAL (
           SELECT p.session_id
             FROM sessions.session_index AS p
            WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
              AND p.harness = d.harness AND p.deleted_at IS NULL
              AND (p.session_id = d.session_id
                   OR coalesce(p.canonical_session_id, p.session_id) = d.session_id)
            ORDER BY p.last_activity_at DESC, p.revision DESC, p.machine_id DESC LIMIT 1
         ) AS physical
        WHERE d.deleted_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM sessions.session_briefs AS b
                           WHERE b.org_id = d.org_id AND b.workspace_id = d.workspace_id
                             AND b.harness = d.harness AND b.session_id = physical.session_id)
          AND NOT EXISTS (SELECT 1 FROM sessions.session_brief_wakeups AS w
                           WHERE w.org_id = d.org_id AND w.workspace_id = d.workspace_id
                             AND w.harness = d.harness AND w.session_id = physical.session_id)
        ORDER BY d.last_activity_at DESC LIMIT $1),
     armed AS (
       INSERT INTO sessions.session_brief_wakeups
         (org_id, workspace_id, harness, session_id, admission, due_at)
       SELECT org_id, workspace_id, harness, session_id, 'sweep', now() FROM missing
       ON CONFLICT DO NOTHING RETURNING 1)
     SELECT count(*)::int AS n FROM armed`,
    [limit],
  );
  return rows[0]?.n ?? 0;
}
