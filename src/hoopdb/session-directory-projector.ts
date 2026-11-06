/**
 * Projects one logical session (org, workspace, harness, session) from its physical
 * `sessions.session_index` pointer rows into `sessions.session_directory`. Called by
 * the HoopDb indexer after every pointer upsert and pointer tombstone.
 *
 * V1 precedence (approved; provenance in docs/session-search-index-contract.md,
 * "Projector precedence"):
 *   - created_at: stable historical: the earliest created_at ever projected, never
 *     moved later (not even when the earliest pointer is tombstoned);
 *   - the latest live pointer wins every counter and its non-null scalar metadata.
 *     Project/repository/branch fall back to the complete raw-record catalog when the
 *     pointer has no value. Counts are not summed across machines and last_activity_at
 *     is the winning pointer's;
 *   - deterministic ties: latest last_activity_at, then highest revision, then
 *     highest machine_id;
 *   - machine_ids / user_ids and the membership tables are the union of LIVE pointers,
 *     each membership row carrying the directory row's last_activity_at;
 *   - tombstone when no live pointer remains (the trigger clears short_summary and
 *     search_text; memberships are deleted);
 *   - resurrection only for a genuinely new pointer revision (decision 0002: a deleted
 *     upload or pointer never comes back): a live pointer whose revision is strictly
 *     above its machine's resurrection fence (recorded when the row was tombstoned), or
 *     from a machine with no fence. Re-sent, stale or un-deleted revisions stay fenced.
 *     The row comes back with the historical created_at and no summary until Bob
 *     Costas writes one;
 *   - task_title, agent_name and short_summary are not in session_index and are kept
 *     as already projected (short_summary is written only by writeShortSummary);
 *   - the catalog dimensions (agent names, catalog models, project key, relationships)
 *     are written only by writeDirectoryCatalog. Database triggers (20261101050000) keep
 *     every companion's last_activity_at equal to the row's in the row's own transaction,
 *     and on tombstone or delete remove the session's agent rows and its edges as parent
 *     and as child; the tombstone trigger clears the catalog columns.
 *   - a model-written brief title (sessions.session_briefs, src/session-brief) wins over
 *     the latest pointer's title while the brief exists. Deleting a pointer deletes the
 *     brief (trigger), so the pointer title returns on the next projection.
 *
 * search_text is always rebuilt with sessions.session_directory_search_text, so the
 * scrubbed short_summary stays part of it. The rollout marker is never set here.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";

export async function projectSessionDirectory(
  db: SqlExecutor,
  key: SessionDirectoryKey,
): Promise<{ projected: boolean; tombstoned: boolean }> {
  const params = [key.orgId, key.workspaceId, key.harness, key.sessionId];
  const { rows } = await db.query<{ projected: boolean }>(
    `WITH live AS (
       SELECT * FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4
          AND deleted_at IS NULL
     ), latest AS (
       SELECT * FROM live ORDER BY last_activity_at DESC, revision DESC, machine_id DESC LIMIT 1
     ), identity AS (
       SELECT min(coalesce(canonical_session_id, session_id)) AS canonical_session_id
         FROM live
       HAVING count(DISTINCT coalesce(canonical_session_id, session_id)) = 1
     ), target AS (
       -- Do not create a duplicate while a legacy physical-id row is still live. A
       -- controlled re-projection removes that row first; new sessions use the source id.
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
     ), brief AS (
       SELECT title FROM sessions.session_briefs
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4
          AND title IS NOT NULL
     ), agg AS (
       SELECT min(created_at) AS created_at, max(last_activity_at) AS last_activity_at,
              coalesce(array_agg(DISTINCT coalesce(canonical_machine_id, machine_id)
                                 ORDER BY coalesce(canonical_machine_id, machine_id)), '{}') AS machine_ids,
              coalesce(array_agg(DISTINCT user_id ORDER BY user_id)
                       FILTER (WHERE user_id <> 'unknown'), '{}') AS user_ids
         FROM live
     )
     INSERT INTO sessions.session_directory AS d
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
        title, project_id, git_repo, git_branch, model, state,
        event_count, input_tokens, output_tokens, cost_usd_micros,
        machine_ids, user_ids, search_text, projected_at)
     SELECT $1, $2, $3, t.session_id, agg.created_at, agg.last_activity_at,
            coalesce((SELECT title FROM brief), l.title), l.project_id, l.git_repo, l.git_branch,
            l.model, l.state,
            l.event_count, l.input_tokens, l.output_tokens, l.cost_usd_micros,
            agg.machine_ids, agg.user_ids,
            sessions.session_directory_search_text(
              coalesce((SELECT title FROM brief), l.title), NULL, NULL, l.project_id,
              l.git_repo, l.git_branch, l.model, NULL),
            now()
       FROM latest AS l CROSS JOIN agg CROSS JOIN target AS t
     ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET
       created_at = LEAST(d.created_at, excluded.created_at),
       deleted_at = NULL,
       last_activity_at = excluded.last_activity_at,
       title = excluded.title,
       project_id = coalesce(excluded.project_id, d.project_id),
       git_repo = coalesce(excluded.git_repo, d.git_repo),
       git_branch = coalesce(excluded.git_branch, d.git_branch),
       model = excluded.model,
       state = excluded.state,
       event_count = CASE WHEN d.event_projection_version > 0 THEN d.event_count ELSE excluded.event_count END,
       input_tokens = CASE WHEN d.event_projection_version > 0 THEN d.input_tokens ELSE excluded.input_tokens END,
       output_tokens = CASE WHEN d.event_projection_version > 0 THEN d.output_tokens ELSE excluded.output_tokens END,
       cost_usd_micros = CASE WHEN d.event_projection_version > 0 THEN d.cost_usd_micros ELSE excluded.cost_usd_micros END,
       machine_ids = excluded.machine_ids,
       user_ids = excluded.user_ids,
       search_text = sessions.session_directory_search_text(
         excluded.title, d.task_title, d.agent_name, coalesce(excluded.project_id, d.project_id),
         coalesce(excluded.git_repo, d.git_repo), coalesce(excluded.git_branch, d.git_branch),
         excluded.model, d.short_summary),
       projected_at = now()
     WHERE d.deleted_at IS NULL
        OR EXISTS (
          -- Resurrection only for a genuinely new pointer revision: live, and strictly
          -- above its machine's fence (or a machine with no fence at all).
          SELECT 1 FROM sessions.session_index AS later
           WHERE later.org_id = d.org_id AND later.workspace_id = d.workspace_id
             AND later.harness = d.harness AND later.session_id = $4
             AND later.deleted_at IS NULL
             AND NOT EXISTS (
               SELECT 1 FROM sessions.session_directory_resurrection_fences AS f
                WHERE f.org_id = later.org_id AND f.workspace_id = later.workspace_id
                  AND f.harness = later.harness AND f.session_id = d.session_id
                  AND f.machine_id = coalesce(later.canonical_machine_id, later.machine_id)
                  AND f.revision >= later.revision))
     RETURNING true AS projected`,
    params,
  );
  if (rows.length === 0) {
    // No live pointer left (the last machine manifest was tombstoned), or the row is
    // already a tombstone. Tombstone it (the trigger clears short_summary and
    // search_text), empty the membership arrays and delete the membership rows. A row
    // that still has a live pointer is a tombstone being re-sent and is left alone.
    const gone = await db.query<{ tombstoned: boolean }>(
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
          SET deleted_at = coalesce(d.deleted_at, now()),
              machine_ids = '{}', user_ids = '{}', projected_at = now()
         FROM target AS t
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
          AND d.session_id = t.session_id
          AND NOT EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id AND p.harness = d.harness
               AND p.session_id = $4 AND p.deleted_at IS NULL)
        RETURNING true AS tombstoned`,
      params,
    );
    if (gone.rows.length === 0) return { projected: false, tombstoned: false };
    for (const table of [
      "session_directory_users",
      "session_directory_machines",
    ] as const) {
      await db.query(
        `DELETE FROM sessions.${table} AS m
          USING sessions.session_directory AS d
          WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
            AND d.deleted_at IS NOT NULL
            AND m.org_id = d.org_id AND m.workspace_id = d.workspace_id
            AND m.harness = d.harness AND m.session_id = d.session_id
            AND (d.session_id = $4 OR EXISTS (
              SELECT 1 FROM sessions.session_index AS p
               WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
                 AND p.harness = d.harness AND p.session_id = $4
                 AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))`,
        params,
      );
    }
    return { projected: false, tombstoned: true };
  }
  // Membership companions: exactly the live pointers' users and machines.
  for (const [table, column] of [
    ["session_directory_users", "user_id"],
    ["session_directory_machines", "machine_id"],
  ] as const) {
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
       DELETE FROM sessions.${table} AS m USING target AS t
        WHERE m.org_id = $1 AND m.workspace_id = $2 AND m.harness = $3 AND m.session_id = t.session_id
          AND NOT EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = m.org_id AND p.workspace_id = m.workspace_id AND p.harness = m.harness
               AND p.session_id = $4 AND p.deleted_at IS NULL
               AND ${column === "user_id" ? "p.user_id <> 'unknown' AND p.user_id" : "coalesce(p.canonical_machine_id, p.machine_id)"} = m.${column})`,
      params,
    );
    // last_activity_at is the logical session's, not the member's own: the search RPC
    // drives membership filters from this index and pages with the directory keyset,
    // so the two orders must agree.
    await db.query(
      `WITH target AS (
         SELECT d.session_id, d.last_activity_at
           FROM sessions.session_directory AS d
          WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
            AND d.deleted_at IS NULL
            AND (d.session_id = $4 OR EXISTS (
              SELECT 1 FROM sessions.session_index AS p
               WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
                 AND p.harness = d.harness AND p.session_id = $4
                 AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))
          ORDER BY (d.session_id <> $4) DESC LIMIT 1
       )
       INSERT INTO sessions.${table} (org_id, workspace_id, harness, session_id, ${column}, last_activity_at)
       SELECT DISTINCT p.org_id, p.workspace_id, p.harness, t.session_id,
              ${column === "user_id" ? "p.user_id" : "coalesce(p.canonical_machine_id, p.machine_id)"},
              t.last_activity_at
         FROM sessions.session_index AS p
         CROSS JOIN target AS t
        WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.harness = $3
          AND p.session_id = $4 AND p.deleted_at IS NULL
          ${column === "user_id" ? "AND p.user_id <> 'unknown'" : ""}
       ON CONFLICT (org_id, workspace_id, harness, session_id, ${column})
       DO UPDATE SET last_activity_at = excluded.last_activity_at`,
      params,
    );
  }
  return { projected: true, tombstoned: false };
}
