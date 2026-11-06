/**
 * One logical session under two machine ids: a cold export filed under
 * `unknown-origin-*`, and the live client under its own. They never share a
 * manifest, so no takeover fixes them and each looks complete on its own. The
 * export must not count as finished while one exists.
 */
import type { SqlExecutor } from "./queue.js";

export const SESSION_MACHINE_SPLIT = "session_machine_split" as const;

export interface SessionMachineSplit {
  type: typeof SESSION_MACHINE_SPLIT;
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
  machineIds: string[];
}

/** Every session id filed under more than one machine, in one scope. */
export async function detectMachineSplits(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId?: string },
): Promise<SessionMachineSplit[]> {
  const { rows } = await db.query<{
    workspace_id: string;
    harness: string;
    session_id: string;
    machine_ids: string[];
  }>(
    `SELECT workspace_id, harness, session_id,
            array_agg(machine_id ORDER BY machine_id) AS machine_ids
       FROM sessions.session_index
      WHERE org_id = $1 AND ($2::text IS NULL OR workspace_id = $2)
      GROUP BY workspace_id, harness, session_id
     HAVING count(*) > 1
        AND bool_or(machine_id LIKE 'unknown-origin-%')`,
    [scope.orgId, scope.workspaceId ?? null],
  );
  return rows.map((row) => ({
    type: SESSION_MACHINE_SPLIT,
    orgId: scope.orgId,
    workspaceId: row.workspace_id,
    harness: row.harness,
    sessionId: row.session_id,
    machineIds: row.machine_ids,
  }));
}

export const EXPORT_DEFERRED = "export_deferred" as const;

export interface ExportDeferred {
  type: typeof EXPORT_DEFERRED;
  workspaceId: string;
  manifestKey: string;
  reason: string;
}

/**
 * The exporter could not finish this session, e.g. `straddling_session`: a
 * live manifest exists and no signed entry can append to it yet. Repeating
 * keeps the first deferral and reopens one that was resolved.
 */
export async function recordExportDeferred(
  db: SqlExecutor,
  item: {
    orgId: string;
    workspaceId: string;
    manifestKey: string;
    reason: string;
  },
): Promise<void> {
  await db.query(
    `INSERT INTO sessions.export_deferred_sessions
       (org_id, workspace_id, manifest_key, reason)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (org_id, manifest_key) DO UPDATE
       SET reason = excluded.reason, resolved_at = NULL`,
    [item.orgId, item.workspaceId, item.manifestKey, item.reason],
  );
}

/** The session's pre-T0 records landed, e.g. its backfill epoch committed. */
export async function resolveExportDeferred(
  db: SqlExecutor,
  item: { orgId: string; manifestKey: string },
): Promise<void> {
  await db.query(
    `UPDATE sessions.export_deferred_sessions SET resolved_at = now()
      WHERE org_id = $1 AND manifest_key = $2 AND resolved_at IS NULL`,
    [item.orgId, item.manifestKey],
  );
}

export async function detectExportDeferred(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId?: string },
): Promise<ExportDeferred[]> {
  const { rows } = await db.query<{
    workspace_id: string;
    manifest_key: string;
    reason: string;
  }>(
    `SELECT workspace_id, manifest_key, reason
       FROM sessions.export_deferred_sessions
      WHERE org_id = $1 AND ($2::text IS NULL OR workspace_id = $2)
        AND resolved_at IS NULL
      ORDER BY manifest_key`,
    [scope.orgId, scope.workspaceId ?? null],
  );
  return rows.map((row) => ({
    type: EXPORT_DEFERRED,
    workspaceId: row.workspace_id,
    manifestKey: row.manifest_key,
    reason: row.reason,
  }));
}

export type ExportIncomplete = SessionMachineSplit | ExportDeferred;

/** Why the export is not yet authoritative; empty when it is. */
export async function exportCompletionIssues(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId?: string },
): Promise<ExportIncomplete[]> {
  return [
    ...(await detectMachineSplits(db, scope)),
    ...(await detectExportDeferred(db, scope)),
  ];
}

/**
 * The completion check. "Every session exported" is not authoritative while a
 * session's records sit in two places or its pre-T0 records have not landed.
 */
export async function assertExportComplete(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId?: string },
): Promise<void> {
  const issues = await exportCompletionIssues(db, scope);
  if (issues.length > 0)
    throw Object.assign(
      new Error(`${issues.length} sessions keep the export incomplete`),
      { code: "export_incomplete", issues },
    );
}
