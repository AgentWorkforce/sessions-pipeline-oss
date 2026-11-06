/**
 * Supabase liveness checks over `sessions.session_index` (the per-machine
 * pointer table; deletion is a tombstone, `deleted_at`). Runs as the dedicated
 * `bulls96_read_api` role, which may SELECT only the id, `created_at` and
 * `deleted_at` columns (supabase/bootstrap/session_read_api_role.sql): no
 * content, no metadata, no writes. Tenancy is enforced twice: every statement
 * binds org and workspace from verified auth, and the role's RLS policy admits
 * only rows of the org and workspace this transaction set with set_config
 * (`bulls96_read.org_id`, `bulls96_read.workspace_id`). Each check is one
 * READ ONLY transaction, so the settings never outlive it.
 *
 * A session with no pointer row is NOT treated as erased: history backfilled
 * into ClickHouse can predate its pointers. Erasure is "every pointer row is
 * tombstoned", which is exactly what the eraser and the pipeline write.
 */
import type { Liveness, SessionKey } from "./page.ts";
import { keyOf } from "./page.ts";

export interface SqlQuery {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

/** Runs `work` in one READ ONLY transaction with the tenant settings bound. */
export type TenantRunner = <T>(orgId: string, workspaceId: string, work: (q: SqlQuery) => Promise<T>) => Promise<T>;

export const TENANT_SETTINGS_SQL =
  "SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)";

/** A TenantRunner over a pg-style pool (connect/release clients). */
export function poolTenantRunner(pool: {
  connect(): Promise<{ query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>; release(): void }>;
}): TenantRunner {
  return async (orgId, workspaceId, work) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN READ ONLY");
      await client.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      const result = await work({
        async query<R>(text: string, params?: unknown[]) {
          const r = await client.query(text, params);
          return { rows: r.rows as R[] };
        },
      });
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };
}

export const ERASED_SQL = `SELECT harness, session_id
  FROM sessions.session_index
 WHERE org_id = $1 AND workspace_id = $2
   AND (harness, session_id) IN (SELECT h, s FROM unnest($3::text[], $4::text[]) AS k(h, s))
 GROUP BY harness, session_id
HAVING bool_and(deleted_at IS NOT NULL)`;

export const SESSION_START_SQL = `SELECT count(*)::int AS pointers,
       (count(*) FILTER (WHERE deleted_at IS NULL))::int AS live,
       (extract(epoch FROM min(created_at) FILTER (WHERE deleted_at IS NULL)) * 1000)::bigint AS created_at_ms
  FROM sessions.session_index
 WHERE org_id = $1 AND workspace_id = $2 AND session_id = $3
   AND ($4::text IS NULL OR harness = $4)`;

export const LIVE_POINTER_SESSIONS_SQL = `SELECT DISTINCT session_id
  FROM sessions.session_index
 WHERE org_id = $1 AND workspace_id = $2 AND deleted_at IS NULL
   AND session_id = ANY($3::text[])`;

export function sqlLiveness(run: TenantRunner): Liveness {
  return {
    async erased(orgId, workspaceId, keys: SessionKey[]) {
      if (keys.length === 0) return new Set();
      const { rows } = await run(orgId, workspaceId, (q) =>
        q.query<{ harness: string; session_id: string }>(ERASED_SQL, [
          orgId,
          workspaceId,
          keys.map((k) => k.source),
          keys.map((k) => k.sessionId),
        ]),
      );
      return new Set(rows.map((r) => keyOf({ source: r.harness, sessionId: r.session_id })));
    },
    async livePointerSessions(orgId, workspaceId, sessionIds) {
      if (sessionIds.length === 0) return new Set();
      const { rows } = await run(orgId, workspaceId, (q) =>
        q.query<{ session_id: string }>(LIVE_POINTER_SESSIONS_SQL, [orgId, workspaceId, sessionIds]),
      );
      return new Set(rows.map((r) => r.session_id));
    },
    async sessionStart(orgId, workspaceId, sessionId, source) {
      const { rows } = await run(orgId, workspaceId, (q) =>
        q.query<{ pointers: number; live: number; created_at_ms: string | number | null }>(SESSION_START_SQL, [
          orgId,
          workspaceId,
          sessionId,
          source ?? null,
        ]),
      );
      const r = rows[0];
      return {
        pointers: Number(r?.pointers ?? 0),
        live: Number(r?.live ?? 0),
        createdAtMs: r?.created_at_ms === null || r?.created_at_ms === undefined ? null : Number(r.created_at_ms),
      };
    },
  };
}
