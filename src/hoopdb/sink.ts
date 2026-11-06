/**
 * The pipeline event sink over plain SQL. Upstream only has PostgREST (hosted)
 * or log-only; self-host needs rows in `sessions.pipeline_events` so
 * `GET /v1/pipeline/traces/:id` works and the smoke test can follow a session.
 */
import { toRow, fromRow, MAX_TRACE_EVENTS, type PipelineEvent, type PipelineEventSink, type TraceScope, type TraceEvents } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";

export function sqlPipelineEventSink(db: Pick<HoopDb, "query">): PipelineEventSink {
  return {
    kind: "sql",
    async record(events: PipelineEvent[]) {
      for (const event of events) {
        const r = toRow(event);
        // No unique constraint upstream (partitioned table); dedupe by key at write time.
        await db.query(
          `INSERT INTO sessions.pipeline_events
             (trace_id, workspace_id, org_id, machine_id, harness, session_id, stage, status, attempt,
              error, attrs, at, duration_ms, idempotency_key)
           SELECT $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12::timestamptz, $13, $14
           WHERE NOT EXISTS (SELECT 1 FROM sessions.pipeline_events WHERE idempotency_key = $14)`,
          [
            r.trace_id, r.workspace_id, r.org_id, r.machine_id, r.harness, r.session_id, r.stage, r.status, r.attempt,
            r.error === null ? null : JSON.stringify(r.error), JSON.stringify(r.attrs ?? {}), r.at, r.duration_ms, r.idempotency_key,
          ],
        );
      }
    },
    async byTrace(scope: TraceScope, traceId: string): Promise<TraceEvents> {
      const { rows } = await db.query<Parameters<typeof fromRow>[0]>(
        `SELECT trace_id, workspace_id, org_id, machine_id, harness, session_id, stage, status, attempt,
                error, attrs, at, duration_ms, idempotency_key
           FROM sessions.pipeline_events
          WHERE org_id = $1 AND workspace_id = $2 AND trace_id = $3::uuid
          ORDER BY at ASC
          LIMIT ${MAX_TRACE_EVENTS + 1}`,
        [scope.orgId, scope.workspaceId, traceId],
      );
      const truncated = rows.length > MAX_TRACE_EVENTS;
      return { events: rows.slice(0, MAX_TRACE_EVENTS).map((row) => fromRow({ ...row, at: new Date(row.at as unknown as string).toISOString() })), truncated };
    },
  };
}
