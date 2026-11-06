/**
 * `DeadLetterStore` straight over SQL (`pipeline_dead_letters`), for the
 * Postgres queue backend where no PostgREST sits in front of the database. Same
 * table and semantics as `postgrestDeadLetterStore`; here `quarantine` is one
 * upsert, so `dead_count` has no lost-update window, and `claimForReplay` is a
 * conditional UPDATE on the listed version.
 */
import type {
  DeadLetter,
  DeadLetterFilter,
  DeadLetterState,
  DeadLetterStore,
  StageMessage,
} from "./dlq.js";
import { deadLetterId } from "./dlq.js";
import type { PostgresQueueOptions, SqlExecutor } from "./queue.js";
import type { PipelineStage } from "./trace.js";

type Row = {
  id: string;
  stage: PipelineStage;
  trace_id: string;
  workspace_id: string;
  message: StageMessage;
  state: DeadLetterState;
  first_dead_at: string;
  last_dead_at: string;
  dead_count: number;
  replay_count: number;
  last_replay_id: string | null;
  last_replay_at: string | null;
  resolved_at: string | null;
};

const iso = (column: string) =>
  `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS ${column}`;

const COLUMNS = [
  "id",
  "stage",
  "trace_id::text AS trace_id",
  "workspace_id",
  "message",
  "state",
  iso("first_dead_at"),
  iso("last_dead_at"),
  "dead_count",
  "replay_count",
  "last_replay_id::text AS last_replay_id",
  iso("last_replay_at"),
  iso("resolved_at"),
].join(", ");

function fromRow(row: Row): DeadLetter {
  return {
    id: row.id,
    stage: row.stage,
    traceId: row.trace_id,
    workspaceId: row.workspace_id,
    message: row.message,
    state: row.state,
    firstDeadAt: row.first_dead_at,
    lastDeadAt: row.last_dead_at,
    deadCount: row.dead_count,
    replayCount: row.replay_count,
    ...(row.last_replay_id ? { lastReplayId: row.last_replay_id } : {}),
    ...(row.last_replay_at ? { lastReplayAt: row.last_replay_at } : {}),
    ...(row.resolved_at ? { resolvedAt: row.resolved_at } : {}),
  };
}

export function postgresDeadLetterStore(
  sql: SqlExecutor,
  options: PostgresQueueOptions = {},
): DeadLetterStore {
  const schema = options.schema ?? "sessions";
  if (!/^[a-z_][a-z0-9_]*$/.test(schema))
    throw new Error(`bad dead letter schema ${schema}`);
  const letters = `${schema}.pipeline_dead_letters`;

  return {
    async quarantine(message, at) {
      const { rows } = await sql.query<Row>(
        `INSERT INTO ${letters} AS d
           (id, stage, trace_id, org_id, workspace_id, session_id, machine_id,
            message, first_dead_at, last_dead_at)
         VALUES ($1, $2, $3::uuid, $4, $5, $8, $9, $6::jsonb, $7::timestamptz, $7::timestamptz)
         ON CONFLICT (id) DO UPDATE SET
           message = EXCLUDED.message,
           state = 'dead',
           last_dead_at = EXCLUDED.last_dead_at,
           dead_count = d.dead_count + 1,
           resolved_at = NULL,
           session_id = COALESCE(EXCLUDED.session_id, d.session_id),
           machine_id = COALESCE(EXCLUDED.machine_id, d.machine_id)
         RETURNING ${COLUMNS}`,
        [
          deadLetterId(message.stage, message.ctx, message.workKey),
          message.stage,
          message.ctx.traceId,
          message.ctx.orgId,
          message.ctx.workspaceId,
          JSON.stringify(message),
          at.toISOString(),
          message.ctx.sessionId ?? null,
          message.ctx.machineId ?? null,
        ],
      );
      return fromRow(rows[0]!);
    },
    async list(filter: DeadLetterFilter) {
      const params: unknown[] = [];
      const where: string[] = [];
      const bind = (value: unknown) => `$${params.push(value)}`;
      if (filter.stage) where.push(`stage = ${bind(filter.stage)}`);
      if (filter.traceId)
        where.push(`trace_id = ${bind(filter.traceId)}::uuid`);
      if (filter.workspaceId)
        where.push(`workspace_id = ${bind(filter.workspaceId)}`);
      if (filter.replayable) {
        // `dead`, or `replaying` whose claim is older than the lease (or has no
        // claim time at all); every `replaying` letter without a cutoff.
        const cutoff = filter.replayable.leaseCutoff;
        where.push(
          cutoff
            ? `(state = 'dead' OR (state = 'replaying' AND (last_replay_at IS NULL OR last_replay_at < ${bind(cutoff)}::timestamptz)))`
            : `state IN ('dead', 'replaying')`,
        );
      } else if (filter.states) {
        where.push(`state = ANY(${bind(filter.states)}::text[])`);
      }
      const { rows } = await sql.query<Row>(
        `SELECT ${COLUMNS} FROM ${letters}
          ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
          ORDER BY first_dead_at, id
          LIMIT ${bind(filter.limit ?? 100)}`,
        params,
      );
      return rows.map(fromRow);
    },
    async claimForReplay(id, expected, replayId, at) {
      const { rows } = await sql.query(
        `UPDATE ${letters}
            SET state = 'replaying', replay_count = replay_count + 1,
                last_replay_id = $4::uuid, last_replay_at = $5::timestamptz
          WHERE id = $1 AND state = $2 AND replay_count = $3
          RETURNING id`,
        [id, expected.state, expected.replayCount, replayId, at.toISOString()],
      );
      return rows.length === 1;
    },
    async releaseReplay(id, replayId) {
      await sql.query(
        `UPDATE ${letters}
            SET state = 'dead', replay_count = greatest(0, replay_count - 1)
          WHERE id = $1 AND state = 'replaying' AND last_replay_id = $2::uuid`,
        [id, replayId],
      );
    },
    async resolve(id, at) {
      await sql.query(
        `UPDATE ${letters} SET state = 'resolved', resolved_at = $2::timestamptz
          WHERE id = $1 AND state <> 'resolved'`,
        [id, at.toISOString()],
      );
    },
  };
}
