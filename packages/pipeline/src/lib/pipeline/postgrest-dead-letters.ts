/**
 * `DeadLetterStore` over PostgREST (`pipeline_dead_letters`), beside the
 * PostgREST event sink. State transitions are conditional PATCHes filtered on the
 * listed version (state and `replay_count`), so `claimForReplay` is a
 * compare-and-set: two replays racing on one letter see exactly one returned
 * row between them.
 *
 * `quarantine` reads, then inserts or does a conditional PATCH on the version it
 * read, and never writes replay fields back, so a concurrent replay claim is
 * never undone. `fetcher` is injectable.
 */
import type {
  DeadLetter,
  DeadLetterFilter,
  DeadLetterState,
  DeadLetterStore,
  StageMessage,
} from "./dlq.js";
import { deadLetterId } from "./dlq.js";
import { STORE_TIMEOUT_MS } from "./events.js";
import type { PipelineStage } from "./trace.js";

type Row = {
  id: string;
  stage: PipelineStage;
  trace_id: string;
  org_id: string | null;
  workspace_id: string;
  session_id?: string | null;
  machine_id?: string | null;
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

/** Read-then-compare-and-set passes before `quarantine` gives up (the caller retries). */
const QUARANTINE_ATTEMPTS = 4;

export function postgrestDeadLetterStore(options: {
  url: string;
  token: string;
  schema?: string;
  table?: string;
  fetcher?: typeof fetch;
}): DeadLetterStore {
  const base = `${options.url.replace(/\/+$/, "")}/${options.table ?? "pipeline_dead_letters"}`;
  const schema = options.schema ?? "sessions";
  const send = options.fetcher ?? fetch;
  const headers = {
    apikey: options.token,
    Authorization: `Bearer ${options.token}`,
    "Content-Type": "application/json",
    "Accept-Profile": schema,
    "Content-Profile": schema,
  };
  const call = async (
    query: URLSearchParams,
    init: RequestInit = {},
  ): Promise<Row[]> => {
    const response = await send(`${base}?${query}`, {
      ...init,
      headers: { ...headers, ...(init.headers as Record<string, string>) },
      // Every store call is on the queue path (resolve runs after each success),
      // so a stalled store must fail fast rather than hold the batch.
      signal: AbortSignal.timeout(STORE_TIMEOUT_MS),
    });
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`dead letter store request failed: ${response.status}`);
    }
    const text = await response.text();
    return text ? (JSON.parse(text) as Row[]) : [];
  };
  const byId = (id: string) => new URLSearchParams({ id: `eq.${id}` });

  return {
    async quarantine(message, at) {
      const id = deadLetterId(message.stage, message.ctx, message.workKey);
      // PostgREST has no increment, so a death is a compare-and-set on the
      // version just read. It never writes the replay fields back, so a replay
      // that claims the letter in between keeps its claim (and its count).
      for (let attempt = 0; attempt < QUARANTINE_ATTEMPTS; attempt += 1) {
        const [existing] = await call(byId(id));
        if (!existing) {
          const row: Row = {
            id,
            stage: message.stage,
            trace_id: message.ctx.traceId,
            org_id: message.ctx.orgId,
            workspace_id: message.ctx.workspaceId,
            session_id: message.ctx.sessionId ?? null,
            machine_id: message.ctx.machineId ?? null,
            message,
            state: "dead",
            first_dead_at: at.toISOString(),
            last_dead_at: at.toISOString(),
            dead_count: 1,
            replay_count: 0,
            last_replay_id: null,
            last_replay_at: null,
            resolved_at: null,
          };
          // A plain insert: losing a race to another death is a conflict,
          // and the next pass updates the row that won.
          const inserted = await call(new URLSearchParams(), {
            method: "POST",
            headers: { Prefer: "return=representation" },
            body: JSON.stringify(row),
          }).catch(() => undefined);
          if (inserted?.[0]) return fromRow(inserted[0]);
          continue;
        }
        const query = byId(id);
        query.set("dead_count", `eq.${existing.dead_count}`);
        query.set("replay_count", `eq.${existing.replay_count}`);
        query.set("state", `eq.${existing.state}`);
        const [saved] = await call(query, {
          method: "PATCH",
          headers: { Prefer: "return=representation" },
          body: JSON.stringify({
            message,
            state: "dead",
            last_dead_at: at.toISOString(),
            dead_count: existing.dead_count + 1,
            resolved_at: null,
            // A later death can know the session/machine an earlier one did not.
            ...(message.ctx.sessionId
              ? { session_id: message.ctx.sessionId }
              : {}),
            ...(message.ctx.machineId
              ? { machine_id: message.ctx.machineId }
              : {}),
          }),
        });
        if (saved) return fromRow(saved);
      }
      throw new Error("dead letter changed while quarantining; retry");
    },
    async list(filter: DeadLetterFilter) {
      const limit = filter.limit ?? 100;
      const base = () => {
        const query = new URLSearchParams({
          order: "first_dead_at.asc",
          limit: String(limit),
        });
        if (filter.stage) query.set("stage", `eq.${filter.stage}`);
        if (filter.traceId) query.set("trace_id", `eq.${filter.traceId}`);
        if (filter.workspaceId)
          query.set("workspace_id", `eq.${filter.workspaceId}`);
        return query;
      };
      if (filter.replayable) {
        // Eligibility is a filter in the query, ahead of the limit: `dead`, and
        // `replaying` whose claim is older than the lease. Two indexed queries
        // merged, rather than an `or=` the stand-in tests would also need.
        const dead = base();
        dead.set("state", "eq.dead");
        const expired = base();
        expired.set("state", "eq.replaying");
        if (filter.replayable.leaseCutoff)
          expired.set("last_replay_at", `lt.${filter.replayable.leaseCutoff}`);
        // A `replaying` row with no claim time has no lease to wait out; `lt`
        // never matches NULL, so it gets its own query.
        const undated = base();
        undated.set("state", "eq.replaying");
        undated.set("last_replay_at", "is.null");
        const rows = [
          ...(await call(dead)),
          ...(await call(expired)),
          ...(filter.replayable.leaseCutoff ? await call(undated) : []),
        ];
        return rows
          .map(fromRow)
          .sort((a, b) => a.firstDeadAt.localeCompare(b.firstDeadAt))
          .slice(0, limit);
      }
      const query = base();
      if (filter.states) query.set("state", `in.(${filter.states.join(",")})`);
      return (await call(query)).map(fromRow);
    },
    async claimForReplay(id, expected, replayId, at) {
      // One conditional PATCH on the listed version: no read-then-write window.
      const query = byId(id);
      query.set("state", `eq.${expected.state}`);
      query.set("replay_count", `eq.${expected.replayCount}`);
      const claimed = await call(query, {
        method: "PATCH",
        headers: { Prefer: "return=representation" },
        body: JSON.stringify({
          state: "replaying",
          replay_count: expected.replayCount + 1,
          last_replay_id: replayId,
          last_replay_at: at.toISOString(),
        }),
      });
      return claimed.length === 1;
    },
    async releaseReplay(id, replayId) {
      const [current] = await call(byId(id));
      if (current?.state !== "replaying" || current.last_replay_id !== replayId)
        return;
      const query = byId(id);
      query.set("state", "eq.replaying");
      query.set("last_replay_id", `eq.${replayId}`);
      await call(query, {
        method: "PATCH",
        headers: { Prefer: "return=minimal" },
        body: JSON.stringify({
          state: "dead",
          replay_count: Math.max(0, current.replay_count - 1),
        }),
      });
    },
    async resolve(id, at) {
      const query = byId(id);
      query.set("state", "neq.resolved");
      await call(query, {
        method: "PATCH",
        headers: { Prefer: "return=minimal" },
        body: JSON.stringify({
          state: "resolved",
          resolved_at: at.toISOString(),
        }),
      });
    },
  };
}
