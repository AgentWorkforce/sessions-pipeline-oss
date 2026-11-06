import {
  resolvedPublicTurnSourceSql,
  selectedPublicTurnPredicateSql,
} from "../../shared/legacy-turn-source.ts";
/** Optional legacy evidence export. No reads occur unless includeLegacySources is selected. */
import {
  buildSessionStream,
  type Queryable,
  type DeliveryRecord,
} from "../relayhistory-export.ts";
import {
  boundStrings,
  type SessionUnit,
  type UnitSource,
  type PageHook,
  type GroupActivity,
} from "./neon-reader.ts";
export const LEGACY_TABLES = [
  {
    table: "conversation_turns",
    kind: "legacy_conversation_turn",
    key: "id",
    cast: "uuid",
    time: "ts",
    user: "NULL::text",
  },
  {
    table: "session_links",
    kind: "legacy_session_link",
    key: "id",
    cast: "bigint",
    time: "coalesce(link_ts,updated_at,created_at)",
    user: "NULL::text",
  },
  {
    table: "session_outcomes",
    kind: "legacy_session_outcome",
    key: "commit_sha",
    cast: "text",
    time: "coalesce(shipped_at,ingested_at,created_at)",
    user: "user_id",
  },
] as const;
const scope = "org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4";
/** Per-table scans preserve the source key and microsecond timestamp in the payload. */
export function withLegacySource(
  db: Queryable,
  base: UnitSource,
  onPage: PageHook,
  pageRows = 500,
): UnitSource {
  return {
    fenced: (u, f) => base.fenced(u, f),
    async *lines(unit) {
      let machine: string | undefined,
        userId: string | null = null;
      for await (const line of base.lines(unit)) {
        machine ??= line.machine;
        userId ??= line.userId ?? null;
        yield line;
      }
      for (const spec of LEGACY_TABLES) {
        let after: string | undefined;
        for (;;) {
          const selectedScope =
            spec.table === "conversation_turns"
              ? `t.org_id=$1 AND t.workspace_id=$2 AND t.session_id=$4 AND ${selectedPublicTurnPredicateSql("t", "$3")}`
              : scope;
          // The public source is attribution, while the original overloaded
          // column remains explicit metadata. Content, IDs and turn fields stay exact.
          const attribution =
            spec.table === "conversation_turns"
              ? ",'source',$3::text,'legacy_stored_source',t.source"
              : "";
          const { rows } = await db.query<{
            key: string;
            payload_json: string;
            received_at: string;
            user_id: string | null;
          }>(
            `SELECT ${spec.key}::text AS key,(to_jsonb(t)||jsonb_build_object('${spec.key}',${spec.key}::text${attribution}))::text AS payload_json,
      (${spec.time})::text AS received_at,${spec.user} AS user_id FROM sessions.${spec.table} t
      WHERE ${selectedScope} ${after === undefined ? "" : `AND ${spec.key}>$6::${spec.cast}`} ORDER BY ${spec.key} LIMIT $5`,
            [
              unit.orgId,
              unit.workspaceId,
              unit.source,
              unit.sessionId,
              pageRows,
              ...(after === undefined ? [] : [after]),
            ],
          );
          await onPage(
            rows.length,
            rows.reduce((n, r) => n + r.payload_json.length, 0),
          );
          for (const row of rows) {
            machine ??= "legacy:records";
            userId ??= row.user_id;
            const bounded = boundStrings(row.payload_json);
            const record: DeliveryRecord = {
              kind: spec.kind,
              origin_id: "legacy:" + spec.table,
              record_id: "legacy:" + spec.table + ":" + row.key,
              revision: "1",
              operation: "upsert",
              received_at: row.received_at,
              payload: {},
              payload_json: bounded.text,
            };
            yield {
              machine,
              bytes: buildSessionStream([record]).bytes,
              userId: row.user_id ?? userId ?? "unknown",
              ...(bounded.truncated
                ? {
                    truncated: bounded.truncated,
                    truncatedMaxChars: bounded.maxChars,
                  }
                : {}),
            };
          }
          if (rows.length < pageRows) break;
          after = rows.at(-1)!.key;
        }
      }
      // Even empty tables have an explicit, raw-hash-bound completeness statement.
      const record: DeliveryRecord = {
        kind: "legacy_scope",
        origin_id: "legacy:scope",
        record_id: "legacy:scope:coverage",
        revision: "1",
        operation: "upsert",
        received_at: new Date(unit.lastMs).toISOString(),
        payload: {
          version: 1,
          org_id: unit.orgId,
          workspace_id: unit.workspaceId,
          source: unit.source,
          session_id: unit.sessionId,
          tables: LEGACY_TABLES.map((s) => s.table),
        },
      };
      yield {
        machine: machine ?? "legacy:records",
        userId: userId ?? "unknown",
        bytes: buildSessionStream([record]).bytes,
      };
    },
  };
}
export interface LegacyActivity extends GroupActivity {
  workspace_id: string;
  source: string;
}
const legacyUnion = LEGACY_TABLES.map((s) =>
  s.table === "conversation_turns"
    ? // Empty source is only an unbound coverage sentinel. It is rejected below
      // before a unit/provider is created; it is never a default harness.
      `SELECT t.org_id,t.workspace_id,COALESCE(${resolvedPublicTurnSourceSql("t")},'') AS source,t.session_id,t.ts FROM sessions.conversation_turns t`
    : `SELECT org_id,workspace_id,source,session_id,${s.time} AS ts FROM sessions.${s.table}`,
).join(" UNION ALL ");
function assertBound(rows: Array<{ source: string }>) {
  if (rows.some((r) => typeof r.source !== "string" || !r.source))
    throw new Error("legacy_source_unbound");
}
/** Includes turn/link/outcome-only groups that rollups/catalog discovery cannot see. */
export async function legacyActivityPage(
  db: Queryable,
  after: Pick<
    LegacyActivity,
    "org_id" | "workspace_id" | "source" | "session_id"
  > | null,
  limit = 5000,
): Promise<LegacyActivity[]> {
  const { rows } = await db.query<LegacyActivity>(
    `WITH evidence AS (${legacyUnion})
 SELECT org_id,workspace_id,source,session_id,floor(extract(epoch FROM max(ts))*1000)::bigint::text AS last_ms,'0' AS events
 FROM evidence ${after ? "WHERE (org_id,workspace_id,source,session_id)>($2,$3,$4,$5)" : ""}
 GROUP BY org_id,workspace_id,source,session_id ORDER BY org_id,workspace_id,source,session_id LIMIT $1`,
    after
      ? [
          limit,
          after.org_id,
          after.workspace_id,
          after.source,
          after.session_id,
        ]
      : [limit],
  );
  assertBound(rows);
  return rows;
}
export async function includeLegacyUnits(
  db: Queryable,
  orgId: string,
  sessionId: string,
  units: SessionUnit[],
): Promise<SessionUnit[]> {
  const { rows } = await db.query<{
    workspace_id: string;
    source: string;
    first_ms: string;
    last_ms: string;
  }>(
    `WITH evidence AS (${legacyUnion}) SELECT workspace_id,source,
 floor(extract(epoch FROM min(ts))*1000)::bigint::text AS first_ms,floor(extract(epoch FROM max(ts))*1000)::bigint::text AS last_ms
 FROM evidence WHERE org_id=$1 AND session_id=$2 GROUP BY workspace_id,source`,
    [orgId, sessionId],
  );
  assertBound(rows);
  const result = new Map(
    units.map((u) => [JSON.stringify([u.workspaceId, u.source]), { ...u }]),
  );
  for (const r of rows) {
    const key = JSON.stringify([r.workspace_id, r.source]),
      prior = result.get(key),
      first = Number(r.first_ms),
      last = Number(r.last_ms);
    if (!Number.isFinite(first) || !Number.isFinite(last))
      throw new Error("legacy evidence clock invalid");
    if (prior) {
      prior.firstMs = Math.min(prior.firstMs, first);
      prior.lastMs = Math.max(prior.lastMs, last);
    } else
      result.set(key, {
        orgId,
        sessionId,
        workspaceId: r.workspace_id,
        source: r.source,
        firstMs: first,
        lastMs: last,
        machineIds: [],
        totals: {},
      });
  }
  return [...result.values()];
}
