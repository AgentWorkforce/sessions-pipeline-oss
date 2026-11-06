import type {
  ClickHouseClient,
  QueryParams,
} from "../session-pipeline/vendor/search-index/clickhouse.ts";
import {
  featureVersion,
  readFeatureSnapshots,
  type FeatureReader,
  type FeatureReference,
  type FeatureSnapshot,
} from "./source.ts";
import { EVENT_FEATURES_TABLE } from "./event-schema.ts";
export class EventFeatureError extends Error {
  constructor(readonly reason: string) {
    super("Event feature publication refused");
    this.name = "EventFeatureError";
  }
}
export const EVENT_FEATURE_ROW_MAX_BYTES = 16 * 1024 * 1024;
const fail = (reason: string): never => {
  throw new EventFeatureError(reason);
};
const stamp = (v: unknown): v is string =>
  typeof v === "string" &&
  /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(v) &&
  Number.isFinite(Date.parse(v));
const time = (v: string) => v.replace("T", " ").replace(/Z$/, "");
const nullable = (v: unknown) => v === null || typeof v === "string";
const fields = [
  "orgId",
  "workspaceId",
  "source",
  "sessionId",
  "machineId",
  "kind",
  "eventId",
  "ts",
] as const;
export function eventFeatureRow(
  snapshot: FeatureSnapshot,
  baselineH?: string,
): Record<string, unknown> {
  const { reference: r, row: p } = snapshot,
    k = r.key;
  if (
    r.kind !== "event" ||
    Object.keys(k).length !== fields.length ||
    fields.some((f) => typeof k[f] !== "string" || !k[f]) ||
    !stamp(k.ts) ||
    !/^[a-f0-9]{64}$/.test(r.sourceHash) ||
    typeof r.deleted !== "boolean"
  )
    return fail("reference_invalid");
  const base = {
    org_id: k.orgId,
    workspace_id: k.workspaceId,
    source: k.source,
    session_id: k.sessionId,
    machine_id: k.machineId,
    kind: k.kind,
    event_id: k.eventId,
    ts: time(k.ts!),
    version: featureVersion(baselineH ?? r.revision),
    source_revision: r.revision,
    source_hash: r.sourceHash,
    deleted: r.deleted ? 1 : 0,
  };
  featureVersion(r.revision);
  if (r.deleted) {
    if (p !== null) return fail("tombstone_invalid");
    return {
      ...base,
      ingested_at: "1970-01-01 00:00:00.000000",
      user_id: "",
      actor_name: null,
      actor_role: null,
      project_id: null,
      project_folded: null,
      task_title: null,
      task_title_folded: null,
      content: null,
      content_folded: null,
      task_ref_text: "{}",
      task_ref_text_folded: "{}",
      tags_json: "[]",
      embedding: [],
      embedding_model: null,
      embedding_dim: 0,
      content_hash: null,
    };
  }
  if (
    !p ||
    p.org_id !== k.orgId ||
    p.workspace_id !== k.workspaceId ||
    p.source !== k.source ||
    p.session_id !== k.sessionId ||
    p.machine_id !== k.machineId ||
    p.kind !== k.kind ||
    p.event_id !== k.eventId ||
    p.ts !== k.ts ||
    !stamp(p.ingested_at) ||
    typeof p.user_id !== "string" ||
    ![
      "actor_name",
      "actor_role",
      "project_id",
      "project_folded",
      "task_title",
      "task_title_folded",
      "content",
      "content_folded",
      "embedding_model",
      "content_hash",
    ].every((f) => nullable(p[f])) ||
    typeof p.task_ref_text !== "string" ||
    typeof p.task_ref_text_folded !== "string" ||
    (p.embedding !== null && typeof p.embedding !== "string")
  )
    return fail("payload_invalid");
  for (const [raw, folded] of [
    ["project_id", "project_folded"],
    ["task_title", "task_title_folded"],
    ["content", "content_folded"],
  ]) {
    if ((p[raw!] === null) !== (p[folded!] === null))
      return fail("payload_invalid");
  }
  let embedding: unknown;
  try {
    embedding = p.embedding === null ? [] : JSON.parse(p.embedding as string);
  } catch {
    return fail("vector_invalid");
  }
  if (
    !Array.isArray(embedding) ||
    (embedding.length !== 1536 &&
      !(p.embedding === null && embedding.length === 0)) ||
    embedding.some(
      (v) =>
        typeof v !== "number" ||
        !Number.isFinite(v) ||
        !Number.isFinite(Math.fround(v)),
    )
  )
    return fail("vector_invalid");
  const row = {
    ...base,
    ingested_at: time(p.ingested_at),
    user_id: p.user_id,
    actor_name: p.actor_name,
    actor_role: p.actor_role,
    project_id: p.project_id,
    project_folded: p.project_folded,
    task_title: p.task_title,
    task_title_folded: p.task_title_folded,
    content: p.content,
    content_folded: p.content_folded,
    task_ref_text: p.task_ref_text,
    task_ref_text_folded: p.task_ref_text_folded,
    tags_json: JSON.stringify(p.tags),
    embedding,
    embedding_model: p.embedding_model,
    embedding_dim: embedding.length,
    content_hash: p.content_hash,
  };
  if (
    Buffer.byteLength(JSON.stringify(row), "utf8") > EVENT_FEATURE_ROW_MAX_BYTES
  )
    return fail("payload_bound");
  return row;
}
const same = (a: FeatureReference, b: FeatureReference) =>
  a.kind === b.kind &&
  a.revision === b.revision &&
  a.sourceHash === b.sourceHash &&
  a.deleted === b.deleted &&
  JSON.stringify(Object.entries(a.key).sort()) ===
    JSON.stringify(Object.entries(b.key).sort());
const keyFields = [
  "org_id",
  "workspace_id",
  "source",
  "session_id",
  "machine_id",
  "kind",
  "event_id",
  "ts",
] as const;
export async function publishEventFeatureSnapshots(
  deps: { db: FeatureReader; ch: ClickHouseClient; signal?: AbortSignal },
  snapshots: FeatureSnapshot[],
  options: { baselineH?: string } = {},
): Promise<FeatureReference[]> {
  if (snapshots.length > 200) return fail("batch_invalid");
  if (!snapshots.length) return [];
  const rows = snapshots.map((s) => eventFeatureRow(s, options.baselineH)),
    seen = new Set<string>();
  for (const row of rows) {
    const key = JSON.stringify(keyFields.map((f) => row[f]));
    if (seen.has(key)) return fail("duplicate");
    seen.add(key);
  }
  deps.signal?.throwIfAborted();
  try {
    await deps.ch.insert(EVENT_FEATURES_TABLE, rows);
    const erased = rows.filter((r) => r.deleted === 1);
    if (erased.length) {
      const params: QueryParams = {};
      const where = erased.map((r, n) => {
        const terms = keyFields.map((f, i) => {
          const name = "k" + n + "_" + i;
          params[name] = r[f] as string;
          return f === "ts"
            ? f + "=parseDateTime64BestEffort({" + name + ":String},6,'UTC')"
            : f + "={" + name + ":String}";
        });
        params["v" + n] = r.version as string;
        terms.push("version<{v" + n + ":UInt64}");
        return "(" + terms.join(" AND ") + ")";
      });
      await deps.ch.command(
        "DELETE FROM " + EVENT_FEATURES_TABLE + " WHERE " + where.join(" OR "),
        {
          params,
          settings: { lightweight_deletes_sync: 2, max_query_size: 1048576 },
        },
      );
    }
    deps.signal?.throwIfAborted();
    const current = await readFeatureSnapshots(
      deps.db,
      snapshots.map((s) => s.reference),
    );
    if (
      current.length !== snapshots.length ||
      current.some((s, n) => !same(s.reference, snapshots[n]!.reference))
    )
      return fail("source_changed");
    return snapshots.map((s) => s.reference);
  } catch (error) {
    if (error instanceof EventFeatureError) throw error;
    throw new EventFeatureError("publication_failed");
  }
}
