/** Portable readonly-source brief copier; caller owns durable lease/checkpoint.
 * No environment reads, provider requests, clients or resources are created. */
import type {
  ClickHouseClient,
  QueryParams,
} from "../session-pipeline/vendor/search-index/clickhouse.ts";
import {
  featureVersion,
  readFeatureSnapshots,
  type FeatureReader,
  type FeatureSnapshot,
  type FeatureReference,
} from "./source.ts";
import { BRIEF_FEATURES_TABLE } from "./brief-schema.ts";
export class BriefFeatureError extends Error {
  constructor(readonly reason: string) {
    super("Brief feature publication refused");
    this.name = "BriefFeatureError";
  }
}
const fail = (r: string): never => {
  throw new BriefFeatureError(r);
};
const stamp = (v: unknown): v is string =>
  typeof v === "string" &&
  /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(v) &&
  Number.isFinite(Date.parse(v));
export function briefFeatureRow(
  s: FeatureSnapshot,
  baselineH?: string,
): Record<string, unknown> {
  const r = s.reference,
    k = r.key,
    p = s.row;
  if (
    r.kind !== "brief" ||
    Object.keys(k).length !== 4 ||
    !["orgId", "source", "sessionId", "workspaceId"].every((f) => f in k) ||
    typeof k.orgId !== "string" ||
    !k.orgId ||
    typeof k.sessionId !== "string" ||
    !k.sessionId ||
    typeof k.source !== "string" ||
    !k.source ||
    !(typeof k.workspaceId === "string" || k.workspaceId === null) ||
    typeof r.deleted !== "boolean" ||
    !/^[a-f0-9]{64}$/.test(r.sourceHash)
  )
    return fail("reference_invalid");
  const version = featureVersion(baselineH ?? r.revision);
  featureVersion(r.revision);
  // An ineligible baseline can have no frozen workspace. It never had a routable
  // live CH feature; a null-workspace tombstone remains isolated under empty key.
  const base = {
    org_id: k.orgId,
    workspace_id: k.workspaceId ?? "",
    source: k.source,
    session_id: k.sessionId,
    version,
    source_revision: r.revision,
    source_hash: r.sourceHash,
    deleted: r.deleted ? 1 : 0,
  };
  if (r.deleted) {
    if (p !== null) return fail("tombstone_invalid");
    return {
      ...base,
      title: "",
      description: "",
      title_folded: "",
      description_folded: "",
      brief_completed_at: "1970-01-01 00:00:00.000000",
      embedding: [],
      embedding_model: "",
      embedding_dim: 0,
      content_hash: "",
    };
  }
  if (
    !p ||
    p.org_id !== k.orgId ||
    p.workspace_id !== k.workspaceId ||
    p.source !== k.source ||
    p.session_id !== k.sessionId ||
    !k.workspaceId ||
    typeof p.title !== "string" ||
    typeof p.description !== "string" ||
    typeof p.title_folded !== "string" ||
    typeof p.description_folded !== "string" ||
    !stamp(p.brief_completed_at) ||
    (p.embedding !== null && typeof p.embedding !== "string") ||
    (p.embedding_model !== null && typeof p.embedding_model !== "string") ||
    typeof p.content_hash !== "string"
  )
    return fail("payload_invalid");
  let vector: unknown;
  try {
    vector = p.embedding === null ? [] : JSON.parse(p.embedding);
  } catch {
    return fail("vector_invalid");
  }
  if (
    !Array.isArray(vector) ||
    (vector.length !== 1536 &&
      !(p.embedding === null && vector.length === 0)) ||
    vector.some(
      (v) =>
        typeof v !== "number" ||
        !Number.isFinite(v) ||
        !Number.isFinite(Math.fround(v)),
    )
  )
    return fail("vector_invalid");
  if (Buffer.byteLength(JSON.stringify(p), "utf8") > 128 * 1024)
    return fail("payload_bound");
  return {
    ...base,
    title: p.title,
    description: p.description,
    title_folded: p.title_folded,
    description_folded: p.description_folded,
    brief_completed_at: p.brief_completed_at
      .replace("T", " ")
      .replace(/Z$/, ""),
    embedding: vector,
    embedding_model: p.embedding_model,
    embedding_dim: vector.length,
    content_hash: p.content_hash,
  };
}
const same = (a: FeatureReference, b: FeatureReference) =>
  a.kind === b.kind &&
  a.revision === b.revision &&
  a.sourceHash === b.sourceHash &&
  a.deleted === b.deleted &&
  JSON.stringify(Object.entries(a.key).sort()) ===
    JSON.stringify(Object.entries(b.key).sort());
export async function publishBriefFeatureSnapshots(
  deps: { db: FeatureReader; ch: ClickHouseClient; signal?: AbortSignal },
  snapshots: FeatureSnapshot[],
  options: { baselineH?: string } = {},
): Promise<FeatureReference[]> {
  if (snapshots.length > 200) return fail("batch_invalid");
  if (!snapshots.length) return [];
  const rows = snapshots.map((s) => briefFeatureRow(s, options.baselineH)),
    seen = new Set<string>();
  for (const r of rows) {
    const key = JSON.stringify([
      r.org_id,
      r.workspace_id,
      r.source,
      r.session_id,
    ]);
    if (seen.has(key)) return fail("duplicate");
    seen.add(key);
  }
  deps.signal?.throwIfAborted();
  try {
    await deps.ch.insert(BRIEF_FEATURES_TABLE, rows);
    const erased = rows.filter((r) => r.deleted === 1);
    if (erased.length) {
      const params: QueryParams = {},
        where = erased.map((r, n) => {
          for (const [f, k] of [
            ["org_id", "o"],
            ["workspace_id", "w"],
            ["source", "h"],
            ["session_id", "s"],
            ["version", "v"],
          ] as const)
            params[k + n] = r[f] as string;
          return `(org_id={o${n}:String} AND workspace_id={w${n}:String} AND source={h${n}:String} AND session_id={s${n}:String} AND version<{v${n}:UInt64})`;
        });
      await deps.ch.command(
        `DELETE FROM ${BRIEF_FEATURES_TABLE} WHERE ${where.join(" OR ")}`,
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
  } catch (e) {
    if (e instanceof BriefFeatureError) throw e;
    throw new BriefFeatureError("publication_failed");
  }
}
