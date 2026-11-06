/** Default-off portable stage. Runtime wiring supplies existing reviewed DB/CH
 * identities; this module creates no clients, grants, resources or schedules. */
import { randomUUID } from "node:crypto";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import type {
  ClickHouseClient,
  QueryParams,
} from "./vendor/search-index/clickhouse.ts";
import { BRIEF_DOCUMENTS_TABLE } from "./brief-documents-schema.ts";
export interface BriefDocument {
  version: 1;
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
  deleted: boolean;
  visible: boolean;
  title: string;
  summary: string;
  projectId: string;
  lastActivityAt: string;
}
interface Claim {
  document: BriefDocument;
  epoch: string;
  hash: string;
  leaseNonce: string;
}
export class BriefDocumentError extends Error {
  constructor(readonly reason: string) {
    super("Brief document stage refused");
    this.name = "BriefDocumentError";
  }
}
const fail = (code: string): never => {
  throw new BriefDocumentError(code);
};
function checked(value: unknown, nonce: string): Claim {
  if (value === null || typeof value !== "object") return fail("claim_invalid");
  const c = value as Claim,
    d = c.document;
  if (
    c.leaseNonce !== nonce ||
    typeof c.epoch !== "string" ||
    !/^[1-9][0-9]{0,18}$/.test(c.epoch) ||
    BigInt(c.epoch) > 9223372036854775807n ||
    typeof c.hash !== "string" ||
    !/^[0-9a-f]{64}$/.test(c.hash) ||
    !d ||
    d.version !== 1 ||
    typeof d.deleted !== "boolean" ||
    typeof d.visible !== "boolean"
  )
    return fail("claim_invalid");
  for (const field of [
    "orgId",
    "workspaceId",
    "source",
    "sessionId",
    "title",
    "summary",
    "projectId",
    "lastActivityAt",
  ] as const)
    if (typeof d[field] !== "string") return fail("claim_invalid");
  if (
    !d.orgId ||
    !d.sessionId ||
    d.sessionId.length > 255 ||
    !/^[a-z0-9][a-z0-9._-]{0,63}$/.test(d.source) ||
    !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(d.lastActivityAt) ||
    !Number.isFinite(Date.parse(d.lastActivityAt))
  )
    return fail("claim_invalid");
  if (d.deleted && (d.title !== "" || d.summary !== "" || d.projectId !== ""))
    return fail("tombstone_invalid");
  // Explicit fail-closed admission; never truncate a stored title or summary.
  if (Buffer.byteLength(JSON.stringify(d), "utf8") > 128 * 1024)
    return fail("document_bound");
  return c;
}
export async function publishBriefDocumentsOnce(deps: {
  db: SqlExecutor;
  ch: ClickHouseClient;
  batch?: number;
  signal?: AbortSignal;
}): Promise<{ claimed: number; published: number; backfilled: number }> {
  const batch = deps.batch ?? 100;
  if (!Number.isInteger(batch) || batch < 1 || batch > 200)
    return fail("batch_invalid");
  if (deps.signal?.aborted) return { claimed: 0, published: 0, backfilled: 0 };
  const backfill = (
    await deps.db.query<{ r: { processed: number } }>(
      "SELECT sessions.backfill_session_brief_documents($1) r",
      [batch],
    )
  ).rows[0]?.r;
  if (
    !backfill ||
    !Number.isInteger(backfill.processed) ||
    backfill.processed < 0 ||
    backfill.processed > batch
  )
    return fail("backfill_invalid");
  const nonce = randomUUID(),
    raw = (
      await deps.db.query<{ r: unknown }>(
        "SELECT sessions.claim_session_brief_documents($1::uuid,$2) r",
        [nonce, batch],
      )
    ).rows[0]?.r;
  if (!Array.isArray(raw) || raw.length > batch) return fail("claim_invalid");
  if (!raw.length)
    return { claimed: 0, published: 0, backfilled: backfill.processed };
  try {
    const claims = raw.map((v) => checked(v, nonce)),
      keys = new Set<string>();
    for (const c of claims) {
      const d = c.document,
        key = JSON.stringify([d.orgId, d.workspaceId, d.source, d.sessionId]);
      if (keys.has(key)) return fail("claim_duplicate");
      keys.add(key);
    }
    deps.signal?.throwIfAborted();
    await deps.ch.insert(
      BRIEF_DOCUMENTS_TABLE,
      claims.map(({ document: d, epoch, hash }) => ({
        org_id: d.orgId,
        workspace_id: d.workspaceId,
        source: d.source,
        session_id: d.sessionId,
        epoch,
        document_hash: hash,
        deleted: d.deleted ? 1 : 0,
        visible: d.visible ? 1 : 0,
        title: d.title,
        summary: d.summary,
        title_folded: d.title.toLowerCase(),
        summary_folded: d.summary.toLowerCase(),
        project_id: d.projectId,
        last_activity_at: d.lastActivityAt.replace("T", " ").replace(/Z$/, ""),
      })),
    );
    // Mask old text after tombstone insertion; a delayed eraser cannot remove a
    // newer resurrection because every predicate has a strict epoch upper bound.
    const erased = claims.filter((c) => c.document.deleted);
    if (erased.length) {
      const params: QueryParams = {},
        where = erased.map((c, n) => {
          const d = c.document;
          params["o" + n] = d.orgId;
          params["w" + n] = d.workspaceId;
          params["h" + n] = d.source;
          params["s" + n] = d.sessionId;
          params["e" + n] = c.epoch;
          return `(org_id={o${n}:String} AND workspace_id={w${n}:String} AND source={h${n}:String} AND session_id={s${n}:String} AND epoch<{e${n}:UInt64})`;
        });
      await deps.ch.command(
        `DELETE FROM ${BRIEF_DOCUMENTS_TABLE} WHERE ${where.join(" OR ")}`,
        {
          params,
          settings: { lightweight_deletes_sync: 2, max_query_size: 1048576 },
        },
      );
    }
    deps.signal?.throwIfAborted();
    const items = claims.map((c) => ({
      orgId: c.document.orgId,
      workspaceId: c.document.workspaceId,
      source: c.document.source,
      sessionId: c.document.sessionId,
      epoch: c.epoch,
      hash: c.hash,
    }));
    const n = (
      await deps.db.query<{ n: number }>(
        "SELECT sessions.ack_session_brief_documents($1::uuid,$2::jsonb) n",
        [nonce, JSON.stringify(items)],
      )
    ).rows[0]?.n;
    if (!Number.isInteger(n) || n! < 0 || n! > claims.length)
      return fail("ack_invalid");
    return {
      claimed: claims.length,
      published: n!,
      backfilled: backfill.processed,
    };
  } catch {
    await deps.db.query(
      "SELECT sessions.retry_session_brief_documents($1::uuid,$2)",
      [nonce, "brief_publication_failed"],
    );
    throw new BriefDocumentError("publication_failed");
  }
}
