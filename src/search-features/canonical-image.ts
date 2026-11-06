/**
 * Bounded canonical-image projection: turns one publication image page into
 * the producer's row inputs. Hard bounds on page rows/bytes and every row —
 * an over-bounds page hard-refuses rather than truncating, since a truncated
 * image silently drops tombstones the pools must retire.
 * The S3/PostgREST fetch itself is outside this module (native transport);
 * callers inject the page.
 */
import type {
  CanonicalBriefRow,
  CanonicalBriefTombstone,
  CanonicalEventRow,
  CanonicalTombstone,
} from "./canonical-producer.ts";

export const CANONICAL_PAGE_MAX_ROWS = 1000;
export const CANONICAL_PAGE_MAX_BYTES = 16 * 1024 * 1024;
export const CANONICAL_ROW_MAX_BYTES = 16 * 1024 * 1024;

export type CanonicalProjectResult<T> =
  | { kind: "rows"; rows: T[] }
  | { kind: "refused"; reason: string };

const stamp =
  /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/;
const required = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const text = (v: unknown) => typeof v === "string" && v !== "";
const string = (v: unknown) => typeof v === "string";
const nullableText = (v: unknown) => v === null || typeof v === "string";

/** Project one opaque image event row into the event-pool input contract. */
export function projectEventRow(
  v: unknown,
): { kind: "row"; row: CanonicalEventRow } | { kind: "refused"; reason: string } {
  if (!required(v)) return { kind: "refused", reason: "row_invalid" };
  const f = [
    "orgId", "source", "sessionId", "machineId", "kind",
    "eventId", "ts", "ingestedAt", "userId",
  ];
  if (!f.every((k) => text(v[k])) || !string(v.workspaceId) ||
      !stamp.test(v.ts as string) ||
      !stamp.test(v.ingestedAt as string))
    return { kind: "refused", reason: "row_invalid" };
  for (const k of [
    "actorName", "actorRole", "projectId", "projectFolded", "taskTitle",
    "taskTitleFolded", "content", "contentFolded", "embedding",
    "embeddingModel", "contentHash",
  ])
    if (!nullableText(v[k])) return { kind: "refused", reason: "row_invalid" };
  for (const k of ["taskRefText", "taskRefTextFolded"])
    if (!text(v[k])) return { kind: "refused", reason: "row_invalid" };
  if (Buffer.byteLength(JSON.stringify(v), "utf8") > CANONICAL_ROW_MAX_BYTES)
    return { kind: "refused", reason: "row_bound" };
  return {
    kind: "row",
    row: {
      orgId: v.orgId as string, workspaceId: v.workspaceId as string,
      source: v.source as string, sessionId: v.sessionId as string,
      machineId: v.machineId as string, kind: v.kind as string,
      eventId: v.eventId as string, ts: v.ts as string,
      ingestedAt: v.ingestedAt as string, userId: v.userId as string,
      actorName: v.actorName as string | null,
      actorRole: v.actorRole as string | null,
      projectId: v.projectId as string | null,
      projectFolded: v.projectFolded as string | null,
      taskTitle: v.taskTitle as string | null,
      taskTitleFolded: v.taskTitleFolded as string | null,
      content: v.content as string | null,
      contentFolded: v.contentFolded as string | null,
      taskRefText: v.taskRefText as string,
      taskRefTextFolded: v.taskRefTextFolded as string,
      tags: v.tags ?? [],
      embedding: v.embedding as string | null,
      embeddingModel: v.embeddingModel as string | null,
      contentHash: v.contentHash as string | null,
    },
  };
}
/** Page-level bounds: row count and total byte ceiling. */
export function boundPage(rows: unknown[]): CanonicalProjectResult<unknown> {
  if (rows.length > CANONICAL_PAGE_MAX_ROWS)
    return { kind: "refused", reason: "page_rows_bound" };
  if (Buffer.byteLength(JSON.stringify(rows), "utf8") > CANONICAL_PAGE_MAX_BYTES)
    return { kind: "refused", reason: "page_bytes_bound" };
  return { kind: "rows", rows };
}
/** Project one opaque image brief row into the brief-pool input contract. */
export function projectBriefRow(
  v: unknown,
): { kind: "row"; row: CanonicalBriefRow } | { kind: "refused"; reason: string } {
  if (!required(v)) return { kind: "refused", reason: "row_invalid" };
  const f = ["orgId", "source", "sessionId", "title",
    "description", "titleFolded", "descriptionFolded", "briefCompletedAt",
    "contentHash"];
  if (!f.every((k) => text(v[k])) || !string(v.workspaceId) ||
      !stamp.test(v.briefCompletedAt as string))
    return { kind: "refused", reason: "row_invalid" };
  for (const k of ["embedding", "embeddingModel"])
    if (!nullableText(v[k])) return { kind: "refused", reason: "row_invalid" };
  if (Buffer.byteLength(JSON.stringify(v), "utf8") > 128 * 1024)
    return { kind: "refused", reason: "row_bound" };
  return {
    kind: "row",
    row: {
      orgId: v.orgId as string, workspaceId: v.workspaceId as string,
      source: v.source as string, sessionId: v.sessionId as string,
      title: v.title as string, description: v.description as string,
      titleFolded: v.titleFolded as string,
      descriptionFolded: v.descriptionFolded as string,
      briefCompletedAt: v.briefCompletedAt as string,
      embedding: v.embedding as string | null,
      embeddingModel: v.embeddingModel as string | null,
      contentHash: v.contentHash as string,
    },
  };
}
/** Prior-image keyset entries retire exactly; malformed keysets refuse. */
export function projectTombstone(
  v: unknown,
): { kind: "row"; row: CanonicalTombstone } | { kind: "refused"; reason: string } {
  if (!required(v)) return { kind: "refused", reason: "row_invalid" };
  const f = ["orgId", "source", "sessionId", "machineId",
    "kind", "eventId", "ts"];
  if (!f.every((k) => text(v[k])) || !string(v.workspaceId) ||
      !stamp.test(v.ts as string))
    return { kind: "refused", reason: "row_invalid" };
  return { kind: "row", row: v as unknown as CanonicalTombstone };
}
export function projectBriefTombstone(
  v: unknown,
): { kind: "row"; row: CanonicalBriefTombstone } | { kind: "refused"; reason: string } {
  if (!required(v)) return { kind: "refused", reason: "row_invalid" };
  if (!["orgId", "source", "sessionId"].every((k) => text(v[k])) ||
      !string(v.workspaceId))
    return { kind: "refused", reason: "row_invalid" };
  return { kind: "row", row: v as unknown as CanonicalBriefTombstone };
}
