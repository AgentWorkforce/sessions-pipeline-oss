/**
 * Canonical feature producer: projects canonical publication-image rows into
 * the exact FeatureSnapshot contract the legacy journal copier feeds
 * eventFeatureRow/briefFeatureRow. Reuses the legacy row validators unedited —
 * row-shape parity is by construction. Authority fields (revision, sourceHash,
 * version) carry REAL canonical values: a managed version counter initialized
 * above the last legacy feature clock, and the image's content hash. Nothing is
 * fabricated; missing inputs hard-refuse via the same EventFeatureError.
 *
 * Retirement contract (native 040600): the producer consumes durable
 * publication + retirement state only — S3 events and old Neon tombstones are
 * insufficient. Replaced images emit tombstones for their exact prior keyset.
 */
import {
  EventFeatureError,
  eventFeatureRow,
} from "./event-publisher.ts";
import {
  briefFeatureRow,
} from "./brief-publisher.ts";
import type { FeatureSnapshot } from "./source.ts";

/** One projected row from a canonical image page. */
export interface CanonicalEventRow {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  machineId: string; kind: string; eventId: string; ts: string;
  ingestedAt: string; userId: string;
  actorName: string | null; actorRole: string | null;
  projectId: string | null; projectFolded: string | null;
  taskTitle: string | null; taskTitleFolded: string | null;
  content: string | null; contentFolded: string | null;
  taskRefText: string; taskRefTextFolded: string;
  tags: unknown; embedding: string | null; embeddingModel: string | null;
  contentHash: string | null;
}
/** One projected ML-brief row (accepted ML contract; NOT a Bob summary). */
export interface CanonicalBriefRow {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  title: string; description: string;
  titleFolded: string; descriptionFolded: string;
  briefCompletedAt: string;
  embedding: string | null; embeddingModel: string | null;
  contentHash: string;
}
/** Brief tombstone: prior-session keyset retired by a replaced image. */
export interface CanonicalBriefTombstone {
  orgId: string; workspaceId: string; source: string; sessionId: string;
}

/** Prior-image keyset entry emitted as a tombstone on replacement/retire. */
export interface CanonicalTombstone {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  machineId: string; kind: string; eventId: string; ts: string;
}

/** Image identity: schemaGeneration/imageKey/revision + a real content hash. */
export interface CanonicalImageEvidence {
  schemaGeneration: string;
  imageKey: string;
  revision: string;
  /** 64-hex lower-case hash over the image's canonical content. */
  sourceHash: string;
}

export type CanonicalProduceResult =
  | { kind: "rows"; rows: Record<string, unknown>[] }
  | { kind: "refused"; reason: string };

const eventSnapshot = (
  r: CanonicalEventRow,
  e: CanonicalImageEvidence,
): FeatureSnapshot => ({
  reference: {
    kind: "event",
    key: {
      orgId: r.orgId, workspaceId: r.workspaceId, source: r.source,
      sessionId: r.sessionId, machineId: r.machineId, kind: r.kind,
      eventId: r.eventId, ts: r.ts,
    },
    revision: e.revision, sourceHash: e.sourceHash, deleted: false,
  },
  row: {
    org_id: r.orgId, workspace_id: r.workspaceId, source: r.source,
    session_id: r.sessionId, machine_id: r.machineId, kind: r.kind,
    event_id: r.eventId, ts: r.ts, ingested_at: r.ingestedAt,
    user_id: r.userId, actor_name: r.actorName, actor_role: r.actorRole,
    project_id: r.projectId, project_folded: r.projectFolded,
    task_title: r.taskTitle, task_title_folded: r.taskTitleFolded,
    content: r.content, content_folded: r.contentFolded,
    task_ref_text: r.taskRefText, task_ref_text_folded: r.taskRefTextFolded,
    tags: r.tags, embedding: r.embedding, embedding_model: r.embeddingModel,
    content_hash: r.contentHash,
  },
});
const briefSnapshot = (
  r: CanonicalBriefRow,
  e: CanonicalImageEvidence,
): FeatureSnapshot => ({
  reference: {
    kind: "brief",
    key: {
      orgId: r.orgId, source: r.source, sessionId: r.sessionId,
      workspaceId: r.workspaceId,
    },
    revision: e.revision, sourceHash: e.sourceHash, deleted: false,
  },
  row: {
    org_id: r.orgId, workspace_id: r.workspaceId, source: r.source,
    session_id: r.sessionId,
    title: r.title, description: r.description,
    title_folded: r.titleFolded, description_folded: r.descriptionFolded,
    brief_completed_at: r.briefCompletedAt,
    embedding: r.embedding, embedding_model: r.embeddingModel,
    content_hash: r.contentHash,
  },
});
const briefTombstoneSnapshot = (
  t: CanonicalBriefTombstone,
  e: CanonicalImageEvidence,
): FeatureSnapshot => ({
  reference: {
    kind: "brief",
    key: {
      orgId: t.orgId, source: t.source, sessionId: t.sessionId,
      workspaceId: t.workspaceId,
    },
    revision: e.revision, sourceHash: e.sourceHash, deleted: true,
  },
  row: null,
});

/** Brief-pool rows for one canonical image page + its replaced keyset. */
export function produceBriefFeatures(
  rows: CanonicalBriefRow[],
  tombstones: CanonicalBriefTombstone[],
  e: CanonicalImageEvidence,
  retentionDays = 0,
): CanonicalProduceResult {
  try {
    return {
      kind: "rows",
      rows: [
        ...rows.map((r) => briefFeatureRow(briefSnapshot(r, e), e.revision)),
        ...tombstones.map((t) =>
          briefFeatureRow(briefTombstoneSnapshot(t, e), e.revision)),
      ].map((r) => ({ ...r, retention_days: retentionDays })),
    };
  } catch (err) {
    if (err instanceof EventFeatureError)
      return { kind: "refused", reason: err.reason };
    throw err;
  }
}

const tombstoneSnapshot = (
  t: CanonicalTombstone,
  e: CanonicalImageEvidence,
): FeatureSnapshot => ({
  reference: {
    kind: "event",
    key: {
      orgId: t.orgId, workspaceId: t.workspaceId, source: t.source,
      sessionId: t.sessionId, machineId: t.machineId, kind: t.kind,
      eventId: t.eventId, ts: t.ts,
    },
    revision: e.revision, sourceHash: e.sourceHash, deleted: true,
  },
  row: null,
});

/** Event-pool rows for one canonical image page + its replaced keyset. */
export function produceEventFeatures(
  rows: CanonicalEventRow[],
  tombstones: CanonicalTombstone[],
  e: CanonicalImageEvidence,
  retentionDays = 0,
): CanonicalProduceResult {
  try {
    return {
      kind: "rows",
      rows: [
        ...rows.map((r) => eventFeatureRow(eventSnapshot(r, e), e.revision)),
        ...tombstones.map((t) =>
          eventFeatureRow(tombstoneSnapshot(t, e), e.revision)),
      ].map((r) => ({ ...r, retention_days: retentionDays })),
    };
  } catch (err) {
    if (err instanceof EventFeatureError)
      return { kind: "refused", reason: err.reason };
    throw err;
  }
}
