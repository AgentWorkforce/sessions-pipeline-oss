import { describe, expect, test } from "bun:test";
import { eventFeatureRow } from "../event-publisher.ts";
import { featureVersion } from "../source.ts";
import { briefFeatureRow } from "../brief-publisher.ts";
import {
  produceEventFeatures,
  produceBriefFeatures,
  type CanonicalBriefRow,
  type CanonicalBriefTombstone,
  type CanonicalEventRow,
  type CanonicalTombstone,
  type CanonicalImageEvidence,
} from "../canonical-producer.ts";

const HASH = "a".repeat(64);
const LEGACY_REV = "41";
const CANON_REV = "1000001"; // managed counter above the last legacy clock

const row = (): CanonicalEventRow => ({
  orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
  machineId: "m-1", kind: "session_event", eventId: "e-1",
  ts: "2026-10-30T10:00:00.000000Z", ingestedAt: "2026-10-30T10:01:00.000000Z",
  userId: "u-1", actorName: null, actorRole: "assistant",
  projectId: null, projectFolded: null,
  taskTitle: "T", taskTitleFolded: "t",
  content: "hello", contentFolded: "hello",
  taskRefText: "{}", taskRefTextFolded: "{}",
  tags: ["a"], embedding: null, embeddingModel: null, contentHash: null,
});
const ev: CanonicalImageEvidence = {
  schemaGeneration: "g-1", imageKey: "k-1", revision: CANON_REV,
  sourceHash: HASH,
};
const legacyPayload = (r: CanonicalEventRow) => ({
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
});
const legacyRef = (r: CanonicalEventRow) => ({
  kind: "event" as const,
  key: { orgId: r.orgId, workspaceId: r.workspaceId, source: r.source,
    sessionId: r.sessionId, machineId: r.machineId, kind: r.kind,
    eventId: r.eventId, ts: r.ts },
  revision: LEGACY_REV, sourceHash: HASH, deleted: false,
});
const AUTHORITY = ["version", "source_revision", "source_hash", "retention_days"];

describe("canonical feature producer", () => {
  test("non-authority fields are byte-identical to the legacy copier row", () => {
    const r = row();
    const legacy = eventFeatureRow(
      { reference: legacyRef(r), row: legacyPayload(r) });
    const out = produceEventFeatures([r], [], ev);
    if (out.kind !== "rows") throw new Error(out.reason);
    const canonical = out.rows[0]!;
    for (const k of Object.keys(legacy))
      if (!AUTHORITY.includes(k)) expect(canonical[k]).toEqual(legacy[k]);
    for (const k of Object.keys(canonical))
      if (!AUTHORITY.includes(k)) expect(canonical[k]).toEqual(legacy[k]);
  });
  test("authority fields carry canonical values, never the legacy clock", () => {
    const out = produceEventFeatures([row()], [], ev);
    if (out.kind !== "rows") throw new Error(out.reason);
    const c = out.rows[0]!;
    expect(c.version).toBe(featureVersion(CANON_REV));
    expect(c.source_revision).toBe(CANON_REV);
    expect(c.source_hash).toBe(HASH);
  });
  test("replaced-image keyset emits tombstones with exact prior keys", () => {
    const t: CanonicalTombstone = {
      orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
      machineId: "m-1", kind: "session_event", eventId: "e-old",
      ts: "2026-10-29T10:00:00.000000Z",
    };
    const out = produceEventFeatures([], [t], ev);
    if (out.kind !== "rows") throw new Error(out.reason);
    const c = out.rows[0]!;
    expect(c.deleted).toBe(1);
    expect(c.event_id).toBe("e-old");
    expect(c.content).toBeNull();
    expect(c.embedding_dim).toBe(0);
    expect(c.ingested_at).toBe("1970-01-01 00:00:00.000000");
  });
  test("bad embedding dims refuse, never fabricate", () => {
    const r = row();
    r.embedding = JSON.stringify([1, 2, 3]);
    const out = produceEventFeatures([r], [], ev);
    expect(out).toEqual({ kind: "refused", reason: "vector_invalid" });
  });
  test("missing/folded-mismatched pairs refuse", () => {
    const r = row();
    r.contentFolded = null;
    const out = produceEventFeatures([r], [], ev);
    expect(out).toEqual({ kind: "refused", reason: "payload_invalid" });
  });
  test("brief rows are byte-identical to the legacy copier (non-authority)", () => {
    const b: CanonicalBriefRow = {
      orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
      title: "T", description: "D", titleFolded: "t", descriptionFolded: "d",
      briefCompletedAt: "2026-10-30T12:00:00.000000Z",
      embedding: null, embeddingModel: null, contentHash: "h",
    };
    const legacy = briefFeatureRow({
      reference: { kind: "brief",
        key: { orgId: "org", source: "codex", sessionId: "s-1",
          workspaceId: "ws" },
        revision: LEGACY_REV, sourceHash: HASH, deleted: false },
      row: { org_id: "org", workspace_id: "ws", source: "codex",
        session_id: "s-1", title: "T", description: "D",
        title_folded: "t", description_folded: "d",
        brief_completed_at: "2026-10-30T12:00:00.000000Z",
        embedding: null, embedding_model: null, content_hash: "h" },
    });
    const out = produceBriefFeatures([b], [], ev);
    if (out.kind !== "rows") throw new Error(out.reason);
    const c = out.rows[0]!;
    for (const k of Object.keys(legacy))
      if (!AUTHORITY.includes(k)) expect(c[k]).toEqual(legacy[k]);
    expect(c.version).toBe(featureVersion(CANON_REV));
    expect(c.source_revision).toBe(CANON_REV);
  });
  test("retention_days stamps onto produced rows, default 0 = keep forever", () => {
    const out = produceEventFeatures([row()], [], ev, 90);
    if (out.kind !== "rows") throw new Error(out.reason);
    expect(out.rows[0]!.retention_days).toBe(90);
    const keep = produceEventFeatures([row()], [], ev);
    if (keep.kind !== "rows") throw new Error(keep.reason);
    expect(keep.rows[0]!.retention_days).toBe(0);
    const bout = produceBriefFeatures([], [{
      orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" }], ev, 30);
    if (bout.kind !== "rows") throw new Error(bout.reason);
    expect(bout.rows[0]!.retention_days).toBe(30);
  });
  test("brief tombstone carries empty fields + epoch stamp", () => {
    const t: CanonicalBriefTombstone = {
      orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" };
    const out = produceBriefFeatures([], [t], ev);
    if (out.kind !== "rows") throw new Error(out.reason);
    const c = out.rows[0]!;
    expect(c.deleted).toBe(1);
    expect(c.title).toBe("");
    expect(c.embedding_dim).toBe(0);
    expect(c.brief_completed_at).toBe("1970-01-01 00:00:00.000000");
  });
});
