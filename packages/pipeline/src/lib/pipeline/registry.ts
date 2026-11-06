/**
 * The queued stages of the session pipeline, in one table.
 *
 *   object-landed ─┬→ session-indexed ─┬→ summary-short
 *                  │                   ├→ summary-long
 *                  │                   └→ status-classify
 *                  └→ file-normalize  ──→ file-maintenance
 *
 * Every stage has its own queue and DLQ (`stageQueueName`), the same message
 * shape (`StageMessage`) and the same consumer (`handleStageQueue`), whichever
 * `PipelineQueue` backend carries it. A stage that is registered but not
 * `implemented` still has a queue name and a binding, so producers can be wired
 * ahead of the consumer: its messages are quarantined as `stage_not_configured`
 * and replay once a handler exists.
 *
 * Only `object-landed` and `session-indexed` are implemented here, and only the
 * `object-landed` fan-out is wired; the other `next` edges are the intended
 * shape for the stages that follow.
 */
import type { PipelineStage } from "./trace.js";

export const QUEUED_STAGES = [
  "object-landed",
  "session-indexed",
  "summary-short",
  "summary-long",
  "status-classify",
  "file-normalize",
  "file-maintenance",
  "backfill-activation",
] as const satisfies readonly PipelineStage[];
export type QueuedStage = (typeof QUEUED_STAGES)[number];

export interface StageDefinition {
  stage: QueuedStage;
  summary: string;
  /** False: registered stub; its messages wait in the dead letter store. */
  implemented: boolean;
  /** The Worker binding for this stage's Cloudflare queue producer. */
  binding: `PIPELINE_${string}`;
  /** Stages this one enqueues work for. */
  next: readonly QueuedStage[];
  /** Deliveries before a message moves to the DLQ (Cloudflare `max_retries` + 1). */
  maxAttempts: number;
}

export const STAGE_REGISTRY: { readonly [S in QueuedStage]: StageDefinition } =
  {
    "object-landed": {
      stage: "object-landed",
      summary:
        "Validate a landed manifest and fan out its index and normalize work",
      implemented: true,
      binding: "PIPELINE_OBJECT_LANDED",
      next: ["session-indexed", "file-normalize"],
      maxAttempts: 6,
    },
    "session-indexed": {
      stage: "session-indexed",
      summary: "Upsert the raw session pointer into the sessions index",
      implemented: true,
      binding: "PIPELINE_SESSION_INDEXED",
      next: ["summary-short", "summary-long", "status-classify"],
      maxAttempts: 6,
    },
    "summary-short": {
      stage: "summary-short",
      summary: "One-line session summary",
      implemented: false,
      binding: "PIPELINE_SUMMARY_SHORT",
      next: [],
      maxAttempts: 4,
    },
    "summary-long": {
      stage: "summary-long",
      summary: "Long-form session summary",
      implemented: false,
      binding: "PIPELINE_SUMMARY_LONG",
      next: [],
      maxAttempts: 4,
    },
    "status-classify": {
      stage: "status-classify",
      summary: "Classify the session's status",
      implemented: false,
      binding: "PIPELINE_STATUS_CLASSIFY",
      next: [],
      maxAttempts: 4,
    },
    "file-normalize": {
      stage: "file-normalize",
      summary: "Normalize raw chunks into the derived file layout",
      implemented: false,
      binding: "PIPELINE_FILE_NORMALIZE",
      next: ["file-maintenance"],
      maxAttempts: 6,
    },
    "file-maintenance": {
      stage: "file-maintenance",
      summary: "Compact and expire derived files",
      implemented: false,
      binding: "PIPELINE_FILE_MAINTENANCE",
      next: [],
      maxAttempts: 4,
    },
    "backfill-activation": {
      stage: "backfill-activation",
      summary: "Re-check the activation gate for a refused backfill",
      implemented: true,
      binding: "PIPELINE_BACKFILL_ACTIVATION",
      next: [],
      maxAttempts: 4,
    },
  };

export function isQueuedStage(value: unknown): value is QueuedStage {
  return (
    typeof value === "string" &&
    (QUEUED_STAGES as readonly string[]).includes(value)
  );
}
