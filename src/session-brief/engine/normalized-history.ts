/** Structural subset of ai-hist's normalized history used by Skip's analysis engine.
 * The Cloud adapter is responsible for supplying these records in chronological order.
 */
export interface NormalizedEvent {
  id: number;
  source: string;
  sessionId: string;
  eventUid: string;
  /** Import identity can reuse an event ID across machines or raw kinds. */
  machineId?: string;
  sourceKind?: string;
  tsMs: number;
  role: string;
  kind: string;
  text: string | null;
  model: string | null;
  tokenUsage: Record<string, unknown> | null;
  messageId: string | null;
  parentId: string | null;
}

export interface NormalizedToolCall {
  id: number;
  source: string;
  sessionId: string;
  toolUseId: string;
  machineId?: string;
  sourceKind?: string;
  eventUid?: string;
  callIndex?: number;
  name: string;
  target: string | null;
  args: Record<string, unknown> | null;
  isError: boolean | null;
  tsMs: number | null;
}

/** Shared citation IDs keep metrics, evidence sampling and imported provenance aligned. */
export function eventEvidenceId(event: NormalizedEvent): string {
  const uid =
    event.machineId && event.sourceKind
      ? JSON.stringify([event.machineId, event.sourceKind, event.eventUid])
      : event.eventUid;
  return `event:${event.source}:${event.sessionId}:${uid}`;
}

export function toolEvidenceId(tool: NormalizedToolCall): string {
  const uid =
    tool.machineId &&
    tool.sourceKind &&
    tool.eventUid &&
    tool.callIndex !== undefined
      ? JSON.stringify([
          tool.machineId,
          tool.sourceKind,
          tool.eventUid,
          tool.callIndex,
          tool.toolUseId,
        ])
      : tool.toolUseId;
  return `tool:${tool.source}:${tool.sessionId}:${uid}`;
}

export interface NormalizedFileEdit {
  id: number;
  source: string;
  sessionId: string;
  toolUseId: string | null;
  /** Exact imported tool citation when tool-use IDs collide across machines. */
  toolEvidenceId?: string | null;
  filePath: string;
  toolName: string | null;
  linesAdded: number | null;
  linesRemoved: number | null;
  structuredPatch: unknown | null;
  structuredPatchJson: string | null;
  userModified: boolean | null;
  tsMs: number | null;
}

export interface NormalizedTree {
  nodes: Array<{
    source: string;
    sessionId: string;
    relationship: null | {
      relationshipUid: string;
      childAgentName: string | null;
      childAgentType: string | null;
      spawnedAtMs: number | null;
    };
  }>;
  unlinked: unknown[];
  diagnostics: unknown[];
  truncated: boolean;
}

export interface NormalizedHydration {
  capability: "full" | "partial" | "shallow_only";
  discoveryState?: string;
  status: string;
  indexedThrough: { sourceStamp: string | null; lastEventAtMs: number | null };
  diagnostics: unknown[];
}

export interface CollectedEvidence {
  tree: NormalizedTree;
  events: NormalizedEvent[];
  tools: NormalizedToolCall[];
  edits: NormalizedFileEdit[];
}
