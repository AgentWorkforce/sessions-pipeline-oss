/**
 * Durable canonical feature journal and producer-progress contract.
 *
 * The legacy copier's ReplacingMergeTree `version` derives from the Neon
 * feature clock (featureVersion(rev) = 2*rev+1). The merged 025000 journal is
 * the managed counter the pools need, initialized ABOVE the last legacy clock
 * value before the copier is stopped/drained.
 *
 * Ownership: this file declares the runtime CONTRACT only. The 025000 SQL
 * writer owns seed/allocation/append; producer progress is isolated in the
 * registry-assigned 030000 consumer migration.
 */

/** Durable publication + retirement journal — the only valid input. */
export interface CanonicalJournalPort {
  /**
   * Committed entries in order: publication or retirement, CAS-fenced.
   * hasMore distinguishes a bounded page from a complete enumeration —
   * a returned limit is never implicit completeness.
   */
  entries(after: string, limit: number): Promise<{
    entries: CanonicalJournalEntry[];
    nextRevision: string;
    hasMore: boolean;
  }>;
}
export type CanonicalJournalEntry =
  | { kind: "publish"; revision: string; imageKey: string;
      schemaGeneration: string; sourceHash: string; scope: CanonicalScope;
      sourceAuthority: CanonicalSourceAuthority; pointerSet: CanonicalPointerSet;
      /** Replacement only: prior authority + prior SET hash (retire context). */
      priorSourceAuthority?: CanonicalSourceAuthority;
      priorKeysetHash?: string }
  | { kind: "retire"; revision: string; imageKey: string;
      schemaGeneration: string; sourceHash: string; priorKeysetHash: string;
      scope: CanonicalScope;
      priorSourceAuthority: CanonicalSourceAuthority; pointerSet: CanonicalPointerSet };

/** One logical scope the journal is always scoped to (native contract). */
export interface CanonicalScope {
  orgId: string; workspaceId: string; harness: string; sessionId: string;
  generation: string;
}
/** Replay source authority: publication epoch + snapshot fingerprint. */
export interface CanonicalSourceAuthority {
  authorityId: string; epoch: string; fingerprint: string;
}
/** Exact captured inventory identity — hash and count, never payload bytes. */
export interface CanonicalPointerSet {
  authorityId: string; hash: string; count: string; }

/** The 025000 invoker input — both-or-neither prior pair, type-level enforced. */
export type CanonicalFeatureChangedInput = {
  scope: CanonicalScope;
  source: CanonicalSourceAuthority;
  pointerSet: CanonicalPointerSet;
} & (
  | { priorSource: null; priorPointerSet?: never }
  | { priorSource: CanonicalSourceAuthority;
      priorPointerSet: CanonicalPointerSet }
);

/** The 025000 transaction port: OWNER-ONLY INVOKER inside the 620 final CAS. */
export interface CanonicalFeatureChangedPort {
  canonicalFeatureChanged(
    input: CanonicalFeatureChangedInput): Promise<string>;
}

/** Per-page acknowledgment — idempotent restart-safe progress. */
export interface CanonicalPageAckPort {
  /** Freeze the entry's bindings before the first CH write — drift refuses. */
  begin(entry: CanonicalJournalEntry, profileBinding: string,
        targetBinding: string)
      : Promise<"started" | "resumed" | "already_complete">;
  ackPage(input: {
    entry: CanonicalJournalEntry; kind: "event" | "brief";
    cursor: string;                 // the request cursor that produced this page
    nextCursor: string | null;      // the following page's cursor (null=terminal)
    pageSha256: string;
  }): Promise<void>;
  /** Durable resume: the verified next edge from the page chain. */
  resumeCursor(
    entry: CanonicalJournalEntry, pool: "event" | "brief",
  ): Promise<{ state: "empty" | "resume" | "complete" | "corrupt";
               cursor: string | null; terminalSha256?: string }>;
  /** finish CAS: both pool terminal descriptors + all page ACKs + current
   *  source authority + unchanged target/profile bindings. */
  finish(input: {
    entry: CanonicalJournalEntry;
    terminalEvent: string; terminalBrief: string;
    sourceAuthority: CanonicalSourceAuthority;
    profileBinding: string;
    targetBinding: string;
  }): Promise<void>;
}
