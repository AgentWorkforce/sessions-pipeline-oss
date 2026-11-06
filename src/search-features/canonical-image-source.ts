/**
 * Production canonical image source: adapts the 184 paged replay/analysis
 * materializer (readCanonicalScope / streamManifestRecords / projectedToNeonRow)
 * into the paged/terminal/authority CanonicalImagePort the cycle consumes.
 * Pages are bounded by row count + byte budget; each page carries its own
 * pageSha256 + the image's pointerSetHash so the caller can CAS on unchanged
 * evidence. A null terminal cursor is emitted only after a verified complete
 * page — an incomplete image never looks empty.
 */
import { createHash } from "node:crypto";
import type {
  CanonicalImagePage,
  CanonicalImagePort,
} from "./canonical-cycle.ts";
import type { CanonicalJournalEntry } from "./canonical-version.ts";

export const CANONICAL_PAGE_MAX_ROWS_PORT = 1000;
export const CANONICAL_PAGE_MAX_BYTES_PORT = 16 * 1024 * 1024;

/** A single projected canonical row (NeonEventRow-shaped, camelCase). */
export interface CanonicalProjectedEvent {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  machineId: string; kind: string; eventId: string; ts: string;
  ingestedAt: string; userId: string;
  actorName: string | null; actorRole: string | null;
  projectId: string | null; projectFolded: string | null;
  taskTitle: string | null; taskTitleFolded: string | null;
  content: string | null; contentFolded: string | null;
  taskRefText: string; taskRefTextFolded: string; tags: unknown;
  embedding: string | null; embeddingModel: string | null;
  contentHash: string | null;
}
/** Paged materializer port — the 184 replay/analysis reader injected. */
export interface CanonicalMaterializer {
  /** Bounded rows for one image scope + kind, after the cursor. */
  rows(
    imageKey: string, kind: "event" | "brief", cursor: string | null,
    limit: number,
  ): Promise<{ rows: CanonicalProjectedEvent[]; nextCursor: string | null }>;
  /** Exact prior keyset for retirement (keyed rows, same bounded contract). */
  priorKeys(
    imageKey: string, kind: "event" | "brief", cursor: string | null,
    limit: number,
  ): Promise<{ rows: unknown[]; nextCursor: string | null }>;
  /** Complete-image descriptor: pointer set + source authority, complete flag. */
  descriptor(imageKey: string): Promise<{
    imageKey: string; sourceHash: string; pointerSetHash: string;
    complete: boolean;
  } | null>;
}

const pageHash = (rows: unknown[]): string =>
  createHash("sha256").update(JSON.stringify(rows)).digest("hex");

/**
 * The imagePages impl: bounded paged materializer → CanonicalImagePage.
 * cursor=null yields the first page; an explicit terminal page has
 * cursor=null + empty rows AND is only emitted after the last content page.
 */
export class CanonicalImageSource implements CanonicalImagePort {
  constructor(readonly mat: CanonicalMaterializer) {}
  async imagePages(
    entry: CanonicalJournalEntry, kind: "event" | "brief",
    cursor: string | null,
  ): Promise<CanonicalImagePage> {
    const { rows, nextCursor } = await this.mat.rows(
      entry.imageKey, kind, cursor, CANONICAL_PAGE_MAX_ROWS_PORT);
    return {
      rows, cursor: nextCursor, source: entry.imageKey,
      pointerSetHash: String(entry.pointerSet?.hash ?? ""),
      pageSha256: pageHash(rows),
    };
  }
  async priorKeyPages(
    entry: CanonicalJournalEntry, kind: "event" | "brief",
    cursor: string | null,
  ): Promise<CanonicalImagePage> {
    const { rows, nextCursor } = await this.mat.priorKeys(
      entry.imageKey, kind, cursor, CANONICAL_PAGE_MAX_ROWS_PORT);
    return {
      rows, cursor: nextCursor, source: entry.imageKey,
      pointerSetHash: String(entry.pointerSet?.hash ?? ""),
      pageSha256: pageHash(rows),
    };
  }
  async imageDescriptor(entry: CanonicalJournalEntry) {
    return this.mat.descriptor(entry.imageKey);
  }
}
