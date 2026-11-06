/**
 * Canonical historical backfill driver: turns the exported org→keys manifest
 * index (S3 v1 corpus, ~129k groups + catch-up) into publish journal entries
 * the producer drains. The append port is intentionally external: only an
 * owner-controlled durable RPC may allocate a revision, and it must dedupe an
 * already-published image. This helper never writes the journal directly.
 * Bounded: per-image enumeration, no bulk payload reads.
 */
/** One image descriptor from the org→keys manifest index. */
export interface CanonicalBackfillImage {
  orgId: string; workspaceId: string; harness: string; sessionId: string;
  imageKey: string; sourceHash: string;
  schemaGeneration: string; epoch: string; fingerprint: string;
  pointerSetHash: string; pointerSetCount: number;
}

/** The index enumeration port — a stream of image descriptors (JSONL or RPC). */
export type CanonicalBackfillIndex =
  AsyncGenerator<CanonicalBackfillImage, void, void>;

export type CanonicalBackfillResult =
  | { kind: "copied"; orgId: string; entries: number; lastRevision: string }
  | { kind: "refused"; reason: string };

/**
 * Future owner-only backfill RPC boundary. publishOnce must atomically return
 * the existing revision for an exact duplicate, or allocate and append once.
 */
export interface CanonicalBackfillAppendPort {
  highWater(): Promise<string | null>;
  publishOnce(image: CanonicalBackfillImage): Promise<{
    revision: string;
    inserted: boolean;
  }>;
}

/**
 * Write publish journal entries for one image enumeration. This helper is a
 * bounded source primitive, not a resumable production driver: callers must
 * durably checkpoint/deduplicate the image enumeration.
 */
export async function backfillFromIndex(
  journal: CanonicalBackfillAppendPort,
  images: CanonicalBackfillIndex,
  legacyHighWater: string,
): Promise<CanonicalBackfillResult> {
  if (!/^(0|[1-9][0-9]*)$/.test(legacyHighWater))
    throw new Error("invalid_legacy_high_water");
  const last = await journal.highWater();
  if (last === null)
    return { kind: "refused", reason: "canonical_journal_unseeded" };
  if (!/^[1-9][0-9]*$/.test(last))
    throw new Error("canonical_backfill_invalid_revision");
  if (BigInt(last) < BigInt(legacyHighWater))
    throw new Error("canonical_journal_below_legacy_high_water");
  let entries = 0, maxRevision = BigInt(last), orgId = "";
  for await (const img of images) {
    orgId = img.orgId;
    const published = await journal.publishOnce(img);
    if (!/^[1-9][0-9]*$/.test(published.revision))
      throw new Error("canonical_backfill_invalid_revision");
    const revision = BigInt(published.revision);
    if (published.inserted && revision <= maxRevision)
      throw new Error("canonical_backfill_nonmonotonic_revision");
    if (published.inserted) entries += 1;
    if (revision > maxRevision) maxRevision = revision;
  }
  return { kind: "copied", orgId, entries,
    lastRevision: maxRevision.toString() };
}
