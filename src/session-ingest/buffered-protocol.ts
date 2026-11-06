/** Internal metadata only. No payload or queue TTL is delivery acknowledgement. */
export type IngestAcceptanceMode = "canonical" | "buffered";
export interface BufferedStageSpec {
  key: string;
  sha256: string;
  bytes: number;
  /** Each stored g maps to original claim record ordinals, never scrubbed ids. */
  groupOrdinals: number[][];
}
export interface BufferedStageIntent {
  stageToken: string;
  expiresAt: string;
}
export interface BufferedClaimRecord {
  ord: number;
  source: string;
  session_id: string | null;
  record_id: string;
  revision_id: string;
  revision: number;
  digest: string;
}
/** Exact internal JSON contract shared by040510 and Root's GC runtime. */
export interface BufferedGcTicket {
  stageToken: string;
  claimToken: string;
  leaseToken: string;
  receiptState: "preparing" | "acknowledged";
  key: string;
  sha256: string;
  bytes: number;
  groupOrdinals: number[][];
  /** Sorted original claim ordinals, including subsets within a stored group. */
  keepOrdinals: number[];
  /** Derived indexes of groups containing at least one retained ordinal. */
  keepGroupIndexes: number[];
  resetEpoch: string;
  rowVersion: string;
  phase: "plan" | "cleanup";
  cleanupState: "live" | "retired" | "redacted";
  replacement?: BufferedStageSpec & { stageToken: string };
}

/** Validate the SQL retention mask and preserve original line/group ordering. */
export function bufferedOrdinalSelection(
  groupOrdinals: number[][], keepOrdinals: number[], keepGroupIndexes: number[],
): { groupIndex: number; ordinals: number[]; lineIndexes: number[] }[] {
  if (!Array.isArray(keepOrdinals) || !Array.isArray(keepGroupIndexes)) throw new Error("buffered_keep_invalid");
  const original = new Set(groupOrdinals.flat());
  let previous = -1;
  for (const ord of keepOrdinals) {
    if (!Number.isInteger(ord) || ord <= previous || !original.has(ord)) throw new Error("buffered_keep_invalid");
    previous = ord;
  }
  const keep = new Set(keepOrdinals);
  const selected = groupOrdinals.flatMap((group, groupIndex) => {
    const lineIndexes = group.flatMap((ord, i) => keep.has(ord) ? [i] : []);
    return lineIndexes.length ? [{ groupIndex, lineIndexes, ordinals: lineIndexes.map(i => group[i]!) }] : [];
  });
  if (JSON.stringify(keepGroupIndexes) !== JSON.stringify(selected.map(s => s.groupIndex))) throw new Error("buffered_keep_invalid");
  return selected;
}
