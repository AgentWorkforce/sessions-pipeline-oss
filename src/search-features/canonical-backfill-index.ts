/**
 * JSONL manifest-index adapter: the org→keys snapshot (one line per session,
 * keys[] of S3 manifest paths) drives the backfill enumeration. Authority
 * fields come from each manifest's descriptor — the index is key enumeration
 * only, never a source of authority values.
 */
import { createInterface } from "node:readline";
import { createReadStream } from "node:fs";
import type { CanonicalBackfillImage } from "./canonical-backfill.ts";

export interface ManifestIndexLine {
  org_id: string; session_id: string; revision: number;
  error: boolean; keys: string[];
}
/** The descriptor lookup — real authority/hash comes from the manifest. */
export interface CanonicalDescriptor {
  imageKey: string; sourceHash: string; schemaGeneration: string;
  epoch: string; fingerprint: string;
  pointerSetHash: string; pointerSetCount: number;
}
export type CanonicalDescriptorPort =
  (imageKey: string) => Promise<CanonicalDescriptor | null>;

/** Parse the ws=/m=/h=/d=/s= key path into scope parts. */
function scopeFromKey(key: string): {
  workspaceId: string; machineId: string; harness: string; sessionId: string;
} {
  const m = /ws=([^/]+)\/m=([^/]+)\/h=([^/]+)\/d=[^/]+\/[^/]+\/[^/]+\/s=([^/]+)\//.exec(key);
  if (!m) throw new Error("key_shape: " + key.slice(0, 40));
  return { workspaceId: m[1]!, machineId: m[2]!, harness: m[3]!,
           sessionId: m[4]! };
}

/** Stream the JSONL index: one CanonicalBackfillImage per manifest key. */
export async function* manifestIndexImages(
  path: string,
  descriptor: CanonicalDescriptorPort,
): AsyncGenerator<CanonicalBackfillImage, void, void> {
  const rl = createInterface({
    input: createReadStream(path, { encoding: "utf8" }),
    crlfDelay: Number.POSITIVE_INFINITY,
  });
  for await (const line of rl) {
    if (!line.trim()) continue;
    const l = JSON.parse(line) as ManifestIndexLine;
    if (l.error) continue;                       // errored exports skipped
    for (const key of l.keys) {
      const scope = scopeFromKey(key);
      const d = await descriptor(key);
      if (!d) continue;                          // descriptor missing → skip
      yield {
        orgId: l.org_id,
        workspaceId: scope.workspaceId,
        harness: scope.harness,
        sessionId: l.session_id,
        imageKey: key,
        sourceHash: d.sourceHash,
        schemaGeneration: d.schemaGeneration,
        epoch: d.epoch,
        fingerprint: d.fingerprint,
        pointerSetHash: d.pointerSetHash,
        pointerSetCount: d.pointerSetCount,
      };
    }
  }
}
