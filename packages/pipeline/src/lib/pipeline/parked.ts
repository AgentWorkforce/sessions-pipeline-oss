/**
 * Parked manifests: what a client delivered while it could not take over a cold
 * export. They sit outside the session, so anything that sweeps or erases one
 * must come through here. There is no scheduled job: an expired one goes when
 * its session is next written, and erasure calls `deleteParkedSession`.
 */
import {
  parseManifest,
  type ManifestV1,
} from "../../../../object-store/src/manifest.js";
import {
  isObjectStoreError,
  manifestLocation,
  objectKey,
  parseSessionKey,
  rawChunkKey,
  readAll,
  type ObjectStore,
} from "./object-layout.js";

/** How long a parked manifest outlives its last write. */
export const PARKED_TTL_MS = 30 * 24 * 60 * 60 * 1000;
export const META_PARKED_AT = "parked-at";

export const parkedManifestKey = (manifestKey: string, machineId: string) =>
  manifestKey.replace(/manifest\.json$/, `parked/${machineId}/manifest.json`);

async function readManifest(
  store: ObjectStore,
  key: string,
  validateAs: string,
): Promise<ManifestV1 | undefined> {
  try {
    const body = await readAll(await store.get(objectKey(key)));
    return await parseManifest(new TextDecoder().decode(body), validateAs);
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return undefined;
    throw error;
  }
}

/**
 * Remove a parked manifest and the chunks only it lists. Chunks the session's
 * own manifest lists stay: they are the session's. Safe to repeat, and safe
 * when nothing is parked. For erasure, delete the session's own objects too.
 */
export async function deleteParkedSession(
  store: ObjectStore,
  manifestKey: string,
  machineId: string,
): Promise<{ manifest: boolean; chunks: number }> {
  const parkedKey = parkedManifestKey(manifestKey, machineId);
  const parked = await readManifest(store, parkedKey, manifestKey);
  if (!parked) return { manifest: false, chunks: 0 };
  const session = await readManifest(store, manifestKey, manifestKey);
  const keep = new Set(session?.raw.chunks.map((c) => c.stored_sha256));
  // The parked chunks live under the session's own deployment prefix
  // (`96bulls/<org>/v1/...` with PIPELINE_KEY_PREFIX); without it the deletes
  // went to the unprefixed path and erasure left every parked chunk behind.
  const location = manifestLocation(parked, parseSessionKey(manifestKey)?.prefix);
  let chunks = 0;
  for (const chunk of parked.raw.chunks) {
    if (keep.has(chunk.stored_sha256)) continue;
    await store.delete(
      objectKey(
        rawChunkKey(
          location,
          chunk.seq,
          chunk.start_offset,
          chunk.sha256,
          parked.raw.format,
        ),
      ),
    );
    chunks += 1;
  }
  // Last, so a failed run can be repeated: the manifest still names the chunks.
  await store.delete(objectKey(parkedKey));
  return { manifest: true, chunks };
}

/** Drop a parked manifest past its TTL. Nothing to do is the common case. */
export async function expireParkedSession(
  store: ObjectStore,
  manifestKey: string,
  machineId: string,
  now: Date,
  ttlMs = PARKED_TTL_MS,
): Promise<boolean> {
  const head = await store.head(
    objectKey(parkedManifestKey(manifestKey, machineId)),
  );
  if (!head) return false;
  const parkedAt = Date.parse(head.metadata[META_PARKED_AT] ?? "");
  // No stamp: keep it. Losing a client's only copy is worse than a late sweep.
  if (!Number.isFinite(parkedAt) || now.getTime() - parkedAt <= ttlMs)
    return false;
  await deleteParkedSession(store, manifestKey, machineId);
  return true;
}
