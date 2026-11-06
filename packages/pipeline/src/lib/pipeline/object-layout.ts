/**
 * The raw-object key layout, manifest validator and store drivers, from the
 * object-store package (one implementation, not a copy). Imported from source
 * because the Worker build does not build that package's `dist/`; switch this
 * re-export to `@relayhistory/object-store` once the Worker depends on the
 * built package.
 */
export {
  ObjectStoreError,
  isManifestRest,
  isPublicationManifestRest,
  parseUploadGenerationPrefix,
  RAW_FORMATS,
  chunkKeys,
  computeContentSha256,
  createGcsStore,
  createS3Store,
  isObjectStoreError,
  isScrubbed,
  joinKey,
  manifestKey,
  manifestLocation,
  normalizedKey,
  objectKey,
  parseManifest,
  parseRawChunkName,
  parseSessionKey,
  pointerFields,
  rawChunkKey,
  readAll,
  type ChunkEntry,
  type ManifestV1,
  type ObjectKey,
  type ObjectMeta,
  type ObjectStore,
  type RawFormat,
  type GcsConfig,
  type GcsToken,
  type S3Config,
  type SessionLocation,
} from "../../../../object-store/src/index.js";
