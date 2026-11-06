import type { ObjectKey } from "./key.js";

/** Metadata key carrying the session trace id (UUIDv7). */
export const META_TRACE_ID = "trace-id";
/** Metadata key carrying the lowercase hex SHA-256 of the stored body. */
export const META_CONTENT_SHA256 = "content-sha256";
/** Longest presigned URL lifetime accepted (the S3 SigV4 ceiling). */
export const MAX_PRESIGN_TTL_SECONDS = 7 * 24 * 60 * 60;
/** Default and maximum list page size. */
export const DEFAULT_PAGE_SIZE = 1000;

export type Metadata = Record<string, string>;
export type ByteStream = ReadableStream<Uint8Array>;

/**
 * Opaque version of a stored object, for `ifMatch`. S3 and Azure compare
 * `eTag`; GCS compares `version` (the generation); fs compares `eTag`.
 */
export interface ObjectVersion {
  eTag?: string;
  version?: string;
}

export interface ObjectMeta {
  key: ObjectKey;
  size: number;
  lastModified: Date;
  version: ObjectVersion;
  /** User metadata. Populated by `head`; empty in `list` results. */
  metadata: Metadata;
}

export interface Page<T> {
  items: T[];
  /** Pass back to `list` to continue; `undefined` when the listing is done. Opaque. */
  nextCursor?: string;
}

export interface PutOpts {
  /** Create only. Identical content already stored is an idempotent success. */
  ifNoneMatch?: boolean;
  /** Replace only this version (compare-and-swap). */
  ifMatch?: ObjectVersion;
  /** Expected lowercase hex SHA-256 of the body; checked before upload. */
  contentSha256?: string;
  metadata?: Metadata;
  /** Inferred from the key when absent (`.zst`, `.json`). */
  contentType?: string;
}

export interface PutAck {
  key: ObjectKey;
  version: ObjectVersion;
  contentSha256: string;
  size: number;
  /** `false` for an idempotent re-put of identical content. */
  created: boolean;
}

export interface ObjectStore {
  /** `s3`, `gcs`, `azure` or `fs`. */
  readonly backend: string;
  /** `undefined` when the object does not exist. */
  head(key: ObjectKey): Promise<ObjectMeta | undefined>;
  /** The body, or the half-open byte range `[start, end)`. Throws `not_found`. */
  get(key: ObjectKey, range?: ByteRange): Promise<ByteStream>;
  /**
   * Like `get`, plus the attributes and version **of the same response**.
   * Compare-and-swap writers (the manifest protocol) must take
   * `meta.version` from here, never from a separate `head`, which could
   * describe a newer version than the body that was read.
   */
  getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject>;
  /**
   * Objects under `prefix` on a `/` boundary, in key order, at most
   * `pageSize` (default {@link DEFAULT_PAGE_SIZE}) per page.
   */
  list(prefix: ObjectKey, cursor?: string): Promise<Page<ObjectMeta>>;
  /** A URL granting GET on `key` for `ttlSeconds`. */
  presignGet(key: ObjectKey, ttlSeconds: number): Promise<URL>;
  /** Single-request put with create / compare-and-swap semantics. */
  put(key: ObjectKey, body: Uint8Array, opts?: PutOpts): Promise<PutAck>;
  /** Remove an object. Deleting one that does not exist succeeds. */
  delete(key: ObjectKey): Promise<void>;
}

/** Body and whole-object attributes from one `getObject` response. */
export interface GetObject {
  meta: ObjectMeta;
  body: ByteStream;
}

/** Half-open byte range `[start, end)`. */
export interface ByteRange {
  start: number;
  end: number;
}
