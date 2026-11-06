/**
 * Backend-independent semantics. A {@link Transport} does raw HTTP (or file)
 * operations; {@link BaseStore} adds what the contract promises on top: digest
 * checks, idempotent create, metadata validation, range/TTL validation and
 * key filtering. Mirrors `core.rs` in the Rust crate.
 */
import { sha256Hex } from "./crypto.js";
import { ObjectStoreError, isObjectStoreError } from "./errors.js";
import { isObjectKey, objectKey, type ObjectKey } from "./key.js";
import {
  DEFAULT_PAGE_SIZE,
  MAX_PRESIGN_TTL_SECONDS,
  META_CONTENT_SHA256,
  type ByteRange,
  type ByteStream,
  type GetObject,
  type Metadata,
  type ObjectMeta,
  type ObjectStore,
  type ObjectVersion,
  type Page,
  type PutAck,
  type PutOpts,
} from "./types.js";

export type PutMode =
  | { kind: "overwrite" }
  | { kind: "create" }
  | { kind: "update"; version: ObjectVersion };

/** Raw operations. Errors are `ObjectStoreError`s with contract codes. */
export interface Transport {
  readonly backend: string;
  put(
    key: ObjectKey,
    body: Uint8Array,
    mode: PutMode,
    metadata: Metadata,
    contentType: string,
  ): Promise<ObjectVersion>;
  head(key: ObjectKey): Promise<ObjectMeta | undefined>;
  getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject>;
  delete(key: ObjectKey): Promise<void>;
  /**
   * Rewrite lost adapter metadata for an object whose bytes were verified by
   * hashing (fs sidecars only; cloud metadata is atomic with the object).
   */
  repairMetadata?(
    key: ObjectKey,
    version: ObjectVersion,
    metadata: Metadata,
    contentType: string,
  ): Promise<void>;
  /** `prefix` ends with `/`. Results are in key order. */
  list(
    prefix: string,
    cursor: string | undefined,
    pageSize: number,
  ): Promise<{ items: ObjectMeta[]; nextCursor?: string }>;
  presignGet(key: ObjectKey, ttlSeconds: number): Promise<URL>;
}

const MAX_METADATA_BYTES = 2048;
/** Room kept for the adapter's own `content-sha256` entry (key + 64 hex). */
const RESERVED_DIGEST_BYTES = META_CONTENT_SHA256.length + 64;
export const MAX_USER_METADATA_BYTES =
  MAX_METADATA_BYTES - RESERVED_DIGEST_BYTES;

export class BaseStore implements ObjectStore {
  readonly backend: string;
  readonly #transport: Transport;
  readonly #pageSize: number;

  constructor(transport: Transport, pageSize = DEFAULT_PAGE_SIZE) {
    this.backend = transport.backend;
    this.#transport = transport;
    this.#pageSize = Math.min(
      Math.max(1, Math.floor(pageSize)),
      DEFAULT_PAGE_SIZE,
    );
  }

  async head(key: ObjectKey): Promise<ObjectMeta | undefined> {
    return this.#transport.head(objectKey(key));
  }

  async get(key: ObjectKey, range?: ByteRange): Promise<ByteStream> {
    return (await this.getObject(key, range)).body;
  }

  async getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject> {
    if (range) {
      if (
        !Number.isSafeInteger(range.start) ||
        !Number.isSafeInteger(range.end) ||
        range.start < 0 ||
        range.start >= range.end
      ) {
        throw this.#invalid(
          `empty or invalid range [${range.start}, ${range.end})`,
        );
      }
    }
    return this.#transport.getObject(objectKey(key), range);
  }

  async list(prefix: ObjectKey, cursor?: string): Promise<Page<ObjectMeta>> {
    const page = await this.#transport.list(
      `${objectKey(prefix)}/`,
      cursor,
      this.#pageSize,
    );
    // Objects written by other tools that the contract cannot address are
    // skipped rather than failing the whole listing.
    const items = page.items.filter((m) => isObjectKey(m.key));
    return page.nextCursor ? { items, nextCursor: page.nextCursor } : { items };
  }

  async presignGet(key: ObjectKey, ttlSeconds: number): Promise<URL> {
    // Signatures carry whole seconds, so anything under 1s would be an
    // already-expired URL.
    if (!(ttlSeconds >= 1) || ttlSeconds > MAX_PRESIGN_TTL_SECONDS) {
      throw this.#invalid(
        `presign ttl must be in [1, ${MAX_PRESIGN_TTL_SECONDS}] seconds, got ${ttlSeconds}`,
      );
    }
    return this.#transport.presignGet(objectKey(key), Math.floor(ttlSeconds));
  }

  async delete(key: ObjectKey): Promise<void> {
    await this.#transport.delete(objectKey(key));
  }

  async put(
    key: ObjectKey,
    body: Uint8Array,
    opts: PutOpts = {},
  ): Promise<PutAck> {
    objectKey(key);
    if (opts.ifNoneMatch && opts.ifMatch) {
      throw this.#invalid("ifNoneMatch and ifMatch are mutually exclusive");
    }
    const sha = await sha256Hex(body);
    if (opts.contentSha256 && opts.contentSha256.toLowerCase() !== sha) {
      throw new ObjectStoreError(
        "content_mismatch",
        this.backend,
        `${key}: expected ${opts.contentSha256}, computed ${sha}`,
      );
    }
    const metadata = this.#withDigest(opts.metadata ?? {}, sha);
    const contentType = opts.contentType ?? inferContentType(key);
    const mode: PutMode = opts.ifNoneMatch
      ? { kind: "create" }
      : opts.ifMatch
        ? { kind: "update", version: opts.ifMatch }
        : { kind: "overwrite" };
    try {
      const version = await this.#transport.put(
        key,
        body,
        mode,
        metadata,
        contentType,
      );
      return {
        key,
        version,
        contentSha256: sha,
        size: body.length,
        created: true,
      };
    } catch (err) {
      if (
        mode.kind === "create" &&
        (isObjectStoreError(err, "already_exists") ||
          isObjectStoreError(err, "precondition_failed"))
      ) {
        return this.#reconcileExisting(
          key,
          sha,
          body.length,
          metadata,
          contentType,
        );
      }
      if (mode.kind === "update" && isObjectStoreError(err, "not_found")) {
        throw new ObjectStoreError(
          "precondition_failed",
          this.backend,
          `${key}: compare-and-swap against a missing object`,
          { cause: err },
        );
      }
      throw err;
    }
  }

  async #reconcileExisting(
    key: ObjectKey,
    sha: string,
    size: number,
    metadata: Metadata,
    contentType: string,
  ): Promise<PutAck> {
    const existing = await this.head(key);
    if (!existing) {
      throw new ObjectStoreError(
        "transient",
        this.backend,
        `${key} vanished while reconciling a create`,
      );
    }
    const recorded = existing.metadata[META_CONTENT_SHA256];
    const stored =
      recorded ?? (await sha256Hex(await readAll(await this.get(key))));
    if (existing.size === size && stored.toLowerCase() === sha) {
      if (recorded === undefined) {
        await this.#transport.repairMetadata?.(
          key,
          existing.version,
          metadata,
          contentType,
        );
      }
      return {
        key,
        version: existing.version,
        contentSha256: sha,
        size,
        created: false,
      };
    }
    throw new ObjectStoreError(
      "already_exists",
      this.backend,
      `${key} already exists with different content`,
    );
  }

  #withDigest(metadata: Metadata, sha: string): Metadata {
    if (META_CONTENT_SHA256 in metadata) {
      throw this.#invalid(
        `\`${META_CONTENT_SHA256}\` metadata is set by the adapter; pass contentSha256`,
      );
    }
    let total = 0;
    for (const [k, v] of Object.entries(metadata)) {
      if (!/^[a-z][a-z0-9-]{0,127}$/.test(k)) {
        throw this.#invalid(
          `metadata key ${JSON.stringify(k)} must match [a-z][a-z0-9-]*`,
        );
      }
      if (!/^[\x20-\x7e]*$/.test(v)) {
        throw this.#invalid(
          `metadata value for ${JSON.stringify(k)} must be printable ASCII`,
        );
      }
      // HTTP strips leading/trailing whitespace from header values, so such
      // a value would differ between the cloud stores and the fs store.
      if (v.startsWith(" ") || v.endsWith(" ")) {
        throw this.#invalid(
          `metadata value for ${JSON.stringify(k)} must not start or end with a space`,
        );
      }
      total += k.length + v.length;
    }
    if (total > MAX_USER_METADATA_BYTES) {
      throw this.#invalid(
        `metadata is ${total} bytes; the limit is ${MAX_USER_METADATA_BYTES} (${MAX_METADATA_BYTES} minus ${RESERVED_DIGEST_BYTES} reserved for ${META_CONTENT_SHA256})`,
      );
    }
    return { ...metadata, [META_CONTENT_SHA256]: sha };
  }

  #invalid(message: string): ObjectStoreError {
    return new ObjectStoreError("invalid_argument", this.backend, message);
  }
}

export function inferContentType(key: string): string {
  if (key.endsWith(".zst")) return "application/zstd";
  if (key.endsWith(".json")) return "application/json";
  return "application/octet-stream";
}

/** Concatenate a stream into one buffer. */
export async function readAll(stream: ByteStream): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let length = 0;
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    length += value.length;
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

/** Collect `x-<vendor>-meta-*` headers into contract metadata. */
export function metadataFromHeaders(
  headers: Headers,
  headerPrefix: string,
  separator: "-" | "_" = "-",
): Metadata {
  const out: Metadata = {};
  headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower.startsWith(headerPrefix)) {
      const key = lower.slice(headerPrefix.length);
      out[separator === "-" ? key : key.replaceAll(separator, "-")] = value;
    }
  });
  return out;
}

/** Credentials that expire; refreshed `refreshBeforeSeconds` before `expiresAt`. */
export interface Expiring {
  expiresAt?: Date;
}

export type Provider<T> = T | (() => Promise<T>);

/** Cache a provider's result until shortly before it expires. */
export function cachedProvider<T extends Expiring>(
  provider: Provider<T>,
  refreshBeforeSeconds = 300,
): () => Promise<T> {
  if (typeof provider !== "function") return () => Promise.resolve(provider);
  let cached: T | undefined;
  let inflight: Promise<T> | undefined;
  return async () => {
    const now = Date.now();
    if (
      cached &&
      (!cached.expiresAt ||
        cached.expiresAt.getTime() - refreshBeforeSeconds * 1000 > now)
    ) {
      return cached;
    }
    inflight ??= (provider as () => Promise<T>)().finally(() => {
      inflight = undefined;
    });
    cached = await inflight;
    return cached;
  };
}

export async function errorBody(response: Response): Promise<string> {
  try {
    return (await response.text()).replace(/\s+/g, " ").trim();
  } catch {
    return "";
  }
}

/** Wrap `fetch` so network failures are `transient`. */
export async function send(
  backend: string,
  key: string,
  url: URL | string,
  init: RequestInit,
  fetchImpl: typeof fetch,
): Promise<Response> {
  try {
    return await fetchImpl(url, init);
  } catch (cause) {
    throw new ObjectStoreError("transient", backend, `${key}: request failed`, {
      cause,
    });
  }
}

/** Whole-object size from a GET response (range responses carry it in Content-Range). */
export function objectSizeFromHeaders(headers: Headers): number {
  const range = headers.get("content-range");
  const total = range ? /\/(\d+)$/.exec(range)?.[1] : undefined;
  return Number(total ?? headers.get("content-length") ?? 0);
}
