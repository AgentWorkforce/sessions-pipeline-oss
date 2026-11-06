/** Amazon S3 and S3-compatible stores (Cloudflare R2, MinIO). */
import {
  BaseStore,
  cachedProvider,
  errorBody,
  metadataFromHeaders,
  objectSizeFromHeaders,
  send,
  type PutMode,
  type Provider,
  type Transport,
} from "./core.js";
import { encodeKeyPath, sha256Hex } from "./crypto.js";
import { ObjectStoreError, errorForStatus } from "./errors.js";
import type { ObjectKey } from "./key.js";
import { presignUrl, signRequest, type AwsCredentials } from "./sigv4.js";
import type {
  ByteRange,
  GetObject,
  Metadata,
  ObjectMeta,
  ObjectVersion,
} from "./types.js";
import { xmlElements, xmlText } from "./xml.js";

export type { AwsCredentials } from "./sigv4.js";

export interface S3Config {
  bucket: string;
  /** `auto` for R2. */
  region: string;
  /**
   * Override for S3-compatible stores, e.g. `https://<acct>.r2.cloudflarestorage.com`
   * or `http://localhost:9000`. Requests are path-style when set.
   */
  endpoint?: string;
  /** Virtual-hosted-style (`bucket.host`). Default: true on AWS, false with `endpoint`. */
  virtualHostedStyle?: boolean;
  /**
   * Static keys or a function returning scoped, expiring credentials (cached
   * and refreshed 5 minutes before `expiresAt`). Never persisted.
   */
  credentials: Provider<AwsCredentials>;
  pageSize?: number;
  fetch?: typeof fetch;
  /**
   * Sent as `x-amz-server-side-encryption` on every put, for stores that do
   * not encrypt at rest by default (MinIO with a KMS configured, AWS buckets
   * without default encryption). R2 always encrypts at rest; leave it unset.
   */
  serverSideEncryption?: "AES256" | "aws:kms";
  /**
   * With `aws:kms`: the key ARN sent as
   * `x-amz-server-side-encryption-aws-kms-key-id`, for buckets whose policy
   * refuses `aws:kms` without naming the bucket's own key.
   */
  sseKmsKeyId?: string;
}

const BACKEND = "s3";

class S3Transport implements Transport {
  readonly backend = BACKEND;
  readonly #config: S3Config;
  readonly #credentials: () => Promise<AwsCredentials>;
  readonly #fetch: typeof fetch;

  constructor(config: S3Config) {
    this.#config = config;
    this.#credentials = cachedProvider(config.credentials);
    this.#fetch = config.fetch ?? ((...a) => fetch(...a));
  }

  #url(key = ""): URL {
    const { bucket, region, endpoint } = this.#config;
    const virtual = this.#config.virtualHostedStyle ?? endpoint === undefined;
    const base = endpoint
      ? new URL(endpoint)
      : new URL(`https://s3.${region}.amazonaws.com`);
    const path = key ? `/${encodeKeyPath(key)}` : "/";
    if (virtual) {
      base.host = `${bucket}.${base.host}`;
      base.pathname = path;
    } else {
      base.pathname = `${base.pathname.replace(/\/+$/, "")}/${bucket}${key ? path : "/"}`;
    }
    return base;
  }

  async #request(
    key: string,
    method: string,
    url: URL,
    headers: Headers,
    body?: Uint8Array,
  ): Promise<Response> {
    await signRequest({
      method,
      url,
      headers,
      payloadSha256: await sha256Hex(body ?? new Uint8Array()),
      region: this.#config.region,
      credentials: await this.#credentials(),
    });
    return send(
      BACKEND,
      key,
      url,
      {
        method,
        headers,
        ...(body ? { body: body as Uint8Array<ArrayBuffer> } : {}),
      },
      this.#fetch,
    );
  }

  async put(
    key: ObjectKey,
    body: Uint8Array,
    mode: PutMode,
    metadata: Metadata,
    contentType: string,
  ): Promise<ObjectVersion> {
    const headers = new Headers({ "content-type": contentType });
    for (const [k, v] of Object.entries(metadata))
      headers.set(`x-amz-meta-${k}`, v);
    if (this.#config.serverSideEncryption)
      headers.set(
        "x-amz-server-side-encryption",
        this.#config.serverSideEncryption,
      );
    if (this.#config.sseKmsKeyId) {
      if (this.#config.serverSideEncryption !== "aws:kms")
        throw new ObjectStoreError(
          "invalid_argument",
          BACKEND,
          "sseKmsKeyId needs serverSideEncryption aws:kms",
        );
      headers.set(
        "x-amz-server-side-encryption-aws-kms-key-id",
        this.#config.sseKmsKeyId,
      );
    }
    if (mode.kind === "create") headers.set("if-none-match", "*");
    if (mode.kind === "update") {
      if (!mode.version.eTag) {
        throw new ObjectStoreError(
          "invalid_argument",
          BACKEND,
          `${key}: ifMatch needs an eTag`,
        );
      }
      headers.set("if-match", mode.version.eTag);
    }
    const res = await this.#request(key, "PUT", this.#url(key), headers, body);
    if (res.ok) return { eTag: res.headers.get("etag") ?? undefined };
    const text = await errorBody(res);
    // A failed `If-None-Match: *` is 412; a concurrent conditional write is 409.
    if (mode.kind === "create" && (res.status === 412 || res.status === 409)) {
      throw new ObjectStoreError("already_exists", BACKEND, `${key} exists`, {
        status: res.status,
      });
    }
    if (mode.kind === "update" && res.status === 409) {
      throw new ObjectStoreError(
        "precondition_failed",
        BACKEND,
        `${key}: concurrent write`,
        {
          status: 409,
        },
      );
    }
    throw errorForStatus(BACKEND, key, res.status, text);
  }

  async delete(key: ObjectKey): Promise<void> {
    const res = await this.#request(
      key,
      "DELETE",
      this.#url(key),
      new Headers(),
    );
    if (!res.ok && res.status !== 404)
      throw errorForStatus(BACKEND, key, res.status, "");
  }

  async head(key: ObjectKey): Promise<ObjectMeta | undefined> {
    const res = await this.#request(key, "HEAD", this.#url(key), new Headers());
    if (res.status === 404) return undefined;
    if (!res.ok) throw errorForStatus(BACKEND, key, res.status, "");
    return {
      key,
      size: Number(res.headers.get("content-length") ?? 0),
      lastModified: new Date(res.headers.get("last-modified") ?? 0),
      version: versionOf(
        res.headers.get("etag"),
        res.headers.get("x-amz-version-id"),
      ),
      metadata: metadataFromHeaders(res.headers, "x-amz-meta-"),
    };
  }

  async getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject> {
    const headers = new Headers();
    if (range) headers.set("range", `bytes=${range.start}-${range.end - 1}`);
    const res = await this.#request(key, "GET", this.#url(key), headers);
    if (!res.ok || !res.body)
      throw errorForStatus(BACKEND, key, res.status, await errorBody(res));
    return {
      meta: {
        key,
        size: objectSizeFromHeaders(res.headers),
        lastModified: new Date(res.headers.get("last-modified") ?? 0),
        version: versionOf(
          res.headers.get("etag"),
          res.headers.get("x-amz-version-id"),
        ),
        metadata: metadataFromHeaders(res.headers, "x-amz-meta-"),
      },
      body: res.body,
    };
  }

  async list(prefix: string, cursor: string | undefined, pageSize: number) {
    const url = this.#url();
    url.searchParams.set("list-type", "2");
    url.searchParams.set("prefix", prefix);
    url.searchParams.set("max-keys", String(pageSize));
    if (cursor) url.searchParams.set("continuation-token", cursor);
    const res = await this.#request(prefix, "GET", url, new Headers());
    const text = await res.text();
    if (!res.ok) throw errorForStatus(BACKEND, prefix, res.status, text);
    const items: ObjectMeta[] = xmlElements(text, "Contents").map((c) => ({
      key: (xmlText(c, "Key") ?? "") as ObjectKey,
      size: Number(xmlText(c, "Size") ?? 0),
      lastModified: new Date(xmlText(c, "LastModified") ?? 0),
      version: versionOf(xmlText(c, "ETag") ?? null, null),
      metadata: {},
    }));
    const truncated = xmlText(text, "IsTruncated") === "true";
    const next = xmlText(text, "NextContinuationToken");
    return truncated && next
      ? // `xmlText` already decoded entities; decoding again would alter a
        // cursor that legitimately contains "&amp;".
        { items, nextCursor: next }
      : { items };
  }

  async presignGet(key: ObjectKey, ttlSeconds: number): Promise<URL> {
    return presignUrl({
      method: "GET",
      url: this.#url(key),
      region: this.#config.region,
      credentials: await this.#credentials(),
      expiresSeconds: ttlSeconds,
    });
  }
}

function versionOf(eTag: string | null, version: string | null): ObjectVersion {
  return {
    ...(eTag ? { eTag } : {}),
    ...(version && version !== "null" ? { version } : {}),
  };
}

export function createS3Store(config: S3Config): BaseStore {
  return new BaseStore(new S3Transport(config), config.pageSize);
}
