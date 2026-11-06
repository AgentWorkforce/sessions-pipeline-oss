/**
 * Google Cloud Storage. Objects go through the XML API (the same one the Rust
 * adapter's `object_store` uses, so `x-goog-meta-*` round-trips); listings use
 * the JSON API.
 */
import {
  BaseStore,
  cachedProvider,
  errorBody,
  metadataFromHeaders,
  objectSizeFromHeaders,
  send,
  type Expiring,
  type PutMode,
  type Provider,
  type Transport,
} from "./core.js";
import {
  encodeKeyPath,
  rfc3986,
  rsaSha256Sign,
  sha256Hex,
  toHex,
} from "./crypto.js";
import { ObjectStoreError, errorForStatus } from "./errors.js";
import type { ObjectKey } from "./key.js";
import type {
  ByteRange,
  GetObject,
  Metadata,
  ObjectMeta,
  ObjectVersion,
} from "./types.js";

export interface GcsToken extends Expiring {
  /** OAuth2 access token, e.g. a downscoped (Credential Access Boundary) token. */
  accessToken: string;
}

/** Service-account key used only to sign V4 URLs; held in memory. */
export interface GcsSigningKey {
  clientEmail: string;
  /** PEM PKCS#8 (`-----BEGIN PRIVATE KEY-----`). */
  privateKey: string;
}

export interface GcsConfig {
  bucket: string;
  /** API base, e.g. `http://localhost:4443` for fake-gcs-server. */
  baseUrl?: string;
  /** A bearer token or a function minting one (cached until 5 min before expiry). */
  token: Provider<GcsToken>;
  /** Needed for `presignGet`; without it presign is `unsupported`. */
  signingKey?: GcsSigningKey;
  pageSize?: number;
  fetch?: typeof fetch;
}

const BACKEND = "gcs";
const DEFAULT_BASE = "https://storage.googleapis.com";

interface JsonListResponse {
  items?: {
    name: string;
    size: string;
    updated: string;
    etag?: string;
    generation?: string;
  }[];
  nextPageToken?: string;
}

class GcsTransport implements Transport {
  readonly backend = BACKEND;
  readonly #config: GcsConfig;
  readonly #base: string;
  readonly #token: () => Promise<GcsToken>;
  readonly #fetch: typeof fetch;

  constructor(config: GcsConfig) {
    this.#config = config;
    this.#base = (config.baseUrl ?? DEFAULT_BASE).replace(/\/+$/, "");
    this.#token = cachedProvider(config.token);
    this.#fetch = config.fetch ?? ((...a) => fetch(...a));
  }

  #objectUrl(key: string): URL {
    return new URL(
      `${this.#base}/${rfc3986(this.#config.bucket)}/${encodeKeyPath(key)}`,
    );
  }

  async #request(
    key: string,
    method: string,
    url: URL,
    headers: Headers,
    body?: Uint8Array,
  ) {
    headers.set("authorization", `Bearer ${(await this.#token()).accessToken}`);
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
      headers.set(`x-goog-meta-${k}`, v);
    if (mode.kind === "create") headers.set("x-goog-if-generation-match", "0");
    if (mode.kind === "update") {
      if (!mode.version.version) {
        throw new ObjectStoreError(
          "invalid_argument",
          BACKEND,
          `${key}: ifMatch needs the object generation (version)`,
        );
      }
      headers.set("x-goog-if-generation-match", mode.version.version);
    }
    const res = await this.#request(
      key,
      "PUT",
      this.#objectUrl(key),
      headers,
      body,
    );
    if (res.ok) return versionOf(res.headers);
    const text = await errorBody(res);
    if (mode.kind === "create" && res.status === 412) {
      throw new ObjectStoreError("already_exists", BACKEND, `${key} exists`, {
        status: 412,
      });
    }
    throw errorForStatus(BACKEND, key, res.status, text);
  }

  async delete(key: ObjectKey): Promise<void> {
    const res = await this.#request(
      key,
      "DELETE",
      this.#objectUrl(key),
      new Headers(),
    );
    if (!res.ok && res.status !== 404)
      throw errorForStatus(BACKEND, key, res.status, "");
  }

  async head(key: ObjectKey): Promise<ObjectMeta | undefined> {
    const res = await this.#request(
      key,
      "HEAD",
      this.#objectUrl(key),
      new Headers(),
    );
    if (res.status === 404) return undefined;
    if (!res.ok) throw errorForStatus(BACKEND, key, res.status, "");
    return {
      key,
      size: Number(
        res.headers.get("x-goog-stored-content-length") ??
          res.headers.get("content-length") ??
          0,
      ),
      lastModified: new Date(res.headers.get("last-modified") ?? 0),
      version: versionOf(res.headers),
      metadata: metadataFromHeaders(res.headers, "x-goog-meta-"),
    };
  }

  async getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject> {
    const headers = new Headers();
    if (range) headers.set("range", `bytes=${range.start}-${range.end - 1}`);
    const res = await this.#request(key, "GET", this.#objectUrl(key), headers);
    if (!res.ok || !res.body)
      throw errorForStatus(BACKEND, key, res.status, await errorBody(res));
    const stored = res.headers.get("x-goog-stored-content-length");
    return {
      meta: {
        key,
        size:
          stored && !range
            ? Number(stored)
            : objectSizeFromHeaders(res.headers),
        lastModified: new Date(res.headers.get("last-modified") ?? 0),
        version: versionOf(res.headers),
        metadata: metadataFromHeaders(res.headers, "x-goog-meta-"),
      },
      body: res.body,
    };
  }

  /**
   * Keyset paging: the cursor is the last key returned and the next page
   * starts at `startOffset` (inclusive, so that key is dropped). This does not
   * depend on the server honouring `maxResults` exactly (fake-gcs-server
   * does not), and a cursor stays valid across processes.
   */
  async list(prefix: string, cursor: string | undefined, pageSize: number) {
    const url = new URL(
      `${this.#base}/storage/v1/b/${rfc3986(this.#config.bucket)}/o`,
    );
    url.searchParams.set("prefix", prefix);
    url.searchParams.set("maxResults", String(pageSize + 2));
    url.searchParams.set(
      "fields",
      "items(name,size,updated,etag,generation),nextPageToken",
    );
    if (cursor) url.searchParams.set("startOffset", cursor);
    const res = await this.#request(prefix, "GET", url, new Headers());
    if (!res.ok)
      throw errorForStatus(BACKEND, prefix, res.status, await errorBody(res));
    const json = (await res.json()) as JsonListResponse;
    const all: ObjectMeta[] = (json.items ?? [])
      .filter((o) => cursor === undefined || o.name > cursor)
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
      .map((o) => ({
        key: o.name as ObjectKey,
        size: Number(o.size),
        lastModified: new Date(o.updated),
        version: {
          ...(o.etag ? { eTag: o.etag } : {}),
          ...(o.generation ? { version: o.generation } : {}),
        },
        metadata: {},
      }));
    const items = all.slice(0, pageSize);
    const more =
      all.length > pageSize ||
      (json.nextPageToken !== undefined && items.length > 0);
    const last = items.at(-1);
    return more && last ? { items, nextCursor: last.key } : { items };
  }

  /** V4 signed URL (GOOG4-RSA-SHA256). */
  async presignGet(key: ObjectKey, ttlSeconds: number): Promise<URL> {
    const signer = this.#config.signingKey;
    if (!signer) {
      throw new ObjectStoreError(
        "unsupported",
        BACKEND,
        "presignGet needs a signingKey (a bearer token alone cannot sign URLs)",
      );
    }
    const url = this.#objectUrl(key);
    const datetime = new Date()
      .toISOString()
      .replace(/[-:]/g, "")
      .replace(/\.\d{3}/, "");
    const scope = `${datetime.slice(0, 8)}/auto/storage/goog4_request`;
    url.searchParams.set("X-Goog-Algorithm", "GOOG4-RSA-SHA256");
    url.searchParams.set("X-Goog-Credential", `${signer.clientEmail}/${scope}`);
    url.searchParams.set("X-Goog-Date", datetime);
    url.searchParams.set("X-Goog-Expires", String(ttlSeconds));
    url.searchParams.set("X-Goog-SignedHeaders", "host");
    const query = [...url.searchParams.entries()]
      .map(([k, v]) => [rfc3986(k), rfc3986(v)] as const)
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .map(([k, v]) => `${k}=${v}`)
      .join("&");
    const canonicalRequest = [
      "GET",
      url.pathname,
      query,
      `host:${url.host}\n`,
      "host",
      "UNSIGNED-PAYLOAD",
    ].join("\n");
    const stringToSign = [
      "GOOG4-RSA-SHA256",
      datetime,
      scope,
      await sha256Hex(canonicalRequest),
    ].join("\n");
    const signature = toHex(
      await rsaSha256Sign(signer.privateKey, stringToSign),
    );
    const out = new URL(`${url.origin}${url.pathname}?${query}`);
    out.searchParams.set("X-Goog-Signature", signature);
    return out;
  }
}

function versionOf(headers: Headers): ObjectVersion {
  const eTag = headers.get("etag");
  const generation = headers.get("x-goog-generation");
  return {
    ...(eTag ? { eTag } : {}),
    ...(generation ? { version: generation } : {}),
  };
}

export function createGcsStore(config: GcsConfig): BaseStore {
  return new BaseStore(new GcsTransport(config), config.pageSize);
}
