/**
 * Azure Blob Storage. Shared Key, SAS or Entra ID bearer auth. Metadata names
 * must be C# identifiers, so `trace-id` is stored as `trace_id` and mapped
 * back on read (the Rust adapter does the same).
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
  fromBase64,
  hmacSha256,
  rfc3986,
  toBase64,
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
import { xmlElements, xmlText } from "./xml.js";

export const AZURE_API_VERSION = "2021-12-02";

export type AzureAuth =
  /** Storage account key (customer config, Azurite). Enables presign. */
  | { kind: "sharedKey"; accountKey: string }
  /** A SAS (query string without `?`), e.g. a vended user-delegation SAS. */
  | { kind: "sas"; token: Provider<{ sas: string } & Expiring> }
  /** Entra ID access token. */
  | { kind: "bearer"; token: Provider<{ accessToken: string } & Expiring> };

export interface AzureBlobConfig {
  account: string;
  container: string;
  /**
   * Blob endpoint. Default `https://{account}.blob.core.windows.net`; Azurite
   * is `http://127.0.0.1:10000/devstoreaccount1`.
   */
  endpoint?: string;
  auth: AzureAuth;
  pageSize?: number;
  fetch?: typeof fetch;
}

const BACKEND = "azure";

class AzureTransport implements Transport {
  readonly backend = BACKEND;
  readonly #config: AzureBlobConfig;
  readonly #base: string;
  readonly #fetch: typeof fetch;
  readonly #sas?: () => Promise<{ sas: string }>;
  readonly #bearer?: () => Promise<{ accessToken: string }>;

  constructor(config: AzureBlobConfig) {
    this.#config = config;
    this.#base = (
      config.endpoint ?? `https://${config.account}.blob.core.windows.net`
    ).replace(/\/+$/, "");
    this.#fetch = config.fetch ?? ((...a) => fetch(...a));
    if (config.auth.kind === "sas")
      this.#sas = cachedProvider(config.auth.token);
    if (config.auth.kind === "bearer")
      this.#bearer = cachedProvider(config.auth.token);
  }

  #blobUrl(key: string): URL {
    return new URL(
      `${this.#base}/${rfc3986(this.#config.container)}/${encodeKeyPath(key)}`,
    );
  }

  async #request(
    key: string,
    method: string,
    url: URL,
    headers: Headers,
    body?: Uint8Array,
  ) {
    headers.set("x-ms-version", AZURE_API_VERSION);
    headers.set("x-ms-date", new Date().toUTCString());
    const auth = this.#config.auth;
    if (auth.kind === "sharedKey") {
      headers.set(
        "authorization",
        `SharedKey ${this.#config.account}:${await this.#sharedKeySignature(
          method,
          url,
          headers,
          body?.length ?? 0,
          auth.accountKey,
        )}`,
      );
    } else if (this.#sas) {
      const sas = new URLSearchParams(
        (await this.#sas()).sas.replace(/^\?/, ""),
      );
      sas.forEach((v, k) => url.searchParams.set(k, v));
    } else if (this.#bearer) {
      headers.set(
        "authorization",
        `Bearer ${(await this.#bearer()).accessToken}`,
      );
    }
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

  async #sharedKeySignature(
    method: string,
    url: URL,
    headers: Headers,
    contentLength: number,
    accountKey: string,
  ): Promise<string> {
    const h = (name: string) => headers.get(name) ?? "";
    const msHeaders: [string, string][] = [];
    headers.forEach((value, name) => {
      const lower = name.toLowerCase();
      if (lower.startsWith("x-ms-")) msHeaders.push([lower, value.trim()]);
    });
    msHeaders.sort(([a], [b]) => (a < b ? -1 : 1));
    const params = [...url.searchParams.entries()]
      .map(([k, v]) => [k.toLowerCase(), v] as const)
      .sort(([a], [b]) => (a < b ? -1 : 1));
    const resource =
      `/${this.#config.account}${url.pathname}` +
      params.map(([k, v]) => `\n${k}:${v}`).join("");
    const stringToSign = [
      method,
      h("content-encoding"),
      h("content-language"),
      contentLength > 0 ? String(contentLength) : "",
      h("content-md5"),
      h("content-type"),
      "",
      h("if-modified-since"),
      h("if-match"),
      h("if-none-match"),
      h("if-unmodified-since"),
      h("range"),
      msHeaders.map(([k, v]) => `${k}:${v}`).join("\n"),
      resource,
    ].join("\n");
    return toBase64(await hmacSha256(fromBase64(accountKey), stringToSign));
  }

  async put(
    key: ObjectKey,
    body: Uint8Array,
    mode: PutMode,
    metadata: Metadata,
    contentType: string,
  ): Promise<ObjectVersion> {
    const headers = new Headers({
      "content-type": contentType,
      "x-ms-blob-type": "BlockBlob",
    });
    for (const [k, v] of Object.entries(metadata)) {
      headers.set(`x-ms-meta-${k.replaceAll("-", "_")}`, v);
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
    const res = await this.#request(
      key,
      "PUT",
      this.#blobUrl(key),
      headers,
      body,
    );
    if (res.ok) return eTagOf(res.headers.get("etag"));
    const text = await errorBody(res);
    if (mode.kind === "create" && (res.status === 409 || res.status === 412)) {
      throw new ObjectStoreError("already_exists", BACKEND, `${key} exists`, {
        status: res.status,
      });
    }
    throw errorForStatus(BACKEND, key, res.status, text);
  }

  async delete(key: ObjectKey): Promise<void> {
    const res = await this.#request(
      key,
      "DELETE",
      this.#blobUrl(key),
      new Headers(),
    );
    if (!res.ok && res.status !== 404)
      throw errorForStatus(BACKEND, key, res.status, "");
  }

  async head(key: ObjectKey): Promise<ObjectMeta | undefined> {
    const res = await this.#request(
      key,
      "HEAD",
      this.#blobUrl(key),
      new Headers(),
    );
    if (res.status === 404) return undefined;
    if (!res.ok) throw errorForStatus(BACKEND, key, res.status, "");
    return {
      key,
      size: Number(res.headers.get("content-length") ?? 0),
      lastModified: new Date(res.headers.get("last-modified") ?? 0),
      version: eTagOf(res.headers.get("etag")),
      metadata: metadataFromHeaders(res.headers, "x-ms-meta-", "_"),
    };
  }

  async getObject(key: ObjectKey, range?: ByteRange): Promise<GetObject> {
    const headers = new Headers();
    if (range)
      headers.set("x-ms-range", `bytes=${range.start}-${range.end - 1}`);
    const res = await this.#request(key, "GET", this.#blobUrl(key), headers);
    if (!res.ok || !res.body)
      throw errorForStatus(BACKEND, key, res.status, await errorBody(res));
    return {
      meta: {
        key,
        size: objectSizeFromHeaders(res.headers),
        lastModified: new Date(res.headers.get("last-modified") ?? 0),
        version: eTagOf(res.headers.get("etag")),
        metadata: metadataFromHeaders(res.headers, "x-ms-meta-", "_"),
      },
      body: res.body,
    };
  }

  async list(prefix: string, cursor: string | undefined, pageSize: number) {
    const url = new URL(`${this.#base}/${rfc3986(this.#config.container)}`);
    url.searchParams.set("restype", "container");
    url.searchParams.set("comp", "list");
    url.searchParams.set("prefix", prefix);
    url.searchParams.set("maxresults", String(pageSize));
    if (cursor) url.searchParams.set("marker", cursor);
    const res = await this.#request(prefix, "GET", url, new Headers());
    const text = await res.text();
    if (!res.ok) throw errorForStatus(BACKEND, prefix, res.status, text);
    const items: ObjectMeta[] = xmlElements(text, "Blob").map((b) => ({
      key: (xmlText(b, "Name") ?? "") as ObjectKey,
      size: Number(xmlText(b, "Content-Length") ?? 0),
      lastModified: new Date(xmlText(b, "Last-Modified") ?? 0),
      version: eTagOf(xmlText(b, "Etag") ?? null),
      metadata: {},
    }));
    const next = xmlText(text, "NextMarker");
    // `xmlText` already decoded entities; decoding again would alter a
    // cursor that legitimately contains "&amp;".
    return next ? { items, nextCursor: next } : { items };
  }

  /** Service SAS (read-only, blob scope) signed with the account key. */
  async presignGet(key: ObjectKey, ttlSeconds: number): Promise<URL> {
    const auth = this.#config.auth;
    if (auth.kind !== "sharedKey") {
      throw new ObjectStoreError(
        "unsupported",
        BACKEND,
        "presignGet needs an account key (a vended SAS or bearer token cannot mint a SAS here)",
      );
    }
    const expiry = new Date(Date.now() + ttlSeconds * 1000)
      .toISOString()
      .replace(/\.\d{3}Z$/, "Z");
    const canonicalResource = `/blob/${this.#config.account}/${this.#config.container}/${key}`;
    const fields = {
      sp: "r",
      st: "",
      se: expiry,
      cr: canonicalResource,
      si: "",
      sip: "",
      spr: "",
      sv: AZURE_API_VERSION,
      sr: "b",
      snapshot: "",
      ses: "",
      rscc: "",
      rscd: "",
      rsce: "",
      rscl: "",
      rsct: "",
    };
    const sig = toBase64(
      await hmacSha256(
        fromBase64(auth.accountKey),
        Object.values(fields).join("\n"),
      ),
    );
    const url = this.#blobUrl(key);
    url.searchParams.set("sv", fields.sv);
    url.searchParams.set("sr", fields.sr);
    url.searchParams.set("sp", fields.sp);
    url.searchParams.set("se", fields.se);
    url.searchParams.set("sig", sig);
    return url;
  }
}

function eTagOf(eTag: string | null): ObjectVersion {
  return eTag ? { eTag } : {};
}

export function createAzureBlobStore(config: AzureBlobConfig): BaseStore {
  return new BaseStore(new AzureTransport(config), config.pageSize);
}
