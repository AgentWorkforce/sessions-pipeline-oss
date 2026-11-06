/**
 * Permanent erasure for the sessions-prod transcript bucket, with only the
 * eraser key (96BullSessions #100: ListBucket/ListBucketVersions under
 * `96bulls/`, DeleteObject/DeleteObjectVersion, no GetObject, no key use).
 *
 * Every version and delete marker of a key (or under a prefix) is deleted by
 * version id, so nothing waits for lifecycle: a plain DELETE on a versioned
 * bucket would only add a delete marker and leave the bytes for a day.
 */
import { encodeKeyPath, sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { signRequest, type AwsCredentials } from "../../../packages/object-store/src/sigv4.ts";
import { xmlElements, xmlText } from "../../../packages/object-store/src/xml.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";

export interface Eraser {
  /** Every version of exactly this key. Returns versions removed. */
  eraseObject(key: string): Promise<number>;
  /** Every version of every key under `prefix` (which ends in `/`), manifests first. */
  erasePrefix(prefix: string): Promise<number>;
  /**
   * Every version of every key under `base` (ends in `/`) that sits in the
   * session directory `base + d=yyyy/mm/dd/ + sessionSegment + /`, whatever
   * its date. Used for relayhistory-api's live objects, whose `d=` the eraser
   * cannot read (it holds no GetObject).
   */
  eraseSessionAnyDate(base: string, sessionSegment: string, before?: Date): Promise<{ deleted: number; kept: number }>;
  /** Every version of exactly this key last modified before `before` (all when unset). */
  eraseObjectBefore(key: string, before: Date): Promise<{ deleted: number; kept: number }>;
  /** Keys with any version under `prefix` (ends in `/`). */
  listKeys(prefix: string): Promise<string[]>;
}

/** True when `key` is inside `base` + `d=yyyy/mm/dd/` + `sessionSegment` + `/`. */
export function inSessionAnyDate(base: string, sessionSegment: string, key: string): boolean {
  if (!key.startsWith(base)) return false;
  const rest = key.slice(base.length);
  const m = /^d=\d{4}\/\d{2}\/\d{2}\/([^/]+)\//.exec(rest);
  return !!m && m[1] === sessionSegment;
}

export interface S3EraserConfig {
  bucket: string;
  region: string;
  /** Path-style override (MinIO). */
  endpoint?: string;
  credentials: AwsCredentials;
  fetch?: typeof fetch;
}

export interface PreservedVersionEraser {
  /** Keep the current non-delete barrier version; permanently remove only older versions. */
  eraseObjectExcept(key: string, preservedVersionId: string): Promise<{ deleted: number; kept: number }>;
}

interface Version {
  key: string;
  versionId: string;
  lastModified?: number;
  latest: boolean;
  deleteMarker: boolean;
}

/** Split versions into those strictly older than `before` (erasable) and the rest (kept). */
function cut<T extends { lastModified?: number }>(list: T[], before?: Date): { erase: T[]; kept: number } {
  if (!before) return { erase: list, kept: 0 };
  const limit = before.getTime();
  // A version with no readable time is kept: never erase what might be new.
  const erase = list.filter((v) => v.lastModified !== undefined && v.lastModified < limit);
  return { erase, kept: list.length - erase.length };
}

const coded = (code: string, message: string) => Object.assign(new Error(message), { code });

// S3 inventory is destructive authority. The lightweight value extractor alone
// cannot distinguish a complete document from truncated/malformed XML.
function assertInventoryXml(xml: string): void {
  const stack: string[] = []; let at = 0, roots = 0;
  const tags = /<[^>]*>/g; let token: RegExpExecArray | null;
  while ((token = tags.exec(xml))) {
    const text = xml.slice(at, token.index);
    if (text.includes("<") || (!stack.length && text.trim())) throw coded("erasure_inventory", "malformed version inventory");
    const tag = token[0]; at = tags.lastIndex;
    if (/^<\?xml\s[^?]*\?>$/.test(tag) && !roots && !stack.length) continue;
    const close = /^<\/([A-Za-z_][\w:.-]*)\s*>$/.exec(tag);
    if (close) { if (stack.pop() !== close[1]) throw coded("erasure_inventory", "unbalanced version inventory"); continue; }
    const open = /^<([A-Za-z_][\w:.-]*)(?:\s+[A-Za-z_][\w:.-]*=(?:"[^"<>]*"|'[^'<>]*'))*\s*(\/?)>$/.exec(tag);
    if (!open) throw coded("erasure_inventory", "malformed version inventory");
    if (!stack.length && (++roots !== 1 || open[1] !== "ListVersionsResult")) throw coded("erasure_inventory", "unknown version inventory");
    if (!open[2]) stack.push(open[1]!);
  }
  if (stack.length || roots !== 1 || xml.slice(at).trim()) throw coded("erasure_inventory", "incomplete version inventory");
}

export function createS3Eraser(cfg: S3EraserConfig): Eraser & PreservedVersionEraser {
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const url = (key = "") => {
    if (cfg.endpoint) {
      const u = new URL(cfg.endpoint);
      u.pathname = `${u.pathname.replace(/\/+$/, "")}/${cfg.bucket}/${key ? encodeKeyPath(key) : ""}`;
      return u;
    }
    return new URL(`https://${cfg.bucket}.s3.${cfg.region}.amazonaws.com/${key ? encodeKeyPath(key) : ""}`);
  };
  const request = async (method: string, u: URL) => {
    const headers = new Headers();
    await signRequest({ method, url: u, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: cfg.region, credentials: cfg.credentials });
    const res = await doFetch(u, { method, headers });
    // Status only: S3 error bodies can echo keys.
    if (!res.ok && !(method === "DELETE" && res.status === 404)) throw coded(`s3_${res.status}`, `S3 ${method} failed`);
    return res;
  };

  async function versions(prefix: string, strict = false): Promise<Version[]> {
    const out: Version[] = [];
    const pages = new Set<string>();
    let keyMarker: string | undefined;
    let versionMarker: string | undefined;
    for (;;) {
      const u = url();
      u.searchParams.set("versions", "");
      u.searchParams.set("prefix", prefix);
      u.searchParams.set("max-keys", "1000");
      if (keyMarker !== undefined) u.searchParams.set("key-marker", keyMarker);
      if (versionMarker !== undefined) u.searchParams.set("version-id-marker", versionMarker);
      const response = await request("GET", u);
      let xml: string;
      if (!strict) xml = await response.text();
      else {
        const reader = response.body?.getReader(), chunks: Uint8Array[] = []; let size = 0;
        try {
          if (!reader) throw coded("erasure_inventory", "empty version inventory");
          for (;;) {
            const r = await reader.read(); if (r.done) break;
            size += r.value.byteLength;
            if (size > 16 * 1024 * 1024) throw coded("erasure_inventory", "version inventory byte bound");
            chunks.push(r.value);
          }
          const body = new Uint8Array(size); let at = 0;
          for (const chunk of chunks) { body.set(chunk, at); at += chunk.byteLength; }
          try { xml = new TextDecoder("utf-8", { fatal: true }).decode(body); }
          catch { throw coded("erasure_inventory", "version inventory encoding"); }
        } finally { await reader?.cancel().catch(() => {}); reader?.releaseLock(); }
      }
      if (strict) assertInventoryXml(xml);
      for (const tag of ["Version", "DeleteMarker"])
        for (const el of xmlElements(xml, tag)) {
          const key = xmlText(el, "Key");
          const versionId = xmlText(el, "VersionId");
          const modified = Date.parse(xmlText(el, "LastModified") ?? "");
          const latest = xmlText(el, "IsLatest");
          if (strict && (!key || !versionId || /[\x00-\x1f\x7f]/u.test(versionId) || (latest !== "true" && latest !== "false")))
            throw coded("erasure_inventory", "incomplete version entry");
          // xmlText already decodes entities; decoding again would mangle a literal `&amp;`.
          if (key !== undefined && versionId !== undefined)
            out.push({ key, versionId, latest: xmlText(el, "IsLatest") === "true", deleteMarker: tag === "DeleteMarker", ...(Number.isFinite(modified) ? { lastModified: modified } : {}) });
        }
      const truncated = xmlText(xml, "IsTruncated");
      if (strict && out.length > 10000) throw coded("erasure_inventory", "version inventory exceeds bound");
      if (strict && truncated !== "true" && truncated !== "false") throw coded("erasure_inventory", "version pagination is unproven");
      if (truncated !== "true") return out;
      keyMarker = xmlText(xml, "NextKeyMarker");
      versionMarker = xmlText(xml, "NextVersionIdMarker");
      if (strict) {
        const cursor = JSON.stringify([keyMarker, versionMarker]);
        if (keyMarker === undefined || versionMarker === undefined || pages.has(cursor) || pages.size >= 100 || out.length > 10000)
          throw coded("erasure_inventory", "version pagination is incomplete or unbounded");
        pages.add(cursor);
      }
      if (keyMarker === undefined) return out;
    }
  }

  async function remove(list: Version[]): Promise<number> {
    if (list.some((v) => v.key.split("/").some((seg) => seg === "." || seg === "..")))
      throw coded("erasure_key_shape", "a key with a dot segment would be normalized away");
    // Manifests first, so no reader finds a manifest whose chunks are gone.
    list.sort((a, b) => Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")));
    for (const v of list) {
      const u = url(v.key);
      u.searchParams.set("versionId", v.versionId);
      await request("DELETE", u);
    }
    return list.length;
  }

  return {
    eraseObjectExcept: async (key, preservedVersionId) => {
      if (!preservedVersionId || /[\x00-\x1f\x7f]/u.test(preservedVersionId))
        throw coded("erasure_preserved_version", "a concrete preserved version is required");
      const all = (await versions(key, true)).filter((v) => v.key === key);
      const latest = all.filter((v) => v.latest);
      if (latest.length !== 1 || latest[0]!.deleteMarker || latest[0]!.versionId !== preservedVersionId)
        throw coded("erasure_barrier_not_current", "the preserved non-delete barrier must be current");
      const erase = all.filter((v) => v.versionId !== preservedVersionId);
      const deleted = await remove(erase);
      const remaining = (await versions(key, true)).filter((v) => v.key === key);
      if (remaining.length !== 1 || remaining[0]!.versionId !== preservedVersionId || !remaining[0]!.latest || remaining[0]!.deleteMarker)
        throw coded("erasure_inventory_changed", "permanent erasure must leave only the verified current barrier");
      return { deleted, kept: 1 };
    },
    eraseObject: async (key) => remove((await versions(key)).filter((v) => v.key === key)),
    erasePrefix: async (prefix) => {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      return remove(await versions(prefix));
    },
    eraseSessionAnyDate: async (base, segment, before) => {
      if (!base.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const { erase, kept } = cut((await versions(`${base}d=`)).filter((v) => inSessionAnyDate(base, segment, v.key)), before);
      return { deleted: await remove(erase), kept };
    },
    eraseObjectBefore: async (key, before) => {
      const { erase, kept } = cut((await versions(key)).filter((v) => v.key === key), before);
      return { deleted: await remove(erase), kept };
    },
    listKeys: async (prefix) => {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      return [...new Set((await versions(prefix)).map((v) => v.key))];
    },
  };
}

/** The same contract over a plain (unversioned) store, for tests and local runs. */
export function storeEraser(store: ObjectStore): Eraser {
  return {
    async eraseObject(key) {
      const had = await store.head(objectKey(key));
      await store.delete(objectKey(key));
      return had ? 1 : 0;
    },
    async erasePrefix(prefix) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => i.key));
        cursor = page.nextCursor;
      } while (cursor);
      keys.sort((a, b) => Number(b.endsWith("/manifest.json")) - Number(a.endsWith("/manifest.json")));
      for (const k of keys) await store.delete(objectKey(k));
      return keys.length;
    },
    async eraseSessionAnyDate(base, segment, before) {
      if (!base.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const items: { key: string; lastModified: number }[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(base.slice(0, -1)), cursor);
        for (const i of page.items)
          if (inSessionAnyDate(base, segment, i.key)) items.push({ key: i.key, lastModified: i.lastModified.getTime() });
        cursor = page.nextCursor;
      } while (cursor);
      const { erase, kept } = cut(items, before);
      erase.sort((a, b) => Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")));
      for (const i of erase) await store.delete(objectKey(i.key));
      return { deleted: erase.length, kept };
    },
    async eraseObjectBefore(key, before) {
      const meta = await store.head(objectKey(key));
      if (!meta) return { deleted: 0, kept: 0 };
      if (meta.lastModified.getTime() >= before.getTime()) return { deleted: 0, kept: 1 };
      await store.delete(objectKey(key));
      return { deleted: 1, kept: 0 };
    },
    async listKeys(prefix) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => i.key));
        cursor = page.nextCursor;
      } while (cursor);
      return keys;
    },
  };
}
