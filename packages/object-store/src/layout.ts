/**
 * The canonical raw-session object layout (v1), identical to the Rust
 * `layout.rs`:
 *
 * ```text
 * {prefix}/v1/ws={workspace_id}/m={machine_id}/h={harness}/d={yyyy}/{mm}/{dd}/s={session_id}/
 *     manifest.json
 *     raw/{seq:010}-{start_offset}-{sha256_8}.{jsonl|ndjson|sqlite-rows}.zst
 *     normalized/v{N}/...
 * ```
 */
import { ObjectStoreError } from "./errors.js";
import { joinKey, objectKey, type ObjectKey } from "./key.js";

export const LAYOUT_VERSION = "v1";
export const MANIFEST_FILE = "manifest.json";

/** Upload generations are independent of raw.epoch (which is the scrub epoch). */
const PUBLICATION_UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
export function uploadGenerationPrefix(base: string, generation: string): string {
  if (!PUBLICATION_UUID.test(generation) || !base || base.split("/").some(x => !x || x === "." || x === ".."))
    throw new ObjectStoreError("invalid_argument", "layout", "invalid upload generation prefix");
  return base + "/upload-generations/g=" + generation;
}
export function parseUploadGenerationPrefix(prefix: string | undefined): { base: string; generation: string } | undefined {
  const marker = "/upload-generations/g=";
  const index = prefix?.lastIndexOf(marker) ?? -1;
  if (!prefix || index <= 0) return undefined;
  const generation = prefix.slice(index + marker.length), base = prefix.slice(0,index);
  if (!PUBLICATION_UUID.test(generation) || base.split("/").some(x => !x || x === "." || x === "..") || base.includes(marker)) return undefined;
  return {base,generation};
}
export function isPublicationManifestRest(rest: string): boolean {
  const match = /^publications\/([^/]+)\/manifest\.json$/.exec(rest);
  return !!match && PUBLICATION_UUID.test(match[1]!);
}
export function isManifestRest(rest: string): boolean {
  return rest === MANIFEST_FILE || isPublicationManifestRest(rest);
}
/** Immutable manifest; rawChunkKey remains beside the session root, shared in one generation. */
export function publicationManifestKey(loc: SessionLocation, publication: string): ObjectKey {
  if (!parseUploadGenerationPrefix(loc.prefix) || !PUBLICATION_UUID.test(publication))
    throw new ObjectStoreError("invalid_argument", "layout", "invalid upload publication");
  return joinKey(sessionPrefix(loc), "publications/" + publication + "/" + MANIFEST_FILE);
}


export const RAW_FORMATS = ["jsonl", "ndjson", "sqlite-rows"] as const;
export type RawFormat = (typeof RAW_FORMATS)[number];

export interface SessionLocation {
  /** Deployment-wide prefix; may contain `/`. */
  prefix?: string;
  workspaceId: string;
  machineId: string;
  harness: string;
  /** Session start date, UTC, as `yyyy-mm-dd` or a Date (its UTC date is used). */
  startDate: string | Date;
  sessionId: string;
}

export function sessionPrefix(loc: SessionLocation): ObjectKey {
  checkComponent("workspaceId", loc.workspaceId);
  checkComponent("machineId", loc.machineId);
  checkComponent("harness", loc.harness);
  checkComponent("sessionId", loc.sessionId);
  const [y, m, d] = utcDate(loc.startDate);
  return withPrefix(
    loc.prefix,
    `${LAYOUT_VERSION}/ws=${loc.workspaceId}/m=${loc.machineId}/h=${loc.harness}/d=${y}/${m}/${d}/s=${loc.sessionId}`,
  );
}

/** The workspace+machine prefix a vended credential is scoped to. */
export function machinePrefix(
  loc: Pick<SessionLocation, "prefix" | "workspaceId" | "machineId">,
): ObjectKey {
  checkComponent("workspaceId", loc.workspaceId);
  checkComponent("machineId", loc.machineId);
  return withPrefix(
    loc.prefix,
    `${LAYOUT_VERSION}/ws=${loc.workspaceId}/m=${loc.machineId}`,
  );
}

export function manifestKey(loc: SessionLocation): ObjectKey {
  return joinKey(sessionPrefix(loc), MANIFEST_FILE);
}

export function rawPrefix(loc: SessionLocation): ObjectKey {
  return joinKey(sessionPrefix(loc), "raw");
}

/** `sha256Hex` is the digest of the uncompressed chunk; its first 8 chars are used. */
export function rawChunkKey(
  loc: SessionLocation,
  seq: number,
  startOffset: number,
  sha256Hex: string,
  format: RawFormat,
): ObjectKey {
  if (!/^[0-9a-fA-F]{8,}$/.test(sha256Hex)) {
    throw new ObjectStoreError(
      "invalid_argument",
      "layout",
      `chunk digest ${JSON.stringify(sha256Hex)} is not hex`,
    );
  }
  checkInteger("seq", seq);
  checkInteger("startOffset", startOffset);
  const short = sha256Hex.slice(0, 8).toLowerCase();
  return joinKey(
    sessionPrefix(loc),
    `raw/${String(seq).padStart(10, "0")}-${startOffset}-${short}.${format}.zst`,
  );
}

export function normalizedKey(
  loc: SessionLocation,
  version: number,
  name: string,
): ObjectKey {
  checkInteger("version", version);
  return joinKey(sessionPrefix(loc), `normalized/v${version}/${name}`);
}

export interface RawChunkName {
  seq: number;
  startOffset: number;
  sha256_8: string;
  format: RawFormat;
}

/** Parse `{seq:010}-{start_offset}-{sha256_8}.{ext}.zst`. */
export function parseRawChunkName(fileName: string): RawChunkName | undefined {
  const m =
    /^(\d{10})-(\d+)-([0-9a-f]{8})\.(jsonl|ndjson|sqlite-rows)\.zst$/.exec(
      fileName,
    );
  if (!m) return undefined;
  return {
    seq: Number(m[1]),
    startOffset: Number(m[2]),
    sha256_8: m[3]!,
    format: m[4] as RawFormat,
  };
}

/** Parse a session prefix or any key under it back into its location. */
export function parseSessionKey(
  key: string,
): (SessionLocation & { startDate: string; rest: string }) | undefined {
  const m =
    /^(?:(.+)\/)?v1\/ws=([^/]+)\/m=([^/]+)\/h=([^/]+)\/d=(\d{4})\/(\d{2})\/(\d{2})\/s=([^/]+)(?:\/(.*))?$/.exec(
      key,
    );
  if (!m) return undefined;
  return {
    ...(m[1] ? { prefix: m[1] } : {}),
    workspaceId: m[2]!,
    machineId: m[3]!,
    harness: m[4]!,
    startDate: `${m[5]}-${m[6]}-${m[7]}`,
    sessionId: m[8]!,
    rest: m[9] ?? "",
  };
}

function withPrefix(prefix: string | undefined, tail: string): ObjectKey {
  const p = (prefix ?? "").replace(/^\/+|\/+$/g, "");
  return objectKey(p ? `${p}/${tail}` : tail);
}

function utcDate(date: string | Date): [string, string, string] {
  if (typeof date === "string") {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date);
    if (!m) {
      throw new ObjectStoreError(
        "invalid_argument",
        "layout",
        `startDate ${JSON.stringify(date)} must be yyyy-mm-dd`,
      );
    }
    return [m[1]!, m[2]!, m[3]!];
  }
  const iso = date.toISOString();
  return [iso.slice(0, 4), iso.slice(5, 7), iso.slice(8, 10)];
}

function checkComponent(name: string, value: string): void {
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(value) || value.startsWith(".")) {
    throw new ObjectStoreError(
      "invalid_argument",
      "layout",
      `${name} ${JSON.stringify(value)} must be 1-128 chars of [A-Za-z0-9._-] not starting with \`.\``,
    );
  }
}

function checkInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ObjectStoreError(
      "invalid_argument",
      "layout",
      `${name} must be a non-negative safe integer`,
    );
  }
}
