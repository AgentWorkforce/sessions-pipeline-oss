import { ObjectStoreError } from "./errors.js";

const MAX_KEY_BYTES = 1024;
declare const objectKeyBrand: unique symbol;

/**
 * A validated object key: `/`-separated non-empty segments of printable
 * ASCII, no `.`/`..` segment, no leading `.` (reserved for adapter
 * bookkeeping such as the fs store's `.rh/`), and none of the characters
 * backends encode differently. Same rules as the Rust `ObjectKey`.
 */
export type ObjectKey = string & { readonly [objectKeyBrand]: true };

export function objectKey(key: string): ObjectKey {
  const reason = invalidReason(key);
  if (reason) {
    throw new ObjectStoreError(
      "invalid_key",
      "key",
      `${JSON.stringify(key)}: ${reason}`,
    );
  }
  return key as ObjectKey;
}

export function isObjectKey(key: string): key is ObjectKey {
  return invalidReason(key) === undefined;
}

export function joinKey(base: ObjectKey, rest: string): ObjectKey {
  return objectKey(`${base}/${rest}`);
}

export function keyFileName(key: ObjectKey): string {
  return key.slice(key.lastIndexOf("/") + 1);
}

function invalidReason(key: string): string | undefined {
  if (key.length === 0) return "empty";
  if (key.length > MAX_KEY_BYTES) return "longer than 1024 bytes";
  if (key.startsWith(".")) return "leading `.` is reserved";
  for (const segment of key.split("/")) {
    if (segment === "")
      return "empty segment (leading, trailing or doubled `/`)";
    if (segment === "." || segment === "..") return "`.` or `..` segment";
  }
  for (const c of key) {
    const code = c.charCodeAt(0);
    if (code < 0x21 || code > 0x7e || "\\%*?#[]".includes(c)) {
      return `character ${JSON.stringify(c)} is not allowed`;
    }
  }
  return undefined;
}
