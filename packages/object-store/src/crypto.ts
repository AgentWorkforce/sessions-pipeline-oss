/** WebCrypto helpers; available in Cloudflare Workers and Node >= 20. */

const encoder = new TextEncoder();

export function utf8(s: string): Uint8Array {
  return encoder.encode(s);
}

export function toHex(bytes: ArrayBuffer | Uint8Array): string {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let out = "";
  for (const b of view) out += b.toString(16).padStart(2, "0");
  return out;
}

export function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("invalid hex");
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

export function toBase64(bytes: ArrayBuffer | Uint8Array): string {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let s = "";
  for (const b of view) s += String.fromCharCode(b);
  return btoa(s);
}

export function fromBase64(b64: string): Uint8Array {
  const s = atob(b64);
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

function buf(data: string | Uint8Array): Uint8Array<ArrayBuffer> {
  const bytes = typeof data === "string" ? utf8(data) : data;
  // Copy into a plain ArrayBuffer-backed view for WebCrypto's BufferSource.
  return new Uint8Array(bytes) as Uint8Array<ArrayBuffer>;
}

export async function sha256(data: string | Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", buf(data)));
}

export async function sha256Hex(data: string | Uint8Array): Promise<string> {
  return toHex(await sha256(data));
}

export async function hmacSha256(
  key: string | Uint8Array,
  data: string | Uint8Array,
): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    buf(key),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, buf(data)));
}

/** RSASSA-PKCS1-v1_5 / SHA-256 with a PEM PKCS#8 key (GCS V4 signing). */
export async function rsaSha256Sign(
  pkcs8Pem: string,
  data: string,
): Promise<Uint8Array> {
  const body = pkcs8Pem
    .replace(/-----BEGIN PRIVATE KEY-----/, "")
    .replace(/-----END PRIVATE KEY-----/, "")
    .replace(/\s+/g, "");
  const key = await crypto.subtle.importKey(
    "pkcs8",
    buf(fromBase64(body)),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return new Uint8Array(
    await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, buf(data)),
  );
}

/** RFC 3986 encoding (encodeURIComponent plus `!'()*`). */
export function rfc3986(s: string): string {
  return encodeURIComponent(s).replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

/** Encode each `/`-separated segment; `/` stays literal. */
export function encodeKeyPath(key: string): string {
  return key.split("/").map(rfc3986).join("/");
}
