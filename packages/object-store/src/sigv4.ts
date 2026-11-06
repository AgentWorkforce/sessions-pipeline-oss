/** AWS Signature Version 4 for S3 (headers and presigned query), WebCrypto only. */
import { hmacSha256, rfc3986, sha256Hex, toHex } from "./crypto.js";

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  expiresAt?: Date;
}

export const UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

function amzDate(now: Date): { date: string; datetime: string } {
  const datetime = now
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}/, "");
  return { date: datetime.slice(0, 8), datetime };
}

function canonicalQuery(params: URLSearchParams): string {
  return [...params.entries()]
    .map(([k, v]) => [rfc3986(k), rfc3986(v)] as const)
    .sort(([a, av], [b, bv]) =>
      a < b ? -1 : a > b ? 1 : av < bv ? -1 : av > bv ? 1 : 0,
    )
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
}

async function signingKey(
  secret: string,
  date: string,
  region: string,
): Promise<Uint8Array> {
  const kDate = await hmacSha256(`AWS4${secret}`, date);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, "s3");
  return hmacSha256(kService, "aws4_request");
}

/**
 * Sign a request in place: adds `x-amz-date`, `x-amz-content-sha256`,
 * the session token and `authorization` to `headers`. Signs `host` and every
 * `x-amz-*` header.
 */
export async function signRequest(args: {
  method: string;
  url: URL;
  headers: Headers;
  payloadSha256: string;
  region: string;
  credentials: AwsCredentials;
  now?: Date;
}): Promise<void> {
  const { method, url, headers, region, credentials } = args;
  const { date, datetime } = amzDate(args.now ?? new Date());
  headers.set("x-amz-date", datetime);
  headers.set("x-amz-content-sha256", args.payloadSha256);
  if (credentials.sessionToken)
    headers.set("x-amz-security-token", credentials.sessionToken);

  const signed: [string, string][] = [["host", url.host]];
  headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower.startsWith("x-amz-"))
      signed.push([lower, value.trim().replace(/\s+/g, " ")]);
  });
  signed.sort(([a], [b]) => (a < b ? -1 : 1));
  const signedHeaders = signed.map(([k]) => k).join(";");
  const canonicalRequest = [
    method,
    url.pathname,
    canonicalQuery(url.searchParams),
    signed.map(([k, v]) => `${k}:${v}\n`).join(""),
    signedHeaders,
    args.payloadSha256,
  ].join("\n");
  const scope = `${date}/${region}/s3/aws4_request`;
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    datetime,
    scope,
    await sha256Hex(canonicalRequest),
  ].join("\n");
  const signature = toHex(
    await hmacSha256(
      await signingKey(credentials.secretAccessKey, date, region),
      stringToSign,
    ),
  );
  headers.set(
    "authorization",
    `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
  );
}

/** Presigned GET (query-string auth), host-only signed headers. */
export async function presignUrl(args: {
  method: string;
  url: URL;
  region: string;
  credentials: AwsCredentials;
  expiresSeconds: number;
  now?: Date;
}): Promise<URL> {
  const { url, region, credentials } = args;
  const { date, datetime } = amzDate(args.now ?? new Date());
  const scope = `${date}/${region}/s3/aws4_request`;
  const out = new URL(url);
  out.searchParams.set("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
  out.searchParams.set(
    "X-Amz-Credential",
    `${credentials.accessKeyId}/${scope}`,
  );
  out.searchParams.set("X-Amz-Date", datetime);
  out.searchParams.set("X-Amz-Expires", String(args.expiresSeconds));
  if (credentials.sessionToken)
    out.searchParams.set("X-Amz-Security-Token", credentials.sessionToken);
  out.searchParams.set("X-Amz-SignedHeaders", "host");
  const canonicalRequest = [
    args.method,
    out.pathname,
    canonicalQuery(out.searchParams),
    `host:${out.host}\n`,
    "host",
    UNSIGNED_PAYLOAD,
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    datetime,
    scope,
    await sha256Hex(canonicalRequest),
  ].join("\n");
  const signature = toHex(
    await hmacSha256(
      await signingKey(credentials.secretAccessKey, date, region),
      stringToSign,
    ),
  );
  out.searchParams.set("X-Amz-Signature", signature);
  return out;
}
