/**
 * Locker init: creates the raw bucket (idempotent) and enables versioning.
 * Runs as a one-shot container or Kubernetes Job in every stage, because the
 * upstream MinIO chart's bucket setup is a Helm hook and Alchemy drops hooks.
 * Plain SigV4 over fetch; no SDK.
 */
import { createHash, createHmac } from "node:crypto";
import { lockerEnv } from "../shared/env.ts";
import { lockerCredentials } from "../shared/locker.ts";
import { errorFields, log } from "../shared/log.ts";

const sha256 = (s: string | Uint8Array) => createHash("sha256").update(s).digest("hex");
const hmac = (k: Buffer | string, s: string) => createHmac("sha256", k).update(s).digest();

export async function s3Request(input: {
  endpoint: string; region: string; accessKeyId: string; secretAccessKey: string; sessionToken?: string;
  method: string; path: string; query?: string; body?: string;
}): Promise<Response> {
  const url = new URL(`${input.endpoint.replace(/\/$/, "")}${input.path}${input.query ? `?${input.query}` : ""}`);
  const now = new Date();
  const amzDate = now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  const date = amzDate.slice(0, 8);
  const body = input.body ?? "";
  const payloadHash = sha256(body);
  const headers: Record<string, string> = { host: url.host, "x-amz-content-sha256": payloadHash, "x-amz-date": amzDate };
  if (input.sessionToken) headers["x-amz-security-token"] = input.sessionToken;
  const signedHeaders = Object.keys(headers).sort().join(";");
  const canonicalHeaders = Object.keys(headers).sort().map((k) => `${k}:${headers[k]}\n`).join("");
  const canonicalQuery = url.searchParams.toString();
  const canonicalRequest = [input.method, url.pathname, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash].join("\n");
  const scope = `${date}/${input.region}/s3/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", amzDate, scope, sha256(canonicalRequest)].join("\n");
  const kSigning = hmac(hmac(hmac(hmac(`AWS4${input.secretAccessKey}`, date), input.region), "s3"), "aws4_request");
  const signature = createHmac("sha256", kSigning).update(stringToSign).digest("hex");
  headers.authorization = `AWS4-HMAC-SHA256 Credential=${input.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  const { host: _h, ...sendHeaders } = headers;
  return fetch(url, { method: input.method, headers: sendHeaders, body: body || undefined });
}

export async function ensureBucket(timeoutMs = 120_000): Promise<void> {
  const l = lockerEnv();
  const provider = lockerCredentials();
  const creds = typeof provider === "function" ? await provider() : provider;
  // No endpoint: AWS S3 in the bucket's region (path-style URL on the regional endpoint).
  const endpoint = l.endpoint ?? `https://s3.${l.region}.amazonaws.com`;
  const base = { endpoint, region: l.region, accessKeyId: creds.accessKeyId, secretAccessKey: creds.secretAccessKey, ...(creds.sessionToken ? { sessionToken: creds.sessionToken } : {}) };
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  for (;;) {
    attempt += 1;
    try {
      // An existing bucket (e.g. created by IaC on AWS) is left alone: only versioning is confirmed.
      const head = await s3Request({ ...base, method: "HEAD", path: `/${l.bucket}` });
      if (head.ok) {
        const v = await s3Request({ ...base, method: "GET", path: `/${l.bucket}`, query: "versioning" });
        const body = v.ok ? await v.text() : "";
        if (/<Status>Enabled<\/Status>/.test(body)) {
          log.info("locker bucket ready", { ok: true, attempt, reason: "exists" });
          return;
        }
      }
      const res = await s3Request({ ...base, method: "PUT", path: `/${l.bucket}` });
      if (res.ok || res.status === 409) {
        const v = await s3Request({ ...base, method: "PUT", path: `/${l.bucket}`, query: "versioning", body: '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' });
        if (!v.ok) {
          // Manifests are overwritten per revision; without versioning there is no history. Not ready.
          log.warn("locker versioning not enabled yet", { attempt, error_code: `http_${v.status}` });
        } else {
          log.info("locker bucket ready", { ok: true, attempt, reason: res.status === 409 ? "exists" : "created" });
          return;
        }
      } else {
      const text = await res.text();
      if (!/BucketAlreadyOwnedByYou|BucketAlreadyExists/.test(text))
        log.warn("locker bucket create refused", { attempt, error_code: `http_${res.status}` });
      }
    } catch (err) {
      if (attempt % 5 === 1) log.info("waiting for locker", { attempt, ...errorFields(err) });
    }
    if (Date.now() > deadline) throw new Error("96bulls: locker did not become ready");
    await new Promise((r) => setTimeout(r, 2_000));
  }
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "locker-init";
  ensureBucket()
    .then(() => process.exit(0))
    .catch((err) => {
      log.error("locker init failed", errorFields(err));
      process.exit(1);
    });
}
