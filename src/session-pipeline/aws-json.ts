/**
 * The two AWS JSON-protocol calls the session pipeline Lambda makes (Secrets
 * Manager GetSecretValue, SQS SendMessage), signed with SigV4 by hand so the
 * bundle carries no AWS SDK. The object store has its own S3 signer
 * (packages/object-store/src/sigv4.ts), which is fixed to the `s3` service.
 *
 * Credentials come from the Lambda environment (the execution role's session).
 * Nothing here logs; an error carries the AWS error type and status only, never
 * a response body (which can echo the request).
 */
import { createHash, createHmac } from "node:crypto";

export interface AwsSessionCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export class AwsCallError extends Error {
  readonly code: string;
  readonly status: number;
  readonly retryable: boolean;
  constructor(service: string, status: number, type: string | null) {
    super(`${service} call failed (status ${status}${type ? `, ${type}` : ""})`);
    this.name = "AwsCallError";
    this.status = status;
    this.code = type ?? `http_${status}`;
    // Throttling and server errors are worth another attempt; client errors are not.
    this.retryable = status >= 500 || status === 429 || /Throttl/i.test(type ?? "");
  }
}

const sha256Hex = (data: string) => createHash("sha256").update(data, "utf8").digest("hex");
const hmac = (key: Buffer | string, data: string) => createHmac("sha256", key).update(data, "utf8").digest();

function amzDate(now: Date) {
  const datetime = now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
  return { datetime, date: datetime.slice(0, 8) };
}

function canonicalQuery(search: URLSearchParams): string {
  const enc = (s: string) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
  return [...search.entries()]
    .map(([k, v]) => [enc(k), enc(v)] as const)
    .sort(([a, av], [b, bv]) => (a < b ? -1 : a > b ? 1 : av < bv ? -1 : av > bv ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
}

/**
 * SigV4 for any service: returns the headers to send (the given headers plus
 * x-amz-date, the session token and authorization). Signs host and every
 * header passed in.
 */
export function signV4(args: {
  method: string;
  url: URL;
  service: string;
  region: string;
  headers: Record<string, string>;
  body: string;
  credentials: AwsSessionCredentials;
  now?: Date;
}): Record<string, string> {
  const { datetime, date } = amzDate(args.now ?? new Date());
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(args.headers)) headers[k.toLowerCase()] = v;
  headers["host"] = args.url.host;
  headers["x-amz-date"] = datetime;
  if (args.credentials.sessionToken) headers["x-amz-security-token"] = args.credentials.sessionToken;
  const names = Object.keys(headers).sort();
  const signedHeaders = names.join(";");
  const canonicalRequest = [
    args.method,
    args.url.pathname || "/",
    canonicalQuery(args.url.searchParams),
    names.map((n) => `${n}:${headers[n]!.trim().replace(/\s+/g, " ")}\n`).join(""),
    signedHeaders,
    sha256Hex(args.body),
  ].join("\n");
  const scope = `${date}/${args.region}/${args.service}/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", datetime, scope, sha256Hex(canonicalRequest)].join("\n");
  const kDate = hmac(`AWS4${args.credentials.secretAccessKey}`, date);
  const kSigning = hmac(hmac(hmac(kDate, args.region), args.service), "aws4_request");
  const signature = createHmac("sha256", kSigning).update(stringToSign, "utf8").digest("hex");
  headers["authorization"] =
    `AWS4-HMAC-SHA256 Credential=${args.credentials.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  delete headers["host"];
  return headers;
}

/** The Lambda runtime's role credentials. */
export function lambdaCredentials(env: NodeJS.ProcessEnv = process.env): AwsSessionCredentials {
  const accessKeyId = env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) throw new Error("no AWS credentials in the environment");
  return { accessKeyId, secretAccessKey, ...(env.AWS_SESSION_TOKEN ? { sessionToken: env.AWS_SESSION_TOKEN } : {}) };
}

/**
 * The AWS JSON protocol version each service speaks. Secrets Manager is awsJson1_1: sent
 * `application/x-amz-json-1.0` it answers 404 `UnknownOperationException` with no `__type`
 * (seen on aws-sessions-prod 2026-11-02, every GetSecretValue). SQS is awsJson1_0.
 */
export const JSON_PROTOCOL: Record<"secretsmanager" | "sqs", string> = {
  secretsmanager: "application/x-amz-json-1.1",
  sqs: "application/x-amz-json-1.0",
};

/** One JSON-protocol call (`X-Amz-Target`), e.g. secretsmanager.GetSecretValue. */
export async function awsJsonCall<T>(args: {
  service: "secretsmanager" | "sqs";
  target: string;
  region: string;
  body: Record<string, unknown>;
  credentials: AwsSessionCredentials;
  fetcher?: typeof fetch;
  timeoutMs?: number;
}): Promise<T> {
  const url = new URL(`https://${args.service}.${args.region}.amazonaws.com/`);
  const body = JSON.stringify(args.body);
  const headers = signV4({
    method: "POST",
    url,
    service: args.service,
    region: args.region,
    headers: { "content-type": JSON_PROTOCOL[args.service], "x-amz-target": args.target },
    body,
    credentials: args.credentials,
  });
  const response = await (args.fetcher ?? fetch)(url, {
    method: "POST",
    headers,
    body,
    signal: AbortSignal.timeout(args.timeoutMs ?? 10_000),
  });
  const text = await response.text();
  if (!response.ok) {
    let type: string | null = null;
    try {
      const parsed = JSON.parse(text) as { __type?: unknown };
      if (typeof parsed.__type === "string") type = parsed.__type.split("#").pop() ?? null;
    } catch {
      // Not JSON: report the status only.
    }
    throw new AwsCallError(args.service, response.status, type);
  }
  return (text ? JSON.parse(text) : {}) as T;
}

/** Asynchronous Lambda invoke (`X-Amz-Invocation-Type: Event`): the reconcile hands its cursor to the next link. */
export async function invokeAsync(args: {
  region: string;
  functionName: string;
  payload: string;
  credentials: AwsSessionCredentials;
  fetcher?: typeof fetch;
}): Promise<void> {
  if (!args.functionName) throw new Error("invokeAsync: no function name");
  const url = new URL(`https://lambda.${args.region}.amazonaws.com/2015-03-31/functions/${encodeURIComponent(args.functionName)}/invocations`);
  const headers = signV4({
    method: "POST",
    url,
    service: "lambda",
    region: args.region,
    headers: { "content-type": "application/json", "x-amz-invocation-type": "Event" },
    body: args.payload,
    credentials: args.credentials,
  });
  const response = await (args.fetcher ?? fetch)(url, { method: "POST", headers, body: args.payload, signal: AbortSignal.timeout(10_000) });
  await response.arrayBuffer();
  if (response.status !== 202) throw new AwsCallError("lambda", response.status, response.headers.get("x-amzn-errortype"));
}
