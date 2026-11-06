import { describe, expect, test } from "bun:test";
import { awsJsonCall } from "../aws-json.ts";

const credentials = { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" };

/** Records the one request awsJsonCall makes and answers 200 with `{}`. */
function recorder() {
  const seen: { url: string; headers: Record<string, string> }[] = [];
  const fetcher = (async (input: URL | RequestInfo, init?: RequestInit) => {
    seen.push({ url: String(input), headers: Object.fromEntries(Object.entries((init?.headers ?? {}) as Record<string, string>).map(([k, v]) => [k.toLowerCase(), v])) });
    return new Response("{}", { status: 200 });
  }) as typeof fetch;
  return { seen, fetcher };
}

describe("awsJsonCall speaks each service's JSON protocol", () => {
  // Secrets Manager is awsJson1_1. Sent 1.0 it answers 404 UnknownOperationException, so the
  // pipeline could never read its secret (aws-sessions-prod, 2026-11-02).
  test("Secrets Manager gets application/x-amz-json-1.1", async () => {
    const r = recorder();
    await awsJsonCall({ service: "secretsmanager", target: "secretsmanager.GetSecretValue", region: "us-east-1", body: { SecretId: "s" }, credentials, fetcher: r.fetcher });
    expect(r.seen).toHaveLength(1);
    expect(r.seen[0].url).toBe("https://secretsmanager.us-east-1.amazonaws.com/");
    expect(r.seen[0].headers["content-type"]).toBe("application/x-amz-json-1.1");
    expect(r.seen[0].headers["x-amz-target"]).toBe("secretsmanager.GetSecretValue");
  });

  test("SQS gets application/x-amz-json-1.0", async () => {
    const r = recorder();
    await awsJsonCall({ service: "sqs", target: "AmazonSQS.SendMessage", region: "us-east-1", body: { QueueUrl: "q", MessageBody: "{}" }, credentials, fetcher: r.fetcher });
    expect(r.seen[0].url).toBe("https://sqs.us-east-1.amazonaws.com/");
    expect(r.seen[0].headers["content-type"]).toBe("application/x-amz-json-1.0");
  });

  test("the content type is part of the SigV4 signature", async () => {
    const r = recorder();
    await awsJsonCall({ service: "secretsmanager", target: "secretsmanager.GetSecretValue", region: "us-east-1", body: {}, credentials, fetcher: r.fetcher });
    const signed = /SignedHeaders=([^,]+)/.exec(r.seen[0].headers["authorization"] ?? "")?.[1]?.split(";") ?? [];
    expect(signed).toContain("content-type");
  });
});
