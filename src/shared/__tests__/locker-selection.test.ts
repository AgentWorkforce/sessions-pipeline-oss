import { describe, expect, test } from "bun:test";
import { vendorBindings } from "../env.ts";
import { buildLockerStore, gcsMetadataToken } from "../locker.ts";
import {
  assertLockerSelection,
  lockerProvider,
  trustedGcsEndpoint,
  trustedGcsTokenUri,
} from "../locker-selection.ts";

describe("locker provider selection", () => {
  test("defaults to S3 when the provider is unset, blank, or explicit", () => {
    const s3Env = {
      LOCKER_BUCKET: "locker",
      LOCKER_ACCESS_KEY_ID: "a",
      LOCKER_SECRET_ACCESS_KEY: "b",
      LOCKER_ENDPOINT: "http://127.0.0.1:9000",
    };
    for (const provider of [undefined, "", "  ", "s3"]) {
      const raw = buildLockerStore({ ...s3Env, ...(provider === undefined ? {} : { LOCKER_PROVIDER: provider }) });
      expect(raw.provider).toBe("s3");
      expect(raw.bucket).toBe("locker");
      expect(raw.store.backend).toBe("s3");
    }
    expect(lockerProvider({})).toBe("s3");
    expect(() =>
      buildLockerStore({
        LOCKER_BUCKET: "locker",
        LOCKER_ENDPOINT: "http://127.0.0.1:9000",
      }),
    ).toThrow("no Locker credentials");
  });

  test("selects the GCS adapter only when asked", () => {
    const raw = buildLockerStore({
      LOCKER_PROVIDER: "gcs",
      LOCKER_BUCKET: "locker-proof",
      LOCKER_ENDPOINT: "http://127.0.0.1:4443",
    });
    expect(raw.provider).toBe("gcs");
    expect(raw.bucket).toBe("locker-proof");
    expect(raw.store.backend).toBe("gcs");
  });

  test("refuses an unknown provider without echoing the value", () => {
    expect(() => lockerProvider({ LOCKER_PROVIDER: "azure" })).toThrow("must be s3 or gcs");
  });

  test("refuses to point the S3 client at the GCS API", () => {
    expect(() =>
      assertLockerSelection({
        LOCKER_PROVIDER: "s3",
        LOCKER_ENDPOINT: "https://storage.googleapis.com",
        LOCKER_ACCESS_KEY_ID: "hmac",
        LOCKER_SECRET_ACCESS_KEY: "secret",
      }),
    ).toThrow(/HMAC-as-S3/);
  });

  test("refuses HMAC keys on the GCS adapter", () => {
    expect(() =>
      assertLockerSelection({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_ACCESS_KEY_ID: "hmac",
        LOCKER_SECRET_ACCESS_KEY: "secret",
      }),
    ).toThrow(/not HMAC keys/);
  });

  test("GCS fails closed when the bucket, endpoint, or token configuration is incomplete", () => {
    expect(() => buildLockerStore({ LOCKER_PROVIDER: "gcs" })).toThrow("requires LOCKER_BUCKET");
    expect(() => buildLockerStore({ LOCKER_PROVIDER: "gcs", LOCKER_BUCKET: "   " })).toThrow("requires LOCKER_BUCKET");
    expect(() =>
      buildLockerStore({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_ACCESS_KEY_ID: "hmac",
      }),
    ).toThrow(/not HMAC keys/);
    expect(() =>
      buildLockerStore({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_ENDPOINT: "not a url",
      }),
    ).toThrow("official HTTPS API or loopback emulator");
    expect(() =>
      buildLockerStore({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_ENDPOINT: "https://user:password@storage.googleapis.com",
      }),
    ).toThrow("official HTTPS API or loopback emulator");
    expect(() =>
      buildLockerStore({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_ENDPOINT: "https://attacker.example/gcs",
      }),
    ).toThrow("official HTTPS API or loopback emulator");
    expect(() =>
      buildLockerStore({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_GCS_TOKEN_URI: "http://evil.example/token",
      }),
    ).toThrow("not the GKE metadata server or loopback");
  });

  test("accepts the metadata server and loopback, and refuses other token hosts", () => {
    expect(trustedGcsTokenUri("http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token")).toBe(true);
    expect(trustedGcsTokenUri("http://127.0.0.1:9/token")).toBe(true);
    expect(trustedGcsTokenUri("http://user:password@127.0.0.1:9/token")).toBe(false);
    expect(trustedGcsTokenUri("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")).toBe(false);
    expect(trustedGcsTokenUri("https://storage.googleapis.com/token")).toBe(false);
    expect(() =>
      assertLockerSelection({
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        LOCKER_GCS_TOKEN_URI: "http://evil.example/token",
      }),
    ).toThrow("not the GKE metadata server or loopback");
  });

  test("sends GCS bearer tokens only to the official API or loopback emulators", () => {
    expect(trustedGcsEndpoint("https://storage.googleapis.com")).toBe(true);
    expect(trustedGcsEndpoint("http://127.0.0.1:4443/storage/v1")).toBe(true);
    expect(trustedGcsEndpoint("http://localhost:4443/storage/v1")).toBe(true);
    expect(trustedGcsEndpoint("https://attacker.example/storage/v1")).toBe(false);
    expect(trustedGcsEndpoint("http://storage.googleapis.com")).toBe(false);
    expect(trustedGcsEndpoint("https://storage.googleapis.com.attacker.example")).toBe(false);
    expect(trustedGcsEndpoint("https://storage.googleapis.com:4443")).toBe(false);
    expect(trustedGcsEndpoint("https://storage.googleapis.com/?redirect=attacker")).toBe(false);
  });

  test("reads a metadata token and does not put it in the error path", async () => {
    let flavor: string | null = null;
    const fake = (async (_u: string | URL | Request, init?: RequestInit) => {
      flavor = new Headers(init?.headers).get("Metadata-Flavor");
      return new Response(JSON.stringify({ access_token: "ya29.test", expires_in: 3600, token_type: "Bearer" }));
    }) as unknown as typeof fetch;
    const token = await gcsMetadataToken({ LOCKER_GCS_TOKEN_URI: "http://127.0.0.1:9/token" }, fake);
    expect(flavor).toBe("Google");
    expect(token.accessToken).toBe("ya29.test");
    expect(token.expiresAt).toBeInstanceOf(Date);
    await expect(gcsMetadataToken({ LOCKER_GCS_TOKEN_URI: "http://evil.example/token" }, fake)).rejects.toThrow("not the GKE metadata server");
    const denied = (async () => new Response("no", { status: 403 })) as unknown as typeof fetch;
    await expect(gcsMetadataToken({ LOCKER_GCS_TOKEN_URI: "http://127.0.0.1:9/token" }, denied)).rejects.toThrow("http 403");
    const empty = (async () => new Response(JSON.stringify({ token_type: "Bearer" }))) as unknown as typeof fetch;
    await expect(gcsMetadataToken({ LOCKER_GCS_TOKEN_URI: "http://127.0.0.1:9/token" }, empty)).rejects.toThrow("no access_token");
  });
});

describe("vendor bindings follow the selected provider", () => {
  const keys = ["LOCKER_PROVIDER", "LOCKER_BUCKET", "LOCKER_ENDPOINT", "LOCKER_REGION", "LOCKER_ACCESS_KEY_ID", "LOCKER_SECRET_ACCESS_KEY", "TENANT_ORG_ID"] as const;

  function withEnv(values: Record<string, string | undefined>, run: () => void) {
    const previous = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
    for (const key of keys) {
      const value = values[key];
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    try {
      run();
    } finally {
      for (const key of keys) {
        const value = previous[key];
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }

  test("keeps S3 bindings, including keys, when the provider is unset", () => {
    withEnv(
      {
        LOCKER_BUCKET: "locker",
        LOCKER_ENDPOINT: "http://127.0.0.1:9000",
        LOCKER_ACCESS_KEY_ID: "a",
        LOCKER_SECRET_ACCESS_KEY: "b",
        TENANT_ORG_ID: "org",
      },
      () => {
        const bindings = vendorBindings();
        expect(bindings.PIPELINE_RAW_PROVIDER).toBe("s3");
        expect(bindings.PIPELINE_RAW_S3_ENDPOINT).toBe("http://127.0.0.1:9000");
        expect(bindings.PIPELINE_RAW_S3_REGION).toBe("us-east-1");
        expect(bindings.PIPELINE_RAW_S3_ACCESS_KEY_ID).toBe("a");
        expect(bindings.PIPELINE_RAW_S3_SECRET_ACCESS_KEY).toBe("b");
      },
    );
  });

  test("omits S3 HMAC keys when the locker is GCS", () => {
    withEnv(
      {
        LOCKER_PROVIDER: "gcs",
        LOCKER_BUCKET: "locker-proof",
        TENANT_ORG_ID: "org",
      },
      () => {
        const bindings = vendorBindings();
        expect(bindings.PIPELINE_RAW_PROVIDER).toBe("gcs");
        expect(bindings.PIPELINE_RAW_S3_BUCKET).toBe("locker-proof");
        expect(bindings.PIPELINE_RAW_S3_ACCESS_KEY_ID).toBeUndefined();
        expect(bindings.PIPELINE_RAW_S3_SECRET_ACCESS_KEY).toBeUndefined();
        expect(JSON.stringify(bindings)).not.toMatch(/hmac|secret|ya29/i);
      },
    );
  });
});
