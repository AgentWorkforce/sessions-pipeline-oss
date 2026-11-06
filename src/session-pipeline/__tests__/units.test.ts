import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { signV4 } from "../aws-json.ts";
import { decodeS3Key, parseQueueBody, sequencerAfter } from "../s3-events.ts";
import { SUPABASE_PROD_CA_2021 } from "../supabase-ca.ts";
import { analysisDeps, analysisSourceReadonlyUrl, handler, lambdaEnv, parseSecret, runtimeFromSecret } from "../lambda.ts";
import { searchIndexLimits, SEARCH_INDEX_DEFAULT_LIMITS, REPLAY_SEARCH_INDEX_LIMITS } from "../search-index.ts";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));

describe("SigV4 for JSON-protocol services", () => {
  test("matches the AWS documentation example (IAM ListUsers, 2015-08-30)", () => {
    const headers = signV4({
      method: "GET",
      url: new URL("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08"),
      service: "iam",
      region: "us-east-1",
      headers: { "content-type": "application/x-www-form-urlencoded; charset=utf-8" },
      body: "",
      credentials: { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" },
      now: new Date("2015-08-30T12:36:00Z"),
    });
    expect(headers.authorization).toBe(
      "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
    );
    expect(headers.host).toBeUndefined();
  });

  test("signs the session token", () => {
    const headers = signV4({
      method: "POST",
      url: new URL("https://sqs.us-east-1.amazonaws.com/"),
      service: "sqs",
      region: "us-east-1",
      headers: {},
      body: "{}",
      credentials: { accessKeyId: "AKID", secretAccessKey: "secret", sessionToken: "token" },
      now: new Date("2026-11-01T00:00:00Z"),
    });
    expect(headers["x-amz-security-token"]).toBe("token");
    expect(headers.authorization).toContain("SignedHeaders=host;x-amz-date;x-amz-security-token");
  });
});

const KEY = "96bulls/org_1/v1/ws=ws_1/m=srv_ab/h=claude/d=2026/11/01/s=sess 1/manifest.json";
const s3Body = (eventName: string, object: Record<string, unknown>) =>
  JSON.stringify({
    Records: [
      {
        eventVersion: "2.1",
        eventSource: "aws:s3",
        eventName,
        eventTime: "2026-11-01T18:00:00.000Z",
        s3: { bucket: { name: "bucket" }, object },
      },
    ],
  });

describe("S3 event notifications", () => {
  test("a put carries key, ETag, version and sequencer", () => {
    const parsed = parseQueueBody(
      s3Body("ObjectCreated:Put", {
        key: "96bulls/org_1/v1/ws%3Dws_1/m%3Dsrv_ab/h%3Dclaude/d%3D2026/11/01/s%3Dsess+1/manifest.json",
        size: 10,
        eTag: "abc",
        versionId: "v1",
        sequencer: "0055AED6DCD90281E5",
      }),
    );
    expect(parsed).toEqual({
      kind: "events",
      events: [
        {
          provider: "s3",
          key: KEY,
          bucket: "bucket",
          etag: "abc",
          version: "v1",
          size: 10,
          eventTime: "2026-11-01T18:00:00.000Z",
          sequencer: "0055AED6DCD90281E5",
        },
      ],
    });
  });

  test("a version delete and a delete marker are deletes without an identity", () => {
    for (const name of ["ObjectRemoved:Delete", "ObjectRemoved:DeleteMarkerCreated"]) {
      const parsed = parseQueueBody(s3Body(name, { key: "a", versionId: "v9", sequencer: "01" }));
      expect(parsed).toEqual({
        kind: "events",
        events: [{ provider: "s3", key: "a", bucket: "bucket", eventTime: "2026-11-01T18:00:00.000Z", sequencer: "01", deleted: true }],
      });
    }
  });

  test("restores, replication and other sources are ignored; the test event is recognized", () => {
    expect(parseQueueBody(s3Body("ObjectRestore:Completed", { key: "a" }))).toEqual({ kind: "events", events: [] });
    expect(parseQueueBody(JSON.stringify({ Records: [{ eventSource: "aws:sqs", eventName: "ObjectCreated:Put", s3: { object: { key: "a" } } }] }))).toEqual({
      kind: "events",
      events: [],
    });
    expect(parseQueueBody(JSON.stringify({ Service: "Amazon S3", Event: "s3:TestEvent", Bucket: "b" }))).toEqual({ kind: "test" });
    expect(parseQueueBody("not json")).toEqual({ kind: "invalid" });
    expect(parseQueueBody(JSON.stringify({ reconcile: {} }))).toEqual({ kind: "invalid" });
  });

  test("reconcile bodies become puts", () => {
    expect(parseQueueBody(JSON.stringify({ reconcile: { key: KEY, etag: "e" } }))).toEqual({
      kind: "events",
      events: [{ provider: "s3", key: KEY, etag: "e" }],
    });
  });

  test("key decoding and sequencer order", () => {
    expect(decodeS3Key("a+b%2Bc")).toBe("a b+c");
    expect(decodeS3Key("%E0%A4%A")).toBeNull();
    expect(sequencerAfter("0055AED6DCD90281E6", "0055AED6DCD90281E5")).toBe(true);
    expect(sequencerAfter("0055AED6DCD90281E5", "0055AED6DCD90281E5FF")).toBe(false);
    expect(sequencerAfter("01", undefined)).toBe(true);
    expect(sequencerAfter(undefined, "01")).toBe(false);
  });
});

describe("pinned inputs", () => {
  test("the inlined Supabase CA is the pinned certificate", () => {
    expect(SUPABASE_PROD_CA_2021).toBe(readFileSync(here("../../../certs/supabase-prod-ca-2021.crt"), "utf8"));
  });

  test("the vendored ClickHouse helpers are unmodified copies", () => {
    const source = JSON.parse(readFileSync(here("../vendor/search-index/SOURCE.json"), "utf8")) as { files: Record<string, string> };
    expect(Object.keys(source.files).sort()).toEqual(["clickhouse.ts", "reconcile.ts", "rows.ts", "schema.ts"]);
    for (const [file, digest] of Object.entries(source.files))
      expect(createHash("sha256").update(readFileSync(here(`../vendor/search-index/${file}`))).digest("hex")).toBe(digest);
  });
});

describe("configuration", () => {
  const base = {
    AWS_REGION: "us-east-1",
    PIPELINE_RAW_S3_BUCKET: "b",
    PIPELINE_KEY_PREFIX: "/96bulls/",
    PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "arn",
    SESSION_PIPELINE_QUEUE_URL: "https://sqs",
  };

  test("search index limits stay 64 MiB / 100k / 500k unless the replay stage is on", () => {
    const original = { maxScopeBytes: 64 * 1024 * 1024, maxScopeEvents: 100_000, maxRows: 500_000, insertBatch: 5_000 };
    expect({ ...SEARCH_INDEX_DEFAULT_LIMITS }).toEqual(original);
    expect(searchIndexLimits(lambdaEnv(base).replayPagesEnabled)).toEqual(original);
    expect(searchIndexLimits(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" }).replayPagesEnabled)).toEqual(original);
    expect(searchIndexLimits(lambdaEnv({ ...base, SESSION_REPLAY_PAGES_ENABLED: "true" }).replayPagesEnabled)).toEqual(original);
    const raised = { maxScopeBytes: 256 * 1024 * 1024, maxScopeEvents: 1_000_000, maxRows: 1_000_000, insertBatch: 5_000 };
    expect(searchIndexLimits(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SESSION_REPLAY_PAGES_ENABLED: "1" }).replayPagesEnabled)).toEqual(raised);
    expect({ ...REPLAY_SEARCH_INDEX_LIMITS }).toEqual(raised);
    // The lambda wires the search stage through this switch, never a raised constant.
    const lambdaSource = readFileSync(here("../lambda.ts"), "utf8");
    expect(lambdaSource).toContain("limits: searchIndexLimits(env.replayPagesEnabled),");
    expect(lambdaSource).not.toContain("REPLAY_SEARCH_INDEX_LIMITS");
  });

  test("defaults keep the search index and fan-out off", () => {
    expect(lambdaEnv(base)).toMatchObject({ keyPrefix: "96bulls", searchEnabled: false, replayPagesEnabled: false, fanout: "off" });
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SESSION_REPLAY_PAGES_ENABLED: "1", PIPELINE_FANOUT: "postgres" })).toMatchObject({ searchEnabled: true, replayPagesEnabled: true, fanout: "postgres" });
  });

  test("large-scope handoff stays off unless SESSION_LARGE_READ_HANDOFF_ENABLED is exactly 1", () => {
    expect(lambdaEnv(base).largeScopeHandoffEnabled).toBe(false);
    for (const v of ["0", "", "true", "yes", " 1"])
      expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SESSION_LARGE_READ_HANDOFF_ENABLED: v }).largeScopeHandoffEnabled).toBe(false);
    expect(lambdaEnv({ ...base, SESSION_LARGE_READ_HANDOFF_ENABLED: "1" }).largeScopeHandoffEnabled).toBe(true);
    // deps.large (the only routing call) is wired behind that switch alone: flag off, no route.
    const lambdaSource = readFileSync(here("../lambda.ts"), "utf8");
    expect(lambdaSource).toContain("if(env.largeScopeHandoffEnabled&&(env.searchEnabled||env.replayPagesEnabled))deps.large=");
    expect(lambdaSource.match(/deps\.large\s*=/g)).toHaveLength(1);
  });

  test("desktop catalog: only the literal \"1\" enables it; SEARCH_INDEX_ENABLED=1 alone never does", () => {
    expect(lambdaEnv(base).desktopCatalogEnabled).toBe(false);
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" })).toMatchObject({ searchEnabled: true, desktopCatalogEnabled: false });
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SESSION_DESKTOP_CATALOG_ENABLED: "1" })).toMatchObject({ searchEnabled: true, desktopCatalogEnabled: true });
    for (const value of ["", "0", "true", "TRUE", "yes", "on", " 1", "1 ", "01", "2"])
      expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SESSION_DESKTOP_CATALOG_ENABLED: value }).desktopCatalogEnabled).toBe(false);
    // The search deps carry the switch only when it is on, so the off path's deps are unchanged.
    const lambdaSource = readFileSync(here("../lambda.ts"), "utf8");
    expect(lambdaSource).toContain("...(env.desktopCatalogEnabled ? { desktopCatalog: true as const } : {}),");
  });

  test("refuses the single-tenant layout and unknown fan-out", () => {
    expect(() => lambdaEnv({ ...base, PIPELINE_ORG_FROM_PREFIX: undefined })).toThrow(/ORG_FROM_PREFIX/);
    expect(() => lambdaEnv({ ...base, PIPELINE_FANOUT: "sqs" })).toThrow(/FANOUT/);
  });

  test("adapter selection: unset is today's lambda + clickhouse; other runtimes or backends are refused", () => {
    expect(() => lambdaEnv(base)).not.toThrow();
    expect(() => lambdaEnv({ ...base, PIPELINE_RUNTIME: "lambda", SEARCH_INDEX_BACKEND: "clickhouse" })).not.toThrow();
    expect(() => lambdaEnv({ ...base, PIPELINE_RUNTIME: "workers" })).toThrow(/PIPELINE_RUNTIME/);
    expect(() => lambdaEnv({ ...base, SEARCH_INDEX_BACKEND: "opensearch" })).toThrow(/SEARCH_INDEX_BACKEND/);
    expect(() => lambdaEnv({ ...base, SEARCH_INDEX_BACKEND: "nope" })).toThrow(/unknown/);
  });

  test("secret shape is checked without echoing values", () => {
    expect(parseSecret(JSON.stringify({ hoopdb_url: "postgresql://u:p@h:5432/db" })).hoopdb_url).toContain("postgresql://");
    let message = "";
    try {
      parseSecret(JSON.stringify({ hoopdb_url: "mysql://hunter2" }));
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).toBe("session pipeline secret has no hoopdb_url");
    expect(() => parseSecret(JSON.stringify({ hoopdb_url: "postgres://x", clickhouse: { url: "https://c", user: "u", password: "", database: "d" } }))).toThrow(
      /clickhouse.password/,
    );
  });

  test("openai_api_key is optional; when present it must be a non-empty string, and no error echoes it", () => {
    expect(parseSecret(JSON.stringify({ hoopdb_url: "postgres://x" })).openai_api_key).toBeUndefined();
    expect(parseSecret(JSON.stringify({ hoopdb_url: "postgres://x", openai_api_key: "sk-test-1" })).openai_api_key).toBe("sk-test-1");
    for (const bad of ["", "   ", 42, null]) {
      let message = "";
      try {
        parseSecret(JSON.stringify({ hoopdb_url: "postgres://x", openai_api_key: bad }));
      } catch (error) {
        message = (error as Error).message;
      }
      expect(message).toBe("session pipeline secret openai_api_key must be a non-empty string when present");
    }
  });
});

describe("brief schedule invocation", () => {
  test("with SESSION_BRIEF_ENABLED unset or not exactly true it returns before init: no env, secret or pool is needed", async () => {
    const saved = process.env.SESSION_BRIEF_ENABLED;
    try {
      for (const value of [undefined, "false", "TRUE", "1"]) {
        if (value === undefined) delete process.env.SESSION_BRIEF_ENABLED;
        else process.env.SESSION_BRIEF_ENABLED = value;
        // lambdaEnv() would throw here (no AWS_REGION, bucket or secret ARN): reaching init fails the test.
        expect(await handler({ brief: true })).toEqual({ ok: true });
      }
    } finally {
      if (saved === undefined) delete process.env.SESSION_BRIEF_ENABLED;
      else process.env.SESSION_BRIEF_ENABLED = saved;
    }
  });

  test("enabled, a failed init is logged and answered ok:false, never thrown", async () => {
    const saved = { e: process.env.SESSION_BRIEF_ENABLED, r: process.env.AWS_REGION };
    try {
      process.env.SESSION_BRIEF_ENABLED = "true";
      delete process.env.AWS_REGION;
      expect(await handler({ brief: true })).toEqual({ ok: false });
    } finally {
      if (saved.e === undefined) delete process.env.SESSION_BRIEF_ENABLED;
      else process.env.SESSION_BRIEF_ENABLED = saved.e;
      if (saved.r !== undefined) process.env.AWS_REGION = saved.r;
    }
  });
});

test("analysis cache defaults off before runtime init and rejects invalid readonly source secret shape",async()=>{const saved=process.env.SESSION_ANALYSIS_CACHE_ENABLED;try{for(const value of [undefined,"0","true"]){if(value===undefined)delete process.env.SESSION_ANALYSIS_CACHE_ENABLED;else process.env.SESSION_ANALYSIS_CACHE_ENABLED=value;expect(await handler({analysisCache:true})).toEqual({ok:true});expect(await handler({analysisCacheReconcile:true})).toEqual({ok:true})}expect(()=>parseSecret(JSON.stringify({hoopdb_url:"postgres://fixture",analysis_source_readonly_url:"mysql://fixture"}))).not.toThrow();expect(()=>analysisSourceReadonlyUrl("mysql://fixture")).toThrow(/readonly_url/)}finally{if(saved===undefined)delete process.env.SESSION_ANALYSIS_CACHE_ENABLED;else process.env.SESSION_ANALYSIS_CACHE_ENABLED=saved}});

describe("analysis source secret field is validated only in the analysis branch", () => {
  const base = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs", SEARCH_INDEX_ENABLED: "1" };
  const secret = (field: Record<string, unknown>) => JSON.stringify({ hoopdb_url: "postgres://u:p@127.0.0.1:1/db", clickhouse: { url: "https://ch.example", user: "u", password: "p", database: "d" }, ...field });
  test("flag off + malformed field: search init succeeds and analysis stays unavailable", () => {
    for (const field of [{ analysis_source_readonly_url: "mysql://hunter2" }, { analysis_source_readonly_url: 42 }, { analysis_source_readonly_url: "" }]) {
      const rt = runtimeFromSecret(lambdaEnv(base), secret(field), "fixture-generation");
      expect(rt.deps.search).toBeDefined();
      expect(rt.analysis).toBeUndefined();
      expect(() => analysisDeps(rt)).toThrow("analysis cache runtime unavailable");
    }
  });
  test("flag on + missing or malformed field: only the analysis branch fails, search unaffected, no value echoed", () => {
    const env = lambdaEnv({ ...base, SESSION_ANALYSIS_CACHE_ENABLED: "1" });
    expect(env.analysisCacheEnabled).toBe(true);
    for (const [field, error] of [[{}, /readonly source configuration.*missing/], [{ analysis_source_readonly_url: "mysql://hunter2" }, /must be a postgres URL/]] as const) {
      const rt = runtimeFromSecret(env, secret(field), "fixture-generation");
      expect(rt.deps.search).toBeDefined();
      let message = "";
      try { analysisDeps(rt); } catch (e) { message = (e as Error).message; }
      expect(message).toMatch(error);
      expect(message).not.toContain("hunter2");
      expect(rt.analysis).toBeUndefined();
      expect(rt.deps.search).toBeDefined();
    }
    const ok = runtimeFromSecret(env, secret({ analysis_source_readonly_url: "postgres://ro:p@127.0.0.1:1/db" }), "fixture-generation");
    const deps = analysisDeps(ok);
    expect(analysisDeps(ok)).toBe(deps);
    expect(deps.db).toBe(ok.deps.pointer.db);
  });
});
