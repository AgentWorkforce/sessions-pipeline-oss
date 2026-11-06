import { describe, expect, test } from "bun:test";
import { DEFAULT_MODEL_BASE_URL, modelFetch, modelResponsesUrl } from "../config.ts";
import { createAnalysisTransport } from "../engine/transport.ts";

describe("OPENAI_BASE_URL", () => {
  test("default is the hosted Responses API and leaves the engine fetch untouched", () => {
    expect(modelResponsesUrl({})).toBe(`${DEFAULT_MODEL_BASE_URL}/responses`);
    expect(modelFetch(modelResponsesUrl({}))).toBeUndefined();
  });

  test("an https or in-cluster gateway is accepted; trailing slashes are folded", () => {
    expect(modelResponsesUrl({ OPENAI_BASE_URL: "https://llm.example.com/v1/" })).toBe(
      "https://llm.example.com/v1/responses",
    );
    expect(
      modelResponsesUrl({
        OPENAI_BASE_URL: "http://llm-gateway.ai.svc:4000/v1",
        ALLOW_IN_CLUSTER_HTTP: "true",
      }),
    ).toBe("http://llm-gateway.ai.svc:4000/v1/responses");
  });

  test("the key never goes over public plain http or to a credentialed URL", () => {
    expect(() =>
      modelResponsesUrl({ OPENAI_BASE_URL: "http://llm-gateway.ai.svc:4000/v1" }),
    ).toThrow(/OPENAI_BASE_URL must use https/);
    expect(() =>
      modelResponsesUrl({ OPENAI_BASE_URL: "http://llm.example.com/v1", ALLOW_IN_CLUSTER_HTTP: "true" }),
    ).toThrow(/OPENAI_BASE_URL must use https/);
    expect(() =>
      modelResponsesUrl({ OPENAI_BASE_URL: "https://u:p@llm.example.com/v1" }),
    ).toThrow(/must not contain credentials/);
  });

  test("the engine's request is sent to the configured endpoint", async () => {
    const seen: string[] = [];
    const fetchModel = modelFetch("https://llm.example.com/v1/responses", (async (input: string | URL | Request) => {
      seen.push(String(input));
      return new Response("{}", { status: 500 });
    }) as typeof fetch)!;
    const transport = createAnalysisTransport({ apiKey: () => "sk-test-only", fetch: fetchModel });
    await expect(
      transport.complete({
        model: "m",
        instructions: "i",
        input: "x",
        schemaName: "s",
        schema: {},
        timeoutMs: 1_000,
      } as never),
    ).rejects.toBeDefined();
    expect(seen).toEqual(["https://llm.example.com/v1/responses"]);
    await expect(fetchModel("https://elsewhere.example/x")).rejects.toThrow(/unexpected model request URL/);
  });
});
