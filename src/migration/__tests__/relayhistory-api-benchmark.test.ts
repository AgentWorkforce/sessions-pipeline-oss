import { afterEach, describe, expect, test } from "bun:test";
import {
  evaluateHttpGate,
  httpLatencyStats,
  responseFingerprint,
  runHttpBenchmark,
  type HttpBenchmarkConfig,
} from "../relayhistory-api-benchmark.ts";

const servers: Array<ReturnType<typeof Bun.serve>> = [];

afterEach(() => {
  while (servers.length) servers.pop()!.stop(true);
});

function apiServer(label: string, mismatch = false) {
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      expect(request.headers.get("authorization")).toBe("Bearer secret-benchmark-token");
      if (url.pathname === "/v1/sessions") {
        return Response.json({ sessions: [{ id: "session-a", count: 3 }], correlationId: `${label}-dynamic` });
      }
      if (url.pathname === "/v1/events") {
        return Response.json({
          events: [{ id: "event-a", kind: mismatch ? "different" : "message" }],
          correlationId: crypto.randomUUID(),
        });
      }
      if (url.pathname.includes("/events")) {
        return Response.json({ events: [{ id: "event-a" }], requestId: crypto.randomUUID() });
      }
      return Response.json({ error: "not found" }, { status: 404 });
    },
  });
  servers.push(server);
  return `http://127.0.0.1:${server.port}`;
}

function config(legacyUrl: string, compatibilityUrl: string): HttpBenchmarkConfig {
  return {
    legacyUrl,
    compatibilityUrl,
    legacyToken: "secret-benchmark-token",
    compatibilityToken: "secret-benchmark-token",
    query: "private search phrase",
    sessionId: "private-session-id",
    workspace: "private-workspace-id",
    runs: 4,
    warmups: 1,
    concurrency: 2,
    timeoutMs: 2_000,
  };
}

describe("relayhistory API benchmark", () => {
  test("calculates nearest-rank latency percentiles", () => {
    expect(httpLatencyStats([10, 2, 4, 8, 6])).toEqual({ p50Ms: 6, p95Ms: 10, p99Ms: 10 });
  });

  test("ignores volatile ids and canonicalizes JSON object keys", () => {
    expect(responseFingerprint('{"value":1,"correlationId":"one"}')).toBe(
      responseFingerprint('{"requestId":"two","value":1}'),
    );
  });

  test("reports parity and aggregate HTTP performance without sensitive content", async () => {
    const report = await runHttpBenchmark(config(apiServer("legacy"), apiServer("compatibility")));
    const output = JSON.stringify(report);

    expect(report.configuration.cases).toEqual(["recent_sessions", "event_text_search", "session_replay"]);
    for (const value of Object.values(report.results) as Array<any>) {
      expect(value.parity).toEqual({ status: true, response: true });
      expect(value.legacy.statuses).toEqual({ "200": 4 });
      expect(value.compatibility.statuses).toEqual({ "200": 4 });
      expect(value.legacy.latency.p99Ms).toBeGreaterThanOrEqual(0);
      expect(value.compatibility.throughputQps).toBeGreaterThan(0);
    }
    for (const secret of [
      "secret-benchmark-token",
      "private search phrase",
      "private-session-id",
      "private-workspace-id",
      "127.0.0.1",
    ]) {
      expect(output).not.toContain(secret);
    }
  });

  test("detects a response mismatch independently from status parity", async () => {
    const report = await runHttpBenchmark(config(apiServer("legacy"), apiServer("compatibility", true)));
    const result = report.results.event_text_search as any;
    expect(result.parity).toEqual({ status: true, response: false });
  });
});

describe("HTTP benchmark gate", () => {
  const side = (p95Ms: number, statuses: Record<string, number> = { "200": 10 }, responseStable = true) => ({
    latency: { minMs: 1, p50Ms: 1, p95Ms, p99Ms: p95Ms, maxMs: p95Ms, meanMs: 1 },
    statuses,
    responseStable,
  });
  const ok = { status: true, response: true };

  test("passes 2xx-only, matching, stable, no-slower results", () => {
    expect(
      evaluateHttpGate({ sessions: { legacy: side(50), compatibility: side(40), parity: ok } }, 1),
    ).toEqual({ passed: true, failures: [] });
  });

  test("fails on non-2xx, mismatch, instability or a p95 regression", () => {
    const gate = evaluateHttpGate(
      {
        errors: { legacy: side(50), compatibility: side(40, { "200": 9, "503": 1 }), parity: { status: false, response: true } },
        drift: { legacy: side(50, { "200": 10 }, false), compatibility: side(40), parity: { status: true, response: false } },
        slower: { legacy: side(50), compatibility: side(51), parity: ok },
        both404: { legacy: side(50, { "404": 10 }), compatibility: side(40, { "404": 10 }), parity: ok },
      },
      1,
    );
    expect(gate.passed).toBe(false);
    expect(gate.failures).toEqual([
      "errors: compatibility returned 503",
      "errors: status mix differs",
      "drift: legacy responses changed between runs",
      "drift: responses differ",
      "slower: compatibility p95 51 ms > 1 x legacy 50 ms",
      "both404: legacy returned 404",
      "both404: compatibility returned 404",
    ]);
  });
});
