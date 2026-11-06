import { describe, expect, test } from "bun:test";
import { EVENT_TEXT_SEARCH_SQL, latencyStats, evaluateGate } from "../relayhistory-benchmark.ts";

describe("relayhistory benchmark stats", () => {
  test("uses nearest-rank percentiles and rounds output", () => {
    expect(latencyStats([10, 2, 4, 8, 6])).toEqual({
      minMs: 2,
      p50Ms: 6,
      p95Ms: 10,
      p99Ms: 10,
      maxMs: 10,
      meanMs: 6,
    });
  });

  test("benchmarks the production bounded content and task-title search predicate", () => {
    expect(EVENT_TEXT_SEARCH_SQL).toContain("left(content, 4096) ILIKE");
    expect(EVENT_TEXT_SEARCH_SQL).toContain("task_title ILIKE");
    expect(EVENT_TEXT_SEARCH_SQL).not.toMatch(/\bcontent ILIKE/);
  });
});

describe("database benchmark gate", () => {
  const side = (p95Ms: number, nodes: Array<Record<string, unknown>>, overrides = {}) => ({
    latency: { minMs: 1, p50Ms: 1, p95Ms, p99Ms: p95Ms, maxMs: p95Ms, meanMs: 1 },
    resultHash: "h",
    stable: true,
    plan: { nodes },
    ...overrides,
  });
  const indexScan = [{ node: "Index Scan", relation: "convergence_events", index: "x" }];

  test("passes identical, stable, no-slower results on the same access path", () => {
    expect(
      evaluateGate({ search: { source: side(10, indexScan), target: side(9, indexScan) } }, 1),
    ).toEqual({ passed: true, failures: [] });
  });

  test("fails on a result mismatch, instability, a p95 regression or a lost index", () => {
    const gate = evaluateGate(
      {
        mismatch: { source: side(10, []), target: side(10, [], { resultHash: "other" }) },
        unstable: { source: side(10, [], { stable: false }), target: side(10, []) },
        slower: { source: side(10, []), target: side(10.5, []) },
        seqscan: {
          source: side(10, indexScan),
          target: side(5, [{ node: "Seq Scan", relation: "convergence_events" }]),
        },
      },
      1,
    );
    expect(gate.passed).toBe(false);
    expect(gate.failures).toEqual([
      "mismatch: results differ",
      "unstable: results changed between runs",
      "slower: target p95 10.5 ms > 1 x source 10 ms",
      "seqscan: target seq-scans convergence_events, which the source reads by index",
    ]);
  });
});
