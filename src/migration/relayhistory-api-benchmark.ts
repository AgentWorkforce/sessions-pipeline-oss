/**
 * Content-safe HTTP benchmark for the legacy RelayHistory API and its
 * self-hosted compatibility API. Output contains aggregate timings, status
 * counts and parity booleans only; URLs, bearer tokens, queries, session ids
 * and response bodies are never printed.
 *
 * Required env: RELAYHISTORY_LEGACY_API_URL, RELAYHISTORY_COMPAT_API_URL,
 * RELAYHISTORY_API_TOKEN, BENCH_HTTP_QUERY. Optional:
 * RELAYHISTORY_COMPAT_API_TOKEN, BENCH_HTTP_SESSION_ID, BENCH_HTTP_WORKSPACE,
 * BENCH_HTTP_RUNS, BENCH_HTTP_WARMUPS, BENCH_HTTP_CONCURRENCY,
 * BENCH_HTTP_TIMEOUT_MS.
 */
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";

export interface HttpLatencyStats {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

interface BenchmarkCase {
  name: string;
  path: string;
}

export interface HttpBenchmarkConfig {
  legacyUrl: string;
  compatibilityUrl: string;
  legacyToken: string;
  compatibilityToken: string;
  query: string;
  sessionId?: string;
  workspace?: string;
  runs: number;
  warmups: number;
  concurrency: number;
  timeoutMs: number;
}

type Fetch = typeof globalThis.fetch;

interface EndpointResult {
  latency: HttpLatencyStats;
  throughputQps: number;
  statuses: Record<string, number>;
  responseStable: boolean;
  representativeStatus: number;
  representativeHash: string;
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}

export function httpLatencyStats(values: number[]): HttpLatencyStats {
  if (values.length === 0) throw new Error("benchmark needs at least one timing");
  const sorted = [...values].sort((a, b) => a - b);
  const at = (percentile: number) =>
    sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * percentile) - 1))]!;
  return {
    p50Ms: round(at(0.5)),
    p95Ms: round(at(0.95)),
    p99Ms: round(at(0.99)),
  };
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([key]) => key !== "correlationId" && key !== "requestId")
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, child]) => [key, canonicalize(child)]),
  );
}

export function responseFingerprint(body: string): string {
  let normalized = body;
  try {
    normalized = JSON.stringify(canonicalize(JSON.parse(body)));
  } catch {
    // Non-JSON responses are still comparable without exposing their content.
  }
  return createHash("sha256").update(normalized).digest("hex");
}

function benchmarkCases(config: HttpBenchmarkConfig): BenchmarkCase[] {
  const common = new URLSearchParams({ limit: "50" });
  if (config.workspace) common.set("workspace", config.workspace);

  const search = new URLSearchParams({ q: config.query, limit: "200", maxContent: "0" });
  if (config.workspace) search.set("workspace", config.workspace);

  const cases: BenchmarkCase[] = [
    { name: "recent_sessions", path: `/v1/sessions?${common}` },
    { name: "event_text_search", path: `/v1/events?${search}` },
  ];
  if (config.sessionId) {
    const replay = new URLSearchParams({ limit: "200", maxContent: "0" });
    if (config.workspace) replay.set("workspace", config.workspace);
    cases.push({
      name: "session_replay",
      path: `/v1/sessions/${encodeURIComponent(config.sessionId)}/events?${replay}`,
    });
  }
  return cases;
}

function endpoint(base: string, path: string): string {
  const url = new URL(base);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("API URLs must use http or https");
  }
  if (url.username || url.password) throw new Error("API URLs must not contain credentials");
  return new URL(path, url).toString();
}

async function request(
  fetchImpl: Fetch,
  baseUrl: string,
  token: string,
  item: BenchmarkCase,
  timeoutMs: number,
): Promise<{ elapsedMs: number; status: number; hash: string }> {
  const before = performance.now();
  let response: Response;
  try {
    response = await fetchImpl(endpoint(baseUrl, item.path), {
      method: "GET",
      headers: { accept: "application/json", authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch {
    throw new Error(`${item.name} request failed`);
  }
  const body = await response.text();
  return {
    elapsedMs: performance.now() - before,
    status: response.status,
    hash: responseFingerprint(body),
  };
}

async function runEndpoint(
  fetchImpl: Fetch,
  baseUrl: string,
  token: string,
  item: BenchmarkCase,
  config: HttpBenchmarkConfig,
): Promise<EndpointResult> {
  for (let index = 0; index < config.warmups; index += 1) {
    await request(fetchImpl, baseUrl, token, item, config.timeoutMs);
  }

  const timings: number[] = [];
  const statuses = new Map<number, number>();
  const hashes = new Set<string>();
  let representativeStatus = 0;
  let representativeHash = "";
  let next = 0;
  const started = performance.now();
  await Promise.all(
    Array.from({ length: Math.min(config.concurrency, config.runs) }, async () => {
      for (;;) {
        const run = next++;
        if (run >= config.runs) return;
        const result = await request(fetchImpl, baseUrl, token, item, config.timeoutMs);
        timings.push(result.elapsedMs);
        statuses.set(result.status, (statuses.get(result.status) ?? 0) + 1);
        hashes.add(result.hash);
        if (!representativeHash) {
          representativeStatus = result.status;
          representativeHash = result.hash;
        }
      }
    }),
  );
  const wallMs = performance.now() - started;
  return {
    latency: httpLatencyStats(timings),
    throughputQps: round((config.runs * 1_000) / wallMs),
    statuses: Object.fromEntries([...statuses].sort(([left], [right]) => left - right).map(([key, value]) => [String(key), value])),
    responseStable: hashes.size === 1,
    representativeStatus,
    representativeHash,
  };
}

function publicEndpointResult(result: EndpointResult) {
  return {
    latency: result.latency,
    throughputQps: result.throughputQps,
    statuses: result.statuses,
    responseStable: result.responseStable,
  };
}

function ratio(numerator: number, denominator: number): number | null {
  return denominator === 0 ? null : round(numerator / denominator);
}

interface PublicEndpoint {
  latency: HttpLatencyStats;
  statuses: Record<string, number>;
  responseStable: boolean;
}

/**
 * The cutover gate over one HTTP run: only 2xx on both sides, identical status
 * mix and response, stable responses, compatibility p95 within maxP95Ratio.
 */
export function evaluateHttpGate(
  results: Record<string, { legacy: PublicEndpoint; compatibility: PublicEndpoint; parity: { status: boolean; response: boolean } }>,
  maxP95Ratio: number,
): { passed: boolean; failures: string[] } {
  const failures: string[] = [];
  for (const [name, result] of Object.entries(results)) {
    for (const side of ["legacy", "compatibility"] as const) {
      const bad = Object.keys(result[side].statuses).filter((status) => !/^2\d\d$/.test(status));
      if (bad.length) failures.push(`${name}: ${side} returned ${bad.join(", ")}`);
      if (!result[side].responseStable) failures.push(`${name}: ${side} responses changed between runs`);
    }
    if (!result.parity.status) failures.push(`${name}: status mix differs`);
    if (!result.parity.response) failures.push(`${name}: responses differ`);
    if (result.compatibility.latency.p95Ms > result.legacy.latency.p95Ms * maxP95Ratio)
      failures.push(`${name}: compatibility p95 ${result.compatibility.latency.p95Ms} ms > ${maxP95Ratio} x legacy ${result.legacy.latency.p95Ms} ms`);
  }
  return { passed: failures.length === 0, failures };
}

export async function runHttpBenchmark(config: HttpBenchmarkConfig, fetchImpl: Fetch = globalThis.fetch) {
  const results: Record<string, Parameters<typeof evaluateHttpGate>[0][string] & Record<string, unknown>> = {};
  for (const item of benchmarkCases(config)) {
    // Keep the two systems in the same measurement window. Running every
    // legacy sample first and every compatibility sample second would let
    // transient network/load changes masquerade as a platform speedup.
    const [legacy, compatibility] = await Promise.all([
      runEndpoint(fetchImpl, config.legacyUrl, config.legacyToken, item, config),
      runEndpoint(fetchImpl, config.compatibilityUrl, config.compatibilityToken, item, config),
    ]);
    results[item.name] = {
      legacy: publicEndpointResult(legacy),
      compatibility: publicEndpointResult(compatibility),
      parity: {
        status:
          legacy.representativeStatus === compatibility.representativeStatus &&
          JSON.stringify(legacy.statuses) === JSON.stringify(compatibility.statuses),
        response: legacy.representativeHash === compatibility.representativeHash,
      },
      compatibilitySpeedup: {
        p50: ratio(legacy.latency.p50Ms, compatibility.latency.p50Ms),
        p95: ratio(legacy.latency.p95Ms, compatibility.latency.p95Ms),
        p99: ratio(legacy.latency.p99Ms, compatibility.latency.p99Ms),
        throughput: ratio(compatibility.throughputQps, legacy.throughputQps),
      },
    };
  }
  return {
    generatedAt: new Date().toISOString(),
    configuration: {
      cases: Object.keys(results),
      runs: config.runs,
      warmups: config.warmups,
      concurrency: config.concurrency,
      timeoutMs: config.timeoutMs,
    },
    results,
  };
}

function integer(name: string, fallback: number, allowZero = false): number {
  const value = Number(process.env[name] ?? fallback);
  const minimum = allowZero ? 0 : 1;
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} must be ${allowZero ? "a non-negative" : "a positive"} integer`);
  }
  return value;
}

export function configFromEnv(): HttpBenchmarkConfig {
  const legacyUrl = process.env.RELAYHISTORY_LEGACY_API_URL;
  const compatibilityUrl = process.env.RELAYHISTORY_COMPAT_API_URL;
  const legacyToken = process.env.RELAYHISTORY_API_TOKEN;
  const query = process.env.BENCH_HTTP_QUERY;
  if (!legacyUrl || !compatibilityUrl || !legacyToken || !query) {
    throw new Error(
      "RELAYHISTORY_LEGACY_API_URL, RELAYHISTORY_COMPAT_API_URL, RELAYHISTORY_API_TOKEN and BENCH_HTTP_QUERY are required",
    );
  }
  return {
    legacyUrl,
    compatibilityUrl,
    legacyToken,
    compatibilityToken: process.env.RELAYHISTORY_COMPAT_API_TOKEN ?? legacyToken,
    query,
    sessionId: process.env.BENCH_HTTP_SESSION_ID || undefined,
    workspace: process.env.BENCH_HTTP_WORKSPACE || undefined,
    runs: integer("BENCH_HTTP_RUNS", 30),
    warmups: integer("BENCH_HTTP_WARMUPS", 3, true),
    concurrency: integer("BENCH_HTTP_CONCURRENCY", 4),
    timeoutMs: integer("BENCH_HTTP_TIMEOUT_MS", 30_000),
  };
}

export async function main() {
  const report = await runHttpBenchmark(configFromEnv());
  const maxP95Ratio = Number(process.env.BENCH_HTTP_MAX_P95_RATIO ?? 1);
  if (!(maxP95Ratio > 0)) throw new Error("BENCH_HTTP_MAX_P95_RATIO must be a positive number");
  const gate = evaluateHttpGate(report.results, maxP95Ratio);
  process.stdout.write(`${JSON.stringify({ ...report, gate: { maxP95Ratio, ...gate } }, null, 2)}\n`);
  // A red benchmark fails the step; it is never explained away.
  if (!gate.passed) process.exitCode = 2;
}

if (import.meta.main) {
  main().catch((error) => {
    const message = error instanceof Error && /^(BENCH_HTTP_|RELAYHISTORY_|API URLs)/.test(error.message)
      ? error.message
      : "HTTP benchmark request failed";
    console.error(`96bulls API benchmark failed: ${message}`);
    process.exit(1);
  });
}
