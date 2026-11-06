/**
 * Read-only, content-safe comparison of the legacy RelayHistory search paths
 * on Neon and the Supabase target. It prints timings, plan summaries and hashes of
 * result identities; it never prints connection strings, tenant ids, search
 * text, session content or raw query plans.
 *
 * Required env: RELAYHISTORY_SOURCE_URL, BULLS_TARGET_URL, BENCH_ORG_ID,
 * BENCH_QUERY. Optional: BENCH_SESSION_ID, BENCH_RUNS, BENCH_WARMUPS,
 * BENCH_CONCURRENCY, BENCH_STATEMENT_TIMEOUT_MS.
 */
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import pg from "pg";

interface BenchCase {
  name: string;
  sql: string;
  params: (input: BenchInput) => unknown[];
}

interface BenchInput {
  orgId: string;
  query: string;
  sessionId: string;
}

export interface LatencyStats {
  minMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
  meanMs: number;
}

/** Must stay expression-identical to production recall's selective trigram path. */
export const EVENT_TEXT_SEARCH_SQL = `SELECT session_id, source, machine_id, kind, event_id, ts
  FROM sessions.convergence_events
 WHERE org_id = $1
   AND (left(content, 4096) ILIKE '%' || $2 || '%' OR task_title ILIKE '%' || $2 || '%')
 ORDER BY ts DESC, event_id DESC, machine_id DESC, source DESC, session_id DESC, kind DESC
 LIMIT 200`;

const CASES: BenchCase[] = [
  {
    name: "recent_sessions",
    sql: `SELECT session_id, source, last_ts, event_count
            FROM sessions.session_rollups
           WHERE org_id = $1
           ORDER BY last_ts DESC, session_id DESC, source DESC
           LIMIT 50`,
    params: ({ orgId }) => [orgId],
  },
  {
    name: "event_text_search",
    sql: EVENT_TEXT_SEARCH_SQL,
    params: ({ orgId, query }) => [orgId, query],
  },
  {
    name: "session_replay",
    sql: `SELECT session_id, source, machine_id, kind, event_id, ts,
                 octet_length(coalesce(content, '')) AS content_bytes
            FROM sessions.convergence_events
           WHERE org_id = $1 AND session_id = $2
           ORDER BY ts ASC, event_id ASC, machine_id ASC, source ASC, session_id ASC, kind ASC
           LIMIT 201`,
    params: ({ orgId, sessionId }) => [orgId, sessionId],
  },
  {
    name: "delivery_keyset",
    sql: `SELECT origin_id, record_id, revision_id, revision, kind, source, session_id, operation
            FROM sessions.delivery_records
           WHERE org_id = $1
           ORDER BY origin_id, record_id
           LIMIT 501`,
    params: ({ orgId }) => [orgId],
  },
];

function round(value: number): number {
  return Math.round(value * 100) / 100;
}

export function latencyStats(values: number[]): LatencyStats {
  if (!values.length) throw new Error("benchmark needs at least one timing");
  const sorted = [...values].sort((a, b) => a - b);
  const at = (p: number) => sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1))]!;
  return {
    minMs: round(sorted[0]!),
    p50Ms: round(at(0.5)),
    p95Ms: round(at(0.95)),
    p99Ms: round(at(0.99)),
    maxMs: round(sorted.at(-1)!),
    meanMs: round(sorted.reduce((sum, value) => sum + value, 0) / sorted.length),
  };
}

function resultHash(rows: unknown[]): string {
  return createHash("sha256").update(JSON.stringify(rows)).digest("hex");
}

function identityHash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 16);
}

interface PlanNode {
  "Node Type"?: string;
  "Relation Name"?: string;
  "Index Name"?: string;
  "Actual Rows"?: number;
  "Actual Total Time"?: number;
  "Actual Loops"?: number;
  "Shared Hit Blocks"?: number;
  "Shared Read Blocks"?: number;
  Plans?: PlanNode[];
}

function planNodes(node: PlanNode | undefined): Array<Record<string, unknown>> {
  if (!node) return [];
  return [
    {
      node: node["Node Type"],
      ...(node["Relation Name"] ? { relation: node["Relation Name"] } : {}),
      ...(node["Index Name"] ? { index: node["Index Name"] } : {}),
      rows: node["Actual Rows"],
      totalMs: round(node["Actual Total Time"] ?? 0),
      loops: node["Actual Loops"],
      sharedHits: node["Shared Hit Blocks"] ?? 0,
      sharedReads: node["Shared Read Blocks"] ?? 0,
    },
    ...(node.Plans ?? []).flatMap(planNodes),
  ];
}

async function explain(pool: pg.Pool, item: BenchCase, input: BenchInput) {
  const result = await pool.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${item.sql}`, item.params(input));
  const root = result.rows[0]?.["QUERY PLAN"]?.[0] as {
    Plan?: PlanNode;
    "Planning Time"?: number;
    "Execution Time"?: number;
  } | undefined;
  return {
    planningMs: round(root?.["Planning Time"] ?? 0),
    executionMs: round(root?.["Execution Time"] ?? 0),
    nodes: planNodes(root?.Plan),
  };
}

async function runCase(pool: pg.Pool, item: BenchCase, input: BenchInput, warmups: number, runs: number, concurrency: number) {
  const params = item.params(input);
  for (let index = 0; index < warmups; index += 1) await pool.query(item.sql, params);

  const timings: number[] = [];
  const hashes = new Set<string>();
  const started = performance.now();
  let next = 0;
  await Promise.all(Array.from({ length: concurrency }, async () => {
    for (;;) {
      const run = next++;
      if (run >= runs) return;
      const before = performance.now();
      const result = await pool.query(item.sql, params);
      timings.push(performance.now() - before);
      hashes.add(resultHash(result.rows));
    }
  }));
  const wallMs = performance.now() - started;
  return {
    latency: latencyStats(timings),
    throughputQps: round((runs * 1000) / wallMs),
    resultHash: [...hashes][0] ?? "",
    // Every run returned the same rows; a live table changing mid-run is not a valid sample.
    stable: hashes.size === 1,
    plan: await explain(pool, item, input),
  };
}

export interface CaseResult {
  latency: LatencyStats;
  resultHash: string;
  stable: boolean;
  plan: { nodes: Array<Record<string, unknown>> };
}

const INDEXED = /Index|Bitmap/;

/**
 * The cutover gate over one benchmark run: identical results, stable samples,
 * target p95 within maxP95Ratio of Neon's, and no sequential scan on a
 * relation Neon reaches through an index.
 */
export function evaluateGate(
  results: Record<string, { source: CaseResult; target: CaseResult }>,
  maxP95Ratio: number,
): { passed: boolean; failures: string[] } {
  const failures: string[] = [];
  for (const [name, { source, target }] of Object.entries(results)) {
    if (source.resultHash !== target.resultHash) failures.push(`${name}: results differ`);
    if (!source.stable || !target.stable) failures.push(`${name}: results changed between runs`);
    if (target.latency.p95Ms > source.latency.p95Ms * maxP95Ratio)
      failures.push(`${name}: target p95 ${target.latency.p95Ms} ms > ${maxP95Ratio} x source ${source.latency.p95Ms} ms`);
    const indexed = new Set(
      source.plan.nodes
        .filter((node) => INDEXED.test(String(node.node)) && node.relation)
        .map((node) => String(node.relation)),
    );
    for (const node of target.plan.nodes)
      if (node.node === "Seq Scan" && indexed.has(String(node.relation)))
        failures.push(`${name}: target seq-scans ${String(node.relation)}, which the source reads by index`);
  }
  return { passed: failures.length === 0, failures };
}

async function chooseSession(pool: pg.Pool, orgId: string): Promise<string> {
  const result = await pool.query<{ session_id: string }>(
    `SELECT session_id FROM sessions.session_rollups
      WHERE org_id = $1 ORDER BY last_ts DESC, session_id DESC LIMIT 1`,
    [orgId],
  );
  const value = result.rows[0]?.session_id;
  if (!value) throw new Error("benchmark org has no session_rollups row; set BENCH_SESSION_ID explicitly");
  return value;
}

function positive(name: string, fallback: number): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
  return value;
}

export async function main() {
  const sourceUrl = process.env.RELAYHISTORY_SOURCE_URL;
  const targetUrl = process.env.BULLS_TARGET_URL;
  const orgId = process.env.BENCH_ORG_ID;
  const query = process.env.BENCH_QUERY;
  if (!sourceUrl || !targetUrl || !orgId || !query)
    throw new Error("RELAYHISTORY_SOURCE_URL, BULLS_TARGET_URL, BENCH_ORG_ID and BENCH_QUERY are required");
  const runs = positive("BENCH_RUNS", 30);
  const warmups = positive("BENCH_WARMUPS", 3);
  const concurrency = positive("BENCH_CONCURRENCY", 4);
  const statementTimeout = positive("BENCH_STATEMENT_TIMEOUT_MS", 120_000);
  const source = new pg.Pool({ connectionString: sourceUrl, max: concurrency, statement_timeout: statementTimeout, application_name: "96bulls-search-benchmark-source" });
  const target = new pg.Pool({ connectionString: targetUrl, max: concurrency, statement_timeout: statementTimeout, application_name: "96bulls-search-benchmark-target" });
  try {
    const sessionId = process.env.BENCH_SESSION_ID ?? await chooseSession(source, orgId);
    const input = { orgId, query, sessionId };
    const results: Record<string, { source: CaseResult; target: CaseResult } & Record<string, unknown>> = {};
    for (const item of CASES) {
      const sourceResult = await runCase(source, item, input, warmups, runs, concurrency);
      const targetResult = await runCase(target, item, input, warmups, runs, concurrency);
      results[item.name] = {
        source: sourceResult,
        target: targetResult,
        resultParity: sourceResult.resultHash === targetResult.resultHash,
        speedup: {
          p50: round(sourceResult.latency.p50Ms / targetResult.latency.p50Ms),
          p95: round(sourceResult.latency.p95Ms / targetResult.latency.p95Ms),
          throughput: round(targetResult.throughputQps / sourceResult.throughputQps),
        },
      };
    }
    const maxP95Ratio = Number(process.env.BENCH_MAX_P95_RATIO ?? 1);
    if (!(maxP95Ratio > 0)) throw new Error("BENCH_MAX_P95_RATIO must be a positive number");
    const gate = evaluateGate(results, maxP95Ratio);
    process.stdout.write(`${JSON.stringify({
      generatedAt: new Date().toISOString(),
      inputs: { orgHash: identityHash(orgId), queryHash: identityHash(query), sessionHash: identityHash(sessionId), runs, warmups, concurrency, maxP95Ratio },
      gate,
      results,
    }, null, 2)}\n`);
    // A red benchmark fails the step; it is never explained away.
    if (!gate.passed) process.exitCode = 2;
  } finally {
    await Promise.all([source.end(), target.end()]);
  }
}

if (import.meta.main) main().catch((error) => {
  console.error(`96bulls benchmark failed: ${error instanceof Error ? error.message : "unknown error"}`);
  process.exit(1);
});
