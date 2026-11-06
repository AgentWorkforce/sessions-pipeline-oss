/**
 * Sequence fence for the T0 cutover (docs/relayhistory-t0-runbook.md steps 4
 * and 10). Every `sessions` sequence on Supabase must sit at least GAP above
 * Neon's, so no row written after T0 can take an id Neon has used or could
 * still have used before the fence closed.
 *
 *   check (default, read-only): every Neon sequence exists on the target, has
 *         been called, and its value is >= Neon's + GAP. A sequence Neon never
 *         used counts from its start value. Exit status 2 on any gap.
 *   --apply: raise each target sequence to that bound with setval, never
 *         lower it (safe to rerun: before T0, and again in the hold before the
 *         flip). Needs --execute and --expect-supabase-ref (target-guard).
 *
 * Usage: RELAYHISTORY_SOURCE_URL=... BULLS_TARGET_URL=... \
 *   bun run migration:sequence-fence [--gap=1000000] [--apply --execute --expect-supabase-ref=<ref>]
 */
import pg from "pg";
import { guardFromArgv } from "./target-guard.js";

export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

export const GAP = 1_000_000n;

interface SequenceState {
  name: string;
  lastValue: bigint | null;
  startValue: bigint;
  increment: bigint;
}

async function sequences(db: Queryable): Promise<Map<string, SequenceState>> {
  const { rows } = await db.query<{
    name: string;
    last_value: string | null;
    start_value: string;
    increment_by: string;
  }>(
    `SELECT sequencename AS name, last_value::text, start_value::text, increment_by::text
       FROM pg_sequences WHERE schemaname = 'sessions' ORDER BY sequencename`,
  );
  return new Map(
    rows.map((row) => [
      row.name,
      {
        name: row.name,
        lastValue: row.last_value === null ? null : BigInt(row.last_value),
        startValue: BigInt(row.start_value),
        increment: BigInt(row.increment_by),
      },
    ]),
  );
}

export interface FenceRow {
  name: string;
  required: string;
  target: string | null;
  fenced: boolean;
  reason?: string;
}

/** The value each target sequence must have reached, and whether it has. */
export async function checkFence(
  source: Queryable,
  target: Queryable,
  gap = GAP,
): Promise<{ passed: boolean; sequences: FenceRow[] }> {
  const [neon, supabase] = await Promise.all([sequences(source), sequences(target)]);
  const rows: FenceRow[] = [];
  for (const sequence of neon.values()) {
    if (sequence.increment <= 0n) {
      rows.push({ name: sequence.name, required: "-", target: null, fenced: false, reason: "descending sequence" });
      continue;
    }
    // A never-called Neon sequence would hand out its start value next.
    const required = (sequence.lastValue ?? sequence.startValue) + gap;
    const found = supabase.get(sequence.name);
    const value = found?.lastValue ?? null;
    rows.push({
      name: sequence.name,
      required: required.toString(),
      target: value === null ? null : value.toString(),
      fenced: value !== null && value >= required,
      ...(!found
        ? { reason: "missing on target" }
        : value === null
          ? { reason: "never called on target" }
          : value < required
            ? { reason: "below the fence" }
            : {}),
    });
  }
  return { passed: rows.every((row) => row.fenced), sequences: rows };
}

/** Raise every target sequence to its bound; never lowers one. */
export async function applyFence(source: Queryable, target: Queryable, gap = GAP) {
  const { sequences: rows } = await checkFence(source, target, gap);
  for (const row of rows) {
    if (row.fenced || row.reason === "missing on target" || row.reason === "descending sequence")
      continue;
    await target.query(
      `SELECT setval(format('sessions.%I', $1::text)::regclass,
                     greatest($2::bigint, coalesce((SELECT last_value FROM pg_sequences
                       WHERE schemaname = 'sessions' AND sequencename = $1::text), 0)), true)`,
      [row.name, row.required],
    );
  }
  return checkFence(source, target, gap);
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

export async function main() {
  const sourceUrl = process.env.RELAYHISTORY_SOURCE_URL;
  const targetUrl = process.env.BULLS_TARGET_URL;
  if (!sourceUrl || !targetUrl)
    throw new Error("RELAYHISTORY_SOURCE_URL and BULLS_TARGET_URL are required");
  const gap = BigInt(argument("gap") ?? GAP.toString());
  if (gap < 1n) throw new Error("--gap must be positive");
  const apply = process.argv.includes("--apply");
  const write = apply && guardFromArgv(process.argv, targetUrl, sourceUrl) === "execute";
  if (apply && !write) throw new Error("--apply needs --execute and --expect-supabase-ref");
  const source = new pg.Client({ connectionString: sourceUrl, application_name: "96bulls-sequence-fence-source" });
  const target = new pg.Client({ connectionString: targetUrl, application_name: "96bulls-sequence-fence-target" });
  await Promise.all([source.connect(), target.connect()]);
  try {
    await source.query("SET default_transaction_read_only = on");
    if (!write) await target.query("SET default_transaction_read_only = on");
    const result = write ? await applyFence(source, target, gap) : await checkFence(source, target, gap);
    process.stdout.write(`${JSON.stringify({ applied: write, gap: gap.toString(), ...result }, null, 2)}\n`);
    if (!result.passed) process.exitCode = 2;
  } finally {
    await Promise.all([source.end(), target.end()]);
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls sequence fence failed: ${error instanceof Error ? error.message : "unknown error"}`);
    process.exit(1);
  });
