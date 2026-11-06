/**
 * The history-feature migration track (P5b): tables ported off relayhistory-cloud's
 * Neon `sessions` schema into the Supabase `history` schema.
 *
 * It is deliberately SEPARATE from the HoopDb ingest track (src/hoopdb/migrate.ts):
 * its files live in supabase/history-migrations/, it has its own ledger, readiness
 * marker, advisory lock and generation, and the ingest generation() never reads it.
 * The live ingest Worker, the buffered GC and every ingest admission row are pinned
 * to the ingest generation; adding a history table must never move it.
 *
 * Retention-ready by contract (see historyRetentionViolations): every history table
 * carries org_id and a timestamptz column, so a per-org TTL can be enabled later.
 */
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
export const HISTORY_MIGRATIONS = join(ROOT, "supabase/history-migrations");
export const HISTORY_SCHEMA = "history";
export const HISTORY_LEDGER = "history.__history_migrations";
export const HISTORY_READINESS = "history.__history_readiness";
export const HISTORY_READINESS_NAME = "history_track";
/** Distinct from the ingest migrator's (961996, 1). */
export const HISTORY_LOCK = [961996, 2] as const;
const NAME = /^[0-9]{4}_[a-z0-9_]+\.sql$/;

export interface HistoryClient {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}
export interface HistoryStep {
  name: string;
  path: string;
}

const refuse = (code: string, message: string): never => {
  throw Object.assign(new Error(`history: ${message}`), { code });
};

export async function historySteps(dir = HISTORY_MIGRATIONS): Promise<HistoryStep[]> {
  const files = (await readdir(dir)).filter((f) => f.endsWith(".sql")).sort();
  for (const f of files) if (!NAME.test(f)) refuse("history_migration_name", `bad migration file name ${f}`);
  return files.map((f) => ({ name: f, path: join(dir, f) }));
}

const sha = (text: string) => createHash("sha256").update(text).digest("hex");

/** The history track's own generation: never part of the ingest generation(). */
export async function historyGeneration(dir = HISTORY_MIGRATIONS): Promise<string> {
  const parts: string[][] = [];
  for (const s of await historySteps(dir)) parts.push([s.name, sha(await readFile(s.path, "utf8"))]);
  return sha(JSON.stringify(parts));
}

export function historyLedgerSha256(ledger: Map<string, string>): string {
  return sha(JSON.stringify([...ledger].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))));
}

/**
 * Retention-ready contract: every CREATE TABLE in the track must declare org_id and
 * at least one timestamptz column. Returns the offending "file: table" pairs.
 */
export async function historyRetentionViolations(dir = HISTORY_MIGRATIONS): Promise<string[]> {
  const out: string[] = [];
  for (const s of await historySteps(dir)) {
    const text = (await readFile(s.path, "utf8")).replace(/--[^\n]*/g, "");
    for (const m of text.matchAll(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z0-9_."]+)\s*\(([\s\S]*?)\)\s*;/gi)) {
      const body = m[2]!.toLowerCase();
      if (/__history_/.test(m[1]!)) continue; // the runner's own bookkeeping tables
      if (!/\borg_id\b/.test(body) || !/\btimestamptz\b|\btimestamp with time zone\b/.test(body))
        out.push(`${s.name}: ${m[1]}`);
    }
  }
  return out;
}

async function ledgerOf(client: HistoryClient): Promise<Map<string, string>> {
  const exists = (await client.query<{ t: string | null }>("SELECT to_regclass($1)::text AS t", [HISTORY_LEDGER])).rows[0]?.t;
  if (!exists) return new Map();
  return new Map((await client.query<{ name: string; sha256: string }>(`SELECT name, sha256 FROM ${HISTORY_LEDGER}`)).rows.map((r) => [r.name, r.sha256]));
}

/** Read-only: what is applied, what is pending, and the source generation. */
export async function inspectHistory(client: HistoryClient, dir = HISTORY_MIGRATIONS) {
  const steps = await historySteps(dir);
  const ledger = await ledgerOf(client);
  const inventory = [];
  for (const s of steps) {
    const hash = sha(await readFile(s.path, "utf8"));
    const seen = ledger.get(s.name);
    if (seen !== undefined && seen !== hash) refuse("history_migration_changed", `${s.name} changed after it was applied`);
    inventory.push({ name: s.name, sha256: hash, pending: seen === undefined });
  }
  for (const name of ledger.keys())
    if (!steps.some((s) => s.name === name)) refuse("history_ledger_unknown", `ledger has ${name}, which is not in the source`);
  return { generation: await historyGeneration(dir), ledgerSha256: historyLedgerSha256(ledger), inventory };
}

export interface HistoryRunOptions {
  dir?: string;
  /** The exact ledger the approved read-only plan saw; any drift refuses before writing. */
  expectedLedgerSha256?: string;
  beforeWrite?: () => Promise<void>;
}

/**
 * Applies the pending history migrations, each in its own transaction with its ledger
 * row, under the history advisory lock. Readiness goes pending for this generation
 * first and flips to pass only when every source migration is recorded.
 */
export async function runHistoryMigrations(client: HistoryClient, opts: HistoryRunOptions = {}) {
  const dir = opts.dir ?? HISTORY_MIGRATIONS;
  let applied = 0, skipped = 0;
  await client.query(`SELECT pg_advisory_lock(${HISTORY_LOCK[0]}, ${HISTORY_LOCK[1]})`);
  try {
    const before = await inspectHistory(client, dir);
    if (opts.expectedLedgerSha256 !== undefined && before.ledgerSha256 !== opts.expectedLedgerSha256)
      refuse("history_ledger_changed", "history ledger changed since the approved read-only plan");
    if (opts.beforeWrite) await opts.beforeWrite();
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${HISTORY_SCHEMA}`);
    await client.query(`REVOKE ALL ON SCHEMA ${HISTORY_SCHEMA} FROM PUBLIC`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${HISTORY_LEDGER} (name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${HISTORY_READINESS} (name text PRIMARY KEY, generation text NOT NULL,
      status text NOT NULL CHECK (status IN ('pending','pass')), updated_at timestamptz NOT NULL DEFAULT now())`);
    await client.query(`REVOKE ALL ON ${HISTORY_LEDGER}, ${HISTORY_READINESS} FROM PUBLIC`);
    await client.query(`INSERT INTO ${HISTORY_READINESS} (name, generation, status) VALUES ($1, $2, 'pending')
      ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, status = 'pending', updated_at = now()`,
      [HISTORY_READINESS_NAME, before.generation]);
    for (const step of before.inventory) {
      if (!step.pending) { skipped++; continue; }
      const text = await readFile(join(dir, step.name), "utf8");
      await client.query("BEGIN");
      try {
        await client.query(text);
        await client.query(`INSERT INTO ${HISTORY_LEDGER} (name, sha256) VALUES ($1, $2)`, [step.name, step.sha256]);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
      applied++;
    }
    const after = await inspectHistory(client, dir);
    if (after.inventory.some((s) => s.pending) || after.generation !== before.generation)
      refuse("history_incomplete", "history migrations did not all record");
    await client.query(`UPDATE ${HISTORY_READINESS} SET status = 'pass', updated_at = now()
      WHERE name = $1 AND generation = $2 AND status = 'pending'`, [HISTORY_READINESS_NAME, after.generation]);
    return { applied, skipped, generation: after.generation, ledgerSha256: after.ledgerSha256 };
  } finally {
    await client.query(`SELECT pg_advisory_unlock(${HISTORY_LOCK[0]}, ${HISTORY_LOCK[1]})`).catch(() => undefined);
  }
}

/** For readers: the history track is ready for exactly this generation. */
export async function historyReady(client: HistoryClient, generation: string): Promise<boolean> {
  const t = (await client.query<{ t: string | null }>("SELECT to_regclass($1)::text AS t", [HISTORY_READINESS])).rows[0]?.t;
  if (!t) return false;
  const row = (await client.query<{ generation: string; status: string }>(`SELECT generation, status FROM ${HISTORY_READINESS} WHERE name = $1`, [HISTORY_READINESS_NAME])).rows[0];
  return row?.generation === generation && row.status === "pass";
}
