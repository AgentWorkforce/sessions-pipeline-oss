/**
 * Continuous Neon to S3 transcript export (96Bulls layout), CLI.
 *
 *   bun run migration:neon-s3-sync [--execute] [--once] [--max-groups=N]
 *
 * Without `--execute` it is a dry run: it reads Neon and builds every chunk and
 * manifest but stores nothing (a discarding store). With `--execute` it
 * writes, and only to `S3X_BUCKET` when that equals `S3X_EXPECT_BUCKET`.
 *
 * Configuration (environment; secrets only through files, never argv):
 *   NEON_URL_FILE        file holding the read-only Neon URL (e.g. a <(...) fd)
 *   S3X_CREDS_FILE       writer/exporter key: {"accessKeyId","secretAccessKey","sessionToken"?}
 *                        (else AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN)
 *   The exporter is write-only and never loads the eraser key; deletions are
 *   queued in the state file for `neon-s3-erasure` (its own process and key).
 *   S3X_BUCKET, S3X_EXPECT_BUCKET, S3X_REGION (us-east-1), S3X_ENDPOINT (MinIO)
 *   S3X_SSE              aws:kms | AES256; for sessions-prod `aws:kms` with S3X_SSE_KMS_KEY_ID
 *   S3X_SSE_KMS_KEY_ID   the bucket's key ARN (its policy refuses `aws:kms` without it)
 *   S3X_KEY_ROOT         96bulls
 *   S3X_GRANT_ID         neon-s3-sync
 *   S3X_STATE            SQLite watermark path
 *   S3X_ROWS_PER_SEC     1500      S3X_BYTES_PER_SEC  4000000
 *   S3X_SLOW_WINDOWS     2026-10-31T15:00Z/2026-11-01T00:00Z   S3X_SLOW_FACTOR 0.5
 *   S3X_RECENT_DAYS      14
 *   S3X_LANES            2  (parallel Neon connections; Neon is ~80 ms away, so latency, not load, bounds a lane)
 *
 * Logs are JSON lines of counts and codes only: never content, ids of
 * sessions, keys or credentials.
 */
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, rmSync, unlinkSync, writeSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import { createS3Store, objectKey, ObjectStoreError, type ObjectStore } from "../../shared/vendor.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { writeOnly } from "./export-session.ts";
import { sessionReadOnly } from "./neon-reader.ts";
import { SyncState } from "./state.ts";
import { DEFAULT_SYNC_CONFIG, Syncer, errorCode } from "./sync.ts";
import { Throttle, parseWindows } from "./throttle.ts";

/**
 * Optional non-secret tuning file (`S3X_TUNING_FILE`, default ~/.agent-tmp/s3x/tuning.json, JSON of S3X_* names to
 * values), read at each start, so a restart under the runner can change rates
 * or lanes without re-reading credentials. Only rate, lane and window keys.
 */
const TUNABLE = new Set(["S3X_LANES", "S3X_ROWS_PER_SEC", "S3X_BYTES_PER_SEC", "S3X_SLOW_WINDOWS", "S3X_SLOW_FACTOR"]);
const tuning: Record<string, string> = (() => {
  const file = process.env.S3X_TUNING_FILE?.trim() || `${process.env.HOME}/.agent-tmp/s3x/tuning.json`;
  try {
    const t = JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
    return Object.fromEntries(Object.entries(t).filter(([k]) => TUNABLE.has(k)).map(([k, v]) => [k, String(v)]));
  } catch {
    return {};
  }
})();
const env = (k: string, d?: string) => tuning[k]?.trim() || process.env[k]?.trim() || d;
const flag = (name: string) => process.argv.includes(`--${name}`);
const arg = (name: string) => process.argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3);

function log(fields: Record<string, unknown>) {
  process.stdout.write(`${JSON.stringify({ at: new Date().toISOString(), service: "neon-s3-sync", ...fields })}\n`);
}

/** A store that keeps nothing: a dry run computes every object and writes none. */
export function discardingStore(): ObjectStore {
  const notFound = (key: string) => new ObjectStoreError("not_found", "dry-run", `${key} not stored`);
  return {
    backend: "dry-run",
    head: async () => undefined,
    get: async (key) => {
      throw notFound(key);
    },
    getObject: async (key) => {
      throw notFound(key);
    },
    list: async () => ({ items: [] }),
    presignGet: async () => {
      throw new ObjectStoreError("unsupported", "dry-run", "no presign in a dry run");
    },
    put: async (key, body) => ({
      key: objectKey(key),
      version: {},
      contentSha256: createHash("sha256").update(body).digest("hex"),
      size: body.byteLength,
      created: true,
    }),
    delete: async () => undefined,
  };
}

function credentialsFrom(file: string | undefined, fallbackEnv: boolean) {
  if (file) {
    const c = JSON.parse(readFileSync(file, "utf8")) as Record<string, string>;
    if (!c.accessKeyId || !c.secretAccessKey) throw new Error("a credentials file lacks accessKeyId/secretAccessKey");
    return { accessKeyId: c.accessKeyId, secretAccessKey: c.secretAccessKey, ...(c.sessionToken ? { sessionToken: c.sessionToken } : {}) };
  }
  if (!fallbackEnv) return undefined;
  const accessKeyId = env("AWS_ACCESS_KEY_ID");
  const secretAccessKey = env("AWS_SECRET_ACCESS_KEY");
  if (!accessKeyId || !secretAccessKey) throw new Error("no S3 credentials (S3X_CREDS_FILE or AWS_* env)");
  const sessionToken = env("AWS_SESSION_TOKEN");
  return { accessKeyId, secretAccessKey, ...(sessionToken ? { sessionToken } : {}) };
}

function storeFromEnv(execute: boolean): ObjectStore {
  if (!execute) return discardingStore();
  const bucket = env("S3X_BUCKET");
  if (!bucket) throw new Error("S3X_BUCKET is required with --execute");
  if (env("S3X_EXPECT_BUCKET") !== bucket) throw new Error("S3X_EXPECT_BUCKET must equal S3X_BUCKET (write guard)");
  const sse = env("S3X_SSE");
  if (sse && sse !== "aws:kms" && sse !== "AES256") throw new Error("S3X_SSE must be aws:kms or AES256");
  const endpoint = env("S3X_ENDPOINT");
  const kmsKey = env("S3X_SSE_KMS_KEY_ID");
  if (kmsKey && !/^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/.test(kmsKey))
    throw new Error("S3X_SSE_KMS_KEY_ID must be a KMS key ARN");
  if (kmsKey && sse !== "aws:kms") throw new Error("S3X_SSE_KMS_KEY_ID needs S3X_SSE=aws:kms");
  const store = (credentials: NonNullable<ReturnType<typeof credentialsFrom>>) =>
    createS3Store({
      bucket,
      region: env("S3X_REGION", "us-east-1")!,
      ...(endpoint ? { endpoint } : {}),
      credentials,
      ...(sse ? { serverSideEncryption: sse as "aws:kms" | "AES256" } : {}),
      ...(kmsKey ? { sseKmsKeyId: kmsKey } : {}),
    });
  if (env("S3X_ERASER_CREDS_FILE")) throw new Error("the exporter must not be given the eraser key");
  return writeOnly(store(credentialsFrom(env("S3X_CREDS_FILE"), true)!));
}

/** The AWS error code (e.g. AccessDenied, InvalidAccessKeyId) from a store error: a code, never content. */
function s3Code(error: unknown): string | undefined {
  const m = /<Code>([A-Za-z0-9]{1,64})<\/Code>/.exec(String((error as { message?: unknown })?.message ?? ""));
  return m?.[1];
}

/**
 * Before any Neon read: the writer key can list the bucket (so a missing key
 * is a 404, not a 403) and HEAD a missing manifest. Logs status and AWS error
 * codes only. A failure waits 10 min before exiting, so a restart loop never
 * spins on a bad key.
 */
async function preflight(store: ObjectStore, root: string): Promise<boolean> {
  const steps: [string, () => Promise<unknown>][] = [
    ["list", () => store.list(objectKey(root))],
    ["head_missing", () => store.head(objectKey(`${root}/preflight/v1/ws=w/m=m/h=h/d=2026/01/01/s=s/manifest.json`))],
  ];
  for (const [step, run] of steps) {
    try {
      await run();
      log({ message: "preflight ok", step });
    } catch (error) {
      log({ message: "preflight failed", step, error_code: errorCode(error), aws_code: s3Code(error), status: (error as { status?: number }).status });
      return false;
    }
  }
  return true;
}

/**
 * The state file a run may write. `--execute` uses the watermark itself. A dry
 * run never does: it works on a private copy (`VACUUM INTO`, so the WAL is
 * included) of the watermark, or on an empty file, under
 * `~/.agent-tmp/s3x/dry-runs/`, removed by `cleanup`. A dry run's store
 * discards every write, so recording its "exports" in the real watermark would
 * mark sessions exported that are not in S3.
 */
export function stateForRun(execute: boolean, statePath: string, scratchDir = `${process.env.HOME}/.agent-tmp/s3x/dry-runs`): {
  path: string;
  cleanup: () => void;
} {
  if (execute) return { path: statePath, cleanup: () => {} };
  mkdirSync(scratchDir, { recursive: true, mode: 0o700 });
  const copy = join(scratchDir, `dry-${process.pid}-${Date.now()}.sqlite`);
  if (existsSync(statePath)) {
    const src = new DatabaseSync(statePath, { readOnly: true });
    try {
      src.exec(`VACUUM INTO '${copy.replace(/'/g, "''")}'`);
    } finally {
      src.close();
    }
  }
  return {
    path: copy,
    cleanup: () => {
      for (const f of [copy, `${copy}-wal`, `${copy}-shm`, `${copy}.lock`]) rmSync(f, { force: true });
    },
  };
}

export async function main() {
  const execute = flag("execute");
  const urlFile = env("NEON_URL_FILE");
  const url = urlFile ? readFileSync(urlFile, "utf8").trim() : env("NEON_URL");
  if (!url) throw new Error("NEON_URL_FILE (or NEON_URL) is required");
  const store = storeFromEnv(execute);
  if (execute && !(await preflight(store, env("S3X_KEY_ROOT", "96bulls")!))) {
    await new Promise((r) => setTimeout(r, 10 * 60 * 1000));
    process.exit(3);
  }
  const run = stateForRun(execute, env("S3X_STATE", `${process.env.HOME}/.agent-tmp/s3x/state.sqlite`)!);
  const statePath = run.path;
  if (!execute) log({ message: "dry run on a private copy of the watermark; the real state file is never written" });
  // One exporter per watermark: two would delete each other's freshly sealed chunks.
  const lockPath = `${statePath}.lock`;
  let lockFd: number;
  try {
    lockFd = openSync(lockPath, "wx", 0o600);
  } catch {
    const pid = Number(readFileSync(lockPath, "utf8").trim());
    let alive = false;
    try {
      process.kill(pid, 0);
      alive = Number.isInteger(pid) && pid > 0;
    } catch {}
    if (alive) throw Object.assign(new Error("another exporter holds the state lock"), { code: "state_locked" });
    unlinkSync(lockPath);
    lockFd = openSync(lockPath, "wx", 0o600);
  }
  writeSync(lockFd, String(process.pid));
  closeSync(lockFd);
  const state = new SyncState(statePath);
  if (flag("reexport-truncated")) {
    // One-shot, after the scrub rule set 2 sealer lands: queue the ledgered groups.
    log({ message: "re-export of truncated groups queued", count: state.forceReexportTruncated() });
  }
  const throttle = new Throttle({
    rowsPerSec: Number(env("S3X_ROWS_PER_SEC", "1500")),
    bytesPerSec: Number(env("S3X_BYTES_PER_SEC", "4000000")),
    slowWindows: parseWindows(env("S3X_SLOW_WINDOWS")),
    slowFactor: Number(env("S3X_SLOW_FACTOR", "0.5")),
  });
  if (/-pooler\./.test(new URL(url).hostname))
    throw new Error("use a direct (unpooled) Neon URL: session settings must not leak through a pooler");
  const lanes = Number(env("S3X_LANES", "2"));
  if (!Number.isInteger(lanes) || lanes < 1 || lanes > 8) throw new Error("S3X_LANES must be 1-8");
  const { default: pg } = await import("pg");
  const clients = Array.from({ length: lanes }, () => new pg.Client({ connectionString: url, application_name: "96bulls-neon-s3-sync" }));
  for (const c of clients) await c.connect();
  let stop = false;
  for (const sig of ["SIGINT", "SIGTERM"] as const)
    process.on(sig, () => {
      stop = true;
      log({ message: "stopping", signal: sig });
    });
  try {
    const dbs: Queryable[] = [];
    for (const c of clients) dbs.push(await sessionReadOnly(c as unknown as Queryable));
    const [db, ...extra] = dbs as [Queryable, ...Queryable[]];
    const syncer = new Syncer(
      db,
      state,
      {
        store,
        keyRoot: env("S3X_KEY_ROOT", "96bulls")!,
        grantId: env("S3X_GRANT_ID", "neon-s3-sync")!,
      },
      throttle,
      log,
      {
        ...DEFAULT_SYNC_CONFIG,
        includeLegacySources: env("S3X_INCLUDE_LEGACY_SOURCES", "false") === "true",
        recentMs: Number(env("S3X_RECENT_DAYS", "14")) * 86_400_000,
      },
      () => stop,
      undefined,
      extra,
    );
    log({ message: "start", execute, backend: store.backend, lanes, rates: throttle.rates() });
    const maxGroups = arg("max-groups");
    await syncer.run({ once: flag("once"), ...(maxGroups ? { maxGroups: Number(maxGroups) } : {}) });
  } finally {
    state.close();
    try {
      unlinkSync(lockPath);
    } catch {}
    run.cleanup();
    for (const c of clients) await c.end().catch(() => undefined);
  }
}

if (import.meta.main)
  main().catch((error) => {
    log({ message: "exit on error", error_code: errorCode(error) });
    process.exit(1);
  });
