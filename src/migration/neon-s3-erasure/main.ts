/**
 * `neon-s3-erasure` CLI: the transcript bucket's only deleter.
 *
 *   bun run migration:neon-s3-erasure [--execute] [--once]
 *
 * Holds only the eraser key (`S3X_ERASER_CREDS_FILE`, 1Password item
 * "sessions-prod transcript eraser"); refuses a writer key. Without
 * `--execute` it sweeps and reports what it would erase, deleting nothing.
 * Shares the exporter's watermark (`S3X_STATE`) on the same host.
 *
 *   NEON_URL_FILE, S3X_STATE, S3X_BUCKET, S3X_EXPECT_BUCKET, S3X_REGION, S3X_ENDPOINT, S3X_KEY_ROOT,
 *   S3X_ERASER_RECOGNIZES_LEGACY ("true" keeps legacy-only groups; default false)
 */
import { readFileSync } from "node:fs";
import type { Queryable } from "../relayhistory-export.ts";
import { sessionReadOnly } from "../neon-s3-sync/neon-reader.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { errorCode } from "../neon-s3-sync/sync.ts";
import { createS3Eraser, type Eraser } from "./eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "./job.ts";

const env = (k: string, d?: string) => process.env[k]?.trim() || d;
const flag = (name: string) => process.argv.includes(`--${name}`);
function log(fields: Record<string, unknown>) {
  process.stdout.write(`${JSON.stringify({ at: new Date().toISOString(), service: "neon-s3-erasure", ...fields })}\n`);
}

function eraserFromEnv(execute: boolean): Eraser {
  if (env("S3X_CREDS_FILE") || env("AWS_ACCESS_KEY_ID")) throw new Error("the erasure job must not be given the writer key");
  if (!execute)
    return {
      eraseObject: async () => 0,
      erasePrefix: async () => 0,
      eraseSessionAnyDate: async () => ({ deleted: 0, kept: 0 }),
      eraseObjectBefore: async () => ({ deleted: 0, kept: 0 }),
      listKeys: async () => [],
    };
  const bucket = env("S3X_BUCKET");
  if (!bucket || env("S3X_EXPECT_BUCKET") !== bucket) throw new Error("S3X_BUCKET must equal S3X_EXPECT_BUCKET (write guard)");
  const file = env("S3X_ERASER_CREDS_FILE");
  if (!file) throw new Error("S3X_ERASER_CREDS_FILE is required with --execute");
  const c = JSON.parse(readFileSync(file, "utf8")) as Record<string, string>;
  if (!c.accessKeyId || !c.secretAccessKey) throw new Error("the eraser credentials file lacks accessKeyId/secretAccessKey");
  const endpoint = env("S3X_ENDPOINT");
  return createS3Eraser({
    bucket,
    region: env("S3X_REGION", "us-east-1")!,
    ...(endpoint ? { endpoint } : {}),
    credentials: { accessKeyId: c.accessKeyId, secretAccessKey: c.secretAccessKey, ...(c.sessionToken ? { sessionToken: c.sessionToken } : {}) },
  });
}

export async function main() {
  const execute = flag("execute");
  const urlFile = env("NEON_URL_FILE");
  const url = urlFile ? readFileSync(urlFile, "utf8").trim() : env("NEON_URL");
  if (!url) throw new Error("NEON_URL_FILE (or NEON_URL) is required");
  if (/-pooler\./.test(new URL(url).hostname)) throw new Error("use a direct (unpooled) Neon URL");
  const eraser = eraserFromEnv(execute);
  const state = new SyncState(env("S3X_STATE", `${process.env.HOME}/.agent-tmp/s3x/state.sqlite`)!);
  const { default: pg } = await import("pg");
  const client = new pg.Client({ connectionString: url, application_name: "96bulls-neon-s3-erasure" });
  await client.connect();
  let stop = false;
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => void (stop = true));
  try {
    const db = await sessionReadOnly(client as unknown as Queryable);
    const cfg = {
      ...DEFAULT_ERASURE_CONFIG,
      keyRoot: env("S3X_KEY_ROOT", "96bulls")!,
      recognizeLegacy: env("S3X_ERASER_RECOGNIZES_LEGACY", "false") === "true",
    };
    const job = new ErasureJob(db, state, eraser, log, cfg, Date.now, undefined, () => stop);
    log({ message: "start", execute, recognize_legacy: cfg.recognizeLegacy });
    // A dry run changes nothing: it sweeps and reports what it would erase.
    if (!execute) {
      await job.sweep({ dryRun: true });
      job.progress(true);
      return;
    }
    await job.run({ once: flag("once") });
  } finally {
    state.close();
    await client.end().catch(() => undefined);
  }
}

if (import.meta.main)
  main().catch((error) => {
    log({ message: "exit on error", error_code: errorCode(error) });
    process.exit(1);
  });
