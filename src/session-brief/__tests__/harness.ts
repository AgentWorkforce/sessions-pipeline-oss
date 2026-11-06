/**
 * Test harness for the session brief: in-process PGlite with the full HoopDb migration
 * plan (as src/hoopdb/migrate.ts orders it), an fs Locker, fake manifest reader and a fake
 * model transport. Nothing here reaches a network, a real database or a real model.
 */
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { computeContentSha256 } from "../../../packages/object-store/src/manifest.ts";
import { rawChunkKey, type ManifestV1, type ObjectStore } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { SESSION_DIRECTORY_SEARCH_RPC } from "../../hoopdb/search-index-contract.ts";
import type {
  AnalysisModelTransport,
  ModelTransportRequest,
  ModelTransportResponse,
} from "../engine/model.ts";
import { briefConfig, type BriefConfig } from "../config.ts";
import type { SessionSummaryState } from "../engine/summary-contract.ts";

export const root = join(import.meta.dir, "../../..");
export const read = (path: string) => readFileSync(join(root, path), "utf8");
export const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;
export const TOKEN_ISSUER = "https://issuer.test/session-directory";
export const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
export const KEY = { orgId: "org-a", workspaceId: "ws-a", harness: "claude", sessionId: "s-1" };

export async function database(opts: { hoop?: boolean } = {}): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort())
    await db.exec(read(`supabase/migrations/${file}`));
  if (opts.hoop !== false)
    for (const file of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort())
      await db.exec(read(`src/hoopdb/migrations/${file}`));
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, SESSION_DIRECTORY_SEARCH_RPC.audience],
  );
  return db;
}

/** The HoopDb shape the brief code uses, over one PGlite connection. */
export function hoopDb(pg: PGlite): HoopDb {
  const q = async (text: string, params?: unknown[]) => ({
    rows: (await pg.query(text, params as never)).rows as never[],
  });
  return {
    query: q,
    pool: { connect: async () => ({ query: q, release() {} }) },
    end: async () => {},
  } as unknown as HoopDb;
}

export const claims = (org: string, workspaces: unknown[], overrides: Record<string, unknown> = {}) => {
  const iat = Math.floor(Date.now() / 1000);
  return JSON.stringify({
    role: WORKER,
    iss: TOKEN_ISSUER,
    aud: SESSION_DIRECTORY_SEARCH_RPC.audience,
    iat,
    exp: iat + 60,
    app_metadata: { org_id: org, workspace_ids: workspaces },
    ...overrides,
  });
};

export async function asRole<T>(pg: PGlite, role: string, claimsJson: string | null, run: () => Promise<T>): Promise<T> {
  await pg.query("SELECT set_config('request.jwt.claims', $1, false)", [claimsJson ?? ""]);
  await pg.exec(`SET ROLE ${role}`);
  try {
    return await run();
  } finally {
    await pg.exec("RESET ROLE");
    await pg.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

/** sessions.session_brief as the Worker role with claims for `org`/`workspaces`. */
export const briefRpc = (
  pg: PGlite,
  key = KEY,
  claimsJson = claims(key.orgId, [key.workspaceId]),
  role = WORKER,
) =>
  asRole(pg, role, claimsJson, async () =>
    (
      await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_brief($1, $2, $3)", [
        key.workspaceId,
        key.harness,
        key.sessionId,
      ])
    ).rows,
  );

export const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

/** A session_index pointer, then the directory projection, as the indexer does. */
export async function pointer(
  pg: PGlite,
  opts: { key?: typeof KEY; machine?: string; revision?: number; title?: string | null; manifestKey?: string } = {},
) {
  const key = opts.key ?? KEY;
  const machine = opts.machine ?? "m-1";
  await pg.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title)
     VALUES ($1, $2, $3, $4, $5, 'u-1', now(), now(), 'fs', 'bucket', $6,
             decode(repeat('ab', 32), 'hex'), 'jsonl', $7, $8, $9)
     ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
       revision = excluded.revision, manifest_key = excluded.manifest_key,
       title = excluded.title, last_activity_at = now(), deleted_at = NULL`,
    [
      key.orgId,
      key.workspaceId,
      key.harness,
      key.sessionId,
      machine,
      opts.manifestKey ?? `manifest/${key.sessionId}/${machine}`,
      TRACE,
      opts.revision ?? 1,
      opts.title === undefined ? "pointer title" : opts.title,
    ],
  );
  await projectSessionDirectory(hoopDb(pg), key);
}

export const loc = (key = KEY, machine = "m-1") => ({
  workspaceId: key.workspaceId,
  machineId: machine,
  harness: key.harness,
  sessionId: key.sessionId,
  startDate: "2026-10-29",
});

export function tempStore(): ObjectStore {
  return createFsStore({ root: mkdtempSync(join(tmpdir(), "brief-")) });
}

const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

/** A scrubbed (unless `scrubbed: false`) JSONL harness transcript manifest with its chunk stored. */
export async function transcript(
  store: ObjectStore,
  lines: unknown[],
  opts: { key?: typeof KEY; machine?: string; revision?: number; scrubbed?: boolean } = {},
): Promise<ManifestV1> {
  const key = opts.key ?? KEY;
  const machine = opts.machine ?? "m-1";
  const raw = new TextEncoder().encode(lines.map((l) => JSON.stringify(l)).join("\n") + "\n");
  const digest = sha(raw);
  const stored = new Uint8Array(zstdCompressSync(raw));
  await store.put(rawChunkKey(loc(key, machine), 0, 0, digest, "jsonl"), stored);
  const chunk = {
    seq: 0,
    epoch: 0,
    start_offset: 0,
    bytes: raw.byteLength,
    stored_bytes: stored.byteLength,
    sha256: digest,
    sha256_8: digest.slice(0, 8),
    stored_sha256: sha(stored),
    stored_raw_bytes: raw.byteLength,
    raw_sha256: digest,
    compression: "zstd",
    records: lines.length,
  };
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision: opts.revision ?? 1,
    created_at: "2026-10-29T10:00:00.000Z",
    updated_at: "2026-10-29T10:00:00.000Z",
    session: {
      workspace_id: key.workspaceId,
      machine_id: machine,
      harness: key.harness,
      session_id: key.sessionId,
      started_at: "2026-10-29T10:00:00.000Z",
    },
    writer: { delivery_instance_id: machine, plugin_version: "test" },
    raw: {
      owner: machine,
      state: "open",
      updated_at: "2026-10-29T10:00:00.000Z",
      format: "jsonl",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: raw.byteLength,
      stored_bytes: stored.byteLength,
      content_sha256: await computeContentSha256([chunk] as never),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: "2026-10-29T10:00:00.000Z", reason: "initial", first_seq: 0, opened_by: machine }],
      next_offset: raw.byteLength,
      chunks: [chunk],
      ...(opts.scrubbed === false ? {} : { scrub: { ruleset: "relayhistory-scrub", version: 1 } }),
    },
  } as unknown as ManifestV1;
}

/** A RelayHistory record-stream manifest (the Neon export shape) with its chunk stored. */
export async function recordStream(
  store: ObjectStore,
  records: Record<string, unknown>[],
  opts: { key?: typeof KEY; machine?: string; revision?: number } = {},
): Promise<ManifestV1> {
  const key = opts.key ?? KEY;
  const machine = opts.machine ?? "m-1";
  const raw = new TextEncoder().encode(records.map((r) => JSON.stringify(r)).join("\n") + "\n");
  const digest = sha(raw);
  const stored = new Uint8Array(zstdCompressSync(raw));
  await store.put(rawChunkKey(loc(key, machine), 0, 0, digest, "ndjson"), stored);
  const AT = "2026-10-30T12:00:00.000Z";
  const chunk = {
    seq: 0,
    epoch: 0,
    start_offset: 0,
    bytes: raw.byteLength,
    stored_bytes: stored.byteLength,
    sha256: digest,
    sha256_8: digest.slice(0, 8),
    stored_sha256: sha(stored),
    stored_raw_bytes: raw.byteLength,
    raw_sha256: digest,
    compression: "zstd",
    records: records.length,
  };
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision: opts.revision ?? 1,
    created_at: AT,
    updated_at: AT,
    session: {
      workspace_id: key.workspaceId,
      machine_id: machine,
      harness: key.harness,
      session_id: key.sessionId,
      started_at: "2026-10-29T10:00:00.000Z",
    },
    writer: { delivery_instance_id: "migration:mgr_test", plugin_version: "test" },
    raw: {
      owner: "migration:mgr_test",
      state: "open",
      updated_at: AT,
      format: "ndjson",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: raw.byteLength,
      stored_bytes: stored.byteLength,
      content_sha256: await computeContentSha256([chunk] as never),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "migration:mgr_test" }],
      next_offset: raw.byteLength,
      chunks: [chunk],
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: {
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
        grant_id: "mgr_test",
        plan_digest: "a".repeat(64),
        source_user_id: "user-1",
      },
      "96bulls": { user_id: "user-1" },
    },
  } as unknown as ManifestV1;
}

/** A manifest reader over an in-memory map (the pointer's manifest_key -> manifest). */
export function mapReader(manifests: Map<string, ManifestV1>) {
  return {
    provider: "fs",
    read: async (k: string) => {
      const manifest = manifests.get(k);
      return manifest ? { manifest } : null;
    },
  } as never;
}

export class FakeModel implements AnalysisModelTransport {
  requests: ModelTransportRequest[] = [];
  next: Array<ModelTransportResponse | Error> = [];
  reply: { title: string; description: string; state: SessionSummaryState } = {
    title: "Fix the flaky queue test",
    description: "Diagnosed a race in the pipeline queue test. Awaiting the lease renewal before asserting the ack count fixed it.",
    state: "completed",
  };
  async complete(request: ModelTransportRequest): Promise<ModelTransportResponse> {
    this.requests.push(request);
    const queued = this.next.shift();
    if (queued instanceof Error) throw queued;
    return (
      queued ?? {
        outputText: JSON.stringify(this.reply),
        inputTokens: 1234,
        outputTokens: 56,
        providerRequestId: "resp_test",
        model: "gpt-5.6-luna",
      }
    );
  }
}

export function testConfig(overrides: Partial<BriefConfig> = {}): BriefConfig {
  return { ...briefConfig({ SESSION_BRIEF_ENABLED: "true" }), ...overrides };
}

/** Make every wakeup due now (the debounce is exercised on its own). */
export const dueNow = (pg: PGlite) => pg.exec("UPDATE sessions.session_brief_wakeups SET due_at = now() - interval '1 second'");
