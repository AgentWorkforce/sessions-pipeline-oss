/**
 * What opens the writer gate for `backfill` and `adopted` epochs. Unknown epoch
 * reasons fail validation, so one reader that is not ready breaks every session
 * that gets one. Each reader attests for itself: at startup it proves it can
 * parse the fixture manifests, then records its build's `RELEASE_SHA`, signed
 * with a server key. Nobody can open the gate by editing configuration.
 */
import {
  hmacSha256,
  toHex,
  utf8,
} from "../../../../object-store/src/crypto.js";
import {
  MANIFEST_EPOCHS_CAPABILITY,
  computeContentSha256,
  parseManifest,
  type ManifestV1,
} from "../../../../object-store/src/manifest.js";
import type { SqlExecutor } from "./queue.js";

/** Every reader that must understand the new epochs before one is written. */
export const MANIFEST_READERS = [
  "transform",
  "session-index",
  "summaries",
  "hoopdb",
  "fetch",
] as const;
export type ManifestReader = (typeof MANIFEST_READERS)[number];

declare const __RELEASE_SHA__: string | undefined;

/**
 * This build's commit, baked in with `--define __RELEASE_SHA__=...`. It is a
 * build-time constant, not env text, so a running process cannot claim another
 * build. An unbaked build reports `unreleased`, which never attests.
 */
export const RELEASE_SHA: string =
  typeof __RELEASE_SHA__ === "string" ? __RELEASE_SHA__ : "unreleased";

/** A reader that stops attesting stops counting, so a rollback cannot linger. */
export const READINESS_TTL_MS = 7 * 24 * 60 * 60 * 1000;

export interface ReadinessRow {
  reader: string;
  capability: string;
  releaseSha: string;
  attestedAt: string;
  hmac: string;
}

export interface ReadinessStore {
  /** Each reader's newest row. */
  list(): Promise<ReadinessRow[]>;
  put(row: ReadinessRow): Promise<void>;
}

async function mac(
  key: string,
  row: Omit<ReadinessRow, "hmac">,
): Promise<string> {
  const text = JSON.stringify([
    row.reader,
    row.capability,
    row.releaseSha,
    row.attestedAt,
  ]);
  return toHex(await hmacSha256(utf8(key), text));
}

/**
 * Record that this build of `reader` is ready. `selfCheck` must parse the
 * fixture manifests and throw if it cannot: attesting without proof is the
 * failure this exists to prevent.
 */
export async function attestReader(input: {
  reader: ManifestReader;
  key: string;
  store: ReadinessStore;
  selfCheck: () => Promise<void>;
  releaseSha?: string;
  now?: Date;
}): Promise<ReadinessRow> {
  const releaseSha = input.releaseSha ?? RELEASE_SHA;
  if (releaseSha === "unreleased")
    throw new Error("an unreleased build cannot attest readiness");
  await input.selfCheck();
  const row = {
    reader: input.reader,
    capability: MANIFEST_EPOCHS_CAPABILITY,
    releaseSha,
    attestedAt: (input.now ?? new Date()).toISOString(),
  };
  const attested = { ...row, hmac: await mac(input.key, row) };
  await input.store.put(attested);
  return attested;
}

/** Readers with no valid, fresh attestation. Empty means the gate may open. */
export async function missingReaders(input: {
  /** Undefined until the credential exists: then nothing is ready. */
  key: string | undefined;
  store: ReadinessStore | undefined;
  now?: Date;
  ttlMs?: number;
}): Promise<string[]> {
  const { key, store } = input;
  if (!key || !store) return [...MANIFEST_READERS];
  const now = (input.now ?? new Date()).getTime();
  const ttl = input.ttlMs ?? READINESS_TTL_MS;
  const ready = new Set<string>();
  for (const row of await store.list()) {
    const age = now - Date.parse(row.attestedAt);
    const { hmac, ...signed } = row;
    if (
      row.capability === MANIFEST_EPOCHS_CAPABILITY &&
      row.releaseSha !== "unreleased" &&
      age >= 0 &&
      age <= ttl &&
      (await mac(key, signed)) === hmac
    )
      ready.add(row.reader);
  }
  return MANIFEST_READERS.filter((reader) => !ready.has(reader));
}

/**
 * What a reader runs before it attests: this build's validator must accept a
 * `backfill` then `resumed` session and an `adopted` cold export. An older validator rejects the reasons
 * and so never attests.
 */
export async function selfCheckManifestEpochs(): Promise<void> {
  const at = "2026-10-30T00:00:00.000Z";
  const hex = (c: string) => c.repeat(64);
  const chunk = (seq: number, epoch: number, start: number) => ({
    seq,
    epoch,
    start_offset: start,
    bytes: 10,
    stored_bytes: 5,
    sha256: hex(String(seq)),
    sha256_8: String(seq).repeat(8),
    stored_sha256: hex("a"),
    stored_raw_bytes: 10,
    raw_sha256: hex("b"),
    compression: "zstd" as const,
  });
  const epoch = (
    n: number,
    reason: ManifestV1["raw"]["epochs"][number]["reason"],
    first: number,
    by: string,
    resumes?: number,
  ) => ({
    epoch: n,
    opened_at: at,
    reason,
    first_seq: first,
    opened_by: by,
    ...(resumes === undefined ? {} : { resumes_epoch: resumes }),
  });
  const session = (
    owner: string,
    epochs: ManifestV1["raw"]["epochs"],
    chunks: ReturnType<typeof chunk>[],
  ) =>
    ({
      schema_v: 1,
      layout: "v1",
      trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
      revision: 3,
      created_at: at,
      updated_at: at,
      session: {
        workspace_id: "w",
        machine_id: "m",
        harness: "h",
        session_id: "s",
        started_at: at,
      },
      writer: { delivery_instance_id: owner, plugin_version: "self-check" },
      raw: {
        owner,
        state: "open",
        updated_at: at,
        format: "ndjson",
        compression: "zstd",
        chunk_count: chunks.length,
        raw_bytes: chunks.length * 10,
        stored_bytes: chunks.length * 5,
        epoch: epochs.length - 1,
        epochs,
        next_offset: chunks.at(-1)!.start_offset + 10,
        chunks,
      },
    }) as unknown as ManifestV1;
  const exporter = "migration:self-check";
  for (const manifest of [
    session(
      "client",
      [
        epoch(0, "initial", 0, "client"),
        epoch(1, "backfill", 1, exporter),
        epoch(2, "resumed", 2, "client", 0),
      ],
      [chunk(0, 0, 0), chunk(1, 1, 0), chunk(2, 2, 10)],
    ),
    session(
      "client",
      [epoch(0, "initial", 0, exporter), epoch(1, "adopted", 1, "client")],
      [chunk(0, 0, 0), chunk(1, 1, 0)],
    ),
  ]) {
    manifest.raw.content_sha256 = await computeContentSha256(
      manifest.raw.chunks,
    );
    await parseManifest(manifest);
  }
}

export function postgresReadinessStore(db: SqlExecutor): ReadinessStore {
  return {
    async list() {
      const { rows } = await db.query<{
        reader: string;
        capability: string;
        release_sha: string;
        attested_at: string | Date;
        hmac: string;
      }>(
        `SELECT reader, capability, release_sha, attested_at, hmac
           FROM sessions.manifest_readiness`,
      );
      return rows.map((row) => ({
        reader: row.reader,
        capability: row.capability,
        releaseSha: row.release_sha,
        attestedAt: new Date(row.attested_at).toISOString(),
        hmac: row.hmac,
      }));
    },
    async put(row) {
      await db.query(
        `INSERT INTO sessions.manifest_readiness
           (reader, capability, release_sha, attested_at, hmac)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (reader, capability) DO UPDATE
           SET release_sha = excluded.release_sha,
               attested_at = excluded.attested_at,
               hmac = excluded.hmac`,
        [row.reader, row.capability, row.releaseSha, row.attestedAt, row.hmac],
      );
    },
  };
}
