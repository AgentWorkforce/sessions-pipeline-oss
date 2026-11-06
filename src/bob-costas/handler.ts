/**
 * Bob Costas: summaries. Consumes `summary-short` and `summary-long`. Reads the
 * scrubbed chunks from the Locker (never the unscrubbed source), builds a
 * digest and summarises it.
 *
 * The summary is stored as an encrypted object next to the session's raw,
 * `summary/{short,long}.json.enc` (`summary-objects.ts`, ported from upstream),
 * under the workspace key. `sessions.session_summaries` holds only the pointer:
 * object key, ciphertext SHA-256, key id, status and revision, never the text
 * or the model name. Object first, then pointer, inside one transaction holding
 * a per-session advisory lock, so two runs never publish in different orders.
 * Idempotent by revision and raw content hash.
 */
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import {
  PipelineError,
  parseSessionKey,
  rawChunkKey,
  readAll,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
  type RawPointer,
  type StageHandler,
} from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { ctxOf } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";
import { digestRecords, type Summarizer } from "./summarizer.ts";
import {
  ShortSummaryRejected,
  clearShortSummary,
  writeShortSummary,
  type SessionDirectoryKey,
  type ShortSummaryInput,
} from "../hoopdb/session-directory-summary.ts";
import {
  clearSummaryObject,
  readSummaryObject,
  summaryObjectKey,
  writeSummaryObject,
  type SummaryKind,
  type SummaryRecord,
  type SummaryStatus,
  type WorkspaceKeyring,
} from "./summary-objects.ts";

export interface BobCostasDeps {
  db: HoopDb;
  reader: ManifestReader;
  store: ObjectStore;
  summarizer: Summarizer;
  keyring: WorkspaceKeyring;
  now?: () => Date;
  /** The session_directory short_summary projection; defaults to the SQL writer on `db`. */
  directory?: ShortSummaryDirectory;
}

/** Where Bob Costas publishes the bounded, scrubbed short summary for search (G6). */
export interface ShortSummaryDirectory {
  write(input: ShortSummaryInput): Promise<{ written: boolean }>;
  clear(key: SessionDirectoryKey): Promise<boolean>;
}

export function sqlShortSummaryDirectory(db: HoopDb): ShortSummaryDirectory {
  return {
    write: (input) => writeShortSummary(db, input),
    clear: (key) => clearShortSummary(db, key),
  };
}

/**
 * Publish (or clear) the directory short summary. Idempotent: writeShortSummary only
 * replaces an older-or-equal generated_at, clearShortSummary is a no-op when empty.
 */
async function publishShortSummary(
  directory: ShortSummaryDirectory,
  key: SessionDirectoryKey,
  ready: { summary: string; model: string; generatedAt: Date } | null,
  traceId: string,
): Promise<void> {
  if (!ready) {
    await directory.clear(key);
    return;
  }
  try {
    await directory.write({
      ...key,
      summary: ready.summary,
      source: `bob-costas/${ready.model}`,
      generatedAt: ready.generatedAt,
    });
  } catch (err) {
    if (!(err instanceof ShortSummaryRejected)) throw err;
    log.info("short summary not projected", {
      stage: "summary-short",
      reason: err.reason,
      trace_id: traceId,
    });
  }
}

/** SHA-256 of the manifest as indexed: the `summary-manifest` fence for the object. */
export function manifestSha256(m: ManifestV1): string {
  return createHash("sha256").update(JSON.stringify(m)).digest("hex");
}

interface Outcome {
  status: SummaryStatus;
  summary?: string;
  model?: string;
  tokensIn: number;
  tokensOut: number;
  records: number;
}

/**
 * The session's location, including the deployment prefix the manifest lives
 * under (`<PIPELINE_KEY_PREFIX>[/<org>]`, E3): chunks and the summary object
 * sit beside the manifest, never at the unprefixed `v1/...` key.
 */
function sessionLocation(m: ManifestV1, manifestKey: string) {
  const s = m.session;
  const prefix = parseSessionKey(manifestKey)?.prefix;
  return {
    ...(prefix ? { prefix } : {}),
    workspaceId: s.workspace_id,
    machineId: s.machine_id,
    harness: s.harness,
    sessionId: s.session_id,
    startDate: s.started_at.slice(0, 10),
  };
}

async function buildOutcome(
  kind: SummaryKind,
  m: ManifestV1,
  deps: BobCostasDeps,
  manifestKey: string,
): Promise<Outcome> {
  const none = { tokensIn: 0, tokensOut: 0, records: 0 };
  // Scrub quarantine: an unscrubbed session is never read.
  if (!m.raw.scrub) return { status: "quarantined", ...none };
  if (m.raw.format === "sqlite-rows") return { status: "unsupported", ...none };
  const s = m.session;
  const loc = sessionLocation(m, manifestKey);
  const lines: string[] = [];
  for (const chunk of m.raw.chunks) {
    const key = rawChunkKey(
      loc,
      chunk.seq,
      chunk.start_offset,
      chunk.sha256_8,
      m.raw.format,
    );
    let stored: Uint8Array;
    try {
      stored = await readAll(await deps.store.get(key));
    } catch (err) {
      if ((err as { code?: string }).code === "not_found")
        throw new PipelineError(
          "chunk_missing",
          "a listed chunk is not in object storage",
          true,
        );
      throw err;
    }
    lines.push(
      ...zstdDecompressSync(Buffer.from(stored)).toString("utf8").split("\n"),
    );
  }
  const digest = digestRecords(s.harness, lines);
  if (digest.records === 0) return { status: "empty", ...none };
  const summary = await deps.summarizer.summarize(kind, digest);
  return {
    status: "ready",
    summary: summary.text,
    model: summary.modelId,
    tokensIn: summary.tokensIn,
    tokensOut: summary.tokensOut,
    records: digest.records,
  };
}

export function bobCostasHandler(
  kind: SummaryKind,
  deps: BobCostasDeps,
): StageHandler {
  return async (message, recorder) => {
    const pointer = message.payload as RawPointer;
    if (!pointer || typeof pointer.manifestKey !== "string")
      throw new PipelineError(
        "invalid_pointer",
        `summary-${kind} payload is not a raw pointer`,
        false,
      );
    const read = await deps.reader.read(pointer.manifestKey);
    if (!read) return { status: "skipped", reason: "manifest_missing" };
    const m = read.manifest;
    const s = m.session;
    const pk = [
      message.ctx.orgId,
      s.workspace_id,
      s.harness,
      s.session_id,
      s.machine_id,
    ];
    const existing = await deps.db.query<{
      content_sha256: string;
      revision: string;
      status: string;
      key_id: string | null;
      object_key: string | null;
      object_sha256: string | null;
    }>(
      `SELECT content_sha256, revision, status, key_id, object_key, object_sha256 FROM sessions.session_summaries
        WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
      [...pk, kind],
    );
    const row = existing.rows[0];
    if (row && Number(row.revision) > m.revision)
      return { status: "skipped", reason: "stale_revision" };
    const ids = { orgId: message.ctx.orgId, workspaceId: s.workspace_id };
    // Current only if it is the same revision and content and, when ready, under the current key
    // (after a rotation the summary is re-encrypted rather than stranded on the old key) and at
    // the object key beside this manifest (a session moved under a prefix is rebuilt there).
    const currentKeyId = (await deps.keyring.currentKey(ids)).keyId;
    const expectedKey = summaryObjectKey(
      sessionLocation(m, pointer.manifestKey),
      kind,
    );
    if (
      row &&
      Number(row.revision) === m.revision &&
      row.content_sha256 === m.raw.content_sha256 &&
      (row.status !== "ready" ||
        (row.key_id === currentKeyId && row.object_key === expectedKey))
    ) {
      // The encrypted object and pointer are current, but a previous run may have
      // failed after COMMIT and before the directory write. Reconcile the directory
      // from the published object on every retry.
      if (kind === "short") {
        const directory = deps.directory ?? sqlShortSummaryDirectory(deps.db);
        const dirKey = {
          orgId: message.ctx.orgId,
          workspaceId: s.workspace_id,
          harness: s.harness,
          sessionId: canonicalManifestIdentity(m).sessionId,
        };
        if (row.status !== "ready") {
          await publishShortSummary(directory, dirKey, null, m.trace_id);
        } else {
          // Always authenticate and republish the current object, even when the row
          // already holds a summary: it may be an older revision's text left behind by
          // a failed write. writeShortSummary's generated_at fence makes this idempotent
          // and never lets an older object replace a newer one.
          // Integrity failures are permanent: clear first (no possibly stale text stays
          // searchable), then fail non-retryably so the message goes to the DLQ instead
          // of retrying forever.
          const integrity = async (message: string): Promise<never> => {
            // Ids and hashes only: the object key is a path, so it is logged as its SHA-256.
            log.warn("short summary object integrity failure; dead-lettering", {
              stage: "summary-short",
              error_code: "summary_object_integrity",
              retryable: false,
              org_id: dirKey.orgId,
              workspace_id: dirKey.workspaceId,
              harness: dirKey.harness,
              session_id: dirKey.sessionId,
              machine_id: s.machine_id,
              revision: m.revision,
              trace_id: m.trace_id,
              key_sha256: row.object_key
                ? createHash("sha256").update(row.object_key).digest("hex")
                : undefined,
            });
            await publishShortSummary(directory, dirKey, null, m.trace_id);
            throw new PipelineError("summary_object_integrity", message, false);
          };
          if (!row.object_key || !row.object_sha256)
            await integrity("a ready short summary pointer has no object");
          let published: SummaryRecord | null;
          try {
            published = await readSummaryObject({
              store: deps.store,
              keyring: deps.keyring,
              ids,
              key: row.object_key!,
              sha256: row.object_sha256!,
              kind,
              revision: m.revision,
            });
          } catch (err) {
            // Record the original failure first, so a failing clear cannot mask it. Then
            // clear (best effort) and rethrow the original error with its own retry
            // class: a transient store error retries; a non-retryable keyring error (an
            // unknown or retired key id) dead-letters instead of looping.
            const original = err instanceof PipelineError ? err : null;
            log.warn("short summary object read failed", {
              stage: "summary-short",
              error_code: original?.code ?? "summary_object_read_failed",
              retryable: original?.retryable ?? true,
              session_id: dirKey.sessionId,
              revision: m.revision,
              trace_id: m.trace_id,
            });
            try {
              await publishShortSummary(directory, dirKey, null, m.trace_id);
            } catch (clearErr) {
              log.warn("short summary clear after read failure also failed", {
                stage: "summary-short",
                error_code:
                  clearErr instanceof PipelineError
                    ? clearErr.code
                    : "summary_directory_clear_failed",
                session_id: dirKey.sessionId,
                trace_id: m.trace_id,
              });
            }
            throw err;
          }
          if (
            published?.status !== "ready" ||
            !published.summary ||
            !published.model
          ) {
            return await integrity(
              "the current short summary object is missing or cannot be authenticated",
            );
          }
          await publishShortSummary(
            directory,
            dirKey,
            {
              summary: published.summary,
              model: published.model,
              generatedAt: new Date(published.built_at),
            },
            m.trace_id,
          );
        }
      }
      return { status: "skipped", reason: "summary_current" };
    }

    const outcome = await buildOutcome(kind, m, deps, pointer.manifestKey);
    const loc = sessionLocation(m, pointer.manifestKey);
    const key = summaryObjectKey(loc, kind);
    const current = { revision: m.revision, manifestSha256: manifestSha256(m) };
    const record: SummaryRecord = {
      v: 1,
      kind,
      revision: m.revision,
      status: outcome.status,
      ...(outcome.status === "ready"
        ? {
            summary: outcome.summary!,
            model: outcome.model!,
            source_content_sha256: m.raw.content_sha256,
          }
        : {}),
      built_at: (deps.now?.() ?? new Date()).toISOString(),
      trace_id: m.trace_id,
    };
    const ctx = ctxOf(message);

    const client = await deps.db.pool.connect();
    let published: { sha256: string; keyId: string } | undefined;
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [`96bulls:summary:${key}`],
      );
      // Under the lock, a newer revision may have published meanwhile: it owns the object.
      const locked = await client.query<{ revision: string }>(
        `SELECT revision FROM sessions.session_summaries
          WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
        [...pk, kind],
      );
      if (locked.rows[0] && Number(locked.rows[0].revision) > m.revision) {
        await client.query("ROLLBACK");
        return { status: "skipped", reason: "stale_revision" };
      }
      if (outcome.status === "ready") {
        const written = await writeSummaryObject({
          store: deps.store,
          keyring: deps.keyring,
          ids,
          key,
          record,
          current,
        });
        if (written.outcome !== "written") {
          await client.query("ROLLBACK");
          return { status: "skipped", reason: "stale_revision" };
        }
        published = { sha256: written.sha256, keyId: written.keyId };
      } else {
        // Overwrite any earlier summary with a text-free record; create nothing otherwise.
        await clearSummaryObject({
          store: deps.store,
          keyring: deps.keyring,
          ids,
          key,
          record,
          current,
        });
      }
      await client.query(
        `INSERT INTO sessions.session_summaries
           (org_id, workspace_id, harness, session_id, machine_id, kind, session_uuid, trace_id, revision,
            content_sha256, status, object_key, object_sha256, key_id, tokens_in, tokens_out, generated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8::uuid,$9,$10,$11,$12,$13,$14,$15,$16, now())
         ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id, kind) DO UPDATE SET
           session_uuid = coalesce(excluded.session_uuid, sessions.session_summaries.session_uuid),
           trace_id = excluded.trace_id, revision = excluded.revision, content_sha256 = excluded.content_sha256,
           status = excluded.status, object_key = excluded.object_key, object_sha256 = excluded.object_sha256,
           key_id = excluded.key_id, tokens_in = excluded.tokens_in, tokens_out = excluded.tokens_out, generated_at = now()`,
        [
          ...pk,
          kind,
          ctx.session_uuid ?? null,
          m.trace_id,
          m.revision,
          m.raw.content_sha256,
          outcome.status,
          published ? String(key) : null,
          published?.sha256 ?? null,
          published?.keyId ?? null,
          outcome.tokensIn,
          outcome.tokensOut,
        ],
      );
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
    // G6: the short summary also feeds the tenant-scoped directory search. The writer
    // re-scrubs and bounds it and records provenance; a refused summary is logged by
    // reason only (the encrypted object above is unaffected), and any other summary
    // status clears a stale one.
    if (kind === "short") {
      const directory = deps.directory ?? sqlShortSummaryDirectory(deps.db);
      const dirKey = {
        orgId: message.ctx.orgId,
        workspaceId: s.workspace_id,
        harness: s.harness,
        sessionId: canonicalManifestIdentity(m).sessionId,
      };
      await publishShortSummary(
        directory,
        dirKey,
        outcome.status === "ready"
          ? {
              summary: outcome.summary!,
              model: outcome.model!,
              generatedAt: new Date(record.built_at),
            }
          : null,
        m.trace_id,
      );
    }
    // Ids, counts and statuses only: never the summary or the model's output.
    recorder.log.info("summary written", {
      kind,
      status: outcome.status,
      tokens_in: outcome.tokensIn,
      tokens_out: outcome.tokensOut,
    });
    log.info("bob costas summary", {
      stage: `summary-${kind}`,
      kind,
      status: outcome.status,
      model_id: outcome.model,
      tokens_in: outcome.tokensIn,
      tokens_out: outcome.tokensOut,
      trace_id: m.trace_id,
      revision: m.revision,
      session_uuid: ctx.session_uuid,
      count: outcome.records,
    });
  };
}
