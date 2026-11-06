/** Shared canonical S3 reader/projection. The live CH writer and read Worker
 * invoke this same implementation; neither maintains a shadow event mapping. */
import { canonicalDirectoryValues } from "./canonical-directory-values.ts";
import { createHash } from "node:crypto";
import {
  CONVERGENCE_EVENT_KIND,
  projectRelayhistorySession,
  deliveryMachine,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {
  normalizeRelayhistoryRecords,
  isLegacyRelayhistoryRecord,
  parseRelayhistoryRecord,
  relayhistoryExportProvenance,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { PipelineError } from "../../packages/pipeline/src/lib/pipeline/errors.ts";
import {
  chunkKeys,
  parseSessionKey,
  isManifestRest,
  isPublicationManifestRest,
  parseUploadGenerationPrefix,
  type ManifestV1,
  type ObjectStore,
} from "../../packages/object-store/src/index.ts";
import {
  readCapped,
  type ManifestReader,
} from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import {
  toIndexRow,
  type IndexRow,
  type NeonEventRow,
} from "../session-pipeline/vendor/search-index/rows.ts";
import {
  directoryCatalogFromProjection,
  type DirectoryCatalog,
  type CatalogProjectionRow,
} from "../hoopdb/session-directory-catalog.ts";
import {
  directoryEventsFromProjection,
  type DirectoryEvents,
  type DirectoryEventInput,
} from "../hoopdb/session-directory-events.ts";
import {
  canonicalManifestIdentity,
  manifestUserId as admittedManifestUserId,
} from "../shared/manifest-identity.ts";
interface SessionScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}
interface SearchIndexDeps {
  reader: ManifestReader;
  store: ObjectStore;
  prefixFor: (orgId: string) => string;
  decompress: (stored: Uint8Array, maxBytes: number) => Uint8Array;
}
interface SearchIndexLimits {
  maxScopeBytes: number;
  maxScopeEvents: number;
  maxRows: number;
  insertBatch: number;
  /**
   * Read-artifact projection only; legacy CH writer does not construct display values.
   * "required" (artifact producer): a directory_display_* refusal makes the projection
   * incomplete with that reason code. "optional" (live raw read): the display values are
   * omitted and the code is reported in `displayOmitted`; the read itself still succeeds.
   */
  directoryValues?: "required" | "optional";
}
const MAX_CHUNK_BYTES = 16 * 1024 * 1024;
const SEP = "\u001f";
const keyOf = (i: {
  machineId: string;
  source: string;
  kind: string;
  eventId: string;
}) => [i.machineId, i.source, i.kind, i.eventId].join(SEP);
const sha256 = (bytes: Uint8Array) =>
  createHash("sha256").update(bytes).digest("hex");

/** The user a manifest's events belong to: the export grant's source user, else the 96bulls extension. */
export function manifestUserId(manifest: ManifestV1): string {
  try {
    return relayhistoryExportProvenance(manifest).sourceUserId;
  } catch {
    return admittedManifestUserId(manifest) ?? "";
  }
}

/** Every canonical record of one manifest, digests verified. Throws a PipelineError with a code. */
export async function readManifestRecords(
  manifest: ManifestV1,
  store: ObjectStore,
  prefix: string,
  decompress: (stored: Uint8Array, maxOutputLength: number) => Uint8Array,
  maxRecords = Number.POSITIVE_INFINITY,
): Promise<RelayhistoryStreamRecord[]> {
  const rows: RelayhistoryStreamRecord[] = [];
  for await (const row of streamManifestRecords(manifest, store, prefix, decompress, maxRecords))
    rows.push(row);
  return rows;
}

/** Bounded chunk-by-chunk reader; proofs need not retain full session payloads. */
export async function* streamManifestRecords(
  manifest: ManifestV1,
  store: ObjectStore,
  prefix: string,
  decompress: (stored: Uint8Array, maxOutputLength: number) => Uint8Array,
  maxRecords = Number.POSITIVE_INFINITY,
): AsyncGenerator<RelayhistoryStreamRecord> {
  if (manifest.raw.format !== "ndjson")
    throw new PipelineError(
      "raw_format_unsupported",
      "only ndjson raw is indexed",
      false,
    );
  let records = 0;
  for (const { entry, key } of chunkKeys(manifest, { prefix })) {
    if (
      entry.stored_bytes > MAX_CHUNK_BYTES ||
      (entry.stored_raw_bytes ?? entry.bytes) > MAX_CHUNK_BYTES
    )
      throw new PipelineError(
        "chunk_too_large",
        "a raw chunk exceeds the index limit",
        false,
      );
    let stored: Uint8Array | null;
    try {
      stored = await readCapped(await store.get(key), entry.stored_bytes);
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code === "not_found")
        throw new PipelineError(
          "chunk_missing",
          "a listed raw chunk is missing",
          true,
        );
      throw new PipelineError(
        "chunk_unavailable",
        "a raw chunk read failed",
        true,
      );
    }
    if (
      !stored ||
      stored.byteLength !== entry.stored_bytes ||
      sha256(stored) !== entry.stored_sha256
    )
      throw new PipelineError(
        "chunk_digest_mismatch",
        "a raw chunk has the wrong stored digest",
        false,
      );
    let raw: Uint8Array;
    try {
      raw = decompress(stored, entry.stored_raw_bytes ?? MAX_CHUNK_BYTES);
    } catch (error) {
      if (error instanceof PipelineError) throw error;
      throw new PipelineError(
        "chunk_decompress_failed",
        "a raw chunk is not valid zstd",
        false,
      );
    }
    if (entry.raw_sha256 && sha256(raw) !== entry.raw_sha256)
      throw new PipelineError(
        "chunk_digest_mismatch",
        "a raw chunk has the wrong raw digest",
        false,
      );
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
        raw,
      );
    } catch {
      throw new PipelineError(
        "chunk_not_utf8",
        "a raw chunk is not UTF-8",
        false,
      );
    }
    for (const line of text.split("\n"))
      if (line) {
        if (records >= maxRecords)
          throw new PipelineError(
            "scope_too_large",
            "actual canonical record count exceeds scope limit",
            false,
          );
        records += 1;
        yield parseRelayhistoryRecord(line);
      }
  }
}

type Json = Record<string, unknown>;

const str = (v: unknown): string | null =>
  typeof v === "string" ? v : v === null || v === undefined ? null : String(v);

/** A projected `convergence_events` row as the Neon reader's row shape, so `toIndexRow` maps it. */
export function projectedToNeonRow(
  row: Json,
  scope: SessionScope,
  userId: string,
): NeonEventRow {
  const taskRef =
    row.task_ref && typeof row.task_ref === "object"
      ? (row.task_ref as Json)
      : {};
  const base: Omit<NeonEventRow, "fingerprint"> = {
    org_id: scope.orgId,
    workspace_id: scope.workspaceId,
    machine_id: String(row.machine_id),
    user_id: str(row.user_id) ?? userId,
    source: String(row.source),
    session_id: String(row.session_id),
    event_id: String(row.event_id),
    kind: String(row.kind),
    type: str(row.type) ?? String(row.kind),
    ts_text: String(row.ts),
    actor_name: str(row.actor_name),
    actor_role: str(row.actor_role),
    subagent_id: str(row.subagent_id),
    project_id: str(row.project_id),
    task_title: str(row.task_title),
    task_ref: JSON.stringify(taskRef),
    task_ref_id: str(taskRef.id),
    tags: JSON.stringify(Array.isArray(row.tags) ? row.tags : []),
    files_touched: JSON.stringify(
      Array.isArray(row.files_touched) ? row.files_touched : [],
    ),
    tool_name: str(row.tool_name),
    tool_status: str(row.tool_status),
    delivery_record_id: str(row.delivery_record_id),
    content: str(row.content),
    ingested_text: String(row.ingested_at ?? row.ts),
  };
  // Covers every stored column but ts and workspace_id (compared separately,
  // as in the Neon fingerprint). The `s3` prefix keeps it apart from Neon's.
  const { ts_text: _ts, workspace_id: _ws, ...covered } = base;
  const fingerprint = `s3${createHash("sha256").update(JSON.stringify(covered)).digest("hex").slice(0, 14)}`;
  return { ...base, fingerprint };
}

export interface ScopeRead {
  records: RelayhistoryStreamRecord[];
  projectionRows: Array<{ table: string; row: Record<string, unknown> }>;
  rows: IndexRow[];
  manifests: number;
  complete: boolean;
  /** Too large to index whole: nothing was read. */
  partial: boolean;
  /** Highest manifest revision of the scope, recorded with its index state. */
  revision: number;
  reasons: string[];
  catalog: DirectoryCatalog;
  directorySessionId: string | null;
  events: DirectoryEvents | null;
  /** directory_display_* code when optional display values were omitted. */
  displayOmitted?: string | null;
  sources: DirectoryEventInput["sources"];
}

export async function readCanonicalScope(
  scope: SessionScope,
  manifestKeys: string[],
  deps: SearchIndexDeps,
  limits: SearchIndexLimits,
  version: number,
  expectedCanonicalId?: string,
): Promise<ScopeRead> {
  const decompress = deps.decompress;
  const prefix = deps.prefixFor(scope.orgId);
  const reasons: string[] = [];
  let complete = true;
  type Part = {
    manifest: ManifestV1;
    records: RelayhistoryStreamRecord[];
    userId: string;
  };
  const parts: Part[] = [];
  const readable: ManifestV1[] = [];
  const manifestKeysByBody = new Map<ManifestV1, string>();
  for (const key of manifestKeys) {
    const read = await deps.reader.read(key);
    if (!read) {
      // A live pointer whose manifest is gone: its delete event is in flight.
      complete = false;
      reasons.push("manifest_missing");
      continue;
    }
    const manifest = read.manifest;
    // The body must describe the key it was read from (as the pointer stage
    // requires): chunk keys are built from the body, rows are stamped with the scope.
    const s = manifest.session;
    const at = parseSessionKey(key);
    if (
      !at || !isManifestRest(at.rest) ||
      ((parseUploadGenerationPrefix(at.prefix)?.base ?? at.prefix ?? "") !== prefix) ||
      (isPublicationManifestRest(at.rest) && !parseUploadGenerationPrefix(at.prefix)) ||
      s.workspace_id !== scope.workspaceId ||
      s.harness !== scope.harness ||
      s.session_id !== at.sessionId ||
      (!expectedCanonicalId && s.session_id !== scope.sessionId) ||
      s.machine_id !== at.machineId
    ) {
      complete = false;
      reasons.push("manifest_key_mismatch");
      continue;
    }
    // Unscrubbed raw is quarantined and never read (manifest spec, "Scrubbing").
    // Not reading it is not knowing it: the pass deletes nothing.
    if (!manifest.raw.scrub) {
      complete = false;
      reasons.push("manifest_unscrubbed");
      continue;
    }
    if (
      expectedCanonicalId &&
      canonicalManifestIdentity(manifest).sessionId !== expectedCanonicalId
    ) {
      complete = false;
      reasons.push("canonical_session_mismatch");
      continue;
    }
    readable.push(manifest);
    manifestKeysByBody.set(manifest, key);
  }

  // Size the whole session from its manifests before reading a chunk. A
  // session past either bound is not read at all: it is marked partial (see
  // indexSearchScope), never folded from a subset of its records, which could
  // index a record's older revision whose newer one sits in an unread chunk.
  const revision = Math.max(0, ...readable.map((m) => m.revision));
  const bytes = readable.reduce(
    (n, m) =>
      n +
      m.raw.chunks.reduce(
        (b, c) => b + Math.max(c.bytes, c.stored_raw_bytes ?? 0),
        0,
      ),
    0,
  );
  const events = readable.reduce(
    (n, m) => n + m.raw.chunks.reduce((b, c) => b + (c.records ?? 0), 0),
    0,
  );
  if (bytes > limits.maxScopeBytes || events > limits.maxScopeEvents)
    return {
      rows: [],
      manifests: readable.length,
      complete: false,
      partial: true,
      revision,
      reasons: [...new Set([...reasons, "scope_too_large"])],
      catalog: directoryCatalogFromProjection([]),
      directorySessionId: null,
      events: null,
      sources: [],
      projectionRows: [],
      records: [],
    };

  let parsedRecords = 0;
  for (const manifest of readable) {
    try {
      const records = await readManifestRecords(
        manifest,
        deps.store,
        parseSessionKey(manifestKeysByBody.get(manifest)!)?.prefix ?? prefix,
        decompress,
        limits.maxScopeEvents - parsedRecords,
      );
      parsedRecords += records.length;
      parts.push({ manifest, records, userId: manifestUserId(manifest) });
    } catch (error) {
      if (error instanceof PipelineError && error.code === "scope_too_large")
        return {
          rows: [],
          manifests: parts.length,
          complete: false,
          partial: true,
          revision,
          reasons: [...new Set([...reasons, "scope_too_large"])],
          catalog: directoryCatalogFromProjection([]),
          directorySessionId: null,
          events: null,
          sources: [],
          projectionRows: [],
          records: [],
        };
      if (error instanceof PipelineError && error.retryable) throw error;
      complete = false;
      reasons.push(
        error instanceof PipelineError ? error.code : "manifest_unreadable",
      );
    }
  }

  // Fold every copy together; a conflicting copy (same revision, different
  // bytes) falls back to folding each manifest on its own, without deletes.
  let canonicalRecords: RelayhistoryStreamRecord[] = [];
  let groups: { records: RelayhistoryStreamRecord[]; part: Part }[];
  try {
    const all = normalizeRelayhistoryRecords(parts.flatMap((p) => p.records));
    canonicalRecords = all;
    const owner = new Map<string, Part>();
    for (const part of parts)
      for (const r of part.records)
        if (!owner.has(`${r.origin_id}${SEP}${r.record_id}`))
          owner.set(`${r.origin_id}${SEP}${r.record_id}`, part);
    // Direct convergence_event lines are their machine's own rows: project them
    // with the manifest that carried them. Delivery records fold across copies.
    const direct = new Map<Part, RelayhistoryStreamRecord[]>();
    const activity: RelayhistoryStreamRecord[] = [];
    for (const r of all) {
      if (r.kind === CONVERGENCE_EVENT_KIND) {
        const part = owner.get(`${r.origin_id}${SEP}${r.record_id}`)!;
        direct.set(part, [...(direct.get(part) ?? []), r]);
      } else activity.push(r);
    }
    groups = [...direct].map(([part, records]) => ({ records, part }));
    if (activity.length > 0 && parts[0])
      groups.push({ records: activity, part: parts[0] });
  } catch (error) {
    complete = false;
    reasons.push(error instanceof PipelineError ? error.code : "fold_failed");
    groups = [];
    for (const part of parts) {
      try {
        groups.push({
          records: normalizeRelayhistoryRecords(part.records),
          part,
        });
      } catch {
        reasons.push("manifest_fold_failed");
      }
    }
  }

  const ownerByEvent = new Map<string, { userId: string; sessionId: string }>();
  for (const part of parts)
    for (const record of part.records) {
      if (
        record.kind === CONVERGENCE_EVENT_KIND ||
        isLegacyRelayhistoryRecord(record)
      )
        continue;
      const identity = JSON.stringify([
        deliveryMachine(scope.workspaceId, record.origin_id),
        record.record_id,
      ]);
      const prior = ownerByEvent.get(identity);
      if (prior?.userId && part.userId && prior.userId !== part.userId) {
        complete = false;
        reasons.push("event_owner_conflict");
      } else if (part.userId || prior === undefined)
        ownerByEvent.set(identity, {
          userId: part.userId,
          sessionId: canonicalManifestIdentity(part.manifest).sessionId,
        });
    }
  const byKey = new Map<string, IndexRow>();
  const metadataByKey = new Map<string, Json>();
  const catalogRows: CatalogProjectionRow[] = [];
  for (const { records, part } of groups) {
    let projected;
    try {
      projected = projectRelayhistorySession(records, {
        workspaceId: scope.workspaceId,
        source: scope.harness,
        sessionId: canonicalManifestIdentity(part.manifest).sessionId,
        machineId: canonicalManifestIdentity(part.manifest).machineId,
        userId: part.userId,
      });
    } catch (error) {
      complete = false;
      reasons.push(
        error instanceof PipelineError ? error.code : "projection_failed",
      );
      continue;
    }
    for (const { table, row } of projected.rows) {
      if (table !== "convergence_events") {
        catalogRows.push({ table, row });
        continue;
      }
      // Delivery projection is tenant-free; restore the record's admitted owner,
      // not the uploader/first-manifest owner of a multi-machine logical scope.
      const owner = ownerByEvent.get(
        JSON.stringify([row.machine_id, row.delivery_record_id]),
      );
      const canonicalRow = {
        ...row,
        session_id: row.delivery_record_id
          ? (owner?.sessionId ?? row.session_id)
          : row.session_id,
        user_id: row.user_id ?? owner?.userId ?? part.userId,
      };
      const indexRow = toIndexRow(
        projectedToNeonRow(canonicalRow, scope, part.userId),
        version,
      );
      const identity = keyOf({
        machineId: indexRow.machine_id,
        source: indexRow.source,
        kind: indexRow.kind,
        eventId: indexRow.event_id,
      });
      byKey.set(identity, indexRow);
      metadataByKey.set(identity, {
        ...canonicalRow,
        _index_fingerprint: indexRow.fingerprint,
      });
    }
  }
  const directoryIds = new Set(
    parts.map(({ manifest }) => canonicalManifestIdentity(manifest).sessionId),
  );
  if (directoryIds.size > 1) {
    complete = false;
    reasons.push("canonical_session_mismatch");
  }
  let metadata: DirectoryEvents | null = null;
  try {
    metadata = directoryEventsFromProjection([...metadataByKey.values()]);
  } catch {
    complete = false;
    reasons.push("directory_event_metadata_invalid");
  }
  // Both locators were selected from metadataByKey. Fetch only those rows:
  // display materialization never scans/copies the full transcript a second time.
  let displayValues: ReturnType<typeof canonicalDirectoryValues> = [];
  let displayOmitted: string | null = null;
  if (limits.directoryValues && metadata) {
    try {
      displayValues = canonicalDirectoryValues({orgId:scope.orgId,workspaceId:scope.workspaceId},metadata,
        [...new Set([metadata.openingEvent,metadata.projectEvent].flatMap(event=>event ? [keyOf({machineId:event.machine_id,source:event.source,kind:event.kind,eventId:event.event_id})] : []))]
          .map(key=>metadataByKey.get(key)!));
    } catch (error) {
      const code = error instanceof Error && /^directory_display_[a-z_]+$/.test(error.message) ? error.message : null;
      if (!code) throw error;
      reasons.push(code);
      if (limits.directoryValues === "required") complete = false;
      else displayOmitted = code;
    }
  }
  return {
    records: canonicalRecords,
    projectionRows: [
      ...catalogRows,
      ...[...metadataByKey.values()].map(
        ({ _index_fingerprint: _fingerprint, ...row }) => ({
          table: "convergence_events",
          row,
        }),
      ),
      ...displayValues,
    ],
    rows: [...byKey.values()],
    manifests: parts.length,
    complete,
    partial: false,
    revision,
    reasons: [...new Set(reasons)],
    catalog: directoryCatalogFromProjection(catalogRows),
    directorySessionId: directoryIds.size === 1 ? [...directoryIds][0]! : null,
    events: metadata,
    displayOmitted,
    sources: parts.map(({ manifest }) => ({
      manifestKey: manifestKeysByBody.get(manifest)!,
      revision: String(manifest.revision),
    })),
  };
}
