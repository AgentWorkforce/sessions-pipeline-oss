/**
 * "Did my upload land?" for one trace id, end to end.
 *
 * Folds a trace's `pipeline_events` into one entry per stage (latest status,
 * attempts, first/last time, last error) and a verdict an agent can act on:
 *
 *   - `indexed`: the raw manifest is in object storage and its pointer is in the
 *     sessions index. This is "landed". Transform state is reported beside it and
 *     never changes it, because raw is canonical and the transform is derived.
 *   - `stored`: the object store reported the manifest, the pointer is pending.
 *   - `uploaded`: the client recorded a successful put, no storage event yet.
 *   - `deleted`: the manifest was deleted and the index applied the deletion.
 *   - `failed`: a required stage is dead-lettered (or failed permanently).
 *   - `in_progress`: events exist, nothing above applies yet.
 *   - `not_found`: no events for this trace in the caller's workspace.
 */
import type { PipelineEvent } from "./events.js";
import type { PipelineErrorInfo } from "./errors.js";
import {
  PIPELINE_STAGES,
  type PipelineStage,
  type PipelineStatus,
} from "./trace.js";

export type TraceVerdict =
  | "indexed"
  | "deleted"
  | "stored"
  | "uploaded"
  | "failed"
  | "in_progress"
  | "not_found";

export interface StageSummary {
  stage: PipelineStage;
  status: PipelineStatus | "not_seen";
  attempts: number;
  events: number;
  firstAt?: string;
  lastAt?: string;
  lastError?: PipelineErrorInfo;
}

export interface TraceSummary {
  traceId: string;
  /** Only the oldest events were read; the verdict may be behind the trace. */
  truncated: boolean;
  verdict: TraceVerdict;
  landed: boolean;
  /** One sentence for an agent or a CLI to print verbatim. */
  answer: string;
  sessionId?: string;
  machineId?: string;
  stages: Record<PipelineStage, StageSummary>;
  /**
   * `deliver.put` per destination (`attrs.destination`). Only the canonical
   * object storage destination decides the verdict; the legacy RelayHistory
   * destination is reported here and never makes a session count as landed.
   */
  destinations: Record<string, StageSummary>;
  events: PipelineEvent[];
}

export const OBJECT_STORAGE_DESTINATION = "object-storage";

function destinationOf(event: PipelineEvent) {
  const destination = event.attrs?.destination;
  return typeof destination === "string" && destination
    ? destination
    : OBJECT_STORAGE_DESTINATION;
}

/** The unit of work a client put is about (`attrs.chunk`, `seq` or `offset`). */
function putUnitOf(event: PipelineEvent) {
  const unit = event.attrs?.chunk ?? event.attrs?.seq ?? event.attrs?.offset;
  return unit === undefined ? "" : String(unit);
}

/** Worst first: the status a destination reports while any put is not `ok`. */
const UNFINISHED: readonly PipelineStatus[] = [
  "dead_lettered",
  "error",
  "retrying",
  "started",
  "replayed",
  "skipped",
];

/**
 * One destination's puts, folded per unit (chunk) first: the destination is `ok`
 * only when every unit's latest outcome is, so one chunk's success never hides
 * another's failure.
 */
function aggregatePuts(units: StageSummary[]): StageSummary {
  const all = emptyStage("deliver.put");
  for (const unit of units) {
    all.events += unit.events;
    all.attempts = Math.max(all.attempts, unit.attempts);
    if (unit.firstAt && (!all.firstAt || unit.firstAt < all.firstAt))
      all.firstAt = unit.firstAt;
    if (unit.lastAt && (!all.lastAt || unit.lastAt > all.lastAt))
      all.lastAt = unit.lastAt;
  }
  const worst = UNFINISHED.map((status) =>
    units.filter((unit) => unit.status === status),
  ).find((matching) => matching.length);
  if (!worst) {
    all.status = units.length ? "ok" : "not_seen";
    return all;
  }
  all.status = worst[0]!.status;
  const failed = worst.filter((unit) => unit.lastError);
  if (failed.length) all.lastError = failed.at(-1)!.lastError;
  return all;
}

/** The manifest a work key (`<object key>@<version>`) is a version of. */
function manifestOf(workKey: string) {
  const at = workKey.lastIndexOf("@");
  return at < 0 ? workKey : workKey.slice(0, at);
}

function emptyStage(stage: PipelineStage): StageSummary {
  return { stage, status: "not_seen", attempts: 0, events: 0 };
}

function fold(entry: StageSummary, event: PipelineEvent) {
  entry.events += 1;
  entry.attempts = Math.max(entry.attempts, event.attempt);
  entry.firstAt ??= event.at;
  entry.lastAt = event.at;
  // `ok` is sticky: a late `retrying` from a redelivered duplicate does not undo it.
  if (entry.status !== "ok" || event.status === "ok")
    entry.status = event.status;
  if (event.error) entry.lastError = event.error;
}

/**
 * The newest unit of work. Revisions count per manifest, so they only order
 * versions of one manifest (the work key minus `@<version>`); a version
 * without a revision is newer if first seen later. Between manifests, the one
 * whose chosen version first appeared last decides: another machine's
 * manifest at revision 1 is not older than this one's revision 5, and this
 * one's next version after it is newer again.
 */
function newestWork(
  items: StageSummary[],
  revisions: Map<StageSummary, number>,
  workKeys: Map<StageSummary, string>,
): StageSummary {
  // `items` is in first-seen order: a version's position is when it appeared.
  const seen = new Map(items.map((item, index) => [item, index]));
  const byManifest = new Map<string, StageSummary>();
  for (const item of items) {
    const manifest = manifestOf(workKeys.get(item) ?? "");
    const current = byManifest.get(manifest);
    const ra = current && revisions.get(current);
    const rb = revisions.get(item);
    byManifest.set(
      manifest,
      current && ra !== undefined && rb !== undefined && ra !== rb && ra > rb
        ? current
        : item,
    );
  }
  // Between manifests, by when each one's chosen version appeared (not when
  // the manifest first did): A@v2 after B@v1 is newer than B@v1.
  return [...byManifest.values()].reduce((a, b) =>
    seen.get(b)! > seen.get(a)! ? b : a,
  );
}

/** Stages whose failure means the upload did not land. Transform and fetch are not among them. */
const REQUIRED: readonly PipelineStage[] = [
  "ingest",
  "deliver.put",
  "object-landed",
  "session-indexed",
];

export function summarizeTrace(
  traceId: string,
  events: PipelineEvent[],
  truncated = false,
): TraceSummary {
  // The table has no unique key (it is partitioned by `at`), so a redelivered
  // transition can be stored twice; the first recording stands.
  const seen = new Set<string>();
  const ordered = [...events]
    .sort((a, b) => a.at.localeCompare(b.at))
    .filter((event) => {
      if (seen.has(event.idempotencyKey)) return false;
      seen.add(event.idempotencyKey);
      return true;
    });
  const stages = Object.fromEntries(
    PIPELINE_STAGES.map((stage) => [stage, emptyStage(stage)]),
  ) as Record<PipelineStage, StageSummary>;
  // A Map, not an object: destination names come from clients, and a name
  // like `constructor` or `__proto__` must never reach a shared prototype.
  const destinations = new Map<string, Map<string, StageSummary>>();
  // Fold each unit of work (`attrs.work_key`, e.g. one manifest version) on its
  // own, so `ok` is sticky only against redeliveries of the same work: a newer
  // version that dead-letters is not hidden by an older one that indexed.
  const items = new Map<PipelineStage, Map<string, StageSummary>>();
  const revisions = new Map<StageSummary, number>();
  const chunkSkips = new Set<StageSummary>();
  const workKeys = new Map<StageSummary, string>();
  const deletes = new Set<StageSummary>();
  for (const event of ordered) {
    if (!stages[event.stage]) continue;
    if (event.stage === "deliver.put") {
      // Per destination, per chunk; the stage itself is the object storage
      // destination's aggregate (below).
      const destination = destinationOf(event);
      const units =
        destinations.get(destination) ?? new Map<string, StageSummary>();
      destinations.set(destination, units);
      const unit = putUnitOf(event);
      const summary = units.get(unit) ?? emptyStage("deliver.put");
      units.set(unit, summary);
      fold(summary, event);
      continue;
    }
    const byWork = items.get(event.stage) ?? new Map<string, StageSummary>();
    items.set(event.stage, byWork);
    const workKey =
      typeof event.attrs?.work_key === "string" ? event.attrs.work_key : "";
    const item = byWork.get(workKey) ?? emptyStage(event.stage);
    byWork.set(workKey, item);
    workKeys.set(item, workKey);
    fold(item, event);
    if (Number.isSafeInteger(event.attrs?.revision))
      revisions.set(item, event.attrs.revision as number);
    if (event.attrs?.deleted === true) deletes.add(item);
    if (event.status === "skipped" && event.attrs?.reason === "chunk")
      chunkSkips.add(item);
    else chunkSkips.delete(item);
  }
  const latestWork = new Map<PipelineStage, StageSummary>();
  for (const [stage, byWork] of items) {
    const all = [...byWork.values()];
    // The newest unit of work decides. A chunk notification is not a unit of
    // session work (only its manifest is), so it decides only when it is all
    // there is; any other skip (a disabled transform) is a real, current state.
    const deciding = all.filter((item) => !chunkSkips.has(item));
    const latest = newestWork(
      deciding.length ? deciding : all,
      revisions,
      workKeys,
    );
    latestWork.set(stage, latest);
    stages[stage] = {
      ...latest,
      events: all.reduce((sum, item) => sum + item.events, 0),
      firstAt: all.reduce(
        (min, item) => (item.firstAt! < min ? item.firstAt! : min),
        latest.firstAt!,
      ),
      lastAt: all.reduce(
        (max, item) => (item.lastAt! > max ? item.lastAt! : max),
        latest.lastAt!,
      ),
    };
  }

  const destinationSummaries = new Map(
    [...destinations].map(([name, units]) => [
      name,
      aggregatePuts([...units.values()]),
    ]),
  );
  stages["deliver.put"] =
    destinationSummaries.get(OBJECT_STORAGE_DESTINATION) ??
    emptyStage("deliver.put");

  // An index success only counts if it is for the newest manifest work. With
  // revisions on both sides they decide; otherwise only an exact work-key
  // match does. Timestamps cannot: index runs on its own queue, so an older
  // version's index can finish after a newer version's storage event.
  // A skipped chunk is not manifest work: while it is all the storage stage
  // has seen, there is no manifest for an index success to be behind.
  const latestStorage = [latestWork.get("object-landed")].find(
    (item) => item && !chunkSkips.has(item),
  );
  const indexOk = [...(items.get("session-indexed")?.values() ?? [])].filter(
    (item) => item.status === "ok",
  );
  // Per index success: by revision when it and the storage work both have
  // one and are versions of the same manifest (revisions count per manifest,
  // so another machine's revision 1 says nothing about this one's), otherwise
  // only an exact work-key match. One legacy success without a revision must
  // not hide a newer one that has it.
  const coversLatest = (item: StageSummary) =>
    revisions.has(item) &&
    revisions.has(latestStorage!) &&
    manifestOf(workKeys.get(item)!) ===
      manifestOf(workKeys.get(latestStorage!)!)
      ? revisions.get(item)! >= revisions.get(latestStorage!)!
      : workKeys.get(item) === workKeys.get(latestStorage!);
  const indexBehind = Boolean(
    latestStorage && indexOk.length && !indexOk.some(coversLatest),
  );
  const ok = (stage: PipelineStage) =>
    stages[stage].status === "ok" &&
    !(stage === "session-indexed" && indexBehind);
  const deadRequired = REQUIRED.find(
    (stage) =>
      stages[stage].status === "dead_lettered" ||
      (stages[stage].status === "error" &&
        stages[stage].lastError?.retryable === false),
  );
  let verdict: TraceVerdict;
  if (!ordered.length) verdict = "not_found";
  else if (ok("session-indexed"))
    // The index's own newest success decides deleted vs indexed: the storage
    // event is best effort and may be missing or behind.
    verdict = deletes.has(newestWork(indexOk, revisions, workKeys))
      ? "deleted"
      : "indexed";
  else if (deadRequired) verdict = "failed";
  else if (ok("object-landed")) verdict = "stored";
  else if (ok("deliver.put")) verdict = "uploaded";
  else verdict = "in_progress";

  const first = ordered[0];
  return {
    traceId,
    verdict,
    landed: verdict === "indexed",
    truncated,
    answer: `${truncated ? "Partial: this trace has more events than one lookup reads, so later stages may be missing. " : ""}${answer(verdict, stages, deadRequired, indexBehind)}`,
    ...(first?.sessionId ? { sessionId: first.sessionId } : {}),
    ...(first?.machineId ? { machineId: first.machineId } : {}),
    stages,
    destinations: Object.fromEntries(destinationSummaries),
    events: ordered,
  };
}

function answer(
  verdict: TraceVerdict,
  stages: Record<PipelineStage, StageSummary>,
  deadRequired: PipelineStage | undefined,
  indexBehind = false,
): string {
  const transform = stages["file-normalize"];
  const transformNote =
    transform.status === "not_seen" || transform.status === "ok"
      ? transform.status === "ok"
        ? " Normalized output is ready."
        : ""
      : transform.status === "skipped"
        ? " Normalize transform is not enabled."
        : ` Normalize transform is ${transform.status.replace("_", "-")}${transform.lastError ? ` (${transform.lastError.code})` : ""}; the raw upload is unaffected.`;
  switch (verdict) {
    case "indexed":
      return `Yes. The raw session is stored and indexed (session-indexed at ${stages["session-indexed"].lastAt}).${transformNote}`;
    case "deleted":
      return `Deleted. The manifest was removed from object storage and the session's index pointer was removed (session-indexed at ${stages["session-indexed"].lastAt}).`;
    case "stored":
      return `Partly. The raw session is in object storage; the index pointer is ${indexBehind ? "not yet updated for the latest manifest" : describe(stages["session-indexed"])}.${transformNote}`;
    case "uploaded":
      return `Not confirmed yet. The client finished the upload; no storage event has been recorded.`;
    case "failed":
      return `No. ${deadRequired} ${describe(stages[deadRequired!])}${stages[deadRequired!].lastError ? ` (${stages[deadRequired!].lastError!.code})` : ""}; replay its dead letter once the cause is fixed.`;
    case "in_progress":
      return `Not yet. The upload is in progress.`;
    case "not_found":
      return `No events are recorded for this trace in this workspace.`;
  }
}

function describe(stage: StageSummary): string {
  if (stage.status === "not_seen") return "not started";
  if (stage.status === "retrying")
    return `retrying (attempt ${stage.attempts})`;
  return stage.status.replace("_", "-");
}
