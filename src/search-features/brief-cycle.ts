/** One bounded page. Durable lease/claim and CAS persistence belong to the
 * runtime caller; an in-memory checkpoint must never authorize public search. */
import {
  readFeatureJournalPage,
  readFeatureBaselinePage,
  readFeatureSnapshots,
  featureVersion,
  type FeatureReader,
} from "./source.ts";
import {
  publishBriefFeatureSnapshots,
  BriefFeatureError,
} from "./brief-publisher.ts";
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
export interface BriefFeatureCheckpoint {
  phase: "baseline" | "journal";
  baselineH: string | null;
  baselineCursor: string | null;
  checkpoint: string;
  observedRevision: string;
  polled: boolean;
}
export const initialBriefFeatureCheckpoint = (): BriefFeatureCheckpoint => ({
  phase: "baseline",
  baselineH: null,
  baselineCursor: null,
  checkpoint: "0",
  observedRevision: "0",
  polled: false,
});
export async function copyBriefFeaturePage(
  deps: { db: FeatureReader; ch: ClickHouseClient; signal?: AbortSignal },
  previous: BriefFeatureCheckpoint,
  batch = 100,
): Promise<{
  next: BriefFeatureCheckpoint;
  published: number;
  complete: boolean;
}> {
  if (
    !Number.isInteger(batch) ||
    batch < 1 ||
    batch > 200 ||
    !previous ||
    !["baseline", "journal"].includes(previous.phase) ||
    typeof previous.polled !== "boolean" ||
    !(
      previous.baselineCursor === null ||
      typeof previous.baselineCursor === "string"
    )
  )
    throw new BriefFeatureError("checkpoint_invalid");
  if (
    previous.phase === "baseline" &&
    (previous.polled ||
      (previous.baselineH === null &&
        (previous.baselineCursor !== null ||
          previous.checkpoint !== "0" ||
          previous.observedRevision !== "0")) ||
      (previous.baselineH !== null &&
        previous.checkpoint !== previous.baselineH))
  )
    throw new BriefFeatureError("checkpoint_invalid");
  featureVersion(previous.checkpoint);
  featureVersion(previous.observedRevision);
  if (previous.baselineH !== null) featureVersion(previous.baselineH);
  if (
    BigInt(previous.checkpoint) > BigInt(previous.observedRevision) ||
    (previous.phase === "journal" &&
      (previous.baselineH === null ||
        previous.baselineCursor !== null ||
        BigInt(previous.checkpoint) < BigInt(previous.baselineH)))
  )
    throw new BriefFeatureError("checkpoint_invalid");
  deps.signal?.throwIfAborted();
  if (previous.phase === "baseline") {
    // Runtime must persist captured H under its lease BEFORE any CH write.
    // Recapturing H after a partial baseline crash can skip an intervening delete.
    if (previous.baselineH === null)
      throw new BriefFeatureError("baseline_uninitialized");
    const h = previous.baselineH;
    const page = await readFeatureBaselinePage(
      deps.db,
      "brief",
      previous.baselineCursor,
      batch,
    );
    await publishBriefFeatureSnapshots(deps, page.rows, { baselineH: h });
    return {
      published: page.rows.length,
      complete: false,
      next: {
        phase: page.nextCursor === null ? "journal" : "baseline",
        baselineH: h,
        baselineCursor: page.nextCursor,
        checkpoint: h,
        observedRevision: h,
        polled: false,
      },
    };
  }
  const page = await readFeatureJournalPage(
    deps.db,
    "brief",
    previous.checkpoint,
    batch,
  );
  const snapshots = await readFeatureSnapshots(deps.db, page.changes);
  await publishBriefFeatureSnapshots(deps, snapshots);
  return {
    published: snapshots.length,
    complete: page.nextRevision === page.observedRevision,
    next: {
      ...previous,
      checkpoint: page.nextRevision,
      observedRevision: page.observedRevision,
      polled: true,
    },
  };
}
