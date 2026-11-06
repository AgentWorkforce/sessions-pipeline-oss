import { semanticCheckpoint } from "./semantic-copy-protocol.ts";
import {
  readFeatureJournalPage,
  readFeatureBaselinePage,
  readFeatureSnapshots,
  type FeatureReader,
} from "./source.ts";
import {
  publishEventFeatureSnapshots,
  EventFeatureError,
} from "./event-publisher.ts";
import type { BriefFeatureCheckpoint } from "./brief-cycle.ts";
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
export type EventFeatureCheckpoint = BriefFeatureCheckpoint;
export const initialEventFeatureCheckpoint = (): EventFeatureCheckpoint => ({
  phase: "baseline",
  baselineH: null,
  baselineCursor: null,
  checkpoint: "0",
  observedRevision: "0",
  polled: false,
});
/** Runtime must durably CAS this capture BEFORE invoking a copying page. */
export async function captureEventFeatureBaseline(
  db: FeatureReader,
): Promise<EventFeatureCheckpoint> {
  const h = (await readFeatureJournalPage(db, "event", "0", 1))
    .observedRevision;
  return {
    ...initialEventFeatureCheckpoint(),
    baselineH: h,
    checkpoint: h,
    observedRevision: h,
  };
}
export async function copyEventFeaturePage(
  deps: { db: FeatureReader; ch: ClickHouseClient; signal?: AbortSignal },
  previous: EventFeatureCheckpoint,
  batch = 100,
): Promise<{
  next: EventFeatureCheckpoint;
  published: number;
  complete: boolean;
}> {
  if (!Number.isInteger(batch) || batch < 1 || batch > 200)
    throw new EventFeatureError("checkpoint_invalid");
  try {
    semanticCheckpoint(previous);
  } catch {
    throw new EventFeatureError("checkpoint_invalid");
  }
  if (previous.baselineH === null)
    throw new EventFeatureError("baseline_not_captured");
  deps.signal?.throwIfAborted();
  if (previous.phase === "baseline") {
    const page = await readFeatureBaselinePage(
      deps.db,
      "event",
      previous.baselineCursor,
      batch,
    );
    await publishEventFeatureSnapshots(deps, page.rows, {
      baselineH: previous.baselineH,
    });
    return {
      published: page.rows.length,
      complete: false,
      next: {
        ...previous,
        phase: page.nextCursor === null ? "journal" : "baseline",
        baselineCursor: page.nextCursor,
        polled: false,
      },
    };
  }
  const page = await readFeatureJournalPage(
    deps.db,
    "event",
    previous.checkpoint,
    batch,
  );
  const snapshots = await readFeatureSnapshots(deps.db, page.changes);
  await publishEventFeatureSnapshots(deps, snapshots);
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
