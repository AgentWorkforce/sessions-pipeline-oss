import type { LaneManifest } from "../types.ts";
import { briefDocuments } from "./brief-documents.ts";
import { canonicalFeatureConsumer } from "./canonical-feature-consumer.ts";
import { canonicalProjectEvents } from "./canonical-project-events.ts";
import { ingestHistory } from "./ingest-history.ts";
import { readApi } from "./read-api.ts";

/** Every lane the unified generator composes. Order does not affect the output. */
export const LANES: readonly LaneManifest[] = [briefDocuments, ingestHistory, readApi, canonicalProjectEvents, canonicalFeatureConsumer];
