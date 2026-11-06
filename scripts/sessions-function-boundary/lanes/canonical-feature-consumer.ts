/** Lane: canonical-feature-consumer progress and finish CAS (#233). */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105030000_canonical_feature_consumer_acks.sql";

export const canonicalFeatureConsumer: LaneManifest = {
  lane: "canonical-feature-consumer",
  owner: "#233 (migration-completeness)",
  migration: MIGRATION,
  prefixes: ["sessions.canonical_feature_page_ack_", "sessions.finish_canonical_"],
  grantSources: [
    { path: MIGRATION, roles: ["bulls96_canonical_feature_producer"] },
  ],
  functions: {
    "sessions.canonical_feature_page_ack_fence()": { secdef: true, grants: [] },
    "sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text)": {
      secdef: true,
      grants: ["bulls96_canonical_feature_producer"],
    },
  },
  replaces: {},
};
