/** Lane: dormant brief-document publication and its owner activation switch (#233). */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105024000_session_brief_documents.sql";

export const briefDocuments: LaneManifest = {
  lane: "brief-documents",
  owner: "#233 (migration-completeness)",
  migration: MIGRATION,
  prefixes: [
    "sessions.ack_session_brief_documents(",
    "sessions.backfill_session_brief_documents(",
    "sessions.claim_session_brief_documents(",
    "sessions.queue_session_brief_document(",
    "sessions.retry_session_brief_documents(",
    "sessions.session_brief_document_",
    "sessions.set_session_brief_documents_live(",
  ],
  grantSources: [
    { path: MIGRATION, roles: ["service_role", "session_directory_search_worker"] },
  ],
  functions: {
    "sessions.ack_session_brief_documents(uuid,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.backfill_session_brief_documents(integer)": { secdef: true, grants: ["service_role"] },
    "sessions.claim_session_brief_documents(uuid,integer)": { secdef: true, grants: ["service_role"] },
    "sessions.queue_session_brief_document(jsonb)": { secdef: true, grants: [] },
    "sessions.retry_session_brief_documents(uuid,text)": { secdef: true, grants: ["service_role"] },
    "sessions.session_brief_document_authority(text,text[],text[])": {
      secdef: true,
      grants: ["session_directory_search_worker"],
    },
    "sessions.session_brief_document_changed()": { secdef: true, grants: [] },
    "sessions.session_brief_document_json(sessions.session_directory,boolean)": { secdef: false, grants: [] },
    "sessions.set_session_brief_documents_live(boolean)": { secdef: true, grants: [] },
  },
  replaces: {},
};
