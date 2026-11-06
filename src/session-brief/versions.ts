import { SESSION_SUMMARY_PROMPT_VERSION, SESSION_SUMMARY_VERSION } from "./engine/summary-contract.ts";

/** The brief's prompt, relayhistory's `session-summary-v3` (two-sentence description and state). */
export const SESSION_BRIEF_PROMPT_VERSION = SESSION_SUMMARY_PROMPT_VERSION;
/** relayhistory's is `session-summary-v2:workspace-v1`; this input adapter is the 96Bulls one. */
export const SESSION_BRIEF_ENGINE_VERSION = `${SESSION_SUMMARY_PROMPT_VERSION}:96bulls-v1`;
export const SESSION_BRIEF_SCHEMA_VERSION = SESSION_SUMMARY_VERSION;
/** Producer prefix for session_directory.short_summary_source (precedence in writeShortSummary). */
export { BRIEF_SUMMARY_SOURCE_PREFIX as SESSION_BRIEF_SOURCE_PREFIX } from "../hoopdb/session-directory-summary.ts";
