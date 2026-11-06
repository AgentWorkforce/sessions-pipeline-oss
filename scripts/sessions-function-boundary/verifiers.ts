import type { VerifierTarget } from "./types.ts";

const quoted = (sigs: readonly string[]) => sigs.map((k) => " '" + k + "'").join(",\n");

/**
 * Role verifiers whose approved-function arrays are derived from the composed ACL. A lane that
 * introduces a role with its own verifier (e.g. bulls96_project_events, #237) adds it here.
 */
export const VERIFIERS: readonly VerifierTarget[] = [
  {
    role: "bulls96_session_ingest",
    path: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
  },
  {
    role: "session_directory_search_worker",
    path: "supabase/bootstrap/session_directory_search_worker_verify.sql",
    pattern: /v_approved text\[\] := ARRAY\[[\s\S]*?\];/,
    render: (sigs) => "v_approved text[] := ARRAY[\n" + quoted(sigs) + "\n ];",
  },
  // SLOT (#237): bulls96_project_events -> supabase/bootstrap/canonical_project_events_verify.sql, e.g.
  // { role: "bulls96_project_events", path: "supabase/bootstrap/canonical_project_events_verify.sql",
  //   template: "supabase/bootstrap/session_ingest_verify.sql", pattern: <the v_allowed pattern>, render: <same>,
  //   transform: (t) => t.replaceAll("bulls96_session_ingest", "bulls96_project_events").replaceAll("session ingest", "canonical project events") },
];
