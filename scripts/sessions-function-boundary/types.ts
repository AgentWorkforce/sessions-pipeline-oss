/**
 * The typed manifest one function-boundary lane checks in. A lane is one migration that owns a
 * set of `sessions.*` functions. The unified generator (scripts/sync-sessions-function-boundary.ts)
 * composes every lane into the shared artifacts: src/hoopdb/sessions-function-acl.json, the
 * Supabase boundary wrapper, migrate-guards.ts and the per-role verifier SQL.
 *
 * The manifest is the reviewed contract. The generator re-parses the lane's SQL and refuses
 * (exit 1) when the SQL and the manifest disagree, so a new function, grant or SECURITY DEFINER
 * change is a manifest edit that review sees, never a silent regeneration.
 */
export interface OwnedFunction {
  /** The definition is SECURITY DEFINER (listed in the boundary's reviewed SECDEF set). */
  readonly secdef: boolean;
  /** Roles granted EXECUTE by this lane's grant sources, in emitted order (compared as a set). */
  readonly grants: readonly string[];
}

/** A function another migration owns that this lane CREATE OR REPLACEs (its ACL is kept). */
export interface ReplacedFunction {
  readonly secdef: boolean;
  /** Migration holding the expected pre-lane body; the lane's rollback restores it from there. */
  readonly original: string;
}

export interface GrantSource {
  readonly path: string;
  /** Only GRANT EXECUTE ... TO <one of these> statements are read from this file. */
  readonly roles: readonly string[];
  /** An absent optional source contributes no grants (e.g. a not-yet-landed migration). */
  readonly optional?: boolean;
}

export interface LaneContext {
  read(path: string): Promise<string>;
  /** The lane migration's text. */
  readonly sql: string;
  /** Every function the SQL defines, in definition order. */
  readonly defined: readonly string[];
  readonly lane: LaneManifest;
}

export interface LaneManifest {
  /** Stable lane name: "ingest-history", "read-api", "canonical-project-events", ... */
  readonly lane: string;
  /** Who fills and owns it (PR or agent). */
  readonly owner: string;
  /** The SQL authority. null marks an EMPTY SLOT: no functions, prefixes or outputs yet. */
  readonly migration: string | null;
  /**
   * ACL keys starting with one of these belong to this lane: a key here that the manifest no
   * longer lists is removed (a dropped function cannot linger). No two lanes may overlap.
   */
  readonly prefixes: readonly string[];
  readonly grantSources: readonly GrantSource[];
  readonly functions: Readonly<Record<string, OwnedFunction>>;
  readonly replaces: Readonly<Record<string, ReplacedFunction>>;
  /** Lane-specific generated files (e.g. a rollback derived from the same SQL). */
  extraOutputs?(ctx: LaneContext): Promise<Record<string, string>>;
}

/** A role verifier whose approved-function array is derived from the final ACL. */
export interface VerifierTarget {
  readonly role: string;
  /** The generated verifier file. */
  readonly path: string;
  /** Derive it from another verifier's text instead of rewriting it in place (e.g. a new role cloned from the ingest verifier). */
  readonly template?: string;
  transform?(text: string): string;
  readonly pattern: RegExp;
  render(signatures: readonly string[]): string;
}
