/**
 * Read-only canonical feature journal: the ONLY input the producer trusts for
 * publication + retirement ordering. The #230 owner-only install transaction
 * owns seed/allocation/append; this adapter cannot mutate that authority.
 */
import { canonicalImageKey } from "./canonical-image-key.ts";
import type {
  CanonicalJournalEntry,
  CanonicalJournalPort,
  CanonicalScope,
  CanonicalSourceAuthority,
  CanonicalPointerSet,
} from "./canonical-version.ts";

export interface SqlQuery {
  query<T>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

type Row = {
  revision: string | number;
  kind: string;
  org_id: string;
  workspace_id: string;
  // jsonb columns: drivers return parsed objects, fixtures may give strings.
  scope: unknown;
  schema_generation: string;
  image_key: string;
  source_hash: string;
  prior_keyset_hash: string | null;
  source_authority: unknown;
  prior_source_authority: unknown;
  pointer_set: unknown;
};
/** jsonb decode: a real driver returns the parsed object; a raw string parses. */
const jsonb = <T>(v: unknown): T => {
  if (typeof v === "string") return JSON.parse(v) as T;
  if (v && typeof v === "object") return v as T;
  throw new Error("journal_row_invalid");
};
const HS1 = /^hs1:[a-f0-9]{64}:[a-f0-9]{64}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX64 = /^[a-f0-9]{64}$/;
// Canonical positive integer — nonzero, no leading zeros, writer bounds:
// epoch/revision at most 19 digits (bigint-safe).
const DEC = /^[1-9][0-9]{0,18}$/;
// Count may be zero, at most 6 digits (writer ABI bound).
const DEC0 = /^(0|[1-9][0-9]{0,5})$/;
/** Fail-closed row validation — every field + cross-bound shape checked. */
const exactKeys = (o: unknown, keys: string[]): boolean =>
  !!o && typeof o === "object" &&
  new Set(Object.keys(o)).size === keys.length &&
  keys.every((k) => k in (o as Record<string, unknown>));
const validAuth = (a: CanonicalSourceAuthority | undefined) =>
  !!a && exactKeys(a, ["authorityId", "epoch", "fingerprint"]) &&
  UUID.test(a.authorityId) && DEC.test(a.epoch) &&
  HEX64.test(a.fingerprint);
const validPtr = (p: CanonicalPointerSet | undefined) =>
  !!p && exactKeys(p, ["authorityId", "hash", "count"]) &&
  UUID.test(p.authorityId) && HEX64.test(p.hash) &&
  DEC0.test(p.count);
const validScope = (s: CanonicalScope | undefined) =>
  !!s && exactKeys(s, ["orgId", "workspaceId", "harness", "sessionId",
                       "generation"]) &&
  typeof s.orgId === "string" && typeof s.workspaceId === "string" &&
  typeof s.harness === "string" && typeof s.sessionId === "string" &&
  HEX64.test(s.generation);
const toEntry = (r: Row): CanonicalJournalEntry => {
  if (r.kind !== "publish" && r.kind !== "retire")
    throw new Error("journal_kind_invalid");
  if (!HS1.test(r.image_key)) throw new Error("journal_image_key_invalid");
  const scope = jsonb<CanonicalScope>(r.scope);
  const pointerSet = jsonb<CanonicalPointerSet>(r.pointer_set);
  if (!validScope(scope) || !validPtr(pointerSet))
    throw new Error("journal_row_invalid");
  if (!HEX64.test(r.source_hash) || !HEX64.test(r.schema_generation) ||
      !DEC.test(String(r.revision)) ||
      (r.prior_keyset_hash !== null && !HEX64.test(r.prior_keyset_hash)))
    throw new Error("journal_row_invalid");
  const hs1Auth = r.image_key.split(":")[3]!;
  const hs1Gen = r.image_key.split(":")[2]!;
  const hs1Scope = r.image_key.split(":")[1]!;
  // The #230 digest: sha256 of the unit-separator-joined identity fields
  // prefixed 'history-scope-v1' — NOT JSON.stringify(scope).
  if (canonicalImageKey(scope, hs1Auth) !== r.image_key ||
      hs1Gen !== scope.generation || hs1Gen !== r.schema_generation ||
      r.org_id !== scope.orgId || r.workspace_id !== scope.workspaceId)
    throw new Error("journal_row_invalid");
  if (r.kind === "retire") {
    const prior = jsonb<CanonicalSourceAuthority>(r.source_authority);
    // Retire: prior identity in source_authority; prior_keyset_hash must be
    // present and equal to the retired pointer set's hash;
    // prior_source_authority must be NULL (the 610 CHECK shape).
    if (!validAuth(prior) || prior.authorityId !== pointerSet.authorityId ||
        prior.authorityId !== hs1Auth ||
        r.prior_keyset_hash === null ||
        r.prior_keyset_hash !== pointerSet.hash ||
        r.prior_source_authority !== null ||
        r.source_hash !== prior.fingerprint)
      throw new Error("journal_row_invalid");
  } else {
    const src = jsonb<CanonicalSourceAuthority>(r.source_authority);
    // Publish: authority + hs1 + source_hash == fingerprint; prior fields
    // both-or-neither (a replacement carries prior authority + keyset hash).
    if (!validAuth(src) || src.authorityId !== pointerSet.authorityId ||
        src.authorityId !== hs1Auth ||
        r.source_hash !== src.fingerprint ||
        ((r.prior_keyset_hash === null) !==
         (r.prior_source_authority === null)))
      throw new Error("journal_row_invalid");
    if (r.prior_source_authority) {
      const prior = jsonb<CanonicalSourceAuthority>(r.prior_source_authority);
      if (!validAuth(prior) ||
          prior.fingerprint === src.fingerprint ||
          prior.authorityId === src.authorityId)
        throw new Error("journal_row_invalid");
    }
  }
  return r.kind === "retire"
    ? { kind: "retire", revision: String(r.revision),
        imageKey: r.image_key, schemaGeneration: r.schema_generation,
        sourceHash: r.source_hash, priorKeysetHash: r.prior_keyset_hash ?? "",
        scope,
        // 610 CHECK: prior authority lives in source_authority on retire
        // (prior_source_authority is always NULL for a retire row).
        priorSourceAuthority:
          jsonb<CanonicalSourceAuthority>(r.source_authority),
        pointerSet }
    : { kind: "publish", revision: String(r.revision),
        imageKey: r.image_key, schemaGeneration: r.schema_generation,
        sourceHash: r.source_hash, scope,
        sourceAuthority:
          jsonb<CanonicalSourceAuthority>(r.source_authority),
        ...(r.prior_source_authority
          ? { priorSourceAuthority:
              jsonb<CanonicalSourceAuthority>(r.prior_source_authority) }
          : {}),
        ...(r.prior_keyset_hash ? { priorKeysetHash: r.prior_keyset_hash } : {}),
        pointerSet };
};

export class CanonicalJournalStore implements CanonicalJournalPort {
  constructor(readonly db: SqlQuery) {}
  async entries(after: string, limit: number) {
    if (!/^(0|[1-9][0-9]{0,18})$/.test(after) ||
        !Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("journal_invalid_page");
    const { rows } = await this.db.query<Row>(
      "SELECT revision,kind,org_id,workspace_id,scope,schema_generation," +
        " image_key,source_hash,prior_keyset_hash,source_authority," +
        " prior_source_authority,pointer_set" +
        " FROM sessions.canonical_feature_journal" +
        " WHERE revision > $1::bigint AND kind <> 'seed'" +
        " ORDER BY revision LIMIT $2",
      [after, limit + 1]);
    const hasMore = rows.length > limit;
    const entries = rows.slice(0, limit).map(toEntry);
    return {
      entries,
      nextRevision: entries.length ? entries[entries.length - 1]!.revision : after,
      hasMore,
    };
  }
}
