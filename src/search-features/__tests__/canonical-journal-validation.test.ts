import { describe, expect, test } from "bun:test";
import { CanonicalJournalStore } from "../canonical-journal.ts";
import { canonicalImageKey } from "../canonical-image-key.ts";

const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: A };
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
const HS1 = canonicalImageKey(SCOPE, UID);
const base = {
  revision: "10", kind: "publish", org_id: "o", workspace_id: "w",
  scope: SCOPE, schema_generation: A, image_key: HS1,
  source_hash: A, prior_keyset_hash: null,
  source_authority: AUTH, prior_source_authority: null,
  pointer_set: PTR,
};
// A store over a fake that returns ONE row (the decoder validates it).
const store = (row: Record<string, unknown>) =>
  new CanonicalJournalStore({
    query: async <T>(_t: string, _p?: unknown[]) =>
      ({ rows: [row] as T[] }),
  });
const dec = (row: Record<string, unknown>) =>
  store(row).entries("0", 10);

describe("journal decoder — cross-field fail-closed negatives", () => {
  test("valid row decodes", async () => {
    const out = await dec(base);
    expect(out.entries[0]!.kind).toBe("publish");
  });
  const neg = (name: string, patch: Record<string, unknown>) =>
    test(name, () =>
      expect(dec({ ...base, ...patch })).rejects.toThrow("journal_row_invalid"));
  neg("hs1 digest mismatch", { image_key: HS1.replace("a", "b") });
  neg("hs1 generation mismatch", { schema_generation: "b".repeat(64) });
  neg("routing org mismatch", { org_id: "other" });
  neg("routing ws mismatch", { workspace_id: "other" });
  neg("scope generation != hs1", {
    scope: { ...SCOPE, generation: "b".repeat(64) } });
  neg("source_hash != fingerprint", { source_hash: "b".repeat(64) });
  neg("pointer authority != source", {
    pointer_set: { ...PTR, authorityId: "87654321-4321-4321-4321-210987654321" } });
  neg("epoch not positive", {
    source_authority: { ...AUTH, epoch: "0" } });
  neg("epoch leading zero", {
    source_authority: { ...AUTH, epoch: "01" } });
  neg("epoch too long", {
    source_authority: { ...AUTH, epoch: "1".repeat(20) } });
  neg("count too long", { pointer_set: { ...PTR, count: "1".repeat(7) } });
  neg("extra key in scope", { scope: { ...SCOPE, x: "y" } });
  neg("extra key in pointer set", { pointer_set: { ...PTR, x: "y" } });
  neg("publish: prior present without keyset", {
    prior_source_authority: AUTH });
  test("unknown kind", () =>
    expect(dec({ ...base, kind: "update" }))
      .rejects.toThrow("journal_kind_invalid"));
});

// Retire + replacement rows: the 610 CHECK shape and cross-field fences.
const PRIOR = { authorityId: "87654321-4321-4321-4321-210987654321",
  epoch: "1", fingerprint: "b".repeat(64) };
const PRIOR_PTR = { authorityId: PRIOR.authorityId,
  hash: "d".repeat(64), count: "1" };
const RET = {
  revision: "9", kind: "retire", org_id: "o", workspace_id: "w",
  scope: SCOPE, schema_generation: A,
  image_key: canonicalImageKey(SCOPE, PRIOR.authorityId),
  source_hash: PRIOR.fingerprint, prior_keyset_hash: PRIOR_PTR.hash,
  source_authority: PRIOR, prior_source_authority: null,
  pointer_set: PRIOR_PTR,
};
const REPL = {
  ...base, revision: "10",
  prior_keyset_hash: PRIOR_PTR.hash,
  prior_source_authority: PRIOR,
};
describe("journal decoder — retire + replacement negatives", () => {
  test("valid retire decodes", async () => {
    const out = await dec(RET);
    expect(out.entries[0]!.kind).toBe("retire");
  });
  test("valid replacement decodes", async () => {
    const out = await dec(REPL);
    expect(out.entries[0]!.kind).toBe("publish");
  });
  const neg = (name: string, row: Record<string, unknown>) =>
    test(name, () =>
      expect(dec(row)).rejects.toThrow("journal_row_invalid"));
  neg("retire: NULL prior_keyset_hash",
    { ...RET, prior_keyset_hash: null });
  neg("retire: prior_keyset_hash != pointerSet.hash",
    { ...RET, prior_keyset_hash: "e".repeat(64) });
  neg("retire: non-NULL prior_source_authority",
    { ...RET, prior_source_authority: PRIOR });
  neg("retire: source_hash != prior fingerprint",
    { ...RET, source_hash: A });
  neg("retire: prior authority != pointer authority",
    { ...RET, source_authority: AUTH });
  neg("retire: prior authority != hs1 suffix",
    { ...RET, image_key: HS1 });
  neg("replacement: prior fingerprint == new",
    { ...REPL, prior_source_authority: { ...PRIOR, fingerprint: A } });
  neg("replacement: prior authority == new",
    { ...REPL, prior_source_authority: AUTH });
});
