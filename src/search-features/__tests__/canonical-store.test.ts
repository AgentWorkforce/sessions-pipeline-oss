import { describe, expect, test } from "bun:test";
import { CanonicalJournalStore } from "../canonical-journal.ts";
import { canonicalImageKey } from "../canonical-image-key.ts";

const HASH = "a".repeat(64);
const AUTHORITY_ID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: HASH };
const row = (revision: string, kind: "publish" | "retire" = "publish") => ({
  revision,
  kind,
  org_id: SCOPE.orgId,
  workspace_id: SCOPE.workspaceId,
  scope: SCOPE,
  schema_generation: HASH,
  image_key: canonicalImageKey(SCOPE, AUTHORITY_ID),
  source_hash: HASH,
  prior_keyset_hash: kind === "retire" ? "b".repeat(64) : null,
  source_authority: { authorityId: AUTHORITY_ID, epoch: "1", fingerprint: HASH },
  prior_source_authority: null,
  pointer_set: { authorityId: AUTHORITY_ID,
    hash: kind === "retire" ? "b".repeat(64) : "c".repeat(64), count: "1" },
});
const fakeDb = (table: ReturnType<typeof row>[]) => ({
  query: async <T>(_text: string, params: unknown[] = []) => ({
    rows: table
      .filter((r) => BigInt(r.revision) > BigInt(String(params[0])))
      .sort((a, b) => Number(BigInt(a.revision) - BigInt(b.revision)))
      .slice(0, Number(params[1])) as T[],
  }),
});

describe("canonical journal read store", () => {
  test("maps postgres jsonb objects and string pointer counts", async () => {
    const out = await new CanonicalJournalStore(fakeDb([
      row("10"), row("20", "retire"),
    ])).entries("0", 10);
    expect(out.entries.map((entry) => entry.kind)).toEqual(["publish", "retire"]);
    expect(out.entries[0]!.pointerSet.count).toBe("1");
    expect(out.nextRevision).toBe("20");
    expect(out.hasMore).toBe(false);
  });

  test("uses limit+1 so an exact final page is complete", async () => {
    const store = new CanonicalJournalStore(fakeDb([row("10"), row("20"), row("30")]));
    const first = await store.entries("0", 2);
    expect(first.entries.map((entry) => entry.revision)).toEqual(["10", "20"]);
    expect(first.hasMore).toBe(true);
    const final = await store.entries("20", 1);
    expect(final.entries.map((entry) => entry.revision)).toEqual(["30"]);
    expect(final.hasMore).toBe(false);
  });

  test("rejects invalid pages and untrusted journal shapes", async () => {
    const store = new CanonicalJournalStore(fakeDb([row("10")]));
    await expect(store.entries("-1", 10)).rejects.toThrow("journal_invalid_page");
    await expect(store.entries("0", 0)).rejects.toThrow("journal_invalid_page");
    const invalid = row("10");
    invalid.pointer_set = { authorityId: AUTHORITY_ID,
      hash: "not-a-hash", count: "1" };
    await expect(new CanonicalJournalStore(fakeDb([invalid])).entries("0", 10))
      .rejects.toThrow("journal_row_invalid");
  });
});
