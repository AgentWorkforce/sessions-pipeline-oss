/**
 * Plan entry v2 (one backfill epoch on a live manifest) against
 * docs/contracts/relayhistory-export-plan-entry-v2.vectors.json, shared with
 * relayhistory-cloud, relayauth (issuer) and the E1 exporter.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  backfillSha256,
  baseReadIdentityOf,
  canonicalJson,
  exportPlanDigest,
  exportPlanDigestV2,
  migrationWorkKey,
  parsePlanEntryV2,
  parseUploadIdentity,
  planEntryV2Sha256,
  uploadIdentityV2Of,
  type PlanEntry,
  type PlanEntryV2,
  type UploadIdentityV2,
} from "../src/lib/pipeline/migration-assertions.js";
import { objectLandedWorkKey } from "../src/lib/pipeline/stages.js";

const read = (name: string) =>
  JSON.parse(
    readFileSync(
      fileURLToPath(
        new URL(`../../../docs/contracts/${name}`, import.meta.url).href,
      ),
      "utf8",
    ),
  );
const v2 = read("relayhistory-export-plan-entry-v2.vectors.json") as {
  grant_id: string;
  exported_at: string;
  backfill_entry: PlanEntryV2;
  backfill_entry_canonical_json: string;
  backfill_entry_sha256: string;
  plan: { canonical_json: string; plan_digest: string };
  base: UploadIdentityV2["base"];
  upload_identity: UploadIdentityV2;
  base_read_identity: Record<string, unknown>;
  legacy_plan_digests: {
    absent: string;
    empty: string;
    v1_vectors_plan_digest: string;
  };
  work_key: string;
  appended_chunks: { seq: number; epoch: number }[];
  appended_epoch: { epoch: number; first_seq: number; opened_by: string };
};
const v1 = read("relayhistory-export-plan-entry-v1.vectors.json") as {
  entries: { entry: PlanEntry }[];
  plan: { plan_digest: string };
};

const issuer = read(
  "relayhistory-export-plan-entry-v2-issuer.vectors.json",
) as {
  legacy: {
    absent: { plan_digest: string };
    empty: { plan_digest: string; stored_plan_has_backfills_key: boolean };
  };
  v2_plan: { plan_digest: string; stored_plan_canonical_json: string };
  backfill_entry_stored: PlanEntryV2;
  backfill_entry_sha256: string;
  base_read_meta_read: Record<string, unknown>;
  commit_meta_upload: UploadIdentityV2;
};

describe("export plan entry contract v2 (backfill epoch)", () => {
  it("reproduces the entry, backfill digest, plan digest, identity and work key", async () => {
    const e = v2.backfill_entry;
    expect(canonicalJson(e)).toBe(v2.backfill_entry_canonical_json);
    expect(await planEntryV2Sha256(e)).toBe(v2.backfill_entry_sha256);
    expect(await backfillSha256(e.chunks)).toBe(e.backfill_sha256);
    expect(parsePlanEntryV2(v2.backfill_entry_canonical_json)).toEqual(e);
    const entries = v1.entries.map((x) => x.entry);
    expect(
      await exportPlanDigestV2({
        grantId: v2.grant_id,
        exportedAt: v2.exported_at,
        entries,
        backfills: [e],
      }),
    ).toBe(v2.plan.plan_digest);
    // Without backfills the plan digest is exactly v1's (v1 unchanged).
    expect(
      await exportPlanDigestV2({
        grantId: v2.grant_id,
        exportedAt: v2.exported_at,
        entries,
        backfills: [],
      }),
    ).toBe(v1.plan.plan_digest);
    expect(
      await exportPlanDigest({
        grantId: v2.grant_id,
        exportedAt: v2.exported_at,
        entries,
      }),
    ).toBe(v1.plan.plan_digest);
    expect(await uploadIdentityV2Of(e, v2.plan.plan_digest, v2.base)).toEqual(
      v2.upload_identity,
    );
    expect(parseUploadIdentity(v2.upload_identity)).toEqual(v2.upload_identity);
    expect(migrationWorkKey(v2.upload_identity)).toBe(v2.work_key);
    // The base-read capability's meta.read, and the legacy conditional.
    expect(await baseReadIdentityOf(e, v2.plan.plan_digest)).toEqual(
      v2.base_read_identity,
    );
    expect(parseUploadIdentity(v2.base_read_identity)).toEqual(
      v2.base_read_identity,
    );
    expect(v2.legacy_plan_digests.absent).toBe(v1.plan.plan_digest);
    expect(v2.legacy_plan_digests.empty).toBe(v1.plan.plan_digest);
    expect(v2.appended_chunks.map((c) => c.seq)).toEqual(
      e.chunks.map((_, i) => v2.base.chunk_count + i),
    );
    expect(v2.appended_epoch).toMatchObject({
      epoch: v2.base.epoch + 1,
      first_seq: v2.base.chunk_count,
      opened_by: `migration:${v2.grant_id}`,
    });
  });

  it("reproduces the issuer vectors of the spec of record (main f67f741)", async () => {
    const entries = v1.entries.map((x) => x.entry);
    expect(issuer.legacy.absent.plan_digest).toBe(
      await exportPlanDigest({
        grantId: v2.grant_id,
        exportedAt: v2.exported_at,
        entries,
      }),
    );
    expect(issuer.legacy.empty.plan_digest).toBe(
      await exportPlanDigestV2({
        grantId: v2.grant_id,
        exportedAt: v2.exported_at,
        entries,
        backfills: [],
      }),
    );
    expect(issuer.legacy.empty.stored_plan_has_backfills_key).toBe(false);
    expect(issuer.v2_plan.plan_digest).toBe(
      await exportPlanDigestV2({
        grantId: v2.grant_id,
        exportedAt: v2.exported_at,
        entries,
        backfills: [issuer.backfill_entry_stored],
      }),
    );
    expect(await planEntryV2Sha256(issuer.backfill_entry_stored)).toBe(
      issuer.backfill_entry_sha256,
    );
    expect(
      await baseReadIdentityOf(
        issuer.backfill_entry_stored,
        issuer.v2_plan.plan_digest,
      ),
    ).toEqual(issuer.base_read_meta_read);
    expect(
      await uploadIdentityV2Of(
        issuer.backfill_entry_stored,
        issuer.v2_plan.plan_digest,
        v2.base,
      ),
    ).toEqual(issuer.commit_meta_upload);
  });

  it("refuses malformed v2 identities and entries", () => {
    const u = v2.upload_identity;
    for (const bad of [
      { ...u, extra: 1 },
      { ...u, kind: "cold" },
      { ...u, source_user_id: `relay_${u.source_user_id}` },
      { ...u, base: { ...u.base, owner: "migration:mgr_x" } },
      { ...u, base: { ...u.base, revision: 0 } },
      { ...u, base: { ...u.base, extra: 1 } },
      { ...u, base: { ...u.base, content_sha256: "x" } },
      { ...u, base: { ...u.base, revision: Number.MAX_SAFE_INTEGER } },
      {
        ...u,
        base: { ...u.base, chunk_count: Number.MAX_SAFE_INTEGER - 4095 },
      },
      { ...u, base: { ...u.base, epoch: 4294967295 } },
      { ...u, base: { ...u.base, revision: 1.5 } },
      { ...u, base: { ...u.base, chunk_count: "4" } },
    ])
      expect(parseUploadIdentity(bad)).toBeUndefined();
    const e = v2.backfill_entry;
    for (const bad of [
      { ...e, format: "jsonl" },
      { ...e, chunks: [] },
      {
        ...e,
        chunks: [{ ...e.chunks[0]!, start_offset: 5 }, ...e.chunks.slice(1)],
      },
      { ...e, raw_bytes: e.raw_bytes + 1 },
      { ...e, chunks: e.chunks.map((c) => ({ ...c, seq: 0 })) },
      // records is mandatory.
      { ...e, chunks: e.chunks.map(({ records: _r, ...c }) => c) },
      {
        ...e,
        chunks: [
          { ...e.chunks[0]!, bytes: Number.MAX_SAFE_INTEGER },
          ...e.chunks.slice(1),
        ],
      },
    ])
      expect(parsePlanEntryV2(canonicalJson(bad))).toBeUndefined();
    expect(parsePlanEntryV2(JSON.stringify(e, null, 1))).toBeUndefined();
  });

  it("objectLandedWorkKey: a backfill job's work key carries its signed base revision", () => {
    const key = `deploy-a/${v2.upload_identity.key}`;
    const migration = {
      key: v2.upload_identity.key,
      entry_sha256: v2.upload_identity.entry_sha256,
      base_revision: v2.base.revision,
    };
    expect(
      objectLandedWorkKey({ key, kind: "manifest", etag: "e", migration }),
    ).toBe(v2.work_key);
    for (const base_revision of [0, -1, 1.5])
      expect(
        objectLandedWorkKey({
          key,
          kind: "manifest",
          etag: "e",
          migration: { ...migration, base_revision },
        }),
      ).toBeUndefined();
  });
});
