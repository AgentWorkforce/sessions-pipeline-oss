/**
 * Cross-repo contract test: the reference derivation in
 * src/lib/pipeline/migration-assertions.ts reproduces every value of
 * docs/contracts/relayhistory-export-plan-entry-v1.vectors.json (shared with
 * relayhistory-cloud #228, the #40 courier and relayauth #110).
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { computeContentSha256 } from "../../object-store/src/manifest.js";
import {
  canonicalJson,
  derivePlanEntry,
  exportPlanDigest,
  migrationSessionMatches,
  migrationWorkKey,
  normalizeRelayId,
  parseUploadIdentity,
  planEntrySha256,
  sessionManifestKey,
  uploadIdentityOf,
  type PlanEntry,
  type UploadIdentity,
} from "../src/lib/pipeline/migration-assertions.js";
import { parseSessionKey } from "../src/lib/pipeline/object-layout.js";

const vectors = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../../docs/contracts/relayhistory-export-plan-entry-v1.vectors.json", import.meta.url).href),
    "utf8",
  ),
) as {
  grant_id: string;
  exported_at: string;
  entries: { entry: PlanEntry; canonical_json: string; entry_sha256: string }[];
  plan: { canonical_json: string; plan_digest: string };
  upload_identities: UploadIdentity[];
  negative: { name: string; upload: UploadIdentity; request_key?: string; sponsor_id?: string; expect: string }[];
  work_keys: { key: string; entry_sha256: string; work_key: string }[];
};

/** The auth context the verifier would set for an assertion signing `upload`. */
function authFor(upload: UploadIdentity, sponsorId = upload.source_user_id) {
  return {
    claims: { org: upload.org_id, wks: upload.workspace_id, sponsorId },
    migration: {
      jti: "tok_x", nonce: "n".repeat(16), grantId: upload.grant_id, planDigest: upload.plan_digest,
      sourceUserId: sponsorId, workspaceId: upload.workspace_id, expiresAt: 0, upload,
    },
  };
}

describe("export plan entry contract v1 (vectors; relayauth #111 @2adf414a reproduces them)", () => {
  it("reproduces every entry, identity and the plan digest", async () => {
    for (const [i, vector] of vectors.entries.entries()) {
      expect(await computeContentSha256(vector.entry.chunks)).toBe(vector.entry.content_sha256);
      expect(canonicalJson(vector.entry)).toBe(vector.canonical_json);
      expect(await planEntrySha256(vector.entry)).toBe(vector.entry_sha256);
      expect(sessionManifestKey(parseSessionKey(vector.entry.key)!)).toBe(vector.entry.key);
      expect(await uploadIdentityOf(vector.entry, vectors.plan.plan_digest)).toEqual(vectors.upload_identities[i]);
      expect(parseUploadIdentity(vectors.upload_identities[i])).toEqual(vectors.upload_identities[i]);
    }
    const sorted = [...vectors.entries.map((v) => v.entry)].sort((a, b) => (a.key < b.key ? -1 : 1));
    expect(canonicalJson({ v: 1, grant_id: vectors.grant_id, exported_at: vectors.exported_at, manifests: sorted })).toBe(
      vectors.plan.canonical_json,
    );
    expect(
      await exportPlanDigest({ grantId: vectors.grant_id, exportedAt: vectors.exported_at, entries: vectors.entries.map((v) => v.entry) }),
    ).toBe(vectors.plan.plan_digest);
  });

  it("derives the entry from a manifest built from the vector, and matches the signed identity", async () => {
    for (const [i, vector] of vectors.entries.entries()) {
      const e = vector.entry;
      const upload = vectors.upload_identities[i]!;
      const manifest = {
        session: { ...e.session, workspace_id: e.workspace_id },
        raw: { chunks: e.chunks, content_sha256: e.content_sha256, chunk_count: e.chunks.length, state: e.state },
      } as never;
      const location = parseSessionKey(e.key)!;
      expect(await derivePlanEntry(authFor(upload), location, manifest)).toEqual(e);
      expect(migrationSessionMatches(authFor(upload), location)).toBe(true);
    }
  });

  it("refuses every negative vector", async () => {
    for (const negative of vectors.negative) {
      if (negative.expect === "invalid_claims") {
        expect(parseUploadIdentity(negative.upload), negative.name).toBeUndefined();
        continue;
      }
      const location = parseSessionKey(negative.request_key ?? negative.upload.key)!;
      expect(migrationSessionMatches(authFor(negative.upload, negative.sponsor_id), location), negative.name).toBe(false);
    }
  });

  it("derives the work key from signed fields only: <key>@<entry_sha256>", () => {
    expect(vectors.work_keys).toHaveLength(vectors.upload_identities.length);
    for (const [i, w] of vectors.work_keys.entries()) {
      const upload = vectors.upload_identities[i]!;
      expect(w).toEqual({ key: upload.key, entry_sha256: upload.entry_sha256, work_key: `${upload.key}@${upload.entry_sha256}` });
      expect(migrationWorkKey(upload)).toBe(w.work_key);
    }
  });

  it("normalizes one leading relay_ only", () => {
    expect(normalizeRelayId("relay_user_a")).toBe("user_a");
    expect(normalizeRelayId("user_a")).toBe("user_a");
    expect(normalizeRelayId("relay_relay_x")).toBe("relay_x");
  });
});
