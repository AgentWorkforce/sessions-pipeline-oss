/**
 * THE hs1 image key — mirrors #230's sessions.canonical_feature_image_key:
 *   sha256(utf8(join(chr(31), 'history-scope-v1', orgId, workspaceId,
 *                    harness, sessionId))) : generation : authorityId
 * The SQL writer is the ONLY definition; this is the byte-identical mirror.
 */
import { createHash } from "node:crypto";
import type { CanonicalScope } from "./canonical-version.ts";

const US = "\x1f";   // U+001F — the SQL chr(31) separator
const TAG = "history-scope-v1";

export function canonicalImageKey(
  scope: Pick<CanonicalScope,
    "orgId" | "workspaceId" | "harness" | "sessionId" | "generation">,
  authorityId: string,
): string {
  const digest = createHash("sha256")
    .update([TAG, scope.orgId, scope.workspaceId, scope.harness,
             scope.sessionId].join(US), "utf8")
    .digest("hex");
  return `hs1:${digest}:${scope.generation}:${authorityId}`;
}
