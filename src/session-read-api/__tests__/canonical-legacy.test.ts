import { expect, test } from "bun:test";
import { canonicalLegacy } from "../canonical-legacy.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
const identity = {
  orgId: "o",
  workspaceId: "w",
  source: "claude",
  sessionId: "s",
};
const record = (
  payload: Record<string, unknown>,
): RelayhistoryStreamRecord => ({
  kind: "legacy_conversation_turn",
  origin_id: "legacy:conversation_turns",
  record_id: "legacy:conversation_turns:id",
  revision: "1",
  operation: "upsert",
  received_at: "2026-11-01T00:00:00Z",
  payload: {
    org_id: "o",
    workspace_id: "w",
    source: "claude",
    session_id: "s",
    id: "id",
    turn_index: 0,
    role: "assistant",
    session_owner: "owner",
    content: "body",
    actor_name: "Agent",
    actor_role: "owner",
    ts: "2026-11-01T00:00:00.123456Z",
    ...payload,
  },
});
test.each(["org_id", "workspace_id", "source", "session_id"])(
  "legacy %s cannot cross manifest scope",
  (field) => {
    expect(() =>
      canonicalLegacy([record({ [field]: "foreign" })], identity),
    ).toThrow();
  },
);
test("absence of explicit legacy coverage remains unknown", () => {
  expect(canonicalLegacy([record({})], identity).coverage).toBeNull();
});
test("coverage requires every table exactly once", () => {
  const r = record({
    version: 1,
    tables: ["conversation_turns", "session_links"],
  });
  r.kind = "legacy_scope";
  r.origin_id = "legacy:scope";
  r.record_id = "legacy:scope:coverage";
  expect(() => canonicalLegacy([r], identity)).toThrow();
});
