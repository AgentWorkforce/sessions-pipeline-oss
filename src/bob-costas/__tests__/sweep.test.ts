import { describe, expect, it } from "bun:test";
import { sweepMessage } from "../sweep.ts";

describe("summary sweep message", () => {
  it("is a normal summary stage message for the session's current revision, ids only", () => {
    const m = sweepMessage("summary-long", {
      org_id: "o", workspace_id: "w", harness: "claude", session_id: "s", machine_id: "m",
      manifest_key: "v1/ws=w/m=m/h=claude/d=2026/10/29/s=s/manifest.json",
      trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b", revision: "4", session_uuid: "01a188e1-ef86-7892-9389-39149894eeec",
    });
    expect(m.stage).toBe("summary-long");
    expect(m.revision).toBe(4);
    expect(m.workKey).toBe("v1/ws=w/m=m/h=claude/d=2026/10/29/s=s/manifest.json@summary-sweep:4");
    expect(m.payload).toEqual({ traceId: "01a188e1-ef86-7b16-938a-e91b6b78643b", workspaceId: "w", machineId: "m", harness: "claude", sessionId: "s", manifestKey: "v1/ws=w/m=m/h=claude/d=2026/10/29/s=s/manifest.json" });
    expect((m.ctx as Record<string, unknown>).session_uuid).toBe("01a188e1-ef86-7892-9389-39149894eeec");
  });
});
