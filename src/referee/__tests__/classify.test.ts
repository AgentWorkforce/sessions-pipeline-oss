import { describe, expect, test } from "bun:test";
import { classify } from "../handler.ts";

const now = new Date("2026-10-28T12:00:00Z");
describe("referee rules", () => {
  test("closed raw stream is done", () => {
    expect(classify({ closed: true, lastActivity: now, now, abandonAfterMinutes: 60 })).toEqual({ state: "done", rule: "raw_closed" });
  });
  test("recent activity is active", () => {
    expect(classify({ closed: false, lastActivity: new Date(now.getTime() - 5 * 60_000), now, abandonAfterMinutes: 60 }).state).toBe("active");
  });
  test("idle beyond the threshold is abandoned", () => {
    expect(classify({ closed: false, lastActivity: new Date(now.getTime() - 61 * 60_000), now, abandonAfterMinutes: 60 }).state).toBe("abandoned");
  });
});
