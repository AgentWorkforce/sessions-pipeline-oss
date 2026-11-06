import { describe, expect, test } from "bun:test";
import { healthVerdict, readyVerdict } from "../worker.ts";

const opts = { maxFailures: 3, maxTickAgeMs: 60_000 };
describe("worker health", () => {
  test("healthy while ticking and the database answers", () => {
    expect(healthVerdict({ ready: true, lastTickAt: 1_000, consecutiveDbFailures: 0 }, 2_000, opts)).toEqual({ ok: true, reason: "ok" });
  });
  test("unhealthy after N consecutive database failures (rotated password)", () => {
    expect(healthVerdict({ ready: true, lastTickAt: 1_000, consecutiveDbFailures: 3 }, 2_000, opts).reason).toBe("hoopdb_unreachable");
  });
  test("unhealthy when the poll loop stalls", () => {
    expect(healthVerdict({ ready: true, lastTickAt: 0, consecutiveDbFailures: 0 }, 61_000, opts).reason).toBe("loop_stalled");
  });
});

describe("worker readiness and startup", () => {
  test("starting (schema wait) is alive, not stalled", () => {
    expect(healthVerdict({ ready: false, lastTickAt: 1_000, consecutiveDbFailures: 0 }, 2_000, opts)).toEqual({ ok: true, reason: "starting" });
  });
  test("readiness drops when HoopDb stops answering after startup", () => {
    expect(readyVerdict({ ready: true, lastTickAt: 0, consecutiveDbFailures: 0 }, { maxFailures: 3 })).toBe(true);
    expect(readyVerdict({ ready: true, lastTickAt: 0, consecutiveDbFailures: 3 }, { maxFailures: 3 })).toBe(false);
    expect(readyVerdict({ ready: false, lastTickAt: 0, consecutiveDbFailures: 0 }, { maxFailures: 3 })).toBe(false);
  });
});
