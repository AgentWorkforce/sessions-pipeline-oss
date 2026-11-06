import { describe, expect, test } from "bun:test";
import { hexTraceId, isUuidV7, parseTraceparent, traceparentFor, uuidv7 } from "../ids.ts";

describe("ids", () => {
  test("uuidv7 is version 7, variant 10, and time-ordered", () => {
    const a = uuidv7(1_000_000);
    const b = uuidv7(2_000_000);
    expect(isUuidV7(a)).toBe(true);
    expect(isUuidV7(b)).toBe(true);
    expect(a < b).toBe(true);
  });
  test("traceparent carries the pipeline trace id as the W3C trace id", () => {
    const trace = uuidv7();
    const tp = traceparentFor(trace, "0123456789abcdef");
    expect(tp).toBe(`00-${hexTraceId(trace)}-0123456789abcdef-01`);
    expect(parseTraceparent(tp)).toEqual({ traceId: hexTraceId(trace), spanId: "0123456789abcdef", sampled: true });
    expect(parseTraceparent("garbage")).toBeUndefined();
  });
});
