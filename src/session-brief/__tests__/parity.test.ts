import { describe, expect, test } from "bun:test";
import { compare, jaccard, parse } from "../../../scripts/session-brief-parity.ts";

describe("session brief parity comparator", () => {
  test("joins on org and session, drops ambiguous keys, and reports only aggregates", () => {
    const neon = parse(
      ["o | s1 | Fix the flaky queue test | Fixed a race.", "o | s2 | Add briefs | Added a table.", "o | s3 | A | x", "o | s3 | B | y"].join("\n"),
    );
    const supa = parse(["o | s1 | Fix the flaky queue test | Fixed a race in acks.", "o | s2 | Port briefs to Supabase | Added a table.", "o | s4 | C | z"].join("\n"));
    expect(neon.ambiguous).toBe(1);
    const out = compare(neon.rows, supa.rows);
    expect(out).toMatchObject({ neon: 2, supabase: 3, both: 2, neon_only: 0, supabase_only: 1, title_exact: 1 });
    const printed = JSON.stringify(out);
    for (const secret of ["flaky", "briefs", "s1", "race"]) expect(printed).not.toContain(secret);
  });

  test("token Jaccard", () => {
    expect(jaccard("Fix the build", "fix the build")).toBe(1);
    expect(jaccard("a b", "c d")).toBe(0);
    expect(jaccard("a b c", "a b d")).toBeCloseTo(0.5);
  });
});
