import { describe, expect, test } from "bun:test";
import { supabaseProjectRef, writeMode } from "../target-guard.ts";

const REF = "abcdefghijklmnopqrst";
const OTHER = "uvwxyzabcdefghijklmn";
const DIRECT = `postgres://postgres:x@db.${REF}.supabase.co:5432/postgres`;
const POOLED = `postgres://postgres.${REF}:x@aws-0-us-east-1.pooler.supabase.com:6543/postgres`;
const NEON = "postgres://owner:x@ep-cool-1.us-east-1.aws.neon.tech/neondb";

describe("target identity", () => {
  test("derives the project ref from the direct host or the pooler user, nothing else", () => {
    expect(supabaseProjectRef(DIRECT)).toBe(REF);
    expect(supabaseProjectRef(POOLED)).toBe(REF);
    expect(supabaseProjectRef(NEON)).toBeNull();
    expect(supabaseProjectRef(`postgres://x@db.${REF}.supabase.co.evil.example/p`)).toBeNull();
    expect(supabaseProjectRef(`postgres://postgres:x@x.pooler.supabase.com/p`)).toBeNull();
    expect(supabaseProjectRef("not a url")).toBeNull();
  });
});

describe("write mode", () => {
  test("is a dry run unless --execute", () => {
    expect(writeMode({ targetUrl: NEON, sourceUrl: DIRECT, execute: false })).toBe("dry-run");
  });

  test("executes only against the positively identified Supabase project", () => {
    for (const targetUrl of [DIRECT, POOLED])
      expect(
        writeMode({ targetUrl, sourceUrl: NEON, execute: true, expectSupabaseRef: REF }),
      ).toBe("execute");
  });

  test("refuses swapped, foreign and unidentified targets", () => {
    const refuse = (input: Parameters<typeof writeMode>[0], message: string) =>
      expect(() => writeMode(input)).toThrow(message);
    // Swapped: Neon as target, Supabase as source.
    refuse({ targetUrl: NEON, sourceUrl: DIRECT, execute: true, expectSupabaseRef: REF }, "source is a Supabase URL");
    refuse({ targetUrl: NEON, execute: true, expectSupabaseRef: REF }, "not the expected Supabase project");
    refuse({ targetUrl: DIRECT.replace(REF, OTHER), sourceUrl: NEON, execute: true, expectSupabaseRef: REF }, "not the expected Supabase project");
    refuse({ targetUrl: DIRECT, sourceUrl: NEON, execute: true }, "--execute needs");
    refuse({ targetUrl: DIRECT, sourceUrl: NEON, execute: true, expectSupabaseRef: "bad" }, "not a Supabase project ref");
    refuse({ targetUrl: NEON, sourceUrl: NEON.replace("owner", "reader"), execute: true, expectTargetHost: "ep-cool-1.us-east-1.aws.neon.tech" }, "same host");
    // Rehearsal host flag cannot be pointed at a managed database.
    refuse({ targetUrl: NEON, execute: true, expectTargetHost: "ep-cool-1.us-east-1.aws.neon.tech" }, "local rehearsals only");
    refuse({ targetUrl: "postgres://x@rh-dst:5432/p", execute: true, expectTargetHost: "rh-other" }, "not the expected host");
  });

  test("allows a named local rehearsal host", () => {
    expect(
      writeMode({
        targetUrl: "postgres://x@rh-dst:5432/p",
        sourceUrl: "postgres://x@rh-src:5432/p",
        execute: true,
        expectTargetHost: "rh-dst",
      }),
    ).toBe("execute");
  });
});
