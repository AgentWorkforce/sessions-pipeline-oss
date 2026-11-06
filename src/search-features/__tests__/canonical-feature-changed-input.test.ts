import { describe, expect, test } from "bun:test";
import type { CanonicalFeatureChangedInput } from "../canonical-version.ts";

const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: "a".repeat(64) };
const AUTH = { authorityId: "12345678-1234-1234-1234-123456789012",
  epoch: "1", fingerprint: "a".repeat(64) };
const AUTH2 = { authorityId: "87654321-4321-4321-4321-210987654321",
  epoch: "2", fingerprint: "b".repeat(64) };
const PS = { authorityId: "12345678-1234-1234-1234-123456789012",
  hash: "c".repeat(64), count: "1" };
const PPS = { authorityId: "87654321-4321-4321-4321-210987654321",
  hash: "d".repeat(64), count: "1" };

// The union is type-level: a valid call compiles, an incomplete pair does not.
describe("canonical_feature_changed input ABI (type-level)", () => {
  test("new-scope publish is expressible (priorSource null)", () => {
    const input: CanonicalFeatureChangedInput = {
      scope: SCOPE, source: AUTH, pointerSet: PS, priorSource: null };
    expect(input.priorSource).toBeNull();
  });
  test("replacement pair is expressible", () => {
    const input: CanonicalFeatureChangedInput = {
      scope: SCOPE, source: AUTH, pointerSet: PS,
      priorSource: AUTH2, priorPointerSet: PPS };
    expect(input.priorPointerSet).toEqual(PPS);
  });
  test("incomplete pair is rejected at TYPE level", () => {
    // @ts-expect-error priorPointerSet missing when priorSource is set
    const _bad: CanonicalFeatureChangedInput = {
      scope: SCOPE, source: AUTH, pointerSet: PS,
      priorSource: AUTH2 };
    expect(_bad.priorSource.authorityId).toBeTruthy();
  });
  test("stray priorPointerSet without priorSource is rejected", () => {
    // @ts-expect-error priorPointerSet cannot appear when priorSource is null
    const _bad: CanonicalFeatureChangedInput = {
      scope: SCOPE, source: AUTH, pointerSet: PS,
      priorSource: null, priorPointerSet: PPS };
    expect(_bad).toBeTruthy();
  });
});
