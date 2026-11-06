import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import type { AppEnv } from "../../shared/vendor.ts";
import { allowedOrgIdsFromEnv, orgAllowlist } from "../org-allowlist.ts";

const app = (orgId?: string) => {
  const a = new Hono<AppEnv>();
  a.use("*", async (c, next) => {
    if (orgId) c.set("auth" as never, { orgId } as never);
    await next();
  });
  a.use("*", orgAllowlist(["org_a", "org_b"]));
  a.get("/v1/x", (c) => c.json({ ok: true }));
  return a;
};

describe("tenant org allowlist", () => {
  test("admits listed orgs; refuses any other verified org, and a missing auth, with 403 org_not_allowed", async () => {
    expect((await app("org_a").request("/v1/x")).status).toBe(200);
    expect((await app("org_b").request("/v1/x")).status).toBe(200);
    for (const org of ["org_c", "ORG_A", "org_a ", undefined]) {
      const r = await app(org).request("/v1/x");
      expect(r.status).toBe(403);
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe(
        "org_not_allowed",
      );
    }
  });
  test("TENANT_ORG_IDS: comma-separated, trimmed; unset or empty means no allowlist", () => {
    expect(allowedOrgIdsFromEnv("org_a, org_b,")).toEqual(["org_a", "org_b"]);
    expect(allowedOrgIdsFromEnv("")).toEqual([]);
    expect(allowedOrgIdsFromEnv(undefined)).toEqual([]);
  });
});
