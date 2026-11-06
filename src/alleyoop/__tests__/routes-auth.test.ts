import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import { createPipelineRoutes, type AppEnv } from "../../shared/vendor.ts";
import { createCourierManifestRoutes } from "../manifests.ts";
import { PUBLIC_INGEST_PATHS, mountIngestApi } from "../routes.ts";

/**
 * The public ALB contract (docs/aws-stage.md "Ingest exposure"): only health
 * checks and the public JWKS answer without credentials. Every other route the
 * ingest mounts must refuse a request with no token, or a token that is not a
 * RelayAuth JWT, before touching any dependency (the deps below throw).
 */
const boom = () => {
  throw new Error("dependency reached before authentication");
};
const deps = new Proxy({}, { get: () => boom }) as never;

function app() {
  const a = new Hono<AppEnv>();
  for (const path of PUBLIC_INGEST_PATHS) a.get(path, (c) => c.json({ ok: true }));
  mountIngestApi(a, { courierManifests: createCourierManifestRoutes(deps), pipeline: createPipelineRoutes(deps) }, new Hono<AppEnv>());
  return a;
}

const env = { ALLOW_INTERIM_RELAYAUTH_BEARER: "true", RELAYAUTH_JWKS_URL: "http://127.0.0.1:9/.well-known/jwks.json", RELAYAUTH_ISSUER: "https://relayauth.example", PIPELINE_OPS_SECRET: "ops-secret-for-test" };
const concrete = (path: string) => path.replace(/:([A-Za-z]+)(\{[^}]*\})?/g, "00000000-0000-7000-8000-000000000000").replace(/\*/g, "x");

describe("ingest authentication", () => {
  const a = app();
  const routes = a.routes.filter((r) => r.method !== "ALL" && !(PUBLIC_INGEST_PATHS as readonly string[]).includes(r.path));

  test("mounts the ingest, manifest and ops routes (the check covers a real surface)", () => {
    const paths = routes.map((r) => `${r.method} ${r.path}`);
    expect(paths.some((p) => p.includes("/v1/pipeline/chunks"))).toBe(true);
    expect(paths.some((p) => p.includes("/v1/pipeline/manifests"))).toBe(true);
    expect(paths.some((p) => p.includes("/v1/internal/pipeline/"))).toBe(true);
    expect(routes.length).toBeGreaterThan(5);
  });

  test("every non-public route refuses a request without credentials", async () => {
    for (const r of routes) {
      const res = await a.request(concrete(r.path), { method: r.method, headers: { "content-type": "application/json" }, body: r.method === "GET" || r.method === "HEAD" ? undefined : "{}" }, env);
      expect({ route: `${r.method} ${r.path}`, status: res.status }).toEqual({ route: `${r.method} ${r.path}`, status: 401 });
    }
  });

  test("every non-public route refuses a bearer that is not a valid RelayAuth JWT", async () => {
    for (const r of routes) {
      const res = await a.request(concrete(r.path), { method: r.method, headers: { authorization: "Bearer not-a-jwt", "content-type": "application/json" }, body: r.method === "GET" || r.method === "HEAD" ? undefined : "{}" }, env);
      expect({ route: `${r.method} ${r.path}`, status: res.status }).toEqual({ route: `${r.method} ${r.path}`, status: 401 });
    }
  });

  test("only health checks and the JWKS are public", () => {
    expect([...PUBLIC_INGEST_PATHS].sort()).toEqual(["/.well-known/jwks.json", "/health", "/healthz", "/readyz"]);
  });
});

describe("tenant org allowlist (TENANT_ORG_IDS)", () => {
  const { orgAllowlist, allowedOrgIdsFromEnv } = require("../routes.ts") as typeof import("../routes.ts");
  const withAuth = (orgId?: string) => {
    const a = new Hono<AppEnv>();
    a.use("*", async (c, next) => {
      if (orgId) c.set("auth" as never, { orgId } as never);
      await next();
    });
    a.use("*", orgAllowlist(["org_aws_proof_1", "org_aws_proof_2"]));
    a.get("/v1/x", (c) => c.json({ ok: true }));
    return a;
  };
  test("admits listed orgs and refuses any other authenticated org with 403", async () => {
    expect((await withAuth("org_aws_proof_1").request("/v1/x")).status).toBe(200);
    expect((await withAuth("org_aws_proof_2").request("/v1/x")).status).toBe(200);
    const r = await withAuth("org_other").request("/v1/x");
    expect(r.status).toBe(403);
    expect(((await r.json()) as { error: { code: string } }).error.code).toBe("org_not_allowed");
    expect((await withAuth(undefined).request("/v1/x")).status).toBe(403);
  });
  test("parses TENANT_ORG_IDS; unset means single-tenant (no allowlist)", () => {
    expect(allowedOrgIdsFromEnv("org_a, org_b,")).toEqual(["org_a", "org_b"]);
    expect(allowedOrgIdsFromEnv(undefined)).toEqual([]);
  });
});

describe("multi-tenant ingest", () => {
  test("mounts the allowlist only for multi-tenant deployments, after authentication", async () => {
    const make = (allowedOrgIds?: string[]) => {
      const a = new Hono<AppEnv>();
      mountIngestApi(a, { courierManifests: createCourierManifestRoutes(deps), pipeline: createPipelineRoutes(deps) }, new Hono<AppEnv>(), { allowedOrgIds });
      return a;
    };
    // Unauthenticated: 401 either way (auth runs first).
    expect((await make(["org_a"]).request("/v1/pipeline/events", { method: "POST", body: "{}" }, env)).status).toBe(401);
    const src = require("node:fs").readFileSync(new URL("../routes.ts", import.meta.url), "utf8") as string;
    const auth = src.indexOf('v1.use("*", async (c, next) => requireAuth(c as never, next));');
    const allow = src.indexOf('if (opts.allowedOrgIds?.length) v1.use("*", orgAllowlist(opts.allowedOrgIds));');
    expect(auth).toBeGreaterThan(0);
    expect(allow).toBeGreaterThan(auth);
    const ingest = require("node:fs").readFileSync(new URL("../ingest.ts", import.meta.url), "utf8") as string;
    expect(ingest).toContain("allowedOrgIds: allowedOrgIdsFromEnv(process.env.TENANT_ORG_IDS)");
  });
});
