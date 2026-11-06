import { test, expect } from "bun:test";
import {
  releaseInputs,
  releaseConnection,
  SCHEMA_RELEASE as P,
} from "../schema-release-contract.ts";
import { githubReleaseGate } from "../../../scripts/schema-release-github.ts";
import { validateReleasePlan } from "../../../scripts/schema-release-database.ts";
const sha = "a".repeat(40),
  generation = "b".repeat(64);
const base = {
  RELEASE_MODE: "plan",
  RELEASE_SHA: sha,
  RELEASE_GENERATION: generation,
  RELEASE_BOUNDARY: "event-trigger",
  GITHUB_REPOSITORY: P.repository,
  GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main",
  GITHUB_SHA: sha,
  GITHUB_RUN_ATTEMPT: "1",
  GITHUB_RUN_ID: "100",
};
const url = `postgresql://${P.owner}.${P.project}:synthetic-only@${P.host}:5432/postgres?sslmode=require`;
test("strict pooler/project/owner pins and URL options cannot weaken TLS", () => {
  expect(releaseConnection(url).host).toBe(P.host);
  for (const bad of [
    url.replace(P.project, "foreign"),
    url.replace(P.owner, "postgres"),
    url.replace(P.host, "evil.example"),
    url.replace(":5432/", ":6543/"),
    url.replace("/postgres?", "/other?"),
    url + "&sslmode=disable",
    url + "&sslrootcert=anything",
    url + "&options=-c%20role%3Dpostgres",
    url + "&ssl=true",
    url + "#fragment",
    url.replace(":synthetic-only", ""),
  ])
    expect(() => releaseConnection(bad)).toThrow();
});
test("typed approval binds source, generation, project and prior plan", () => {
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} PLAN 90`,
  };
  expect(releaseInputs(apply).mode).toBe("apply");
  for (const [k, v] of Object.entries({
    RELEASE_SHA: "c".repeat(40),
    RELEASE_GENERATION: "d".repeat(64),
    RELEASE_PLAN_RUN: "91",
    RELEASE_APPROVAL: "yes",
    RELEASE_BOUNDARY: "none",
  }))
    expect(() => releaseInputs({ ...apply, [k]: v })).toThrow();
  expect(() =>
    releaseInputs({ ...base, RELEASE_APPROVAL: "anything" }),
  ).toThrow();
});
function api() {
  const run = {
    id: 100,
    head_sha: sha,
    head_branch: "main",
    event: "workflow_dispatch",
    path: P.workflow,
    run_attempt: 1,
  };
  const env = {
    id: 7,
    name: P.environment,
    protection_rules: [
      {
        type: "required_reviewers",
        reviewers: [{ type: "User", reviewer: { type: "User", id: 1724137 } }],
      },
    ],
    deployment_branch_policy: {
      protected_branches: false,
      custom_branch_policies: true,
    },
  };
  return {
    ref: { object: { sha } },
    run,
    env,
    branches: {
      total_count: 1,
      branch_policies: [{ name: "main", type: "branch" }],
    },
    approvals: [
      {
        state: "approved",
        user: { type: "User", id: 1724137 },
        environments: [{ id: 7, name: P.environment }],
      },
    ],
    ci: {
      workflow_runs: [
        {
          id: 10,
          head_sha: sha,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          run_attempt: 1,
        },
      ],
    },
    pg: {
      workflow_runs: [
        {
          id: 11,
          head_sha: sha,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          run_attempt: 1,
        },
      ],
    },
    jobs10: {
      total_count: 2,
      jobs: ["checks", "session-read-worker / worker"].map((name) => ({
        name,
        head_sha: sha,
        status: "completed",
        conclusion: "success",
      })),
    },
    jobs11: {
      total_count: 3,
      jobs: [15, 16, 17].map((v) => ({
        name: "real PostgreSQL " + v,
        head_sha: sha,
        status: "completed",
        conclusion: "success",
      })),
    },
    plan: {
      ...run,
      id: 90,
      status: "completed",
      conclusion: "success",
      created_at: new Date().toISOString(),
    },
  };
}
type Fixture = ReturnType<typeof api>;
function get(f: Fixture) {
  return async (path: string): Promise<any> => {
    if (path.endsWith("/git/ref/heads/main")) return f.ref;
    if (path.endsWith("/actions/runs/100")) return f.run;
    if (path.endsWith("/actions/runs/90")) return f.plan;
    if (path.endsWith("/environments/" + P.environment)) return f.env;
    if (path.includes("deployment-branch-policies")) return f.branches;
    if (path.endsWith("/approvals")) return f.approvals;
    if (path.includes("/workflows/ci.yml/")) return f.ci;
    if (path.includes("/workflows/pg-proof.yml/")) return f.pg;
    if (path.includes("/runs/10/")) return f.jobs10;
    if (path.includes("/runs/11/")) return f.jobs11;
    throw new Error("unexpected " + path);
  };
}
test("real main, both exact-head workflows/jobs and actual named human approval required", async () => {
  expect((await githubReleaseGate(base, get(api()), true)).sha).toBe(sha);
});
const mutations: Array<[string, (f: Fixture) => void]> = [
  ["main moved", (f) => (f.ref.object.sha = "e".repeat(40))],
  ["wrong workflow", (f) => (f.run.path = ".github/workflows/untrusted.yml")],
  ["missing human rule", (f) => (f.env.protection_rules = [])],
  [
    "team-only reviewer",
    (f) => (f.env.protection_rules[0]!.reviewers[0]!.type = "Team"),
  ],
  [
    "unrestricted branch",
    (f) => (f.env.deployment_branch_policy.custom_branch_policies = false),
  ],
  ["branch pattern", (f) => (f.branches.branch_policies[0]!.name = "*")],
  ["tag main", (f) => (f.branches.branch_policies[0]!.type = "tag")],
  ["ci missing", (f) => (f.ci.workflow_runs = [])],
  ["ci running", (f) => (f.ci.workflow_runs[0]!.status = "in_progress")],
  ["ci failed", (f) => (f.ci.workflow_runs[0]!.conclusion = "failure")],
  ["wrong tested SHA", (f) => (f.jobs10.jobs[0]!.head_sha = "e".repeat(40))],
  [
    "worker omitted",
    (f) => {
      f.jobs10.jobs.pop();
      f.jobs10.total_count--;
    },
  ],
  ["PG16 skipped", (f) => (f.jobs11.jobs[1]!.conclusion = "skipped")],
  ["job truncated", (f) => (f.jobs11.total_count = 101)],
  ["bypassed human gate", (f) => (f.approvals = [])],
  ["bot approval", (f) => (f.approvals[0]!.user.type = "Bot")],
  ["foreign human", (f) => (f.approvals[0]!.user.id = 123)],
  ["different environment", (f) => (f.approvals[0]!.environments[0]!.id = 8)],
];
for (const [label, mutate] of mutations)
  test(label + " refuses", async () => {
    const f = api();
    mutate(f);
    await expect(githubReleaseGate(base, get(f), true)).rejects.toThrow();
  });
for (const [k, v] of Object.entries({
  GITHUB_REF: "refs/heads/feature",
  GITHUB_RUN_ATTEMPT: "2",
  GITHUB_EVENT_NAME: "pull_request",
  GITHUB_REPOSITORY: "foreign/repo",
  GITHUB_SHA: "e".repeat(40),
}))
  test(
    k + " refuses",
    async () =>
      await expect(
        githubReleaseGate({ ...base, [k]: v }, get(api()), true),
      ).rejects.toThrow(),
  );
test("apply rejects foreign, stale, replayed or failed prior plan and artifact bindings", async () => {
  const env = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} PLAN 90`,
  };
  expect((await githubReleaseGate(env, get(api()), true)).mode).toBe("apply");
  for (const values of [
    { head_sha: "e".repeat(40) },
    { run_attempt: 2 },
    { conclusion: "failure" },
    { created_at: new Date(Date.now() - 25 * 3600000).toISOString() },
    { path: "wrong" },
  ]) {
    const f = api();
    Object.assign(f.plan, values);
    await expect(githubReleaseGate(env, get(f), true)).rejects.toThrow();
  }
  const input = { sha, generation, boundary: "event-trigger", planRun: "90" };
  const plan = {
    version: 1,
    project: P.project,
    sha,
    generation,
    boundary: "event-trigger",
    runId: "90",
    ledgerSha256: "c".repeat(64),
    inventory: [{}],
  };
  expect(validateReleasePlan(plan, input)).toBe("c".repeat(64));
  for (const field of [
    "project",
    "sha",
    "generation",
    "boundary",
    "runId",
    "ledgerSha256",
  ])
    expect(() =>
      validateReleasePlan({ ...plan, [field]: "wrong" }, input),
    ).toThrow();
});
// The hosted apply-session-schema workflow and its credential-scoping test remain
// with the private deployment composition; this source distribution has no apply workflow.

test("connection enforces pinned CA/hostname and release profile ignores ambient bootstrap switches", async () => {
  const { releaseClient, releaseProfile } =
    await import("../schema-release-inspect.ts");
  const client = releaseClient(url);
  const ssl = client.connectionParameters.ssl as any;
  expect(ssl.rejectUnauthorized).toBe(true);
  expect(ssl.servername).toBe(P.host);
  expect(ssl.ca).toContain("BEGIN CERTIFICATE");
  const profile = releaseProfile("global-default-revoke");
  expect(profile).toEqual({
    mode: "supabase",
    shim: false,
    bootstrap: false,
    verify: true,
    runtimeRole: P.runtime,
    runtimeLogin: true,
    functionBoundary: "global-default-revoke",
  });
});
test("operator inspection starts READ ONLY and rolls back on wrong owner without schema SQL", async () => {
  const { inspectRelease } = await import("../schema-release-inspect.ts");
  const statements: string[] = [];
  const client = {
    query: async (sql: string) => {
      statements.push(sql);
      if (sql.includes("current_user AS me"))
        return {
          rows: [
            {
              me: "postgres",
              session: "postgres",
              db: "postgres",
              rolcanlogin: true,
            },
          ],
        };
      return { rows: [] };
    },
  };
  await expect(
    inspectRelease(client as any, "global-default-revoke"),
  ).rejects.toThrow("release_owner");
  expect(statements[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  expect(statements.at(-1)).toBe("ROLLBACK");
  expect(
    statements.some((s) => /CREATE|ALTER|GRANT|INSERT|UPDATE|DELETE/.test(s)),
  ).toBe(false);
});
test("operator plan refuses apply mode before reading a secret or connecting", () => {
  const r = Bun.spawnSync(
    [process.execPath, "scripts/schema-release-operator-plan.ts"],
    {
      cwd: new URL("../../../", import.meta.url).pathname,
      env: {
        ...process.env,
        ...base,
        RELEASE_MODE: "apply",
        RELEASE_PLAN_RUN: "90",
        RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} PLAN 90`,
        SUPABASE_MIGRATION_OWNER_URL: "",
      },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  expect(r.exitCode).toBe(1);
  expect(JSON.parse(new TextDecoder().decode(r.stdout))).toEqual({
    version: 1,
    status: "refused",
    code: "operator_plan_only",
  });
  expect(new TextDecoder().decode(r.stderr)).toBe("");
});
