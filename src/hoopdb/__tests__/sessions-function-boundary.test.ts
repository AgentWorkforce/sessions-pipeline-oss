/**
 * The unified sessions function-boundary generator (scripts/sync-sessions-function-boundary.ts):
 * deterministic output, --check catches drift, manifests must match their SQL, and no function
 * may be owned by two lanes.
 */
import { describe, expect, test } from "bun:test";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { generate, ACL_PATH, type Reader } from "../../../scripts/sessions-function-boundary/generate.ts";
import { LANES } from "../../../scripts/sessions-function-boundary/lanes/index.ts";
import { ingestHistory } from "../../../scripts/sessions-function-boundary/lanes/ingest-history.ts";
import type { LaneManifest } from "../../../scripts/sessions-function-boundary/types.ts";
import { VERIFIERS } from "../../../scripts/sessions-function-boundary/verifiers.ts";
import { run } from "../../../scripts/sync-sessions-function-boundary.ts";

const root = join(import.meta.dir, "../../..");
const reader = (base: string): Reader => async (p) => { try { return readFileSync(join(base, p), "utf8"); } catch { return undefined; } };
const repo = reader(root);
const OWNED = Object.keys(ingestHistory.functions);
const SAMPLE = OWNED.find((k) => ingestHistory.functions[k]!.grants.length)!;

/** A scratch checkout with exactly the files the generator reads and writes. */
function scratch(): string {
  const dir = mkdtempSync(join(tmpdir(), "sessions-boundary-"));
  for (const p of ["supabase/migrations", "supabase/rollbacks", "supabase/bootstrap", "src/hoopdb/sessions-function-acl.json", "src/hoopdb/migrate-guards.ts"]) {
    mkdirSync(dirname(join(dir, p)), { recursive: true });
    cpSync(join(root, p), join(dir, p), { recursive: true });
  }
  return dir;
}
const cli = (args: string[]) => Bun.spawnSync(["bun", ...args], { cwd: root, stdout: "pipe", stderr: "pipe" });

describe("unified sessions function-boundary generator", () => {
  test("the committed artifacts are exactly the generator's output (no drift)", async () => {
    expect((await run({ root, check: true })).drift).toEqual([]);
  });

  test("deterministic: two runs, and any lane order, give byte-identical outputs", async () => {
    const a = await generate(repo, LANES, VERIFIERS);
    const b = await generate(repo, LANES, VERIFIERS);
    const c = await generate(repo, [...LANES].reverse(), [...VERIFIERS].reverse());
    expect(b.outputs).toEqual(a.outputs);
    expect(c.outputs).toEqual(a.outputs);
    expect(Object.keys(a.outputs).sort()).toEqual([
      "src/hoopdb/migrate-guards.ts", "src/hoopdb/sessions-function-acl.json",
      "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
      "supabase/bootstrap/session_directory_search_worker_verify.sql", "supabase/bootstrap/session_ingest_verify.sql",
      "supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql",
    ]);
  });

  test("--check catches drift in every generated artifact and writes nothing; a write run repairs it", async () => {
    const dir = scratch();
    try {
      const acl = JSON.parse(readFileSync(join(dir, ACL_PATH), "utf8"));
      acl.functions[SAMPLE] = ["<owner>:EXECUTE", "anon:EXECUTE"];                         // a widened grant
      acl.functions["sessions.ingest_history_dropped_fn(jsonb)"] = ["<owner>:EXECUTE"];     // a stale lane entry
      writeFileSync(join(dir, ACL_PATH), JSON.stringify(acl, null, 2) + "\n");
      const down = "supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql";
      writeFileSync(join(dir, down), readFileSync(join(dir, down), "utf8") + "-- hand edit\n");
      const wrapper = "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql";
      writeFileSync(join(dir, wrapper), readFileSync(join(dir, wrapper), "utf8").replace("'sessions.ingest_history_mask_register(jsonb)',\n", ""));   // a SECDEF entry dropped by hand
      const tampered = readFileSync(join(dir, ACL_PATH), "utf8");
      const checked = await run({ root: dir, check: true });
      // Lane-owned ACL entries are regenerated from the manifest (the wrapper/guards were never touched).
      expect(checked.drift).toEqual([ACL_PATH, wrapper, down].sort());
      expect(readFileSync(join(dir, ACL_PATH), "utf8")).toBe(tampered);              // --check wrote nothing
      const proc = cli(["scripts/sync-sessions-function-boundary.ts", "--check", "--root=" + dir]);
      expect(proc.exitCode).toBe(1);
      expect(proc.stderr.toString()).toContain("sessions function boundary drift: " + ACL_PATH);
      const old = cli(["scripts/sync-ingest-history-boundary.ts", "--check"]);          // the thin wrapper, on the clean repo
      expect(old.exitCode).toBe(0);
      expect(JSON.parse(old.stdout.toString())).toMatchObject({ functions: 63, publicationRPCs: 28, check: true });
      expect((await run({ root: dir })).drift.length).toBeGreaterThan(0);              // write mode repairs
      expect((await run({ root: dir, check: true })).drift).toEqual([]);
      const fixed = JSON.parse(readFileSync(join(dir, ACL_PATH), "utf8")).functions;
      expect(fixed[SAMPLE]).toEqual(["<owner>:EXECUTE", ...ingestHistory.functions[SAMPLE]!.grants.map((r) => r + ":EXECUTE")]);
      expect(fixed["sessions.ingest_history_dropped_fn(jsonb)"]).toBeUndefined();
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  test("two manifests claiming the same function fail (exact signature and prefix overlap); so does a non-empty empty slot", async () => {
    const rival: LaneManifest = { lane: "rival", owner: "test", migration: ingestHistory.migration, prefixes: ["sessions.rival_"],
      grantSources: [], functions: { [SAMPLE]: ingestHistory.functions[SAMPLE]! }, replaces: {} };
    await expect(generate(repo, [ingestHistory, rival], VERIFIERS)).rejects.toThrow(`function ownership conflict: ${SAMPLE} is claimed by lanes ingest-history and rival`);
    const prefixed: LaneManifest = { ...rival, functions: {}, prefixes: ["sessions.ingest_history_scope_"] };
    await expect(generate(repo, [ingestHistory, prefixed], VERIFIERS)).rejects.toThrow("function ownership conflict: prefixes");
    const slot: LaneManifest = { lane: "slot", owner: "test", migration: null, prefixes: [], grantSources: [], functions: { "sessions.x()": { secdef: false, grants: [] } }, replaces: {} };
    await expect(generate(repo, [ingestHistory, slot], VERIFIERS)).rejects.toThrow("empty slot slot must not claim");
    const cliDup = cli(["-e", `import {run} from "./scripts/sync-sessions-function-boundary.ts";import {LANES} from "./scripts/sessions-function-boundary/lanes/index.ts";
      const l=LANES[0]!;await run({check:true,lanes:[...LANES,{...l,lane:"dup",prefixes:[],extraOutputs:undefined}]});`]);
    expect(cliDup.exitCode).not.toBe(0);
    expect(cliDup.stderr.toString()).toContain("function ownership conflict");
  });

  test("a manifest that disagrees with its SQL fails: unlisted function, wrong SECURITY DEFINER, missing grant", async () => {
    const { [SAMPLE]: dropped, ...rest } = ingestHistory.functions;
    await expect(generate(repo, [{ ...ingestHistory, functions: rest }], VERIFIERS)).rejects.toThrow(`manifest drift (ingest-history): ${ingestHistory.migration} defines ${SAMPLE}`);
    await expect(generate(repo, [{ ...ingestHistory, functions: { ...ingestHistory.functions, [SAMPLE]: { ...dropped!, secdef: !dropped!.secdef } } }], VERIFIERS)).rejects.toThrow("SECURITY DEFINER");
    await expect(generate(repo, [{ ...ingestHistory, functions: { ...ingestHistory.functions, [SAMPLE]: { ...dropped!, grants: [] } } }], VERIFIERS)).rejects.toThrow(`manifest drift (ingest-history): ${SAMPLE} grants`);
    await expect(generate(repo, [{ ...ingestHistory, functions: { ...ingestHistory.functions, "sessions.ingest_history_ghost()": { secdef: false, grants: [] } } }], VERIFIERS)).rejects.toThrow("which supabase/migrations/20261105026000_canonical_history_lineage.sql does not define");
  });

  test("an undeclared grantee in a lane's SQL fails (no role is silently dropped from the manifest)", async () => {
    const narrowed: LaneManifest = { ...ingestHistory, grantSources: ingestHistory.grantSources.map((g) => g.path === ingestHistory.migration ? { ...g, roles: g.roles.filter((r) => r !== "bulls96_canonical_feature_producer") } : g) };
    await expect(generate(repo, [narrowed], VERIFIERS)).rejects.toThrow("grants EXECUTE to bulls96_canonical_feature_producer, which the lane's grant sources do not declare");
  });

  test("a templated verifier (a new role cloned from another verifier) is derived deterministically; a duplicate verifier path fails", async () => {
    const base = VERIFIERS[0]!;
    const clone = { ...base, role: "session_directory_search_worker", path: "supabase/bootstrap/zz_clone_verify.sql", template: base.path, transform: (t: string) => t.replaceAll("bulls96_session_ingest", "bulls96_clone") };
    const out = (await generate(repo, LANES, [...VERIFIERS, clone])).outputs[clone.path]!;
    expect(out).toContain("bulls96_clone");
    expect(out).not.toContain("bulls96_session_ingest");
    const workerFns = OWNED.filter((k) => ingestHistory.functions[k]!.grants.includes("session_directory_search_worker"));
    expect(workerFns.length).toBeGreaterThan(0);
    for (const sig of workerFns) expect(out).toContain("'" + sig + "'");
    await expect(generate(repo, LANES, [...VERIFIERS, VERIFIERS[0]!])).rejects.toThrow("is configured twice");
  });

  test("the remaining empty read-api and canonical-project-events slots are inert; the consumer slot is filled", async () => {
    const slots = LANES.filter((l) => l.migration === null);
    expect(slots.map((l) => l.lane).sort()).toEqual(["canonical-project-events", "read-api"]);
    for (const l of slots) {
      expect([Object.keys(l.functions).length, Object.keys(l.replaces).length, l.prefixes.length, l.grantSources.length]).toEqual([0, 0, 0, 0]);
      expect(l.extraOutputs).toBeUndefined();
    }
    const summary = (await generate(repo, LANES, VERIFIERS)).summary.lanes;
    for (const l of slots) expect(summary[l.lane]).toEqual({ functions: 0, replaces: 0, grants: 0 });
    expect(summary["canonical-feature-consumer"]).toEqual({ functions: 2, replaces: 0, grants: 1 });
  });
});
