import { expect, test } from "bun:test";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { githubJSON, REPOSITORY, validateRequest, verifyAncestry, verifySourceCI } from "../../../scripts/check-image-release.mjs";
const sha = "a".repeat(40);
const env = () => ({ GITHUB_REPOSITORY: REPOSITORY, GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REF: "refs/heads/main", GITHUB_ACTOR: "khaliqgant", GITHUB_TRIGGERING_ACTOR: "khaliqgant", GITHUB_WORKFLOW_REF: `${REPOSITORY}/.github/workflows/publish-image.yml@refs/heads/main`, GITHUB_SHA: "b".repeat(40), SOURCE_SHA: sha, CONFIRM: "publish 96bulls-services", APPROVAL_NOTE: "Approved in operator chat" });
const run = () => ({ id: 123, run_attempt: 1, head_sha: sha, head_branch: "main", event: "push", repository: { full_name: REPOSITORY }, head_repository: { full_name: REPOSITORY }, status: "completed", conclusion: "success" });
const jobs = () => ({ total_count: 1, jobs: [{ name: "checks", head_sha: sha, status: "completed", conclusion: "success" }] });
const api = (runs = [run()], jobData = jobs()) => async (path: string) => path.includes("/workflows/") ? { workflow_runs: runs } : path.includes("/jobs?") ? jobData : runs.reduce((a, b) => a.id > b.id ? a : b);
test("accepts an explicit owner/main/source release with a public approval reference", () => expect(validateRequest(env())).toEqual({ sourceSha: sha, workflowSha: "b".repeat(40), note: "Approved in operator chat" }));
for (const [field, value] of Object.entries({ GITHUB_REPOSITORY: "fork/sessions-pipeline-oss", GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/topic", GITHUB_ACTOR: "another-actor", GITHUB_TRIGGERING_ACTOR: "another-actor", GITHUB_WORKFLOW_REF: `${REPOSITORY}/.github/workflows/other.yml@refs/heads/main`, CONFIRM: "publish", GITHUB_SHA: "bad" })) {
  test(`refuses foreign ${field}`, () => expect(() => validateRequest({ ...env(), [field]: value })).toThrow());
}
for (const value of ["", "main", "a".repeat(7), "A".repeat(40), sha + "\n", "a".repeat(39) + ";"]) test(`refuses invalid source ${JSON.stringify(value)}`, () => expect(() => validateRequest({ ...env(), SOURCE_SHA: value })).toThrow());
for (const note of ["", "  ", "line\norg.opencontainers.image.revision=other", "line\rnext", "line\u2028next", "x".repeat(241)]) test(`refuses unsafe note ${JSON.stringify(note)}`, () => expect(() => validateRequest({ ...env(), APPROVAL_NOTE: note })).toThrow());
for (const failingCommand of ["fetch", "cat-file", "merge-base"]) {
 test(`sanitizes ${failingCommand} failures and exercises the ancestry gate`, () => {
  let message = ""; const calls: string[] = [];
  try { verifyAncestry(sha, (_command: string, args: string[]) => { calls.push(args[0]); if(args[0] === failingCommand) throw new Error("private diagnostic"); }); }
  catch (error) { message = (error as Error).message; }
  expect(calls).toEqual(["fetch", "cat-file", "merge-base"].slice(0,["fetch", "cat-file", "merge-base"].indexOf(failingCommand)+1));
  expect(message).toBe(`Image release refused: ${failingCommand === "fetch" ? "cannot fetch current main" : "source is not a commit on current main"}`);
  expect(message).not.toContain("private diagnostic");
 });
}
test("accepts only the latest exact-source successful main run and its jobs", async () => expect(await verifySourceCI(sha, api())).toBe(123));
for (const patch of [{ head_sha: "c".repeat(40) }, { head_branch: "topic" }, { event: "pull_request" }, { repository: { full_name: "fork/repo" } }, { head_repository: { full_name: "fork/repo" } }, { status: "in_progress" }, { conclusion: "failure" }, { conclusion: "cancelled" }, { conclusion: "skipped" }, { run_attempt: 0 }, { id: -1 }]) {
  test(`refuses invalid CI ${JSON.stringify(patch)}`, async () => { await expect(verifySourceCI(sha, api([{ ...run(), ...patch }]))).rejects.toThrow(); });
}
test("does not reuse an older green run after a newer failure", async () => { await expect(verifySourceCI(sha, api([run(), { ...run(), id: 124, conclusion: "failure" }]))).rejects.toThrow(); });
for (const data of [{}, { workflow_runs: [] }]) test(`refuses missing CI ${JSON.stringify(data)}`, async () => { await expect(verifySourceCI(sha, async () => data)).rejects.toThrow(); });
for (const patch of [{ name: "other" }, { head_sha: "c".repeat(40) }, { status: "in_progress" }, { conclusion: "skipped" }, { conclusion: "failure" }]) {
  test(`refuses unverified job ${JSON.stringify(patch)}`, async () => { await expect(verifySourceCI(sha, api([run()], { total_count: 1, jobs: [{ ...jobs().jobs[0], ...patch }] }))).rejects.toThrow(); });
}
test("refuses incomplete job pagination", async () => { await expect(verifySourceCI(sha, api([run()], { ...jobs(), total_count: 2 }))).rejects.toThrow(); });
test("GitHub metadata errors fail closed without reflecting response bodies", async () => { const get = githubJSON("test-only", async () => new Response("private diagnostic", { status: 403 })); let message = ""; try { await get("/repos/test"); } catch(error) { message = (error as Error).message; }
 expect(message).toBe("Image release refused: GitHub metadata HTTP 403"); expect(message).not.toContain("private diagnostic"); });
test("rejects malformed API JSON", async () => { const get = githubJSON("test-only", async () => new Response("not json")); await expect(get("/repos/test")).rejects.toThrow("invalid GitHub metadata"); });

test("refuses a rerun that starts during validation", async () => {
 const get = async (path: string) => path.includes("/workflows/") ? {workflow_runs:[run()]} : path.includes("/jobs?") ? jobs() : {...run(),run_attempt:2,status:"in_progress"};
 await expect(verifySourceCI(sha,get)).rejects.toThrow("changed during validation");
});

for (const status of ["in_progress", "completed"]) test(`refuses a newer CI run appearing mid-validation: ${status}`, async () => {
 let lists=0; const newer={...run(),id:124,status,conclusion:status === "completed" ? "failure" : null};
 const get=async(path:string)=>path.includes("/workflows/") ? {workflow_runs:++lists===1 ? [run()] : [newer,run()]} : path.includes("/jobs?") ? jobs() : run();
 await expect(verifySourceCI(sha,get)).rejects.toThrow("changed during validation");
});

test("documented verification stops if build provenance fails", () => {
 const doc=readFileSync(new URL("../../../docs/services-image-release.md",import.meta.url),"utf8");
 const block=doc.match(/```bash\n([\s\S]*?)```/)?.[1]; expect(block).toBeDefined();
 const dir=mkdtempSync(join(tmpdir(),"image-verification-"));
 try {
  const script=`n=0; gh() { n=$((n+1)); if [[ $n == 1 ]]; then return 42; fi; echo unexpected-later-check; }; jq() { echo unexpected-later-check; };\n${block}`;
  const result=Bun.spawnSync(["bash","-c",script],{cwd:dir,env:{PATH:process.env.PATH??"",image:"test",source_sha:sha,workflow_sha:sha,ci_run_id:"123"},stdout:"pipe",stderr:"pipe"});
  expect(result.exitCode).toBe(42);expect(result.stdout.toString()).not.toContain("unexpected-later-check");
 } finally {rmSync(dir,{recursive:true,force:true});}
});
