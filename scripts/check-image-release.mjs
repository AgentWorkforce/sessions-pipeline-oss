import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

export const REPOSITORY = "AgentWorkforce/sessions-pipeline-oss";
const SHA = /^[0-9a-f]{40}$/;
const refuse = (reason) => { throw new Error(`Image release refused: ${reason}`); };

export function validateRequest(env) {
  if (env.GITHUB_REPOSITORY !== REPOSITORY ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
      env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_ACTOR !== "khaliqgant" ||
      env.GITHUB_WORKFLOW_REF !== `${REPOSITORY}/.github/workflows/publish-image.yml@refs/heads/main` ||
      env.CONFIRM !== "publish 96bulls-services") refuse("dispatcher or workflow identity");
  if (typeof env.SOURCE_SHA !== "string" || !SHA.test(env.SOURCE_SHA) ||
      typeof env.GITHUB_SHA !== "string" || !SHA.test(env.GITHUB_SHA)) refuse("full lowercase source/workflow SHA required");
  const note = env.APPROVAL_NOTE;
  if (typeof note !== "string" || !note.trim() || Buffer.byteLength(note) > 240 ||
      /[\u0000-\u001f\u007f\u2028\u2029]/u.test(note)) refuse("single-line non-sensitive approval reference required");
  return { sourceSha: env.SOURCE_SHA, workflowSha: env.GITHUB_SHA, note: note.trim() };
}

export function verifyAncestry(sourceSha, run = execFileSync) {
  if (!SHA.test(sourceSha)) refuse("full source SHA required");
  try {
    run("git", ["fetch", "--no-tags", "origin", "main"], { stdio: "pipe" });
    run("git", ["cat-file", "-e", `${sourceSha}^{commit}`], { stdio: "pipe" });
    run("git", ["merge-base", "--is-ancestor", sourceSha, "origin/main"], { stdio: "pipe" });
  } catch { refuse("source is not a commit on current main"); }
}

export async function verifySourceCI(sourceSha, getJSON) {
  if (!SHA.test(sourceSha)) refuse("full source SHA required");
  const base = `/repos/${REPOSITORY}/actions`;
  const response = await getJSON(`${base}/workflows/ci.yml/runs?head_sha=${sourceSha}&event=push&branch=main&per_page=100`);
  if (!Array.isArray(response.workflow_runs)) refuse("missing source CI metadata");
  // Never fall back to an older green run when the newest matching run is red or pending.
  const matching = response.workflow_runs.filter(run => run.head_sha === sourceSha &&
    run.head_branch === "main" && run.event === "push" &&
    run.repository?.full_name === REPOSITORY && run.head_repository?.full_name === REPOSITORY);
  if (!matching.length) refuse("source has no official main CI run");
  if (matching.some(run => !Number.isSafeInteger(run.id) || run.id <= 0)) refuse("invalid CI identity");
  const latest = matching.reduce((a, b) => a.id > b.id ? a : b);
  if (latest.status !== "completed" || latest.conclusion !== "success" ||
      !Number.isSafeInteger(latest.run_attempt) || latest.run_attempt < 1) refuse("latest source CI is not successful");
  const jobs = await getJSON(`${base}/runs/${latest.id}/attempts/${latest.run_attempt}/jobs?per_page=100`);
  if (!Array.isArray(jobs.jobs) || jobs.total_count !== jobs.jobs.length || !jobs.jobs.length ||
      !jobs.jobs.every(job => job.head_sha === sourceSha && job.status === "completed" && job.conclusion === "success") ||
      !jobs.jobs.some(job => job.name === "checks")) refuse("source CI jobs are incomplete or unsuccessful");
  const current = await getJSON(`${base}/runs/${latest.id}`);
  if (current.head_sha !== sourceSha || current.run_attempt !== latest.run_attempt ||
      current.status !== "completed" || current.conclusion !== "success") refuse("source CI changed during validation");
  return latest.id;
}

export function githubJSON(token, fetcher = fetch) {
  if (!token) refuse("GitHub metadata token required");
  return async path => {
    const response = await fetcher(`https://api.github.com${path}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
      redirect: "error", signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) refuse(`GitHub metadata HTTP ${response.status}`);
    try { return await response.json(); } catch { refuse("invalid GitHub metadata"); }
  };
}

async function main() {
  const request = validateRequest(process.env);
  verifyAncestry(request.sourceSha);
  const ciRun = await verifySourceCI(request.sourceSha, githubJSON(process.env.GH_TOKEN));
  if (!process.env.GITHUB_OUTPUT) refuse("workflow output missing");
  appendFileSync(process.env.GITHUB_OUTPUT, `sha=${request.sourceSha}\nci_run_id=${ciRun}\napproval_note=${request.note}\n`);
  console.log(`Approved source ${request.sourceSha}; successful main CI run ${ciRun}.`);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
