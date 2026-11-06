/**
 * PLAN ONLY. For an operator's reviewed SSH session when protected GitHub reviewers
 * are unavailable. There is deliberately no migration/apply branch or fallback.
 */
import { execFileSync } from "node:child_process";
import { generation } from "../src/hoopdb/migrate.ts";
import {
  releaseInputs,
  SCHEMA_RELEASE as P,
  refuse,
} from "../src/hoopdb/schema-release-contract.ts";
import {
  inspectRelease,
  releaseClient,
  releaseErrorCode,
} from "../src/hoopdb/schema-release-inspect.ts";
export async function operatorPlan() {
  const input = releaseInputs(process.env);
  if (input.mode !== "plan" || process.argv.length !== 2)
    refuse("operator_plan_only");
  if (
    execFileSync("git", ["rev-parse", "HEAD"], {
      encoding: "utf8",
      env: { PATH: process.env.PATH },
    }).trim() !== input.sha ||
    (await generation()) !== input.generation
  )
    refuse("release_checkout_generation");
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  try {
    await client.connect();
    const report = await inspectRelease(client, input.boundary);
    console.log(
      JSON.stringify(
        {
          version: 1,
          status: "planned",
          mode: "read_only_operator_plan",
          project: P.project,
          sha: input.sha,
          boundary: input.boundary,
          ...report,
        },
        null,
        2,
      ),
    );
  } finally {
    await client.end().catch(() => {});
  }
}
if (import.meta.main)
  operatorPlan().catch((error) => {
    console.log(
      JSON.stringify({
        version: 1,
        status: "refused",
        code: releaseErrorCode(error),
      }),
    );
    process.exitCode = 1;
  });
