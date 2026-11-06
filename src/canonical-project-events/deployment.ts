import { assertProjectEventScope, type ProjectEventScope } from "./protocol.ts";

/** Render only. The sole production operator reviews and applies the config.
 * IDs/pins are non-secret; Hyperdrive retains the actual login outside state. */
export function canonicalProjectPublisherConfig(input: {
  hyperdriveId: string; schemaGeneration: string; scope: ProjectEventScope;
  queueName: string; enabled?: boolean;
}) {
  assertProjectEventScope(input.scope);
  if (!/^[a-f0-9]{32}$/.test(input.hyperdriveId) || !/^[a-f0-9]{64}$/.test(input.schemaGeneration) ||
      !/^relayhistory-canonical-project-events(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$/.test(input.queueName) || input.queueName.includes("-dlq"))
    throw new Error("canonical_project_publisher_config_invalid");
  return {
    name: "96bulls-canonical-project-events", main: "src/canonical-project-events/worker.ts",
    compatibility_date: "2026-07-02", compatibility_flags: ["nodejs_compat"],
    workers_dev: false, preview_urls: false, routes: [], triggers: { crons: ["* * * * *"] },
    hyperdrive: [{ binding: "HYPERDRIVE", id: input.hyperdriveId }],
    queues: { producers: [{ binding: "CANONICAL_PROJECT_EVENTS", queue: input.queueName }] },
    vars: { INGEST_CANONICAL_PROJECT_EVENTS_ENABLED: input.enabled === true ? "true" : "false",
      INGEST_EXPECT_GENERATION: input.schemaGeneration, INGEST_CANONICAL_PROJECT_EVENTS_ORG_ID: input.scope.orgId,
      INGEST_CANONICAL_PROJECT_EVENTS_WORKSPACE_ID: input.scope.workspaceId },
    observability: { enabled: true },
  };
}
