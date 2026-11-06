import { assertProjectEventScope, type CloudProjectEventMessage } from "./protocol.ts";
export interface CanonicalProjectPublisherEnv {
  INGEST_CANONICAL_PROJECT_EVENTS_ENABLED?: string;
  INGEST_CANONICAL_PROJECT_EVENTS_ORG_ID?: string;
  INGEST_CANONICAL_PROJECT_EVENTS_WORKSPACE_ID?: string;
  INGEST_EXPECT_GENERATION?: string;
  HYPERDRIVE?: { connectionString: string };
  CANONICAL_PROJECT_EVENTS?: { send(body: CloudProjectEventMessage): Promise<void> };
}
export function canonicalProjectPublisherSettings(env: CanonicalProjectPublisherEnv) {
  if (env.INGEST_CANONICAL_PROJECT_EVENTS_ENABLED !== "true") return null;
  const scope = { orgId: env.INGEST_CANONICAL_PROJECT_EVENTS_ORG_ID, workspaceId: env.INGEST_CANONICAL_PROJECT_EVENTS_WORKSPACE_ID };
  assertProjectEventScope(scope);
  if (!env.INGEST_EXPECT_GENERATION || !/^[a-f0-9]{64}$/.test(env.INGEST_EXPECT_GENERATION) || !env.HYPERDRIVE || !env.CANONICAL_PROJECT_EVENTS)
    throw new Error("canonical_project_publisher_not_configured");
  return { scope, schemaGeneration: env.INGEST_EXPECT_GENERATION, queue: env.CANONICAL_PROJECT_EVENTS, connectionString: env.HYPERDRIVE.connectionString };
}
