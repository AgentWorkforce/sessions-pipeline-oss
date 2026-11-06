import { expect, test } from "bun:test";
import { canonicalProjectPublisherConfig } from "../deployment.ts";
import { canonicalProjectPublisherSettings } from "../settings.ts";
import worker from "../worker.ts";
const input = { hyperdriveId: "a".repeat(32), schemaGeneration: "e".repeat(64), queueName: "relayhistory-canonical-project-events",
  scope: { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b", workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" } };
test("rendered publisher is private, default off, and uses only external bindings", async () => {
  const config = canonicalProjectPublisherConfig(input);
  expect(config.workers_dev).toBe(false);
  expect(config.routes).toEqual([]);
  expect(config.queues.producers).toEqual([{ binding: "CANONICAL_PROJECT_EVENTS", queue: input.queueName }]);
  expect(canonicalProjectPublisherSettings(config.vars)).toBeNull();
  await worker.scheduled({}, {}); // No connection/queue bindings needed while off.
  expect(() => canonicalProjectPublisherSettings({ ...config.vars, INGEST_CANONICAL_PROJECT_EVENTS_ENABLED: "true" })).toThrow("canonical_project_publisher_not_configured");
  for (const bad of [{ ...input, hyperdriveId: "wrong" }, { ...input, schemaGeneration: "latest" }, { ...input, queueName: "relayhistory-project-events" }, { ...input, queueName: input.queueName + "-dlq" }])
    expect(() => canonicalProjectPublisherConfig(bad)).toThrow("canonical_project_publisher_config_invalid");
});
