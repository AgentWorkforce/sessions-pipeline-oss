import { afterEach, expect, test, vi } from "vitest";
import { createLogger } from "../src/lib/logger.js";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

test.each([
  {},
  { posthogApiKey: "test-only" },
  { posthogHost: "https://telemetry.example.invalid" },
])("does not export without both explicit host and key: %j", async (options) => {
  vi.spyOn(console, "info").mockImplementation(() => {});
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const logger = createLogger({ environment: "test", ...options }, "test");
  logger.info("metadata only");
  await logger.flush();
  expect(fetch).not.toHaveBeenCalled();
});

test("exports to the explicitly configured host", async () => {
  vi.spyOn(console, "info").mockImplementation(() => {});
  const fetch = vi.fn().mockResolvedValue(new Response(null, { status: 200 }));
  vi.stubGlobal("fetch", fetch);
  const logger = createLogger({ environment: "test", posthogApiKey: "test-only", posthogHost: "https://telemetry.example.invalid/" }, "test");
  logger.info("metadata only");
  await logger.flush();
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch.mock.calls[0]?.[0]).toBe("https://telemetry.example.invalid/i/v1/logs");
});
