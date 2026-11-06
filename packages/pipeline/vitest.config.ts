import { defineConfig } from "vitest/config";

// Mirrors relayhistory-cloud packages/relayhistory/vitest.config.ts (minus the
// cloud-client alias, which nothing here imports).
export default defineConfig({
  test: {
    // PGlite boot can exceed Vitest's 10s default when DB-backed files start together.
    hookTimeout: 30_000,
    include: ["test/**/*.test.ts"],
  },
});
