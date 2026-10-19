import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["./tests/integration/global-setup.ts"],
    include: ["tests/integration/*.test.ts"],
    passWithNoTests: false,
    allowOnly: false,
    expect: { requireAssertions: true },
    maxWorkers: 1,
    fileParallelism: false,
    testTimeout: 60000,
    hookTimeout: 30000,
  },
});
