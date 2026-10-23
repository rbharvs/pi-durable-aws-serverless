import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["infra/tests/**/*.test.ts"],
    passWithNoTests: false,
    allowOnly: false,
    expect: { requireAssertions: true },
    maxWorkers: 1,
    fileParallelism: false,
    testTimeout: 60000,
  },
});
