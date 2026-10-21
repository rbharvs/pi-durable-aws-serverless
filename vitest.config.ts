import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    setupFiles: ["./tests/aws-setup.ts"],
    include: ["src/**/*.test.ts"],
    passWithNoTests: false,
    allowOnly: false,
    expect: { requireAssertions: true },
    maxWorkers: 1,
    fileParallelism: false,
    testTimeout: 60000,
    hookTimeout: 30000,
  },
});
