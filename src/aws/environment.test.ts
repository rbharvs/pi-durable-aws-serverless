import { expect, it } from "vitest";
import { awsTestEnv } from "../../tests/aws-env.ts";

it("AWS test workers exclude ambient credentials, profiles, endpoints and shared files", () => {
  expect(
    Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("AWS_"))),
  ).toEqual(awsTestEnv);
  expect(process.env.AWS_CONFIG_FILE).toBe("/dev/null/pi-durable-test-config");
  expect(process.env.AWS_SHARED_CREDENTIALS_FILE).toBe("/dev/null/pi-durable-test-credentials");
});
