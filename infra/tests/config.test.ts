import { afterEach, expect, it } from "vitest";
import * as pulumi from "@pulumi/pulumi";
import { loadConfig } from "../src/config.ts";

pulumi.runtime.setMocks(
  { newResource: (args) => ({ id: args.name, state: args.inputs }), call: (args) => args.inputs },
  "pi-durable-aws",
  "test",
);
function configure(values: Record<string, string> = {}) {
  pulumi.runtime.setAllConfig({ "pi-durable-aws:accountId": "123456789012", ...values });
}
afterEach(() => pulumi.runtime.setAllConfig({}));
it("a new stack defaults to retained data and us-east-1", () => {
  configure();
  expect(loadConfig()).toMatchObject({
    allowDataDeletion: false,
    region: "us-east-1",
    schema: "durable",
  });
});
it("requires a worker policy array and preserves valid statements", () => {
  const statement = {
    Effect: "Allow",
    Action: ["example:Read"],
    Resource: ["arn:example:resource"],
  };
  configure({ "pi-durable-aws:workerPolicyStatements": JSON.stringify(statement) });
  expect(loadConfig).toThrow("workerPolicyStatements must be an array");
  configure({ "pi-durable-aws:workerPolicyStatements": JSON.stringify([statement]) });
  expect(loadConfig().workerPolicyStatements).toEqual([statement]);
});
it("checks resource inputs and accepts explicit deletion opt-in", () => {
  const invalid: Record<string, string>[] = [
    { "aws:region": "us-west-2" },
    { "pi-durable-aws:accountId": "not-an-account" },
    { "pi-durable-aws:prefix": "a".repeat(28) },
    { "pi-durable-aws:schema": "public" },
  ];
  for (const values of invalid) {
    configure(values);
    expect(loadConfig).toThrow();
  }
  configure({
    "pi-durable-aws:prefix": "a".repeat(27),
    "pi-durable-aws:allowDataDeletion": "true",
  });
  expect(loadConfig()).toMatchObject({ allowDataDeletion: true });
});
