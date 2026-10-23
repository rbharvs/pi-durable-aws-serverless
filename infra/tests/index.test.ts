import { expect, it, vi } from "vitest";
import * as pulumi from "@pulumi/pulumi";

vi.mock("../src/artifacts.ts", () => ({
  artifact: (name: string) =>
    new pulumi.asset.AssetArchive({
      [`${name}.cjs`]: new pulumi.asset.FileAsset(`/prebuilt/${name}.cjs`),
    }),
}));
const resources: pulumi.runtime.MockResourceArgs[] = [];
pulumi.runtime.setMocks(
  {
    newResource(args) {
      resources.push(args);
      return {
        id: args.name,
        state: {
          ...args.inputs,
          arn: `arn:mock:${args.name}`,
          identifier: "testcluster",
          bucket: "test-bucket",
        },
      };
    },
    call(args) {
      throw new Error(`Unexpected lookup: ${args.token}`);
    },
  },
  "pi-durable-aws",
  "test",
);

it("the actual entry uses the selected profile, account restriction and unreserved compute", async () => {
  pulumi.runtime.setAllConfig({
    "pi-durable-aws:accountId": "123456789012",
    "aws:profile": "test-deploy",
  });
  await pulumi.runtime.runInPulumiStack(async () => {
    const stack = await import("../src/index.ts");
    await new Promise((resolve) =>
      pulumi.all([stack.functionNames, stack.scheduleName]).apply(resolve),
    );
  });
  const provider = resources.find((resource) => resource.type === "pulumi:providers:aws")!;
  expect(provider.inputs).toMatchObject({ region: "us-east-1", profile: "test-deploy" });
  expect(JSON.parse(provider.inputs.allowedAccountIds)).toEqual(["123456789012"]);
  const functions = resources.filter(
    (resource) => resource.type === "aws:lambda/function:Function",
  );
  expect(functions).toHaveLength(3);
  expect(functions.every((fn) => fn.inputs.reservedConcurrentExecutions === -1)).toBe(true);
  expect(resources.find((resource) => resource.name === "reconciliation")?.inputs.state).toBe(
    "ENABLED",
  );
});
