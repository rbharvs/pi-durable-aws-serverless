import { expect, it } from "vitest";
import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { createStack } from "../src/stack.ts";
import type { Config } from "../src/config.ts";

const resources: pulumi.runtime.MockResourceArgs[] = [];
const options = new Map<string, pulumi.ResourceTransformationArgs>();
pulumi.runtime.setMocks({
  newResource(args) {
    resources.push(args);
    const state = {
      ...args.inputs,
      arn: `arn:mock:${args.name}`,
      name: args.inputs.name ?? args.name,
      bucket: args.inputs.bucket ?? "test-payloads",
      identifier: "testcluster",
    };
    return { id: args.name, state };
  },
  call(args) {
    throw new Error(`Unexpected cloud lookup: ${args.token}`);
  },
});
const cfg: Config = {
  accountId: "123456789012",
  profile: "test",
  region: "us-east-1",
  prefix: "test-app",
  schema: "durable",
  allowDataDeletion: false,
  workerEnvironment: pulumi.secret({
    OPENAI_API_KEY: "synthetic-openai-only",
    ANTHROPIC_API_KEY: "synthetic-anthropic-only",
  }),
  workerPolicyStatements: [],
};
const code = {
  api: new pulumi.asset.FileAsset("/prebuilt/api.cjs"),
  worker: new pulumi.asset.FileAsset("/prebuilt/worker.cjs"),
  reconciler: new pulumi.asset.FileAsset("/prebuilt/reconciler.cjs"),
};
const archives = {
  api: new pulumi.asset.AssetArchive({ "api.cjs": code.api }),
  worker: new pulumi.asset.AssetArchive({ "worker.cjs": code.worker }),
  reconciler: new pulumi.asset.AssetArchive({ "reconciler.cjs": code.reconciler }),
};
function resource(name: string) {
  const found = resources.find((r) => r.name === name);
  if (!found) throw new Error(`Missing resource ${name}`);
  return found.inputs;
}
async function deploy(settings: Config) {
  resources.length = 0;
  options.clear();
  pulumi.runtime.registerStackTransformation((args) => {
    options.set(args.name, args);
    return undefined;
  });
  const result = createStack(
    settings,
    new aws.Provider("test-provider", { region: "us-east-1" }),
    archives,
  );
  await new Promise((resolve) =>
    pulumi.all([...options.values()].map((args) => args.resource.urn)).apply(resolve),
  );
  return result;
}

it("default stack has unreserved compute and retained data and bucket controls", async () => {
  await pulumi.runtime.runInPulumiStack(async () => {
    await deploy(cfg);
  });
  for (const name of ["api", "worker", "reconciler"]) {
    expect(resource(name).reservedConcurrentExecutions).toBe(-1);
  }
  expect(resource("reconciliation").state).toBe("ENABLED");
  expect(resource("database")).toMatchObject({
    deletionProtectionEnabled: true,
    forceDestroy: false,
  });
  expect(resource("payloads")).toMatchObject({ forceDestroy: false });
  expect(resource("payloads-private")).toMatchObject({
    blockPublicAcls: true,
    blockPublicPolicy: true,
    ignorePublicAcls: true,
    restrictPublicBuckets: true,
  });
  expect(resource("payloads-owner").rule).toEqual({ objectOwnership: "BucketOwnerEnforced" });
  for (const name of [
    "database",
    "payloads",
    "payloads-private",
    "payloads-policy",
    "payloads-versions",
    "payloads-owner",
    "payloads-encryption",
  ]) {
    expect(options.get(name)?.opts).toMatchObject({ protect: true, retainOnDelete: true });
  }
  expect(resource("payloads-encryption").rules[0].applyServerSideEncryptionByDefault).toEqual({
    sseAlgorithm: "AES256",
  });
  expect(resource("payloads-versions").versioningConfiguration.status).toBe("Enabled");
});

it("active stack wires file bundles, narrow authority and one-minute scheduling", async () => {
  await pulumi.runtime.runInPulumiStack(async () => {
    const deployed = await deploy(cfg);
    expect(await pulumi.isSecret(deployed.functions.worker.environment)).toBe(true);
    expect(
      await new Promise((resolve) => deployed.functions.worker.environment.apply(resolve)),
    ).toMatchObject({
      variables: {
        OPENAI_API_KEY: "synthetic-openai-only",
        ANTHROPIC_API_KEY: "synthetic-anthropic-only",
      },
    });
  });
  expect(resources.filter((r) => r.type === "aws:lambda/function:Function")).toHaveLength(3);
  expect(
    resources.some((r) => /functionUrl|apigateway|cloudfront|ec2|secretsmanager|kms/.test(r.type)),
  ).toBe(false);
  for (const name of ["api", "worker", "reconciler"]) {
    expect(resource(name)).toMatchObject({
      runtime: "nodejs24.x",
      architectures: ["arm64"],
      handler: `${name}.handler`,
    });
    const assets = await resource(name).code.assets;
    expect(Object.keys(assets)).toEqual([`${name}.cjs`]);
    expect(await assets[`${name}.cjs`].path).toBe(`/prebuilt/${name}.cjs`);
    expect(resource(name).reservedConcurrentExecutions).toBe(-1);
    const scheduleDependencies = options.get("reconciliation")?.opts.dependsOn as pulumi.Resource[];
    expect(scheduleDependencies).toContain(options.get(name)?.resource);
    const dependencies = options.get(name)?.opts.dependsOn as pulumi.Resource[];
    expect(dependencies).toContain(options.get(`${name}-access`)?.resource);
    expect(dependencies).toContain(options.get("payloads-policy")?.resource);
    const statement = JSON.parse(resource(`${name}-access`).policy).Statement;
    expect(statement).toHaveLength(name === "api" ? 5 : 3);
    expect(
      statement.find((s: { Action: string[] }) => s.Action.includes("logs:PutLogEvents")).Resource,
    ).toEqual([`arn:mock:${name}-logs:*`]);
    if (name !== "worker") {
      expect(
        statement.find((s: { Action: string[] }) => s.Action.includes("lambda:InvokeFunction"))
          .Resource,
      ).toEqual(["arn:mock:worker"]);
    }
    if (name !== "reconciler") {
      expect(
        statement.find((s: { Action: string[] }) => s.Action.includes("s3:PutObject")).Resource,
      ).toEqual(["arn:mock:payloads/p/*"]);
    }
    expect(
      statement.find((s: { Action: string[] }) => s.Action.includes("dsql:DbConnectAdmin"))
        .Resource,
    ).toEqual(["arn:mock:database"]);
    expect(statement.flatMap((s: { Action: string[] }) => s.Action)).not.toContain(
      "s3:DeleteObject",
    );
    expect(statement.flatMap((s: { Action: string[] }) => s.Action)).not.toContain("s3:ListBucket");
  }
  expect(
    JSON.parse(resource("worker-access").policy).Statement.some((s: { Action: string[] }) =>
      s.Action.includes("lambda:InvokeFunction"),
    ),
  ).toBe(false);
  expect(
    JSON.parse(resource("reconciler-access").policy).Statement.some((s: { Action: string[] }) =>
      s.Action.some((a) => a.startsWith("s3:")),
    ),
  ).toBe(false);
  expect(resource("api").environment.variables).toMatchObject({
    DSQL_HOST: "testcluster.dsql.us-east-1.on.aws",
    DSQL_SCHEMA: "durable",
    PAYLOAD_BUCKET: "test-payloads",
    WORKER_NAME: "arn:mock:worker",
  });
  expect(resource("api").environment.variables).not.toHaveProperty("AWS_REGION");
  for (const name of ["api", "reconciler"]) {
    expect(resource(name).environment.variables).not.toHaveProperty("OPENAI_API_KEY");
    expect(resource(name).environment.variables).not.toHaveProperty("ANTHROPIC_API_KEY");
  }
  expect(resource("reconciliation")).toMatchObject({
    scheduleExpression: "rate(1 minute)",
    state: "ENABLED",
    flexibleTimeWindow: { mode: "OFF" },
    target: { arn: "arn:mock:reconciler", input: "{}" },
  });
  expect(
    JSON.parse(resource("scheduler-role").assumeRolePolicy).Statement[0].Condition.StringEquals,
  ).toEqual({
    "aws:SourceAccount": cfg.accountId,
    "aws:SourceArn": "arn:mock:reconciliation-group",
  });
  const policy = JSON.parse(resource("payloads-policy").policy).Statement;
  expect(policy).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        Effect: "Deny",
        Action: "s3:PutObject",
        Resource: "arn:mock:payloads/p/*",
        Condition: { Null: { "s3:if-none-match": "true" } },
      }),
      expect.objectContaining({
        Effect: "Deny",
        Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      }),
    ]),
  );
});

it("teardown opt-in removes safeguards without changing compute or force-emptying data", async () => {
  await pulumi.runtime.runInPulumiStack(async () => {
    await deploy({
      ...cfg,
      allowDataDeletion: true,
    });
  });
  for (const name of ["api", "worker", "reconciler"]) {
    expect(resource(name).reservedConcurrentExecutions).toBe(-1);
  }
  expect(resource("reconciliation").state).toBe("ENABLED");
  expect(resource("database").deletionProtectionEnabled).toBe(false);
  expect(resource("database").forceDestroy).toBe(false);
  for (const name of [
    "database",
    "payloads",
    "payloads-private",
    "payloads-policy",
    "payloads-versions",
    "payloads-owner",
    "payloads-encryption",
  ]) {
    expect(options.get(name)?.opts).toMatchObject({ protect: false, retainOnDelete: false });
  }
  expect(
    JSON.parse(resource("payloads-policy").policy).Statement.some(
      (statement: { Action: string | string[] }) => statement.Action.includes("s3:DeleteObject"),
    ),
  ).toBe(false);
  expect(resource("payloads").forceDestroy).toBe(false);
});
