import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import type { Config } from "./config.js";

export type FunctionName = "api" | "worker" | "reconciler";
const policy = (Statement: unknown[]) => JSON.stringify({ Version: "2012-10-17", Statement });
const allow = (Action: string[], Resource: pulumi.Input<string>[]) => ({
  Effect: "Allow",
  Action,
  Resource,
});

export function createStack(
  cfg: Config,
  provider: aws.Provider,
  code: Record<FunctionName, pulumi.asset.Archive>,
) {
  const opts = { provider };
  const retained = {
    ...opts,
    protect: !cfg.allowDataDeletion,
    retainOnDelete: !cfg.allowDataDeletion,
  };
  const cluster = new aws.dsql.Cluster(
    "database",
    {
      deletionProtectionEnabled: !cfg.allowDataDeletion,
      forceDestroy: false,
      tags: { Name: `${cfg.prefix}-database` },
    },
    retained,
  );
  const bucket = new aws.s3.Bucket(
    "payloads",
    {
      bucketPrefix: `${cfg.prefix}-payloads-`,
      forceDestroy: false,
    },
    retained,
  );
  const access = new aws.s3.BucketPublicAccessBlock(
    "payloads-private",
    {
      bucket: bucket.id,
      blockPublicAcls: true,
      blockPublicPolicy: true,
      ignorePublicAcls: true,
      restrictPublicBuckets: true,
    },
    retained,
  );
  const ownership = new aws.s3.BucketOwnershipControls(
    "payloads-owner",
    {
      bucket: bucket.id,
      rule: { objectOwnership: "BucketOwnerEnforced" },
    },
    retained,
  );
  const versioning = new aws.s3.BucketVersioning(
    "payloads-versions",
    {
      bucket: bucket.id,
      versioningConfiguration: { status: "Enabled" },
    },
    retained,
  );
  const encryption = new aws.s3.BucketServerSideEncryptionConfiguration(
    "payloads-encryption",
    {
      bucket: bucket.id,
      rules: [{ applyServerSideEncryptionByDefault: { sseAlgorithm: "AES256" } }],
    },
    retained,
  );
  const bucketPolicy = new aws.s3.BucketPolicy(
    "payloads-policy",
    {
      bucket: bucket.id,
      policy: bucket.arn.apply((arn) =>
        policy([
          {
            Effect: "Deny",
            Principal: "*",
            Action: "s3:*",
            Resource: [arn, `${arn}/*`],
            Condition: { Bool: { "aws:SecureTransport": "false" } },
          },
          // Runtime publications are conditional single PUTs; staging in/* supports multipart.
          {
            Effect: "Deny",
            Principal: "*",
            Action: "s3:PutObject",
            Resource: `${arn}/p/*`,
            Condition: { Null: { "s3:if-none-match": "true" } },
          },
          ...(!cfg.allowDataDeletion
            ? [
                {
                  Effect: "Deny",
                  Principal: "*",
                  Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
                  Resource: `${arn}/p/*`,
                },
              ]
            : []),
        ]),
      ),
    },
    retained,
  );
  const host = pulumi.interpolate`${cluster.identifier}.dsql.${cfg.region}.on.aws`;

  function createFunction(name: FunctionName, worker?: aws.lambda.Function) {
    const physicalName = `${cfg.prefix}-${name}`;
    const logs = new aws.cloudwatch.LogGroup(
      `${name}-logs`,
      {
        name: `/aws/lambda/${physicalName}`,
        retentionInDays: 30,
      },
      opts,
    );
    const role = new aws.iam.Role(
      `${name}-role`,
      {
        name: `${physicalName}-exec`,
        assumeRolePolicy: policy([
          {
            Effect: "Allow",
            Action: "sts:AssumeRole",
            Principal: { Service: "lambda.amazonaws.com" },
          },
        ]),
      },
      opts,
    );
    const statements: pulumi.Input<unknown>[] = [
      allow(["dsql:DbConnectAdmin"], [cluster.arn]),
      allow(["logs:CreateLogStream", "logs:PutLogEvents"], [pulumi.interpolate`${logs.arn}:*`]),
    ];
    if (name !== "reconciler") {
      statements.push(
        allow(["s3:GetObject", "s3:PutObject"], [pulumi.interpolate`${bucket.arn}/p/*`]),
      );
    }
    if (name === "api") {
      statements.push(allow(["s3:GetObject"], [pulumi.interpolate`${bucket.arn}/in/*`]));
    }
    if (worker) statements.push(allow(["lambda:InvokeFunction"], [worker.arn]));
    if (name === "worker") statements.push(...cfg.workerPolicyStatements);
    const executionPolicy = new aws.iam.RolePolicy(
      `${name}-access`,
      {
        role: role.id,
        policy: pulumi.jsonStringify({ Version: "2012-10-17", Statement: statements }),
      },
      opts,
    );
    const environment = pulumi
      .output(name === "worker" ? cfg.workerEnvironment : {})
      .apply((extra) => ({
        ...extra,
        DSQL_HOST: host,
        DSQL_SCHEMA: cfg.schema,
        ...(name === "reconciler" ? {} : { PAYLOAD_BUCKET: bucket.bucket }),
        ...(worker ? { WORKER_NAME: worker.arn } : {}),
      }));
    const fn = new aws.lambda.Function(
      name,
      {
        name: physicalName,
        runtime: "nodejs24.x",
        architectures: ["arm64"],
        handler: `${name}.handler`,
        code: code[name],
        role: role.arn,
        memorySize: name === "worker" ? 1024 : 512,
        timeout: name === "worker" ? 900 : name === "reconciler" ? 180 : 120,
        // Explicitly remove any existing reservation; use the account concurrency pool.
        reservedConcurrentExecutions: -1,
        environment: { variables: environment },
      },
      {
        ...opts,
        dependsOn: [executionPolicy, logs, access, ownership, versioning, encryption, bucketPolicy],
      },
    );
    new aws.lambda.FunctionEventInvokeConfig(
      `${name}-async`,
      {
        functionName: fn.name,
        maximumEventAgeInSeconds: 60,
        maximumRetryAttempts: 0,
      },
      opts,
    );
    return fn;
  }
  const worker = createFunction("worker");
  const api = createFunction("api", worker);
  const reconciler = createFunction("reconciler", worker);
  const functions = { api, worker, reconciler };
  const group = new aws.scheduler.ScheduleGroup("reconciliation-group", { name: cfg.prefix }, opts);
  const schedulerRole = new aws.iam.Role(
    "scheduler-role",
    {
      name: `${cfg.prefix}-scheduler-exec`,
      assumeRolePolicy: group.arn.apply((arn) =>
        policy([
          {
            Effect: "Allow",
            Action: "sts:AssumeRole",
            Principal: { Service: "scheduler.amazonaws.com" },
            Condition: {
              StringEquals: { "aws:SourceAccount": cfg.accountId, "aws:SourceArn": arn },
            },
          },
        ]),
      ),
    },
    opts,
  );
  const schedulerPolicy = new aws.iam.RolePolicy(
    "scheduler-access",
    {
      role: schedulerRole.id,
      policy: reconciler.arn.apply((arn) => policy([allow(["lambda:InvokeFunction"], [arn])])),
    },
    opts,
  );
  const schedule = new aws.scheduler.Schedule(
    "reconciliation",
    {
      name: `${cfg.prefix}-reconcile`,
      groupName: group.name,
      scheduleExpression: "rate(1 minute)",
      flexibleTimeWindow: { mode: "OFF" },
      state: "ENABLED",
      target: {
        arn: reconciler.arn,
        roleArn: schedulerRole.arn,
        input: "{}",
        retryPolicy: { maximumEventAgeInSeconds: 60, maximumRetryAttempts: 0 },
      },
    },
    { ...opts, dependsOn: [schedulerPolicy, ...Object.values(functions)] },
  );
  return { cluster, bucket, host, functions, schedule, group };
}
