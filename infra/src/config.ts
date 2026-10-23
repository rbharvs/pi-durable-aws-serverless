import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";

export function loadConfig() {
  const config = new pulumi.Config();
  const awsConfig = new pulumi.Config("aws");
  const region = awsConfig.get("region") ?? "us-east-1";
  const accountId = config.require("accountId");
  const prefix = config.get("prefix") ?? "pi-durable";
  const schema = config.get("schema") ?? "durable";
  const allowDataDeletion = config.getBoolean("allowDataDeletion") ?? false;
  if (region !== "us-east-1") throw new Error("This stack uses us-east-1");
  if (!/^\d{12}$/.test(accountId)) throw new Error("accountId must be a 12-digit AWS account ID");
  // S3 bucketPrefix allows 37 characters, including our ten-character suffix.
  if (!/^[a-z][a-z0-9-]{2,26}$/.test(prefix)) {
    throw new Error("prefix must be 3–27 lowercase name characters");
  }
  if (
    !/^[a-z][a-z0-9_]{0,62}$/.test(schema) ||
    /^(public|pg_.*|information_schema|sys)$/.test(schema)
  ) {
    throw new Error("Use a dedicated lowercase database schema");
  }
  const workerPolicyStatements =
    config.getObject<aws.iam.PolicyStatement[]>("workerPolicyStatements") ?? [];
  if (!Array.isArray(workerPolicyStatements)) {
    throw new Error("workerPolicyStatements must be an array");
  }
  return {
    region: region as aws.Region,
    profile: awsConfig.get("profile"),
    accountId,
    prefix,
    schema,
    allowDataDeletion,
    workerEnvironment: config.getSecretObject<Record<string, string>>("workerEnvironment") ?? {},
    workerPolicyStatements,
  };
}

export type Config = ReturnType<typeof loadConfig>;
