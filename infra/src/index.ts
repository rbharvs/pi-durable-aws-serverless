import * as aws from "@pulumi/aws";
import { loadConfig } from "./config.js";
import { artifact } from "./artifacts.js";
import { createStack } from "./stack.js";

const config = loadConfig();
const provider = new aws.Provider("deployment", {
  region: config.region,
  profile: config.profile,
  allowedAccountIds: [config.accountId],
  defaultTags: { tags: { Project: "pi-durable-aws", Deployment: config.prefix } },
});
const resources = createStack(config, provider, {
  api: artifact("api"),
  worker: artifact("worker"),
  reconciler: artifact("reconciler"),
});

export const clusterIdentifier = resources.cluster.identifier;
export const dsqlHost = resources.host;
export const payloadBucket = resources.bucket.bucket;
export const apiArn = resources.functions.api.arn;
export const functionNames = Object.values(resources.functions).map((fn) => fn.name);
export const scheduleName = resources.schedule.name;
export const scheduleGroup = resources.group.name;
export const schema = config.schema;
