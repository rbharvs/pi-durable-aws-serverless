import { awsTestEnv } from "./aws-env.ts";

// Static credentials alone do not stop SDK shared-config reads or ambient endpoint selection.
// This changes only the test worker's environment, before importing any test/SDK module.
for (const name of Object.keys(process.env)) if (name.startsWith("AWS_")) delete process.env[name];
Object.assign(process.env, awsTestEnv);
