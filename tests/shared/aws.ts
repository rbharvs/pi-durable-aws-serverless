import type { AwsOptions } from "../../src/aws/config.ts";

/** Explicit static test-only credentials. No default credential-chain or provider calls. */
export const aws = {
  region: "eu-west-1",
  credentials: { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "offline-test-only-secret" },
} satisfies AwsOptions;
export const lambdaContext = {
  awsRequestId: "offline-test",
  getRemainingTimeInMillis: () => 45000,
};
