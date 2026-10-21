import type { S3ClientConfig } from "@aws-sdk/client-s3";
import { check } from "../domain.ts";

export type AwsOptions = {
  readonly region: string;
  readonly credentials?: S3ClientConfig["credentials"];
};

export const REQUEST_MS = 20000;
export const transport = {
  requestTimeout: REQUEST_MS,
  throwOnRequestTimeout: true,
  connectionTimeout: 5000,
};

export function region(value: unknown): string {
  check(
    typeof value === "string" && value.length <= 32 && /^[a-z]{2}(?:-[a-z]+)+-\d+$/.test(value),
    "Invalid AWS region",
  );
  return value;
}

export function required(value: unknown, label: string): string {
  check(
    typeof value === "string" && value.length > 0 && value.trim() === value,
    `Missing ${label}`,
  );
  return value;
}

export function bucketName(value: unknown): string {
  const bucket = required(value, "payload bucket");
  check(
    bucket.length >= 3 &&
      bucket.length <= 63 &&
      /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(bucket) &&
      !bucket.includes("..") &&
      !/^\d+\.\d+\.\d+\.\d+$/.test(bucket),
    "Invalid payload bucket",
  );
  return bucket;
}

export function workerName(value: unknown): string {
  const name = required(value, "worker target");
  check(
    name.length <= 170 &&
      /^(?:arn:aws(?:-[a-z]+)*:lambda:[a-z0-9-]+:\d{12}:function:)?[A-Za-z0-9_-]{1,64}(?::[A-Za-z0-9_-]{1,128})?$/.test(
        name,
      ),
    "Invalid worker target",
  );
  return name;
}
