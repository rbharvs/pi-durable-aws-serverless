import { createHash } from "node:crypto";

export type HarnessId = string & { readonly __harness: unique symbol };

export type RequestId = string & { readonly __request: unique symbol };

export type Sha256 = string & { readonly __sha256: unique symbol };

export function check(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}

export function object(value: unknown): Record<string, unknown> {
  check(typeof value === "object" && value !== null && !Array.isArray(value), "Expected object");

  return value as Record<string, unknown>;
}

export function text(value: unknown): string {
  check(typeof value === "string", "Expected string");

  return value;
}

export function integer(value: unknown): number {
  check(
    typeof value === "number" || (typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value)),
    "Expected integer",
  );

  const n = Number(value);

  check(Number.isSafeInteger(n) && n >= 0, "Unsafe integer");

  return n;
}

export function harnessId(value: unknown): HarnessId {
  const s = text(value);

  check(/^[a-zA-Z0-9_-]{1,80}$/.test(s), "Invalid Harness ID");

  return s as HarnessId;
}

export function requestId(value: unknown): RequestId {
  const s = text(value);

  check(/^[a-zA-Z0-9_.:-]{1,128}$/.test(s), "Invalid request ID");

  return s as RequestId;
}

export function sha(value: string | Uint8Array): Sha256 {
  return createHash("sha256").update(value).digest("hex") as Sha256;
}

export function digest(value: unknown): Sha256 {
  const s = text(value);

  check(/^[a-f0-9]{64}$/.test(s), "Invalid digest");

  return s as Sha256;
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return undefined;

  return typeof error.code === "string" ? error.code : undefined;
}
