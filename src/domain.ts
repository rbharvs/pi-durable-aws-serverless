import { createHash, randomUUID } from "node:crypto";

/** Validated namespace identity; all receipts, leases, and stored history are scoped by it. */
export type HarnessId = string & { readonly __harness: unique symbol };

/** Caller-chosen idempotency key within a Harness ID; reuse only for equal canonical input. */
export type RequestId = string & { readonly __request: unique symbol };

export type OwnerToken = string & { readonly __owner: unique symbol };
export type Epoch = number & { readonly __epoch: unique symbol };
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

/** Validates namespace syntax without checking that the namespace exists. */
export function harnessId(value: unknown): HarnessId {
  const s = text(value);

  check(/^[a-zA-Z0-9_-]{1,80}$/.test(s), "Invalid Harness ID");

  return s as HarnessId;
}

/** Validates key syntax without looking up or reserving a receipt. */
export function requestId(value: unknown): RequestId {
  const s = text(value);

  check(/^[a-zA-Z0-9_.:-]{1,128}$/.test(s), "Invalid request ID");

  return s as RequestId;
}

/** Hashes `value` and returns the lowercase hex SHA-256 digest. */
export function computeSha256(value: string | Uint8Array): Sha256 {
  return createHash("sha256").update(value).digest("hex") as Sha256;
}

/** Validates that `value` is a lowercase hex SHA-256 digest. Does not hash. */
export function parseSha256(value: unknown): Sha256 {
  const s = text(value);

  check(/^[a-f0-9]{64}$/.test(s), "Invalid digest");

  return s as Sha256;
}

export function epoch(value: unknown): Epoch {
  return integer(value) as Epoch;
}

export function ownerToken(value: unknown = randomUUID()): OwnerToken {
  const token = text(value);
  check(
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(token),
    "Invalid owner UUID",
  );
  return token as OwnerToken;
}

/** A FIFO gate owns the connection for each entire history operation. */
export class Mutex {
  private tail: Promise<unknown> = Promise.resolve();
  run<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.tail.then(fn);
    this.tail = result.catch(() => undefined);
    return result;
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return undefined;

  return typeof error.code === "string" ? error.code : undefined;
}
