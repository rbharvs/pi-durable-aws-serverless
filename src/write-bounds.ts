import type { StorageWrite } from "@earendil-works/pi-durable";
import { LIMITS } from "./limits.ts";
import { EnvelopeRejected } from "./payload.ts";

/** Detach at invocation; validation, indexes and payloads all use this same JSON value. */
export function freezeWrites(writes: readonly StorageWrite[]): readonly StorageWrite[] {
  if (writes.length + 1 > LIMITS.maxRows) {
    throw new EnvelopeRejected("Atomic envelope exceeded: write count");
  }

  // Clone first: caller getters run once and custom prototypes cannot hide stored fields
  // from the budget. Normalize undefined/nonfinite values and built-in toJSON values once.
  const json = JSON.stringify(structuredClone(writes));
  const logicalBytes = Buffer.byteLength(json);

  if (logicalBytes > LIMITS.maxLogicalCommitBytes) {
    throw new EnvelopeRejected("Logical commit exceeds 32 MiB memory bound");
  }

  return JSON.parse(json);
}

/** The stored record or document ID that one write creates or changes. */
export function writeId(write: StorageWrite): number {
  switch (write.type) {
    case "document.create":
    case "document.copy":
      return write.record.id;
    case "document.change":
    case "document.retire":
      return write.id;
    default:
      return write.value.id;
  }
}

export function storedHighWater(writes: readonly StorageWrite[]): number {
  let next = 2;
  for (const write of writes) {
    next = Math.max(next, writeId(write) + 1);
  }
  return next;
}
