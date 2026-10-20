import type { StorageWrite } from "@earendil-works/pi-durable";
import { LIMITS } from "./limits.ts";
import { EnvelopeRejected } from "./payload.ts";

/** Only implemented native records. Not a complete Pi Storage. */
export type HistoryWrite = Extract<
  StorageWrite,
  { type: "conversation" | "entry" | "task" | "submission" }
>;

/** Detach at invocation; validation, indexes and payloads all use this same JSON value. */
export function freezeWrites(writes: readonly HistoryWrite[]): readonly HistoryWrite[] {
  if (writes.length + 1 > LIMITS.maxRows)
    throw new EnvelopeRejected("Atomic envelope exceeded: write count");

  // Clone first: caller getters run once and custom prototypes cannot hide stored fields
  // from the budget. Normalize undefined/nonfinite values and built-in toJSON values once.
  const json = JSON.stringify(structuredClone(writes));
  const logicalBytes = Buffer.byteLength(json);

  if (logicalBytes > LIMITS.maxLogicalCommitBytes)
    throw new EnvelopeRejected("Logical commit exceeds 32 MiB memory bound");

  return JSON.parse(json);
}

export function storedHighWater(writes: readonly HistoryWrite[]): number {
  let next = 2;
  for (const write of writes) next = Math.max(next, write.value.id + 1);
  return next;
}
