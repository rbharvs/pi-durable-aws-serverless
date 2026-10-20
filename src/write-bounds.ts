import type { StorageWrite } from "@earendil-works/pi-durable";
import { LIMITS } from "./limits.ts";
import { EnvelopeRejected } from "./payload.ts";

/** Only the implemented, immutable history records. Not a complete Pi Storage. */
export type HistoryWrite = Extract<StorageWrite, { type: "conversation" | "entry" }>;

/** Detach at invocation, before entering the connection gate. */
export function freezeWrites(writes: readonly HistoryWrite[]): readonly HistoryWrite[] {
  if (writes.length + 1 > LIMITS.maxRows)
    throw new EnvelopeRejected("Atomic envelope exceeded: write count");

  const logicalBytes = Buffer.byteLength(JSON.stringify(writes));

  if (logicalBytes > LIMITS.maxLogicalCommitBytes)
    throw new EnvelopeRejected("Logical commit exceeds 32 MiB memory bound");

  return structuredClone(writes);
}

export function storedHighWater(writes: readonly HistoryWrite[]): number {
  let next = 2;
  for (const write of writes) next = Math.max(next, write.value.id + 1);
  return next;
}
