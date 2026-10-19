import type { HarnessId } from "./domain.ts";

export interface Dispatch {
  /** Awaited after authority close. Adapters must bound their own I/O; no cancellation is implied. */
  wake(ns: HarnessId): Promise<void>;
}

export type Events = (name: string, fields: Record<string, unknown>) => void;
