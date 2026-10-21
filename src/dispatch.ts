import type { HarnessId } from "./domain.ts";

/** Advisory wake hints; success does not establish ownership or confirm processing. */
export interface Dispatch {
  /** Awaited after authority close. Adapters must bound their own I/O; no cancellation is implied. */
  wake(ns: HarnessId): Promise<void>;
}

/** Synchronous diagnostic sink; callers must not interpret events as durable acknowledgments. */
export type Events = (name: string, fields: Record<string, unknown>) => void;
