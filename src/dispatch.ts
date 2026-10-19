import type { HarnessId } from "./domain.ts";

export interface Dispatch {
  wake(ns: HarnessId): Promise<void>;
}

export type Events = (name: string, fields: Record<string, unknown>) => void;
