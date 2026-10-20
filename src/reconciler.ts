import type { ReconcilerAuthority } from "./authority.ts";
import type { Dispatch, Events } from "./dispatch.ts";
import type { HarnessId } from "./domain.ts";
import { emit, cleanup } from "./diagnostics.ts";

export type ReconcilerDependencies = {
  connectAuthority(): Promise<ReconcilerAuthority>;
  readonly dispatch: Dispatch;
  readonly event: Events;
};

/**
 * Discovers one bounded page and attempts each advisory wake after closing authority.
 * Returns the candidate count, not delivery or completion confirmations. A rejected wake
 * may have been delivered; overlapping invocations may send the same hint.
 */
export async function reconcile(deps: ReconcilerDependencies): Promise<number> {
  const event: Events = (name, fields) => deps.event(name, fields);
  const control = await deps.connectAuthority();
  let eligible: HarnessId[];
  try {
    eligible = await control.eligible();
  } finally {
    await cleanup(control, event, "authority");
  }

  let failed = 0;
  for (const ns of eligible) {
    try {
      await deps.dispatch.wake(ns);
    } catch {
      failed++;
      emit(event, "dispatch-failed", { ns });
    }
  }
  emit(event, "reconcile", { eligible, failed });
  return eligible.length;
}
