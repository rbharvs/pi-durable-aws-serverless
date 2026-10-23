import type { Harness, Conversation } from "@earendil-works/pi-durable";
import type { SessionEnd } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { check, sleep, type HarnessId, type RequestId } from "./domain.ts";
import { CLOSE_MARGIN_MS, type Lease } from "./lease.ts";
import { emit, cleanup } from "./diagnostics.ts";
import type { WorkerAuthority, Received } from "./authority.ts";
import type { AgentInput } from "./input.ts";
import type { Events } from "./dispatch.ts";
import { admitInput } from "./admit.ts";

/**
 * Closes opened Harness/Storage before release and the authority handle afterward in finally,
 * including when release is skipped.
 */
export interface WorkerDependencies {
  /** Returns a handle owned and closed by this invocation. */
  connectAuthority(): Promise<WorkerAuthority>;
  /** Opens real Pi over the lease's Storage; cleans up acquired resources before rejecting. */
  open(lease: Lease): Promise<{ harness: Harness; root: Conversation }>;
  resolve(item: Received): Promise<AgentInput>;
  event(name: string, fields: Record<string, unknown>): void;
}

/** Ownership outcome, not task completion: released/idle can race with new receipts. */
export type WorkerResult = "held" | "idle" | "released" | "lost";

type PendingInput = { request: RequestId; input: AgentInput };

/** Pi admission commits before monotonic mapping; recovery safely repeats the same request key. */
async function admitPending(
  ns: HarnessId,
  control: WorkerAuthority,
  root: Conversation,
  remaining: () => number,
  deps: WorkerDependencies,
  event: Events,
  pending: PendingInput | undefined,
): Promise<PendingInput | undefined> {
  for (const received of await control.received(ns)) {
    check(received.ns === ns, "Foreign receipt namespace");
    if (remaining() <= CLOSE_MARGIN_MS) break;
    const cached = pending?.request === received.request ? pending.input : undefined;
    if (!cached) emit(event, "resolving", { ns, request: received.request });
    const input = cached ?? (await deps.resolve(received));

    if (remaining() <= CLOSE_MARGIN_MS) break;
    if (!cached) emit(event, "submitting", { ns, request: received.request });

    const submission = await admitInput(root, input, received.request);
    // Preserve receipt order while a model change waits for the current Pi run to finish.
    if (!submission) return { request: received.request, input };

    await control.markAdmitted(received, submission.id);
    emit(event, "admitted", {
      ns,
      request: received.request,
      submission: submission.id,
    });
  }
  return undefined;
}

/** Runs Pi until idle or the close margin; returns only after a clean joined close. */
async function run(
  ns: HarnessId,
  lease: Lease,
  control: WorkerAuthority,
  remaining: () => number,
  deps: WorkerDependencies,
  event: Events,
): Promise<void> {
  const { harness, root } = await deps.open(lease);
  const closed = harness.closed;
  let ended: SessionEnd | undefined;
  let end: SessionEnd;

  emit(event, "open", { ns, epoch: lease.epoch });
  void closed.then((value) => {
    ended = value;
  });

  // At most one validated immutable input waits at the model-change barrier. Keep it
  // only for this invocation so polling never re-downloads a large S3 receipt.
  let pending: PendingInput | undefined;
  try {
    harness.resume();
    while (remaining() > CLOSE_MARGIN_MS) {
      if (ended) {
        if (ended.reason === "failed") throw ended.error;
        throw new Error("Harness closed before worker close");
      }

      pending = await admitPending(ns, control, root, remaining, deps, event, pending);
      if (!(await control.hasWork(ns))) break;
      await sleep(200);
    }
  } finally {
    // `closed` carries a Storage close failure, or the earlier Session failure behind it.
    emit(event, "closing", { ns, epoch: lease.epoch });
    await harness.close(context).catch(() => undefined);
    end = await closed;
    emit(event, "closed", { ns, epoch: lease.epoch, reason: end.reason });
  }

  if (end.reason === "failed") throw end.error;
}

/** Only a clean joined close permits release; errors never trigger a release retry. */
export async function worker(
  ns: HarnessId,
  remaining: () => number,
  deps: WorkerDependencies,
): Promise<WorkerResult> {
  const event: Events = (name, fields) => {
    deps.event(name, fields);
  };
  const control = await deps.connectAuthority();

  try {
    const claimed = await control.claim(ns, remaining());

    if (claimed.kind === "held") {
      emit(event, "held", { ns });

      return "held";
    }

    const lease = claimed.lease;

    emit(event, "claimed", { ns, epoch: lease.epoch, until: lease.until });

    const idle = !(await control.hasWork(ns));

    if (!idle) await run(ns, lease, control, remaining, deps, event);

    const result = await control.release(lease);

    emit(event, "release", { ns, epoch: lease.epoch, result });
    if (result === "lost") return "lost";

    return idle ? "idle" : "released";
  } catch (error) {
    emit(event, "worker-error", {
      ns,
      cause: error instanceof Error ? "Error" : "unknown",
    });

    throw error;
  } finally {
    await cleanup(control, event, "authority");
  }
}
