import type { AgentChange } from "@earendil-works/pi-durable";
import { Harness, type HarnessOptions } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import type { Backend } from "./authority.ts";
import type { WorkerDependencies } from "./worker.ts";
import { resolveInput } from "./submit.ts";
import type { PayloadObjects } from "./s3-payload.ts";
import type { HarnessId } from "./domain.ts";
import type { Events } from "./dispatch.ts";
import { cleanup } from "./diagnostics.ts";

/** Caller-owned backend and Pi configuration used to acquire fresh per-operation resources. */
export interface RuntimeOptions {
  readonly backend: Backend;
  readonly harnessOptions: HarnessOptions;
  /** Fresh namespace-scoped immutable payload reader, closed after each resolution. */
  readonly objects: (
    ns: HarnessId,
  ) => Pick<PayloadObjects, "get"> & { close(): void | Promise<void> };
  readonly event: Events;
  readonly rootAgent: AgentChange;
}

/**
 * Wires receipt resolution and leased Pi sessions without acquiring resources at construction.
 * Resolution closes its object reader even on failure. A successful open transfers the Harness
 * to the worker; a failed open attempts to close the Harness or Storage before rejecting.
 */
export function runtimeDependencies(options: RuntimeOptions) {
  const { backend, harnessOptions, rootAgent } = options;
  const event: Events = (name, fields) => {
    options.event(name, fields);
  };
  return {
    connectAuthority: () => backend.connectAuthority(),
    resolve: async (item) => {
      const store = options.objects(item.ns);

      try {
        return await resolveInput(item, store);
      } finally {
        await cleanup(store, event, "objects");
      }
    },
    open: async (lease) => {
      const storage = await backend.openStorage(lease);
      let harness: Harness | undefined;

      try {
        harness = await Harness.open(storage, harnessOptions, context);

        const root = await harness.root(context, { agent: rootAgent });

        return { harness, root };
      } catch (error) {
        // Harness close joins Pi; a failed Harness.open may already have closed Storage.
        await (harness ?? storage).close(context).catch(() => undefined);

        throw error;
      }
    },
    event,
  } satisfies WorkerDependencies;
}
