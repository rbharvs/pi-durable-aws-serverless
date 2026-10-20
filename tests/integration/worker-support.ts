import { createModels, fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { createRegistry, ROOT_CONVERSATION_ID } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { apiHandler, runtimeDependencies, worker, type HarnessId } from "../../src/index.ts";
import { check, requestId } from "../../src/domain.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import { namespace, provider, wrap, type Query, type QueryHook } from "./postgres.ts";
import { objectFixture } from "../shared/objects.ts";

export const deferred = () => Promise.withResolvers<void>();

/** Actual Pi model stream, deterministic and local, optionally held until finish or abort. */
export function modelWork(hold = false) {
  const models = createModels();
  models.setProvider(fauxProvider({ models: [{ id: "faux-1", contextWindow: 8000000 }] }).provider);
  const started = deferred(),
    finish = deferred();
  models.streamSimple = (_model, _transcript, options) => {
    const stream = new AssistantMessageEventStream();
    started.resolve();
    void (async () => {
      const abort = () => finish.resolve();
      options?.signal?.addEventListener("abort", abort, { once: true });
      try {
        if (hold && !options?.signal?.aborted) await finish.promise;
        const aborted = options?.signal?.aborted;
        const message = fauxAssistantMessage("ok", { stopReason: aborted ? "aborted" : "stop" });
        if (aborted) stream.push({ type: "error", reason: "aborted", error: message });
        else stream.push({ type: "done", reason: "stop", message });
        stream.end();
      } finally {
        options?.signal?.removeEventListener("abort", abort);
      }
    })();
    return stream;
  };
  return {
    models,
    registry: createRegistry(),
    started: started.promise,
    finish: () => finish.resolve(),
  };
}

/** Real PG backend and Harness; only immutable object bytes and the model live in memory. */
export async function workerFixture(label: string, hold = false, hook: QueryHook = async () => {}) {
  const f = await namespace(label);
  const objects = objectFixture(),
    work = modelWork(hold);
  const hints: HarnessId[] = [],
    events: Record<string, unknown>[] = [],
    trace: Query[] = [];
  const backend = createSqlBackend(
    {
      ...provider,
      connect: async () => wrap(await provider.connect(), hook, trace),
    },
    objects.objects,
  );
  const options = {
    backend,
    harnessOptions: { models: work.models, registry: work.registry },
    objects: objects.objects,
    event: (event: string, fields: Record<string, unknown>) => {
      events.push({ event, ...fields });
    },
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
  };
  const deps = runtimeDependencies(options);
  const api = apiHandler({
    connectAuthority: () => backend.connectAuthority(),
    objects: objects.objects,
    event: options.event,
    dispatch: {
      wake: async (ns) => {
        hints.push(ns);
      },
    },
  });
  return {
    ...f,
    objects,
    work,
    hints,
    events,
    trace,
    backend,
    options,
    deps,
    api,
    run: () => {
      const deadline = Date.now() + 45000;
      return worker(f.ns, () => deadline - Date.now(), deps);
    },
    receive: (request = "one", content = "hello", whenBusy: "steer" | "followUp" = "followUp") =>
      api({ ns: f.ns, request, command: { kind: "inline", input: { content, whenBusy } } }),
    head: async () =>
      (await f.authority.inspectionDb.query("SELECT * FROM harness WHERE ns=$1", [f.ns]))[0]!,
    expire: () =>
      f.authority.inspectionDb.query(`UPDATE harness SET lease_until=${DB_NOW_MS}-1 WHERE ns=$1`, [
        f.ns,
      ]),
    async output() {
      const claimed = await f.authority.claim(f.ns, 45000);
      check(claimed.kind === "claimed", "Inspection requires a cleanly released owner");
      const storage = await backend.openStorage(claimed.lease);
      try {
        return {
          entries: (
            await storage.scanEntries(
              { conversationId: ROOT_CONVERSATION_ID, order: "ascending" },
              100,
              undefined,
              context,
            )
          ).items,
          submissions: (await storage.scanSubmissions({}, 100, undefined, context)).items,
          tasks: (await storage.scanTasks({}, 100, undefined, context)).items,
          receipt: await f.authority.get(f.ns, requestId("one")),
        };
      } finally {
        await storage.close(context);
        await f.authority.release(claimed.lease);
      }
    },
    async finish() {
      work.finish();
      await f.finish();
      check(objects.openHandles() === 0, "Worker leaked object handles");
    },
  };
}
