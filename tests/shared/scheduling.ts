import {
  createModels,
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
  Type,
} from "@earendil-works/pi-ai";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { createRegistry, defineTool, defineExtension } from "@earendil-works/pi-durable";
import type { HarnessId } from "../../src/domain.ts";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((a, b) => {
    resolve = a;
    reject = b;
  });

  return { promise, resolve, reject };
}

/** Acceptance records a disposable hint only; delivery is an explicit test action. */
export function manualDispatch() {
  const queue: HarnessId[] = [];

  return {
    queue,

    async wake(ns: HarnessId) {
      queue.push(ns);
    },
  };
}

export function controlledWork(useTools = false) {
  const models = createModels();

  models.setProvider(fauxProvider().provider);

  const registry = createRegistry();
  const starts: Array<{ name: string; finish(): void }> = [];
  let changed = deferred();
  const tools = (["safe", "unsafe"] as const).map((name) =>
    defineTool({
      name,
      description: "Controlled test tool",
      replay: name,
      parameters: Type.Object({}),
      execute: async (_args, _api, ctx) => {
        const gate = deferred();
        const abort = () => gate.reject(ctx.abortSignal?.reason ?? new Error("aborted"));

        ctx.abortSignal?.addEventListener("abort", abort, { once: true });
        starts.push({ name, finish: () => gate.resolve() });
        changed.resolve();
        changed = deferred();

        try {
          if (ctx.abortSignal?.aborted) abort();
          await gate.promise;

          return { output: [{ type: "text" as const, text: "completed" }] };
        } finally {
          ctx.abortSignal?.removeEventListener("abort", abort);
        }
      },
    }),
  );

  registry.install(defineExtension({ name: "controlled", tools }));
  models.streamSimple = (_model, transcript) => {
    const stream = new AssistantMessageEventStream();
    const hasResults = transcript.messages.some((m) => m.role === "toolResult");
    const callTools = useTools && !hasResults;
    const message = fauxAssistantMessage(
      callTools
        ? [
            fauxToolCall("safe", {}, { id: "safe-0" }),
            fauxToolCall("unsafe", {}, { id: "unsafe-0" }),
          ]
        : "done",
      { stopReason: callTools ? "toolUse" : "stop" },
    );

    stream.push({
      type: "done",
      reason: callTools ? "toolUse" : "stop",
      message,
    });
    stream.end();

    return stream;
  };

  return {
    models,
    registry,
    tools,
    starts,

    async started(count: number) {
      while (starts.length < count) {
        await changed.promise;
      }
    },
  };
}
