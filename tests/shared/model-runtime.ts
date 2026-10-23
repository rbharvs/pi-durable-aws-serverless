import { expect, it } from "vitest";
import { createRegistry, type Conversation, type Harness } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { apiHandler, runtimeDependencies, type Backend } from "../../src/index.ts";
import { createHandler } from "../../src/aws/worker.ts";
import { createProviderModels } from "../../src/providers.ts";
import { check, requestId, type HarnessId } from "../../src/domain.ts";
import { objectFixture } from "./objects.ts";

export interface ModelFixture {
  backend: Backend;
  ns: HarnessId;
  expire(): Promise<unknown>;
  close(): Promise<void>;
}
const openai = { provider: "openai", modelId: "gpt-4.1" };
const anthropic = { provider: "anthropic", modelId: "claude-sonnet-4-5" };

/** Shared memory/PG contract: real Pi and official catalogs, only inference is synthetic. */
export function modelRuntimeTests(setup: () => Promise<ModelFixture>) {
  async function fixture(hold = false, failFirst = false) {
    const f = await setup();
    const objects = objectFixture();
    const models = createProviderModels();
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const calls: string[] = [];
    models.streamSimple = (model, _transcript, options) => {
      calls.push(`${model.provider}/${model.id}`);
      const failed = failFirst && calls.length === 1;
      const stream = new AssistantMessageEventStream();
      started.resolve();
      void (async () => {
        const abort = () => finish.resolve();
        options?.signal?.addEventListener("abort", abort, { once: true });
        try {
          if (hold && !options?.signal?.aborted) await finish.promise;
          const aborted = options?.signal?.aborted;
          const message = {
            ...fauxAssistantMessage("synthetic answer", {
              stopReason: aborted ? "aborted" : failed ? "error" : "stop",
              ...(failed ? { errorMessage: "synthetic provider failure" } : {}),
            }),
            provider: model.provider,
            model: model.id,
            api: model.api,
          };
          if (aborted) stream.push({ type: "error", reason: "aborted", error: message });
          else if (failed) stream.push({ type: "error", reason: "error", error: message });
          else stream.push({ type: "done", reason: "stop", message });
          stream.end();
        } finally {
          options?.signal?.removeEventListener("abort", abort);
        }
      })();
      return stream;
    };
    const options = {
      backend: f.backend,
      harnessOptions: {
        models,
        registry: createRegistry(),
        ...(failFirst ? { settings: { retry: { enabled: false } } } : {}),
      },
      objects: objects.objects,
      rootAgent: {},
      event: () => {},
    };
    const deps = runtimeDependencies(options);
    const resolutions = new Map<string, number>();
    const resolve = deps.resolve;
    deps.resolve = async (item) => {
      resolutions.set(item.request, (resolutions.get(item.request) ?? 0) + 1);
      return resolve(item);
    };
    const api = apiHandler({
      connectAuthority: f.backend.connectAuthority,
      objects: objects.objects,
      dispatch: { wake: async () => {} },
      event: () => {},
    });
    const deadline = Date.now() + 45000;
    let remaining = () => deadline - Date.now();
    let opened: { root: Conversation; harness: Harness } | undefined;
    const open = deps.open;
    deps.open = async (lease) => (opened = await open(lease));
    const handler = createHandler(() => deps);
    return {
      ...f,
      deps,
      calls,
      resolutions,
      started: started.promise,
      finish: () => finish.resolve(),
      stop: () => {
        remaining = () => 5000;
      },
      resume: () => {
        remaining = () => deadline - Date.now();
      },
      run: (budget?: () => number) =>
        handler(
          { ns: f.ns },
          {
            awsRequestId: "synthetic-invocation",
            getRemainingTimeInMillis: () => (budget ?? remaining)(),
          },
        ),
      receive: (request: string, model?: typeof openai, whenBusy = "followUp") =>
        api({
          ns: f.ns,
          request,
          command: {
            kind: "inline",
            input: {
              content: request,
              whenBusy,
              ...(model ? { model } : {}),
            },
          },
        }),
      async receiveStaged(request: string, model: typeof openai) {
        const reference = objects.stage(
          f.ns,
          JSON.stringify({
            content: "x".repeat(300000),
            whenBusy: "followUp",
            model,
          }),
        );
        const response = await api({ ns: f.ns, request, command: { kind: "s3", ...reference } });
        objects.deleteStage(reference.key);
        return response;
      },
      opened: () => {
        check(opened, "Harness has not opened");
        return opened;
      },
      async inspect(...requests: string[]) {
        const control = await f.backend.connectAuthority();
        try {
          const claim = await control.claim(f.ns, 45000);
          check(claim.kind === "claimed", "Inspection needs an available lease");
          const live = await open(claim.lease);
          try {
            return {
              agent: await live.root.agent(context),
              submissions: (await live.harness.inspect(context)).submissions,
              records: await live.root.commit(
                async (tx) =>
                  new Map(
                    await Promise.all(
                      requests.map(
                        async (id) => [id, await tx.submissionByRequest(live.root.id, id)] as const,
                      ),
                    ),
                  ),
                context,
              ),
              entries: (await live.root.entries({ order: "ascending" }, 100, undefined, context))
                .items,
            };
          } finally {
            await live.harness.close(context);
            await control.release(claim.lease);
          }
        } finally {
          await control.close();
        }
      },
      async close() {
        finish.resolve();
        await opened?.harness.close(context);
        await f.close();
        expect(objects.openHandles()).toBe(0);
      },
    };
  }

  it("requires a first model, persists selection across cold continuation, switches, and dedupes", async () => {
    const f = await fixture();
    try {
      expect(await f.receive("missing")).toMatchObject({ ok: true });
      expect(await f.run()).toBe("released");
      expect(f.calls).toEqual([]);
      expect((await f.inspect("missing")).records.get("missing")).toMatchObject({
        status: "unanswered",
        reason: "model_required",
      });
      for (const [request, model] of [
        ["one", openai],
        ["two", undefined],
        ["three", anthropic],
      ] as const) {
        expect(await f.receive(request, model)).toMatchObject({ ok: true, duplicate: false });
        expect(await f.run()).toBe("released");
      }
      expect(await f.receive("one", openai)).toMatchObject({ ok: true, duplicate: true });
      expect(await f.run()).toBe("idle");
      expect(f.calls).toEqual(["openai/gpt-4.1", "openai/gpt-4.1", "anthropic/claude-sonnet-4-5"]);
      const output = await f.inspect("one", "two", "three");
      expect(output.agent.model).toEqual(anthropic);
      expect([...output.records.values()].map((r) => r?.status)).toEqual(["done", "done", "done"]);
      expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(3);
      expect(output.entries.filter((e) => e.kind === "pi.assistant")).toHaveLength(3);
    } finally {
      await f.close();
    }
  });

  it("staged model selection executes from immutable published receipt bytes after staging disappears", async () => {
    const f = await fixture();
    try {
      expect(await f.receiveStaged("one", anthropic)).toMatchObject({ ok: true });
      const control = await f.backend.connectAuthority();
      try {
        expect(await control.get(f.ns, requestId("one"))).toMatchObject({
          payload: { kind: "s3" },
        });
      } finally {
        await control.close();
      }
      expect(await f.run()).toBe("released");
      expect(f.calls).toEqual(["anthropic/claude-sonnet-4-5"]);
      expect((await f.inspect("one")).records.get("one")).toMatchObject({ status: "done" });
    } finally {
      await f.close();
    }
  });

  it("recovers a configuration commit with unknown outcome before submission without duplicate input", async () => {
    const f = await fixture();
    const open = f.deps.open;
    let cut = true;
    f.deps.open = async (lease) => {
      const live = await open(lease);
      const commit = live.root.commit.bind(live.root);
      live.root.commit = async (...args) => {
        const result = await commit(...args);
        if (cut) {
          cut = false;
          throw new Error("configuration commit cut");
        }
        return result;
      };
      return live;
    };
    try {
      expect(await f.receive("one", openai)).toMatchObject({ ok: true });
      await expect(f.run()).rejects.toThrow("configuration commit cut");
      expect(await f.run()).toBe("held");
      expect(f.calls).toEqual([]);
      await f.expire();
      expect(await f.run()).toBe("released");
      expect(f.calls).toEqual(["openai/gpt-4.1"]);
      expect((await f.inspect()).entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
    } finally {
      await f.close();
    }
  });

  it("replayed Pi submission never reapplies its older model after a receipt-mapping cut", async () => {
    const f = await fixture();
    const connect = f.deps.connectAuthority;
    let cut = true;
    f.deps.connectAuthority = async () => {
      const control = await connect();
      const mark = control.markAdmitted.bind(control);
      control.markAdmitted = async (...args) => {
        if (cut) {
          cut = false;
          await f.opened().root.waitForIdle(context);
          await f.opened().root.configure({ model: anthropic }, context);
          throw new Error("receipt mapping cut");
        }
        return mark(...args);
      };
      return control;
    };
    try {
      expect(await f.receive("one", openai)).toMatchObject({ ok: true });
      await expect(f.run()).rejects.toThrow("receipt mapping cut");
      expect(await f.run()).toBe("held");
      await f.expire();
      expect(await f.run()).toBe("released");
      expect(f.calls).toEqual(["openai/gpt-4.1"]);
      const output = await f.inspect();
      expect(output.agent.model).toEqual(anthropic);
      expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
    } finally {
      await f.close();
    }
  });

  it("a model switch after a failed run drains queued inputs instead of blocking the namespace", async () => {
    const f = await fixture(true, true);
    const queued = Promise.withResolvers<void>();
    f.deps.event = (event, fields) => {
      if (event === "admitted" && fields.request === "two") queued.resolve();
    };
    let running: Promise<unknown> | undefined;
    try {
      expect(await f.receive("one", openai)).toMatchObject({ ok: true });
      const deadline = Date.now() + 3000;
      running = f.run(() => 5000 + Math.max(0, deadline - Date.now()));
      await f.started;
      expect(await f.receive("two", openai)).toMatchObject({ ok: true });
      await queued.promise;
      f.finish();
      await f.opened().root.waitForIdle(context);
      expect(
        await f
          .opened()
          .root.commit((tx) => tx.submissionByRequest(f.opened().root.id, "two"), context),
      ).toMatchObject({ status: "queued" });
      expect(await f.receive("three", anthropic)).toMatchObject({ ok: true });
      expect(await running).toBe("released");
      const output = await f.inspect("one", "two", "three");
      expect(output.records.get("one")).toMatchObject({
        status: "unanswered",
        reason: "model_error",
      });
      expect(output.records.get("two")).toMatchObject({ status: "done" });
      expect(output.records.get("three")).toMatchObject({ status: "done" });
      expect(output.agent.model).toEqual(anthropic);
      expect(output.submissions).toEqual([]);
      expect(output.entries.filter((entry) => entry.kind === "pi.user")).toHaveLength(3);
      expect(f.calls[0]).toBe("openai/gpt-4.1");
      expect(f.calls.slice(1)).toEqual([
        "anthropic/claude-sonnet-4-5",
        "anthropic/claude-sonnet-4-5",
      ]);
      const control = await f.backend.connectAuthority();
      try {
        expect(await control.received(f.ns)).toEqual([]);
        expect(await control.hasWork(f.ns)).toBe(false);
      } finally {
        await control.close();
      }
    } finally {
      f.finish();
      await running?.catch(() => {});
      await f.close();
    }
  });

  it("a deferred model switch survives the close margin and cold worker continuation", async () => {
    const f = await fixture(true);
    const polled = Promise.withResolvers<void>();
    const events: string[] = [];
    let readsWhileWaiting = 0;
    f.deps.event = (event, fields) => {
      if (fields.request === "switch") events.push(event);
    };
    const connect = f.deps.connectAuthority;
    f.deps.connectAuthority = async () => {
      const control = await connect();
      const received = control.received.bind(control);
      control.received = async (ns) => {
        const result = await received(ns);
        // Observe several polls during the held model request, without a timing sleep.
        if (events.includes("submitting") && ++readsWhileWaiting >= 3) polled.resolve();
        return result;
      };
      return control;
    };
    let running: Promise<unknown> | undefined;
    try {
      expect(await f.receive("one", openai)).toMatchObject({ ok: true });
      running = f.run();
      await f.started;
      expect(await f.receiveStaged("switch", anthropic)).toMatchObject({ ok: true });
      await polled.promise;
      expect(f.resolutions.get("switch")).toBe(1);
      f.stop();
      expect(await running).toBe("released");
      expect(events.filter((event) => event === "resolving")).toEqual(["resolving"]);
      expect(events.filter((event) => event === "submitting")).toEqual(["submitting"]);
      const control = await f.backend.connectAuthority();
      try {
        expect(await control.get(f.ns, requestId("switch"))).toMatchObject({ kind: "received" });
        expect(await control.hasWork(f.ns)).toBe(true);
      } finally {
        await control.close();
      }
      expect(f.calls).toEqual(["openai/gpt-4.1"]);
      f.resume();
      expect(await f.run()).toBe("released");
      const output = await f.inspect("switch");
      expect(output.agent.model).toEqual(anthropic);
      expect(output.records.get("switch")).toMatchObject({ requestId: "switch", status: "done" });
      expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(2);
      expect(f.calls.at(-1)).toBe("anthropic/claude-sonnet-4-5");
      expect(f.resolutions.get("switch")).toBe(2);
    } finally {
      f.finish();
      await running?.catch(() => {});
      await f.close();
    }
  });

  for (const policy of ["steer", "followUp"]) {
    it(`busy ${policy}: same-model admission works; model switch waits without overtaking`, async () => {
      const f = await fixture(true);
      const admitted = Promise.withResolvers<void>();
      const switching = Promise.withResolvers<void>();
      f.deps.event = (event, fields) => {
        if (event === "admitted" && fields.request === "steer") admitted.resolve();
        if (event === "submitting" && fields.request === "switch") switching.resolve();
      };
      let running: Promise<unknown> | undefined;
      try {
        expect(await f.receive("one", openai)).toMatchObject({ ok: true });
        running = f.run();
        await f.started;
        expect(await f.receive("steer", openai, policy)).toMatchObject({ ok: true });
        await admitted.promise;
        expect(await f.receive("switch", anthropic, policy)).toMatchObject({ ok: true });
        await switching.promise;
        expect(await f.receive("after")).toMatchObject({ ok: true });
        expect((await f.opened().root.agent(context)).model).toEqual(openai);
        const control = await f.backend.connectAuthority();
        try {
          expect(await control.get(f.ns, requestId("switch"))).toMatchObject({ kind: "received" });
          expect(await control.get(f.ns, requestId("after"))).toMatchObject({ kind: "received" });
        } finally {
          await control.close();
        }
        expect(f.calls).toEqual(["openai/gpt-4.1"]);
        f.finish();
        expect(await running).toBe("released");
        const output = await f.inspect("one", "steer", "switch", "after");
        expect(output.agent.model).toEqual(anthropic);
        expect([...output.records.values()].map((r) => r?.status)).toEqual(Array(4).fill("done"));
        expect(f.calls.slice(0, 2)).toEqual(["openai/gpt-4.1", "openai/gpt-4.1"]);
        expect(f.calls.slice(2).every((id) => id === "anthropic/claude-sonnet-4-5")).toBe(true);
        expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(4);
      } finally {
        f.finish();
        await running?.catch(() => {});
        await f.close();
      }
    });
  }
}
