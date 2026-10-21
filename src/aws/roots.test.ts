import { afterEach, expect, it, vi } from "vitest";
import { createHandler as apiRoot, handler as apiHandler } from "./api.ts";
import { createHandler as reconcilerRoot, handler as reconcilerHandler } from "./reconciler.ts";
import { createAwsHandler, createHandler as workerRoot } from "./worker.ts";
import * as common from "./common.ts";
import * as objects from "./s3.ts";
import * as hints from "./dispatch.ts";
import { createMemoryBackend } from "../memory.ts";
import { harnessId, requestId } from "../domain.ts";
import { aws, lambdaContext as ctx } from "../../tests/shared/aws.ts";
import { sdkObjects } from "../../tests/shared/aws-objects.ts";
import type { ApiDependencies } from "../submit.ts";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry } from "@earendil-works/pi-durable";
import { acceptedInput } from "../../tests/shared/input.ts";

const ns = harnessId("roots");
const input = {
  ns,
  request: "one",
  command: { kind: "inline", input: { content: "hello", whenBusy: "followUp" } },
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function normal() {
  const backend = createMemoryBackend();
  const objectStore = sdkObjects();
  const wakes: string[] = [];
  const close = vi.fn();
  const config = vi.spyOn(common, "configuration").mockReturnValue({
    aws,
    provider: {
      connect: async () => {
        throw new Error("unexpected SQL connection");
      },
      isRetryableAbort: () => false,
    },
    connectAuthority: () => backend.connectAuthority(),
  });
  const factory = vi.spyOn(objects, "objectFactory").mockReturnValue(objectStore.objects);
  const dispatch = vi.spyOn(hints, "dispatcher").mockReturnValue({
    wake: async (n) => {
      wakes.push(n);
    },
    close,
  });
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  vi.stubEnv("WORKER_NAME", "service-worker");
  return { backend, objectStore, wakes, close, config, factory, dispatch, log };
}

it("normal API root retains stable codes, empty input, canonical cross-transport dedup and immutable receipt", async () => {
  const f = normal();
  const first = await apiHandler(input, ctx);
  expect(first).toMatchObject({ ok: true, duplicate: false });
  expect(await apiHandler({ ...input, ns: "bad/path" }, ctx)).toEqual({
    ok: false,
    code: "invalid-input",
    retryable: false,
  });
  expect(
    await apiHandler(
      {
        ...input,
        command: { kind: "inline", input: { content: "different", whenBusy: "followUp" } },
      },
      ctx,
    ),
  ).toEqual({ ok: false, code: "request-conflict", retryable: false });
  const staged = f.objectStore.stage(ns, JSON.stringify(input.command.input));
  const request = { ...input, command: { kind: "s3", ...staged } };
  expect(await apiHandler(request, ctx)).toEqual({ ...first, duplicate: true });
  f.objectStore.bytes.set(staged.key, Buffer.from("replaced"));
  expect(await apiHandler(request, ctx)).toEqual({
    ok: false,
    code: "staged-content-invalid",
    retryable: false,
  });
  const fresh = f.objectStore.stage(ns, JSON.stringify(input.command.input), "fresh");
  expect(await apiHandler({ ...input, command: { kind: "s3", ...fresh } }, ctx)).toEqual({
    ...first,
    duplicate: true,
  });
  for (const content of ["", []]) {
    expect(
      await apiHandler(
        {
          ...input,
          request: `empty-${typeof content}`,
          command: { kind: "inline", input: { content, whenBusy: "followUp" } },
        },
        ctx,
      ),
    ).toMatchObject({ ok: true });
  }
  expect(f.close).toHaveBeenCalledTimes(8);
  expect(f.objectStore.openHandles()).toBe(0);
  expect(f.wakes).toHaveLength(5);
});

it("normal root lazily acquires object handles and returns unavailable for a failed staged acquisition", async () => {
  const f = normal();
  const acquire = vi.fn(() => {
    throw new Error("private acquisition");
  });
  f.factory.mockReturnValue(acquire);
  expect(await apiHandler(input, ctx)).toMatchObject({ ok: true });
  expect(acquire).not.toHaveBeenCalled();
  const ref = f.objectStore.stage(ns, JSON.stringify(input.command.input));
  expect(await apiHandler({ ...input, command: { kind: "s3", ...ref } }, ctx)).toEqual({
    ok: false,
    code: "unavailable",
    retryable: true,
  });
  expect(acquire).toHaveBeenCalledTimes(1);
  expect(f.close).toHaveBeenCalledTimes(2);
});

it("normal API composition/acquisition diagnostics redact arbitrary provider names, messages and nested causes", async () => {
  const f = normal();
  const error = Object.assign(new Error("SELECT private credential body"), {
    name: "secret-password",
    cause: new Error("nested-token"),
  });
  f.config.mockImplementationOnce(() => {
    throw error;
  });
  expect(await apiHandler(input, ctx)).toEqual({ ok: false, code: "internal", retryable: true });
  expect(f.close).not.toHaveBeenCalled();
  f.config.mockReturnValue({
    ...f.config.getMockImplementation()!(),
    connectAuthority: async () => {
      throw error;
    },
  });
  expect(await apiHandler(input, ctx)).toEqual({ ok: false, code: "unavailable", retryable: true });
  const rows = f.log.mock.calls.map(([row]) => JSON.parse(row));
  expect(rows.filter((r) => /diagnostic|failed/.test(r.event))).toEqual([
    {
      source: "api",
      invocation: ctx.awsRequestId,
      at: expect.any(Number),
      event: "api-diagnostic",
      phase: "compose",
    },
    {
      source: "api",
      invocation: ctx.awsRequestId,
      at: expect.any(Number),
      event: "submit-failed",
      code: "unavailable",
      phase: "acquire",
    },
  ]);
  expect(JSON.stringify(rows)).not.toMatch(/SELECT|private|credential|password|nested-token|body/);
  expect(error.cause.message).toBe("nested-token");
  f.log.mockImplementation(() => {
    throw new Error("logging failure");
  });
  expect(await apiHandler(input, ctx)).toEqual({ ok: false, code: "unavailable", retryable: true });
});

it("missing resource config fails before dispatch acquisition and normal reconciler never constructs objects", async () => {
  const f = normal();
  vi.stubEnv("WORKER_NAME", "");
  expect(await apiHandler(input, ctx)).toEqual({ ok: false, code: "internal", retryable: true });
  expect(f.dispatch).not.toHaveBeenCalled();
  vi.stubEnv("WORKER_NAME", "service-worker");
  expect(await reconcilerHandler({}, ctx)).toBe(0);
  expect(f.factory).not.toHaveBeenCalled();
  expect(f.close).toHaveBeenCalledTimes(1);
});

it("injected API root preserves every dependency receiver and joins async composition cleanup after wake", async () => {
  const backend = createMemoryBackend();
  const trace: string[] = [];
  const observers: Array<{ kind: string; receiver: unknown }> = [];
  const deps: ApiDependencies & { close(): Promise<void> } = {
    async connectAuthority() {
      expect(this).toBe(deps);
      const a = await backend.connectAuthority();
      const close = a.close.bind(a);
      a.close = async () => {
        await close();
        trace.push("authority");
      };
      return a;
    },
    dispatch: {
      async wake() {
        expect(this).toBe(deps.dispatch);
        trace.push("wake");
      },
    },
    event() {
      observers.push({ kind: "event", receiver: this });
    },
    report() {
      observers.push({ kind: "report", receiver: this });
    },
    async close() {
      expect(this).toBe(deps);
      await Promise.resolve();
      trace.push("composition");
      throw new Error("secondary cleanup");
    },
  };
  expect(await apiRoot(() => deps)(input, ctx)).toMatchObject({ ok: true });
  expect(trace).toEqual(["authority", "wake", "composition"]);
  expect(await apiRoot(() => deps)(null, ctx)).toEqual({
    ok: false,
    code: "invalid-input",
    retryable: false,
  });
  // Even structurally compatible core dependencies cannot expose private errors through this root.
  expect(observers.map((o) => o.kind)).toEqual(["event", "event", "event", "event"]);
  expect(observers.every((o) => o.receiver === deps)).toBe(true);
});

for (const applied of [false, true]) {
  it(`unknown receipt outcome applied=${applied} retains all cleanup and same-request retry semantics through root`, async () => {
    const backend = createMemoryBackend();
    const f = sdkObjects();
    const staged = f.stage(ns, JSON.stringify(input.command.input));
    const trace: string[] = [];
    const deps: ApiDependencies & { close(): Promise<void> } = {
      async connectAuthority() {
        const a = await backend.connectAuthority();
        return {
          get: a.get.bind(a),
          async receive(...args) {
            if (applied) await a.receive(...args);
            throw new Error("unknown commit");
          },
          async close() {
            await a.close();
            trace.push("authority");
            throw new Error("secondary");
          },
        };
      },
      objects(n) {
        const store = f.objects(n);
        const close = store.close.bind(store);
        store.close = () => {
          close();
          trace.push("objects");
          throw new Error("secondary");
        };
        return store;
      },
      event() {
        throw new Error("observer");
      },
      dispatch: {
        wake: async () => {
          trace.push("wake");
        },
      },
      async close() {
        await Promise.resolve();
        trace.push("composition");
        throw new Error("secondary");
      },
    };
    expect(
      await apiRoot(() => deps)({ ...input, command: { kind: "s3", ...staged } }, ctx),
    ).toEqual({ ok: false, code: "unavailable", retryable: true });
    expect(trace).toEqual(["authority", "objects", "composition"]);
    expect(f.openHandles()).toBe(0);
    deps.connectAuthority = () => backend.connectAuthority();
    expect(await apiRoot(() => deps)(input, ctx)).toMatchObject({ ok: true, duplicate: applied });
  });
}

it("reconciler root closes before wakes, continues failures and preserves discovery failure over async cleanup", async () => {
  const trace: string[] = [];
  const observers: unknown[] = [];
  const primary = new Error("discovery");
  let fail = false;
  const deps = {
    async connectAuthority() {
      expect(this).toBe(deps);
      return {
        async eligible() {
          if (fail) throw primary;
          return [harnessId("a"), harnessId("b")];
        },
        async close() {
          trace.push("authority");
        },
      };
    },
    dispatch: {
      async wake(n: string) {
        expect(this).toBe(deps.dispatch);
        trace.push(n);
        if (n === "a") throw new Error("unknown wake");
      },
    },
    event() {
      observers.push(this);
    },
    async close() {
      expect(this).toBe(deps);
      await Promise.resolve();
      trace.push("composition");
      throw new Error("secondary");
    },
  };
  const run = reconcilerRoot(() => deps);
  expect(await run({}, ctx)).toBe(2);
  expect(trace).toEqual(["authority", "a", "b", "composition"]);
  trace.length = 0;
  fail = true;
  await expect(run({}, ctx)).rejects.toBe(primary);
  expect(trace).toEqual(["authority", "composition"]);
  expect(observers).toHaveLength(4);
  expect(observers.every((receiver) => receiver === deps)).toBe(true);
});

it("worker entry validates namespace before composition and retains context/dependency receivers", async () => {
  const backend = createMemoryBackend();
  const observers: unknown[] = [];
  const deps = {
    async connectAuthority() {
      expect(this).toBe(deps);
      return backend.connectAuthority();
    },
    async open() {
      throw new Error("idle must not open Pi");
    },
    async resolve() {
      throw new Error("idle must not resolve");
    },
    event() {
      observers.push(this);
    },
  };
  const compose = vi.fn(() => deps);
  const run = workerRoot(compose);
  await expect(run({ ns: "bad/path" }, ctx)).rejects.toThrow();
  expect(compose).not.toHaveBeenCalled();
  const authority = await backend.connectAuthority();
  try {
    await authority.receive(ns, requestId("held"), acceptedInput(input.command.input));
    await authority.claim(ns, 45000);
  } finally {
    await authority.close();
  }
  const context = {
    ...ctx,
    getRemainingTimeInMillis() {
      expect(this).toBe(context);
      return 45000;
    },
  };
  expect(await run({ ns }, context)).toBe("held");
  expect(observers).toHaveLength(1);
  expect(observers[0]).toBe(deps);
});

it("operator-configured worker factory is lazy, needs no invocation target, and wires actual SQL authority", async () => {
  const f = normal();
  vi.stubEnv("WORKER_NAME", "");
  const run = createAwsHandler(
    { models: createModels(), registry: createRegistry() },
    { model: { provider: "faux", modelId: "configured" } },
  );
  expect(f.config).not.toHaveBeenCalled();
  await expect(run({ ns }, ctx)).rejects.toThrow("unexpected SQL connection");
  expect(f.dispatch).not.toHaveBeenCalled();
  expect(f.factory).toHaveBeenCalledTimes(1);
  const a = await f.backend.connectAuthority();
  try {
    expect(await a.get(ns, requestId("one"))).toBeUndefined();
  } finally {
    await a.close();
  }
});
