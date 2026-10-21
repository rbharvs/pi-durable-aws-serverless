import { it, expect, vi } from "vitest";
import { createMemoryBackend } from "../../src/memory.ts";
import { memoryFixture } from "../shared/memory.ts";
import { objectFixture as memoryObjectsFixture } from "../shared/objects.ts";
import { controlledWork } from "../shared/scheduling.ts";
import { runtimeDependencies } from "../../src/runtime.ts";
import { apiHandler } from "../../src/submit.ts";
import { worker } from "../../src/worker.ts";
import { harnessId, requestId } from "../../src/domain.ts";
import type { Backend } from "../../src/authority.ts";
import { createModels } from "@earendil-works/pi-ai";
import {
  createRegistry,
  Harness,
  type Storage,
  type SessionEnd,
  type ConversationId,
} from "@earendil-works/pi-durable";
import type { Lease } from "../../src/lease.ts";
import { idFromNumber } from "../../src/ids.ts";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";

it("late asynchronous failed Session after final durable scheduler work rejects and never releases", async () => {
  const ns = harnessId("late");
  const objects = memoryObjectsFixture();
  const work = controlledWork();
  let time = 10000;
  const backend = createMemoryBackend({ now: () => time });
  const authority = await backend.connectAuthority();
  const lost = new Error("final scheduler acknowledgement lost");
  let fired = false;
  let releases = 0;
  let closeCompleted = false;
  let closed: SessionEnd | undefined;
  let lease: Lease | undefined;
  let harness: Harness | undefined;
  const wrapped: Backend = {
    ...backend,
    connectAuthority: async () => {
      const a = await backend.connectAuthority();

      return {
        ...a,
        release: async (l) => {
          releases++;

          return a.release(l);
        },
      };
    },
    openStorage: async (l) => {
      lease = l;

      const s = await backend.openStorage(l);

      return new Proxy(s, {
        get(target, key) {
          if (key === "then") return undefined;
          if (key === "commit") {
            return async (...args: Parameters<Storage["commit"]>) => {
              const seq = await target.commit(...args);

              if (!fired && !(await authority.hasWork(ns))) {
                expect((await authority.get(ns, requestId("r")))?.kind).toBe("admitted");
                expect(
                  args[0].some((w) => w.type === "task" && w.value.state.status === "terminal"),
                ).toBe(true);
                fired = true;

                throw lost;
              }

              return seq;
            };
          }
          if (key === "close") {
            return async (c: Parameters<Storage["close"]>[0]) => {
              await target.close(c);
              closeCompleted = true;
            };
          }

          const value = Reflect.get(target, key);

          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    },
  };
  const deps = runtimeDependencies({
    backend: wrapped,
    harnessOptions: work,
    objects: objects.objects,
    event: () => {},
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
  });
  const open = deps.open;

  deps.open = async (l) => {
    const result = await open(l);

    harness = result.harness;
    void harness.closed.then((end) => {
      closed = end;
    });

    return result;
  };

  const api = apiHandler({
    connectAuthority: backend.connectAuthority,
    objects: objects.objects,
    dispatch: { wake: async () => {} },
    event: () => {},
  });

  try {
    expect(
      await api({
        ns,
        request: "r",
        command: {
          kind: "inline",
          input: { content: "hello", whenBusy: "followUp" },
        },
      }),
    ).toMatchObject({ ok: true });

    const outcome = await worker(ns, () => 45000, deps).then(
      (value) => ({ value, closedAtSettlement: closed, closeCompleted }),
      (error) => ({ error, closedAtSettlement: closed, closeCompleted }),
    );

    expect(fired).toBe(true);
    expect(outcome).toMatchObject({
      error: lost,
      closedAtSettlement: { reason: "failed" },
      closeCompleted: true,
    });
    expect(releases).toBe(0);
    expect(await authority.hasWork(ns)).toBe(false);
    time++;
    expect(await authority.claim(ns, 45000)).toEqual({ kind: "held" });

    const fresh = await backend.openStorage(lease!);

    try {
      expect(
        (await fresh.scanTasks({}, 100, undefined, context)).items.every(
          (t) => t.state.status === "terminal",
        ),
      ).toBe(true);
      expect(
        (await fresh.scanSubmissions({}, 100, undefined, context)).items.map((s) => s.status),
      ).toEqual(["done"]);
    } finally {
      await fresh.close(context);
    }
  } finally {
    await harness?.close(context);
    await authority.close();
  }
});

it("runtime open failure before Harness creation closes acquired actual Storage", async () => {
  const f = await memoryFixture();
  const objects = memoryObjectsFixture();
  let acquired: Storage | undefined;
  let closes = 0;
  const backend: Backend = {
    ...f.backend,
    openStorage: async (lease) => {
      acquired = await f.backend.openStorage(lease);

      const close = acquired.close.bind(acquired);

      return new Proxy(acquired, {
        get(target, key) {
          if (key === "then") return undefined;
          if (key === "close") {
            return async (c: Parameters<Storage["close"]>[0]) => {
              closes++;
              await close(c);
            };
          }

          const value = Reflect.get(target, key);

          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    },
  };
  const registry = createRegistry();
  const snapshot = registry.snapshot();

  // A malformed registry reaches real Harness.open's pre-creation validation, not a fake Harness.
  registry.snapshot = () =>
    new Proxy(snapshot, {
      get(target, key) {
        if (key === "task") return () => undefined;

        const value = Reflect.get(target, key);

        return typeof value === "function" ? value.bind(target) : value;
      },
    });

  const deps = runtimeDependencies({
    backend,
    harnessOptions: { models: createModels(), registry },
    objects: objects.objects,
    event: () => {},
    rootAgent: {},
  });

  try {
    await expect(deps.open(f.lease)).rejects.toThrow("Registry lacks built-in tasks");
    expect(closes).toBe(1);
    expect(acquired).toBeDefined();
    await expect(acquired!.mintId()).rejects.toThrow("closed");
  } finally {
    await acquired?.close(context);
    await f.finish();
  }
});

it("successful Harness.open followed by root-creation failure closes actual Harness and Storage without release", async () => {
  const ns = harnessId("root-failure");
  const backend = createMemoryBackend({ now: () => 10000 });
  const objects = memoryObjectsFixture();
  const work = controlledWork();
  const cause = new Error("root callback failed");
  const authority = await backend.connectAuthority();
  let actual: Harness | undefined;
  let closed: SessionEnd | undefined;
  let acquired: Storage | undefined;
  let lease: Lease | undefined;
  let closes = 0;
  let releases = 0;
  let rootCallbacks = 0;
  const originalOpen = Harness.open;
  const openSpy = vi.spyOn(Harness, "open").mockImplementation(async (...args) => {
    const h = await originalOpen(...args);

    actual = h;
    void h.closed.then((end) => {
      closed = end;
    });

    return h;
  });
  const wrapped: Backend = {
    ...backend,
    connectAuthority: async () => {
      const a = await backend.connectAuthority();

      return {
        ...a,
        release: async (l) => {
          releases++;

          return a.release(l);
        },
      };
    },
    openStorage: async (l) => {
      lease = l;
      acquired = await backend.openStorage(l);

      return new Proxy(acquired, {
        get(target, key) {
          if (key === "then") return undefined;
          if (key === "close") {
            return async (c: Parameters<Storage["close"]>[0]) => {
              closes++;

              return target.close(c);
            };
          }

          const value = Reflect.get(target, key);

          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    },
  };
  const deps = runtimeDependencies({
    backend: wrapped,
    harnessOptions: {
      ...work,
      conversationCreated: async () => {
        rootCallbacks++;

        throw cause;
      },
    },
    objects: objects.objects,
    event: () => {},
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
  });
  const api = apiHandler({
    connectAuthority: backend.connectAuthority,
    objects: objects.objects,
    dispatch: { wake: async () => {} },
    event: () => {},
  });

  try {
    expect(
      await api({
        ns,
        request: "r",
        command: {
          kind: "inline",
          input: { content: "hello", whenBusy: "followUp" },
        },
      }),
    ).toMatchObject({ ok: true });
    await expect(worker(ns, () => 45000, deps)).rejects.toBe(cause);
    expect(openSpy).toHaveBeenCalledTimes(1);
    expect(actual).toBeDefined();
    expect(rootCallbacks).toBe(1);
    expect(closed).toMatchObject({ reason: "closed" });
    expect(closes).toBe(1);
    expect(releases).toBe(0);
    await expect(acquired!.mintId()).rejects.toThrow("closed");
    expect(await authority.claim(ns, 45000)).toEqual({ kind: "held" });

    const fresh = await backend.openStorage(lease!);

    try {
      expect(await fresh.conversation(idFromNumber<ConversationId>(1), context)).toBeUndefined();
    } finally {
      await fresh.close(context);
    }
  } finally {
    openSpy.mockRestore();
    await actual?.close(context);
    await acquired?.close(context);
    await authority.close();
  }
});
