import { it, expect } from "vitest";
import { createMemoryBackend } from "../../src/memory.ts";
import { objectFixture as memoryObjectsFixture } from "../shared/objects.ts";
import { controlledWork, manualDispatch } from "../shared/scheduling.ts";
import { runtimeDependencies } from "../../src/runtime.ts";
import { apiHandler } from "../../src/submit.ts";
import { worker } from "../../src/worker.ts";
import { reconcile } from "../../src/reconciler.ts";
import { harnessId, requestId, computeSha256 as sha } from "../../src/domain.ts";
import type { Backend } from "../../src/authority.ts";
import type { Context } from "@earendil-works/chord";

const ns = harnessId("cuts"),
  command = {
    kind: "inline",
    input: { content: "hello", whenBusy: "followUp" },
  };

function fixture() {
  let time = 10000,
    budget = 45000;
  const backend = createMemoryBackend({ now: () => time }),
    objects = memoryObjectsFixture(),
    work = controlledWork(),
    dispatch = manualDispatch();
  const events: string[] = [];
  const deps = runtimeDependencies({
    backend: backend,
    harnessOptions: work,
    objects: objects.objects,
    event: (name) => events.push(name),
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
  });
  const api = apiHandler({
    connectAuthority: backend.connectAuthority,
    objects: objects.objects,
    dispatch,
    event: () => {},
  });

  return {
    backend,
    objects,
    deps,
    dispatch,
    events,
    api,
    advance: () => {
      time += 900001;
    },
    tick: () => {
      time++;
    },
    stop: () => {
      budget = 5000;
    },
    run: () => worker(ns, () => budget, deps),
  };
}

for (const cut of ["open", "resolve", "hasWork"] as const)
  it(`real worker cut at ${cut} retains received input and does not release`, async () => {
    const f = fixture(),
      a = await f.backend.connectAuthority();
    const original = Error(`cut-${cut}`);

    await f.api({ ns, request: "r", command });
    if (cut === "open")
      f.deps.open = async () => {
        throw original;
      };
    if (cut === "resolve")
      f.deps.resolve = async () => {
        throw original;
      };

    if (cut === "hasWork") {
      const connect = f.deps.connectAuthority;

      f.deps.connectAuthority = async () => ({
        ...(await connect()),
        hasWork: async () => {
          throw original;
        },
      });
    }

    try {
      await expect(f.run()).rejects.toBe(original);
      expect(f.events).not.toContain("release");
      expect((await a.get(ns, requestId("r")))?.kind).toBe("received");
      expect(await a.claim(ns, 45000)).toEqual({ kind: "held" });
    } finally {
      await a.close();
    }
  });

it("a post-resolve deadline leaves input received, joins Harness closure, then releases cleanly", async () => {
  const f = fixture(),
    a = await f.backend.connectAuthority();

  await f.api({ ns, request: "r", command });

  const resolve = f.deps.resolve;

  f.deps.resolve = async (item) => {
    const value = await resolve(item);

    f.stop();

    return value;
  };

  try {
    expect(await f.run()).toBe("released");
    expect((await a.get(ns, requestId("r")))?.kind).toBe("received");
    expect(f.events.indexOf("closed")).toBeGreaterThanOrEqual(0);
    expect(f.events.indexOf("closed")).toBeLessThan(f.events.indexOf("release"));
  } finally {
    await a.close();
  }
});

it("a Storage close failure is an actual failed Session, never a clean release", async () => {
  const f = fixture(),
    a = await f.backend.connectAuthority();

  await f.api({ ns, request: "r", command });

  const original = Error("close failure");
  const backend: Backend = {
    ...f.backend,
    openStorage: async (lease) => {
      const s = await f.backend.openStorage(lease),
        close = s.close.bind(s);

      return new Proxy(s, {
        get(target, key) {
          if (key === "then") return undefined;
          if (key === "close")
            return async (c: Context) => {
              await close(c);

              throw original;
            };

          const v = Reflect.get(target, key);

          return typeof v === "function" ? v.bind(target) : v;
        },
      });
    },
  };
  const work = controlledWork(),
    deps = runtimeDependencies({
      backend: backend,
      harnessOptions: work,
      objects: f.objects.objects,
      event: () => {},
      rootAgent: {
        model: { provider: "faux", modelId: "faux-1" },
      },
    });

  try {
    await expect(worker(ns, () => 45000, deps)).rejects.toBe(original);
    expect(await a.claim(ns, 45000)).toEqual({ kind: "held" });
  } finally {
    await a.close();
  }
});

it("arrival after final work poll but before release stays discoverable; no worker dispatch or reclaim", async () => {
  const f = fixture(),
    a = await f.backend.connectAuthority();

  await f.api({ ns, request: "first", command });

  const connect = f.deps.connectAuthority;

  f.deps.connectAuthority = async () => {
    const control = await connect();

    return {
      ...control,
      release: async (lease) => {
        await a.receive(ns, requestId("late"), {
          hash: sha('{"content":"late","whenBusy":"followUp"}'),
          payload: {
            kind: "inline",
            json: '{"content":"late","whenBusy":"followUp"}',
          },
        });

        return control.release(lease);
      },
    };
  };

  try {
    expect(await f.run()).toBe("released");
    expect(f.dispatch.queue).toEqual([ns]);
    expect((await a.get(ns, requestId("late")))?.kind).toBe("received");
    expect(await a.claim(ns, 45000)).toEqual({ kind: "held" });
    f.tick();
    await reconcile({
      connectAuthority: f.backend.connectAuthority,
      dispatch: f.dispatch,
      event: () => {},
    });
    expect(f.dispatch.queue).toEqual([ns, ns]);
  } finally {
    await a.close();
  }
});

for (const applied of [false, true])
  it(`release acknowledgement loss applied=${applied} is not retried or followed by reclaim`, async () => {
    const f = fixture(),
      a = await f.backend.connectAuthority();

    await f.api({ ns, request: "r", command });

    let attempts = 0;
    const connect = f.deps.connectAuthority;

    f.deps.connectAuthority = async () => {
      const control = await connect();

      return {
        ...control,
        release: async (lease) => {
          attempts++;
          if (applied) await control.release(lease);

          throw Error("release unknown");
        },
      };
    };

    try {
      await expect(f.run()).rejects.toThrow("release unknown");
      expect(attempts).toBe(1);
      expect(await a.hasWork(ns)).toBe(false);
      f.tick();
      expect((await a.claim(ns, 45000)).kind).toBe(applied ? "claimed" : "held");
    } finally {
      await a.close();
    }
  });
