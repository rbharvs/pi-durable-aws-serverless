import { expect, it } from "vitest";
import { Harness, type Storage } from "@earendil-works/pi-durable";
import { runtimeDependencies, worker, type Backend, type Lease } from "../../src/index.ts";
import { requestId } from "../../src/domain.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { deferred, workerFixture } from "./worker-support.ts";

for (const cut of ["claim", "hasWork", "open", "resolve"] as const)
  it(`worker failure at ${cut} closes acquired handles and never releases`, async () => {
    const f = await workerFixture(`worker_${cut}`);
    const cause = Error(`cut-${cut}`);
    let closes = 0,
      releases = 0;
    const connect = f.deps.connectAuthority;
    f.deps.connectAuthority = async () => {
      const c = await connect(),
        close = c.close.bind(c);
      c.close = async () => {
        closes++;
        await close();
      };
      c.release = async () => {
        releases++;
        throw Error("unexpected release");
      };
      if (cut === "claim" || cut === "hasWork")
        c[cut] = async () => {
          throw cause;
        };
      return c;
    };
    if (cut === "open" || cut === "resolve")
      f.deps[cut] = async () => {
        throw cause;
      };
    try {
      await f.receive();
      await expect(f.run()).rejects.toBe(cause);
      expect(closes).toBe(1);
      expect(releases).toBe(0);
      expect((await f.authority.get(f.ns, requestId("one")))?.kind).toBe("received");
      if (cut !== "claim") expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
      expect(f.objects.openHandles()).toBe(0);
    } finally {
      await f.finish();
    }
  });

it("authority acquisition failure does not open Storage or payload objects", async () => {
  const f = await workerFixture("worker_connect");
  const cause = Error("connect failed");
  try {
    await f.receive();
    const before = await f.head();
    f.deps.connectAuthority = async () => {
      throw cause;
    };
    await expect(f.run()).rejects.toBe(cause);
    expect(await f.head()).toEqual(before);
    expect(f.objects.openHandles()).toBe(0);
    expect(f.events.some((e) => e.event === "open")).toBe(false);
  } finally {
    await f.finish();
  }
});

it("post-resolve deadline leaves the receipt unmapped and joins close before releasing, without a wake", async () => {
  const f = await workerFixture("worker_deadline");
  let budget = 45000;
  const resolve = f.deps.resolve;
  f.deps.resolve = async (item) => {
    const input = await resolve(item);
    budget = 5000;
    return input;
  };
  const order: string[] = [];
  const open = f.deps.open,
    connect = f.deps.connectAuthority;
  f.deps.open = async (lease) => {
    const opened = await open(lease);
    void opened.harness.closed.then(() => order.push("closed"));
    return opened;
  };
  f.deps.connectAuthority = async () => {
    const c = await connect(),
      release = c.release.bind(c);
    c.release = async (lease) => {
      order.push("release");
      return release(lease);
    };
    return c;
  };
  try {
    await f.receive();
    expect(await worker(f.ns, () => budget, f.deps)).toBe("released");
    expect(order).toEqual(["closed", "release"]);
    expect(f.hints).toEqual([f.ns]);
    expect((await f.authority.get(f.ns, requestId("one")))?.kind).toBe("received");
    expect((await f.output()).submissions).toEqual([]);
    expect((await f.head()).owner).toBeNull();
  } finally {
    await f.finish();
  }
});

for (const cut of ["registry", "root", "close"] as const)
  it(`actual runtime ${cut} failure closes Storage and retains the lease`, async () => {
    const f = await workerFixture(`runtime_${cut}`);
    const cause = Error(`${cut} failure`);
    let acquired: Storage | undefined,
      closes = 0,
      callbacks = 0;
    const open = f.backend.openStorage;
    f.backend.openStorage = async (lease) => {
      acquired = await open(lease);
      const close = acquired.close.bind(acquired);
      acquired.close = async (c) => {
        closes++;
        await close(c);
        if (cut === "close") throw cause;
      };
      return acquired;
    };
    const options = {
      ...f.options,
      harnessOptions: {
        ...f.options.harnessOptions,
        conversationCreated: async () => {
          callbacks++;
          if (cut === "root") throw cause;
        },
      },
    };
    if (cut === "registry")
      options.harnessOptions.registry.snapshot = () => {
        throw cause;
      };
    const deps = runtimeDependencies(options);
    try {
      await f.receive();
      await expect(worker(f.ns, () => 45000, deps)).rejects.toBe(cause);
      expect(closes).toBe(1);
      expect(callbacks).toBe(cut === "registry" ? 0 : 1);
      expect(acquired).toBeDefined();
      await expect(acquired!.mintId()).rejects.toThrow("closed");
      expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
      expect(f.events.some((e) => e.event === "release")).toBe(false);
      expect(f.objects.openHandles()).toBe(0);
    } finally {
      await f.finish();
    }
  });

it("asynchronous Session failure surfaces its cause, joins closure, and retains the lease", async () => {
  let armed = false;
  const cause = Error("background durability failure");
  const f = await workerFixture("worker_session", true, async (phase, sql) => {
    if (armed && phase === "before" && sql === BEGIN_WRITE) {
      armed = false;
      throw cause;
    }
  });
  const mapped = deferred();
  const connect = f.deps.connectAuthority,
    open = f.deps.open;
  let harness: Harness | undefined;
  f.deps.open = async (lease) => {
    const opened = await open(lease);
    harness = opened.harness;
    return opened;
  };
  f.deps.connectAuthority = async () => {
    const c = await connect(),
      mark = c.markAdmitted.bind(c);
    c.markAdmitted = async (item, id) => {
      await mark(item, id);
      mapped.resolve();
    };
    return c;
  };
  let running: ReturnType<typeof f.run> | undefined;
  try {
    await f.receive();
    running = f.run();
    const failure = expect(running).rejects.toBe(cause);
    await Promise.all([mapped.promise, f.work.started]);
    armed = true;
    f.work.finish();
    await failure;
    expect(await harness!.closed).toEqual({ reason: "failed", error: cause });
    expect(f.events.some((e) => e.event === "release")).toBe(false);
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    f.work.finish();
    await running?.catch(() => {});
    await f.finish();
  }
});

it("runtime factory retains Backend and resource method receivers", async () => {
  const f = await workerFixture("worker_receivers");
  class BoundBackend implements Backend {
    constructor(private readonly backend: Backend) {}
    connectAuthority() {
      return this.backend.connectAuthority();
    }
    openStorage(lease: Lease) {
      return this.backend.openStorage(lease);
    }
  }
  let readersClosed = 0,
    reads = 0;
  const options = {
    ...f.options,
    backend: new BoundBackend(f.backend),
    objectStore: f.objects,
    objects(ns: typeof f.ns) {
      const store = this.objectStore.objects(ns);
      const reader = {
        store,
        get(ref: Parameters<typeof store.get>[0]) {
          reads++;
          return this.store.get(ref);
        },
        close() {
          this.store.close();
          readersClosed++;
        },
      };
      return reader;
    },
  };
  const deps = runtimeDependencies(options);
  try {
    const staged = f.objects.stage(
      f.ns,
      JSON.stringify({ content: "x".repeat(300000), whenBusy: "followUp" }),
    );
    expect(
      await f.api({ ns: f.ns, request: "one", command: { kind: "s3", ...staged } }),
    ).toMatchObject({ ok: true });
    expect(await worker(f.ns, () => 45000, deps)).toBe("released");
    expect(readersClosed).toBe(1);
    expect(reads).toBe(1);
    expect((await f.output()).submissions[0]!.status).toBe("done");
  } finally {
    await f.finish();
  }
});

for (const applied of [false, true])
  it(`unknown release applied=${applied} is attempted once, only after clean close`, async () => {
    const f = await workerFixture("worker_release");
    let attempts = 0,
      closed = false;
    const connect = f.deps.connectAuthority,
      open = f.deps.open;
    f.deps.open = async (lease) => {
      const result = await open(lease);
      void result.harness.closed.then(() => {
        closed = true;
      });
      return result;
    };
    f.deps.connectAuthority = async () => {
      const c = await connect(),
        release = c.release.bind(c);
      c.release = async (lease) => {
        expect(closed).toBe(true);
        attempts++;
        if (applied) await release(lease);
        throw Error("release unknown");
      };
      return c;
    };
    try {
      await f.receive();
      await expect(f.run()).rejects.toThrow("release unknown");
      expect(attempts).toBe(1);
      expect(await f.authority.hasWork(f.ns)).toBe(false);
      expect((await f.head()).owner === null).toBe(applied);
      expect(f.hints).toEqual([f.ns]);
    } finally {
      await f.finish();
    }
  });

it("arrival during close remains durable for the next invocation; worker never dispatches", async () => {
  const f = await workerFixture("worker_late");
  const open = f.backend.openStorage;
  let late = false;
  f.backend.openStorage = async (lease) => {
    const s = await open(lease),
      close = s.close.bind(s);
    s.close = async (c) => {
      if (!late) {
        late = true;
        expect(await f.receive("late", "later")).toMatchObject({ ok: true });
      }
      await close(c);
    };
    return s;
  };
  try {
    await f.receive();
    expect(await f.run()).toBe("released");
    expect(await f.authority.hasWork(f.ns)).toBe(true);
    expect(f.hints).toEqual([f.ns, f.ns]);
    expect((await f.authority.get(f.ns, requestId("late")))?.kind).toBe("received");
    expect(await f.run()).toBe("released");
    expect(await f.authority.hasWork(f.ns)).toBe(false);
  } finally {
    await f.finish();
  }
});

it("worker waits for actual Storage close before releasing or returning", async () => {
  const f = await workerFixture("worker_join");
  const closing = deferred(),
    join = deferred();
  const open = f.backend.openStorage;
  let returned = false;
  f.backend.openStorage = async (lease) => {
    const s = await open(lease),
      close = s.close.bind(s);
    s.close = async (c) => {
      closing.resolve();
      await join.promise;
      await close(c);
    };
    return s;
  };
  let running: ReturnType<typeof f.run> | undefined;
  try {
    await f.receive();
    running = f.run().finally(() => {
      returned = true;
    });
    await closing.promise;
    expect(returned).toBe(false);
    expect((await f.head()).owner).not.toBeNull();
    expect(f.events.some((e) => e.event === "release")).toBe(false);
    expect(await f.run()).toBe("held");
    join.resolve();
    expect(await running).toBe("released");
    expect((await f.head()).owner).toBeNull();
  } finally {
    join.resolve();
    await running?.catch(() => {});
    await f.finish();
  }
});
