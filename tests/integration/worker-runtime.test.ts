import { expect, it } from "vitest";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { check, computeSha256 } from "../../src/domain.ts";
import { defineDoc } from "@earendil-works/pi-durable";
import { runtimeDependencies, worker } from "../../src/index.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import { EnvelopeRejected } from "../../src/payload.ts";
import { provider } from "./postgres.ts";
import { workerFixture } from "./worker-support.ts";

for (const cut of ["object-factory", "storage-connect"] as const)
  it(`runtime acquisition failure at ${cut} closes everything acquired and retains ownership`, async () => {
    const f = await workerFixture("worker_acquire");
    const cause = Error(cut);
    let closes = 0;
    const broken = createSqlBackend(
      {
        ...provider,
        connect: async () => {
          throw cause;
        },
      },
      (ns) => {
        if (cut === "object-factory") throw cause;
        const store = f.objects.objects(ns),
          close = store.close.bind(store);
        store.close = () => {
          closes++;
          close();
        };
        return store;
      },
    );
    const deps = runtimeDependencies({
      ...f.options,
      backend: {
        connectAuthority: () => f.backend.connectAuthority(),
        openStorage: broken.openStorage,
      },
    });
    try {
      await f.receive();
      await expect(worker(f.ns, () => 45000, deps)).rejects.toBe(cause);
      expect(closes).toBe(cut === "storage-connect" ? 1 : 0);
      expect(f.objects.openHandles()).toBe(0);
      expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
      expect(f.events.some((e) => e.event === "release")).toBe(false);
    } finally {
      await f.finish();
    }
  });

it("runtime tolerates repeated Storage close after Harness initialization fails", async () => {
  const f = await workerFixture("worker_open_tasks");
  const cause = Error("open tasks failed");
  let closes = 0;
  const open = f.backend.openStorage;
  f.backend.openStorage = async (lease) => {
    const storage = await open(lease),
      scanTasks = storage.scanTasks.bind(storage),
      close = storage.close.bind(storage);
    storage.scanTasks = async (...args) => {
      await scanTasks(...args);
      throw cause;
    };
    storage.close = async (c) => {
      closes++;
      await close(c);
    };
    return storage;
  };
  try {
    await f.receive();
    await expect(f.run()).rejects.toBe(cause);
    // Pi closes its constructed Session; runtime must also cover pre-construction failures.
    expect(closes).toBe(2);
    expect(f.objects.openHandles()).toBe(0);
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    expect(f.events.some((e) => e.event === "release")).toBe(false);
  } finally {
    await f.finish();
  }
});

it("late Session failure after the final durable scheduler commit never turns into a clean release", async () => {
  const f = await workerFixture("worker_late_failure");
  const cause = Error("final acknowledgement lost");
  let fired = false,
    closed = false;
  const open = f.backend.openStorage;
  f.backend.openStorage = async (lease) => {
    const s = await open(lease),
      commit = s.commit.bind(s),
      close = s.close.bind(s);
    s.commit = async (writes, c) => {
      const seq = await commit(writes, c);
      if (
        !fired &&
        writes.some((w) => w.type === "task" && w.value.state.status === "terminal") &&
        !(await f.authority.hasWork(f.ns))
      ) {
        fired = true;
        throw cause;
      }
      return seq;
    };
    s.close = async (c) => {
      await close(c);
      closed = true;
    };
    return s;
  };
  try {
    await f.receive();
    await expect(f.run()).rejects.toBe(cause);
    expect(fired).toBe(true);
    expect(closed).toBe(true);
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    expect(f.events.some((e) => e.event === "release")).toBe(false);
    await f.expire();
    const output = await f.output();
    expect(output.submissions.map((s) => s.status)).toEqual(["done"]);
    expect(output.tasks.every((t) => t.state.status === "terminal")).toBe(true);
    expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
  } finally {
    await f.finish();
  }
});

it("a takeover between clean close and release returns lost without disturbing the new owner", async () => {
  const f = await workerFixture("worker_lost");
  const connect = f.deps.connectAuthority;
  let successor: unknown;
  f.deps.connectAuthority = async () => {
    const c = await connect(),
      release = c.release.bind(c);
    c.release = async (lease) => {
      expect(f.objects.openHandles()).toBe(0);
      expect(f.events.some((e) => e.event === "closed")).toBe(true);
      await f.expire();
      expect((await f.authority.claim(f.ns, 45000)).kind).toBe("claimed");
      successor = await f.head();
      return release(lease);
    };
    return c;
  };
  try {
    await f.receive();
    expect(await f.run()).toBe("lost");
    expect(await f.head()).toEqual(successor);
    expect(f.hints).toEqual([f.ns]);
  } finally {
    await f.finish();
  }
});

it("worker root initialization cannot bypass the aggregate logical write budget", async () => {
  const f = await workerFixture("worker_budget");
  const docs = Array.from({ length: 3 }, (_, i) =>
    defineDoc<{ text: string }>({
      kind: `app.large-${i}`,
      version: 1,
      scope: "conversation",
      history: "rewindable",
      fork: "asOf",
      initial: () => ({ text: "x".repeat(11 * 1024 * 1024) }),
    }),
  );
  const deps = runtimeDependencies({
    ...f.options,
    harnessOptions: {
      ...f.options.harnessOptions,
      conversationCreated: async (tx, conversation) => {
        for (const doc of docs) await tx.doc(doc, conversation.id);
      },
    },
  });
  try {
    await f.receive();
    await expect(worker(f.ns, () => 45000, deps)).rejects.toBeInstanceOf(EnvelopeRejected);
    expect(f.objects.publishedKeys()).toEqual([]);
    expect(
      await f.authority.inspectionDb.query("SELECT id FROM records WHERE ns=$1", [f.ns]),
    ).toEqual([]);
    expect(await f.authority.received(f.ns)).toHaveLength(1);
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    await f.finish();
  }
});

it("worker-created history objects above the receipt limit remain cold-readable", async () => {
  const f = await workerFixture("worker_history_object");
  const value = "x".repeat(9 * 1024 * 1024);
  const token = defineDoc<{ text: string }>({
    kind: "app.large-history",
    version: 1,
    scope: "conversation",
    history: "rewindable",
    fork: "asOf",
    initial: () => ({ text: value }),
  });
  const deps = runtimeDependencies({
    ...f.options,
    harnessOptions: {
      ...f.options.harnessOptions,
      conversationCreated: async (tx, conversation) => {
        await tx.doc(token, conversation.id);
      },
    },
  });
  try {
    await f.receive();
    expect(await worker(f.ns, () => 45000, deps)).toBe("released");
    expect(
      f.objects
        .publishedKeys()
        .some((key) => f.objects.inspectPublished(key)!.length > 8 * 1024 * 1024),
    ).toBe(true);
    const claimed = await f.authority.claim(f.ns, 45000);
    check(claimed.kind === "claimed", "Cold reader ownership");
    const { harness, root } = await deps.open(claimed.lease);
    try {
      const cold = await harness.snapshot(token, root.id, context);
      expect(cold?.text.length).toBe(value.length);
      expect(computeSha256(cold!.text)).toBe(computeSha256(value));
    } finally {
      await harness.close(context);
      await f.authority.release(claimed.lease);
    }
  } finally {
    await f.finish();
  }
});

it("an object reader failure closes its handle and leaves the receipt recoverable", async () => {
  const f = await workerFixture("worker_reader");
  const cause = Error("payload read unavailable");
  let closes = 0;
  const deps = runtimeDependencies({
    ...f.options,
    objects: (ns) => {
      const store = f.objects.objects(ns);
      return {
        get: async () => {
          throw cause;
        },
        close: () => {
          closes++;
          store.close();
        },
      };
    },
  });
  try {
    const staged = f.objects.stage(
      f.ns,
      JSON.stringify({ content: "x".repeat(300000), whenBusy: "followUp" }),
    );
    expect(
      await f.api({ ns: f.ns, request: "one", command: { kind: "s3", ...staged } }),
    ).toMatchObject({ ok: true });
    f.objects.deleteStage(staged.key);
    await expect(worker(f.ns, () => 45000, deps)).rejects.toBe(cause);
    expect(closes).toBe(1);
    expect(await f.authority.received(f.ns)).toHaveLength(1);
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    await f.expire();
    expect(await f.run()).toBe("released");
    expect((await f.output()).submissions[0]!.status).toBe("done");
  } finally {
    await f.finish();
  }
});
