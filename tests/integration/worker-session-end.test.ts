import { expect, it } from "vitest";
import { setImmediate as nextTurn } from "node:timers/promises";
import type { Harness } from "@earendil-works/pi-durable";
import { worker, type WorkerDependencies } from "../../src/index.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { deferred, workerFixture } from "./worker-support.ts";

for (const exit of ["deadline", "in-loop"] as const) {
  it(`failed Session between work polls retains ownership on ${exit} exit`, async () => {
    const cause = new Error("durability failure between worker polls");
    let armed = false;
    const f = await workerFixture("worker_failed_poll", true, async (phase, sql) => {
      if (armed && phase === "before" && sql === BEGIN_WRITE) {
        armed = false;
        throw cause;
      }
    });
    let harness: Harness | undefined;
    let budget = 45000;
    let failedBetweenPolls = false;
    let polledAfterFailure = false;
    const ports: WorkerDependencies = {
      ...f.deps,
      async open(lease) {
        const opened = await f.deps.open(lease);
        harness = opened.harness;
        return opened;
      },
      async connectAuthority() {
        const authority = await f.deps.connectAuthority();
        const hasWork = authority.hasWork.bind(authority);
        authority.hasWork = async (ns) => {
          const work = await hasWork(ns);
          if (!harness) return work; // Keep the real pre-open idle check.
          if (failedBetweenPolls) {
            // Bound a broken worker without waiting for a real invocation deadline.
            polledAfterFailure = true;
            budget = 5000;
            return work;
          }
          expect(work).toBe(true);
          await f.work.started;
          armed = true;
          f.work.finish();
          expect(await harness.closed).toEqual({ reason: "failed", error: cause });
          failedBetweenPolls = true;
          if (exit === "deadline") budget = 5000;
          return work;
        };
        return authority;
      },
    };
    try {
      await f.receive();
      await expect(worker(f.ns, () => budget, ports)).rejects.toBe(cause);
      expect(failedBetweenPolls).toBe(true);
      expect(polledAfterFailure).toBe(false);
      expect(budget).toBe(exit === "deadline" ? 5000 : 45000);
      expect((await f.head()).owner).not.toBeNull();
      expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
      expect(f.events.filter((e) => e.event === "release")).toEqual([]);
    } finally {
      f.work.finish();
      await f.finish();
    }
  });
}

it("resolve failure joins gated writer close before worker rejection", async () => {
  const f = await workerFixture("worker_error_join");
  const closing = deferred();
  const join = deferred();
  const open = f.backend.openStorage;
  let writerClosed = false;
  let workerSettled = false;
  let harness: Harness | undefined;
  let authorityClosing: Promise<void> | undefined;
  f.backend.openStorage = async (lease) => {
    const storage = await open(lease);
    const close = storage.close.bind(storage);
    storage.close = async (context) => {
      closing.resolve();
      await join.promise;
      await close(context);
      writerClosed = true;
    };
    return storage;
  };
  const cause = new Error("resolve failed after open");
  const ports: WorkerDependencies = {
    ...f.deps,
    async open(lease) {
      const opened = await f.deps.open(lease);
      harness = opened.harness;
      return opened;
    },
    async connectAuthority() {
      const authority = await f.deps.connectAuthority();
      const close = authority.close.bind(authority);
      authority.close = () => (authorityClosing = close());
      return authority;
    },
    async resolve() {
      throw cause;
    },
  };
  let running: Promise<unknown> | undefined;
  try {
    await f.receive();
    running = worker(f.ns, () => 45000, ports).then(
      (result) => {
        workerSettled = true;
        return { result };
      },
      (error) => {
        workerSettled = true;
        return { error, writerClosed };
      },
    );
    await closing.promise;
    // Join any authority cleanup already started, then drain its promise continuations.
    // This is an event-loop barrier, not a duration or deadline assertion. A worker may
    // close Authority concurrently, but still must not settle while its writer is gated.
    await authorityClosing;
    await nextTurn();
    expect(writerClosed).toBe(false);
    expect(workerSettled).toBe(false);
    expect((await f.head()).owner).not.toBeNull();
    join.resolve();
    expect(await running).toEqual({ error: cause, writerClosed: true });
    expect(f.events.filter((e) => e.event === "release")).toEqual([]);
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
  } finally {
    join.resolve();
    await running;
    await harness?.closed;
    await f.finish();
  }
});
