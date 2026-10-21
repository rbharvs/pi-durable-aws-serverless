import { expect, it } from "vitest";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import type { Conversation, Harness } from "@earendil-works/pi-durable";
import { worker, type WorkerDependencies } from "../../src/index.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { workerFixture } from "./worker-support.ts";

for (const primary of ["Session", "resolve"] as const) {
  it(`${primary} failure stays primary when writer close also fails`, async () => {
    const cause = new Error(`${primary} cause`);
    const closeFailure = new Error("storage close failed");
    let armed = false;
    const f = await workerFixture("worker_close_cause", false, async (phase, sql) => {
      if (armed && phase === "before" && sql === BEGIN_WRITE) {
        armed = false;
        throw cause;
      }
    });
    const open = f.backend.openStorage;
    let closes = 0;
    let storageClosed = false;
    f.backend.openStorage = async (lease) => {
      const storage = await open(lease);
      const close = storage.close.bind(storage);
      storage.close = async (c) => {
        closes++;
        await close(c);
        storageClosed = true;
        throw closeFailure;
      };
      return storage;
    };
    let harness: Harness | undefined;
    let root: Conversation | undefined;
    let polls = 0;
    let joinedAtError = false;
    const events: string[] = [];
    const ports: WorkerDependencies = {
      ...f.deps,
      async open(lease) {
        const opened = await f.deps.open(lease);
        ({ harness, root } = opened);
        return opened;
      },
      async resolve(item) {
        if (primary === "resolve") throw cause;
        return f.deps.resolve(item);
      },
      async connectAuthority() {
        const authority = await f.deps.connectAuthority();
        const hasWork = authority.hasWork.bind(authority);
        authority.hasWork = async (ns) => {
          // Fail actual Pi while the post-admission poll holds the worker outside its ended guard.
          if (++polls === 2 && primary === "Session") {
            armed = true;
            await root!
              .submit({ type: "input", content: "x", whenBusy: "followUp" }, context)
              .catch(() => {});
            expect(await harness!.closed).toEqual({ reason: "failed", error: cause });
            return false;
          }
          return hasWork(ns);
        };
        return authority;
      },
      event(name) {
        events.push(name);
        if (["closing", "closed", "worker-error"].includes(name)) {
          if (name === "worker-error") joinedAtError = storageClosed;
          throw new Error("observer failure must not change the outcome");
        }
      },
    };
    try {
      await f.receive();
      await expect(worker(f.ns, () => 45000, ports)).rejects.toBe(cause);
      expect(await harness!.closed).toEqual({
        reason: "failed",
        error: primary === "Session" ? cause : closeFailure,
      });
      expect(events.filter((name) => ["closing", "closed", "worker-error"].includes(name))).toEqual(
        ["closing", "closed", "worker-error"],
      );
      expect(events).not.toContain("release");
      expect(joinedAtError).toBe(true);
      expect(closes).toBe(1);
      expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
      expect(f.objects.openHandles()).toBe(0);
    } finally {
      await f.finish();
    }
  });
}
