import { expect, it } from "vitest";
import { runtimeDependencies, worker, type WorkerDependencies } from "../../src/index.ts";
import { workerFixture } from "./worker-support.ts";

for (const fail of [false, true])
  it(`class event method observes ${fail ? "worker error" : "normal release"} and cleanup diagnostics exactly once`, async () => {
    const f = await workerFixture("worker_event_receiver");
    const cause = Error("resolve failed");
    class Ports implements WorkerDependencies {
      readonly seen: { name: string; fields: Record<string, unknown> }[] = [];
      open = f.deps.open;
      async connectAuthority() {
        const authority = await f.deps.connectAuthority(),
          close = authority.close.bind(authority);
        authority.close = async () => {
          await close();
          throw Error("authority cleanup failed");
        };
        return authority;
      }
      async resolve(item: Parameters<WorkerDependencies["resolve"]>[0]) {
        if (fail) throw cause;
        return f.deps.resolve(item);
      }
      event(name: string, fields: Record<string, unknown>) {
        this.seen.push({ name, fields });
        // Observer failure must neither alter the outcome nor cause a second delivery.
        if (["claimed", "closing", "closed", "worker-error"].includes(name))
          throw Error("observer failed");
      }
    }
    const ports = new Ports();
    try {
      await f.receive();
      const running = worker(f.ns, () => 45000, ports);
      if (fail) await expect(running).rejects.toBe(cause);
      else await expect(running).resolves.toBe("released");
      const names = ports.seen.map((e) => e.name);
      expect(names.filter((n) => n === "claimed")).toEqual(["claimed"]);
      expect(names.filter((n) => n === "resolving")).toEqual(["resolving"]);
      expect(names.filter((n) => n === "closing")).toEqual(["closing"]);
      expect(names.filter((n) => n === "closed")).toEqual(["closed"]);
      expect(names.indexOf("closing")).toBeLessThan(names.indexOf("closed"));
      expect(ports.seen.filter((e) => e.name === "cleanup-failed")).toEqual([
        { name: "cleanup-failed", fields: { resource: "authority" } },
      ]);
      if (fail) {
        expect(names.indexOf("closed")).toBeLessThan(names.indexOf("worker-error"));
        expect(names.indexOf("worker-error")).toBeLessThan(names.indexOf("cleanup-failed"));
        expect(ports.seen.filter((e) => e.name === "worker-error")).toEqual([
          { name: "worker-error", fields: { ns: f.ns, cause: "Error" } },
        ]);
        expect(names).not.toContain("release");
        expect((await f.head()).owner).not.toBeNull();
      } else {
        expect(names.filter((n) => n === "submitting")).toEqual(["submitting"]);
        expect(names.filter((n) => n === "admitted")).toEqual(["admitted"]);
        expect(ports.seen.filter((e) => e.name === "release")).toEqual([
          {
            name: "release",
            fields: { ns: f.ns, epoch: Number((await f.head()).epoch), result: "released" },
          },
        ]);
        expect(names.indexOf("closed")).toBeLessThan(names.indexOf("release"));
        expect(names).not.toContain("worker-error");
        expect((await f.output()).submissions[0]!.status).toBe("done");
      }
    } finally {
      await f.finish();
    }
  });

it("runtime preserves its options event receiver for payload cleanup and worker events", async () => {
  const f = await workerFixture("runtime_event_receiver");
  const options = {
    ...f.options,
    seen: [] as { name: string; fields: Record<string, unknown> }[],
    event(name: string, fields: Record<string, unknown>) {
      this.seen.push({ name, fields });
    },
    objects(ns: typeof f.ns) {
      const store = f.objects.objects(ns),
        close = store.close.bind(store);
      store.close = () => {
        close();
        throw Error("reader cleanup failed");
      };
      return store;
    },
  };
  try {
    await f.receive();
    expect(await worker(f.ns, () => 45000, runtimeDependencies(options))).toBe("released");
    expect(options.seen.filter((e) => e.name === "cleanup-failed")).toEqual([
      { name: "cleanup-failed", fields: { resource: "objects" } },
    ]);
    expect(options.seen.filter((e) => e.name === "release")).toHaveLength(1);
    expect((await f.output()).submissions[0]!.status).toBe("done");
  } finally {
    await f.finish();
  }
});
