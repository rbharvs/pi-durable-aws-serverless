import { expect, it } from "vitest";
import { runtimeDependencies, worker } from "../../src/index.ts";
import { requestId } from "../../src/domain.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { deferred, workerFixture } from "./worker-support.ts";

it("public receipt hint drives actual Pi to durable output, then a cold worker continues the same conversation", async () => {
  const f = await workerFixture("worker_public");
  try {
    expect(await f.receive()).toMatchObject({ ok: true, duplicate: false });
    expect(f.hints).toEqual([f.ns]);
    await expect(worker(f.hints.shift()!, () => 45000, f.deps)).resolves.toBe("released");
    const first = await f.output();
    expect(first.receipt).toMatchObject({ kind: "admitted", submission: first.submissions[0]!.id });
    expect(first.submissions.map((s) => s.status)).toEqual(["done"]);
    expect(first.tasks.length).toBeGreaterThan(0);
    expect(first.tasks.every((t) => t.state.status === "terminal")).toBe(true);
    expect(first.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
    expect(first.entries.find((e) => e.kind === "pi.assistant")?.model).toMatchObject([
      { role: "assistant", content: [{ type: "text", text: "ok" }] },
    ]);
    expect(await f.receive("two", "continue")).toMatchObject({ ok: true });
    const cold = runtimeDependencies(f.options);
    await expect(worker(f.hints.shift()!, () => 45000, cold)).resolves.toBe("released");
    const second = await f.output();
    expect(second.submissions.map((s) => s.status)).toEqual(["done", "done"]);
    expect(second.entries.filter((e) => e.kind === "pi.user")).toHaveLength(2);
    expect(new Set(second.entries.map((e) => e.conversationId)).size).toBe(1);
    expect(second.entries.slice(0, first.entries.length)).toEqual(first.entries);
    expect(await f.run()).toBe("idle");
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    await f.finish();
  }
});

it("submit-before-mark cut retains ownership; the next owner recovers one logical submission and user entry", async () => {
  const f = await workerFixture("worker_mark");
  const connect = f.deps.connectAuthority;
  let first = true;
  f.deps.connectAuthority = async () => {
    const c = await connect(),
      mark = c.markAdmitted.bind(c);
    c.markAdmitted = async (item, id) => {
      if (first) {
        first = false;
        throw Error("mapping cut");
      }
      await mark(item, id);
    };
    return c;
  };
  try {
    await f.receive();
    await expect(f.run()).rejects.toThrow("mapping cut");
    expect((await f.head()).owner).not.toBeNull();
    expect(await f.authority.received(f.ns)).toHaveLength(1);
    const before = await f.authority.inspectionDb.query(
      "SELECT id FROM records WHERE ns=$1 AND type='submission'",
      [f.ns],
    );
    expect(before).toHaveLength(1);
    await f.expire();
    await expect(f.run()).resolves.toBe("released");
    const output = await f.output();
    expect(output.submissions.map((s) => String(s.id))).toEqual(before.map((r) => r.id));
    expect(output.submissions[0]!.status).toBe("done");
    expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
    expect(await f.authority.received(f.ns)).toEqual([]);
  } finally {
    await f.finish();
  }
});

it("five held competitors cannot disturb a real gated model, its owner, or publications", async () => {
  const f = await workerFixture("worker_held", true);
  const mapped = deferred();
  const connect = f.deps.connectAuthority;
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
    await Promise.all([f.work.started, mapped.promise]);
    const head = await f.head();
    const writes = f.trace.filter((q) => q.sql === BEGIN_WRITE).length;
    expect(writes).toBeGreaterThan(0);
    expect(await Promise.all(Array.from({ length: 5 }, () => f.run()))).toEqual(
      Array(5).fill("held"),
    );
    expect(await f.head()).toEqual(head);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toHaveLength(writes);
    f.work.finish();
    await expect(running).resolves.toBe("released");
    expect(f.trace.filter((q) => q.error)).toEqual([]);
    expect((await f.output()).submissions.map((s) => s.status)).toEqual(["done"]);
    expect((await f.head()).owner).toBeNull();
  } finally {
    f.work.finish();
    await running?.catch(() => {});
    await f.finish();
  }
});

it("the owner admits steer and follow-up arrivals while its model is running", async () => {
  const f = await workerFixture("worker_arrivals", true);
  const steer = deferred(),
    proceed = deferred();
  const connect = f.deps.connectAuthority;
  f.deps.connectAuthority = async () => {
    const c = await connect(),
      mark = c.markAdmitted.bind(c);
    c.markAdmitted = async (item, id) => {
      await mark(item, id);
      if (item.request === "steer") {
        steer.resolve();
        await proceed.promise;
      }
    };
    return c;
  };
  let running: ReturnType<typeof f.run> | undefined;
  try {
    await f.receive();
    running = f.run();
    await f.work.started;
    const head = await f.head();
    await f.receive("steer", "new steering input", "steer");
    await f.receive("follow", "follow-up");
    await steer.promise;
    expect((await f.head()).owner).toBe(head.owner);
    expect(await f.run()).toBe("held");
    expect((await f.authority.get(f.ns, requestId("follow")))?.kind).toBe("received");
    proceed.resolve();
    f.work.finish();
    expect(await running).toBe("released");
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    expect((await f.output()).submissions.map((s) => s.status)).toEqual(["done", "done", "done"]);
  } finally {
    proceed.resolve();
    f.work.finish();
    await running?.catch(() => {});
    await f.finish();
  }
});
