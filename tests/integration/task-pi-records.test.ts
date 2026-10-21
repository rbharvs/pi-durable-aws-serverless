import type { Context } from "@earendil-works/chord";
import { createModels } from "@earendil-works/pi-ai";
import {
  Harness,
  MemoryStorage,
  StorageRequestError,
  createRegistry,
  defineTask,
  type StorageWrite,
} from "@earendil-works/pi-durable";
import { expect, it } from "vitest";
import { check } from "../../src/domain.ts";
import { context, dataEntry, deferred, historyFixture, memoryObjects } from "./history-support.ts";
import { task, taskId } from "./task-support.ts";

it("rejects a real public JS-style Pi task's string error before SQL, preserving readable neighbours", async () => {
  const captured: Array<Extract<StorageWrite, { type: "task" }>> = [];
  class CaptureStorage extends MemoryStorage {
    override async commit(writes: readonly StorageWrite[], ctx: Context) {
      const seq = await super.commit(writes, ctx);
      for (const write of writes) {
        if (write.type === "task") captured.push(structuredClone(write));
      }
      return seq;
    }
  }
  const invalid = defineTask<null, { phase: "run" }, void>({
    name: "test.js_error",
    version: 1,
    initial: () => ({ phase: "run" }),
    phases: {
      run: async (_task, runtime, ctx) => {
        // A JS caller can violate this type; public Pi currently persists it unchanged.
        await runtime.commit(
          // @ts-expect-error The error must be an object, not a string.
          () => ({ status: "terminal", outcome: { status: "failed", error: "boom" } }),
          ctx,
        );
      },
    },
    abort: async (_task, runtime, ctx) => {
      await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), ctx);
    },
  });
  const registry = createRegistry();
  registry.install({ name: "test", tasks: [invalid] });
  const memory = new CaptureStorage();
  const harness = await Harness.open(memory, { models: createModels(), registry }, context);
  let terminal: Extract<StorageWrite, { type: "task" }> | undefined;
  try {
    const root = await harness.root(context);
    const id = await root.commit(
      (tx) => tx.createTask(invalid, null, { ownership: { kind: "conversation" } }),
      context,
    );
    await harness.waitForTask(id, context);
    terminal = captured.find(
      (w) => w.value.id === Number(id) && w.value.state.status === "terminal",
    );
    check(terminal, "Pi did not emit the invalid terminal record");
    expect(terminal.value.state).toEqual({
      status: "terminal",
      outcome: { status: "failed", error: "boom" },
    });
    expect(await memory.task(id, context)).toEqual(terminal.value);
  } finally {
    await harness.close(context);
  }

  const f = await historyFixture("invalid_pi_write", { objects: memoryObjects });
  try {
    await f.storage.commit([task(20)], context);
    const before = await f.snapshot();
    f.trace.length = 0;
    await expect(
      f.storage.commit([dataEntry(40, "x".repeat(300 * 1024)), terminal], context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    expect(f.trace).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(f.storage.status).toBe("open");
    expect(await f.snapshot()).toEqual(before);
    const cold = await f.cold();
    expect(await cold.task(terminal.value.id, context)).toBeUndefined();
    expect(await cold.task(taskId(20), context)).toEqual(task(20).value);
    expect((await cold.scanTasks({}, 10, undefined, context)).items).toEqual([task(20).value]);
    expect((await cold.scanTasks({ status: "terminal" }, 10, undefined, context)).items).toEqual(
      [],
    );
    expect(await cold.commit([task(22)], context)).toBe(2);
  } finally {
    await f.finish();
  }
});

for (const { label, version, now, input } of [
  { label: "void result", version: 1, now: 123, input: null },
  { label: "void input and result", version: 1, now: 123, input: undefined },
  { label: "fractional version", version: 1.25, now: 123, input: null },
  { label: "negative version", version: -1, now: 123, input: null },
  { label: "fractional clock", version: 1, now: 123.5, input: null },
  { label: "negative clock", version: 1, now: -123.5, input: null },
]) {
  it(`replays real Pi completing/terminal records with ${label} through SQL without changing their shape`, async () => {
    const releaseChild = deferred();
    const completing = deferred();
    const captured: Array<Extract<StorageWrite, { type: "task" }>> = [];
    class CaptureStorage extends MemoryStorage {
      override async commit(writes: readonly StorageWrite[], ctx: Context) {
        const seq = await super.commit(writes, ctx);
        for (const write of writes) {
          if (write.type !== "task" || write.value.kind !== "test.parent") continue;
          captured.push(structuredClone(write));
          if (write.value.state.status === "completing") completing.resolve();
        }
        return seq;
      }
    }
    const child = defineTask<null, { phase: "run" }, void>({
      name: "test.child",
      version: 1,
      initial: () => ({ phase: "run" }),
      phases: {
        run: async (_task, runtime, ctx) => {
          await releaseChild.promise;
          await runtime.commit(
            () => ({ status: "terminal", outcome: { status: "completed", result: undefined } }),
            ctx,
          );
        },
      },
      abort: async (_task, runtime, ctx) => {
        await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), ctx);
      },
    });
    const parent = defineTask<null | void, { phase: "start" } | { phase: "finish" }, void>({
      name: "test.parent",
      version,
      initial: () => ({ phase: "start" }),
      phases: {
        start: async (task, runtime, ctx) => {
          await runtime.commit(async (tx) => {
            await tx.createTask(child, null, { ownership: { kind: "task", taskId: task.id } });
            return { status: "running", checkpoint: { phase: "finish" } };
          }, ctx);
        },
        finish: async (_task, runtime, ctx) => {
          await runtime.commit(
            () => ({ status: "terminal", outcome: { status: "completed", result: undefined } }),
            ctx,
          );
        },
      },
      abort: async (_task, runtime, ctx) => {
        await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), ctx);
      },
    });
    const registry = createRegistry();
    registry.install({ name: "test", tasks: [parent, child] });
    // This is a real complete MemoryStorage. SQL is never passed to Harness or cast to Storage.
    const memory = new CaptureStorage();
    const harness = await Harness.open(
      memory,
      { models: createModels(), registry, now: () => now },
      context,
    );
    try {
      const root = await harness.root(context);
      const id = await root.commit(
        (tx) => tx.createTask(parent, input, { ownership: { kind: "conversation" } }),
        context,
      );
      const settled = harness.waitForTask(id, context);
      await Promise.race([
        completing.promise,
        settled.then(() => {
          throw new Error("Parent settled without the expected completing state");
        }),
      ]);
      releaseChild.resolve();
      await settled;
      for (const status of ["completing", "terminal"] as const) {
        const write = captured.find((w) => w.value.state.status === status);
        check(write, `Pi did not emit ${status}`);
        expect(write.value.state).toEqual({ status, outcome: { status: "completed" } });
        expect(write.value.input).toBe(input);
        expect(Object.hasOwn(write.value, "input")).toBe(input !== undefined);
        expect(write.value.version).toBe(version);
        expect(write.value.startedAt).toBe(now);
        if (status === "terminal") expect(write.value.endedAt).toBe(now);
        const f = await historyFixture("pi_task_record");
        try {
          await f.storage.commit([write], context);
          const cold = await f.cold();
          expect(await cold.task(write.value.id, context)).toEqual(write.value);
          expect((await cold.scanTasks({ status }, 10, undefined, context)).items).toEqual([
            write.value,
          ]);
        } finally {
          await f.finish();
        }
      }
      expect((await memory.task(id, context))?.state).toEqual({
        status: "terminal",
        outcome: { status: "completed" },
      });
    } finally {
      releaseChild.resolve();
      await harness.close(context);
    }
  });
}
