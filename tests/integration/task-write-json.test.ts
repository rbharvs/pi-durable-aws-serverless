import { expect, it } from "vitest";
import { StorageRequestError } from "@earendil-works/pi-durable";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { context, dataEntry, historyFixture, memoryObjects, queryGate } from "./history-support.ts";
import { submission, submissionId, task, taskId } from "./task-support.ts";

it("rejects a malformed frozen record before it can wait for an occupied connection gate", async () => {
  const gate = queryGate("before", (sql) => sql.includes("AND id=$2 AND type=$3"));
  const f = await historyFixture("invalid_before_gate", { hook: gate.hook });
  let reading: Promise<unknown> | undefined;
  let writing: Promise<unknown> | undefined;
  try {
    await f.storage.commit([task()], context);
    gate.arm();
    reading = f.storage.task(taskId(20), context);
    await gate.reached;
    const queries = f.trace.length;
    let rejected: unknown;
    writing = f.storage
      .commit([{ type: "task", value: { ...task().value, version: NaN } }], context)
      .catch((error: unknown) => {
        rejected = error;
      });
    await Promise.resolve();
    expect(rejected).toBeInstanceOf(StorageRequestError);
    expect(f.trace).toHaveLength(queries);
  } finally {
    gate.release();
    await Promise.allSettled([reading, writing]);
    await f.finish();
  }
});

it("uses the serialized numeric identity and flags for indexes and allocation, not boxed caller values", async () => {
  const f = await historyFixture("normalized_indexes");
  try {
    const write = {
      type: "task",
      value: {
        ...task().value,
        id: Object(20),
        kind: Object("boxed"),
        version: Object(1),
        background: Object(false),
        abortRequested: Object(false),
      },
    } as StorageWrite;
    await f.storage.commit([write], context);
    expect(await f.storage.mintId()).toBe(21);
    const expected = { ...task().value, kind: "boxed" };
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual(expected);
    expect(
      (
        await cold.scanTasks(
          { kind: "boxed", background: false, abortRequested: false },
          10,
          undefined,
          context,
        )
      ).items,
    ).toEqual([expected]);
  } finally {
    await f.finish();
  }
});

it("normalizes omitted, nonfinite and toJSON-capable data to the exact stored JSON before validation", async () => {
  const f = await historyFixture("normalized_data");
  try {
    const write = {
      type: "task",
      value: {
        ...task().value,
        input: {
          omitted: undefined,
          values: [undefined, NaN, Infinity],
          time: new Date("2020-01-01T00:00:00Z"),
        },
        state: { status: "terminal", outcome: { status: "completed", result: NaN } },
        startedAt: undefined,
      },
    } as unknown as StorageWrite;
    const requested = {
      ...submission(),
      value: { ...submission().value, requestId: new Date("2020-01-01T00:00:00Z") },
    } as unknown as StorageWrite;
    await f.storage.commit([write, requested], context);
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual({
      ...task().value,
      input: { values: [null, null, null], time: "2020-01-01T00:00:00.000Z" },
      state: { status: "terminal", outcome: { status: "completed", result: null } },
    });
    expect((await cold.submission(submissionId(30), context))?.requestId).toBe(
      "2020-01-01T00:00:00.000Z",
    );
    expect(
      (
        await cold.submissionByRequest(
          task().value.conversationId,
          "2020-01-01T00:00:00.000Z",
          context,
        )
      )?.id,
    ).toBe(30);
  } finally {
    await f.finish();
  }
});

it("rejects nonfinite semantic metadata after JSON normalization, rather than accepting null versions", async () => {
  const f = await historyFixture("normalized_bad_metadata", { objects: memoryObjects });
  try {
    const before = await f.snapshot();
    f.trace.length = 0;
    await expect(
      f.storage.commit([{ type: "task", value: { ...task().value, version: NaN } }], context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    expect(f.trace).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
  } finally {
    await f.finish();
  }
});

it("snapshots own getters once and discards custom prototypes before validation and storage", async () => {
  const f = await historyFixture("normalized_class");
  let reads = 0;
  let serializers = 0;
  let input = "original";
  class CallerValue {
    toJSON() {
      serializers++;
      return { hidden: true };
    }
  }
  try {
    const value = Object.assign(new CallerValue(), task().value);
    Object.defineProperty(value, "input", {
      enumerable: true,
      get() {
        reads++;
        return input;
      },
    });
    const committing = f.storage.commit([{ type: "task", value }], context);
    input = "mutated";
    await committing;
    expect(reads).toBe(1);
    expect(serializers).toBe(0);
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual({ ...task().value, input: "original" });
  } finally {
    await f.finish();
  }
});

it("rejects an own toJSON function as uncloneable without executing it or issuing I/O", async () => {
  const f = await historyFixture("own_serializer");
  let calls = 0;
  try {
    const value = {
      ...task().value,
      toJSON() {
        calls++;
        return task().value;
      },
    };
    const before = await f.snapshot();
    f.trace.length = 0;
    await expect(f.storage.commit([{ type: "task", value }], context)).rejects.toMatchObject({
      name: "DataCloneError",
    });
    expect(calls).toBe(0);
    expect(f.trace).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
  } finally {
    await f.finish();
  }
});

it("enforces the logical budget on detached stored fields, not a caller's small toJSON substitute", async () => {
  const f = await historyFixture("snapshot_budget", { objects: memoryObjects });
  class CallerValue {
    toJSON() {
      return {};
    }
  }
  try {
    const large = "x".repeat(9 * 1024 * 1024);
    const writes = Array.from({ length: 4 }, (_, i) => {
      const write = dataEntry(20 + i, large);
      return { ...write, value: Object.assign(new CallerValue(), write.value) };
    });
    const before = await f.snapshot();
    f.trace.length = 0;
    await expect(f.storage.commit(writes, context)).rejects.toThrow(
      "Logical commit exceeds 32 MiB",
    );
    expect(f.trace).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
  } finally {
    await f.finish();
  }
});
