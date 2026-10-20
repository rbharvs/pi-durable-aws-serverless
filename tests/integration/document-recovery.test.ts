import { expect, it } from "vitest";
import { createModels } from "@earendil-works/pi-ai";
import { Harness, createRegistry, type StorageWrite } from "@earendil-works/pi-durable";
import { EnvelopeRejected } from "../../src/payload.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { context, conversation } from "./history-support.ts";
import { documentFixture } from "./document-support.ts";
import { task } from "./task-support.ts";

for (const count of [120, 334])
  it(`actual Harness cold recovery of ${count} running tasks is ${count === 120 ? "atomic and durable" : "rejected before publication"}`, async () => {
    const f = await documentFixture("pi_recovery");
    try {
      const storage = await f.openStorage();
      await storage.commit([conversation()], context);
      const writes: StorageWrite[] = Array.from({ length: count }, (_, i) => ({
        type: "task",
        value: {
          ...task(i + 20, { status: "running", checkpoint: { phase: "ready" } }).value,
          kind: "never-dispatched",
          input: { text: `${i}:` + "x".repeat(48 * 1024) },
        },
      }));
      for (let i = 0; i < count; i += 100) await storage.commit(writes.slice(i, i + 100), context);
      await storage.close(context);
      const before = await f.snapshot();
      const puts = f.memory.operations.filter((op) => op.operation === "PUT").length;
      const nextSeq = Number((await f.head()).next_seq);
      const cold = await f.openStorage();
      f.trace.length = 0;
      const recovery = Harness.open(
        cold,
        { models: createModels(), registry: createRegistry() },
        context,
      );
      if (count === 120) {
        const h = await recovery;
        try {
          expect((await f.head()).next_seq).toBe(String(nextSeq + 1));
          expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toHaveLength(1);
        } finally {
          await h.close(context);
        }
        const reopened = await f.openStorage();
        const pending = await reopened.scanTasks({ status: "pending" }, 200, undefined, context);
        expect(pending.items).toHaveLength(120);
        expect(pending.items.map((t) => t.input)).toEqual(
          writes.map((w) => (w.type === "task" ? w.value.input : null)),
        );
        expect(
          (await reopened.scanTasks({ status: "running" }, 200, undefined, context)).items,
        ).toEqual([]);
      } else {
        await expect(recovery).rejects.toBeInstanceOf(EnvelopeRejected);
        expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
        expect(f.memory.operations.filter((op) => op.operation === "PUT")).toHaveLength(puts);
        expect(await f.snapshot()).toEqual(before);
        const reopened = await f.openStorage();
        expect(
          (await reopened.scanTasks({ status: "running" }, 400, undefined, context)).items,
        ).toHaveLength(count);
      }
    } finally {
      await f.finish();
    }
  });
