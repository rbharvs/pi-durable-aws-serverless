import { expect, it, vi } from "vitest";
import { BEGIN_READ, BEGIN_WRITE } from "../../src/sql/db.ts";
import { Payloads } from "../../src/sql/payloads.ts";
import { LIMITS } from "../../src/limits.ts";
import {
  assertPublication,
  mutations,
  observePlans,
  parameterBytes,
} from "./history-payload-support.ts";
import {
  context,
  dataEntry,
  deferred,
  entryId,
  historyFixture,
  memoryObjects,
  rootId,
} from "./history-support.ts";

it("requires an explicit object selector before the history fixture can publish offloaded data", async () => {
  const f = await historyFixture("explicit_object_selector");

  try {
    const before = await f.snapshot();

    await expect(
      f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context),
    ).rejects.toThrow("Unexpected object publication in a small-history test");
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    await f.storage.commit([dataEntry(20, "inline")], context);
    expect((await f.storage.entry(entryId(20), context))?.entry.data).toBe("inline");
  } finally {
    await f.finish();
  }
});

it("rejects the physical row envelope before PUT or write BEGIN, without changing durable state", async () => {
  const f = await historyFixture("payload_rows", { objects: memoryObjects });

  try {
    const before = await f.snapshot();
    const writes = Array.from({ length: 500 }, (_, i) =>
      dataEntry(20 + i, i === 0 ? "x".repeat(300 * 1024) : "small"),
    );

    await expect(f.storage.commit(writes, context)).rejects.toThrow("Atomic envelope");
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(mutations(f.trace)).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    await f.storage.commit([dataEntry(2000, "valid")], context);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toHaveLength(1);
  } finally {
    await f.finish();
  }
});

it("rejects a logical commit over 32 MiB before the gate, SQL, or PUT while a read is held", async () => {
  const entered = deferred();
  const release = deferred();
  let hold = false;
  const f = await historyFixture("payload_logical", {
    hook: async (phase, sql) => {
      if (hold && phase === "before" && sql === BEGIN_READ) {
        hold = false;
        entered.resolve();
        await release.promise;
      }
    },
    objects: memoryObjects,
  });
  let read: Promise<unknown> | undefined;
  let publication: Promise<unknown> | undefined;

  try {
    await f.storage.commit([{ type: "conversation", value: { id: rootId } }], context);
    const before = await f.snapshot();
    const data = "x".repeat(11 * 1024 * 1024);
    const writes = [dataEntry(20, data), dataEntry(21, data), dataEntry(22, data)];

    expect(writes.length).toBeLessThan(1000);
    expect(Buffer.byteLength(JSON.stringify(writes))).toBeGreaterThan(32 * 1024 * 1024);
    for (const write of writes)
      expect(Buffer.byteLength(JSON.stringify(write.value))).toBeLessThanOrEqual(16 * 1024 * 1024);
    hold = true;
    read = f.storage.conversation(rootId, context);
    await entered.promise;
    // Discard the held read's BEGIN attempt. The rejected commit must issue no SQL at all.
    f.trace.length = 0;
    publication = f.storage.commit(writes, context).then(
      () => ({ kind: "fulfilled" as const }),
      (reason: unknown) => ({ kind: "rejected" as const, reason }),
    );
    const outcome = await Promise.race([
      publication,
      new Promise<"queued">((resolve) => setImmediate(() => resolve("queued"))),
    ]);

    // A post-gate check would still be blocked by the held read. Observe the rejection
    // before releasing it, and always release in finally so a regression cannot strand SQL.
    expect(outcome).toMatchObject({
      kind: "rejected",
      reason: { message: "Logical commit exceeds 32 MiB memory bound" },
    });
    expect(f.trace).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    release.resolve();
    await Promise.allSettled([read, publication]);
    await f.finish();
  }
});

it("rejects in-batch duplicate IDs before PUT or write BEGIN rather than relying on a SQL unique key", async () => {
  const f = await historyFixture("payload_duplicate", { objects: memoryObjects });

  try {
    await f.storage.commit([{ type: "conversation", value: { id: rootId } }], context);
    const before = await f.snapshot();

    f.trace.length = 0;
    await expect(
      f.storage.commit(
        [dataEntry(20, "x".repeat(300 * 1024)), dataEntry(20, "duplicate")],
        context,
      ),
    ).rejects.toThrow("ID 20 is written more than once");
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(mutations(f.trace)).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    await f.finish();
  }
});

it("rejects oversized SQL metadata before uploading a physically over-byte plan", async () => {
  const f = await historyFixture("payload_bytes", { objects: memoryObjects });
  const actual = Payloads.prototype.plan;
  const spy = vi.spyOn(Payloads.prototype, "plan").mockImplementation(function (
    this: Payloads,
    values,
    metadata,
  ) {
    // Synthetic future metadata pressure: do not pretend normal entry metadata is this large.
    return actual.call(this, values, metadata + LIMITS.maxTransactionBytes);
  });

  try {
    const before = await f.snapshot();

    await expect(
      f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context),
    ).rejects.toThrow("Atomic envelope");
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(mutations(f.trace)).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    spy.mockRestore();
    await f.finish();
  }
});

it("accounts for owner/payload rows, statements, and primitive UTF-8 bind bytes", async () => {
  let transactionOpen = false;
  let uploads = 0;
  const f = await historyFixture("payload_accounting", {
    hook: async (phase, sql) => {
      if (phase !== "after") return;
      if (sql.startsWith("BEGIN")) transactionOpen = true;
      if (sql === "COMMIT" || sql === "ROLLBACK") transactionOpen = false;
      if (sql === BEGIN_WRITE) expect(uploads).toBe(1);
    },
    objects: (_ns, objects) => ({
      ...objects,
      async put(json) {
        expect(transactionOpen).toBe(false);
        const ref = await objects.put(json);

        uploads++;

        return ref;
      },
    }),
  });
  const plans = observePlans();

  try {
    await f.storage.commit(
      [dataEntry(20, '\n"🚀'), dataEntry(21, { text: "x".repeat(300 * 1024) })],
      context,
    );
    expect(plans.plans).toHaveLength(1);
    assertPublication(f.trace, plans.plans[0]!);
    expect(mutations(f.trace)).toHaveLength(5); // fence + two immutable owner/payload pairs
    expect(mutations(f.trace).filter((q) => q.sql.startsWith("DELETE"))).toEqual([]);
    expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(1);
    expect(parameterBytes(["\n", '"', "🚀", true, 123, null])).toBe(37);
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("retries identical frozen publication statements after a confirmed abort, preparing and uploading once", async () => {
  let publishing = false;
  let abort = true;
  const original = Object.assign(Error("confirmed publication abort"), { code: "40001" });
  const f = await historyFixture("payload_retry", {
    hook: async (phase, sql) => {
      if (phase === "after" && sql.startsWith("UPDATE harness SET next_seq")) publishing = true;
      if (phase === "before" && sql === "COMMIT" && publishing && abort) {
        abort = false;
        publishing = false;

        throw original;
      }
    },
    objects: memoryObjects,
  });
  const plans = observePlans();

  try {
    const data = "x".repeat(300 * 1024);

    await f.storage.commit([dataEntry(20, data)], context);
    const starts = f.trace.flatMap((q, i) => (q.sql === BEGIN_WRITE ? [i] : []));

    expect(starts).toHaveLength(2);
    const first = f.trace.slice(starts[0], starts[1]);
    const last = f.trace.slice(starts[1]);
    const sent = (trace: typeof f.trace) =>
      mutations(trace).map((q) => ({ sql: q.sql, params: q.params }));

    expect(sent(first)).toEqual(sent(last));
    expect(f.trace.filter((q) => q.sql === "ROLLBACK")).toHaveLength(1);
    expect(plans.plans).toHaveLength(1);
    expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(1);
    assertPublication(last, plans.plans[0]!);
    expect((await f.storage.entry(entryId(20), context))?.entry.data).toBe(data);
    expect(
      (await f.control.query("SELECT next_seq FROM harness WHERE ns=$1", [f.ns]))[0]!.next_seq,
    ).toBe("2");
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("exhausted confirmed publication aborts leave only one object orphan, not DB payload rows", async () => {
  const failure = Object.assign(Error("confirmed abort"), { code: "40001" });
  const f = await historyFixture("payload_exhausted", {
    hook: async (phase, sql) => {
      if (phase === "before" && sql.startsWith("UPDATE harness SET next_seq")) throw failure;
    },
    objects: memoryObjects,
  });

  try {
    const before = await f.snapshot();

    await expect(f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context)).rejects.toBe(
      failure,
    );
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toHaveLength(3);
    expect(f.trace.filter((q) => q.sql === "ROLLBACK")).toHaveLength(3);
    expect(f.trace.filter((q) => q.sql.startsWith("INSERT"))).toEqual([]);
    expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(1);
    expect(f.memory.bodies.size).toBe(1);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    await f.finish();
  }
});

for (const interruption of ["before-BEGIN", "after-payload-insert"] as const)
  it(`publication interruption ${interruption} permits an object orphan but no DB leak`, async () => {
    const failure = Error("publication interrupted");
    const f = await historyFixture("payload_interrupted", {
      hook: async (phase, sql) => {
        if (
          (interruption === "before-BEGIN" && phase === "before" && sql === BEGIN_WRITE) ||
          (interruption === "after-payload-insert" &&
            phase === "after" &&
            sql.startsWith("INSERT INTO s3_payloads"))
        )
          throw failure;
      },
      objects: memoryObjects,
    });

    try {
      const before = await f.snapshot();

      await expect(f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context)).rejects.toBe(
        failure,
      );
      expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(1);
      expect(f.memory.bodies.size).toBe(1);
      expect(await f.snapshot()).toEqual(before);
    } finally {
      await f.finish();
    }
  });

it("detaches nested writes and their array at invocation, even while queued behind a read", async () => {
  const entered = deferred();
  const release = deferred();
  let hold = false;
  const f = await historyFixture("payload_detached", {
    hook: async (phase, sql) => {
      if (hold && phase === "before" && sql === BEGIN_READ) {
        hold = false;
        entered.resolve();
        await release.promise;
      }
    },
    objects: memoryObjects,
  });

  try {
    await f.storage.commit([{ type: "conversation", value: { id: rootId } }], context);
    hold = true;
    const read = f.storage.conversation(rootId, context);

    await entered.promise;
    const data = { text: "original", nested: ["snapshot"] };
    const write = dataEntry(20, data);
    const writes = [write];
    const publication = f.storage.commit(writes, context);

    data.text = "changed".repeat(50000);
    data.nested[0] = "changed";
    write.value.id = entryId(99);
    write.value.kind = "changed";
    writes.splice(0, 1, dataEntry(21, "replacement"));
    release.resolve();
    await read;
    await publication;

    expect((await f.storage.entry(entryId(20), context))?.entry).toEqual({
      id: 20,
      conversationId: rootId,
      kind: "payload",
      data: { text: "original", nested: ["snapshot"] },
    });
    expect(await f.storage.entry(entryId(21), context)).toBeUndefined();
    expect(await f.storage.entry(entryId(99), context)).toBeUndefined();
    expect(f.memory.operations).toEqual([]);
  } finally {
    release.resolve();
    await f.finish();
  }
});

it("joins bounded in-flight uploads after failure before rejecting, without starting publication", async () => {
  const gates = Array.from({ length: 6 }, () => deferred());
  const started = deferred();
  const failure = Error("upload failed");
  let active = 0;
  let peak = 0;
  let calls = 0;
  const f = await historyFixture("payload_joined", {
    objects: (_ns, objects) => ({
      ...objects,
      async put(json) {
        const index = calls++;

        active++;
        peak = Math.max(peak, active);
        if (calls === 4) started.resolve();

        try {
          await gates[index]!.promise;
          if (index === 0) throw failure;

          return await objects.put(json);
        } finally {
          active--;
        }
      },
    }),
  });
  let settled = false;
  const operation = f.storage
    .commit(
      Array.from({ length: 6 }, (_, i) => dataEntry(20 + i, "x".repeat(300 * 1024))),
      context,
    )
    .then(
      () => {
        settled = true;

        return undefined;
      },
      (error: unknown) => {
        settled = true;

        return error;
      },
    );

  try {
    const before = await f.snapshot();

    await started.promise;
    expect(active).toBe(4);
    expect(calls).toBe(4);
    gates[0]!.resolve();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);
    expect(active).toBe(3);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    for (const gate of gates) gate.resolve();
    expect(await operation).toBe(failure);
    expect(active).toBe(0);
    expect(peak).toBe(4);
    expect(calls).toBe(4);
    expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(3);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(mutations(f.trace)).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    for (const gate of gates) gate.resolve();
    await operation;
    await f.finish();
  }
});
