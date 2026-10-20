import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { computeSha256 } from "../../src/domain.ts";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import { EnvelopeRejected } from "../../src/payload.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { assertPublication, mutations, observePlans } from "./history-payload-support.ts";
import { context, dataEntry, historyFixture, memoryObjects } from "./history-support.ts";
import { task, taskId } from "./task-support.ts";

it("replaces large inline owners using bounded reference metadata, without retrieving their old bodies", async () => {
  let replacing = false;
  let ownershipBytes = 0;
  const f = await historyFixture("replacement_metadata", {
    hook: async (phase, sql, _params, rows) => {
      if (replacing && phase === "after" && sql.includes("FROM records WHERE ns=$1 AND id IN")) {
        expect(rows).toHaveLength(50);
        ownershipBytes += Buffer.byteLength(JSON.stringify(rows));
        expect(ownershipBytes).toBeLessThan(64 * 1024);
      }
    },
  });
  const plans = observePlans();
  try {
    // Separate small commits keep every prior payload inline; their combined bodies exceed 8 MiB.
    for (let batch = 0; batch < 5; batch++) {
      await f.storage.commit(
        Array.from({ length: 10 }, (_, i) => ({
          type: "task" as const,
          value: { ...task(20 + batch * 10 + i).value, input: "x".repeat(240 * 1024) },
        })),
        context,
      );
    }
    const oldRefs = await f.control.query("SELECT payload_ref FROM inline_payloads WHERE ns=$1", [
      f.ns,
    ]);
    expect(oldRefs).toHaveLength(50);
    f.trace.length = 0;
    plans.plans.length = 0;
    replacing = true;
    const writes = Array.from({ length: 50 }, (_, i) => task(20 + i));
    await f.storage.commit(writes, context);
    expect(ownershipBytes).toBeGreaterThan(0);
    assertPublication(f.trace, plans.plans[0]!);
    expect(mutations(f.trace)).toHaveLength(151); // fence + 50 payload/owner/delete triples
    const removed = mutations(f.trace).filter((q) => q.sql.startsWith("DELETE"));
    expect(removed.map((q) => q.params[1]).sort()).toEqual(
      oldRefs.map((r) => r.payload_ref).sort(),
    );
    const cold = await f.cold();
    expect((await cold.scanTasks({}, 100, undefined, context)).items).toEqual(
      writes.map((w) => w.value),
    );
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("supersedes an invalid previous inline body without parsing it, while reads still reject it", async () => {
  const f = await historyFixture("replacement_old_body");
  try {
    await f.storage.commit([task()], context);
    await f.control.query("UPDATE inline_payloads SET json='not-json' WHERE ns=$1", [f.ns]);
    await expect(f.storage.task(taskId(20), context)).rejects.toThrow("PayloadCorrupt");
    await f.storage.commit([task()], context);
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual(task().value);
  } finally {
    await f.finish();
  }
});

for (const kind of ["inline", "s3"] as const)
  for (const corrupt of [
    "missing",
    "wrong-kind",
    "both-present",
    "invalid-ref",
    ...(kind === "s3" ? ["invalid-digest", "foreign-object"] : []),
  ])
    it(`rejects ${corrupt} ${kind} replacement ownership before uploads or publication`, async () => {
      const f = await historyFixture("replacement_corrupt", { objects: memoryObjects });
      try {
        await f.storage.commit(
          [
            {
              type: "task",
              value: {
                ...task().value,
                input: kind === "inline" ? "small" : "x".repeat(300 * 1024),
              },
            },
          ],
          context,
        );
        const ref = String(
          (await f.control.query("SELECT payload_ref FROM records WHERE ns=$1", [f.ns]))[0]!
            .payload_ref,
        );
        if (corrupt === "missing")
          await f.control.query(
            `DELETE FROM ${kind === "inline" ? "inline_payloads" : "s3_payloads"} WHERE ns=$1`,
            [f.ns],
          );
        if (corrupt === "wrong-kind")
          await f.control.query("UPDATE records SET payload_kind=$2 WHERE ns=$1", [
            f.ns,
            kind === "inline" ? "s3" : "inline",
          ]);
        if (corrupt === "both-present") {
          if (kind === "inline")
            await f.control.query(
              "INSERT INTO s3_payloads(ns,payload_ref,object_key,sha256,bytes) VALUES($1,$2,$3,$4,2)",
              [f.ns, ref, `p/${f.ns}/${randomUUID()}`, computeSha256("{}")],
            );
          else
            await f.control.query(
              "INSERT INTO inline_payloads(ns,payload_ref,json) VALUES($1,$2,'{}')",
              [f.ns, ref],
            );
        }
        if (corrupt === "invalid-ref")
          await f.control.query(
            "UPDATE records SET payload_ref='00000000-0000-0000-0000-000000000000' WHERE ns=$1",
            [f.ns],
          );
        if (corrupt === "invalid-digest")
          await f.control.query("UPDATE s3_payloads SET sha256='BAD' WHERE ns=$1", [f.ns]);
        if (corrupt === "foreign-object")
          await f.control.query("UPDATE s3_payloads SET object_key=$2 WHERE ns=$1", [
            f.ns,
            `p/foreign/${randomUUID()}`,
          ]);
        const before = await f.snapshot();
        f.trace.length = 0;
        f.memory.operations.length = 0;
        await expect(
          f.storage.commit(
            [{ type: "task", value: { ...task().value, input: "new".repeat(100000) } }],
            context,
          ),
        ).rejects.toBeInstanceOf(PayloadCorrupt);
        expect(mutations(f.trace)).toEqual([]);
        expect(f.trace.some((q) => q.sql === BEGIN_WRITE)).toBe(false);
        expect(f.memory.operations).toEqual([]);
        expect(await f.snapshot()).toEqual(before);
      } finally {
        // These tests deliberately sever references. Remove only their exact corrupt namespace.
        for (const table of ["records", "inline_payloads", "s3_payloads"])
          await f.control.query(`DELETE FROM ${table} WHERE ns=$1`, [f.ns]);
        await f.finish();
      }
    });

it("relinks and deletes each intermediate payload through inline → S3 → inline replacements in one fenced commit", async () => {
  const f = await historyFixture("replacement_chain", { objects: memoryObjects });
  const plans = observePlans();
  try {
    await f.storage.commit([task()], context);
    const initial = (
      await f.control.query("SELECT payload_ref FROM records WHERE ns=$1", [f.ns])
    )[0]!.payload_ref;
    const writes = ["first", "x".repeat(300 * 1024), "last"].map((input) => ({
      type: "task" as const,
      value: { ...task().value, input },
    }));
    f.trace.length = 0;
    plans.plans.length = 0;
    await f.storage.commit(writes, context);
    const plan = plans.plans[0]!;
    assertPublication(f.trace, plan);
    const physical = mutations(f.trace);
    expect(physical).toHaveLength(10); // fence + 3 × (payload INSERT, owner replace, old payload DELETE)
    const owners = physical.filter((q) => q.sql.startsWith("INSERT INTO records("));
    const inserted = physical.filter((q) => /^INSERT INTO (inline|s3)_payloads/.test(q.sql));
    const removed = physical.filter((q) => q.sql.startsWith("DELETE"));
    expect(inserted.map((q) => q.sql.split("(")[0])).toEqual([
      "INSERT INTO inline_payloads",
      "INSERT INTO s3_payloads",
      "INSERT INTO inline_payloads",
    ]);
    expect(owners.map((q) => q.params.at(-1))).toEqual(inserted.map((q) => q.params[1]));
    expect(removed.map((q) => q.params[1])).toEqual([
      initial,
      inserted[0]!.params[1],
      inserted[1]!.params[1],
    ]);
    expect(removed.map((q) => q.sql.split(" WHERE")[0])).toEqual([
      "DELETE FROM inline_payloads",
      "DELETE FROM inline_payloads",
      "DELETE FROM s3_payloads",
    ]);
    expect(
      await f.control.query("SELECT payload_ref,json FROM inline_payloads WHERE ns=$1", [f.ns]),
    ).toEqual([{ payload_ref: inserted[2]!.params[1], json: JSON.stringify(writes[2]!.value) }]);
    expect(await f.control.query("SELECT * FROM s3_payloads WHERE ns=$1", [f.ns])).toEqual([]);
    expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(1);
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual(writes[2]!.value);
    expect(f.memory.operations.filter((q) => q.operation === "GET")).toEqual([]);
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("accounts for repeated new owners and pressure-induced offload before joining replacement publication", async () => {
  const f = await historyFixture("replacement_pressure", { objects: memoryObjects });
  const plans = observePlans();
  try {
    const writes = [
      ...Array.from({ length: 40 }, (_, i) => dataEntry(100 + i, "x".repeat((200 + i) * 1024))),
      { type: "task" as const, value: { ...task().value, input: "a".repeat(240 * 1024) } },
      { type: "task" as const, value: { ...task().value, input: "small" } },
    ];
    await f.storage.commit(writes, context);
    assertPublication(f.trace, plans.plans[0]!);
    expect(mutations(f.trace)).toHaveLength(86); // fence + 42 payload/owner pairs + one intermediate DELETE
    const ownerWrites = mutations(f.trace).filter(
      (q) => q.sql.startsWith("INSERT INTO records(") && q.params[1] === 20,
    );
    expect(ownerWrites.map((q) => q.params.at(-2))).toEqual(["s3", "inline"]);
    const deletes = mutations(f.trace).filter((q) => q.sql.startsWith("DELETE"));
    expect(deletes).toHaveLength(1);
    expect(deletes[0]!.sql).toMatch(/^DELETE FROM s3_payloads/);
    expect(deletes[0]!.params[1]).toBe(ownerWrites[0]!.params.at(-1));
    const rows = await f.control.query(
      "SELECT (SELECT count(*) FROM inline_payloads WHERE ns=$1)+(SELECT count(*) FROM s3_payloads WHERE ns=$1) AS n",
      [f.ns],
    );
    expect(Number(rows[0]!.n)).toBe(41);
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual(writes.at(-1)!.value);
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("accepts exactly 1000 physical mutations and rejects the next repeated replacement before uploads or SQL", async () => {
  const f = await historyFixture("replacement_bound", { objects: memoryObjects });
  const plans = observePlans();
  try {
    await f.storage.commit([task()], context);
    const writes = Array.from({ length: 333 }, (_, i) => ({
      type: "task" as const,
      value: { ...task().value, input: i },
    }));
    f.trace.length = 0;
    plans.plans.length = 0;
    await f.storage.commit(writes, context);
    assertPublication(f.trace, plans.plans[0]!);
    expect(mutations(f.trace)).toHaveLength(1000);
    expect(await f.storage.task(taskId(20), context)).toEqual(writes.at(-1)!.value);
    const before = await f.snapshot();
    f.trace.length = 0;
    f.memory.operations.length = 0;
    await expect(
      f.storage.commit(
        [...writes, { type: "task", value: { ...task().value, input: "large".repeat(70000) } }],
        context,
      ),
    ).rejects.toBeInstanceOf(EnvelopeRejected);
    expect(f.trace.some((q) => q.sql === BEGIN_WRITE)).toBe(false);
    expect(mutations(f.trace)).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("fences a stale replacement plan when another handle replaces its previous owner during upload", async () => {
  let armed = false;
  let publishOther: (() => Promise<unknown>) | undefined;
  const f = await historyFixture("replacement_fence", {
    objects: (_ns, objects) => ({
      ...objects,
      async put(json) {
        if (armed) {
          armed = false;
          await publishOther!();
        }
        return objects.put(json);
      },
    }),
  });
  try {
    await f.storage.commit([task()], context);
    const other = await f.open();
    const winner = { type: "task" as const, value: { ...task().value, input: "winner" } };
    publishOther = () => other.storage.commit([winner], context);
    armed = true;
    await expect(
      f.storage.commit(
        [{ type: "task", value: { ...task().value, input: "x".repeat(300 * 1024) } }],
        context,
      ),
    ).rejects.toThrow(/Stale/);
    expect(f.storage.status).toBe("failed");
    expect(await other.storage.task(taskId(20), context)).toEqual(winner.value);
    expect(await f.control.query("SELECT * FROM s3_payloads WHERE ns=$1", [f.ns])).toEqual([]);
    expect(await f.control.query("SELECT json FROM inline_payloads WHERE ns=$1", [f.ns])).toEqual([
      { json: JSON.stringify(winner.value) },
    ]);
  } finally {
    await f.finish();
  }
});

it("keeps the previous offloaded owner visible when a later replacement transaction aborts", async () => {
  let armed = false;
  const f = await historyFixture("replacement_abort", {
    objects: memoryObjects,
    hook: async (phase, sql) => {
      if (armed && phase === "after" && sql.startsWith("DELETE FROM s3_payloads")) {
        armed = false;
        throw Error("replacement failed before COMMIT");
      }
    },
  });
  try {
    const original = {
      type: "task" as const,
      value: { ...task().value, input: "x".repeat(300 * 1024) },
    };
    await f.storage.commit([original], context);
    const before = await f.snapshot();
    armed = true;
    await expect(f.storage.commit([task()], context)).rejects.toThrow("replacement failed");
    expect(await f.snapshot()).toEqual(before);
    expect(await f.storage.task(taskId(20), context)).toEqual(original.value);
    await f.storage.commit([task()], context);
    expect(await f.storage.task(taskId(20), context)).toEqual(task().value);
  } finally {
    await f.finish();
  }
});
