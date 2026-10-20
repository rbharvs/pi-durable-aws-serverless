import { expect, it } from "vitest";
import { computeSha256 } from "../../src/domain.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { assertPublication, mutations, observePlans } from "./history-payload-support.ts";
import { context, dataEntry, historyFixture, memoryObjects, rootId } from "./history-support.ts";

it("offloads the largest immutable entries under byte pressure with exact physical accounting and no DB leak", async () => {
  const f = await historyFixture("payload_pressure", { objects: memoryObjects });
  const plans = observePlans();

  try {
    await f.storage.commit([{ type: "conversation", value: { id: rootId } }], context);
    const writes = Array.from({ length: 40 }, (_, i) =>
      dataEntry(20 + i, "x".repeat((200 + i) * 1024)),
    );

    f.trace.length = 0;
    plans.plans.length = 0;
    await f.storage.commit(writes, context);
    expect(plans.plans).toHaveLength(1);
    assertPublication(f.trace, plans.plans[0]!);
    expect(mutations(f.trace)).toHaveLength(81); // fence + 40 immutable owner/payload pairs
    expect(mutations(f.trace).filter((q) => q.sql.startsWith("DELETE"))).toEqual([]);

    const puts = f.memory.operations.filter((q) => q.operation === "PUT");
    const stored = await f.control.query(
      "SELECT id,payload_kind,payload_ref FROM records WHERE ns=$1 AND type='entry' ORDER BY id",
      [f.ns],
    );
    const offloaded = stored
      .filter((row) => row.payload_kind === "s3")
      .map((row) => Number(row.id));

    expect(puts.length).toBeGreaterThan(1);
    expect(puts.length).toBeLessThan(40);
    expect(offloaded).toEqual(Array.from({ length: puts.length }, (_, i) => 60 - puts.length + i));
    expect(stored.filter((row) => row.payload_kind === "inline")).toHaveLength(40 - puts.length);
    expect(
      puts.map((put) => JSON.parse(put.json!).id).sort((a: number, b: number) => a - b),
    ).toEqual(offloaded);
    const payloadRows = await f.control.query(
      `SELECT (SELECT count(*) FROM inline_payloads WHERE ns=$1)
            + (SELECT count(*) FROM s3_payloads WHERE ns=$1) AS n`,
      [f.ns],
    );

    expect(Number(payloadRows[0]!.n)).toBe(41); // includes the root conversation
    const cold = await f.cold();

    try {
      f.memory.operations.length = 0;
      const page = await cold.scanEntries(
        { conversationId: rootId, order: "ascending" },
        100,
        undefined,
        context,
      );

      expect(page.items).toHaveLength(40);
      expect(page.next).toBeUndefined();
      expect(computeSha256(JSON.stringify(page.items))).toBe(
        computeSha256(JSON.stringify(writes.map((write) => write.value))),
      );
      expect(f.memory.operations.filter((q) => q.operation === "GET")).toHaveLength(puts.length);
      expect(f.memory.operations.filter((q) => q.operation === "PUT")).toEqual([]);
    } finally {
      await cold.close(context);
    }
  } finally {
    plans.restore();
    await f.finish();
  }
});

it("rejects replacement of an immutable offloaded entry before new PUTs or write BEGIN", async () => {
  const f = await historyFixture("payload_immutable", { objects: memoryObjects });

  try {
    await f.storage.commit([dataEntry(20, "original".repeat(40000))], context);
    const before = await f.snapshot();

    f.trace.length = 0;
    f.memory.operations.length = 0;
    await expect(
      f.storage.commit(
        [dataEntry(21, "new".repeat(100000)), dataEntry(20, "replacement".repeat(30000))],
        context,
      ),
    ).rejects.toThrow(/already|immutable/);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(mutations(f.trace)).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    await f.finish();
  }
});
