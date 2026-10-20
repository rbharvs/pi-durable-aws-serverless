import { expect, it } from "vitest";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import { errorCode } from "../../src/domain.ts";
import { confirmedAbort, statements } from "./postgres.ts";
import {
  context,
  conversation,
  conversationId,
  entry,
  entryId,
  historyFixture,
} from "./history-support.ts";

it("acknowledged allocations survive mint-only reopen, concurrent same-lease handles, and abrupt connection loss", async () => {
  const f = await historyFixture("mint_persistence");

  try {
    expect(await f.storage.mintId()).toBe(2);
    await f.storage.close(context);
    const a = await f.open();
    const b = await f.open();
    const allocated = await Promise.all(
      Array.from({ length: 12 }, (_, i) => (i % 2 ? a : b).storage.mintId()),
    );

    expect([...allocated].sort((x, y) => x - y)).toEqual(
      Array.from({ length: 12 }, (_, i) => i + 3),
    );
    expect(new Set(allocated).size).toBe(12);
    // The client vanishes without a storage close or a business commit.
    await a.db.end();
    const c = await f.open();

    expect(await c.storage.mintId()).toBe(15);
    expect((await f.head()).next_seq).toBe("1");
    expect(await f.control.query("SELECT * FROM records WHERE ns=$1", [f.ns])).toEqual([]);
  } finally {
    await f.finish();
  }
});

it("rejected batches cannot refund minted IDs or advance high-water; committed explicit IDs advance only once", async () => {
  const f = await historyFixture("mint_highwater");

  try {
    const minted = await f.storage.mintId();

    expect(minted).toBe(2);
    const before = await f.snapshot();

    await expect(
      f.storage.commit([conversation(1000), conversation(1000)], context),
    ).rejects.toThrow();
    expect(await f.snapshot()).toEqual(before);
    await f.storage.close(context);
    const { storage } = await f.open();

    expect(await storage.mintId()).toBe(3);
    expect(await storage.conversation(conversationId(1000), context)).toBeUndefined();
    expect(await storage.commit([conversation(), entry(100)], context)).toBe(1);
    expect(await storage.mintId()).toBe(101);
    // Explicit lower IDs and an empty business commit must not regress allocation.
    expect(await storage.commit([entry(20)], context)).toBe(2);
    expect(await storage.commit([], context)).toBe(3);
    expect(await storage.mintId()).toBe(102);
    expect((await f.head()).next_id).toBe("103");
    expect((await f.head()).next_seq).toBe("4");
  } finally {
    await f.finish();
  }
});

it("high-water counts stored identities only, not references or numeric JSON payloads", async () => {
  const f = await historyFixture("identity_highwater");

  try {
    await f.storage.commit(
      [
        conversation(),
        entry(20, { data: { arbitraryNumber: Number.MAX_SAFE_INTEGER }, head: entryId(9000) }),
        conversation(30, { parent: { conversationId: conversationId(1), at: entryId(8000) } }),
      ],
      context,
    );
    expect(await f.storage.mintId()).toBe(31);
    expect((await f.head()).next_id).toBe("32");
  } finally {
    await f.finish();
  }
});

it("last safe ID is allocated exactly once, exhaustion persists across reopen, and reads/empty commits still work", async () => {
  const f = await historyFixture("mint_exhaustion");

  try {
    await f.storage.commit([conversation(Number.MAX_SAFE_INTEGER - 1)], context);
    expect(await f.storage.mintId()).toBe(Number.MAX_SAFE_INTEGER);
    const before = await f.snapshot();

    for (let i = 0; i < 2; i++)
      await expect(f.storage.mintId()).rejects.toThrow("ID space is exhausted");
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    expect(
      await f.storage.conversation(conversationId(Number.MAX_SAFE_INTEGER - 1), context),
    ).toEqual(conversation(Number.MAX_SAFE_INTEGER - 1).value);
    await f.storage.close(context);
    const reopened = await f.open();

    await expect(reopened.storage.mintId()).rejects.toThrow("ID space is exhausted");
    expect(await reopened.storage.commit([], context)).toBe(2);
    expect((await f.head()).next_id).toBe("9007199254740992");
    expect((await f.head()).next_seq).toBe("3");
  } finally {
    await f.finish();
  }
});

it("an explicit maximum-safe identity exhausts the allocator without losing the stored record", async () => {
  const f = await historyFixture("explicit_last_id");

  try {
    expect(await f.storage.commit([conversation(Number.MAX_SAFE_INTEGER)], context)).toBe(1);
    await expect(f.storage.mintId()).rejects.toThrow("ID space is exhausted");
    expect(await f.storage.conversation(conversationId(Number.MAX_SAFE_INTEGER), context)).toEqual(
      conversation(Number.MAX_SAFE_INTEGER).value,
    );
    expect((await f.head()).next_id).toBe("9007199254740992");
  } finally {
    await f.finish();
  }
});

it("confirmed allocation abort retries are bounded and preserve the final original error without poisoning the handle", async () => {
  const causes: Error[] = [];
  let armed = true;
  const f = await historyFixture("mint_retry_bound", {
    hook: async (phase, sql) => {
      if (!armed || phase !== "before" || !sql.startsWith("UPDATE harness SET next_id=")) return;
      const error = confirmedAbort();

      causes.push(error);
      throw error;
    },
  });

  try {
    const before = await f.snapshot();
    const result = await f.storage.mintId().catch((error: unknown) => error);

    expect(causes).toHaveLength(3);
    expect(result).toBe(causes[2]);
    expect(statements(f.trace, "UPDATE harness SET next_id=")).toHaveLength(3);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    armed = false;
    expect(await f.storage.mintId()).toBe(2);
  } finally {
    await f.finish();
  }
});

it("an actual server serialization abort during allocation retries and acknowledges only the next durable ID", async () => {
  const f = await historyFixture("mint_server_abort");

  try {
    const { db, storage } = await f.open();
    let first = true;
    let attempts = 0;
    const errors: unknown[] = [];
    // Ordinary PG autocommit is READ COMMITTED. Give the allocation statement one
    // explicit RR envelope so a real concurrent row update deterministically aborts it.
    const query = db.query.bind(db);

    db.query = async (sql, params = []) => {
      if (!sql.startsWith("UPDATE harness SET next_id=")) return query(sql, params);
      attempts++;
      try {
        await query(BEGIN_WRITE);
        await query("SELECT next_id FROM harness WHERE ns=$1", [f.ns]);
        if (first) {
          first = false;
          await f.control.query("UPDATE harness SET next_id=next_id WHERE ns=$1", [f.ns]);
        }
        const result = await query(sql, params);

        await query("COMMIT");
        return result;
      } catch (error) {
        errors.push(error);
        await query("ROLLBACK");
        throw error;
      }
    };
    expect(await storage.mintId()).toBe(2);
    expect(attempts).toBe(2);
    expect(errors.map(errorCode)).toEqual(["40001"]);
    expect((await f.head()).next_id).toBe("3");
    expect((await f.head()).next_seq).toBe("1");
  } finally {
    await f.finish();
  }
});

for (const representation of ["string", "number"] as const)
  it(`cold-reopens the final safe commit with an exhausted ${representation} sequence head`, async () => {
    const f = await historyFixture("sequence_exhaustion");
    try {
      await f.storage.close(context);
      await f.control.query("UPDATE harness SET next_seq=$2 WHERE ns=$1", [
        f.ns,
        Number.MAX_SAFE_INTEGER,
      ]);
      const last = await f.open();
      expect(
        await last.storage.commit([conversation(), entry(20, { data: "last safe" })], context),
      ).toBe(Number.MAX_SAFE_INTEGER);
      expect((await f.head()).next_seq).toBe("9007199254740992");
      await last.storage.close(context);

      let openedWithNumber = false;
      const cold = await f.open({
        hook: async (phase, sql, _params, rows) => {
          if (
            representation === "number" &&
            phase === "after" &&
            sql.startsWith("SELECT epoch,owner,")
          ) {
            rows![0]!.next_seq = Number(rows![0]!.next_seq);
            openedWithNumber = true;
          }
        },
      });
      expect(openedWithNumber).toBe(representation === "number");
      expect(await cold.storage.entry(entryId(20), context)).toEqual({
        entry: entry(20, { data: "last safe" }).value,
        commitSeq: Number.MAX_SAFE_INTEGER,
      });
      expect(await cold.storage.conversation(conversationId(1), context)).toEqual(
        conversation().value,
      );
      const before = await f.snapshot();
      cold.trace.length = 0;
      await expect(cold.storage.commit([entry(21)], context)).rejects.toThrow("sequence");
      await expect(cold.storage.commit([], context)).rejects.toThrow("sequence");
      expect(cold.trace).toEqual([]);
      expect(await f.snapshot()).toEqual(before);
      expect(cold.storage.status).toBe("open");
      expect(
        (
          await cold.storage.scanEntries(
            { conversationId: conversationId(1) },
            10,
            undefined,
            context,
          )
        ).items.map((e) => e.id),
      ).toEqual([20]);
    } finally {
      await f.finish();
    }
  });

it("cold-reopens when both counters are exhausted and reports ID exhaustion without poisoning reads", async () => {
  const f = await historyFixture("both_exhausted");
  try {
    await f.storage.close(context);
    await f.control.query("UPDATE harness SET next_seq=$2 WHERE ns=$1", [
      f.ns,
      Number.MAX_SAFE_INTEGER,
    ]);
    const last = await f.open();
    expect(
      await last.storage.commit([conversation(), entry(Number.MAX_SAFE_INTEGER)], context),
    ).toBe(Number.MAX_SAFE_INTEGER);
    await last.storage.close(context);
    let openedWithNumbers = false;
    const cold = await f.open({
      hook: async (phase, sql, _params, rows) => {
        if (phase === "after" && sql.startsWith("SELECT epoch,owner,")) {
          rows![0]!.next_id = Number(rows![0]!.next_id);
          rows![0]!.next_seq = Number(rows![0]!.next_seq);
          openedWithNumbers = true;
        }
      },
    });
    expect(openedWithNumbers).toBe(true);
    const before = await f.snapshot();
    await expect(cold.storage.mintId()).rejects.toThrow("ID space is exhausted");
    expect(await f.snapshot()).toEqual(before);
    expect(cold.storage.status).toBe("open");
    expect((await cold.storage.entry(entryId(Number.MAX_SAFE_INTEGER), context))?.commitSeq).toBe(
      Number.MAX_SAFE_INTEGER,
    );
  } finally {
    await f.finish();
  }
});

for (const column of ["next_id", "next_seq"] as const)
  for (const invalid of ["9007199254740993", "9007199254740994"])
    it(`rejects nonsentinel unsafe ${column} head ${invalid} when opening`, async () => {
      const f = await historyFixture("invalid_head");
      try {
        await f.storage.close(context);
        await f.control.query(`UPDATE harness SET ${column}=$2 WHERE ns=$1`, [f.ns, invalid]);
        const before = await f.snapshot();
        await expect(f.open()).rejects.toThrow("Unsafe integer");
        expect(await f.snapshot()).toEqual(before);
      } finally {
        await f.finish();
      }
    });
