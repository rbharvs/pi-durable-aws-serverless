import { expect, it } from "vitest";
import { errorCode, harnessId, requestId } from "../../src/domain.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { BEGIN_READ, BEGIN_WRITE, Database } from "../../src/sql/db.ts";
import { acceptedInput } from "../shared/input.ts";
import { documentCreate } from "./document-support.ts";
import { InspectableAuthority } from "./authority.ts";
import { OWNED_TABLES, confirmedAbort, provider, statements, type Query } from "./postgres.ts";
import {
  context,
  conversation,
  conversationId,
  dataEntry,
  deferred,
  entry,
  entryId,
  historyFixture,
  memoryObjects,
  queryGate,
  type HistoryFixture,
} from "./history-support.ts";

const fence = (sql: string) => sql.startsWith("UPDATE harness SET next_seq");
const allocation = (sql: string) => sql.startsWith("UPDATE harness SET next_id=");
const recordInsert = (sql: string) => sql.startsWith("INSERT INTO records");

for (const operation of ["mint", "commit"] as const)
  for (const applied of [false, true])
    it(`unknown ${operation} ACK applied=${applied} poisons raw reads and queued writes, never retries or releases`, async () => {
      const cause = Error(`${operation} acknowledgement lost`);
      const reached = deferred();
      const proceed = deferred();
      let writing = false;
      let armed = true;
      const f = await historyFixture(`ack_${operation}`, {
        hook: async (phase, sql) => {
          if (phase === "after" && fence(sql)) writing = true;
          const target = operation === "mint" ? allocation(sql) : writing && sql === "COMMIT";

          if (!armed || !target || phase !== (applied ? "after" : "before")) return;
          armed = false;
          reached.resolve();
          await proceed.promise;
          throw cause;
        },
      });

      try {
        f.trace.length = 0;
        const pending = (
          operation === "mint"
            ? f.storage.mintId()
            : f.storage.commit([conversation(), entry(20)], context)
        ).catch((error: unknown) => error);

        await reached.promise;
        // Queue while the uncertain operation still owns the gate; attach handlers before releasing it.
        const queued = Promise.allSettled([
          f.storage.entry(entryId(20), context),
          f.storage.mintId(),
          f.storage.commit([entry(30)], context),
        ]);

        proceed.resolve();
        expect(await pending).toBe(cause);
        expect(
          (await queued).every((r) => r.status === "rejected" && /failed/.test(String(r.reason))),
        ).toBe(true);
        expect(f.storage.status).toBe("failed");
        const afterFailure = f.trace.length;

        await expect(f.storage.conversation(conversationId(1), context)).rejects.toThrow("failed");
        await expect(f.storage.mintId()).rejects.toThrow("failed");
        expect(f.trace).toHaveLength(afterFailure);
        const failedIndex = f.trace.findIndex((q) => q.error === cause);

        expect(failedIndex).toBeGreaterThanOrEqual(0);
        expect(f.trace.slice(failedIndex + 1)).toEqual([]);
        expect(
          f.trace.filter((q) => (operation === "mint" ? allocation(q.sql) : q.sql === BEGIN_WRITE)),
        ).toHaveLength(1);
        const head = await f.head();

        expect(head.owner).toBe(f.lease.owner);
        expect(head.next_id).toBe(String(applied ? (operation === "mint" ? 3 : 21) : 2));
        expect(head.next_seq).toBe(String(applied && operation === "commit" ? 2 : 1));
        await f.storage.close(context);
        expect(f.endCalls()).toBe(1);
        expect((await f.head()).owner).toBe(f.lease.owner);
        const reopened = await f.open();

        expect(await reopened.storage.entry(entryId(20), context)).toEqual(
          operation === "commit" && applied ? { entry: entry(20).value, commitSeq: 1 } : undefined,
        );
        expect(await reopened.storage.mintId()).toBe(applied ? (operation === "mint" ? 3 : 21) : 2);
      } finally {
        proceed.resolve();
        await f.finish();
      }
    });

it("a reader queued after an uncommitted INSERT sees rollback, and a confirmed rollback keeps the original cause", async () => {
  const inserted = queryGate("after", recordInsert);
  const original = Error("statement response failed after INSERT");
  let armed = false;
  const f = await historyFixture("rollback_reader", {
    hook: async (...args) => {
      await inserted.hook(...args);
      if (armed && args[0] === "after" && recordInsert(args[1])) {
        armed = false;
        throw original;
      }
    },
  });

  try {
    await f.storage.commit([conversation()], context);
    const before = await f.snapshot();

    inserted.arm();
    armed = true;
    const writing = f.storage.commit([entry(20)], context).catch((error: unknown) => error);

    await inserted.reached;
    const traceAtGate = f.trace.length;
    const reading = f.storage.entry(entryId(20), context);

    // A separate connection also cannot see the half-published batch.
    expect(await f.control.query("SELECT id FROM records WHERE ns=$1 AND id=20", [f.ns])).toEqual(
      [],
    );
    expect(f.trace).toHaveLength(traceAtGate);
    inserted.release();
    expect(await writing).toBe(original);
    expect(await reading).toBeUndefined();
    expect(f.storage.status).toBe("open");
    expect(await f.snapshot()).toEqual(before);
    expect(await f.storage.commit([entry(20)], context)).toBe(2);
  } finally {
    inserted.release();
    await f.finish();
  }
});

for (const applied of [false, true])
  it(`unknown ROLLBACK ACK applied=${applied} retains both errors and fails queued operations`, async () => {
    const original = Error("original statement failure");
    const rollback = Error("rollback acknowledgement lost");
    const reached = deferred();
    const proceed = deferred();
    let armed = false;
    const f = await historyFixture("rollback_unknown", {
      hook: async (phase, sql) => {
        if (!armed) return;
        if (phase === "after" && recordInsert(sql)) throw original;
        if (sql === "ROLLBACK" && phase === (applied ? "after" : "before")) {
          armed = false;
          reached.resolve();
          await proceed.promise;
          throw rollback;
        }
      },
    });

    try {
      await f.storage.commit([conversation()], context);
      const before = await f.snapshot();

      armed = true;
      const pending = f.storage.commit([entry(20)], context).catch((error: unknown) => error);

      await reached.promise;
      const queued = Promise.allSettled([
        f.storage.entry(entryId(20), context),
        f.storage.mintId(),
      ]);

      proceed.resolve();
      const failed = await pending;

      expect(failed).toBeInstanceOf(AggregateError);
      expect((failed as AggregateError).cause).toBe(original);
      expect((failed as AggregateError).errors).toEqual([original, rollback]);
      expect((await queued).every((r) => r.status === "rejected")).toBe(true);
      expect(f.storage.status).toBe("failed");
      expect((await f.head()).owner).toBe(f.lease.owner);
      await f.storage.close(context);
      expect(await f.snapshot()).toEqual(before);
    } finally {
      proceed.resolve();
      await f.finish();
    }
  });

it("close is immediate for new/queued work, drains the active operation, ends once, and does not release ownership", async () => {
  const gate = queryGate("after", recordInsert);
  const f = await historyFixture("close_drain", { hook: gate.hook });

  try {
    gate.arm();
    const active = f.storage.commit([conversation()], context);

    await gate.reached;
    const queued = Promise.allSettled([
      f.storage.conversation(conversationId(1), context),
      f.storage.commit([entry(20)], context),
      f.storage.mintId(),
    ]);
    const closing = f.storage.close(context);
    const closingAgain = f.storage.close(context);

    expect(closingAgain).toBe(closing);
    expect(f.storage.status).toBe("closed");
    expect(f.endCalls()).toBe(0);
    await expect(f.storage.commit([entry(20)], context)).rejects.toThrow("closed");
    gate.release();
    expect(await active).toBe(1);
    expect(
      (await queued).every((r) => r.status === "rejected" && /closed/.test(String(r.reason))),
    ).toBe(true);
    await closing;
    await f.storage.close(context);
    expect(f.endCalls()).toBe(1);
    expect((await f.head()).owner).toBe(f.lease.owner);
    const { storage } = await f.open();

    expect(await storage.conversation(conversationId(1), context)).toEqual(conversation().value);
    expect(await storage.entry(entryId(20), context)).toBeUndefined();
    expect(await storage.mintId()).toBe(2);
    expect(await storage.commit([], context)).toBe(2);
  } finally {
    gate.release();
    await f.finish();
  }
});

for (const operation of ["mint", "commit"] as const)
  it(`same-lease stale sequence rejects ${operation} without changing history or counters`, async () => {
    const f = await historyFixture("stale_sequence");

    try {
      const stale = await f.open();
      const seq = await f.storage.commit([conversation(), entry(20)], context);
      const minted = await f.storage.mintId();
      const before = await f.snapshot();

      await expect(
        operation === "mint"
          ? stale.storage.mintId()
          : stale.storage.commit([entry(1000)], context),
      ).rejects.toThrow("Stale");
      expect(await f.snapshot()).toEqual(before);
      expect(await f.storage.entry(entryId(1000), context)).toBeUndefined();
      expect(await f.storage.mintId()).toBe(minted + 1);
      expect(await f.storage.commit([], context)).toBe(seq + 1);
    } finally {
      await f.finish();
    }
  });

it("exactly one competing writer publishes, including its payload, head and high-water update", async () => {
  const f = await historyFixture("competing_writers");

  try {
    await f.storage.commit([conversation()], context);
    const other = await f.open();
    const result = await Promise.allSettled([
      f.storage.commit([entry(20, { data: "one" })], context),
      other.storage.commit([entry(40, { data: "two" })], context),
    ]);
    const winningIndex = result.findIndex((r) => r.status === "fulfilled");

    expect(result.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(result.filter((r) => r.status === "rejected")).toHaveLength(1);
    const reader = await f.open();
    const rows = await reader.storage.scanEntries(
      { conversationId: conversationId(1) },
      10,
      undefined,
      context,
    );

    expect(rows.items).toEqual([
      entry(winningIndex === 0 ? 20 : 40, { data: winningIndex === 0 ? "one" : "two" }).value,
    ]);
    expect((await f.head()).next_seq).toBe("3");
    expect(await reader.storage.mintId()).toBe(winningIndex === 0 ? 21 : 41);
    const payloads = await f.control.query(
      "SELECT count(*) AS n FROM inline_payloads WHERE ns=$1",
      [f.ns],
    );

    expect(payloads[0]!.n).toBe("2");
  } finally {
    await f.finish();
  }
});

for (const cut of ["before-write", "snapshot-in-flight"] as const)
  it(`takeover ${cut} fences old mint/commit/open and stale release cannot clear the new owner`, async () => {
    let armed = false;
    let f: HistoryFixture;
    let takeover: Awaited<ReturnType<HistoryFixture["authority"]["claim"]>> | undefined;
    f = await historyFixture("lease_takeover", {
      hook: async (phase, sql) => {
        if (
          !armed ||
          phase !== "before" ||
          (cut === "before-write" ? sql !== BEGIN_WRITE : !fence(sql))
        )
          return;
        armed = false;
        if (cut === "snapshot-in-flight")
          await f.db.query("SELECT epoch FROM harness WHERE ns=$1", [f.ns]);
        await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
        takeover = await f.authority.claim(f.ns, 180000);
      },
    });

    try {
      await f.storage.commit([conversation()], context);
      const staleAllocator = await f.open();
      const minted = await f.storage.mintId();

      armed = true;
      await expect(f.storage.commit([entry(1000)], context)).rejects.toThrow("Stale");
      expect(takeover?.kind).toBe("claimed");
      if (takeover?.kind !== "claimed") throw Error("Takeover did not claim");
      expect(takeover.lease.epoch).toBe(f.lease.epoch + 1);
      const before = await f.snapshot();

      await expect(staleAllocator.storage.mintId()).rejects.toThrow("Stale");
      await expect(f.open()).rejects.toThrow("Stale");
      expect(await f.authority.release(f.lease)).toBe("lost");
      expect(await f.snapshot()).toEqual(before);
      const current = await f.open({ lease: takeover.lease });

      expect(await current.storage.entry(entryId(1000), context)).toBeUndefined();
      expect(await current.storage.mintId()).toBe(minted + 1);
      expect((await f.head()).owner).toBe(takeover.lease.owner);
    } finally {
      await f.finish();
    }
  });

it("expiration alone does not fence an unchanged owner: open, mint and commit still work until a successful takeover", async () => {
  const f = await historyFixture("expiration_not_fence");

  try {
    await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
    const reopened = await f.open();

    expect(await reopened.storage.mintId()).toBe(2);
    expect(await reopened.storage.commit([conversation(), entry(20)], context)).toBe(1);
    expect((await f.head()).owner).toBe(f.lease.owner);
    const next = await f.authority.claim(f.ns, 180000);

    expect(next.kind).toBe("claimed");
    const before = await f.snapshot();

    await expect(reopened.storage.commit([entry(30)], context)).rejects.toThrow("Stale");
    expect(await f.snapshot()).toEqual(before);
  } finally {
    await f.finish();
  }
});

it("held claims cannot renew or replace a live owner; release is fenced and the next claim uses database time", async () => {
  const f = await historyFixture("claim_release");

  try {
    const before = await f.head();
    const rivals: InspectableAuthority[] = [];

    try {
      for (let i = 0; i < 5; i++)
        rivals.push(new InspectableAuthority(await Database.connect(provider), provider));
      const claims = await Promise.all(rivals.map((rival) => rival.claim(f.ns, 25000)));

      expect(claims).toEqual(Array.from({ length: 5 }, () => ({ kind: "held" })));
      expect(await f.head()).toEqual(before);
    } finally {
      await Promise.all(rivals.map((rival) => rival.close()));
    }
    await f.storage.close(context);
    expect(await f.authority.release(f.lease)).toBe("released");
    expect(await f.authority.release(f.lease)).toBe("lost");
    const released = await f.head();

    expect(released.owner).toBeNull();
    expect(released.epoch).toBe(String(f.lease.epoch));
    expect(released.next_id).toBe(before.next_id);
    expect(released.next_seq).toBe(before.next_seq);
    // Eligibility is strict '<', not '<=': make the already-released row older by one
    // server millisecond without a sleep or an assumption about workstation time.
    await f.control.query("UPDATE harness SET lease_until=lease_until-1 WHERE ns=$1", [f.ns]);
    const lower = Number((await f.control.query(`SELECT ${DB_NOW_MS} AS at`))[0]!.at);
    const next = await f.authority.claim(f.ns, 25000);
    const upper = Number((await f.control.query(`SELECT ${DB_NOW_MS} AS at`))[0]!.at);

    expect(next.kind).toBe("claimed");
    if (next.kind !== "claimed") throw Error("Released namespace was not claimable");
    expect(next.lease.owner).not.toBe(f.lease.owner);
    expect(next.lease.epoch).toBe(f.lease.epoch + 1);
    // The documented 2-second close margin is in addition to the 25-second budget.
    expect(next.lease.until).toBeGreaterThanOrEqual(lower + 27000);
    expect(next.lease.until).toBeLessThanOrEqual(upper + 27000);
    expect(Object.isFrozen(next.lease)).toBe(true);
    expect(await f.authority.release(f.lease)).toBe("lost");
    expect((await f.head()).owner).toBe(next.lease.owner);
    expect(await f.authority.release(next.lease)).toBe("released");
  } finally {
    await f.finish();
  }
});

/** Compare actual SQL bind values, excluding protocol framing and acknowledgements. */
const mutations = (trace: readonly Query[]) =>
  trace
    .filter((q) => /^(INSERT|UPDATE|DELETE) /.test(q.sql))
    .map(({ sql, params }) => ({ sql, params }));

it("a real PostgreSQL serialization abort retries the exact frozen publication, not caller-owned mutated writes", async () => {
  let armed = false;
  let f: HistoryFixture;
  const write = entry(20, { data: { value: "original" } });
  f = await historyFixture("real_serialization", {
    hook: async (phase, sql) => {
      if (!armed || phase !== "after" || !recordInsert(sql)) return;
      armed = false;
      // The first attempt has already inserted its payload and record. Force a server-side
      // concurrent-update abort on a row in the established REPEATABLE READ snapshot.
      await f.db.query("SELECT * FROM inbox WHERE ns=$1 AND request_id='fixture'", [f.ns]);
      await f.control.query(
        "UPDATE inbox SET received_at=received_at WHERE ns=$1 AND request_id='fixture'",
        [f.ns],
      );
      write.value.data = { value: "caller mutation during first attempt" };
      await f.db.query(
        "UPDATE inbox SET received_at=received_at WHERE ns=$1 AND request_id='fixture'",
        [f.ns],
      );
    },
  });

  try {
    await f.storage.commit([conversation()], context);
    f.trace.length = 0;
    armed = true;
    expect(await f.storage.commit([write], context)).toBe(2);
    expect(
      f.trace.filter((q) => q.sql.startsWith("UPDATE inbox") && errorCode(q.error) === "40001"),
    ).toHaveLength(1);
    const begins = f.trace.flatMap((q, i) => (q.sql === BEGIN_WRITE ? [i] : []));

    expect(begins).toHaveLength(2);
    const first = f.trace.slice(begins[0], begins[1]);
    const second = f.trace.slice(begins[1]);
    // The fixture's deliberate inbox UPDATE is the only non-publication mutation.
    expect(mutations(first).filter((q) => !q.sql.startsWith("UPDATE inbox"))).toEqual(
      mutations(second),
    );
    expect(statements(first, "ROLLBACK")).toHaveLength(1);
    expect(statements(second, "COMMIT").filter((q) => q.acknowledged)).toHaveLength(1);
    expect(f.trace.filter((q) => q.sql === BEGIN_READ)).toHaveLength(1);
    expect((await f.storage.entry(entryId(20), context))?.entry.data).toEqual({
      value: "original",
    });
    expect((await f.head()).next_seq).toBe("3");
    expect(
      (await f.control.query("SELECT count(*) AS n FROM records WHERE ns=$1 AND id=20", [f.ns]))[0]!
        .n,
    ).toBe("1");
  } finally {
    await f.finish();
  }
});

it("persistent confirmed publication aborts stop at three, preserve state, and leave the raw handle usable", async () => {
  const causes: Error[] = [];
  let armed = false;
  const f = await historyFixture("write_retry_bound", {
    hook: async (phase, sql) => {
      if (!armed || phase !== "before" || !fence(sql)) return;
      const cause = confirmedAbort();

      causes.push(cause);
      throw cause;
    },
  });

  try {
    const before = await f.snapshot();

    armed = true;
    expect(
      await f.storage.commit([conversation(), entry(20)], context).catch((error: unknown) => error),
    ).toBe(causes[2]);
    expect(causes).toHaveLength(3);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    armed = false;
    expect(await f.storage.commit([conversation(), entry(20)], context)).toBe(1);
  } finally {
    await f.finish();
  }
});

it("fixture teardown closes open and cold handles before namespace deletion and preserves a prefix-neighbor namespace", async () => {
  const f = await historyFixture("fixture_owned_handles");
  const neighbor = harnessId(`${f.ns}_neighbor`);
  const observer = await Database.connect(provider);
  let ownerAtCleanup: unknown;

  try {
    await f.authority.receive(
      neighbor,
      requestId("neighbor"),
      acceptedInput({ content: "neighbor", whenBusy: "followUp" }),
    );
    const cold = await f.cold();
    const other = await f.open();
    const query = f.control.query.bind(f.control);

    f.control.query = async (sql, params) => {
      if (sql === "DELETE FROM inbox WHERE ns=$1") {
        expect(f.endCalls()).toBe(1);
        expect(other.endCalls()).toBe(1);
        expect(cold.status).toBe("closed");
        ownerAtCleanup = (await query("SELECT owner FROM harness WHERE ns=$1", [f.ns]))[0]!.owner;
      }
      return query(sql, params);
    };
    const finishing = f.finish();

    expect(f.finish()).toBe(finishing);
    await finishing;
    expect(ownerAtCleanup).toBe(f.lease.owner);
    await expect(cold.mintId()).rejects.toThrow("closed");
    await expect(other.db.query("SELECT 1")).rejects.toThrow();
    expect(await observer.query("SELECT ns FROM harness WHERE ns=$1", [f.ns])).toEqual([]);
    expect(await observer.query("SELECT ns FROM harness WHERE ns=$1", [neighbor])).toEqual([
      { ns: neighbor },
    ]);
    expect(await observer.query("SELECT request_id FROM inbox WHERE ns=$1", [neighbor])).toEqual([
      { request_id: "neighbor" },
    ]);
  } finally {
    try {
      await f.finish();
    } finally {
      try {
        await observer.query("DELETE FROM inbox WHERE ns=$1", [neighbor]);
        await observer.query("DELETE FROM harness WHERE ns=$1", [neighbor]);
      } finally {
        await observer.end();
      }
    }
  }
});

it("fixture teardown joins close failures and both SQL leak checks, and deletes its exact namespace without release", async () => {
  const f = await historyFixture("fixture_cleanup_failures", { objects: memoryObjects });
  const failures = [Error("first close failed"), Error("second close failed")];
  let ownerAtCleanup: unknown;

  try {
    await f.storage.commit(
      [conversation(), dataEntry(20, "x".repeat(300 * 1024)), documentCreate(30)],
      context,
    );
    expect(
      await f.control.query("SELECT document_id FROM revisions WHERE ns=$1", [f.ns]),
    ).toHaveLength(1);
    const second = await f.open();
    const third = await f.open();

    for (const [i, handle] of [f, second].entries()) {
      const end = handle.db.end.bind(handle.db);

      handle.db.end = async () => {
        await end();
        throw failures[i];
      };
    }
    // Deliberately detach both kinds of SQL payload, so teardown must report both leaks.
    await f.control.query("DELETE FROM records WHERE ns=$1", [f.ns]);
    const query = f.control.query.bind(f.control);

    f.control.query = async (sql, params) => {
      if (sql === "DELETE FROM inbox WHERE ns=$1")
        ownerAtCleanup = (await query("SELECT owner FROM harness WHERE ns=$1", [f.ns]))[0]!.owner;
      return query(sql, params);
    };
    const error = await f.finish().catch((error: unknown) => error);

    expect(error).toBeInstanceOf(AggregateError);
    expect((error as AggregateError).errors).toEqual([
      ...failures,
      expect.objectContaining({
        message: expect.stringContaining("Unreferenced history payload rows in inline_payloads"),
      }),
      expect.objectContaining({
        message: expect.stringContaining("Unreferenced history payload rows in s3_payloads"),
      }),
    ]);
    expect(ownerAtCleanup).toBe(f.lease.owner);
    expect(f.endCalls()).toBe(1);
    expect(second.endCalls()).toBe(1);
    expect(third.endCalls()).toBe(1);
    await expect(third.db.query("SELECT 1")).rejects.toThrow();
    const observer = await Database.connect(provider);

    try {
      for (const table of OWNED_TABLES)
        expect(await observer.query(`SELECT * FROM ${table} WHERE ns=$1`, [f.ns])).toEqual([]);
      // Independent of OWNED_TABLES: an omitted revision table must still fail cleanup.
      expect(await observer.query("SELECT * FROM revisions WHERE ns=$1", [f.ns])).toEqual([]);
    } finally {
      await observer.end();
    }
  } finally {
    // The intentional cleanup rejection is asserted above; repeated finish still joins it.
    await Promise.allSettled([f.finish()]);
  }
});
