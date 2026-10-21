import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { requestId } from "../../src/domain.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { acceptedInput } from "../shared/input.ts";
import { confirmedAbort, faultOnce, statements, traced } from "./postgres.ts";
import { context, entry, historyFixture } from "./history-support.ts";

it("claim uses a strict database-clock boundary, issues a frozen nominal lease, and adds its safety margin", async () => {
  const f = await historyFixture("strict_clock");

  try {
    // PostgreSQL now() is stable in this explicit test transaction. Equality is exact,
    // not a timing race against the local machine's clock or a millisecond timer.
    await f.control.query("BEGIN");
    try {
      await f.control.query(`UPDATE harness SET lease_until=${DB_NOW_MS} WHERE ns=$1`, [f.ns]);
      const before = await f.head();
      expect(await f.authority.claim(f.ns, 1234)).toEqual({ kind: "held" });
      expect(await f.head()).toEqual(before);
      await f.control.query(`UPDATE harness SET lease_until=${DB_NOW_MS}-1 WHERE ns=$1`, [f.ns]);
      const claim = await f.authority.claim(f.ns, 1234);
      expect(claim.kind).toBe("claimed");
      if (claim.kind !== "claimed") throw new Error("Expired namespace must be claimable");
      expect(Object.isFrozen(claim.lease)).toBe(true);
      expect(claim.lease.epoch).toBe(f.lease.epoch + 1);
      expect(claim.lease.owner).not.toBe(f.lease.owner);
      expect(claim.lease.until).toBe(Number(before.lease_until) + 1234 + 2000);
      expect(await f.authority.release(f.lease)).toBe("lost");
      expect(await f.authority.release(claim.lease)).toBe("released");
      expect(await f.authority.claim(f.ns, 1234)).toEqual({ kind: "held" });
      expect((await f.head()).owner).toBeNull();
    } finally {
      await f.control.query("ROLLBACK");
    }
  } finally {
    await f.finish();
  }
});

it("receipt ingress never resets or reallocates an existing leased ownership head", async () => {
  const f = await historyFixture("receive_head");
  try {
    await f.storage.mintId();
    await f.storage.commit([entry(20)], context);
    const before = await f.head();
    await f.authority.receive(
      f.ns,
      requestId("new"),
      acceptedInput({ content: "new", whenBusy: "steer" }),
    );
    expect(await f.head()).toEqual(before);
  } finally {
    await f.finish();
  }
});

for (const changed of ["owner", "epoch"] as const) {
  it(`the ${changed} component fences independently of the unchanged sequence and other ownership component`, async () => {
    const f = await historyFixture(`independent_${changed}`);
    try {
      const allocator = await f.open();
      if (changed === "owner") {
        await f.control.query("UPDATE harness SET owner=$2 WHERE ns=$1", [f.ns, randomUUID()]);
      } else await f.control.query("UPDATE harness SET epoch=epoch+1 WHERE ns=$1", [f.ns]);
      const before = await f.snapshot();
      await expect(f.open()).rejects.toThrow("Stale");
      await expect(f.storage.commit([entry(1000)], context)).rejects.toThrow("Stale");
      await expect(allocator.storage.mintId()).rejects.toThrow("Stale");
      expect(await f.snapshot()).toEqual(before);
      expect(await f.authority.release(f.lease)).toBe("lost");
      expect(await f.snapshot()).toEqual(before);
    } finally {
      await f.finish();
    }
  });
}

for (const applied of [false, true]) {
  it(`unknown claim ACK applied=${applied} is never retried or turned into a lease`, async () => {
    const f = await historyFixture("claim_unknown");
    const failure = new Error("claim acknowledgement lost");
    const { authority, trace } = await traced(
      faultOnce(applied ? "after" : "before", "UPDATE harness SET epoch", () => failure),
    );
    try {
      await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
      const before = await f.head();
      await expect(authority.claim(f.ns, 1234)).rejects.toBe(failure);
      expect(statements(trace, "UPDATE harness SET epoch")).toHaveLength(1);
      const after = await f.head();
      expect(after.epoch).toBe(String(Number(before.epoch) + (applied ? 1 : 0)));
      expect(after.owner === before.owner).toBe(!applied);
      expect(after.next_id).toBe(before.next_id);
      expect(after.next_seq).toBe(before.next_seq);
    } finally {
      await authority.close();
      await f.finish();
    }
  });
}

for (const exhausted of [false, true]) {
  it(`claim retries only confirmed aborts with one fixed owner token and a three-attempt limit (exhausted=${exhausted})`, async () => {
    const f = await historyFixture("claim_abort");
    const failures: Error[] = [];
    const { authority, trace } = await traced(async (phase, sql) => {
      if (phase !== "before" || !sql.startsWith("UPDATE harness SET epoch")) return;
      if (!exhausted && failures.length === 2) return;
      failures.push(confirmedAbort());
      throw failures.at(-1);
    });
    try {
      await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
      const before = await f.head();
      const result = authority.claim(f.ns, 1234);
      if (exhausted) {
        const failed = await result.catch((error: unknown) => error);
        expect(failed).toBe(failures[2]);
        expect(failures).toHaveLength(3);
        expect(await f.head()).toEqual(before);
      } else {
        const claimed = await result;
        expect(claimed.kind).toBe("claimed");
        expect(Number((await f.head()).epoch)).toBe(Number(before.epoch) + 1);
      }
      const attempts = statements(trace, "UPDATE harness SET epoch");
      expect(attempts).toHaveLength(3);
      expect(new Set(attempts.map((q) => q.params[1])).size).toBe(1);
    } finally {
      await authority.close();
      await f.finish();
    }
  });
}
