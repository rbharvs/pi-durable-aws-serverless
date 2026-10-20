import { expect, it } from "vitest";
import { check, harnessId, requestId } from "../../src/domain.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { BEGIN_READ } from "../../src/sql/db.ts";
import { acceptedInput } from "../shared/input.ts";
import { context, conversation, entryId, historyFixture } from "./history-support.ts";
import { namespace } from "./postgres.ts";
import { submission, task } from "./task-support.ts";

const input = acceptedInput({ content: "discovery", whenBusy: "followUp" });
const done = {
  type: "submission" as const,
  value: {
    ...submission().value,
    status: "done" as const,
    entry: entryId(40),
    answer: entryId(41),
  },
};

it("uses strict database-clock expiry at minus one, equality and plus one without changing ownership", async () => {
  const f = await namespace("discovery_clock");
  const db = f.authority.inspectionDb;
  try {
    await f.authority.receive(f.ns, requestId("one"), input);
    const claim = await f.authority.claim(f.ns, 45000);
    check(claim.kind === "claimed", "initial owner");
    // now() is fixed for this real PG transaction, including the actual eligible/claim SQL.
    await db.query("BEGIN");
    try {
      for (const offset of [1, 0, -1]) {
        await db.query(`UPDATE harness SET lease_until=${DB_NOW_MS}+$2::bigint WHERE ns=$1`, [
          f.ns,
          offset,
        ]);
        const before = await db.query("SELECT * FROM harness WHERE ns=$1", [f.ns]);
        expect(await f.authority.hasWork(f.ns)).toBe(true);
        expect(await f.authority.eligible()).toEqual(offset < 0 ? [f.ns] : []);
        expect(await db.query("SELECT * FROM harness WHERE ns=$1", [f.ns])).toEqual(before);
        if (offset >= 0) expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
      }
      const takeover = await f.authority.claim(f.ns, 45000);
      check(takeover.kind === "claimed", "strictly expired owner can be replaced");
      expect(takeover.lease.epoch).toBe(claim.lease.epoch + 1);
      expect(takeover.lease.owner).not.toBe(claim.lease.owner);
      expect(await f.authority.eligible()).toEqual([]);
      expect(await f.authority.release(claim.lease)).toBe("lost");
      expect(await f.authority.release(takeover.lease)).toBe("released");
      // Release is exactly now(), not immediately eligible under strict '<'.
      expect(await f.authority.eligible()).toEqual([]);
      await db.query(`UPDATE harness SET lease_until=${DB_NOW_MS}-1 WHERE ns=$1`, [f.ns]);
      expect(await f.authority.eligible()).toEqual([f.ns]);
    } finally {
      await db.query("ROLLBACK");
    }
  } finally {
    await f.finish();
  }
});

it("returns exactly five by lease expiry then namespace, excluding owned and idle namespaces", async () => {
  const f = await namespace("discovery_order");
  const ns = (name: string) => harnessId(`${f.ns}_${name}`);
  // Creation order is neither lease nor namespace order; b/a tie at -700.
  const offsets: Record<string, number> = {
    late: -100,
    owned: 60000,
    b: -700,
    idle: -3000,
    w: -500,
    z: -900,
    a: -700,
    u: -300,
    y: -800,
    v: -400,
    x: -600,
  };
  try {
    for (const name of Object.keys(offsets))
      await f.authority.receive(ns(name), requestId("one"), input);
    expect((await f.authority.claim(ns("owned"), 45000)).kind).toBe("claimed");
    const db = f.authority.inspectionDb;
    await db.query("DELETE FROM inbox WHERE ns=$1", [ns("idle")]);
    await db.query("BEGIN");
    try {
      for (const [name, offset] of Object.entries(offsets))
        await db.query(`UPDATE harness SET lease_until=${DB_NOW_MS}+$2::bigint WHERE ns=$1`, [
          ns(name),
          offset,
        ]);
      expect(await f.authority.eligible()).toEqual(["z", "y", "a", "b", "x"].map(ns));
    } finally {
      await db.query("ROLLBACK");
    }
  } finally {
    await f.finish();
  }
});

it("breaks a full expiry tie by namespace across the five-candidate cutoff", async () => {
  const f = await namespace("discovery_tie");
  const names = ["a", "b", "c", "d", "e", "f", "g", "h", "i"].map((suffix) =>
    harnessId(`${f.ns}_${suffix}`),
  );
  try {
    // All nine expiries tie, and physical creation order opposes the expected namespace order.
    for (const ns of names.toReversed()) {
      await f.authority.receive(ns, requestId("one"), input);
      await f.authority.inspectionDb.query("UPDATE harness SET lease_until=7 WHERE ns=$1", [ns]);
    }
    expect(await f.authority.eligible()).toEqual(names.slice(0, 5));
  } finally {
    await f.finish();
  }
});

it("receipt page uses received_at before request_id with exact eight-item cutoff and excludes mapped rows", async () => {
  const f = await historyFixture("discovery_receipts");
  try {
    await f.storage.commit([done], context);
    const fixture = await f.authority.get(f.ns, requestId("fixture"));
    check(fixture?.kind === "received", "fixture receipt");
    await f.authority.markAdmitted(fixture, done.value.id);
    // Equal-time groups straddle the cutoff; 'a' is lexically first but received last.
    for (const [name, time] of [
      ["a", 300],
      ["j", 200],
      ["h", 200],
      ["f", 200],
      ["d", 200],
      ["i", 200],
      ["g", 200],
      ["e", 200],
      ["c", 200],
      ["z", 100],
    ] as const) {
      await f.authority.receive(f.ns, requestId(name), input);
      await f.control.query("UPDATE inbox SET received_at=$3 WHERE ns=$1 AND request_id=$2", [
        f.ns,
        name,
        time,
      ]);
    }
    const page = await f.authority.received(f.ns);
    const expected = [
      ["z", 100],
      ["c", 200],
      ["d", 200],
      ["e", 200],
      ["f", 200],
      ["g", 200],
      ["h", 200],
      ["i", 200],
    ];
    expect(page.map((r) => [r.request, r.receivedAt])).toEqual(expected);
    // The unmapped index includes request_id; also exercise the SQL ordering without its help.
    const plannerSettings =
      "SELECT current_setting('enable_indexscan') AS indexscan, current_setting('enable_indexonlyscan') AS indexonlyscan, current_setting('enable_bitmapscan') AS bitmapscan";
    const before = await f.control.query(plannerSettings);
    await f.control.query(BEGIN_READ);
    try {
      await f.control.query("SET LOCAL enable_indexscan=off");
      await f.control.query("SET LOCAL enable_indexonlyscan=off");
      await f.control.query("SET LOCAL enable_bitmapscan=off");
      // Authority and control share this connection and its read snapshot.
      expect((await f.authority.received(f.ns)).map((r) => [r.request, r.receivedAt])).toEqual(
        expected,
      );
    } finally {
      await f.control.query("ROLLBACK");
    }
    expect(await f.control.query(plannerSettings)).toEqual(before);
    for (const receipt of page) await f.authority.markAdmitted(receipt, done.value.id);
    expect((await f.authority.received(f.ns)).map((r) => r.request)).toEqual(["j", "a"]);
  } finally {
    await f.finish();
  }
});

it("a rejected live-task batch cannot create eligible work or move publication counters", async () => {
  const f = await historyFixture("discovery_rejected");
  try {
    await f.storage.commit([done], context);
    const received = await f.authority.get(f.ns, requestId("fixture"));
    check(received?.kind === "received", "fixture receipt");
    await f.authority.markAdmitted(received, done.value.id);
    await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
    const before = await f.snapshot();
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    expect(await f.authority.eligible()).toEqual([]);
    await expect(
      f.storage.commit([task(), conversation(), conversation()], context),
    ).rejects.toThrow();
    expect(await f.snapshot()).toEqual(before);
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    expect(await f.authority.eligible()).toEqual([]);
  } finally {
    await f.finish();
  }
});
