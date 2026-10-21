import { expect, it } from "vitest";
import type { Received } from "../../src/authority.ts";
import { check, computeSha256, requestId } from "../../src/domain.ts";
import { canonical } from "../../src/input.ts";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import { apiHandler } from "../../src/submit.ts";
import { acceptedInput } from "../shared/input.ts";
import { context, entry, entryId, historyFixture, memoryObjects } from "./history-support.ts";
import { confirmedAbort, traced } from "./postgres.ts";
import { submission, submissionId, task } from "./task-support.ts";

async function fixtureReceipt(f: Awaited<ReturnType<typeof historyFixture>>): Promise<Received> {
  const received = await f.authority.get(f.ns, requestId("fixture"));
  check(received?.kind === "received", "Expected unmapped fixture receipt");
  return received;
}

it("maps only an actual stored submission, preserves immutable receipt fields, and survives takeover and original ingress retry", async () => {
  const f = await historyFixture("receipt_mapping");
  const peer = await traced();
  try {
    const received = await fixtureReceipt(f);
    await expect(f.authority.markAdmitted(received, submissionId(30))).rejects.toThrow(/missing/);
    await f.storage.commit([entry(30)], context);
    await expect(f.authority.markAdmitted(received, submissionId(30))).rejects.toThrow(/missing/);
    await f.storage.commit([submission(31, received.request), submission(32, "other")], context);
    await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
    const takeover = await peer.authority.claim(f.ns, 10000);
    expect(takeover.kind).toBe("claimed");
    await Promise.all([
      f.authority.markAdmitted(received, submissionId(31)),
      peer.authority.markAdmitted(received, submissionId(31)),
    ]);
    const expected = { ...received, kind: "admitted", submission: submissionId(31) };
    expect(await peer.authority.get(f.ns, received.request)).toEqual(expected);
    await f.authority.markAdmitted(received, submissionId(31));
    await expect(f.authority.markAdmitted(received, submissionId(32))).rejects.toThrow(
      "mapping changed",
    );
    await expect(
      f.authority.markAdmitted({ ...received, hash: computeSha256("changed") }, submissionId(31)),
    ).rejects.toThrow("identity changed");
    expect(await f.authority.received(f.ns)).toEqual([]);
    const raw = await f.control.query("SELECT * FROM inbox WHERE ns=$1", [f.ns]);
    const input = { content: "history fixture", whenBusy: "followUp" as const };
    const handler = apiHandler({
      connectAuthority: async () => (await traced()).authority,
      dispatch: { async wake() {} },
      event() {},
    });
    expect(
      await handler({ ns: f.ns, request: "fixture", command: { kind: "inline", input } }),
    ).toEqual({ ok: true, duplicate: true, receivedAt: received.receivedAt });
    expect(await f.authority.receive(f.ns, received.request, acceptedInput(input))).toEqual({
      duplicate: true,
      item: expected,
    });
    expect(
      await handler({
        ns: f.ns,
        request: "fixture",
        command: { kind: "inline", input: { ...input, content: "changed" } },
      }),
    ).toEqual({ ok: false, code: "request-conflict", retryable: false });
    expect(await f.control.query("SELECT * FROM inbox WHERE ns=$1", [f.ns])).toEqual(raw);
  } finally {
    await Promise.all([peer.authority.close(), f.finish()]);
  }
});

it("retains a published receipt's immutable payload and timestamp after mapping and duplicate admission", async () => {
  const f = await historyFixture("mapped_object_receipt", { objects: memoryObjects });
  try {
    const input = canonical({ content: "x".repeat(300 * 1024), whenBusy: "followUp" });
    const accepted = { hash: input.sha256, payload: await f.objects.put(input.json) };
    const result = await f.authority.receive(f.ns, requestId("large"), accepted);
    check(result.item.kind === "received", "Expected original receipt");
    await f.storage.commit([submission(30, "large")], context);
    await f.authority.markAdmitted(result.item, submissionId(30));
    f.memory.operations.length = 0;
    const expected = { ...result.item, kind: "admitted", submission: submissionId(30) };
    expect(await f.authority.get(f.ns, requestId("large"))).toEqual(expected);
    expect(await f.authority.receive(f.ns, requestId("large"), accepted)).toEqual({
      duplicate: true,
      item: expected,
    });
    expect(f.memory.operations).toEqual([]);
  } finally {
    await f.finish();
  }
});

it("resolves concurrent differing mappings to one immutable winner", async () => {
  const f = await historyFixture("mapping_race");
  const peer = await traced();
  try {
    const received = await fixtureReceipt(f);
    await f.storage.commit([submission(30), submission(31)], context);
    const results = await Promise.allSettled([
      f.authority.markAdmitted(received, submissionId(30)),
      peer.authority.markAdmitted(received, submissionId(31)),
    ]);
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((r) => r.status === "rejected")).toHaveLength(1);
    const stored = await f.authority.get(f.ns, received.request);
    check(stored?.kind === "admitted", "Missing winner");
    expect([30, 31]).toContain(stored.submission);
    expect(stored).toEqual({ ...received, kind: "admitted", submission: stored.submission });
  } finally {
    await Promise.all([peer.authority.close(), f.finish()]);
  }
});

it("bounds and orders received receipts, excludes mapped ones, and isolates mapping and work by namespace", async () => {
  const f = await historyFixture("received_page");
  const other = await historyFixture("received_other");
  try {
    const first = await fixtureReceipt(f);
    await other.storage.commit([submission(30)], context);
    await expect(f.authority.markAdmitted(first, submissionId(30))).rejects.toThrow("missing");
    await f.storage.commit([submission(30)], context);
    await f.authority.markAdmitted(first, submissionId(30));
    for (let n = 9; n >= 0; n--) {
      await f.authority.receive(
        f.ns,
        requestId(`request-${n}`),
        acceptedInput({ content: "x", whenBusy: "followUp" }),
      );
    }
    await f.control.query(
      "UPDATE inbox SET received_at=123 WHERE ns=$1 AND request_id<>'fixture'",
      [f.ns],
    );
    const page = await f.authority.received(f.ns);
    expect(page.map((r) => r.request)).toEqual(Array.from({ length: 8 }, (_, i) => `request-${i}`));
    expect(page.every((r) => r.kind === "received" && r.ns === f.ns && r.receivedAt === 123)).toBe(
      true,
    );
    expect((await other.authority.received(other.ns)).map((r) => r.request)).toEqual(["fixture"]);
    await f.storage.commit(
      [
        {
          type: "submission",
          value: { ...submission().value, status: "unanswered", reason: "done" },
        },
      ],
      context,
    );
    for (const received of await f.authority.received(f.ns)) {
      await f.authority.markAdmitted(received, submissionId(30));
    }
    expect((await f.authority.received(f.ns)).map((r) => r.request)).toEqual([
      "request-8",
      "request-9",
    ]);
    for (const received of await f.authority.received(f.ns)) {
      await f.authority.markAdmitted(received, submissionId(30));
    }
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    expect(await other.authority.hasWork(other.ns)).toBe(true);
  } finally {
    await Promise.all([f.finish(), other.finish()]);
  }
});

it("observes receipts, every live task state and queued/placed submissions in one authoritative statement regardless of flags", async () => {
  const f = await historyFixture("work_predicate");
  const observer = await traced();
  const work = async (expected: boolean) => {
    observer.trace.length = 0;
    expect(await observer.authority.hasWork(f.ns)).toBe(expected);
    expect(observer.trace).toHaveLength(1);
    expect(observer.trace[0]!.acknowledged).toBe(true);
    observer.trace.length = 0;
    expect((await observer.authority.eligible()).includes(f.ns)).toBe(expected);
    expect(observer.trace).toHaveLength(1);
    expect(observer.trace[0]!.acknowledged).toBe(true);
  };
  try {
    await f.control.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [f.ns]);
    await work(true); // only the real fixture receipt
    const received = await fixtureReceipt(f);
    const done = {
      ...submission().value,
      status: "done" as const,
      entry: entryId(40),
      answer: entryId(41),
    };
    await f.storage.commit([{ type: "submission", value: done }], context);
    await f.authority.markAdmitted(received, done.id);
    await work(false);
    for (const state of [
      { status: "pending", checkpoint: null },
      { status: "running", checkpoint: null },
      { status: "waiting", checkpoint: null, on: [], policy: "failFast" },
      { status: "completing", outcome: { status: "completed", result: null } },
      { status: "terminal", outcome: { status: "completed", result: null } },
    ] as const) {
      for (const flags of [
        { background: false, abortRequested: false },
        { background: true, abortRequested: true },
      ]) {
        await f.storage.commit(
          [{ type: "task", value: { ...task(20, state).value, ...flags } }],
          context,
        );
        await work(state.status !== "terminal");
      }
    }
    for (const value of [
      submission().value,
      { ...submission().value, status: "placed" as const, entry: entryId(40) },
      done,
      { ...submission().value, status: "unanswered" as const, reason: "cancelled" },
    ]) {
      await f.storage.commit([{ type: "submission", value }], context);
      await work(value.status === "queued" || value.status === "placed");
    }
  } finally {
    await Promise.all([observer.authority.close(), f.finish()]);
  }
});

for (const phase of ["confirmed-abort", "unknown-before", "unknown-after"] as const) {
  it(`mapping handles ${phase} without inventing an acknowledgement`, async () => {
    const f = await historyFixture("mapping_ack");
    let injected = false;
    const actor = await traced(async (at, sql) => {
      if (
        injected ||
        !sql.startsWith("UPDATE inbox SET submission_id=") ||
        at !== (phase === "unknown-after" ? "after" : "before")
      ) {
        return;
      }
      injected = true;
      throw phase === "confirmed-abort"
        ? confirmedAbort()
        : new Error("unknown mapping acknowledgement");
    });
    try {
      const received = await fixtureReceipt(f);
      await f.storage.commit([submission()], context);
      const operation = actor.authority.markAdmitted(received, submissionId(30));
      if (phase === "confirmed-abort") await operation;
      else await expect(operation).rejects.toThrow("unknown mapping acknowledgement");
      expect(
        actor.trace.filter((q) => q.sql.startsWith("UPDATE inbox SET submission_id=")),
      ).toHaveLength(phase === "confirmed-abort" ? 2 : 1);
      expect((await f.authority.get(f.ns, received.request))?.kind).toBe(
        phase === "unknown-before" ? "received" : "admitted",
      );
      await f.authority.markAdmitted(received, submissionId(30));
      expect(await f.authority.get(f.ns, received.request)).toEqual({
        ...received,
        kind: "admitted",
        submission: submissionId(30),
      });
    } finally {
      await Promise.all([actor.authority.close(), f.finish()]);
    }
  });
}

for (const invalid of [0, -1, 9007199254740992]) {
  it(`classifies invalid stored mapped identity ${invalid} as corruption`, async () => {
    const f = await historyFixture("mapping_corrupt");
    try {
      await f.control.query("UPDATE inbox SET submission_id=$2 WHERE ns=$1", [
        f.ns,
        String(invalid),
      ]);
      await expect(f.authority.get(f.ns, requestId("fixture"))).rejects.toBeInstanceOf(
        PayloadCorrupt,
      );
    } finally {
      await f.finish();
    }
  });
}
