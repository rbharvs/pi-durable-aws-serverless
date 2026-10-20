import { expect, it } from "vitest";
import {
  apiHandler,
  reconcile,
  runtimeDependencies,
  worker,
  type HarnessId,
} from "../../src/index.ts";
import { harnessId, requestId } from "../../src/domain.ts";
import { SqlAuthority } from "../../src/sql/authority.ts";
import { Database } from "../../src/sql/db.ts";
import { acceptedInput } from "../shared/input.ts";
import { faultOnce, provider, wrap } from "./postgres.ts";
import { deferred, workerFixture } from "./worker-support.ts";

it("lost API wake leaves a durable receipt; fresh discovery and explicit delivery run real Pi to done and cold continuation", async () => {
  const f = await workerFixture("reconcile_missed");
  let failedWakes = 0;
  const api = apiHandler({
    ...f.apiDeps,
    dispatch: {
      async wake() {
        failedWakes++;
        throw Error("lost API wake");
      },
    },
  });
  const receive = (request: string, content: string) =>
    api({
      ns: f.ns,
      request,
      command: { kind: "inline", input: { content, whenBusy: "followUp" } },
    });
  try {
    const ack = await receive("one", "hello");
    expect(ack).toMatchObject({ ok: true, duplicate: false });
    expect(failedWakes).toBe(1);
    expect(f.hints).toEqual([]);
    const fresh = await f.backend.connectAuthority();
    let receipt;
    try {
      receipt = await fresh.get(f.ns, requestId("one"));
      expect(receipt).toMatchObject({ kind: "received", receivedAt: ack.ok ? ack.receivedAt : -1 });
    } finally {
      await fresh.close();
    }
    const before = await f.head();
    expect(await reconcile(f.reconcileDeps)).toBe(1);
    expect(f.hints).toEqual([f.ns]);
    expect(await f.head()).toEqual(before);
    expect(
      await f.authority.inspectionDb.query("SELECT * FROM records WHERE ns=$1", [f.ns]),
    ).toEqual([]);
    // Neither API nor reconciliation runs Pi. Deliver the advisory hint explicitly.
    expect(await worker(f.hints.shift()!, () => 45000, f.deps)).toBe("released");
    expect(f.hints).toEqual([]); // A worker has no dispatch capability.
    await f.expire();
    const first = await f.output();
    expect(first.receipt).toEqual({
      ...receipt,
      kind: "admitted",
      submission: first.submissions[0]!.id,
    });
    expect(first.submissions.map((s) => s.status)).toEqual(["done"]);
    expect(first.tasks.length).toBeGreaterThan(0);
    expect(first.tasks.every((t) => t.state.status === "terminal")).toBe(true);
    expect(first.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
    expect(first.entries.find((e) => e.kind === "pi.assistant")?.model).toMatchObject([
      { role: "assistant", content: [{ type: "text", text: "ok" }] },
    ]);
    await f.expire();
    expect(await reconcile(f.reconcileDeps)).toBe(0); // Terminal work stays excluded even expired.
    expect(await receive("two", "continue cold")).toMatchObject({ ok: true });
    expect(await reconcile(f.reconcileDeps)).toBe(1);
    const cold = runtimeDependencies(f.options);
    expect(await worker(f.hints.shift()!, () => 45000, cold)).toBe("released");
    await f.expire();
    const second = await f.output();
    expect(second.submissions.map((s) => s.status)).toEqual(["done", "done"]);
    expect(second.entries.filter((e) => e.kind === "pi.assistant")).toHaveLength(2);
    expect(second.entries.filter((e) => e.kind === "pi.user")).toHaveLength(2);
    expect(second.entries.slice(0, first.entries.length)).toEqual(first.entries);
    expect(new Set(second.entries.map((e) => e.conversationId)).size).toBe(1);
    expect(f.hints).toEqual([]);
    expect(failedWakes).toBe(2);
  } finally {
    await f.finish();
  }
});

it("reconciliation waits for expiry after a real submit-before-map failure and the new owner completes one logical submission", async () => {
  const f = await workerFixture("reconcile_expired");
  const connect = f.deps.connectAuthority;
  let cut = true;
  f.deps.connectAuthority = async () => {
    const authority = await connect();
    const mark = authority.markAdmitted.bind(authority);
    authority.markAdmitted = async (receipt, id) => {
      if (cut) {
        cut = false;
        throw Error("mapping cut");
      }
      await mark(receipt, id);
    };
    return authority;
  };
  try {
    await f.receive();
    await expect(worker(f.hints.shift()!, () => 45000, f.deps)).rejects.toThrow("mapping cut");
    const prior = await f.head();
    expect(prior.owner).not.toBeNull();
    expect(await reconcile(f.reconcileDeps)).toBe(0);
    expect(f.hints).toEqual([]);
    const submissions = await f.authority.inspectionDb.query(
      "SELECT id FROM records WHERE ns=$1 AND type='submission'",
      [f.ns],
    );
    expect(submissions).toHaveLength(1);
    expect((await f.authority.get(f.ns, requestId("one")))?.kind).toBe("received");
    await f.expire();
    expect(await reconcile(f.reconcileDeps)).toBe(1);
    expect(await worker(f.hints.shift()!, () => 45000, runtimeDependencies(f.options))).toBe(
      "released",
    );
    expect(Number((await f.head()).epoch)).toBe(Number(prior.epoch) + 1);
    await f.expire();
    const output = await f.output();
    expect(output.submissions.map((s) => String(s.id))).toEqual(submissions.map((s) => s.id));
    expect(output.submissions[0]!.status).toBe("done");
    expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
    expect(output.entries.find((e) => e.kind === "pi.assistant")?.model).toMatchObject([
      { role: "assistant", content: [{ type: "text", text: "ok" }] },
    ]);
    expect(await f.authority.received(f.ns)).toEqual([]);
    expect(f.hints).toEqual([]);
  } finally {
    await f.finish();
  }
});

it("a partial dispatch failure attempts only the five discovered PG candidates and leaves all durable work intact", async () => {
  const f = await workerFixture("reconcile_partial");
  const candidates = Array.from({ length: 7 }, (_, i) => harnessId(`${f.ns}_${i}`));
  try {
    for (const [i, ns] of candidates.entries()) {
      await f.authority.receive(
        ns,
        requestId("one"),
        acceptedInput({ content: "pending", whenBusy: "followUp" }),
      );
      await f.authority.inspectionDb.query("UPDATE harness SET lease_until=$2 WHERE ns=$1", [
        ns,
        100 + i,
      ]);
    }
    const before = await f.authority.inspectionDb.query(
      "SELECT * FROM harness WHERE starts_with(ns,$1) ORDER BY ns",
      [f.ns],
    );
    expect(
      await reconcile({
        ...f.reconcileDeps,
        dispatch: {
          async wake(ns) {
            await f.dispatch.wake(ns);
            if (ns === candidates[1]) throw Error("lost delivery acknowledgement");
          },
        },
      }),
    ).toBe(5);
    expect(f.hints).toEqual(candidates.slice(0, 5)); // Failure does not prove non-delivery.
    expect(f.events).toEqual([
      { event: "dispatch-failed", ns: candidates[1] },
      { event: "reconcile", eligible: candidates.slice(0, 5), failed: 1 },
    ]);
    expect(
      await f.authority.inspectionDb.query(
        "SELECT * FROM harness WHERE starts_with(ns,$1) ORDER BY ns",
        [f.ns],
      ),
    ).toEqual(before);
    for (const ns of candidates)
      expect((await f.authority.received(ns)).map((r) => r.request)).toEqual(["one"]);
  } finally {
    await f.finish();
  }
});

it("a real discovery connection closes once and retains the primary query error despite cleanup and observer failures", async () => {
  const cause = Error("private discovery failure");
  const client = wrap(
    await provider.connect(),
    faultOnce("before", "SELECT h.ns", () => cause),
  );
  let closes = 0;
  const events: Record<string, unknown>[] = [];
  const authority = new SqlAuthority(
    new Database({
      query: client.query.bind(client),
      async end() {
        closes++;
        await client.end();
        throw Error("secondary close acknowledgement");
      },
    }),
    provider,
  );
  await expect(
    reconcile({
      async connectAuthority() {
        return authority;
      },
      dispatch: {
        async wake() {
          throw Error("must not dispatch");
        },
      },
      event(name, fields) {
        events.push({ name, ...fields });
        throw Error("observer");
      },
    }),
  ).rejects.toBe(cause);
  expect(closes).toBe(1);
  expect(events).toEqual([{ name: "cleanup-failed", resource: "authority" }]);
  await expect(client.query("SELECT 1")).rejects.toThrow();
});

it("overlapping discoveries each dispatch a hint, and sequential delivery admits once", async () => {
  const f = await workerFixture("reconcile_overlap");
  const bothDiscovered = deferred();
  const continueDiscovery = deferred();
  const selections: HarnessId[][] = [];
  let closes = 0;
  const connectAuthority = async () => {
    const authority = await f.backend.connectAuthority();
    const eligible = authority.eligible.bind(authority);
    const close = authority.close.bind(authority);
    authority.eligible = async () => {
      const result = await eligible();
      selections.push(result);
      if (selections.length === 2) bothDiscovered.resolve();
      await continueDiscovery.promise;
      return result;
    };
    authority.close = async () => {
      await close();
      closes++;
    };
    return authority;
  };
  let reconciliations: Promise<number[]> | undefined;
  try {
    await f.receive();
    f.hints.length = 0; // Deliberately lose the API hint.
    reconciliations = Promise.all(
      [1, 2].map(() => reconcile({ ...f.reconcileDeps, connectAuthority })),
    );
    await bothDiscovered.promise;
    expect(selections).toEqual([[f.ns], [f.ns]]);
    expect(closes).toBe(0);
    expect(f.hints).toEqual([]);
    continueDiscovery.resolve();
    expect(await reconciliations).toEqual([1, 1]);
    expect(closes).toBe(2);
    expect(f.hints).toEqual([f.ns, f.ns]);
    expect(await worker(f.hints.shift()!, () => 45000, f.deps)).toBe("released");
    await f.expire();
    expect(await worker(f.hints.shift()!, () => 45000, f.deps)).toBe("idle");
    expect(f.hints).toEqual([]);
    await f.expire();
    const output = await f.output();
    expect(output.submissions.map((s) => s.status)).toEqual(["done"]);
    expect(output.entries.filter((e) => e.kind === "pi.user")).toHaveLength(1);
  } finally {
    continueDiscovery.resolve();
    await reconciliations?.catch(() => {});
    await f.finish();
  }
});
