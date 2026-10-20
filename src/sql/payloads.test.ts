import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { computeSha256, harnessId, type HarnessId } from "../domain.ts";
import { uploadValues } from "../object-data.ts";
import { LIMITS } from "../limits.ts";
import { assertEnvelope } from "../payload.ts";
import type { PayloadObjects } from "../s3-payload.ts";
import { Payloads } from "./payloads.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });

  return { promise, resolve };
}

function objectsFor(ns: HarnessId) {
  const puts: string[] = [];
  const objects: PayloadObjects = {
    async put(json) {
      puts.push(json);

      return {
        kind: "s3",
        key: `p/${ns}/${randomUUID()}` as Awaited<ReturnType<PayloadObjects["put"]>>["key"],
        sha256: computeSha256(json),
        bytes: Buffer.byteLength(json),
      };
    },
    async get() {
      throw Error("Planner must not read objects");
    },
  };

  return { objects, puts };
}

it("plans byte-pressure offload of the largest values before any PUT and counts inline JSON once", async () => {
  const ns = harnessId("planner");
  const memory = objectsFor(ns);
  const payloads = new Payloads(ns, memory.objects);
  const values = Array.from({ length: 40 }, (_, i) => ({
    table: "records" as const,
    ns,
    id: 20 + i,
    json: JSON.stringify({ text: "x".repeat((200 + i) * 1024) }),
  }));
  const plan = payloads.plan(values, 100000);

  expect(memory.puts).toEqual([]);
  expect(plan.rowMutations).toBe(80);
  expect(() => assertEnvelope(1 + plan.rowMutations, plan.estimatedBytes)).not.toThrow();
  const prepared = await plan.upload();
  const offloaded = prepared.flatMap((p, i) => (p.params[0] === "s3" ? [i] : []));

  expect(prepared).toHaveLength(40);
  expect(offloaded.length).toBeGreaterThan(0);
  expect(offloaded.length).toBeLessThan(40);
  expect(offloaded).toEqual(
    Array.from({ length: offloaded.length }, (_, i) => 40 - offloaded.length + i),
  );
  expect(memory.puts).toEqual(offloaded.map((i) => values[i]!.json));
  expect(prepared.flatMap((p) => p.before)).toHaveLength(40);
  expect(prepared.flatMap((p) => p.after)).toEqual([]); // immutable entries have no predecessor deletes
  for (let i = 0; i < prepared.length; i++) {
    const p = prepared[i]!;

    expect(p.before[0]!.params[1]).toBe(p.params[1]);
    if (p.params[0] === "inline") expect(p.before[0]!.params[2]).toBe(values[i]!.json);
  }

  const small = payloads.plan([{ table: "records", ns, id: 1, json: JSON.stringify("x") }], 0);
  const bigger = payloads.plan(
    [{ table: "records", ns, id: 1, json: JSON.stringify("x".repeat(1001)) }],
    0,
  );

  expect(bigger.estimatedBytes - small.estimatedBytes).toBe(1000);
});

it("uses UTF-8 bytes at the inline boundary and keeps prepared payloads in input order", async () => {
  const ns = harnessId("boundary");
  const memory = objectsFor(ns);
  const payloads = new Payloads(ns, memory.objects);
  const jsons = [
    JSON.stringify("x".repeat(LIMITS.maxInlineBytes - 2)),
    JSON.stringify("x".repeat(LIMITS.maxInlineBytes - 1)),
    JSON.stringify("🚀".repeat(LIMITS.maxInlineBytes / 4)),
    JSON.stringify({ small: true }),
  ];
  const plan = payloads.plan(
    jsons.map((json, i) => ({ table: "records", ns, id: 20 + i, json })),
    0,
  );

  expect(memory.puts).toEqual([]);
  const prepared = await plan.upload();

  expect(prepared.map((p) => p.params[0])).toEqual(["inline", "s3", "s3", "inline"]);
  expect(memory.puts).toEqual([jsons[1], jsons[2]]);
  expect(prepared.map((p) => p.params[1])).toEqual(prepared.map((p) => p.before[0]!.params[1]));
  expect(new Set(prepared.map((p) => p.params[1])).size).toBe(4);
});

it("rejects invalid JSON, foreign namespaces, and oversized objects during planning, not after PUT", () => {
  const ns = harnessId("invalid_plan");
  const memory = objectsFor(ns);
  const payloads = new Payloads(ns, memory.objects);

  for (const value of [
    { ns, json: "not-json" },
    { ns: harnessId("foreign"), json: "{}" },
    { ns, json: JSON.stringify("x".repeat(LIMITS.maxObjectBytes)) },
  ])
    expect(() => payloads.plan([{ table: "records", id: 20, ...value }], 0)).toThrow();
  expect(memory.puts).toEqual([]);
});

it.each([
  ["Error", Error("upload failed")],
  ["undefined", undefined],
  ["null", null],
  ["false", false],
  ["zero", 0],
  ["empty string", ""],
] as const)(
  "stops a 20-item upload queue on %s failure and joins in-flight workers",
  async (_label, failure) => {
    const values = Array.from({ length: 20 }, (_, i) => i);
    const gates = values.map(() => deferred());
    const started = deferred();
    const starts: number[] = [];
    const memory = objectsFor(harnessId("uploads"));
    let active = 0;
    let peak = 0;
    let settled = false;
    const operation = uploadValues(values, async (i) => {
      starts.push(i);
      active++;
      peak = Math.max(peak, active);
      if (starts.length === 4) started.resolve();

      try {
        await gates[i]!.promise;
        if (i === 0) throw failure;

        return await memory.objects.put(JSON.stringify(i));
      } finally {
        active--;
      }
    }).then(
      () => {
        settled = true;

        return { kind: "fulfilled" as const };
      },
      (reason: unknown) => {
        settled = true;

        return { kind: "rejected" as const, reason };
      },
    );

    try {
      await started.promise;
      expect(starts).toEqual([0, 1, 2, 3]);
      expect(active).toBe(4);
      gates[0]!.resolve();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(settled).toBe(false);
      expect(active).toBe(3);
      // Let one successful worker finish while the other two remain in flight.
      // It must stop rather than taking item 4 from the queue.
      gates[1]!.resolve();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(starts).toEqual([0, 1, 2, 3]);
      expect(active).toBe(2);
      expect(settled).toBe(false);
      for (const gate of gates) gate.resolve();
      const outcome = await operation;

      expect(outcome).toEqual({ kind: "rejected", reason: failure });
      if (outcome.kind === "rejected") expect(outcome.reason).toBe(failure);
      expect(starts).toEqual([0, 1, 2, 3]);
      expect(memory.puts).toEqual(["1", "2", "3"]);
      expect(active).toBe(0);
      expect(peak).toBe(4);
    } finally {
      for (const gate of gates) gate.resolve();
      await operation;
    }
  },
);

it("returns upload references in input order even when workers complete in reverse order", async () => {
  const gates = Array.from({ length: 3 }, () => deferred());
  const completed: number[] = [];
  const memory = objectsFor(harnessId("ordered_uploads"));
  const operation = uploadValues([0, 1, 2], async (i) => {
    await gates[i]!.promise;
    completed.push(i);

    return memory.objects.put(JSON.stringify(i));
  });

  gates[2]!.resolve();
  gates[1]!.resolve();
  gates[0]!.resolve();
  const result = await operation;

  expect(completed).toEqual([2, 1, 0]);
  expect(result.map((ref) => ref.sha256)).toEqual(
    [0, 1, 2].map((i) => computeSha256(JSON.stringify(i))),
  );
});
