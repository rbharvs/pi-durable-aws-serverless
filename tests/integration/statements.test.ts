import { expect, it } from "vitest";
import { apiHandler } from "../../src/submit.ts";
import { namespace, traced, type Query } from "./postgres.ts";

it("ingress checks namespace existence without rewriting an existing ownership head", async () => {
  const store = await namespace("statements");
  const trace: Query[] = [];
  const run = apiHandler({
    connectAuthority: async () => (await traced(async () => {}, trace)).authority,
    dispatch: { wake: async () => {} },
    event: () => {},
  });

  try {
    const counts: number[] = [];
    const outcomes = [];

    for (const [request, content] of [
      ["one", "hello"],
      ["two", "hello"],
      ["one", "hello"],
      ["one", "different"],
    ]) {
      trace.length = 0;
      outcomes.push(
        await run({
          ns: store.ns,
          request,
          command: { kind: "inline", input: { content, whenBusy: "followUp" } },
        }),
      );
      counts.push(trace.length);
    }

    expect(outcomes).toEqual([
      { ok: true, duplicate: false, receivedAt: expect.any(Number) },
      { ok: true, duplicate: false, receivedAt: expect.any(Number) },
      { ...outcomes[0], duplicate: true },
      { ok: false, code: "request-conflict", retryable: false },
    ]);
    // First request creates the free head; later requests only check that it exists.
    // Receipt operations remain SELECT+INSERT for a new request and SELECT for a repeat.
    expect(counts).toEqual([4, 3, 2, 2]);
    expect(trace.filter((q) => /^(INSERT INTO|UPDATE) harness/.test(q.sql))).toEqual([]);
    console.log("SQL new namespace/new request/duplicate/conflict:", counts);
  } finally {
    await store.finish();
  }
});
