import { expect, it } from "vitest";
import { apiHandler } from "../../src/submit.ts";
import { namespace, traced, type Query } from "./postgres.ts";

it("acceptance needs no namespace state or redundant API read", async () => {
  const f = await namespace("statements"),
    trace: Query[] = [];
  const run = apiHandler({
    connectAuthority: async () => (await traced(async () => {}, trace)).control,
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
          ns: f.ns,
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
    expect(counts).toEqual([2, 2, 1, 1]);
    console.log("SQL new namespace/new request/duplicate/conflict:", counts);
  } finally {
    await f.finish();
  }
});
