import { it, expect } from "vitest";
import { apiHandler, type ApiDependencies } from "./submit.ts";
import type { ApiAuthority } from "./authority.ts";
import { InvalidInput } from "./faults.ts";
import { MAX_INLINE_INPUT_BYTES } from "./input.ts";

const command = { kind: "inline", input: { content: "hello", whenBusy: "followUp" } };

/**
 * A submit handler with recording dependencies. Its report sink records the cause and then
 * throws, so every test also checks that a failing sink cannot change the response.
 */
function submitApi(connectAuthority: () => Promise<ApiAuthority>) {
  const wakes: string[] = [];
  const events: Record<string, unknown>[] = [];
  const reports: unknown[] = [];
  let connects = 0;
  const deps: ApiDependencies = {
    connectAuthority: () => {
      connects++;

      return connectAuthority();
    },
    dispatch: {
      wake: async (ns) => {
        wakes.push(ns);
      },
    },
    event: (name, fields) => events.push({ name, ...fields }),
    report: (cause) => {
      reports.push(cause);

      throw Error("broken diagnostic sink");
    },
  };

  return { run: apiHandler(deps), wakes, events, reports, connects: () => connects };
}

it("rejects malformed requests as invalid input before acquiring authority", async () => {
  const api = submitApi(async () => {
    throw Error("must not connect");
  });
  const sparse: unknown[] = [];

  sparse.length = 1;
  const malformed = [
    {
      ns: "api",
      request: "r",
      command: { kind: "inline", input: { content: sparse, whenBusy: "followUp" } },
    },
    null,
    [],
    "text",
    { ns: "bad/path", request: "r", command },
    { ns: "api", request: "", command },
    { ns: "api", request: "r".repeat(129), command },
    { ns: "api", request: "r" },
    { ns: "api", request: "r", command: { ...command, kind: "s3" } },
    { ns: "api", request: "r", command: { ...command, input: { content: "x" } } },
    {
      ns: "api",
      request: "r",
      command: {
        kind: "inline",
        input: { content: "x".repeat(MAX_INLINE_INPUT_BYTES), whenBusy: "followUp" },
      },
    },
  ];

  for (const input of malformed)
    expect(await api.run(input)).toEqual({ ok: false, code: "invalid-input", retryable: false });

  expect(api.connects()).toBe(0);
  expect(api.wakes).toEqual([]);
  expect(api.reports).toHaveLength(malformed.length);
  for (const report of api.reports) {
    expect(report).toBeInstanceOf(InvalidInput);
    expect((report as InvalidInput).cause).toBeInstanceOf(Error);
  }
  expect(api.events).toEqual(
    malformed.map(() => ({
      name: "submit-failed",
      code: "invalid-input",
      phase: "validation-or-internal",
    })),
  );
});

it("an acquisition failure is retryable unavailable and keeps the cause private", async () => {
  const cause = Error("SELECT private provider secret");
  const api = submitApi(async () => {
    throw cause;
  });
  const response = await api.run({ ns: "api", request: "r", command });

  expect(response).toEqual({ ok: false, code: "unavailable", retryable: true });
  expect(api.reports[0]).toMatchObject({ phase: "acquire", cause });
  expect(api.events).toEqual([{ name: "submit-failed", code: "unavailable", phase: "acquire" }]);
  expect(JSON.stringify([response, api.events])).not.toMatch(/SELECT|private|secret/);
  expect(api.wakes).toEqual([]);
});

it("an authority contract violation is retryable internal, not a claim of non-acceptance", async () => {
  let closes = 0;
  const api = submitApi(async () => ({
    get: async () => undefined,
    // Violates the contract: `receive` must return a receipt or throw.
    receive: async () => undefined as never,
    close: async () => {
      closes++;
    },
  }));

  expect(await api.run({ ns: "api", request: "r", command })).toEqual({
    ok: false,
    code: "internal",
    retryable: true,
  });
  expect(api.events).toEqual([
    { name: "submit-failed", code: "internal", phase: "validation-or-internal" },
  ]);
  expect(closes).toBe(1);
  expect(api.wakes).toEqual([]);
});
