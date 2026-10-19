import { it, expect } from "vitest";
import { apiHandler, type ApiDependencies } from "./submit.ts";
import type { ApiAuthority } from "./authority.ts";
import { InvalidInput } from "./faults.ts";
import { MAX_INLINE_INPUT_BYTES } from "./input.ts";

const command = { kind: "inline", input: { content: "hello", whenBusy: "followUp" } };

function fixture(connectAuthority: () => Promise<ApiAuthority>) {
  const wakes: string[] = [],
    events: Record<string, unknown>[] = [],
    reports: unknown[] = [];
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
  const f = fixture(async () => {
    throw Error("must not connect");
  });
  const malformed = [
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
    expect(await f.run(input)).toEqual({ ok: false, code: "invalid-input", retryable: false });

  expect(f.connects()).toBe(0);
  expect(f.wakes).toEqual([]);
  expect(f.reports).toHaveLength(malformed.length);
  expect(f.reports.every((r) => r instanceof InvalidInput && r.cause instanceof Error)).toBe(true);
  expect(f.events).toEqual(
    malformed.map(() => ({
      name: "submit-failed",
      code: "invalid-input",
      phase: "validation-or-internal",
    })),
  );
});

it("an acquisition failure is retryable unavailable and keeps the cause private", async () => {
  const cause = Error("SELECT private provider secret"),
    f = fixture(async () => {
      throw cause;
    });
  const response = await f.run({ ns: "api", request: "r", command });

  expect(response).toEqual({ ok: false, code: "unavailable", retryable: true });
  expect(f.reports[0]).toMatchObject({ phase: "acquire", cause });
  expect(f.events).toEqual([{ name: "submit-failed", code: "unavailable", phase: "acquire" }]);
  expect(JSON.stringify([response, f.events])).not.toMatch(/SELECT|private|secret/);
  expect(f.wakes).toEqual([]);
});

it("an authority contract violation is retryable internal, not a claim of non-acceptance", async () => {
  let closes = 0;
  const f = fixture(async () => ({
    get: async () => undefined,
    receive: async () => undefined as never,
    close: async () => {
      closes++;
    },
  }));

  expect(await f.run({ ns: "api", request: "r", command })).toEqual({
    ok: false,
    code: "internal",
    retryable: true,
  });
  expect(f.events).toEqual([
    { name: "submit-failed", code: "internal", phase: "validation-or-internal" },
  ]);
  expect(closes).toBe(1);
  expect(f.wakes).toEqual([]);
});
