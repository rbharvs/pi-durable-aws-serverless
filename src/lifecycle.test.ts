import { expect, it } from "vitest";
import { apiHandler } from "./submit.ts";
import { canonical, MAX_BLOCKS } from "./input.ts";

it("closes authority before advisory wake, but still awaits wake completion", async () => {
  const entered = Promise.withResolvers<void>();
  const wake = Promise.withResolvers<void>();
  let closes = 0;
  let settled = false;
  const run = apiHandler({
    connectAuthority: async () => ({
      get: async () => undefined,
      receive: async (ns, request, accepted) => ({
        duplicate: false,
        item: { kind: "received", ns, request, ...accepted, receivedAt: 123 },
      }),
      close: async () => {
        closes++;
      },
    }),
    dispatch: {
      wake: () => {
        entered.resolve();

        return wake.promise;
      },
    },
    event: () => {},
  });
  const reply = run({
    ns: "wake",
    request: "one",
    command: { kind: "inline", input: { content: "hello", whenBusy: "followUp" } },
  }).then((result) => {
    settled = true;

    return result;
  });

  try {
    await entered.promise;
    expect(closes).toBe(1);
    expect(settled).toBe(false);
  } finally {
    wake.resolve();
    await reply;
  }

  expect(await reply).toEqual({ ok: true, duplicate: false, receivedAt: 123 });
  expect(closes).toBe(1);
});

it("rejects oversized arrays before visiting blocks, unlike a whole-input JSON snapshot", () => {
  let visits = 0;
  const content = Array.from({ length: MAX_BLOCKS + 1 }, () => ({ type: "text", text: "" }));

  Object.defineProperty(content, 0, {
    enumerable: true,
    get: () => {
      visits++;

      return { type: "text", text: "potentially expensive getter" };
    },
  });
  const input = { content, whenBusy: "followUp" };

  expect(() => canonical(input)).toThrow("Invalid or too many content blocks");
  expect(visits).toBe(0);
  JSON.parse(JSON.stringify(input));
  expect(visits).toBe(1);
});
