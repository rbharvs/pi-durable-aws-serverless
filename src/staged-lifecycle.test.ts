import { Readable } from "node:stream";
import { expect, it } from "vitest";
import { apiHandler, type ApiDependencies } from "./submit.ts";
import type { ApiAuthority } from "./authority.ts";
import { canonical, MAX_INLINE_INPUT_BYTES } from "./input.ts";
import { computeSha256, harnessId } from "./domain.ts";
import { objectFixture } from "../tests/shared/objects.ts";

const ns = harnessId("lifecycle");
const small = canonical({ content: "hello", whenBusy: "followUp" });
const large = canonical({ content: "x".repeat(MAX_INLINE_INPUT_BYTES), whenBusy: "steer" });

function recordingAuthority(order: string[]): ApiAuthority {
  return {
    get: async () => undefined,
    receive: async (ns, request, accepted) => ({
      duplicate: false,
      item: { kind: "received", ns, request, ...accepted, receivedAt: 123 },
    }),
    close: async () => {
      order.push("authority-closed");
    },
  };
}

it("does not initialize optional objects for inline submissions or malformed staged references", async () => {
  const run = apiHandler({
    objects: () => {
      throw Error("must not initialize objects");
    },
    connectAuthority: async () => recordingAuthority([]),
    dispatch: { wake: async () => {} },
    event: () => {},
  });

  expect(
    await run({ ns, request: "one", command: { kind: "inline", input: small.input } }),
  ).toEqual({ ok: true, duplicate: false, receivedAt: 123 });
  for (const key of ["in/foreign/x", `in/${ns}/.`, `in/${ns}/..`]) {
    expect(
      await run({
        ns,
        request: "bad",
        command: { kind: "s3", key, bytes: 1, sha256: "0".repeat(64) },
      }),
    ).toEqual({ ok: false, code: "invalid-input", retryable: false });
  }
});

it("rejects staged transport without a capability before connecting authority", async () => {
  const fixture = objectFixture();
  const run = apiHandler({
    connectAuthority: async () => {
      throw Error("must not connect");
    },
    dispatch: {
      wake: async () => {
        throw Error("must not wake");
      },
    },
    event: () => {},
  });

  expect(
    await run({ ns, request: "one", command: { kind: "s3", ...fixture.stage(ns, small.json) } }),
  ).toEqual({ ok: false, code: "invalid-input", retryable: false });
  expect(fixture.openHandles()).toBe(0);
});

for (const phase of [
  "objects-acquire",
  "acquire",
  "receipt-read",
  "receipt-write",
  "publish",
] as const)
  it(`cleans up owned resources after ${phase} failure without exposing causes`, async () => {
    const fixture = objectFixture();
    const order: string[] = [];
    const events: Record<string, unknown>[] = [];
    const cause = Error("private provider secret");
    const authority = recordingAuthority(order);
    const deps: ApiDependencies = {
      connectAuthority: async () => {
        if (phase === "acquire") throw cause;
        return authority;
      },
      objects: () => {
        if (phase === "objects-acquire") throw cause;
        const objects = fixture.objects(ns);

        return {
          ...objects,
          put:
            phase === "publish"
              ? async () => {
                  throw cause;
                }
              : objects.put,
        };
      },
      dispatch: {
        wake: async () => {
          order.push("wake");
        },
      },
      event: (name, fields) => {
        events.push({ name, ...fields });
      },
      report: () => {
        throw Error("broken private reporter");
      },
    };

    if (phase === "receipt-read")
      authority.get = async () => {
        throw cause;
      };
    if (phase === "receipt-write")
      authority.receive = async () => {
        throw cause;
      };
    const run = apiHandler(deps);
    const input = phase === "receipt-write" ? small : large;
    const response = await run({
      ns,
      request: "one",
      command: { kind: "s3", ...fixture.stage(ns, input.json) },
    });

    expect(response).toEqual({ ok: false, code: "unavailable", retryable: true });
    expect(events).toEqual([{ name: "submit-failed", code: "unavailable", phase }]);
    expect(JSON.stringify([response, events])).not.toMatch(/private|secret/);
    expect(order).toEqual(
      phase === "acquire" || phase === "objects-acquire" ? [] : ["authority-closed"],
    );
    expect(fixture.openHandles()).toBe(0);
    expect(fixture.publishedKeys()).toEqual([]);
  });

it("clears ownership and closes both resources once before a blocked wake despite cleanup and diagnostic failures", async () => {
  const fixture = objectFixture();
  const order: string[] = [];
  const entered = Promise.withResolvers<void>();
  const wake = Promise.withResolvers<void>();
  const authority = recordingAuthority(order);
  const run = apiHandler({
    connectAuthority: async () => ({
      ...authority,
      close: async () => {
        await authority.close();
        throw Error("unknown close ACK");
      },
    }),
    objects: () => {
      const objects = fixture.objects(ns);

      return {
        ...objects,
        close: async () => {
          await objects.close();
          order.push("objects-closed");
          throw Error("object close failure");
        },
      };
    },
    dispatch: {
      wake: async () => {
        order.push("wake");
        entered.resolve();
        await wake.promise;
        throw Error("wake failure");
      },
    },
    event: () => {
      throw Error("observer failure");
    },
  });
  let settled = false;
  const reply = run({
    ns,
    request: "one",
    command: { kind: "s3", ...fixture.stage(ns, large.json) },
  }).then((result) => {
    settled = true;
    return result;
  });

  try {
    await entered.promise;
    expect(order).toEqual(["authority-closed", "objects-closed", "wake"]);
    expect(fixture.openHandles()).toBe(0);
    expect(settled).toBe(false);
  } finally {
    wake.resolve();
    await reply;
  }

  expect(await reply).toEqual({ ok: true, duplicate: false, receivedAt: 123 });
  expect(order).toEqual(["authority-closed", "objects-closed", "wake"]);
});

it("detaches staged request metadata before object acquisition can mutate the caller", async () => {
  const fixture = objectFixture();
  const ref = fixture.stage(ns, large.json);
  const submission = { ns, request: "original", command: { kind: "s3", ...ref } };
  const order: string[] = [];
  const authority = recordingAuthority(order);
  const run = apiHandler({
    connectAuthority: async () => authority,
    objects: () => {
      submission.request = "mutated";
      submission.command.bytes = 1;
      submission.command.key = "in/foreign/input" as typeof ref.key;
      return fixture.objects(ns);
    },
    dispatch: { wake: async () => {} },
    event: () => {},
  });
  let observed: unknown;
  const receive = authority.receive;

  authority.receive = async (ns, request, accepted) => {
    observed = { ns, request, accepted };
    return receive(ns, request, accepted);
  };

  expect(await run(submission)).toMatchObject({ ok: true });
  expect(observed).toMatchObject({
    ns,
    request: "original",
    accepted: { hash: large.sha256, payload: { kind: "s3", bytes: large.bytes } },
  });
  expect(fixture.openHandles()).toBe(0);
});

it("verifies the declared raw integrity even if the adapter changes the reference it receives", async () => {
  const fixture = objectFixture();
  // Same length as the declared content, so only the declared SHA-256 can reject it.
  const other = Buffer.from(canonical({ content: "jello", whenBusy: "followUp" }).json);
  const run = apiHandler({
    connectAuthority: async () => recordingAuthority([]),
    objects: (ns) => ({
      ...fixture.objects(ns),
      getStaged: async (ref) => {
        Object.assign(ref, { bytes: other.length, sha256: computeSha256(other) });

        return Readable.from([other]);
      },
    }),
    dispatch: { wake: async () => {} },
    event: () => {},
  });

  expect(
    await run({ ns, request: "one", command: { kind: "s3", ...fixture.stage(ns, small.json) } }),
  ).toEqual({ ok: false, code: "staged-content-invalid", retryable: false });
  expect(fixture.openHandles()).toBe(0);
});

it("reports a non-binary adapter stream as unavailable, not as invalid client content", async () => {
  const fixture = objectFixture();
  const events: Record<string, unknown>[] = [];
  const run = apiHandler({
    connectAuthority: async () => recordingAuthority([]),
    objects: (ns) => ({
      ...fixture.objects(ns),
      getStaged: async () => Readable.from([small.json]),
    }),
    dispatch: { wake: async () => {} },
    event: (name, fields) => events.push({ name, ...fields }),
  });

  expect(
    await run({ ns, request: "one", command: { kind: "s3", ...fixture.stage(ns, small.json) } }),
  ).toEqual({ ok: false, code: "unavailable", retryable: true });
  expect(events).toEqual([{ name: "submit-failed", code: "unavailable", phase: "staged-read" }]);
  expect(fixture.openHandles()).toBe(0);
});

it("keeps the original read limit when an adapter enlarges the reference", async () => {
  const fixture = objectFixture();
  let chunksRead = 0;
  const run = apiHandler({
    connectAuthority: async () => recordingAuthority([]),
    objects: (scope) => ({
      ...fixture.objects(scope),
      getStaged: async (ref) => {
        Object.assign(ref, { bytes: 8 * 1024 * 1024 });

        return (async function* () {
          for (let index = 0; index < 128; index++) {
            chunksRead++;
            yield Buffer.alloc(64 * 1024, 32);
          }
        })();
      },
    }),
    dispatch: { wake: async () => {} },
    event: () => {},
  });

  expect(
    await run({ ns, request: "one", command: { kind: "s3", ...fixture.stage(ns, small.json) } }),
  ).toEqual({ ok: false, code: "staged-content-invalid", retryable: false });
  expect(chunksRead).toBe(1);
  expect(fixture.openHandles()).toBe(0);
});
