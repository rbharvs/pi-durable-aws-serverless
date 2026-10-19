import { Readable } from "node:stream";
import { expect, it } from "vitest";
import { apiHandler, type ApiDependencies } from "../../src/submit.ts";
import { requestId, type HarnessId } from "../../src/domain.ts";
import { canonical, MAX_INLINE_INPUT_BYTES, MAX_INPUT_BYTES } from "../../src/input.ts";
import { objectFixture } from "../shared/objects.ts";
import {
  namespace,
  confirmedAbort,
  faultOnce,
  traced,
  statements,
  type Query,
} from "./postgres.ts";

const small = { content: "hello", whenBusy: "followUp" as const };
const large = { ...small, content: "x".repeat(MAX_INLINE_INPUT_BYTES) };
const unavailable = { ok: false, code: "unavailable", retryable: true };
const invalidContent = { ok: false, code: "staged-content-invalid", retryable: false };
const conflict = { ok: false, code: "request-conflict", retryable: false };

function submitApi(
  fixture: ReturnType<typeof objectFixture>,
  overrides: Partial<ApiDependencies> = {},
) {
  const wakes: string[] = [];
  const events: Record<string, unknown>[] = [];
  const reports: unknown[] = [];
  const run = apiHandler({
    connectAuthority: async () => (await traced()).authority,
    objects: (ns) => fixture.objects(ns),
    dispatch: {
      wake: async (ns) => {
        wakes.push(ns);
      },
    },
    event: (name, fields) => events.push({ name, ...fields }),
    report: (cause) => reports.push(cause),
    ...overrides,
  });

  return { run, wakes, events, reports };
}

function staged(
  fixture: ReturnType<typeof objectFixture>,
  ns: HarnessId,
  raw: string | Uint8Array,
  name = "input",
) {
  return { kind: "s3", ...fixture.stage(ns, raw, name) };
}

for (const extraByte of [0, 1])
  it(`stores staged canonical input at 256 KiB + ${extraByte} bytes in the correct form and preserves duplicates`, async () => {
    const store = await namespace("staged_inline_boundary");
    const fixture = objectFixture();
    const api = submitApi(fixture);
    const inlineLimit = 256 * 1024;
    const envelopeBytes = Buffer.byteLength(JSON.stringify({ ...small, content: "" }));
    const input = { ...small, content: "x".repeat(inlineLimit + extraByte - envelopeBytes) };
    const expected = canonical(input);
    const command = staged(fixture, store.ns, expected.json);
    const submission = { ns: store.ns, request: "one", command };

    try {
      expect(expected.bytes).toBe(inlineLimit + extraByte);
      const first = await api.run(submission);

      expect(first).toEqual({ ok: true, duplicate: false, receivedAt: expect.any(Number) });
      const item = await store.authority.get(store.ns, requestId("one"));

      if (extraByte === 0) {
        expect(item?.payload).toEqual({ kind: "inline", json: expected.json });
        expect(fixture.publishedKeys()).toEqual([]);
      } else {
        if (item?.payload.kind !== "s3")
          throw Error("Expected published receipt above the inline limit");
        expect(item.payload).toMatchObject({ sha256: expected.sha256, bytes: inlineLimit + 1 });
        expect(fixture.publishedKeys()).toEqual([item.payload.key]);
        expect(fixture.inspectPublished(item.payload.key)?.toString()).toBe(expected.json);
      }

      const publications = fixture.publishedKeys();

      expect(await api.run(submission)).toEqual({ ...first, duplicate: true });
      expect(await api.run({ ...submission, command: { kind: "inline", input } })).toEqual(
        extraByte === 0
          ? { ...first, duplicate: true }
          : { ok: false, code: "invalid-input", retryable: false },
      );
      fixture.deleteStage(command.key);
      expect(await api.run(submission)).toEqual(invalidContent);
      const replacement = staged(fixture, store.ns, ` \n${expected.json}\n `, "replacement.json");

      expect(await api.run({ ...submission, command: replacement })).toEqual({
        ...first,
        duplicate: true,
      });
      expect(await store.authority.get(store.ns, requestId("one"))).toEqual(item);
      expect(fixture.publishedKeys()).toEqual(publications);
      expect(fixture.openHandles()).toBe(0);
    } finally {
      await store.finish();
    }
  });

for (const [name, raw] of [
  ["deep", "[ [ [ [ ] ] ] ]"],
  ["wide", "[" + Array.from({ length: 2048 }, () => "{}").join(",") + "]"],
] as const)
  it(`rejects ${name} staged JSON before authority acquisition`, async () => {
    const store = await namespace("staged_structure");
    const fixture = objectFixture();
    const api = submitApi(fixture, {
      connectAuthority: async () => {
        throw Error("must reject before acquisition");
      },
    });

    try {
      expect(
        await api.run({ ns: store.ns, request: "bad", command: staged(fixture, store.ns, raw) }),
      ).toEqual(invalidContent);
      expect(await store.inbox()).toEqual([]);
      expect(fixture.publishedKeys()).toEqual([]);
      expect(fixture.openHandles()).toBe(0);
      expect(api.wakes).toEqual([]);
    } finally {
      await store.finish();
    }
  });

it("small staged content becomes inline and deduplicates by canonical, not raw, identity", async () => {
  const store = await namespace("staged_small");
  const fixture = objectFixture();
  const api = submitApi(fixture);
  const raw = ' { "whenBusy": "followUp", "content": "hello" } ';
  const command = staged(fixture, store.ns, raw);
  const submission = { ns: store.ns, request: "one", command };

  try {
    const first = await api.run(submission);

    expect(first).toEqual({ ok: true, duplicate: false, receivedAt: expect.any(Number) });
    const item = await store.authority.get(store.ns, requestId("one"));

    expect(item).toMatchObject({
      hash: canonical(small).sha256,
      payload: { kind: "inline", json: canonical(small).json },
    });
    expect(command.sha256).not.toBe(item?.hash);
    expect(
      await api.run({
        ...submission,
        command: { kind: "inline", input: small },
      }),
    ).toEqual({ ...first, duplicate: true });

    const changed = staged(fixture, store.ns, JSON.stringify({ ...small, content: "different" }));

    // The original raw reference no longer identifies the mutable source.
    expect(await api.run(submission)).toEqual(invalidContent);
    expect(await api.run({ ...submission, command: changed })).toEqual(conflict);
    fixture.deleteStage(changed.key);
    expect(await api.run({ ...submission, command: changed })).toEqual(invalidContent);
    expect(await store.authority.get(store.ns, requestId("one"))).toEqual(item);
    expect(fixture.publishedKeys()).toEqual([]);
    expect(fixture.openHandles()).toBe(0);
  } finally {
    await store.finish();
  }
});

it("publishes large canonical JSON independently of mutable staging and preserves its original metadata", async () => {
  const store = await namespace("staged_large");
  const fixture = objectFixture();
  const api = submitApi(fixture);
  const input = canonical(large);
  const command = staged(
    fixture,
    store.ns,
    ` { "whenBusy":"followUp", "content":${JSON.stringify(large.content)} } `,
  );
  const submission = { ns: store.ns, request: "one", command };

  try {
    const first = await api.run(submission);
    const item = await store.authority.get(store.ns, requestId("one"));

    expect(first).toMatchObject({ ok: true, duplicate: false });
    expect(item?.payload).toEqual({
      kind: "s3",
      key: expect.stringMatching(new RegExp(`^p/${store.ns}/`)),
      sha256: input.sha256,
      bytes: input.bytes,
    });
    if (item?.payload.kind !== "s3") throw Error("Expected published receipt");
    expect(command.sha256).not.toBe(input.sha256);
    expect(fixture.inspectPublished(item.payload.key)?.toString()).toBe(input.json);

    fixture.stage(store.ns, JSON.stringify(small));
    fixture.deleteStage(command.key);
    const retry = staged(fixture, store.ns, `\n${input.json}\n`, "retry");

    expect(await api.run({ ...submission, command: retry })).toEqual({ ...first, duplicate: true });
    expect(
      await api.run({
        ...submission,
        command: staged(
          fixture,
          store.ns,
          JSON.stringify({ ...large, whenBusy: "steer" }),
          "conflict",
        ),
      }),
    ).toEqual(conflict);
    expect(fixture.publishedKeys()).toEqual([item.payload.key]);
    expect(await store.authority.get(store.ns, requestId("one"))).toEqual(item);
    expect(fixture.inspectPublished(item.payload.key)?.toString()).toBe(input.json);
    expect(fixture.openHandles()).toBe(0);
  } finally {
    await store.finish();
  }
});

it("accepts exactly 8 MiB raw staging and canonical JSON, but rejects an oversized raw reference", async () => {
  const store = await namespace("staged_bounds");
  const fixture = objectFixture();
  const api = submitApi(fixture);
  const json = canonical(small).json;
  const envelope = canonical({ ...small, content: "" }).bytes;
  const maximum = canonical({
    ...small,
    content: "x".repeat(MAX_INPUT_BYTES - envelope),
  });

  try {
    expect(
      await api.run({
        ns: store.ns,
        request: "raw_max",
        command: staged(
          fixture,
          store.ns,
          json + " ".repeat(MAX_INPUT_BYTES - Buffer.byteLength(json)),
        ),
      }),
    ).toMatchObject({ ok: true });
    expect((await store.authority.get(store.ns, requestId("raw_max")))?.payload.kind).toBe(
      "inline",
    );
    expect(
      await api.run({
        ns: store.ns,
        request: "canonical_max",
        command: staged(fixture, store.ns, maximum.json),
      }),
    ).toMatchObject({ ok: true });
    expect(
      (await store.authority.get(store.ns, requestId("canonical_max")))?.payload,
    ).toMatchObject({
      kind: "s3",
      bytes: MAX_INPUT_BYTES,
    });
    expect(
      await api.run({
        ns: store.ns,
        request: "raw_over",
        command: {
          kind: "s3",
          key: `in/${store.ns}/input`,
          sha256: maximum.sha256,
          bytes: MAX_INPUT_BYTES + 1,
        },
      }),
    ).toEqual({ ok: false, code: "invalid-input", retryable: false });
    expect((await store.inbox()).map((row) => row.request_id)).toEqual([
      "canonical_max",
      "raw_max",
    ]);
  } finally {
    await store.finish();
  }
});

for (const [name, raw] of [
  ["malformed JSON", "{"],
  ["unknown input field", JSON.stringify({ ...small, ignored: true })],
  ["invalid busy policy", JSON.stringify({ ...small, whenBusy: "reject" })],
  [
    "invalid base64 image",
    JSON.stringify({ ...small, content: [{ type: "image", data: "AR==", mimeType: "image/png" }] }),
  ],
  ["malformed UTF-8", Buffer.from([0xc0, 0x80])],
  [
    "UTF-8 BOM",
    Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from(JSON.stringify(small))]),
  ],
] as const)
  it(`rejects staged ${name} without a receipt, publication, or wake`, async () => {
    const store = await namespace("staged_invalid");
    const fixture = objectFixture();
    const api = submitApi(fixture);

    try {
      expect(
        await api.run({ ns: store.ns, request: "bad", command: staged(fixture, store.ns, raw) }),
      ).toEqual(invalidContent);
      expect(await store.inbox()).toEqual([]);
      expect(fixture.publishedKeys()).toEqual([]);
      expect(fixture.openHandles()).toBe(0);
      expect(api.wakes).toEqual([]);
    } finally {
      await store.finish();
    }
  });

for (const mismatch of ["length", "hash"] as const)
  it(`rejects staged ${mismatch} mismatch even with valid JSON content`, async () => {
    const store = await namespace("staged_integrity");
    const fixture = objectFixture();
    const api = submitApi(fixture);
    const command = staged(fixture, store.ns, JSON.stringify(small));

    try {
      expect(
        await api.run({
          ns: store.ns,
          request: "bad",
          command: {
            ...command,
            ...(mismatch === "length" ? { bytes: command.bytes + 1 } : { sha256: "0".repeat(64) }),
          },
        }),
      ).toEqual(invalidContent);
      expect(await store.inbox()).toEqual([]);
      expect(fixture.openHandles()).toBe(0);
    } finally {
      await store.finish();
    }
  });

for (const mismatch of ["hash", "length", "namespace", "staging-key", "bounds"] as const)
  it(`does not record a receipt when publication returns mismatched ${mismatch}`, async () => {
    const store = await namespace("publication_invalid");
    const fixture = objectFixture();
    const api = submitApi(fixture, {
      objects: (ns) => {
        const objects = fixture.objects(ns);

        return {
          ...objects,
          async put(json) {
            const ref = await objects.put(json);
            const changes = {
              hash: { sha256: "0".repeat(64) },
              length: { bytes: ref.bytes + 1 },
              namespace: { key: ref.key.replace(`p/${ns}/`, "p/foreign/") },
              "staging-key": { key: `in/${ns}/input` },
              bounds: { bytes: MAX_INPUT_BYTES + 1 },
            };

            return { ...ref, ...changes[mismatch] } as typeof ref;
          },
        };
      },
    });

    try {
      expect(
        await api.run({
          ns: store.ns,
          request: "one",
          command: staged(fixture, store.ns, canonical(large).json),
        }),
      ).toEqual(unavailable);
      expect(await store.inbox()).toEqual([]);
      expect(fixture.publishedKeys()).toHaveLength(1);
      expect(fixture.openHandles()).toBe(0);
      expect(api.events).toEqual([
        { name: "submit-failed", code: "unavailable", phase: "publish" },
      ]);
      expect(api.wakes).toEqual([]);
    } finally {
      await store.finish();
    }
  });

it("has no receipt while publication is awaiting durable confirmation", async () => {
  const store = await namespace("publication_order");
  const fixture = objectFixture();
  const entered = Promise.withResolvers<void>();
  const confirmed = Promise.withResolvers<void>();
  const api = submitApi(fixture, {
    objects: (ns) => {
      const objects = fixture.objects(ns);

      return {
        ...objects,
        async put(json) {
          const ref = await objects.put(json);

          entered.resolve();
          await confirmed.promise;

          return ref;
        },
      };
    },
  });
  const reply = api.run({
    ns: store.ns,
    request: "one",
    command: staged(fixture, store.ns, canonical(large).json),
  });

  try {
    await entered.promise;
    expect(fixture.publishedKeys()).toHaveLength(1);
    expect(await store.inbox()).toEqual([]);
    expect(api.wakes).toEqual([]);
  } finally {
    confirmed.resolve();
    await reply;
    await store.finish();
  }
  expect(await reply).toMatchObject({ ok: true });
});

for (const applied of [false, true])
  it(`reports upload uncertainty (applied=${applied}) without a receipt or automatic retry`, async () => {
    const store = await namespace("publication_unknown");
    const fixture = objectFixture();
    const api = submitApi(fixture, {
      objects: (ns) => {
        const objects = fixture.objects(ns);

        return {
          ...objects,
          async put(json) {
            if (applied) await objects.put(json);
            throw Error("private upload acknowledgement lost");
          },
        };
      },
    });
    const submission = {
      ns: store.ns,
      request: "one",
      command: staged(fixture, store.ns, canonical(large).json),
    };

    try {
      expect(await api.run(submission)).toEqual(unavailable);
      expect(await store.inbox()).toEqual([]);
      expect(fixture.publishedKeys()).toHaveLength(applied ? 1 : 0);
      expect(api.wakes).toEqual([]);
      expect(await submitApi(fixture).run(submission)).toMatchObject({
        ok: true,
        duplicate: false,
      });
      expect(fixture.publishedKeys()).toHaveLength(applied ? 2 : 1);
      expect(fixture.openHandles()).toBe(0);
    } finally {
      await store.finish();
    }
  });

for (const scenario of ["confirmed-abort", "missing-unique-winner"] as const)
  it(`preserves published identity after ${scenario} without automatically republishing`, async () => {
    const store = await namespace("staged_insert_fault");
    const fixture = objectFixture();
    const trace: Query[] = [];
    const cause =
      scenario === "confirmed-abort"
        ? confirmedAbort()
        : Object.assign(Error("unique violation without a visible winner"), { code: "23505" });
    const api = submitApi(fixture, {
      connectAuthority: async () =>
        (
          await traced(
            faultOnce("before", "INSERT INTO inbox", () => cause),
            trace,
          )
        ).authority,
    });

    try {
      const response = await api.run({
        ns: store.ns,
        request: "one",
        command: staged(fixture, store.ns, canonical(large).json),
      });

      expect(fixture.publishedKeys()).toHaveLength(1);
      expect(fixture.openHandles()).toBe(0);
      if (scenario === "confirmed-abort") {
        expect(response).toMatchObject({ ok: true, duplicate: false });
        expect(statements(trace, "INSERT INTO inbox")).toHaveLength(2);
        expect((await store.authority.get(store.ns, requestId("one")))?.payload).toMatchObject({
          key: fixture.publishedKeys()[0],
        });
      } else {
        expect(response).toEqual(unavailable);
        expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
        expect(api.reports[0]).toMatchObject({ phase: "receipt-write", cause });
        expect(await store.inbox()).toEqual([]);
      }
    } finally {
      await store.finish();
    }
  });

for (const applied of [false, true])
  it(`preserves receipt uncertainty (applied=${applied}) and retries without replacing a committed publication`, async () => {
    const store = await namespace("staged_receipt_unknown");
    const fixture = objectFixture();
    const trace: Query[] = [];
    const api = submitApi(fixture, {
      connectAuthority: async () =>
        (
          await traced(
            faultOnce(applied ? "after" : "before", "INSERT INTO inbox", () =>
              Error("lost receipt ACK"),
            ),
            trace,
          )
        ).authority,
    });
    const submission = {
      ns: store.ns,
      request: "one",
      command: staged(fixture, store.ns, canonical(large).json),
    };

    try {
      expect(await api.run(submission)).toEqual(unavailable);
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect(fixture.publishedKeys()).toHaveLength(1);
      expect(api.wakes).toEqual([]);
      const original = await store.authority.get(store.ns, requestId("one"));

      expect(Boolean(original)).toBe(applied);
      fixture.deleteStage(submission.command.key);
      expect(await submitApi(fixture).run(submission)).toEqual(invalidContent);
      expect(await store.authority.get(store.ns, requestId("one"))).toEqual(original);
      expect(fixture.publishedKeys()).toHaveLength(1);
      fixture.stage(store.ns, canonical(large).json);

      const retry = await submitApi(fixture).run(submission);

      expect(retry).toMatchObject({ ok: true, duplicate: applied });
      expect(fixture.publishedKeys()).toHaveLength(applied ? 1 : 2);
      if (original) expect(retry).toMatchObject({ receivedAt: original.receivedAt });
      expect(fixture.openHandles()).toBe(0);
    } finally {
      await store.finish();
    }
  });

for (const same of [true, false])
  it(`concurrent large publications keep one immutable receipt (same input=${same}) and retain loser orphans`, async () => {
    const store = await namespace("staged_race");
    const fixture = objectFixture();
    const publications = Promise.withResolvers<void>();
    let arrived = 0;
    const api = submitApi(fixture, {
      objects: (ns) => {
        const objects = fixture.objects(ns);

        return {
          ...objects,
          async put(json) {
            const ref = await objects.put(json);

            arrived++;
            if (arrived === 2) publications.resolve();
            await publications.promise;

            return ref;
          },
        };
      },
    });
    const other = canonical(same ? large : { ...large, whenBusy: "steer" });

    try {
      const replies = await Promise.all(
        [canonical(large), other].map((input, index) =>
          api.run({
            ns: store.ns,
            request: "race",
            command: staged(fixture, store.ns, input.json, `source-${index}`),
          }),
        ),
      );

      expect(replies.filter((reply) => reply.ok && !reply.duplicate)).toHaveLength(1);
      if (same) {
        expect(replies.filter((reply) => reply.ok && reply.duplicate)).toHaveLength(1);
        expect(new Set(replies.map((reply) => reply.ok && reply.receivedAt)).size).toBe(1);
      } else expect(replies).toContainEqual(conflict);
      const item = await store.authority.get(store.ns, requestId("race"));

      if (item?.payload.kind !== "s3") throw Error("Expected published receipt");
      expect(await store.inbox()).toHaveLength(1);
      const keys = fixture.publishedKeys();

      expect(keys).toHaveLength(2);
      expect(keys).toContain(item.payload.key);
      expect(
        canonical(JSON.parse(fixture.inspectPublished(item.payload.key)!.toString())).sha256,
      ).toBe(item.hash);
      for (const key of keys) expect(fixture.inspectPublished(key)).toBeDefined();
      expect(fixture.openHandles()).toBe(0);
    } finally {
      await store.finish();
    }
  });

it("stream transport failure stays unavailable, closes objects, and keeps diagnostics private", async () => {
  const store = await namespace("staged_transport");
  const fixture = objectFixture();
  const command = staged(fixture, store.ns, canonical(small).json);
  const body = Readable.from(
    (async function* () {
      yield Buffer.from("{");
      throw Error("private stream secret");
    })(),
  );
  const api = submitApi(fixture, {
    objects: (ns) => ({
      ...fixture.objects(ns),
      getStaged: async () => body,
    }),
    report: () => {
      throw Error("broken private report");
    },
  });

  try {
    const reply = await api.run({ ns: store.ns, request: "one", command });

    expect(reply).toEqual(unavailable);
    expect(api.events).toEqual([
      { name: "submit-failed", code: "unavailable", phase: "staged-read" },
    ]);
    expect(JSON.stringify([reply, api.events])).not.toMatch(/secret|private/);
    expect(body.destroyed).toBe(true);
    expect(fixture.openHandles()).toBe(0);
    expect(await store.inbox()).toEqual([]);
  } finally {
    await store.finish();
  }
});
