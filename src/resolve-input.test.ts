import { expect, it } from "vitest";
import { canonical, MAX_INPUT_BYTES } from "./input.ts";
import { computeSha256, harnessId, requestId } from "./domain.ts";
import { resolveInput } from "./submit.ts";
import type { Received } from "./authority.ts";
import { objectFixture } from "../tests/shared/objects.ts";

const ns = harnessId("resolve");
const receipt = (json: string): Received => ({
  kind: "received",
  ns,
  request: requestId("one"),
  receivedAt: 123,
  hash: computeSha256(json),
  payload: { kind: "inline", json },
});

it("resolves canonical inline input without reading an object", async () => {
  const input = canonical({ content: "hello", whenBusy: "followUp" });
  await expect(
    resolveInput(receipt(input.json), {
      get: async () => {
        throw Error("unexpected object read");
      },
    }),
  ).resolves.toEqual(input.input);
});

for (const [label, json] of [
  ["noncanonical JSON", '{ "content":"hello","whenBusy":"followUp"}'],
  ["invalid command", '{"content":"hello","whenBusy":"reject"}'],
  [
    "oversize input",
    JSON.stringify({ content: "x".repeat(MAX_INPUT_BYTES), whenBusy: "followUp" }),
  ],
] as const)
  it(`rejects ${label} even with a matching receipt digest`, async () => {
    await expect(
      resolveInput(receipt(json), {
        get: async () => {
          throw Error("unexpected read");
        },
      }),
    ).rejects.toThrow();
  });

it("rejects inline content whose receipt digest differs", async () => {
  const input = canonical({ content: "hello", whenBusy: "followUp" });
  await expect(
    resolveInput({ ...receipt(input.json), hash: computeSha256("other") }, { get: async () => "" }),
  ).rejects.toThrow("Inbox payload integrity");
});

for (const forged of ["namespace", "key", "bytes"] as const)
  it(`rejects forged published ${forged} from a custom authority before object I/O`, async () => {
    const f = objectFixture(),
      store = f.objects(ns);
    const input = canonical({ content: "x".repeat(300000), whenBusy: "followUp" });
    let reads = 0;
    try {
      const payload = await store.put(input.json);
      const item: Received = { ...receipt(input.json), payload };
      const invalid: Received = {
        ...item,
        ns: forged === "namespace" ? harnessId("other") : ns,
        payload: {
          ...payload,
          key: forged === "key" ? (`p/${ns}/not-a-uuid` as typeof payload.key) : payload.key,
          bytes: forged === "bytes" ? MAX_INPUT_BYTES + 1 : payload.bytes,
        },
      };
      // A custom Authority/reader need not enforce SQL Authority's metadata checks.
      await expect(
        resolveInput(invalid, {
          get: async () => {
            reads++;
            return input.json;
          },
        }),
      ).rejects.toThrow(
        forged === "bytes" ? "Inbox object bounds" : "Foreign namespace or invalid published key",
      );
      expect(reads).toBe(0);
    } finally {
      store.close();
    }
  });

it("checks durable object bytes against both receipt and publication identities", async () => {
  const f = objectFixture(),
    store = f.objects(ns);
  const input = canonical({ content: "x".repeat(300000), whenBusy: "followUp" });
  try {
    const payload = await store.put(input.json);
    const item = { ...receipt(input.json), payload };
    await expect(resolveInput(item, store)).resolves.toEqual(input.input);
    await expect(resolveInput({ ...item, hash: computeSha256("other") }, store)).rejects.toThrow(
      "Inbox payload integrity",
    );
    // Fault-inject a reader returning the correct content with wrong declared object metadata.
    await expect(
      resolveInput(
        { ...item, payload: { ...payload, bytes: payload.bytes + 1 } },
        { get: () => store.get(payload) },
      ),
    ).rejects.toThrow("Inbox object integrity");
    await expect(
      resolveInput(
        { ...item, payload: { ...payload, sha256: computeSha256("other") } },
        { get: () => store.get(payload) },
      ),
    ).rejects.toThrow("Inbox object integrity");
  } finally {
    store.close();
  }
  expect(f.openHandles()).toBe(0);
});
