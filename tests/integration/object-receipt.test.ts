import { expect, it } from "vitest";
import { computeSha256, requestId } from "../../src/domain.ts";
import { canonical, MAX_INPUT_BYTES, MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import type { AcceptedInput } from "../../src/authority.ts";
import { objectFixture } from "../shared/objects.ts";
import { withChangingProperty } from "../shared/input.ts";
import { namespace, traced } from "./postgres.ts";

const input = canonical({ content: "x".repeat(MAX_INLINE_INPUT_BYTES), whenBusy: "followUp" });

for (const [name, changes] of [
  ["different nested hash", { sha256: computeSha256("different") }],
  ["malformed nested hash", { sha256: "BAD" }],
  ["foreign namespace", { key: "p/foreign/00000000-0000-4000-8000-000000000001" }],
  ["staging key", { key: "in/foreign/input" }],
  ["malformed publication key", { key: "not-a-key" }],
  ["inline-sized object", { bytes: MAX_INLINE_INPUT_BYTES }],
  ["canonical overflow", { bytes: MAX_INPUT_BYTES + 1 }],
  ["string byte count", { bytes: String(input.bytes) }],
  ["fractional byte count", { bytes: input.bytes + 0.5 }],
] as const)
  it(`raw authority rejects ${name} before SQL`, async () => {
    const store = await namespace("object_reject");
    const fixture = objectFixture();
    const objects = fixture.objects(store.ns);
    const payload = await objects.put(input.json);
    const { authority, trace } = await traced();

    try {
      const accepted = { hash: input.sha256, payload: { ...payload, ...changes } } as AcceptedInput;

      await expect(authority.receive(store.ns, requestId("one"), accepted)).rejects.toThrow();
      expect(trace).toEqual([]);
      expect(await store.inbox()).toEqual([]);
    } finally {
      await objects.close();
      await authority.close();
      await store.finish();
    }
  });

it("raw authority snapshots object identity before awaits and reads the kind only once", async () => {
  const store = await namespace("object_snapshot");
  const fixture = objectFixture();
  const objects = fixture.objects(store.ns);
  const payload = await objects.put(input.json);
  const caller = { ...payload };
  const source = withChangingProperty(caller, "kind", { first: "s3", later: "inline" });
  const { authority } = await traced(async (phase) => {
    if (phase !== "before") return;
    caller.key = "in/foreign/mutated" as typeof caller.key;
    caller.bytes = 1;
    caller.sha256 = computeSha256("mutated");
  });

  try {
    const reply = await authority.receive(store.ns, requestId("one"), {
      hash: input.sha256,
      payload: source,
    });

    expect(reply.item.payload).toEqual(payload);
    expect(await store.authority.get(store.ns, requestId("one"))).toEqual(reply.item);
    expect(fixture.inspectPublished(payload.key)?.toString()).toBe(input.json);
  } finally {
    await objects.close();
    await authority.close();
    await store.finish();
  }
});

for (const [name, assignment] of [
  ["foreign namespace", "object_key='p/foreign/00000000-0000-4000-8000-000000000001'"],
  ["staging key", "object_key='in/foreign/input'"],
  ["invalid key shape", "object_key='p/bad'"],
  ["invalid digest", "payload_sha256='BAD'"],
] as const)
  it(`rejects stored object metadata with ${name} on reads and duplicate attempts`, async () => {
    const store = await namespace("object_corrupt");
    const fixture = objectFixture();
    const objects = fixture.objects(store.ns);
    const payload = await objects.put(input.json);
    const accepted = { hash: input.sha256, payload };

    try {
      await store.authority.receive(store.ns, requestId("one"), accepted);
      await store.authority.inspectionDb.query(`UPDATE inbox SET ${assignment} WHERE ns=$1`, [
        store.ns,
      ]);
      const corrupt = await store.inbox();

      await expect(store.authority.get(store.ns, requestId("one"))).rejects.toThrow();
      await expect(store.authority.receive(store.ns, requestId("one"), accepted)).rejects.toThrow();
      expect(await store.inbox()).toEqual(corrupt);
    } finally {
      await objects.close();
      await store.finish();
    }
  });

it("SQL rejects incoherent object/inline unions and enforces canonical object bounds", async () => {
  const store = await namespace("object_constraints");
  const fixture = objectFixture();
  const objects = fixture.objects(store.ns);
  const payload = await objects.put(input.json);
  const db = store.authority.inspectionDb;

  try {
    await store.authority.receive(store.ns, requestId("one"), { hash: input.sha256, payload });
    const original = await store.inbox();

    for (const assignment of [
      "object_key=NULL",
      "object_bytes=NULL",
      `object_bytes=${MAX_INLINE_INPUT_BYTES}`,
      `object_bytes=${MAX_INPUT_BYTES + 1}`,
      "object_bytes=0",
      "json='{}'",
      "payload_kind='unexpected'",
      "payload_kind='inline'",
    ]) {
      await expect(
        db.query(`UPDATE inbox SET ${assignment} WHERE ns=$1`, [store.ns]),
      ).rejects.toMatchObject({ code: "23514" });
      expect(await store.inbox()).toEqual(original);
    }
  } finally {
    await objects.close();
    await store.finish();
  }
});
