import { expect, it } from "vitest";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import { context, historyFixture } from "./history-support.ts";
import { documentCreate, documentId } from "./document-support.ts";

for (const root of [[], null, 7]) {
  it(`rejects a non-object root ${JSON.stringify(root)} at an intermediate stored revision even if a later delta restores an object`, async () => {
    const f = await historyFixture("invalid_root_tail");
    try {
      await f.storage.commit([documentCreate()], context);
      await f.storage.commit(
        [
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "delta", version: 1, ops: [["r", root]] },
          },
        ],
        context,
      );
      await f.storage.commit(
        [
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "delta", version: 1, ops: [["r", { restored: true }]] },
          },
        ],
        context,
      );
      const cold = await f.cold();
      await expect(cold.document(documentId(20), "current", context)).rejects.toBeInstanceOf(
        PayloadCorrupt,
      );
    } finally {
      await f.finish();
    }
  });
}

it("rejects decoded nonfinite delta data rather than returning a non-JSON value", async () => {
  const f = await historyFixture("nonfinite_tail");
  try {
    await f.storage.commit([documentCreate()], context);
    await f.storage.commit(
      [
        {
          type: "document.change",
          id: documentId(20),
          content: { kind: "delta", version: 1, ops: [["s", ["count"], 2]] },
        },
      ],
      context,
    );
    // JSON.parse permits numeric overflow; validate the new delta data, not the entire result.
    await f.control.query(
      "UPDATE inline_payloads SET json=$2 WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM revisions WHERE ns=$1 AND kind='delta')",
      [f.ns, '[["s",["count"],1e999]]'],
    );
    const cold = await f.cold();
    await expect(cold.document(documentId(20), "current", context)).rejects.toBeInstanceOf(
      PayloadCorrupt,
    );
  } finally {
    await f.finish();
  }
});

it("replays a long tail across object root replacement, preserves historical values, and detaches returned values", async () => {
  const f = await historyFixture("long_valid_tail");
  try {
    const initial = {
      rows: Array.from({ length: 2000 }, (_, i) => ({
        i,
        text: "x".repeat(20),
        nested: [1, 2, 3],
      })),
    };
    await f.storage.commit(
      [{ ...documentCreate(), content: { kind: "base", version: 1, value: initial } }],
      context,
    );
    let expected = structuredClone(initial);
    for (let i = 0; i < 64; i++) {
      await f.storage.commit(
        [
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "delta", version: 1, ops: [["s", ["rows", i, "i"], -(i + 1)]] },
          },
        ],
        context,
      );
      expected.rows[i]!.i = -(i + 1);
    }
    const beforeReplacement = structuredClone(expected);
    const historical = await f.storage.commit([], context);
    expected = { rows: [{ i: 100, text: "replacement", nested: [9] }] };
    await f.storage.commit(
      [
        {
          type: "document.change",
          id: documentId(20),
          content: { kind: "delta", version: 1, ops: [["r", expected]] },
        },
      ],
      context,
    );
    for (let i = 0; i < 64; i++) {
      await f.storage.commit(
        [
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "delta", version: 1, ops: [["a", ["rows", 0, "text"], "."]] },
          },
        ],
        context,
      );
      expected.rows[0]!.text += ".";
    }
    const cold = await f.cold();
    expect((await cold.document(documentId(20), historical, context))?.value).toEqual(
      beforeReplacement,
    );
    const current = await cold.document(documentId(20), "current", context);
    expect(current).toMatchObject({ value: expected, deltasSinceBase: 129 });
    current!.value.rows = [];
    expect((await cold.document(documentId(20), "current", context))?.value).toEqual(expected);
  } finally {
    await f.finish();
  }
});
