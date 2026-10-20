import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { computeSha256, integer } from "../../src/domain.ts";
import { LIMITS } from "../../src/limits.ts";
import { projection } from "../../src/sql/payloads.ts";
import {
  context,
  dataEntry,
  entryId,
  historyFixture,
  memoryObjects,
  rootId,
} from "./history-support.ts";

it("cold-opens inline and S3 history from persisted references, fetching only S3 payloads", async () => {
  const f = await historyFixture("payload_cold", { objects: memoryObjects });
  let cold: Awaited<ReturnType<typeof f.cold>> | undefined;

  try {
    const writes = [dataEntry(20, { text: "inline 🚀" }), dataEntry(21, "x".repeat(300 * 1024))];

    await f.storage.commit([{ type: "conversation", value: { id: rootId } }, ...writes], context);
    const before = await f.snapshot();
    const put = f.memory.operations.filter((q) => q.operation === "PUT");

    expect(put).toHaveLength(1);
    f.memory.operations.length = 0;
    cold = await f.cold();
    expect(await cold.conversation(rootId, context)).toEqual({ id: rootId });
    expect((await cold.entry(entryId(20), context))?.entry).toEqual(writes[0]!.value);
    expect(f.memory.operations).toEqual([]);
    expect((await cold.entry(entryId(21), context))?.entry).toEqual(writes[1]!.value);
    expect(f.memory.operations).toEqual([{ operation: "GET", key: put[0]!.key }]);
    expect(await f.snapshot()).toEqual(before);
  } finally {
    await cold?.close(context);
    await f.finish();
  }
});

for (const kind of ["inline", "s3"] as const)
  it(`rejects both-present ${kind} entry payloads regardless of their kind tag`, async () => {
    const f = await historyFixture("payload_both", { objects: memoryObjects });
    let ref: string | undefined;

    try {
      await f.storage.commit(
        [dataEntry(20, kind === "inline" ? "small" : "x".repeat(300 * 1024))],
        context,
      );
      expect((await f.storage.entry(entryId(20), context))?.entry.id).toBe(20);
      ref = String(
        (await f.control.query("SELECT payload_ref FROM records WHERE ns=$1 AND id=20", [f.ns]))[0]!
          .payload_ref,
      );
      if (kind === "inline")
        await f.control.query(
          "INSERT INTO s3_payloads(ns,payload_ref,object_key,sha256,bytes) VALUES($1,$2,$3,$4,2)",
          [f.ns, ref, `p/${f.ns}/${randomUUID()}`, computeSha256("{}")],
        );
      else
        await f.control.query("INSERT INTO inline_payloads(ns,payload_ref,json) VALUES($1,$2,$3)", [
          f.ns,
          ref,
          "{}",
        ]);
      f.memory.operations.length = 0;

      await expect(f.storage.entry(entryId(20), context)).rejects.toThrow(`${f.ns}/records/20`);
      expect(f.memory.operations).toEqual([]); // Do not fetch an incoherent S3 union.
    } finally {
      if (ref)
        await f.control.query(
          `DELETE FROM ${kind === "inline" ? "s3_payloads" : "inline_payloads"} WHERE ns=$1 AND payload_ref=$2`,
          [f.ns, ref],
        );
      await f.finish();
    }
  });

for (const state of [
  "missing-row",
  "kind-mismatch",
  "dangling-ref",
  "non-v4-ref",
  "bad-json",
  "oversized-inline",
] as const)
  it(`rejects an inline owner with ${state} and identifies the owner`, async () => {
    const f = await historyFixture("payload_inline_bad", { objects: memoryObjects });

    try {
      await f.storage.commit([dataEntry(20, "original")], context);
      const ref = String(
        (await f.control.query("SELECT payload_ref FROM records WHERE ns=$1 AND id=20", [f.ns]))[0]!
          .payload_ref,
      );

      if (state === "missing-row")
        await f.control.query("DELETE FROM inline_payloads WHERE ns=$1 AND payload_ref=$2", [
          f.ns,
          ref,
        ]);
      if (state === "kind-mismatch")
        await f.control.query("UPDATE records SET payload_kind='s3' WHERE ns=$1 AND id=20", [f.ns]);
      if (state === "dangling-ref" || state === "non-v4-ref")
        await f.control.query("UPDATE records SET payload_ref=$2 WHERE ns=$1 AND id=20", [
          f.ns,
          state === "dangling-ref" ? randomUUID() : "00000000-0000-0000-0000-000000000000",
        ]);
      if (state === "bad-json" || state === "oversized-inline")
        await f.control.query("UPDATE inline_payloads SET json=$3 WHERE ns=$1 AND payload_ref=$2", [
          f.ns,
          ref,
          state === "bad-json" ? "not-json" : JSON.stringify("x".repeat(LIMITS.maxInlineBytes)),
        ]);

      await expect(f.storage.entry(entryId(20), context)).rejects.toThrow(`${f.ns}/records/20`);
      expect(f.memory.operations).toEqual([]);
    } finally {
      // These tests deliberately sever owner/payload links; remove their corrupt rows explicitly.
      for (const table of ["records", "inline_payloads", "s3_payloads"])
        await f.control.query(`DELETE FROM ${table} WHERE ns=$1`, [f.ns]);
      await f.finish();
    }
  });

for (const state of [
  "missing-row",
  "kind-mismatch",
  "missing-object",
  "wrong-bytes",
  "wrong-length",
  "wrong-hash",
  "invalid-digest",
  "foreign-namespace",
  "malformed-key",
  "invalid-json",
  "invalid-utf8",
] as const)
  it(`rejects an S3 owner with ${state} and identifies the owner`, async () => {
    const f = await historyFixture("payload_s3_bad", { objects: memoryObjects });

    try {
      await f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context);
      const row = (
        await f.control.query(
          `SELECT ${projection("records")} FROM records WHERE ns=$1 AND id=20`,
          [f.ns],
        )
      )[0]!;
      const key = String(row.object_key);
      const ref = String(row.payload_ref);
      const update = (column: string, value: string | number) =>
        f.control.query(`UPDATE s3_payloads SET ${column}=$3 WHERE ns=$1 AND payload_ref=$2`, [
          f.ns,
          ref,
          value,
        ]);

      if (state === "missing-row")
        await f.control.query("DELETE FROM s3_payloads WHERE ns=$1 AND payload_ref=$2", [
          f.ns,
          ref,
        ]);
      if (state === "kind-mismatch")
        await f.control.query("UPDATE records SET payload_kind='inline' WHERE ns=$1 AND id=20", [
          f.ns,
        ]);
      if (state === "missing-object") f.memory.bodies.delete(key);
      if (state === "wrong-bytes")
        f.memory.bodies.set(key, Buffer.from("z".repeat(integer(row.bytes))));
      if (state === "wrong-length") await update("bytes", integer(row.bytes) + 1);
      if (state === "wrong-hash") await update("sha256", computeSha256("wrong"));
      if (state === "invalid-digest") await update("sha256", "BAD");
      if (state === "foreign-namespace") await update("object_key", `p/foreign/${randomUUID()}`);
      if (state === "malformed-key") await update("object_key", "not-a-reference");
      if (state === "invalid-json" || state === "invalid-utf8") {
        const body = state === "invalid-json" ? Buffer.from("not-json") : Buffer.from([0xff, 0xfe]);

        f.memory.bodies.set(key, body);
        await update("bytes", body.length);
        await update("sha256", computeSha256(body));
      }
      f.memory.operations.length = 0;

      await expect(f.storage.entry(entryId(20), context)).rejects.toThrow(`${f.ns}/records/20`);
      if (
        [
          "missing-row",
          "kind-mismatch",
          "invalid-digest",
          "foreign-namespace",
          "malformed-key",
        ].includes(state)
      )
        expect(f.memory.operations).toEqual([]);
      else expect(f.memory.operations).toEqual([{ operation: "GET", key }]);
    } finally {
      await f.finish();
    }
  });

for (const kind of ["inline", "s3"] as const)
  it(`does not decode an unselected corrupt ${kind} owner, but rejects it in the next SQL page`, async () => {
    const f = await historyFixture("payload_projection", { objects: memoryObjects });

    try {
      await f.storage.commit(
        [
          { type: "conversation", value: { id: rootId } },
          dataEntry(20, "first"),
          dataEntry(21, "lookahead"),
          dataEntry(22, kind === "inline" ? "corrupt" : "x".repeat(300 * 1024)),
        ],
        context,
      );
      const ref = String(
        (await f.control.query("SELECT payload_ref FROM records WHERE ns=$1 AND id=22", [f.ns]))[0]!
          .payload_ref,
      );

      if (kind === "inline")
        await f.control.query(
          "UPDATE inline_payloads SET json='not-json' WHERE ns=$1 AND payload_ref=$2",
          [f.ns, ref],
        );
      else
        await f.control.query(
          "UPDATE s3_payloads SET object_key='bad-key' WHERE ns=$1 AND payload_ref=$2",
          [f.ns, ref],
        );
      f.memory.operations.length = 0;
      const first = await f.storage.scanEntries(
        { conversationId: rootId, order: "ascending" },
        1,
        undefined,
        context,
      );

      expect(first.items.map((value) => value.id)).toEqual([20]);
      expect(first.next).toBeDefined();
      expect(f.memory.operations).toEqual([]);
      expect((await f.storage.entry(entryId(20), context))?.entry.data).toBe("first");
      // The reference decodes limit+1 rows. Entry22 is outside the first SQL result,
      // then enters the second query's lookahead; do not redefine that pagination contract.
      await expect(
        f.storage.scanEntries(
          { conversationId: rootId, order: "ascending" },
          1,
          first.next,
          context,
        ),
      ).rejects.toThrow(`${f.ns}/records/22`);
      expect(f.memory.operations).toEqual([]);
    } finally {
      await f.finish();
    }
  });
