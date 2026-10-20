import { expect, it } from "vitest";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { EnvelopeRejected } from "../../src/payload.ts";
import { LIMITS } from "../../src/limits.ts";
import { seqFromNumber } from "../../src/ids.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import {
  context,
  conversationId,
  dataEntry,
  entryId,
  historyFixture,
  memoryObjects,
} from "./history-support.ts";
import { documentCreate, documentId } from "./document-support.ts";
import { task, taskId } from "./task-support.ts";

const copy = (id: number): Extract<StorageWrite, { type: "document.copy" }> => ({
  type: "document.copy",
  record: {
    ...documentCreate(id).record,
    scope: { kind: "conversation", conversationId: conversationId(id) },
  },
  source: { id: documentId(20), at: "current" },
});

it("rejects four tiny commands copying 12 MiB each before any upload or write transaction; two copies remain readable", async () => {
  const f = await historyFixture("copy_logical_bound", { objects: memoryObjects });
  try {
    const value = { text: "x".repeat(12 * 1024 * 1024) };
    await f.storage.commit(
      [{ ...documentCreate(), content: { kind: "base", version: 1, value } }],
      context,
    );
    const before = await f.snapshot();
    const copies = [copy(30), copy(31), copy(32), copy(33)];
    expect(Buffer.byteLength(JSON.stringify(copies))).toBeLessThan(1024);
    f.memory.operations.length = 0;
    f.trace.length = 0;
    await expect(f.storage.commit(copies, context)).rejects.toBeInstanceOf(EnvelopeRejected);
    expect(f.memory.operations.filter((op) => op.operation === "PUT")).toEqual([]);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    // Incremental rejection must not materialize every remaining copy.
    expect(f.memory.operations.filter((op) => op.operation === "GET").length).toBeLessThan(4);
    expect(await f.storage.commit(copies.slice(0, 2), context)).toBe(2);
    const cold = await f.cold();
    for (const id of [20, 30, 31])
      expect((await cold.document(documentId(id), "current", context))?.value).toEqual(value);
    for (const id of [32, 33])
      expect(await cold.document(documentId(id), "current", context)).toBeUndefined();
  } finally {
    await f.finish();
  }
});

it("counts mixed resolved UTF-8 payloads once, including copy/retire metadata, empty lifetimes, bases and repeated task versions, at the exact 32 MiB boundary", async () => {
  const f = await historyFixture("copy_exact_bound", { objects: memoryObjects });
  try {
    const value = { text: "é".repeat(6 * 1024 * 1024) };
    const retired = documentCreate(41, "retire");
    const empty = documentCreate(42, "empty");
    const retirements: StorageWrite[] = [
      { type: "document.retire", id: retired.record.id },
      empty,
      { type: "document.retire", id: empty.record.id },
    ];
    await f.storage.commit(
      [{ ...documentCreate(), content: { kind: "base", version: 1, value } }, task(50), retired],
      context,
    );
    const copies = [copy(30), copy(31)];
    const smallBase = documentCreate(40, "mixed");
    const intermediate = {
      type: "task" as const,
      value: { ...task(50).value, input: { text: "é".repeat(1024) } },
    };
    const final = task(50, { status: "terminal", outcome: { status: "completed", result: null } });
    // Expected durable JSON payloads, not command envelopes or SQL metadata. The empty entry
    // gives the fixed overhead; its text fills the remainder with multibyte characters.
    const entry = dataEntry(60, "");
    const fixed = [
      ...copies.flatMap((c) => [{ ...c.record, createdAt: 2 }, value]),
      { ...smallBase.record, createdAt: 2 },
      smallBase.content.value,
      { ...retired.record, createdAt: 1, retiredAt: 2 },
      { ...empty.record, createdAt: 2, retiredAt: 2 },
      empty.content.value,
      intermediate.value,
      final.value,
      entry.value,
    ].reduce((n, payload) => n + Buffer.byteLength(JSON.stringify(payload)), 0);
    const remaining = LIMITS.maxLogicalCommitBytes - fixed;
    expect(remaining).toBeGreaterThan(0);
    entry.value.data = "é".repeat(Math.floor(remaining / 2)) + "x".repeat(remaining % 2);
    const batch: StorageWrite[] = [
      intermediate,
      final,
      entry,
      ...copies,
      smallBase,
      ...retirements,
    ];
    expect(Buffer.byteLength(JSON.stringify(batch))).toBeLessThan(LIMITS.maxLogicalCommitBytes);
    const before = await f.snapshot();
    f.memory.operations.length = 0;
    f.trace.length = 0;
    await expect(
      f.storage.commit(
        [
          intermediate,
          final,
          dataEntry(60, entry.value.data + "x"),
          ...copies,
          smallBase,
          ...retirements,
        ],
        context,
      ),
    ).rejects.toBeInstanceOf(EnvelopeRejected);
    expect(f.memory.operations.filter((op) => op.operation === "PUT")).toEqual([]);
    expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
    expect(await f.snapshot()).toEqual(before);
    expect(await f.storage.commit(batch, context)).toBe(2);
    const cold = await f.cold();
    expect((await cold.document(documentId(30), "current", context))?.value).toEqual(value);
    expect((await cold.document(documentId(31), "current", context))?.value).toEqual(value);
    expect((await cold.document(documentId(40), "current", context))?.value).toEqual(
      smallBase.content.value,
    );
    expect(await cold.document(retired.record.id, "current", context)).toBeUndefined();
    expect(
      (await cold.document(retired.record.id, seqFromNumber(1), context))?.record.retiredAt,
    ).toBe(2);
    expect(await cold.document(empty.record.id, "current", context)).toBeUndefined();
    expect(await cold.document(empty.record.id, seqFromNumber(2), context)).toBeUndefined();
    expect((await cold.task(taskId(50), context))?.state).toEqual(final.value.state);
    expect((await cold.entry(entryId(60), context))?.entry.data).toBe(entry.value.data);
  } finally {
    await f.finish();
  }
});
