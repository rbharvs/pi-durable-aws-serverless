import { expect, it } from "vitest";
import { StorageRequestError, type StorageWrite } from "@earendil-works/pi-durable";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import {
  context,
  conversationId,
  dataEntry,
  historyFixture,
  memoryObjects,
} from "./history-support.ts";
import { documentCreate, documentId } from "./document-support.ts";

for (const mismatch of ["history", "fork", "key"] as const)
  it(`rejects copy ${mismatch} incoherence without another address or shape conflict`, async () => {
    const f = await historyFixture("copy_coherence");
    try {
      const source = documentCreate();
      // Both latest/current and rewindable/current are valid, isolating the history check.
      const record = { ...source.record, fork: "current" as const };
      await f.storage.commit([{ ...source, record }], context);
      const destination = {
        ...record,
        id: documentId(30),
        scope: { kind: "conversation" as const, conversationId: conversationId(2) },
      };
      const changed =
        mismatch === "history"
          ? { ...destination, history: "latest" as const }
          : mismatch === "fork"
            ? { ...destination, fork: "initial" as const }
            : { ...destination, key: "other" };
      const before = await f.snapshot();
      await expect(
        f.storage.commit(
          [{ type: "document.copy", record: changed, source: { id: record.id, at: "current" } }],
          context,
        ),
      ).rejects.toMatchObject({ cause: { message: "Copy source mismatch" } });
      expect(await f.snapshot()).toEqual(before);
      expect(
        await f.storage.commit(
          [
            {
              type: "document.copy",
              record: destination,
              source: { id: record.id, at: "current" },
            },
          ],
          context,
        ),
      ).toBe(2);
      const cold = await f.cold();
      expect((await cold.document(destination.id, "current", context))?.value).toEqual(
        source.content.value,
      );
    } finally {
      await f.finish();
    }
  });

for (const command of ["document.change", "document.retire"] as const)
  it(`rejects ${command} on an already retired incarnation, retaining its original lifetime and history`, async () => {
    const f = await historyFixture("retired_command");
    try {
      const create = documentCreate();
      const at = await f.storage.commit([create], context);
      await f.storage.commit([{ type: "document.retire", id: create.record.id }], context);
      const before = await f.snapshot();
      const write: StorageWrite =
        command === "document.change"
          ? {
              type: command,
              id: create.record.id,
              content: { kind: "base", version: 1, value: { changed: true } },
            }
          : { type: command, id: create.record.id };
      await expect(f.storage.commit([write], context)).rejects.toThrow("Document 20 is retired");
      expect(await f.snapshot()).toEqual(before);
      const cold = await f.cold();
      expect(await cold.document(create.record.id, "current", context)).toBeUndefined();
      expect((await cold.document(create.record.id, at, context))?.value).toEqual(
        create.content.value,
      );
    } finally {
      await f.finish();
    }
  });

it("an empty create-retire lifetime cannot free an address occupied by another incarnation", async () => {
  const f = await historyFixture("empty_lifetime_conflict", { objects: memoryObjects });
  try {
    const source = documentCreate();
    await f.storage.commit([source], context);
    const before = await f.snapshot();
    await expect(
      f.storage.commit(
        [
          dataEntry(40, "x".repeat(300 * 1024)),
          documentCreate(21),
          { type: "document.retire", id: documentId(21) },
          documentCreate(22),
        ],
        context,
      ),
    ).rejects.toThrow("address already has a current incarnation");
    expect(await f.snapshot()).toEqual(before);
    expect(f.memory.operations).toEqual([]);
    const cold = await f.cold();
    expect(
      (
        await cold.scanDocuments(
          { scope: source.record.scope, at: "current" },
          10,
          undefined,
          context,
        )
      ).items.map((record) => record.id),
    ).toEqual([source.record.id]);
  } finally {
    await f.finish();
  }
});

it("rejects a reversed decoded lifetime even when both endpoints are valid points and match SQL indexes", async () => {
  const f = await historyFixture("reversed_lifetime");
  try {
    const create = documentCreate();
    await f.storage.commit([create], context);
    await f.control.query("UPDATE records SET created_at=5,retired_at=4 WHERE ns=$1 AND id=20", [
      f.ns,
    ]);
    await f.control.query(
      "UPDATE inline_payloads SET json=$2 WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=20)",
      [f.ns, JSON.stringify({ ...create.record, createdAt: 5, retiredAt: 4 })],
    );
    const cold = await f.cold();
    await expect(cold.document(create.record.id, "current", context)).rejects.toBeInstanceOf(
      PayloadCorrupt,
    );
  } finally {
    await f.finish();
  }
});

it("rejects raw document write before SQL or object I/O and keeps valid document commands healthy", async () => {
  const f = await historyFixture("raw_document", { objects: memoryObjects });
  try {
    const before = await f.storage.commit([], context);
    f.trace.length = 0;
    await expect(
      f.storage.commit(
        [
          dataEntry(39, "x".repeat(300 * 1024)),
          {
            type: "document",
            value: { id: 40, kind: "raw", scope: { kind: "session" }, createdAt: 0 },
          } as unknown as StorageWrite,
        ],
        context,
      ),
    ).rejects.toBeInstanceOf(StorageRequestError);
    expect(f.trace).toEqual([]);
    expect(f.memory.operations).toEqual([]);
    expect(await f.storage.mintId()).toBe(2);
    expect(await f.storage.commit([documentCreate()], context)).toBe(before + 1);
    expect(await f.storage.document(documentId(20), "current", context)).toMatchObject({
      record: { id: 20 },
      value: { count: 1, rows: [] },
    });
  } finally {
    await f.finish();
  }
});

for (const command of ["document.create", "document.copy"] as const)
  for (const field of ["createdAt", "retiredAt"] as const)
    it(`rejects caller-supplied ${field} on ${command} before I/O`, async () => {
      const f = await historyFixture("caller_lifetime");
      try {
        await f.storage.commit([documentCreate()], context);
        const record = {
          ...documentCreate(30).record,
          scope: { kind: "conversation" as const, conversationId: conversationId(2) },
          [field]: 2,
        };
        const write: StorageWrite =
          command === "document.create"
            ? { type: command, record, content: documentCreate().content }
            : { type: command, record, source: { id: documentId(20), at: "current" } };
        const before = await f.snapshot();
        f.trace.length = 0;
        await expect(f.storage.commit([write], context)).rejects.toBeInstanceOf(
          StorageRequestError,
        );
        expect(f.trace).toEqual([]);
        expect(await f.snapshot()).toEqual(before);
      } finally {
        await f.finish();
      }
    });

it("a same-batch change keeps its incarnation current, so a create at its address is rejected", async () => {
  const f = await historyFixture("changed_occupant");
  try {
    await f.storage.commit([documentCreate()], context);
    const before = await f.snapshot();
    const writes = [
      {
        type: "document.change" as const,
        id: documentId(20),
        content: { kind: "delta" as const, version: 1, ops: [["s", ["count"], 2] as const] },
      },
      documentCreate(21),
    ];
    for (const batch of [writes, [...writes].reverse()])
      await expect(f.storage.commit(batch, context)).rejects.toThrow(
        "address already has a current incarnation",
      );
    expect(await f.snapshot()).toEqual(before);
  } finally {
    await f.finish();
  }
});
