import { expect, it } from "vitest";
import { MemoryStorage, StorageRequestError, type StorageWrite } from "@earendil-works/pi-durable";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import {
  context,
  historyFixture,
  memoryObjects,
  queryGate,
  dataEntry,
  rootId,
} from "./history-support.ts";
import { documentCreate, documentId } from "./document-support.ts";

for (const corruption of [
  "base value",
  "base version",
  "delta value",
  "delta version",
  "missing base",
] as const)
  it(`rejects corrupt revision ${corruption} rather than returning absence or a partial value`, async () => {
    const f = await historyFixture("revision_corrupt");
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
      if (corruption === "base version" || corruption === "delta version")
        await f.control.query("UPDATE revisions SET version=$2 WHERE ns=$1 AND kind=$3", [
          f.ns,
          corruption === "base version" ? "NaN" : "2",
          corruption === "base version" ? "base" : "delta",
        ]);
      else if (corruption === "missing base")
        await f.control.query("UPDATE revisions SET kind='delta' WHERE ns=$1 AND kind='base'", [
          f.ns,
        ]);
      else
        await f.control.query(
          "UPDATE inline_payloads SET json=$2 WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM revisions WHERE ns=$1 AND kind=$3)",
          [
            f.ns,
            corruption === "base value" ? "[]" : '[["unknown",[]]]',
            corruption === "base value" ? "base" : "delta",
          ],
        );
      const cold = await f.cold();
      await expect(cold.document(documentId(20), "current", context)).rejects.toBeInstanceOf(
        PayloadCorrupt,
      );
    } finally {
      await f.finish();
    }
  });

it("reads only the selected checkpoint and its tail while retaining older object revisions", async () => {
  const f = await historyFixture("revision_projection", { objects: memoryObjects });
  try {
    const create = documentCreate();
    const at = await f.storage.commit(
      [
        {
          ...create,
          content: { kind: "base", version: 1, value: { body: "x".repeat(300 * 1024) } },
        },
      ],
      context,
    );
    const old = f.memory.operations.find((op) => op.operation === "PUT")!;
    await f.storage.commit(
      [
        {
          type: "document.change",
          id: documentId(20),
          content: { kind: "base", version: 2, value: { current: true } },
        },
      ],
      context,
    );
    f.memory.bodies.set(old.key, Buffer.from("corrupt"));
    f.memory.operations.length = 0;
    const cold = await f.cold();
    expect((await cold.document(documentId(20), "current", context))?.value).toEqual({
      current: true,
    });
    expect(f.memory.operations).toEqual([]);
    await expect(cold.document(documentId(20), at, context)).rejects.toThrow();
    expect(f.memory.operations.map((op) => op.key)).toEqual([old.key]);
    expect(
      await f.control.query("SELECT count(*)::integer AS count FROM revisions WHERE ns=$1", [f.ns]),
    ).toEqual([{ count: 2 }]);
  } finally {
    await f.finish();
  }
});

it("freezes document commands before waiting for a gate, using the same normalized JSON for address, IDs and content", async () => {
  const gate = queryGate("after", (sql) => sql.includes("FROM records"));
  const f = await historyFixture("document_detached", { hook: gate.hook });
  try {
    gate.arm();
    const read = f.storage.conversation(rootId, context);
    await gate.reached;
    const write = documentCreate();
    const commit = f.storage.commit([write], context);
    write.record.key = "changed";
    write.content.value.count = 99;
    gate.release();
    await Promise.all([read, commit]);
    expect((await f.storage.findDocument(documentCreate().record, "current", context))?.id).toBe(
      20,
    );
    expect((await f.storage.document(documentId(20), "current", context))?.value.count).toBe(1);
    const address = documentCreate().record;
    gate.arm();
    const blocking = f.storage.conversation(rootId, context);
    await gate.reached;
    const finding = f.storage.findDocument(address, "current", context);
    address.key = "later";
    gate.release();
    await blocking;
    expect((await finding)?.id).toBe(20);
  } finally {
    gate.release();
    await f.finish();
  }
});

for (const invalid of [
  {
    type: "document.create",
    record: { ...documentCreate().record, id: "20" },
    content: documentCreate().content,
  },
  {
    type: "document.create",
    record: documentCreate().record,
    content: { kind: "delta", version: 1, ops: [] },
  },
  {
    type: "document.create",
    record: documentCreate().record,
    content: { kind: "base", version: Infinity, value: {} },
  },
  { type: "document.change", id: 20, content: { kind: "delta", version: 1, ops: [["bad"]] } },
  { type: "document.copy", record: documentCreate().record, source: { id: 20, at: -1 } },
  { type: "document.retire", id: 0 },
])
  it(`rejects invalid normalized ${JSON.stringify(invalid)} before SQL or object I/O`, async () => {
    const f = await historyFixture("document_invalid", { objects: memoryObjects });
    try {
      f.trace.length = 0;
      await expect(
        f.storage.commit([dataEntry(40, "x".repeat(300 * 1024)), invalid as StorageWrite], context),
      ).rejects.toBeInstanceOf(StorageRequestError);
      expect(f.trace).toEqual([]);
      expect(f.memory.operations).toEqual([]);
      expect(f.storage.status).toBe("open");
      expect(await f.storage.commit([documentCreate()], context)).toBe(1);
    } finally {
      await f.finish();
    }
  });

for (const version of [-1.25, 0, 2.5])
  it(`round-trips raw public StorageWrite version ${version} like MemoryStorage, without imposing defineDoc policy`, async () => {
    const f = await historyFixture("document_raw_version");
    const memory = new MemoryStorage();
    try {
      const create: StorageWrite = {
        ...documentCreate(),
        content: { kind: "base", version, value: { count: 1 } },
      };
      const delta: StorageWrite = {
        type: "document.change",
        id: documentId(20),
        content: { kind: "delta", version, ops: [["s", ["count"], 2]] },
      };
      for (const storage of [memory, f.storage]) {
        await storage.commit([create], context);
        await storage.commit([delta], context);
      }
      const cold = await f.cold();
      expect(await cold.document(documentId(20), "current", context)).toEqual(
        await memory.document(documentId(20), "current", context),
      );
    } finally {
      await memory.close(context);
      await f.finish();
    }
  });
