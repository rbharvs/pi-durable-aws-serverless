import { expect, it } from "vitest";
import { StorageRequestError, type StorageWrite } from "@earendil-works/pi-durable";
import { seqFromNumber } from "../../src/ids.ts";
import { EnvelopeRejected } from "../../src/payload.ts";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import {
  context,
  conversation,
  dataEntry,
  entryId,
  historyFixture,
  memoryObjects,
  queryGate,
} from "./history-support.ts";
import { documentCreate, documentId } from "./document-support.ts";
import { task, taskId, submission } from "./task-support.ts";

it("document creation advances the shared ID head to the exact exhausted sentinel without returning it", async () => {
  const f = await historyFixture("document_max_id");
  try {
    const create = documentCreate(Number.MAX_SAFE_INTEGER);
    await f.storage.commit([create], context);
    const cold = await f.cold();
    expect((await cold.document(create.record.id, "current", context))?.value.count).toBe(1);
    expect((await f.head()).next_id).toBe("9007199254740992");
    await expect(cold.mintId()).rejects.toThrow("ID space is exhausted");
  } finally {
    await f.finish();
  }
});

it("retains base/delta histories, replaces addresses atomically, and copies independently after cold reopen", async () => {
  const f = await historyFixture("document_history", { objects: memoryObjects });
  try {
    const created = documentCreate();
    const first = await f.storage.commit([conversation(), created], context);
    const second = await f.storage.commit(
      [
        {
          type: "document.change",
          id: created.record.id,
          content: { kind: "delta", version: 1, ops: [["s", ["count"], 2]] },
        },
      ],
      context,
    );
    const third = await f.storage.commit(
      [
        {
          type: "document.change",
          id: created.record.id,
          content: { kind: "base", version: -1.25, value: { count: 3 } },
        },
      ],
      context,
    );
    await expect(
      f.storage.commit(
        [
          {
            type: "document.change",
            id: created.record.id,
            content: { kind: "delta", version: 1, ops: [] },
          },
        ],
        context,
      ),
    ).rejects.toThrow("version transition requires a base");
    const copy = {
      ...created.record,
      id: documentId(21),
      scope: { kind: "conversation" as const, conversationId: conversation(2).value.id },
    };
    await f.storage.commit(
      [{ type: "document.copy", record: copy, source: { id: created.record.id, at: second } }],
      context,
    );
    const replacement = documentCreate(22);
    const retired = await f.storage.commit(
      [replacement, { type: "document.retire", id: created.record.id }],
      context,
    );
    const cold = await f.cold();
    expect((await cold.document(created.record.id, first, context))?.value).toEqual({
      count: 1,
      rows: [],
    });
    expect(await cold.document(created.record.id, second, context)).toMatchObject({
      value: { count: 2 },
      version: 1,
      deltasSinceBase: 1,
    });
    expect(await cold.document(created.record.id, third, context)).toMatchObject({
      value: { count: 3 },
      version: -1.25,
      deltasSinceBase: 0,
    });
    expect(await cold.document(created.record.id, retired, context)).toBeUndefined();
    expect(await cold.document(created.record.id, "current", context)).toBeUndefined();
    expect((await cold.findDocument(created.record, second, context))?.id).toBe(created.record.id);
    expect((await cold.findDocument(created.record, retired, context))?.id).toBe(
      replacement.record.id,
    );
    expect((await cold.document(copy.id, "current", context))?.value).toEqual({
      count: 2,
      rows: [],
    });
    expect(
      await f.control.query(
        "SELECT count(*)::integer AS count FROM revisions WHERE ns=$1 AND document_id=$2",
        [f.ns, created.record.id],
      ),
    ).toEqual([{ count: 3 }]);
  } finally {
    await f.finish();
  }
});

it("materializes a base and tail from one repeatable snapshot across a concurrent committed update", async () => {
  const gate = queryGate(
    "after",
    (sql) => sql.includes("FROM revisions") && sql.includes("kind='base'"),
  );
  const f = await historyFixture("materialize_snapshot", { hook: gate.hook });
  try {
    await f.storage.commit([documentCreate()], context);
    const writer = await f.open();
    gate.arm();
    const reading = f.storage.document(documentId(20), "current", context);
    try {
      await gate.reached;
      await writer.storage.commit(
        [
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "delta", version: 1, ops: [["s", ["count"], 99]] },
          },
        ],
        context,
      );
    } finally {
      gate.release();
    }
    expect((await reading)?.value).toEqual({ count: 1, rows: [] });
    expect((await f.storage.document(documentId(20), "current", context))?.value).toEqual({
      count: 99,
      rows: [],
    });
  } finally {
    gate.release();
    await f.finish();
  }
});

it("validates copy source and ordinary writes in the same snapshot, then fences concurrent changes", async () => {
  const gate = queryGate(
    "after",
    (sql) => sql.includes("FROM revisions") && sql.includes("kind='base'"),
  );
  const f = await historyFixture("copy_snapshot", { hook: gate.hook, objects: memoryObjects });
  try {
    await f.storage.commit([documentCreate(), task(25)], context);
    const writer = await f.open();
    gate.arm();
    const batch = f.storage.commit(
      [
        task(30),
        dataEntry(40, "x".repeat(300 * 1024)),
        {
          type: "document.copy",
          record: {
            ...documentCreate(21).record,
            scope: { kind: "conversation", conversationId: conversation(2).value.id },
          },
          source: { id: documentId(20), at: "current" },
        },
      ],
      context,
    );
    const rejection = expect(batch).rejects.toThrow("Stale");
    let after;
    try {
      await gate.reached;
      await writer.storage.commit(
        [
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "base", version: 1, value: { concurrent: true } },
          },
        ],
        context,
      );
      after = await f.snapshot();
    } finally {
      gate.release();
    }
    await rejection;
    expect(await f.snapshot()).toEqual(after);
    const cold = await f.cold();
    expect(await cold.task(taskId(30), context)).toBeUndefined();
    expect(await cold.document(documentId(21), "current", context)).toBeUndefined();
  } finally {
    gate.release();
    await f.finish();
  }
});

it("publishes repeated tasks with entries and revisions atomically, and rejects a changed copy source before uploading", async () => {
  const f = await historyFixture("document_atomic", { objects: memoryObjects });
  try {
    await f.storage.commit([documentCreate(), task(25)], context);
    const before = await f.snapshot();
    await expect(
      f.storage.commit(
        [
          dataEntry(40, "x".repeat(300 * 1024)),
          task(20),
          task(20),
          {
            type: "document.change",
            id: documentId(20),
            content: { kind: "base", version: 1, value: {} },
          },
        ],
        context,
      ),
    ).rejects.toThrow();
    expect(await f.snapshot()).toEqual(before);
    expect(f.memory.operations).toEqual([]);
    const copy = {
      type: "document.copy" as const,
      record: {
        ...documentCreate(21).record,
        scope: { kind: "conversation" as const, conversationId: conversation(2).value.id },
      },
      source: { id: documentId(20), at: "current" as const },
    };
    await expect(
      f.storage.commit(
        [
          dataEntry(40, "x".repeat(300 * 1024)),
          copy,
          { type: "document.retire", id: documentId(20) },
        ],
        context,
      ),
    ).rejects.toThrow("source is changed");
    expect(await f.snapshot()).toEqual(before);
    expect(f.memory.operations).toEqual([]);
    await f.storage.commit(
      [
        task(30),
        task(30, { status: "terminal", outcome: { status: "completed", result: null } }),
        dataEntry(40, { atomic: true }),
        {
          type: "document.change",
          id: documentId(20),
          content: { kind: "delta", version: 1, ops: [["s", ["count"], 8]] },
        },
      ],
      context,
    );
    const cold = await f.cold();
    expect((await cold.task(taskId(30), context))?.state.status).toBe("terminal");
    expect((await cold.entry(entryId(40), context))?.entry.data).toEqual({ atomic: true });
    expect((await cold.document(documentId(20), "current", context))?.value.count).toBe(8);
  } finally {
    await f.finish();
  }
});

for (const command of [
  "document.create",
  "document.copy",
  "document.change",
  "document.retire",
] as const) {
  for (const owner of ["task", "entry", "conversation", "submission"] as const) {
    it(`rejects ${command} reusing an existing ${owner} ID without mutation`, async () => {
      const f = await historyFixture("document_global_id");
      try {
        const id = documentId(30);
        const existing: StorageWrite =
          owner === "task"
            ? task(30)
            : owner === "entry"
              ? dataEntry(30, null)
              : owner === "conversation"
                ? conversation(30)
                : submission(30);
        await f.storage.commit([existing, documentCreate()], context);
        const before = await f.snapshot();
        const write: StorageWrite =
          command === "document.create"
            ? documentCreate(30, "collision")
            : command === "document.copy"
              ? {
                  type: command,
                  record: {
                    ...documentCreate(30).record,
                    scope: { kind: "conversation", conversationId: conversation(2).value.id },
                  },
                  source: { id: documentId(20), at: "current" },
                }
              : command === "document.retire"
                ? { type: command, id }
                : { type: command, id, content: { kind: "base", version: 1, value: {} } };
        await expect(f.storage.commit([write], context)).rejects.toThrow(
          command === "document.create" || command === "document.copy"
            ? `ID 30 already belongs to ${owner}`
            : "ID 30 belongs to another record type",
        );
        expect(await f.snapshot()).toEqual(before);
      } finally {
        await f.finish();
      }
    });
  }
}

for (const state of ["current", "retired"] as const) {
  for (const command of ["document.create", "document.copy"] as const) {
    it(`rejects ${command} reusing a ${state} document ID at a free address without mutation`, async () => {
      const f = await historyFixture("document_id_reuse");
      try {
        const existing = documentCreate(30, "other");
        const first = await f.storage.commit([documentCreate(20), existing], context);
        if (state === "retired") {
          await f.storage.commit([{ type: "document.retire", id: existing.record.id }], context);
        }
        const original = await f.storage.document(existing.record.id, first, context);
        expect(original).toMatchObject({
          record: { ...existing.record, createdAt: first },
          value: existing.content.value,
        });
        const write: StorageWrite =
          command === "document.create"
            ? documentCreate(30, "collision")
            : {
                type: command,
                record: {
                  ...documentCreate(30).record,
                  scope: { kind: "conversation", conversationId: conversation(2).value.id },
                },
                source: { id: documentId(20), at: "current" },
              };
        expect(await f.storage.findDocument(write.record, "current", context)).toBeUndefined();
        const before = await f.snapshot();
        await expect(f.storage.commit([write], context)).rejects.toThrow(
          /^ID 30 already belongs to document$/,
        );
        expect(await f.snapshot()).toEqual(before);
        const cold = await f.cold();
        expect(await cold.document(existing.record.id, first, context)).toEqual(original);
        expect(await cold.findDocument(existing.record, first, context)).toEqual(original!.record);
        expect(await cold.document(existing.record.id, "current", context)).toEqual(
          state === "retired" ? undefined : original,
        );
      } finally {
        await f.finish();
      }
    });
  }
}

for (const [column, value] of [
  ["id", "21"],
  ["kind_hash", "wrong"],
  ["scope_kind", "task"],
  ["scope_owner", "9"],
  ["address_hash", "wrong"],
  ["created_at", "0"],
  ["retired_at", "4"],
] as const) {
  it(`rejects document index corruption in ${column} on a cold point read`, async () => {
    const f = await historyFixture("document_index");
    try {
      await f.storage.commit([documentCreate()], context);
      await f.control.query(`UPDATE records SET ${column}=$2 WHERE ns=$1`, [f.ns, value]);
      const cold = await f.cold();
      await expect(
        cold.document(documentId(column === "id" ? 21 : 20), "current", context),
      ).rejects.toBeInstanceOf(PayloadCorrupt);
    } finally {
      await f.finish();
    }
  });
}

for (const [name, patch] of [
  ["id", { id: "20" }],
  ["createdAt", { createdAt: "1" }],
  ["retiredAt", { retiredAt: -1 }],
  ["scope kind", { scope: { kind: "wrong" } }],
  ["scope owner", { scope: { kind: "conversation", conversationId: 0 } }],
  ["kind", { kind: false }],
  ["key", { key: 2 }],
  ["history", { history: "wrong" }],
  ["fork", { fork: "wrong" }],
] as const) {
  it(`rejects corrupt document body ${name} before returning metadata or materialized values`, async () => {
    const f = await historyFixture("document_body");
    try {
      await f.storage.commit([documentCreate()], context);
      const record = await f.storage.findDocument(documentCreate().record, "current", context);
      await f.control.query(
        "UPDATE inline_payloads SET json=$2 WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=20)",
        [f.ns, JSON.stringify({ ...record, ...patch })],
      );
      const cold = await f.cold();
      await expect(cold.document(documentId(20), "current", context)).rejects.toBeInstanceOf(
        PayloadCorrupt,
      );
      await expect(
        cold.findDocument(documentCreate().record, "current", context),
      ).rejects.toBeInstanceOf(PayloadCorrupt);
      await expect(
        cold.scanDocuments(
          { scope: documentCreate().record.scope, at: "current" },
          10,
          undefined,
          context,
        ),
      ).rejects.toBeInstanceOf(PayloadCorrupt);
    } finally {
      await f.finish();
    }
  });
}

it("counts both revision and lifecycle payload owners before uploading any oversized batch", async () => {
  const f = await historyFixture("revision_budget", { objects: memoryObjects });
  try {
    const writes = Array.from({ length: 250 }, (_, i) => documentCreate(i + 20, String(i)));
    const before = await f.snapshot();
    await expect(f.storage.commit(writes, context)).rejects.toBeInstanceOf(EnvelopeRejected);
    expect(await f.snapshot()).toEqual(before);
    expect(f.memory.operations).toEqual([]);
    await f.storage.commit(writes.slice(0, 249), context);
    const cold = await f.cold();
    expect(
      (
        await cold.scanDocuments(
          { scope: writes[0]!.record.scope, at: "current" },
          300,
          undefined,
          context,
        )
      ).items,
    ).toHaveLength(249);
    expect((await cold.document(writes[248]!.record.id, "current", context))?.value.count).toBe(1);
  } finally {
    await f.finish();
  }
});

it("current-only rejects numeric content but retains historical membership and all revisions", async () => {
  const f = await historyFixture("current_only");
  try {
    const create: StorageWrite = {
      type: "document.create",
      record: { id: documentId(20), kind: "session", scope: { kind: "session" } },
      content: { kind: "base", version: 0.5, value: { n: 1 } },
    };
    const at = await f.storage.commit([create], context);
    await f.storage.commit(
      [
        {
          type: "document.change",
          id: documentId(20),
          content: { kind: "base", version: -2, value: { n: 2 } },
        },
        { type: "document.retire", id: documentId(20) },
      ],
      context,
    );
    const cold = await f.cold();
    await expect(cold.document(documentId(20), at, context)).rejects.toBeInstanceOf(
      StorageRequestError,
    );
    expect((await cold.findDocument(create.record, at, context))?.id).toBe(20);
    expect(await cold.document(documentId(20), "current", context)).toBeUndefined();
    expect(await cold.findDocument(create.record, seqFromNumber(-1), context)).toBeUndefined();
    expect(
      await f.control.query("SELECT count(*)::integer AS count FROM revisions WHERE ns=$1", [f.ns]),
    ).toEqual([{ count: 2 }]);
  } finally {
    await f.finish();
  }
});
