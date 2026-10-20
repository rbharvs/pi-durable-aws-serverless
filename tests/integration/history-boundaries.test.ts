import { expect, it } from "vitest";
import {
  MemoryStorage,
  StorageRequestError,
  type ConversationId,
  type EntryId,
  type TaskId,
} from "@earendil-works/pi-durable";
import { idFromNumber } from "../../src/ids.ts";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { computeSha256, object } from "../../src/domain.ts";
import { BEGIN_READ, BEGIN_WRITE } from "../../src/sql/db.ts";
import {
  context,
  conversation,
  conversationId,
  dataEntry,
  deferred,
  entry,
  entryId,
  historyFixture,
  memoryObjects,
  type HistoryFixture,
} from "./history-support.ts";

it("checks returned object bytes itself, even when the object adapter returns unchecked text", async () => {
  const f = await historyFixture("unchecked_get", {
    objects: (_ns, objects) => ({
      ...objects,
      async get(ref) {
        const json = await objects.get(ref);
        // Same byte length and valid JSON, but not the immutable published value.
        return json.replace("original", "tampered");
      },
    }),
  });

  try {
    await f.storage.commit(
      [dataEntry(20, { text: "original", pad: "x".repeat(300 * 1024) })],
      context,
    );
    await expect(f.storage.entry(entryId(20), context)).rejects.toMatchObject({
      cause: { message: "Object payload integrity mismatch" },
    });
    expect(f.storage.status).toBe("open");
  } finally {
    await f.finish();
  }
});

for (const wrong of ["bytes", "sha256", "namespace"] as const)
  it(`rejects an upload adapter's wrong ${wrong} before write BEGIN`, async () => {
    const f = await historyFixture("unchecked_put", {
      objects: (_ns, objects) => ({
        ...objects,
        async put(json) {
          const ref = await objects.put(json);
          if (wrong === "bytes") return { ...ref, bytes: ref.bytes + 1 };
          if (wrong === "sha256") return { ...ref, sha256: computeSha256("different") };
          return { ...ref, key: ref.key.replace(/^p\/[^/]+\//, "p/foreign/") as typeof ref.key };
        },
      }),
    });

    try {
      const before = await f.snapshot();
      await expect(
        f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context),
      ).rejects.toThrow();
      expect(f.trace.filter((q) => q.sql === BEGIN_WRITE)).toEqual([]);
      expect(f.memory.bodies.size).toBe(1); // Allowed object orphan, never database history.
      expect(await f.snapshot()).toEqual(before);
    } finally {
      await f.finish();
    }
  });

it("one fork-aware read uses one snapshot even when another connection publishes during ancestry lookup", async () => {
  let armed = false;
  let other: Awaited<ReturnType<HistoryFixture["open"]>>;
  const f = await historyFixture("read_snapshot", {
    hook: async (phase, sql, params) => {
      if (armed && phase === "after" && sql.includes("AND id=$2 AND type=$3") && params[1] === 1) {
        armed = false;
        await other.storage.commit([dataEntry(20, "concurrent")], context);
      }
    },
  });

  try {
    await f.storage.commit([conversation(), dataEntry(10, "initial")], context);
    other = await f.open();
    armed = true;
    const query = { conversationId: conversationId(1), order: "ascending" as const };
    const first = await f.storage.scanEntries(query, 10, undefined, context);
    expect(armed).toBe(false);
    expect(first.items.map((e) => e.id)).toEqual([10]);
    expect(
      (await f.storage.scanEntries(query, 10, undefined, context)).items.map((e) => e.id),
    ).toEqual([10, 20]);
  } finally {
    await f.finish();
  }
});

for (const operation of ["read", "validation"] as const)
  it(`resets the connection after an unknown ${operation} BEGIN acknowledgement`, async () => {
    const failure = Error("lost read-BEGIN acknowledgement");
    let armed = true;
    const f = await historyFixture("begin_recovery", {
      hook: async (phase, sql) => {
        if (armed && phase === "after" && sql === BEGIN_READ) {
          armed = false;
          throw failure;
        }
      },
    });

    try {
      const attempt =
        operation === "read"
          ? f.storage.conversation(conversationId(1), context)
          : f.storage.commit([dataEntry(20, "not published")], context);
      await expect(attempt).rejects.toBe(failure);
      expect(f.trace.at(-1)).toMatchObject({ sql: "ROLLBACK", acknowledged: true });
      expect(f.storage.status).toBe("open");
      expect(await f.storage.mintId()).toBe(2);
      expect(await f.storage.commit([dataEntry(20, "published")], context)).toBe(1);
      expect((await f.storage.entry(entryId(20), context))?.entry.data).toBe("published");
    } finally {
      await f.finish();
    }
  });

it("keeps SQL integrity expectations when an object adapter rewrites its passed reference", async () => {
  const f = await historyFixture("get_reference_mutation", {
    objects: (_ns, objects) => ({
      ...objects,
      async get(ref) {
        const original = object(JSON.parse(await objects.get(ref)));
        const altered = JSON.stringify({ ...original, data: "y".repeat(300 * 1024 + 1) });
        Object.assign(ref, { sha256: computeSha256(altered), bytes: Buffer.byteLength(altered) });
        return altered;
      },
    }),
  });
  try {
    await f.storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context);
    const before = await f.snapshot();
    await expect(f.storage.entry(entryId(20), context)).rejects.toMatchObject({
      cause: { message: "Object payload integrity mismatch" },
    });
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
  } finally {
    await f.finish();
  }
});

const invalidWriteNumbers = [
  {
    name: "entry ID",
    write: { type: "entry", value: { ...dataEntry(30, "bad").value, id: "30" } },
  },
  {
    name: "allocation high-water ID",
    write: { type: "entry", value: { ...dataEntry(30, "bad").value, id: "1000000000000000" } },
  },
  { name: "conversation ID", write: { type: "conversation", value: { id: "30" } } },
  {
    name: "entry conversation index",
    write: { type: "entry", value: { ...dataEntry(30, "bad").value, conversationId: "1" } },
  },
  {
    name: "owner conversation index",
    write: { type: "conversation", value: { id: 30, owner: { conversationId: "1", taskId: 50 } } },
  },
  {
    name: "owner task index",
    write: { type: "conversation", value: { id: 30, owner: { conversationId: 1, taskId: "50" } } },
  },
  {
    name: "string head point",
    write: { type: "entry", value: { ...dataEntry(30, "bad").value, head: "10" } },
  },
  {
    name: "string parent conversation ID",
    write: { type: "conversation", value: { id: 30, parent: { conversationId: "1", at: 10 } } },
  },
  {
    name: "zero parent conversation ID",
    write: { type: "conversation", value: { id: 30, parent: { conversationId: 0, at: 10 } } },
  },
  {
    name: "string fork point",
    write: { type: "conversation", value: { id: 30, parent: { conversationId: 1, at: "10" } } },
  },
  ...[-1, 1.5, Number.MAX_SAFE_INTEGER + 1].flatMap((point) => [
    {
      name: `invalid head point ${point}`,
      write: { type: "entry", value: { ...dataEntry(30, "bad").value, head: point } },
    },
    {
      name: `invalid fork point ${point}`,
      write: { type: "conversation", value: { id: 30, parent: { conversationId: 1, at: point } } },
    },
  ]),
  ...[0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, NaN, Infinity].map((id) => ({
    name: `non-positive or unsafe ID ${id}`,
    write: { type: "entry", value: { ...dataEntry(30, "bad").value, id } },
  })),
];

for (const { name, write } of invalidWriteNumbers)
  it(`rejects ${name} before allocation arithmetic, SQL or object publication`, async () => {
    const f = await historyFixture("numeric_index_boundary", { objects: memoryObjects });
    try {
      const before = await f.snapshot();
      f.trace.length = 0;
      await expect(
        f.storage.commit([dataEntry(20, "x".repeat(300 * 1024)), write as StorageWrite], context),
      ).rejects.toBeInstanceOf(StorageRequestError);
      expect(f.trace).toEqual([]);
      expect(f.memory.operations).toEqual([]);
      expect(await f.snapshot()).toEqual(before);
      expect(f.storage.status).toBe("open");
      expect(await f.storage.mintId()).toBe(2);
    } finally {
      await f.finish();
    }
  });

it("does not let PostgreSQL coerce numeric-string lookup IDs or filters", async () => {
  const f = await historyFixture("strict_lookups");
  try {
    await f.storage.commit(
      [
        conversation(),
        dataEntry(10, "valid"),
        conversation(30, {
          owner: { conversationId: conversationId(1), taskId: idFromNumber<TaskId>(50) },
        }),
      ],
      context,
    );
    const before = await f.snapshot();
    const invalidConversation = "1" as unknown as ConversationId;
    const invalidEntry = "10" as unknown as EntryId;
    const invalidTask = "50" as unknown as TaskId;
    await expect(f.storage.conversation(invalidConversation, context)).rejects.toBeInstanceOf(
      StorageRequestError,
    );
    await expect(f.storage.entry(invalidEntry, context)).rejects.toBeInstanceOf(
      StorageRequestError,
    );
    await expect(f.storage.entry(invalidConversation, entryId(10), context)).rejects.toBeInstanceOf(
      StorageRequestError,
    );
    await expect(
      f.storage.scanEntries({ conversationId: invalidConversation }, 10, undefined, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(
      f.storage.scanEntries(
        { conversationId: conversationId(1), minEntryId: invalidEntry },
        10,
        undefined,
        context,
      ),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(
      f.storage.scanEntries(
        { conversationId: conversationId(1), maxEntryId: invalidEntry },
        10,
        undefined,
        context,
      ),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(
      f.storage.scanConversations({ ownerTaskId: invalidTask }, 10, undefined, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(
      f.storage.scanConversations(
        { ownerConversationId: invalidConversation },
        10,
        undefined,
        context,
      ),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(
      f.storage.findLatestHeadMarker(invalidConversation, undefined, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(
      f.storage.findLatestHeadMarker(conversationId(1), invalidEntry, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    expect((await f.storage.entry(entryId(10), context))?.entry.data).toBe("valid");
  } finally {
    await f.finish();
  }
});

it("a known stale publication fails the handle before queued work can upload again", async () => {
  const reached = deferred(),
    proceed = deferred();
  const f = await historyFixture("stale_publication", {
    hook: async (phase, sql) => {
      if (phase === "before" && sql.startsWith("UPDATE harness SET next_seq")) {
        reached.resolve();
        await proceed.promise;
      }
    },
    objects: memoryObjects,
  });
  let operation: Promise<unknown> | undefined;
  try {
    await f.control.query("UPDATE harness SET epoch=epoch+1 WHERE ns=$1", [f.ns]);
    const before = await f.snapshot();
    operation = f.storage
      .commit([dataEntry(20, "x".repeat(300 * 1024))], context)
      .catch((error: unknown) => error);
    await reached.promise;
    const queued = Promise.allSettled([
      f.storage.commit([dataEntry(21, "y".repeat(300 * 1024))], context),
      f.storage.entry(entryId(20), context),
    ]);
    proceed.resolve();
    expect(await operation).toMatchObject({ message: "Stale owner epoch or sequence" });
    expect(
      (await queued).every((r) => r.status === "rejected" && /failed/.test(String(r.reason))),
    ).toBe(true);
    expect(f.storage.status).toBe("failed");
    const sqlCount = f.trace.length;
    await expect(
      f.storage.commit([dataEntry(22, "z".repeat(300 * 1024))], context),
    ).rejects.toThrow("failed");
    expect(f.trace).toHaveLength(sqlCount);
    expect(f.memory.operations.filter((q) => q.operation === "PUT")).toHaveLength(1);
    expect(await f.snapshot()).toEqual(before);
  } finally {
    proceed.resolve();
    await operation;
    await f.finish();
  }
});

for (const point of [0, Number.MAX_SAFE_INTEGER])
  it(`stores numeric head/fork point ${point} and reads it after reopen like public MemoryStorage`, async () => {
    const f = await historyFixture("metadata_points");
    const memory = new MemoryStorage();
    const writes: StorageWrite[] = [
      conversation(),
      entry(10, { head: entryId(point) }),
      entry(20),
      conversation(30, { parent: { conversationId: conversationId(1), at: entryId(point) } }),
      entry(40, { conversationId: conversationId(30) }),
    ];
    try {
      await f.storage.commit(writes, context);
      await memory.commit(writes, context);
      await f.storage.close(context);
      const reopened = await f.open();
      for (const store of [reopened.storage, memory]) {
        expect(await store.conversation(conversationId(30), context)).toEqual(
          conversation(30, { parent: { conversationId: conversationId(1), at: entryId(point) } })
            .value,
        );
        const marker = await store.findLatestHeadMarker(conversationId(1), undefined, context);
        expect(marker?.head).toBe(point);
        const rootPage = await store.scanEntries(
          {
            conversationId: conversationId(1),
            minEntryId: marker!.head,
            maxEntryId: entryId(20),
          },
          10,
          undefined,
          context,
        );
        expect(rootPage.items.map((e) => e.id)).toEqual(point === 0 ? [20, 10] : []);
        expect(
          (
            await store.scanEntries({ conversationId: conversationId(30) }, 10, undefined, context)
          ).items.map((e) => e.id),
        ).toEqual(point === 0 ? [40] : [40, 20, 10]);
      }
      expect(await reopened.storage.mintId()).toBe(41);
    } finally {
      await memory.close(context);
      await f.finish();
    }
  });

for (const selector of [0, -5])
  it(`safe numeric selector ${selector} matches public MemoryStorage without a positive floor`, async () => {
    const f = await historyFixture("read_selectors");
    const memory = new MemoryStorage();
    const writes: StorageWrite[] = [
      conversation(),
      entry(10),
      entry(20, { head: entryId(10) }),
      conversation(30, {
        owner: { conversationId: conversationId(1), taskId: idFromNumber<TaskId>(50) },
      }),
    ];
    try {
      await f.storage.commit(writes, context);
      await memory.commit(writes, context);
      const id = entryId(selector),
        conv = conversationId(selector);
      for (const store of [f.storage, memory]) {
        expect(await store.conversation(conv, context)).toBeUndefined();
        expect(await store.entry(id, context)).toBeUndefined();
        expect(await store.entry(conversationId(1), id, context)).toBeUndefined();
        expect(await store.findLatestHeadMarker(conversationId(1), id, context)).toBeUndefined();
        for (const order of ["ascending", "descending"] as const) {
          const lower = await store.scanEntries(
            { conversationId: conversationId(1), minEntryId: id, order },
            10,
            undefined,
            context,
          );
          expect(lower.items.map((e) => e.id)).toEqual(order === "ascending" ? [10, 20] : [20, 10]);
          expect(
            (
              await store.scanEntries(
                { conversationId: conversationId(1), maxEntryId: id, order },
                10,
                undefined,
                context,
              )
            ).items,
          ).toEqual([]);
        }
        expect(
          (await store.scanConversations({ ownerConversationId: conv }, 10, undefined, context))
            .items,
        ).toEqual([]);
        expect(
          (
            await store.scanConversations(
              { ownerTaskId: idFromNumber<TaskId>(selector) },
              10,
              undefined,
              context,
            )
          ).items,
        ).toEqual([]);
        await expect(store.entry(conv, entryId(10), context)).rejects.toThrow(
          `Unknown conversation: ${selector}`,
        );
        await expect(
          store.scanEntries({ conversationId: conv }, 10, undefined, context),
        ).rejects.toThrow(`Unknown conversation: ${selector}`);
        await expect(store.findLatestHeadMarker(conv, undefined, context)).rejects.toThrow(
          `Unknown conversation: ${selector}`,
        );
      }
      expect(f.storage.status).toBe("open");
    } finally {
      await memory.close(context);
      await f.finish();
    }
  });
