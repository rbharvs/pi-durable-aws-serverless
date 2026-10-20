import { expect, it } from "vitest";
import {
  StorageRequestError,
  type Cursor,
  type EntryQuery,
  type ScanOrder,
  type TaskId,
} from "@earendil-works/pi-durable";
import { computeSha256 } from "../../src/domain.ts";
import { idFromNumber } from "../../src/ids.ts";
import { BEGIN_READ } from "../../src/sql/db.ts";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import {
  context,
  conversation,
  conversationId,
  entry,
  entryId,
  historyFixture,
  queryGate,
} from "./history-support.ts";

it("round-trips complete raw records, detached values, immutable IDs and commit sequences after reopen", async () => {
  const f = await historyFixture("history_roundtrip");
  const data = { nested: ["original", null, true], unicode: "\ud800 \u0000 🚀 __proto__" };
  const write = entry(20, {
    kind: "message",
    data,
    model: [{ role: "user", content: [{ type: "text", text: "hello" }], timestamp: 123 }],
    edits: [{ target: entryId(10), action: "omit" }],
    head: entryId(10),
    byTaskId: idFromNumber<TaskId>(500),
  });
  const original = structuredClone(write.value);

  try {
    const pending = f.storage.commit([conversation(), entry(10), write], context);

    data.nested[0] = "caller changed immediately";
    const seq = await pending;
    const result = await f.storage.entry(entryId(20), context);

    expect(result).toEqual({ entry: original, commitSeq: seq });
    (result!.entry.data as { nested: unknown[] }).nested[0] = "reader changed result";
    expect(await f.storage.entry(entryId(20), context)).toEqual({
      entry: original,
      commitSeq: seq,
    });
    expect(await f.storage.conversation(conversationId(1), context)).toEqual(conversation().value);
    const before = await f.snapshot();

    for (const replacement of [entry(20, { kind: "replacement" }), conversation(20)])
      await expect(f.storage.commit([entry(1000), replacement], context)).rejects.toThrow();
    expect(await f.snapshot()).toEqual(before);
    expect(f.storage.status).toBe("open");
    await f.storage.close(context);
    const reopened = await f.open();

    expect(await reopened.storage.entry(entryId(20), context)).toEqual({
      entry: original,
      commitSeq: seq,
    });
    expect(await reopened.storage.conversation(conversationId(999), context)).toBeUndefined();
    expect(await reopened.storage.entry(entryId(999), context)).toBeUndefined();
    expect(await reopened.storage.commit([entry(30)], context)).toBe(seq + 1);
    expect((await reopened.storage.entry(entryId(30), context))?.commitSeq).toBe(seq + 1);
  } finally {
    await f.finish();
  }
});

it("nested forks inherit inclusive cutoffs, not later ancestor writes; pages and head markers agree", async () => {
  const f = await historyFixture("fork_cutoffs");

  try {
    await f.storage.commit(
      [
        conversation(),
        entry(10, { head: entryId(10) }),
        entry(20, { head: entryId(20) }),
        entry(30, { head: entryId(30) }),
        conversation(40, { parent: { conversationId: conversationId(1), at: entryId(20) } }),
        entry(50, { conversationId: conversationId(40), head: entryId(10) }),
        entry(60, { conversationId: conversationId(40), head: entryId(60) }),
        conversation(70, { parent: { conversationId: conversationId(40), at: entryId(50) } }),
        entry(80, { conversationId: conversationId(70), head: entryId(50) }),
        entry(90, { conversationId: conversationId(70) }),
      ],
      context,
    );
    await f.storage.commit(
      [
        entry(100, { head: entryId(100) }),
        entry(110, { conversationId: conversationId(40), head: entryId(110) }),
      ],
      context,
    );
    await f.storage.close(context);
    const { storage } = await f.open();
    const scope = conversationId(70);
    const visible = [10, 20, 50, 80, 90];

    for (const order of ["ascending", "descending"] as const) {
      const ids: number[] = [];
      let cursor: Cursor | undefined;

      for (let pages = 0; ; pages++) {
        expect(pages).toBeLessThan(4);
        const query: EntryQuery = cursor
          ? { conversationId: scope }
          : { conversationId: scope, order };
        const page = await storage.scanEntries(query, 2, cursor, context);

        ids.push(...page.items.map((r) => r.id));
        if (!page.next) break;
        cursor = JSON.parse(JSON.stringify(page.next)) as Cursor;
      }
      expect(ids).toEqual(order === "ascending" ? visible : [...visible].reverse());
    }
    expect(
      (await storage.scanEntries({ conversationId: scope }, 20, undefined, context)).items.map(
        (r) => r.id,
      ),
    ).toEqual([...visible].reverse());
    expect(
      (
        await storage.scanEntries(
          {
            conversationId: scope,
            minEntryId: entryId(20),
            maxEntryId: entryId(80),
            order: "ascending",
          },
          20,
          undefined,
          context,
        )
      ).items.map((r) => r.id),
    ).toEqual([20, 50, 80]);
    for (const id of [10, 20, 50, 80, 90])
      expect((await storage.entry(scope, entryId(id), context))?.entry.id).toBe(id);
    for (const id of [30, 60, 100, 110]) {
      expect(await storage.entry(scope, entryId(id), context)).toBeUndefined();
      expect((await storage.entry(entryId(id), context))?.entry.id).toBe(id);
    }
    for (const [cutoff, expected] of [
      [undefined, 80],
      [80, 80],
      [79, 50],
      [50, 50],
      [20, 20],
      [9, undefined],
    ] as const)
      expect(
        (
          await storage.findLatestHeadMarker(
            scope,
            cutoff === undefined ? undefined : entryId(cutoff),
            context,
          )
        )?.id,
      ).toBe(expected);
    expect((await storage.findLatestHeadMarker(scope, undefined, context))?.head).toBe(50);
    await expect(
      storage.scanEntries({ conversationId: conversationId(999) }, 2, undefined, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    await expect(storage.entry(conversationId(999), entryId(10), context)).rejects.toBeInstanceOf(
      StorageRequestError,
    );
  } finally {
    await f.finish();
  }
});

it("conversation owner filters and JSON cursor order survive reopening without skipped or repeated rows", async () => {
  const f = await historyFixture("conversation_pages");
  const owner = { conversationId: conversationId(1), taskId: idFromNumber<TaskId>(100) };

  try {
    await f.storage.commit(
      [
        conversation(),
        conversation(2, { owner }),
        conversation(3, { owner }),
        conversation(4, { owner: { ...owner, taskId: idFromNumber<TaskId>(101) } }),
        conversation(5),
      ],
      context,
    );
    const first = await f.storage.scanConversations(
      { ownerTaskId: owner.taskId, order: "descending" },
      1,
      undefined,
      context,
    );

    expect(first.items.map((r) => r.id)).toEqual([3]);
    expect(first.next).toBeDefined();
    await f.storage.close(context);
    const { storage } = await f.open();
    const cursor = JSON.parse(JSON.stringify(first.next)) as Cursor;
    const second = await storage.scanConversations(
      { ownerTaskId: owner.taskId },
      1,
      cursor,
      context,
    );

    expect(second.items.map((r) => r.id)).toEqual([2]);
    expect(second.next).toBeUndefined();
    expect(
      (
        await storage.scanConversations(
          { ownerConversationId: owner.conversationId },
          10,
          undefined,
          context,
        )
      ).items.map((r) => r.id),
    ).toEqual([2, 3, 4]);
    expect(
      (
        await storage.scanConversations(
          { ownerConversationId: owner.conversationId, ownerTaskId: idFromNumber<TaskId>(101) },
          10,
          undefined,
          context,
        )
      ).items.map((r) => r.id),
    ).toEqual([4]);
    await expect(
      storage.scanConversations({ order: "ascending" }, 1, cursor, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
    expect(storage.status).toBe("open");
  } finally {
    await f.finish();
  }
});

for (const field of [
  "conversationId",
  "order",
  "minEntryId",
  "maxEntryId",
  "cursor.after",
  "cursor.order",
] as const)
  it(`scanEntries detaches ${field} at invocation, before a held read releases the gate`, async () => {
    const gate = queryGate("after", (sql) => sql === BEGIN_READ);
    const f = await historyFixture("entry_scan_detach", { hook: gate.hook });

    try {
      await f.storage.commit(
        [conversation(), conversation(2), entry(10), entry(20), entry(30), entry(40), entry(50)],
        context,
      );
      const query = {
        conversationId: conversationId(1),
        order: "ascending" as ScanOrder,
        minEntryId: entryId(20),
        maxEntryId: entryId(50),
      };
      const first = await f.storage.scanEntries(query, 1, undefined, context);

      expect(first.items.map((r) => r.id)).toEqual([20]);
      expect(first.next).toBeDefined();
      const cursor = { ...first.next };

      gate.arm();
      const held = f.storage.conversation(conversationId(1), context);

      await gate.reached;
      const beforeScan = f.trace.length;
      const pending = f.storage.scanEntries(query, 2, cursor, context);

      expect(f.trace).toHaveLength(beforeScan);
      // Change one input at a time: each would independently change the page or reject it.
      switch (field) {
        case "conversationId":
          query.conversationId = conversationId(2);
          break;
        case "order":
          query.order = "descending";
          break;
        case "minEntryId":
          query.minEntryId = entryId(40);
          break;
        case "maxEntryId":
          query.maxEntryId = entryId(30);
          break;
        case "cursor.after":
          cursor.after = 40;
          break;
        case "cursor.order":
          cursor.order = "descending";
          break;
      }
      const result = Promise.all([held, pending]);

      gate.release();
      const [record, page] = await result;

      expect(record).toEqual(conversation().value);
      expect(page.items.map((r) => r.id)).toEqual([30, 40]);
      expect(page.next).toBeDefined();
      expect(
        (
          await f.storage.scanEntries(
            { conversationId: conversationId(1), minEntryId: entryId(20), maxEntryId: entryId(50) },
            2,
            page.next,
            context,
          )
        ).items.map((r) => r.id),
      ).toEqual([50]);
    } finally {
      gate.release();
      await f.finish();
    }
  });

for (const field of [
  "ownerConversationId",
  "ownerTaskId",
  "order",
  "cursor.after",
  "cursor.order",
] as const)
  it(`scanConversations detaches ${field} at invocation, before a held read releases the gate`, async () => {
    const gate = queryGate("after", (sql) => sql === BEGIN_READ);
    const f = await historyFixture("conversation_scan_detach", { hook: gate.hook });
    const owner = { conversationId: conversationId(1), taskId: idFromNumber<TaskId>(100) };

    try {
      await f.storage.commit(
        [
          conversation(),
          conversation(2, { owner }),
          conversation(3, { owner }),
          conversation(4, { owner }),
          conversation(5, { owner }),
          conversation(6, { owner: { ...owner, taskId: idFromNumber<TaskId>(101) } }),
          conversation(7, { owner: { ...owner, conversationId: conversationId(8) } }),
          conversation(8),
        ],
        context,
      );
      const query = {
        ownerConversationId: owner.conversationId,
        ownerTaskId: owner.taskId,
        order: "descending" as ScanOrder,
      };
      const first = await f.storage.scanConversations(query, 1, undefined, context);

      expect(first.items.map((r) => r.id)).toEqual([5]);
      expect(first.next).toBeDefined();
      const cursor = { ...first.next };

      gate.arm();
      const held = f.storage.conversation(conversationId(1), context);

      await gate.reached;
      const beforeScan = f.trace.length;
      const pending = f.storage.scanConversations(query, 2, cursor, context);

      expect(f.trace).toHaveLength(beforeScan);
      switch (field) {
        case "ownerConversationId":
          query.ownerConversationId = conversationId(8);
          break;
        case "ownerTaskId":
          query.ownerTaskId = idFromNumber<TaskId>(101);
          break;
        case "order":
          query.order = "ascending";
          break;
        case "cursor.after":
          cursor.after = 3;
          break;
        case "cursor.order":
          cursor.order = "ascending";
          break;
      }
      const result = Promise.all([held, pending]);

      gate.release();
      const [record, page] = await result;

      expect(record).toEqual(conversation().value);
      expect(page.items.map((r) => r.id)).toEqual([4, 3]);
      expect(page.next).toBeDefined();
      expect(
        (
          await f.storage.scanConversations(
            { ownerConversationId: owner.conversationId, ownerTaskId: owner.taskId },
            2,
            page.next,
            context,
          )
        ).items.map((r) => r.id),
      ).toEqual([2]);
    } finally {
      gate.release();
      await f.finish();
    }
  });

for (const scan of ["entries", "conversations"] as const)
  it(`a queued ${scan} scan preserves an originally invalid cursor even if the caller repairs it`, async () => {
    const gate = queryGate("after", (sql) => sql === BEGIN_READ);
    const f = await historyFixture("invalid_cursor_detach", { hook: gate.hook });

    try {
      await f.storage.commit([conversation(), entry(20)], context);
      const cursor = { after: "invalid" as string | number, order: "ascending" as ScanOrder };
      const query = { conversationId: conversationId(1), order: "ascending" as ScanOrder };

      gate.arm();
      const held = f.storage.conversation(conversationId(1), context);

      await gate.reached;
      const pending = (
        scan === "entries"
          ? f.storage.scanEntries(query, 2, cursor, context)
          : f.storage.scanConversations(query, 2, cursor, context)
      ).catch((error: unknown) => error);

      cursor.after = 0;
      gate.release();
      await held;
      expect(await pending).toBeInstanceOf(StorageRequestError);
      expect(f.storage.status).toBe("open");
    } finally {
      gate.release();
      await f.finish();
    }
  });

for (const scan of ["entries", "conversations"] as const)
  for (const source of ["query", "cursor"] as const)
    it(`${scan} scan returns a rejected Promise, not a synchronous throw, for a failing ${source} getter`, async () => {
      const f = await historyFixture("scan_getter_rejection");
      const cause = Error(`caller ${source} getter failed`);
      const query =
        source === "query"
          ? {
              conversationId: conversationId(1),
              get order(): ScanOrder {
                throw cause;
              },
            }
          : { conversationId: conversationId(1) };
      const cursor =
        source === "cursor"
          ? {
              get after(): number {
                throw cause;
              },
            }
          : undefined;

      try {
        const before = f.trace.length;
        let pending: Promise<unknown> | undefined;

        expect(() => {
          pending =
            scan === "entries"
              ? f.storage.scanEntries(query, 2, cursor, context)
              : f.storage.scanConversations(query, 2, cursor, context);
        }).not.toThrow();
        await expect(pending).rejects.toBe(cause);
        expect(f.trace).toHaveLength(before);
        expect(f.storage.status).toBe("open");
      } finally {
        await f.finish();
      }
    });

const invalidCursors: Cursor[] = [{ after: "10" }, { after: 1.5 }, { after: 2, order: "sideways" }];

for (const cursor of invalidCursors)
  it(`rejects malformed entry and conversation cursors ${JSON.stringify(cursor)} without changing durable state`, async () => {
    const f = await historyFixture("bad_cursor");

    try {
      await f.storage.commit([conversation(), entry(10)], context);
      const before = await f.snapshot();

      await expect(
        f.storage.scanEntries({ conversationId: conversationId(1) }, 2, cursor, context),
      ).rejects.toBeInstanceOf(StorageRequestError);
      await expect(f.storage.scanConversations({}, 2, cursor, context)).rejects.toBeInstanceOf(
        StorageRequestError,
      );
      expect(await f.snapshot()).toEqual(before);
      expect(f.storage.status).toBe("open");
    } finally {
      await f.finish();
    }
  });

it("entry cursor order is retained when omitted and an explicit opposite order is rejected", async () => {
  const f = await historyFixture("entry_cursor_order");

  try {
    await f.storage.commit([conversation(), entry(10), entry(20), entry(30)], context);
    const first = await f.storage.scanEntries(
      { conversationId: conversationId(1), order: "ascending" },
      1,
      undefined,
      context,
    );

    expect(first.items.map((r) => r.id)).toEqual([10]);
    expect(first.next).toBeDefined();
    expect(
      (
        await f.storage.scanEntries({ conversationId: conversationId(1) }, 10, first.next, context)
      ).items.map((r) => r.id),
    ).toEqual([20, 30]);
    await expect(
      f.storage.scanEntries(
        { conversationId: conversationId(1), order: "descending" },
        1,
        first.next,
        context,
      ),
    ).rejects.toBeInstanceOf(StorageRequestError);
  } finally {
    await f.finish();
  }
});

const corruptions = [
  ["entry conversation index", "UPDATE records SET conversation_id=999 WHERE ns=$1 AND id=20"],
  [
    "entry kind index",
    `UPDATE records SET kind_hash='${computeSha256(JSON.stringify("other"))}' WHERE ns=$1 AND id=20`,
  ],
  ["head index", "UPDATE records SET has_head=true WHERE ns=$1 AND id=20"],
  [
    "payload identity",
    "UPDATE inline_payloads SET json=jsonb_set(json::jsonb,'{id}','999')::text WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=20)",
  ],
  [
    "malformed JSON",
    "UPDATE inline_payloads SET json='{' WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=20)",
  ],
  [
    "missing payload",
    "DELETE FROM inline_payloads WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=20)",
  ],
] as const;

for (const [name, sql] of corruptions)
  it(`rejects ${name} on global and scoped entry reads without masking corruption as absence`, async () => {
    const f = await historyFixture("history_corrupt");

    try {
      await f.storage.commit([conversation(), entry(20)], context);
      await f.control.query(sql, [f.ns]);
      const before = await f.snapshot();

      await expect(f.storage.entry(entryId(20), context)).rejects.toThrow(`${f.ns}/records/20`);
      await expect(f.storage.entry(conversationId(1), entryId(20), context)).rejects.toThrow(
        `${f.ns}/records/20`,
      );
      expect(await f.snapshot()).toEqual(before);
    } finally {
      await f.finish();
    }
  });

for (const [parent, message] of [
  [1, "Conversation ancestry cycle"],
  [999, "Unknown conversation"],
] as const)
  it(`rejects corrupt ancestry (${message}) instead of returning a partial history`, async () => {
    const f = await historyFixture("ancestry_corrupt");

    try {
      await f.storage.commit([conversation(), entry(20)], context);
      await f.control.query(
        "UPDATE inline_payloads SET json=jsonb_set(json::jsonb,'{parent}',$2::jsonb)::text WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=1)",
        [f.ns, JSON.stringify({ conversationId: parent, at: 20 })],
      );
      const before = await f.snapshot();

      await expect(
        f.storage.scanEntries({ conversationId: conversationId(1) }, 10, undefined, context),
      ).rejects.toThrow(message);
      await expect(f.storage.entry(conversationId(1), entryId(20), context)).rejects.toThrow(
        message,
      );
      await expect(
        f.storage.findLatestHeadMarker(conversationId(1), undefined, context),
      ).rejects.toThrow(message);
      expect((await f.storage.entry(entryId(20), context))?.entry.id).toBe(20);
      expect(await f.snapshot()).toEqual(before);
    } finally {
      await f.finish();
    }
  });

it("corrupt conversation owners are rejected by both direct reads and scans", async () => {
  const f = await historyFixture("owner_corrupt");

  try {
    await f.storage.commit([conversation()], context);
    await f.control.query("UPDATE records SET owner_task_id=50 WHERE ns=$1 AND id=1", [f.ns]);
    await expect(f.storage.conversation(conversationId(1), context)).rejects.toThrow(
      `${f.ns}/records/1`,
    );
    await expect(f.storage.scanConversations({}, 10, undefined, context)).rejects.toThrow(
      `${f.ns}/records/1`,
    );
  } finally {
    await f.finish();
  }
});

it("history reads, allocation and cleanup stay within their exact namespace", async () => {
  const a = await historyFixture("namespace_a");
  let aFinished = false;
  let b: Awaited<ReturnType<typeof historyFixture>> | undefined;

  try {
    b = await historyFixture("namespace_b");
    await a.storage.commit([conversation(), entry(20, { data: "a" })], context);
    await b.storage.commit([conversation(), entry(20, { data: "b" }), entry(30)], context);
    expect((await a.storage.entry(entryId(20), context))?.entry.data).toBe("a");
    expect((await b.storage.entry(entryId(20), context))?.entry.data).toBe("b");
    expect(await a.storage.entry(entryId(30), context)).toBeUndefined();
    expect(
      (
        await a.storage.scanEntries({ conversationId: conversationId(1) }, 10, undefined, context)
      ).items.map((r) => r.id),
    ).toEqual([20]);
    const before = await b.snapshot();

    expect(await a.storage.mintId()).toBe(21);
    expect(await b.snapshot()).toEqual(before);
    aFinished = true;
    await a.finish();
    expect(await b.snapshot()).toEqual(before);
    expect((await b.storage.entry(entryId(20), context))?.entry.data).toBe("b");
  } finally {
    try {
      if (!aFinished) await a.finish();
    } finally {
      await b?.finish();
    }
  }
});

it("validates a complete batch with one bounded ownership query before publication", async () => {
  const f = await historyFixture("batch_ownership");
  const ids = Array.from({ length: 100 }, (_, i) => 20 + i);
  try {
    f.trace.length = 0;
    await f.storage.commit([conversation(), ...ids.map((id) => entry(id))], context);
    const lookups = f.trace.filter(
      (q) => q.sql.startsWith("SELECT") && q.sql.includes("FROM records"),
    );
    expect(lookups).toHaveLength(1);
    expect(lookups[0]!.acknowledged).toBe(true);
    expect(lookups[0]!.params).toEqual([f.ns, 1, ...ids]);
    expect(
      (
        await f.storage.scanEntries(
          { conversationId: conversationId(1), order: "ascending" },
          100,
          undefined,
          context,
        )
      ).items.map((e) => e.id),
    ).toEqual(ids);
  } finally {
    await f.finish();
  }
});

const corruptJsonNumbers = [
  { name: "conversation ID string", type: "conversation", path: "{id}", value: "30" },
  {
    name: "owner conversation string",
    type: "conversation",
    path: "{owner,conversationId}",
    value: "1",
  },
  { name: "owner task string", type: "conversation", path: "{owner,taskId}", value: "50" },
  {
    name: "parent conversation string",
    type: "conversation",
    path: "{parent,conversationId}",
    value: "1",
  },
  {
    name: "parent conversation zero",
    type: "conversation",
    path: "{parent,conversationId}",
    value: 0,
  },
  { name: "parent cutoff string", type: "conversation", path: "{parent,at}", value: "10" },
  { name: "entry ID string", type: "entry", path: "{id}", value: "40" },
  { name: "entry conversation string", type: "entry", path: "{conversationId}", value: "30" },
  { name: "entry head string", type: "entry", path: "{head}", value: "10" },
] as const;

for (const corruption of corruptJsonNumbers)
  it(`classifies durable ${corruption.name} metadata as PayloadCorrupt, not a bad request`, async () => {
    const f = await historyFixture("json_numeric_corrupt");
    try {
      await f.storage.commit(
        [
          conversation(),
          entry(10),
          conversation(30, {
            parent: { conversationId: conversationId(1), at: entryId(10) },
            owner: { conversationId: conversationId(1), taskId: idFromNumber<TaskId>(50) },
          }),
          entry(40, { conversationId: conversationId(30), head: entryId(10) }),
        ],
        context,
      );
      const id = corruption.type === "conversation" ? 30 : 40;
      await f.control.query(
        "UPDATE inline_payloads SET json=jsonb_set(json::jsonb,$3::text[],$4::jsonb)::text WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM records WHERE ns=$1 AND id=$2)",
        [f.ns, id, corruption.path, JSON.stringify(corruption.value)],
      );
      const before = await f.snapshot();
      const direct = () =>
        corruption.type === "conversation"
          ? f.storage.conversation(conversationId(30), context)
          : f.storage.entry(entryId(40), context);
      for (const read of [
        direct,
        () => f.storage.entry(conversationId(30), entryId(40), context),
        () => f.storage.scanEntries({ conversationId: conversationId(30) }, 10, undefined, context),
        () => f.storage.findLatestHeadMarker(conversationId(30), undefined, context),
      ]) {
        const result = read();
        await expect(result).rejects.toBeInstanceOf(PayloadCorrupt);
        await expect(result).rejects.not.toBeInstanceOf(StorageRequestError);
      }
      expect(await f.snapshot()).toEqual(before);
      expect(f.storage.status).toBe("open");
    } finally {
      await f.finish();
    }
  });
