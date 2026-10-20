import { describe, it, expect } from "vitest";
import {
  StorageRequestError,
  type Storage,
  type TaskId,
  type StorageWrite,
  type ConversationId,
  type EntryId,
  type SubmissionId,
  type DocumentId,
  type Seq,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { idFromNumber } from "../../src/ids.ts";
import { check, requestId } from "../../src/domain.ts";
import { input, publishSubmission, type Factory } from "./storage.contract.ts";

export function boundaries(name: string, make: Factory) {
  describe(`${name}/observable boundaries`, () => {
    for (const tag of ["document", "unknown", undefined])
      it(`rejects raw write tag ${String(tag)} without publication, allocation or sequence changes`, async () => {
        const f = await make(),
          s = await f.backend.openStorage(f.lease);
        try {
          const before = await s.commit([], context);
          const result = await s
            .commit(
              [
                { type: "conversation", value: { id: idFromNumber<ConversationId>(39) } },
                {
                  type: tag,
                  value: { id: 40, kind: "raw", scope: { kind: "session" }, createdAt: 0 },
                } as unknown as StorageWrite,
              ],
              context,
            )
            .then(
              (seq) => ({ seq }),
              (error: unknown) => ({ error }),
            );
          const document = await s.document(idFromNumber<DocumentId>(40), "current", context);
          const conversation = await s.conversation(idFromNumber<ConversationId>(39), context);
          const documents = await s.scanDocuments(
            { scope: { kind: "session" }, at: "current" },
            10,
            undefined,
            context,
          );
          const minted = await s.mintId<DocumentId>();
          // A legitimate document command must still publish and decode the stored document shape.
          const after = await s.commit(
            [
              {
                type: "document.create",
                record: {
                  id: minted,
                  kind: "valid",
                  scope: { kind: "session" },
                },
                content: { kind: "base", version: 1, value: { healthy: true } },
              },
            ],
            context,
          );
          const observed = { result, document, conversation, documents, minted, after };
          expect(
            observed,
            JSON.stringify({
              ...observed,
              result: "error" in result ? String(result.error) : result,
            }),
          ).toEqual({
            result: { error: expect.any(StorageRequestError) },
            document: undefined,
            conversation: undefined,
            documents: { items: [] },
            minted: f.firstId,
            after: before + 1,
          });
          expect(await s.document(minted, "current", context)).toMatchObject({
            record: { id: minted, kind: "valid", createdAt: after },
            value: { healthy: true },
          });
        } finally {
          await s.close(context);
          await f.finish();
        }
      });
    it("raw document rejection precedes queueing even when close immediately seals the handle", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      try {
        const result = s
          .commit(
            [
              {
                type: "document",
                value: {
                  id: 40,
                  kind: "raw",
                  scope: { kind: "session" },
                  createdAt: 0,
                },
              } as unknown as StorageWrite,
            ],
            context,
          )
          .catch((error: unknown) => error);
        await s.close(context);
        expect(await result).toBeInstanceOf(StorageRequestError);
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    it("receipt responses and scans cannot mutate the original payload, identity or timestamp", async () => {
      const f = await make();
      try {
        const first = (await f.authority.receive(f.ns, requestId("detached"), input)).item;
        const original = structuredClone(first);
        Object.assign(first, { receivedAt: 0, request: "changed", hash: "invalid" });
        Object.assign(first.payload, { json: "changed" });
        const scan = await f.authority.received(f.ns);
        Object.assign(scan[0]!, { receivedAt: 1 });
        Object.assign(scan[0]!.payload, { json: "changed again" });
        const got = await f.authority.get(f.ns, requestId("detached"));
        expect(got).toEqual(original);
        check(got, "stored receipt");
        Object.assign(got, { receivedAt: 2, request: "get-mutated", hash: "invalid" });
        Object.assign(got.payload, { json: "get-mutated" });
        expect(await f.authority.get(f.ns, requestId("detached"))).toEqual(original);
        await f.advanceAuthorityClock(5);
        expect(await f.authority.receive(f.ns, requestId("detached"), input)).toEqual({
          duplicate: true,
          item: original,
        });
      } finally {
        await f.finish();
      }
    });
    it("unminted document copies reserve IDs through retirement, rejected batches and takeover", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      let cold: Storage | undefined;
      const record = (id: number, conversation = 10) => ({
        id: idFromNumber<DocumentId>(id),
        kind: "allocator",
        scope: {
          kind: "conversation" as const,
          conversationId: idFromNumber<ConversationId>(conversation),
        },
        history: "rewindable" as const,
        fork: "asOf" as const,
      });
      const content = { kind: "base" as const, version: 1, value: { retained: true } };
      const copy = (id: number) => ({
        type: "document.copy" as const,
        record: record(id, 11),
        source: { id: idFromNumber<DocumentId>(40), at: "current" as const },
      });
      try {
        await s.commit(
          [
            { type: "conversation", value: { id: idFromNumber<ConversationId>(10) } },
            { type: "conversation", value: { id: idFromNumber<ConversationId>(11) } },
            { type: "document.create", record: record(40), content },
          ],
          context,
        );
        expect(await s.mintId()).toBe(41);
        // Preparing a high-ID copy must not reserve anything if a later command fails.
        await expect(
          s.commit(
            [copy(1000), { type: "document.retire", id: idFromNumber<DocumentId>(999) }],
            context,
          ),
        ).rejects.toThrow();
        expect(
          await s.document(idFromNumber<DocumentId>(1000), "current", context),
        ).toBeUndefined();
        expect(await s.mintId()).toBe(42);
        // An empty-lifetime copy still consumes its explicit ID, even without a prior mint.
        const seq = await s.commit(
          [copy(80), { type: "document.retire", id: idFromNumber<DocumentId>(80) }],
          context,
        );
        await s.close(context);
        await f.expire();
        const claimed = await f.authority.claim(f.ns, 45000);
        check(claimed.kind === "claimed", "takeover");
        cold = await f.backend.openStorage(claimed.lease);
        expect(await cold.mintId()).toBe(81);
        expect(
          await cold.document(idFromNumber<DocumentId>(80), "current", context),
        ).toBeUndefined();
        for (const write of [
          copy(80),
          { type: "document.create" as const, record: record(80, 11), content },
        ])
          await expect(cold.commit([write], context)).rejects.toThrow();
        expect(await cold.mintId()).toBe(82);
        expect(await cold.commit([], context)).toBe(seq + 1);
        expect(await cold.document(idFromNumber<DocumentId>(40), "current", context)).toMatchObject(
          { value: { retained: true } },
        );
      } finally {
        await cold?.close(context);
        await s.close(context);
        await f.finish();
      }
    });
    it("exhaustion sentinel survives close/reopen without wrapping or poisoning reads and commits", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      const id = idFromNumber<ConversationId>(Number.MAX_SAFE_INTEGER);
      let cold: Storage | undefined;
      try {
        await s.commit([{ type: "conversation", value: { id } }], context);
        await expect(s.mintId()).rejects.toThrow("ID space is exhausted");
        await s.close(context);
        cold = await f.backend.openStorage(f.lease);
        await expect(cold.mintId()).rejects.toThrow("ID space is exhausted");
        expect(await cold.conversation(id, context)).toEqual({ id });
        await expect(cold.commit([], context)).resolves.toBeGreaterThan(0);
        await expect(cold.mintId()).rejects.toThrow("ID space is exhausted");
      } finally {
        await cold?.close(context);
        await s.close(context);
        await f.finish();
      }
    });
    it("each Pi live task/submission state alone is discoverable, terminal states are not", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      try {
        await f.expire();
        const task = {
          id: idFromNumber<TaskId<null>>(30),
          conversationId: idFromNumber<ConversationId>(1),
          kind: "background",
          version: 1,
          input: {},
          background: true,
          abortRequested: false,
        };
        const tasks = [
          { ...task, state: { status: "pending", checkpoint: {} } },
          { ...task, state: { status: "running", checkpoint: {} } },
          { ...task, state: { status: "waiting", checkpoint: {}, on: [], policy: "allSettled" } },
          {
            ...task,
            state: { status: "completing", outcome: { status: "completed", result: null } },
          },
          {
            ...task,
            state: { status: "terminal", outcome: { status: "completed", result: null } },
          },
        ] satisfies Extract<StorageWrite, { type: "task" }>["value"][];
        for (const value of tasks) {
          await s.commit([{ type: "task", value }], context);
          expect(await f.authority.hasWork(f.ns)).toBe(value.state.status !== "terminal");
          expect((await f.authority.eligible()).includes(f.ns)).toBe(
            value.state.status !== "terminal",
          );
        }
        const submission = {
          id: idFromNumber<SubmissionId>(31),
          conversationId: idFromNumber<ConversationId>(1),
          type: "input" as const,
        };
        for (const state of [
          { status: "queued" },
          { status: "placed", entry: idFromNumber<EntryId>(32) },
          { status: "done", entry: idFromNumber<EntryId>(32), answer: idFromNumber<EntryId>(33) },
          { status: "unanswered", reason: "test" },
        ] as const) {
          await s.commit([{ type: "submission", value: { ...submission, ...state } }], context);
          const live = state.status === "queued" || state.status === "placed";
          expect(await f.authority.hasWork(f.ns)).toBe(live);
          expect((await f.authority.eligible()).includes(f.ns)).toBe(live);
        }
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    it("mapping requires a local submission, is immutable, and preserves receipt bytes and time across handles", async () => {
      const f = await make();
      const receipt = (await f.authority.receive(f.ns, requestId("map"), input)).item;
      check(receipt.kind === "received", "receipt");
      const original = structuredClone(receipt);
      let s: Storage | undefined;
      try {
        await expect(f.authority.markAdmitted(receipt, idFromNumber(30))).rejects.toThrow();
        s = await f.backend.openStorage(f.lease);
        await s.commit(
          [{ type: "conversation", value: { id: idFromNumber<ConversationId>(30) } }],
          context,
        );
        await expect(f.authority.markAdmitted(receipt, idFromNumber(30))).rejects.toThrow();
        for (const id of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "31"]) {
          await expect(f.authority.markAdmitted(receipt, id as SubmissionId)).rejects.toThrow();
        }
        await publishSubmission(f, 31);
        await f.expire();
        const takeover = await f.authority.claim(f.ns, 45000);
        check(takeover.kind === "claimed", "takeover");
        await f.authority.markAdmitted(receipt, idFromNumber(31));
        await f.authority.markAdmitted(receipt, idFromNumber(31));
        await expect(f.authority.markAdmitted(receipt, idFromNumber(32))).rejects.toThrow();
        const reopened = await f.backend.connectAuthority();
        try {
          expect(await reopened.get(f.ns, receipt.request)).toEqual({
            ...original,
            kind: "admitted",
            submission: 31,
          });
          const duplicate = await reopened.receive(f.ns, receipt.request, input);
          expect(duplicate).toEqual({
            duplicate: true,
            item: { ...original, kind: "admitted", submission: 31 },
          });
          expect((await reopened.received(f.ns)).some((r) => r.request === receipt.request)).toBe(
            false,
          );
        } finally {
          await reopened.close();
        }
      } finally {
        await s?.close(context);
        await f.finish();
      }
    });
    it("closing handles preserves receipts, allocations and history; closed handles reject operations", async () => {
      const f = await make(),
        a = await f.backend.connectAuthority(),
        s = await f.backend.openStorage(f.lease);
      try {
        const minted = await s.mintId<EntryId>();
        await s.commit(
          [
            {
              type: "entry",
              value: { id: minted, conversationId: idFromNumber(1), kind: "survives" },
            },
          ],
          context,
        );
        const receipt = (await a.receive(f.ns, requestId("persist"), input)).item;
        await Promise.all([s.close(context), s.close(context), a.close(), a.close()]);
        await expect(s.mintId()).rejects.toThrow("closed");
        await expect(s.commit([], context)).rejects.toThrow("closed");
        await expect(s.entry(minted, context)).rejects.toThrow("closed");
        await expect(a.receive(f.ns, requestId("closed"), input)).rejects.toThrow();
        const cold = await f.backend.openStorage(f.lease);
        try {
          expect(await cold.mintId()).toBe(minted + 1);
          expect((await cold.entry(minted, context))?.entry.kind).toBe("survives");
          expect(await f.authority.get(f.ns, receipt.request)).toEqual(receipt);
          expect(await f.authority.get(f.ns, requestId("closed"))).toBeUndefined();
        } finally {
          await cold.close(context);
        }
      } finally {
        await s.close(context);
        await a.close();
        await f.finish();
      }
    });
    it("read query/cursor inputs and returned metadata are detached, including queued reads", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      const root = idFromNumber<ConversationId>(1);
      try {
        await s.commit(
          [
            { type: "conversation", value: { id: root } },
            ...[20, 21, 22].map((id) => ({
              type: "entry" as const,
              value: {
                id: idFromNumber<EntryId>(id),
                conversationId: root,
                kind: "detached",
                data: { original: true },
              },
            })),
          ],
          context,
        );
        const query = { conversationId: root, order: "ascending" as const },
          cursor = { after: 20, order: "ascending" as const };
        const pending = s.scanEntries(query, 1, cursor, context);
        query.conversationId = idFromNumber(999);
        cursor.after = 999;
        const page = await pending;
        expect(page.items.map((e) => e.id)).toEqual([21]);
        (page.items[0]!.data as { original: boolean }).original = false;
        expect((await s.entry(idFromNumber(21), context))?.entry.data).toEqual({ original: true });
        const record = await s.conversation(root, context);
        (record as { id: number }).id = 1000;
        expect(await s.conversation(root, context)).toEqual({ id: root });
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    for (const bad of [NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1, "20"])
      it(`rejects unsafe read selectors ${String(bad)} without poisoning handle`, async () => {
        const f = await make(),
          s = await f.backend.openStorage(f.lease);
        try {
          await expect(s.conversation(bad as ConversationId, context)).rejects.toBeInstanceOf(
            StorageRequestError,
          );
          await expect(s.entry(bad as EntryId, context)).rejects.toBeInstanceOf(
            StorageRequestError,
          );
          await expect(
            s.document(idFromNumber<DocumentId>(20), bad as Seq, context),
          ).rejects.toBeInstanceOf(StorageRequestError);
          await expect(
            s.scanEntries({ conversationId: bad as ConversationId }, 2, undefined, context),
          ).rejects.toBeInstanceOf(StorageRequestError);
          expect(await s.mintId()).toBe(f.firstId);
        } finally {
          await s.close(context);
          await f.finish();
        }
      });
    it("malformed task batch cannot partially publish earlier records, discovery or allocation", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      try {
        await expect(
          s.commit(
            [
              { type: "conversation", value: { id: idFromNumber<ConversationId>(99) } },
              {
                type: "task",
                value: { id: 100, conversationId: 1, kind: "bad", state: { status: "invalid" } },
              } as never,
            ],
            context,
          ),
        ).rejects.toBeInstanceOf(StorageRequestError);
        expect(await s.conversation(idFromNumber(99), context)).toBeUndefined();
        expect(await f.authority.hasWork(f.ns)).toBe(false);
        expect(await s.mintId()).toBe(f.firstId);
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
  });
}
