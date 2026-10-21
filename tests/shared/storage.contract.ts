import { describe, it, expect } from "vitest";
import {
  Harness,
  createRegistry,
  type Storage,
  type ConversationId,
  type EntryId,
  type TaskId,
  type SubmissionId,
  type DocumentId,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai";
import { idFromNumber } from "../../src/ids.ts";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import type { Backend, Authority, InboxItem } from "../../src/authority.ts";
import type { HarnessId } from "../../src/domain.ts";
import { requestId, check } from "../../src/domain.ts";
import type { Lease } from "../../src/lease.ts";
import { acceptedInput as inlineInput } from "./input.ts";
import type { AcceptedInput } from "../../src/authority.ts";
import { canonical, MAX_INLINE_INPUT_BYTES, MAX_INPUT_BYTES } from "../../src/input.ts";
import { computeSha256 as sha } from "../../src/domain.ts";
import type { ObjectKey } from "../../src/payload.ts";

export type LeaseExpiry = Pick<Lease, "ns" | "until">;

export type ContractFixture = {
  backend: Backend;
  ns: HarnessId;
  authority: Authority;
  lease: Lease;
  /** Empty history starts at 2; shared authority contracts settle a real seed submission first. */
  firstId: number;
  expire(): Promise<void>;
  /** Fixture control: memory advances its clock; PG waits for actual server time. */
  advanceAuthorityClock(ms: number): Promise<void>;
  /** Test-only tie setup; PG edits only its isolated schema, memory already has a fixed clock. */
  orderingTies(): Promise<void>;
  /** Set exact test-namespace expiries, expire all of them, and return their actual stored times. */
  setExpiredLeases(expiries: readonly LeaseExpiry[]): Promise<LeaseExpiry[]>;
  finish(): Promise<void>;
};

export type Factory = () => Promise<ContractFixture>;

export const input = inlineInput({ content: "hello", whenBusy: "followUp" });

/** Publish through the real Storage; mapping is not a namespace-seeding shortcut. */
export async function publishSubmission(
  f: Pick<ContractFixture, "backend" | "lease">,
  id: number,
  lease = f.lease,
) {
  const storage = await f.backend.openStorage(lease);
  try {
    await storage.commit(
      [
        {
          type: "submission",
          value: {
            id: idFromNumber<SubmissionId>(id),
            conversationId: idFromNumber<ConversationId>(1),
            type: "input",
            status: "unanswered",
            reason: "fixture",
          },
        },
      ],
      context,
    );
  } finally {
    await storage.close(context);
  }
}

export function contracts(name: string, make: Factory) {
  describe(`${name}/shared authority and durable allocation`, () => {
    for (const invalid of [
      "canonical-hash",
      "noncanonical-json",
      "inline-limit",
      "foreign-published-key",
      "object-limit",
      "object-inline-coherence",
      "invalid-kind",
    ] as const)
      it(`raw receive rejects ${invalid} without receipt namespace creation or existing-state mutation`, async () => {
        const f = await make(),
          json = '{"content":"hello","whenBusy":"followUp"}',
          large = canonical({
            content: "x".repeat(MAX_INLINE_INPUT_BYTES),
            whenBusy: "followUp",
          });
        const foreign = `${f.ns}_invalid` as HarnessId,
          key =
            `p/${invalid === "foreign-published-key" ? "other" : f.ns}/00000000-0000-4000-8000-000000000000` as ObjectKey;
        let accepted: AcceptedInput = {
          hash: input.hash,
          payload: { kind: "inline", json },
        };

        if (invalid === "canonical-hash")
          accepted = { ...accepted, hash: sha("different canonical input") };
        if (invalid === "noncanonical-json")
          accepted = {
            ...accepted,
            payload: { kind: "inline", json: ` ${json} ` },
          };
        if (invalid === "inline-limit")
          accepted = {
            hash: large.sha256,
            payload: { kind: "inline", json: large.json },
          };
        if (["foreign-published-key", "object-limit", "object-inline-coherence"].includes(invalid))
          accepted = {
            hash: input.hash,
            payload: {
              kind: "s3",
              key,
              sha256: input.hash,
              bytes:
                invalid === "object-limit"
                  ? MAX_INPUT_BYTES + 1
                  : invalid === "object-inline-coherence"
                    ? 1
                    : MAX_INLINE_INPUT_BYTES + 1,
            },
          };
        if (invalid === "invalid-kind")
          accepted = {
            hash: input.hash,
            payload: { kind: "unexpected", json },
          } as unknown as AcceptedInput;

        try {
          const eligible = await f.authority.eligible(),
            seed = await f.authority.get(f.ns, requestId("seed"));

          for (const ns of [f.ns, foreign]) {
            await expect(f.authority.receive(ns, requestId("bad"), accepted)).rejects.toThrow();
            await expect(f.authority.get(ns, requestId("bad"))).resolves.toBeUndefined();
            await expect(f.authority.received(ns)).resolves.toEqual([]);
          }

          expect(await f.authority.get(f.ns, requestId("seed"))).toEqual(seed);
          expect(await f.authority.eligible()).toEqual(eligible);
          await expect(f.authority.claim(foreign, 45000)).rejects.toThrow();
          await expect(f.authority.hasWork(foreign)).rejects.toThrow();
        } finally {
          await f.finish();
        }
      });
    it("received on an unknown namespace is an empty read and does not create authority state", async () => {
      const f = await make(),
        ns = `${f.ns}_unknown` as HarnessId;

      try {
        await expect(f.authority.received(ns)).resolves.toEqual([]);
        await expect(f.authority.get(ns, requestId("none"))).resolves.toBeUndefined();
        await expect(f.authority.hasWork(ns)).rejects.toThrow();
        await expect(f.authority.claim(ns, 45000)).rejects.toThrow();
      } finally {
        await f.finish();
      }
    });
    it("same-lease stale-sequence COMMIT preserves records revisions head and allocation", async () => {
      const f = await make(),
        doc = idFromNumber<DocumentId>(20);
      let stale: Storage | undefined, winner: Storage | undefined;

      try {
        const init = await f.backend.openStorage(f.lease);

        try {
          await init.commit(
            [
              {
                type: "conversation",
                value: { id: idFromNumber<ConversationId>(1) },
              },
              {
                type: "document.create",
                record: {
                  id: doc,
                  kind: "sequence",
                  scope: {
                    kind: "conversation",
                    conversationId: idFromNumber<ConversationId>(1),
                  },
                  history: "rewindable",
                  fork: "asOf",
                },
                content: {
                  kind: "base",
                  version: 1,
                  value: { state: "initial" },
                },
              },
            ],
            context,
          );
        } finally {
          await init.close(context);
        }

        stale = await f.backend.openStorage(f.lease);
        winner = await f.backend.openStorage(f.lease);

        const seq = await winner.commit(
          [
            {
              type: "document.change",
              id: doc,
              content: { kind: "base", version: 1, value: { state: "winner" } },
            },
          ],
          context,
        );
        // An acknowledged mint precedes the rejected business batch and must survive it.
        const minted = await winner.mintId();
        const before = {
          current: await winner.document(doc, "current", context),
          history: await winner.document(doc, seq, context),
        };

        await expect(
          stale.commit(
            [
              {
                type: "entry",
                value: {
                  id: idFromNumber<EntryId>(1000),
                  conversationId: idFromNumber<ConversationId>(1),
                  kind: "must-not-exist",
                },
              },
              {
                type: "document.change",
                id: doc,
                content: {
                  kind: "base",
                  version: 2,
                  value: { state: "stale" },
                },
              },
            ],
            context,
          ),
        ).rejects.toThrow("Stale");
        expect(await winner.entry(idFromNumber<EntryId>(1000), context)).toBeUndefined();
        expect({
          current: await winner.document(doc, "current", context),
          history: await winner.document(doc, seq, context),
        }).toEqual(before);
        expect(await winner.mintId()).toBe(minted + 1);
        expect(await winner.commit([], context)).toBe(seq + 1);
      } finally {
        await stale?.close(context);
        await winner?.close(context);
        await f.finish();
      }
    });
    it("rejected task batch preserves authoritative work discovery and counters", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);

      try {
        await f.expire();

        const seq = await s.commit([], context),
          minted = await s.mintId();

        expect(await f.authority.hasWork(f.ns)).toBe(false);
        expect(await f.authority.eligible()).not.toContain(f.ns);
        await expect(
          s.commit(
            [
              {
                type: "task",
                value: {
                  id: idFromNumber<TaskId<null>>(20),
                  conversationId: idFromNumber<ConversationId>(1),
                  kind: "background",
                  version: 1,
                  input: {},
                  background: true,
                  abortRequested: false,
                  state: { status: "running", checkpoint: {} },
                },
              },
              {
                type: "conversation",
                value: { id: idFromNumber<ConversationId>(1) },
              },
              {
                type: "conversation",
                value: { id: idFromNumber<ConversationId>(1) },
              },
            ],
            context,
          ),
        ).rejects.toThrow();
        expect(await s.task(idFromNumber<TaskId>(20), context)).toBeUndefined();
        expect(await s.conversation(idFromNumber<ConversationId>(1), context)).toBeUndefined();
        expect(await f.authority.hasWork(f.ns)).toBe(false);
        expect(await f.authority.eligible()).not.toContain(f.ns);
        expect(await s.mintId()).toBe(minted + 1);
        expect(await s.commit([], context)).toBe(seq + 1);
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    it("pending limit eight uses receivedAt before request identity across clock changes", async () => {
      const f = await make();

      try {
        const receipts: InboxItem[] = [];

        for (let i = 11; i >= 0; i--) {
          receipts.push(
            (
              await f.authority.receive(
                f.ns,
                requestId(`time-${String(i).padStart(2, "0")}`),
                input,
              )
            ).item,
          );
          await f.advanceAuthorityClock(5);
        }

        expect(receipts.every((r, i) => i === 0 || r.receivedAt > receipts[i - 1].receivedAt)).toBe(
          true,
        );
        expect(await f.authority.received(f.ns)).toEqual(receipts.slice(0, 8));
      } finally {
        await f.finish();
      }
    });
    for (const setupGapMs of [0, 100])
      it(`eligible limit five orders actual lease expiry before namespace and creation order${setupGapMs ? " with 100ms setup gaps" : ""}`, async () => {
        const f = await make();

        try {
          const targets: LeaseExpiry[] = [];

          for (const [i, offset] of [71, 11, 51, 1, 61, 21, 41].entries()) {
            const ns = `${f.ns}_${6 - i}` as HarnessId;

            await f.authority.receive(ns, requestId("one"), input);
            targets.push({ ns, until: 1000000 + offset });
            if (setupGapMs) await f.advanceAuthorityClock(setupGapMs);
          }

          // Round trips can exceed the expiry differences. Control persisted expiry, not
          // claim response timing; the delayed variant covers DSQL-scale setup latency.
          const leases = await f.setExpiredLeases(targets);

          expect(leases).toEqual(targets);
          expect(new Set(leases.map((l) => l.until)).size).toBeGreaterThan(3);

          const expected = [...leases]
            .sort((a, b) => a.until - b.until || (a.ns < b.ns ? -1 : a.ns > b.ns ? 1 : 0))
            .slice(0, 5)
            .map((l) => l.ns);

          expect(expected).not.toEqual(leases.slice(0, 5).map((l) => l.ns));
          expect(expected).not.toEqual(
            leases
              .map((l) => l.ns)
              .sort()
              .slice(0, 5),
          );
          expect(await f.authority.eligible()).toEqual(expected);
        } finally {
          await f.finish();
        }
      });
    it("authority detaches accepted input and admission identity before awaiting", async () => {
      const f = await make();

      try {
        const json = '{"content":"hello","whenBusy":"followUp"}',
          mutable = {
            hash: input.hash,
            payload: { kind: "inline" as const, json },
          };
        const pending = f.authority.receive(f.ns, requestId("a"), mutable);

        mutable.payload.json = '{"content":"changed","whenBusy":"followUp"}';

        const receipt = (await pending).item;

        expect(receipt.payload).toEqual({ kind: "inline", json });
        expect((await f.authority.get(f.ns, requestId("a")))?.payload).toEqual({
          kind: "inline",
          json,
        });
        check(receipt.kind === "received", "received");
        await f.authority.receive(f.ns, requestId("b"), input);

        await publishSubmission(f, 30);
        const identity = { ...receipt },
          mark = f.authority.markAdmitted(identity, idFromNumber(30));

        identity.request = requestId("b");
        await mark;
        expect(await f.authority.get(f.ns, requestId("a"))).toMatchObject({
          kind: "admitted",
          submission: 30,
        });
        expect(await f.authority.get(f.ns, requestId("b"))).toMatchObject({
          kind: "received",
        });
      } finally {
        await f.finish();
      }
    });
    it("MAX_SAFE can be allocated once, repeated exhaustion keeps raw handle usable", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);

      try {
        await s.commit(
          [
            {
              type: "conversation",
              value: {
                id: idFromNumber<ConversationId>(Number.MAX_SAFE_INTEGER - 1),
              },
            },
          ],
          context,
        );
        expect(await s.mintId()).toBe(Number.MAX_SAFE_INTEGER);
        for (let i = 0; i < 2; i++)
          await expect(s.mintId()).rejects.toMatchObject({
            message: "ID space is exhausted",
          });
        expect(
          await s.conversation(idFromNumber<ConversationId>(Number.MAX_SAFE_INTEGER - 1), context),
        ).toBeDefined();
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    it("background tasks and queued submissions each keep work discoverable", async () => {
      const f = await make(),
        s = await f.backend.openStorage(f.lease);
      const task = {
        id: idFromNumber<TaskId<null>>(20),
        conversationId: idFromNumber<ConversationId>(1),
        kind: "background",
        version: 1,
        input: {},
        background: true,
        abortRequested: false,
      };
      const submission = {
        id: idFromNumber<SubmissionId>(21),
        conversationId: idFromNumber<ConversationId>(1),
        type: "input" as const,
      };

      try {
        expect(await f.authority.hasWork(f.ns)).toBe(false);
        await s.commit(
          [
            {
              type: "task",
              value: { ...task, state: { status: "running", checkpoint: {} } },
            },
          ],
          context,
        );
        expect(await f.authority.hasWork(f.ns)).toBe(true);
        await s.commit(
          [
            {
              type: "task",
              value: {
                ...task,
                state: {
                  status: "terminal",
                  outcome: { status: "completed", result: null },
                },
              },
            },
            { type: "submission", value: { ...submission, status: "queued" } },
          ],
          context,
        );
        expect(await f.authority.hasWork(f.ns)).toBe(true);
        await f.expire();
        expect(await f.authority.eligible()).toContain(f.ns);
        await s.commit(
          [
            {
              type: "submission",
              value: {
                ...submission,
                status: "unanswered",
                reason: "test end",
              },
            },
          ],
          context,
        );
        expect(await f.authority.hasWork(f.ns)).toBe(false);
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    it("immutable receive races, mapping after takeover, stale release and namespace discovery", async () => {
      const f = await make();

      try {
        const a = f.authority;
        const r = requestId("same");
        const replies = await Promise.all(
          Array.from({ length: 4 }, () => a.receive(f.ns, r, input)),
        );

        expect(replies.filter((v) => !v.duplicate)).toHaveLength(1);
        expect(new Set(replies.map((v) => v.item.receivedAt)).size).toBe(1);
        await expect(
          a.receive(f.ns, r, inlineInput({ content: "different", whenBusy: "followUp" })),
        ).rejects.toThrow("mismatch");

        const received = replies[0].item;

        check(received.kind === "received", "received");
        expect(await a.claim(f.ns, 45000)).toEqual({ kind: "held" });
        await f.expire();

        const next = await a.claim(f.ns, 45000);

        check(next.kind === "claimed", "claimed");
        expect(next.lease.epoch).toBe(f.lease.epoch + 1);
        expect(await a.release(f.lease)).toBe("lost");
        await publishSubmission(f, 30, next.lease);
        await a.markAdmitted(received, idFromNumber(30));
        await a.markAdmitted(received, idFromNumber(30));
        await expect(a.markAdmitted(received, idFromNumber(31))).rejects.toThrow("mapping");
      } finally {
        await f.finish();
      }
    });
    it("equal receipt timestamps use request identity as the stable capped tie-breaker", async () => {
      const f = await make();

      try {
        const receipts: InboxItem[] = [];

        for (let i = 11; i >= 0; i--)
          receipts.push(
            (await f.authority.receive(f.ns, requestId(`r${String(i).padStart(2, "0")}`), input))
              .item,
          );
        await f.orderingTies();

        const tied = await Promise.all(receipts.map((r) => f.authority.get(f.ns, r.request)));

        expect(new Set(tied.map((r) => r!.receivedAt)).size).toBe(1);

        const expected = [...receipts]
          .sort((a, b) => (a.request < b.request ? -1 : 1))
          .slice(0, 8)
          .map((r) => r.request);

        expect((await f.authority.received(f.ns)).map((r) => r.request)).toEqual(expected);
      } finally {
        await f.finish();
      }
    });
    it("equal eligibility times use namespace as the stable capped tie-breaker", async () => {
      const f = await make();

      try {
        for (let i = 8; i >= 0; i--)
          await f.authority.receive(`${f.ns}_${i}` as HarnessId, requestId("one"), input);
        await f.orderingTies();

        const eligible = await f.authority.eligible();

        expect(eligible).toHaveLength(5);
        expect(eligible).toEqual(Array.from({ length: 5 }, (_, i) => `${f.ns}_${i}`));
      } finally {
        await f.finish();
      }
    });
    it("acknowledged mint persists across lost handle/reopen; same-lease allocation order", async () => {
      const f = await make();
      const handles: Storage[] = [];

      try {
        const a = await f.backend.openStorage(f.lease);

        handles.push(a);
        expect(await a.mintId()).toBe(f.firstId);

        const b = await f.backend.openStorage(f.lease);

        handles.push(b);
        expect(await b.mintId()).toBe(f.firstId + 1);
        expect(await a.mintId()).toBe(f.firstId + 2);
        await a.close(context);

        const c = await f.backend.openStorage(f.lease);

        handles.push(c);
        expect(await c.mintId()).toBe(f.firstId + 3);
        await expect(
          c.commit(
            [
              {
                type: "conversation",
                value: { id: idFromNumber<ConversationId>(1) },
              },
              {
                type: "conversation",
                value: { id: idFromNumber<ConversationId>(1) },
              },
            ],
            context,
          ),
        ).rejects.toThrow();
        await c.close(context);

        const d = await f.backend.openStorage(f.lease);

        handles.push(d);
        expect(await d.mintId()).toBe(f.firstId + 4);
      } finally {
        await Promise.all(handles.map((s) => s.close(context)));
        await f.finish();
      }
    });
    it("real Pi callback/init failure consumes durable IDs with no failed business records", async () => {
      const f = await make();
      let h: Awaited<ReturnType<typeof Harness.open>> | undefined,
        s: Storage | undefined,
        seen = 0;
      const options = () => ({
        models: createModels(),
        registry: createRegistry(),
      });

      try {
        s = await f.backend.openStorage(f.lease);
        h = await Harness.open(s, options(), context);

        const root = await h.root(context);

        await expect(
          root.commit(async (tx) => {
            seen = (await tx.createConversation({ ownership: { kind: "ownerless" } })).id;

            throw Error("callback cut");
          }, context),
        ).rejects.toThrow("callback cut");
        await h.close(context);
        s = await f.backend.openStorage(f.lease);
        expect(await s.mintId()).toBeGreaterThan(seen);
        h = await Harness.open(s, options(), context);
        await expect(
          h.createConversation(
            {
              ownership: { kind: "ownerless" },
              init: (_tx, id) => {
                seen = id;

                throw Error("init cut");
              },
            },
            context,
          ),
        ).rejects.toThrow("init cut");
        await h.close(context);
        s = await f.backend.openStorage(f.lease);
        expect(await s.mintId()).toBeGreaterThan(seen);
        expect(await s.conversation(idFromNumber<ConversationId>(seen), context)).toBeUndefined();
      } finally {
        await h?.close(context);
        await s?.close(context);
        await f.finish();
      }
    });
    it("invocation-time clone, detached reads and commit sequence", async () => {
      const f = await make();
      const s = await f.backend.openStorage(f.lease);

      try {
        const id = idFromNumber<EntryId>(20);
        const write = {
          type: "entry" as const,
          value: {
            id,
            conversationId: idFromNumber<ConversationId>(1),
            kind: "test",
            data: { nested: "before" },
          },
        };
        const p = s.commit([write], context);

        write.value.data.nested = "after";

        const seq = await p;
        const result = await s.entry(id, context);

        expect(result?.commitSeq).toBe(seq);
        expect(result?.entry.data).toEqual({ nested: "before" });
        (result!.entry.data as { nested: string }).nested = "mutated";
        expect((await s.entry(id, context))?.entry.data).toEqual({
          nested: "before",
        });
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
    it("stale sequence/owner open, mint and commit reject with no business effect", async () => {
      const f = await make();
      const old = await f.backend.openStorage(f.lease),
        other = await f.backend.openStorage(f.lease);
      let current: Storage | undefined;

      try {
        const n = await old.mintId();

        await other.commit([], context);
        await expect(old.mintId()).rejects.toThrow("Stale");
        await f.expire();

        const next = await f.authority.claim(f.ns, 45000);

        check(next.kind === "claimed", "claimed");
        await expect(f.backend.openStorage(f.lease)).rejects.toThrow("Stale");
        await expect(
          other.commit(
            [
              {
                type: "conversation",
                value: { id: idFromNumber<ConversationId>(40) },
              },
            ],
            context,
          ),
        ).rejects.toThrow("Stale");
        current = await f.backend.openStorage(next.lease);
        expect(
          await current.conversation(idFromNumber<ConversationId>(40), context),
        ).toBeUndefined();
        expect(await current.mintId()).toBeGreaterThan(n);
      } finally {
        await old.close(context);
        await other.close(context);
        await current?.close(context);
        await f.finish();
      }
    });
    it("logical size and write count bounds reject without publication", async () => {
      const f = await make();
      const s = await f.backend.openStorage(f.lease);

      try {
        await expect(
          s.commit(
            [
              {
                type: "entry",
                value: {
                  id: idFromNumber<EntryId>(20),
                  conversationId: idFromNumber<ConversationId>(1),
                  kind: "big",
                  data: "x".repeat(32 * 1024 * 1024),
                },
              },
            ],
            context,
          ),
        ).rejects.toThrow("Logical commit");
        await expect(
          s.commit(
            Array.from({ length: 1000 }, () => ({
              type: "conversation" as const,
              value: { id: idFromNumber<ConversationId>(1) },
            })),
            context,
          ),
        ).rejects.toThrow();
        expect(await s.entry(idFromNumber<EntryId>(20), context)).toBeUndefined();
        expect(await s.mintId()).toBe(f.firstId);
      } finally {
        await s.close(context);
        await f.finish();
      }
    });
  });
}
