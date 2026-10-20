import assert from "node:assert/strict";
import type { Storage, EntryId, ConversationId } from "@earendil-works/pi-durable";
import { Harness, createRegistry } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai";
import { idFromNumber } from "../../src/ids.ts";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import type { ContractFixture } from "./storage.contract.ts";
import { loseAcknowledgement } from "./faults.ts";
import { objectFixture as memoryObjectsFixture } from "./objects.ts";
import { apiHandler } from "../../src/submit.ts";
import { requestId, check } from "../../src/domain.ts";

export type Operation =
  | {
      kind:
        | "mint"
        | "reopen"
        | "takeover"
        | "store"
        | "invalid"
        | "transport-equivalence"
        | "submit-mark-gap";
    }
  | { kind: "unknown-mint" | "unknown-commit"; applied: boolean };

/** Small independent event ledger: returned IDs, stored generated entries, immutable requests.
 * No SQL, indexes, payload representation or retry selectors or production work predicate in the oracle.
 */
export async function runHistory(
  f: ContractFixture,
  operations: readonly Operation[],
): Promise<void> {
  let lease = f.lease,
    s = await f.backend.openStorage(lease),
    maximum = 1,
    ordinal = 0;
  const entries = new Map<number, string>();
  const objects = memoryObjectsFixture();

  function allocated(id: number) {
    assert(id > maximum, `ID ${id} reused or out of order after ${maximum}`);
    maximum = id;

    return id;
  }

  const entry = (id: number, label: string) => ({
    type: "entry" as const,
    value: {
      id: idFromNumber<EntryId>(id),
      conversationId: idFromNumber<ConversationId>(1),
      kind: "history",
      data: label,
    },
  });

  function recordMints(storage: Storage): Storage {
    return new Proxy(storage, {
      get(target, key) {
        if (key === "then") return undefined;
        if (key === "mintId") return async () => allocated(await target.mintId());

        const v = Reflect.get(target, key);

        return typeof v === "function" ? v.bind(target) : v;
      },
    });
  }

  const reopen = async () => {
    await s.close(context);
    s = await f.backend.openStorage(lease);
  };

  await s.commit(
    [{ type: "conversation", value: { id: idFromNumber<ConversationId>(1) } }],
    context,
  );

  try {
    for (const op of operations) {
      ordinal++;

      switch (op.kind) {
        case "mint":
          allocated(await s.mintId());
          break;

        case "reopen":
          await reopen();
          allocated(await s.mintId());
          break;

        case "store": {
          const id = maximum + 10,
            label = `stored-${ordinal}`;

          await s.commit([entry(id, label)], context);
          maximum = id;
          entries.set(id, label);
          break;
        }

        case "invalid": {
          const id = maximum + 1;

          await assert.rejects(
            s.commit(
              [
                entry(id, "never"),
                {
                  type: "conversation",
                  value: { id: idFromNumber<ConversationId>(1) },
                },
              ],
              context,
            ),
          );
          break;
        }

        case "takeover": {
          await f.expire();

          const c = await f.authority.claim(f.ns, 45000);

          check(c.kind === "claimed", "takeover");

          const old = s;

          lease = c.lease;
          s = await f.backend.openStorage(lease);
          await assert.rejects(old.commit([entry(maximum + 1, "stale")], context));
          await assert.rejects(old.mintId());
          await old.close(context);

          break;
        }

        case "unknown-mint": {
          const wrapped = loseAcknowledgement(s, "mintId", op.applied);

          await assert.rejects(wrapped.mintId());
          await assert.rejects(wrapped.mintId());
          await wrapped.close(context);
          s = await f.backend.openStorage(lease);
          const next = await s.mintId();
          assert.equal(
            next,
            maximum + (op.applied ? 2 : 1),
            "unknown allocation effect after reopen",
          );
          allocated(next);
          break;
        }

        case "unknown-commit": {
          const id = allocated(await s.mintId()),
            label = `unknown-${ordinal}`;
          const wrapped = loseAcknowledgement(s, "commit", op.applied);

          await assert.rejects(wrapped.commit([entry(id, label)], context));
          await assert.rejects(wrapped.mintId());
          await wrapped.close(context);
          s = await f.backend.openStorage(lease);
          if (op.applied) entries.set(id, label);
          break;
        }

        case "transport-equivalence": {
          const request = requestId(`transport-${ordinal}`),
            input = { content: `value-${ordinal}`, whenBusy: "followUp" },
            api = apiHandler({
              connectAuthority: f.backend.connectAuthority,
              objects: objects.objects,
              dispatch: { wake: async () => {} },
              event: () => {},
            });
          const first = await api({
            ns: f.ns,
            request,
            command: { kind: "inline", input },
          });

          assert(first.ok && !first.duplicate);

          const ref = objects.stage(
            f.ns,
            ` { "whenBusy":"followUp", "content":"value-${ordinal}" } `,
          );

          assert.deepEqual(await api({ ns: f.ns, request, command: { kind: "s3", ...ref } }), {
            ...first,
            duplicate: true,
          });
          assert.deepEqual(
            await api({
              ns: f.ns,
              request,
              command: {
                kind: "inline",
                input: { ...input, content: "changed" },
              },
            }),
            { ok: false, code: "request-conflict", retryable: false },
          );

          break;
        }

        case "submit-mark-gap": {
          await s.close(context);

          const request = requestId(`gap-${ordinal}`),
            a = f.authority;
          const receipt = await a.receive(f.ns, request, {
            hash: (await import("../../src/input.ts")).canonical({
              content: "gap",
              whenBusy: "followUp",
            }).sha256,
            payload: {
              kind: "inline",
              json: '{"content":"gap","whenBusy":"followUp"}',
            },
          });

          check(receipt.item.kind === "received", "received");

          const open = async () =>
            Harness.open(
              recordMints(await f.backend.openStorage(lease)),
              { models: createModels(), registry: createRegistry() },
              context,
            );
          const first = await open();
          let id;

          try {
            const root = await first.root(context);

            id = (await root.submit({ type: "input", content: "gap", requestId: request }, context))
              .id;
          } finally {
            await first.close(context);
          }

          const second = await open();

          try {
            const root = await second.root(context);

            assert.equal(
              (await root.submit({ type: "input", content: "gap", requestId: request }, context))
                .id,
              id,
            );
            await a.markAdmitted(receipt.item, id);
          } finally {
            await second.close(context);
          }

          s = await f.backend.openStorage(lease);

          break;
        }
      }

      // Every completed operation checks safety, not just final state or expected rejection.
      for (const [id, label] of entries)
        assert.equal((await s.entry(idFromNumber<EntryId>(id), context))?.entry.data, label);

      const all = (
        await s.scanEntries(
          {
            conversationId: idFromNumber<ConversationId>(1),
            order: "ascending",
          },
          10000,
          undefined,
          context,
        )
      ).items.filter((e) => e.kind === "history");

      assert.equal(all.length, entries.size);
      assert(!all.some((e) => e.data === "never" || e.data === "stale"));
    }
  } finally {
    await s.close(context);
  }
}
