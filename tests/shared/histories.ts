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
  fixture: ContractFixture,
  operations: readonly Operation[],
): Promise<void> {
  let lease = fixture.lease;
  let storage = await fixture.backend.openStorage(lease);
  let maximum = 1;
  let ordinal = 0;
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
    await storage.close(context);
    storage = await fixture.backend.openStorage(lease);
  };

  await storage.commit(
    [{ type: "conversation", value: { id: idFromNumber<ConversationId>(1) } }],
    context,
  );

  try {
    for (const operation of operations) {
      ordinal++;

      switch (operation.kind) {
        case "mint":
          allocated(await storage.mintId());
          break;

        case "reopen":
          await reopen();
          allocated(await storage.mintId());
          break;

        case "store": {
          const id = maximum + 10;
          const label = `stored-${ordinal}`;

          await storage.commit([entry(id, label)], context);
          maximum = id;
          entries.set(id, label);
          break;
        }

        case "invalid": {
          const id = maximum + 1;

          await assert.rejects(
            storage.commit(
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
          await fixture.expire();

          const claim = await fixture.authority.claim(fixture.ns, 45000);

          check(claim.kind === "claimed", "takeover");

          const old = storage;

          lease = claim.lease;
          storage = await fixture.backend.openStorage(lease);
          await assert.rejects(old.commit([entry(maximum + 1, "stale")], context));
          await assert.rejects(old.mintId());
          await old.close(context);

          break;
        }

        case "unknown-mint": {
          const wrapped = loseAcknowledgement(storage, "mintId", operation.applied);

          await assert.rejects(wrapped.mintId());
          await assert.rejects(wrapped.mintId());
          await wrapped.close(context);
          storage = await fixture.backend.openStorage(lease);
          const next = await storage.mintId();
          assert.equal(
            next,
            maximum + (operation.applied ? 2 : 1),
            "unknown allocation effect after reopen",
          );
          allocated(next);
          break;
        }

        case "unknown-commit": {
          const id = allocated(await storage.mintId());
          const label = `unknown-${ordinal}`;
          const wrapped = loseAcknowledgement(storage, "commit", operation.applied);

          await assert.rejects(wrapped.commit([entry(id, label)], context));
          await assert.rejects(wrapped.mintId());
          await wrapped.close(context);
          storage = await fixture.backend.openStorage(lease);
          if (operation.applied) entries.set(id, label);
          break;
        }

        case "transport-equivalence": {
          const request = requestId(`transport-${ordinal}`);
          const input = { content: `value-${ordinal}`, whenBusy: "followUp" };
          const api = apiHandler({
            connectAuthority: fixture.backend.connectAuthority,
            objects: objects.objects,
            dispatch: { wake: async () => {} },
            event: () => {},
          });
          const first = await api({
            ns: fixture.ns,
            request,
            command: { kind: "inline", input },
          });

          assert(first.ok && !first.duplicate);

          const ref = objects.stage(
            fixture.ns,
            ` { "whenBusy":"followUp", "content":"value-${ordinal}" } `,
          );

          assert.deepEqual(
            await api({ ns: fixture.ns, request, command: { kind: "s3", ...ref } }),
            {
              ...first,
              duplicate: true,
            },
          );
          assert.deepEqual(
            await api({
              ns: fixture.ns,
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
          await storage.close(context);

          const request = requestId(`gap-${ordinal}`);
          const authority = fixture.authority;
          const receipt = await authority.receive(fixture.ns, request, {
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
              recordMints(await fixture.backend.openStorage(lease)),
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
            await authority.markAdmitted(receipt.item, id);
          } finally {
            await second.close(context);
          }

          storage = await fixture.backend.openStorage(lease);

          break;
        }
      }

      // Every completed operation checks safety, not just final state or expected rejection.
      for (const [id, label] of entries) {
        assert.equal((await storage.entry(idFromNumber<EntryId>(id), context))?.entry.data, label);
      }

      const all = (
        await storage.scanEntries(
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
    await storage.close(context);
  }
}
