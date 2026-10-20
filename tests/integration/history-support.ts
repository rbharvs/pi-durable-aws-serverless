import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import {
  ROOT_CONVERSATION_ID as rootId,
  type ConversationId,
  type ConversationRecord,
  type EntryId,
  type EntryRecord,
} from "@earendil-works/pi-durable";
import { expect } from "vitest";
import { check, computeSha256, requestId, type HarnessId } from "../../src/domain.ts";
import { idFromNumber } from "../../src/ids.ts";
import type { Lease } from "../../src/lease.ts";
import type { PayloadObjects } from "../../src/s3-payload.ts";
import { BEGIN_READ } from "../../src/sql/db.ts";
import { SqlStorage } from "../../src/sql/storage.ts";
import { acceptedInput } from "../shared/input.ts";
import {
  OWNED_TABLES,
  historyConnection,
  namespace,
  provider,
  type QueryHook,
} from "./postgres.ts";

export { context, rootId };
export const conversationId = (n: number) => idFromNumber<ConversationId>(n);
export const entryId = (n: number) => idFromNumber<EntryId>(n);
export const conversation = (n = 1, extra: Omit<ConversationRecord, "id"> = {}) => ({
  type: "conversation" as const,
  value: { id: conversationId(n), ...extra },
});
export const entry = (n: number, extra: Partial<Omit<EntryRecord, "id">> = {}) => ({
  type: "entry" as const,
  value: { id: entryId(n), conversationId: conversationId(1), kind: "history", ...extra },
});

/** Use extra record fields with entry(); use dataEntry() when the argument is payload data. */
export const dataEntry = (n: number, data: EntryRecord["data"]) =>
  entry(n, { kind: "payload", data });

/** Small history cases must not silently start publishing objects. Object I/O has its own suite. */
const inlineOnly: PayloadObjects = {
  async put() {
    throw Error("Unexpected object publication in a small-history test");
  },
  async get() {
    throw Error("Unexpected object fetch in a small-history test");
  },
};

/** Only immutable object bytes are emulated. SQL and storage are always real. */
function payloadObjects() {
  const bodies = new Map<string, Buffer>();
  const operations: { operation: "PUT" | "GET"; key: string; json?: string }[] = [];

  return {
    bodies,
    operations,
    objects(ns: HarnessId): PayloadObjects {
      return {
        async put(json) {
          const key = `p/${ns}/${randomUUID()}`;
          const body = Buffer.from(json);

          check(!bodies.has(key), "Create-only object collision");
          bodies.set(key, body);
          operations.push({ operation: "PUT", key, json });

          return {
            kind: "s3",
            key: key as Awaited<ReturnType<PayloadObjects["put"]>>["key"],
            sha256: computeSha256(body),
            bytes: body.length,
          };
        },
        async get(ref) {
          check(ref.key.startsWith(`p/${ns}/`), "Foreign object namespace");
          operations.push({ operation: "GET", key: ref.key });
          const body = bodies.get(ref.key);

          check(body, "Missing object");
          check(body.length === ref.bytes, "Object length mismatch");
          check(computeSha256(body) === ref.sha256, "Object checksum mismatch");
          const json = new TextDecoder("utf-8", { fatal: true }).decode(body);

          JSON.parse(json);

          return json;
        },
      };
    },
  };
}

/** Explicitly select memory objects, or wrap them to inject object-adapter faults. */
export type ObjectSelector = (ns: HarnessId, memory: PayloadObjects) => PayloadObjects;
export const memoryObjects: ObjectSelector = (_ns, memory) => memory;

export type HistoryFixtureOptions = { hook?: QueryHook; objects?: ObjectSelector };
export type HistoryOpenOptions = { hook?: QueryHook; lease?: Lease };

type HistoryHandle = { storage: SqlStorage } & Awaited<ReturnType<typeof historyConnection>>;

function throwFailures(errors: unknown[], message: string) {
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, message, { cause: errors[0] });
}

export function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });

  return { promise, resolve };
}

/** Explicit operation gate; no timing assumption and no fake database error. */
export function queryGate(phase: "before" | "after", matches: (sql: string) => boolean) {
  const reached = deferred();
  const proceed = deferred();
  let armed = false;
  const hook: QueryHook = async (at, sql) => {
    if (!armed || phase !== at || !matches(sql)) return;
    armed = false;
    reached.resolve();
    await proceed.promise;
  };

  return { hook, reached: reached.promise, release: proceed.resolve, arm: () => (armed = true) };
}

/** Raw adapter only: no Harness, full Storage implementation, or provider/model calls. */
export async function historyFixture(label: string, options: HistoryFixtureOptions = {}) {
  const owned = await namespace(label);
  const control = owned.authority.inspectionDb;
  const memory = payloadObjects();
  const handles: HistoryHandle[] = [];
  const opening = new Set<Promise<HistoryHandle>>();
  const cleanupErrors: unknown[] = [];
  let finishing: Promise<void> | undefined;

  // Shared by normal teardown and setup failure. Do not let one failure hide another
  // or prevent the other handles, leak checks, exact row cleanup, or authority close.
  const cleanup = async () => {
    await Promise.allSettled(opening);
    const closed = await Promise.allSettled(
      handles.map(async ({ storage }) => storage.close(context)),
    );

    for (const result of closed)
      if (result.status === "rejected") cleanupErrors.push(result.reason);

    for (const table of ["inline_payloads", "s3_payloads"]) {
      try {
        const orphan = await control.query(
          `SELECT p.payload_ref FROM ${table} p WHERE p.ns=$1
           AND NOT EXISTS(SELECT 1 FROM records r WHERE r.ns=p.ns AND r.payload_ref=p.payload_ref)
           AND NOT EXISTS(SELECT 1 FROM revisions r WHERE r.ns=p.ns AND r.payload_ref=p.payload_ref)`,
          [owned.ns],
        );

        expect(orphan, `Unreferenced history payload rows in ${table}`).toEqual([]);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }

    // Teardown never releases leases; it deletes only the exact owned namespace.
    for (const table of OWNED_TABLES) {
      try {
        await control.query(`DELETE FROM ${table} WHERE ns=$1`, [owned.ns]);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    try {
      await owned.authority.close();
    } catch (error) {
      cleanupErrors.push(error);
    }
    throwFailures(cleanupErrors, "History fixture cleanup failed");
  };
  const finish = () => (finishing ??= cleanup());

  try {
    // Exercise real namespace creation through ingress, not a forged ownership row or lease.
    await owned.authority.receive(
      owned.ns,
      requestId("fixture"),
      acceptedInput({ content: "history fixture", whenBusy: "followUp" }),
    );
    const claimed = await owned.authority.claim(owned.ns, 180000);

    check(claimed.kind === "claimed", "Fresh history namespace unexpectedly held");
    const lease = claimed.lease;
    const objects = options.objects
      ? options.objects(owned.ns, memory.objects(owned.ns))
      : inlineOnly;
    const open = ({ hook, lease: selectedLease = lease }: HistoryOpenOptions = {}) => {
      if (finishing) return Promise.reject<HistoryHandle>(Error("History fixture is finishing"));
      const operation = (async () => {
        check(selectedLease.ns === owned.ns, "Foreign history fixture lease");
        const connection = await historyConnection(hook);

        try {
          const storage = await SqlStorage.open(connection.db, selectedLease, provider, objects);
          const handle = { storage, ...connection };

          handles.push(handle);
          return handle;
        } catch (error) {
          try {
            await connection.db.end();
          } catch (closeError) {
            cleanupErrors.push(closeError);
            throw new AggregateError([error, closeError], "History open and close failed", {
              cause: error,
            });
          }
          throw error;
        }
      })();

      opening.add(operation);
      void operation.then(
        () => opening.delete(operation),
        () => opening.delete(operation),
      );
      return operation;
    };
    const first = await open({ hook: options.hook });

    return {
      ns: owned.ns,
      authority: owned.authority,
      control,
      lease,
      objects,
      memory,
      ...first,
      open,
      /** Close the initial handle, then open a tracked handle with the same object adapter. */
      async cold(options: HistoryOpenOptions = {}) {
        await first.storage.close(context);
        return (await open(options)).storage;
      },
      async head() {
        return (await control.query("SELECT * FROM harness WHERE ns=$1", [owned.ns]))[0]!;
      },
      async snapshot() {
        await control.query(BEGIN_READ);
        try {
          const result: Record<string, unknown> = {};

          for (const table of ["harness", "records", "revisions", "inline_payloads", "s3_payloads"])
            result[table] = await control.query(`SELECT * FROM ${table} WHERE ns=$1 ORDER BY 1,2`, [
              owned.ns,
            ]);
          await control.query("COMMIT");

          return result;
        } catch (error) {
          try {
            await control.query("ROLLBACK");
          } catch (rollbackError) {
            throw new AggregateError([error, rollbackError], "History snapshot rollback failed", {
              cause: error,
            });
          }
          throw error;
        }
      },
      finish,
    };
  } catch (error) {
    try {
      await finish();
    } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], "History setup and cleanup failed", {
        cause: error,
      });
    }
    throw error;
  }
}

export type HistoryFixture = Awaited<ReturnType<typeof historyFixture>>;
