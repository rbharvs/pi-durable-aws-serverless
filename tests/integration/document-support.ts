import type { DocumentId, Storage, StorageWrite } from "@earendil-works/pi-durable";
import { idFromNumber } from "../../src/ids.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import {
  context,
  conversationId,
  historyFixture,
  memoryObjects,
  type HistoryFixtureOptions,
} from "./history-support.ts";
import { provider, wrap, type Query } from "./postgres.ts";

export const documentId = (n: number) => idFromNumber<DocumentId>(n);
export const documentCreate = (n = 20, key = "one") =>
  ({
    type: "document.create",
    record: {
      id: documentId(n),
      kind: "test.doc",
      key,
      scope: { kind: "conversation", conversationId: conversationId(1) },
      history: "rewindable",
      fork: "asOf",
    },
    content: { kind: "base", version: 1, value: { count: 1, rows: [] } },
  }) satisfies Extract<StorageWrite, { type: "document.create" }>;

/** Real paired backend, real authority-issued lease, immutable memory object bytes only. */
export async function documentFixture(label: string, options: HistoryFixtureOptions = {}) {
  const f = await historyFixture(label, { objects: memoryObjects, ...options });
  await f.storage.close(context);
  const trace: Query[] = [];
  const backend = createSqlBackend(
    {
      ...provider,
      connect: async () => wrap(await provider.connect(), options.hook ?? (async () => {}), trace),
    },
    () => ({ ...f.objects, close() {} }),
  );
  const handles: Storage[] = [];
  return {
    ...f,
    backend,
    trace,
    async openStorage() {
      const storage = await backend.openStorage(f.lease);
      handles.push(storage);
      return storage;
    },
    async finish() {
      try {
        await Promise.all(handles.map((storage) => storage.close(context)));
      } finally {
        await f.finish();
      }
    },
  };
}
