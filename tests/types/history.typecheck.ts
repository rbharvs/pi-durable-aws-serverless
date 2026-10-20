import type { Context } from "@earendil-works/chord";
import type { Storage, EntryId, StorageWrite } from "@earendil-works/pi-durable";
import type { SqlStorage } from "../../src/sql/storage.ts";
import type { Lease } from "../../src/lease.ts";

export function historyBoundary(
  storage: SqlStorage,
  context: Context,
  lease: Lease,
  write: StorageWrite,
) {
  // @ts-expect-error History is not a complete Pi Storage.
  const complete: Storage = storage;
  void complete;
  // @ts-expect-error Deferred writes cannot be committed by the history adapter.
  void storage.commit([write], context);
  // @ts-expect-error Authority-issued leases cannot be structurally manufactured.
  const forged: Lease = {
    ns: lease.ns,
    owner: lease.owner,
    epoch: lease.epoch,
    until: lease.until,
  };
  void forged;
  // @ts-expect-error No task placeholder.
  void storage.task;
  // @ts-expect-error No document placeholder.
  void storage.document;
  const id: Promise<EntryId> = storage.mintId<EntryId>();
  return id;
}
