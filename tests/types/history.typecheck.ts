import type { Context } from "@earendil-works/chord";
import type { Storage, EntryId, StorageWrite } from "@earendil-works/pi-durable";
import type { SqlStorage } from "../../src/sql/storage.ts";
import type { Lease } from "../../src/lease.ts";

export function historyBoundary(
  storage: SqlStorage,
  context: Context,
  lease: Lease,
  write: StorageWrite,
  taskWrite: Extract<StorageWrite, { type: "task" }>,
  submissionWrite: Extract<StorageWrite, { type: "submission" }>,
) {
  const complete: Storage = storage;
  void complete;
  void storage.commit([write], context);
  // @ts-expect-error Authority-issued leases cannot be structurally manufactured.
  const forged: Lease = {
    ns: lease.ns,
    owner: lease.owner,
    epoch: lease.epoch,
    until: lease.until,
  };
  void forged;
  const task: Storage["task"] = storage.task.bind(storage);
  const submission: Storage["submission"] = storage.submission.bind(storage);
  const tasks: Storage["scanTasks"] = storage.scanTasks.bind(storage);
  const submissions: Storage["scanSubmissions"] = storage.scanSubmissions.bind(storage);
  const byRequest: Storage["submissionByRequest"] = storage.submissionByRequest.bind(storage);
  void [task, submission, tasks, submissions, byRequest];
  void storage.commit([taskWrite, submissionWrite], context);
  const document: Storage["document"] = storage.document.bind(storage);
  void document;
  const id: Promise<EntryId> = storage.mintId<EntryId>();
  return id;
}
