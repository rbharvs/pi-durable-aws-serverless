import type {
  ConversationRecord,
  DocumentRecord,
  DocumentAddress,
  DocumentCreate,
  DocumentPoint,
  EntryRecord,
  TaskRecord,
  SubmissionRecord,
} from "@earendil-works/pi-durable";
import type { Row } from "./db.ts";
import type { JsonValue } from "@earendil-works/chord";
import { check, integer, object, computeSha256 } from "../domain.ts";
import { recordShape } from "../storage-validation.ts";
export { assertJson, jsonObject, writeShape } from "../storage-validation.ts";

export interface Records {
  conversation: ConversationRecord;
  entry: EntryRecord;
  task: TaskRecord<JsonValue, JsonValue, JsonValue>;
  submission: SubmissionRecord;
  document: DocumentRecord;
}
export type RecordType = keyof Records;

export function scope(scope: DocumentRecord["scope"]): [string, number] {
  return [
    scope.kind,
    scope.kind === "conversation" ? scope.conversationId : scope.kind === "task" ? scope.taskId : 0,
  ];
}

export function addressHash(address: DocumentAddress | DocumentCreate): string {
  return computeSha256(
    JSON.stringify([
      address.kind,
      scope(address.scope),
      address.key === undefined ? null : address.key,
    ]),
  );
}

export function currentOnly(record: DocumentRecord): boolean {
  return record.scope.kind !== "conversation" || record.history === "latest";
}

export function alive(record: DocumentRecord, at: DocumentPoint): boolean {
  return at === "current"
    ? record.retiredAt === undefined
    : record.createdAt <= at && (record.retiredAt === undefined || record.retiredAt > at);
}

/** Shape policy is checked once; each SQL index is then compared with the decoded source. */
export function validateRecord(value: unknown, row: Row, type: RecordType): void {
  recordShape(value, type);

  const sourceRecord = object(value);

  check(sourceRecord.id === integer(row.id), "Record ID mismatch");

  switch (type) {
    case "conversation": {
      const owner = sourceRecord.owner === undefined ? undefined : object(sourceRecord.owner);

      check(
        (owner?.conversationId ?? null) ===
          (row.owner_conversation_id === null ? null : integer(row.owner_conversation_id)) &&
          (owner?.taskId ?? null) ===
            (row.owner_task_id === null ? null : integer(row.owner_task_id)),
        "Conversation owner mismatch",
      );
      break;
    }

    case "document": {
      // recordShape established the document shape.
      const record = sourceRecord as unknown as DocumentRecord;
      const [kind, owner] = scope(record.scope);

      check(computeSha256(JSON.stringify(record.kind)) === row.kind_hash, "Document kind mismatch");
      check(
        kind === row.scope_kind && owner === integer(row.scope_owner),
        "Document scope mismatch",
      );
      check(addressHash(record) === row.address_hash, "Document address mismatch");
      check(
        record.createdAt === integer(row.created_at) &&
          (record.retiredAt ?? null) === (row.retired_at === null ? null : integer(row.retired_at)),
        "Document lifetime mismatch",
      );
      break;
    }

    case "entry":
      check(
        sourceRecord.conversationId === integer(row.conversation_id),
        "Conversation ID mismatch",
      );
      check(computeSha256(JSON.stringify(sourceRecord.kind)) === row.kind_hash, "Kind mismatch");
      check((sourceRecord.head !== undefined) === row.has_head, "Head mismatch");
      break;

    case "task":
      check(
        sourceRecord.conversationId === integer(row.conversation_id),
        "Conversation ID mismatch",
      );
      check(computeSha256(JSON.stringify(sourceRecord.kind)) === row.kind_hash, "Kind mismatch");
      check(object(sourceRecord.state).status === row.status, "Task state mismatch");
      check(
        sourceRecord.background === row.background &&
          sourceRecord.abortRequested === row.abort_requested,
        "Task flags mismatch",
      );
      break;

    case "submission":
      check(
        sourceRecord.conversationId === integer(row.conversation_id),
        "Conversation ID mismatch",
      );
      check(sourceRecord.status === row.status, "Submission state mismatch");
      check(
        (sourceRecord.requestId === undefined
          ? null
          : computeSha256(JSON.stringify(sourceRecord.requestId))) === row.request_hash,
        "Request mismatch",
      );
      break;
  }
}
